// Package cluster 负责组建集群：启动本节点的 gRPC 服务（磁盘数据面 + 节点控制面）、
// 发现并连接 peer、按确定性顺序聚合出全局磁盘列表，并校验/固化布局。
package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"sync/atomic"
	"time"

	"github.com/kxj/gos3/internal/disk"
	"github.com/kxj/gos3/internal/format"
	"github.com/kxj/gos3/internal/health"
	"github.com/kxj/gos3/internal/lock"
	"github.com/kxj/gos3/internal/peer"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

const maxMessageSize = 128 << 20 // gRPC 单条消息上限：128 MiB

// Options 是构建集群所需的参数。
type Options struct {
	Advertise   string        // 本节点对外广播的 gRPC 地址（其他节点用它访问本节点）
	Listen      string        // 本节点 gRPC 实际监听地址
	LocalDirs   []string      // 本节点的本地数据目录
	Peers       []string      // 其他节点的 gRPC 地址列表
	Logger      *slog.Logger  // 日志器
	DialTimeout time.Duration // 连接对端 peer 的超时时间
}

// Cluster 是一个已组建的分布式集群视图：
// 它把本地磁盘与各远端节点的磁盘聚合成一个统一的 disk.Disk 切片，
// 屏蔽本地/远程差异，供纠删码存储层使用。
type Cluster struct {
	disks      []disk.Disk        // 聚合后的全部磁盘（本地 + 远程），顺序全局一致
	grpcServer *grpc.Server       // 本节点的 gRPC 服务（DiskService + PeerService + LockService）
	peers      *peer.Manager      // 到各 peer 的连接与在线状态
	lockers    []lock.NodeLocker  // 命名空间锁的节点集合（本节点 + 在线 peer）
	diskSkips  []func() bool      // 与 disks 对应：远端盘所在节点离线时跳过健康探测
	stopProbe  context.CancelFunc // 停止后台探测循环
	log        *slog.Logger
}

// Build 组建集群：
//  1. 把本节点的本地目录封装为本地磁盘；
//  2. 启动 gRPC 服务并注册磁盘数据面（DiskService）与节点控制面（PeerService）；
//  3. 依次连接每个 peer，通过 PeerService.Info 获取其对外地址、磁盘数量与布局信息；
//  4. 按地址排序合并所有节点的磁盘，形成确定性顺序的统一磁盘列表；
//  5. 校验/生成 format.json 布局（成员非对称会在这里启动失败），并把盘 UUID 交给数据面校验；
//  6. 与各 peer 交叉校验部署 ID 与布局摘要；
//  7. 启动后台探测，持续维护 peer 在线状态。
//
// 返回可直接交给 store.NewErasure 使用的 Cluster。
func Build(ctx context.Context, opts Options) (*Cluster, error) {
	log := opts.Logger
	// 1) 本地目录 -> 本地磁盘，ID 形如 "<advertise>/<序号>"
	localDisks := make([]disk.Disk, 0, len(opts.LocalDirs))
	for i, dir := range opts.LocalDirs {
		d, err := disk.NewLocal(fmt.Sprintf("%s/%d", opts.Advertise, i), dir)
		if err != nil {
			return nil, err
		}
		localDisks = append(localDisks, d)
	}

	// 2) 启动本节点的 gRPC 服务（含 OTel 统计处理器、大消息上限）
	lis, err := net.Listen("tcp", opts.Listen)
	if err != nil {
		return nil, fmt.Errorf("grpc listen %s: %w", opts.Listen, err)
	}
	gs := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(maxMessageSize),
		grpc.MaxSendMsgSize(maxMessageSize),
	)
	// 磁盘数据面：其他节点通过它读写本节点磁盘
	diskSrv := disk.NewServer(opts.Advertise, localDisks, log)
	diskSrv.Register(gs)
	// 节点控制面：其他节点通过它获取成员信息/探测存活
	driveIDs := make([]string, len(localDisks))
	for i, d := range localDisks {
		driveIDs[i] = d.ID()
	}
	peerSrv := peer.NewServer(opts.Advertise, driveIDs, log)
	peerSrv.Register(gs)
	// 锁服务：命名空间锁的节点侧锁表，其他节点通过它申请/续期/释放锁
	lockSrv := lock.NewServer(opts.Advertise, log)
	lockSrv.Register(gs)
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Error("[gos3: grpc-serve]", "error", err.Error())
		}
	}()
	log.Info("[gos3: grpc-listen]", "listen", opts.Listen, "advertise", opts.Advertise, "local-drives", len(localDisks))

	// 3) 连接各 peer：拿到对外地址与本地盘数量
	mgr := peer.NewManager(log)
	cleanup := func() {
		mgr.Close()
		gs.Stop()
	}
	counts := map[string]int{opts.Advertise: len(localDisks)}
	conns := map[string]*grpc.ClientConn{}
	for _, addr := range opts.Peers {
		st, err := mgr.Dial(ctx, addr, opts.DialTimeout)
		if err != nil {
			cleanup()
			return nil, err
		}
		conn, ok := mgr.Conn(addr)
		if !ok {
			cleanup()
			return nil, fmt.Errorf("peer %s: connection missing after dial", addr)
		}
		counts[st.Advertise] = st.Drives
		conns[st.Advertise] = conn
		log.Info("[gos3: peer-joined]", "address", st.Advertise, "drives", st.Drives)
	}

	// 4) 按地址排序，保证所有节点得到一致的磁盘顺序（纠删码分片映射依赖此顺序）
	addrs := make([]string, 0, len(counts))
	for addr := range counts {
		addrs = append(addrs, addr)
	}
	sort.Strings(addrs)

	// 布局快照：远程盘每次请求都读它来带盘 UUID 校验，Bootstrap 之后才填上。
	var layoutRef atomic.Pointer[format.Format]
	var disks []disk.Disk
	// diskSkips 与 disks 一一对应：远端盘在对端节点离线时跳过健康探测
	var diskSkips []func() bool
	slot := 0
	for _, addr := range addrs {
		n := counts[addr]
		if addr == opts.Advertise {
			disks = append(disks, localDisks...)
			for range localDisks {
				diskSkips = append(diskSkips, nil)
			}
			slot += n
			continue
		}
		conn := conns[addr]
		skip := func() bool { return !mgr.Online(addr) }
		for i := 0; i < n; i++ {
			s := slot + i // 该盘在全局顺序中的槽位
			disks = append(disks, disk.NewRemote(fmt.Sprintf("%s/%d", addr, i), conn, i, func() string {
				if f := layoutRef.Load(); f != nil {
					return f.DriveUUID(s)
				}
				return ""
			}))
			diskSkips = append(diskSkips, skip)
		}
		slot += n
	}

	// 5) 布局固化：所有盘必须拥有一致的 format.json。
	// 首个节点负责在全新集群上生成布局，其余节点等待并校验；
	// 成员配置非对称会导致布局不一致，这里直接启动失败而不是静默错配。
	ref, err := format.Bootstrap(ctx, disks, isFirstNode(opts.Advertise, opts.Peers), log, time.Second)
	if err != nil {
		cleanup()
		return nil, err
	}
	layoutRef.Store(ref)
	peerSrv.SetLayout(ref.ID, ref.LayoutHash())
	// 把本机每块盘的 UUID 交给磁盘数据面：此后对端请求里的 drive_uuid 会被校验
	for i, d := range localDisks {
		f, err := format.Load(ctx, d)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("format: load %s: %w", d.ID(), err)
		}
		diskSrv.SetDriveUUID(i, f.XL.This)
	}

	// 6) 与各 peer 互验部署 ID/布局摘要：任何一方不一致都不能启动
	if err := verifyPeerLayouts(ctx, mgr, opts.Peers, ref, log); err != nil {
		cleanup()
		return nil, err
	}

	log.Info("[gos3: cluster-ready]",
		"nodes", len(addrs), "drives", len(disks),
		"deployment", ref.ID, "layout", ref.LayoutHash(),
		"order", fmt.Sprint(addrs))

	// 7) 后台探测 peer 在线状态（用独立 ctx：启动用的 ctx 可能带超时）
	probeCtx, stopProbe := context.WithCancel(context.Background())
	go mgr.ProbeLoop(probeCtx, 0)

	// 8) 锁客户端集合：本节点直调 + 各 peer 的 gRPC 客户端（在线状态取自 peer.Manager）。
	// 离线节点不参与锁的法定人数，否则单节点掉线就会让整个集群写不进去。
	lockers := make([]lock.NodeLocker, 0, len(opts.Peers)+1)
	lockers = append(lockers, lock.NewLocalNode(opts.Advertise, lockSrv))
	for _, addr := range opts.Peers {
		conn, ok := mgr.Conn(addr)
		if !ok {
			continue
		}
		lockers = append(lockers, lock.NewRemoteNode(addr, conn, func() bool { return mgr.Online(addr) }))
	}

	return &Cluster{disks: disks, grpcServer: gs, peers: mgr, stopProbe: stopProbe, lockers: lockers, diskSkips: diskSkips, log: log}, nil
}

// isFirstNode 判断本节点是否为地址集合中排序最靠前的节点（布局初始化者）。
// 所有节点用同一规则得出相同结果，保证只有一个节点执行首次格式化。
func isFirstNode(self string, peers []string) bool {
	all := make([]string, 0, len(peers)+1)
	all = append(all, self)
	all = append(all, peers...)
	sort.Strings(all)
	return len(all) > 0 && all[0] == self
}

// verifyPeerLayouts 逐个向 peer 询问布局信息，要求其部署 ID 与布局摘要与本节点完全一致。
func verifyPeerLayouts(ctx context.Context, mgr *peer.Manager, peers []string, ref *format.Format, log *slog.Logger) error {
	for _, addr := range peers {
		if err := verifyPeerLayout(ctx, mgr, addr, ref, log); err != nil {
			return err
		}
	}
	return nil
}

// verifyPeerLayout 等待单个 peer 完成布局固化，并比对其部署 ID/布局摘要。
// 对端可能仍在等待/写入 format.json，此时按秒重试直到 ctx 超时；
// 一旦发现不一致立即返回 ErrInconsistent（不可重试，直接启动失败）。
func verifyPeerLayout(ctx context.Context, mgr *peer.Manager, addr string, ref *format.Format, log *slog.Logger) error {
	conn, ok := mgr.Conn(addr)
	if !ok {
		return fmt.Errorf("peer %s: not dialed", addr)
	}
	client := peer.NewPeerServiceClient(conn)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		info, err := client.Info(cctx, &peer.InfoRequest{})
		cancel()
		switch {
		case err != nil:
			log.Info("[gos3: waiting-for-peer-layout]", "peer", addr, "error", err.Error())
		case info.DeploymentId == "":
			log.Info("[gos3: waiting-for-peer-layout]", "peer", addr, "state", "unformatted")
		case info.DeploymentId != ref.ID || info.LayoutHash != ref.LayoutHash():
			return fmt.Errorf("%w: peer %s deployment=%s layout=%s, local deployment=%s layout=%s",
				format.ErrInconsistent, addr, info.DeploymentId, info.LayoutHash, ref.ID, ref.LayoutHash())
		default:
			log.Info("[gos3: peer-layout-ok]", "peer", addr, "deployment", info.DeploymentId)
			mgr.UpdateLayout(addr, info.DeploymentId, info.LayoutHash)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// Disks 返回聚合后的全部磁盘（本地 + 远程），顺序稳定。
func (c *Cluster) Disks() []disk.Disk {
	return c.disks
}

// Peers 返回 peer 连接管理器（在线状态、后续的分布式锁都走它）。
func (c *Cluster) Peers() *peer.Manager {
	return c.peers
}

// Lockers 返回节点级锁客户端集合（本节点 + 各 peer），供上层组装分布式读写锁。
// 顺序上第一个元素是本节点，调用方通常直接把它交给 lock.NewDRWMutex。
func (c *Cluster) Lockers() []lock.NodeLocker {
	return c.lockers
}

// HealthTargets 返回各盘的健康探测目标（顺序与 Disks 一致）：
// 本地盘直接探测；远端盘在对端节点离线时跳过（避免等待 RPC 超时）。
func (c *Cluster) HealthTargets() []health.Target {
	out := make([]health.Target, len(c.disks))
	for i, d := range c.disks {
		var skip func() bool
		if i < len(c.diskSkips) {
			skip = c.diskSkips[i]
		}
		out[i] = health.Target{Disk: d, Skip: skip}
	}
	return out
}

// Close 停止后台探测、关闭到所有 peer 的连接，并停止本节点的 gRPC 服务。
func (c *Cluster) Close() {
	if c.stopProbe != nil {
		c.stopProbe()
	}
	if c.peers != nil {
		c.peers.Close()
	}
	if c.grpcServer != nil {
		c.grpcServer.Stop()
	}
}
