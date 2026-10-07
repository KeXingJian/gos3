package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"time"

	"github.com/kxj/gos3/internal/disk"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	disks      []disk.Disk        // 聚合后的全部磁盘（本地 + 远程）
	grpcServer *grpc.Server       // 本节点的 gRPC 服务，用于响应其他节点的磁盘读写
	conns      []*grpc.ClientConn // 到各 peer 的客户端连接，Close 时统一关闭
	log        *slog.Logger
}

// Build 组建集群：
//  1. 把本节点的本地目录封装为本地磁盘；
//  2. 启动 gRPC 服务并注册磁盘服务，供其他节点读写本机磁盘；
//  3. 依次连接每个 peer，获取其节点信息（地址 + 磁盘数），封装为远程磁盘；
//  4. 按地址排序合并所有节点的磁盘，形成确定性顺序的统一磁盘列表。
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
	// 注册磁盘服务：其他节点将通过该服务访问本节点的 localDisks
	disk.NewServer(opts.Advertise, localDisks, log).Register(gs)
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Error("[gos3: grpc-serve]", "error", err.Error())
		}
	}()
	log.Info("[gos3: grpc-listen]", "listen", opts.Listen, "advertise", opts.Advertise, "local-drives", len(localDisks))

	// 3) 以本节点地址为 key 汇集各节点磁盘，随后逐个连接 peer
	nodes := map[string][]disk.Disk{opts.Advertise: localDisks}
	var conns []*grpc.ClientConn

	for _, peer := range opts.Peers {
		conn, err := dialPeer(ctx, peer, opts.DialTimeout, log)
		if err != nil {
			gs.Stop()
			return nil, err
		}
		conns = append(conns, conn)
		client := disk.NewDiskServiceClient(conn)
		// 询问对端节点信息：其 advertise 地址与磁盘数量
		info, err := client.Info(ctx, &disk.NodeInfoRequest{})
		if err != nil {
			gs.Stop()
			return nil, fmt.Errorf("peer %s info: %w", peer, err)
		}
		// 对端每个磁盘封装为一个远程 disk.Disk（通过 conn 转发读写）
		remote := make([]disk.Disk, 0, len(info.Drives))
		for i := range info.Drives {
			remote = append(remote, disk.NewRemote(fmt.Sprintf("%s/%d", info.Address, i), conn, i))
		}
		nodes[info.Address] = remote
		log.Info("[gos3: peer-joined]", "address", info.Address, "drives", len(info.Drives))
	}

	// 4) 按地址排序，保证所有节点得到一致的磁盘顺序（纠删码分片映射依赖此顺序）
	addrs := make([]string, 0, len(nodes))
	for addr := range nodes {
		addrs = append(addrs, addr)
	}
	sort.Strings(addrs)
	var disks []disk.Disk
	for _, addr := range addrs {
		disks = append(disks, nodes[addr]...)
	}
	log.Info("[gos3: cluster-ready]", "nodes", len(addrs), "drives", len(disks), "order", fmt.Sprint(addrs))
	return &Cluster{disks: disks, grpcServer: gs, conns: conns, log: log}, nil
}

// dialPeer 在超时时间内反复尝试连接 peer，直到连接成功且能成功调用 Info 为止。
// 采用重试 + 每秒一次的方式，支持节点启动顺序不一致（对端可能尚未就绪）。
func dialPeer(ctx context.Context, peer string, timeout time.Duration, log *slog.Logger) (*grpc.ClientConn, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		conn, err := grpc.NewClient(peer,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxMessageSize),
				grpc.MaxCallSendMsgSize(maxMessageSize),
			),
		)
		if err == nil {
			// 建连后做一次 2 秒超时的 Info 健康探测，确认对端真正可用
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, infoErr := disk.NewDiskServiceClient(conn).Info(cctx, &disk.NodeInfoRequest{})
			cancel()
			if infoErr == nil {
				return conn, nil
			}
			_ = conn.Close()
			lastErr = infoErr
		} else {
			lastErr = err
		}
		log.Info("[gos3: waiting-for-peer]", "peer", peer, "error", fmt.Sprint(lastErr))
		time.Sleep(time.Second)
	}
	return nil, fmt.Errorf("peer %s not reachable: %w", peer, lastErr)
}

// Disks 返回聚合后的全部磁盘（本地 + 远程），顺序稳定。
func (c *Cluster) Disks() []disk.Disk {
	return c.disks
}

// Close 关闭到所有 peer 的连接，并停止本节点的 gRPC 服务。
func (c *Cluster) Close() {
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	if c.grpcServer != nil {
		c.grpcServer.Stop()
	}
}
