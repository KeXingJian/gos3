// Package lock 实现集群命名空间锁（简化版 MinIO dsync）：
//   - lockTable 是每个节点本地的锁表，负责授予/释放/续期资源锁，并让「1 分钟未续期」的锁自动过期；
//   - Server 把锁表暴露为 gRPC LockService，NodeLocker 抽象一个节点的锁服务（本地进程内直调 / 远程 gRPC）；
//   - DRWMutex 是上层使用的分布式读写锁：向所有在线节点并发请求，凑够法定人数才算持锁成功。
//
// 目的：多节点并发写同一个 bucket/object 时把它们串行化，避免元数据互相覆盖、丢更新。
package lock

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
)

const (
	// defaultExpiry 是节点侧锁的过期时间：持锁者超过这个时间没有 Refresh 就自动释放。
	defaultExpiry = time.Minute
	// defaultRefresh 是 DRWMutex 后台续期周期。
	defaultRefresh = 10 * time.Second
)

// ErrQuorum 表示锁未能达到法定人数（拿不到锁，调用方应重试）。
var ErrQuorum = errors.New("lock: quorum not reached")

// lockEntry 是单个资源上的持锁情况。
type lockEntry struct {
	// readers 是读锁持有者集合：uid -> 最近一次续期时间
	readers map[string]time.Time
	// writer 是写锁持有者 uid（空串表示没有写锁）
	writer string
	// writerAt 是写锁最近一次续期时间
	writerAt time.Time
}

// lockTable 是节点本地的纯内存锁表（不依赖 gRPC，便于本地直调与单测）。
type lockTable struct {
	address string
	expiry  time.Duration
	log     *slog.Logger

	mu   sync.Mutex
	held map[string]*lockEntry
}

func newLockTable(address string, expiry time.Duration, log *slog.Logger) *lockTable {
	if expiry <= 0 {
		expiry = defaultExpiry
	}
	return &lockTable{address: address, expiry: expiry, log: log, held: map[string]*lockEntry{}}
}

// lock 申请写锁：与其它写者、其它读者互斥；同一 uid 重复申请视为续期。
func (t *lockTable) lock(resource, uid string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.purge(now)
	e := t.held[resource]
	if e == nil {
		e = &lockEntry{readers: map[string]time.Time{}}
		t.held[resource] = e
	}
	if e.writer != "" && e.writer != uid {
		return false
	}
	for reader := range e.readers {
		if reader != uid {
			return false
		}
	}
	e.writer = uid
	e.writerAt = now
	return true
}

// unlock 释放写锁；force 为 true 时忽略 uid 直接清空该资源的锁。
func (t *lockTable) unlock(resource, uid string, force bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.purge(now)
	e := t.held[resource]
	if e == nil {
		return true
	}
	if force {
		delete(t.held, resource)
		return true
	}
	if e.writer != uid {
		return false
	}
	e.writer = ""
	e.writerAt = time.Time{}
	if len(e.readers) == 0 {
		delete(t.held, resource)
	}
	return true
}

// rlock 申请读锁：只与写者互斥，读锁之间共享；同一 uid 重复申请视为续期。
func (t *lockTable) rlock(resource, uid string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.purge(now)
	e := t.held[resource]
	if e == nil {
		e = &lockEntry{readers: map[string]time.Time{}}
		t.held[resource] = e
	}
	if e.writer != "" && e.writer != uid {
		return false
	}
	e.readers[uid] = now
	return true
}

// runlock 释放读锁。
func (t *lockTable) runlock(resource, uid string, force bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.purge(now)
	e := t.held[resource]
	if e == nil {
		return true
	}
	if _, ok := e.readers[uid]; !ok && !force {
		return false
	}
	delete(e.readers, uid)
	if e.writer == "" && len(e.readers) == 0 {
		delete(t.held, resource)
	}
	return true
}

// refresh 续期：只有当前持有者（写者或读锁成员）才能续期。
func (t *lockTable) refresh(resource, uid string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.purge(now)
	e := t.held[resource]
	if e == nil {
		return false
	}
	if e.writer == uid {
		e.writerAt = now
		return true
	}
	if _, ok := e.readers[uid]; ok {
		e.readers[uid] = now
		return true
	}
	return false
}

// purge 清理超过 expiry 未续期的锁：节点崩溃或网络分区后，锁会被其它节点自动回收。
func (t *lockTable) purge(now time.Time) {
	for resource, e := range t.held {
		if e.writer != "" && now.Sub(e.writerAt) > t.expiry {
			t.log.Warn("[gos3: lock-expired]", "node", t.address, "resource", resource, "uid", e.writer, "idle", now.Sub(e.writerAt).String())
			e.writer = ""
			e.writerAt = time.Time{}
		}
		for uid, refreshed := range e.readers {
			if !refreshed.IsZero() && now.Sub(refreshed) > t.expiry {
				t.log.Warn("[gos3: lock-expired]", "node", t.address, "resource", resource, "uid", uid, "mode", "read", "idle", now.Sub(refreshed).String())
				delete(e.readers, uid)
			}
		}
		if e.writer == "" && len(e.readers) == 0 {
			delete(t.held, resource)
		}
	}
}

// Server 把本地锁表暴露为 gRPC LockService（与 DiskService/PeerService 共用一个端口）。
type Server struct {
	UnimplementedLockServiceServer
	table *lockTable
}

// NewServer 创建节点锁服务：address 为本节点地址（日志用）。
func NewServer(address string, log *slog.Logger) *Server {
	return &Server{table: newLockTable(address, defaultExpiry, log)}
}

// Table 返回本地锁表，供本节点进程内直调（不去绕一圈 gRPC）。
func (s *Server) Table() *lockTable {
	return s.table
}

// Register 把本服务注册到 gRPC server 上。
func (s *Server) Register(g *grpc.Server) {
	RegisterLockServiceServer(g, s)
}

func (s *Server) Lock(ctx context.Context, req *LockRequest) (*LockResponse, error) {
	return &LockResponse{Ok: s.table.lock(req.Resource, req.Uid)}, nil
}

func (s *Server) Unlock(ctx context.Context, req *LockRequest) (*LockResponse, error) {
	return &LockResponse{Ok: s.table.unlock(req.Resource, req.Uid, req.Force)}, nil
}

func (s *Server) RLock(ctx context.Context, req *LockRequest) (*LockResponse, error) {
	return &LockResponse{Ok: s.table.rlock(req.Resource, req.Uid)}, nil
}

func (s *Server) RUnlock(ctx context.Context, req *LockRequest) (*LockResponse, error) {
	return &LockResponse{Ok: s.table.runlock(req.Resource, req.Uid, req.Force)}, nil
}

func (s *Server) Refresh(ctx context.Context, req *LockRequest) (*LockResponse, error) {
	return &LockResponse{Ok: s.table.refresh(req.Resource, req.Uid)}, nil
}

// NodeLocker 是一个节点的锁服务抽象：本地进程内直调或远程 gRPC，DRWMutex 不区分两者。
type NodeLocker interface {
	Address() string
	// Online 表示该节点当前是否可用：离线节点不参与法定人数计算（否则节点掉线会写不进数据）。
	Online() bool
	Lock(ctx context.Context, resource, uid string) (bool, error)
	Unlock(ctx context.Context, resource, uid string, force bool) (bool, error)
	RLock(ctx context.Context, resource, uid string) (bool, error)
	RUnlock(ctx context.Context, resource, uid string, force bool) (bool, error)
	Refresh(ctx context.Context, resource, uid string) (bool, error)
}

// localNode 是本节点的 NodeLocker：直接调用进程内锁表。
type localNode struct {
	addr  string
	table *lockTable
}

// NewLocalNode 用本节点的锁服务创建本地 NodeLocker。
func NewLocalNode(addr string, srv *Server) NodeLocker {
	return &localNode{addr: addr, table: srv.Table()}
}

func (l *localNode) Address() string { return l.addr }
func (l *localNode) Online() bool    { return true }

func (l *localNode) Lock(ctx context.Context, resource, uid string) (bool, error) {
	return l.table.lock(resource, uid), nil
}

func (l *localNode) Unlock(ctx context.Context, resource, uid string, force bool) (bool, error) {
	return l.table.unlock(resource, uid, force), nil
}

func (l *localNode) RLock(ctx context.Context, resource, uid string) (bool, error) {
	return l.table.rlock(resource, uid), nil
}

func (l *localNode) RUnlock(ctx context.Context, resource, uid string, force bool) (bool, error) {
	return l.table.runlock(resource, uid, force), nil
}

func (l *localNode) Refresh(ctx context.Context, resource, uid string) (bool, error) {
	return l.table.refresh(resource, uid), nil
}

// remoteNode 是远程节点的 NodeLocker：经 gRPC 调用对端锁表。
type remoteNode struct {
	addr   string
	client LockServiceClient
	online func() bool
}

// NewRemoteNode 创建远程节点锁客户端；online 由上层注入（通常是 peer.Manager 的在线状态）。
func NewRemoteNode(addr string, conn grpc.ClientConnInterface, online func() bool) NodeLocker {
	if online == nil {
		online = func() bool { return true }
	}
	return &remoteNode{addr: addr, client: NewLockServiceClient(conn), online: online}
}

func (r *remoteNode) Address() string { return r.addr }
func (r *remoteNode) Online() bool    { return r.online() }

func (r *remoteNode) Lock(ctx context.Context, resource, uid string) (bool, error) {
	resp, err := r.client.Lock(ctx, &LockRequest{Resource: resource, Uid: uid, Source: r.addr})
	if err != nil {
		return false, err
	}
	return resp.Ok, nil
}

func (r *remoteNode) Unlock(ctx context.Context, resource, uid string, force bool) (bool, error) {
	resp, err := r.client.Unlock(ctx, &LockRequest{Resource: resource, Uid: uid, Source: r.addr, Force: force})
	if err != nil {
		return false, err
	}
	return resp.Ok, nil
}

func (r *remoteNode) RLock(ctx context.Context, resource, uid string) (bool, error) {
	resp, err := r.client.RLock(ctx, &LockRequest{Resource: resource, Uid: uid, Source: r.addr})
	if err != nil {
		return false, err
	}
	return resp.Ok, nil
}

func (r *remoteNode) RUnlock(ctx context.Context, resource, uid string, force bool) (bool, error) {
	resp, err := r.client.RUnlock(ctx, &LockRequest{Resource: resource, Uid: uid, Source: r.addr, Force: force})
	if err != nil {
		return false, err
	}
	return resp.Ok, nil
}

func (r *remoteNode) Refresh(ctx context.Context, resource, uid string) (bool, error) {
	resp, err := r.client.Refresh(ctx, &LockRequest{Resource: resource, Uid: uid, Source: r.addr})
	if err != nil {
		return false, err
	}
	return resp.Ok, nil
}
