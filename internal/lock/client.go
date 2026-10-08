package lock

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	mathrand "math/rand/v2"
	"sync"
	"time"
)

// Locker 是存储层需要的命名空间锁语义：按资源键加写锁/读锁，返回释放函数。
type Locker interface {
	// LockWrite 加写锁（互斥）：同名资源的并发写会串行化。
	LockWrite(ctx context.Context, resource string) (func(), error)
	// LockRead 加读锁（共享）。
	LockRead(ctx context.Context, resource string) (func(), error)
}

const (
	// opTimeout 是单次锁 RPC 的超时；锁请求不能拖慢整个写请求。
	opTimeout = 2 * time.Second
	// defaultWait 是抢锁的最长等待时间：并发写在这里排队，超时才返回 503。
	defaultWait = 10 * time.Second
	// defaultRetry 是抢锁重试的基础间隔（实际会加随机抖动）。
	defaultRetry = 50 * time.Millisecond
)

// DRWMutex 是分布式读写锁（简化版 MinIO dsync）：
// 向所有在线节点并发请求同一资源键，凑够法定人数才算成功，否则回滚已获得的授权。
type DRWMutex struct {
	uid     string
	lockers []NodeLocker
	log     *slog.Logger

	refresh time.Duration // 后台续期周期
	wait    time.Duration // 抢锁最长等待时间
	retry   time.Duration // 抢锁重试间隔
	mu      sync.Mutex
	seq     uint64
}

// NewDRWMutex 创建分布式读写锁；uid 是本节点的唯一标识（通常用 -advertise 地址）。
// 内部会再拼一个随机实例后缀，保证「同一节点的不同写者/重启后的新进程」不会被锁表认成同一个持有者。
func NewDRWMutex(uid string, lockers []NodeLocker, log *slog.Logger) *DRWMutex {
	var b [4]byte
	_, _ = cryptorand.Read(b[:])
	return &DRWMutex{
		uid:     fmt.Sprintf("%s-%s", uid, hex.EncodeToString(b[:])),
		lockers: lockers,
		log:     log,
		refresh: defaultRefresh,
		wait:    defaultWait,
		retry:   defaultRetry,
	}
}

// NewSingleNode 创建只有本节点的锁：单机多盘模式与测试使用（退化为进程内互斥）。
// 注意：锁表随实例创建，同一个进程只应创建一个（集群模式由 cluster.Build 统一创建）。
func NewSingleNode(uid string, log *slog.Logger) *DRWMutex {
	return NewDRWMutex(uid, []NodeLocker{NewLocalNode(uid, NewServer(uid, log))}, log)
}

// SetRefreshInterval 调整后台续期周期（仅测试使用；<=0 表示不续期）。
func (m *DRWMutex) SetRefreshInterval(d time.Duration) {
	m.refresh = d
}

// SetWaitTimeout 调整抢锁最长等待时间（仅测试使用）。
func (m *DRWMutex) SetWaitTimeout(d time.Duration) {
	m.wait = d
}

// LockWrite 加写锁，返回释放函数。
func (m *DRWMutex) LockWrite(ctx context.Context, resource string) (func(), error) {
	return m.acquire(ctx, resource, false)
}

// LockRead 加读锁，返回释放函数。
func (m *DRWMutex) LockRead(ctx context.Context, resource string) (func(), error) {
	return m.acquire(ctx, resource, true)
}

// quorumFor 按 MinIO dsync 的口径计算法定人数：
// tolerance = N/2，quorum = N - tolerance；当 quorum == tolerance（偶数节点）时写锁再 +1。
// 效果：写锁始终是多数派（N/2+1），读锁在偶数节点时允许更宽松的 quorum。
func quorumFor(n int, read bool) int {
	if n <= 0 {
		return 0
	}
	tolerance := n / 2
	quorum := n - tolerance
	if !read && quorum == tolerance {
		quorum++
	}
	return quorum
}

// activeLockers 返回当前在线、需要参与法定人数计算的节点（含本节点）。
func (m *DRWMutex) activeLockers() []NodeLocker {
	out := make([]NodeLocker, 0, len(m.lockers))
	for _, l := range m.lockers {
		if l.Online() {
			out = append(out, l)
		}
	}
	return out
}

// acquire 是加锁主流程：在超时窗口内反复尝试（并发冲突靠等待串行化，而不是立刻失败）。
func (m *DRWMutex) acquire(ctx context.Context, resource string, read bool) (func(), error) {
	deadline := time.Now().Add(m.wait)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	var lastErr error
	for attempt := 0; ; attempt++ {
		release, err := m.tryAcquire(ctx, resource, read)
		if err == nil {
			if attempt > 0 {
				m.log.Info("[gos3: lock-acquired-after-wait]", "resource", resource, "mode", modeName(read), "attempts", attempt+1)
			}
			return release, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrQuorum, resource, ctx.Err())
		}
		if time.Now().After(deadline) {
			break
		}
		// 加一点抖动，避免所有等待者同时重试
		wait := m.retry + time.Duration(mathrand.IntN(int(m.retry/time.Millisecond)+1))*time.Millisecond
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: %s: %v", ErrQuorum, resource, ctx.Err())
		case <-time.After(wait):
		}
	}
	return nil, fmt.Errorf("%w: %s (waited %s for the %s lock: %v)", ErrQuorum, resource, m.wait, modeName(read), lastErr)
}

// tryAcquire 只尝试一次：并发向所有在线节点请求 → 统计授权数 → 达 quorum 才算成功，否则回滚。
func (m *DRWMutex) tryAcquire(ctx context.Context, resource string, read bool) (func(), error) {
	nodes := m.activeLockers()
	if len(nodes) == 0 {
		return nil, fmt.Errorf("%w: no online lock nodes for %s", ErrQuorum, resource)
	}
	uid := m.newUID()
	quorum := quorumFor(len(nodes), read)

	granted := make([]bool, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, n NodeLocker) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, opTimeout)
			defer cancel()
			var (
				ok  bool
				err error
			)
			if read {
				ok, err = n.RLock(cctx, resource, uid)
			} else {
				ok, err = n.Lock(cctx, resource, uid)
			}
			if err != nil {
				m.log.Warn("[gos3: lock-request-failed]", "node", n.Address(), "resource", resource, "mode", modeName(read), "error", err.Error())
			}
			granted[i] = ok
		}(i, n)
	}
	wg.Wait()

	held := make([]NodeLocker, 0, len(nodes))
	for i, n := range nodes {
		if granted[i] {
			held = append(held, n)
		}
	}
	if len(held) < quorum {
		m.release(ctx, held, resource, uid, read, false)
		return nil, fmt.Errorf("%w: %s (%s lock, granted %d/%d, online nodes %d)", ErrQuorum, resource, modeName(read), len(held), quorum, len(nodes))
	}

	stop := make(chan struct{})
	var once sync.Once
	if m.refresh > 0 {
		go m.refreshLoop(stop, resource, uid, read, held)
	}
	m.log.Info("[gos3: lock-acquired]", "resource", resource, "mode", modeName(read), "uid", uid, "granted", len(held), "quorum", quorum, "nodes", len(nodes))

	return func() {
		once.Do(func() {
			close(stop)
			m.release(context.Background(), held, resource, uid, read, false)
			m.log.Info("[gos3: lock-released]", "resource", resource, "mode", modeName(read), "uid", uid)
		})
	}, nil
}

// release 向授权节点发送解锁（force 用于丢法定人数后的兜底清理）。
func (m *DRWMutex) release(ctx context.Context, nodes []NodeLocker, resource, uid string, read, force bool) {
	for _, n := range nodes {
		cctx, cancel := context.WithTimeout(ctx, opTimeout)
		if read {
			_, _ = n.RUnlock(cctx, resource, uid, force)
		} else {
			_, _ = n.Unlock(cctx, resource, uid, force)
		}
		cancel()
	}
}

// refreshLoop 周期性续期；一旦续期授权数低于法定人数，就广播强制释放并退出，
// 让其它节点能尽快接手，而不是等锁自然过期。
func (m *DRWMutex) refreshLoop(stop <-chan struct{}, resource, uid string, read bool, held []NodeLocker) {
	ticker := time.NewTicker(m.refresh)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		ok := 0
		for _, n := range held {
			cctx, cancel := context.WithTimeout(context.Background(), opTimeout)
			granted, err := n.Refresh(cctx, resource, uid)
			cancel()
			if err == nil && granted {
				ok++
			}
		}
		quorum := quorumFor(len(m.activeLockers()), read)
		if ok < quorum {
			m.log.Warn("[gos3: lock-lost-quorum]", "resource", resource, "uid", uid, "refreshed", ok, "quorum", quorum)
			m.release(context.Background(), held, resource, uid, read, true)
			return
		}
	}
}

// newUID 生成一次持锁的唯一标识（节点标识 + 进程内自增序号）。
func (m *DRWMutex) newUID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	return fmt.Sprintf("%s-%d", m.uid, m.seq)
}

func modeName(read bool) string {
	if read {
		return "read"
	}
	return "write"
}
