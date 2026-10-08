package peer

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// defaultProbeInterval 是存活探测周期。
	defaultProbeInterval = 5 * time.Second
	// probeTimeout 是单次 Health 探测的超时；离线 peer 必须快速失败，不能拖住调用方。
	probeTimeout = 2 * time.Second
)

// State 是一个 peer 的连接与在线状态快照。
type State struct {
	Addr         string    // 拨号地址（-peers 里配置的那个）
	Advertise    string    // 对端自报的对外地址（Info.address）
	Online       bool      // 最近一次探测是否成功
	LastSeen     time.Time // 最近一次探测成功的时间
	Drives       int       // 对端本地盘数量
	DeploymentID string    // 对端回报的部署 ID（启动期互验时记录）
	LayoutHash   string    // 对端回报的布局摘要
	LastError    string    // 最近一次探测失败原因
}

// entry 是一个 peer 的内部状态：连接 + 最新状态快照。
type entry struct {
	conn  *grpc.ClientConn
	state State
}

// Manager 维护到每个 peer 的单条 gRPC 连接与在线状态。
// 对端掉线不会删除连接，gRPC 会自动重连，探测成功后状态自动回到在线。
type Manager struct {
	log *slog.Logger

	mu      sync.RWMutex
	entries map[string]*entry
}

// NewManager 创建 peer 连接管理器。
func NewManager(log *slog.Logger) *Manager {
	return &Manager{log: log, entries: map[string]*entry{}}
}

// Dial 在 timeout 内反复尝试连接 addr 并调用 Info，成功后记录该 peer。
// 采用重试 + 每秒一次的方式，支持节点启动顺序不一致（对端可能尚未就绪）。
func (m *Manager) Dial(ctx context.Context, addr string, timeout time.Duration) (State, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return State{}, err
		}
		st, err := m.dialOnce(ctx, addr)
		if err == nil {
			m.mu.Lock()
			m.entries[addr] = &entry{conn: st.conn, state: st.state}
			m.mu.Unlock()
			return st.state, nil
		}
		lastErr = err
		m.log.Info("[gos3: waiting-for-peer]", "peer", addr, "error", fmt.Sprint(lastErr))
		select {
		case <-ctx.Done():
			return State{}, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return State{}, fmt.Errorf("peer %s not reachable: %w", addr, lastErr)
}

// dialOnce 建连并做一次 Info 探测。
func (m *Manager) dialOnce(ctx context.Context, addr string) (*entry, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, probeTimeout)
	info, infoErr := NewPeerServiceClient(conn).Info(cctx, &InfoRequest{})
	cancel()
	if infoErr != nil {
		_ = conn.Close()
		return nil, infoErr
	}
	return &entry{
		conn: conn,
		state: State{
			Addr:      addr,
			Advertise: info.Address,
			Online:    true,
			LastSeen:  time.Now(),
			Drives:    len(info.Drives),
		},
	}, nil
}

// Conn 返回与某个 peer 的连接。
func (m *Manager) Conn(addr string) (*grpc.ClientConn, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.entries[addr]
	if !ok {
		return nil, false
	}
	return e.conn, true
}

// Online 返回某个 peer 当前是否在线。
func (m *Manager) Online(addr string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.entries[addr]
	return ok && e.state.Online
}

// State 返回某个 peer 的状态快照。
func (m *Manager) State(addr string) (State, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.entries[addr]
	if !ok {
		return State{}, false
	}
	return e.state, true
}

// States 按地址排序返回所有 peer 的状态快照。
func (m *Manager) States() []State {
	m.mu.RLock()
	out := make([]State, 0, len(m.entries))
	for _, e := range m.entries {
		out = append(out, e.state)
	}
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}

// UpdateLayout 记录对端在启动期互验时回报的部署 ID 与布局摘要。
func (m *Manager) UpdateLayout(addr, deploymentID, layoutHash string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[addr]; ok {
		e.state.DeploymentID = deploymentID
		e.state.LayoutHash = layoutHash
	}
}

// Probe 探测单个 peer 的存活，更新在线状态并返回最新快照。
func (m *Manager) Probe(ctx context.Context, addr string) (State, error) {
	m.mu.RLock()
	e, ok := m.entries[addr]
	m.mu.RUnlock()
	if !ok {
		return State{}, fmt.Errorf("peer %s: not dialed", addr)
	}
	cctx, cancel := context.WithTimeout(ctx, probeTimeout)
	_, err := NewPeerServiceClient(e.conn).Health(cctx, &HealthRequest{})
	cancel()

	m.mu.Lock()
	defer m.mu.Unlock()
	st := e.state
	st.LastError = ""
	if err != nil {
		st.Online = false
		st.LastError = err.Error()
	} else {
		st.Online = true
		st.LastSeen = time.Now()
	}
	e.state = st
	return st, err
}

// ProbeLoop 周期性探测所有 peer，并在在线状态翻转时打日志（peer-offline / peer-online）。
// 阻塞运行直到 ctx 结束。
func (m *Manager) ProbeLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = defaultProbeInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, st := range m.States() {
			before := st.Online
			after, err := m.Probe(ctx, st.Addr)
			// ctx 结束时不做状态判定，避免把「本节点关闭」误报成对端掉线
			if ctx.Err() != nil {
				return
			}
			switch {
			case err != nil && before:
				m.log.Warn("[gos3: peer-offline]", "peer", st.Addr, "error", err.Error())
			case err == nil && !before:
				m.log.Info("[gos3: peer-online]", "peer", st.Addr, "last-seen", after.LastSeen.Format(time.RFC3339))
			}
		}
	}
}

// Close 关闭所有 peer 连接。
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.entries {
		_ = e.conn.Close()
	}
	m.entries = map[string]*entry{}
}
