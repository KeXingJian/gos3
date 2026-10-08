// Package health 提供「盘级健康 + 集群法定人数」的健康视图：
//   - Monitor 周期探测每块盘（写/读/删一个探针文件），维护 online/faulty 状态；
//   - Cluster 汇总盘状态、读写法定人数与 peer 状态，供 /minio/health/* 端点使用。
//
// 对端节点已知离线时，其盘会被直接判定为不可用（跳过探测），避免每次探测都要等 RPC 超时。
package health

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/kxj/gos3/internal/disk"
)

const (
	// defaultInterval 是盘健康探测周期。
	defaultInterval = 15 * time.Second
	// probeTimeout 是单次探测超时。
	probeTimeout = 5 * time.Second
	// probePrefix 是探针文件前缀（位于布局目录下，不会被对象列举扫描到）。
	// 每个监视器用自己的 ID 拼出独立文件名：多个节点会同时探测同一块盘，
	// 共用路径会互相 rename/delete 造成「假坏盘」。
	probePrefix = ".gos3.sys/health-probe-"
)

// probePayload 是探针文件内容（约 2 KiB）。
var probePayload = bytes.Repeat([]byte("gos3-health-check"), 128)

// Target 是一块待探测的盘。
type Target struct {
	Disk disk.Disk
	// Skip 可选：返回 true 表示该盘所在节点已知离线，本次直接判定不可用（不发起 RPC）。
	Skip func() bool
}

// DiskState 是一块盘的健康快照。
type DiskState struct {
	Index     int       `json:"index"`
	ID        string    `json:"id"`
	Online    bool      `json:"online"`
	LastSeen  time.Time `json:"lastSeen,omitempty"`
	LastError string    `json:"lastError,omitempty"`
}

// Monitor 维护各盘的健康状态。
type Monitor struct {
	targets   []Target
	log       *slog.Logger
	interval  time.Duration
	probePath string

	mu     sync.RWMutex
	states []DiskState
	probes int64 // 累计探测轮次
}

// NewMonitor 创建盘健康监视器；id 用于生成该监视器独有的探针文件名（通常传本节点地址）。
func NewMonitor(id string, targets []Target, log *slog.Logger) *Monitor {
	states := make([]DiskState, len(targets))
	for i, t := range targets {
		states[i] = DiskState{Index: i, ID: t.Disk.ID(), Online: true, LastSeen: time.Now()}
	}
	return &Monitor{targets: targets, log: log, interval: defaultInterval, probePath: probePathFor(id), states: states}
}

// probePathFor 生成监视器独有的探针路径，把节点地址里的非法字符替换掉。
func probePathFor(id string) string {
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			return r
		default:
			return '_'
		}
	}, id)
	if safe == "" {
		safe = "local"
	}
	return probePrefix + safe
}

// SetInterval 调整探测周期（测试用）。
func (m *Monitor) SetInterval(d time.Duration) {
	if d > 0 {
		m.interval = d
	}
}

// Probe 探测一块盘：写探针文件 -> 读回校验 -> 删除。任一环节失败即判定该盘不可用。
func (m *Monitor) Probe(ctx context.Context, index int) error {
	if index < 0 || index >= len(m.targets) {
		return fmt.Errorf("health: disk index %d out of range", index)
	}
	t := m.targets[index]
	if t.Skip != nil && t.Skip() {
		return fmt.Errorf("health: peer for disk %s is offline", t.Disk.ID())
	}
	cctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	return probeDisk(cctx, t.Disk, m.probePath)
}

// probeDisk 是实际的探针动作：写入约 2KB、读回比对长度、再删除。
func probeDisk(ctx context.Context, d disk.Disk, path string) error {
	if err := d.WriteFile(ctx, path, probePayload); err != nil {
		return fmt.Errorf("probe write: %w", err)
	}
	data, err := d.ReadFile(ctx, path)
	if err != nil {
		return fmt.Errorf("probe read: %w", err)
	}
	if len(data) != len(probePayload) {
		return fmt.Errorf("probe size mismatch: got %d want %d", len(data), len(probePayload))
	}
	if err := d.DeleteFile(ctx, path); err != nil {
		return fmt.Errorf("probe delete: %w", err)
	}
	return nil
}

// ProbeAll 探测所有盘并更新状态（返回本次探测后的在线盘数）。
func (m *Monitor) ProbeAll(ctx context.Context) int {
	online := 0
	for i := range m.targets {
		err := m.Probe(ctx, i)
		m.update(i, err)
		if err == nil {
			online++
		}
	}
	m.mu.Lock()
	m.probes++
	m.mu.Unlock()
	return online
}

// update 记录一次探测结果，并在状态翻转时打日志。
func (m *Monitor) update(index int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.states[index]
	was := st.Online
	if err == nil {
		st.Online = true
		st.LastSeen = time.Now()
		st.LastError = ""
	} else {
		st.Online = false
		st.LastError = err.Error()
	}
	m.states[index] = st
	if was != st.Online {
		if st.Online {
			m.log.Info("[gos3: disk-online]", "disk", st.ID, "index", index)
		} else {
			m.log.Warn("[gos3: disk-faulty]", "disk", st.ID, "index", index, "error", st.LastError)
		}
	}
}

// Run 周期探测所有盘，阻塞直到 ctx 结束。
func (m *Monitor) Run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	// 启动时先探一次，尽快反映真实状态
	m.ProbeAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.ProbeAll(ctx)
		}
	}
}

// States 返回各盘健康快照（顺序与磁盘列表一致）。
func (m *Monitor) States() []DiskState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]DiskState, len(m.states))
	copy(out, m.states)
	return out
}

// Online 返回某块盘是否在线。
func (m *Monitor) Online(index int) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if index < 0 || index >= len(m.states) {
		return false
	}
	return m.states[index].Online
}

// OnlineCount 返回当前在线盘数。
func (m *Monitor) OnlineCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := 0
	for _, st := range m.states {
		if st.Online {
			n++
		}
	}
	return n
}

// PeerInfo 是健康视图里对端节点的状态。
type PeerInfo struct {
	Addr         string    `json:"addr"`
	Advertise    string    `json:"advertise,omitempty"`
	Online       bool      `json:"online"`
	LastSeen     time.Time `json:"lastSeen,omitempty"`
	DeploymentID string    `json:"deploymentId,omitempty"`
}

// Snapshot 是集群健康快照（/minio/health/cluster 的响应体）。
type Snapshot struct {
	Drives      []DiskState `json:"drives"`
	Online      int         `json:"onlineDrives"`
	Total       int         `json:"totalDrives"`
	ReadQuorum  int         `json:"readQuorum"`
	WriteQuorum int         `json:"writeQuorum"`
	Peers       []PeerInfo  `json:"peers,omitempty"`
	PendingHeal int         `json:"pendingHeals"`
}

// Cluster 汇总盘健康与法定人数，回答「集群现在能不能读/写」。
type Cluster struct {
	monitor     *Monitor
	readQuorum  int
	writeQuorum int
	peers       func() []PeerInfo
	pendingHeal func() int
}

// NewCluster 创建集群健康视图；monitor/peers/pendingHeal 可为 nil。
func NewCluster(m *Monitor, readQuorum, writeQuorum int, peers func() []PeerInfo, pendingHeal func() int) *Cluster {
	if readQuorum < 1 {
		readQuorum = 1
	}
	if writeQuorum < 1 {
		writeQuorum = 1
	}
	return &Cluster{monitor: m, readQuorum: readQuorum, writeQuorum: writeQuorum, peers: peers, pendingHeal: pendingHeal}
}

// Snapshot 返回当前健康快照。在线盘数不足读法定人数时，快照里体现为「不可读」。
func (c *Cluster) Snapshot() Snapshot {
	if c.monitor == nil {
		// 单盘 FS 模式：没有盘级监控，只报告法定人数
		return Snapshot{ReadQuorum: c.readQuorum, WriteQuorum: c.writeQuorum}
	}
	states := c.monitor.States()
	online := 0
	for _, st := range states {
		if st.Online {
			online++
		}
	}
	snap := Snapshot{
		Drives:      states,
		Online:      online,
		Total:       len(states),
		ReadQuorum:  c.readQuorum,
		WriteQuorum: c.writeQuorum,
	}
	if c.peers != nil {
		snap.Peers = c.peers()
	}
	if c.pendingHeal != nil {
		snap.PendingHeal = c.pendingHeal()
	}
	return snap
}

// Readable 表示在线盘数达到读法定人数（可以对外提供读服务）。
// 没有盘级监控（单盘 FS 模式）时恒为 true。
func (c *Cluster) Readable() bool {
	if c.monitor == nil {
		return true
	}
	return c.monitor.OnlineCount() >= c.readQuorum
}

// Ready 表示在线盘数达到写法定人数（可以对外提供写服务）。
func (c *Cluster) Ready() bool {
	if c.monitor == nil {
		return true
	}
	return c.monitor.OnlineCount() >= c.writeQuorum
}
