package health

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/kxj/gos3/internal/disk"
)

// fakeDisk 是最小的内存盘实现，可注入写失败模拟坏盘。
type fakeDisk struct {
	id        string
	data      map[string][]byte
	failWrite bool
}

func newFakeDisk(id string) *fakeDisk { return &fakeDisk{id: id, data: map[string][]byte{}} }

func (f *fakeDisk) ID() string { return f.id }

func (f *fakeDisk) ReadFile(ctx context.Context, path string) ([]byte, error) {
	data, ok := f.data[path]
	if !ok {
		return nil, disk.ErrNotExist
	}
	return data, nil
}

func (f *fakeDisk) WriteFile(ctx context.Context, path string, data []byte) error {
	if f.failWrite {
		return context.DeadlineExceeded
	}
	f.data[path] = data
	return nil
}

func (f *fakeDisk) Rename(ctx context.Context, src, dst string) error {
	data, ok := f.data[src]
	if !ok {
		return disk.ErrNotExist
	}
	delete(f.data, src)
	f.data[dst] = data
	return nil
}

func (f *fakeDisk) DeleteFile(ctx context.Context, path string) error {
	delete(f.data, path)
	return nil
}

func (f *fakeDisk) DeleteDir(ctx context.Context, dir string) error { return nil }
func (f *fakeDisk) MakeDir(ctx context.Context, dir string) error   { return nil }
func (f *fakeDisk) Stat(ctx context.Context, path string) (disk.FileInfo, error) {
	_, ok := f.data[path]
	return disk.FileInfo{Exists: ok}, nil
}
func (f *fakeDisk) ListDir(ctx context.Context, path string) ([]disk.Entry, error) {
	return nil, nil
}
func (f *fakeDisk) Walk(ctx context.Context, path string) ([]string, []string, error) {
	return nil, nil, nil
}
func (f *fakeDisk) Health(ctx context.Context) error { return nil }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestMonitorMarksFaultyAndRecovers 校验坏盘被标记为 faulty，恢复后回到 online。
func TestMonitorMarksFaultyAndRecovers(t *testing.T) {
	good := newFakeDisk("mem/0")
	bad := newFakeDisk("mem/1")
	m := NewMonitor("test-node", []Target{{Disk: good}, {Disk: bad}}, testLogger())

	if got := m.ProbeAll(context.Background()); got != 2 {
		t.Fatalf("ProbeAll = %d, want 2", got)
	}
	bad.failWrite = true
	if got := m.ProbeAll(context.Background()); got != 1 {
		t.Fatalf("ProbeAll = %d, want 1 after a disk fails", got)
	}
	if m.Online(1) {
		t.Fatal("faulty disk should be offline")
	}
	states := m.States()
	if states[1].LastError == "" {
		t.Fatal("faulty disk should record LastError")
	}
	bad.failWrite = false
	if got := m.ProbeAll(context.Background()); got != 2 {
		t.Fatalf("ProbeAll = %d, want 2 after recovery", got)
	}
	if !m.Online(1) {
		t.Fatal("recovered disk should be online")
	}
	if m.States()[1].LastError != "" {
		t.Fatal("LastError should be cleared after recovery")
	}
}

// TestMonitorSkipOfflinePeer 校验对端节点已知离线时其盘直接判定不可用（不发起探测）。
func TestMonitorSkipOfflinePeer(t *testing.T) {
	local := newFakeDisk("mem/0")
	remote := newFakeDisk("remote:9001/0")
	remoteOffline := true
	m := NewMonitor("test-node", []Target{
		{Disk: local},
		{Disk: remote, Skip: func() bool { return remoteOffline }},
	}, testLogger())

	if got := m.ProbeAll(context.Background()); got != 1 {
		t.Fatalf("ProbeAll = %d, want 1 (remote peer offline)", got)
	}
	if !m.Online(0) || m.Online(1) {
		t.Fatalf("unexpected online states: %v", m.States())
	}
	remoteOffline = false
	if got := m.ProbeAll(context.Background()); got != 2 {
		t.Fatalf("ProbeAll = %d, want 2 after peer recovery", got)
	}
}

// TestClusterReadiness 校验集群视图的读/写就绪判定与快照内容。
func TestClusterReadiness(t *testing.T) {
	disks := []disk.Disk{newFakeDisk("mem/0"), newFakeDisk("mem/1"), newFakeDisk("mem/2"), newFakeDisk("mem/3")}
	targets := make([]Target, len(disks))
	for i, d := range disks {
		targets[i] = Target{Disk: d}
	}
	m := NewMonitor("test-node", targets, testLogger())
	c := NewCluster(m, 2, 3, nil, func() int { return 7 })
	m.ProbeAll(context.Background())

	snap := c.Snapshot()
	if snap.Online != 4 || snap.Total != 4 || snap.ReadQuorum != 2 || snap.WriteQuorum != 3 || snap.PendingHeal != 7 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
	if !c.Readable() || !c.Ready() {
		t.Fatal("cluster should be both readable and ready with all drives up")
	}

	// 掉两块盘（剩 2）：达到读 quorum，但未达写 quorum
	disks[2].(*fakeDisk).failWrite = true
	disks[3].(*fakeDisk).failWrite = true
	m.ProbeAll(context.Background())
	if !c.Readable() {
		t.Fatal("2 online drives should still be readable (read quorum 2)")
	}
	if c.Ready() {
		t.Fatal("2 online drives must not be ready (write quorum 3)")
	}
	if !c.Readable() || c.Ready() {
		t.Fatal("unexpected readiness state")
	}

	// 再掉一块：连读 quorum 都不够
	disks[1].(*fakeDisk).failWrite = true
	m.ProbeAll(context.Background())
	if c.Readable() {
		t.Fatal("1 online drive must not be readable (read quorum 2)")
	}
}

// TestClusterWithoutMonitor 校验单盘 FS 模式（无盘级监控）时永远就绪。
func TestClusterWithoutMonitor(t *testing.T) {
	c := NewCluster(nil, 1, 1, nil, nil)
	if !c.Readable() || !c.Ready() {
		t.Fatal("cluster without a monitor should always be ready")
	}
	if snap := c.Snapshot(); snap.Online != 0 {
		t.Fatalf("snapshot = %+v", snap)
	}
}

// TestMonitorInterval 校验探测周期设置生效。
func TestMonitorInterval(t *testing.T) {
	m := NewMonitor("test-node", []Target{{Disk: newFakeDisk("mem/0")}}, testLogger())
	m.SetInterval(5 * time.Millisecond)
	if m.interval != 5*time.Millisecond {
		t.Fatalf("interval = %s", m.interval)
	}
}
