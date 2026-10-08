package format

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kxj/gos3/internal/disk"
)

// memDisk 是内存磁盘，用于在不依赖真实文件系统与 gRPC 的前提下测试布局逻辑。
type memDisk struct {
	id    string
	mu    sync.Mutex
	files map[string][]byte
	// failWrite 为 true 时写入直接失败，用于模拟坏盘/不可达节点。
	failWrite bool
	// failRead 为 true 时读取返回错误，用于模拟节点不可达。
	failRead bool
}

func newMemDisk(id string) *memDisk {
	return &memDisk{id: id, files: map[string][]byte{}}
}

var errDiskDown = errors.New("memdisk: down")

func (m *memDisk) ID() string { return m.id }

func (m *memDisk) ReadFile(ctx context.Context, p string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failRead {
		return nil, errDiskDown
	}
	data, ok := m.files[p]
	if !ok {
		return nil, disk.ErrNotExist
	}
	return data, nil
}

func (m *memDisk) WriteFile(ctx context.Context, p string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failWrite {
		return errDiskDown
	}
	m.files[p] = data
	return nil
}

func (m *memDisk) Rename(ctx context.Context, src, dst string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[src]
	if !ok {
		return disk.ErrNotExist
	}
	delete(m.files, src)
	m.files[dst] = data
	return nil
}

func (m *memDisk) DeleteFile(ctx context.Context, p string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, p)
	return nil
}

func (m *memDisk) DeleteDir(ctx context.Context, p string) error { return nil }

func (m *memDisk) MakeDir(ctx context.Context, p string) error { return nil }

func (m *memDisk) Stat(ctx context.Context, p string) (disk.FileInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.files[p]
	return disk.FileInfo{Exists: ok}, nil
}

func (m *memDisk) ListDir(ctx context.Context, p string) ([]disk.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[string]bool{}
	var out []disk.Entry
	for f := range m.files {
		if path.Dir(f) != p {
			continue
		}
		name := path.Base(f)
		if !seen[name] {
			seen[name] = true
			out = append(out, disk.Entry{Name: name})
		}
	}
	return out, nil
}

func (m *memDisk) Walk(ctx context.Context, p string) ([]string, []string, error) {
	return nil, nil, nil
}

func (m *memDisk) Health(ctx context.Context) error { return nil }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func memDisks(n int) []disk.Disk {
	out := make([]disk.Disk, n)
	for i := range out {
		out[i] = newMemDisk("mem/" + string(rune('a'+i)))
	}
	return out
}

// TestSaveLoadRoundTrip 校验布局写入/读取后内容一致，且 This 指向本盘槽位。
func TestSaveLoadRoundTrip(t *testing.T) {
	ctx := context.Background()
	f, err := New(1, 3)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if f.Drives() != 3 {
		t.Fatalf("Drives = %d, want 3", f.Drives())
	}
	d := newMemDisk("mem/0")
	if _, err := Load(ctx, d); !errors.Is(err, ErrUnformatted) {
		t.Fatalf("Load on empty disk = %v, want ErrUnformatted", err)
	}
	if err := Save(ctx, d, f.ForSlot(1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := Load(ctx, d)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.ID != f.ID || got.LayoutHash() != f.LayoutHash() {
		t.Fatalf("round trip mismatch: %s/%s vs %s/%s", got.ID, got.LayoutHash(), f.ID, f.LayoutHash())
	}
	if got.XL.This != f.DriveUUID(1) {
		t.Fatalf("This = %s, want slot 1 uuid %s", got.XL.This, f.DriveUUID(1))
	}
	// 每块盘的 This 不同，但布局摘要必须相同（摘要不含 This）
	if f.ForSlot(0).LayoutHash() != f.ForSlot(2).LayoutHash() {
		t.Fatal("LayoutHash must not depend on This")
	}
}

// TestFindDiskIndex 校验按盘 UUID 反查槽位。
func TestFindDiskIndex(t *testing.T) {
	f, err := New(2, 2)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for slot := 0; slot < 4; slot++ {
		if got := f.FindDiskIndex(f.DriveUUID(slot)); got != slot {
			t.Fatalf("FindDiskIndex(slot %d) = %d", slot, got)
		}
	}
	if got := f.FindDiskIndex("not-a-uuid"); got != -1 {
		t.Fatalf("FindDiskIndex(unknown) = %d, want -1", got)
	}
	if u := f.DriveUUID(4); u != "" {
		t.Fatalf("DriveUUID(out of range) = %q, want empty", u)
	}
}

// TestBootstrapInit 校验全新集群由初始化节点生成布局，并让所有盘持有同一布局。
func TestBootstrapInit(t *testing.T) {
	ctx := context.Background()
	disks := memDisks(4)
	ref, err := Bootstrap(ctx, disks, true, testLogger(), time.Millisecond)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if ref.Drives() != 4 {
		t.Fatalf("Drives = %d, want 4", ref.Drives())
	}
	for i, d := range disks {
		f, err := Load(ctx, d)
		if err != nil {
			t.Fatalf("drive %d load: %v", i, err)
		}
		if f.XL.This != ref.DriveUUID(i) {
			t.Fatalf("drive %d This = %s, want %s", i, f.XL.This, ref.DriveUUID(i))
		}
	}
}

// TestBootstrapInitRollback 校验初始化写入未达多数派时回滚，所有盘回到未格式化状态。
func TestBootstrapInitRollback(t *testing.T) {
	ctx := context.Background()
	disks := memDisks(4)
	// 只有 1 块盘可写，达不到多数派(3)
	disks[1].(*memDisk).failWrite = true
	disks[2].(*memDisk).failWrite = true
	disks[3].(*memDisk).failWrite = true

	cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := Bootstrap(cctx, disks, true, testLogger(), time.Millisecond); err == nil {
		t.Fatal("Bootstrap should fail without write quorum")
	}
	for i, d := range disks {
		if _, err := Load(ctx, d); !errors.Is(err, ErrUnformatted) {
			t.Fatalf("drive %d = %v, want ErrUnformatted after rollback", i, err)
		}
	}
}

// TestBootstrapNonInitializerWaits 校验非初始化节点在布局出现前只等待，不自己造布局。
func TestBootstrapNonInitializerWaits(t *testing.T) {
	ctx := context.Background()
	disks := memDisks(2)
	cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := Bootstrap(cctx, disks, false, testLogger(), time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Bootstrap = %v, want DeadlineExceeded", err)
	}
	for i, d := range disks {
		if _, err := Load(ctx, d); !errors.Is(err, ErrUnformatted) {
			t.Fatalf("drive %d was formatted by non-initializer", i)
		}
	}
}

// TestBootstrapRepairsUnformatted 校验多数派布局存在时会补齐未格式化的盘。
func TestBootstrapRepairsUnformatted(t *testing.T) {
	ctx := context.Background()
	disks := memDisks(4)
	ref, err := New(1, 4)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// 前 3 块已格式化（达到多数派），第 4 块缺失
	for i := 0; i < 3; i++ {
		if err := Save(ctx, disks[i], ref.ForSlot(i)); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	got, err := Bootstrap(ctx, disks, false, testLogger(), time.Millisecond)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if got.ID != ref.ID {
		t.Fatalf("deployment = %s, want %s", got.ID, ref.ID)
	}
	f, err := Load(ctx, disks[3])
	if err != nil {
		t.Fatalf("repaired drive load: %v", err)
	}
	if f.XL.This != ref.DriveUUID(3) {
		t.Fatalf("repaired This = %s, want %s", f.XL.This, ref.DriveUUID(3))
	}
}

// TestBootstrapRejectsAsymmetricMembers 校验成员非对称（盘数与布局不符）时启动失败。
func TestBootstrapRejectsAsymmetricMembers(t *testing.T) {
	ctx := context.Background()
	disks := memDisks(6)
	ref, err := New(1, 6)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// 模拟「本节点只看见 4 块盘，但集群布局是 6 块」
	view := disks[:4]
	for i := range view {
		if err := Save(ctx, view[i], ref.ForSlot(i)); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	_, err = Bootstrap(ctx, view, false, testLogger(), time.Millisecond)
	if !errors.Is(err, ErrInconsistent) {
		t.Fatalf("Bootstrap = %v, want ErrInconsistent", err)
	}
}

// TestBootstrapRejectsReorderedDrives 校验盘顺序被改动（UUID 不在期望槽位）时启动失败。
func TestBootstrapRejectsReorderedDrives(t *testing.T) {
	ctx := context.Background()
	disks := memDisks(4)
	ref, err := New(1, 4)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// 第 3、4 块盘的布局互换了槽位，模拟 -data-dirs 顺序被改动
	if err := Save(ctx, disks[0], ref.ForSlot(0)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := Save(ctx, disks[1], ref.ForSlot(1)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := Save(ctx, disks[2], ref.ForSlot(3)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := Save(ctx, disks[3], ref.ForSlot(2)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_, err = Bootstrap(ctx, disks, false, testLogger(), time.Millisecond)
	if !errors.Is(err, ErrInconsistent) {
		t.Fatalf("Bootstrap = %v, want ErrInconsistent", err)
	}
	if !strings.Contains(err.Error(), "slot 3") {
		t.Fatalf("error should report the actual slot, got %v", err)
	}
}
