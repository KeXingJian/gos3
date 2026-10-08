package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kxj/gos3/internal/disk"
	"github.com/kxj/gos3/internal/heal"
)

// memDisk 是内存磁盘：支持目录/文件、按需注入写或 rename 失败，用于验证 quorum 语义。
type memDisk struct {
	id string

	mu         sync.Mutex
	files      map[string][]byte
	dirs       map[string]bool
	failWrite  bool
	failRename bool
	offline    bool
}

func newMemDisk(id string) *memDisk {
	return &memDisk{id: id, files: map[string][]byte{}, dirs: map[string]bool{}}
}

var errInjected = errors.New("memdisk: injected failure")

func (m *memDisk) ID() string { return m.id }

func (m *memDisk) ReadFile(ctx context.Context, p string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.offline {
		return nil, errInjected
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
	if m.offline || m.failWrite {
		return errInjected
	}
	m.files[p] = data
	return nil
}

func (m *memDisk) Rename(ctx context.Context, src, dst string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.offline || m.failRename {
		return errInjected
	}
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

func (m *memDisk) DeleteDir(ctx context.Context, p string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	prefix := strings.TrimSuffix(p, "/") + "/"
	for f := range m.files {
		if f == p || strings.HasPrefix(f, prefix) {
			delete(m.files, f)
		}
	}
	for d := range m.dirs {
		if d == p || strings.HasPrefix(d, prefix) {
			delete(m.dirs, d)
		}
	}
	return nil
}

func (m *memDisk) MakeDir(ctx context.Context, p string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.offline {
		return errInjected
	}
	m.dirs[p] = true
	return nil
}

func (m *memDisk) Stat(ctx context.Context, p string) (disk.FileInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.offline {
		return disk.FileInfo{}, errInjected
	}
	if data, ok := m.files[p]; ok {
		return disk.FileInfo{Exists: true, Size: int64(len(data))}, nil
	}
	if m.dirs[p] {
		return disk.FileInfo{Exists: true, IsDir: true}, nil
	}
	return disk.FileInfo{}, nil
}

func (m *memDisk) ListDir(ctx context.Context, p string) ([]disk.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.offline {
		return nil, errInjected
	}
	prefix := strings.TrimSuffix(p, "/") + "/"
	seen := map[string]bool{}
	var out []disk.Entry
	for f := range m.files {
		if !strings.HasPrefix(f, prefix) {
			continue
		}
		rest := strings.TrimPrefix(f, prefix)
		name := strings.SplitN(rest, "/", 2)[0]
		if seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, disk.Entry{Name: name, IsDir: strings.Contains(rest, "/")})
	}
	for d := range m.dirs {
		if !strings.HasPrefix(d, prefix) {
			continue
		}
		rest := strings.TrimPrefix(d, prefix)
		name := strings.SplitN(rest, "/", 2)[0]
		if seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, disk.Entry{Name: name, IsDir: true})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *memDisk) Walk(ctx context.Context, p string) ([]string, []string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.offline {
		return nil, nil, errInjected
	}
	prefix := strings.TrimSuffix(p, "/") + "/"
	var files, dirs []string
	for f := range m.files {
		if strings.HasPrefix(f, prefix) {
			files = append(files, strings.TrimPrefix(f, prefix))
		}
	}
	for d := range m.dirs {
		if strings.HasPrefix(d, prefix) {
			dirs = append(dirs, strings.TrimPrefix(d, prefix))
		}
	}
	sort.Strings(files)
	sort.Strings(dirs)
	return files, dirs, nil
}

func (m *memDisk) Health(ctx context.Context) error {
	if m.offline {
		return errInjected
	}
	return nil
}

// filesWithPrefix 返回该盘上以 prefix 开头的文件路径（测试断言用）。
func (m *memDisk) filesWithPrefix(prefix string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for f := range m.files {
		if strings.HasPrefix(f, prefix) {
			out = append(out, f)
		}
	}
	sort.Strings(out)
	return out
}

// putFile 直接塞入文件（模拟「只写到部分盘」的中间状态）。
func (m *memDisk) putFile(p string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[p] = data
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestErasure 建一个有 n 块盘的纠删码后端（默认 2 数据 + 2 校验：读 quorum 2、写 quorum 3）。
func newTestErasure(t *testing.T, n, data, parity int) (*Erasure, []*memDisk) {
	t.Helper()
	disks := make([]*memDisk, n)
	list := make([]disk.Disk, n)
	for i := range disks {
		disks[i] = newMemDisk("mem/" + string(rune('a'+i)))
		list[i] = disks[i]
	}
	e, err := NewErasure(list, data, parity, testLogger(), noopLocker{}, heal.NewQueue(testLogger()))
	if err != nil {
		t.Fatalf("NewErasure: %v", err)
	}
	return e, disks
}

func mustMakeBucket(t *testing.T, e *Erasure, bucket string) {
	t.Helper()
	if err := e.MakeBucket(context.Background(), bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
}

func mustPut(t *testing.T, e *Erasure, bucket, object, body string) ObjectInfo {
	t.Helper()
	info, err := e.PutObject(context.Background(), bucket, object, strings.NewReader(body), int64(len(body)), "text/plain", nil)
	if err != nil {
		t.Fatalf("PutObject(%s): %v", body, err)
	}
	return info
}

func getBody(t *testing.T, e *Erasure, bucket, object string) (string, error) {
	t.Helper()
	rc, _, err := e.GetObject(context.Background(), bucket, object, "")
	if err != nil {
		return "", err
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// TestQuorumFormulas 校验读写法定人数公式：data == parity 时写 quorum 需要 +1。
func TestQuorumFormulas(t *testing.T) {
	e, _ := newTestErasure(t, 4, 2, 2)
	if e.readQuorum() != 2 || e.writeQuorum() != 3 {
		t.Fatalf("2+2: read=%d write=%d, want 2/3", e.readQuorum(), e.writeQuorum())
	}
	e2, _ := newTestErasure(t, 5, 3, 2)
	if e2.readQuorum() != 3 || e2.writeQuorum() != 3 {
		t.Fatalf("3+2: read=%d write=%d, want 3/3", e2.readQuorum(), e2.writeQuorum())
	}
}

// TestOverwriteKeepsOldVersionOnQuorumFailure 是 P2 的核心验收：
// 写入未达写法定人数时必须报错，且旧版本数据完好（不能出现「先删旧再写新」的丢数据窗口）。
func TestOverwriteKeepsOldVersionOnQuorumFailure(t *testing.T) {
	ctx := context.Background()
	e, disks := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	mustPut(t, e, "bkt", "obj", "version-1")

	// 让两块盘拒绝写入：分片只能落到 2 块盘，达不到写 quorum(3)
	disks[2].failWrite = true
	disks[3].failWrite = true

	if _, err := e.PutObject(ctx, "bkt", "obj", strings.NewReader("version-2"), 9, "text/plain", nil); !errors.Is(err, ErrWriteQuorum) {
		t.Fatalf("overwrite = %v, want ErrWriteQuorum", err)
	}
	body, err := getBody(t, e, "bkt", "obj")
	if err != nil {
		t.Fatalf("GetObject after failed overwrite: %v", err)
	}
	if body != "version-1" {
		t.Fatalf("body = %q, want old version-1", body)
	}
}

// TestRenameFailureRollsBack 校验第二阶段（rename 提交）失败时回滚：旧版本可读且不留临时文件。
func TestRenameFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	e, disks := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	mustPut(t, e, "bkt", "obj", "version-1")

	disks[2].failRename = true
	disks[3].failRename = true

	if _, err := e.PutObject(ctx, "bkt", "obj", strings.NewReader("version-2"), 9, "text/plain", nil); !errors.Is(err, ErrWriteQuorum) {
		t.Fatalf("overwrite = %v, want ErrWriteQuorum", err)
	}
	body, err := getBody(t, e, "bkt", "obj")
	if err != nil || body != "version-1" {
		t.Fatalf("body = %q err = %v, want version-1", body, err)
	}
	// 回滚后不应残留临时分片（对象数据目录下的 .tmp/）
	for _, d := range disks {
		for _, f := range d.filesWithPrefix(".data/bkt/obj/") {
			if strings.Contains(f, "/.tmp/") {
				t.Fatalf("disk %s left temp shards: %s", d.ID(), f)
			}
		}
	}
}

// TestPutSucceedsWithOneDriveDown 校验掉一块盘（仍达写法定人数 3）时写入成功且可读。
func TestPutSucceedsWithOneDriveDown(t *testing.T) {
	e, disks := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	disks[3].failWrite = true

	mustPut(t, e, "bkt", "obj", "value-with-one-drive-down")
	body, err := getBody(t, e, "bkt", "obj")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if body != "value-with-one-drive-down" {
		t.Fatalf("body = %q", body)
	}
}

// TestReadMetaQuorumIgnoresStaleMeta 校验「只有单块盘上的新元数据」不会被当成权威值。
func TestReadMetaQuorumIgnoresStaleMeta(t *testing.T) {
	ctx := context.Background()
	e, disks := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	mustPut(t, e, "bkt", "obj", "committed")

	// 人为在 1 块盘上伪造一份「更新」的元数据（模拟只写成功一块盘的失败写入）
	meta := fsMeta{Name: "obj", Versions: []fsVersion{{
		VersionID: NullVersionID,
		DataID:    "phantom",
		Size:      7,
		ETag:      "phantom-etag",
		ModTime:   time.Now().UTC().Add(time.Hour),
	}}}
	data, _ := json.Marshal(meta)
	disks[0].putFile(diskMetaPath("bkt", "obj"), data)

	body, err := getBody(t, e, "bkt", "obj")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if body != "committed" {
		t.Fatalf("body = %q, want committed (stale meta must not win)", body)
	}

	res, err := e.ListObjects(ctx, "bkt", ListOptions{})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(res.Objects) != 1 || res.Objects[0].ETag == "phantom-etag" {
		t.Fatalf("ListObjects returned %+v, want the committed version", res.Objects)
	}
}

// TestListObjectsSkipsPartialObject 校验「只写到一块盘」的对象不会出现在列举结果里。
func TestListObjectsSkipsPartialObject(t *testing.T) {
	ctx := context.Background()
	e, disks := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	mustPut(t, e, "bkt", "real", "committed")

	ghost := fsMeta{Name: "ghost", Versions: []fsVersion{{
		VersionID: NullVersionID, DataID: "ghost", Size: 3, ETag: "ghost-etag", ModTime: time.Now().UTC(),
	}}}
	data, _ := json.Marshal(ghost)
	disks[1].putFile(diskMetaPath("bkt", "ghost"), data)

	res, err := e.ListObjects(ctx, "bkt", ListOptions{})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(res.Objects) != 1 || res.Objects[0].Name != "real" {
		t.Fatalf("ListObjects = %+v, want only [real]", res.Objects)
	}
}

// TestReadQuorumNotReached 校验分片不足读法定人数时返回 ErrReadQuorum（而不是返回坏数据）。
func TestReadQuorumNotReached(t *testing.T) {
	e, disks := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	mustPut(t, e, "bkt", "obj", "payload")

	// 丢掉 3 块盘上的分片，只剩 1 份 < read quorum(2)
	meta, err := e.readMetaQuorum(context.Background(), "bkt", "obj")
	if err != nil {
		t.Fatalf("readMetaQuorum: %v", err)
	}
	key := meta.Versions[0].dataKey()
	for _, d := range disks[1:] {
		_ = d.DeleteFile(context.Background(), diskObjectPartPath("bkt", "obj", key, 1))
	}
	if _, err := getBody(t, e, "bkt", "obj"); !errors.Is(err, ErrReadQuorum) {
		t.Fatalf("GetObject = %v, want ErrReadQuorum", err)
	}
}

// TestMakeBucketNeedsWriteQuorum 校验 bucket 创建同样受写法定人数约束。
func TestMakeBucketNeedsWriteQuorum(t *testing.T) {
	e, disks := newTestErasure(t, 4, 2, 2)
	disks[2].offline = true
	disks[3].offline = true
	if err := e.MakeBucket(context.Background(), "bkt"); !errors.Is(err, ErrWriteQuorum) {
		t.Fatalf("MakeBucket = %v, want ErrWriteQuorum", err)
	}
}

// TestBucketExistsNeedsReadQuorum 校验可见盘不足时不会谎报「bucket 不存在」。
func TestBucketExistsNeedsReadQuorum(t *testing.T) {
	ctx := context.Background()
	e, disks := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")

	if _, ok, err := e.BucketExists(ctx, "bkt"); err != nil || !ok {
		t.Fatalf("all drives up: ok=%v err=%v, want true/nil", ok, err)
	}
	if _, ok, err := e.BucketExists(ctx, "missing"); err != nil || ok {
		t.Fatalf("missing bucket: ok=%v err=%v, want false/nil", ok, err)
	}

	// 掉两块盘：仍有 2 块可见 = 读法定人数，可以判定
	disks[2].offline = true
	disks[3].offline = true
	if _, ok, err := e.BucketExists(ctx, "bkt"); err != nil || !ok {
		t.Fatalf("2 visible drives: ok=%v err=%v, want true/nil", ok, err)
	}

	// 只剩 1 块盘可见：无法判定，必须报 ErrReadQuorum
	disks[1].offline = true
	if _, _, err := e.BucketExists(ctx, "bkt"); !errors.Is(err, ErrReadQuorum) {
		t.Fatalf("1 visible drive: err=%v, want ErrReadQuorum", err)
	}
}

// failingLocker 模拟「拿不到命名空间锁」。
type failingLocker struct{}

func (failingLocker) LockWrite(context.Context, string) (func(), error) {
	return nil, errors.New("no lock quorum")
}
func (failingLocker) LockRead(context.Context, string) (func(), error) {
	return nil, errors.New("no lock quorum")
}

// TestWriteFailsWithoutNamespaceLock 校验拿不到命名空间锁时写/删直接失败（不会绕过锁继续写）。
func TestWriteFailsWithoutNamespaceLock(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	mustPut(t, e, "bkt", "obj", "committed")

	e.locker = failingLocker{}
	if _, err := e.PutObject(ctx, "bkt", "obj", strings.NewReader("nope"), 4, "text/plain", nil); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("PutObject = %v, want ErrLockTimeout", err)
	}
	if _, err := e.DeleteObject(ctx, "bkt", "obj", ""); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("DeleteObject = %v, want ErrLockTimeout", err)
	}
}

// TestDeleteObjectCommitsBeforeDataRemoval 校验删除提交后对象不再可见。
func TestDeleteObjectCommitsBeforeDataRemoval(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	mustPut(t, e, "bkt", "obj", "payload")

	if _, err := e.DeleteObject(ctx, "bkt", "obj", ""); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, err := getBody(t, e, "bkt", "obj"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("GetObject after delete = %v, want ErrObjectNotFound", err)
	}
	res, err := e.ListObjects(ctx, "bkt", ListOptions{})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(res.Objects) != 0 {
		t.Fatalf("ListObjects after delete = %+v, want empty", res.Objects)
	}
}

// TestVersionedPutKeepsPreviousVersionReadable 校验版本控制下新旧版本各自可读。
func TestVersionedPutKeepsPreviousVersionReadable(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	if err := e.SetBucketVersioning(ctx, "bkt", VersioningEnabled); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}
	v1 := mustPut(t, e, "bkt", "obj", "one")
	v2 := mustPut(t, e, "bkt", "obj", "two")

	if got, err := getBody(t, e, "bkt", "obj"); err != nil || got != "two" {
		t.Fatalf("latest = %q err=%v, want two", got, err)
	}
	rc, _, err := e.GetObject(ctx, "bkt", "obj", v1.VersionID)
	if err != nil {
		t.Fatalf("GetObject(v1): %v", err)
	}
	defer func() { _ = rc.Close() }()
	data, _ := io.ReadAll(rc)
	if string(data) != "one" {
		t.Fatalf("v1 body = %q, want one", string(data))
	}
	if v1.VersionID == v2.VersionID {
		t.Fatal("version ids must differ")
	}
}

// TestBytesReaderPut 是给编码器的一个兜底用例：大一点的内容往返一致。
func TestBytesReaderPut(t *testing.T) {
	e, _ := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	payload := bytes.Repeat([]byte("gos3-quorum-"), 512)
	if _, err := e.PutObject(context.Background(), "bkt", "big", bytes.NewReader(payload), int64(len(payload)), "application/octet-stream", nil); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	body, err := getBody(t, e, "bkt", "big")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if body != string(payload) {
		t.Fatalf("round trip mismatch: got %d bytes want %d", len(body), len(payload))
	}
}

// multipartMeta 读取已完成对象的元数据（测试辅助）。
func multipartMeta(t *testing.T, e *Erasure, bucket, object string) fsMeta {
	t.Helper()
	meta, err := e.readMetaQuorum(context.Background(), bucket, object)
	if err != nil {
		t.Fatalf("readMetaQuorum: %v", err)
	}
	return meta
}

// TestMultipartUploadDistributed 校验分片上传不再依赖 disks[0]：
// 每个 part 独立纠删编码到所有盘，完成时只做 rename + 提交元数据，对象可完整读回。
func TestMultipartUploadDistributed(t *testing.T) {
	ctx := context.Background()
	e, disks := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")

	uploadID, err := e.NewMultipartUpload(ctx, "bkt", "obj", "text/plain", map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("NewMultipartUpload: %v", err)
	}
	part1, err := e.PutObjectPart(ctx, "bkt", "obj", uploadID, 1, strings.NewReader("part-one-"))
	if err != nil {
		t.Fatalf("PutObjectPart 1: %v", err)
	}
	part2, err := e.PutObjectPart(ctx, "bkt", "obj", uploadID, 2, strings.NewReader("part-two"))
	if err != nil {
		t.Fatalf("PutObjectPart 2: %v", err)
	}

	parts, err := e.ListObjectParts(ctx, "bkt", "obj", uploadID)
	if err != nil {
		t.Fatalf("ListObjectParts: %v", err)
	}
	if len(parts) != 2 || parts[0].PartNumber != 1 || parts[1].PartNumber != 2 {
		t.Fatalf("ListObjectParts = %+v", parts)
	}

	info, err := e.CompleteMultipartUpload(ctx, "bkt", "obj", uploadID, []CompletePart{
		{PartNumber: 1, ETag: part1.ETag},
		{PartNumber: 2, ETag: part2.ETag},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if info.Size != int64(len("part-one-")+len("part-two")) {
		t.Fatalf("size = %d", info.Size)
	}
	if !strings.HasSuffix(info.ETag, "-2") {
		t.Fatalf("composite etag = %s, want suffix -2", info.ETag)
	}

	body, err := getBody(t, e, "bkt", "obj")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if body != "part-one-part-two" {
		t.Fatalf("body = %q", body)
	}

	// 每个 part 的分片必须落在**所有**盘上（不再是 disks[0] 单点）
	meta := multipartMeta(t, e, "bkt", "obj")
	version := meta.Versions[0]
	if len(version.Parts) != 2 {
		t.Fatalf("meta versions parts = %+v", version.Parts)
	}
	for _, d := range disks {
		for _, part := range version.Parts {
			if got := d.filesWithPrefix(diskObjectPartPath("bkt", "obj", version.dataKey(), part.Number)); len(got) != 1 {
				t.Fatalf("disk %s missing shard for part %d: %v", d.ID(), part.Number, got)
			}
		}
	}
	// 上传暂存目录应已清理
	for _, d := range disks {
		if got := d.filesWithPrefix(diskUploadDir("bkt", uploadID)); len(got) != 0 {
			t.Fatalf("disk %s left upload staging files: %v", d.ID(), got)
		}
	}
}

// TestMultipartReadableAfterDriveLoss 校验完成后的对象在丢掉一块盘的分片后仍可完整读取。
func TestMultipartReadableAfterDriveLoss(t *testing.T) {
	ctx := context.Background()
	e, disks := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	uploadID, _ := e.NewMultipartUpload(ctx, "bkt", "obj", "text/plain", nil)
	p1, err := e.PutObjectPart(ctx, "bkt", "obj", uploadID, 1, strings.NewReader("alpha-"))
	if err != nil {
		t.Fatalf("PutObjectPart: %v", err)
	}
	p2, err := e.PutObjectPart(ctx, "bkt", "obj", uploadID, 2, strings.NewReader("beta"))
	if err != nil {
		t.Fatalf("PutObjectPart: %v", err)
	}
	if _, err := e.CompleteMultipartUpload(ctx, "bkt", "obj", uploadID, []CompletePart{
		{PartNumber: 1, ETag: p1.ETag}, {PartNumber: 2, ETag: p2.ETag},
	}); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	// 模拟「原第一块盘」丢失：删掉 disks[0] 上的所有 part 分片
	meta := multipartMeta(t, e, "bkt", "obj")
	version := meta.Versions[0]
	for _, part := range version.Parts {
		_ = disks[0].DeleteFile(ctx, diskObjectPartPath("bkt", "obj", version.dataKey(), part.Number))
	}
	body, err := getBody(t, e, "bkt", "obj")
	if err != nil {
		t.Fatalf("GetObject after drive loss: %v", err)
	}
	if body != "alpha-beta" {
		t.Fatalf("body = %q", body)
	}
}

// TestListObjectPartsQuorum 校验只写到少数盘的 part 元数据不会被列出。
func TestListObjectPartsQuorum(t *testing.T) {
	ctx := context.Background()
	e, disks := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	uploadID, _ := e.NewMultipartUpload(ctx, "bkt", "obj", "text/plain", nil)
	if _, err := e.PutObjectPart(ctx, "bkt", "obj", uploadID, 1, strings.NewReader("payload")); err != nil {
		t.Fatalf("PutObjectPart: %v", err)
	}
	// 只保留一块盘上的 part 元数据：达不到读法定人数，不应出现在结果里
	for _, d := range disks[1:] {
		_ = d.DeleteFile(ctx, diskPartMetaPath("bkt", uploadID, 1))
	}
	parts, err := e.ListObjectParts(ctx, "bkt", "obj", uploadID)
	if err != nil {
		t.Fatalf("ListObjectParts: %v", err)
	}
	if len(parts) != 0 {
		t.Fatalf("parts = %+v, want empty (unquorum part meta)", parts)
	}
}

// TestAbortMultipartCleansAllDrives 校验 abort 会清理所有盘上的暂存目录。
func TestAbortMultipartCleansAllDrives(t *testing.T) {
	ctx := context.Background()
	e, disks := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	uploadID, _ := e.NewMultipartUpload(ctx, "bkt", "obj", "text/plain", nil)
	if _, err := e.PutObjectPart(ctx, "bkt", "obj", uploadID, 1, strings.NewReader("payload")); err != nil {
		t.Fatalf("PutObjectPart: %v", err)
	}
	if err := e.AbortMultipartUpload(ctx, "bkt", "obj", uploadID); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}
	for _, d := range disks {
		if got := d.filesWithPrefix(diskUploadDir("bkt", uploadID)); len(got) != 0 {
			t.Fatalf("disk %s still has upload files: %v", d.ID(), got)
		}
	}
	if _, err := e.ListObjectParts(ctx, "bkt", "obj", uploadID); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("ListObjectParts after abort = %v, want ErrUploadNotFound", err)
	}
}

// TestCompleteMultipartRollbackKeepsUpload 校验 completion 提交失败时把分片搬回上传目录，上传仍可重试。
func TestCompleteMultipartRollbackKeepsUpload(t *testing.T) {
	ctx := context.Background()
	e, disks := newTestErasure(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	uploadID, _ := e.NewMultipartUpload(ctx, "bkt", "obj", "text/plain", nil)
	p1, err := e.PutObjectPart(ctx, "bkt", "obj", uploadID, 1, strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("PutObjectPart: %v", err)
	}

	// 两块盘 rename 失败：part 无法达到写法定人数
	disks[2].failRename = true
	disks[3].failRename = true
	if _, err := e.CompleteMultipartUpload(ctx, "bkt", "obj", uploadID, []CompletePart{{PartNumber: 1, ETag: p1.ETag}}); !errors.Is(err, ErrWriteQuorum) {
		t.Fatalf("CompleteMultipartUpload = %v, want ErrWriteQuorum", err)
	}
	// 失败后上传仍完整：part 元数据还在，分片被搬回暂存目录
	parts, err := e.ListObjectParts(ctx, "bkt", "obj", uploadID)
	if err != nil || len(parts) != 1 {
		t.Fatalf("ListObjectParts after failed complete = %+v err=%v", parts, err)
	}

	// 恢复后重试成功
	disks[2].failRename = false
	disks[3].failRename = false
	if _, err := e.CompleteMultipartUpload(ctx, "bkt", "obj", uploadID, []CompletePart{{PartNumber: 1, ETag: p1.ETag}}); err != nil {
		t.Fatalf("retry CompleteMultipartUpload: %v", err)
	}
	if body, err := getBody(t, e, "bkt", "obj"); err != nil || body != "payload" {
		t.Fatalf("body = %q err = %v", body, err)
	}
}

// newTestErasureWithHealer 建一个带修复队列的纠删码后端（用于自愈测试）。
func newTestErasureWithHealer(t *testing.T, n, data, parity int) (*Erasure, []*memDisk, *heal.Queue) {
	t.Helper()
	disks := make([]*memDisk, n)
	list := make([]disk.Disk, n)
	for i := range disks {
		disks[i] = newMemDisk("mem/" + string(rune('a'+i)))
		list[i] = disks[i]
	}
	q := heal.NewQueue(testLogger())
	e, err := NewErasure(list, data, parity, testLogger(), noopLocker{}, q)
	if err != nil {
		t.Fatalf("NewErasure: %v", err)
	}
	return e, disks, q
}

// TestReadSchedulesAndRebuildsMissingShard 是 P5 的核心验收：
// 人为删掉某块盘的分片后，一次 GET 会把修复任务入队，后台 worker 用其余分片重建并写回。
func TestReadSchedulesAndRebuildsMissingShard(t *testing.T) {
	ctx := context.Background()
	e, disks, q := newTestErasureWithHealer(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	mustPut(t, e, "bkt", "obj", "heal-me")

	meta := multipartMeta(t, e, "bkt", "obj")
	version := meta.Versions[0]
	path := diskObjectPartPath("bkt", "obj", version.dataKey(), 1)

	// 人为删除 disks[3] 上的分片：读 quorum(2) 仍然满足，所以 GET 成功
	if err := disks[3].DeleteFile(ctx, path); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	if _, present := e.readShardsAt(ctx, path); present != 3 {
		t.Fatalf("present = %d, want 3", present)
	}
	if body, err := getBody(t, e, "bkt", "obj"); err != nil || body != "heal-me" {
		t.Fatalf("GetObject = %q err=%v", body, err)
	}
	if q.Len() != 1 {
		t.Fatalf("heal queue length = %d, want 1", q.Len())
	}

	// 启动 worker：应当把缺失分片补齐
	q.SetRetryDelay(5 * time.Millisecond)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go q.Run(cctx, e, nil)

	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, present := e.readShardsAt(ctx, path); present == 4 {
			break
		}
		if time.Now().After(deadline) {
			_, present := e.readShardsAt(ctx, path)
			t.Fatalf("shard was not rebuilt: present = %d, queue = %d", present, q.Len())
		}
		time.Sleep(10 * time.Millisecond)
	}
	rebuilt, gaveUp := q.Stats()
	if rebuilt != 1 || gaveUp != 0 {
		t.Fatalf("heal stats: rebuilt=%d gaveUp=%d", rebuilt, gaveUp)
	}
	// 补齐后数据仍然正确
	if body, err := getBody(t, e, "bkt", "obj"); err != nil || body != "heal-me" {
		t.Fatalf("GetObject after heal = %q err=%v", body, err)
	}
}

// TestRebuildRefusesWithoutReadQuorum 校验分片不足读 quorum 时不会「猜」数据。
func TestRebuildRefusesWithoutReadQuorum(t *testing.T) {
	ctx := context.Background()
	e, disks, _ := newTestErasureWithHealer(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	mustPut(t, e, "bkt", "obj", "payload")

	meta := multipartMeta(t, e, "bkt", "obj")
	version := meta.Versions[0]
	path := diskObjectPartPath("bkt", "obj", version.dataKey(), 1)
	// 只剩 1 份分片 < 读 quorum(2)
	for _, d := range disks[1:] {
		_ = d.DeleteFile(ctx, path)
	}
	err := e.Rebuild(ctx, heal.Task{Bucket: "bkt", Object: "obj", DataKey: version.dataKey(), PartNumber: 1, Size: version.Size, DiskIndex: 1})
	if !errors.Is(err, ErrReadQuorum) {
		t.Fatalf("Rebuild = %v, want ErrReadQuorum", err)
	}
}

// TestMultipartReadTriggersHeal 校验 part 化对象（分片上传结果）同样能触发并完成自愈。
func TestMultipartReadTriggersHeal(t *testing.T) {
	ctx := context.Background()
	e, disks, q := newTestErasureWithHealer(t, 4, 2, 2)
	mustMakeBucket(t, e, "bkt")
	uploadID, _ := e.NewMultipartUpload(ctx, "bkt", "obj", "text/plain", nil)
	p1, err := e.PutObjectPart(ctx, "bkt", "obj", uploadID, 1, strings.NewReader("part-one-"))
	if err != nil {
		t.Fatalf("PutObjectPart 1: %v", err)
	}
	p2, err := e.PutObjectPart(ctx, "bkt", "obj", uploadID, 2, strings.NewReader("part-two"))
	if err != nil {
		t.Fatalf("PutObjectPart 2: %v", err)
	}
	if _, err := e.CompleteMultipartUpload(ctx, "bkt", "obj", uploadID, []CompletePart{
		{PartNumber: 1, ETag: p1.ETag}, {PartNumber: 2, ETag: p2.ETag},
	}); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	meta := multipartMeta(t, e, "bkt", "obj")
	version := meta.Versions[0]
	part2Path := diskObjectPartPath("bkt", "obj", version.dataKey(), 2)
	_ = disks[0].DeleteFile(ctx, part2Path)

	if body, err := getBody(t, e, "bkt", "obj"); err != nil || body != "part-one-part-two" {
		t.Fatalf("GetObject = %q err=%v", body, err)
	}
	if q.Len() != 1 {
		t.Fatalf("heal queue = %d, want 1", q.Len())
	}
	q.SetRetryDelay(5 * time.Millisecond)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go q.Run(cctx, e, nil)
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, present := e.readShardsAt(ctx, part2Path); present == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("multipart shard was not rebuilt")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
