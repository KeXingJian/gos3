package catalog

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kxj/gos3/internal/store"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timeout waiting for condition")
}

func TestIndexedPutDelete(t *testing.T) {
	dir := t.TempDir()
	logger := testLogger()
	st, err := store.NewFS(dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Dir: filepath.Join(dir, ".catalog"), Gen: 0, Wal: WalConfig{SyncEvery: 1}}
	idx, err := New(st, cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	if err := idx.MakeBucket(context.Background(), "bkt"); err != nil {
		t.Fatal(err)
	}
	_, err = idx.PutObject(context.Background(), "bkt", "obj1", strings.NewReader("hello"), 5, "text/plain", map[string]string{"x-amz-meta-owner": "alice"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return idx.Engine().Status().Docs == 1 })

	d, ok := idx.Engine().View().primary.Get(primaryKey("bkt", "obj1", "null"))
	if !ok {
		t.Fatal("doc missing in index")
	}
	if d.Size != 5 || d.ContentType != "text/plain" || d.UserMeta["x-amz-meta-owner"] != "alice" {
		t.Fatalf("doc mismatch: %+v", d)
	}

	if err := idx.PutObjectTagging(context.Background(), "bkt", "obj1", "", map[string]string{"env": "prod"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		d, _ := idx.Engine().View().primary.Get(primaryKey("bkt", "obj1", "null"))
		return d != nil && d.Tags["env"] == "prod"
	})
	if _, err := idx.DeleteObject(context.Background(), "bkt", "obj1", ""); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return idx.Engine().Status().Docs == 0 })
}

// TestIndexedDeleteObjects 批量删除必须用底层真实结果（"null" 版本）入索引，
// 不能拿请求里的空 VersionID 直接拼主键，否则会留下永远删不掉的脏文档。
func TestIndexedDeleteObjects(t *testing.T) {
	dir := t.TempDir()
	logger := testLogger()
	st, err := store.NewFS(dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := New(st, Config{Dir: filepath.Join(dir, ".catalog"), Wal: WalConfig{SyncEvery: 1}}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	ctx := context.Background()
	if err := idx.MakeBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if _, err := idx.PutObject(ctx, "bkt", key, strings.NewReader("x"), 1, "text/plain", nil); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 2*time.Second, func() bool { return idx.Engine().Status().Docs == 3 })

	errs := idx.DeleteObjects(ctx, "bkt", []store.ObjectToDelete{{Object: "a"}, {Object: "b"}})
	for i, err := range errs {
		if err != nil {
			t.Fatalf("delete objects[%d]: %v", i, err)
		}
	}
	waitFor(t, 2*time.Second, func() bool { return idx.Engine().Status().Docs == 1 })
	if _, ok := idx.Engine().View().primary.Get(primaryKey("bkt", "a", "null")); ok {
		t.Fatal("deleted object a still in index")
	}
	if _, ok := idx.Engine().View().primary.Get(primaryKey("bkt", "c", "null")); !ok {
		t.Fatal("object c should remain")
	}
}

// TestIndexedDeleteObjectsCreatesDeleteMarker 版本化桶的批量删除应生成删除标记，
// 而不是把请求里的空 VersionID 当成硬删。
func TestIndexedDeleteObjectsCreatesDeleteMarker(t *testing.T) {
	dir := t.TempDir()
	logger := testLogger()
	st, err := store.NewFS(dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := New(st, Config{Dir: filepath.Join(dir, ".catalog"), Wal: WalConfig{SyncEvery: 1}}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	ctx := context.Background()
	if err := idx.MakeBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	if err := idx.SetBucketVersioning(ctx, "bkt", store.VersioningEnabled); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.PutObject(ctx, "bkt", "obj", strings.NewReader("x"), 1, "text/plain", nil); err != nil {
		t.Fatal(err)
	}
	if errs := idx.DeleteObjects(ctx, "bkt", []store.ObjectToDelete{{Object: "obj"}}); len(errs) != 1 || errs[0] != nil {
		t.Fatalf("delete objects errs = %v", errs)
	}

	var marker, old *Doc
	waitFor(t, 2*time.Second, func() bool {
		res, err := idx.Search(Filter{Bucket: "bkt", AllVersions: true, IncludeDeleteMarkers: true})
		if err != nil {
			return false
		}
		marker, old = nil, nil
		for i := range res.Docs {
			d := &res.Docs[i]
			if d.DeleteMarker {
				marker = d
			} else {
				old = d
			}
		}
		return marker != nil && old != nil
	})
	if !marker.IsLatest || old.IsLatest {
		t.Fatalf("marker latest=%v old latest=%v", marker.IsLatest, old.IsLatest)
	}
}

// TestIndexedDeleteLatestPromotesPrevious 版本化桶删除最新版本后，
// 索引里的上一个版本必须变成 IsLatest，查询才不会漏对象。
func TestIndexedDeleteLatestPromotesPrevious(t *testing.T) {
	dir := t.TempDir()
	logger := testLogger()
	st, err := store.NewFS(dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := New(st, Config{Dir: filepath.Join(dir, ".catalog"), Wal: WalConfig{SyncEvery: 1}}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	ctx := context.Background()
	if err := idx.MakeBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	if err := idx.SetBucketVersioning(ctx, "bkt", store.VersioningEnabled); err != nil {
		t.Fatal(err)
	}
	v1, err := idx.PutObject(ctx, "bkt", "obj", strings.NewReader("one"), 3, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := idx.PutObject(ctx, "bkt", "obj", strings.NewReader("two"), 3, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return idx.Engine().Status().Docs == 2 })

	if _, err := idx.DeleteObject(ctx, "bkt", "obj", v2.VersionID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		d, ok := idx.Engine().View().primary.Get(primaryKey("bkt", "obj", v1.VersionID))
		return ok && d.IsLatest
	})
}

// TestIndexedRebuildVersionOrder 重建时 WalkVersions 是「新到旧」，引擎 Seq 必须
// 反过来分配（旧到新），否则删除最新版本后 promoteLatest 会选到最旧的版本。
func TestIndexedRebuildVersionOrder(t *testing.T) {
	dir := t.TempDir()
	logger := testLogger()
	st, err := store.NewFS(dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := st.MakeBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetBucketVersioning(ctx, "bkt", store.VersioningEnabled); err != nil {
		t.Fatal(err)
	}
	var versions []string
	for _, body := range []string{"v1", "v2", "v3"} {
		info, err := st.PutObject(ctx, "bkt", "obj", strings.NewReader(body), 2, "text/plain", nil)
		if err != nil {
			t.Fatal(err)
		}
		versions = append(versions, info.VersionID)
	}

	// 创建 catalog 时走 no-manifest 全量重建。
	idx, err := New(st, Config{Dir: filepath.Join(dir, ".catalog"), Wal: WalConfig{SyncEvery: 1}}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	waitFor(t, 2*time.Second, func() bool { return idx.Engine().Status().Docs == 3 })

	if _, err := idx.DeleteObject(ctx, "bkt", "obj", versions[2]); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		res, err := idx.Search(Filter{Bucket: "bkt", AllVersions: true})
		if err != nil {
			return false
		}
		for _, d := range res.Docs {
			if d.VersionID == versions[1] {
				return d.IsLatest
			}
		}
		return false
	})
}

func TestIndexedRebuildFromStore(t *testing.T) {
	dir := t.TempDir()
	logger := testLogger()
	st, err := store.NewFS(dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MakeBucket(context.Background(), "bkt"); err != nil {
		t.Fatal(err)
	}
	// 绕过装饰器，直接往底层 store 写对象（模拟「索引丢了但数据还在」）
	for i := 0; i < 5; i++ {
		_, err := st.PutObject(context.Background(), "bkt", "obj-"+string(rune('a'+i)), strings.NewReader("x"), 1, "text/plain", nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	// 创建 catalog：引擎应通过 rebuild 源从 sidecar 重建出 5 个对象
	cfg := Config{Dir: filepath.Join(dir, ".catalog"), Gen: 0, Wal: WalConfig{SyncEvery: 1}}
	idx, err := New(st, cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	if docs := idx.Engine().Status().Docs; docs != 5 {
		t.Fatalf("docs = %d, want 5 (from rebuild)", docs)
	}
	for i := 0; i < 5; i++ {
		key := "obj-" + string(rune('a'+i))
		if _, ok := idx.Engine().View().primary.Get(primaryKey("bkt", key, "null")); !ok {
			t.Fatalf("rebuilt doc %q missing", key)
		}
	}
}
