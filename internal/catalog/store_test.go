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

	if _, err := idx.DeleteObject(context.Background(), "bkt", "obj1", ""); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return idx.Engine().Status().Docs == 0 })
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
