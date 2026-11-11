package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kxj/gos3/internal/auth"
	"github.com/kxj/gos3/internal/catalog"
	"github.com/kxj/gos3/internal/config"
	"github.com/kxj/gos3/internal/iam"
	"github.com/kxj/gos3/internal/store"
)

// TestCatalogAdminAPIEndToEnd 验证 -catalog 的接线：Indexed store -> server.New
// 自动注入 CatalogService -> /gos3/admin/catalog/search 能查到刚写入的对象。
func TestCatalogAdminAPIEndToEnd(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	st, err := store.NewFS(dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := catalog.New(st, catalog.Config{
		Dir: filepath.Join(dir, ".catalog"),
		Wal: catalog.WalConfig{SyncEvery: 1},
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	iamStore, err := iam.New(filepath.Join(dir, ".iam"), auth.Credentials{AccessKey: "root", SecretKey: "rootpass"}, logger)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.RootUser = "root"
	cfg.RootPass = "rootpass"
	srv := New(cfg, idx, iamStore, logger, nil)

	ctx := context.Background()
	if err := idx.MakeBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.PutObject(ctx, "bkt", "k", strings.NewReader("hello"), 5, "text/plain", nil); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for idx.Status().Docs != 1 {
		if time.Now().After(deadline) {
			t.Fatal("catalog did not index object")
		}
		time.Sleep(2 * time.Millisecond)
	}

	req := httptest.NewRequest(http.MethodGet, "/gos3/admin/catalog/search?bucket=bkt&contentType=text/plain", nil)
	req.SetBasicAuth("root", "rootpass")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("search code=%d body=%s", rec.Code, rec.Body.String())
	}

	var body catalog.SearchResult
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Docs) != 1 || body.Docs[0].Key != "k" || body.Docs[0].Size != 5 {
		t.Fatalf("search body = %+v", body)
	}
	if body.Scan != "index" {
		t.Fatalf("scan = %q, want index", body.Scan)
	}
}
