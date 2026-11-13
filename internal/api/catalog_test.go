package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kxj/gos3/internal/catalog"
)

type fakeCatalog struct {
	lastFilter catalog.Filter
	lastStats  catalog.StatsOptions
	searchRes  catalog.SearchResult
	searchErr  error
	statsRes   catalog.StatsResult
	statsErr   error
	status     catalog.Status
	rebuildErr error
	rebuilt    bool
}

func (f *fakeCatalog) Search(q catalog.Filter) (catalog.SearchResult, error) {
	f.lastFilter = q
	return f.searchRes, f.searchErr
}

func (f *fakeCatalog) Stats(opts catalog.StatsOptions) (catalog.StatsResult, error) {
	f.lastStats = opts
	return f.statsRes, f.statsErr
}

func (f *fakeCatalog) Status() catalog.Status { return f.status }

func (f *fakeCatalog) Rebuild() error {
	f.rebuilt = true
	return f.rebuildErr
}

func TestAdminCatalogRequiresRoot(t *testing.T) {
	h := newAdminTestHandler(t)
	h.Catalog = &fakeCatalog{}

	rec := doAdmin(h, http.MethodGet, "/gos3/admin/catalog/search", "alice", "alicepass")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("alice search code=%d, want 403", rec.Code)
	}
	rec = doAdmin(h, http.MethodGet, "/gos3/admin/catalog/status", "alice", "alicepass")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("alice status code=%d, want 403", rec.Code)
	}
}

func TestAdminCatalogDisabled(t *testing.T) {
	h := newAdminTestHandler(t)
	rec := doAdmin(h, http.MethodGet, "/gos3/admin/catalog/status", "root", "rootpass")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled catalog code=%d, want 503 body=%s", rec.Code, rec.Body.String())
	}
}

func TestAdminCatalogSearchParsing(t *testing.T) {
	h := newAdminTestHandler(t)
	fc := &fakeCatalog{searchRes: catalog.SearchResult{
		Gen:       7,
		Scan:      "index",
		Truncated: false,
		Docs:      []catalog.Doc{{Bucket: "b", Key: "k", VersionID: "null"}},
	}}
	h.Catalog = fc

	target := "/gos3/admin/catalog/search?bucket=b&prefix=p&contentType=text/plain" +
		"&minSize=1&maxSize=10&after=2026-01-02T03:04:05Z" +
		"&meta.x-amz-meta-owner=alice&tag.env=prod&versions=true&deleteMarkers=true&limit=5"
	rec := doAdmin(h, http.MethodGet, target, "root", "rootpass")
	if rec.Code != http.StatusOK {
		t.Fatalf("search code=%d body=%s", rec.Code, rec.Body.String())
	}

	f := fc.lastFilter
	if f.Bucket != "b" || f.Prefix != "p" || f.ContentType != "text/plain" {
		t.Fatalf("basic filter = %+v", f)
	}
	if !f.MinSizeSet || f.MinSize != 1 || !f.MaxSizeSet || f.MaxSize != 10 {
		t.Fatalf("size filter = %+v", f)
	}
	if f.After.Format(time.RFC3339) != "2026-01-02T03:04:05Z" {
		t.Fatalf("after = %v", f.After)
	}
	if f.Meta["owner"] != "alice" {
		t.Fatalf("meta filter = %+v", f.Meta)
	}
	if f.Tag["env"] != "prod" {
		t.Fatalf("tag filter = %+v", f.Tag)
	}
	if !f.AllVersions || !f.IncludeDeleteMarkers || f.Limit != 5 {
		t.Fatalf("flags = %+v", f)
	}
	if got := rec.Header().Get("X-Gos3-Catalog-Scan"); got != "index" {
		t.Fatalf("scan header = %q", got)
	}
	if got := rec.Header().Get("X-Gos3-Catalog-Gen"); got != "7" {
		t.Fatalf("gen header = %q", got)
	}

	var body catalog.SearchResult
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Gen != 7 || len(body.Docs) != 1 || body.Docs[0].Key != "k" {
		t.Fatalf("body = %+v", body)
	}
}

func TestAdminCatalogSQL(t *testing.T) {
	h := newAdminTestHandler(t)
	fc := &fakeCatalog{searchRes: catalog.SearchResult{Gen: 2, Scan: "bucket", Docs: []catalog.Doc{{Bucket: "b", Key: "k", VersionID: "null", Size: 4, IsLatest: true}}}}
	h.Catalog = fc
	req := httptest.NewRequest(http.MethodPost, "/gos3/admin/catalog/sql", strings.NewReader(`{"query":"SELECT key,size FROM S3Object WHERE bucket = 'b'"}`))
	req.SetBasicAuth("root", "rootpass")
	rec := httptest.NewRecorder()
	h.ServeAdmin(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"key":"k"`) {
		t.Fatalf("SQL code=%d body=%s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, "/gos3/admin/catalog/sql", strings.NewReader(`{"query":"DROP TABLE S3Object"}`))
	req.SetBasicAuth("root", "rootpass")
	rec = httptest.NewRecorder()
	h.ServeAdmin(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unsupported SQL code=%d", rec.Code)
	}
}

func TestAdminCatalogErrors(t *testing.T) {
	h := newAdminTestHandler(t)
	fc := &fakeCatalog{}
	h.Catalog = fc

	rec := doAdmin(h, http.MethodGet, "/gos3/admin/catalog/search?minSize=-1", "root", "rootpass")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("negative minSize code=%d, want 400", rec.Code)
	}
	rec = doAdmin(h, http.MethodGet, "/gos3/admin/catalog/search?limit=abc", "root", "rootpass")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad limit code=%d, want 400", rec.Code)
	}

	fc.searchErr = catalog.ErrRestartQuery
	rec = doAdmin(h, http.MethodGet, "/gos3/admin/catalog/search?bucket=b", "root", "rootpass")
	if rec.Code != http.StatusConflict {
		t.Fatalf("restart query code=%d, want 409", rec.Code)
	}

	fc.searchErr = catalog.ErrInvalidToken
	rec = doAdmin(h, http.MethodGet, "/gos3/admin/catalog/search?bucket=b", "root", "rootpass")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid token code=%d, want 400", rec.Code)
	}
}

func TestAdminCatalogStatsStatusReindex(t *testing.T) {
	h := newAdminTestHandler(t)
	fc := &fakeCatalog{
		statsRes: catalog.StatsResult{Gen: 3, Docs: 2, TotalSize: 30},
		status:   catalog.Status{Docs: 2, AppliedSeq: 9, Gen: 3, Dropped: 1, State: "lagging"},
	}
	h.Catalog = fc

	rec := doAdmin(h, http.MethodGet, "/gos3/admin/catalog/stats?bucket=b&groupBy=contentType&versions=true", "root", "rootpass")
	if rec.Code != http.StatusOK {
		t.Fatalf("stats code=%d body=%s", rec.Code, rec.Body.String())
	}
	if fc.lastStats.Bucket != "b" || fc.lastStats.GroupBy != "contentType" || !fc.lastStats.AllVersions {
		t.Fatalf("stats opts = %+v", fc.lastStats)
	}

	rec = doAdmin(h, http.MethodGet, "/gos3/admin/catalog/status", "root", "rootpass")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"dropped":1`) {
		t.Fatalf("status code=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = doAdmin(h, http.MethodGet, "/gos3/admin/catalog/reindex", "root", "rootpass")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET reindex code=%d, want 405", rec.Code)
	}
	rec = doAdmin(h, http.MethodPost, "/gos3/admin/catalog/reindex", "root", "rootpass")
	if rec.Code != http.StatusOK || !fc.rebuilt {
		t.Fatalf("POST reindex code=%d rebuilt=%v body=%s", rec.Code, fc.rebuilt, rec.Body.String())
	}
}
