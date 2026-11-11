package catalog

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// queryIndex 构造一个带白名单的索引，供查询测试复用。
func queryIndex(t *testing.T) *memoryIndex {
	t.Helper()
	ix := newMemoryIndex(IndexConfig{
		MetaWhitelist: map[string]struct{}{"owner": {}},
		TagWhitelist:  map[string]struct{}{"env": {}},
	})
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	docs := []Doc{
		{
			Bucket: "b", Key: "photo/a.jpg", VersionID: "v1", IsLatest: false,
			Size: 900, ContentType: "image/jpeg", ModTime: base,
			UserMeta: map[string]string{"owner": "alice"},
			Tags:     map[string]string{"env": "prod"},
		},
		{
			Bucket: "b", Key: "photo/a.jpg", VersionID: "v2", IsLatest: true,
			Size: 2048, ContentType: "image/jpeg", ModTime: base.Add(time.Hour),
			UserMeta: map[string]string{"owner": "alice"},
			Tags:     map[string]string{"env": "prod"},
		},
		{
			Bucket: "b", Key: "photo/b.png", VersionID: "null", IsLatest: true,
			Size: 512, ContentType: "image/png", ModTime: base.Add(2 * time.Hour),
			UserMeta: map[string]string{"owner": "bob"},
			Tags:     map[string]string{"env": "dev"},
		},
		{
			Bucket: "b", Key: "docs/readme.txt", VersionID: "null", IsLatest: true,
			Size: 100, ContentType: "text/plain", ModTime: base.Add(3 * time.Hour),
		},
		{
			Bucket: "b", Key: "gone.txt", VersionID: "dm1", IsLatest: true,
			DeleteMarker: true, ModTime: base.Add(4 * time.Hour),
		},
		{
			Bucket: "c", Key: "other.bin", VersionID: "null", IsLatest: true,
			Size: 4096, ContentType: "application/octet-stream", ModTime: base.Add(5 * time.Hour),
		},
	}
	for i := range docs {
		d := docs[i]
		d.Seq = uint64(i + 1)
		ix.apply(Event{Op: OpPut, Doc: d})
	}
	return ix
}

func TestSearchFilters(t *testing.T) {
	ix := queryIndex(t)

	// 默认只看最新且非删除标记：a.jpg 只应出现 v2。
	res, err := ix.search(Filter{Bucket: "b", ContentType: "image/jpeg"}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if res.Scan != "index" {
		t.Fatalf("scan = %q, want index", res.Scan)
	}
	if len(res.Docs) != 1 || res.Docs[0].Key != "photo/a.jpg" || res.Docs[0].VersionID != "v2" {
		t.Fatalf("contentType result = %+v", res.Docs)
	}

	// 前缀 + 大小范围。
	res, err = ix.search(Filter{Bucket: "b", Prefix: "photo/", MinSize: 1000, MinSizeSet: true}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if res.Scan != "bucket" {
		t.Fatalf("prefix scan = %q, want bucket", res.Scan)
	}
	if len(res.Docs) != 1 || res.Docs[0].Key != "photo/a.jpg" {
		t.Fatalf("prefix/size result = %+v", res.Docs)
	}

	// 仅按大小范围：命中 sizeBucket 二级索引，再内存精确过滤。
	res, err = ix.search(Filter{Bucket: "b", MinSize: 1000, MaxSize: 5000, MinSizeSet: true, MaxSizeSet: true}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if res.Scan != "index" || len(res.Docs) != 1 || res.Docs[0].Size != 2048 {
		t.Fatalf("size result scan=%s docs=%+v", res.Scan, res.Docs)
	}

	// user-meta 等值（白名单）命中 meta 索引。
	res, err = ix.search(Filter{Bucket: "b", Meta: map[string]string{"owner": "bob"}}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if res.Scan != "index" || len(res.Docs) != 1 || res.Docs[0].Key != "photo/b.png" {
		t.Fatalf("meta result scan=%s docs=%+v", res.Scan, res.Docs)
	}

	// tag 等值命中 tag 索引。
	res, err = ix.search(Filter{Bucket: "b", Tag: map[string]string{"env": "prod"}}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if res.Scan != "index" || len(res.Docs) != 1 || res.Docs[0].VersionID != "v2" {
		t.Fatalf("tag result scan=%s docs=%+v", res.Scan, res.Docs)
	}

	// 时间范围（after 含、before 不含）。
	res, err = ix.search(Filter{Bucket: "b", After: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC), Before: time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if res.Scan != "index" || len(res.Docs) != 2 {
		t.Fatalf("time result scan=%s docs=%+v", res.Scan, res.Docs)
	}

	// versions=true 才能看到旧版本。
	res, err = ix.search(Filter{Bucket: "b", Prefix: "photo/a.jpg", AllVersions: true}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("all versions = %d, want 2", len(res.Docs))
	}

	// 默认过滤删除标记；显式打开才返回。
	res, err = ix.search(Filter{Bucket: "b", Prefix: "gone"}, 7)
	if err != nil || len(res.Docs) != 0 {
		t.Fatalf("delete marker leaked: err=%v docs=%+v", err, res.Docs)
	}
	res, err = ix.search(Filter{Bucket: "b", Prefix: "gone", IncludeDeleteMarkers: true}, 7)
	if err != nil || len(res.Docs) != 1 || !res.Docs[0].DeleteMarker {
		t.Fatalf("delete marker missing: err=%v docs=%+v", err, res.Docs)
	}

	// 不带 bucket 时全量扫描，仍按主键有序。
	res, err = ix.search(Filter{ContentType: "application/octet-stream"}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if res.Scan != "full" || len(res.Docs) != 1 || res.Docs[0].Bucket != "c" {
		t.Fatalf("full scan = %q docs=%+v", res.Scan, res.Docs)
	}
}

func TestSearchPagination(t *testing.T) {
	ix := newMemoryIndex(IndexConfig{})
	for i := 0; i < 5; i++ {
		d := Doc{
			Bucket: "b", Key: fmt.Sprintf("k%d", i), VersionID: "null", IsLatest: true,
			Size: int64(i), ContentType: "text/plain", ModTime: time.Unix(100, 0).UTC(),
		}
		d.Seq = uint64(i + 1)
		ix.apply(Event{Op: OpPut, Doc: d})
	}

	var got []string
	token := ""
	for {
		res, err := ix.search(Filter{Bucket: "b", Limit: 2, Token: token}, 3)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range res.Docs {
			got = append(got, d.Key)
		}
		if !res.Truncated {
			break
		}
		token = res.NextToken
		if token == "" {
			t.Fatal("truncated page has empty token")
		}
	}
	want := []string{"k0", "k1", "k2", "k3", "k4"}
	if len(got) != len(want) {
		t.Fatalf("paged keys = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("paged keys = %v, want %v", got, want)
		}
	}
}

func TestSearchTokenValidation(t *testing.T) {
	ix := queryIndex(t)
	first, err := ix.search(Filter{Bucket: "b", AllVersions: true, Limit: 1}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Truncated || first.NextToken == "" {
		t.Fatalf("expected truncated first page, got %+v", first)
	}

	// gen 变了：要求重启查询。
	if _, err := ix.search(Filter{Bucket: "b", AllVersions: true, Limit: 1, Token: first.NextToken}, 8); !errors.Is(err, ErrRestartQuery) {
		t.Fatalf("gen mismatch err = %v, want ErrRestartQuery", err)
	}
	// 查询条件变了：token 不兼容。
	if _, err := ix.search(Filter{Bucket: "b", ContentType: "image/png", Limit: 1, Token: first.NextToken}, 7); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("query mismatch err = %v, want ErrInvalidToken", err)
	}
	// 坏 token。
	if _, err := ix.search(Filter{Bucket: "b", Token: "not-base64"}, 7); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("bad token err = %v, want ErrInvalidToken", err)
	}
}

func TestStats(t *testing.T) {
	ix := queryIndex(t)

	res, err := ix.stats(StatsOptions{Bucket: "b", GroupBy: "contentType"}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if res.Docs != 3 || res.TotalSize != 2048+512+100 {
		t.Fatalf("stats docs=%d size=%d", res.Docs, res.TotalSize)
	}
	if len(res.Groups) != 3 {
		t.Fatalf("groups = %+v", res.Groups)
	}

	res, err = ix.stats(StatsOptions{Bucket: "b", GroupBy: "sizeBucket"}, 7)
	if err != nil {
		t.Fatal(err)
	}
	// 2048 -> "1"; 512 -> "0"; 100 -> "0"
	if len(res.Groups) != 2 {
		t.Fatalf("size groups = %+v", res.Groups)
	}

	if _, err := ix.stats(StatsOptions{Bucket: "b", GroupBy: "unknown"}, 7); !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("unknown groupBy err = %v", err)
	}
}

func TestEngineSearchAndStatus(t *testing.T) {
	dir := t.TempDir()
	e, err := NewEngine(Config{Dir: dir, Wal: WalConfig{SyncTimeout: time.Hour}, SnapDocs: 0}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if err := e.Append(Event{Op: OpPut, Doc: Doc{
		Bucket: "b", Key: "k", VersionID: "null", IsLatest: true, Size: 1,
		ContentType: "text/plain", ModTime: time.Now().UTC(),
	}}); err != nil {
		t.Fatal(err)
	}
	res, err := e.Search(Filter{Bucket: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Docs) != 1 || res.Gen != e.Status().Gen {
		t.Fatalf("engine search = %+v", res)
	}
	if st := e.Status(); st.Docs != 1 || st.State != "ready" {
		t.Fatalf("engine status = %+v", st)
	}
}
