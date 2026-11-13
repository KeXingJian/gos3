package catalog

import (
	"testing"

	"github.com/kxj/gos3/internal/store"
)

// TestListObjects 验证内存索引列出对象：最新版本/删除标记过滤、delimiter 分组、
// marker 分页，语义对齐 store.ListObjects。
func TestListObjects(t *testing.T) {
	ix := queryIndex(t)

	// 平铺列出（无 delimiter）：按 key 升序，跳过非最新版本与删除标记。
	res := ix.list("b", store.ListOptions{MaxKeys: 100})
	wantKeys := []string{"docs/readme.txt", "photo/a.jpg", "photo/b.png"}
	if len(res.Objects) != len(wantKeys) {
		t.Fatalf("flat objects = %+v, want %v", res.Objects, wantKeys)
	}
	for i, k := range wantKeys {
		if res.Objects[i].Name != k {
			t.Fatalf("flat objects[%d] = %q, want %q", i, res.Objects[i].Name, k)
		}
	}
	if res.IsTruncated {
		t.Fatalf("flat should not be truncated")
	}

	// delimiter "/" 分组：全部归入 common prefixes。
	res = ix.list("b", store.ListOptions{Delimiter: "/", MaxKeys: 100})
	if len(res.Objects) != 0 {
		t.Fatalf("delimited objects = %+v, want none", res.Objects)
	}
	wantPrefixes := []string{"docs/", "photo/"}
	if len(res.CommonPrefixes) != len(wantPrefixes) {
		t.Fatalf("common prefixes = %+v, want %v", res.CommonPrefixes, wantPrefixes)
	}
	for i, p := range wantPrefixes {
		if res.CommonPrefixes[i] != p {
			t.Fatalf("common prefixes[%d] = %q, want %q", i, res.CommonPrefixes[i], p)
		}
	}

	// prefix 限定到某个目录、无 delimiter：列出该目录下的对象。
	res = ix.list("b", store.ListOptions{Prefix: "photo/", MaxKeys: 100})
	if len(res.Objects) != 2 || res.Objects[0].Name != "photo/a.jpg" || res.Objects[1].Name != "photo/b.png" {
		t.Fatalf("prefix objects = %+v", res.Objects)
	}

	// 其他 bucket 不受影响。
	res = ix.list("c", store.ListOptions{MaxKeys: 100})
	if len(res.Objects) != 1 || res.Objects[0].Name != "other.bin" {
		t.Fatalf("bucket c objects = %+v", res.Objects)
	}
}

func TestListObjectsPagination(t *testing.T) {
	ix := queryIndex(t)

	// 每页 2 条，逐页翻。
	var got []string
	marker := ""
	for {
		res := ix.list("b", store.ListOptions{MaxKeys: 2, Marker: marker})
		for _, o := range res.Objects {
			got = append(got, o.Name)
		}
		if !res.IsTruncated {
			break
		}
		marker = res.NextMarker
		if marker == "" {
			t.Fatal("truncated page has empty next marker")
		}
	}
	want := []string{"docs/readme.txt", "photo/a.jpg", "photo/b.png"}
	if len(got) != len(want) {
		t.Fatalf("paged objects = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("paged objects[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
