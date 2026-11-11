package catalog

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func testIndexCfg() IndexConfig {
	return IndexConfig{
		MetaWhitelist: map[string]struct{}{"x-amz-meta-owner": {}, "x-amz-meta-project": {}},
		TagWhitelist:  map[string]struct{}{"env": {}},
	}
}

func mkDoc(bucket, key, versionID string, seq uint64) Doc {
	return Doc{
		Bucket:      bucket,
		Key:         key,
		VersionID:   versionID,
		IsLatest:    true,
		Size:        100,
		ETag:        "etag",
		ContentType: "image/jpeg",
		ModTime:     time.Unix(1700000000, 0).UTC(),
		Seq:         seq,
	}
}

func TestApplyPutPopulatesIndexes(t *testing.T) {
	ix := newMemoryIndex(testIndexCfg())
	d := mkDoc("b", "k", "v1", 1)
	d.UserMeta = map[string]string{"x-amz-meta-owner": "alice", "x-amz-meta-nope": "x"}
	d.Tags = map[string]string{"env": "prod", "other": "y"}
	ix.apply(Event{Op: OpPut, Doc: d})

	if ix.primary.Len() != 1 || ix.ct.Len() != 1 || ix.timeIx.Len() != 1 || ix.sizeIx.Len() != 1 {
		t.Fatalf("system index lens = p%d c%d t%d s%d", ix.primary.Len(), ix.ct.Len(), ix.timeIx.Len(), ix.sizeIx.Len())
	}
	// tag/meta 只索引白名单 key
	if ix.tagIx.Len() != 1 || ix.metaIx.Len() != 1 {
		t.Fatalf("tag/meta lens = %d/%d, want 1/1", ix.tagIx.Len(), ix.metaIx.Len())
	}
	if got, ok := ix.primary.Get(primaryKey("b", "k", "v1")); !ok || got.Bucket != "b" || got.Key != "k" {
		t.Fatal("primary get failed")
	}
	if got, ok := ix.ct.Get(ctKey(&d)); !ok || got.ContentType != "image/jpeg" {
		t.Fatal("ct get failed")
	}
}

func TestApplyIdempotentReplay(t *testing.T) {
	ix := newMemoryIndex(IndexConfig{})
	d := mkDoc("b", "k", "v1", 1)
	ix.apply(Event{Op: OpPut, Doc: d})
	ix.apply(Event{Op: OpPut, Doc: d}) // 重放，应被跳过
	if ix.Len() != 1 {
		t.Fatalf("len = %d after replay, want 1", ix.Len())
	}
	// 更小的 seq 也应被跳过
	d2 := mkDoc("b", "k2", "v1", 2)
	ix.apply(Event{Op: OpPut, Doc: d2})
	ix.apply(Event{Op: OpPut, Doc: d}) // seq=1 <= 2
	if ix.Len() != 2 {
		t.Fatalf("len = %d, want 2", ix.Len())
	}
}

func TestApplyOverwriteSameVersion(t *testing.T) {
	ix := newMemoryIndex(IndexConfig{})
	d1 := mkDoc("b", "k", "null", 1) // content-type image/jpeg, size 100
	ix.apply(Event{Op: OpPut, Doc: d1})
	d2 := mkDoc("b", "k", "null", 2)
	d2.ContentType = "text/plain"
	d2.Size = 200
	ix.apply(Event{Op: OpPut, Doc: d2})

	if ix.Len() != 1 {
		t.Fatalf("primary len = %d, want 1", ix.Len())
	}
	if _, ok := ix.ct.Get(ctKey(&d1)); ok {
		t.Fatal("old content-type entry should be removed")
	}
	if got, ok := ix.ct.Get(ctKey(&d2)); !ok || got.ContentType != "text/plain" || got.Size != 200 {
		t.Fatal("new content-type entry missing")
	}
	if ix.ct.Len() != 1 || ix.sizeIx.Len() != 1 {
		t.Fatalf("ct/size lens = %d/%d, want 1/1", ix.ct.Len(), ix.sizeIx.Len())
	}
}

func TestApplyDelete(t *testing.T) {
	ix := newMemoryIndex(testIndexCfg())
	d := mkDoc("b", "k", "v1", 1)
	d.UserMeta = map[string]string{"x-amz-meta-owner": "alice"}
	d.Tags = map[string]string{"env": "prod"}
	ix.apply(Event{Op: OpPut, Doc: d})
	ix.apply(Event{Op: OpDel, Doc: mkDoc("b", "k", "v1", 2)})

	for name, m := range map[string]*omap{"primary": ix.primary, "ct": ix.ct, "time": ix.timeIx, "size": ix.sizeIx, "tag": ix.tagIx, "meta": ix.metaIx} {
		if m.Len() != 0 {
			t.Fatalf("%s index len = %d after delete, want 0", name, m.Len())
		}
	}
}

func TestClearLatestOnNewVersion(t *testing.T) {
	ix := newMemoryIndex(IndexConfig{})
	ix.apply(Event{Op: OpPut, Doc: mkDoc("b", "k", "v1", 1)})
	ix.apply(Event{Op: OpPut, Doc: mkDoc("b", "k", "v2", 2)})

	v1, _ := ix.primary.Get(primaryKey("b", "k", "v1"))
	v2, _ := ix.primary.Get(primaryKey("b", "k", "v2"))
	if v1.IsLatest {
		t.Fatal("v1 should no longer be latest after v2")
	}
	if !v2.IsLatest {
		t.Fatal("v2 should be latest")
	}
}

// TestDeleteLatestPromotesPrevious 删除最新版本后，索引应把上一个版本顶成最新。
func TestDeleteLatestPromotesPrevious(t *testing.T) {
	ix := newMemoryIndex(IndexConfig{})
	ix.apply(Event{Op: OpPut, Doc: mkDoc("b", "k", "v1", 1)})
	ix.apply(Event{Op: OpPut, Doc: mkDoc("b", "k", "v2", 2)})
	ix.apply(Event{Op: OpDel, Doc: Doc{Bucket: "b", Key: "k", VersionID: "v2", Seq: 3}})

	v1, ok := ix.primary.Get(primaryKey("b", "k", "v1"))
	if !ok {
		t.Fatal("v1 should remain")
	}
	if !v1.IsLatest {
		t.Fatal("v1 should become latest after deleting v2")
	}
	if _, ok := ix.primary.Get(primaryKey("b", "k", "v2")); ok {
		t.Fatal("v2 should be deleted")
	}
}

// TestDeleteLatestMarkerPromotesPreviousMarker 删除最新删除标记后，
// 前一个删除标记应重新成为最新（对象仍然处于已删除状态）。
func TestDeleteLatestMarkerPromotesPreviousMarker(t *testing.T) {
	ix := newMemoryIndex(IndexConfig{})
	ix.apply(Event{Op: OpPut, Doc: mkDoc("b", "k", "v1", 1)})
	m1 := mkDoc("b", "k", "dm1", 2)
	m1.DeleteMarker = true
	ix.apply(Event{Op: OpPut, Doc: m1})
	m2 := mkDoc("b", "k", "dm2", 3)
	m2.DeleteMarker = true
	ix.apply(Event{Op: OpPut, Doc: m2})
	ix.apply(Event{Op: OpDel, Doc: Doc{Bucket: "b", Key: "k", VersionID: "dm2", Seq: 4}})

	dm1, ok := ix.primary.Get(primaryKey("b", "k", "dm1"))
	if !ok {
		t.Fatal("dm1 should remain")
	}
	if !dm1.IsLatest {
		t.Fatal("dm1 should become latest")
	}
}

func TestDeleteMarkerClearsLatest(t *testing.T) {
	ix := newMemoryIndex(IndexConfig{})
	ix.apply(Event{Op: OpPut, Doc: mkDoc("b", "k", "v1", 1)})
	m := mkDoc("b", "k", "dm1", 2)
	m.DeleteMarker = true
	m.Size = 0
	ix.apply(Event{Op: OpPut, Doc: m})

	v1, _ := ix.primary.Get(primaryKey("b", "k", "v1"))
	dm, _ := ix.primary.Get(primaryKey("b", "k", "dm1"))
	if v1.IsLatest {
		t.Fatal("v1 should not be latest after delete marker")
	}
	if !dm.IsLatest || !dm.DeleteMarker {
		t.Fatal("delete marker should be latest + marker")
	}
}

func TestSecondaryIndexUpdateOnOverwrite(t *testing.T) {
	ix := newMemoryIndex(testIndexCfg())
	d1 := mkDoc("b", "k", "null", 1)
	d1.Tags = map[string]string{"env": "prod"}
	ix.apply(Event{Op: OpPut, Doc: d1})
	d2 := mkDoc("b", "k", "null", 2)
	d2.Tags = map[string]string{"env": "dev"}
	ix.apply(Event{Op: OpPut, Doc: d2})

	if _, ok := ix.tagIx.Get(tagMetaKey(&d1, "env", "prod")); ok {
		t.Fatal("old tag entry should be removed")
	}
	if _, ok := ix.tagIx.Get(tagMetaKey(&d2, "env", "dev")); !ok {
		t.Fatal("new tag entry missing")
	}
	if ix.tagIx.Len() != 1 {
		t.Fatalf("tag index len = %d, want 1", ix.tagIx.Len())
	}
}

func TestSizeBucket(t *testing.T) {
	cases := []struct {
		size int64
		want string
	}{
		{0, "0"}, {1023, "0"}, {1024, "1"}, {1<<20 - 1, "1"}, {1 << 20, "2"}, {1 << 26, "3"}, {1 << 30, "4"},
	}
	for _, c := range cases {
		if got := sizeBucket(c.size); got != c.want {
			t.Fatalf("sizeBucket(%d) = %q, want %q", c.size, got, c.want)
		}
	}
}

func TestTimeIndexOrder(t *testing.T) {
	ix := newMemoryIndex(IndexConfig{})
	nanos := []int64{300, 100, 200}
	for i, n := range nanos {
		d := mkDoc("b", fmt.Sprintf("k%d", i), "v", uint64(i+1))
		d.ModTime = time.Unix(0, n).UTC()
		ix.apply(Event{Op: OpPut, Doc: d})
	}
	prefix := encodeKeys("b")
	it := ix.timeIx.Seek(prefix)
	var got []int64
	for it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
		got = append(got, it.Doc().ModTime.UnixNano())
		it.Next()
	}
	want := []int64{100, 200, 300}
	if len(got) != len(want) {
		t.Fatalf("time order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("time order = %v, want %v", got, want)
		}
	}
}
