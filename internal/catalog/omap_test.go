package catalog

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestOmapGetEmpty(t *testing.T) {
	m := newOmap()
	if _, ok := m.Get([]byte("k")); ok {
		t.Fatal("expected miss on empty omap")
	}
}

func TestOmapUpsertGetRoundtrip(t *testing.T) {
	m := newOmap()
	d := &Doc{Bucket: "b", Key: "k", VersionID: "v1", Size: 10}
	old, existed := m.Upsert(primaryKey(d.Bucket, d.Key, d.VersionID), d)
	if existed || old != nil {
		t.Fatalf("first upsert should not report old, got existed=%v old=%v", existed, old)
	}
	got, ok := m.Get(primaryKey("b", "k", "v1"))
	if !ok || got != d {
		t.Fatalf("get after upsert failed: ok=%v got=%v", ok, got)
	}
	if m.Len() != 1 {
		t.Fatalf("len = %d, want 1", m.Len())
	}
}

func TestOmapUpsertReturnsOld(t *testing.T) {
	m := newOmap()
	k := primaryKey("b", "k", "v1")
	old1 := &Doc{Size: 1}
	m.Upsert(k, old1)
	old2 := &Doc{Size: 2}
	got, existed := m.Upsert(k, old2)
	if !existed || got != old1 {
		t.Fatalf("expected old1 back, got existed=%v old=%v", existed, got)
	}
	cur, ok := m.Get(k)
	if !ok || cur != old2 {
		t.Fatalf("expected updated doc old2, got %v", cur)
	}
	if m.Len() != 1 {
		t.Fatalf("len = %d, want 1 (upsert should not grow)", m.Len())
	}
}

func TestOmapDelete(t *testing.T) {
	m := newOmap()
	k := primaryKey("b", "k", "v1")
	d := &Doc{Size: 1}
	m.Upsert(k, d)
	got, ok := m.Delete(k)
	if !ok || got != d {
		t.Fatalf("delete failed: ok=%v got=%v", ok, got)
	}
	if _, ok := m.Get(k); ok {
		t.Fatal("expected miss after delete")
	}
	if m.Len() != 0 {
		t.Fatalf("len = %d, want 0", m.Len())
	}
	if _, ok := m.Delete(k); ok {
		t.Fatal("second delete should be a miss")
	}
}

func TestOmapOrderedIteration(t *testing.T) {
	m := newOmap()
	for i := 0; i < 100; i++ {
		m.Upsert(primaryKey("b", randomKey(8), "v"), &Doc{Size: 1})
	}
	var prev []byte
	it := m.Seek(nil)
	cnt := 0
	for it.Valid() {
		if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("keys out of order: %q >= %q", prev, it.Key())
		}
		prev = append(prev[:0], it.Key()...)
		it.Next()
		cnt++
	}
	if cnt != 100 {
		t.Fatalf("iterated %d keys, want 100", cnt)
	}
}

func TestOmapSeekPrefix(t *testing.T) {
	m := newOmap()
	// 三个共享前缀 bucket/keyA 的版本 + 一个其它 key
	for _, v := range []string{"v3", "v1", "v2"} {
		m.Upsert(primaryKey("b", "obj/a", v), &Doc{VersionID: v})
	}
	m.Upsert(primaryKey("b", "obj/b", "v1"), &Doc{VersionID: "v1"})
	m.Upsert(primaryKey("c", "other", "v1"), &Doc{})

	prefix := encodeKeys("b", "obj/a")
	it := m.Seek(prefix)
	var got []string
	for it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
		got = append(got, it.Doc().VersionID)
		it.Next()
	}
	if len(got) != 3 {
		t.Fatalf("prefix scan returned %d docs, want 3", len(got))
	}
	want := []string{"v1", "v2", "v3"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("prefix scan order = %v, want %v", got, want)
		}
	}
}

func TestOmapDeleteMany(t *testing.T) {
	m := newOmap()
	const n = 1000
	for i := 0; i < n; i++ {
		m.Upsert(primaryKey("b", randomKey(8), "v"), &Doc{Size: int64(i)})
	}
	if m.Len() != n {
		t.Fatalf("len = %d, want %d", m.Len(), n)
	}
	keys := make([][]byte, 0, n)
	it := m.Seek(nil)
	for it.Valid() {
		keys = append(keys, it.Key())
		it.Next()
	}
	for _, k := range keys {
		if _, ok := m.Delete(k); !ok {
			t.Fatalf("delete %q failed", k)
		}
	}
	if m.Len() != 0 {
		t.Fatalf("len = %d after delete all, want 0", m.Len())
	}
	if it := m.Seek(nil); it.Valid() {
		t.Fatal("expected empty iteration after delete all")
	}
}

// randomKey 生成随机 key 字符串（字母数字），避免与其它 case 冲突。
func randomKey(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
