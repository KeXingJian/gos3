package catalog

import (
	"bytes"
	"os"
	"testing"
)

func buildIndexForSnapshot() *memoryIndex {
	ix := newMemoryIndex(testIndexCfg())
	for _, kv := range []struct {
		key string
		v   string
		seq uint64
	}{
		{"a", "v1", 1}, {"a", "v2", 2}, {"b", "v1", 3},
	} {
		d := mkDoc("bkt", kv.key, kv.v, kv.seq)
		d.UserMeta = map[string]string{"x-amz-meta-owner": "alice"}
		d.Tags = map[string]string{"env": "prod"}
		ix.apply(Event{Op: OpPut, Doc: d})
	}
	return ix
}

func TestSnapshotRoundtrip(t *testing.T) {
	ix := buildIndexForSnapshot()
	dir := t.TempDir()
	docs, maxSeq, err := writeSnapshotAtomic(dir, 42, ix)
	if err != nil {
		t.Fatal(err)
	}
	if docs != ix.Len() {
		t.Fatalf("snapshot docs=%d, want %d", docs, ix.Len())
	}
	if maxSeq != 3 {
		t.Fatalf("maxSeq=%d, want 3", maxSeq)
	}

	got, gotMaxSeq, err := readSnapshot(snapshotPath(dir, 42), testIndexCfg())
	if err != nil {
		t.Fatal(err)
	}
	if gotMaxSeq != maxSeq || got.Len() != ix.Len() {
		t.Fatalf("got maxSeq=%d len=%d, want %d/%d", gotMaxSeq, got.Len(), maxSeq, ix.Len())
	}

	a, b := ix.primary.Seek(nil), got.primary.Seek(nil)
	for a.Valid() || b.Valid() {
		if !a.Valid() || !b.Valid() {
			t.Fatal("index length mismatch")
		}
		if !bytes.Equal(a.Key(), b.Key()) {
			t.Fatalf("key mismatch: %q vs %q", a.Key(), b.Key())
		}
		if !equalDoc(a.Doc(), b.Doc()) {
			t.Fatalf("doc mismatch at %q", a.Key())
		}
		a.Next()
		b.Next()
	}

	// IsLatest 应保留：v1 被 v2 顶掉 latest
	v1, _ := got.primary.Get(primaryKey("bkt", "a", "v1"))
	v2, _ := got.primary.Get(primaryKey("bkt", "a", "v2"))
	if v1.IsLatest || !v2.IsLatest {
		t.Fatalf("IsLatest wrong: v1=%v v2=%v", v1.IsLatest, v2.IsLatest)
	}
}

func TestSnapshotAtomicNoTmp(t *testing.T) {
	ix := buildIndexForSnapshot()
	dir := t.TempDir()
	if _, _, err := writeSnapshotAtomic(dir, 1, ix); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(snapshotPath(dir, 1) + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("tmp file should not remain after atomic write")
	}
	if _, err := os.Stat(snapshotPath(dir, 1)); err != nil {
		t.Fatal("snapshot file should exist")
	}
}

func TestSnapshotCorrupt(t *testing.T) {
	ix := buildIndexForSnapshot()
	dir := t.TempDir()
	writeSnapshotAtomic(dir, 1, ix)
	path := snapshotPath(dir, 1)
	data, _ := os.ReadFile(path)
	data[0] ^= 0xFF
	os.WriteFile(path, data, 0o644)
	if _, _, err := readSnapshot(path, testIndexCfg()); err == nil {
		t.Fatal("expected error on corrupt snapshot")
	}
}
