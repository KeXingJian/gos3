package catalog

import (
	"fmt"
	"testing"
	"time"
)

func TestWalAppendReplay(t *testing.T) {
	dir := t.TempDir()
	cfg := WalConfig{Dir: dir, SyncEvery: 1}
	w, err := NewWal(dir, 1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	const n = 100
	for i := 0; i < n; i++ {
		d := Doc{Bucket: "b", Key: fmt.Sprintf("k%03d", i), VersionID: "v", Size: int64(i), IsLatest: true}
		if err := w.Append(uint64(i+1), Event{Op: OpPut, Doc: d}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开（模拟重启），重放
	w2, err := NewWal(dir, 1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	var got []uint64
	err = w2.Replay(func(seq uint64, op Op, doc *Doc) error {
		got = append(got, seq)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("replayed %d, want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		if got[i] != uint64(i+1) {
			t.Fatalf("seq[%d]=%d, want %d", i, got[i], i+1)
		}
	}
}

func TestWalSealRotation(t *testing.T) {
	dir := t.TempDir()
	cfg := WalConfig{Dir: dir, SyncEvery: 1, SegDocs: 3}
	w, _ := NewWal(dir, 1, cfg)
	for i := 0; i < 10; i++ {
		w.Append(uint64(i+1), Event{Op: OpPut, Doc: Doc{Bucket: "b", Key: "k", VersionID: fmt.Sprintf("v%d", i)}})
	}
	w.Close()
	if len(w.Segments()) < 3 {
		t.Fatalf("segments = %d, want >= 3", len(w.Segments()))
	}
	w2, _ := NewWal(dir, 1, cfg)
	defer w2.Close()
	cnt := 0
	w2.Replay(func(seq uint64, op Op, doc *Doc) error { cnt++; return nil })
	if cnt != 10 {
		t.Fatalf("replayed %d, want 10", cnt)
	}
}

func TestWalTruncate(t *testing.T) {
	dir := t.TempDir()
	cfg := WalConfig{Dir: dir, SyncEvery: 1, SegDocs: 2}
	w, _ := NewWal(dir, 1, cfg)
	for i := 0; i < 6; i++ {
		w.Append(uint64(i+1), Event{Op: OpPut, Doc: Doc{Bucket: "b", Key: "k", VersionID: fmt.Sprintf("v%d", i)}})
	}
	w.Close()
	before := len(w.Segments())
	if err := w.Truncate(4); err != nil {
		t.Fatal(err)
	}
	if len(w.Segments()) >= before {
		t.Fatalf("truncate removed nothing: before=%d after=%d", before, len(w.Segments()))
	}
	w2, _ := NewWal(dir, 1, cfg)
	defer w2.Close()
	var got []uint64
	w2.Replay(func(seq uint64, op Op, doc *Doc) error { got = append(got, seq); return nil })
	for _, s := range got {
		if s <= 4 {
			t.Fatalf("truncate left seq %d", s)
		}
	}
}

func TestWalGroupCommitDurability(t *testing.T) {
	dir := t.TempDir()
	cfg := WalConfig{Dir: dir, SyncEvery: 5, SyncTimeout: time.Hour}
	w, _ := NewWal(dir, 1, cfg)
	for i := 0; i < 3; i++ {
		w.Append(uint64(i+1), Event{Op: OpPut, Doc: Doc{Bucket: "b", Key: "k", VersionID: "v"}})
	}
	// 此时 pending=3 < 5，尚未自动 sync；显式 Sync 后应可重放
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	w2, _ := NewWal(dir, 1, cfg)
	defer w2.Close()
	cnt := 0
	w2.Replay(func(seq uint64, op Op, doc *Doc) error { cnt++; return nil })
	if cnt != 3 {
		t.Fatalf("replayed %d, want 3", cnt)
	}
}
