package catalog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func rebuildFrom(docs []Doc) RebuildFunc {
	return func(emit func(Event) error) error {
		for _, d := range docs {
			if err := emit(Event{Op: OpPut, Doc: d}); err != nil {
				return err
			}
		}
		return nil
	}
}

func engineCfg(dir string) Config {
	return Config{Dir: dir, Gen: 0, Wal: WalConfig{Dir: dir, SyncEvery: 1}}
}

func TestEngineCleanStart(t *testing.T) {
	dir := t.TempDir()
	e, err := NewEngine(engineCfg(dir), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	st := e.Status()
	if st.Docs != 0 || st.AppliedSeq != 0 || st.Gen != 0 {
		t.Fatalf("clean start status = %+v", st)
	}
}

func TestEngineAppendReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := engineCfg(dir)
	e, _ := NewEngine(cfg, nil)
	for i := 0; i < 10; i++ {
		d := mkDoc("b", fmt.Sprintf("k%02d", i), "v", 0)
		if err := e.Append(Event{Op: OpPut, Doc: d}); err != nil {
			t.Fatal(err)
		}
	}
	e.Close()

	e2, err := NewEngine(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if st := e2.Status(); st.Docs != 10 || st.AppliedSeq != 10 {
		t.Fatalf("after reopen status = %+v, want docs=10 seq=10", st)
	}
	for i := 0; i < 10; i++ {
		if _, ok := e2.View().primary.Get(primaryKey("b", fmt.Sprintf("k%02d", i), "v")); !ok {
			t.Fatalf("doc k%02d missing after reopen", i)
		}
	}
}

func TestEngineSnapshotRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := engineCfg(dir)
	e, _ := NewEngine(cfg, nil)
	for i := 0; i < 20; i++ {
		e.Append(Event{Op: OpPut, Doc: mkDoc("b", fmt.Sprintf("k%02d", i), "v", 0)})
	}
	if err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}
	for i := 20; i < 25; i++ {
		e.Append(Event{Op: OpPut, Doc: mkDoc("b", fmt.Sprintf("k%02d", i), "v", 0)})
	}
	e.Close()

	e2, err := NewEngine(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if st := e2.Status(); st.Docs != 25 || st.AppliedSeq != 25 {
		t.Fatalf("after snapshot+reopen status = %+v, want docs=25 seq=25", st)
	}
}

func TestEngineGapTriggersRebuild(t *testing.T) {
	dir := t.TempDir()
	// 先写 manifest（gen 0，无快照），再手工造一个带 gap 的 WAL：seq 1、3（缺 2）
	if err := writeManifest(dir, &manifest{Gen: 0, AppliedSeq: 0}); err != nil {
		t.Fatal(err)
	}
	w, _ := NewWal(dir, 0, WalConfig{Dir: dir, SyncEvery: 1})
	w.Append(1, Event{Op: OpPut, Doc: mkDoc("b", "a", "v", 1)})
	w.Append(3, Event{Op: OpPut, Doc: mkDoc("b", "c", "v", 3)})
	w.Close()

	rebuild := rebuildFrom([]Doc{mkDoc("rb", "x", "v1", 0), mkDoc("rb", "y", "v1", 0)})
	e, err := NewEngine(engineCfg(dir), rebuild)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	st := e.Status()
	if st.Gen != 1 {
		t.Fatalf("gen = %d, want 1 (rebuilt)", st.Gen)
	}
	if st.Docs != 2 {
		t.Fatalf("docs = %d, want 2 (from rebuild source)", st.Docs)
	}
	if _, ok := e.View().primary.Get(primaryKey("rb", "x", "v1")); !ok {
		t.Fatal("rebuild doc missing")
	}
}

func TestEngineCRCCorruptTriggersRebuild(t *testing.T) {
	dir := t.TempDir()
	cfg := engineCfg(dir)
	e, _ := NewEngine(cfg, nil)
	for i := 0; i < 5; i++ {
		e.Append(Event{Op: OpPut, Doc: mkDoc("b", fmt.Sprintf("k%d", i), "v", 0)})
	}
	// 快照一次，写出 manifest，让恢复走「有 manifest」的路径
	if err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}
	for i := 5; i < 8; i++ {
		e.Append(Event{Op: OpPut, Doc: mkDoc("b", fmt.Sprintf("k%d", i), "v", 0)})
	}
	e.Close()

	nums, _ := listSegmentNums(dir, 0)
	p := filepath.Join(dir, fmt.Sprintf("wal-0-%06d.wal", nums[len(nums)-1]))
	data, _ := os.ReadFile(p)
	data[len(data)/2] ^= 0xFF
	os.WriteFile(p, data, 0o644)

	rebuild := rebuildFrom([]Doc{mkDoc("rb", "x", "v1", 0)})
	e2, err := NewEngine(cfg, rebuild)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if e2.Status().Gen != 1 || e2.Status().Docs != 1 {
		t.Fatalf("expected rebuild, got %+v", e2.Status())
	}
}

func TestEngineMissingRebuildSource(t *testing.T) {
	dir := t.TempDir()
	cfg := engineCfg(dir)
	e, _ := NewEngine(cfg, nil)
	e.Append(Event{Op: OpPut, Doc: mkDoc("b", "k", "v", 0)})
	e.Close()
	// 破坏 manifest 使恢复需要重建，但 rebuild 为 nil
	os.WriteFile(manifestPath(dir), []byte("{bad json"), 0o644)
	if _, err := NewEngine(cfg, nil); err == nil {
		t.Fatal("expected error when rebuild source is nil")
	}
}
