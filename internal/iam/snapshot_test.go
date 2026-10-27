package iam

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/kxj/gos3/internal/auth"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(t.TempDir(), auth.Credentials{AccessKey: "root", SecretKey: "root-pass"}, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func mustAddUser(t *testing.T, s *Store, accessKey, secretKey string) {
	t.Helper()
	if err := s.AddUser(User{AccessKey: accessKey, SecretKey: secretKey}); err != nil {
		t.Fatalf("AddUser(%s): %v", accessKey, err)
	}
}

func mustApply(t *testing.T, s *Store, snap Snapshot) (bool, uint64) {
	t.Helper()
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	changed, rev, err := s.ApplySnapshotJSON(data)
	if err != nil {
		t.Fatalf("ApplySnapshotJSON: %v", err)
	}
	return changed, rev
}

// TestSnapshotApplyNewerReplaces 校验「修订号更大 -> 整体覆盖」，删除也会随之传播。
func TestSnapshotApplyNewerReplaces(t *testing.T) {
	src := newTestStore(t)
	dst := newTestStore(t)

	mustAddUser(t, src, "alice", "alice123")
	if got := src.Revision(); got != 1 {
		t.Fatalf("revision after one change = %d, want 1", got)
	}

	snap := src.Snapshot()
	changed, rev := mustApply(t, dst, snap)
	if !changed || rev != 1 {
		t.Fatalf("apply snapshot: changed=%v rev=%d, want true/1", changed, rev)
	}
	if _, ok := dst.Get("alice"); !ok {
		t.Fatal("alice should exist on destination after apply")
	}

	// 删除用户后修订号 +1，覆盖式同步应把删除带到对端。
	if err := src.RemoveUser("alice"); err != nil {
		t.Fatalf("RemoveUser: %v", err)
	}
	changed, _ = mustApply(t, dst, src.Snapshot())
	if !changed {
		t.Fatal("delete should change destination state")
	}
	if _, ok := dst.Get("alice"); ok {
		t.Fatal("alice should be removed on destination")
	}
}

// TestSnapshotApplyEqualRevisionMerges 校验修订号相同时取并集，并发新增都不丢。
func TestSnapshotApplyEqualRevisionMerges(t *testing.T) {
	s := newTestStore(t)

	alice := Snapshot{Revision: 1, Users: []User{{AccessKey: "alice", SecretKey: "a1", Status: StatusEnabled}}}
	bob := Snapshot{Revision: 1, Users: []User{{AccessKey: "bob", SecretKey: "b1", Status: StatusEnabled}}}

	mustApply(t, s, alice)
	changed, rev := mustApply(t, s, bob)
	if !changed || rev != 2 {
		t.Fatalf("merge: changed=%v rev=%d, want true/2", changed, rev)
	}
	if _, ok := s.Get("alice"); !ok {
		t.Fatal("alice should survive merge")
	}
	if _, ok := s.Get("bob"); !ok {
		t.Fatal("bob should be merged in")
	}
}

// TestSnapshotApplyOlderIgnored 校验旧修订号不会回滚新状态。
func TestSnapshotApplyOlderIgnored(t *testing.T) {
	s := newTestStore(t)

	mustAddUser(t, s, "alice", "alice123") // rev 1
	mustAddUser(t, s, "bob", "bob123")     // rev 2

	stale := Snapshot{Revision: 1, Users: []User{{AccessKey: "carol", SecretKey: "c1", Status: StatusEnabled}}}
	changed, rev := mustApply(t, s, stale)
	if changed || rev != 2 {
		t.Fatalf("stale snapshot: changed=%v rev=%d, want false/2", changed, rev)
	}
	if _, ok := s.Get("carol"); ok {
		t.Fatal("stale snapshot must not add carol")
	}
}

// TestSnapshotConflictConverges 校验同名冲突的裁决是确定性的：两端合并后一致。
func TestSnapshotConflictConverges(t *testing.T) {
	a := newTestStore(t)
	b := newTestStore(t)

	snapA := Snapshot{Revision: 1, Users: []User{{AccessKey: "alice", SecretKey: "aaa", Status: StatusEnabled}}}
	snapB := Snapshot{Revision: 1, Users: []User{{AccessKey: "alice", SecretKey: "zzz", Status: StatusEnabled}}}

	mustApply(t, a, snapA)
	mustApply(t, b, snapB)
	mustApply(t, a, snapB)
	mustApply(t, b, snapA)

	ca, _ := a.Get("alice")
	cb, _ := b.Get("alice")
	if ca.SecretKey != cb.SecretKey {
		t.Fatalf("conflict did not converge: a=%q b=%q", ca.SecretKey, cb.SecretKey)
	}
}

// TestRevisionPersists 校验修订号落盘，重启后不会退化。
func TestRevisionPersists(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	root := auth.Credentials{AccessKey: "root", SecretKey: "root-pass"}

	s, err := New(dir, root, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	mustAddUser(t, s, "alice", "alice123")

	reopened, err := New(dir, root, log)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := reopened.Revision(); got != 1 {
		t.Fatalf("revision after reload = %d, want 1", got)
	}
	if _, ok := reopened.Get("alice"); !ok {
		t.Fatal("alice should be loaded from disk")
	}
}
