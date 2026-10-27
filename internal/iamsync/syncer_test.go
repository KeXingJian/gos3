package iamsync

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/kxj/gos3/internal/auth"
	"github.com/kxj/gos3/internal/iam"
	"github.com/kxj/gos3/internal/peer"
	"google.golang.org/grpc"
)

func newTestStore(t *testing.T, name string) *iam.Store {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := iam.New(t.TempDir(), auth.Credentials{AccessKey: "root", SecretKey: "root-pass"}, log)
	if err != nil {
		t.Fatalf("iam.New(%s): %v", name, err)
	}
	return store
}

// startPeer 启动一个真实的 PeerService（随机端口），并注入 IAM 存储。
func startPeer(t *testing.T, name string, store *iam.Store) (string, func()) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	srv := peer.NewServer(name+":9001", nil, log)
	srv.SetIAM(store)
	srv.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	return lis.Addr().String(), gs.Stop
}

// TestSyncerReplicatesIAM 覆盖三个关键场景：
//  1. 管理写（用户/策略/绑定）后推送，对端立即可用；
//  2. 对端新增用户后，先推后拉的双向对账能把状态追平；
//  3. 删除用户后再次推送，对端同步删除。
func TestSyncerReplicatesIAM(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	storeA := newTestStore(t, "a")
	storeB := newTestStore(t, "b")

	addr, stop := startPeer(t, "node-b", storeB)
	defer stop()

	mgr := peer.NewManager(log)
	defer mgr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := mgr.Dial(ctx, addr, 3*time.Second); err != nil {
		t.Fatalf("Dial: %v", err)
	}

	syncer := New(storeA, mgr, log)

	// A 上创建用户、策略并绑定，随后推送。
	if err := storeA.AddUser(iam.User{AccessKey: "alice", SecretKey: "alice123"}); err != nil {
		t.Fatalf("AddUser: %v", err)
	}
	var policy iam.Policy
	if err := json.Unmarshal([]byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::demo/*"}]}`), &policy); err != nil {
		t.Fatalf("policy json: %v", err)
	}
	if err := storeA.SetPolicy("readonly", policy); err != nil {
		t.Fatalf("SetPolicy: %v", err)
	}
	if err := storeA.AttachPolicy("alice", "readonly"); err != nil {
		t.Fatalf("AttachPolicy: %v", err)
	}
	syncer.Replicate(ctx)

	if _, ok := storeB.Get("alice"); !ok {
		t.Fatal("alice should be replicated to node B")
	}
	if !storeB.IsAllowed("alice", "s3:GetObject", "arn:aws:s3:::demo/x") {
		t.Fatal("policy binding should be replicated to node B")
	}

	// B 上新增用户，双向对账后 A 也应该看到。
	if err := storeB.AddUser(iam.User{AccessKey: "bob", SecretKey: "bob123"}); err != nil {
		t.Fatalf("AddUser(bob): %v", err)
	}
	syncer.Sync(ctx)
	if _, ok := storeA.Get("bob"); !ok {
		t.Fatal("bob should be pulled back to node A")
	}

	// A 删除 alice，推送后 B 也必须删除。
	if err := storeA.RemoveUser("alice"); err != nil {
		t.Fatalf("RemoveUser: %v", err)
	}
	syncer.Replicate(ctx)
	if _, ok := storeB.Get("alice"); ok {
		t.Fatal("alice should be removed on node B")
	}
	if _, ok := storeB.Get("bob"); !ok {
		t.Fatal("bob should survive the delete replication")
	}
}
