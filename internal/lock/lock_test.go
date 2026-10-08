package lock

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestQuorumFormula 校验法定人数公式：写锁始终多数派，读锁在偶数节点时更宽松。
func TestQuorumFormula(t *testing.T) {
	cases := []struct {
		n           int
		readQuorum  int
		writeQuorum int
	}{
		{1, 1, 1},
		{2, 1, 2},
		{3, 2, 2},
		{4, 2, 3},
		{5, 3, 3},
	}
	for _, c := range cases {
		if got := quorumFor(c.n, true); got != c.readQuorum {
			t.Fatalf("read quorum for %d nodes = %d, want %d", c.n, got, c.readQuorum)
		}
		if got := quorumFor(c.n, false); got != c.writeQuorum {
			t.Fatalf("write quorum for %d nodes = %d, want %d", c.n, got, c.writeQuorum)
		}
	}
}

// TestTableWriteLockIsExclusive 校验同一资源上的写锁互斥、释放后可再次获取。
func TestTableWriteLockIsExclusive(t *testing.T) {
	tb := newLockTable("node-a", time.Minute, testLogger())
	if !tb.lock("bkt/obj", "uid-1") {
		t.Fatal("first writer should get the lock")
	}
	if tb.lock("bkt/obj", "uid-2") {
		t.Fatal("second writer must not get the lock while held")
	}
	if !tb.lock("bkt/obj", "uid-1") {
		t.Fatal("same uid re-lock should be treated as refresh")
	}
	if tb.unlock("bkt/obj", "uid-2", false) {
		t.Fatal("unlock by non-owner should report failure")
	}
	if !tb.unlock("bkt/obj", "uid-1", false) {
		t.Fatal("owner unlock should succeed")
	}
	if !tb.lock("bkt/obj", "uid-2") {
		t.Fatal("lock should be grantable after release")
	}
}

// TestTableReadLockSharedAndBlocksWriter 校验读锁之间共享、但与写锁互斥。
func TestTableReadLockSharedAndBlocksWriter(t *testing.T) {
	tb := newLockTable("node-a", time.Minute, testLogger())
	if !tb.rlock("bkt/obj", "r1") || !tb.rlock("bkt/obj", "r2") {
		t.Fatal("multiple readers should share the lock")
	}
	if tb.lock("bkt/obj", "w1") {
		t.Fatal("writer must not get the lock while readers hold it")
	}
	tb.runlock("bkt/obj", "r1", false)
	if tb.lock("bkt/obj", "w1") {
		t.Fatal("writer must not get the lock while one reader remains")
	}
	tb.runlock("bkt/obj", "r2", false)
	if !tb.lock("bkt/obj", "w1") {
		t.Fatal("writer should get the lock after all readers released")
	}
	if tb.rlock("bkt/obj", "r3") {
		t.Fatal("reader must not get the lock while a writer holds it")
	}
}

// TestTableLockExpiry 校验持锁者不再续期后锁会自动过期（对应「持锁节点崩溃」场景）。
func TestTableLockExpiry(t *testing.T) {
	tb := newLockTable("node-a", 50*time.Millisecond, testLogger())
	if !tb.lock("bkt/obj", "uid-1") {
		t.Fatal("lock should be granted")
	}
	if !tb.refresh("bkt/obj", "uid-1") {
		t.Fatal("holder should be able to refresh")
	}
	time.Sleep(80 * time.Millisecond)
	if !tb.lock("bkt/obj", "uid-2") {
		t.Fatal("lock should expire and be grantable by another uid")
	}
}

// TestRefreshOnlyByHolder 校验非持有者不能续期。
func TestRefreshOnlyByHolder(t *testing.T) {
	tb := newLockTable("node-a", time.Minute, testLogger())
	tb.lock("bkt/obj", "uid-1")
	if tb.refresh("bkt/obj", "uid-2") {
		t.Fatal("non-holder must not refresh")
	}
	if !tb.refresh("bkt/obj", "uid-1") {
		t.Fatal("holder should refresh")
	}
}

// TestDRWMutexSingleNode 校验单节点锁（单机模式）：加锁、互斥、释放。
// 两个 DRWMutex 共享同一个节点锁表，模拟同一进程内的并发写者。
func TestDRWMutexSingleNode(t *testing.T) {
	ctx := context.Background()
	local := NewLocalNode("node-a", NewServer("node-a", testLogger()))
	m := NewDRWMutex("node-a", []NodeLocker{local}, testLogger())
	other := NewDRWMutex("node-a", []NodeLocker{local}, testLogger())
	other.SetWaitTimeout(0) // 只尝试一次，失败立即返回

	release, err := m.LockWrite(ctx, "bkt/obj")
	if err != nil {
		t.Fatalf("LockWrite: %v", err)
	}
	if _, err := other.LockWrite(ctx, "bkt/obj"); err == nil {
		t.Fatal("second writer should fail while the lock is held")
	}
	if _, err := other.LockRead(ctx, "bkt/obj"); err == nil {
		t.Fatal("reader should fail while writer holds the lock")
	}
	release()
	if _, err := other.LockWrite(ctx, "bkt/obj"); err != nil {
		t.Fatalf("lock should be available after release: %v", err)
	}
}

// startLockServer 启动一个真实的 LockService（随机端口），返回连接与停止函数。
func startLockServer(t *testing.T) (*grpc.ClientConn, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	NewServer("node-b", testLogger()).Register(gs)
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return conn, func() {
		_ = conn.Close()
		gs.Stop()
	}
}

// TestDRWMutexAcrossNodes 校验跨节点锁：并发写者互斥，对端离线后按剩余节点算 quorum。
func TestDRWMutexAcrossNodes(t *testing.T) {
	ctx := context.Background()
	conn, stop := startLockServer(t)
	defer stop()

	var mu sync.Mutex
	online := true
	isOnline := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return online
	}
	// 同一个本节点锁表 + 同一个远程节点连接：模拟同一进程内的两个写者
	local := NewLocalNode("node-a", NewServer("node-a", testLogger()))
	mk := func() *DRWMutex {
		m := NewDRWMutex("node-a", []NodeLocker{local, NewRemoteNode("node-b", conn, isOnline)}, testLogger())
		m.SetWaitTimeout(0)
		return m
	}

	m1, m2 := mk(), mk()
	// 两节点写锁 quorum = 2：两个节点都要授予
	release, err := m1.LockWrite(ctx, "bkt/obj")
	if err != nil {
		t.Fatalf("LockWrite: %v", err)
	}
	if _, err := m2.LockWrite(ctx, "bkt/obj"); err == nil {
		t.Fatal("concurrent writer should fail while the lock is held")
	}
	release()

	// 对端离线：quorum 退化为 1（只有本节点），本节点仍能继续写
	mu.Lock()
	online = false
	mu.Unlock()
	release2, err := m1.LockWrite(ctx, "bkt/obj")
	if err != nil {
		t.Fatalf("LockWrite with peer offline: %v", err)
	}
	release2()

	// 对端恢复在线后，写锁重新要求两个节点：另一个写者抢占失败
	mu.Lock()
	online = true
	mu.Unlock()
	release3, err := m1.LockWrite(ctx, "bkt/obj")
	if err != nil {
		t.Fatalf("LockWrite after peer online: %v", err)
	}
	defer release3()
	if _, err := m2.LockWrite(ctx, "bkt/obj"); err == nil {
		t.Fatal("second writer should fail after the peer is back online")
	}
}

// TestDRWMutexQuorumUnreachable 校验 quorum 不足时拿不到写锁，并回滚已获得的授权。
func TestDRWMutexQuorumUnreachable(t *testing.T) {
	ctx := context.Background()
	conn, stop := startLockServer(t)
	defer stop()

	holder := NewDRWMutex("node-a", []NodeLocker{
		NewLocalNode("node-a", NewServer("node-a", testLogger())),
		NewRemoteNode("node-b", conn, nil),
	}, testLogger())
	release, err := holder.LockWrite(ctx, "bkt/obj")
	if err != nil {
		t.Fatalf("first LockWrite: %v", err)
	}
	defer release()

	// 另一个节点抢同一资源：对端拒绝 + 本地没被占用 → 只拿到 1 票 < quorum 2
	other := NewDRWMutex("node-c", []NodeLocker{
		NewLocalNode("node-c", NewServer("node-c", testLogger())),
		NewRemoteNode("node-b", conn, nil),
	}, testLogger())
	other.SetWaitTimeout(0)
	if _, err := other.LockWrite(ctx, "bkt/obj"); err == nil {
		t.Fatal("second node should fail to acquire the lock")
	}

	// 失败方不应留下「半把锁」：本地锁表必须是干净的
	if other.lockers[0].(*localNode).table.lock("bkt/obj", "probe") != true {
		t.Fatal("failed acquisition must roll back the granted lock")
	}
}

// TestLockExpiryOverGRPC 校验经 gRPC 持有锁后不再续期，锁会在过期时间后被其它节点接管
// （对应「持锁节点崩溃，另一节点在 1 分钟内继续写」的验收）。
func TestLockExpiryOverGRPC(t *testing.T) {
	ctx := context.Background()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	// 用较短的过期时间来验证机制（生产默认 1 分钟）
	srv := &Server{table: newLockTable("node-b", 80*time.Millisecond, testLogger())}
	gs := grpc.NewServer()
	srv.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer func() { _ = conn.Close() }()

	first := NewDRWMutex("node-a", []NodeLocker{NewRemoteNode("node-b", conn, nil)}, testLogger())
	release, err := first.LockWrite(ctx, "bkt/obj")
	if err != nil {
		t.Fatalf("first LockWrite: %v", err)
	}
	_ = release // 模拟持锁者崩溃：不释放、不续期

	second := NewDRWMutex("node-c", []NodeLocker{NewRemoteNode("node-b", conn, nil)}, testLogger())
	second.SetWaitTimeout(0)
	if _, err := second.LockWrite(ctx, "bkt/obj"); err == nil {
		t.Fatal("lock should not be grantable before expiry")
	}
	time.Sleep(120 * time.Millisecond)
	release2, err := second.LockWrite(ctx, "bkt/obj")
	if err != nil {
		t.Fatalf("lock should be grantable after expiry: %v", err)
	}
	release2()
}

// TestDRWMutexReadLocksShare 校验读锁可并发持有，写锁被读锁阻塞。
func TestDRWMutexReadLocksShare(t *testing.T) {
	ctx := context.Background()
	local := NewLocalNode("node-a", NewServer("node-a", testLogger()))
	m1 := NewDRWMutex("node-a", []NodeLocker{local}, testLogger())
	m2 := NewDRWMutex("node-a", []NodeLocker{local}, testLogger())

	r1, err := m1.LockRead(ctx, "bkt/obj")
	if err != nil {
		t.Fatalf("LockRead: %v", err)
	}
	r2, err := m2.LockRead(ctx, "bkt/obj")
	if err != nil {
		t.Fatalf("second LockRead should share the lock: %v", err)
	}
	m1.SetWaitTimeout(0)
	if _, err := m1.LockWrite(ctx, "bkt/obj"); err == nil {
		t.Fatal("writer should fail while readers hold the lock")
	}
	r1()
	r2()
	if _, err := m1.LockWrite(ctx, "bkt/obj"); err != nil {
		t.Fatalf("writer should succeed after readers released: %v", err)
	}
}
