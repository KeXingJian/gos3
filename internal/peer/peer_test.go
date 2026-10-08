package peer

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startPeer 启动一个真实的 PeerService 服务端（随机端口），返回监听地址与停止函数。
func startPeer(t *testing.T, advertise string, drives []string, deploymentID, layoutHash string) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	srv := NewServer(advertise, drives, testLogger())
	if deploymentID != "" {
		srv.SetLayout(deploymentID, layoutHash)
	}
	srv.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	return lis.Addr().String(), gs.Stop
}

// TestManagerDialAndInfo 校验拨号成功后能拿到成员信息与布局。
func TestManagerDialAndInfo(t *testing.T) {
	addr, stop := startPeer(t, "node-a:9001", []string{"node-a:9001/0", "node-a:9001/1"}, "dep-1", "layout-1")
	defer stop()

	mgr := NewManager(testLogger())
	defer mgr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := mgr.Dial(ctx, addr, 3*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if st.Advertise != "node-a:9001" || st.Drives != 2 || !st.Online {
		t.Fatalf("unexpected state: %+v", st)
	}
	if !mgr.Online(addr) {
		t.Fatal("peer should be online after Dial")
	}

	conn, ok := mgr.Conn(addr)
	if !ok {
		t.Fatal("Conn missing after Dial")
	}
	info, err := NewPeerServiceClient(conn).Info(ctx, &InfoRequest{})
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Address != "node-a:9001" || info.DeploymentId != "dep-1" || info.LayoutHash != "layout-1" {
		t.Fatalf("unexpected info: %+v", info)
	}
	if len(info.Drives) != 2 {
		t.Fatalf("drives = %v, want 2 entries", info.Drives)
	}
}

// TestManagerProbeDetectsOffline 校验对端停机后探测状态翻转为离线。
func TestManagerProbeDetectsOffline(t *testing.T) {
	addr, stop := startPeer(t, "node-b:9001", []string{"node-b:9001/0"}, "dep-2", "layout-2")

	mgr := NewManager(testLogger())
	defer mgr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := mgr.Dial(ctx, addr, 3*time.Second); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if _, err := mgr.Probe(ctx, addr); err != nil {
		t.Fatalf("Probe on live peer: %v", err)
	}

	// 停掉对端：探测必须失败并把状态置为离线
	stop()
	st, err := mgr.Probe(ctx, addr)
	if err == nil {
		t.Fatal("Probe should fail after peer stops")
	}
	if st.Online {
		t.Fatalf("peer should be offline, state=%+v", st)
	}
	if mgr.Online(addr) {
		t.Fatal("Online() should be false after probe failure")
	}
	if st.LastError == "" {
		t.Fatal("LastError should record the probe failure")
	}
}

// TestManagerProbeUnknownPeer 校验探测未记录的地址直接报错。
func TestManagerProbeUnknownPeer(t *testing.T) {
	mgr := NewManager(testLogger())
	defer mgr.Close()
	if _, err := mgr.Probe(context.Background(), "127.0.0.1:1"); err == nil {
		t.Fatal("Probe on unknown peer should fail")
	}
}

// TestUpdateLayout 校验互验阶段记录的部署信息落到状态快照里。
func TestUpdateLayout(t *testing.T) {
	addr, stop := startPeer(t, "node-c:9001", []string{"node-c:9001/0"}, "", "")
	defer stop()

	mgr := NewManager(testLogger())
	defer mgr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := mgr.Dial(ctx, addr, 3*time.Second); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	mgr.UpdateLayout(addr, "dep-3", "layout-3")
	st, ok := mgr.State(addr)
	if !ok || st.DeploymentID != "dep-3" || st.LayoutHash != "layout-3" {
		t.Fatalf("unexpected state: %+v (ok=%v)", st, ok)
	}
}
