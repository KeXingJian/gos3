package disk

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// fakeDisk 是最小的内存盘实现，仅覆盖测试用到的读写与健康检查。
type fakeDisk struct {
	id   string
	data map[string][]byte
}

func newFakeDisk(id string) *fakeDisk { return &fakeDisk{id: id, data: map[string][]byte{}} }

func (f *fakeDisk) ID() string { return f.id }

func (f *fakeDisk) ReadFile(ctx context.Context, path string) ([]byte, error) {
	data, ok := f.data[path]
	if !ok {
		return nil, ErrNotExist
	}
	return data, nil
}

func (f *fakeDisk) WriteFile(ctx context.Context, path string, data []byte) error {
	f.data[path] = data
	return nil
}

func (f *fakeDisk) Rename(ctx context.Context, src, dst string) error {
	data, ok := f.data[src]
	if !ok {
		return ErrNotExist
	}
	delete(f.data, src)
	f.data[dst] = data
	return nil
}

func (f *fakeDisk) DeleteFile(ctx context.Context, path string) error {
	delete(f.data, path)
	return nil
}

func (f *fakeDisk) DeleteDir(ctx context.Context, path string) error { return nil }
func (f *fakeDisk) MakeDir(ctx context.Context, path string) error   { return nil }

func (f *fakeDisk) Stat(ctx context.Context, path string) (FileInfo, error) {
	_, ok := f.data[path]
	return FileInfo{Exists: ok}, nil
}

func (f *fakeDisk) ListDir(ctx context.Context, path string) ([]Entry, error) { return nil, nil }
func (f *fakeDisk) Walk(ctx context.Context, path string) ([]string, []string, error) {
	return nil, nil, nil
}
func (f *fakeDisk) Health(ctx context.Context) error { return nil }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestServerDriveUUIDValidation 校验服务端按 drive_uuid 拒绝对「已经不是布局里那块」的盘写入。
func TestServerDriveUUIDValidation(t *testing.T) {
	ctx := context.Background()
	srv := NewServer("node-a:9001", []Disk{newFakeDisk("node-a:9001/0")}, testLogger())
	srv.SetDriveUUID(0, "uuid-1")

	// UUID 匹配：正常写入
	if _, err := srv.WriteFile(ctx, &WriteRequest{Drive: 0, Path: "a", Data: []byte("x"), DriveUuid: "uuid-1"}); err != nil {
		t.Fatalf("write with matching uuid: %v", err)
	}
	// UUID 不匹配：FailedPrecondition（盘映射过期）
	_, err := srv.WriteFile(ctx, &WriteRequest{Drive: 0, Path: "a", Data: []byte("x"), DriveUuid: "uuid-2"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("write with stale uuid: code = %v, want FailedPrecondition", status.Code(err))
	}
	// 未带 UUID（布局尚未就绪）：跳过校验
	if _, err := srv.ReadFile(ctx, &FileRequest{Drive: 0, Path: "a"}); err != nil {
		t.Fatalf("read without uuid: %v", err)
	}
	// 越界下标：InvalidArgument
	if _, err := srv.ReadFile(ctx, &FileRequest{Drive: 3, Path: "a"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("out of range drive: code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestRemoteCarriesDriveUUID 校验 Remote 把布局中的盘 UUID 带到每一次请求上。
func TestRemoteCarriesDriveUUID(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	srv := NewServer("node-a:9001", []Disk{newFakeDisk("node-a:9001/0")}, testLogger())
	srv.SetDriveUUID(0, "uuid-1")
	srv.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// UUID 与对端一致：写入/读取成功
	good := NewRemote("node-a:9001/0", conn, 0, func() string { return "uuid-1" })
	if err := good.WriteFile(ctx, "a", []byte("hello")); err != nil {
		t.Fatalf("remote write with matching uuid: %v", err)
	}
	if _, err := good.ReadFile(ctx, "a"); err != nil {
		t.Fatalf("remote read with matching uuid: %v", err)
	}

	// 对端布局与本地认知不一致：被 FailedPrecondition 拒绝
	stale := NewRemote("node-a:9001/0", conn, 0, func() string { return "uuid-2" })
	if err := stale.WriteFile(ctx, "a", []byte("hello")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("remote write with stale uuid: code = %v, want FailedPrecondition", status.Code(err))
	}
	// 布局尚未就绪（返回空串）：不校验，仍可读写
	pending := NewRemote("node-a:9001/0", conn, 0, nil)
	if err := pending.WriteFile(ctx, "b", []byte("hello")); err != nil {
		t.Fatalf("remote write without uuid: %v", err)
	}
}
