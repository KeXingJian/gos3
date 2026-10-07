package disk

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Remote 表示另一个节点上的一块磁盘：所有操作都通过 gRPC 转发到对端，
// 由对端的 disk.Server 落到真实的本地磁盘上。实现 Disk 接口。
type Remote struct {
	id     string
	drive  int32
	client DiskServiceClient
}

// NewRemote 基于到对端的 gRPC 连接 cc 创建远程磁盘。
// id 为磁盘标识；drive 是对端节点内的磁盘下标，用于在服务端定位具体磁盘。
func NewRemote(id string, cc grpc.ClientConnInterface, drive int) *Remote {
	return &Remote{id: id, drive: int32(drive), client: NewDiskServiceClient(cc)}
}

func (r *Remote) ID() string {
	return r.id
}

func (r *Remote) ReadFile(ctx context.Context, path string) ([]byte, error) {
	resp, err := r.client.ReadFile(ctx, &FileRequest{Drive: r.drive, Path: path})
	if err != nil {
		return nil, mapError(err)
	}
	return resp.Data, nil
}

func (r *Remote) WriteFile(ctx context.Context, path string, data []byte) error {
	_, err := r.client.WriteFile(ctx, &WriteRequest{Drive: r.drive, Path: path, Data: data})
	return mapError(err)
}

func (r *Remote) DeleteFile(ctx context.Context, path string) error {
	_, err := r.client.DeleteFile(ctx, &FileRequest{Drive: r.drive, Path: path})
	return mapError(err)
}

func (r *Remote) DeleteDir(ctx context.Context, path string) error {
	_, err := r.client.DeleteDir(ctx, &FileRequest{Drive: r.drive, Path: path})
	return mapError(err)
}

func (r *Remote) MakeDir(ctx context.Context, path string) error {
	_, err := r.client.MakeDir(ctx, &FileRequest{Drive: r.drive, Path: path})
	return mapError(err)
}

func (r *Remote) Stat(ctx context.Context, path string) (FileInfo, error) {
	resp, err := r.client.Stat(ctx, &FileRequest{Drive: r.drive, Path: path})
	if err != nil {
		return FileInfo{}, mapError(err)
	}
	return FileInfo{Exists: resp.Exists, IsDir: resp.IsDir, Size: resp.Size, ModTime: time.Unix(resp.ModUnix, 0)}, nil
}

func (r *Remote) ListDir(ctx context.Context, path string) ([]Entry, error) {
	resp, err := r.client.ListDir(ctx, &FileRequest{Drive: r.drive, Path: path})
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]Entry, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		out = append(out, Entry{Name: e.Name, IsDir: e.IsDir})
	}
	return out, nil
}

func (r *Remote) Walk(ctx context.Context, path string) ([]string, []string, error) {
	resp, err := r.client.Walk(ctx, &FileRequest{Drive: r.drive, Path: path})
	if err != nil {
		return nil, nil, mapError(err)
	}
	return resp.Files, resp.Dirs, nil
}

func (r *Remote) Health(ctx context.Context) error {
	_, err := r.client.Health(ctx, &Empty{})
	return mapError(err)
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.NotFound {
		return ErrNotExist
	}
	return err
}
