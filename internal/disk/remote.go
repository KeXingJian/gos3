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
	// uuid 返回本盘在集群布局中的 UUID；布局就绪前返回空串（服务端会跳过校验）。
	// 用函数而不是固定值，是因为布局要等 format.Bootstrap 之后才知道。
	uuid func() string
}

// NewRemote 基于到对端的 gRPC 连接 cc 创建远程磁盘。
// id 为磁盘标识；drive 是对端节点内的磁盘下标；uuid 返回该盘在布局中的 UUID（可为 nil）。
func NewRemote(id string, cc grpc.ClientConnInterface, drive int, uuid func() string) *Remote {
	return &Remote{id: id, drive: int32(drive), client: NewDiskServiceClient(cc), uuid: uuid}
}

func (r *Remote) ID() string {
	return r.id
}

// driveUUID 取当前已知的本盘 UUID（布局未就绪时为空串）。
func (r *Remote) driveUUID() string {
	if r.uuid == nil {
		return ""
	}
	return r.uuid()
}

// fileReq 组装带盘 UUID 校验的请求。
func (r *Remote) fileReq(path string) *FileRequest {
	return &FileRequest{Drive: r.drive, Path: path, DriveUuid: r.driveUUID()}
}

func (r *Remote) ReadFile(ctx context.Context, path string) ([]byte, error) {
	resp, err := r.client.ReadFile(ctx, r.fileReq(path))
	if err != nil {
		return nil, mapError(err)
	}
	return resp.Data, nil
}

func (r *Remote) WriteFile(ctx context.Context, path string, data []byte) error {
	_, err := r.client.WriteFile(ctx, &WriteRequest{Drive: r.drive, Path: path, Data: data, DriveUuid: r.driveUUID()})
	return mapError(err)
}

func (r *Remote) Rename(ctx context.Context, src, dst string) error {
	_, err := r.client.Rename(ctx, &RenameRequest{Drive: r.drive, Src: src, Dst: dst, DriveUuid: r.driveUUID()})
	return mapError(err)
}

func (r *Remote) DeleteFile(ctx context.Context, path string) error {
	_, err := r.client.DeleteFile(ctx, r.fileReq(path))
	return mapError(err)
}

func (r *Remote) DeleteDir(ctx context.Context, path string) error {
	_, err := r.client.DeleteDir(ctx, r.fileReq(path))
	return mapError(err)
}

func (r *Remote) MakeDir(ctx context.Context, path string) error {
	_, err := r.client.MakeDir(ctx, r.fileReq(path))
	return mapError(err)
}

func (r *Remote) Stat(ctx context.Context, path string) (FileInfo, error) {
	resp, err := r.client.Stat(ctx, r.fileReq(path))
	if err != nil {
		return FileInfo{}, mapError(err)
	}
	return FileInfo{Exists: resp.Exists, IsDir: resp.IsDir, Size: resp.Size, ModTime: time.Unix(resp.ModUnix, 0)}, nil
}

func (r *Remote) ListDir(ctx context.Context, path string) ([]Entry, error) {
	resp, err := r.client.ListDir(ctx, r.fileReq(path))
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
	resp, err := r.client.Walk(ctx, r.fileReq(path))
	if err != nil {
		return nil, nil, mapError(err)
	}
	return resp.Files, resp.Dirs, nil
}

// Health 探测对端这一块盘的健康（盘级）。
func (r *Remote) Health(ctx context.Context) error {
	_, err := r.client.Health(ctx, &FileRequest{Drive: r.drive, DriveUuid: r.driveUUID()})
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
