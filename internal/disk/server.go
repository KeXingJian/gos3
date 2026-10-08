package disk

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server 是实现 DiskService 的 gRPC 服务端，代表一个节点。
// 它持有本节点的若干磁盘，其他节点通过该服务读写这些磁盘。
// 节点级信息（对外地址/成员/布局）由 internal/peer 的 PeerService 负责。
type Server struct {
	UnimplementedDiskServiceServer
	address string
	disks   []Disk
	log     *slog.Logger

	// driveUUIDs 是各槽位盘的 UUID（来自本盘 format.json），用于校验请求里的 drive_uuid；
	// 未就绪时对应元素为空串，此时跳过校验。
	mu         sync.RWMutex
	driveUUIDs []string
}

// NewServer 创建节点磁盘服务：address 为本节点对外地址，disks 为本节点磁盘列表。
func NewServer(address string, disks []Disk, log *slog.Logger) *Server {
	return &Server{address: address, disks: disks, log: log, driveUUIDs: make([]string, len(disks))}
}

// Register 把本服务注册到 gRPC server 上。
func (s *Server) Register(g *grpc.Server) {
	RegisterDiskServiceServer(g, s)
}

// SetDriveUUID 记录某块盘的 UUID（启动时从该盘的 format.json 读出）。
func (s *Server) SetDriveUUID(index int, uuid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index >= 0 && index < len(s.driveUUIDs) {
		s.driveUUIDs[index] = uuid
	}
}

func (s *Server) driveUUID(index int) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if index < 0 || index >= len(s.driveUUIDs) {
		return ""
	}
	return s.driveUUIDs[index]
}

// drive 按下标取出对应磁盘，并校验对端期望的盘 UUID。
// 越界返回 InvalidArgument；UUID 不符返回 FailedPrecondition（对齐 MinIO 的 errDiskStale），
// 避免把分片写到一块「已经不是布局里那块」的盘上。
func (s *Server) drive(index int32, wantUUID string) (Disk, error) {
	if index < 0 || int(index) >= len(s.disks) {
		return nil, status.Errorf(codes.InvalidArgument, "drive index %d out of range", index)
	}
	if wantUUID != "" {
		if got := s.driveUUID(int(index)); got != "" && got != wantUUID {
			return nil, status.Errorf(codes.FailedPrecondition,
				"drive %d uuid mismatch: request expects %s, this drive is %s", index, wantUUID, got)
		}
	}
	return s.disks[index], nil
}

func (s *Server) ReadFile(ctx context.Context, req *FileRequest) (*FileResponse, error) {
	d, err := s.drive(req.Drive, req.DriveUuid)
	if err != nil {
		return nil, err
	}
	data, err := d.ReadFile(ctx, req.Path)
	if err != nil {
		return nil, toStatus(err)
	}
	return &FileResponse{Data: data}, nil
}

func (s *Server) WriteFile(ctx context.Context, req *WriteRequest) (*Empty, error) {
	d, err := s.drive(req.Drive, req.DriveUuid)
	if err != nil {
		return nil, err
	}
	if err := d.WriteFile(ctx, req.Path, req.Data); err != nil {
		return nil, toStatus(err)
	}
	return &Empty{}, nil
}

func (s *Server) Rename(ctx context.Context, req *RenameRequest) (*Empty, error) {
	d, err := s.drive(req.Drive, req.DriveUuid)
	if err != nil {
		return nil, err
	}
	if err := d.Rename(ctx, req.Src, req.Dst); err != nil {
		return nil, toStatus(err)
	}
	return &Empty{}, nil
}

func (s *Server) DeleteFile(ctx context.Context, req *FileRequest) (*Empty, error) {
	d, err := s.drive(req.Drive, req.DriveUuid)
	if err != nil {
		return nil, err
	}
	if err := d.DeleteFile(ctx, req.Path); err != nil {
		return nil, toStatus(err)
	}
	return &Empty{}, nil
}

func (s *Server) DeleteDir(ctx context.Context, req *FileRequest) (*Empty, error) {
	d, err := s.drive(req.Drive, req.DriveUuid)
	if err != nil {
		return nil, err
	}
	if err := d.DeleteDir(ctx, req.Path); err != nil {
		return nil, toStatus(err)
	}
	return &Empty{}, nil
}

func (s *Server) MakeDir(ctx context.Context, req *FileRequest) (*Empty, error) {
	d, err := s.drive(req.Drive, req.DriveUuid)
	if err != nil {
		return nil, err
	}
	if err := d.MakeDir(ctx, req.Path); err != nil {
		return nil, toStatus(err)
	}
	return &Empty{}, nil
}

func (s *Server) Stat(ctx context.Context, req *FileRequest) (*StatResponse, error) {
	d, err := s.drive(req.Drive, req.DriveUuid)
	if err != nil {
		return nil, err
	}
	info, err := d.Stat(ctx, req.Path)
	if err != nil {
		return nil, toStatus(err)
	}
	return &StatResponse{Exists: info.Exists, IsDir: info.IsDir, Size: info.Size, ModUnix: info.ModTime.Unix()}, nil
}

func (s *Server) ListDir(ctx context.Context, req *FileRequest) (*DirResponse, error) {
	d, err := s.drive(req.Drive, req.DriveUuid)
	if err != nil {
		return nil, err
	}
	entries, err := d.ListDir(ctx, req.Path)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &DirResponse{}
	for _, e := range entries {
		resp.Entries = append(resp.Entries, &DirEntry{Name: e.Name, IsDir: e.IsDir})
	}
	return resp, nil
}

func (s *Server) Walk(ctx context.Context, req *FileRequest) (*WalkResponse, error) {
	d, err := s.drive(req.Drive, req.DriveUuid)
	if err != nil {
		return nil, err
	}
	files, dirs, err := d.Walk(ctx, req.Path)
	if err != nil {
		return nil, toStatus(err)
	}
	return &WalkResponse{Files: files, Dirs: dirs}, nil
}

// Health 探测单块盘是否可用（drive 指定下标）。
func (s *Server) Health(ctx context.Context, req *FileRequest) (*Empty, error) {
	d, err := s.drive(req.Drive, req.DriveUuid)
	if err != nil {
		return nil, err
	}
	if err := d.Health(ctx); err != nil {
		return nil, toStatus(err)
	}
	return &Empty{}, nil
}

func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotExist) {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
