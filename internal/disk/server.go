package disk

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	UnimplementedDiskServiceServer
	address string
	disks   []Disk
	log     *slog.Logger
}

func NewServer(address string, disks []Disk, log *slog.Logger) *Server {
	return &Server{address: address, disks: disks, log: log}
}

func (s *Server) Register(g *grpc.Server) {
	RegisterDiskServiceServer(g, s)
}

func (s *Server) drive(index int32) (Disk, error) {
	if index < 0 || int(index) >= len(s.disks) {
		return nil, status.Errorf(codes.InvalidArgument, "drive index %d out of range", index)
	}
	return s.disks[index], nil
}

func (s *Server) Info(ctx context.Context, _ *NodeInfoRequest) (*NodeInfo, error) {
	drives := make([]string, len(s.disks))
	for i := range s.disks {
		drives[i] = fmt.Sprintf("%d", i)
	}
	return &NodeInfo{Address: s.address, Drives: drives}, nil
}

func (s *Server) ReadFile(ctx context.Context, req *FileRequest) (*FileResponse, error) {
	d, err := s.drive(req.Drive)
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
	d, err := s.drive(req.Drive)
	if err != nil {
		return nil, err
	}
	if err := d.WriteFile(ctx, req.Path, req.Data); err != nil {
		return nil, toStatus(err)
	}
	return &Empty{}, nil
}

func (s *Server) DeleteFile(ctx context.Context, req *FileRequest) (*Empty, error) {
	d, err := s.drive(req.Drive)
	if err != nil {
		return nil, err
	}
	if err := d.DeleteFile(ctx, req.Path); err != nil {
		return nil, toStatus(err)
	}
	return &Empty{}, nil
}

func (s *Server) DeleteDir(ctx context.Context, req *FileRequest) (*Empty, error) {
	d, err := s.drive(req.Drive)
	if err != nil {
		return nil, err
	}
	if err := d.DeleteDir(ctx, req.Path); err != nil {
		return nil, toStatus(err)
	}
	return &Empty{}, nil
}

func (s *Server) MakeDir(ctx context.Context, req *FileRequest) (*Empty, error) {
	d, err := s.drive(req.Drive)
	if err != nil {
		return nil, err
	}
	if err := d.MakeDir(ctx, req.Path); err != nil {
		return nil, toStatus(err)
	}
	return &Empty{}, nil
}

func (s *Server) Stat(ctx context.Context, req *FileRequest) (*StatResponse, error) {
	d, err := s.drive(req.Drive)
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
	d, err := s.drive(req.Drive)
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
	d, err := s.drive(req.Drive)
	if err != nil {
		return nil, err
	}
	files, dirs, err := d.Walk(ctx, req.Path)
	if err != nil {
		return nil, toStatus(err)
	}
	return &WalkResponse{Files: files, Dirs: dirs}, nil
}

func (s *Server) Health(ctx context.Context, _ *Empty) (*Empty, error) {
	for _, d := range s.disks {
		if err := d.Health(ctx); err != nil {
			return nil, toStatus(err)
		}
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
