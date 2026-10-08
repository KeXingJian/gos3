// Package peer 是集群的「节点级控制面」：
//   - Server 实现 PeerService，向对端回答成员信息（地址/盘列表/部署 ID/布局摘要）与存活状态；
//   - Manager 维护到每个 peer 的单条 gRPC 连接与在线状态，并周期探测。
//
// 磁盘级数据面仍在 internal/disk（DiskService），两者共用一个 gRPC server 与端口。
// 这样拆分后，节点级接口（成员/健康，后续的分布式锁）不再混在磁盘读写里。
package peer

import (
	"context"
	"log/slog"
	"sync/atomic"

	"google.golang.org/grpc"
)

// Layout 是本节点已固化的布局摘要（来自 format.json），供对端交叉校验。
type Layout struct {
	DeploymentID string
	LayoutHash   string
}

// Server 实现 PeerService。
type Server struct {
	UnimplementedPeerServiceServer
	address string
	drives  []string
	log     *slog.Logger
	// layout 在 format.json 校验通过后写入；未就绪时为 nil。
	layout atomic.Pointer[Layout]
}

// NewServer 创建节点控制面服务：address 为本节点对外地址，drives 为本地盘标识列表。
func NewServer(address string, drives []string, log *slog.Logger) *Server {
	return &Server{address: address, drives: drives, log: log}
}

// Register 把本服务注册到 gRPC server 上。
func (s *Server) Register(g *grpc.Server) {
	RegisterPeerServiceServer(g, s)
}

// SetLayout 发布本节点已固化的布局。
func (s *Server) SetLayout(deploymentID, layoutHash string) {
	s.layout.Store(&Layout{DeploymentID: deploymentID, LayoutHash: layoutHash})
}

// Info 返回本节点成员信息。
func (s *Server) Info(ctx context.Context, _ *InfoRequest) (*InfoResponse, error) {
	resp := &InfoResponse{Address: s.address, Drives: s.drives}
	if l := s.layout.Load(); l != nil {
		resp.DeploymentId = l.DeploymentID
		resp.LayoutHash = l.LayoutHash
	}
	return resp, nil
}

// Health 返回本节点存活状态：只要能应答即为存活，盘级健康由 DiskService.Health 探测。
func (s *Server) Health(ctx context.Context, _ *HealthRequest) (*HealthResponse, error) {
	return &HealthResponse{Ok: true}, nil
}
