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
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Layout 是本节点已固化的布局摘要（来自 format.json），供对端交叉校验。
type Layout struct {
	DeploymentID string
	LayoutHash   string
}

// IAMHandler 是节点控制面对本地 IAM 存储的最小依赖（由 *iam.Store 实现）。
// peer 包不直接依赖 iam，避免控制面与业务包耦合。
type IAMHandler interface {
	// SnapshotJSON 返回当前 IAM 全量快照（JSON）。
	SnapshotJSON() ([]byte, error)
	// ApplySnapshotJSON 合并对端快照，返回状态是否变化以及合并后的修订号。
	ApplySnapshotJSON(data []byte) (bool, uint64, error)
	// Revision 返回当前逻辑修订号。
	Revision() uint64
}

// Server 实现 PeerService。
type Server struct {
	UnimplementedPeerServiceServer
	address string
	drives  []string
	log     *slog.Logger
	// layout 在 format.json 校验通过后写入；未就绪时为 nil。
	layout atomic.Pointer[Layout]
	// iam 在 IAM 存储初始化后注入；启动早期可能为 nil（RPC 返回 Unavailable）。
	iamMu sync.RWMutex
	iam   IAMHandler
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

// SetIAM 注入本地 IAM 存储，供对端拉取/推送快照。
// IAM 存储通常在集群组建完成后才初始化，因此这里允许后置注入。
func (s *Server) SetIAM(h IAMHandler) {
	s.iamMu.Lock()
	s.iam = h
	s.iamMu.Unlock()
}

func (s *Server) iamHandler() IAMHandler {
	s.iamMu.RLock()
	defer s.iamMu.RUnlock()
	return s.iam
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

// GetIAM 返回本节点当前 IAM 全量快照，供对端启动/重连时追平。
func (s *Server) GetIAM(ctx context.Context, _ *GetIAMRequest) (*GetIAMResponse, error) {
	h := s.iamHandler()
	if h == nil {
		return nil, status.Error(codes.Unavailable, "peer: iam not ready")
	}
	data, err := h.SnapshotJSON()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "peer: iam snapshot: %v", err)
	}
	return &GetIAMResponse{Snapshot: data, Revision: h.Revision()}, nil
}

// ApplyIAM 合并对端推送的 IAM 快照。合并是幂等的：重复推送不会产生额外变更。
func (s *Server) ApplyIAM(ctx context.Context, req *IAMRequest) (*IAMResponse, error) {
	h := s.iamHandler()
	if h == nil {
		return nil, status.Error(codes.Unavailable, "peer: iam not ready")
	}
	changed, rev, err := h.ApplySnapshotJSON(req.GetSnapshot())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "peer: iam apply: %v", err)
	}
	if changed {
		s.log.Info("[gos3: iam-applied]", "revision", rev)
	}
	return &IAMResponse{Changed: changed, Revision: rev}, nil
}
