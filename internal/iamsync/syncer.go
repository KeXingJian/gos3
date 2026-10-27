// Package iamsync 在集群节点之间复制 IAM 状态（用户、策略、策略绑定）。
//
// 复制模型是「状态型 + 最后写入者胜」：
//   - 本地管理写操作完成后，同步把全量快照推送给所有在线 peer（Replicate）；
//   - 后台周期对账（Sync）先推后拉，覆盖「写时对端离线」「节点后加入」等场景；
//   - 修订号由 iam.Store 维护：对端更大则覆盖，相等则并集合并，保证最终收敛。
//
// 快照很小（用户/策略数量有限），全量复制比维护增量日志更简单，也不会因为
// 某次 RPC 丢失而永久不一致。
package iamsync

import (
	"context"
	"log/slog"
	"time"

	"github.com/kxj/gos3/internal/iam"
	"github.com/kxj/gos3/internal/peer"
)

const (
	// syncInterval 是后台对账周期：覆盖写时对端离线、节点后加入等场景。
	syncInterval = 10 * time.Second
	// rpcTimeout 是单次快照 RPC 的超时，避免慢节点拖住管理请求。
	rpcTimeout = 3 * time.Second
)

// Syncer 负责把本地 IAM 状态复制到其他节点，并从其他节点追平。
type Syncer struct {
	store *iam.Store
	peers *peer.Manager
	log   *slog.Logger
}

// New 创建 IAM 复制器。
func New(store *iam.Store, peers *peer.Manager, log *slog.Logger) *Syncer {
	return &Syncer{store: store, peers: peers, log: log}
}

// Replicate 把本地 IAM 全量快照推送给所有在线 peer。
// 供管理写操作后同步调用：返回时在线 peer 已经应用（或明确失败，由后台对账兜底）。
func (s *Syncer) Replicate(ctx context.Context) {
	// 管理请求结束后 ctx 可能被取消，但复制仍需完成，因此脱钩后单独加超时。
	s.pushAll(context.WithoutCancel(ctx))
}

// pushAll 把本地快照推送给所有在线 peer；每个 RPC 单独限时。
func (s *Syncer) pushAll(ctx context.Context) {
	snap, err := s.store.SnapshotJSON()
	if err != nil {
		s.log.Warn("[gos3: iam-snapshot-failed]", "error", err.Error())
		return
	}
	revision := s.store.Revision()

	for _, st := range s.peers.States() {
		if !st.Online {
			continue
		}
		conn, ok := s.peers.Conn(st.Addr)
		if !ok {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		resp, err := peer.NewPeerServiceClient(conn).ApplyIAM(cctx, &peer.IAMRequest{Snapshot: snap})
		cancel()
		if err != nil {
			s.log.Warn("[gos3: iam-push-failed]", "peer", st.Addr, "revision", revision, "error", err.Error())
			continue
		}
		if resp.GetChanged() {
			s.log.Info("[gos3: iam-pushed]", "peer", st.Addr, "revision", resp.GetRevision())
		}
	}
}

// Sync 先推送本地快照，再拉取各 peer 的快照合并，双向追平。
func (s *Syncer) Sync(ctx context.Context) {
	s.pushAll(ctx)
	s.pullAll(ctx)
}

// Run 启动时先追平一次，之后按固定周期对账，阻塞直到 ctx 结束。
func (s *Syncer) Run(ctx context.Context) {
	s.Sync(ctx)

	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Sync(ctx)
		}
	}
}

// pullAll 拉取各在线 peer 的 IAM 快照，按修订号合并到本地。
func (s *Syncer) pullAll(ctx context.Context) {
	for _, st := range s.peers.States() {
		if !st.Online {
			continue
		}
		conn, ok := s.peers.Conn(st.Addr)
		if !ok {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		resp, err := peer.NewPeerServiceClient(conn).GetIAM(cctx, &peer.GetIAMRequest{})
		cancel()
		if err != nil {
			s.log.Warn("[gos3: iam-pull-failed]", "peer", st.Addr, "error", err.Error())
			continue
		}
		changed, revision, err := s.store.ApplySnapshotJSON(resp.GetSnapshot())
		if err != nil {
			s.log.Warn("[gos3: iam-merge-failed]", "peer", st.Addr, "error", err.Error())
			continue
		}
		if changed {
			s.log.Info("[gos3: iam-pulled]", "peer", st.Addr, "revision", revision)
		}
	}
}
