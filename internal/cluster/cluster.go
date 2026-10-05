package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"time"

	"github.com/kxj/gos3/internal/disk"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const maxMessageSize = 128 << 20

type Options struct {
	Advertise   string
	Listen      string
	LocalDirs   []string
	Peers       []string
	Logger      *slog.Logger
	DialTimeout time.Duration
}

type Cluster struct {
	disks      []disk.Disk
	grpcServer *grpc.Server
	conns      []*grpc.ClientConn
	log        *slog.Logger
}

func Build(ctx context.Context, opts Options) (*Cluster, error) {
	log := opts.Logger
	localDisks := make([]disk.Disk, 0, len(opts.LocalDirs))
	for i, dir := range opts.LocalDirs {
		d, err := disk.NewLocal(fmt.Sprintf("%s/%d", opts.Advertise, i), dir)
		if err != nil {
			return nil, err
		}
		localDisks = append(localDisks, d)
	}

	lis, err := net.Listen("tcp", opts.Listen)
	if err != nil {
		return nil, fmt.Errorf("grpc listen %s: %w", opts.Listen, err)
	}
	gs := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(maxMessageSize),
		grpc.MaxSendMsgSize(maxMessageSize),
	)
	disk.NewServer(opts.Advertise, localDisks, log).Register(gs)
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Error("[gos3: grpc-serve]", "error", err.Error())
		}
	}()
	log.Info("[gos3: grpc-listen]", "listen", opts.Listen, "advertise", opts.Advertise, "local-drives", len(localDisks))

	nodes := map[string][]disk.Disk{opts.Advertise: localDisks}
	var conns []*grpc.ClientConn

	for _, peer := range opts.Peers {
		conn, err := dialPeer(ctx, peer, opts.DialTimeout, log)
		if err != nil {
			gs.Stop()
			return nil, err
		}
		conns = append(conns, conn)
		client := disk.NewDiskServiceClient(conn)
		info, err := client.Info(ctx, &disk.NodeInfoRequest{})
		if err != nil {
			gs.Stop()
			return nil, fmt.Errorf("peer %s info: %w", peer, err)
		}
		remote := make([]disk.Disk, 0, len(info.Drives))
		for i := range info.Drives {
			remote = append(remote, disk.NewRemote(fmt.Sprintf("%s/%d", info.Address, i), conn, i))
		}
		nodes[info.Address] = remote
		log.Info("[gos3: peer-joined]", "address", info.Address, "drives", len(info.Drives))
	}

	addrs := make([]string, 0, len(nodes))
	for addr := range nodes {
		addrs = append(addrs, addr)
	}
	sort.Strings(addrs)
	var disks []disk.Disk
	for _, addr := range addrs {
		disks = append(disks, nodes[addr]...)
	}
	log.Info("[gos3: cluster-ready]", "nodes", len(addrs), "drives", len(disks), "order", fmt.Sprint(addrs))
	return &Cluster{disks: disks, grpcServer: gs, conns: conns, log: log}, nil
}

func dialPeer(ctx context.Context, peer string, timeout time.Duration, log *slog.Logger) (*grpc.ClientConn, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		conn, err := grpc.NewClient(peer,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxMessageSize),
				grpc.MaxCallSendMsgSize(maxMessageSize),
			),
		)
		if err == nil {
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, infoErr := disk.NewDiskServiceClient(conn).Info(cctx, &disk.NodeInfoRequest{})
			cancel()
			if infoErr == nil {
				return conn, nil
			}
			_ = conn.Close()
			lastErr = infoErr
		} else {
			lastErr = err
		}
		log.Info("[gos3: waiting-for-peer]", "peer", peer, "error", fmt.Sprint(lastErr))
		time.Sleep(time.Second)
	}
	return nil, fmt.Errorf("peer %s not reachable: %w", peer, lastErr)
}

func (c *Cluster) Disks() []disk.Disk {
	return c.disks
}

func (c *Cluster) Close() {
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	if c.grpcServer != nil {
		c.grpcServer.Stop()
	}
}
