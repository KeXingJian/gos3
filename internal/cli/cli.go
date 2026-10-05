package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kxj/gos3/internal/auth"
	"github.com/kxj/gos3/internal/cluster"
	"github.com/kxj/gos3/internal/config"
	"github.com/kxj/gos3/internal/disk"
	"github.com/kxj/gos3/internal/iam"
	"github.com/kxj/gos3/internal/server"
	"github.com/kxj/gos3/internal/store"
	"github.com/kxj/gos3/internal/telemetry"
	"github.com/kxj/gos3/internal/version"
)

const usageText = `gos3 - a minimal S3-compatible object storage for learning Go

USAGE:
  gos3 server [flags] <data-dir> [data-dir...]
  gos3 version
  gos3 help

  One data directory runs a single-drive filesystem backend.
  Two or more directories enable erasure coding across drives.

SERVER FLAGS:
  -address         listen address (default ":9000")
  -region          region name (default "us-east-1")
  -root-user       root access key (env GOS3_ROOT_USER)
  -root-password   root secret key (env GOS3_ROOT_PASSWORD)
  -data-shards     erasure data shards (0 = auto)
  -parity-shards   erasure parity shards (0 = auto)
  -grpc-address    internode gRPC listen address (default ":9001")
  -advertise       gRPC address other nodes use to reach this node
  -peers           comma-separated peer gRPC addresses (distributed mode)
`

func Main(args []string) int {
	if len(args) < 2 {
		fmt.Print(usageText)
		return 1
	}
	switch args[1] {
	case "server":
		return runServer(args[2:])
	case "version", "--version", "-v":
		fmt.Println(version.String())
		return 0
	case "help", "--help", "-h":
		fmt.Print(usageText)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", args[1])
		fmt.Print(usageText)
		return 1
	}
}

func runServer(args []string) int {
	cfg := config.Default()
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.StringVar(&cfg.Address, "address", cfg.Address, "listen address")
	fs.StringVar(&cfg.Region, "region", cfg.Region, "region name")
	fs.StringVar(&cfg.RootUser, "root-user", cfg.RootUser, "root access key")
	fs.StringVar(&cfg.RootPass, "root-password", cfg.RootPass, "root secret key")
	fs.IntVar(&cfg.DataShards, "data-shards", 0, "erasure data shards (0 = auto)")
	fs.IntVar(&cfg.ParityShards, "parity-shards", 0, "erasure parity shards (0 = auto)")
	fs.StringVar(&cfg.GRPCAddress, "grpc-address", cfg.GRPCAddress, "internode gRPC listen address")
	fs.StringVar(&cfg.Advertise, "advertise", "", "gRPC address other nodes use to reach this node")
	fs.DurationVar(&cfg.ScanInterval, "scan-interval", cfg.ScanInterval, "lifecycle scan interval")
	peersFlag := fs.String("peers", "", "comma-separated peer gRPC addresses")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "error: at least one data directory is required")
		fmt.Print(usageText)
		return 1
	}
	cfg.DataDirs = fs.Args()
	cfg.DataDir = cfg.DataDirs[0]
	cfg.Peers = splitPeers(*peersFlag)
	if cfg.IAMDir == "" {
		cfg.IAMDir = filepath.Join(cfg.DataDir, ".iam")
	}

	logger := slog.New(telemetry.ContextHandler{
		Handler: slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
	})

	shutdownTelemetry, err := telemetry.Setup(context.Background(), "gos3", version.Version)
	if err != nil {
		logger.Error("[gos3: telemetry-init-failed]", "error", err.Error())
		return 1
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTelemetry(shutdownCtx)
	}()

	st, stopStore, err := buildStore(cfg, logger)
	if err != nil {
		logger.Error("[gos3: store-init-failed]", "error", err.Error())
		return 1
	}
	defer stopStore()

	iamStore, err := iam.New(cfg.IAMDir, auth.Credentials{AccessKey: cfg.RootUser, SecretKey: cfg.RootPass}, logger)
	if err != nil {
		logger.Error("[gos3: iam-init-failed]", "error", err.Error())
		return 1
	}
	srv := server.New(cfg, st, iamStore, logger)

	httpServer := &http.Server{
		Addr:              cfg.Address,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go cleanupLoop(ctx, st, logger)
	go lifecycleLoop(ctx, st, logger, cfg.ScanInterval)

	errCh := make(chan error, 1)
	go func() {
		logger.Info("[gos3: server-start]",
			"address", cfg.Address,
			"drives", len(cfg.DataDirs),
			"region", cfg.Region,
			"root-user", cfg.RootUser,
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		logger.Error("[gos3: server-error]", "error", err.Error())
		return 1
	case <-ctx.Done():
		logger.Info("[gos3: server-shutdown]")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("[gos3: shutdown-error]", "error", err.Error())
		return 1
	}
	return 0
}

func buildStore(cfg config.Config, logger *slog.Logger) (store.Store, func(), error) {
	if len(cfg.Peers) == 0 {
		if len(cfg.DataDirs) == 1 {
			st, err := store.NewFS(cfg.DataDirs[0], logger)
			return st, func() {}, err
		}
		dataShards, parityShards, err := layout(len(cfg.DataDirs), cfg.DataShards, cfg.ParityShards)
		if err != nil {
			return nil, nil, err
		}
		disks := make([]disk.Disk, len(cfg.DataDirs))
		for i, dir := range cfg.DataDirs {
			d, err := disk.NewLocal(fmt.Sprintf("local/%d", i), dir)
			if err != nil {
				return nil, nil, err
			}
			disks[i] = d
		}
		st, err := store.NewErasure(disks, dataShards, parityShards, logger)
		return st, func() {}, err
	}

	if cfg.Advertise == "" {
		return nil, nil, errors.New("distributed mode requires -advertise")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cl, err := cluster.Build(ctx, cluster.Options{
		Advertise:   cfg.Advertise,
		Listen:      cfg.GRPCAddress,
		LocalDirs:   cfg.DataDirs,
		Peers:       cfg.Peers,
		Logger:      logger,
		DialTimeout: 90 * time.Second,
	})
	if err != nil {
		return nil, nil, err
	}
	dataShards, parityShards, err := layout(len(cl.Disks()), cfg.DataShards, cfg.ParityShards)
	if err != nil {
		cl.Close()
		return nil, nil, err
	}
	st, err := store.NewErasure(cl.Disks(), dataShards, parityShards, logger)
	if err != nil {
		cl.Close()
		return nil, nil, err
	}
	return st, cl.Close, nil
}

func layout(total, dataShards, parityShards int) (int, int, error) {
	if total < 2 {
		return 0, 0, fmt.Errorf("erasure coding needs at least 2 drives, got %d", total)
	}
	switch {
	case dataShards == 0 && parityShards == 0:
		parityShards = total / 2
		if parityShards < 1 {
			parityShards = 1
		}
		dataShards = total - parityShards
	case dataShards == 0:
		dataShards = total - parityShards
	case parityShards == 0:
		parityShards = total - dataShards
	}
	if dataShards < 1 || parityShards < 1 || dataShards+parityShards != total {
		return 0, 0, fmt.Errorf("invalid erasure layout: %d drives, %d data + %d parity", total, dataShards, parityShards)
	}
	return dataShards, parityShards, nil
}

func splitPeers(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func lifecycleLoop(ctx context.Context, st store.Store, logger *slog.Logger, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runLifecycle(ctx, st, logger)
		}
	}
}

func runLifecycle(ctx context.Context, st store.Store, logger *slog.Logger) {
	buckets, err := st.ListBuckets(ctx)
	if err != nil {
		return
	}
	for _, b := range buckets {
		cfg, err := st.GetBucketLifecycle(ctx, b.Name)
		if err != nil || len(cfg.Rules) == 0 {
			continue
		}
		marker := ""
		for {
			res, err := st.ListObjects(ctx, b.Name, store.ListOptions{MaxKeys: 1000, Marker: marker})
			if err != nil {
				break
			}
			now := time.Now().UTC()
			for _, obj := range res.Objects {
				if !cfg.Expired(obj.Name, obj.ModTime, now) {
					continue
				}
				if _, err := st.DeleteObject(ctx, b.Name, obj.Name, ""); err == nil {
					logger.Info("[gos3: lifecycle-expire]", "bucket", b.Name, "object", obj.Name)
				}
			}
			if !res.IsTruncated || res.NextMarker == "" {
				break
			}
			marker = res.NextMarker
		}
	}
}

func cleanupLoop(ctx context.Context, st store.Store, logger *slog.Logger) {
	const (
		interval = 6 * time.Hour
		stale    = 24 * time.Hour
	)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			removed, err := st.CleanupStaleUploads(ctx, stale)
			if err != nil {
				logger.Warn("[gos3: cleanup-failed]", "error", err.Error())
				continue
			}
			if removed > 0 {
				logger.Info("[gos3: cleanup-stale-uploads]", "removed", removed)
			}
		}
	}
}
