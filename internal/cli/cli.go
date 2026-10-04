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
	"syscall"
	"time"

	"github.com/kxj/gos3/internal/auth"
	"github.com/kxj/gos3/internal/config"
	"github.com/kxj/gos3/internal/server"
	"github.com/kxj/gos3/internal/store"
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

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	st, err := newStore(cfg, logger)
	if err != nil {
		logger.Error("[gos3: store-init-failed]", "error", err.Error())
		return 1
	}
	creds := auth.NewStore(auth.Credentials{AccessKey: cfg.RootUser, SecretKey: cfg.RootPass})
	srv := server.New(cfg, st, creds, logger)

	httpServer := &http.Server{
		Addr:              cfg.Address,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go cleanupLoop(ctx, st, logger)

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

func newStore(cfg config.Config, logger *slog.Logger) (store.Store, error) {
	total := len(cfg.DataDirs)
	if total == 1 {
		return store.NewFS(cfg.DataDirs[0], logger)
	}
	dataShards, parityShards := cfg.DataShards, cfg.ParityShards
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
		return nil, fmt.Errorf("invalid erasure layout: %d drives, %d data + %d parity", total, dataShards, parityShards)
	}
	return store.NewErasure(cfg.DataDirs, dataShards, parityShards, logger)
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
