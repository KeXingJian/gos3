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

// runServer 启动 S3 兼容服务端的核心入口。
// 负责解析命令行参数、初始化日志/存储/IAM、启动后台任务，并处理优雅退出。
func runServer(args []string) int {
	// 加载默认配置，随后由命令行参数覆盖
	cfg := config.Default()
	// 创建 server 子命令的参数解析器，解析出错时不自动退出（由我们自行处理）
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
	// peers 采用逗号分隔字符串，稍后再拆分为切片
	peersFlag := fs.String("peers", "", "comma-separated peer gRPC addresses")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	// 至少需要一个数据目录（非 flag 剩余参数）
	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "error: at least one data directory is required")
		fmt.Print(usageText)
		return 1
	}
	// 其余位置参数全部视为数据目录，第一个作为主数据目录
	cfg.DataDirs = fs.Args()
	cfg.DataDir = cfg.DataDirs[0]
	cfg.Peers = splitPeers(*peersFlag)
	// IAM 凭据目录默认放在主数据目录下的 .iam
	if cfg.IAMDir == "" {
		cfg.IAMDir = filepath.Join(cfg.DataDir, ".iam")
	}

	// 构建带上下文的文本日志器，输出到标准输出，级别为 Info
	logger := slog.New(telemetry.ContextHandler{
		Handler: slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
	})

	// 初始化遥测（链路追踪等），并注册退出时的关闭回调
	shutdownTelemetry, err := telemetry.Setup(context.Background(), "gos3", version.Version)
	if err != nil {
		logger.Error("[gos3: telemetry-init-failed]", "error", err.Error())
		return 1
	}
	defer func() {
		// 关闭遥测时最多等待 5 秒
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTelemetry(shutdownCtx)
	}()

	// 根据配置构建存储后端（单盘文件系统 / 本地纠删码 / 分布式集群）
	st, stopStore, err := buildStore(cfg, logger)
	if err != nil {
		logger.Error("[gos3: store-init-failed]", "error", err.Error())
		return 1
	}
	defer stopStore()

	// 初始化 IAM 凭据存储，注入 root 用户凭据
	iamStore, err := iam.New(cfg.IAMDir, auth.Credentials{AccessKey: cfg.RootUser, SecretKey: cfg.RootPass}, logger)
	if err != nil {
		logger.Error("[gos3: iam-init-failed]", "error", err.Error())
		return 1
	}
	// 创建 S3 服务实例，并取其 HTTP 处理器
	srv := server.New(cfg, st, iamStore, logger)

	httpServer := &http.Server{
		Addr:              cfg.Address,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}

	// 监听 Ctrl+C（SIGINT）与 SIGTERM，收到信号后取消 ctx 以触发退出流程
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 后台任务：清理过期分片上传、执行生命周期规则
	go cleanupLoop(ctx, st, logger)
	go lifecycleLoop(ctx, st, logger, cfg.ScanInterval)

	// 用于把 HTTP 服务的致命错误传回主 goroutine
	errCh := make(chan error, 1)
	go func() {
		logger.Info("[gos3: server-start]",
			"address", cfg.Address,
			"drives", len(cfg.DataDirs),
			"region", cfg.Region,
			"root-user", cfg.RootUser,
		)
		// 监听失败（非正常关闭）时把错误写入通道
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// 阻塞等待：服务出错或收到退出信号
	select {
	case err := <-errCh:
		logger.Error("[gos3: server-error]", "error", err.Error())
		return 1
	case <-ctx.Done():
		logger.Info("[gos3: server-shutdown]")
	}

	// 优雅关闭：等待在途请求处理完成，超时时间由配置决定
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("[gos3: shutdown-error]", "error", err.Error())
		return 1
	}
	return 0
}

// buildStore 根据配置选择合适的存储后端。
// 返回存储实例、关闭函数以及错误。
func buildStore(cfg config.Config, logger *slog.Logger) (store.Store, func(), error) {
	// 未配置 peers：本地模式
	if len(cfg.Peers) == 0 {
		// 只有一个数据目录时使用简单的文件系统后端
		if len(cfg.DataDirs) == 1 {
			st, err := store.NewFS(cfg.DataDirs[0], logger)
			return st, func() {}, err
		}
		// 多个本地目录时使用纠删码：先计算数据/校验分片数
		dataShards, parityShards, err := layout(len(cfg.DataDirs), cfg.DataShards, cfg.ParityShards)
		if err != nil {
			return nil, nil, err
		}
		// 每个目录封装为一个本地磁盘
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

	// 分布式模式必须提供对外可达的 gRPC 地址
	if cfg.Advertise == "" {
		return nil, nil, errors.New("distributed mode requires -advertise")
	}
	// 构建集群（含内部 gRPC 通信），最多等待 90 秒
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
	// 基于集群所有磁盘计算纠删码布局
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

// layout 根据磁盘总数和用户指定的分片数，计算最终的数据分片与校验分片配置。
// dataShards、parityShards 为 0 表示自动推导。
func layout(total, dataShards, parityShards int) (int, int, error) {
	// 纠删码至少需要 2 块盘
	if total < 2 {
		return 0, 0, fmt.Errorf("erasure coding needs at least 2 drives, got %d", total)
	}
	switch {
	case dataShards == 0 && parityShards == 0:
		// 均未指定：默认约一半作为校验分片，至少 1 个
		parityShards = total / 2
		if parityShards < 1 {
			parityShards = 1
		}
		dataShards = total - parityShards
	case dataShards == 0:
		// 只给了校验分片数，数据分片数由剩余磁盘数得出
		dataShards = total - parityShards
	case parityShards == 0:
		// 只给了数据分片数
		parityShards = total - dataShards
	}
	// 合法性校验：分片数必须为正，且两者之和等于磁盘总数
	if dataShards < 1 || parityShards < 1 || dataShards+parityShards != total {
		return 0, 0, fmt.Errorf("invalid erasure layout: %d drives, %d data + %d parity", total, dataShards, parityShards)
	}
	return dataShards, parityShards, nil
}

// splitPeers 将逗号分隔的字符串拆分为去空白的地址切片。
func splitPeers(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		// 去除首尾空白，忽略空项
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// lifecycleLoop 按固定间隔周期性执行对象生命周期规则。
// interval <= 0 时禁用该任务。
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

// runLifecycle 遍历所有 bucket，按生命周期规则删除已过期的对象。
func runLifecycle(ctx context.Context, st store.Store, logger *slog.Logger) {
	buckets, err := st.ListBuckets(ctx)
	if err != nil {
		return
	}
	for _, b := range buckets {
		// 获取 bucket 的生命周期配置，无规则则跳过
		cfg, err := st.GetBucketLifecycle(ctx, b.Name)
		if err != nil || len(cfg.Rules) == 0 {
			continue
		}
		// 分页遍历对象，marker 用于翻页
		marker := ""
		for {
			res, err := st.ListObjects(ctx, b.Name, store.ListOptions{MaxKeys: 1000, Marker: marker})
			if err != nil {
				break
			}
			now := time.Now().UTC()
			for _, obj := range res.Objects {
				// 未过期则继续检查下一个
				if !cfg.Expired(obj.Name, obj.ModTime, now) {
					continue
				}
				// 删除过期对象（忽略删除失败，下轮可重试）
				if _, err := st.DeleteObject(ctx, b.Name, obj.Name, ""); err == nil {
					logger.Info("[gos3: lifecycle-expire]", "bucket", b.Name, "object", obj.Name)
				}
			}
			// 没有更多数据则结束分页
			if !res.IsTruncated || res.NextMarker == "" {
				break
			}
			marker = res.NextMarker
		}
	}
}

// cleanupLoop 周期性清理超过 stale 时长仍未完成的分片上传。
func cleanupLoop(ctx context.Context, st store.Store, logger *slog.Logger) {
	const (
		interval = 6 * time.Hour  // 清理周期
		stale    = 24 * time.Hour // 超过该时长视为残留上传
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
