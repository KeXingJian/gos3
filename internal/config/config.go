package config

import (
	"os"
	"time"
)

const (
	DefaultAddress = ":9000"
	DefaultRegion  = "us-east-1"

	DefaultOwnerID = "02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4"
)

type Config struct {
	Address           string
	DataDir           string
	DataDirs          []string
	DataShards        int
	ParityShards      int
	GRPCAddress       string
	Advertise         string
	Peers             []string
	Region            string
	RootUser          string
	RootPass          string
	OwnerID           string
	IAMDir            string
	ScanInterval      time.Duration
	MaxSkew           time.Duration
	ReadHeaderTimeout time.Duration
	ShutdownTimeout   time.Duration

	// Catalog 启用元数据索引（默认 false，显式开启）
	Catalog bool
	// CatalogDir 索引目录（默认 <主数据目录>/.catalog）
	CatalogDir string
	// CatalogMetaWhitelist 允许建二级索引的 user-meta key（小写、不含 x-amz-meta- 前缀）
	CatalogMetaWhitelist []string
	// CatalogTagWhitelist 允许建二级索引的 tag key
	CatalogTagWhitelist []string
	CatalogReconcile    time.Duration
}

func Default() Config {
	return Config{
		Address:           DefaultAddress,
		GRPCAddress:       ":9001",
		Region:            DefaultRegion,
		RootUser:          envOr("GOS3_ROOT_USER", "minioadmin"),
		RootPass:          envOr("GOS3_ROOT_PASSWORD", "minioadmin"),
		OwnerID:           DefaultOwnerID,
		ScanInterval:      time.Minute,
		MaxSkew:           15 * time.Minute,
		ReadHeaderTimeout: 10 * time.Second,
		ShutdownTimeout:   10 * time.Second,
		Catalog:           true,
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
