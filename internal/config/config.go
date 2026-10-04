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
	MaxSkew           time.Duration
	ReadHeaderTimeout time.Duration
	ShutdownTimeout   time.Duration
}

func Default() Config {
	return Config{
		Address:           DefaultAddress,
		GRPCAddress:       ":9001",
		Region:            DefaultRegion,
		RootUser:          envOr("GOS3_ROOT_USER", "minioadmin"),
		RootPass:          envOr("GOS3_ROOT_PASSWORD", "minioadmin"),
		OwnerID:           DefaultOwnerID,
		MaxSkew:           15 * time.Minute,
		ReadHeaderTimeout: 10 * time.Second,
		ShutdownTimeout:   10 * time.Second,
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
