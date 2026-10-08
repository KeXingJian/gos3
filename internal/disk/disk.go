package disk

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotExist = errors.New("disk: file does not exist")
)

type FileInfo struct {
	Exists  bool
	IsDir   bool
	Size    int64
	ModTime time.Time
}

type Entry struct {
	Name  string
	IsDir bool
}

// Disk 是存储层的磁盘抽象：屏蔽本地目录(Local)与远程节点(Remote)的差异，
// 使纠删码存储层无需关心某个分片究竟落在本机还是别的节点上。
type Disk interface {
	ID() string
	ReadFile(ctx context.Context, path string) ([]byte, error)
	WriteFile(ctx context.Context, path string, data []byte) error
	// Rename 原子地把 src 移动到 dst（同盘内），是「临时写入 -> 提交」两阶段写的提交动作。
	Rename(ctx context.Context, src, dst string) error
	DeleteFile(ctx context.Context, path string) error
	DeleteDir(ctx context.Context, path string) error
	MakeDir(ctx context.Context, path string) error
	Stat(ctx context.Context, path string) (FileInfo, error)
	ListDir(ctx context.Context, path string) ([]Entry, error)
	Walk(ctx context.Context, path string) (files []string, dirs []string, err error)
	Health(ctx context.Context) error
}
