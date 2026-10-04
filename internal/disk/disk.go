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

type Disk interface {
	ID() string
	ReadFile(ctx context.Context, path string) ([]byte, error)
	WriteFile(ctx context.Context, path string, data []byte) error
	DeleteFile(ctx context.Context, path string) error
	DeleteDir(ctx context.Context, path string) error
	MakeDir(ctx context.Context, path string) error
	Stat(ctx context.Context, path string) (FileInfo, error)
	ListDir(ctx context.Context, path string) ([]Entry, error)
	Walk(ctx context.Context, path string) (files []string, dirs []string, err error)
	Health(ctx context.Context) error
}
