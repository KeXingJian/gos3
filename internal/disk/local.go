package disk

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Local 表示一个本地磁盘（实际是一个本地目录），实现 Disk 接口。
type Local struct {
	id   string
	root string
}

// NewLocal 创建一个本地磁盘：把 root 转成绝对路径并确保目录存在。
// id 为磁盘标识（集群里形如 "<advertise>/<序号>"），用于日志与定位。
func NewLocal(id, root string) (*Local, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, err
	}
	return &Local{id: id, root: abs}, nil
}

func (l *Local) ID() string {
	return l.id
}

func (l *Local) full(path string) string {
	return filepath.Join(l.root, filepath.FromSlash(path))
}

func (l *Local) ReadFile(ctx context.Context, path string) ([]byte, error) {
	data, err := os.ReadFile(l.full(path))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotExist
	}
	return data, err
}

func (l *Local) WriteFile(ctx context.Context, path string, data []byte) error {
	full := l.full(path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	tmp := full + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, full)
}

func (l *Local) DeleteFile(ctx context.Context, path string) error {
	err := os.Remove(l.full(path))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (l *Local) DeleteDir(ctx context.Context, path string) error {
	return os.RemoveAll(l.full(path))
}

func (l *Local) MakeDir(ctx context.Context, path string) error {
	return os.MkdirAll(l.full(path), 0o755)
}

func (l *Local) Stat(ctx context.Context, path string) (FileInfo, error) {
	fi, err := os.Stat(l.full(path))
	if errors.Is(err, os.ErrNotExist) {
		return FileInfo{}, nil
	}
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{Exists: true, IsDir: fi.IsDir(), Size: fi.Size(), ModTime: fi.ModTime()}, nil
}

func (l *Local) ListDir(ctx context.Context, path string) ([]Entry, error) {
	entries, err := os.ReadDir(l.full(path))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]Entry, 0, len(entries))
	for _, e := range entries {
		out = append(out, Entry{Name: e.Name(), IsDir: e.IsDir()})
	}
	return out, nil
}

func (l *Local) Walk(ctx context.Context, path string) ([]string, []string, error) {
	base := l.full(path)
	if _, err := os.Stat(base); errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	var files, dirs []string
	err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(base, p)
		if relErr != nil {
			return relErr
		}
		if rel == "." {
			return nil
		}
		relSlash := filepath.ToSlash(rel)
		if d.IsDir() {
			dirs = append(dirs, relSlash)
		} else {
			files = append(files, relSlash)
		}
		return nil
	})
	return files, dirs, err
}

func (l *Local) Health(ctx context.Context) error {
	return nil
}
