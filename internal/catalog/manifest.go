package catalog

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

const (
	manifestFileName = "MANIFEST"
	// schemaVersion=2：重建时按「旧版本先 emit」分配 Seq，修正旧索引里
	// 同一 key 的 Seq 顺序（最新版本必须 Seq 最大）。旧版本 manifest 会被
	// 判定为不兼容并触发全量重建，不会带着错误的 Seq 继续服务。
	schemaVersion = 2
)

// manifest 是目录下的唯一指针：记录当前 gen、已持久化 seq 与快照信息。
// SnapshotMaxSeq/Docs 仅作状态展示；恢复时以快照 footer 为准。
type manifest struct {
	SchemaVersion  int    `json:"schemaVersion"`
	Gen            uint64 `json:"gen"`
	AppliedSeq     uint64 `json:"appliedSeq"`
	SnapshotFile   string `json:"snapshotFile,omitempty"`
	SnapshotDocs   int    `json:"snapshotDocs,omitempty"`
	SnapshotMaxSeq uint64 `json:"snapshotMaxSeq,omitempty"`
}

func manifestPath(dir string) string {
	return filepath.Join(dir, manifestFileName)
}

// readManifest 读取 manifest；不存在返回 (nil, nil)；损坏返回错误。
func readManifest(dir string) (*manifest, error) {
	data, err := os.ReadFile(manifestPath(dir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if m.SchemaVersion != schemaVersion {
		return nil, errors.New("manifest: unsupported schema version")
	}
	return &m, nil
}

// writeManifest 原子写 manifest（tmp + rename）。
func writeManifest(dir string, m *manifest) error {
	m.SchemaVersion = schemaVersion
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	path := manifestPath(dir)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
