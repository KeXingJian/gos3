// Package catalog 实现对象存储元数据索引（Catalog）。
//
// 这是对 MinIO 商业版 Catalog / Metadata Search 的简化实现：对对象命名空间与
// 系统/用户元数据建二级索引，支持按属性检索。索引是旁挂派生数据，以 sidecar
// 元数据为唯一真相，可随时全量重建。
//
// 设计文档：docs/metadata-catalog.md、docs/catalog-storage-engine.md。
package catalog

import "time"

// Doc 是索引文档，与一个对象版本（sidecar 的 fsVersion）一一对应。
type Doc struct {
	Bucket       string            `json:"bucket"`
	Key          string            `json:"key"`
	VersionID    string            `json:"versionId"`
	IsLatest     bool              `json:"isLatest"`
	DeleteMarker bool              `json:"deleteMarker,omitempty"`
	Size         int64             `json:"size"`
	ETag         string            `json:"etag,omitempty"`
	ContentType  string            `json:"contentType,omitempty"`
	ModTime      time.Time         `json:"modTime"`
	UserMeta     map[string]string `json:"userMeta,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	Seq          uint64            `json:"seq,omitempty"` // 全局单调事件号，写路径分配；不参与主键
}

// Op 是索引事件的操作类型。
type Op uint8

const (
	// OpPut 写入一个版本（含删除标记：DeleteMarker=true 的 Doc）。
	OpPut Op = iota
	// OpDel 硬删除一个版本（无版本化/挂起态删除、DeleteBucket）。
	OpDel
)

// Event 是一次索引变更事件。
type Event struct {
	Op  Op
	Doc Doc
}

// encodeKeys 用 NUL 字节连接若干字段，每个字段后都带一个 NUL，便于前缀扫描。
// S3 的 bucket/key 不允许出现 NUL，因此 NUL 作为分隔符是安全的。
func encodeKeys(parts ...string) []byte {
	n := 0
	for _, p := range parts {
		n += len(p) + 1
	}
	buf := make([]byte, 0, n)
	for _, p := range parts {
		buf = append(buf, p...)
		buf = append(buf, 0)
	}
	return buf
}

// primaryKey 编码主键：bucket + NUL + key + NUL + versionID + NUL。
func primaryKey(bucket, key, versionID string) []byte {
	return encodeKeys(bucket, key, versionID)
}
