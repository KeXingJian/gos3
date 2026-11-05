package catalog

import (
	"bytes"
	"fmt"
)

// IndexConfig 是内存索引的建索引策略。
type IndexConfig struct {
	// MetaWhitelist 是允许建二级索引的 user-meta key 集合；空 = 不建 meta 索引。
	MetaWhitelist map[string]struct{}
	// TagWhitelist 是允许建二级索引的 tag key 集合；空 = 不建 tag 索引。
	TagWhitelist map[string]struct{}
}

func (c *IndexConfig) allowMeta(k string) bool {
	if len(c.MetaWhitelist) == 0 {
		return false
	}
	_, ok := c.MetaWhitelist[k]
	return ok
}

func (c *IndexConfig) allowTag(k string) bool {
	if len(c.TagWhitelist) == 0 {
		return false
	}
	_, ok := c.TagWhitelist[k]
	return ok
}

// memoryIndex 是 Catalog 的内存索引：主键 + 5 个二级索引，全部由 omap 实现。
// 非并发安全：apply 由单 applier 串行调用（分片内串行）。
type memoryIndex struct {
	primary *omap // PK：bucket+NUL+key+NUL+versionID
	ct      *omap // bucket+NUL+contentType+NUL+key+NUL+versionID
	timeIx  *omap // bucket+NUL+modTimeNano+NUL+key+NUL+versionID
	sizeIx  *omap // bucket+NUL+sizeBucket+NUL+key+NUL+versionID
	tagIx   *omap // 多值：bucket+NUL+k+NUL+v+NUL+key+NUL+versionID
	metaIx  *omap // 多值：同 tagIx，白名单

	cfg IndexConfig

	// appliedSeq 是本分片已 apply 的最大事件号，用于幂等。
	// 事件 seq 从 1 开始（sequencer 的 seq=seq++，首个为 1）。
	appliedSeq uint64
}

func newMemoryIndex(cfg IndexConfig) *memoryIndex {
	return &memoryIndex{
		primary: newOmap(),
		ct:      newOmap(),
		timeIx:  newOmap(),
		sizeIx:  newOmap(),
		tagIx:   newOmap(),
		metaIx:  newOmap(),
		cfg:     cfg,
	}
}

// apply 应用一个事件，按 seq 幂等（重放时 seq 更小的直接跳过）。
func (ix *memoryIndex) apply(ev Event) {
	d := &ev.Doc
	if d.Seq <= ix.appliedSeq {
		return
	}
	ix.appliedSeq = d.Seq

	pk := primaryKey(d.Bucket, d.Key, d.VersionID)
	switch ev.Op {
	case OpPut:
		if d.IsLatest {
			ix.clearLatest(d.Bucket, d.Key, d.VersionID)
		}
		if old, existed := ix.primary.Upsert(pk, d); existed {
			ix.removeSecondary(old)
		}
		ix.addSecondary(d)
	case OpDel:
		if old, existed := ix.primary.Delete(pk); existed {
			ix.removeSecondary(old)
		}
	}
}

// clearLatest 把该 key 下除 exceptVersionID 外所有版本的 IsLatest 清掉。
// IsLatest 不参与任何二级索引键，直接原地改即可。
func (ix *memoryIndex) clearLatest(bucket, key, exceptVersionID string) {
	prefix := encodeKeys(bucket, key)
	it := ix.primary.Seek(prefix)
	for it.Valid() && bytes.HasPrefix(it.Key(), prefix) {
		d := it.Doc()
		if d.VersionID != exceptVersionID {
			d.IsLatest = false
		}
		it.Next()
	}
}

// addSecondary 按当前 Doc 的内容插入所有二级索引条目。
func (ix *memoryIndex) addSecondary(d *Doc) {
	ix.ct.Upsert(ctKey(d), d)
	ix.timeIx.Upsert(timeIndexKey(d), d)
	ix.sizeIx.Upsert(sizeIndexKey(d), d)
	for k, v := range d.Tags {
		if ix.cfg.allowTag(k) {
			ix.tagIx.Upsert(tagMetaKey(d, k, v), d)
		}
	}
	for k, v := range d.UserMeta {
		if ix.cfg.allowMeta(k) {
			ix.metaIx.Upsert(tagMetaKey(d, k, v), d)
		}
	}
}

// removeSecondary 按旧 Doc 的内容摘除所有二级索引条目。
// 必须用「当初插入时的同一个 Doc」调用，否则键对不上。
func (ix *memoryIndex) removeSecondary(d *Doc) {
	ix.ct.Delete(ctKey(d))
	ix.timeIx.Delete(timeIndexKey(d))
	ix.sizeIx.Delete(sizeIndexKey(d))
	for k, v := range d.Tags {
		if ix.cfg.allowTag(k) {
			ix.tagIx.Delete(tagMetaKey(d, k, v))
		}
	}
	for k, v := range d.UserMeta {
		if ix.cfg.allowMeta(k) {
			ix.metaIx.Delete(tagMetaKey(d, k, v))
		}
	}
}

// Len 返回主键索引里的文档数（= 索引中的对象版本数）。
func (ix *memoryIndex) Len() int { return ix.primary.Len() }

// load 直接把一个 Doc 装入索引（快照加载用）。快照是最终一致状态，
// 不做幂等判断，也不 clearLatest（IsLatest 已在快照里正确）。
func (ix *memoryIndex) load(d *Doc) {
	ix.primary.Upsert(primaryKey(d.Bucket, d.Key, d.VersionID), d)
	ix.addSecondary(d)
}

// ---- 二级索引键编码 ----

func ctKey(d *Doc) []byte {
	return encodeKeys(d.Bucket, d.ContentType, d.Key, d.VersionID)
}

func timeIndexKey(d *Doc) []byte {
	return encodeKeys(d.Bucket, timeKey(d.ModTime.UnixNano()), d.Key, d.VersionID)
}

func sizeIndexKey(d *Doc) []byte {
	return encodeKeys(d.Bucket, sizeBucket(d.Size), d.Key, d.VersionID)
}

func tagMetaKey(d *Doc, k, v string) []byte {
	return encodeKeys(d.Bucket, k, v, d.Key, d.VersionID)
}

// sizeBucket 把 size 归到固定分档（基数控制：避免每个不同字节数都写一条索引）。
// 返回单字符 "0".."4"，保证字节序 = 分档序。
func sizeBucket(size int64) string {
	switch {
	case size < 1<<10:
		return "0"
	case size < 1<<20:
		return "1"
	case size < 1<<26:
		return "2"
	case size < 1<<30:
		return "3"
	default:
		return "4"
	}
}

// timeKey 把 UnixNano 时间戳编码成定宽 19 位十进制字符串，保证字节序 = 时间序。
// 仅适用于非负时间戳（ModTime 恒为当前时间，必然非负）。
func timeKey(nano int64) string {
	return fmt.Sprintf("%019d", nano)
}
