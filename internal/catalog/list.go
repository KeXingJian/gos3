package catalog

import (
	"bytes"
	"strings"

	"github.com/kxj/gos3/internal/store"
)

// List 从内存 LSM 索引列出 bucket 下指定前缀的对象（含 delimiter 分组与 marker 分页）。
// 语义对齐 store.ListObjects，但数据来自内存索引而非暴力遍历 sidecar 元数据文件，
// 供管理端 bucket 浏览页使用，避免每次列目录都全量扫描磁盘。
func (e *Engine) List(bucket string, opts store.ListOptions) store.ListObjectsResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.ix.list(bucket, opts)
}

// list 在主键索引上做前缀扫描。调用方必须持有 Engine 读锁。
func (ix *memoryIndex) list(bucket string, opts store.ListOptions) store.ListObjectsResult {
	max := opts.MaxKeys
	if max <= 0 || max > 1000 {
		max = 1000
	}
	prefix := keyPrefix(bucket, opts.Prefix)

	type entry struct {
		sortKey  string
		info     store.ObjectInfo
		prefix   string
		isPrefix bool
	}
	var entries []entry
	seen := map[string]struct{}{}

	// 主键索引按 key 有序，这里只触碰 bucket+prefix 范围内的文档。
	for x := ix.primary.Seek(prefix); x.Valid(); x.Next() {
		if !bytes.HasPrefix(x.Key(), prefix) {
			break
		}
		d := x.Doc()
		if !d.IsLatest || d.DeleteMarker {
			continue
		}
		if opts.Delimiter == "" {
			entries = append(entries, entry{sortKey: d.Key, info: docToObjectInfo(d)})
			continue
		}
		rest := d.Key[len(opts.Prefix):]
		if idx := strings.Index(rest, opts.Delimiter); idx < 0 {
			entries = append(entries, entry{sortKey: d.Key, info: docToObjectInfo(d)})
			continue
		} else {
			cp := opts.Prefix + rest[:idx+len(opts.Delimiter)]
			if _, ok := seen[cp]; ok {
				continue
			}
			seen[cp] = struct{}{}
			entries = append(entries, entry{sortKey: cp, prefix: cp, isPrefix: true})
		}
	}

	// 主键有序扫描产出的 entries 已按 sortKey 升序（对象 key 与派生 common prefix
	// 都随扫描单调不减），无需二次排序。
	var result store.ListObjectsResult
	lastMarker := ""
	for _, en := range entries {
		if opts.Marker != "" && en.sortKey <= opts.Marker {
			continue
		}
		if len(result.Objects)+len(result.CommonPrefixes) >= max {
			result.IsTruncated = true
			result.NextMarker = lastMarker
			break
		}
		if en.isPrefix {
			result.CommonPrefixes = append(result.CommonPrefixes, en.prefix)
		} else {
			result.Objects = append(result.Objects, en.info)
		}
		lastMarker = en.sortKey
	}
	return result
}

// docToObjectInfo 把索引文档转成 store.ObjectInfo。
func docToObjectInfo(d *Doc) store.ObjectInfo {
	return store.ObjectInfo{
		Bucket: d.Bucket, Name: d.Key, VersionID: d.VersionID,
		DeleteMarker: d.DeleteMarker, Size: d.Size, ETag: d.ETag,
		ContentType: d.ContentType, UserMetadata: d.UserMeta, Tags: d.Tags,
		ModTime: d.ModTime,
	}
}
