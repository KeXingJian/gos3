package catalog

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"hash/fnv"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultLimit 是查询未显式指定 limit 时的返回条数。
	DefaultLimit = 100
	// MaxLimit 是单次查询允许返回的最大条数，防止一次拉爆内存/响应。
	MaxLimit = 1000
)

// 查询层错误：API 层据此映射 400/409。
var (
	ErrInvalidFilter = errors.New("catalog: invalid filter")
	ErrInvalidToken  = errors.New("catalog: invalid continuation token")
	ErrRestartQuery  = errors.New("catalog: index generation changed; restart query")
)

// Filter 是元数据查询条件。零值表示「所有最新版本、不含删除标记」。
type Filter struct {
	Bucket      string
	Prefix      string
	ContentType string

	MinSize    int64
	MaxSize    int64
	MinSizeSet bool
	MaxSizeSet bool

	// After 是 ModTime 下界（含）；Before 是上界（不含）。零值表示不限。
	After  time.Time
	Before time.Time

	// Meta / Tag 是必须全部满足的等值条件；只有白名单 key 才能命中二级索引，
	// 非白名单 key 仍可作为内存过滤条件。
	Meta map[string]string
	Tag  map[string]string

	// AllVersions=false 时只返回 IsLatest 文档；IncludeDeleteMarkers=false
	// 时过滤删除标记（默认只看到「当前存在的对象」）。
	AllVersions          bool
	IncludeDeleteMarkers bool

	Limit int
	Token string
}

// SearchResult 是一次查询的结果页。
type SearchResult struct {
	Gen       uint64 `json:"gen"`
	Scan      string `json:"scan"` // index / bucket / full
	Scanned   int    `json:"scanned"`
	Truncated bool   `json:"truncated"`
	NextToken string `json:"nextToken,omitempty"`
	Docs      []Doc  `json:"docs"`
}

// StatsOptions 是聚合查询参数。
type StatsOptions struct {
	Bucket      string
	GroupBy     string // "" / bucket / contentType / sizeBucket
	AllVersions bool
}

// StatsGroup 是一组聚合结果。
type StatsGroup struct {
	Key       string `json:"key"`
	Count     int    `json:"count"`
	TotalSize int64  `json:"totalSize"`
}

// StatsResult 是聚合查询结果。
type StatsResult struct {
	Gen       uint64       `json:"gen"`
	Docs      int          `json:"docs"`
	TotalSize int64        `json:"totalSize"`
	GroupBy   string       `json:"groupBy,omitempty"`
	Groups    []StatsGroup `json:"groups,omitempty"`
}

// pageToken 是分页游标：带上 gen 与查询指纹，避免索引重建或换查询条件后
// 沿用旧 token 导致重复/遗漏。
type pageToken struct {
	Gen       uint64 `json:"g"`
	Query     string `json:"q"`
	Bucket    string `json:"b"`
	Key       string `json:"k"`
	VersionID string `json:"v"`
}

func encodePageToken(gen uint64, f Filter, d *Doc) string {
	b, _ := json.Marshal(pageToken{
		Gen:       gen,
		Query:     filterFingerprint(f),
		Bucket:    d.Bucket,
		Key:       d.Key,
		VersionID: d.VersionID,
	})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodePageToken(s string) (pageToken, error) {
	var t pageToken
	data, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return t, ErrInvalidToken
	}
	if err := json.Unmarshal(data, &t); err != nil {
		return t, ErrInvalidToken
	}
	if t.Query == "" || t.Bucket == "" || t.Key == "" || t.VersionID == "" {
		return t, ErrInvalidToken
	}
	return t, nil
}

// filterFingerprint 生成查询条件指纹（不含 limit/token）。
func filterFingerprint(f Filter) string {
	c := f
	c.Limit = 0
	c.Token = ""
	b, _ := json.Marshal(c)
	h := fnv.New64a()
	_, _ = h.Write(b)
	return strconv.FormatUint(h.Sum64(), 16)
}

func (f Filter) normalized() (Filter, error) {
	if f.MinSizeSet && f.MinSize < 0 {
		return f, ErrInvalidFilter
	}
	if f.MaxSizeSet && f.MaxSize < 0 {
		return f, ErrInvalidFilter
	}
	if f.MinSizeSet && f.MaxSizeSet && f.MinSize > f.MaxSize {
		return f, ErrInvalidFilter
	}
	switch {
	case f.Limit <= 0:
		f.Limit = DefaultLimit
	case f.Limit > MaxLimit:
		f.Limit = MaxLimit
	}
	return f, nil
}

// match 判断一条 Doc 是否满足全部过滤条件（索引候选集之外的内存过滤）。
func (f Filter) match(d *Doc) bool {
	if f.Bucket != "" && d.Bucket != f.Bucket {
		return false
	}
	if f.Prefix != "" && !strings.HasPrefix(d.Key, f.Prefix) {
		return false
	}
	if f.ContentType != "" && d.ContentType != f.ContentType {
		return false
	}
	if f.MinSizeSet && d.Size < f.MinSize {
		return false
	}
	if f.MaxSizeSet && d.Size > f.MaxSize {
		return false
	}
	if !f.After.IsZero() && d.ModTime.Before(f.After) {
		return false
	}
	if !f.Before.IsZero() && !d.ModTime.Before(f.Before) {
		return false
	}
	for k, v := range f.Meta {
		if d.UserMeta[k] != v {
			return false
		}
	}
	for k, v := range f.Tag {
		if d.Tags[k] != v {
			return false
		}
	}
	if !f.AllVersions && !d.IsLatest {
		return false
	}
	if !f.IncludeDeleteMarkers && d.DeleteMarker {
		return false
	}
	return true
}

// search 在内存索引上执行查询。调用方必须持有 Engine 的读锁。
func (ix *memoryIndex) search(f Filter, gen uint64) (SearchResult, error) {
	f, err := f.normalized()
	if err != nil {
		return SearchResult{}, err
	}
	var tok *pageToken
	if f.Token != "" {
		t, err := decodePageToken(f.Token)
		if err != nil {
			return SearchResult{}, err
		}
		if t.Gen != gen {
			return SearchResult{}, ErrRestartQuery
		}
		if t.Query != filterFingerprint(f) {
			return SearchResult{}, ErrInvalidToken
		}
		tok = &t
	}

	scan, iter := ix.candidates(f, tok)
	res := SearchResult{Gen: gen, Scan: scan, Docs: make([]Doc, 0, f.Limit)}
	var tokenPK []byte
	if tok != nil {
		tokenPK = primaryKey(tok.Bucket, tok.Key, tok.VersionID)
	}

	if scan == "bucket" || scan == "full" {
		// 主键本身有序：直接流式收集 limit+1 条，不需要排序/全量物化。
		iter(func(d *Doc) bool {
			res.Scanned++
			if tokenPK != nil && bytes.Compare(docPK(d), tokenPK) <= 0 {
				return true
			}
			if !f.match(d) {
				return true
			}
			res.Docs = append(res.Docs, cloneDoc(d))
			return len(res.Docs) <= f.Limit
		})
	} else {
		// 二级索引只用于裁剪候选集；最终统一按主键排序，保证分页稳定。
		matched := make([]queryDoc, 0)
		iter(func(d *Doc) bool {
			res.Scanned++
			if f.match(d) {
				matched = append(matched, queryDoc{pk: docPK(d), doc: d})
			}
			return true
		})
		sort.Slice(matched, func(i, j int) bool { return bytes.Compare(matched[i].pk, matched[j].pk) < 0 })
		start := 0
		if tokenPK != nil {
			start = sort.Search(len(matched), func(i int) bool {
				return bytes.Compare(matched[i].pk, tokenPK) > 0
			})
		}
		for i := start; i < len(matched) && len(res.Docs) <= f.Limit; i++ {
			res.Docs = append(res.Docs, cloneDoc(matched[i].doc))
		}
	}

	if len(res.Docs) > f.Limit {
		res.Truncated = true
		res.Docs = res.Docs[:f.Limit]
	}
	if res.Truncated && len(res.Docs) > 0 {
		res.NextToken = encodePageToken(gen, f, &res.Docs[len(res.Docs)-1])
	}
	return res, nil
}

type queryDoc struct {
	pk  []byte
	doc *Doc
}

// candidates 选择最 selective 的索引并返回候选迭代器与扫描方式。
// 返回的迭代器只在读锁内使用。
func (ix *memoryIndex) candidates(f Filter, tok *pageToken) (string, func(func(*Doc) bool)) {
	if f.Bucket != "" {
		// 1) user-meta 等值（仅白名单 key 有索引）
		for _, k := range sortedKeys(f.Meta) {
			if ix.cfg.allowMeta(k) {
				return "index", scanPrefix(ix.metaIx, encodeKeys(f.Bucket, k, f.Meta[k]))
			}
		}
		// 2) tag 等值（仅白名单 key 有索引）
		for _, k := range sortedKeys(f.Tag) {
			if ix.cfg.allowTag(k) {
				return "index", scanPrefix(ix.tagIx, encodeKeys(f.Bucket, k, f.Tag[k]))
			}
		}
		// 3) contentType
		if f.ContentType != "" {
			return "index", scanPrefix(ix.ct, encodeKeys(f.Bucket, f.ContentType))
		}
		// 4) 时间范围（time 索引按时间有序，可提前结束）
		if (!f.After.IsZero() || !f.Before.IsZero()) &&
			(f.After.IsZero() || f.After.UnixNano() >= 0) {
			prefix := encodeKeys(f.Bucket)
			start := prefix
			if !f.After.IsZero() {
				start = encodeKeys(f.Bucket, timeKey(f.After.UnixNano()))
			}
			before := f.Before
			return "index", func(yield func(*Doc) bool) {
				for x := ix.timeIx.Seek(start); x.Valid() && bytes.HasPrefix(x.Key(), prefix); x.Next() {
					d := x.Doc()
					if !before.IsZero() && !d.ModTime.Before(before) {
						return
					}
					if !yield(d) {
						return
					}
				}
			}
		}
		// 5) 大小分档（仅当没有更 selective 的 prefix 时；分档粗，仍需内存精确过滤）
		if f.Prefix == "" && (f.MinSizeSet || f.MaxSizeSet) {
			from, to := "0", "4"
			if f.MinSizeSet {
				from = sizeBucket(f.MinSize)
			}
			if f.MaxSizeSet {
				to = sizeBucket(f.MaxSize)
			}
			return "index", func(yield func(*Doc) bool) {
				for b := from[0]; b <= to[0] && b <= '4'; b++ {
					prefix := encodeKeys(f.Bucket, string([]byte{b}))
					for x := ix.sizeIx.Seek(prefix); x.Valid() && bytes.HasPrefix(x.Key(), prefix); x.Next() {
						if !yield(x.Doc()) {
							return
						}
					}
				}
			}
		}
		// 6) 主键前缀（bucket + object prefix）
		prefix := keyPrefix(f.Bucket, f.Prefix)
		start := prefix
		if tok != nil {
			tp := primaryKey(tok.Bucket, tok.Key, tok.VersionID)
			if bytes.Compare(tp, start) > 0 {
				start = tp
			}
		}
		return "bucket", scanPrimary(ix.primary, start, prefix)
	}

	// 无 bucket：只能全索引扫描（按主键有序）。
	var start []byte
	if tok != nil {
		start = primaryKey(tok.Bucket, tok.Key, tok.VersionID)
	}
	return "full", scanPrimary(ix.primary, start, nil)
}

func scanPrefix(m *omap, prefix []byte) func(func(*Doc) bool) {
	return func(yield func(*Doc) bool) {
		for x := m.Seek(prefix); x.Valid() && bytes.HasPrefix(x.Key(), prefix); x.Next() {
			if !yield(x.Doc()) {
				return
			}
		}
	}
}

func scanPrimary(m *omap, start, prefix []byte) func(func(*Doc) bool) {
	return func(yield func(*Doc) bool) {
		for x := m.Seek(start); x.Valid(); x.Next() {
			if prefix != nil && !bytes.HasPrefix(x.Key(), prefix) {
				return
			}
			if !yield(x.Doc()) {
				return
			}
		}
	}
}

func keyPrefix(bucket, prefix string) []byte {
	b := make([]byte, 0, len(bucket)+len(prefix)+1)
	b = append(b, bucket...)
	b = append(b, 0)
	b = append(b, prefix...)
	return b
}

func docPK(d *Doc) []byte { return primaryKey(d.Bucket, d.Key, d.VersionID) }

func cloneDoc(d *Doc) Doc {
	c := *d
	c.UserMeta = maps.Clone(d.UserMeta)
	c.Tags = maps.Clone(d.Tags)
	return c
}

func sortedKeys(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// stats 在内存索引上执行聚合。调用方必须持有 Engine 的读锁。
func (ix *memoryIndex) stats(opts StatsOptions, gen uint64) (StatsResult, error) {
	switch strings.ToLower(opts.GroupBy) {
	case "":
		opts.GroupBy = ""
	case "bucket":
		opts.GroupBy = "bucket"
	case "contenttype":
		opts.GroupBy = "contentType"
	case "sizebucket":
		opts.GroupBy = "sizeBucket"
	default:
		return StatsResult{}, ErrInvalidFilter
	}

	var prefix []byte
	if opts.Bucket != "" {
		prefix = encodeKeys(opts.Bucket)
	}
	res := StatsResult{Gen: gen, GroupBy: opts.GroupBy}
	groups := make(map[string]*StatsGroup)
	for x := ix.primary.Seek(prefix); x.Valid(); x.Next() {
		if prefix != nil && !bytes.HasPrefix(x.Key(), prefix) {
			break
		}
		d := x.Doc()
		if !opts.AllVersions && !d.IsLatest {
			continue
		}
		if d.DeleteMarker {
			continue
		}
		res.Docs++
		res.TotalSize += d.Size

		var key string
		switch opts.GroupBy {
		case "":
			continue
		case "bucket":
			key = d.Bucket
		case "contentType":
			key = d.ContentType
		case "sizeBucket":
			key = sizeBucket(d.Size)
		}
		g := groups[key]
		if g == nil {
			g = &StatsGroup{Key: key}
			groups[key] = g
		}
		g.Count++
		g.TotalSize += d.Size
	}
	for _, g := range groups {
		res.Groups = append(res.Groups, *g)
	}
	sort.Slice(res.Groups, func(i, j int) bool { return res.Groups[i].Key < res.Groups[j].Key })
	return res, nil
}
