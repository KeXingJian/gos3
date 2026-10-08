package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/kxj/gos3/internal/disk"
)

// ReadQuorum 返回读法定人数：只要能凑出 dataShards 份分片/元数据即可判定权威值。
func ReadQuorum(dataShards int) int {
	return dataShards
}

// WriteQuorum 返回写法定人数：通常是 dataShards，但当 data == parity 时需要 +1，
// 否则「2 数据 + 2 校验」这类布局在只剩一半盘时仍能写入，容错语义会退化成 quorum 之外。
func WriteQuorum(dataShards, parityShards int) int {
	if dataShards == parityShards {
		return dataShards + 1
	}
	return dataShards
}

func (e *Erasure) readQuorum() int {
	return ReadQuorum(e.dataShards)
}

func (e *Erasure) writeQuorum() int {
	return WriteQuorum(e.dataShards, e.parityShards)
}

// reduceErrs 从一组错误中挑出「出现次数最多」的那一个（对齐 MinIO 的 reduceErrs 思路）：
// 多数盘报同一个错时，这个错才是集群层面的真实原因，而不是个别盘的偶发错误。
func reduceErrs(errs []error) error {
	counts := map[string]int{}
	byMsg := map[string]error{}
	for _, err := range errs {
		if err == nil {
			continue
		}
		counts[err.Error()]++
		if _, ok := byMsg[err.Error()]; !ok {
			byMsg[err.Error()] = err
		}
	}
	best, bestCount := error(nil), 0
	for msg, c := range counts {
		if c > bestCount {
			best, bestCount = byMsg[msg], c
		}
	}
	return best
}

// metaSig 是「对象最新版本」的签名：读路径靠它判断各盘的元数据是否指向同一个版本。
type metaSig struct {
	versionID string
	modUnix   int64
	marker    bool
}

// sigOf 取元数据中最新版本的签名；无版本时返回 ok=false。
func sigOf(meta fsMeta) (metaSig, bool) {
	if len(meta.Versions) == 0 {
		return metaSig{}, false
	}
	latest := meta.Versions[0]
	return metaSig{versionID: latest.VersionID, modUnix: latest.ModTime.UnixNano(), marker: latest.DeleteMarker}, true
}

// pickQuorumMeta 从各盘读到的同一对象元数据中选出「权威版本」：
//   - 按最新版本签名分组，只有出现次数达到 quorum 的签名才是候选（避免单盘脏数据被读到）；
//   - 候选里取 modTime 最新的那个（覆盖写只有在多数盘都更新后才算生效）；
//   - 没有任何签名达到 quorum 时返回 false，由调用方按「对象不存在/读法定人数不足」处理。
func pickQuorumMeta(metas []fsMeta, quorum int) (fsMeta, bool) {
	type group struct {
		meta  fsMeta
		count int
	}
	groups := map[metaSig]*group{}
	for _, m := range metas {
		sig, ok := sigOf(m)
		if !ok {
			continue
		}
		g := groups[sig]
		if g == nil {
			g = &group{meta: m}
			groups[sig] = g
		}
		g.count++
	}
	var best *group
	for _, g := range groups {
		if g.count < quorum {
			continue
		}
		if best == nil {
			best = g
			continue
		}
		if sigTime(g.meta).After(sigTime(best.meta)) {
			best = g
		}
	}
	if best == nil {
		return fsMeta{}, false
	}
	return best.meta, true
}

// sigTime 返回元数据中最新版本的修改时间。
func sigTime(meta fsMeta) time.Time {
	if len(meta.Versions) == 0 {
		return time.Time{}
	}
	return meta.Versions[0].ModTime
}

// partSig 是 part 元数据的签名：part 号 + 大小 + ETag + DataID。
func partSig(m fsPartMeta) string {
	return fmt.Sprintf("%d|%d|%s|%s", m.PartNumber, m.Size, m.ETag, m.DataID)
}

// pickQuorumPartMeta 从各盘读到的同一 part 元数据中选出达到 quorum 的那一份。
// 只有一块盘上有、或者内容互相矛盾的 part 不会被采用（同样的 quorum 思路用于对象元数据）。
func pickQuorumPartMeta(metas []fsPartMeta, quorum int) (fsPartMeta, bool) {
	counts := map[string]int{}
	bySig := map[string]fsPartMeta{}
	for _, m := range metas {
		sig := partSig(m)
		counts[sig]++
		if _, ok := bySig[sig]; !ok {
			bySig[sig] = m
		}
	}
	best, bestCount := fsPartMeta{}, 0
	for sig, c := range counts {
		if c > bestCount {
			best, bestCount = bySig[sig], c
		}
	}
	if bestCount < quorum {
		return fsPartMeta{}, false
	}
	return best, true
}

// readMetas 并行读取所有盘上的同一对象元数据。
// 返回值与 e.disks 一一对应：err 为 ErrObjectNotFound 表示该盘确认没有这个对象。
func (e *Erasure) readMetas(ctx context.Context, bucket, object string) []fsMetaResult {
	results := make([]fsMetaResult, len(e.disks))
	var wg sync.WaitGroup
	for i, d := range e.disks {
		wg.Add(1)
		go func(i int, d disk.Disk) {
			defer wg.Done()
			meta, err := diskReadMeta(ctx, d, bucket, object)
			results[i] = fsMetaResult{meta: meta, err: err, disk: d.ID()}
		}(i, d)
	}
	wg.Wait()
	return results
}

// fsMetaResult 是单块盘上对象元数据的读取结果。
type fsMetaResult struct {
	meta fsMeta
	err  error
	disk string
}

// readMetaQuorum 读取对象的权威元数据：
//   - 达到读法定人数的版本签名 → 返回该元数据；
//   - 未达读法定人数，但「确认不存在」的盘达到写法定人数 → 判定对象不存在
//     （删除只有在多数盘上生效才算数）；
//   - 否则返回 ErrReadQuorum，绝不返回单盘上的可疑值。
func (e *Erasure) readMetaQuorum(ctx context.Context, bucket, object string) (fsMeta, error) {
	results := e.readMetas(ctx, bucket, object)
	var metas []fsMeta
	absent := 0
	var errs []error
	for _, r := range results {
		switch {
		case r.err == nil:
			metas = append(metas, r.meta)
		case errors.Is(r.err, ErrObjectNotFound):
			absent++
		default:
			errs = append(errs, r.err)
			e.log.Warn("[gos3: read-meta-failed]", "disk", r.disk, "bucket", bucket, "object", object, "error", r.err.Error())
		}
	}
	if meta, ok := pickQuorumMeta(metas, e.readQuorum()); ok {
		return meta, nil
	}
	if absent >= e.writeQuorum() {
		return fsMeta{}, ErrObjectNotFound
	}
	if len(errs) > 0 {
		return fsMeta{}, fmt.Errorf("%w: %s/%s (%v)", ErrReadQuorum, bucket, object, reduceErrs(errs))
	}
	return fsMeta{}, fmt.Errorf("%w: %s/%s (metas=%d absent=%d)", ErrReadQuorum, bucket, object, len(metas), absent)
}

// readMetasAll 并行遍历所有盘上的 .meta/<bucket>，返回 key -> 各盘元数据。
// 返回成功列出目录的盘数，用于判断列举本身是否达到读法定人数。
func (e *Erasure) readMetasAll(ctx context.Context, bucket string) (map[string][]fsMeta, int, error) {
	type walkResult struct {
		entries []objectMetaEntry
		err     error
		disk    string
	}
	results := make([]walkResult, len(e.disks))
	var wg sync.WaitGroup
	for i, d := range e.disks {
		wg.Add(1)
		go func(i int, d disk.Disk) {
			defer wg.Done()
			entries, err := diskWalkMetas(ctx, d, bucket)
			results[i] = walkResult{entries: entries, err: err, disk: d.ID()}
		}(i, d)
	}
	wg.Wait()

	byKey := map[string][]fsMeta{}
	listed, lastErr := 0, error(nil)
	for _, r := range results {
		if r.err != nil {
			lastErr = r.err
			e.log.Warn("[gos3: walk-metas-failed]", "disk", r.disk, "bucket", bucket, "error", r.err.Error())
			continue
		}
		listed++
		for _, entry := range r.entries {
			byKey[entry.key] = append(byKey[entry.key], entry.meta)
		}
	}
	if listed < e.readQuorum() {
		return nil, listed, fmt.Errorf("%w: list %s (listable drives %d/%d): %v",
			ErrReadQuorum, bucket, listed, e.readQuorum(), lastErr)
	}
	return byKey, listed, nil
}

// walkMetasQuorum 列出 bucket 下的权威元数据：先汇总各盘元数据，再逐 key 做 quorum 归约。
// 只有达到读法定人数的对象才会出现在结果里，因此「只写成功一部分盘」的中间状态不会被列出。
func (e *Erasure) walkMetasQuorum(ctx context.Context, bucket string) ([]objectMetaEntry, error) {
	byKey, _, err := e.readMetasAll(ctx, bucket)
	if err != nil {
		return nil, err
	}
	out := make([]objectMetaEntry, 0, len(byKey))
	for key, metas := range byKey {
		meta, ok := pickQuorumMeta(metas, e.readQuorum())
		if !ok {
			e.log.Info("[gos3: list-skip-unquorum]", "bucket", bucket, "object", key, "metas", len(metas))
			continue
		}
		out = append(out, objectMetaEntry{key: key, meta: meta})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out, nil
}
