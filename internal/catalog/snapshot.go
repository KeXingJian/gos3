package catalog

import (
	"fmt"
	"os"
	"path/filepath"
)

// snapshotPath 返回指定 gen 的快照文件路径。
func snapshotPath(dir string, gen uint64) string {
	return filepath.Join(dir, fmt.Sprintf("snap-%d.sst", gen))
}

// writeSnapshotAtomic 把内存索引序列化为快照，原子写入（tmp + fsync + rename）。
// 返回写入的文档数与快照的 maxSeq（= 当时的 appliedSeq）。
func writeSnapshotAtomic(dir string, gen uint64, ix *memoryIndex) (docs int, maxSeq uint64, err error) {
	var buf []byte
	it := ix.primary.Seek(nil)
	for it.Valid() {
		buf = appendSnapRecord(buf, it.Doc().Seq, it.Doc())
		docs++
		it.Next()
	}
	maxSeq = ix.appliedSeq
	buf = appendSnapFooter(buf, uint64(docs), maxSeq)

	path := snapshotPath(dir, gen)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, 0, err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return 0, 0, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return 0, 0, err
	}
	if err := f.Close(); err != nil {
		return 0, 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return 0, 0, err
	}
	return docs, maxSeq, nil
}

// readSnapshot 读取快照文件，重建内存索引。返回索引与快照的 maxSeq。
func readSnapshot(path string, cfg IndexConfig) (*memoryIndex, uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	records, docs, maxSeq, err := verifySnapFooter(data)
	if err != nil {
		return nil, 0, err
	}
	ix := newMemoryIndex(cfg)
	var count uint64
	rest := records
	for len(rest) > 0 {
		_, doc, r, err := decodeSnapRecord(rest)
		if err != nil {
			return nil, 0, err
		}
		ix.load(doc)
		count++
		rest = r
	}
	if count != docs {
		return nil, 0, fmt.Errorf("snapshot: footer docs=%d but read %d records", docs, count)
	}
	ix.appliedSeq = maxSeq
	return ix, maxSeq, nil
}
