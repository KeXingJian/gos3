package catalog

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkOmapUpsert 测量 skip list 单点写入吞吐。
func BenchmarkOmapUpsert(b *testing.B) {
	m := newOmap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Upsert(primaryKey("bkt", fmt.Sprintf("k%d", i), "v"), &Doc{Size: 1})
	}
}

// BenchmarkMemoryIndexApply 测量内存索引 apply（主键 + 4 个二级索引）吞吐。
func BenchmarkMemoryIndexApply(b *testing.B) {
	ix := newMemoryIndex(IndexConfig{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := Doc{Bucket: "bkt", Key: fmt.Sprintf("k%d", i), VersionID: "v", Size: int64(i), ContentType: "image/jpeg", ModTime: time.Now().UTC(), IsLatest: true, Seq: uint64(i + 1)}
		ix.apply(Event{Op: OpPut, Doc: d})
	}
}

// BenchmarkEngineAppend 测量引擎端到端写吞吐（WAL 组提交 + 索引 apply）。
// 用大 SyncEvery 避免 fsync 主导，测量内存 + 缓冲写入上限。
func BenchmarkEngineAppend(b *testing.B) {
	dir := b.TempDir()
	cfg := Config{Dir: dir, Gen: 0, Wal: WalConfig{Dir: dir, SyncEvery: 1 << 30, SyncTimeout: time.Hour}}
	e, err := NewEngine(cfg, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := Doc{Bucket: "bkt", Key: fmt.Sprintf("k%d", i), VersionID: "v", Size: int64(i), ModTime: time.Now().UTC()}
		if err := e.Append(Event{Op: OpPut, Doc: d}); err != nil {
			b.Fatal(err)
		}
	}
}
