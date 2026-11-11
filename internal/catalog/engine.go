package catalog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Config 是 Catalog 引擎的配置。
type Config struct {
	Dir      string      // 目录（本地，节点私有）
	Gen      uint64      // 初始 gen（无 manifest 时使用）
	Wal      WalConfig   // WAL 持久化策略（Dir 字段忽略，用 Config.Dir）
	Index    IndexConfig // 建索引策略
	SnapDocs int         // 累计 WAL 记录数达到则触发快照（<=0 不自动快照）
}

// Status 是引擎/装饰器的运行状态摘要。
type Status struct {
	Docs       int    `json:"docs"`
	AppliedSeq uint64 `json:"appliedSeq"`
	Gen        uint64 `json:"gen"`
	Dropped    uint64 `json:"dropped"`
	Queued     int    `json:"queued"`
	Capacity   int    `json:"capacity"`
	State      string `json:"state"`
}

// RebuildFunc 是全量重建的数据源：从 sidecar 元数据逐个 emit 事件。
type RebuildFunc func(emit func(Event) error) error

// Engine 是元数据存储引擎：内存索引 + WAL + 快照 + manifest 的编排。
// 写路径为同步（seq 分配 + WAL 追加 + 索引 apply）；非阻塞投递由上层装饰器负责。
type Engine struct {
	dir     string
	cfg     Config
	wal     *Wal
	ix      *memoryIndex
	gen     uint64
	seq     uint64 // 下一个要分配的 seq（= 已 apply 的最大 seq）
	rebuild RebuildFunc
	walDocs int // 自上次快照以来的 WAL 记录数

	// mu 保护 ix/gen/seq/wal：写路径（Append/Rebuild）拿写锁，查询/Status 拿读锁。
	mu sync.RWMutex
	// syncLoop 负责把「低写入速率下未达到 SyncEvery 的尾部记录」按 SyncTimeout
	// 主动 fsync，避免它们无限期留在缓冲区里。
	stopSync  chan struct{}
	syncDone  chan struct{}
	syncEvery time.Duration
}

// NewEngine 创建或恢复引擎。rebuild 提供全量重建数据源（可能为 nil，
// 此时若需重建会报错）。
func NewEngine(cfg Config, rebuild RebuildFunc) (*Engine, error) {
	if cfg.Dir == "" {
		return nil, errors.New("catalog: dir required")
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	e := &Engine{
		dir:       cfg.Dir,
		cfg:       cfg,
		gen:       cfg.Gen,
		rebuild:   rebuild,
		syncEvery: cfg.Wal.withDefaults().SyncTimeout,
		stopSync:  make(chan struct{}),
		syncDone:  make(chan struct{}),
	}
	if err := e.recover(); err != nil {
		return nil, err
	}
	go e.syncLoop()
	return e, nil
}

// syncLoop 周期性把 WAL 缓冲刷到磁盘。Append 只在写请求发生时检查超时，
// 若写入流突然停止，尾部记录会一直留在 bufio 缓冲里；这里补上定时兜底。
func (e *Engine) syncLoop() {
	defer close(e.syncDone)
	t := time.NewTicker(e.syncEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			e.mu.Lock()
			if e.wal != nil && e.wal.Pending() > 0 {
				_ = e.wal.Sync()
			}
			e.mu.Unlock()
		case <-e.stopSync:
			return
		}
	}
}

// recover 启动恢复：manifest -> 快照 -> WAL 重放。任何不一致走全量重建。
func (e *Engine) recover() error {
	m, err := readManifest(e.dir)
	if err != nil {
		return e.fullRebuild()
	}
	if m == nil {
		// 无 manifest：全新启动。有 rebuild 源则从 sidecar 重建，
		// 覆盖「索引丢失但数据还在」的场景（空 store 重建出 0 条，无副作用）。
		if e.rebuild != nil {
			return e.fullRebuild()
		}
		wal, err := NewWal(e.dir, e.gen, e.cfg.Wal)
		if err != nil {
			return err
		}
		e.wal = wal
		e.ix = newMemoryIndex(e.cfg.Index)
		if err := e.replayWAL(0); err != nil {
			return e.fullRebuild()
		}
		e.seq = e.ix.appliedSeq
		return nil
	}

	// 有 manifest：正常恢复（快照 + WAL 重放）
	e.gen = m.Gen
	wal, err := NewWal(e.dir, e.gen, e.cfg.Wal)
	if err != nil {
		return err
	}
	e.wal = wal

	var appliedSeq uint64
	if m.SnapshotFile != "" {
		ix, snapMaxSeq, err := readSnapshot(filepath.Join(e.dir, m.SnapshotFile), e.cfg.Index)
		if err != nil {
			return e.fullRebuild()
		}
		e.ix = ix
		appliedSeq = snapMaxSeq
	} else {
		e.ix = newMemoryIndex(e.cfg.Index)
	}
	if err := e.replayWAL(appliedSeq); err != nil {
		return e.fullRebuild()
	}
	e.seq = e.ix.appliedSeq
	return nil
}

// replayWAL 以 appliedSeq 为起点重放 WAL，检测断号；seq <= appliedSeq 的记录跳过。
func (e *Engine) replayWAL(appliedSeq uint64) error {
	expected := appliedSeq + 1
	return e.wal.Replay(func(seq uint64, op Op, doc *Doc) error {
		doc.Seq = seq
		if seq <= appliedSeq {
			return nil // 快照已包含（截断前残留的旧段）
		}
		if seq != expected {
			return fmt.Errorf("catalog: wal gap at seq %d, want %d", seq, expected)
		}
		expected++
		e.ix.apply(Event{Op: op, Doc: *doc})
		return nil
	})
}

// fullRebuild 以 sidecar 为唯一真相全量重建：新 gen 快照 + 清旧段 + 换新 WAL。
func (e *Engine) fullRebuild() error {
	if e.rebuild == nil {
		return errors.New("catalog: full rebuild required but no rebuild source")
	}
	newGen := e.gen + 1

	ix := newMemoryIndex(e.cfg.Index)
	var maxSeq uint64
	if err := e.rebuild(func(ev Event) error {
		maxSeq++
		d := ev.Doc
		d.Seq = maxSeq
		ix.load(&d)
		return nil
	}); err != nil {
		return err
	}
	ix.appliedSeq = maxSeq

	if e.wal != nil {
		if err := e.wal.Close(); err != nil {
			return err
		}
	}
	if err := removeSegmentFiles(e.dir, e.gen); err != nil {
		return err
	}

	if _, _, err := writeSnapshotAtomic(e.dir, newGen, ix); err != nil {
		return err
	}
	if err := writeManifest(e.dir, &manifest{
		Gen:            newGen,
		AppliedSeq:     maxSeq,
		SnapshotFile:   fmt.Sprintf("snap-%d.sst", newGen),
		SnapshotDocs:   ix.Len(),
		SnapshotMaxSeq: maxSeq,
	}); err != nil {
		return err
	}

	wal, err := NewWal(e.dir, newGen, e.cfg.Wal)
	if err != nil {
		return err
	}
	e.wal = wal
	e.ix = ix
	e.gen = newGen
	e.seq = maxSeq
	e.walDocs = 0
	return nil
}

// Append 分配 seq、写 WAL、更新索引（同步）。
func (e *Engine) Append(ev Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.appendLocked(ev)
}

func (e *Engine) appendLocked(ev Event) error {
	e.seq++
	ev.Doc.Seq = e.seq
	if err := e.wal.Append(e.seq, ev); err != nil {
		return err
	}
	e.ix.apply(ev)
	e.walDocs++
	if e.cfg.SnapDocs > 0 && e.walDocs >= e.cfg.SnapDocs {
		return e.snapshotLocked()
	}
	return nil
}

// Snapshot 把当前索引落快照、截断 WAL、更新 manifest。
func (e *Engine) Snapshot() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshotLocked()
}

func (e *Engine) snapshotLocked() error {
	// 先封口当前段，让所有已写记录进入 sealed，才能被截断回收
	if err := e.wal.Seal(); err != nil {
		return err
	}
	docs, maxSeq, err := writeSnapshotAtomic(e.dir, e.gen, e.ix)
	if err != nil {
		return err
	}
	if err := e.wal.Truncate(maxSeq); err != nil {
		return err
	}
	if err := writeManifest(e.dir, &manifest{
		Gen:            e.gen,
		AppliedSeq:     e.ix.appliedSeq,
		SnapshotFile:   fmt.Sprintf("snap-%d.sst", e.gen),
		SnapshotDocs:   docs,
		SnapshotMaxSeq: maxSeq,
	}); err != nil {
		return err
	}
	e.walDocs = 0
	return nil
}

// Close 停止定时 fsync，封口 WAL。
func (e *Engine) Close() error {
	close(e.stopSync)
	<-e.syncDone
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.wal.Close()
}

// Rebuild 以 sidecar 为唯一真相重建全量索引（管理端 reindex 使用）。
// 重建期间持有写锁，写事件会在队列里排队；重建完成后新 gen 生效，旧分页 token 失效。
func (e *Engine) Rebuild() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.fullRebuild()
}

// View 返回内存索引。仅供同包测试/诊断使用：返回后引擎仍可能并发写，
// 调用方不能在无锁状态下访问；线上查询请走 Search/Stats。
func (e *Engine) View() *memoryIndex { return e.ix }

// Status 返回运行状态摘要。
func (e *Engine) Status() Status {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return Status{Docs: e.ix.Len(), AppliedSeq: e.ix.appliedSeq, Gen: e.gen, State: "ready"}
}

// Search 在只读锁内执行一次元数据查询。
func (e *Engine) Search(f Filter) (SearchResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.ix.search(f, e.gen)
}

// Stats 在只读锁内执行一次聚合查询。
func (e *Engine) Stats(opts StatsOptions) (StatsResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.ix.stats(opts, e.gen)
}

// removeSegmentFiles 删除指定 gen 的全部 WAL 段文件。
func removeSegmentFiles(dir string, gen uint64) error {
	nums, err := listSegmentNums(dir, gen)
	if err != nil {
		return err
	}
	for _, num := range nums {
		p := filepath.Join(dir, fmt.Sprintf("wal-%d-%06d.wal", gen, num))
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
