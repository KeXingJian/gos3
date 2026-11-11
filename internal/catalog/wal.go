package catalog

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// WalConfig 是 WAL 的持久化策略。
type WalConfig struct {
	Dir         string        // 段文件目录
	SyncEvery   int           // 组提交条数（<=0 用默认）
	SyncTimeout time.Duration // 组提交间隔（<=0 用默认）
	SegBytes    int64         // 段封口字节（<=0 用默认）
	SegDocs     int           // 段封口条数（<=0 用默认）
}

func (c WalConfig) withDefaults() WalConfig {
	if c.SyncEvery <= 0 {
		c.SyncEvery = 256
	}
	if c.SyncTimeout <= 0 {
		c.SyncTimeout = 20 * time.Millisecond
	}
	if c.SegBytes <= 0 {
		c.SegBytes = 64 << 20
	}
	if c.SegDocs <= 0 {
		c.SegDocs = 100000
	}
	return c
}

// walSegment 描述一个已封口段的持久化状态。
type walSegment struct {
	file     string
	firstSeq uint64
	lastSeq  uint64
	docs     int
}

// Wal 是段式预写日志：单写者，组提交 fsync，段封口 + 轮转。
// 非并发安全，只允许一个 goroutine 调用写方法。
type Wal struct {
	dir string
	gen uint64
	cfg WalConfig

	f       *os.File // 当前段文件（nil = 已关闭）
	curNum  int      // 当前段编号
	nextNum int      // 下一个新段编号
	w       *bufio.Writer

	pending  int // 已写入缓冲、尚未 fsync 的记录数
	lastSync time.Time

	firstSeq uint64 // 当前段第一条 seq（0 = 空段）
	lastSeq  uint64 // 当前段已写最后一条 seq
	docs     int
	bytes    int64

	sealed []walSegment // 本进程内已封口的段（按 firstSeq 升序）
}

// NewWal 打开（或创建）目录下的 WAL。已存在段会被扫描以决定下一个段号；
// 追加总是写新段，不续写可能带 torn tail 的旧段。
func NewWal(dir string, gen uint64, cfg WalConfig) (*Wal, error) {
	cfg = cfg.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &Wal{dir: dir, gen: gen, cfg: cfg, lastSync: time.Now()}
	nums, err := listSegmentNums(dir, gen)
	if err != nil {
		return nil, err
	}
	if len(nums) > 0 {
		w.nextNum = nums[len(nums)-1] + 1
	}
	if err := w.openSegment(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Wal) segFileName(num int) string {
	return filepath.Join(w.dir, fmt.Sprintf("wal-%d-%06d.wal", w.gen, num))
}

// listSegmentNums 扫描目录下属于该 gen 的段，返回按编号升序的段号列表。
func listSegmentNums(dir string, gen uint64) ([]int, error) {
	prefix := fmt.Sprintf("wal-%d-", gen)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var nums []int
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".wal") {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".wal")
		n, err := strconv.Atoi(mid)
		if err != nil {
			continue
		}
		nums = append(nums, n)
	}
	sort.Ints(nums)
	return nums, nil
}

func (w *Wal) openSegment() error {
	name := w.segFileName(w.nextNum)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.f = f
	w.w = bufio.NewWriter(f)
	w.curNum = w.nextNum
	w.nextNum++
	w.firstSeq, w.lastSeq, w.docs, w.bytes, w.pending = 0, 0, 0, 0, 0
	return nil
}

// Append 追加一条记录，触发组提交与段封口。
func (w *Wal) Append(seq uint64, ev Event) error {
	if w.f == nil {
		return errors.New("wal: no open segment")
	}
	rec := appendWALRecord(nil, seq, ev.Op, &ev.Doc)
	if _, err := w.w.Write(rec); err != nil {
		return err
	}
	w.pending++
	w.docs++
	w.bytes += int64(len(rec))
	if w.firstSeq == 0 {
		w.firstSeq = seq
	}
	w.lastSeq = seq

	needSeal := w.bytes >= w.cfg.SegBytes || w.docs >= w.cfg.SegDocs
	if needSeal {
		return w.seal()
	}
	needSync := w.pending >= w.cfg.SyncEvery || time.Since(w.lastSync) >= w.cfg.SyncTimeout
	if needSync {
		return w.Sync()
	}
	return nil
}

// Pending 返回尚未 fsync 的记录数（调用方需自行保证与写路径互斥）。
func (w *Wal) Pending() int { return w.pending }

// Sync 把缓冲 flush 到文件并 fsync。
func (w *Wal) Sync() error {
	if w.w == nil {
		return nil
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.pending = 0
	w.lastSync = time.Now()
	return nil
}

// finishSegment 同步并关闭当前段，记入 sealed。
func (w *Wal) finishSegment() error {
	if w.f == nil {
		return nil
	}
	if err := w.Sync(); err != nil {
		return err
	}
	name := w.segFileName(w.curNum)
	if err := w.f.Close(); err != nil {
		return err
	}
	w.f, w.w = nil, nil
	if w.docs > 0 {
		w.sealed = append(w.sealed, walSegment{file: name, firstSeq: w.firstSeq, lastSeq: w.lastSeq, docs: w.docs})
	} else if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// seal 封口当前段并打开新段（用于轮转）。
func (w *Wal) seal() error {
	if err := w.finishSegment(); err != nil {
		return err
	}
	return w.openSegment()
}

// Seal 封口当前段并打开新段（导出给引擎在快照前调用，使当前记录进入 sealed 可被截断）。
func (w *Wal) Seal() error { return w.seal() }

// Close 封口当前段并关闭（不打开新段）。
func (w *Wal) Close() error {
	return w.finishSegment()
}

// Truncate 删除 lastSeq <= maxSeq 的已封口段（快照后回收）。
func (w *Wal) Truncate(maxSeq uint64) error {
	kept := w.sealed[:0]
	for _, s := range w.sealed {
		if s.lastSeq <= maxSeq {
			if err := os.Remove(s.file); err != nil && !os.IsNotExist(err) {
				return err
			}
			continue
		}
		kept = append(kept, s)
	}
	w.sealed = kept
	return nil
}

// Segments 返回已封口段清单（不含当前未封口段）。
func (w *Wal) Segments() []walSegment {
	return w.sealed
}

// Replay 顺序重放目录下所有段（含当前段），对每条记录调用 fn。
// 先 flush 当前缓冲保证读全；遇 crc 损坏返回 ErrCRC，遇 torn tail 则静默停止。
func (w *Wal) Replay(fn func(seq uint64, op Op, doc *Doc) error) error {
	if w.w != nil {
		if err := w.w.Flush(); err != nil {
			return err
		}
	}
	nums, err := listSegmentNums(w.dir, w.gen)
	if err != nil {
		return err
	}
	for _, num := range nums {
		data, err := os.ReadFile(w.segFileName(num))
		if err != nil {
			return err
		}
		rest := data
		for len(rest) > 0 {
			seq, op, doc, n, err := decodeWALRecord(rest)
			if err != nil {
				if errors.Is(err, ErrCRC) {
					return err
				}
				break // torn tail：尾部不完整，忽略
			}
			if err := fn(seq, op, doc); err != nil {
				return err
			}
			rest = rest[n:]
		}
	}
	return nil
}
