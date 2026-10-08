// Package heal 是「最小自愈」：读路径发现某块盘缺分片时把任务入队，
// 后台 worker 用其余盘的分片重建并写回该盘。
//
// 不做的事情（按计划）：bitrot 深度扫描、MRF 持久化、rebalance。
package heal

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	// maxAttempts 是同一个分片的最大重建尝试次数（掉线盘会等它回来再试）。
	maxAttempts = 5
	// defaultRetryDelay 是默认的重试/节流间隔。
	defaultRetryDelay = 2 * time.Second
)

// LegacyPart 表示历史布局（无 parts 字段）的单一分片文件。
const LegacyPart = 0

// Task 描述「把某个 part 的分片在指定盘上重建」。
type Task struct {
	Bucket     string
	Object     string
	DataKey    string // 对象数据键（新布局为 dataID，历史对象为 versionID）
	PartNumber int    // 1..n；LegacyPart(=0) 表示历史单文件布局
	Size       int64  // 该 part 的原始字节数（解码时需要）
	DiskIndex  int    // 目标盘下标
}

// Rebuilder 由存储层实现：从其余盘读分片、解码、重编码，并把目标分片写回该盘。
type Rebuilder interface {
	Rebuild(ctx context.Context, task Task) error
}

// Queue 是带去重与重试上限的内存修复队列。
type Queue struct {
	log   *slog.Logger
	delay time.Duration

	mu      sync.Mutex
	pending map[Task]int // 任务 -> 已尝试次数
	order   []Task       // 入队顺序
	rebuilt int64        // 累计成功重建数
	gaveUp  int64        // 累计放弃数
}

// NewQueue 创建修复队列。
func NewQueue(log *slog.Logger) *Queue {
	return &Queue{log: log, delay: defaultRetryDelay, pending: map[Task]int{}}
}

// Enqueue 入队一个修复任务（相同任务只保留一份）。
func (q *Queue) Enqueue(t Task) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.pending[t]; ok {
		return
	}
	q.pending[t] = 0
	q.order = append(q.order, t)
}

// Len 返回待修复任务数。
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.order)
}

// Stats 返回 (已重建数, 已放弃数)。
func (q *Queue) Stats() (int64, int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.rebuilt, q.gaveUp
}

// front 返回队首任务（不出队），用于在探测目标盘状态前做判断。
func (q *Queue) front() (Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.order) == 0 {
		return Task{}, false
	}
	return q.order[0], true
}

// pop 取出一个任务及其已尝试次数。
func (q *Queue) pop() (Task, int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.order) == 0 {
		return Task{}, 0, false
	}
	t := q.order[0]
	q.order = q.order[1:]
	attempts := q.pending[t]
	delete(q.pending, t)
	return t, attempts, true
}

// retry 记录一次失败：未超过上限则重新入队。
func (q *Queue) retry(t Task, attempts int, reason string) {
	if attempts+1 >= maxAttempts {
		q.mu.Lock()
		q.gaveUp++
		q.mu.Unlock()
		q.log.Warn("[gos3: heal-give-up]", "bucket", t.Bucket, "object", t.Object, "part", t.PartNumber, "disk", t.DiskIndex, "attempts", attempts+1, "error", reason)
		return
	}
	q.mu.Lock()
	q.pending[t] = attempts + 1
	q.order = append(q.order, t)
	q.mu.Unlock()
}

// Run 消费队列直到 ctx 结束。gate 返回 false（目标盘离线）时任务保持排队，
// 既不消耗重试次数也不发起重建，等盘恢复后再处理。
func (q *Queue) Run(ctx context.Context, r Rebuilder, gate func(diskIndex int) bool) {
	ticker := time.NewTicker(q.delay)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if head, ok := q.front(); !ok {
			continue
		} else if gate != nil && !gate(head.DiskIndex) {
			continue
		}
		task, attempts, ok := q.pop()
		if !ok {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := r.Rebuild(cctx, task)
		cancel()
		if err != nil {
			q.retry(task, attempts, err.Error())
			continue
		}
		q.mu.Lock()
		q.rebuilt++
		q.mu.Unlock()
		q.log.Info("[gos3: heal-rebuilt]", "bucket", task.Bucket, "object", task.Object, "part", task.PartNumber, "disk", task.DiskIndex)
	}
}

// SetRetryDelay 调整消费/重试间隔（测试用）。
func (q *Queue) SetRetryDelay(d time.Duration) {
	if d > 0 {
		q.delay = d
	}
}
