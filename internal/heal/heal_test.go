package heal

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeRebuilder 记录被调用的任务，并按 failTimes 注入失败。
type fakeRebuilder struct {
	mu        sync.Mutex
	rebuilt   []Task
	failTimes int
	calls     int
	done      chan struct{}
}

func newFakeRebuilder(failTimes int) *fakeRebuilder {
	return &fakeRebuilder{failTimes: failTimes, done: make(chan struct{}, 16)}
}

func (f *fakeRebuilder) Rebuild(ctx context.Context, t Task) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failTimes {
		return fmt.Errorf("injected rebuild failure %d", f.calls)
	}
	f.rebuilt = append(f.rebuilt, t)
	f.done <- struct{}{}
	return nil
}

func (f *fakeRebuilder) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rebuilt)
}

func sampleTask() Task {
	return Task{Bucket: "bkt", Object: "obj", DataKey: "data-1", PartNumber: 1, Size: 1024, DiskIndex: 2}
}

// TestQueueDedup 校验相同任务只入队一次。
func TestQueueDedup(t *testing.T) {
	q := NewQueue(testLogger())
	q.Enqueue(sampleTask())
	q.Enqueue(sampleTask())
	if q.Len() != 1 {
		t.Fatalf("Len = %d, want 1 (dedup)", q.Len())
	}
}

// TestQueueRebuilds 校验 worker 会消费任务并重建。
func TestQueueRebuilds(t *testing.T) {
	q := NewQueue(testLogger())
	q.SetRetryDelay(10 * time.Millisecond)
	r := newFakeRebuilder(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.Run(ctx, r, nil)

	q.Enqueue(sampleTask())
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		t.Fatal("rebuild was not executed")
	}
	if r.count() != 1 || q.Len() != 0 {
		t.Fatalf("rebuilt=%d pending=%d", r.count(), q.Len())
	}
}

// TestQueueSkipsOfflineDisk 校验目标盘离线时任务会保留并重试，盘恢复后才重建。
func TestQueueSkipsOfflineDisk(t *testing.T) {
	q := NewQueue(testLogger())
	q.SetRetryDelay(10 * time.Millisecond)
	r := newFakeRebuilder(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	online := false
	gate := func(int) bool {
		mu.Lock()
		defer mu.Unlock()
		return online
	}
	go q.Run(ctx, r, gate)

	q.Enqueue(sampleTask())
	time.Sleep(100 * time.Millisecond)
	if r.count() != 0 {
		t.Fatal("rebuild must not run while the target disk is offline")
	}
	if q.Len() == 0 {
		t.Fatal("task should stay queued while the disk is offline")
	}

	mu.Lock()
	online = true
	mu.Unlock()
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		t.Fatal("rebuild was not executed after the disk came back")
	}
}

// TestQueueGivesUpAfterMaxAttempts 校验反复失败的任务最终放弃，不会无限重试。
func TestQueueGivesUpAfterMaxAttempts(t *testing.T) {
	q := NewQueue(testLogger())
	q.SetRetryDelay(5 * time.Millisecond)
	r := newFakeRebuilder(100) // 永远失败
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.Run(ctx, r, nil)

	q.Enqueue(sampleTask())
	deadline := time.After(2 * time.Second)
	for {
		if _, gaveUp := q.Stats(); gaveUp > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("task never gave up: calls=%d pending=%d", r.calls, q.Len())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if q.Len() != 0 {
		t.Fatalf("pending = %d, want 0 after giving up", q.Len())
	}
}
