package catalog

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/kxj/gos3/internal/store"
)

// Indexed 是 store.Store 的装饰器：写路径投递事件给引擎（write-behind），
// 未覆盖的方法原样透传（内嵌 Store）。
type Indexed struct {
	store.Store
	eng     *Engine
	q       chan Event
	dropped atomic.Uint64
	log     *slog.Logger
	stop    chan struct{}
	done    chan struct{}
}

// New 构建 Catalog：创建引擎（含全量重建源）并用 Indexed 包装底层 store。
// 返回的 *Indexed 实现 store.Store，可直接替换底层 store 使用。
func New(st store.Store, cfg Config, log *slog.Logger) (*Indexed, error) {
	if log == nil {
		log = slog.Default()
	}
	eng, err := NewEngine(cfg, rebuildFromStore(context.Background(), st))
	if err != nil {
		return nil, err
	}
	return NewIndexed(st, eng, 0, log), nil
}

// NewIndexed 用指定引擎包装底层 store。
func NewIndexed(st store.Store, eng *Engine, queueSize int, log *slog.Logger) *Indexed {
	if queueSize <= 0 {
		queueSize = 64 * 1024
	}
	if log == nil {
		log = slog.Default()
	}
	s := &Indexed{Store: st, eng: eng, q: make(chan Event, queueSize), log: log, stop: make(chan struct{}), done: make(chan struct{})}
	go s.run()
	return s
}

// run 串行消费事件队列，调用引擎 Append（单写者，保持 seq 有序）。
func (s *Indexed) run() {
	defer close(s.done)
	for {
		select {
		case ev := <-s.q:
			if err := s.eng.Append(ev); err != nil {
				s.log.Warn("[gos3: catalog-apply-failed]", "error", err.Error())
			}
		case <-s.stop:
			for {
				select {
				case ev := <-s.q:
					if err := s.eng.Append(ev); err != nil {
						s.log.Warn("[gos3: catalog-apply-failed]", "error", err.Error())
					}
				default:
					return
				}
			}
		}
	}
}

// enqueue 非阻塞投递；队列满则丢事件并计数（由 reconcile 补齐）。
func (s *Indexed) enqueue(ev Event) {
	select {
	case s.q <- ev:
	default:
		s.dropped.Add(1)
	}
}

// Close 停止投递，排空队列，关闭引擎。
func (s *Indexed) Close() {
	close(s.stop)
	<-s.done
	_ = s.eng.Close()
}

// Engine 返回底层引擎（供查询层/状态接口使用）。
func (s *Indexed) Engine() *Engine { return s.eng }

// Dropped 返回因队列满被丢弃的事件数。
func (s *Indexed) Dropped() uint64 { return s.dropped.Load() }

// ---- 覆盖写路径 ----

func (s *Indexed) PutObject(ctx context.Context, bucket, object string, data io.Reader, size int64, contentType string, userMeta map[string]string) (store.ObjectInfo, error) {
	info, err := s.Store.PutObject(ctx, bucket, object, data, size, contentType, userMeta)
	if err == nil {
		s.enqueue(Event{Op: OpPut, Doc: docFromObjectInfo(info, true)})
	}
	return info, err
}

func (s *Indexed) DeleteObject(ctx context.Context, bucket, object, versionID string) (store.DeleteResult, error) {
	res, err := s.Store.DeleteObject(ctx, bucket, object, versionID)
	if err == nil {
		if res.DeleteMarker {
			s.enqueue(Event{Op: OpPut, Doc: Doc{Bucket: bucket, Key: object, VersionID: res.VersionID, DeleteMarker: true, IsLatest: true, ModTime: time.Now().UTC()}})
		} else {
			s.enqueue(Event{Op: OpDel, Doc: Doc{Bucket: bucket, Key: object, VersionID: res.VersionID}})
		}
	}
	return res, err
}

func (s *Indexed) DeleteObjects(ctx context.Context, bucket string, objects []store.ObjectToDelete) []error {
	errs := s.Store.DeleteObjects(ctx, bucket, objects)
	for i, err := range errs {
		if err == nil {
			// 简化：批量删除不区分删除标记，统一按硬删处理；删除标记场景由 reconcile/重建修正。
			s.enqueue(Event{Op: OpDel, Doc: Doc{Bucket: bucket, Key: objects[i].Object, VersionID: objects[i].VersionID}})
		}
	}
	return errs
}

func (s *Indexed) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []store.CompletePart) (store.ObjectInfo, error) {
	info, err := s.Store.CompleteMultipartUpload(ctx, bucket, object, uploadID, parts)
	if err == nil {
		s.enqueue(Event{Op: OpPut, Doc: docFromObjectInfo(info, true)})
	}
	return info, err
}

// ---- 工具 ----

func docFromObjectInfo(info store.ObjectInfo, latest bool) Doc {
	// store 在非版本化 PUT 返回的 ObjectInfo.VersionID 为空串，但 sidecar 存的是 "null"，
	// 归一化到 null，保证写路径与重建路径的主键一致。
	if info.VersionID == "" {
		info.VersionID = store.NullVersionID
	}
	return Doc{
		Bucket: info.Bucket, Key: info.Name, VersionID: info.VersionID,
		IsLatest: latest, DeleteMarker: info.DeleteMarker,
		Size: info.Size, ETag: info.ETag, ContentType: info.ContentType,
		UserMeta: info.UserMetadata, ModTime: info.ModTime,
	}
}

// rebuildFromStore 从底层 store 的 sidecar 元数据全量重建（作为引擎的 rebuild 源）。
func rebuildFromStore(ctx context.Context, st store.Store) RebuildFunc {
	return func(emit func(Event) error) error {
		buckets, err := st.ListBuckets(ctx)
		if err != nil {
			return err
		}
		for _, b := range buckets {
			err := st.WalkVersions(ctx, b.Name, func(v store.VersionInfo) error {
				d := Doc{
					Bucket: b.Name, Key: v.Name, VersionID: v.VersionID,
					IsLatest: v.IsLatest, DeleteMarker: v.DeleteMarker,
					Size: v.Size, ETag: v.ETag, ContentType: v.ContentType,
					UserMeta: v.UserMetadata, ModTime: v.ModTime,
				}
				return emit(Event{Op: OpPut, Doc: d})
			})
			if err != nil {
				return err
			}
		}
		return nil
	}
}
