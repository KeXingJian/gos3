package catalog

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kxj/gos3/internal/store"
)

// Indexed 是 store.Store 的装饰器：写路径投递事件给引擎（write-behind），
// 未覆盖的方法原样透传（内嵌 Store）。
type Indexed struct {
	store.Store
	eng               *Engine
	q                 chan Event
	dropped           atomic.Uint64
	log               *slog.Logger
	reconcileInterval time.Duration
	stop              chan struct{}
	done              chan struct{}
	reconcileDone     chan struct{}
	reconcileMu       sync.Mutex
	applyMu           sync.RWMutex
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
	indexed := NewIndexed(st, eng, 0, log)
	interval := cfg.ReconcileInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	indexed.reconcileInterval = interval
	indexed.reconcileDone = make(chan struct{})
	go indexed.reconcileLoop()
	return indexed, nil
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
func (s *Indexed) reconcileLoop() {
	defer close(s.reconcileDone)
	ticker := time.NewTicker(s.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.reconcileMu.Lock()
			s.applyMu.Lock()
			if err := s.eng.Rebuild(); err != nil {
				s.log.Warn("[gos3: catalog-reconcile-failed]", "error", err.Error())
			} else {
				s.dropped.Store(0)
				s.log.Info("[gos3: catalog-reconciled]", "docs", s.eng.Status().Docs)
			}
			s.applyMu.Unlock()
			s.reconcileMu.Unlock()
		case <-s.stop:
			return
		}
	}
}

func (s *Indexed) run() {
	defer close(s.done)
	for {
		select {
		case ev := <-s.q:
			s.applyMu.RLock()
			if err := s.eng.Append(ev); err != nil {
				s.log.Warn("[gos3: catalog-apply-failed]", "error", err.Error())
			}
			s.applyMu.RUnlock()
		case <-s.stop:
			for {
				select {
				case ev := <-s.q:
					s.applyMu.RLock()
					if err := s.eng.Append(ev); err != nil {
						s.log.Warn("[gos3: catalog-apply-failed]", "error", err.Error())
					}
					s.applyMu.RUnlock()
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
	if s.reconcileDone != nil {
		<-s.reconcileDone
	}
	_ = s.eng.Close()
}

// Engine 返回底层引擎（供查询层/状态接口使用）。
func (s *Indexed) Engine() *Engine { return s.eng }

// Dropped 返回因队列满被丢弃的事件数。
func (s *Indexed) Dropped() uint64 { return s.dropped.Load() }

// Status 返回引擎 + 队列的运行状态，供 /gos3/admin/catalog/status 使用。
func (s *Indexed) Status() Status {
	st := s.eng.Status()
	st.Dropped = s.dropped.Load()
	st.Queued = len(s.q)
	st.Capacity = cap(s.q)
	if st.Dropped > 0 || (st.Capacity > 0 && st.Queued >= st.Capacity) {
		st.State = "lagging"
	}
	return st
}

// Rebuild 触发一次全量重建（管理端 reindex）。重建以 sidecar 为真相，
// 成功后可清空累计 dropped 计数，让 status 从 lagging 回到 ready。
func (s *Indexed) Rebuild() error {
	if s.reconcileDone != nil {
		s.reconcileMu.Lock()
		defer s.reconcileMu.Unlock()
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	if err := s.eng.Rebuild(); err != nil {
		return err
	}
	s.dropped.Store(0)
	return nil
}

// Search 查询索引。
func (s *Indexed) Search(f Filter) (SearchResult, error) { return s.eng.Search(f) }

// Stats 聚合查询索引。
func (s *Indexed) Stats(opts StatsOptions) (StatsResult, error) { return s.eng.Stats(opts) }

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
		s.indexDelete(bucket, object, res)
	}
	return res, err
}

// DeleteObjects 逐个调用底层 DeleteObject 以拿到真实的删除结果。
// 不能复用请求里的 VersionID：非版本化删除的主键是 "null"、版本化删除会生成
// 删除标记，直接按请求参数入队会写错主键或丢掉删除标记。
func (s *Indexed) DeleteObjects(ctx context.Context, bucket string, objects []store.ObjectToDelete) []error {
	errs := make([]error, len(objects))
	for i, obj := range objects {
		res, err := s.Store.DeleteObject(ctx, bucket, obj.Object, obj.VersionID)
		if err != nil {
			errs[i] = err
			continue
		}
		s.indexDelete(bucket, obj.Object, res)
	}
	return errs
}

// indexDelete 按底层删除的真实结果投递事件。
func (s *Indexed) indexDelete(bucket, object string, res store.DeleteResult) {
	if res.VersionID == "" {
		res.VersionID = store.NullVersionID
	}
	if res.DeleteMarker {
		s.enqueue(Event{Op: OpPut, Doc: Doc{
			Bucket: bucket, Key: object, VersionID: res.VersionID,
			DeleteMarker: true, IsLatest: true, ModTime: time.Now().UTC(),
		}})
		return
	}
	s.enqueue(Event{Op: OpDel, Doc: Doc{Bucket: bucket, Key: object, VersionID: res.VersionID}})
}

func (s *Indexed) PutObjectTagging(ctx context.Context, bucket, object, versionID string, tags map[string]string) error {
	if err := s.Store.PutObjectTagging(ctx, bucket, object, versionID, tags); err != nil {
		return err
	}
	if versionID == "" {
		versionID = store.NullVersionID
	}
	s.enqueueTagVersion(ctx, bucket, object, versionID)
	return nil
}
func (s *Indexed) DeleteObjectTagging(ctx context.Context, bucket, object, versionID string) error {
	if err := s.Store.DeleteObjectTagging(ctx, bucket, object, versionID); err != nil {
		return err
	}
	if versionID == "" {
		versionID = store.NullVersionID
	}
	s.enqueueTagVersion(ctx, bucket, object, versionID)
	return nil
}
func (s *Indexed) enqueueTagVersion(ctx context.Context, bucket, object, versionID string) {
	_ = s.Store.WalkVersions(ctx, bucket, func(v store.VersionInfo) error {
		if v.Name == object && (versionID == v.VersionID || versionID == "" && v.IsLatest) {
			d := docFromObjectInfo(v.ObjectInfo, v.IsLatest)
			s.enqueue(Event{Op: OpPut, Doc: d})
		}
		return nil
	})
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
		UserMeta: info.UserMetadata, Tags: info.Tags, ModTime: info.ModTime,
	}
}

// rebuildFromStore 从底层 store 的 sidecar 元数据全量重建（作为引擎的 rebuild 源）。
//
// WalkVersions 对每个 key 是「最新版本在前」，而引擎按 emit 顺序递增分配 Seq。
// 为了让「最后写入的版本 Seq 最大」，这里按 key 缓冲后倒序 emit（旧版本先出），
// 否则重建后删除最新版本时 promoteLatest 会选错下一个版本。
func rebuildFromStore(ctx context.Context, st store.Store) RebuildFunc {
	return func(emit func(Event) error) error {
		buckets, err := st.ListBuckets(ctx)
		if err != nil {
			return err
		}
		for _, b := range buckets {
			var pending []Doc
			curKey := ""
			flush := func() error {
				for i := len(pending) - 1; i >= 0; i-- {
					if err := emit(Event{Op: OpPut, Doc: pending[i]}); err != nil {
						return err
					}
				}
				pending = nil
				return nil
			}

			err := st.WalkVersions(ctx, b.Name, func(v store.VersionInfo) error {
				if v.Name != curKey {
					if err := flush(); err != nil {
						return err
					}
					curKey = v.Name
				}
				pending = append(pending, docFromObjectInfo(v.ObjectInfo, v.IsLatest))
				return nil
			})
			if err != nil {
				return err
			}
			if err := flush(); err != nil {
				return err
			}
		}
		return nil
	}
}
