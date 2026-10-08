package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kxj/gos3/internal/disk"
	"github.com/kxj/gos3/internal/erasure"
	"github.com/kxj/gos3/internal/heal"
	"github.com/kxj/gos3/internal/lifecycle"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// NSLocker 是存储层需要的命名空间锁：由 internal/lock 的 DRWMutex 实现。
// 写成接口是为了让 store 不依赖 lock 包的具体实现，单测可注入假锁。
type NSLocker interface {
	// LockWrite 加写锁（互斥），返回释放函数。
	LockWrite(ctx context.Context, resource string) (func(), error)
	// LockRead 加读锁（共享），返回释放函数。
	LockRead(ctx context.Context, resource string) (func(), error)
}

// noopLocker 在未配置锁（例如单测直接构造）时使用，保证代码路径统一。
type noopLocker struct{}

func (noopLocker) LockWrite(context.Context, string) (func(), error) { return func() {}, nil }
func (noopLocker) LockRead(context.Context, string) (func(), error)  { return func() {}, nil }

type Erasure struct {
	disks        []disk.Disk
	encoder      *erasure.Encoder
	dataShards   int
	parityShards int
	locker       NSLocker
	healer       *heal.Queue
	log          *slog.Logger
}

// NewErasure 创建纠删码存储后端。locker 为 nil 时退化为「不加锁」并打警告；
// healer 为 nil 时不启用读修复（读路径仍按 quorum 工作）。
func NewErasure(disks []disk.Disk, dataShards, parityShards int, log *slog.Logger, locker NSLocker, healer *heal.Queue) (*Erasure, error) {
	if dataShards < 1 || parityShards < 1 {
		return nil, errors.New("data and parity shards must both be >= 1")
	}
	if len(disks) != dataShards+parityShards {
		return nil, fmt.Errorf("disk count (%d) must equal data+parity shards (%d)", len(disks), dataShards+parityShards)
	}
	enc, err := erasure.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	if locker == nil {
		log.Warn("[gos3: erasure-locking-disabled]")
		locker = noopLocker{}
	}
	e := &Erasure{disks: disks, encoder: enc, dataShards: dataShards, parityShards: parityShards, locker: locker, healer: healer, log: log}
	ids := make([]string, len(disks))
	for i, d := range disks {
		ids[i] = d.ID()
	}
	log.Info("[gos3: erasure-ready]", "disks", len(disks), "data-shards", dataShards, "parity-shards", parityShards, "ids", strings.Join(ids, ","))
	return e, nil
}

// objectResource 是对象写操作的命名空间锁键（对齐 MinIO 的 "bucket/object" 口径）。
func objectResource(bucket, object string) string {
	return bucket + "/" + object
}

// partResource 是分片上传单个 part 的锁键：同一 (uploadID, partN) 的并发重传要串行化。
func partResource(bucket, object, uploadID string, partNumber int) string {
	return fmt.Sprintf("%s/%s/%s/part%d", bucket, object, uploadID, partNumber)
}

// lockObject 给 bucket/object 加写锁：多节点并发写同一个 key 时串行化。
// 拿不到锁（法定人数不足）时返回 ErrLockTimeout，调用方应重试。
func (e *Erasure) lockObject(ctx context.Context, bucket, object string) (func(), error) {
	return e.lockResource(ctx, objectResource(bucket, object))
}

// lockResource 给任意资源键加写锁。
func (e *Erasure) lockResource(ctx context.Context, resource string) (func(), error) {
	release, err := e.locker.LockWrite(ctx, resource)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrLockTimeout, resource, err)
	}
	return release, nil
}

func (e *Erasure) MakeBucket(ctx context.Context, bucket string) error {
	if !validBucketName(bucket) {
		return ErrInvalidBucketName
	}
	created, existed := 0, 0
	var errs []error
	for _, d := range e.disks {
		fi, err := d.Stat(ctx, diskBucketMetaDir(bucket))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if fi.Exists {
			existed++
			continue
		}
		if err := d.MakeDir(ctx, diskBucketMetaDir(bucket)); err != nil {
			errs = append(errs, err)
			continue
		}
		created++
	}
	if existed > 0 {
		return ErrBucketExists
	}
	// 创建 bucket 是元数据修改，必须达到写法定人数，否则各节点对「bucket 是否存在」会各说各话。
	if created < e.writeQuorum() {
		if err := reduceErrs(errs); err != nil {
			return fmt.Errorf("%w: make bucket %s (%d/%d created): %v", ErrWriteQuorum, bucket, created, e.writeQuorum(), err)
		}
		return fmt.Errorf("%w: make bucket %s (%d/%d created)", ErrWriteQuorum, bucket, created, e.writeQuorum())
	}
	e.log.Info("[gos3: erasure-make-bucket]", "bucket", bucket)
	return nil
}

func (e *Erasure) DeleteBucket(ctx context.Context, bucket string) error {
	if !validBucketName(bucket) {
		return ErrInvalidBucketName
	}
	if _, ok, err := e.BucketExists(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return ErrBucketNotFound
	}
	empty, err := e.bucketEmpty(ctx, bucket)
	if err != nil {
		return err
	}
	if !empty {
		return ErrBucketNotEmpty
	}
	for _, d := range e.disks {
		_ = d.DeleteDir(ctx, diskBucketMetaDir(bucket))
		_ = d.DeleteDir(ctx, path.Join(dataDir, bucket))
		_ = d.DeleteDir(ctx, path.Join(multipartDir, bucket))
	}
	e.log.Info("[gos3: erasure-delete-bucket]", "bucket", bucket)
	return nil
}

// bucketEmpty 判断 bucket 是否为空：只要有达到读法定人数的盘可列举，
// 且这些盘上都没有对象元数据，就认为为空（单盘不可达不影响判断）。
func (e *Erasure) bucketEmpty(ctx context.Context, bucket string) (bool, error) {
	listed := 0
	var errs []error
	for _, d := range e.disks {
		files, _, err := d.Walk(ctx, diskBucketMetaDir(bucket))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		listed++
		for _, f := range files {
			base := path.Base(f)
			if strings.HasPrefix(base, ".") || !strings.HasSuffix(base, ".json") {
				continue
			}
			return false, nil
		}
	}
	if listed < e.readQuorum() {
		if err := reduceErrs(errs); err != nil {
			return false, fmt.Errorf("%w: bucket %s (listable drives %d/%d): %v", ErrReadQuorum, bucket, listed, e.readQuorum(), err)
		}
		return false, fmt.Errorf("%w: bucket %s (listable drives %d/%d)", ErrReadQuorum, bucket, listed, e.readQuorum())
	}
	return true, nil
}

// BucketExists 判断 bucket 是否存在：需要达到读法定人数的盘上存在该 bucket 才算存在。
// 可见盘数不足、或只有少数盘上有该 bucket 时返回 ErrReadQuorum —— 宁可报「无法判定」，
// 也不能谎报「不存在」，否则客户端会误以为 bucket 被删掉了。
func (e *Erasure) BucketExists(ctx context.Context, bucket string) (time.Time, bool, error) {
	if !validBucketName(bucket) {
		return time.Time{}, false, ErrInvalidBucketName
	}
	existed, tried := 0, 0
	var modTime time.Time
	for _, d := range e.disks {
		fi, err := d.Stat(ctx, diskBucketMetaDir(bucket))
		if err != nil {
			continue
		}
		tried++
		if !fi.Exists {
			continue
		}
		existed++
		if modTime.IsZero() {
			modTime = fi.ModTime
		}
	}
	switch {
	case existed >= e.readQuorum():
		return modTime, true, nil
	case tried < e.readQuorum():
		return time.Time{}, false, fmt.Errorf("%w: bucket %s (visible drives %d/%d)", ErrReadQuorum, bucket, tried, e.readQuorum())
	case existed == 0:
		return time.Time{}, false, nil
	default:
		return time.Time{}, false, fmt.Errorf("%w: bucket %s exists on %d/%d drives", ErrReadQuorum, bucket, existed, e.readQuorum())
	}
}

// ListBuckets 汇总所有盘上的 bucket 目录，只返回达到读法定人数的 bucket。
func (e *Erasure) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	counts := map[string]int{}
	created := map[string]time.Time{}
	listed := 0
	for _, d := range e.disks {
		entries, err := d.ListDir(ctx, metaDir)
		if err != nil {
			continue
		}
		listed++
		for _, en := range entries {
			if !en.IsDir || !validBucketName(en.Name) {
				continue
			}
			counts[en.Name]++
			if _, ok := created[en.Name]; !ok {
				fi, _ := d.Stat(ctx, diskBucketMetaDir(en.Name))
				created[en.Name] = fi.ModTime
			}
		}
	}
	if listed < e.readQuorum() {
		return nil, fmt.Errorf("%w: list buckets (listable drives %d/%d)", ErrReadQuorum, listed, e.readQuorum())
	}
	var buckets []BucketInfo
	for name, c := range counts {
		if c < e.readQuorum() {
			continue
		}
		buckets = append(buckets, BucketInfo{Name: name, Created: created[name]})
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Name < buckets[j].Name })
	return buckets, nil
}

func (e *Erasure) GetBucketVersioning(ctx context.Context, bucket string) (string, error) {
	if _, ok, err := e.BucketExists(ctx, bucket); err != nil {
		return "", err
	} else if !ok {
		return "", ErrBucketNotFound
	}
	var lastErr error
	for _, d := range e.disks {
		data, err := d.ReadFile(ctx, diskBucketConfigPath(bucket))
		if errors.Is(err, disk.ErrNotExist) {
			return VersioningDisabled, nil
		}
		if err != nil {
			lastErr = err
			continue
		}
		var cfg fsBucketConfig
		if json.Unmarshal(data, &cfg) == nil {
			return cfg.Versioning, nil
		}
	}
	if lastErr != nil {
		return "", lastErr
	}
	return VersioningDisabled, nil
}

func (e *Erasure) SetBucketVersioning(ctx context.Context, bucket, status string) error {
	if status != VersioningEnabled && status != VersioningSuspended {
		return ErrInvalidVersioning
	}
	if _, ok, err := e.BucketExists(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return ErrBucketNotFound
	}
	data, err := json.Marshal(fsBucketConfig{Versioning: status})
	if err != nil {
		return err
	}
	ok := 0
	for _, d := range e.disks {
		if err := d.WriteFile(ctx, diskBucketConfigPath(bucket), data); err == nil {
			ok++
		}
	}
	if ok < e.writeQuorum() {
		return fmt.Errorf("%w: versioning %s (%d/%d)", ErrWriteQuorum, bucket, ok, e.writeQuorum())
	}
	e.log.Info("[gos3: erasure-set-bucket-versioning]", "bucket", bucket, "status", status)
	return nil
}

func (e *Erasure) GetBucketLifecycle(ctx context.Context, bucket string) (lifecycle.Configuration, error) {
	if !validBucketName(bucket) {
		return lifecycle.Configuration{}, ErrInvalidBucketName
	}
	if _, ok, err := e.BucketExists(ctx, bucket); err != nil {
		return lifecycle.Configuration{}, err
	} else if !ok {
		return lifecycle.Configuration{}, ErrBucketNotFound
	}
	var lastErr error
	for _, d := range e.disks {
		data, err := d.ReadFile(ctx, diskLifecyclePath(bucket))
		if errors.Is(err, disk.ErrNotExist) {
			return lifecycle.Configuration{}, ErrNoLifecycleConfig
		}
		if err != nil {
			lastErr = err
			continue
		}
		var cfg lifecycle.Configuration
		if json.Unmarshal(data, &cfg) == nil {
			return cfg, nil
		}
	}
	if lastErr != nil {
		return lifecycle.Configuration{}, lastErr
	}
	return lifecycle.Configuration{}, ErrNoLifecycleConfig
}

func (e *Erasure) SetBucketLifecycle(ctx context.Context, bucket string, cfg lifecycle.Configuration) error {
	if _, ok, err := e.BucketExists(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return ErrBucketNotFound
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	ok := 0
	for _, d := range e.disks {
		if err := d.WriteFile(ctx, diskLifecyclePath(bucket), data); err == nil {
			ok++
		}
	}
	if ok < e.writeQuorum() {
		return fmt.Errorf("%w: lifecycle %s (%d/%d)", ErrWriteQuorum, bucket, ok, e.writeQuorum())
	}
	e.log.Info("[gos3: erasure-set-bucket-lifecycle]", "bucket", bucket, "rules", len(cfg.Rules))
	return nil
}

func (e *Erasure) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	if _, err := e.GetBucketLifecycle(ctx, bucket); err != nil {
		return err
	}
	for _, d := range e.disks {
		_ = d.DeleteFile(ctx, diskLifecyclePath(bucket))
	}
	return nil
}

// PutObject 写入对象（纠删码后端入口）。
// 流程：校验名称/bucket -> 读取版本控制状态并分配 versionID ->
// 读入完整数据 -> 交给 putBytes 编码落盘 -> 未开启版本控制时对外隐藏 versionID。
func (e *Erasure) PutObject(ctx context.Context, bucket, object string, data io.Reader, size int64, contentType string, userMeta map[string]string) (ObjectInfo, error) {
	if !validBucketName(bucket) {
		return ObjectInfo{}, ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return ObjectInfo{}, ErrInvalidObjectName
	}
	// 命名空间写锁：同一 key 的并发写（可能来自不同节点）在此串行化
	unlock, err := e.lockObject(ctx, bucket, object)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer unlock()
	if _, ok, err := e.BucketExists(ctx, bucket); err != nil {
		return ObjectInfo{}, err
	} else if !ok {
		return ObjectInfo{}, ErrBucketNotFound
	}
	ctx, span := otel.Tracer("gos3/store").Start(ctx, "erasure.PutObject",
		trace.WithAttributes(attribute.String("bucket", bucket), attribute.String("object", object)))
	defer span.End()

	// 依据 bucket 的版本控制状态决定版本号
	state, err := e.GetBucketVersioning(ctx, bucket)
	if err != nil {
		return ObjectInfo{}, err
	}
	versionID, err := assignVersionID(state)
	if err != nil {
		return ObjectInfo{}, err
	}
	// 一次性读入对象全部字节（当前实现非流式）
	raw, err := io.ReadAll(data)
	if err != nil {
		return ObjectInfo{}, err
	}
	// 交给纠删码写路径：etag 用内容 md5；replaceNull 表示覆盖旧的 "null" 版本
	info, err := e.putBytes(ctx, bucket, object, versionID, raw, contentType, userMeta, md5hex(raw), versionID == NullVersionID)
	if err != nil {
		return ObjectInfo{}, err
	}
	e.log.Info("[gos3: erasure-put-object]", "bucket", bucket, "object", object, "version", versionID, "size", info.Size)
	// 未开启版本控制时，不向客户端暴露内部使用的 "null" 版本号
	if state == VersioningDisabled {
		info.VersionID = ""
	}
	return info, nil
}

func (e *Erasure) GetObject(ctx context.Context, bucket, object, versionID string) (io.ReadSeekCloser, ObjectInfo, error) {
	if !validBucketName(bucket) {
		return nil, ObjectInfo{}, ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return nil, ObjectInfo{}, ErrInvalidObjectName
	}
	ctx, span := otel.Tracer("gos3/store").Start(ctx, "erasure.GetObject",
		trace.WithAttributes(attribute.String("bucket", bucket), attribute.String("object", object)))
	defer span.End()

	meta, err := e.readMetaQuorum(ctx, bucket, object)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	version, _, ok := resolveVersion(meta, versionID)
	if !ok {
		return nil, ObjectInfo{}, ErrNoSuchVersion
	}
	if version.DeleteMarker {
		return nil, ObjectInfo{}, ErrDeleteMarker
	}
	// part 化对象逐 part 解码；历史对象按单一分片文件解码（见 readVersionData）
	raw, err := e.readVersionData(ctx, bucket, object, version)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	return newReadSeekCloser(raw), version.toInfo(bucket, object), nil
}

func (e *Erasure) StatObject(ctx context.Context, bucket, object, versionID string) (ObjectInfo, error) {
	if !validBucketName(bucket) {
		return ObjectInfo{}, ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return ObjectInfo{}, ErrInvalidObjectName
	}
	meta, err := e.readMetaQuorum(ctx, bucket, object)
	if err != nil {
		return ObjectInfo{}, err
	}
	version, _, ok := resolveVersion(meta, versionID)
	if !ok {
		return ObjectInfo{}, ErrNoSuchVersion
	}
	if version.DeleteMarker {
		return ObjectInfo{}, ErrDeleteMarker
	}
	return version.toInfo(bucket, object), nil
}

func (e *Erasure) DeleteObject(ctx context.Context, bucket, object, versionID string) (DeleteResult, error) {
	if !validBucketName(bucket) {
		return DeleteResult{}, ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return DeleteResult{}, ErrInvalidObjectName
	}
	// 命名空间写锁：删除与写入互斥
	unlock, err := e.lockObject(ctx, bucket, object)
	if err != nil {
		return DeleteResult{}, err
	}
	defer unlock()
	if versionID != "" {
		meta, err := e.readMetaQuorum(ctx, bucket, object)
		if err != nil {
			if errors.Is(err, ErrObjectNotFound) {
				return DeleteResult{}, ErrNoSuchVersion
			}
			return DeleteResult{}, err
		}
		version, _, ok := resolveVersion(meta, versionID)
		if !ok {
			return DeleteResult{}, ErrNoSuchVersion
		}
		old := meta
		meta.Versions = removeVersion(meta.Versions, versionID)
		if err := e.writeOrRemoveMeta(ctx, bucket, object, old, meta); err != nil {
			return DeleteResult{}, err
		}
		// 元数据提交成功后再删数据：中途崩溃只会留下孤立分片，不会出现「元数据指向不存在的数据」
		if !version.DeleteMarker {
			e.removeVersionData(ctx, bucket, object, version)
		}
		return DeleteResult{VersionID: versionID}, nil
	}

	state, err := e.GetBucketVersioning(ctx, bucket)
	if err != nil {
		return DeleteResult{}, err
	}
	meta, err := e.readMetaQuorum(ctx, bucket, object)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return DeleteResult{}, err
	}
	switch state {
	case VersioningEnabled, VersioningSuspended:
		id := NullVersionID
		old := meta
		var replaced []fsVersion
		if state == VersioningEnabled {
			id, err = newVersionID()
			if err != nil {
				return DeleteResult{}, err
			}
		} else {
			meta, replaced = dropVersionMeta(meta, NullVersionID)
		}
		marker := fsVersion{VersionID: id, DeleteMarker: true, ModTime: time.Now().UTC()}
		meta.Name = object
		meta.Versions = append([]fsVersion{marker}, meta.Versions...)
		if err := e.writeMetaAll(ctx, bucket, object, meta); err != nil {
			e.restoreMeta(ctx, bucket, object, old)
			return DeleteResult{}, err
		}
		for _, v := range replaced {
			e.removeVersionData(ctx, bucket, object, v)
		}
		return DeleteResult{VersionID: id, DeleteMarker: true}, nil
	default:
		if errors.Is(err, ErrObjectNotFound) {
			return DeleteResult{VersionID: NullVersionID}, nil
		}
		old := meta
		var removed []fsVersion
		meta, removed = dropVersionMeta(meta, NullVersionID)
		if err := e.writeOrRemoveMeta(ctx, bucket, object, old, meta); err != nil {
			return DeleteResult{}, err
		}
		for _, v := range removed {
			e.removeVersionData(ctx, bucket, object, v)
		}
		return DeleteResult{VersionID: NullVersionID}, nil
	}
}

func (e *Erasure) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete) []error {
	errs := make([]error, len(objects))
	for i, spec := range objects {
		if _, err := e.DeleteObject(ctx, bucket, spec.Object, spec.VersionID); err != nil {
			errs[i] = err
		}
	}
	return errs
}

func (e *Erasure) ListObjects(ctx context.Context, bucket string, opts ListOptions) (ListObjectsResult, error) {
	if !validBucketName(bucket) {
		return ListObjectsResult{}, ErrInvalidBucketName
	}
	if _, ok, err := e.BucketExists(ctx, bucket); err != nil {
		return ListObjectsResult{}, err
	} else if !ok {
		return ListObjectsResult{}, ErrBucketNotFound
	}
	metas, err := e.walkMetasQuorum(ctx, bucket)
	if err != nil {
		return ListObjectsResult{}, err
	}
	return buildObjectsResult(metas, bucket, opts), nil
}

func (e *Erasure) ListObjectVersions(ctx context.Context, bucket string, opts ListOptions) (ListVersionsResult, error) {
	if !validBucketName(bucket) {
		return ListVersionsResult{}, ErrInvalidBucketName
	}
	if _, ok, err := e.BucketExists(ctx, bucket); err != nil {
		return ListVersionsResult{}, err
	} else if !ok {
		return ListVersionsResult{}, ErrBucketNotFound
	}
	metas, err := e.walkMetasQuorum(ctx, bucket)
	if err != nil {
		return ListVersionsResult{}, err
	}
	return buildVersionsResult(metas, bucket, opts), nil
}

func (e *Erasure) NewMultipartUpload(ctx context.Context, bucket, object, contentType string, userMeta map[string]string) (string, error) {
	if !validBucketName(bucket) {
		return "", ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return "", ErrInvalidObjectName
	}
	if _, ok, err := e.BucketExists(ctx, bucket); err != nil {
		return "", err
	} else if !ok {
		return "", ErrBucketNotFound
	}
	state, err := e.GetBucketVersioning(ctx, bucket)
	if err != nil {
		return "", err
	}
	uploadID, err := newUploadID()
	if err != nil {
		return "", err
	}
	meta := fsUploadMeta{
		Bucket:            bucket,
		Object:            object,
		UploadID:          uploadID,
		ContentType:       contentType,
		UserMetadata:      userMeta,
		Initiated:         time.Now().UTC(),
		VersioningEnabled: state == VersioningEnabled,
	}
	// 上传元数据按写法定人数写到所有盘：任一节点存活都能继续这个上传（去掉 disks[0] 单点）。
	if err := e.writeUploadMeta(ctx, bucket, uploadID, meta); err != nil {
		return "", err
	}
	e.log.Info("[gos3: new-multipart-upload]", "bucket", bucket, "object", object, "upload-id", uploadID)
	return uploadID, nil
}

// PutObjectPart 上传一个分片：该 part 独立纠删编码后写到**所有盘**，
// 再把 part 元数据（号/大小/ETag/DataID）按写法定人数写到所有盘。
func (e *Erasure) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partNumber int, data io.Reader) (PartInfo, error) {
	if !validBucketName(bucket) {
		return PartInfo{}, ErrInvalidBucketName
	}
	if !validUploadID(uploadID) {
		return PartInfo{}, ErrInvalidUploadID
	}
	if partNumber < 1 || partNumber > 10000 {
		return PartInfo{}, ErrInvalidPart
	}
	if _, err := e.readUploadMeta(ctx, bucket, uploadID); err != nil {
		return PartInfo{}, err
	}
	// 同一个 (uploadID, partN) 串行化：并发重传同一 part 不会互相覆盖分片文件
	unlock, err := e.lockResource(ctx, partResource(bucket, object, uploadID, partNumber))
	if err != nil {
		return PartInfo{}, err
	}
	defer unlock()

	partBytes, err := io.ReadAll(data)
	if err != nil {
		return PartInfo{}, err
	}
	dataID, err := newVersionID()
	if err != nil {
		return PartInfo{}, err
	}
	part := fsPartMeta{
		PartNumber: partNumber,
		ETag:       md5hex(partBytes),
		Size:       int64(len(partBytes)),
		ModTime:    time.Now().UTC(),
		DataID:     dataID,
	}
	// 1) part 分片先写临时路径再 rename 提交（与对象写同一条两阶段路径）
	if err := e.stageAndCommitPart(ctx, bucket, uploadID, partNumber, dataID, partBytes); err != nil {
		return PartInfo{}, err
	}
	// 2) part 元数据按写法定人数写所有盘；失败则回滚分片，避免留下没有元数据的分片
	metaData, err := json.Marshal(part)
	if err != nil {
		return PartInfo{}, err
	}
	ok := 0
	var errs []error
	for _, d := range e.disks {
		if err := d.WriteFile(ctx, diskPartMetaPath(bucket, uploadID, partNumber), metaData); err != nil {
			errs = append(errs, err)
			continue
		}
		ok++
	}
	if ok < e.writeQuorum() {
		e.deleteOnAllDisks(ctx, diskUploadPartPath(bucket, uploadID, partNumber, dataID))
		return PartInfo{}, quorumError(ErrWriteQuorum, "part meta", bucket, uploadID, ok, e.writeQuorum(), errs)
	}
	e.log.Info("[gos3: upload-part]", "bucket", bucket, "object", object, "upload-id", uploadID, "part", partNumber, "size", part.Size)
	return PartInfo{PartNumber: part.PartNumber, ETag: part.ETag, Size: part.Size, LastModified: part.ModTime}, nil
}

// stageAndCommitPart 把一个 part 编码后两阶段写到所有盘（临时路径 -> 最终路径）。
func (e *Erasure) stageAndCommitPart(ctx context.Context, bucket, uploadID string, partNumber int, dataID string, partBytes []byte) error {
	shards, err := e.encoder.Encode(partBytes)
	if err != nil {
		return err
	}
	tmpPath := diskUploadPartTmpPath(bucket, uploadID, partNumber, dataID)
	finalPath := diskUploadPartPath(bucket, uploadID, partNumber, dataID)

	staged := 0
	var errs []error
	for i, shard := range shards {
		if i >= len(e.disks) {
			break
		}
		if err := e.disks[i].WriteFile(ctx, tmpPath, shard); err != nil {
			errs = append(errs, err)
			e.log.Warn("[gos3: part-stage-failed]", "disk", e.disks[i].ID(), "error", err.Error())
			continue
		}
		staged++
	}
	if staged < e.writeQuorum() {
		e.deleteOnAllDisks(ctx, tmpPath)
		return quorumError(ErrWriteQuorum, "stage part", bucket, uploadID, staged, e.writeQuorum(), errs)
	}

	committed := 0
	for i := range shards {
		if i >= len(e.disks) {
			break
		}
		if err := e.disks[i].Rename(ctx, tmpPath, finalPath); err != nil {
			errs = append(errs, err)
			continue
		}
		committed++
	}
	if committed < e.writeQuorum() {
		e.deleteOnAllDisks(ctx, finalPath)
		e.deleteOnAllDisks(ctx, tmpPath)
		return quorumError(ErrWriteQuorum, "commit part", bucket, uploadID, committed, e.writeQuorum(), errs)
	}
	return nil
}

func (e *Erasure) ListObjectParts(ctx context.Context, bucket, object, uploadID string) ([]PartInfo, error) {
	if !validBucketName(bucket) {
		return nil, ErrInvalidBucketName
	}
	if !validUploadID(uploadID) {
		return nil, ErrInvalidUploadID
	}
	if _, err := e.readUploadMeta(ctx, bucket, uploadID); err != nil {
		return nil, err
	}
	metas, err := e.readPartMetas(ctx, bucket, uploadID)
	if err != nil {
		return nil, err
	}
	parts := make([]PartInfo, 0, len(metas))
	for _, m := range metas {
		parts = append(parts, PartInfo{PartNumber: m.PartNumber, ETag: m.ETag, Size: m.Size, LastModified: m.ModTime})
	}
	return parts, nil
}

// CompleteMultipartUpload 合并分片为最终对象（纠删码后端）。
// 与旧实现不同：这里**不重新编码**，而是把各 part 的分片文件从上传暂存目录 rename 到
// 对象数据目录（part.<n>），再提交对象元数据；失败会回滚 rename，上传仍然可用。
// 复合 ETag 格式为 <各分片 md5 拼接后再 md5>-<分片数>。
func (e *Erasure) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []CompletePart) (ObjectInfo, error) {
	if !validBucketName(bucket) {
		return ObjectInfo{}, ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return ObjectInfo{}, ErrInvalidObjectName
	}
	if !validUploadID(uploadID) {
		return ObjectInfo{}, ErrInvalidUploadID
	}
	// 命名空间写锁：完成分片上传会写对象数据与元数据，必须与同 key 的普通写互斥
	unlock, err := e.lockObject(ctx, bucket, object)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer unlock()

	upload, err := e.readUploadMeta(ctx, bucket, uploadID)
	if err != nil {
		return ObjectInfo{}, err
	}
	if len(parts) == 0 {
		return ObjectInfo{}, ErrInvalidPart
	}
	for i := 1; i < len(parts); i++ {
		if parts[i].PartNumber <= parts[i-1].PartNumber {
			return ObjectInfo{}, ErrInvalidPartOrder
		}
	}

	// 1) 用 quorum 归约出的 part 元数据校验客户端提交的分片列表（不再读回分片字节）
	stored, err := e.readPartMetas(ctx, bucket, uploadID)
	if err != nil {
		return ObjectInfo{}, err
	}
	byNumber := make(map[int]fsPartMeta, len(stored))
	for _, m := range stored {
		byNumber[m.PartNumber] = m
	}
	refs := make([]fsPartRef, 0, len(parts))
	composite := md5.New()
	var total int64
	for _, part := range parts {
		m, ok := byNumber[part.PartNumber]
		if !ok || m.DataID == "" || m.Size == 0 {
			return ObjectInfo{}, ErrInvalidPart
		}
		if part.ETag != "" && !strings.EqualFold(stripQuotes(part.ETag), m.ETag) {
			return ObjectInfo{}, ErrInvalidPart
		}
		sum, err := hex.DecodeString(m.ETag)
		if err != nil {
			return ObjectInfo{}, ErrInvalidPart
		}
		composite.Write(sum)
		total += m.Size
		refs = append(refs, fsPartRef{Number: m.PartNumber, Size: m.Size, ETag: m.ETag})
	}

	versionID := NullVersionID
	if upload.VersioningEnabled {
		versionID, err = newVersionID()
		if err != nil {
			return ObjectInfo{}, err
		}
	}
	objDataID, err := newVersionID()
	if err != nil {
		return ObjectInfo{}, err
	}
	version := fsVersion{
		VersionID:    versionID,
		DataID:       objDataID,
		Size:         total,
		ETag:         hex.EncodeToString(composite.Sum(nil)) + "-" + strconv.Itoa(len(parts)),
		ContentType:  upload.ContentType,
		UserMetadata: upload.UserMetadata,
		ModTime:      time.Now().UTC(),
		Parts:        refs,
	}

	// 2) 读取旧元数据（用于覆盖 null 版本与失败回滚）
	oldMeta, err := e.readMetaQuorum(ctx, bucket, object)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return ObjectInfo{}, err
	}
	meta := oldMeta
	var stale []fsVersion
	if versionID == NullVersionID {
		meta, stale = dropVersionMeta(meta, NullVersionID)
	}

	// 3) 把 part 分片 rename 到对象数据目录（每个盘要么全部成功，要么不算数）
	moved, committed := e.movePartsToObject(ctx, bucket, object, uploadID, objDataID, refs, byNumber)
	if committed < e.writeQuorum() {
		e.rollbackMoves(ctx, moved)
		e.deleteDirOnAllDisks(ctx, diskObjectDataDir(bucket, object, objDataID))
		return ObjectInfo{}, quorumError(ErrWriteQuorum, "commit parts", bucket, object, committed, e.writeQuorum(), nil)
	}

	// 4) 提交对象元数据；失败则回滚数据（把分片搬回上传目录），上传仍可重试
	meta.Name = object
	meta.Versions = append([]fsVersion{version}, meta.Versions...)
	if err := e.writeMetaAll(ctx, bucket, object, meta); err != nil {
		e.rollbackMoves(ctx, moved)
		e.deleteDirOnAllDisks(ctx, diskObjectDataDir(bucket, object, objDataID))
		e.restoreMeta(ctx, bucket, object, oldMeta)
		return ObjectInfo{}, err
	}

	// 5) 提交成功后清理上传暂存目录与被覆盖的旧版本数据
	for _, d := range e.disks {
		_ = d.DeleteDir(ctx, diskUploadDir(bucket, uploadID))
	}
	for _, v := range stale {
		e.removeVersionData(ctx, bucket, object, v)
	}
	e.log.Info("[gos3: complete-multipart-upload]", "bucket", bucket, "object", object, "version", versionID, "size", total, "parts", len(refs))
	return version.toInfo(bucket, object), nil
}

// movedPart 记录一次 part 分片 rename 的源/目标，便于失败时回滚。
type movedPart struct {
	disk disk.Disk
	src  string
	dst  string
}

// movePartsToObject 把所有 part 的分片文件从上传暂存目录搬到对象数据目录。
// 返回搬迁明细与「所有 part 都搬成功的盘数」：某个盘只要有一个 part 失败，该盘就不计入。
func (e *Erasure) movePartsToObject(ctx context.Context, bucket, object, uploadID, objDataID string, refs []fsPartRef, byNumber map[int]fsPartMeta) ([]movedPart, int) {
	moved := make([]movedPart, 0, len(e.disks)*len(refs))
	committedDisks := 0
	for _, d := range e.disks {
		ok := true
		for _, ref := range refs {
			m := byNumber[ref.Number]
			src := diskUploadPartPath(bucket, uploadID, ref.Number, m.DataID)
			dst := diskObjectPartPath(bucket, object, objDataID, ref.Number)
			if err := d.Rename(ctx, src, dst); err != nil {
				ok = false
				e.log.Warn("[gos3: part-move-failed]", "disk", d.ID(), "part", ref.Number, "error", err.Error())
				continue
			}
			moved = append(moved, movedPart{disk: d, src: src, dst: dst})
		}
		if ok {
			committedDisks++
		}
	}
	return moved, committedDisks
}

// rollbackMoves 尽力把已搬迁的 part 分片搬回上传暂存目录。
func (e *Erasure) rollbackMoves(ctx context.Context, moved []movedPart) {
	for _, m := range moved {
		if m.src == "" || m.dst == "" {
			continue
		}
		if err := m.disk.Rename(ctx, m.dst, m.src); err != nil {
			e.log.Warn("[gos3: part-move-rollback-failed]", "disk", m.disk.ID(), "src", m.src, "error", err.Error())
		}
	}
}

func (e *Erasure) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	if !validBucketName(bucket) {
		return ErrInvalidBucketName
	}
	if !validUploadID(uploadID) {
		return ErrInvalidUploadID
	}
	if _, err := e.readUploadMeta(ctx, bucket, uploadID); err != nil {
		return err
	}
	// 所有盘一起清理，不再只看 disks[0]
	ok := 0
	var errs []error
	for _, d := range e.disks {
		if err := d.DeleteDir(ctx, diskUploadDir(bucket, uploadID)); err != nil {
			errs = append(errs, err)
			continue
		}
		ok++
	}
	if ok < e.writeQuorum() {
		return quorumError(ErrWriteQuorum, "abort upload", bucket, uploadID, ok, e.writeQuorum(), errs)
	}
	e.log.Info("[gos3: abort-multipart-upload]", "bucket", bucket, "object", object, "upload-id", uploadID)
	return nil
}

// ListMultipartUploads 汇总所有盘上的上传，只返回达到读法定人数的那些。
func (e *Erasure) ListMultipartUploads(ctx context.Context, bucket string) ([]MultipartInfo, error) {
	if !validBucketName(bucket) {
		return nil, ErrInvalidBucketName
	}
	byID, err := e.walkUploads(ctx, bucket)
	if err != nil {
		return nil, err
	}
	uploads := make([]MultipartInfo, 0, len(byID))
	for _, m := range byID {
		uploads = append(uploads, MultipartInfo{Bucket: bucket, Object: m.Object, UploadID: m.UploadID, Initiated: m.Initiated})
	}
	sort.Slice(uploads, func(i, j int) bool { return uploads[i].Object < uploads[j].Object })
	return uploads, nil
}

// walkUploads 汇总各盘上的分片上传（同一 uploadID 需达到读法定人数才算数）。
func (e *Erasure) walkUploads(ctx context.Context, bucket string) (map[string]fsUploadMeta, error) {
	counts := map[string]int{}
	metas := map[string]fsUploadMeta{}
	listed := 0
	for _, d := range e.disks {
		entries, err := d.ListDir(ctx, path.Join(multipartDir, bucket))
		if err != nil {
			continue
		}
		listed++
		for _, en := range entries {
			if !en.IsDir || !validUploadID(en.Name) {
				continue
			}
			meta, err := readUploadMetaFromDisk(ctx, d, bucket, en.Name)
			if err != nil {
				continue
			}
			counts[meta.UploadID]++
			if _, ok := metas[meta.UploadID]; !ok {
				metas[meta.UploadID] = meta
			}
		}
	}
	if listed < e.readQuorum() {
		return nil, fmt.Errorf("%w: list uploads %s (listable drives %d/%d)", ErrReadQuorum, bucket, listed, e.readQuorum())
	}
	out := map[string]fsUploadMeta{}
	for id, c := range counts {
		if c >= e.readQuorum() {
			out[id] = metas[id]
		}
	}
	return out, nil
}

// CleanupStaleUploads 清理各盘上超过 olderThan 仍未完成的分片上传。
func (e *Erasure) CleanupStaleUploads(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoff := time.Now().Add(-olderThan)
	// 先按 bucket 汇总「哪些盘上有哪些上传」
	type uploadKey struct{ bucket, id string }
	keys := map[uploadKey]struct{}{}
	for _, d := range e.disks {
		_, dirs, err := d.Walk(ctx, multipartDir)
		if err != nil {
			continue
		}
		for _, dir := range dirs {
			parts := strings.Split(dir, "/")
			if len(parts) != 2 || !validBucketName(parts[0]) || !validUploadID(parts[1]) {
				continue
			}
			keys[uploadKey{parts[0], parts[1]}] = struct{}{}
		}
	}
	removed := 0
	for key := range keys {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		// 以任意一块可读盘上的元数据判断是否过期；读不到就跳过（宁可不删）
		var meta fsUploadMeta
		found := false
		for _, d := range e.disks {
			m, err := readUploadMetaFromDisk(ctx, d, key.bucket, key.id)
			if err != nil {
				continue
			}
			meta, found = m, true
			break
		}
		if !found || meta.Initiated.After(cutoff) {
			continue
		}
		for _, d := range e.disks {
			_ = d.DeleteDir(ctx, diskUploadDir(key.bucket, key.id))
		}
		removed++
		e.log.Info("[gos3: cleanup-stale-upload]", "bucket", key.bucket, "upload-id", key.id, "initiated", meta.Initiated.Format(time.RFC3339))
	}
	return removed, nil
}

// readUploadMetaFromDisk 从单块盘读取上传元数据。
func readUploadMetaFromDisk(ctx context.Context, d disk.Disk, bucket, uploadID string) (fsUploadMeta, error) {
	data, err := d.ReadFile(ctx, diskUploadMetaPath(bucket, uploadID))
	if errors.Is(err, disk.ErrNotExist) {
		return fsUploadMeta{}, ErrUploadNotFound
	}
	if err != nil {
		return fsUploadMeta{}, err
	}
	var meta fsUploadMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return fsUploadMeta{}, err
	}
	return meta, nil
}

// putBytes 是纠删码后端的核心写路径：把一个完整对象编码成分片并落盘，
// 再写入元数据完成提交。被 PutObject 与 CompleteMultipartUpload 复用。
//
// 参数：
//   - versionID:   目标版本号（未开启版本控制时为 NullVersionID="null"）
//   - raw:         对象完整字节（已由调用方读入内存）
//   - contentType/userMeta: 对象类型与用户元数据
//   - etag:        对象 ETag
//   - replaceNull: 是否覆盖已有的 "null" 版本（未开启版本控制时的覆盖语义）
//
// 返回：写入成功的对象信息 ObjectInfo。
func (e *Erasure) putBytes(ctx context.Context, bucket, object, versionID string, raw []byte, contentType string, userMeta map[string]string, etag string, replaceNull bool) (ObjectInfo, error) {
	// 1) 读取该对象的权威元数据（quorum 归约）；不存在则视为新建，用零值 meta 继续
	meta, err := e.readMetaQuorum(ctx, bucket, object)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return ObjectInfo{}, err
	}
	oldMeta := meta
	// 2) 覆盖写：先把旧的 "null" 版本从元数据里摘掉（数据留到提交成功后再删）
	var stale []fsVersion
	if replaceNull {
		meta, stale = dropVersionMeta(meta, NullVersionID)
	}

	// 3) 纠删码编码：把 raw 切成 dataShards 份数据分片，并计算 parityShards 份校验分片
	shards, err := e.encoder.Encode(raw)
	if err != nil {
		return ObjectInfo{}, err
	}

	// 4) 新数据使用独立的 DataID：即便是 "null" 版本，也不会和旧数据落在同一路径上，
	//    因此「先写新版本 -> 提交 -> 再删旧版本」全程没有覆盖窗口。
	dataID, err := newVersionID()
	if err != nil {
		return ObjectInfo{}, err
	}
	version := fsVersion{
		VersionID:    versionID,
		DataID:       dataID,
		Size:         int64(len(raw)),
		ETag:         etag,
		ContentType:  contentType,
		UserMetadata: userMeta,
		ModTime:      time.Now().UTC(),
		// 普通 PUT 就是一个 part；与分片上传完成后的对象布局保持一致（数据目录下 part.<n>）
		Parts: []fsPartRef{{Number: 1, Size: int64(len(raw)), ETag: etag}},
	}

	// 5) 第一阶段：所有分片先写临时路径
	tmpPath := diskObjectPartTmpPath(bucket, object, dataID, 1)
	finalPath := diskObjectPartPath(bucket, object, dataID, 1)
	staged := 0
	var errs []error
	for i, shard := range shards {
		if i >= len(e.disks) {
			break
		}
		if err := e.disks[i].WriteFile(ctx, tmpPath, shard); err != nil {
			errs = append(errs, err)
			e.log.Warn("[gos3: erasure-stage-shard-failed]", "disk", e.disks[i].ID(), "error", err.Error())
			continue
		}
		staged++
	}
	if staged < e.writeQuorum() {
		e.deleteOnAllDisks(ctx, tmpPath)
		return ObjectInfo{}, quorumError(ErrWriteQuorum, "stage shards", bucket, object, staged, e.writeQuorum(), errs)
	}

	// 6) 第二阶段：rename 到最终路径（本地/远程都是同盘原子 rename），完成数据提交
	committed := 0
	for i := range shards {
		if i >= len(e.disks) {
			break
		}
		if err := e.disks[i].Rename(ctx, tmpPath, finalPath); err != nil {
			errs = append(errs, err)
			continue
		}
		committed++
	}
	if committed < e.writeQuorum() {
		// 回滚：删掉已提交的新数据与残留临时文件，旧版本的数据与元数据都还在
		e.deleteDirOnAllDisks(ctx, diskObjectDataDir(bucket, object, dataID))
		e.deleteOnAllDisks(ctx, tmpPath)
		return ObjectInfo{}, quorumError(ErrWriteQuorum, "commit shards", bucket, object, committed, e.writeQuorum(), errs)
	}

	// 7) 提交元数据：同样要求写法定人数；失败则回滚新数据并尽力恢复旧元数据
	meta.Name = object
	meta.Versions = append([]fsVersion{version}, meta.Versions...)
	if err := e.writeMetaAll(ctx, bucket, object, meta); err != nil {
		e.deleteDirOnAllDisks(ctx, diskObjectDataDir(bucket, object, dataID))
		e.restoreMeta(ctx, bucket, object, oldMeta)
		return ObjectInfo{}, err
	}

	// 8) 提交成功后清理被替换的旧版本数据；清理失败只留下孤立文件，不影响一致性
	for _, v := range stale {
		e.removeVersionData(ctx, bucket, object, v)
	}
	return version.toInfo(bucket, object), nil
}

// quorumError 组装一个可判定的 quorum 错误：带上错误归约结果（多数盘的真实原因）。
func quorumError(sentinel error, stage, bucket, object string, got, want int, errs []error) error {
	if reduced := reduceErrs(errs); reduced != nil {
		return fmt.Errorf("%w: %s %s/%s (%d/%d): %v", sentinel, stage, bucket, object, got, want, reduced)
	}
	return fmt.Errorf("%w: %s %s/%s (%d/%d)", sentinel, stage, bucket, object, got, want)
}

// writeMetaAll 把对象元数据（json）写入所有磁盘，要求成功数 >= 写法定人数。
// 元数据全盘冗余，读取时按 quorum 归约（见 readMetaQuorum）。
func (e *Erasure) writeMetaAll(ctx context.Context, bucket, object string, meta fsMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	ok := 0
	var errs []error
	for _, d := range e.disks {
		if err := d.WriteFile(ctx, diskMetaPath(bucket, object), data); err != nil {
			errs = append(errs, err)
			continue
		}
		ok++
	}
	if ok < e.writeQuorum() {
		return quorumError(ErrWriteQuorum, "meta", bucket, object, ok, e.writeQuorum(), errs)
	}
	return nil
}

// restoreMeta 尽力把旧元数据写回所有盘（回滚失败的部分写入），避免「失败的写入」被读到。
func (e *Erasure) restoreMeta(ctx context.Context, bucket, object string, old fsMeta) {
	if len(old.Versions) == 0 {
		e.deleteOnAllDisks(ctx, diskMetaPath(bucket, object))
		return
	}
	if err := e.writeMetaAll(ctx, bucket, object, old); err != nil {
		e.log.Warn("[gos3: erasure-meta-rollback-failed]", "bucket", bucket, "object", object, "error", err.Error())
	}
}

// writeOrRemoveMeta 根据是否还有版本决定写元数据还是从所有磁盘删除元数据文件。
// 两种动作都要求达到写法定人数，否则旧元数据会被尽力恢复。
func (e *Erasure) writeOrRemoveMeta(ctx context.Context, bucket, object string, old, meta fsMeta) error {
	if len(meta.Versions) > 0 {
		return e.writeMetaAll(ctx, bucket, object, meta)
	}
	ok := 0
	var errs []error
	for _, d := range e.disks {
		if err := d.DeleteFile(ctx, diskMetaPath(bucket, object)); err != nil {
			errs = append(errs, err)
			continue
		}
		ok++
	}
	if ok < e.writeQuorum() {
		e.restoreMeta(ctx, bucket, object, old)
		return quorumError(ErrWriteQuorum, "remove meta", bucket, object, ok, e.writeQuorum(), errs)
	}
	return nil
}

// removeVersionData 删除某版本对象在每一块磁盘上的分片数据文件（忽略单盘错误）。
// part 化对象删整个数据目录，历史对象删单个分片文件。
func (e *Erasure) removeVersionData(ctx context.Context, bucket, object string, version fsVersion) {
	if len(version.Parts) == 0 {
		e.deleteOnAllDisks(ctx, diskDataPath(bucket, object, version.dataKey()))
		return
	}
	e.deleteDirOnAllDisks(ctx, diskObjectDataDir(bucket, object, version.dataKey()))
}

// deleteOnAllDisks 在所有磁盘上删除同一路径（忽略单盘错误）：用于回滚临时/已提交数据。
func (e *Erasure) deleteOnAllDisks(ctx context.Context, diskPath string) {
	for _, d := range e.disks {
		_ = d.DeleteFile(ctx, diskPath)
	}
}

// deleteDirOnAllDisks 在所有磁盘上删除同一目录（忽略单盘错误）。
func (e *Erasure) deleteDirOnAllDisks(ctx context.Context, diskPath string) {
	for _, d := range e.disks {
		_ = d.DeleteDir(ctx, diskPath)
	}
}

// readShardsAt 读取「同一路径」在各盘上的分片：缺失位置为 nil，返回成功读到的分片数。
// 供按 part 解码与 legacy 单块解码共用。
func (e *Erasure) readShardsAt(ctx context.Context, diskPath string) ([][]byte, int) {
	shards := make([][]byte, e.encoder.Shards())
	present := 0
	for i, d := range e.disks {
		if i >= len(shards) {
			break
		}
		data, err := d.ReadFile(ctx, diskPath)
		if err != nil {
			continue
		}
		shards[i] = data
		present++
	}
	return shards, present
}

// readVersionData 读出并解码某版本的全部数据：
//   - 有 Parts 描述时逐个 part 解码再拼接（每个 part 都要满足读法定人数）；
//   - 历史对象（无 Parts）按单一分片文件解码。
//
// 读取过程中发现某块盘缺分片时，会把修复任务交给 healer（后台异步补齐）。
func (e *Erasure) readVersionData(ctx context.Context, bucket, object string, version fsVersion) ([]byte, error) {
	if len(version.Parts) == 0 {
		path := diskDataPath(bucket, object, version.dataKey())
		shards, present := e.readShardsAt(ctx, path)
		if present < e.readQuorum() {
			return nil, fmt.Errorf("%w: %s/%s (%d/%d shards)", ErrReadQuorum, bucket, object, present, e.readQuorum())
		}
		e.scheduleHeal(bucket, object, version.dataKey(), heal.LegacyPart, version.Size, shards)
		return e.encoder.Decode(shards, int(version.Size))
	}
	var buf bytes.Buffer
	for _, part := range version.Parts {
		path := diskObjectPartPath(bucket, object, version.dataKey(), part.Number)
		shards, present := e.readShardsAt(ctx, path)
		if present < e.readQuorum() {
			return nil, fmt.Errorf("%w: %s/%s part %d (%d/%d shards)", ErrReadQuorum, bucket, object, part.Number, present, e.readQuorum())
		}
		e.scheduleHeal(bucket, object, version.dataKey(), part.Number, part.Size, shards)
		raw, err := e.encoder.Decode(shards, int(part.Size))
		if err != nil {
			return nil, err
		}
		buf.Write(raw)
	}
	return buf.Bytes(), nil
}

// scheduleHeal 把缺失分片的盘加入修复队列（healer 未启用时什么都不做）。
func (e *Erasure) scheduleHeal(bucket, object, dataKey string, partNumber int, size int64, shards [][]byte) {
	if e.healer == nil {
		return
	}
	for i, shard := range shards {
		if shard != nil {
			continue
		}
		e.healer.Enqueue(heal.Task{
			Bucket:     bucket,
			Object:     object,
			DataKey:    dataKey,
			PartNumber: partNumber,
			Size:       size,
			DiskIndex:  i,
		})
	}
}

// shardPathForTask 返回修复任务对应的分片路径。
func shardPathForTask(t heal.Task) string {
	if t.PartNumber == heal.LegacyPart {
		return diskDataPath(t.Bucket, t.Object, t.DataKey)
	}
	return diskObjectPartPath(t.Bucket, t.Object, t.DataKey, t.PartNumber)
}

// Rebuild 实现 heal.Rebuilder：从其余盘读分片、解码出原始数据、重新编码，
// 并把目标盘缺失的那一份分片写回。目标盘上的其它副本不受影响。
func (e *Erasure) Rebuild(ctx context.Context, t heal.Task) error {
	if t.DiskIndex < 0 || t.DiskIndex >= len(e.disks) {
		return fmt.Errorf("heal: disk index %d out of range", t.DiskIndex)
	}
	path := shardPathForTask(t)
	shards, present := e.readShardsAt(ctx, path)
	if present < e.readQuorum() {
		return fmt.Errorf("%w: cannot rebuild %s/%s part %d (%d/%d shards)", ErrReadQuorum, t.Bucket, t.Object, t.PartNumber, present, e.readQuorum())
	}
	raw, err := e.encoder.Decode(shards, int(t.Size))
	if err != nil {
		return fmt.Errorf("heal decode: %w", err)
	}
	full, err := e.encoder.Encode(raw)
	if err != nil {
		return fmt.Errorf("heal encode: %w", err)
	}
	if t.DiskIndex >= len(full) {
		return fmt.Errorf("heal: shard %d missing after encode", t.DiskIndex)
	}
	if err := e.disks[t.DiskIndex].WriteFile(ctx, path, full[t.DiskIndex]); err != nil {
		return fmt.Errorf("heal write: %w", err)
	}
	return nil
}

// uploadSig 是上传元数据的签名，用于在多数派中挑选权威的一份。
func uploadSig(m fsUploadMeta) string {
	return m.UploadID + "|" + m.Initiated.UTC().Format(time.RFC3339Nano)
}

// readUploadMeta 以 quorum 方式读取上传元数据：
// 达到读法定人数的签名才作数；没有签名达到读法定人数但「确认不存在」的盘达到写法定人数时返回 ErrUploadNotFound。
func (e *Erasure) readUploadMeta(ctx context.Context, bucket, uploadID string) (fsUploadMeta, error) {
	type result struct {
		meta fsUploadMeta
		err  error
	}
	results := make([]result, len(e.disks))
	var wg sync.WaitGroup
	for i, d := range e.disks {
		wg.Add(1)
		go func(i int, d disk.Disk) {
			defer wg.Done()
			meta, err := readUploadMetaFromDisk(ctx, d, bucket, uploadID)
			results[i] = result{meta: meta, err: err}
		}(i, d)
	}
	wg.Wait()

	counts := map[string]int{}
	bySig := map[string]fsUploadMeta{}
	absent := 0
	for _, r := range results {
		switch {
		case errors.Is(r.err, ErrUploadNotFound):
			absent++
		case r.err != nil:
			// 盘不可达：不计入
		default:
			sig := uploadSig(r.meta)
			counts[sig]++
			if _, ok := bySig[sig]; !ok {
				bySig[sig] = r.meta
			}
		}
	}
	best, bestCount := fsUploadMeta{}, 0
	for sig, c := range counts {
		if c > bestCount {
			best, bestCount = bySig[sig], c
		}
	}
	if bestCount >= e.readQuorum() {
		return best, nil
	}
	if absent >= e.writeQuorum() {
		return fsUploadMeta{}, ErrUploadNotFound
	}
	return fsUploadMeta{}, fmt.Errorf("%w: upload %s (metas %d/%d)", ErrReadQuorum, uploadID, bestCount, e.readQuorum())
}

// writeUploadMeta 把上传元数据按写法定人数写到所有盘。
func (e *Erasure) writeUploadMeta(ctx context.Context, bucket, uploadID string, meta fsUploadMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	ok := 0
	var errs []error
	for _, d := range e.disks {
		if err := d.WriteFile(ctx, diskUploadMetaPath(bucket, uploadID), data); err != nil {
			errs = append(errs, err)
			continue
		}
		ok++
	}
	if ok < e.writeQuorum() {
		return quorumError(ErrWriteQuorum, "upload meta", bucket, uploadID, ok, e.writeQuorum(), errs)
	}
	return nil
}

// readPartMetas 汇总所有盘上的 part 元数据，按 part 号取达到读法定人数的那一份。
func (e *Erasure) readPartMetas(ctx context.Context, bucket, uploadID string) ([]fsPartMeta, error) {
	uploadDir := diskUploadDir(bucket, uploadID)
	byNumber := map[int][]fsPartMeta{}
	listed := 0
	for _, d := range e.disks {
		entries, err := d.ListDir(ctx, uploadDir)
		if err != nil {
			continue
		}
		listed++
		for _, en := range entries {
			if en.IsDir || !strings.HasPrefix(en.Name, "part.") || !strings.HasSuffix(en.Name, ".json") {
				continue
			}
			data, err := d.ReadFile(ctx, path.Join(uploadDir, en.Name))
			if err != nil {
				continue
			}
			var m fsPartMeta
			if json.Unmarshal(data, &m) != nil {
				continue
			}
			byNumber[m.PartNumber] = append(byNumber[m.PartNumber], m)
		}
	}
	if listed < e.readQuorum() {
		return nil, fmt.Errorf("%w: list parts %s (listable drives %d/%d)", ErrReadQuorum, uploadID, listed, e.readQuorum())
	}
	out := make([]fsPartMeta, 0, len(byNumber))
	for _, metas := range byNumber {
		if m, ok := pickQuorumPartMeta(metas, e.readQuorum()); ok {
			out = append(out, m)
		} else {
			e.log.Info("[gos3: list-skip-unquorum-part]", "upload-id", uploadID, "metas", len(metas))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PartNumber < out[j].PartNumber })
	return out, nil
}

func diskWalkMetas(ctx context.Context, d disk.Disk, bucket string) ([]objectMetaEntry, error) {
	files, _, err := d.Walk(ctx, diskBucketMetaDir(bucket))
	if err != nil {
		return nil, err
	}
	var out []objectMetaEntry
	for _, f := range files {
		base := path.Base(f)
		if strings.HasPrefix(base, ".") || !strings.HasSuffix(base, ".json") {
			continue
		}
		data, err := d.ReadFile(ctx, path.Join(diskBucketMetaDir(bucket), f))
		if err != nil {
			continue
		}
		var meta fsMeta
		if json.Unmarshal(data, &meta) != nil {
			continue
		}
		out = append(out, objectMetaEntry{key: strings.TrimSuffix(f, ".json"), meta: meta})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out, nil
}

func diskReadMeta(ctx context.Context, d disk.Disk, bucket, object string) (fsMeta, error) {
	data, err := d.ReadFile(ctx, diskMetaPath(bucket, object))
	if errors.Is(err, disk.ErrNotExist) {
		return fsMeta{}, ErrObjectNotFound
	}
	if err != nil {
		return fsMeta{}, err
	}
	var meta fsMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return fsMeta{}, err
	}
	return meta, nil
}

// diskMetaPath 返回对象元数据在单块磁盘上的路径：<metaDir>/<bucket>/<object>.json
func diskMetaPath(bucket, object string) string {
	return path.Join(metaDir, bucket, object+".json")
}

func diskBucketMetaDir(bucket string) string {
	return path.Join(metaDir, bucket)
}

func diskBucketConfigPath(bucket string) string {
	return path.Join(metaDir, bucket, bucketConfigFile)
}

func diskLifecyclePath(bucket string) string {
	return path.Join(metaDir, bucket, ".lifecycle.json")
}

// diskDataPath 返回历史布局下某版本对象分片在单块磁盘上的路径：<dataDir>/<bucket>/<object>/<dataKey>
// （仅用于没有 Parts 描述的旧对象。）
func diskDataPath(bucket, object, dataKey string) string {
	return path.Join(dataDir, bucket, object, dataKey)
}

// diskObjectDataDir 返回某版本对象的数据目录：<dataDir>/<bucket>/<object>/<dataKey>
func diskObjectDataDir(bucket, object, dataKey string) string {
	return path.Join(dataDir, bucket, object, dataKey)
}

// diskObjectPartPath 返回对象某个 part 在单块磁盘上的分片路径：<数据目录>/part.<n>
func diskObjectPartPath(bucket, object, dataKey string, partNumber int) string {
	return path.Join(diskObjectDataDir(bucket, object, dataKey), fmt.Sprintf("part.%d", partNumber))
}

// diskObjectPartTmpPath 是对象分片两阶段写的临时路径：<数据目录>/.tmp/part.<n>
func diskObjectPartTmpPath(bucket, object, dataKey string, partNumber int) string {
	return path.Join(diskObjectDataDir(bucket, object, dataKey), tmpDir, fmt.Sprintf("part.%d", partNumber))
}

// diskTmpDataPath 返回历史布局（无 Parts）两阶段写的临时路径。
func diskTmpDataPath(bucket, object, dataKey string) string {
	return path.Join(dataDir, bucket, object, tmpDir, dataKey)
}

func diskUploadDir(bucket, uploadID string) string {
	return path.Join(multipartDir, bucket, uploadID)
}

func diskUploadMetaPath(bucket, uploadID string) string {
	return path.Join(diskUploadDir(bucket, uploadID), "meta.json")
}

// diskUploadPartPath 返回分片上传暂存目录里该盘持有的 part 分片：part.<n>.<dataID>
func diskUploadPartPath(bucket, uploadID string, partNumber int, dataID string) string {
	return path.Join(diskUploadDir(bucket, uploadID), fmt.Sprintf("part.%d.%s", partNumber, dataID))
}

// diskUploadPartTmpPath 是 part 分片两阶段写的临时路径：<uploadDir>/.tmp/part.<n>.<dataID>
func diskUploadPartTmpPath(bucket, uploadID string, partNumber int, dataID string) string {
	return path.Join(diskUploadDir(bucket, uploadID), tmpDir, fmt.Sprintf("part.%d.%s", partNumber, dataID))
}

func md5hex(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}

type bytesReadSeekCloser struct {
	*bytes.Reader
}

func (bytesReadSeekCloser) Close() error {
	return nil
}

func newReadSeekCloser(data []byte) io.ReadSeekCloser {
	return bytesReadSeekCloser{bytes.NewReader(data)}
}

// diskPartMetaPath 返回 part 元数据路径：<uploadDir>/part.<n>.json
func diskPartMetaPath(bucket, uploadID string, partNumber int) string {
	return path.Join(diskUploadDir(bucket, uploadID), fmt.Sprintf("part.%d.json", partNumber))
}
