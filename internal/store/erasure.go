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
	"time"

	"github.com/kxj/gos3/internal/disk"
	"github.com/kxj/gos3/internal/erasure"
	"github.com/kxj/gos3/internal/lifecycle"
)

type Erasure struct {
	disks        []disk.Disk
	encoder      *erasure.Encoder
	dataShards   int
	parityShards int
	log          *slog.Logger
}

func NewErasure(disks []disk.Disk, dataShards, parityShards int, log *slog.Logger) (*Erasure, error) {
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
	e := &Erasure{disks: disks, encoder: enc, dataShards: dataShards, parityShards: parityShards, log: log}
	ids := make([]string, len(disks))
	for i, d := range disks {
		ids[i] = d.ID()
	}
	log.Info("[gos3: erasure-ready]", "disks", len(disks), "data-shards", dataShards, "parity-shards", parityShards, "ids", strings.Join(ids, ","))
	return e, nil
}

func (e *Erasure) MakeBucket(ctx context.Context, bucket string) error {
	if !validBucketName(bucket) {
		return ErrInvalidBucketName
	}
	created, existed := 0, 0
	for _, d := range e.disks {
		fi, err := d.Stat(ctx, diskBucketMetaDir(bucket))
		if err != nil {
			return err
		}
		if fi.Exists {
			existed++
			continue
		}
		if err := d.MakeDir(ctx, diskBucketMetaDir(bucket)); err != nil {
			return err
		}
		created++
	}
	if created == 0 && existed == len(e.disks) {
		return ErrBucketExists
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

func (e *Erasure) bucketEmpty(ctx context.Context, bucket string) (bool, error) {
	var lastErr error
	for _, d := range e.disks {
		files, _, err := d.Walk(ctx, diskBucketMetaDir(bucket))
		if err != nil {
			lastErr = err
			continue
		}
		for _, f := range files {
			base := path.Base(f)
			if strings.HasPrefix(base, ".") || !strings.HasSuffix(base, ".json") {
				continue
			}
			return false, nil
		}
		return true, nil
	}
	return false, lastErr
}

func (e *Erasure) BucketExists(ctx context.Context, bucket string) (time.Time, bool, error) {
	if !validBucketName(bucket) {
		return time.Time{}, false, ErrInvalidBucketName
	}
	for _, d := range e.disks {
		fi, err := d.Stat(ctx, diskBucketMetaDir(bucket))
		if err != nil {
			continue
		}
		if fi.Exists {
			return fi.ModTime, true, nil
		}
	}
	return time.Time{}, false, nil
}

func (e *Erasure) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	for _, d := range e.disks {
		entries, err := d.ListDir(ctx, metaDir)
		if err != nil {
			continue
		}
		var buckets []BucketInfo
		for _, en := range entries {
			if !en.IsDir || !validBucketName(en.Name) {
				continue
			}
			fi, _ := d.Stat(ctx, diskBucketMetaDir(en.Name))
			buckets = append(buckets, BucketInfo{Name: en.Name, Created: fi.ModTime})
		}
		sort.Slice(buckets, func(i, j int) bool { return buckets[i].Name < buckets[j].Name })
		return buckets, nil
	}
	return nil, nil
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
	if ok < e.dataShards {
		return fmt.Errorf("versioning write quorum not reached (%d/%d)", ok, e.dataShards)
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
	if ok < e.dataShards {
		return fmt.Errorf("lifecycle write quorum not reached (%d/%d)", ok, e.dataShards)
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

func (e *Erasure) PutObject(ctx context.Context, bucket, object string, data io.Reader, size int64, contentType string, userMeta map[string]string) (ObjectInfo, error) {
	if !validBucketName(bucket) {
		return ObjectInfo{}, ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return ObjectInfo{}, ErrInvalidObjectName
	}
	if _, ok, err := e.BucketExists(ctx, bucket); err != nil {
		return ObjectInfo{}, err
	} else if !ok {
		return ObjectInfo{}, ErrBucketNotFound
	}
	state, err := e.GetBucketVersioning(ctx, bucket)
	if err != nil {
		return ObjectInfo{}, err
	}
	versionID, err := assignVersionID(state)
	if err != nil {
		return ObjectInfo{}, err
	}
	raw, err := io.ReadAll(data)
	if err != nil {
		return ObjectInfo{}, err
	}
	info, err := e.putBytes(ctx, bucket, object, versionID, raw, contentType, userMeta, md5hex(raw), versionID == NullVersionID)
	if err != nil {
		return ObjectInfo{}, err
	}
	e.log.Info("[gos3: erasure-put-object]", "bucket", bucket, "object", object, "version", versionID, "size", info.Size)
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
	meta, err := e.readMetaAny(ctx, bucket, object)
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
	shards, present := e.readShards(ctx, bucket, object, version.VersionID)
	if present < e.dataShards {
		return nil, ObjectInfo{}, fmt.Errorf("read quorum not reached (%d/%d shards)", present, e.dataShards)
	}
	raw, err := e.encoder.Decode(shards, int(version.Size))
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
	meta, err := e.readMetaAny(ctx, bucket, object)
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
	if versionID != "" {
		meta, err := e.readMetaAny(ctx, bucket, object)
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
		if !version.DeleteMarker {
			e.removeVersionData(ctx, bucket, object, version.VersionID)
		}
		meta.Versions = removeVersion(meta.Versions, versionID)
		if err := e.writeOrRemoveMeta(ctx, bucket, object, meta); err != nil {
			return DeleteResult{}, err
		}
		return DeleteResult{VersionID: versionID}, nil
	}

	state, err := e.GetBucketVersioning(ctx, bucket)
	if err != nil {
		return DeleteResult{}, err
	}
	meta, err := e.readMetaAny(ctx, bucket, object)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return DeleteResult{}, err
	}
	switch state {
	case VersioningEnabled, VersioningSuspended:
		id := NullVersionID
		if state == VersioningEnabled {
			id, err = newVersionID()
			if err != nil {
				return DeleteResult{}, err
			}
		} else {
			meta = e.dropVersion(ctx, bucket, object, meta, NullVersionID)
		}
		marker := fsVersion{VersionID: id, DeleteMarker: true, ModTime: time.Now().UTC()}
		meta.Name = object
		meta.Versions = append([]fsVersion{marker}, meta.Versions...)
		if err := e.writeMetaAll(ctx, bucket, object, meta); err != nil {
			return DeleteResult{}, err
		}
		return DeleteResult{VersionID: id, DeleteMarker: true}, nil
	default:
		if errors.Is(err, ErrObjectNotFound) {
			return DeleteResult{VersionID: NullVersionID}, nil
		}
		meta = e.dropVersion(ctx, bucket, object, meta, NullVersionID)
		meta.Versions = removeVersion(meta.Versions, NullVersionID)
		if err := e.writeOrRemoveMeta(ctx, bucket, object, meta); err != nil {
			return DeleteResult{}, err
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
	metas, err := e.walkMetas(ctx, bucket)
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
	metas, err := e.walkMetas(ctx, bucket)
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
	if err := e.writeUploadMeta(ctx, bucket, uploadID, meta); err != nil {
		return "", err
	}
	e.log.Info("[gos3: new-multipart-upload]", "bucket", bucket, "object", object, "upload-id", uploadID)
	return uploadID, nil
}

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
	partBytes, err := io.ReadAll(data)
	if err != nil {
		return PartInfo{}, err
	}
	part := fsPartMeta{
		PartNumber: partNumber,
		ETag:       md5hex(partBytes),
		Size:       int64(len(partBytes)),
		ModTime:    time.Now().UTC(),
	}
	if err := e.disks[0].WriteFile(ctx, diskPartPath(bucket, uploadID, partNumber), partBytes); err != nil {
		return PartInfo{}, err
	}
	metaData, _ := json.Marshal(part)
	if err := e.disks[0].WriteFile(ctx, diskPartMetaPath(bucket, uploadID, partNumber), metaData); err != nil {
		return PartInfo{}, err
	}
	e.log.Info("[gos3: upload-part]", "bucket", bucket, "object", object, "upload-id", uploadID, "part", partNumber, "size", len(partBytes))
	return PartInfo{PartNumber: partNumber, ETag: part.ETag, Size: part.Size, LastModified: part.ModTime}, nil
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
	entries, err := e.disks[0].ListDir(ctx, diskUploadDir(bucket, uploadID))
	if err != nil {
		return nil, err
	}
	var parts []PartInfo
	for _, en := range entries {
		if !strings.HasPrefix(en.Name, "part.") || !strings.HasSuffix(en.Name, ".json") {
			continue
		}
		data, err := e.disks[0].ReadFile(ctx, path.Join(diskUploadDir(bucket, uploadID), en.Name))
		if err != nil {
			continue
		}
		var part fsPartMeta
		if json.Unmarshal(data, &part) != nil {
			continue
		}
		parts = append(parts, PartInfo{PartNumber: part.PartNumber, ETag: part.ETag, Size: part.Size, LastModified: part.ModTime})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	return parts, nil
}

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

	var buf bytes.Buffer
	composite := md5.New()
	for _, part := range parts {
		if part.PartNumber < 1 || part.PartNumber > 10000 {
			return ObjectInfo{}, ErrInvalidPart
		}
		partBytes, err := e.disks[0].ReadFile(ctx, diskPartPath(bucket, uploadID, part.PartNumber))
		if err != nil {
			if errors.Is(err, disk.ErrNotExist) {
				return ObjectInfo{}, ErrInvalidPart
			}
			return ObjectInfo{}, err
		}
		sum := md5.Sum(partBytes)
		if part.ETag != "" && !strings.EqualFold(stripQuotes(part.ETag), hex.EncodeToString(sum[:])) {
			return ObjectInfo{}, ErrInvalidPart
		}
		composite.Write(sum[:])
		buf.Write(partBytes)
	}

	versionID := NullVersionID
	if upload.VersioningEnabled {
		versionID, err = newVersionID()
		if err != nil {
			return ObjectInfo{}, err
		}
	}
	etag := hex.EncodeToString(composite.Sum(nil)) + "-" + strconv.Itoa(len(parts))
	info, err := e.putBytes(ctx, bucket, object, versionID, buf.Bytes(), upload.ContentType, upload.UserMetadata, etag, versionID == NullVersionID)
	if err != nil {
		return ObjectInfo{}, err
	}
	_ = e.disks[0].DeleteDir(ctx, diskUploadDir(bucket, uploadID))
	e.log.Info("[gos3: erasure-complete-multipart]", "bucket", bucket, "object", object, "version", versionID, "size", info.Size)
	return info, nil
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
	if err := e.disks[0].DeleteDir(ctx, diskUploadDir(bucket, uploadID)); err != nil {
		return err
	}
	e.log.Info("[gos3: abort-multipart-upload]", "bucket", bucket, "object", object, "upload-id", uploadID)
	return nil
}

func (e *Erasure) ListMultipartUploads(ctx context.Context, bucket string) ([]MultipartInfo, error) {
	if !validBucketName(bucket) {
		return nil, ErrInvalidBucketName
	}
	dirs, _, err := e.disks[0].Walk(ctx, path.Join(multipartDir, bucket))
	if err != nil {
		return nil, err
	}
	var uploads []MultipartInfo
	for _, dir := range dirs {
		if strings.Contains(dir, "/") {
			continue
		}
		data, err := e.disks[0].ReadFile(ctx, path.Join(multipartDir, bucket, dir, "meta.json"))
		if err != nil {
			continue
		}
		var meta fsUploadMeta
		if json.Unmarshal(data, &meta) != nil {
			continue
		}
		uploads = append(uploads, MultipartInfo{Bucket: bucket, Object: meta.Object, UploadID: meta.UploadID, Initiated: meta.Initiated})
	}
	sort.Slice(uploads, func(i, j int) bool { return uploads[i].Object < uploads[j].Object })
	return uploads, nil
}

func (e *Erasure) CleanupStaleUploads(ctx context.Context, olderThan time.Duration) (int, error) {
	base := path.Join(multipartDir, "")
	dirs, _, err := e.disks[0].Walk(ctx, base)
	if err != nil {
		return 0, err
	}
	cutoff := time.Now().Add(-olderThan)
	removed := 0
	for _, dir := range dirs {
		if len(strings.Split(dir, "/")) != 2 {
			continue
		}
		fi, err := e.disks[0].Stat(ctx, path.Join(multipartDir, dir))
		if err != nil || !fi.Exists {
			continue
		}
		if fi.ModTime.After(cutoff) {
			continue
		}
		if err := e.disks[0].DeleteDir(ctx, path.Join(multipartDir, dir)); err != nil {
			return removed, err
		}
		removed++
		e.log.Info("[gos3: cleanup-stale-upload]", "path", dir)
	}
	return removed, nil
}

func (e *Erasure) putBytes(ctx context.Context, bucket, object, versionID string, raw []byte, contentType string, userMeta map[string]string, etag string, replaceNull bool) (ObjectInfo, error) {
	meta, err := e.readMetaAny(ctx, bucket, object)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return ObjectInfo{}, err
	}
	if replaceNull {
		meta = e.dropVersion(ctx, bucket, object, meta, NullVersionID)
	}

	shards, err := e.encoder.Encode(raw)
	if err != nil {
		return ObjectInfo{}, err
	}
	written := 0
	for i, shard := range shards {
		if i >= len(e.disks) {
			break
		}
		if err := e.disks[i].WriteFile(ctx, diskDataPath(bucket, object, versionID), shard); err != nil {
			e.log.Warn("[gos3: erasure-write-shard-failed]", "disk", e.disks[i].ID(), "error", err.Error())
			continue
		}
		written++
	}
	if written < e.dataShards {
		e.removeVersionData(ctx, bucket, object, versionID)
		return ObjectInfo{}, fmt.Errorf("write quorum not reached (%d/%d shards)", written, e.dataShards)
	}

	version := fsVersion{
		VersionID:    versionID,
		Size:         int64(len(raw)),
		ETag:         etag,
		ContentType:  contentType,
		UserMetadata: userMeta,
		ModTime:      time.Now().UTC(),
	}
	meta.Name = object
	meta.Versions = append([]fsVersion{version}, meta.Versions...)
	if err := e.writeMetaAll(ctx, bucket, object, meta); err != nil {
		return ObjectInfo{}, err
	}
	return version.toInfo(bucket, object), nil
}

func (e *Erasure) writeMetaAll(ctx context.Context, bucket, object string, meta fsMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	ok := 0
	for _, d := range e.disks {
		if err := d.WriteFile(ctx, diskMetaPath(bucket, object), data); err == nil {
			ok++
		}
	}
	if ok < e.dataShards {
		return fmt.Errorf("meta write quorum not reached (%d/%d)", ok, e.dataShards)
	}
	return nil
}

func (e *Erasure) writeOrRemoveMeta(ctx context.Context, bucket, object string, meta fsMeta) error {
	if len(meta.Versions) == 0 {
		for _, d := range e.disks {
			_ = d.DeleteFile(ctx, diskMetaPath(bucket, object))
		}
		return nil
	}
	return e.writeMetaAll(ctx, bucket, object, meta)
}

func (e *Erasure) dropVersion(ctx context.Context, bucket, object string, meta fsMeta, versionID string) fsMeta {
	kept := make([]fsVersion, 0, len(meta.Versions))
	for _, version := range meta.Versions {
		if version.VersionID == versionID {
			e.removeVersionData(ctx, bucket, object, version.VersionID)
			continue
		}
		kept = append(kept, version)
	}
	meta.Versions = kept
	return meta
}

func (e *Erasure) removeVersionData(ctx context.Context, bucket, object, versionID string) {
	for _, d := range e.disks {
		_ = d.DeleteFile(ctx, diskDataPath(bucket, object, versionID))
	}
}

func (e *Erasure) readShards(ctx context.Context, bucket, object, versionID string) ([][]byte, int) {
	shards := make([][]byte, e.encoder.Shards())
	present := 0
	for i, d := range e.disks {
		if i >= len(shards) {
			break
		}
		data, err := d.ReadFile(ctx, diskDataPath(bucket, object, versionID))
		if err != nil {
			continue
		}
		shards[i] = data
		present++
	}
	return shards, present
}

func (e *Erasure) readMetaAny(ctx context.Context, bucket, object string) (fsMeta, error) {
	lastErr := error(ErrObjectNotFound)
	for _, d := range e.disks {
		meta, err := diskReadMeta(ctx, d, bucket, object)
		if err == nil {
			return meta, nil
		}
		lastErr = err
	}
	return fsMeta{}, lastErr
}

func (e *Erasure) walkMetas(ctx context.Context, bucket string) ([]objectMetaEntry, error) {
	var lastErr error
	for _, d := range e.disks {
		entries, err := diskWalkMetas(ctx, d, bucket)
		if err != nil {
			lastErr = err
			continue
		}
		return entries, nil
	}
	return nil, lastErr
}

func (e *Erasure) readUploadMeta(ctx context.Context, bucket, uploadID string) (fsUploadMeta, error) {
	data, err := e.disks[0].ReadFile(ctx, diskUploadMetaPath(bucket, uploadID))
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

func (e *Erasure) writeUploadMeta(ctx context.Context, bucket, uploadID string, meta fsUploadMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return e.disks[0].WriteFile(ctx, diskUploadMetaPath(bucket, uploadID), data)
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

func diskDataPath(bucket, object, versionID string) string {
	return path.Join(dataDir, bucket, object, versionID)
}

func diskUploadDir(bucket, uploadID string) string {
	return path.Join(multipartDir, bucket, uploadID)
}

func diskUploadMetaPath(bucket, uploadID string) string {
	return path.Join(diskUploadDir(bucket, uploadID), "meta.json")
}

func diskPartPath(bucket, uploadID string, partNumber int) string {
	return path.Join(diskUploadDir(bucket, uploadID), fmt.Sprintf("part.%d", partNumber))
}

func diskPartMetaPath(bucket, uploadID string, partNumber int) string {
	return path.Join(diskUploadDir(bucket, uploadID), fmt.Sprintf("part.%d.json", partNumber))
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
