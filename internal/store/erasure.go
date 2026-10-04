package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kxj/gos3/internal/erasure"
)

type Erasure struct {
	drives       []*FS
	encoder      *erasure.Encoder
	dataShards   int
	parityShards int
	log          *slog.Logger
}

func NewErasure(drivePaths []string, dataShards, parityShards int, log *slog.Logger) (*Erasure, error) {
	if dataShards < 1 || parityShards < 1 {
		return nil, errors.New("data and parity shards must both be >= 1")
	}
	if len(drivePaths) != dataShards+parityShards {
		return nil, fmt.Errorf("drive count (%d) must equal data+parity shards (%d)", len(drivePaths), dataShards+parityShards)
	}
	enc, err := erasure.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	drives := make([]*FS, 0, len(drivePaths))
	for _, path := range drivePaths {
		d, err := NewFS(path, log)
		if err != nil {
			return nil, err
		}
		drives = append(drives, d)
	}
	e := &Erasure{drives: drives, encoder: enc, dataShards: dataShards, parityShards: parityShards, log: log}
	log.Info("[gos3: erasure-ready]", "drives", len(drives), "data-shards", dataShards, "parity-shards", parityShards)
	return e, nil
}

func (e *Erasure) MakeBucket(ctx context.Context, bucket string) error {
	if !validBucketName(bucket) {
		return ErrInvalidBucketName
	}
	created, existed := 0, 0
	for _, d := range e.drives {
		switch err := d.MakeBucket(ctx, bucket); {
		case err == nil:
			created++
		case errors.Is(err, ErrBucketExists):
			existed++
		default:
			return err
		}
	}
	if created == 0 && existed == len(e.drives) {
		return ErrBucketExists
	}
	e.log.Info("[gos3: erasure-make-bucket]", "bucket", bucket)
	return nil
}

func (e *Erasure) DeleteBucket(ctx context.Context, bucket string) error {
	if !validBucketName(bucket) {
		return ErrInvalidBucketName
	}
	if _, ok, err := e.drives[0].BucketExists(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return ErrBucketNotFound
	}
	empty, err := e.drives[0].bucketEmpty(bucket)
	if err != nil {
		return err
	}
	if !empty {
		return ErrBucketNotEmpty
	}
	for _, d := range e.drives {
		_ = d.DeleteBucket(ctx, bucket)
	}
	e.log.Info("[gos3: erasure-delete-bucket]", "bucket", bucket)
	return nil
}

func (e *Erasure) BucketExists(ctx context.Context, bucket string) (time.Time, bool, error) {
	return e.drives[0].BucketExists(ctx, bucket)
}

func (e *Erasure) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	return e.drives[0].ListBuckets(ctx)
}

func (e *Erasure) GetBucketVersioning(ctx context.Context, bucket string) (string, error) {
	return e.drives[0].GetBucketVersioning(ctx, bucket)
}

func (e *Erasure) SetBucketVersioning(ctx context.Context, bucket, status string) error {
	return e.drives[0].SetBucketVersioning(ctx, bucket, status)
}

func (e *Erasure) PutObject(ctx context.Context, bucket, object string, data io.Reader, size int64, contentType string, userMeta map[string]string) (ObjectInfo, error) {
	if !validBucketName(bucket) {
		return ObjectInfo{}, ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return ObjectInfo{}, ErrInvalidObjectName
	}
	if _, ok, err := e.drives[0].BucketExists(ctx, bucket); err != nil {
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
	info, err := e.putBytes(bucket, object, versionID, raw, contentType, userMeta, md5hex(raw), versionID == NullVersionID)
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
	meta, err := e.readMetaAny(bucket, object)
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
	shards, present := e.readShards(bucket, object, version.VersionID)
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
	meta, err := e.readMetaAny(bucket, object)
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
		meta, err := e.readMetaAny(bucket, object)
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
			e.removeVersionData(bucket, object, version.VersionID)
		}
		meta.Versions = removeVersion(meta.Versions, versionID)
		if err := e.writeOrRemoveMeta(bucket, object, meta); err != nil {
			return DeleteResult{}, err
		}
		return DeleteResult{VersionID: versionID}, nil
	}

	state, err := e.GetBucketVersioning(ctx, bucket)
	if err != nil {
		return DeleteResult{}, err
	}
	meta, err := e.readMetaAny(bucket, object)
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
			meta = e.dropVersion(bucket, object, meta, NullVersionID)
		}
		marker := fsVersion{VersionID: id, DeleteMarker: true, ModTime: time.Now().UTC()}
		meta.Name = object
		meta.Versions = append([]fsVersion{marker}, meta.Versions...)
		if err := e.writeMetaAll(bucket, object, meta); err != nil {
			return DeleteResult{}, err
		}
		e.log.Info("[gos3: erasure-delete-marker]", "bucket", bucket, "object", object, "version", id)
		return DeleteResult{VersionID: id, DeleteMarker: true}, nil
	default:
		if errors.Is(err, ErrObjectNotFound) {
			return DeleteResult{VersionID: NullVersionID}, nil
		}
		meta = e.dropVersion(bucket, object, meta, NullVersionID)
		meta.Versions = removeVersion(meta.Versions, NullVersionID)
		if err := e.writeOrRemoveMeta(bucket, object, meta); err != nil {
			return DeleteResult{}, err
		}
		e.log.Info("[gos3: erasure-delete-object]", "bucket", bucket, "object", object)
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
	return e.drives[0].ListObjects(ctx, bucket, opts)
}

func (e *Erasure) ListObjectVersions(ctx context.Context, bucket string, opts ListOptions) (ListVersionsResult, error) {
	return e.drives[0].ListObjectVersions(ctx, bucket, opts)
}

func (e *Erasure) NewMultipartUpload(ctx context.Context, bucket, object, contentType string, userMeta map[string]string) (string, error) {
	return e.drives[0].NewMultipartUpload(ctx, bucket, object, contentType, userMeta)
}

func (e *Erasure) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partNumber int, data io.Reader) (PartInfo, error) {
	return e.drives[0].PutObjectPart(ctx, bucket, object, uploadID, partNumber, data)
}

func (e *Erasure) ListObjectParts(ctx context.Context, bucket, object, uploadID string) ([]PartInfo, error) {
	return e.drives[0].ListObjectParts(ctx, bucket, object, uploadID)
}

func (e *Erasure) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	return e.drives[0].AbortMultipartUpload(ctx, bucket, object, uploadID)
}

func (e *Erasure) ListMultipartUploads(ctx context.Context, bucket string) ([]MultipartInfo, error) {
	return e.drives[0].ListMultipartUploads(ctx, bucket)
}

func (e *Erasure) CleanupStaleUploads(ctx context.Context, olderThan time.Duration) (int, error) {
	return e.drives[0].CleanupStaleUploads(ctx, olderThan)
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
	upload, err := e.drives[0].readUploadMeta(bucket, uploadID)
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
		partBytes, err := os.ReadFile(e.drives[0].partPath(bucket, uploadID, part.PartNumber))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
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
	info, err := e.putBytes(bucket, object, versionID, buf.Bytes(), upload.ContentType, upload.UserMetadata, etag, versionID == NullVersionID)
	if err != nil {
		return ObjectInfo{}, err
	}
	_ = os.RemoveAll(e.drives[0].uploadDir(bucket, uploadID))
	e.log.Info("[gos3: erasure-complete-multipart]", "bucket", bucket, "object", object, "version", versionID, "size", info.Size)
	return info, nil
}

func (e *Erasure) putBytes(bucket, object, versionID string, raw []byte, contentType string, userMeta map[string]string, etag string, replaceNull bool) (ObjectInfo, error) {
	meta, err := e.readMetaAny(bucket, object)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return ObjectInfo{}, err
	}
	if replaceNull {
		meta = e.dropVersion(bucket, object, meta, NullVersionID)
	}

	shards, err := e.encoder.Encode(raw)
	if err != nil {
		return ObjectInfo{}, err
	}
	written := 0
	for i, shard := range shards {
		if i >= len(e.drives) {
			break
		}
		if err := writeShard(e.drives[i].dataPath(bucket, object, versionID), shard); err != nil {
			e.log.Warn("[gos3: erasure-write-shard-failed]", "drive", i, "error", err.Error())
			continue
		}
		written++
	}
	if written < e.dataShards {
		e.removeVersionData(bucket, object, versionID)
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
	if err := e.writeMetaAll(bucket, object, meta); err != nil {
		return ObjectInfo{}, err
	}
	return version.toInfo(bucket, object), nil
}

func (e *Erasure) writeMetaAll(bucket, object string, meta fsMeta) error {
	var firstErr error
	for _, d := range e.drives {
		if err := d.writeObjectMeta(bucket, object, meta); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (e *Erasure) writeOrRemoveMeta(bucket, object string, meta fsMeta) error {
	if len(meta.Versions) == 0 {
		for _, d := range e.drives {
			_ = os.Remove(d.metaPath(bucket, object))
		}
		return nil
	}
	return e.writeMetaAll(bucket, object, meta)
}

func (e *Erasure) dropVersion(bucket, object string, meta fsMeta, versionID string) fsMeta {
	kept := make([]fsVersion, 0, len(meta.Versions))
	for _, version := range meta.Versions {
		if version.VersionID == versionID {
			e.removeVersionData(bucket, object, version.VersionID)
			continue
		}
		kept = append(kept, version)
	}
	meta.Versions = kept
	return meta
}

func (e *Erasure) removeVersionData(bucket, object, versionID string) {
	for _, d := range e.drives {
		_ = os.Remove(d.dataPath(bucket, object, versionID))
	}
}

func (e *Erasure) readShards(bucket, object, versionID string) ([][]byte, int) {
	shards := make([][]byte, e.encoder.Shards())
	present := 0
	for i, d := range e.drives {
		if i >= len(shards) {
			break
		}
		data, err := os.ReadFile(d.dataPath(bucket, object, versionID))
		if err != nil {
			continue
		}
		shards[i] = data
		present++
	}
	return shards, present
}

func (e *Erasure) readMetaAny(bucket, object string) (fsMeta, error) {
	lastErr := error(ErrObjectNotFound)
	for _, d := range e.drives {
		meta, err := d.readObjectMeta(bucket, object)
		if err == nil {
			return meta, nil
		}
		lastErr = err
	}
	return fsMeta{}, lastErr
}

func writeShard(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
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
