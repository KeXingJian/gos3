package store

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kxj/gos3/internal/lifecycle"
)

const (
	metaDir      = ".meta"
	dataDir      = ".data"
	tmpDir       = ".tmp"
	multipartDir = ".multipart"

	bucketConfigFile = ".bucket.json"
)

type fsVersion struct {
	VersionID    string            `json:"versionId"`
	DeleteMarker bool              `json:"deleteMarker,omitempty"`
	Size         int64             `json:"size"`
	ETag         string            `json:"etag"`
	ContentType  string            `json:"contentType"`
	UserMetadata map[string]string `json:"userMetadata,omitempty"`
	ModTime      time.Time         `json:"modTime"`
}

type fsMeta struct {
	Name     string      `json:"name"`
	Versions []fsVersion `json:"versions"`
}

type fsBucketConfig struct {
	Versioning string `json:"versioning"`
}

type fsUploadMeta struct {
	Bucket            string            `json:"bucket"`
	Object            string            `json:"object"`
	UploadID          string            `json:"uploadId"`
	ContentType       string            `json:"contentType"`
	UserMetadata      map[string]string `json:"userMetadata,omitempty"`
	Initiated         time.Time         `json:"initiated"`
	VersioningEnabled bool              `json:"versioningEnabled"`
}

type fsPartMeta struct {
	PartNumber int       `json:"partNumber"`
	ETag       string    `json:"etag"`
	Size       int64     `json:"size"`
	ModTime    time.Time `json:"modTime"`
}

// toInfo 把内部版本记录 fsVersion 转换为对外返回的 ObjectInfo。
func (v fsVersion) toInfo(bucket, object string) ObjectInfo {
	return ObjectInfo{
		Bucket:       bucket,
		Name:         object,
		VersionID:    v.VersionID,
		DeleteMarker: v.DeleteMarker,
		Size:         v.Size,
		ETag:         v.ETag,
		ContentType:  v.ContentType,
		UserMetadata: v.UserMetadata,
		ModTime:      v.ModTime,
	}
}

type FS struct {
	root string
	log  *slog.Logger
}

func NewFS(root string, log *slog.Logger) (*FS, error) {
	if root == "" {
		return nil, errors.New("data directory is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, err
	}
	f := &FS{root: abs, log: log}
	log.Info("[gos3: storage-ready]", "root", abs)
	return f, nil
}

func (f *FS) dataPath(bucket, object, versionID string) string {
	return filepath.Join(f.root, dataDir, bucket, filepath.FromSlash(object), versionID)
}

func (f *FS) metaPath(bucket, object string) string {
	return filepath.Join(f.root, metaDir, bucket, filepath.FromSlash(object)+".json")
}

func (f *FS) bucketMetaDir(bucket string) string {
	return filepath.Join(f.root, metaDir, bucket)
}

func (f *FS) bucketConfigPath(bucket string) string {
	return filepath.Join(f.bucketMetaDir(bucket), bucketConfigFile)
}

func (f *FS) tempDir() string {
	return filepath.Join(f.root, tmpDir)
}

func (f *FS) uploadDir(bucket, uploadID string) string {
	return filepath.Join(f.root, multipartDir, bucket, uploadID)
}

func (f *FS) uploadMetaPath(bucket, uploadID string) string {
	return filepath.Join(f.uploadDir(bucket, uploadID), "meta.json")
}

func (f *FS) partPath(bucket, uploadID string, partNumber int) string {
	return filepath.Join(f.uploadDir(bucket, uploadID), fmt.Sprintf("part.%d", partNumber))
}

func (f *FS) partMetaPath(bucket, uploadID string, partNumber int) string {
	return filepath.Join(f.uploadDir(bucket, uploadID), fmt.Sprintf("part.%d.json", partNumber))
}

func (f *FS) MakeBucket(ctx context.Context, bucket string) error {
	if !validBucketName(bucket) {
		return ErrInvalidBucketName
	}
	dir := f.bucketMetaDir(bucket)
	if _, err := os.Stat(dir); err == nil {
		return ErrBucketExists
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f.log.Info("[gos3: make-bucket]", "bucket", bucket)
	return nil
}

func (f *FS) DeleteBucket(ctx context.Context, bucket string) error {
	if !validBucketName(bucket) {
		return ErrInvalidBucketName
	}
	dir := f.bucketMetaDir(bucket)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrBucketNotFound
		}
		return err
	}
	empty, err := f.bucketEmpty(bucket)
	if err != nil {
		return err
	}
	if !empty {
		return ErrBucketNotEmpty
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	_ = os.RemoveAll(filepath.Join(f.root, dataDir, bucket))
	_ = os.RemoveAll(filepath.Join(f.root, multipartDir, bucket))
	f.log.Info("[gos3: delete-bucket]", "bucket", bucket)
	return nil
}

func (f *FS) BucketExists(ctx context.Context, bucket string) (time.Time, bool, error) {
	if !validBucketName(bucket) {
		return time.Time{}, false, ErrInvalidBucketName
	}
	fi, err := os.Stat(f.bucketMetaDir(bucket))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, err
	}
	if !fi.IsDir() {
		return time.Time{}, false, nil
	}
	return fi.ModTime(), true, nil
}

func (f *FS) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	entries, err := os.ReadDir(filepath.Join(f.root, metaDir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var buckets []BucketInfo
	for _, e := range entries {
		if !e.IsDir() || !validBucketName(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		buckets = append(buckets, BucketInfo{Name: e.Name(), Created: info.ModTime()})
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Name < buckets[j].Name })
	return buckets, nil
}

func (f *FS) bucketEmpty(bucket string) (bool, error) {
	empty := true
	err := filepath.WalkDir(f.bucketMetaDir(bucket), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
			return nil
		}
		empty = false
		return fs.SkipAll
	})
	if err != nil {
		return false, err
	}
	return empty, nil
}

func (f *FS) GetBucketVersioning(ctx context.Context, bucket string) (string, error) {
	if !validBucketName(bucket) {
		return "", ErrInvalidBucketName
	}
	if _, ok, err := f.BucketExists(ctx, bucket); err != nil {
		return "", err
	} else if !ok {
		return "", ErrBucketNotFound
	}
	var cfg fsBucketConfig
	if err := readJSONFile(f.bucketConfigPath(bucket), &cfg); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return VersioningDisabled, nil
		}
		return "", err
	}
	return cfg.Versioning, nil
}

func (f *FS) SetBucketVersioning(ctx context.Context, bucket, status string) error {
	if status != VersioningEnabled && status != VersioningSuspended {
		return ErrInvalidVersioning
	}
	if _, ok, err := f.BucketExists(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return ErrBucketNotFound
	}
	if err := writeJSONFile(f.bucketConfigPath(bucket), fsBucketConfig{Versioning: status}); err != nil {
		return err
	}
	f.log.Info("[gos3: set-bucket-versioning]", "bucket", bucket, "status", status)
	return nil
}

func (f *FS) lifecyclePath(bucket string) string {
	return filepath.Join(f.bucketMetaDir(bucket), ".lifecycle.json")
}

func (f *FS) GetBucketLifecycle(ctx context.Context, bucket string) (lifecycle.Configuration, error) {
	if !validBucketName(bucket) {
		return lifecycle.Configuration{}, ErrInvalidBucketName
	}
	if _, ok, err := f.BucketExists(ctx, bucket); err != nil {
		return lifecycle.Configuration{}, err
	} else if !ok {
		return lifecycle.Configuration{}, ErrBucketNotFound
	}
	var cfg lifecycle.Configuration
	if err := readJSONFile(f.lifecyclePath(bucket), &cfg); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return lifecycle.Configuration{}, ErrNoLifecycleConfig
		}
		return lifecycle.Configuration{}, err
	}
	return cfg, nil
}

func (f *FS) SetBucketLifecycle(ctx context.Context, bucket string, cfg lifecycle.Configuration) error {
	if _, ok, err := f.BucketExists(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return ErrBucketNotFound
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := writeJSONFile(f.lifecyclePath(bucket), cfg); err != nil {
		return err
	}
	f.log.Info("[gos3: set-bucket-lifecycle]", "bucket", bucket, "rules", len(cfg.Rules))
	return nil
}

func (f *FS) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	if _, ok, err := f.BucketExists(ctx, bucket); err != nil {
		return err
	} else if !ok {
		return ErrBucketNotFound
	}
	err := os.Remove(f.lifecyclePath(bucket))
	if errors.Is(err, os.ErrNotExist) {
		return ErrNoLifecycleConfig
	}
	return err
}

func (f *FS) PutObject(ctx context.Context, bucket, object string, data io.Reader, size int64, contentType string, userMeta map[string]string) (ObjectInfo, error) {
	if !validBucketName(bucket) {
		return ObjectInfo{}, ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return ObjectInfo{}, ErrInvalidObjectName
	}
	if _, ok, err := f.BucketExists(ctx, bucket); err != nil {
		return ObjectInfo{}, err
	} else if !ok {
		return ObjectInfo{}, ErrBucketNotFound
	}
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	state, err := f.GetBucketVersioning(ctx, bucket)
	if err != nil {
		return ObjectInfo{}, err
	}
	versionID, err := assignVersionID(state)
	if err != nil {
		return ObjectInfo{}, err
	}

	if err := os.MkdirAll(f.tempDir(), 0o700); err != nil {
		return ObjectInfo{}, err
	}
	tmp, err := os.CreateTemp(f.tempDir(), "put-*")
	if err != nil {
		return ObjectInfo{}, err
	}
	tmpName := tmp.Name()
	defer func() {
		if tmpName != "" {
			_ = os.Remove(tmpName)
		}
	}()

	hash := md5.New()
	written, err := io.Copy(io.MultiWriter(tmp, hash), data)
	if err != nil {
		_ = tmp.Close()
		return ObjectInfo{}, err
	}
	if err := tmp.Close(); err != nil {
		return ObjectInfo{}, err
	}

	version := fsVersion{
		VersionID:    versionID,
		Size:         written,
		ETag:         hex.EncodeToString(hash.Sum(nil)),
		ContentType:  contentType,
		UserMetadata: userMeta,
		ModTime:      time.Now().UTC(),
	}
	if err := f.commitVersion(bucket, object, version, tmpName, versionID == NullVersionID); err != nil {
		return ObjectInfo{}, err
	}
	tmpName = ""
	f.log.Info("[gos3: put-object]", "bucket", bucket, "object", object, "version", versionID, "size", written)
	if state == VersioningDisabled {
		version.VersionID = ""
	}
	return version.toInfo(bucket, object), nil
}

func (f *FS) commitVersion(bucket, object string, version fsVersion, tmpPath string, replaceNull bool) error {
	meta, err := f.readObjectMeta(bucket, object)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return err
	}
	if replaceNull {
		meta = f.dropVersion(bucket, object, meta, NullVersionID)
	}
	if tmpPath != "" {
		dst := f.dataPath(bucket, object, version.VersionID)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := os.Rename(tmpPath, dst); err != nil {
			return err
		}
	}
	meta.Name = object
	meta.Versions = append([]fsVersion{version}, meta.Versions...)
	return f.writeObjectMeta(bucket, object, meta)
}

func (f *FS) GetObject(ctx context.Context, bucket, object, versionID string) (io.ReadSeekCloser, ObjectInfo, error) {
	if !validBucketName(bucket) {
		return nil, ObjectInfo{}, ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return nil, ObjectInfo{}, ErrInvalidObjectName
	}
	meta, err := f.readObjectMeta(bucket, object)
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
	file, err := os.Open(f.dataPath(bucket, object, version.VersionID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ObjectInfo{}, ErrObjectNotFound
		}
		return nil, ObjectInfo{}, err
	}
	return file, version.toInfo(bucket, object), nil
}

func (f *FS) StatObject(ctx context.Context, bucket, object, versionID string) (ObjectInfo, error) {
	if !validBucketName(bucket) {
		return ObjectInfo{}, ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return ObjectInfo{}, ErrInvalidObjectName
	}
	meta, err := f.readObjectMeta(bucket, object)
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

func (f *FS) DeleteObject(ctx context.Context, bucket, object, versionID string) (DeleteResult, error) {
	if !validBucketName(bucket) {
		return DeleteResult{}, ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return DeleteResult{}, ErrInvalidObjectName
	}
	if versionID != "" {
		return f.deleteVersion(bucket, object, versionID)
	}
	state, err := f.GetBucketVersioning(ctx, bucket)
	if err != nil {
		return DeleteResult{}, err
	}
	switch state {
	case VersioningEnabled:
		id, err := newVersionID()
		if err != nil {
			return DeleteResult{}, err
		}
		meta, err := f.readObjectMeta(bucket, object)
		if err != nil && !errors.Is(err, ErrObjectNotFound) {
			return DeleteResult{}, err
		}
		marker := fsVersion{VersionID: id, DeleteMarker: true, ModTime: time.Now().UTC()}
		meta.Name = object
		meta.Versions = append([]fsVersion{marker}, meta.Versions...)
		if err := f.writeObjectMeta(bucket, object, meta); err != nil {
			return DeleteResult{}, err
		}
		f.log.Info("[gos3: delete-marker]", "bucket", bucket, "object", object, "version", id)
		return DeleteResult{VersionID: id, DeleteMarker: true}, nil
	case VersioningSuspended:
		meta, err := f.readObjectMeta(bucket, object)
		if err != nil && !errors.Is(err, ErrObjectNotFound) {
			return DeleteResult{}, err
		}
		meta = f.dropVersion(bucket, object, meta, NullVersionID)
		marker := fsVersion{VersionID: NullVersionID, DeleteMarker: true, ModTime: time.Now().UTC()}
		meta.Name = object
		meta.Versions = append([]fsVersion{marker}, meta.Versions...)
		if err := f.writeObjectMeta(bucket, object, meta); err != nil {
			return DeleteResult{}, err
		}
		f.log.Info("[gos3: delete-marker]", "bucket", bucket, "object", object, "version", NullVersionID)
		return DeleteResult{VersionID: NullVersionID, DeleteMarker: true}, nil
	default:
		meta, err := f.readObjectMeta(bucket, object)
		if err != nil {
			if errors.Is(err, ErrObjectNotFound) {
				return DeleteResult{VersionID: NullVersionID}, nil
			}
			return DeleteResult{}, err
		}
		f.dropVersion(bucket, object, meta, NullVersionID)
		meta.Versions = removeVersion(meta.Versions, NullVersionID)
		if len(meta.Versions) == 0 {
			_ = os.Remove(f.metaPath(bucket, object))
		} else if err := f.writeObjectMeta(bucket, object, meta); err != nil {
			return DeleteResult{}, err
		}
		f.log.Info("[gos3: delete-object]", "bucket", bucket, "object", object)
		return DeleteResult{VersionID: NullVersionID}, nil
	}
}

func (f *FS) deleteVersion(bucket, object, versionID string) (DeleteResult, error) {
	meta, err := f.readObjectMeta(bucket, object)
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
		_ = os.Remove(f.dataPath(bucket, object, version.VersionID))
	}
	meta.Versions = removeVersion(meta.Versions, versionID)
	if len(meta.Versions) == 0 {
		_ = os.Remove(f.metaPath(bucket, object))
	} else if err := f.writeObjectMeta(bucket, object, meta); err != nil {
		return DeleteResult{}, err
	}
	f.log.Info("[gos3: delete-version]", "bucket", bucket, "object", object, "version", versionID)
	return DeleteResult{VersionID: versionID}, nil
}

func (f *FS) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete) []error {
	errs := make([]error, len(objects))
	for i, spec := range objects {
		if _, err := f.DeleteObject(ctx, bucket, spec.Object, spec.VersionID); err != nil {
			errs[i] = err
		}
	}
	return errs
}

func (f *FS) ListObjects(ctx context.Context, bucket string, opts ListOptions) (ListObjectsResult, error) {
	if !validBucketName(bucket) {
		return ListObjectsResult{}, ErrInvalidBucketName
	}
	if _, ok, err := f.BucketExists(ctx, bucket); err != nil {
		return ListObjectsResult{}, err
	} else if !ok {
		return ListObjectsResult{}, ErrBucketNotFound
	}
	metas, err := f.walkMetas(bucket)
	if err != nil {
		return ListObjectsResult{}, err
	}
	return buildObjectsResult(metas, bucket, opts), nil
}

type listObjectItem struct {
	key  string
	info ObjectInfo
}

func buildObjectsResult(metas []objectMetaEntry, bucket string, opts ListOptions) ListObjectsResult {
	var items []listObjectItem
	for _, entry := range metas {
		if !strings.HasPrefix(entry.key, opts.Prefix) || len(entry.meta.Versions) == 0 {
			continue
		}
		latest := entry.meta.Versions[0]
		if latest.DeleteMarker {
			continue
		}
		items = append(items, listObjectItem{key: entry.key, info: latest.toInfo(bucket, entry.key)})
	}
	return paginateObjects(items, opts)
}

func paginateObjects(items []listObjectItem, opts ListOptions) ListObjectsResult {
	max := normalizeMaxKeys(opts.MaxKeys)
	type entry struct {
		sortKey  string
		key      string
		prefix   string
		isPrefix bool
	}
	var entries []entry
	if opts.Delimiter == "" {
		for _, it := range items {
			entries = append(entries, entry{sortKey: it.key, key: it.key})
		}
	} else {
		seen := make(map[string]struct{})
		for _, it := range items {
			rest := it.key[len(opts.Prefix):]
			idx := strings.Index(rest, opts.Delimiter)
			if idx < 0 {
				entries = append(entries, entry{sortKey: it.key, key: it.key})
				continue
			}
			cp := opts.Prefix + rest[:idx+len(opts.Delimiter)]
			if _, ok := seen[cp]; ok {
				continue
			}
			seen[cp] = struct{}{}
			entries = append(entries, entry{sortKey: cp, prefix: cp, isPrefix: true})
		}
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].sortKey < entries[j].sortKey })

	infoByKey := make(map[string]ObjectInfo, len(items))
	for _, it := range items {
		infoByKey[it.key] = it.info
	}
	var result ListObjectsResult
	lastMarker := ""
	for _, e := range entries {
		if opts.Marker != "" && e.sortKey <= opts.Marker {
			continue
		}
		if len(result.Objects)+len(result.CommonPrefixes) >= max {
			result.IsTruncated = true
			result.NextMarker = lastMarker
			break
		}
		if e.isPrefix {
			result.CommonPrefixes = append(result.CommonPrefixes, e.prefix)
		} else {
			result.Objects = append(result.Objects, infoByKey[e.key])
		}
		lastMarker = e.sortKey
	}
	return result
}

func (f *FS) ListObjectVersions(ctx context.Context, bucket string, opts ListOptions) (ListVersionsResult, error) {
	if !validBucketName(bucket) {
		return ListVersionsResult{}, ErrInvalidBucketName
	}
	if _, ok, err := f.BucketExists(ctx, bucket); err != nil {
		return ListVersionsResult{}, err
	} else if !ok {
		return ListVersionsResult{}, ErrBucketNotFound
	}
	metas, err := f.walkMetas(bucket)
	if err != nil {
		return ListVersionsResult{}, err
	}
	return buildVersionsResult(metas, bucket, opts), nil
}

func buildVersionsResult(metas []objectMetaEntry, bucket string, opts ListOptions) ListVersionsResult {
	max := normalizeMaxKeys(opts.MaxKeys)
	var result ListVersionsResult
	seenPrefix := make(map[string]struct{})
	count := 0
	lastKey, lastVersion := "", ""
	for _, entry := range metas {
		if !strings.HasPrefix(entry.key, opts.Prefix) {
			continue
		}
		if opts.Marker != "" && entry.key <= opts.Marker {
			continue
		}
		if opts.Delimiter != "" {
			rest := entry.key[len(opts.Prefix):]
			if idx := strings.Index(rest, opts.Delimiter); idx >= 0 {
				cp := opts.Prefix + rest[:idx+len(opts.Delimiter)]
				if _, ok := seenPrefix[cp]; ok {
					continue
				}
				if count >= max {
					result.IsTruncated = true
					result.NextKeyMarker = lastKey
					result.NextVersionIDMarker = lastVersion
					break
				}
				seenPrefix[cp] = struct{}{}
				result.CommonPrefixes = append(result.CommonPrefixes, cp)
				count++
				lastKey, lastVersion = entry.key, ""
				continue
			}
		}
		for i, version := range entry.meta.Versions {
			if count >= max {
				result.IsTruncated = true
				result.NextKeyMarker = lastKey
				result.NextVersionIDMarker = lastVersion
				break
			}
			result.Versions = append(result.Versions, VersionInfo{
				ObjectInfo: version.toInfo(bucket, entry.key),
				IsLatest:   i == 0,
			})
			count++
			lastKey, lastVersion = entry.key, version.VersionID
		}
		if result.IsTruncated {
			break
		}
	}
	return result
}

func (f *FS) NewMultipartUpload(ctx context.Context, bucket, object, contentType string, userMeta map[string]string) (string, error) {
	if !validBucketName(bucket) {
		return "", ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return "", ErrInvalidObjectName
	}
	if _, ok, err := f.BucketExists(ctx, bucket); err != nil {
		return "", err
	} else if !ok {
		return "", ErrBucketNotFound
	}
	uploadID, err := newUploadID()
	if err != nil {
		return "", err
	}
	state, err := f.GetBucketVersioning(ctx, bucket)
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
	if err := writeJSONFile(f.uploadMetaPath(bucket, uploadID), meta); err != nil {
		return "", err
	}
	f.log.Info("[gos3: new-multipart-upload]", "bucket", bucket, "object", object, "upload-id", uploadID)
	return uploadID, nil
}

func (f *FS) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partNumber int, data io.Reader) (PartInfo, error) {
	if !validBucketName(bucket) {
		return PartInfo{}, ErrInvalidBucketName
	}
	if !validUploadID(uploadID) {
		return PartInfo{}, ErrInvalidUploadID
	}
	if partNumber < 1 || partNumber > 10000 {
		return PartInfo{}, ErrInvalidPart
	}
	if _, err := f.readUploadMeta(bucket, uploadID); err != nil {
		return PartInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return PartInfo{}, err
	}
	dir := f.uploadDir(bucket, uploadID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return PartInfo{}, err
	}
	tmp, err := os.CreateTemp(dir, "part-*")
	if err != nil {
		return PartInfo{}, err
	}
	tmpName := tmp.Name()
	defer func() {
		if tmpName != "" {
			_ = os.Remove(tmpName)
		}
	}()

	hash := md5.New()
	written, err := io.Copy(io.MultiWriter(tmp, hash), data)
	if err != nil {
		_ = tmp.Close()
		return PartInfo{}, err
	}
	if err := tmp.Close(); err != nil {
		return PartInfo{}, err
	}
	if err := os.Rename(tmpName, f.partPath(bucket, uploadID, partNumber)); err != nil {
		return PartInfo{}, err
	}
	tmpName = ""

	part := fsPartMeta{
		PartNumber: partNumber,
		ETag:       hex.EncodeToString(hash.Sum(nil)),
		Size:       written,
		ModTime:    time.Now().UTC(),
	}
	if err := writeJSONFile(f.partMetaPath(bucket, uploadID, partNumber), part); err != nil {
		return PartInfo{}, err
	}
	f.log.Info("[gos3: upload-part]", "bucket", bucket, "object", object, "upload-id", uploadID, "part", partNumber, "size", written)
	return PartInfo{PartNumber: partNumber, ETag: part.ETag, Size: part.Size, LastModified: part.ModTime}, nil
}

func (f *FS) ListObjectParts(ctx context.Context, bucket, object, uploadID string) ([]PartInfo, error) {
	if !validBucketName(bucket) {
		return nil, ErrInvalidBucketName
	}
	if !validUploadID(uploadID) {
		return nil, ErrInvalidUploadID
	}
	if _, err := f.readUploadMeta(bucket, uploadID); err != nil {
		return nil, err
	}
	dir := f.uploadDir(bucket, uploadID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var parts []PartInfo
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "part.") || !strings.HasSuffix(name, ".json") {
			continue
		}
		var part fsPartMeta
		if err := readJSONFile(filepath.Join(dir, name), &part); err != nil {
			continue
		}
		parts = append(parts, PartInfo{
			PartNumber:   part.PartNumber,
			ETag:         part.ETag,
			Size:         part.Size,
			LastModified: part.ModTime,
		})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	return parts, nil
}

func (f *FS) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []CompletePart) (ObjectInfo, error) {
	if !validBucketName(bucket) {
		return ObjectInfo{}, ErrInvalidBucketName
	}
	if !validObjectName(object) {
		return ObjectInfo{}, ErrInvalidObjectName
	}
	if !validUploadID(uploadID) {
		return ObjectInfo{}, ErrInvalidUploadID
	}
	upload, err := f.readUploadMeta(bucket, uploadID)
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

	if err := os.MkdirAll(f.tempDir(), 0o700); err != nil {
		return ObjectInfo{}, err
	}
	tmp, err := os.CreateTemp(f.tempDir(), "complete-*")
	if err != nil {
		return ObjectInfo{}, err
	}
	tmpName := tmp.Name()
	defer func() {
		if tmpName != "" {
			_ = os.Remove(tmpName)
		}
	}()

	composite := md5.New()
	var total int64
	for _, part := range parts {
		if part.PartNumber < 1 || part.PartNumber > 10000 {
			_ = tmp.Close()
			return ObjectInfo{}, ErrInvalidPart
		}
		partFile, err := os.Open(f.partPath(bucket, uploadID, part.PartNumber))
		if err != nil {
			_ = tmp.Close()
			if errors.Is(err, os.ErrNotExist) {
				return ObjectInfo{}, ErrInvalidPart
			}
			return ObjectInfo{}, err
		}
		partHash := md5.New()
		n, err := io.Copy(tmp, io.TeeReader(partFile, partHash))
		_ = partFile.Close()
		if err != nil {
			_ = tmp.Close()
			return ObjectInfo{}, err
		}
		sum := partHash.Sum(nil)
		if part.ETag != "" && !strings.EqualFold(stripQuotes(part.ETag), hex.EncodeToString(sum)) {
			_ = tmp.Close()
			return ObjectInfo{}, ErrInvalidPart
		}
		composite.Write(sum)
		total += n
	}
	if err := tmp.Close(); err != nil {
		return ObjectInfo{}, err
	}

	versionID := NullVersionID
	if upload.VersioningEnabled {
		versionID, err = newVersionID()
		if err != nil {
			return ObjectInfo{}, err
		}
	}
	version := fsVersion{
		VersionID:    versionID,
		Size:         total,
		ETag:         hex.EncodeToString(composite.Sum(nil)) + "-" + strconv.Itoa(len(parts)),
		ContentType:  upload.ContentType,
		UserMetadata: upload.UserMetadata,
		ModTime:      time.Now().UTC(),
	}
	if err := f.commitVersion(bucket, object, version, tmpName, versionID == NullVersionID); err != nil {
		return ObjectInfo{}, err
	}
	tmpName = ""
	_ = os.RemoveAll(f.uploadDir(bucket, uploadID))
	f.log.Info("[gos3: complete-multipart-upload]", "bucket", bucket, "object", object, "version", versionID, "size", total)
	return version.toInfo(bucket, object), nil
}

func (f *FS) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	if !validBucketName(bucket) {
		return ErrInvalidBucketName
	}
	if !validUploadID(uploadID) {
		return ErrInvalidUploadID
	}
	if _, err := f.readUploadMeta(bucket, uploadID); err != nil {
		return err
	}
	if err := os.RemoveAll(f.uploadDir(bucket, uploadID)); err != nil {
		return err
	}
	f.log.Info("[gos3: abort-multipart-upload]", "bucket", bucket, "object", object, "upload-id", uploadID)
	return nil
}

func (f *FS) ListMultipartUploads(ctx context.Context, bucket string) ([]MultipartInfo, error) {
	if !validBucketName(bucket) {
		return nil, ErrInvalidBucketName
	}
	base := filepath.Join(f.root, multipartDir, bucket)
	entries, err := os.ReadDir(base)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var uploads []MultipartInfo
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		var meta fsUploadMeta
		if err := readJSONFile(filepath.Join(base, e.Name(), "meta.json"), &meta); err != nil {
			continue
		}
		uploads = append(uploads, MultipartInfo{Bucket: bucket, Object: meta.Object, UploadID: meta.UploadID, Initiated: meta.Initiated})
	}
	sort.Slice(uploads, func(i, j int) bool { return uploads[i].Object < uploads[j].Object })
	return uploads, nil
}

func (f *FS) CleanupStaleUploads(ctx context.Context, olderThan time.Duration) (int, error) {
	base := filepath.Join(f.root, multipartDir)
	cutoff := time.Now().Add(-olderThan)
	removed := 0
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, relErr := filepath.Rel(base, path)
		if relErr != nil {
			return relErr
		}
		if len(strings.Split(filepath.ToSlash(rel), "/")) != 2 {
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			return infoErr
		}
		if info.ModTime().After(cutoff) {
			return filepath.SkipDir
		}
		if err := os.RemoveAll(path); err != nil {
			return err
		}
		removed++
		f.log.Info("[gos3: cleanup-stale-upload]", "path", path)
		return filepath.SkipDir
	})
	if err != nil {
		return removed, err
	}
	return removed, nil
}

type objectMetaEntry struct {
	key  string
	meta fsMeta
}

func (f *FS) walkMetas(bucket string) ([]objectMetaEntry, error) {
	base := f.bucketMetaDir(bucket)
	var out []objectMetaEntry
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
			return nil
		}
		rel, relErr := filepath.Rel(base, path)
		if relErr != nil {
			return relErr
		}
		var meta fsMeta
		if err := readJSONFile(path, &meta); err != nil {
			return nil
		}
		key := strings.TrimSuffix(filepath.ToSlash(rel), ".json")
		out = append(out, objectMetaEntry{key: key, meta: meta})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out, nil
}

func (f *FS) readUploadMeta(bucket, uploadID string) (fsUploadMeta, error) {
	var meta fsUploadMeta
	if err := readJSONFile(f.uploadMetaPath(bucket, uploadID), &meta); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fsUploadMeta{}, ErrUploadNotFound
		}
		return fsUploadMeta{}, err
	}
	return meta, nil
}

func (f *FS) readObjectMeta(bucket, object string) (fsMeta, error) {
	var meta fsMeta
	if err := readJSONFile(f.metaPath(bucket, object), &meta); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fsMeta{}, ErrObjectNotFound
		}
		return fsMeta{}, err
	}
	return meta, nil
}

func (f *FS) writeObjectMeta(bucket, object string, meta fsMeta) error {
	return writeJSONFile(f.metaPath(bucket, object), meta)
}

func (f *FS) dropVersion(bucket, object string, meta fsMeta, versionID string) fsMeta {
	kept := make([]fsVersion, 0, len(meta.Versions))
	for _, version := range meta.Versions {
		if version.VersionID == versionID {
			_ = os.Remove(f.dataPath(bucket, object, version.VersionID))
			continue
		}
		kept = append(kept, version)
	}
	meta.Versions = kept
	return meta
}

func resolveVersion(meta fsMeta, versionID string) (fsVersion, int, bool) {
	if versionID == "" {
		if len(meta.Versions) == 0 {
			return fsVersion{}, -1, false
		}
		return meta.Versions[0], 0, true
	}
	for i, version := range meta.Versions {
		if version.VersionID == versionID {
			return version, i, true
		}
	}
	return fsVersion{}, -1, false
}

func removeVersion(versions []fsVersion, versionID string) []fsVersion {
	out := make([]fsVersion, 0, len(versions))
	for _, version := range versions {
		if version.VersionID == versionID {
			continue
		}
		out = append(out, version)
	}
	return out
}

// assignVersionID 依据 bucket 的版本控制状态分配版本号：
// 开启版本控制时生成随机版本号，否则返回常量 "null"。
func assignVersionID(state string) (string, error) {
	if state == VersioningEnabled {
		return newVersionID()
	}
	return NullVersionID, nil
}

func newVersionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func newUploadID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func validUploadID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
			continue
		}
		return false
	}
	return true
}

func stripQuotes(s string) string {
	return strings.Trim(s, "\"")
}

func normalizeMaxKeys(maxKeys int) int {
	if maxKeys <= 0 || maxKeys > 1000 {
		return 1000
	}
	return maxKeys
}

func writeJSONFile(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readJSONFile(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func validBucketName(bucket string) bool {
	l := len(bucket)
	if l < 3 || l > 63 {
		return false
	}
	if strings.Contains(bucket, "..") {
		return false
	}
	for i := 0; i < l; i++ {
		c := bucket[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '-' {
			continue
		}
		return false
	}
	return isAlnum(bucket[0]) && isAlnum(bucket[l-1])
}

func validObjectName(object string) bool {
	if object == "" || len(object) > 1024 {
		return false
	}
	if strings.HasPrefix(object, "/") || strings.HasSuffix(object, "/") {
		return false
	}
	if strings.ContainsRune(object, 0) || strings.Contains(object, "\\") {
		return false
	}
	for _, seg := range strings.Split(object, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

func isAlnum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}
