package store

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/kxj/gos3/internal/lifecycle"
)

var (
	ErrBucketNotFound    = errors.New("bucket not found")
	ErrObjectNotFound    = errors.New("object not found")
	ErrBucketExists      = errors.New("bucket already exists")
	ErrBucketNotEmpty    = errors.New("bucket not empty")
	ErrInvalidBucketName = errors.New("invalid bucket name")
	ErrInvalidObjectName = errors.New("invalid object name")

	ErrUploadNotFound   = errors.New("multipart upload not found")
	ErrInvalidUploadID  = errors.New("invalid upload id")
	ErrInvalidPart      = errors.New("invalid part")
	ErrInvalidPartOrder = errors.New("parts not in ascending order")

	ErrNoSuchVersion     = errors.New("no such version")
	ErrDeleteMarker      = errors.New("object is a delete marker")
	ErrInvalidVersioning = errors.New("invalid versioning status")
	ErrNoLifecycleConfig = errors.New("no lifecycle configuration")
)

const (
	VersioningDisabled  = ""
	VersioningEnabled   = "Enabled"
	VersioningSuspended = "Suspended"

	NullVersionID = "null"
)

type BucketInfo struct {
	Name    string
	Created time.Time
}

type ObjectInfo struct {
	Bucket       string
	Name         string
	VersionID    string
	DeleteMarker bool
	Size         int64
	ETag         string
	ContentType  string
	UserMetadata map[string]string
	ModTime      time.Time
}

type VersionInfo struct {
	ObjectInfo
	IsLatest bool
}

type MultipartInfo struct {
	Bucket    string
	Object    string
	UploadID  string
	Initiated time.Time
}

type PartInfo struct {
	PartNumber   int
	ETag         string
	Size         int64
	LastModified time.Time
}

type CompletePart struct {
	PartNumber int
	ETag       string
}

type ObjectToDelete struct {
	Object    string
	VersionID string
}

type DeleteResult struct {
	VersionID    string
	DeleteMarker bool
}

type ListOptions struct {
	Prefix    string
	Delimiter string
	Marker    string
	MaxKeys   int
}

type ListObjectsResult struct {
	Objects        []ObjectInfo
	CommonPrefixes []string
	IsTruncated    bool
	NextMarker     string
}

type ListVersionsResult struct {
	Versions            []VersionInfo
	CommonPrefixes      []string
	IsTruncated         bool
	NextKeyMarker       string
	NextVersionIDMarker string
}

type Store interface {
	MakeBucket(ctx context.Context, bucket string) error
	DeleteBucket(ctx context.Context, bucket string) error
	BucketExists(ctx context.Context, bucket string) (time.Time, bool, error)
	ListBuckets(ctx context.Context) ([]BucketInfo, error)

	GetBucketVersioning(ctx context.Context, bucket string) (string, error)
	SetBucketVersioning(ctx context.Context, bucket, status string) error

	GetBucketLifecycle(ctx context.Context, bucket string) (lifecycle.Configuration, error)
	SetBucketLifecycle(ctx context.Context, bucket string, cfg lifecycle.Configuration) error
	DeleteBucketLifecycle(ctx context.Context, bucket string) error

	PutObject(ctx context.Context, bucket, object string, data io.Reader, size int64, contentType string, userMeta map[string]string) (ObjectInfo, error)
	GetObject(ctx context.Context, bucket, object, versionID string) (io.ReadSeekCloser, ObjectInfo, error)
	StatObject(ctx context.Context, bucket, object, versionID string) (ObjectInfo, error)
	DeleteObject(ctx context.Context, bucket, object, versionID string) (DeleteResult, error)
	DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete) []error
	ListObjects(ctx context.Context, bucket string, opts ListOptions) (ListObjectsResult, error)
	ListObjectVersions(ctx context.Context, bucket string, opts ListOptions) (ListVersionsResult, error)

	NewMultipartUpload(ctx context.Context, bucket, object, contentType string, userMeta map[string]string) (string, error)
	PutObjectPart(ctx context.Context, bucket, object, uploadID string, partNumber int, data io.Reader) (PartInfo, error)
	ListObjectParts(ctx context.Context, bucket, object, uploadID string) ([]PartInfo, error)
	CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []CompletePart) (ObjectInfo, error)
	AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error
	ListMultipartUploads(ctx context.Context, bucket string) ([]MultipartInfo, error)
	CleanupStaleUploads(ctx context.Context, olderThan time.Duration) (int, error)
}
