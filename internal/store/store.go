package store

import (
	"context"
	"errors"
	"io"
	"time"
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
)

type BucketInfo struct {
	Name    string
	Created time.Time
}

type ObjectInfo struct {
	Bucket       string
	Name         string
	Size         int64
	ETag         string
	ContentType  string
	UserMetadata map[string]string
	ModTime      time.Time
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

type Store interface {
	MakeBucket(ctx context.Context, bucket string) error
	DeleteBucket(ctx context.Context, bucket string) error
	BucketExists(ctx context.Context, bucket string) (time.Time, bool, error)
	ListBuckets(ctx context.Context) ([]BucketInfo, error)

	PutObject(ctx context.Context, bucket, object string, data io.Reader, size int64, contentType string, userMeta map[string]string) (ObjectInfo, error)
	GetObject(ctx context.Context, bucket, object string) (io.ReadSeekCloser, ObjectInfo, error)
	StatObject(ctx context.Context, bucket, object string) (ObjectInfo, error)
	DeleteObject(ctx context.Context, bucket, object string) error
	DeleteObjects(ctx context.Context, bucket string, objects []string) []error
	ListObjects(ctx context.Context, bucket string, opts ListOptions) (ListObjectsResult, error)

	NewMultipartUpload(ctx context.Context, bucket, object, contentType string, userMeta map[string]string) (string, error)
	PutObjectPart(ctx context.Context, bucket, object, uploadID string, partNumber int, data io.Reader) (PartInfo, error)
	ListObjectParts(ctx context.Context, bucket, object, uploadID string) ([]PartInfo, error)
	CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []CompletePart) (ObjectInfo, error)
	AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error
	ListMultipartUploads(ctx context.Context, bucket string) ([]MultipartInfo, error)
	CleanupStaleUploads(ctx context.Context, olderThan time.Duration) (int, error)
}
