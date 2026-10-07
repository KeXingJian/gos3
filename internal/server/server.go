package server

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/kxj/gos3/internal/api"
	"github.com/kxj/gos3/internal/config"
	"github.com/kxj/gos3/internal/iam"
	"github.com/kxj/gos3/internal/store"
)

type Server struct {
	api     *api.Handler
	iam     *iam.Store
	logger  *slog.Logger
	handler http.Handler
}

func New(cfg config.Config, st store.Store, iamStore *iam.Store, logger *slog.Logger) *Server {
	s := &Server{
		api: &api.Handler{
			Store:    st,
			IAM:      iamStore,
			Region:   cfg.Region,
			OwnerID:  cfg.OwnerID,
			RootUser: cfg.RootUser,
			RootPass: cfg.RootPass,
			Logger:   logger,
		},
		iam:    iamStore,
		logger: logger,
	}
	s.handler = s.middleware(http.HandlerFunc(s.route), cfg.MaxSkew)
	return s
}

func (s *Server) Handler() http.Handler {
	return s.handler
}

// route 是 HTTP 路由总入口，按 URL 形态与方法分发到具体处理器。
// 匹配顺序：管理接口 /gos3/admin/* -> 内置 UI -> 公开路径 -> 根路径 -> bucket/object。
// 完整路由（S3 兼容）：
//
//	GET    /                                 列出 bucket (ListBuckets)
//	GET    /healthz, /minio/health/*         健康检查
//	GET    /ui, /ui/                         内置管理页
//	/gos3/admin/*                            管理接口（见 api.ServeAdmin）
//
// Bucket 级（/{bucket}）：
//
//	GET    ?location     GetBucketLocation
//	GET    ?versioning   GetBucketVersioning
//	GET    ?lifecycle    GetBucketLifecycle
//	GET    ?versions     ListObjectVersions
//	GET    ?uploads      ListMultipartUploads
//	GET                  ListObjects
//	PUT    ?versioning   SetBucketVersioning
//	PUT    ?lifecycle    SetBucketLifecycle
//	PUT                  CreateBucket
//	HEAD                 HeadBucket
//	DELETE ?lifecycle    DeleteBucketLifecycle
//	DELETE               DeleteBucket
//	POST   ?delete       DeleteObjects
//
// Object 级（/{bucket}/{object}）：
//
//	POST   ?uploads               CreateMultipartUpload
//	POST   ?uploadId=             CompleteMultipartUpload
//	PUT    ?uploadId=&partNumber= UploadPart
//	PUT                           PutObject
//	GET    ?uploadId=             ListParts
//	GET                           GetObject
//	HEAD                          HeadObject
//	DELETE ?uploadId=             AbortMultipartUpload
//	DELETE                        DeleteObject
func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	if s.api.ServeAdmin(w, r) {
		return
	}
	if s.api.ServeUI(w, r) {
		return
	}
	if isPublicPath(r.URL.Path) {
		s.api.Health(w, r)
		return
	}
	if r.URL.Path == "/" {
		if r.Method == http.MethodGet {
			s.api.ListBuckets(w, r)
			return
		}
		api.WriteError(w, r, api.ErrMethodNotAllowed)
		return
	}

	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	bucket, object, _ := strings.Cut(trimmed, "/")
	if bucket == "" {
		api.WriteError(w, r, api.ErrInvalidBucketName)
		return
	}

	query := r.URL.Query()

	if object == "" {
		switch r.Method {
		case http.MethodGet:
			switch {
			case query.Has("lifecycle"):
				s.api.GetBucketLifecycle(w, r, bucket)
				return
			case query.Has("versions"):
				s.api.ListObjectVersions(w, r, bucket)
				return
			case query.Has("uploads"):
				s.api.ListMultipartUploads(w, r, bucket)
				return
			}
			s.api.ListObjects(w, r, bucket)
		case http.MethodPut:
			switch {
			case query.Has("versioning"):
				s.api.SetBucketVersioning(w, r, bucket)
				return
			case query.Has("lifecycle"):
				s.api.SetBucketLifecycle(w, r, bucket)
				return
			}
			s.api.CreateBucket(w, r, bucket)
		case http.MethodHead:
			s.api.HeadBucket(w, r, bucket)
		case http.MethodDelete:
			if query.Has("lifecycle") {
				s.api.DeleteBucketLifecycle(w, r, bucket)
				return
			}
			s.api.DeleteBucket(w, r, bucket)
		case http.MethodPost:
			if query.Has("delete") {
				s.api.DeleteObjects(w, r, bucket)
				return
			}
			api.WriteError(w, r, api.ErrMethodNotAllowed)
		default:
			api.WriteError(w, r, api.ErrMethodNotAllowed)
		}
		return
	}

	hasUpload := query.Has("uploadId")
	switch r.Method {
	case http.MethodPut:
		if hasUpload && query.Has("partNumber") {
			s.api.UploadPart(w, r, bucket, object)
			return
		}
		s.api.PutObject(w, r, bucket, object)
	case http.MethodGet:
		if hasUpload {
			s.api.ListParts(w, r, bucket, object)
			return
		}
		s.api.GetObject(w, r, bucket, object)
	case http.MethodHead:
		s.api.HeadObject(w, r, bucket, object)
	case http.MethodDelete:
		if hasUpload {
			s.api.AbortMultipartUpload(w, r, bucket, object)
			return
		}
		s.api.DeleteObject(w, r, bucket, object)
	case http.MethodPost:
		if query.Has("uploads") {
			s.api.CreateMultipartUpload(w, r, bucket, object)
			return
		}
		if hasUpload {
			s.api.CompleteMultipartUpload(w, r, bucket, object)
			return
		}
		api.WriteError(w, r, api.ErrMethodNotAllowed)
	default:
		api.WriteError(w, r, api.ErrMethodNotAllowed)
	}
}

func isPublicPath(path string) bool {
	if path == "/healthz" {
		return true
	}
	if strings.HasPrefix(path, "/minio/health/") {
		return true
	}
	if strings.HasPrefix(path, "/gos3/admin/") {
		return true
	}
	return path == "/ui" || path == "/ui/"
}

func actionAndResource(r *http.Request) (string, string) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		return "s3:ListAllMyBuckets", "*"
	}
	bucket, object, _ := strings.Cut(path, "/")
	q := r.URL.Query()
	if object == "" {
		bucketRes := "arn:aws:s3:::" + bucket
		switch r.Method {
		case http.MethodGet:
			switch {
			case q.Has("lifecycle"):
				return "s3:GetLifecycleConfiguration", bucketRes
			case q.Has("versions"):
				return "s3:ListBucketVersions", bucketRes
			case q.Has("uploads"):
				return "s3:ListBucketMultipartUploads", bucketRes
			case q.Has("versioning"):
				return "s3:GetBucketVersioning", bucketRes
			case q.Has("location"):
				return "s3:GetBucketLocation", bucketRes
			default:
				return "s3:ListBucket", bucketRes
			}
		case http.MethodHead:
			return "s3:ListBucket", bucketRes
		case http.MethodPut:
			switch {
			case q.Has("versioning"):
				return "s3:PutBucketVersioning", bucketRes
			case q.Has("lifecycle"):
				return "s3:PutLifecycleConfiguration", bucketRes
			default:
				return "s3:CreateBucket", bucketRes
			}
		case http.MethodDelete:
			if q.Has("lifecycle") {
				return "s3:PutLifecycleConfiguration", bucketRes
			}
			return "s3:DeleteBucket", bucketRes
		default:
			return "s3:ListBucket", bucketRes
		}
	}
	objRes := "arn:aws:s3:::" + bucket + "/" + object
	switch r.Method {
	case http.MethodPut:
		return "s3:PutObject", objRes
	case http.MethodGet:
		if q.Has("uploadId") {
			return "s3:ListMultipartUploadParts", objRes
		}
		return "s3:GetObject", objRes
	case http.MethodHead:
		return "s3:GetObject", objRes
	case http.MethodDelete:
		if q.Has("uploadId") {
			return "s3:AbortMultipartUpload", objRes
		}
		return "s3:DeleteObject", objRes
	default:
		return "s3:PutObject", objRes
	}
}
