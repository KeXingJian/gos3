package server

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/kxj/gos3/internal/api"
	"github.com/kxj/gos3/internal/auth"
	"github.com/kxj/gos3/internal/config"
	"github.com/kxj/gos3/internal/store"
)

type Server struct {
	api     *api.Handler
	creds   *auth.Store
	logger  *slog.Logger
	handler http.Handler
}

func New(cfg config.Config, st store.Store, creds *auth.Store, logger *slog.Logger) *Server {
	s := &Server{
		api: &api.Handler{
			Store:   st,
			Region:  cfg.Region,
			OwnerID: cfg.OwnerID,
			Logger:  logger,
		},
		creds:  creds,
		logger: logger,
	}
	s.handler = s.middleware(http.HandlerFunc(s.route), cfg.MaxSkew)
	return s
}

func (s *Server) Handler() http.Handler {
	return s.handler
}

func (s *Server) route(w http.ResponseWriter, r *http.Request) {
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
			if query.Has("uploads") {
				s.api.ListMultipartUploads(w, r, bucket)
				return
			}
			s.api.ListObjects(w, r, bucket)
		case http.MethodPut:
			s.api.CreateBucket(w, r, bucket)
		case http.MethodHead:
			s.api.HeadBucket(w, r, bucket)
		case http.MethodDelete:
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
	return strings.HasPrefix(path, "/minio/health/")
}
