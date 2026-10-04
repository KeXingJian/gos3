package server

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	"github.com/kxj/gos3/internal/api"
	"github.com/kxj/gos3/internal/sign"
)

func (s *Server) middleware(next http.Handler, skew time.Duration) http.Handler {
	h := next
	h = s.auth(h, skew)
	h = s.accessLog(h)
	h = s.requestID(h)
	h = s.recoverer(h)
	return h
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.logger.Error("[gos3: panic-recovered]", "path", r.URL.Path, "panic", rec)
				api.WriteError(w, r, api.ErrInternalError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := newRequestID()
		w.Header().Set("x-amz-request-id", id)
		next.ServeHTTP(w, r.WithContext(api.WithRequestID(r.Context(), id)))
	})
}

func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		s.logger.Info("[gos3: request]",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"duration", time.Since(start).String(),
			"request-id", api.GetRequestID(r.Context()),
		)
	})
}

func (s *Server) auth(next http.Handler, skew time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isPublicPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		res, err := s.authenticate(r, skew)
		if err != nil {
			s.writeAuthError(w, r, err)
			return
		}
		action, resource := actionAndResource(r)
		if !s.iam.IsAllowed(res.Credentials.AccessKey, action, resource) {
			s.logger.Warn("[gos3: access-denied]", "path", r.URL.Path, "access-key", res.Credentials.AccessKey, "action", action)
			api.WriteError(w, r, api.ErrAccessDenied)
			return
		}
		if res.Streaming {
			r.Body = sign.NewChunkedReader(r.Body, res.SigningKey, res.Scope, res.AmzDate, res.Signature)
			r.ContentLength = -1
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) authenticate(r *http.Request, skew time.Duration) (sign.Result, error) {
	if r.Header.Get("Authorization") != "" {
		return sign.VerifyHeader(r, s.iam, time.Now(), skew)
	}
	if r.URL.Query().Get("X-Amz-Algorithm") != "" {
		return sign.VerifyQuery(r, s.iam, time.Now(), 7*24*time.Hour)
	}
	return sign.Result{}, sign.ErrMissingAuth
}

func (s *Server) writeAuthError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, sign.ErrSignature):
		api.WriteError(w, r, api.ErrSignatureDoesNotMatch)
	case errors.Is(err, sign.ErrRequestExpired):
		api.WriteError(w, r, api.ErrRequestTimeTooSkewed)
	case errors.Is(err, sign.ErrAccessDenied):
		api.WriteError(w, r, api.ErrInvalidAccessKeyID)
	case errors.Is(err, sign.ErrMissingAuth), errors.Is(err, sign.ErrInvalidAuth):
		api.WriteError(w, r, api.ErrAccessDenied)
	case errors.Is(err, sign.ErrUnsupportedPayload):
		api.WriteError(w, r, api.ErrNotImplemented)
	default:
		s.logger.Warn("[gos3: auth-failed]", "path", r.URL.Path, "error", err.Error())
		api.WriteError(w, r, api.ErrAccessDenied)
	}
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return hex.EncodeToString([]byte(time.Now().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(b[:])
}
