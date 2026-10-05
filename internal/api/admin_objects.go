package api

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kxj/gos3/internal/auth"
	"github.com/kxj/gos3/internal/sign"
	"github.com/kxj/gos3/internal/store"
)

const maxPresignExpiry = 7 * 24 * 3600

func (h *Handler) adminPresign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	bucket := q.Get("bucket")
	object := q.Get("object")
	if bucket == "" || object == "" {
		writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": "bucket and object are required"})
		return
	}
	expires := 3600
	if v := q.Get("expires"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			expires = n
		}
	}
	if expires < 1 {
		expires = 3600
	}
	if expires > maxPresignExpiry {
		expires = maxPresignExpiry
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	creds := auth.Credentials{AccessKey: h.RootUser, SecretKey: h.RootPass}
	link := sign.PresignGetURL(scheme, r.Host, "/"+bucket+"/"+object, creds, h.Region, time.Now(), time.Duration(expires)*time.Second)
	writeJSONStatus(w, http.StatusOK, map[string]any{"url": link, "expires": expires})
}

func (h *Handler) serveAdminBuckets(w http.ResponseWriter, r *http.Request, rest string) {
	if rest == "" || rest == "/" {
		switch r.Method {
		case http.MethodGet:
			buckets, err := h.Store.ListBuckets(r.Context())
			if err != nil {
				writeAdminError(w, err)
				return
			}
			type bucketView struct {
				Name    string `json:"name"`
				Created string `json:"created"`
			}
			out := make([]bucketView, 0, len(buckets))
			for _, b := range buckets {
				out = append(out, bucketView{Name: b.Name, Created: b.Created.UTC().Format(time.RFC3339)})
			}
			writeJSONStatus(w, http.StatusOK, out)
		case http.MethodPut:
			if err := h.Store.MakeBucket(r.Context(), r.URL.Query().Get("name")); err != nil {
				writeAdminError(w, err)
				return
			}
			writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	}

	rest = strings.TrimPrefix(rest, "/")
	bucket, tail, _ := strings.Cut(rest, "/")
	if tail == "" {
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := h.Store.DeleteBucket(r.Context(), bucket); err != nil {
			writeAdminError(w, err)
			return
		}
		writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	tail = strings.TrimPrefix(tail, "objects")
	tail = strings.TrimPrefix(tail, "/")
	if tail == "" {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		res, err := h.Store.ListObjects(r.Context(), bucket, store.ListOptions{
			Prefix:    q.Get("prefix"),
			Delimiter: q.Get("delimiter"),
			MaxKeys:   1000,
		})
		if err != nil {
			writeAdminError(w, err)
			return
		}
		type objectView struct {
			Key      string `json:"key"`
			Size     int64  `json:"size"`
			ETag     string `json:"etag"`
			Modified string `json:"modified"`
		}
		out := struct {
			Objects  []objectView `json:"objects"`
			Prefixes []string     `json:"prefixes"`
		}{Objects: []objectView{}, Prefixes: res.CommonPrefixes}
		for _, o := range res.Objects {
			out.Objects = append(out.Objects, objectView{Key: o.Name, Size: o.Size, ETag: o.ETag, Modified: o.ModTime.UTC().Format(time.RFC3339)})
		}
		writeJSONStatus(w, http.StatusOK, out)
		return
	}

	object := tail
	switch r.Method {
	case http.MethodGet:
		rc, info, err := h.Store.GetObject(r.Context(), bucket, object, "")
		if err != nil {
			h.writeStoreError(w, r, err)
			return
		}
		defer rc.Close()
		w.Header().Set("Content-Type", info.ContentType)
		w.Header().Set("ETag", quoteETag(info.ETag))
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
		_, _ = io.Copy(w, rc)
	case http.MethodPut:
		info, err := h.Store.PutObject(r.Context(), bucket, object, r.Body, r.ContentLength, objectContentType(r), userMetadata(r))
		if err != nil {
			h.writeStoreError(w, r, err)
			return
		}
		writeJSONStatus(w, http.StatusOK, map[string]string{"etag": info.ETag, "versionId": info.VersionID})
	case http.MethodDelete:
		if _, err := h.Store.DeleteObject(r.Context(), bucket, object, ""); err != nil {
			h.writeStoreError(w, r, err)
			return
		}
		writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
