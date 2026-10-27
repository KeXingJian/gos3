package api

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kxj/gos3/internal/sign"
	"github.com/kxj/gos3/internal/store"
)

const maxPresignExpiry = 7 * 24 * 3600

// adminPresign 生成对象下载的预签名 URL（使用当前登录身份的凭据签名）。
// GET /gos3/admin/presign?bucket=&object=[&expires=秒]
// expires 默认 3600，范围 1..maxPresignExpiry（7 天）
// 响应：200 JSON {"url": "...", "expires": N}
func (h *Handler) adminPresign(w http.ResponseWriter, r *http.Request, sess adminSession) {
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
	// 预签名 URL 的权限等同于当前身份的 s3:GetObject；普通用户只能签自己有权读的对象。
	resource := "arn:aws:s3:::" + bucket + "/" + object
	if !h.IAM.IsAllowed(sess.AccessKey, "s3:GetObject", resource) {
		h.Logger.Warn("[gos3: admin-presign-denied]", "access-key", sess.AccessKey, "resource", resource)
		WriteError(w, r, ErrAccessDenied)
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
	link := sign.PresignGetURL(scheme, r.Host, "/"+bucket+"/"+object, sess.Credentials, h.Region, time.Now(), time.Duration(expires)*time.Second)
	writeJSONStatus(w, http.StatusOK, map[string]any{"url": link, "expires": expires})
}

// serveAdminBuckets 管理端的 bucket/object 操作（返回 JSON，供 UI 使用）。
// 依据 path 后缀与 method 分发：
//   - GET    /gos3/admin/buckets                              列出 bucket
//   - PUT    /gos3/admin/buckets?name=                        创建 bucket
//   - DELETE /gos3/admin/buckets/{bucket}                     删除 bucket
//   - GET    /gos3/admin/buckets/{bucket}/objects[?prefix=&delimiter=]  列出对象
//   - GET    /gos3/admin/buckets/{bucket}/objects/{object}           下载对象内容
//   - PUT    /gos3/admin/buckets/{bucket}/objects/{object}           上传对象
//   - DELETE /gos3/admin/buckets/{bucket}/objects/{object}           删除对象
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

	// 严格匹配 objects / objects/...；ServeAdmin 已先行拒绝未识别路由，
	// 这里再兜一层，避免 /bucket/objectsfoo 被当成对象 "foo"。
	if tail != "objects" && !strings.HasPrefix(tail, "objects/") {
		http.NotFound(w, r)
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
