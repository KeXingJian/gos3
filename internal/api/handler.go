package api

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kxj/gos3/internal/store"
)

type Handler struct {
	Store   store.Store
	Region  string
	OwnerID string
	Logger  *slog.Logger
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
}

func (h *Handler) owner() Owner {
	return Owner{ID: h.OwnerID, DisplayName: "gos3"}
}

func (h *Handler) ListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := h.Store.ListBuckets(r.Context())
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	out := listAllMyBucketsResult{Xmlns: s3Namespace, Owner: h.owner()}
	for _, b := range buckets {
		out.Buckets = append(out.Buckets, bucketEntry{
			Name:         b.Name,
			CreationDate: b.Created.UTC().Format(time.RFC3339),
		})
	}
	writeXML(w, http.StatusOK, out)
}

func (h *Handler) CreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	_, _ = io.Copy(io.Discard, r.Body)
	if err := h.Store.MakeBucket(r.Context(), bucket); err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) HeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	created, ok, err := h.Store.BucketExists(r.Context(), bucket)
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	if !ok {
		WriteError(w, r, ErrNoSuchBucket)
		return
	}
	w.Header().Set("x-amz-bucket-region", h.Region)
	w.Header().Set("Last-Modified", created.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) DeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := h.Store.DeleteBucket(r.Context(), bucket); err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) ListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	if q.Has("location") {
		writeXML(w, http.StatusOK, locationConstraint{Xmlns: s3Namespace, Value: h.Region})
		return
	}
	if q.Has("versioning") {
		writeXML(w, http.StatusOK, versioningConfiguration{Xmlns: s3Namespace})
		return
	}

	opts := store.ListOptions{
		Prefix:    q.Get("prefix"),
		Delimiter: q.Get("delimiter"),
		MaxKeys:   1000,
	}
	if v := q.Get("max-keys"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			opts.MaxKeys = n
		}
	}
	if opts.MaxKeys <= 0 || opts.MaxKeys > 1000 {
		opts.MaxKeys = 1000
	}
	contToken := q.Get("continuation-token")
	marker := q.Get("marker")
	if contToken != "" {
		if m := decodeToken(contToken); m != "" {
			marker = m
		}
	}
	if marker == "" {
		marker = q.Get("start-after")
	}
	opts.Marker = marker

	res, err := h.Store.ListObjects(r.Context(), bucket, opts)
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}

	out := listBucketResult{
		Xmlns:             s3Namespace,
		Name:              bucket,
		Prefix:            opts.Prefix,
		Delimiter:         opts.Delimiter,
		MaxKeys:           opts.MaxKeys,
		IsTruncated:       res.IsTruncated,
		Marker:            q.Get("marker"),
		ContinuationToken: contToken,
		StartAfter:        q.Get("start-after"),
	}
	for _, o := range res.Objects {
		out.Contents = append(out.Contents, objectEntry{
			Key:          o.Name,
			LastModified: o.ModTime.UTC().Format(time.RFC3339),
			ETag:         quoteETag(o.ETag),
			Size:         o.Size,
			StorageClass: "STANDARD",
		})
	}
	for _, p := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{Prefix: p})
	}
	out.KeyCount = len(out.Contents) + len(out.CommonPrefixes)
	if res.IsTruncated && res.NextMarker != "" {
		out.NextMarker = res.NextMarker
		out.NextContinuationToken = encodeToken(res.NextMarker)
	}
	writeXML(w, http.StatusOK, out)
}

func (h *Handler) PutObject(w http.ResponseWriter, r *http.Request, bucket, object string) {
	info, err := h.Store.PutObject(r.Context(), bucket, object, r.Body, r.ContentLength, objectContentType(r), userMetadata(r))
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteETag(info.ETag))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) GetObject(w http.ResponseWriter, r *http.Request, bucket, object string) {
	rc, info, err := h.Store.GetObject(r.Context(), bucket, object)
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	defer rc.Close()
	h.setObjectHeaders(w, info)
	http.ServeContent(w, r, "", info.ModTime, rc)
}

func (h *Handler) HeadObject(w http.ResponseWriter, r *http.Request, bucket, object string) {
	info, err := h.Store.StatObject(r.Context(), bucket, object)
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	h.setObjectHeaders(w, info)
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.Header().Set("Last-Modified", info.ModTime.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) DeleteObject(w http.ResponseWriter, r *http.Request, bucket, object string) {
	if err := h.Store.DeleteObject(r.Context(), bucket, object); err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) DeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	var req deleteRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, r, ErrInvalidArgument)
		return
	}
	keys := make([]string, 0, len(req.Objects))
	for _, o := range req.Objects {
		keys = append(keys, o.Key)
	}
	errs := h.Store.DeleteObjects(r.Context(), bucket, keys)
	out := deleteResult{Xmlns: s3Namespace}
	for i, key := range keys {
		if errs != nil && i < len(errs) && errs[i] != nil {
			out.Errors = append(out.Errors, deleteError{Key: key, Code: "InternalError", Message: errs[i].Error()})
			continue
		}
		if !req.Quiet {
			out.Deleted = append(out.Deleted, deletedObject{Key: key})
		}
	}
	writeXML(w, http.StatusOK, out)
}

func (h *Handler) CreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, object string) {
	uploadID, err := h.Store.NewMultipartUpload(r.Context(), bucket, object, objectContentType(r), userMetadata(r))
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{
		Xmlns:    s3Namespace,
		Bucket:   bucket,
		Key:      object,
		UploadID: uploadID,
	})
}

func (h *Handler) UploadPart(w http.ResponseWriter, r *http.Request, bucket, object string) {
	uploadID := r.URL.Query().Get("uploadId")
	partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil {
		WriteError(w, r, ErrInvalidArgument)
		return
	}
	part, err := h.Store.PutObjectPart(r.Context(), bucket, object, uploadID, partNumber, r.Body)
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteETag(part.ETag))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) ListParts(w http.ResponseWriter, r *http.Request, bucket, object string) {
	uploadID := r.URL.Query().Get("uploadId")
	parts, err := h.Store.ListObjectParts(r.Context(), bucket, object, uploadID)
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	out := listPartsResult{
		Xmlns:        s3Namespace,
		Bucket:       bucket,
		Key:          object,
		UploadID:     uploadID,
		Initiator:    h.owner(),
		Owner:        h.owner(),
		StorageClass: "STANDARD",
		MaxParts:     1000,
	}
	for _, p := range parts {
		out.Parts = append(out.Parts, partXML{
			PartNumber:   p.PartNumber,
			LastModified: p.LastModified.UTC().Format(time.RFC3339),
			ETag:         quoteETag(p.ETag),
			Size:         p.Size,
		})
	}
	writeXML(w, http.StatusOK, out)
}

func (h *Handler) CompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, object string) {
	uploadID := r.URL.Query().Get("uploadId")
	var req completeMultipartUploadRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, r, ErrInvalidArgument)
		return
	}
	if len(req.Parts) == 0 {
		WriteError(w, r, ErrInvalidPart)
		return
	}
	parts := make([]store.CompletePart, 0, len(req.Parts))
	for _, p := range req.Parts {
		parts = append(parts, store.CompletePart{PartNumber: p.PartNumber, ETag: p.ETag})
	}
	info, err := h.Store.CompleteMultipartUpload(r.Context(), bucket, object, uploadID, parts)
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	writeXML(w, http.StatusOK, completeMultipartUploadResult{
		Xmlns:    s3Namespace,
		Location: "/" + bucket + "/" + object,
		Bucket:   bucket,
		Key:      object,
		ETag:     quoteETag(info.ETag),
	})
}

func (h *Handler) AbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, object string) {
	uploadID := r.URL.Query().Get("uploadId")
	if err := h.Store.AbortMultipartUpload(r.Context(), bucket, object, uploadID); err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) ListMultipartUploads(w http.ResponseWriter, r *http.Request, bucket string) {
	uploads, err := h.Store.ListMultipartUploads(r.Context(), bucket)
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	out := listMultipartUploadsResult{
		Xmlns:      s3Namespace,
		Bucket:     bucket,
		Prefix:     r.URL.Query().Get("prefix"),
		MaxUploads: 1000,
	}
	for _, u := range uploads {
		out.Uploads = append(out.Uploads, uploadXML{
			Key:       u.Object,
			UploadID:  u.UploadID,
			Initiated: u.Initiated.UTC().Format(time.RFC3339),
		})
	}
	writeXML(w, http.StatusOK, out)
}

func objectContentType(r *http.Request) string {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		return ct
	}
	return "application/octet-stream"
}

func userMetadata(r *http.Request) map[string]string {
	meta := make(map[string]string)
	for k, vals := range r.Header {
		if !strings.HasPrefix(k, "X-Amz-Meta-") || len(vals) == 0 {
			continue
		}
		meta[strings.ToLower(strings.TrimPrefix(k, "X-Amz-Meta-"))] = vals[0]
	}
	return meta
}

func (h *Handler) setObjectHeaders(w http.ResponseWriter, info store.ObjectInfo) {
	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set("ETag", quoteETag(info.ETag))
	w.Header().Set("Accept-Ranges", "bytes")
	for k, v := range info.UserMetadata {
		w.Header().Set("X-Amz-Meta-"+k, v)
	}
}

func (h *Handler) writeStoreError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, store.ErrBucketNotFound):
		WriteError(w, r, ErrNoSuchBucket)
	case errors.Is(err, store.ErrObjectNotFound):
		WriteError(w, r, ErrNoSuchKey)
	case errors.Is(err, store.ErrBucketExists):
		WriteError(w, r, ErrBucketAlreadyOwnedByYou)
	case errors.Is(err, store.ErrBucketNotEmpty):
		WriteError(w, r, ErrBucketNotEmpty)
	case errors.Is(err, store.ErrInvalidBucketName):
		WriteError(w, r, ErrInvalidBucketName)
	case errors.Is(err, store.ErrInvalidObjectName):
		WriteError(w, r, ErrInvalidArgument)
	case errors.Is(err, store.ErrUploadNotFound), errors.Is(err, store.ErrInvalidUploadID):
		WriteError(w, r, ErrNoSuchUpload)
	case errors.Is(err, store.ErrInvalidPart):
		WriteError(w, r, ErrInvalidPart)
	case errors.Is(err, store.ErrInvalidPartOrder):
		WriteError(w, r, ErrInvalidPartOrder)
	default:
		h.Logger.Error("[gos3: internal-error]", "path", r.URL.Path, "error", err.Error())
		WriteError(w, r, ErrInternalError)
	}
}

func quoteETag(etag string) string {
	if etag == "" {
		return etag
	}
	return "\"" + etag + "\""
}

func encodeToken(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}

func decodeToken(s string) string {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return ""
	}
	return string(b)
}
