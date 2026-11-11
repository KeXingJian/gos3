package api

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kxj/gos3/internal/catalog"
)

// adminCatalog 处理 /gos3/admin/catalog/*。
//
//	GET  /gos3/admin/catalog/search   查询对象元数据
//	GET  /gos3/admin/catalog/stats    按 contentType / sizeBucket / bucket 聚合
//	GET  /gos3/admin/catalog/status   索引运行状态
//	POST /gos3/admin/catalog/reindex  从 sidecar 全量重建索引
func (h *Handler) adminCatalog(w http.ResponseWriter, r *http.Request) {
	if h.Catalog == nil {
		writeJSONStatus(w, http.StatusServiceUnavailable,
			map[string]string{"error": "catalog is not enabled (start server with -catalog)"})
		return
	}

	switch r.URL.Path {
	case "/gos3/admin/catalog/search":
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		f, err := parseCatalogFilter(r.URL.Query())
		if err != nil {
			writeCatalogError(w, err)
			return
		}
		res, err := h.Catalog.Search(f)
		if err != nil {
			writeCatalogError(w, err)
			return
		}
		w.Header().Set("X-Gos3-Catalog-Gen", strconv.FormatUint(res.Gen, 10))
		w.Header().Set("X-Gos3-Catalog-Scan", res.Scan)
		writeJSONStatus(w, http.StatusOK, res)

	case "/gos3/admin/catalog/stats":
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		opts, err := parseCatalogStats(r.URL.Query())
		if err != nil {
			writeCatalogError(w, err)
			return
		}
		res, err := h.Catalog.Stats(opts)
		if err != nil {
			writeCatalogError(w, err)
			return
		}
		w.Header().Set("X-Gos3-Catalog-Gen", strconv.FormatUint(res.Gen, 10))
		writeJSONStatus(w, http.StatusOK, res)

	case "/gos3/admin/catalog/status":
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSONStatus(w, http.StatusOK, h.Catalog.Status())

	case "/gos3/admin/catalog/reindex":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := h.Catalog.Rebuild(); err != nil {
			writeCatalogError(w, err)
			return
		}
		if h.Logger != nil {
			h.Logger.Info("[gos3: catalog-rebuild]")
		}
		writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})

	default:
		http.NotFound(w, r)
	}
}

func parseCatalogFilter(q url.Values) (catalog.Filter, error) {
	f := catalog.Filter{
		Bucket:      q.Get("bucket"),
		Prefix:      q.Get("prefix"),
		ContentType: q.Get("contentType"),
		Token:       q.Get("token"),
	}

	if s := q.Get("minSize"); s != "" {
		n, err := parseNonNegativeInt(s)
		if err != nil {
			return f, err
		}
		f.MinSize, f.MinSizeSet = n, true
	}
	if s := q.Get("maxSize"); s != "" {
		n, err := parseNonNegativeInt(s)
		if err != nil {
			return f, err
		}
		f.MaxSize, f.MaxSizeSet = n, true
	}
	if s := q.Get("after"); s != "" {
		t, err := parseCatalogTime(s)
		if err != nil {
			return f, err
		}
		f.After = t
	}
	if s := q.Get("before"); s != "" {
		t, err := parseCatalogTime(s)
		if err != nil {
			return f, err
		}
		f.Before = t
	}
	if s := q.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return f, catalog.ErrInvalidFilter
		}
		f.Limit = n
	}

	var err error
	if f.AllVersions, err = parseCatalogBool(q.Get("versions")); err != nil {
		return f, err
	}
	if f.IncludeDeleteMarkers, err = parseCatalogBool(q.Get("deleteMarkers")); err != nil {
		return f, err
	}

	for k, vals := range q {
		if len(vals) == 0 {
			continue
		}
		switch {
		case strings.HasPrefix(k, "meta."):
			key := normalizeMetaKey(strings.TrimPrefix(k, "meta."))
			if key == "" {
				return f, catalog.ErrInvalidFilter
			}
			if f.Meta == nil {
				f.Meta = make(map[string]string)
			}
			f.Meta[key] = vals[0]
		case strings.HasPrefix(k, "tag."):
			key := strings.TrimSpace(strings.TrimPrefix(k, "tag."))
			if key == "" {
				return f, catalog.ErrInvalidFilter
			}
			if f.Tag == nil {
				f.Tag = make(map[string]string)
			}
			f.Tag[key] = vals[0]
		}
	}
	return f, nil
}

func parseCatalogStats(q url.Values) (catalog.StatsOptions, error) {
	opts := catalog.StatsOptions{
		Bucket:  q.Get("bucket"),
		GroupBy: q.Get("groupBy"),
	}
	var err error
	if opts.AllVersions, err = parseCatalogBool(q.Get("versions")); err != nil {
		return opts, err
	}
	return opts, nil
}

func parseNonNegativeInt(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, catalog.ErrInvalidFilter
	}
	return n, nil
}

func parseCatalogBool(s string) (bool, error) {
	if s == "" {
		return false, nil
	}
	v, err := strconv.ParseBool(s)
	if err != nil {
		return false, catalog.ErrInvalidFilter
	}
	return v, nil
}

// parseCatalogTime 支持 RFC3339（含纳秒）与 Unix 秒。
func parseCatalogTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(n, 0).UTC(), nil
	}
	return time.Time{}, catalog.ErrInvalidFilter
}

// normalizeMetaKey 把 x-amz-meta-Owner 归一化为 owner，与 PutObject 侧一致。
func normalizeMetaKey(k string) string {
	k = strings.ToLower(strings.TrimSpace(k))
	return strings.TrimPrefix(k, "x-amz-meta-")
}

func writeCatalogError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, catalog.ErrInvalidFilter), errors.Is(err, catalog.ErrInvalidToken):
		status = http.StatusBadRequest
	case errors.Is(err, catalog.ErrRestartQuery):
		status = http.StatusConflict
	}
	writeJSONStatus(w, status, map[string]string{"error": err.Error()})
}
