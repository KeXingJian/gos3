package api

import (
	_ "embed"
	"net/http"
)

//go:embed ui/index.html
var uiIndexHTML []byte

// ServeUI 提供内置的简易管理页面（静态 HTML，公开访问）。
// GET /ui、/ui/
// 命中返回 true（已处理）；否则返回 false。
func (h *Handler) ServeUI(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path != "/ui" && r.URL.Path != "/ui/" {
		return false
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(uiIndexHTML)
	return true
}
