package api

import (
	_ "embed"
	"net/http"
)

//go:embed ui/index.html
var uiIndexHTML []byte

func (h *Handler) ServeUI(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path != "/ui" && r.URL.Path != "/ui/" {
		return false
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(uiIndexHTML)
	return true
}
