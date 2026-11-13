package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kxj/gos3/internal/store"
)

func TestObjectTaggingHandlers(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.NewFS(t.TempDir(), logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MakeBucket(context.Background(), "bucket"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutObject(context.Background(), "bucket", "key", strings.NewReader("x"), 1, "text/plain", nil); err != nil {
		t.Fatal(err)
	}
	h := &Handler{Store: st, Logger: logger}
	do := func(method, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/bucket/key?tagging", strings.NewReader(body))
		rec := httptest.NewRecorder()
		switch method {
		case http.MethodPut:
			h.PutObjectTagging(rec, req, "bucket", "key")
		case http.MethodGet:
			h.GetObjectTagging(rec, req, "bucket", "key")
		case http.MethodDelete:
			h.DeleteObjectTagging(rec, req, "bucket", "key")
		}
		return rec
	}
	put := do(http.MethodPut, `<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>`)
	if put.Code != http.StatusOK {
		t.Fatalf("put=%d %s", put.Code, put.Body.String())
	}
	get := do(http.MethodGet, "")
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), "prod") {
		t.Fatalf("get=%d %s", get.Code, get.Body.String())
	}
	del := do(http.MethodDelete, "")
	if del.Code != http.StatusNoContent {
		t.Fatalf("delete=%d", del.Code)
	}
}
