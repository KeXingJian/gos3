package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/kxj/gos3/internal/auth"
	"github.com/kxj/gos3/internal/iam"
)

func newAdminTestHandler(t *testing.T) *Handler {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := iam.New(t.TempDir(), auth.Credentials{AccessKey: "root", SecretKey: "rootpass"}, log)
	if err != nil {
		t.Fatalf("iam.New: %v", err)
	}
	if err := store.AddUser(iam.User{AccessKey: "alice", SecretKey: "alicepass"}); err != nil {
		t.Fatalf("AddUser: %v", err)
	}
	return &Handler{IAM: store, RootUser: "root", RootPass: "rootpass", Logger: log}
}

func doAdmin(h *Handler, method, target, user, pass string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	req.SetBasicAuth(user, pass)
	rec := httptest.NewRecorder()
	h.ServeAdmin(rec, req)
	return rec
}

// TestAdminWhoami 校验 root 与普通 IAM 用户都能识别身份，禁用用户/错误密码不能登录。
func TestAdminWhoami(t *testing.T) {
	h := newAdminTestHandler(t)

	rec := doAdmin(h, http.MethodGet, "/gos3/admin/whoami", "root", "rootpass")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"root":true`) {
		t.Fatalf("root whoami: code=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = doAdmin(h, http.MethodGet, "/gos3/admin/whoami", "alice", "alicepass")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"root":false`) {
		t.Fatalf("alice whoami: code=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"accessKey":"alice"`) {
		t.Fatalf("alice whoami accessKey missing: %s", rec.Body.String())
	}

	rec = doAdmin(h, http.MethodGet, "/gos3/admin/whoami", "alice", "wrongpass")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("bad password: code=%d, want 403", rec.Code)
	}

	if err := h.IAM.AddUser(iam.User{AccessKey: "bob", SecretKey: "bobpass", Status: iam.StatusDisabled}); err != nil {
		t.Fatalf("AddUser(bob): %v", err)
	}
	rec = doAdmin(h, http.MethodGet, "/gos3/admin/whoami", "bob", "bobpass")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("disabled user: code=%d, want 403", rec.Code)
	}
}

// TestAdminRootOnlyEndpoints 校验普通用户不能访问用户/策略管理接口，root 可以。
func TestAdminRootOnlyEndpoints(t *testing.T) {
	h := newAdminTestHandler(t)

	for _, target := range []string{"/gos3/admin/users", "/gos3/admin/policies"} {
		rec := doAdmin(h, http.MethodGet, target, "alice", "alicepass")
		if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "root privileges required") {
			t.Fatalf("alice %s: code=%d body=%s", target, rec.Code, rec.Body.String())
		}
		rec = doAdmin(h, http.MethodGet, target, "root", "rootpass")
		if rec.Code != http.StatusOK {
			t.Fatalf("root %s: code=%d body=%s", target, rec.Code, rec.Body.String())
		}
	}
}

// TestAdminBucketAction 校验管理端路由到 S3 action/resource 的映射。
func TestAdminBucketAction(t *testing.T) {
	cases := []struct {
		method   string
		rest     string
		query    url.Values
		action   string
		resource string
		routed   bool
	}{
		{http.MethodGet, "", nil, "s3:ListAllMyBuckets", "*", true},
		{http.MethodPut, "", url.Values{"name": {"demo"}}, "s3:CreateBucket", "arn:aws:s3:::demo", true},
		{http.MethodDelete, "/demo", nil, "s3:DeleteBucket", "arn:aws:s3:::demo", true},
		{http.MethodGet, "/demo/objects", nil, "s3:ListBucket", "arn:aws:s3:::demo", true},
		{http.MethodGet, "/demo/objects/a/b.txt", nil, "s3:GetObject", "arn:aws:s3:::demo/a/b.txt", true},
		{http.MethodPut, "/demo/objects/a.txt", nil, "s3:PutObject", "arn:aws:s3:::demo/a.txt", true},
		{http.MethodDelete, "/demo/objects/a.txt", nil, "s3:DeleteObject", "arn:aws:s3:::demo/a.txt", true},
		{http.MethodPost, "/demo/objects", nil, "", "", false},
		{http.MethodGet, "/demo/unknown", nil, "", "", false},
	}
	for _, tc := range cases {
		action, resource, routed := adminBucketAction(tc.method, tc.rest, tc.query)
		if routed != tc.routed || action != tc.action || resource != tc.resource {
			t.Errorf("%s %s: got (%q,%q,%v), want (%q,%q,%v)",
				tc.method, tc.rest, action, resource, routed, tc.action, tc.resource, tc.routed)
		}
	}
}

// TestAdminBucketsAuthorization 校验普通用户没有策略时被挡在存储访问之前。
func TestAdminBucketsAuthorization(t *testing.T) {
	h := newAdminTestHandler(t)

	rec := doAdmin(h, http.MethodGet, "/gos3/admin/buckets", "alice", "alicepass")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("alice list buckets without policy: code=%d, want 403", rec.Code)
	}
}

// TestAdminPresignRequiresGetObject 校验预签名按当前身份的 s3:GetObject 授权，
// 并用该身份自己的密钥签名（而不是 root）。
func TestAdminPresignRequiresGetObject(t *testing.T) {
	h := newAdminTestHandler(t)

	rec := doAdmin(h, http.MethodGet, "/gos3/admin/presign?bucket=demo&object=a.txt", "alice", "alicepass")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("presign without policy: code=%d, want 403", rec.Code)
	}

	var policy iam.Policy
	if err := json.Unmarshal([]byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::demo/*"}]}`), &policy); err != nil {
		t.Fatalf("policy json: %v", err)
	}
	if err := h.IAM.SetPolicy("read", policy); err != nil {
		t.Fatalf("SetPolicy: %v", err)
	}
	if err := h.IAM.AttachPolicy("alice", "read"); err != nil {
		t.Fatalf("AttachPolicy: %v", err)
	}

	rec = doAdmin(h, http.MethodGet, "/gos3/admin/presign?bucket=demo&object=a.txt", "alice", "alicepass")
	if rec.Code != http.StatusOK {
		t.Fatalf("presign with policy: code=%d body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode presign body: %v", err)
	}
	u, err := url.Parse(body.URL)
	if err != nil {
		t.Fatalf("parse presign url: %v", err)
	}
	if cred := u.Query().Get("X-Amz-Credential"); !strings.HasPrefix(cred, "alice/") {
		t.Fatalf("presign credential = %q, want alice/...", cred)
	}
}
