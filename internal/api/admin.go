package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/kxj/gos3/internal/iam"
)

// ServeAdmin 分发管理接口 /gos3/admin/*。统一使用 HTTP Basic（仅 root）鉴权。
// 路由表：
//   - GET|PUT|DELETE /gos3/admin/users        用户管理
//   - GET|PUT|DELETE /gos3/admin/policies     策略管理
//   - PUT            /gos3/admin/attach       给用户绑定策略
//   - PUT            /gos3/admin/detach       解绑策略
//   - GET            /gos3/admin/presign      生成预签名下载 URL
//   - /gos3/admin/buckets...                  管理端 bucket/object 操作
//
// 命中该前缀即返回 true（表示已处理）。
func (h *Handler) ServeAdmin(w http.ResponseWriter, r *http.Request) bool {
	if !strings.HasPrefix(r.URL.Path, "/gos3/admin/") {
		return false
	}
	if !h.adminAuthorized(r) {
		WriteError(w, r, ErrAccessDenied)
		return true
	}
	if h.IAM == nil {
		writeJSONStatus(w, http.StatusInternalServerError, map[string]string{"error": "iam not configured"})
		return true
	}
	p := r.URL.Path
	switch {
	case p == "/gos3/admin/users":
		h.adminUsers(w, r)
	case p == "/gos3/admin/policies":
		h.adminPolicies(w, r)
	case p == "/gos3/admin/attach":
		h.adminAttach(w, r)
	case p == "/gos3/admin/detach":
		h.adminDetach(w, r)
	case p == "/gos3/admin/presign":
		h.adminPresign(w, r)
	case strings.HasPrefix(p, "/gos3/admin/buckets"):
		h.serveAdminBuckets(w, r, strings.TrimPrefix(p, "/gos3/admin/buckets"))
	default:
		http.NotFound(w, r)
	}
	return true
}

// adminAuthorized 校验管理接口的 Basic 认证，仅认 root 账号。
func (h *Handler) adminAuthorized(r *http.Request) bool {
	user, pass, ok := r.BasicAuth()
	return ok && user == h.RootUser && pass == h.RootPass
}

// adminUsers 用户管理。
//   - GET    /gos3/admin/users                              列出所有用户
//   - PUT    /gos3/admin/users?accessKey=&secretKey=[&status=]  新增/覆盖用户
//   - DELETE /gos3/admin/users?accessKey=                   删除用户
func (h *Handler) adminUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSONStatus(w, http.StatusOK, h.IAM.ListUsers())
	case http.MethodPut:
		q := r.URL.Query()
		status := q.Get("status")
		if status == "" {
			status = iam.StatusEnabled
		}
		err := h.IAM.AddUser(iam.User{
			AccessKey: q.Get("accessKey"),
			SecretKey: q.Get("secretKey"),
			Status:    status,
		})
		if err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
	case http.MethodDelete:
		if err := h.IAM.RemoveUser(r.URL.Query().Get("accessKey")); err != nil {
			writeAdminError(w, err)
			return
		}
		writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// adminPolicies 策略管理。
//   - GET    /gos3/admin/policies           列出所有策略名
//   - PUT    /gos3/admin/policies?name=     新增/覆盖策略（请求体为策略 JSON）
//   - DELETE /gos3/admin/policies?name=     删除策略
func (h *Handler) adminPolicies(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSONStatus(w, http.StatusOK, h.IAM.ListPolicies())
	case http.MethodPut:
		name := r.URL.Query().Get("name")
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		var policy iam.Policy
		if err := json.Unmarshal(body, &policy); err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": "invalid policy JSON"})
			return
		}
		if err := h.IAM.SetPolicy(name, policy); err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
	case http.MethodDelete:
		if err := h.IAM.DeletePolicy(r.URL.Query().Get("name")); err != nil {
			writeAdminError(w, err)
			return
		}
		writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// adminAttach 给指定用户绑定一条策略。
// PUT /gos3/admin/attach?accessKey=&policy=
func (h *Handler) adminAttach(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	if err := h.IAM.AttachPolicy(q.Get("accessKey"), q.Get("policy")); err != nil {
		writeAdminError(w, err)
		return
	}
	writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
}

// adminDetach 解除指定用户的某条策略绑定。
// PUT /gos3/admin/detach?accessKey=&policy=
func (h *Handler) adminDetach(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	if err := h.IAM.DetachPolicy(q.Get("accessKey"), q.Get("policy")); err != nil {
		writeAdminError(w, err)
		return
	}
	writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeAdminError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, iam.ErrUserNotFound) || errors.Is(err, iam.ErrPolicyNotFound) {
		status = http.StatusNotFound
	}
	writeJSONStatus(w, status, map[string]string{"error": err.Error()})
}

func writeJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
