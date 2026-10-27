package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/kxj/gos3/internal/auth"
	"github.com/kxj/gos3/internal/iam"
)

// adminSession 是一次管理接口访问的身份：root，或已启用的 IAM 用户。
type adminSession struct {
	AccessKey   string
	Credentials auth.Credentials
	Root        bool
}

// ServeAdmin 分发管理接口 /gos3/admin/*。使用 HTTP Basic 鉴权：
//   - root 可访问全部接口；
//   - 普通 IAM 用户可登录控制台（whoami），访问 buckets/objects/presign 时
//     按该用户的 S3 策略逐操作授权（ListAllMyBuckets/CreateBucket/ListBucket/...）；
//   - users/policies/attach/detach 仍仅限 root，避免普通用户提权。
//
// 路由表：
//   - GET            /gos3/admin/whoami       当前身份（控制台登录用）
//   - GET|PUT|DELETE /gos3/admin/users        用户管理（仅 root）
//   - GET|PUT|DELETE /gos3/admin/policies     策略管理（仅 root）
//   - PUT            /gos3/admin/attach       给用户绑定策略（仅 root）
//   - PUT            /gos3/admin/detach       解绑策略（仅 root）
//   - GET            /gos3/admin/presign      生成预签名下载 URL（需 s3:GetObject）
//   - /gos3/admin/buckets...                  管理端 bucket/object 操作（按策略授权）
//
// 命中该前缀即返回 true（表示已处理）。
func (h *Handler) ServeAdmin(w http.ResponseWriter, r *http.Request) bool {
	if !strings.HasPrefix(r.URL.Path, "/gos3/admin/") {
		return false
	}
	if h.IAM == nil {
		writeJSONStatus(w, http.StatusInternalServerError, map[string]string{"error": "iam not configured"})
		return true
	}
	sess, ok := h.adminSession(r)
	if !ok {
		WriteError(w, r, ErrAccessDenied)
		return true
	}
	p := r.URL.Path
	switch {
	case p == "/gos3/admin/whoami":
		writeJSONStatus(w, http.StatusOK, map[string]any{"accessKey": sess.AccessKey, "root": sess.Root})
	case p == "/gos3/admin/users":
		if !sess.Root {
			writeAdminForbidden(w)
			return true
		}
		h.adminUsers(w, r)
	case p == "/gos3/admin/policies":
		if !sess.Root {
			writeAdminForbidden(w)
			return true
		}
		h.adminPolicies(w, r)
	case p == "/gos3/admin/attach":
		if !sess.Root {
			writeAdminForbidden(w)
			return true
		}
		h.adminAttach(w, r)
	case p == "/gos3/admin/detach":
		if !sess.Root {
			writeAdminForbidden(w)
			return true
		}
		h.adminDetach(w, r)
	case p == "/gos3/admin/presign":
		h.adminPresign(w, r, sess)
	case strings.HasPrefix(p, "/gos3/admin/buckets"):
		rest := strings.TrimPrefix(p, "/gos3/admin/buckets")
		// 只放行能明确映射到 S3 action 的路由：未识别路径直接 404，
		// 避免下层宽松解析把它当成对象路径而绕过策略授权。
		action, resource, routed := adminBucketAction(r.Method, rest, r.URL.Query())
		if !routed {
			http.NotFound(w, r)
			return true
		}
		if !h.IAM.IsAllowed(sess.AccessKey, action, resource) {
			h.Logger.Warn("[gos3: admin-access-denied]",
				"access-key", sess.AccessKey, "action", action, "resource", resource)
			WriteError(w, r, ErrAccessDenied)
			return true
		}
		h.serveAdminBuckets(w, r, rest)
	default:
		http.NotFound(w, r)
	}
	return true
}

// adminSession 校验管理接口的 Basic 凭据：先认 root，再认已启用的 IAM 用户。
// 使用常量时间比较，避免密码比较泄露长度/前缀信息。
func (h *Handler) adminSession(r *http.Request) (adminSession, bool) {
	user, pass, ok := r.BasicAuth()
	if !ok {
		return adminSession{}, false
	}
	if constantTimeEqual(user, h.RootUser) && constantTimeEqual(pass, h.RootPass) {
		return adminSession{
			AccessKey:   user,
			Credentials: auth.Credentials{AccessKey: h.RootUser, SecretKey: h.RootPass},
			Root:        true,
		}, true
	}
	creds, ok := h.IAM.Get(user)
	if !ok || !constantTimeEqual(pass, creds.SecretKey) {
		return adminSession{}, false
	}
	return adminSession{AccessKey: creds.AccessKey, Credentials: creds}, true
}

func constantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func writeAdminForbidden(w http.ResponseWriter) {
	writeJSONStatus(w, http.StatusForbidden, map[string]string{"error": "root privileges required"})
}

// adminBucketAction 把管理端 bucket/object 路由映射为 S3 action + resource，
// 供普通 IAM 用户按策略授权；未识别的路由返回 routed=false，交由处理器返回 405。
func adminBucketAction(method, rest string, q url.Values) (action, resource string, routed bool) {
	if rest == "" || rest == "/" {
		switch method {
		case http.MethodGet:
			return "s3:ListAllMyBuckets", "*", true
		case http.MethodPut:
			return "s3:CreateBucket", "arn:aws:s3:::" + q.Get("name"), true
		}
		return "", "", false
	}

	rest = strings.TrimPrefix(rest, "/")
	bucket, tail, _ := strings.Cut(rest, "/")
	bucketRes := "arn:aws:s3:::" + bucket
	if tail == "" {
		if method == http.MethodDelete {
			return "s3:DeleteBucket", bucketRes, true
		}
		return "", "", false
	}
	if tail == "objects" || tail == "objects/" {
		if method == http.MethodGet {
			return "s3:ListBucket", bucketRes, true
		}
		return "", "", false
	}
	if !strings.HasPrefix(tail, "objects/") {
		return "", "", false
	}

	objectRes := bucketRes + "/" + strings.TrimPrefix(tail, "objects/")
	switch method {
	case http.MethodGet:
		return "s3:GetObject", objectRes, true
	case http.MethodPut:
		return "s3:PutObject", objectRes, true
	case http.MethodDelete:
		return "s3:DeleteObject", objectRes, true
	}
	return "", "", false
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
		h.Logger.Info("[gos3: admin-user-put]", "access-key", q.Get("accessKey"))
		h.replicateIAM(r.Context())
		writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
	case http.MethodDelete:
		accessKey := r.URL.Query().Get("accessKey")
		if err := h.IAM.RemoveUser(accessKey); err != nil {
			writeAdminError(w, err)
			return
		}
		h.Logger.Info("[gos3: admin-user-delete]", "access-key", accessKey)
		h.replicateIAM(r.Context())
		writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// adminPolicies 策略管理。
//   - GET    /gos3/admin/policies           列出所有策略名；带 ?name= 返回该策略文档
//   - PUT    /gos3/admin/policies?name=     新增/覆盖策略（请求体为策略 JSON）
//   - DELETE /gos3/admin/policies?name=     删除策略
func (h *Handler) adminPolicies(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// 带 name 时返回策略文档（供控制台回显/编辑），不带时返回策略名列表。
		if name := r.URL.Query().Get("name"); name != "" {
			p, ok := h.IAM.GetPolicy(name)
			if !ok {
				writeJSONStatus(w, http.StatusNotFound, map[string]string{"error": iam.ErrPolicyNotFound.Error()})
				return
			}
			writeJSONStatus(w, http.StatusOK, map[string]any{"name": name, "policy": p})
			return
		}
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
		h.Logger.Info("[gos3: admin-policy-put]", "policy", name)
		h.replicateIAM(r.Context())
		writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
	case http.MethodDelete:
		name := r.URL.Query().Get("name")
		if err := h.IAM.DeletePolicy(name); err != nil {
			writeAdminError(w, err)
			return
		}
		h.Logger.Info("[gos3: admin-policy-delete]", "policy", name)
		h.replicateIAM(r.Context())
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
	h.Logger.Info("[gos3: admin-policy-attach]", "access-key", q.Get("accessKey"), "policy", q.Get("policy"))
	h.replicateIAM(r.Context())
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
	h.Logger.Info("[gos3: admin-policy-detach]", "access-key", q.Get("accessKey"), "policy", q.Get("policy"))
	h.replicateIAM(r.Context())
	writeJSONStatus(w, http.StatusOK, map[string]string{"status": "ok"})
}

// replicateIAM 把本次管理变更同步推送到其他节点。
// 对端离线时只记日志，由后台周期对账兜底，管理请求仍然成功。
func (h *Handler) replicateIAM(ctx context.Context) {
	if h.IAMReplicator == nil {
		return
	}
	h.IAMReplicator.Replicate(ctx)
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
