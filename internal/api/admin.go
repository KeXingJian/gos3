package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/kxj/gos3/internal/iam"
)

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
	switch r.URL.Path {
	case "/gos3/admin/users":
		h.adminUsers(w, r)
	case "/gos3/admin/policies":
		h.adminPolicies(w, r)
	case "/gos3/admin/attach":
		h.adminAttach(w, r)
	case "/gos3/admin/detach":
		h.adminDetach(w, r)
	default:
		http.NotFound(w, r)
	}
	return true
}

func (h *Handler) adminAuthorized(r *http.Request) bool {
	user, pass, ok := r.BasicAuth()
	return ok && user == h.RootUser && pass == h.RootPass
}

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
