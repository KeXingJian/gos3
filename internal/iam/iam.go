package iam

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/kxj/gos3/internal/auth"
)

const (
	StatusEnabled  = "enabled"
	StatusDisabled = "disabled"
)

type User struct {
	AccessKey string   `json:"accessKey"`
	SecretKey string   `json:"secretKey"`
	Policies  []string `json:"policies,omitempty"`
	Status    string   `json:"status"`
}

type Store struct {
	mu       sync.RWMutex
	dir      string
	users    map[string]User
	policies map[string]Policy
	root     auth.Credentials
	log      *slog.Logger
}

func New(dir string, root auth.Credentials, log *slog.Logger) (*Store, error) {
	s := &Store{
		dir:      dir,
		users:    make(map[string]User),
		policies: make(map[string]Policy),
		root:     root,
		log:      log,
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s.load()
	log.Info("[gos3: iam-ready]", "dir", dir, "users", len(s.users), "policies", len(s.policies))
	return s, nil
}

func (s *Store) Get(accessKey string) (auth.Credentials, bool) {
	if accessKey == s.root.AccessKey {
		return s.root, true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[accessKey]
	if !ok || u.Status == StatusDisabled {
		return auth.Credentials{}, false
	}
	return auth.Credentials{AccessKey: u.AccessKey, SecretKey: u.SecretKey}, true
}

func (s *Store) IsRoot(accessKey string) bool {
	return accessKey == s.root.AccessKey
}

func (s *Store) AddUser(u User) error {
	if u.AccessKey == "" || u.SecretKey == "" {
		return errors.New("accessKey and secretKey are required")
	}
	if u.Status == "" {
		u.Status = StatusEnabled
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[u.AccessKey] = u
	return s.saveUsersLocked()
}

func (s *Store) RemoveUser(accessKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[accessKey]; !ok {
		return ErrUserNotFound
	}
	delete(s.users, accessKey)
	return s.saveUsersLocked()
}

func (s *Store) ListUsers() []User {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]User, 0, len(s.users))
	for _, u := range s.users {
		out = append(out, u)
	}
	return out
}

func (s *Store) SetPolicy(name string, p Policy) error {
	if name == "" {
		return errors.New("policy name is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.policies[name] = p
	return s.savePoliciesLocked()
}

func (s *Store) DeletePolicy(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.policies, name)
	return s.savePoliciesLocked()
}

func (s *Store) ListPolicies() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.policies))
	for name := range s.policies {
		out = append(out, name)
	}
	return out
}

func (s *Store) AttachPolicy(accessKey, policy string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[accessKey]
	if !ok {
		return ErrUserNotFound
	}
	if _, ok := s.policies[policy]; !ok {
		return ErrPolicyNotFound
	}
	for _, p := range u.Policies {
		if p == policy {
			return nil
		}
	}
	u.Policies = append(u.Policies, policy)
	s.users[accessKey] = u
	return s.saveUsersLocked()
}

func (s *Store) DetachPolicy(accessKey, policy string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[accessKey]
	if !ok {
		return ErrUserNotFound
	}
	kept := make([]string, 0, len(u.Policies))
	for _, p := range u.Policies {
		if p != policy {
			kept = append(kept, p)
		}
	}
	u.Policies = kept
	s.users[accessKey] = u
	return s.saveUsersLocked()
}

func (s *Store) IsAllowed(accessKey, action, resource string) bool {
	if s.IsRoot(accessKey) {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[accessKey]
	if !ok || u.Status == StatusDisabled {
		return false
	}
	allowed := false
	for _, name := range u.Policies {
		policy, ok := s.policies[name]
		if !ok {
			continue
		}
		for _, st := range policy.Statement {
			if !matchAny(st.Action, action) || !matchAny(st.Resource, resource) {
				continue
			}
			switch strings.ToLower(st.Effect) {
			case "deny":
				return false
			case "allow":
				allowed = true
			}
		}
	}
	return allowed
}

func (s *Store) load() {
	var users []User
	if err := readJSON(filepath.Join(s.dir, "users.json"), &users); err == nil {
		for _, u := range users {
			s.users[u.AccessKey] = u
		}
	}
	var policies map[string]Policy
	if err := readJSON(filepath.Join(s.dir, "policies.json"), &policies); err == nil {
		s.policies = policies
	}
}

func (s *Store) saveUsersLocked() error {
	users := make([]User, 0, len(s.users))
	for _, u := range s.users {
		users = append(users, u)
	}
	return writeJSON(filepath.Join(s.dir, "users.json"), users)
}

func (s *Store) savePoliciesLocked() error {
	return writeJSON(filepath.Join(s.dir, "policies.json"), s.policies)
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
