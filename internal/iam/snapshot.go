package iam

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// revisionFile 保存本地 IAM 状态的逻辑修订号。
// 修订号不进入 users.json/policies.json，避免破坏已有文件的格式。
const revisionFile = "revision"

// Snapshot 是 IAM 状态的全量快照，也是节点间复制的载荷。
// Revision 越大表示状态越新：接收方据此决定「整体覆盖」还是「并集合并」。
type Snapshot struct {
	Revision uint64            `json:"revision"`
	Users    []User            `json:"users"`
	Policies map[string]Policy `json:"policies"`
}

// Revision 返回当前逻辑修订号。
func (s *Store) Revision() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rev
}

// Snapshot 返回当前 IAM 状态的一致性快照。
// 用户按 accessKey 排序，保证同样的状态总是编码出同样的字节，便于比较与测试。
func (s *Store) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotLocked()
}

func (s *Store) snapshotLocked() Snapshot {
	users := make([]User, 0, len(s.users))
	for _, u := range s.users {
		// 深拷贝 Policies：避免快照/编码期间与 AttachPolicy 的 append 共享底层数组。
		c := u
		c.Policies = append([]string(nil), u.Policies...)
		users = append(users, c)
	}
	sort.Slice(users, func(i, j int) bool { return users[i].AccessKey < users[j].AccessKey })

	policies := make(map[string]Policy, len(s.policies))
	for name, p := range s.policies {
		policies[name] = p
	}
	return Snapshot{Revision: s.rev, Users: users, Policies: policies}
}

// SnapshotJSON 返回快照的 JSON 编码，用作节点间复制载荷。
func (s *Store) SnapshotJSON() ([]byte, error) {
	return json.Marshal(s.Snapshot())
}

// ApplySnapshotJSON 合并对端推送的快照，返回状态是否发生变化以及合并后的修订号。
//
// 状态型复制规则（无中心节点，最后写入者胜）：
//   - 对端修订号更大：整体覆盖本地状态，删除也随快照一起传播；
//   - 修订号相等：取并集，同名冲突用稳定的字典序规则裁决，保证各节点收敛到同一结果；
//   - 对端修订号更小：忽略（本地状态更新）。
func (s *Store) ApplySnapshotJSON(data []byte) (bool, uint64, error) {
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return false, s.Revision(), fmt.Errorf("iam: invalid snapshot: %w", err)
	}
	return s.applySnapshot(snap)
}

func (s *Store) applySnapshot(snap Snapshot) (bool, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	incomingUsers := sanitizeUsers(snap.Users)
	incomingPolicies := sanitizePolicies(snap.Policies)

	changed := false
	switch {
	case snap.Revision > s.rev:
		// 对端更新：以对端为准。整体替换才能让删除操作也传播过来。
		s.users = incomingUsers
		s.policies = incomingPolicies
		s.rev = snap.Revision
		changed = true
	case snap.Revision == s.rev:
		// 修订号相同但内容可能不同（并发写/旧数据无修订号）：取并集。
		if s.mergeLocked(incomingUsers, incomingPolicies) {
			s.rev++
			changed = true
		}
	default:
		return false, s.rev, nil
	}
	if !changed {
		return false, s.rev, nil
	}

	// 先落盘再返回：调用方收到 changed=true 时，本地状态已经持久化。
	if err := s.saveUsersLocked(); err != nil {
		return true, s.rev, err
	}
	if err := s.savePoliciesLocked(); err != nil {
		return true, s.rev, err
	}
	if err := s.saveRevLocked(); err != nil {
		return true, s.rev, err
	}
	return true, s.rev, nil
}

// mergeLocked 把对端状态并入本地（只增不减），返回是否有变化。
// 同名冲突用 canonical JSON 的字典序裁决：同一份输入在所有节点得到同一结果。
func (s *Store) mergeLocked(users map[string]User, policies map[string]Policy) bool {
	changed := false
	for key, incoming := range users {
		local, ok := s.users[key]
		if !ok {
			s.users[key] = incoming
			changed = true
			continue
		}
		if canonicalUser(incoming) > canonicalUser(local) {
			s.users[key] = incoming
			changed = true
		}
	}
	for name, incoming := range policies {
		local, ok := s.policies[name]
		if !ok {
			s.policies[name] = incoming
			changed = true
			continue
		}
		if canonicalPolicy(incoming) > canonicalPolicy(local) {
			s.policies[name] = incoming
			changed = true
		}
	}
	return changed
}

func sanitizeUsers(in []User) map[string]User {
	out := make(map[string]User, len(in))
	for _, u := range in {
		if u.AccessKey == "" || u.SecretKey == "" {
			continue
		}
		if u.Status == "" {
			u.Status = StatusEnabled
		}
		out[u.AccessKey] = u
	}
	return out
}

func sanitizePolicies(in map[string]Policy) map[string]Policy {
	out := make(map[string]Policy, len(in))
	for name, p := range in {
		if name == "" {
			continue
		}
		out[name] = p
	}
	return out
}

func canonicalUser(u User) string {
	c := u
	c.Policies = append([]string(nil), u.Policies...)
	sort.Strings(c.Policies)
	data, _ := json.Marshal(c)
	return string(data)
}

func canonicalPolicy(p Policy) string {
	data, _ := json.Marshal(p)
	return string(data)
}

// loadRevision 读取持久化的逻辑修订号。
// 兼容旧版本（没有 revision 文件）：已有数据视为已发生过一次变更，
// 这样它与空节点合并时修订号不同，能正确进入覆盖/追平流程。
func (s *Store) loadRevision() {
	data, err := os.ReadFile(filepath.Join(s.dir, revisionFile))
	if err == nil {
		if v, parseErr := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64); parseErr == nil {
			s.rev = v
		}
	}
	if s.rev == 0 && (len(s.users) > 0 || len(s.policies) > 0) {
		s.rev = 1
	}
}

// bumpRevLocked 在本地状态变更后递增修订号并持久化（调用方需持有写锁）。
// 修订号写失败不影响本次内存变更，只记录日志：下次同步会带着新修订号重试。
func (s *Store) bumpRevLocked() {
	s.rev++
	if err := s.saveRevLocked(); err != nil {
		s.log.Warn("[gos3: iam-revision-save-failed]", "error", err.Error())
	}
}

func (s *Store) saveRevLocked() error {
	path := filepath.Join(s.dir, revisionFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(s.rev, 10)), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
