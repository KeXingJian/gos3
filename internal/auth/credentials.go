package auth

import "sync"

type Credentials struct {
	AccessKey string
	SecretKey string
}

type Store struct {
	mu    sync.RWMutex
	creds map[string]Credentials
}

func NewStore(list ...Credentials) *Store {
	s := &Store{creds: make(map[string]Credentials, len(list))}
	for _, c := range list {
		if c.AccessKey != "" {
			s.creds[c.AccessKey] = c
		}
	}
	return s
}

func (s *Store) Get(accessKey string) (Credentials, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.creds[accessKey]
	return c, ok
}
