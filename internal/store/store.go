package store

import "sync"

// Store is a concurrency-safe in-memory key/value map. It is the seam where
// durability (an append-only log, then a real on-disk engine) will hook in for
// later versions.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// New returns an empty Store.
func New() *Store {
	return &Store{data: make(map[string][]byte)}
}

// Set stores value under key, replacing any existing value.
func (s *Store) Set(key string, value []byte) {
	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()
}

// Get returns the value for key and whether it was present.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	return v, ok
}

// Delete removes the given keys and returns how many were actually present.
func (s *Store) Delete(keys ...string) int {
	s.mu.Lock()
	n := 0
	for _, k := range keys {
		if _, ok := s.data[k]; ok {
			delete(s.data, k)
			n++
		}
	}
	s.mu.Unlock()
	return n
}
