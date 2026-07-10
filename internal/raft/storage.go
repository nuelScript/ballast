package raft

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
)

// Storage persists the state Raft must not lose across a restart: the current
// term, the vote for that term, and the log.
type Storage interface {
	Save(term int, votedFor string, log []LogEntry) error
	Load() (term int, votedFor string, log []LogEntry, err error)
}

type persisted struct {
	Term     int
	VotedFor string
	Log      []LogEntry
}

// FileStorage persists the whole state as JSON, replaced atomically on each Save.
type FileStorage struct {
	path string
}

func NewFileStorage(path string) *FileStorage { return &FileStorage{path: path} }

func (s *FileStorage) Save(term int, votedFor string, log []LogEntry) error {
	data, err := json.Marshal(persisted{term, votedFor, log})
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *FileStorage) Load() (int, string, []LogEntry, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, "", nil, nil
	}
	if err != nil {
		return 0, "", nil, err
	}
	var p persisted
	if err := json.Unmarshal(data, &p); err != nil {
		return 0, "", nil, err
	}
	return p.Term, p.VotedFor, p.Log, nil
}

// MemStorage keeps state in memory; a fresh MemStorage over the same value
// simulates a restart in tests.
type MemStorage struct {
	mu       sync.Mutex
	term     int
	votedFor string
	log      []LogEntry
	saved    bool
}

func (s *MemStorage) Save(term int, votedFor string, log []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term, s.votedFor, s.saved = term, votedFor, true
	s.log = append([]LogEntry(nil), log...)
	return nil
}

func (s *MemStorage) Load() (int, string, []LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.saved {
		return 0, "", nil, nil
	}
	return s.term, s.votedFor, append([]LogEntry(nil), s.log...), nil
}
