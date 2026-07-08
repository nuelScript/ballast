// Package engine is the storage layer: an in-memory dataset made durable by an
// append-only log. It is the seam that later versions grow into an on-disk
// engine without touching the protocol or server code.
package engine

import (
	"strings"
	"sync"

	"github.com/nuelScript/ballast/internal/aof"
	"github.com/nuelScript/ballast/internal/store"
)

// Engine couples the in-memory store with its durability log.
type Engine struct {
	mu    sync.Mutex // serializes writes so the log's order matches memory's
	store *store.Store
	aof   *aof.Log // nil when running purely in memory
}

// Open builds an Engine. If path is non-empty, an existing log there is replayed
// to restore state and subsequent writes are appended to it. An empty path runs
// purely in memory with no durability.
func Open(path string) (*Engine, error) {
	e := &Engine{store: store.New()}
	if path == "" {
		return e, nil
	}
	if err := aof.Replay(path, e.apply); err != nil {
		return nil, err
	}
	log, err := aof.Open(path)
	if err != nil {
		return nil, err
	}
	e.aof = log
	return e, nil
}

// Close releases the log, if any.
func (e *Engine) Close() error {
	if e.aof == nil {
		return nil
	}
	return e.aof.Close()
}

// Get returns the value for key and whether it is present.
func (e *Engine) Get(key string) ([]byte, bool) {
	return e.store.Get(key)
}

// Set logs then stores value under key. Logging first (write-ahead) means a
// reply of OK implies the write is already durable.
func (e *Engine) Set(key string, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.aof != nil {
		if err := e.aof.Append([][]byte{[]byte("SET"), []byte(key), value}); err != nil {
			return err
		}
	}
	e.store.Set(key, value)
	return nil
}

// Delete logs then removes keys, returning how many were present.
func (e *Engine) Delete(keys ...string) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.aof != nil {
		args := make([][]byte, 0, len(keys)+1)
		args = append(args, []byte("DEL"))
		for _, k := range keys {
			args = append(args, []byte(k))
		}
		if err := e.aof.Append(args); err != nil {
			return 0, err
		}
	}
	return e.store.Delete(keys...), nil
}

// apply replays one logged command into memory during Open. It runs before any
// connection is served, so it needs no locking. Unknown records are ignored to
// stay forward-compatible with logs written by later versions.
func (e *Engine) apply(args [][]byte) error {
	switch strings.ToUpper(string(args[0])) {
	case "SET":
		if len(args) == 3 {
			e.store.Set(string(args[1]), args[2])
		}
	case "DEL":
		keys := make([]string, 0, len(args)-1)
		for _, k := range args[1:] {
			keys = append(keys, string(k))
		}
		e.store.Delete(keys...)
	}
	return nil
}
