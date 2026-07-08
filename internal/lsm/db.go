// Package lsm is a log-structured merge-tree key/value engine. Writes go to a
// write-ahead log and an in-memory memtable; when the memtable fills it is
// flushed to an immutable, sorted SSTable. Reads check the memtable, then the
// SSTables newest-first, using a per-table bloom filter to skip those that
// cannot hold the key. Merging rewrites the SSTables to drop dead records.
package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
)

const defaultMemLimit = 4 << 20

type DB struct {
	dir string

	// mu guards the memtable and SSTable set. Reads hold RLock for their whole
	// duration, so a merge cannot delete an SSTable mid-read.
	mu       sync.RWMutex
	mem      *memtable
	wal      *wal
	sstables []*sstable // ascending by id; newest last
	nextID   uint32
	memLimit int
}

func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// Discard SSTables a crash left half-written before their atomic rename.
	if tmps, _ := filepath.Glob(filepath.Join(dir, "*.tmp")); tmps != nil {
		for _, t := range tmps {
			os.Remove(t)
		}
	}

	db := &DB{dir: dir, mem: newMemtable(), memLimit: defaultMemLimit}

	ids, err := db.sstIDs()
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		s, err := openSSTable(db.sstPath(id), id)
		if err != nil {
			return nil, err
		}
		db.sstables = append(db.sstables, s)
	}
	db.nextID = 1
	if len(ids) > 0 {
		db.nextID = ids[len(ids)-1] + 1
	}

	if err := replayWAL(db.walPath(), func(kind byte, key string, value []byte) {
		db.mem.put(key, kind, value)
	}); err != nil {
		return nil, err
	}
	w, err := openWAL(db.walPath())
	if err != nil {
		return nil, err
	}
	db.wal = w
	return db, nil
}

func (db *DB) Get(key string) ([]byte, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.lookup(key)
}

// lookup assumes the caller holds mu (for reading or writing).
func (db *DB) lookup(key string) ([]byte, bool, error) {
	if e, ok := db.mem.get(key); ok {
		if e.kind == kindTombstone {
			return nil, false, nil
		}
		return append([]byte(nil), e.value...), true, nil
	}
	for i := len(db.sstables) - 1; i >= 0; i-- {
		e, ok, err := db.sstables[i].get(key)
		if err != nil {
			return nil, false, err
		}
		if ok {
			if e.kind == kindTombstone {
				return nil, false, nil
			}
			return e.value, true, nil
		}
	}
	return nil, false, nil
}

func (db *DB) Set(key string, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.wal.append(kindPut, key, value); err != nil {
		return err
	}
	db.mem.put(key, kindPut, value)
	return db.maybeFlush()
}

func (db *DB) Delete(keys ...string) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	n := 0
	for _, key := range keys {
		_, ok, err := db.lookup(key)
		if err != nil {
			return n, err
		}
		if !ok {
			continue
		}
		if err := db.wal.append(kindTombstone, key, nil); err != nil {
			return n, err
		}
		db.mem.put(key, kindTombstone, nil)
		n++
	}
	if err := db.maybeFlush(); err != nil {
		return n, err
	}
	return n, nil
}

// Merge rewrites the live records from every SSTable into one, dropping
// overwritten keys and tombstones. The memtable is newer, so it is untouched.
func (db *DB) Merge() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.sstables) < 2 {
		return nil
	}

	merged := make(map[string]kvEntry)
	for _, s := range db.sstables { // oldest -> newest, so newest wins
		entries, err := s.all()
		if err != nil {
			return err
		}
		for _, e := range entries {
			merged[e.key] = e
		}
	}
	out := make([]kvEntry, 0, len(merged))
	for _, e := range merged {
		if e.kind != kindTombstone {
			out = append(out, e)
		}
	}
	slices.SortFunc(out, func(a, b kvEntry) int { return strings.Compare(a.key, b.key) })

	old := db.sstables
	db.sstables = nil
	if len(out) > 0 {
		s, err := db.writeAndOpen(out)
		if err != nil {
			return err
		}
		db.sstables = []*sstable{s}
	}
	for _, s := range old {
		s.f.Close()
		os.Remove(db.sstPath(s.id))
	}
	return nil
}

func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.wal == nil {
		return nil
	}
	return db.wal.f.Sync()
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	var firstErr error
	for _, s := range db.sstables {
		if err := s.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if db.wal != nil {
		if err := db.wal.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// maybeFlush and flush assume mu is held for writing.
func (db *DB) maybeFlush() error {
	if db.mem.size < db.memLimit {
		return nil
	}
	return db.flush()
}

func (db *DB) flush() error {
	if len(db.mem.data) == 0 {
		return nil
	}
	s, err := db.writeAndOpen(db.mem.sorted())
	if err != nil {
		return err
	}
	db.sstables = append(db.sstables, s)
	db.mem = newMemtable()
	return db.wal.reset()
}

// writeAndOpen writes entries to a new SSTable via a temp file and atomic
// rename, then opens it for reading.
func (db *DB) writeAndOpen(entries []kvEntry) (*sstable, error) {
	id := db.nextID
	db.nextID++
	path := db.sstPath(id)
	tmp := path + ".tmp"
	if err := writeSSTable(tmp, entries); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	return openSSTable(path, id)
}

func (db *DB) sstPath(id uint32) string {
	return filepath.Join(db.dir, fmt.Sprintf("%06d.sst", id))
}

func (db *DB) walPath() string {
	return filepath.Join(db.dir, "wal.log")
}

func (db *DB) sstIDs() ([]uint32, error) {
	entries, err := os.ReadDir(db.dir)
	if err != nil {
		return nil, err
	}
	var ids []uint32
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sst") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), ".sst"), 10, 32)
		if err != nil {
			continue
		}
		ids = append(ids, uint32(n))
	}
	slices.Sort(ids)
	return ids, nil
}
