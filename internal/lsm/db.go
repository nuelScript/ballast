// Package lsm is a log-structured merge-tree key/value engine with MVCC. Every
// write gets a sequence number and keys keep multiple versions; a read at a
// snapshot sees the newest version at or below its sequence. Writes go to a
// write-ahead log and an in-memory memtable that flushes to immutable, sorted
// SSTables; reads check the memtable then the SSTables newest-first, skipping
// tables a bloom filter rules out. Merging drops versions no snapshot needs.
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

	// mu guards the memtable, SSTable set, sequence counter, and snapshots.
	// Reads hold RLock for their whole duration, so a merge cannot delete an
	// SSTable mid-read.
	mu       sync.RWMutex
	mem      *memtable
	wal      *wal
	sstables []*sstable // ascending by id; newest last
	nextID   uint32
	memLimit int

	seq   uint64
	snaps map[uint64]int // active snapshot read-sequences -> refcount
}

func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if tmps, _ := filepath.Glob(filepath.Join(dir, "*.tmp")); tmps != nil {
		for _, t := range tmps {
			os.Remove(t)
		}
	}

	db := &DB{dir: dir, mem: newMemtable(), memLimit: defaultMemLimit, snaps: make(map[uint64]int)}

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
		db.seq = max(db.seq, s.maxSeq)
	}
	db.nextID = 1
	if len(ids) > 0 {
		db.nextID = ids[len(ids)-1] + 1
	}

	if err := replayWAL(db.walPath(), func(seq uint64, kind byte, key string, value []byte) {
		db.mem.put(key, seq, kind, value)
		db.seq = max(db.seq, seq)
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
	return db.lookup(key, db.seq)
}

// lookup returns the newest value of key visible at maxSeq. It assumes the
// caller holds mu. The first source with a version at or below maxSeq wins,
// because a key's versions grow newer from older SSTables to the memtable.
func (db *DB) lookup(key string, maxSeq uint64) ([]byte, bool, error) {
	if e, ok := db.mem.get(key, maxSeq); ok {
		if e.kind == kindTombstone {
			return nil, false, nil
		}
		return append([]byte(nil), e.value...), true, nil
	}
	for i := len(db.sstables) - 1; i >= 0; i-- {
		e, ok, err := db.sstables[i].get(key, maxSeq)
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

// latestSeq returns the sequence of the newest version of key, if any. It
// assumes the caller holds mu.
func (db *DB) latestSeq(key string) (uint64, bool) {
	if vs := db.mem.data[key]; len(vs) > 0 {
		return vs[len(vs)-1].seq, true
	}
	for i := len(db.sstables) - 1; i >= 0; i-- {
		if e, ok, err := db.sstables[i].get(key, ^uint64(0)); err == nil && ok {
			return e.seq, true
		}
	}
	return 0, false
}

func (db *DB) Set(key string, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.commit([]batchEntry{{kindPut, key, value}})
}

func (db *DB) Delete(keys ...string) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	var batch []batchEntry
	for _, key := range keys {
		if _, ok, err := db.lookup(key, db.seq); err != nil {
			return 0, err
		} else if ok {
			batch = append(batch, batchEntry{kindTombstone, key, nil})
		}
	}
	if err := db.commit(batch); err != nil {
		return 0, err
	}
	return len(batch), nil
}

// commit applies a batch atomically at a fresh sequence. It assumes mu is held.
func (db *DB) commit(batch []batchEntry) error {
	if len(batch) == 0 {
		return nil
	}
	db.seq++
	if err := db.wal.append(db.seq, batch); err != nil {
		return err
	}
	for _, e := range batch {
		db.mem.put(e.key, db.seq, e.kind, e.value)
	}
	return db.maybeFlush()
}

// Merge rewrites the SSTables into one, keeping for each key every version a
// live snapshot could still read and dropping the rest. The memtable holds the
// newest versions and is untouched.
func (db *DB) Merge() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.sstables) == 0 {
		return nil
	}

	var all []kvEntry
	for _, s := range db.sstables {
		es, err := s.all()
		if err != nil {
			return err
		}
		all = append(all, es...)
	}
	slices.SortFunc(all, entryLess)
	out := retainVersions(all, db.smallestSnap())

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

// retainVersions keeps, per key (entries arrive sorted key asc, seq desc), every
// version above the snapshot boundary plus the newest one at or below it; older
// versions are invisible to all snapshots, and a tombstone at the boundary is
// dropped because a deleted key reads the same as an absent one.
func retainVersions(all []kvEntry, smallestSnap uint64) []kvEntry {
	var out []kvEntry
	for i := 0; i < len(all); {
		key := all[i].key
		keptBelow := false
		for i < len(all) && all[i].key == key {
			v := all[i]
			i++
			if v.seq > smallestSnap {
				out = append(out, v)
			} else if !keptBelow {
				keptBelow = true
				if v.kind != kindTombstone {
					out = append(out, v)
				}
			}
		}
	}
	return out
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

func (db *DB) smallestSnap() uint64 {
	smallest := db.seq
	for s := range db.snaps {
		smallest = min(smallest, s)
	}
	return smallest
}

// maybeFlush, flush, and writeAndOpen assume mu is held for writing.
func (db *DB) maybeFlush() error {
	if db.mem.size < db.memLimit {
		return nil
	}
	return db.flush()
}

func (db *DB) flush() error {
	if db.mem.empty() {
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

func (db *DB) writeAndOpen(entries []kvEntry) (*sstable, error) {
	id := db.nextID
	db.nextID++
	path := db.sstPath(id)
	tmp := path + ".tmp"
	if err := writeSSTable(tmp, entries, db.seq); err != nil {
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
