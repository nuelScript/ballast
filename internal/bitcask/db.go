// Package bitcask is a log-structured key/value engine: values live in
// append-only segment files on disk, and only an index (the keydir) mapping each
// key to its location is kept in memory, so the dataset can exceed RAM.
package bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultMaxFileSize = 4 << 20

type loc struct {
	fileID    uint32
	valuePos  int64
	valueSize uint32
	tstamp    int64
}

type DB struct {
	dir string

	// mu guards the keydir and which files exist. Reads hold RLock for their
	// whole duration, so a merge cannot delete a segment mid-read.
	mu     sync.RWMutex
	keydir map[string]loc

	active       *os.File
	activeID     uint32
	activeOffset int64
	nextID       uint32
	maxFileSize  int64

	readersMu sync.Mutex
	readers   map[uint32]*os.File
}

func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{
		dir:         dir,
		keydir:      make(map[string]loc),
		readers:     make(map[uint32]*os.File),
		maxFileSize: defaultMaxFileSize,
	}

	ids, err := db.segmentIDs()
	if err != nil {
		return nil, err
	}

	var lastValidEnd int64
	for i, id := range ids {
		f, err := os.Open(db.segmentPath(id))
		if err != nil {
			return nil, err
		}
		end, err := scanFile(f, func(s scanned) {
			switch s.kind {
			case kindPut:
				db.keydir[string(s.key)] = loc{id, s.valuePos, s.valueSize, s.tstamp}
			case kindTombstone:
				delete(db.keydir, string(s.key))
			}
		})
		f.Close()
		if err != nil {
			return nil, err
		}
		if i == len(ids)-1 {
			lastValidEnd = end
		}
	}

	if len(ids) == 0 {
		if err := db.openActive(1, 0, true); err != nil {
			return nil, err
		}
	} else {
		// Drop any partial record a crash left at the tail before resuming.
		lastID := ids[len(ids)-1]
		if err := os.Truncate(db.segmentPath(lastID), lastValidEnd); err != nil {
			return nil, err
		}
		if err := db.openActive(lastID, lastValidEnd, false); err != nil {
			return nil, err
		}
	}
	db.nextID = db.activeID + 1
	return db, nil
}

func (db *DB) Get(key string) ([]byte, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	l, ok := db.keydir[key]
	if !ok {
		return nil, false, nil
	}
	f, err := db.readerFor(l.fileID)
	if err != nil {
		return nil, false, err
	}
	buf := make([]byte, l.valueSize)
	if _, err := f.ReadAt(buf, l.valuePos); err != nil {
		return nil, false, err
	}
	return buf, true, nil
}

func (db *DB) Set(key string, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	l, err := db.appendRecord(kindPut, key, value)
	if err != nil {
		return err
	}
	db.keydir[key] = l
	return nil
}

// Delete returns how many of the keys existed.
func (db *DB) Delete(keys ...string) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	n := 0
	for _, key := range keys {
		if _, ok := db.keydir[key]; !ok {
			continue
		}
		if _, err := db.appendRecord(kindTombstone, key, nil); err != nil {
			return n, err
		}
		delete(db.keydir, key)
		n++
	}
	return n, nil
}

// Merge rewrites the live records from the immutable segments into a fresh one
// and deletes the originals, reclaiming space from overwritten and deleted keys.
func (db *DB) Merge() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.readersMu.Lock()
	var immutable []uint32
	for id := range db.readers {
		if id != db.activeID {
			immutable = append(immutable, id)
		}
	}
	db.readersMu.Unlock()
	if len(immutable) == 0 {
		return nil
	}
	immutableSet := make(map[uint32]struct{}, len(immutable))
	for _, id := range immutable {
		immutableSet[id] = struct{}{}
	}

	mergeID := db.nextID
	db.nextID++
	mf, err := os.OpenFile(db.segmentPath(mergeID), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	var offset int64
	for key, l := range db.keydir {
		if _, ok := immutableSet[l.fileID]; !ok {
			continue // in the active segment; a later merge handles it
		}
		rf, err := db.readerFor(l.fileID)
		if err != nil {
			mf.Close()
			return err
		}
		val := make([]byte, l.valueSize)
		if _, err := rf.ReadAt(val, l.valuePos); err != nil {
			mf.Close()
			return err
		}
		rec := encodeRecord(kindPut, l.tstamp, []byte(key), val)
		if _, err := mf.Write(rec); err != nil {
			mf.Close()
			return err
		}
		db.keydir[key] = loc{mergeID, offset + headerSize + int64(len(key)), l.valueSize, l.tstamp}
		offset += int64(len(rec))
	}

	db.readersMu.Lock()
	if offset > 0 {
		db.readers[mergeID] = mf
	} else {
		mf.Close()
		os.Remove(db.segmentPath(mergeID))
	}
	for _, id := range immutable {
		if f, ok := db.readers[id]; ok {
			f.Close()
			delete(db.readers, id)
		}
		os.Remove(db.segmentPath(id))
	}
	db.readersMu.Unlock()
	return nil
}

func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.active == nil {
		return nil
	}
	return db.active.Sync()
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.readersMu.Lock()
	defer db.readersMu.Unlock()
	var firstErr error
	for id, f := range db.readers {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(db.readers, id)
	}
	db.active = nil
	return firstErr
}

// appendRecord assumes mu is held for writing.
func (db *DB) appendRecord(kind byte, key string, value []byte) (loc, error) {
	tstamp := time.Now().UnixNano()
	rec := encodeRecord(kind, tstamp, []byte(key), value)
	if db.activeOffset > 0 && db.activeOffset+int64(len(rec)) > db.maxFileSize {
		if err := db.rollActive(); err != nil {
			return loc{}, err
		}
	}
	if _, err := db.active.Write(rec); err != nil {
		return loc{}, err
	}
	l := loc{db.activeID, db.activeOffset + headerSize + int64(len(key)), uint32(len(value)), tstamp}
	db.activeOffset += int64(len(rec))
	return l, nil
}

func (db *DB) rollActive() error {
	id := db.nextID
	db.nextID++
	return db.openActive(id, 0, true)
}

func (db *DB) openActive(id uint32, offset int64, create bool) error {
	flags := os.O_RDWR | os.O_APPEND
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(db.segmentPath(id), flags, 0o644)
	if err != nil {
		return err
	}
	db.active = f
	db.activeID = id
	db.activeOffset = offset
	db.readersMu.Lock()
	db.readers[id] = f
	db.readersMu.Unlock()
	return nil
}

func (db *DB) readerFor(fileID uint32) (*os.File, error) {
	db.readersMu.Lock()
	defer db.readersMu.Unlock()
	if f, ok := db.readers[fileID]; ok {
		return f, nil
	}
	f, err := os.Open(db.segmentPath(fileID))
	if err != nil {
		return nil, err
	}
	db.readers[fileID] = f
	return f, nil
}

func (db *DB) segmentPath(id uint32) string {
	return filepath.Join(db.dir, fmt.Sprintf("%09d.data", id))
}

func (db *DB) segmentIDs() ([]uint32, error) {
	entries, err := os.ReadDir(db.dir)
	if err != nil {
		return nil, err
	}
	var ids []uint32
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".data") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), ".data"), 10, 32)
		if err != nil {
			continue
		}
		ids = append(ids, uint32(n))
	}
	slices.Sort(ids)
	return ids, nil
}
