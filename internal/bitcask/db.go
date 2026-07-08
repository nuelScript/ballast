// Package bitcask is a log-structured key/value engine in the style of Bitcask.
// Values live in append-only segment files on disk; only an index (the keydir)
// mapping each key to its record's location is held in memory, so the dataset
// can exceed RAM. Merging rewrites the live records into a fresh file to reclaim
// the space taken by overwritten and deleted keys.
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

// defaultMaxFileSize is the size at which the active segment rolls over.
const defaultMaxFileSize = 4 << 20 // 4 MiB

// loc points at a value inside a segment file — the keydir's entry per key.
type loc struct {
	fileID    uint32
	valuePos  int64
	valueSize uint32
	tstamp    int64
}

// DB is a Bitcask-style storage engine over a directory of segment files.
type DB struct {
	dir string

	// mu guards the keydir and which files exist. Reads take RLock for their
	// whole duration, so a merge (Lock) cannot delete a file mid-read.
	mu     sync.RWMutex
	keydir map[string]loc

	// The write side is only touched while mu is held for writing.
	active       *os.File
	activeID     uint32
	activeOffset int64
	nextID       uint32
	maxFileSize  int64

	// readers caches read handles by file id. Concurrent readers hold mu.RLock,
	// so the map itself needs its own lock.
	readersMu sync.Mutex
	readers   map[uint32]*os.File
}

// Open opens (creating if needed) the database rooted at dir. Existing segment
// files are scanned to rebuild the keydir, and writes append to a fresh or
// resumed active segment.
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
		// Resume the newest segment, dropping any partial record a crash left at
		// its tail so future appends don't sit behind garbage.
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

// Get returns the value for key and whether it is present, reading it from disk.
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

// Set stores value under key.
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

// Delete writes a tombstone for each present key and returns how many existed.
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

// Merge rewrites the live records from every immutable segment into one fresh
// segment, then deletes the old ones — reclaiming the space held by overwritten
// and deleted keys. The active segment is left untouched. It is stop-the-world:
// reads and writes wait until it completes.
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
			continue // lives in the active segment; leave it for a later merge
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
		db.keydir[key] = loc{
			fileID:    mergeID,
			valuePos:  offset + headerSize + int64(len(key)),
			valueSize: l.valueSize,
			tstamp:    l.tstamp,
		}
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

// Sync fsyncs the active segment, making recent writes durable across a power
// loss (Append already survives a process crash).
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.active == nil {
		return nil
	}
	return db.active.Sync()
}

// Close closes every open segment handle.
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

// appendRecord writes one record to the active segment, rolling it first if the
// record would overflow the size limit. It assumes mu is held for writing.
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
	l := loc{
		fileID:    db.activeID,
		valuePos:  db.activeOffset + headerSize + int64(len(key)),
		valueSize: uint32(len(value)),
		tstamp:    tstamp,
	}
	db.activeOffset += int64(len(rec))
	return l, nil
}

// rollActive retires the current active segment (it stays readable) and starts a
// new one. It assumes mu is held for writing.
func (db *DB) rollActive() error {
	id := db.nextID
	db.nextID++
	return db.openActive(id, 0, true)
}

// openActive opens segment id as the active one for appending. With create set
// it is made if absent. The handle is also registered for reads.
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

// readerFor returns a read handle for the given segment, opening and caching it
// on first use.
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

// segmentIDs returns the ids of the segment files in dir, ascending.
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
