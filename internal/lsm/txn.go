package lsm

import "errors"

// ErrConflict is returned by Commit when another transaction wrote one of this
// transaction's keys after it began.
var ErrConflict = errors.New("transaction conflict")

// Txn is a read/write transaction with snapshot isolation. Reads see the
// database as of Begin plus the transaction's own buffered writes.
type Txn struct {
	db      *DB
	readSeq uint64
	writes  map[string]batchEntry
	done    bool
}

func (db *DB) Begin() *Txn {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.snaps[db.seq]++
	return &Txn{db: db, readSeq: db.seq, writes: make(map[string]batchEntry)}
}

func (t *Txn) Get(key string) ([]byte, bool, error) {
	if e, ok := t.writes[key]; ok {
		if e.kind == kindTombstone {
			return nil, false, nil
		}
		return append([]byte(nil), e.value...), true, nil
	}
	t.db.mu.RLock()
	defer t.db.mu.RUnlock()
	return t.db.lookup(key, t.readSeq)
}

func (t *Txn) Set(key string, value []byte) {
	t.writes[key] = batchEntry{kindPut, key, append([]byte(nil), value...)}
}

func (t *Txn) Delete(key string) {
	t.writes[key] = batchEntry{kindTombstone, key, nil}
}

// Commit applies the buffered writes atomically at a new sequence, or returns
// ErrConflict if any written key changed since the transaction began.
func (t *Txn) Commit() error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if t.done {
		return errors.New("transaction already finished")
	}
	for key := range t.writes {
		if s, ok := t.db.latestSeq(key); ok && s > t.readSeq {
			t.release()
			return ErrConflict
		}
	}
	batch := make([]batchEntry, 0, len(t.writes))
	for _, e := range t.writes {
		batch = append(batch, e)
	}
	t.release()
	return t.db.commit(batch)
}

func (t *Txn) Rollback() {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	t.release()
}

// release drops the transaction's snapshot. It assumes mu is held.
func (t *Txn) release() {
	if t.done {
		return
	}
	t.done = true
	if t.db.snaps[t.readSeq] <= 1 {
		delete(t.db.snaps, t.readSeq)
	} else {
		t.db.snaps[t.readSeq]--
	}
}
