package lsm

import "slices"

const (
	kindPut       byte = 0
	kindTombstone byte = 1
)

type kvEntry struct {
	key   string
	seq   uint64
	kind  byte
	value []byte
}

// entryLess orders entries by key ascending, then sequence descending, so the
// newest version of a key sorts first.
func entryLess(a, b kvEntry) int {
	if a.key != b.key {
		if a.key < b.key {
			return -1
		}
		return 1
	}
	switch {
	case a.seq > b.seq:
		return -1
	case a.seq < b.seq:
		return 1
	default:
		return 0
	}
}

type memtable struct {
	data map[string][]kvEntry // versions per key, appended in ascending seq
	size int
}

func newMemtable() *memtable {
	return &memtable{data: make(map[string][]kvEntry)}
}

func (m *memtable) put(key string, seq uint64, kind byte, value []byte) {
	m.data[key] = append(m.data[key], kvEntry{key, seq, kind, value})
	m.size += len(key) + len(value)
}

// get returns the newest version of key with seq <= maxSeq.
func (m *memtable) get(key string, maxSeq uint64) (kvEntry, bool) {
	versions := m.data[key]
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].seq <= maxSeq {
			return versions[i], true
		}
	}
	return kvEntry{}, false
}

func (m *memtable) empty() bool { return len(m.data) == 0 }

func (m *memtable) sorted() []kvEntry {
	out := make([]kvEntry, 0, m.size)
	for _, vs := range m.data {
		out = append(out, vs...)
	}
	slices.SortFunc(out, entryLess)
	return out
}

func (m *memtable) rangeEntries(start, end string) []kvEntry {
	var out []kvEntry
	for k, vs := range m.data {
		if k >= start && k <= end {
			out = append(out, vs...)
		}
	}
	slices.SortFunc(out, entryLess)
	return out
}
