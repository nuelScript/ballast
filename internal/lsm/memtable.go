package lsm

import (
	"slices"
	"strings"
)

const (
	kindPut       byte = 0
	kindTombstone byte = 1
)

type kvEntry struct {
	key   string
	kind  byte
	value []byte
}

type memtable struct {
	data map[string]kvEntry
	size int
}

func newMemtable() *memtable {
	return &memtable{data: make(map[string]kvEntry)}
}

func (m *memtable) put(key string, kind byte, value []byte) {
	if old, ok := m.data[key]; ok {
		m.size -= len(old.key) + len(old.value)
	}
	m.data[key] = kvEntry{key, kind, value}
	m.size += len(key) + len(value)
}

func (m *memtable) get(key string) (kvEntry, bool) {
	e, ok := m.data[key]
	return e, ok
}

func (m *memtable) sorted() []kvEntry {
	out := make([]kvEntry, 0, len(m.data))
	for _, e := range m.data {
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b kvEntry) int { return strings.Compare(a.key, b.key) })
	return out
}

func (m *memtable) rangeEntries(start, end string) []kvEntry {
	var out []kvEntry
	for _, e := range m.data {
		if e.key >= start && e.key <= end {
			out = append(out, e)
		}
	}
	slices.SortFunc(out, func(a, b kvEntry) int { return strings.Compare(a.key, b.key) })
	return out
}
