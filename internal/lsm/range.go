package lsm

import "container/heap"

type KV struct {
	Key   string
	Value []byte
}

// Range returns key/value pairs with start <= key <= end in ascending order,
// each at the newest version visible now, stopping after limit pairs (limit <= 0
// means no cap).
func (db *DB) Range(start, end string, limit int) ([]KV, error) {
	if start > end {
		return nil, nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	snapshot := db.seq

	sources := [][]kvEntry{db.mem.rangeEntries(start, end)}
	for i := len(db.sstables) - 1; i >= 0; i-- {
		es, err := db.sstables[i].rangeEntries(start, end)
		if err != nil {
			return nil, err
		}
		sources = append(sources, es)
	}
	return mergeRange(sources, snapshot, limit), nil
}

// mergeRange k-way merges the sorted sources. For each key it takes the newest
// version at or below snapshot; a tombstone there drops the key from the output.
func mergeRange(sources [][]kvEntry, snapshot uint64, limit int) []KV {
	h := &mergeHeap{}
	for _, es := range sources {
		if len(es) > 0 {
			heap.Push(h, &mergeCursor{entries: es})
		}
	}

	var out []KV
	for h.Len() > 0 {
		key := (*h)[0].entries[(*h)[0].pos].key
		var chosen kvEntry
		found := false
		for h.Len() > 0 && (*h)[0].entries[(*h)[0].pos].key == key {
			c := heap.Pop(h).(*mergeCursor)
			if e := c.entries[c.pos]; !found && e.seq <= snapshot {
				chosen, found = e, true
			}
			c.pos++
			if c.pos < len(c.entries) {
				heap.Push(h, c)
			}
		}
		if found && chosen.kind != kindTombstone {
			out = append(out, KV{Key: key, Value: chosen.value})
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out
}

type mergeCursor struct {
	entries []kvEntry
	pos     int
}

// The heap yields entries ordered by key ascending, then sequence descending,
// so a key's versions surface newest-first.
type mergeHeap []*mergeCursor

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	a, b := h[i].entries[h[i].pos], h[j].entries[h[j].pos]
	if a.key != b.key {
		return a.key < b.key
	}
	return a.seq > b.seq
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) { *h = append(*h, x.(*mergeCursor)) }

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	c := old[n-1]
	*h = old[:n-1]
	return c
}
