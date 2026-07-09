package lsm

import "container/heap"

type KV struct {
	Key   string
	Value []byte
}

// Range returns key/value pairs with start <= key <= end in ascending order,
// stopping after limit pairs (limit <= 0 means no cap).
func (db *DB) Range(start, end string, limit int) ([]KV, error) {
	if start > end {
		return nil, nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Sources ordered newest -> oldest so the merge can prefer newer versions.
	sources := [][]kvEntry{db.mem.rangeEntries(start, end)}
	for i := len(db.sstables) - 1; i >= 0; i-- {
		es, err := db.sstables[i].rangeEntries(start, end)
		if err != nil {
			return nil, err
		}
		sources = append(sources, es)
	}
	return mergeRange(sources, limit), nil
}

// mergeRange k-way merges the sorted sources. For a key present in several, the
// newest source wins; tombstones drop the key from the output.
func mergeRange(sources [][]kvEntry, limit int) []KV {
	h := &mergeHeap{}
	for recency, es := range sources {
		if len(es) > 0 {
			heap.Push(h, &mergeCursor{entries: es, recency: recency})
		}
	}

	var out []KV
	for h.Len() > 0 {
		winner := heap.Pop(h).(*mergeCursor)
		e := winner.entries[winner.pos]
		advance(h, winner)
		for h.Len() > 0 && (*h)[0].entries[(*h)[0].pos].key == e.key {
			advance(h, heap.Pop(h).(*mergeCursor))
		}
		if e.kind == kindTombstone {
			continue
		}
		out = append(out, KV{Key: e.key, Value: e.value})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func advance(h *mergeHeap, c *mergeCursor) {
	c.pos++
	if c.pos < len(c.entries) {
		heap.Push(h, c)
	}
}

type mergeCursor struct {
	entries []kvEntry
	pos     int
	recency int // 0 = newest source
}

type mergeHeap []*mergeCursor

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	a, b := h[i].entries[h[i].pos], h[j].entries[h[j].pos]
	if a.key != b.key {
		return a.key < b.key
	}
	return h[i].recency < h[j].recency
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
