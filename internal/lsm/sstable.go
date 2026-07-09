package lsm

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

// SSTable layout: sorted data entries, then a sparse index (every Nth key ->
// offset), then the bloom filter, then a fixed footer pointing at both.
//
//	entry:  keyLen(4) | key | kind(1) | valLen(4) | value
//	footer: bloomOffset(8) | bloomLen(8) | indexOffset(8) | indexLen(8) | magic(8)
const (
	indexInterval   = 16
	bloomBitsPerKey = 10
	footerSize      = 40
	sstMagic        = uint64(0x62616c6c61737431)
)

type indexEntry struct {
	key    string
	offset int64
}

func encodeEntry(e kvEntry) []byte {
	buf := make([]byte, 4+len(e.key)+1+4+len(e.value))
	binary.LittleEndian.PutUint32(buf, uint32(len(e.key)))
	n := 4 + copy(buf[4:], e.key)
	buf[n] = e.kind
	n++
	binary.LittleEndian.PutUint32(buf[n:], uint32(len(e.value)))
	n += 4
	copy(buf[n:], e.value)
	return buf
}

func decodeEntry(buf []byte, off int) (kvEntry, int) {
	kl := int(binary.LittleEndian.Uint32(buf[off:]))
	off += 4
	key := string(buf[off : off+kl])
	off += kl
	kind := buf[off]
	off++
	vl := int(binary.LittleEndian.Uint32(buf[off:]))
	off += 4
	val := append([]byte(nil), buf[off:off+vl]...)
	off += vl
	return kvEntry{key, kind, val}, off
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	m, err := c.w.Write(p)
	c.n += int64(m)
	return m, err
}

func writeSSTable(path string, entries []kvEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	cw := &countingWriter{w: bw}

	bl := newBloom(len(entries), bloomBitsPerKey)
	var index []indexEntry
	for i, e := range entries {
		bl.add(e.key)
		if i%indexInterval == 0 {
			index = append(index, indexEntry{e.key, cw.n})
		}
		if _, err := cw.Write(encodeEntry(e)); err != nil {
			f.Close()
			return err
		}
	}
	indexOffset := cw.n

	var idxHdr [4]byte
	binary.LittleEndian.PutUint32(idxHdr[:], uint32(len(index)))
	cw.Write(idxHdr[:])
	for _, ie := range index {
		rec := make([]byte, 4+len(ie.key)+8)
		binary.LittleEndian.PutUint32(rec, uint32(len(ie.key)))
		copy(rec[4:], ie.key)
		binary.LittleEndian.PutUint64(rec[4+len(ie.key):], uint64(ie.offset))
		if _, err := cw.Write(rec); err != nil {
			f.Close()
			return err
		}
	}
	indexLen := cw.n - indexOffset

	bloomOffset := cw.n
	if _, err := cw.Write(bl.encode()); err != nil {
		f.Close()
		return err
	}
	bloomLen := cw.n - bloomOffset

	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:], uint64(bloomOffset))
	binary.LittleEndian.PutUint64(footer[8:], uint64(bloomLen))
	binary.LittleEndian.PutUint64(footer[16:], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[24:], uint64(indexLen))
	binary.LittleEndian.PutUint64(footer[32:], sstMagic)
	if _, err := cw.Write(footer); err != nil {
		f.Close()
		return err
	}

	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

type sstable struct {
	id      uint32
	f       *os.File
	index   []indexEntry
	bloom   *bloom
	dataEnd int64
}

func openSSTable(path string, id uint32) (*sstable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := info.Size()
	if size < footerSize {
		f.Close()
		return nil, fmt.Errorf("sstable %s too small", path)
	}

	footer := make([]byte, footerSize)
	if _, err := f.ReadAt(footer, size-footerSize); err != nil {
		f.Close()
		return nil, err
	}
	if binary.LittleEndian.Uint64(footer[32:]) != sstMagic {
		f.Close()
		return nil, fmt.Errorf("sstable %s bad magic", path)
	}
	bloomOffset := int64(binary.LittleEndian.Uint64(footer[0:]))
	bloomLen := int64(binary.LittleEndian.Uint64(footer[8:]))
	indexOffset := int64(binary.LittleEndian.Uint64(footer[16:]))
	indexLen := int64(binary.LittleEndian.Uint64(footer[24:]))

	idxBuf := make([]byte, indexLen)
	if _, err := f.ReadAt(idxBuf, indexOffset); err != nil {
		f.Close()
		return nil, err
	}
	count := int(binary.LittleEndian.Uint32(idxBuf))
	off := 4
	index := make([]indexEntry, 0, count)
	for i := 0; i < count; i++ {
		kl := int(binary.LittleEndian.Uint32(idxBuf[off:]))
		off += 4
		key := string(idxBuf[off : off+kl])
		off += kl
		offset := int64(binary.LittleEndian.Uint64(idxBuf[off:]))
		off += 8
		index = append(index, indexEntry{key, offset})
	}

	bloomBuf := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomBuf, bloomOffset); err != nil {
		f.Close()
		return nil, err
	}

	return &sstable{id: id, f: f, index: index, bloom: decodeBloom(bloomBuf), dataEnd: indexOffset}, nil
}

func (s *sstable) get(key string) (kvEntry, bool, error) {
	if !s.bloom.mayContain(key) {
		return kvEntry{}, false, nil
	}
	// Largest indexed key <= target bounds the one block that could hold it.
	i := sort.Search(len(s.index), func(i int) bool { return s.index[i].key > key }) - 1
	if i < 0 {
		return kvEntry{}, false, nil
	}
	start := s.index[i].offset
	end := s.dataEnd
	if i+1 < len(s.index) {
		end = s.index[i+1].offset
	}
	buf := make([]byte, end-start)
	if _, err := s.f.ReadAt(buf, start); err != nil {
		return kvEntry{}, false, err
	}
	for off := 0; off < len(buf); {
		e, next := decodeEntry(buf, off)
		if e.key == key {
			return e, true, nil
		}
		if e.key > key {
			break
		}
		off = next
	}
	return kvEntry{}, false, nil
}

// rangeEntries returns the entries with start <= key <= end, in key order. It
// reads only the blocks the sparse index says could hold that range.
func (s *sstable) rangeEntries(start, end string) ([]kvEntry, error) {
	if len(s.index) == 0 {
		return nil, nil
	}
	lo := sort.Search(len(s.index), func(i int) bool { return s.index[i].key > start }) - 1
	if lo < 0 {
		lo = 0
	}
	startOff := s.index[lo].offset
	endOff := s.dataEnd
	if hi := sort.Search(len(s.index), func(i int) bool { return s.index[i].key > end }); hi < len(s.index) {
		endOff = s.index[hi].offset
	}
	if endOff <= startOff {
		return nil, nil
	}

	buf := make([]byte, endOff-startOff)
	if _, err := s.f.ReadAt(buf, startOff); err != nil {
		return nil, err
	}
	var out []kvEntry
	for off := 0; off < len(buf); {
		e, next := decodeEntry(buf, off)
		off = next
		if e.key < start {
			continue
		}
		if e.key > end {
			break
		}
		out = append(out, e)
	}
	return out, nil
}

func (s *sstable) all() ([]kvEntry, error) {
	if s.dataEnd == 0 {
		return nil, nil
	}
	buf := make([]byte, s.dataEnd)
	if _, err := s.f.ReadAt(buf, 0); err != nil {
		return nil, err
	}
	var out []kvEntry
	for off := 0; off < len(buf); {
		e, next := decodeEntry(buf, off)
		out = append(out, e)
		off = next
	}
	return out, nil
}
