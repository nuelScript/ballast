package lsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// A WAL record is one atomic batch (a whole transaction):
//
//	crc(4) | seq(8) | count(4) | bodyLen(4) | body
//	body entry: kind(1) | keyLen(4) | valLen(4) | key | value
//
// The CRC covers everything after it, so a batch torn by a crash is dropped
// whole on replay rather than applied partially.
const walHeaderSize = 4 + 8 + 4 + 4

type batchEntry struct {
	kind  byte
	key   string
	value []byte
}

type wal struct {
	f *os.File
}

func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &wal{f: f}, nil
}

func (w *wal) append(seq uint64, batch []batchEntry) error {
	bodyLen := 0
	for _, e := range batch {
		bodyLen += 1 + 4 + 4 + len(e.key) + len(e.value)
	}
	buf := make([]byte, walHeaderSize+bodyLen)
	binary.LittleEndian.PutUint64(buf[4:], seq)
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(batch)))
	binary.LittleEndian.PutUint32(buf[16:], uint32(bodyLen))
	off := walHeaderSize
	for _, e := range batch {
		buf[off] = e.kind
		off++
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(e.key)))
		off += 4
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(e.value)))
		off += 4
		off += copy(buf[off:], e.key)
		off += copy(buf[off:], e.value)
	}
	binary.LittleEndian.PutUint32(buf, crc32.ChecksumIEEE(buf[4:]))
	_, err := w.f.Write(buf)
	return err
}

func (w *wal) reset() error {
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	_, err := w.f.Seek(0, io.SeekStart)
	return err
}

func (w *wal) close() error {
	return w.f.Close()
}

func replayWAL(path string, apply func(seq uint64, kind byte, key string, value []byte)) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	header := make([]byte, walHeaderSize)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		crc := binary.LittleEndian.Uint32(header[0:])
		seq := binary.LittleEndian.Uint64(header[4:])
		count := binary.LittleEndian.Uint32(header[12:])
		bodyLen := binary.LittleEndian.Uint32(header[16:])

		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(r, body); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // batch torn by a crash
			}
			return err
		}
		sum := crc32.NewIEEE()
		sum.Write(header[4:])
		sum.Write(body)
		if sum.Sum32() != crc {
			return nil
		}

		off := 0
		for i := uint32(0); i < count; i++ {
			kind := body[off]
			off++
			keyLen := int(binary.LittleEndian.Uint32(body[off:]))
			off += 4
			valLen := int(binary.LittleEndian.Uint32(body[off:]))
			off += 4
			key := string(body[off : off+keyLen])
			off += keyLen
			value := append([]byte(nil), body[off:off+valLen]...)
			off += valLen
			apply(seq, kind, key, value)
		}
	}
}
