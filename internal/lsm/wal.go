package lsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// WAL record: crc32(4) | kind(1) | keyLen(4) | valLen(4) | key | value.
// The CRC covers every byte after it.
const walHeaderSize = 4 + 1 + 4 + 4

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

func (w *wal) append(kind byte, key string, value []byte) error {
	buf := make([]byte, walHeaderSize+len(key)+len(value))
	buf[4] = kind
	binary.LittleEndian.PutUint32(buf[5:], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[9:], uint32(len(value)))
	copy(buf[13:], key)
	copy(buf[13+len(key):], value)
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

func replayWAL(path string, apply func(kind byte, key string, value []byte)) error {
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
		kind := header[4]
		keyLen := binary.LittleEndian.Uint32(header[5:])
		valLen := binary.LittleEndian.Uint32(header[9:])
		body := make([]byte, int(keyLen)+int(valLen))
		if _, err := io.ReadFull(r, body); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		sum := crc32.NewIEEE()
		sum.Write(header[4:])
		sum.Write(body)
		if sum.Sum32() != crc {
			return nil
		}
		apply(kind, string(body[:keyLen]), append([]byte(nil), body[keyLen:]...))
	}
}
