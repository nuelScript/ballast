package bitcask

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// On-disk record layout (little-endian):
//
//	| crc32 (4) | tstamp (8) | kind (1) | keySize (4) | valueSize (4) | key | value |
//
// The CRC covers every byte after it, so a torn or bit-rotted record is caught
// on read. A delete is a record with kind == kindTombstone and no value.
const (
	headerSize = 4 + 8 + 1 + 4 + 4

	kindPut       byte = 0
	kindTombstone byte = 1
)

// encodeRecord serializes one record into a fresh byte slice.
func encodeRecord(kind byte, tstamp int64, key, value []byte) []byte {
	buf := make([]byte, headerSize+len(key)+len(value))
	binary.LittleEndian.PutUint64(buf[4:], uint64(tstamp))
	buf[12] = kind
	binary.LittleEndian.PutUint32(buf[13:], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[17:], uint32(len(value)))
	copy(buf[headerSize:], key)
	copy(buf[headerSize+len(key):], value)
	binary.LittleEndian.PutUint32(buf[0:], crc32.ChecksumIEEE(buf[4:]))
	return buf
}

// scanned is one decoded record's metadata, handed to the scan callback.
type scanned struct {
	key       []byte
	kind      byte
	tstamp    int64
	valuePos  int64 // absolute offset of the value bytes within the file
	valueSize uint32
}

// scanFile reads records from f in order, invoking visit for each valid one. It
// returns the byte offset at which valid data ends: a clean EOF, a record
// truncated by a crash mid-write, or the first CRC mismatch all stop the scan
// there. That offset is where the next append must begin.
func scanFile(f *os.File, visit func(scanned)) (int64, error) {
	r := bufio.NewReader(f)
	var offset int64
	header := make([]byte, headerSize)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return offset, nil
			}
			return offset, err
		}
		crc := binary.LittleEndian.Uint32(header[0:])
		tstamp := int64(binary.LittleEndian.Uint64(header[4:]))
		kind := header[12]
		keySize := binary.LittleEndian.Uint32(header[13:])
		valueSize := binary.LittleEndian.Uint32(header[17:])

		body := make([]byte, int(keySize)+int(valueSize))
		if _, err := io.ReadFull(r, body); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return offset, nil // truncated tail from a crash mid-append
			}
			return offset, err
		}

		sum := crc32.NewIEEE()
		sum.Write(header[4:])
		sum.Write(body)
		if sum.Sum32() != crc {
			return offset, nil // corrupt record; treat as the end of valid data
		}

		visit(scanned{
			key:       body[:keySize],
			kind:      kind,
			tstamp:    tstamp,
			valuePos:  offset + headerSize + int64(keySize),
			valueSize: valueSize,
		})
		offset += headerSize + int64(keySize) + int64(valueSize)
	}
}
