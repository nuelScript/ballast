package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// ErrInvalidSyntax means the bytes on the wire were not valid RESP. Once seen,
// the stream is desynced and the connection should be dropped.
var ErrInvalidSyntax = errors.New("resp: invalid syntax")

// maxBulkLen caps a single bulk string, matching the ceiling Redis uses.
const maxBulkLen = 512 * 1024 * 1024

// Reader reads client commands using the RESP protocol. It accepts both the
// array-of-bulk-strings form real clients send and the plain inline form that
// is handy from netcat or telnet.
type Reader struct {
	r *bufio.Reader
}

// NewReader wraps rd with buffering.
func NewReader(rd io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(rd)}
}

// ReadCommand reads one command and returns its arguments; the first element is
// the command name. It returns io.EOF when the client disconnects cleanly.
func (r *Reader) ReadCommand() ([][]byte, error) {
	b, err := r.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if b == '*' {
		return r.readArrayCommand()
	}
	// Inline command: put the byte back and read the whole line.
	if err := r.r.UnreadByte(); err != nil {
		return nil, err
	}
	return r.readInlineCommand()
}

func (r *Reader) readArrayCommand() ([][]byte, error) {
	n, err := r.readInteger()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		// Empty or null array; caller just moves on to the next command.
		return [][]byte{}, nil
	}
	args := make([][]byte, 0, n)
	for i := int64(0); i < n; i++ {
		typ, err := r.r.ReadByte()
		if err != nil {
			return nil, err
		}
		if typ != '$' {
			return nil, fmt.Errorf("%w: expected bulk string, got %q", ErrInvalidSyntax, typ)
		}
		arg, err := r.readBulk()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}

func (r *Reader) readBulk() ([]byte, error) {
	n, err := r.readInteger()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil // null bulk string
	}
	if n > maxBulkLen {
		return nil, fmt.Errorf("%w: bulk length %d too large", ErrInvalidSyntax, n)
	}
	// Read the payload plus its trailing CRLF in one shot. This allocates a
	// fresh slice per argument, so callers may retain it without aliasing the
	// reader's internal buffer.
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return nil, err
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return nil, fmt.Errorf("%w: bulk string not terminated by CRLF", ErrInvalidSyntax)
	}
	return buf[:n], nil
}

// readInteger reads a length/count prefix line and parses it as an integer.
func (r *Reader) readInteger() (int64, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: bad integer %q", ErrInvalidSyntax, line)
	}
	return n, nil
}

// readLine reads through the next '\n' and returns the line without its
// terminator. A preceding '\r' is stripped when present, so lone-LF input from
// netcat is tolerated alongside proper CRLF framing.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-1]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line, nil
}

func (r *Reader) readInlineCommand() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	return splitInline(line), nil
}

// splitInline splits a line on runs of spaces, dropping empty fields.
func splitInline(line []byte) [][]byte {
	var args [][]byte
	i := 0
	for i < len(line) {
		for i < len(line) && line[i] == ' ' {
			i++
		}
		if i >= len(line) {
			break
		}
		start := i
		for i < len(line) && line[i] != ' ' {
			i++
		}
		args = append(args, line[start:i])
	}
	return args
}
