package resp

import (
	"bufio"
	"io"
	"strconv"
)

// Writer serializes replies back to the client using RESP. Writes are buffered;
// call Flush once per command to push a full reply to the connection.
type Writer struct {
	w *bufio.Writer
}

// NewWriter wraps wr with buffering.
func NewWriter(wr io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(wr)}
}

// WriteSimpleString writes a "+OK"-style reply.
func (w *Writer) WriteSimpleString(s string) error {
	w.w.WriteByte('+')
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteError writes a "-ERR ..."-style reply.
func (w *Writer) WriteError(s string) error {
	w.w.WriteByte('-')
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteInteger writes a ":42"-style reply.
func (w *Writer) WriteInteger(n int64) error {
	w.w.WriteByte(':')
	w.w.WriteString(strconv.FormatInt(n, 10))
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteBulk writes a bulk string. A nil slice becomes the null bulk string.
func (w *Writer) WriteBulk(b []byte) error {
	if b == nil {
		return w.WriteNull()
	}
	w.w.WriteByte('$')
	w.w.WriteString(strconv.Itoa(len(b)))
	w.w.WriteString("\r\n")
	w.w.Write(b)
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteNull writes the null bulk string ("$-1").
func (w *Writer) WriteNull() error {
	_, err := w.w.WriteString("$-1\r\n")
	return err
}

// WriteArray writes an array header of length n. For n == 0 this is a complete
// empty-array reply; otherwise the caller writes n elements after it.
func (w *Writer) WriteArray(n int) error {
	w.w.WriteByte('*')
	w.w.WriteString(strconv.Itoa(n))
	_, err := w.w.WriteString("\r\n")
	return err
}

// Flush pushes buffered bytes to the underlying connection.
func (w *Writer) Flush() error {
	return w.w.Flush()
}
