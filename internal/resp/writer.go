package resp

import (
	"bufio"
	"io"
	"strconv"
)

type Writer struct {
	w *bufio.Writer
}

func NewWriter(wr io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(wr)}
}

func (w *Writer) WriteSimpleString(s string) error {
	w.w.WriteByte('+')
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

func (w *Writer) WriteError(s string) error {
	w.w.WriteByte('-')
	w.w.WriteString(s)
	_, err := w.w.WriteString("\r\n")
	return err
}

func (w *Writer) WriteInteger(n int64) error {
	w.w.WriteByte(':')
	w.w.WriteString(strconv.FormatInt(n, 10))
	_, err := w.w.WriteString("\r\n")
	return err
}

// WriteBulk encodes a nil slice as the null bulk string.
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

func (w *Writer) WriteNull() error {
	_, err := w.w.WriteString("$-1\r\n")
	return err
}

// WriteArray writes an array header; for n == 0 it is a complete empty array.
func (w *Writer) WriteArray(n int) error {
	w.w.WriteByte('*')
	w.w.WriteString(strconv.Itoa(n))
	_, err := w.w.WriteString("\r\n")
	return err
}

func (w *Writer) Flush() error {
	return w.w.Flush()
}
