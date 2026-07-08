package resp

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func TestReadArrayCommand(t *testing.T) {
	input := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	args, err := NewReader(strings.NewReader(input)).ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	assertArgs(t, args, "SET", "foo", "bar")
}

func TestReadInlineCommand(t *testing.T) {
	// Lone-LF, as netcat sends, must parse too.
	args, err := NewReader(strings.NewReader("PING\n")).ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	assertArgs(t, args, "PING")
}

func TestReadBulkIsBinarySafe(t *testing.T) {
	val := "a\r\n\x00b" // embedded CRLF and NUL
	input := "*1\r\n$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"
	args, err := NewReader(strings.NewReader(input)).ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 1 || string(args[0]) != val {
		t.Fatalf("got %q, want %q", args, val)
	}
}

func TestReadRejectsBadType(t *testing.T) {
	// Array claims one element but supplies an integer, not a bulk string.
	_, err := NewReader(strings.NewReader("*1\r\n:5\r\n")).ReadCommand()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestWriterEncoding(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteSimpleString("OK")
	w.WriteError("ERR nope")
	w.WriteInteger(42)
	w.WriteBulk([]byte("hi"))
	w.WriteNull()
	w.WriteArray(0)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	want := "+OK\r\n-ERR nope\r\n:42\r\n$2\r\nhi\r\n$-1\r\n*0\r\n"
	if buf.String() != want {
		t.Fatalf("got %q, want %q", buf.String(), want)
	}
}

func assertArgs(t *testing.T, got [][]byte, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d args %q, want %d %q", len(got), got, len(want), want)
	}
	for i, w := range want {
		if string(got[i]) != w {
			t.Errorf("arg %d = %q, want %q", i, got[i], w)
		}
	}
}
