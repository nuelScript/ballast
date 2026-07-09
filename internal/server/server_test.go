package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nuelScript/ballast/internal/lsm"
)

// dialTestServer starts a Server on a random port and returns a connected
// client with a buffered reader. Everything is torn down via t.Cleanup.
func dialTestServer(t *testing.T) (net.Conn, *bufio.Reader) {
	t.Helper()
	db, err := lsm.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go New("", db).Serve(ln)
	t.Cleanup(func() { ln.Close() })

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	return conn, bufio.NewReader(conn)
}

func TestServerCommands(t *testing.T) {
	conn, r := dialTestServer(t)

	send := func(parts ...string) {
		var b strings.Builder
		fmt.Fprintf(&b, "*%d\r\n", len(parts))
		for _, p := range parts {
			fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(p), p)
		}
		if _, err := conn.Write([]byte(b.String())); err != nil {
			t.Fatal(err)
		}
	}
	line := func() string {
		s, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimRight(s, "\r\n")
	}

	cases := []struct {
		name  string
		parts []string
		want  []string // expected reply lines
	}{
		{"ping", []string{"PING"}, []string{"+PONG"}},
		{"set", []string{"SET", "foo", "bar"}, []string{"+OK"}},
		{"get", []string{"GET", "foo"}, []string{"$3", "bar"}},
		{"get-missing", []string{"GET", "nope"}, []string{"$-1"}},
		{"del", []string{"DEL", "foo", "nope"}, []string{":1"}},
		{"get-after-del", []string{"GET", "foo"}, []string{"$-1"}},
		{"echo", []string{"ECHO", "hey"}, []string{"$3", "hey"}},
		{"unknown", []string{"BOGUS"}, []string{"-ERR unknown command 'BOGUS'"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			send(tc.parts...)
			for _, want := range tc.want {
				if got := line(); got != want {
					t.Fatalf("reply = %q, want %q", got, want)
				}
			}
		})
	}
}

// TestBinaryValueRoundTrip stores a value with embedded CRLF/NUL bytes and
// reads it back to prove the pipeline stays binary-safe end to end.
func TestBinaryValueRoundTrip(t *testing.T) {
	conn, r := dialTestServer(t)
	val := "x\r\n\x00y"

	req := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$%d\r\n%s\r\n", len(val), val)
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	if s, _ := r.ReadString('\n'); s != "+OK\r\n" {
		t.Fatalf("SET reply = %q", s)
	}

	if _, err := conn.Write([]byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")); err != nil {
		t.Fatal(err)
	}
	// Expect: $<len>\r\n<val>\r\n
	header, _ := r.ReadString('\n')
	if header != fmt.Sprintf("$%d\r\n", len(val)) {
		t.Fatalf("GET header = %q", header)
	}
	body := make([]byte, len(val)+2)
	if _, err := readFull(r, body); err != nil {
		t.Fatal(err)
	}
	if string(body[:len(val)]) != val {
		t.Fatalf("GET body = %q, want %q", body[:len(val)], val)
	}
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// startServer runs one Server backed by a fresh engine and returns its address.
func startServer(t *testing.T) string {
	t.Helper()
	db, err := lsm.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go New("", db).Serve(ln)
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

// tclient is a tiny RESP client that sends a command and returns its reply as a
// string: "+OK", "-ERR ...", ":1", the bulk body, or "(nil)".
type tclient struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func dialClient(t *testing.T, addr string) *tclient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	return &tclient{t: t, conn: conn, r: bufio.NewReader(conn)}
}

func (c *tclient) cmd(parts ...string) string {
	c.t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(parts))
	for _, p := range parts {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(p), p)
	}
	if _, err := c.conn.Write([]byte(b.String())); err != nil {
		c.t.Fatal(err)
	}
	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatal(err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line[0] == '$' {
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "(nil)"
		}
		body := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, body); err != nil {
			c.t.Fatal(err)
		}
		return string(body[:n])
	}
	return line
}

func TestTransactionsOverRESP(t *testing.T) {
	addr := startServer(t)
	c1 := dialClient(t, addr)
	c2 := dialClient(t, addr)

	// Snapshot isolation: c1's transaction does not see c2's later write.
	c1.cmd("SET", "k", "v0")
	if r := c1.cmd("BEGIN"); r != "+OK" {
		t.Fatalf("BEGIN = %s", r)
	}
	c2.cmd("SET", "k", "v2")
	if r := c1.cmd("GET", "k"); r != "v0" {
		t.Fatalf("in-txn GET = %s, want v0", r)
	}
	if r := c2.cmd("GET", "k"); r != "v2" {
		t.Fatalf("outside GET = %s, want v2", r)
	}
	c1.cmd("COMMIT")

	// Write-write conflict: the second committer loses.
	c1.cmd("SET", "x", "0")
	c1.cmd("BEGIN")
	c2.cmd("BEGIN")
	c1.cmd("SET", "x", "a")
	if r := c1.cmd("COMMIT"); r != "+OK" {
		t.Fatalf("c1 COMMIT = %s", r)
	}
	c2.cmd("SET", "x", "b")
	if r := c2.cmd("COMMIT"); r != "-ERR transaction conflict" {
		t.Fatalf("c2 COMMIT = %s, want conflict", r)
	}
	if r := c1.cmd("GET", "x"); r != "a" {
		t.Fatalf("x = %s, want a", r)
	}
}
