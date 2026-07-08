package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")

	e, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustSet(t, e, "foo", "bar")
	mustSet(t, e, "baz", "qux")
	if _, err := e.Delete("baz"); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: state must be reconstructed entirely from the log.
	e2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	if v, ok := e2.Get("foo"); !ok || string(v) != "bar" {
		t.Fatalf("foo = %q, %v after reopen", v, ok)
	}
	if _, ok := e2.Get("baz"); ok {
		t.Fatal("baz should have stayed deleted after reopen")
	}
}

func TestInMemoryModeCreatesNoLog(t *testing.T) {
	e, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustSet(t, e, "k", "v")
	if v, ok := e.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("in-memory get = %q, %v", v, ok)
	}
}

// TestReplayToleratesTruncatedTail simulates a crash mid-append by writing a
// partial command frame, then checks that reopening keeps every complete record
// before it and ignores the partial one.
func TestReplayToleratesTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trunc.aof")

	e, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustSet(t, e, "a", "1")
	mustSet(t, e, "b", "2")
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// A SET frame cut off partway through its value.
	if _, err := f.WriteString("*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$5\r\nhel"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	e2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen with truncated tail should succeed, got %v", err)
	}
	defer e2.Close()
	if v, _ := e2.Get("a"); string(v) != "1" {
		t.Fatalf("lost a: %q", v)
	}
	if v, _ := e2.Get("b"); string(v) != "2" {
		t.Fatalf("lost b: %q", v)
	}
	if _, ok := e2.Get("c"); ok {
		t.Fatal("partial record for c must not apply")
	}
}

func mustSet(t *testing.T, e *Engine, k, v string) {
	t.Helper()
	if err := e.Set(k, []byte(v)); err != nil {
		t.Fatal(err)
	}
}
