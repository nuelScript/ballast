package bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func mustGet(t *testing.T, db *DB, key string) string {
	t.Helper()
	v, ok, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !ok {
		t.Fatalf("Get(%q): missing", key)
	}
	return string(v)
}

func TestSetGetDelete(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, ok, _ := db.Get("nope"); ok {
		t.Fatal("expected miss")
	}
	if err := db.Set("foo", []byte("bar")); err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, db, "foo"); got != "bar" {
		t.Fatalf("foo = %q", got)
	}
	if err := db.Set("foo", []byte("baz")); err != nil { // overwrite
		t.Fatal(err)
	}
	if got := mustGet(t, db, "foo"); got != "baz" {
		t.Fatalf("foo after overwrite = %q", got)
	}
	if n, _ := db.Delete("foo", "nope"); n != 1 {
		t.Fatalf("Delete counted %d, want 1", n)
	}
	if _, ok, _ := db.Get("foo"); ok {
		t.Fatal("foo should be gone")
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.Set("keep", []byte("value"))
	db.Set("drop", []byte("temp"))
	db.Delete("drop")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := mustGet(t, db2, "keep"); got != "value" {
		t.Fatalf("keep = %q after reopen", got)
	}
	if _, ok, _ := db2.Get("drop"); ok {
		t.Fatal("drop should have stayed deleted (tombstone must persist)")
	}
}

func TestSegmentRolling(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.maxFileSize = 128 // force frequent rolls

	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("key-%02d", i)
		if err := db.Set(key, []byte(fmt.Sprintf("value-number-%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if segs := countSegments(t, dir); segs < 2 {
		t.Fatalf("expected multiple segments, got %d", segs)
	}
	// Every key must still be readable across the segment boundaries.
	for i := 0; i < 40; i++ {
		want := fmt.Sprintf("value-number-%02d", i)
		if got := mustGet(t, db, fmt.Sprintf("key-%02d", i)); got != want {
			t.Fatalf("key-%02d = %q, want %q", i, got, want)
		}
	}
	db.Close()
}

func TestMergeReclaimsSpaceAndKeepsData(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.maxFileSize = 128

	// Write, then overwrite every key twice: most records become dead.
	for round := 0; round < 3; round++ {
		for i := 0; i < 30; i++ {
			key := fmt.Sprintf("k%02d", i)
			if err := db.Set(key, []byte(fmt.Sprintf("round-%d-value-%02d", round, i))); err != nil {
				t.Fatal(err)
			}
		}
	}

	before := dirSize(t, dir)
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	after := dirSize(t, dir)
	if after >= before {
		t.Fatalf("merge did not reclaim space: before=%d after=%d", before, after)
	}

	// Latest values survive the merge...
	for i := 0; i < 30; i++ {
		want := fmt.Sprintf("round-2-value-%02d", i)
		if got := mustGet(t, db, fmt.Sprintf("k%02d", i)); got != want {
			t.Fatalf("k%02d = %q, want %q", i, got, want)
		}
	}
	db.Close()

	// ...and after a reopen, proving the merged segment is what's on disk.
	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < 30; i++ {
		want := fmt.Sprintf("round-2-value-%02d", i)
		if got := mustGet(t, db2, fmt.Sprintf("k%02d", i)); got != want {
			t.Fatalf("after reopen k%02d = %q, want %q", i, got, want)
		}
	}
}

func TestReplayToleratesTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.Set("a", []byte("1"))
	db.Set("b", []byte("2"))
	db.Close()

	// Append a header that promises a body which never arrives — a crash
	// mid-append. Reopen must keep the good records and ignore the partial one.
	seg := filepath.Join(dir, "000000001.data")
	f, err := os.OpenFile(seg, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(encodeRecord(kindPut, 123, []byte("c"), []byte("this body is cut off"))[:headerSize+1])
	f.Close()

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with truncated tail should succeed: %v", err)
	}
	defer db2.Close()
	if got := mustGet(t, db2, "a"); got != "1" {
		t.Fatalf("a = %q", got)
	}
	if got := mustGet(t, db2, "b"); got != "2" {
		t.Fatalf("b = %q", got)
	}
	if _, ok, _ := db2.Get("c"); ok {
		t.Fatal("partial record for c must not apply")
	}
	// The tail was truncated, so new writes land cleanly and reopen again.
	if err := db2.Set("d", []byte("4")); err != nil {
		t.Fatal(err)
	}
}

func TestCRCRejectsCorruption(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.Set("only", []byte("value"))
	db.Close()

	// Flip the last byte of the value; the record's CRC must no longer match.
	seg := filepath.Join(dir, "000000001.data")
	data, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(seg, data, 0o644); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if _, ok, _ := db2.Get("only"); ok {
		t.Fatal("corrupt record must be rejected on scan")
	}
}

// TestConcurrentAccess is meant to run under -race.
func TestConcurrentAccess(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i%8)
			db.Set(key, []byte(fmt.Sprintf("v%d", i)))
			db.Get(key)
			if i%16 == 0 {
				db.Delete(key)
			}
		}(i)
	}
	wg.Wait()
}

func countSegments(t *testing.T, dir string) int {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.data"))
	if err != nil {
		t.Fatal(err)
	}
	return len(matches)
}

func dirSize(t *testing.T, dir string) int64 {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.data"))
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, m := range matches {
		info, err := os.Stat(m)
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
	}
	return total
}
