package lsm

import (
	"errors"
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

func TestBloomNoFalseNegatives(t *testing.T) {
	b := newBloom(1000, bloomBitsPerKey)
	for i := 0; i < 1000; i++ {
		b.add(fmt.Sprintf("key-%d", i))
	}
	for i := 0; i < 1000; i++ {
		if !b.mayContain(fmt.Sprintf("key-%d", i)) {
			t.Fatalf("false negative for key-%d", i)
		}
	}
}

func TestSSTableReadPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.sst")
	// More than indexInterval entries so the sparse index has multiple blocks.
	var entries []kvEntry
	for i := 0; i < 100; i++ {
		entries = append(entries, kvEntry{fmt.Sprintf("k%03d", i), 1, kindPut, []byte(fmt.Sprintf("v%03d", i))})
	}
	entries[50] = kvEntry{"k050", 1, kindTombstone, nil}
	if err := writeSSTable(path, entries, 1); err != nil {
		t.Fatal(err)
	}

	s, err := openSSTable(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.f.Close()

	for i := 0; i < 100; i++ {
		e, ok, err := s.get(fmt.Sprintf("k%03d", i), ^uint64(0))
		if err != nil || !ok {
			t.Fatalf("k%03d: ok=%v err=%v", i, ok, err)
		}
		if i == 50 {
			if e.kind != kindTombstone {
				t.Fatalf("k050 should be a tombstone")
			}
			continue
		}
		if string(e.value) != fmt.Sprintf("v%03d", i) {
			t.Fatalf("k%03d = %q", i, e.value)
		}
	}
	if _, ok, _ := s.get("absent", ^uint64(0)); ok {
		t.Fatal("absent key returned present")
	}
}

func TestTransactionSnapshotIsolation(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Set("k", []byte("v1"))
	txn := db.Begin() // snapshots the world at v1
	db.Set("k", []byte("v2"))

	if v, ok, _ := txn.Get("k"); !ok || string(v) != "v1" {
		t.Fatalf("txn saw %q (ok=%v), want v1", v, ok)
	}
	if v, _, _ := db.Get("k"); string(v) != "v2" {
		t.Fatalf("db saw %q, want v2", v)
	}
	txn.Rollback()
}

func TestTransactionOwnWritesAndCommit(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	txn := db.Begin()
	txn.Set("a", []byte("x"))
	if v, ok, _ := txn.Get("a"); !ok || string(v) != "x" {
		t.Fatalf("txn should see its own write, got %q ok=%v", v, ok)
	}
	if _, ok, _ := db.Get("a"); ok {
		t.Fatal("uncommitted write must not be visible outside the txn")
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := db.Get("a"); !ok || string(v) != "x" {
		t.Fatalf("after commit db = %q ok=%v, want x", v, ok)
	}
}

func TestTransactionConflict(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Set("k", []byte("v0"))
	t1 := db.Begin()
	t2 := db.Begin() // both snapshot v0

	t1.Set("k", []byte("t1"))
	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}
	t2.Set("k", []byte("t2"))
	if err := t2.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("t2 commit = %v, want ErrConflict", err)
	}
	if v, _, _ := db.Get("k"); string(v) != "t1" {
		t.Fatalf("k = %q, want t1 (the winner)", v)
	}
}

func TestMergePreservesHeldSnapshot(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Set("k", []byte("old"))
	db.flush() // "old" lands in an SSTable
	snap := db.Begin()
	db.Set("k", []byte("new"))
	db.flush() // "new" in a second SSTable

	// Merge while the snapshot is held: the old version must be retained.
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := snap.Get("k"); !ok || string(v) != "old" {
		t.Fatalf("snapshot lost its version after merge: %q ok=%v", v, ok)
	}
	snap.Rollback()

	// With no snapshot held, a merge collapses to the newest version.
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	if v, _, _ := db.Get("k"); string(v) != "new" {
		t.Fatalf("after final merge = %q, want new", v)
	}
}

func TestSetGetDelete(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Set("foo", []byte("bar"))
	if got := mustGet(t, db, "foo"); got != "bar" {
		t.Fatalf("foo = %q", got)
	}
	db.Set("foo", []byte("baz"))
	if got := mustGet(t, db, "foo"); got != "baz" {
		t.Fatalf("overwrite: foo = %q", got)
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
	db.memLimit = 256 // force flushes so data lands in SSTables and the WAL
	for i := 0; i < 100; i++ {
		db.Set(fmt.Sprintf("k%03d", i), []byte(fmt.Sprintf("v%03d", i)))
	}
	db.Delete("k000")
	db.Close()

	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if _, ok, _ := db2.Get("k000"); ok {
		t.Fatal("k000 should stay deleted after reopen")
	}
	for i := 1; i < 100; i++ {
		want := fmt.Sprintf("v%03d", i)
		if got := mustGet(t, db2, fmt.Sprintf("k%03d", i)); got != want {
			t.Fatalf("k%03d = %q, want %q", i, got, want)
		}
	}
}

func TestFlushProducesSSTables(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.memLimit = 256
	for i := 0; i < 200; i++ {
		db.Set(fmt.Sprintf("k%03d", i), []byte("some-value-here"))
	}
	if got := countFiles(t, dir, "*.sst"); got < 2 {
		t.Fatalf("expected multiple SSTables, got %d", got)
	}
	db.Close()
}

func TestUnflushedWritesRecoverFromWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Small enough to stay in the memtable — only the WAL holds it on disk.
	db.Set("survive", []byte("me"))
	if got := countFiles(t, dir, "*.sst"); got != 0 {
		t.Fatalf("expected no SSTables yet, got %d", got)
	}
	db.Close()

	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := mustGet(t, db2, "survive"); got != "me" {
		t.Fatalf("survive = %q", got)
	}
}

func TestMergeReclaimsSpaceAndKeepsData(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.memLimit = 256
	for round := 0; round < 4; round++ {
		for i := 0; i < 40; i++ {
			db.Set(fmt.Sprintf("k%03d", i), []byte(fmt.Sprintf("round-%d-%03d", round, i)))
		}
	}
	db.Delete("k000")

	before := dirSize(t, dir)
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	after := dirSize(t, dir)
	if after >= before {
		t.Fatalf("merge did not reclaim space: before=%d after=%d", before, after)
	}
	if got := countFiles(t, dir, "*.sst"); got != 1 {
		t.Fatalf("expected 1 SSTable after merge, got %d", got)
	}

	if _, ok, _ := db.Get("k000"); ok {
		t.Fatal("k000 must stay deleted through a merge")
	}
	for i := 1; i < 40; i++ {
		want := fmt.Sprintf("round-3-%03d", i)
		if got := mustGet(t, db, fmt.Sprintf("k%03d", i)); got != want {
			t.Fatalf("k%03d = %q, want %q", i, got, want)
		}
	}
	db.Close()
}

func TestRange(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.memLimit = 256 // spread data across SSTables + memtable
	for i := 0; i < 60; i++ {
		db.Set(fmt.Sprintf("k%02d", i), []byte(fmt.Sprintf("v%02d", i)))
	}
	db.Set("k10", []byte("newer")) // overwrite: newest (memtable) must win
	db.Delete("k20")               // tombstone must be skipped in results

	got, err := db.Range("k08", "k12", 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []KV{
		{"k08", []byte("v08")},
		{"k09", []byte("v09")},
		{"k10", []byte("newer")},
		{"k11", []byte("v11")},
		{"k12", []byte("v12")},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d pairs, want %d: %v", len(got), len(want), got)
	}
	for i, kv := range got {
		if kv.Key != want[i].Key || string(kv.Value) != string(want[i].Value) {
			t.Fatalf("pair %d = {%s,%s}, want {%s,%s}", i, kv.Key, kv.Value, want[i].Key, want[i].Value)
		}
	}

	deleted, err := db.Range("k20", "k20", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 0 {
		t.Fatalf("deleted key k20 should not appear: %v", deleted)
	}

	limited, err := db.Range("k00", "k59", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 3 || limited[0].Key != "k00" || limited[2].Key != "k02" {
		t.Fatalf("limit not honored in order: %v", limited)
	}
	db.Close()
}

func TestConcurrentAccess(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	db.memLimit = 512
	defer db.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i%16)
			db.Set(key, []byte(fmt.Sprintf("v%d", i)))
			db.Get(key)
			if i%20 == 0 {
				db.Merge()
			}
		}(i)
	}
	wg.Wait()
}

func countFiles(t *testing.T, dir, pattern string) int {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		t.Fatal(err)
	}
	return len(m)
}

func dirSize(t *testing.T, dir string) int64 {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, p := range m {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
	}
	return total
}
