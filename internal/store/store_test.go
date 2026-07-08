package store

import (
	"sync"
	"testing"
)

func TestSetGetDelete(t *testing.T) {
	s := New()

	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected miss on empty store")
	}

	s.Set("foo", []byte("bar"))
	v, ok := s.Get("foo")
	if !ok || string(v) != "bar" {
		t.Fatalf("Get(foo) = %q, %v", v, ok)
	}

	s.Set("foo", []byte("baz")) // overwrite
	if v, _ := s.Get("foo"); string(v) != "baz" {
		t.Fatalf("overwrite failed, got %q", v)
	}

	if n := s.Delete("foo", "missing"); n != 1 {
		t.Fatalf("Delete counted %d, want 1", n)
	}
	if _, ok := s.Get("foo"); ok {
		t.Fatal("foo should be gone after delete")
	}
}

// TestConcurrentAccess is meant to be run under -race.
func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "k"
			s.Set(key, []byte{byte(i)})
			s.Get(key)
			s.Delete(key)
		}(i)
	}
	wg.Wait()
}
