// Package aof implements an append-only log of write commands. The log is a
// sequence of RESP-encoded command records; replaying it from the start
// reconstructs the dataset — the same scheme Redis uses for its AOF.
package aof

import (
	"errors"
	"io"
	"os"

	"github.com/nuelScript/ballast/internal/resp"
)

// Log is an append-only file opened for writing. It is not safe for concurrent
// use; the caller (the engine) serializes appends.
type Log struct {
	f *os.File
	w *resp.Writer
}

// Open opens the log at path for appending, creating it if necessary.
func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &Log{f: f, w: resp.NewWriter(f)}, nil
}

// Append writes one command record and flushes it to the OS. A flushed record
// survives a process crash (kill -9); it is not fsync'd, so a power loss can
// still lose the most recent records. Sync provides that stronger guarantee.
func (l *Log) Append(args [][]byte) error {
	if err := l.w.WriteArray(len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if err := l.w.WriteBulk(a); err != nil {
			return err
		}
	}
	return l.w.Flush()
}

// Sync flushes buffered bytes and fsyncs the file to disk.
func (l *Log) Sync() error {
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// Close flushes and closes the underlying file.
func (l *Log) Close() error {
	flushErr := l.w.Flush()
	closeErr := l.f.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// Replay streams every command record in the log at path to apply, in order. A
// missing file is treated as an empty log. A truncated record at the end of the
// file — the signature of a crash mid-append — stops replay cleanly, preserving
// every fully-written record before it.
func Replay(path string, apply func(args [][]byte) error) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	r := resp.NewReader(f)
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // clean end, or a partial tail from a crash mid-write
			}
			return err
		}
		if len(args) == 0 {
			continue
		}
		if err := apply(args); err != nil {
			return err
		}
	}
}
