package server

import (
	"errors"
	"strings"

	"github.com/nuelScript/ballast/internal/resp"
)

var errQuit = errors.New("client quit")

type Store interface {
	Get(key string) ([]byte, bool, error)
	Set(key string, value []byte) error
	Delete(keys ...string) (int, error)
}

type merger interface {
	Merge() error
}

func handleCommand(w *resp.Writer, store Store, args [][]byte) error {
	if len(args) == 0 {
		return nil
	}
	switch strings.ToUpper(string(args[0])) {
	case "PING":
		return cmdPing(w, args)
	case "ECHO":
		return cmdEcho(w, args)
	case "SET":
		return cmdSet(w, store, args)
	case "GET":
		return cmdGet(w, store, args)
	case "DEL":
		return cmdDel(w, store, args)
	case "COMPACT":
		return cmdCompact(w, store)
	case "COMMAND":
		// redis-cli probes COMMAND DOCS on connect; an empty array keeps it happy.
		return w.WriteArray(0)
	case "QUIT":
		if err := w.WriteSimpleString("OK"); err != nil {
			return err
		}
		return errQuit
	default:
		return w.WriteError("ERR unknown command '" + string(args[0]) + "'")
	}
}

func cmdPing(w *resp.Writer, args [][]byte) error {
	if len(args) > 1 {
		return w.WriteBulk(args[1])
	}
	return w.WriteSimpleString("PONG")
}

func cmdEcho(w *resp.Writer, args [][]byte) error {
	if len(args) != 2 {
		return w.WriteError("ERR wrong number of arguments for 'echo' command")
	}
	return w.WriteBulk(args[1])
}

func cmdSet(w *resp.Writer, store Store, args [][]byte) error {
	if len(args) != 3 {
		return w.WriteError("ERR wrong number of arguments for 'set' command")
	}
	if err := store.Set(string(args[1]), args[2]); err != nil {
		return w.WriteError("ERR " + err.Error())
	}
	return w.WriteSimpleString("OK")
}

func cmdGet(w *resp.Writer, store Store, args [][]byte) error {
	if len(args) != 2 {
		return w.WriteError("ERR wrong number of arguments for 'get' command")
	}
	v, ok, err := store.Get(string(args[1]))
	if err != nil {
		return w.WriteError("ERR " + err.Error())
	}
	if !ok {
		return w.WriteNull()
	}
	return w.WriteBulk(v)
}

func cmdDel(w *resp.Writer, store Store, args [][]byte) error {
	if len(args) < 2 {
		return w.WriteError("ERR wrong number of arguments for 'del' command")
	}
	keys := make([]string, 0, len(args)-1)
	for _, k := range args[1:] {
		keys = append(keys, string(k))
	}
	n, err := store.Delete(keys...)
	if err != nil {
		return w.WriteError("ERR " + err.Error())
	}
	return w.WriteInteger(int64(n))
}

func cmdCompact(w *resp.Writer, store Store) error {
	m, ok := store.(merger)
	if !ok {
		return w.WriteError("ERR compaction not supported")
	}
	if err := m.Merge(); err != nil {
		return w.WriteError("ERR " + err.Error())
	}
	return w.WriteSimpleString("OK")
}
