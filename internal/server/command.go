package server

import (
	"errors"
	"strconv"
	"strings"

	"github.com/nuelScript/ballast/internal/lsm"
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

type scanner interface {
	Range(start, end string, limit int) ([]lsm.KV, error)
}

type transactor interface {
	Begin() *lsm.Txn
}

// session is the per-connection state: the transaction in progress, if any.
type session struct {
	txn *lsm.Txn
}

func handleCommand(w *resp.Writer, store Store, sess *session, args [][]byte) error {
	if len(args) == 0 {
		return nil
	}
	name := strings.ToUpper(string(args[0]))

	switch name {
	case "BEGIN":
		return cmdBegin(w, store, sess)
	case "COMMIT":
		return cmdCommit(w, sess)
	case "ROLLBACK":
		return cmdRollback(w, sess)
	}

	// Inside a transaction, data commands operate on it; the rest fall through.
	if sess.txn != nil {
		switch name {
		case "GET":
			return txnGet(w, sess.txn, args)
		case "SET":
			return txnSet(w, sess.txn, args)
		case "DEL":
			return txnDel(w, sess.txn, args)
		case "RANGE", "COMPACT":
			return w.WriteError("ERR " + name + " is not allowed in a transaction")
		}
	}

	switch name {
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
	case "RANGE":
		return cmdRange(w, store, args)
	case "COMPACT":
		return cmdCompact(w, store)
	case "COMMAND":
		// redis-cli probes COMMAND DOCS on connect; an empty array keeps it happy.
		return w.WriteArray(0)
	case "QUIT":
		if sess.txn != nil {
			sess.txn.Rollback()
			sess.txn = nil
		}
		if err := w.WriteSimpleString("OK"); err != nil {
			return err
		}
		return errQuit
	default:
		return w.WriteError("ERR unknown command '" + string(args[0]) + "'")
	}
}

func cmdBegin(w *resp.Writer, store Store, sess *session) error {
	if sess.txn != nil {
		return w.WriteError("ERR already in a transaction")
	}
	tr, ok := store.(transactor)
	if !ok {
		return w.WriteError("ERR transactions not supported")
	}
	sess.txn = tr.Begin()
	return w.WriteSimpleString("OK")
}

func cmdCommit(w *resp.Writer, sess *session) error {
	if sess.txn == nil {
		return w.WriteError("ERR no transaction in progress")
	}
	err := sess.txn.Commit()
	sess.txn = nil
	if errors.Is(err, lsm.ErrConflict) {
		return w.WriteError("ERR transaction conflict")
	}
	if err != nil {
		return w.WriteError("ERR " + err.Error())
	}
	return w.WriteSimpleString("OK")
}

func cmdRollback(w *resp.Writer, sess *session) error {
	if sess.txn == nil {
		return w.WriteError("ERR no transaction in progress")
	}
	sess.txn.Rollback()
	sess.txn = nil
	return w.WriteSimpleString("OK")
}

func txnGet(w *resp.Writer, txn *lsm.Txn, args [][]byte) error {
	if len(args) != 2 {
		return w.WriteError("ERR wrong number of arguments for 'get' command")
	}
	v, ok, err := txn.Get(string(args[1]))
	if err != nil {
		return w.WriteError("ERR " + err.Error())
	}
	if !ok {
		return w.WriteNull()
	}
	return w.WriteBulk(v)
}

func txnSet(w *resp.Writer, txn *lsm.Txn, args [][]byte) error {
	if len(args) != 3 {
		return w.WriteError("ERR wrong number of arguments for 'set' command")
	}
	txn.Set(string(args[1]), args[2])
	return w.WriteSimpleString("OK")
}

func txnDel(w *resp.Writer, txn *lsm.Txn, args [][]byte) error {
	if len(args) < 2 {
		return w.WriteError("ERR wrong number of arguments for 'del' command")
	}
	n := 0
	for _, k := range args[1:] {
		key := string(k)
		_, ok, err := txn.Get(key)
		if err != nil {
			return w.WriteError("ERR " + err.Error())
		}
		if ok {
			n++
		}
		txn.Delete(key)
	}
	return w.WriteInteger(int64(n))
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

// RANGE start end [LIMIT n] returns key/value pairs with start <= key <= end.
func cmdRange(w *resp.Writer, store Store, args [][]byte) error {
	if len(args) != 3 && len(args) != 5 {
		return w.WriteError("ERR wrong number of arguments for 'range' command")
	}
	sc, ok := store.(scanner)
	if !ok {
		return w.WriteError("ERR range not supported")
	}
	limit := 0
	if len(args) == 5 {
		if !strings.EqualFold(string(args[3]), "LIMIT") {
			return w.WriteError("ERR syntax error")
		}
		n, err := strconv.Atoi(string(args[4]))
		if err != nil || n < 0 {
			return w.WriteError("ERR value is not an integer or out of range")
		}
		limit = n
	}
	pairs, err := sc.Range(string(args[1]), string(args[2]), limit)
	if err != nil {
		return w.WriteError("ERR " + err.Error())
	}
	if err := w.WriteArray(len(pairs) * 2); err != nil {
		return err
	}
	for _, kv := range pairs {
		if err := w.WriteBulk([]byte(kv.Key)); err != nil {
			return err
		}
		if err := w.WriteBulk(kv.Value); err != nil {
			return err
		}
	}
	return nil
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
