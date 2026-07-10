package server

import (
	"errors"
	"strconv"
	"strings"

	"github.com/nuelScript/ballast/internal/lsm"
	"github.com/nuelScript/ballast/internal/resp"
)

var errQuit = errors.New("client quit")

// session is the per-connection state: the transaction in progress, if any.
type session struct {
	txn *lsm.Txn
}

func handleCommand(w *resp.Writer, s *Server, sess *session, args [][]byte) error {
	if len(args) == 0 {
		return nil
	}
	name := strings.ToUpper(string(args[0]))

	switch name {
	case "BEGIN":
		return cmdBegin(w, s, sess)
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
		return cmdSet(w, s, args)
	case "GET":
		return cmdGet(w, s, args)
	case "DEL":
		return cmdDel(w, s, args)
	case "RANGE":
		return cmdRange(w, s, args)
	case "COMPACT":
		if err := s.db.Merge(); err != nil {
			return w.WriteError("ERR " + err.Error())
		}
		return w.WriteSimpleString("OK")
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

// writeErr turns a leadership redirect into a client-visible pointer to the leader.
func writeErr(w *resp.Writer, err error) error {
	var re redirectErr
	if errors.As(err, &re) {
		return w.WriteError("ERR not leader; leader at " + re.addr)
	}
	return w.WriteError("ERR " + err.Error())
}

func cmdBegin(w *resp.Writer, s *Server, sess *session) error {
	if s.raft != nil {
		return w.WriteError("ERR transactions are not available in cluster mode")
	}
	if sess.txn != nil {
		return w.WriteError("ERR already in a transaction")
	}
	sess.txn = s.db.Begin()
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

func cmdSet(w *resp.Writer, s *Server, args [][]byte) error {
	if len(args) != 3 {
		return w.WriteError("ERR wrong number of arguments for 'set' command")
	}
	if _, err := s.write([][]byte{[]byte("SET"), args[1], args[2]}); err != nil {
		return writeErr(w, err)
	}
	return w.WriteSimpleString("OK")
}

func cmdGet(w *resp.Writer, s *Server, args [][]byte) error {
	if len(args) != 2 {
		return w.WriteError("ERR wrong number of arguments for 'get' command")
	}
	v, ok, err := s.db.Get(string(args[1]))
	if err != nil {
		return w.WriteError("ERR " + err.Error())
	}
	if !ok {
		return w.WriteNull()
	}
	return w.WriteBulk(v)
}

func cmdDel(w *resp.Writer, s *Server, args [][]byte) error {
	if len(args) < 2 {
		return w.WriteError("ERR wrong number of arguments for 'del' command")
	}
	res, err := s.write(append([][]byte{[]byte("DEL")}, args[1:]...))
	if err != nil {
		return writeErr(w, err)
	}
	n, _ := res.(int)
	return w.WriteInteger(int64(n))
}

// RANGE start end [LIMIT n] returns key/value pairs with start <= key <= end.
func cmdRange(w *resp.Writer, s *Server, args [][]byte) error {
	if len(args) != 3 && len(args) != 5 {
		return w.WriteError("ERR wrong number of arguments for 'range' command")
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
	pairs, err := s.db.Range(string(args[1]), string(args[2]), limit)
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
