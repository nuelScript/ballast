package server

import (
	"errors"
	"strings"

	"github.com/nuelScript/ballast/internal/engine"
	"github.com/nuelScript/ballast/internal/resp"
)

// errQuit is returned by a handler to signal the connection should close.
var errQuit = errors.New("client quit")

// handleCommand executes one parsed command against eng and writes its reply to
// w. A returned error other than errQuit means the reply could not be written.
func handleCommand(w *resp.Writer, eng *engine.Engine, args [][]byte) error {
	if len(args) == 0 {
		return nil
	}
	switch strings.ToUpper(string(args[0])) {
	case "PING":
		return cmdPing(w, args)
	case "ECHO":
		return cmdEcho(w, args)
	case "SET":
		return cmdSet(w, eng, args)
	case "GET":
		return cmdGet(w, eng, args)
	case "DEL":
		return cmdDel(w, eng, args)
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

func cmdSet(w *resp.Writer, eng *engine.Engine, args [][]byte) error {
	if len(args) != 3 {
		return w.WriteError("ERR wrong number of arguments for 'set' command")
	}
	if err := eng.Set(string(args[1]), args[2]); err != nil {
		return w.WriteError("ERR " + err.Error())
	}
	return w.WriteSimpleString("OK")
}

func cmdGet(w *resp.Writer, eng *engine.Engine, args [][]byte) error {
	if len(args) != 2 {
		return w.WriteError("ERR wrong number of arguments for 'get' command")
	}
	v, ok := eng.Get(string(args[1]))
	if !ok {
		return w.WriteNull()
	}
	return w.WriteBulk(v)
}

func cmdDel(w *resp.Writer, eng *engine.Engine, args [][]byte) error {
	if len(args) < 2 {
		return w.WriteError("ERR wrong number of arguments for 'del' command")
	}
	keys := make([]string, 0, len(args)-1)
	for _, k := range args[1:] {
		keys = append(keys, string(k))
	}
	n, err := eng.Delete(keys...)
	if err != nil {
		return w.WriteError("ERR " + err.Error())
	}
	return w.WriteInteger(int64(n))
}
