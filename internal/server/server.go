package server

import (
	"errors"
	"io"
	"log"
	"net"

	"github.com/nuelScript/ballast/internal/lsm"
	"github.com/nuelScript/ballast/internal/raft"
	"github.com/nuelScript/ballast/internal/resp"
)

type Server struct {
	addr string
	db   *lsm.DB

	// Set only in cluster mode: writes go through Raft, and redirect maps a Raft
	// node id to the client address to point followers' clients at.
	raft     *raft.Node
	redirect map[string]string
}

func New(addr string, db *lsm.DB) *Server {
	return &Server{addr: addr, db: db}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve takes ownership of ln and closes it on return.
func (s *Server) Serve(ln net.Listener) error {
	defer ln.Close()
	log.Printf("ballast listening on %s", ln.Addr())
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	sess := &session{}
	defer func() {
		if sess.txn != nil {
			sess.txn.Rollback() // release the snapshot if the client vanished mid-txn
		}
	}()
	for {
		args, err := r.ReadCommand()
		if err != nil {
			// A protocol error desyncs the stream, so report it and drop the client.
			if errors.Is(err, resp.ErrInvalidSyntax) {
				w.WriteError("ERR Protocol error")
				w.Flush()
			} else if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("read from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if err := handleCommand(w, s, sess, args); err != nil {
			if !errors.Is(err, errQuit) {
				return
			}
			w.Flush()
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}
