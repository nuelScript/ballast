package server

import (
	"errors"
	"io"
	"log"
	"net"

	"github.com/nuelScript/ballast/internal/resp"
)

// Server is a minimal RESP server backed by a storage engine.
type Server struct {
	addr  string
	store Store
}

// New returns a Server that will listen on addr (e.g. ":6379") and serve from
// store.
func New(addr string, store Store) *Server {
	return &Server{addr: addr, store: store}
}

// ListenAndServe binds the configured address and serves until the listener
// fails. It blocks.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve accepts connections on ln until it is closed. It blocks and takes
// ownership of ln, closing it on return.
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
	for {
		args, err := r.ReadCommand()
		if err != nil {
			// A protocol error desyncs the stream: report it, then drop the
			// client. A clean disconnect just ends the loop silently.
			if errors.Is(err, resp.ErrInvalidSyntax) {
				w.WriteError("ERR Protocol error")
				w.Flush()
			} else if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("read from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if err := handleCommand(w, s.store, args); err != nil {
			if !errors.Is(err, errQuit) {
				return // connection is broken; nothing more to do
			}
			w.Flush()
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}
