package server

import (
	"bytes"
	"errors"
	"strings"
	"time"

	"github.com/nuelScript/ballast/internal/lsm"
	"github.com/nuelScript/ballast/internal/raft"
	"github.com/nuelScript/ballast/internal/resp"
)

var errNotLeader = errors.New("not leader")

// redirectErr carries the leader's client address so a follower can point the
// client at it.
type redirectErr struct{ addr string }

func (e redirectErr) Error() string { return "not leader; leader at " + e.addr }

// write performs a mutation. Standalone, it applies directly; in a cluster it is
// proposed to Raft and applied only once committed.
func (s *Server) write(args [][]byte) (any, error) {
	if s.raft == nil {
		return applyWrite(s.db, args), nil
	}
	if !s.raft.IsLeader() {
		return nil, s.redirectToLeader()
	}
	ch, ok := s.raft.Propose(encodeCmd(args))
	if !ok {
		return nil, s.redirectToLeader()
	}
	select {
	case res, ok := <-ch:
		if !ok {
			return nil, errNotLeader // entry lost to a leadership change
		}
		return res, nil
	case <-time.After(2 * time.Second):
		return nil, errNotLeader
	}
}

func (s *Server) redirectToLeader() error {
	if addr, ok := s.redirect[s.raft.Leader()]; ok {
		return redirectErr{addr}
	}
	return errNotLeader
}

// applyWrite is the state machine: the one place a mutation touches the store,
// whether applied directly (standalone) or from a committed Raft entry.
func applyWrite(db *lsm.DB, args [][]byte) any {
	if len(args) == 0 {
		return nil
	}
	switch strings.ToUpper(string(args[0])) {
	case "SET":
		if len(args) == 3 {
			db.Set(string(args[1]), args[2])
		}
	case "DEL":
		keys := make([]string, 0, len(args)-1)
		for _, k := range args[1:] {
			keys = append(keys, string(k))
		}
		n, _ := db.Delete(keys...)
		return n
	}
	return nil
}

func encodeCmd(args [][]byte) []byte {
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	w.WriteArray(len(args))
	for _, a := range args {
		w.WriteBulk(a)
	}
	w.Flush()
	return buf.Bytes()
}

func decodeCmd(b []byte) [][]byte {
	args, _ := resp.NewReader(bytes.NewReader(b)).ReadCommand()
	return args
}

// NewCluster builds a Raft-backed server. redirect maps each node's Raft id to
// its client address. The returned Node's Handler must be served for peers.
func NewCluster(clientAddr string, db *lsm.DB, cfg raft.Config, redirect map[string]string) (*Server, *raft.Node) {
	cfg.Apply = func(cmd []byte) any { return applyWrite(db, decodeCmd(cmd)) }
	node := raft.NewNode(cfg)
	return &Server{addr: clientAddr, db: db, raft: node, redirect: redirect}, node
}
