package raft

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"
)

// HTTPTransport carries Raft RPCs as JSON over HTTP; a peer is a host:port.
type HTTPTransport struct {
	client *http.Client
}

func NewHTTPTransport() *HTTPTransport {
	return &HTTPTransport{client: &http.Client{Timeout: 100 * time.Millisecond}}
}

func (t *HTTPTransport) rpc(peer, path string, args, reply any) error {
	body, err := json.Marshal(args)
	if err != nil {
		return err
	}
	resp, err := t.client.Post("http://"+peer+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(reply)
}

func (t *HTTPTransport) RequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	err := t.rpc(peer, "/raft/requestvote", args, &reply)
	return reply, err
}

func (t *HTTPTransport) AppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	err := t.rpc(peer, "/raft/appendentries", args, &reply)
	return reply, err
}

// Handler serves this node's Raft RPC endpoints for peers to call.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/requestvote", func(w http.ResponseWriter, r *http.Request) {
		var args RequestVoteArgs
		if json.NewDecoder(r.Body).Decode(&args) != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(n.RequestVote(args))
	})
	mux.HandleFunc("/raft/appendentries", func(w http.ResponseWriter, r *http.Request) {
		var args AppendEntriesArgs
		if json.NewDecoder(r.Body).Decode(&args) != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(n.AppendEntries(args))
	})
	return mux
}
