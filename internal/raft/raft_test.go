package raft

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

var errDown = errors.New("unreachable")

// memNet wires nodes together in memory and can drop a node to simulate a crash
// or partition.
type memNet struct {
	mu    sync.Mutex
	nodes map[string]*Node
	down  map[string]bool
}

func newMemNet() *memNet {
	return &memNet{nodes: make(map[string]*Node), down: make(map[string]bool)}
}

func (nw *memNet) register(id string, n *Node) {
	nw.mu.Lock()
	nw.nodes[id] = n
	nw.mu.Unlock()
}

func (nw *memNet) setDown(id string, d bool) {
	nw.mu.Lock()
	nw.down[id] = d
	nw.mu.Unlock()
}

type memTransport struct {
	nw   *memNet
	self string
}

func (t *memTransport) reach(peer string) (*Node, bool) {
	t.nw.mu.Lock()
	defer t.nw.mu.Unlock()
	if t.nw.down[t.self] || t.nw.down[peer] {
		return nil, false
	}
	n, ok := t.nw.nodes[peer]
	return n, ok
}

func (t *memTransport) RequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	n, ok := t.reach(peer)
	if !ok {
		return RequestVoteReply{}, errDown
	}
	return n.RequestVote(args), nil
}

func (t *memTransport) AppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	n, ok := t.reach(peer)
	if !ok {
		return AppendEntriesReply{}, errDown
	}
	return n.AppendEntries(args), nil
}

type applied struct {
	mu   sync.Mutex
	cmds [][]byte
}

func (a *applied) add(cmd []byte) { a.mu.Lock(); a.cmds = append(a.cmds, cmd); a.mu.Unlock() }
func (a *applied) count() int     { a.mu.Lock(); defer a.mu.Unlock(); return len(a.cmds) }

func makeCluster(t *testing.T, size int) (*memNet, []*Node, []*applied) {
	t.Helper()
	nw := newMemNet()
	var ids []string
	for i := 0; i < size; i++ {
		ids = append(ids, fmt.Sprintf("n%d", i))
	}
	var nodes []*Node
	var applies []*applied
	for _, id := range ids {
		var peers []string
		for _, o := range ids {
			if o != id {
				peers = append(peers, o)
			}
		}
		app := &applied{}
		n := NewNode(Config{
			ID:        id,
			Peers:     peers,
			Transport: &memTransport{nw: nw, self: id},
			Storage:   &MemStorage{},
			Apply:     func(cmd []byte) any { app.add(cmd); return nil },
		})
		nw.register(id, n)
		nodes = append(nodes, n)
		applies = append(applies, app)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})
	return nw, nodes, applies
}

func waitLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, n := range nodes {
			if !n.stoppedNow() && n.IsLeader() {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no unique leader elected")
	return nil
}

func (n *Node) stoppedNow() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stopped
}

func TestLeaderElection(t *testing.T) {
	_, nodes, _ := makeCluster(t, 3)
	if leader := waitLeader(t, nodes); leader == nil {
		t.Fatal("expected a leader")
	}
}

func TestLogReplication(t *testing.T) {
	_, nodes, applies := makeCluster(t, 3)
	leader := waitLeader(t, nodes)

	ch, ok := leader.Propose([]byte("cmd-1"))
	if !ok {
		t.Fatal("leader rejected proposal")
	}
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("proposal never applied on leader")
	}

	// Every node must apply the committed entry.
	deadline := time.Now().Add(2 * time.Second)
	for {
		all := true
		for _, a := range applies {
			if a.count() < 1 {
				all = false
			}
		}
		if all {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not all nodes applied the entry: %d/%d", countApplied(applies), len(applies))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReElectionAfterLeaderFailure(t *testing.T) {
	nw, nodes, _ := makeCluster(t, 3)
	leader := waitLeader(t, nodes)

	// Isolate and stop the leader; the remaining two must elect a new one.
	nw.setDown(leader.ID(), true)
	leader.Stop()

	var survivors []*Node
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			survivors = append(survivors, n)
		}
	}
	newLeader := waitLeader(t, survivors)
	if newLeader.ID() == leader.ID() {
		t.Fatal("stopped leader came back")
	}

	ch, ok := newLeader.Propose([]byte("after-failover"))
	if !ok {
		t.Fatal("new leader rejected proposal")
	}
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("new leader could not commit after failover")
	}
}

func TestPersistenceReload(t *testing.T) {
	store := &MemStorage{}
	n1 := NewNode(Config{ID: "solo", Storage: store, Apply: func([]byte) any { return nil }})
	waitLeader(t, []*Node{n1}) // single node elects itself
	ch, ok := n1.Propose([]byte("durable"))
	if !ok {
		t.Fatal("solo node should be leader")
	}
	<-ch
	n1.mu.Lock()
	wantTerm, wantLen := n1.currentTerm, len(n1.log)
	n1.mu.Unlock()
	n1.Stop()

	// A fresh node over the same storage restores term and log.
	n2 := NewNode(Config{ID: "solo", Storage: store, Apply: func([]byte) any { return nil }})
	defer n2.Stop()
	n2.mu.Lock()
	gotTerm, gotLen := n2.currentTerm, len(n2.log)
	n2.mu.Unlock()
	if gotTerm != wantTerm || gotLen != wantLen {
		t.Fatalf("reload = term %d len %d, want term %d len %d", gotTerm, gotLen, wantTerm, wantLen)
	}
}

func countApplied(applies []*applied) int {
	n := 0
	for _, a := range applies {
		if a.count() > 0 {
			n++
		}
	}
	return n
}
