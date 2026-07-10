// Package raft implements the Raft consensus algorithm: a cluster of nodes keeps
// an identical replicated log, and a committed entry is applied to each node's
// state machine in the same order. It provides leader election, log replication
// with the log-matching and election-restriction safety rules, and persistence
// of the term, vote, and log across restarts.
package raft

import (
	"math/rand"
	"sync"
	"time"
)

const heartbeatInterval = 50 * time.Millisecond

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type Config struct {
	ID        string
	Peers     []string
	Transport Transport
	Storage   Storage
	Apply     func(cmd []byte) any
}

type Node struct {
	mu        sync.Mutex
	id        string
	peers     []string
	transport Transport
	storage   Storage
	apply     func(cmd []byte) any

	currentTerm int
	votedFor    string
	log         []LogEntry // index 0 is a sentinel with term 0

	role        Role
	commitIndex int
	lastApplied int
	leaderID    string
	lastHeard   time.Time
	elecTimeout time.Duration

	nextIndex  map[string]int
	matchIndex map[string]int

	// notify wakes a proposer once its entry is applied; notifyTerm detects an
	// entry overwritten by a new leader before it committed.
	notify     map[int]chan any
	notifyTerm map[int]int

	applyCond *sync.Cond
	stopCh    chan struct{}
	stopped   bool
}

func NewNode(cfg Config) *Node {
	n := &Node{
		id:         cfg.ID,
		peers:      cfg.Peers,
		transport:  cfg.Transport,
		storage:    cfg.Storage,
		apply:      cfg.Apply,
		role:       Follower,
		log:        []LogEntry{{Term: 0}},
		nextIndex:  make(map[string]int),
		matchIndex: make(map[string]int),
		notify:     make(map[int]chan any),
		notifyTerm: make(map[int]int),
		stopCh:     make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)
	if cfg.Storage != nil {
		if term, votedFor, log, err := cfg.Storage.Load(); err == nil && log != nil {
			n.currentTerm, n.votedFor, n.log = term, votedFor, log
		}
	}
	n.lastHeard = time.Now()
	n.resetElectionTimeout()
	go n.ticker()
	go n.applier()
	return n
}

func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.applyCond.Broadcast()
}

// Propose appends cmd to the log if this node is the leader and returns a channel
// that receives the state machine's result once the entry is applied, or is
// closed if the entry is lost to a leadership change.
func (n *Node) Propose(cmd []byte) (chan any, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return nil, false
	}
	term := n.currentTerm
	n.log = append(n.log, LogEntry{Term: term, Command: cmd})
	n.persist()
	idx := n.lastLogIndex()
	ch := make(chan any, 1)
	n.notify[idx] = ch
	n.notifyTerm[idx] = term
	n.advanceCommit() // a single-node cluster is its own majority
	go n.broadcast()
	return ch, true
}

func (n *Node) ID() string { return n.id }

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

func (n *Node) Leader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// --- RPC handlers ---

func (n *Node) RequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	reply := RequestVoteReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply
	}
	// Only grant to a candidate whose log is at least as up-to-date as ours.
	upToDate := args.LastLogTerm > n.lastLogTerm() ||
		(args.LastLogTerm == n.lastLogTerm() && args.LastLogIndex >= n.lastLogIndex())
	if (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		n.lastHeard = time.Now()
		n.resetElectionTimeout()
		reply.VoteGranted = true
	}
	n.persist()
	return reply
}

func (n *Node) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	reply := AppendEntriesReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply
	}
	n.role = Follower
	n.leaderID = args.LeaderID
	n.lastHeard = time.Now()
	n.resetElectionTimeout()

	// Log-matching: reject unless our entry at PrevLogIndex has PrevLogTerm,
	// reporting where the conflicting run begins so the leader backs up fast.
	if args.PrevLogIndex > n.lastLogIndex() {
		reply.ConflictIndex = n.lastLogIndex() + 1
		n.persist()
		return reply
	}
	if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		bad := n.log[args.PrevLogIndex].Term
		ci := args.PrevLogIndex
		for ci > 1 && n.log[ci-1].Term == bad {
			ci--
		}
		reply.ConflictIndex = ci
		n.persist()
		return reply
	}

	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx > n.lastLogIndex() {
			n.log = append(n.log, e)
		} else if n.log[idx].Term != e.Term {
			n.log = append(n.log[:idx], e) // drop the conflicting suffix, take theirs
		}
	}
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, n.lastLogIndex())
		n.applyCond.Signal()
	}
	reply.Success = true
	n.persist()
	return reply
}

// --- elections ---

func (n *Node) ticker() {
	for {
		time.Sleep(10 * time.Millisecond)
		select {
		case <-n.stopCh:
			return
		default:
		}
		n.mu.Lock()
		timedOut := n.role != Leader && time.Since(n.lastHeard) >= n.elecTimeout
		n.mu.Unlock()
		if timedOut {
			n.startElection()
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.lastHeard = time.Now()
	n.resetElectionTimeout()
	term := n.currentTerm
	lastIdx, lastTerm := n.lastLogIndex(), n.lastLogTerm()
	n.persist()
	if n.majority() == 1 { // single-node cluster: our own vote is a majority
		n.becomeLeader()
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	votes := 1
	for _, peer := range n.peers {
		go func(peer string) {
			reply, err := n.transport.RequestVote(peer, RequestVoteArgs{term, n.id, lastIdx, lastTerm})
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.role != Candidate || n.currentTerm != term {
				return
			}
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				n.persist()
				return
			}
			if reply.VoteGranted {
				votes++
				if votes == n.majority() {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeLeader assumes mu is held.
func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.id
	last := n.lastLogIndex()
	for _, p := range n.peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	go n.leaderLoop(n.currentTerm)
}

func (n *Node) leaderLoop(term int) {
	for {
		n.mu.Lock()
		ok := !n.stopped && n.role == Leader && n.currentTerm == term
		n.mu.Unlock()
		if !ok {
			return
		}
		n.broadcast()
		time.Sleep(heartbeatInterval)
	}
}

func (n *Node) broadcast() {
	n.mu.Lock()
	leader := n.role == Leader
	peers := n.peers
	n.mu.Unlock()
	if !leader {
		return
	}
	for _, p := range peers {
		go n.replicateTo(p)
	}
}

func (n *Node) replicateTo(peer string) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	ni := n.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	prevIdx := ni - 1
	prevTerm := n.log[prevIdx].Term
	entries := append([]LogEntry(nil), n.log[ni:]...)
	commit := n.commitIndex
	n.mu.Unlock()

	reply, err := n.transport.AppendEntries(peer, AppendEntriesArgs{term, n.id, prevIdx, prevTerm, entries, commit})
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.currentTerm != term {
		return
	}
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		n.persist()
		return
	}
	if reply.Success {
		n.matchIndex[peer] = prevIdx + len(entries)
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommit()
	} else if reply.ConflictIndex > 0 {
		n.nextIndex[peer] = max(1, reply.ConflictIndex)
	} else {
		n.nextIndex[peer] = max(1, ni-1)
	}
}

// advanceCommit assumes mu is held. A leader only commits an entry from its own
// term once a majority stores it; earlier-term entries then commit with it.
func (n *Node) advanceCommit() {
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			continue
		}
		count := 1
		for _, p := range n.peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			n.applyCond.Signal()
			return
		}
	}
}

// becomeFollower assumes mu is held. Pending proposals are failed so their
// waiters do not hang once this node is no longer leader.
func (n *Node) becomeFollower(term int) {
	n.currentTerm = term
	n.role = Follower
	n.votedFor = ""
	for i, ch := range n.notify {
		close(ch)
		delete(n.notify, i)
		delete(n.notifyTerm, i)
	}
}

func (n *Node) applier() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for n.lastApplied >= n.commitIndex && !n.stopped {
			n.applyCond.Wait()
		}
		if n.stopped {
			return
		}
		n.lastApplied++
		idx := n.lastApplied
		entry := n.log[idx]
		n.mu.Unlock()
		var res any
		if len(entry.Command) > 0 && n.apply != nil {
			res = n.apply(entry.Command)
		}
		n.mu.Lock()
		if ch, ok := n.notify[idx]; ok {
			if n.notifyTerm[idx] == entry.Term {
				ch <- res
			} else {
				close(ch)
			}
			delete(n.notify, idx)
			delete(n.notifyTerm, idx)
		}
	}
}

func (n *Node) persist() {
	if n.storage != nil {
		n.storage.Save(n.currentTerm, n.votedFor, n.log)
	}
}

func (n *Node) resetElectionTimeout() {
	n.elecTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (n *Node) lastLogIndex() int { return len(n.log) - 1 }
func (n *Node) lastLogTerm() int  { return n.log[len(n.log)-1].Term }
func (n *Node) majority() int     { return (len(n.peers)+1)/2 + 1 }
