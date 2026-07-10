package raft

type LogEntry struct {
	Term    int
	Command []byte
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// ConflictIndex lets the leader skip a follower's whole conflicting run in
	// one round instead of backing up one entry at a time.
	ConflictIndex int
}

// Transport sends RPCs to a peer, addressed by its id.
type Transport interface {
	RequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error)
}
