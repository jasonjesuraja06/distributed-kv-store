package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Transport is the interface for node-to-node communication.
// The actual implementation uses gRPC, but this abstraction
// lets us test Raft logic without a real network.
type Transport interface {
	RequestVote(target string, req *VoteRequest) (*VoteResponse, error)
	AppendEntries(target string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
}

// ApplyFunc is called when a log entry is committed and should
// be applied to the state machine (the KV store).
type ApplyFunc func(entry LogEntry)

// VoteRequest is sent by candidates to request votes.
type VoteRequest struct {
	Term         uint64 // Candidate's term
	CandidateID  string // Who is requesting the vote
	LastLogIndex uint64 // Index of candidate's last log entry
	LastLogTerm  uint64 // Term of candidate's last log entry
}

// VoteResponse is the reply to a vote request.
type VoteResponse struct {
	Term        uint64 // Current term of the voter (for candidate to update itself)
	VoteGranted bool   // Did the voter grant the vote?
}

// AppendEntriesRequest is sent by the leader to replicate log entries
// and serve as heartbeats (when Entries is empty).
type AppendEntriesRequest struct {
	Term         uint64     // Leader's term
	LeaderID     string     // So followers can redirect clients
	PrevLogIndex uint64     // Index of log entry immediately preceding new ones
	PrevLogTerm  uint64     // Term of PrevLogIndex entry
	Entries      []LogEntry // Log entries to replicate (empty for heartbeat)
	LeaderCommit uint64     // Leader's commit index
}

// AppendEntriesResponse is the reply to an append entries request.
type AppendEntriesResponse struct {
	Term    uint64 // Current term of the follower
	Success bool   // True if follower contained entry matching PrevLogIndex/PrevLogTerm
}

// Node is a single Raft node that participates in consensus.
type Node struct {
	mu sync.Mutex

	// Identity
	id    string
	peers []string // IDs of all other nodes in the cluster

	// Raft state
	role      Role
	persist   PersistentState
	volatile  VolatileState
	leader    LeaderState

	// Communication
	transport Transport
	applyFn   ApplyFunc

	// Timing
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration
	electionTimer      *time.Timer
	heartbeatTicker    *time.Ticker

	// Lifecycle
	stopCh chan struct{}
	logger *log.Logger
}

// NewNode creates a new Raft node.
func NewNode(id string, peers []string, transport Transport, applyFn ApplyFunc) *Node {
	n := &Node{
		id:                 id,
		peers:              peers,
		role:               Follower,
		transport:          transport,
		applyFn:            applyFn,
		electionTimeoutMin: 300 * time.Millisecond,
		electionTimeoutMax: 500 * time.Millisecond,
		heartbeatInterval:  100 * time.Millisecond,
		stopCh:             make(chan struct{}),
		logger:             log.Default(),
	}
	n.persist.Log = make([]LogEntry, 0)
	return n
}

// Start begins the Raft node's main loop.
func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()
	n.logger.Printf("[%s] started as %s (term %d)", n.id, n.role, n.persist.CurrentTerm)
}

// Stop gracefully shuts down the Raft node.
func (n *Node) Stop() {
	close(n.stopCh)
	n.mu.Lock()
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
	}
	n.mu.Unlock()
}

// ID returns the node's identifier.
func (n *Node) ID() string { return n.id }

// Role returns the node's current role.
func (n *Node) CurrentRole() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// Term returns the node's current term.
func (n *Node) CurrentTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.persist.CurrentTerm
}

// LeaderID returns the current leader's ID (empty if unknown).
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == Leader {
		return n.id
	}
	return ""
}

// Propose submits a new command to the Raft log.
// Only the leader can accept proposals. Returns true if accepted.
func (n *Node) Propose(command []byte) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return false
	}

	entry := LogEntry{
		Term:    n.persist.CurrentTerm,
		Index:   n.lastLogIndex() + 1,
		Command: command,
	}
	n.persist.Log = append(n.persist.Log, entry)
	n.leader.MatchIndex[n.id] = entry.Index

	// Trigger immediate replication to followers
	go n.replicateToAll()

	return true
}

// ---- Election Logic ----

func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	timeout := n.electionTimeoutMin + time.Duration(rand.Int63n(int64(n.electionTimeoutMax-n.electionTimeoutMin)))
	n.electionTimer = time.AfterFunc(timeout, func() {
		n.mu.Lock()
		if n.role != Leader {
			n.startElection()
		}
		n.mu.Unlock()
	})
}

func (n *Node) startElection() {
	n.persist.CurrentTerm++
	n.role = Candidate
	n.persist.VotedFor = n.id
	term := n.persist.CurrentTerm
	lastLogIndex := n.lastLogIndex()
	lastLogTerm := n.lastLogTerm()

	n.logger.Printf("[%s] starting election for term %d", n.id, term)
	n.resetElectionTimer()

	votes := 1 // Vote for self
	needed := (len(n.peers)+1)/2 + 1

	for _, peer := range n.peers {
		go func(peer string) {
			resp, err := n.transport.RequestVote(peer, &VoteRequest{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.persist.CurrentTerm {
				n.stepDown(resp.Term)
				return
			}

			if n.role != Candidate || n.persist.CurrentTerm != term {
				return // Election is stale
			}

			if resp.VoteGranted {
				votes++
				if votes >= needed {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

func (n *Node) becomeLeader() {
	n.logger.Printf("[%s] became LEADER for term %d", n.id, n.persist.CurrentTerm)
	n.role = Leader
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	// Initialize leader state
	n.leader.NextIndex = make(map[string]uint64)
	n.leader.MatchIndex = make(map[string]uint64)
	lastIdx := n.lastLogIndex()
	for _, peer := range n.peers {
		n.leader.NextIndex[peer] = lastIdx + 1
		n.leader.MatchIndex[peer] = 0
	}
	n.leader.MatchIndex[n.id] = lastIdx

	// Start heartbeat ticker
	n.heartbeatTicker = time.NewTicker(n.heartbeatInterval)
	go func() {
		for {
			select {
			case <-n.heartbeatTicker.C:
				n.mu.Lock()
				if n.role == Leader {
					n.replicateToAll()
				}
				n.mu.Unlock()
			case <-n.stopCh:
				return
			}
		}
	}()

	// Send initial empty AppendEntries (heartbeat) to assert leadership
	n.replicateToAll()
}

func (n *Node) stepDown(newTerm uint64) {
	n.logger.Printf("[%s] stepping down to follower (term %d -> %d)", n.id, n.persist.CurrentTerm, newTerm)
	n.persist.CurrentTerm = newTerm
	n.role = Follower
	n.persist.VotedFor = ""
	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
		n.heartbeatTicker = nil
	}
	n.resetElectionTimer()
}

// ---- Log Replication ----

func (n *Node) replicateToAll() {
	for _, peer := range n.peers {
		go n.replicateTo(peer)
	}
}

func (n *Node) replicateTo(peer string) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}

	nextIdx := n.leader.NextIndex[peer]
	prevLogIndex := nextIdx - 1
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 && prevLogIndex <= uint64(len(n.persist.Log)) {
		prevLogTerm = n.persist.Log[prevLogIndex-1].Term
	}

	var entries []LogEntry
	if nextIdx <= uint64(len(n.persist.Log)) {
		entries = make([]LogEntry, len(n.persist.Log[nextIdx-1:]))
		copy(entries, n.persist.Log[nextIdx-1:])
	}

	req := &AppendEntriesRequest{
		Term:         n.persist.CurrentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.volatile.CommitIndex,
	}
	term := n.persist.CurrentTerm
	n.mu.Unlock()

	resp, err := n.transport.AppendEntries(peer, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.persist.CurrentTerm {
		n.stepDown(resp.Term)
		return
	}

	if n.role != Leader || n.persist.CurrentTerm != term {
		return
	}

	if resp.Success {
		if len(entries) > 0 {
			n.leader.NextIndex[peer] = entries[len(entries)-1].Index + 1
			n.leader.MatchIndex[peer] = entries[len(entries)-1].Index
			n.advanceCommitIndex()
		}
	} else {
		// Decrement nextIndex and retry (log inconsistency)
		if n.leader.NextIndex[peer] > 1 {
			n.leader.NextIndex[peer]--
		}
	}
}

func (n *Node) advanceCommitIndex() {
	// Find the highest index replicated on a majority of nodes.
	for idx := n.volatile.CommitIndex + 1; idx <= n.lastLogIndex(); idx++ {
		if n.persist.Log[idx-1].Term != n.persist.CurrentTerm {
			continue // Only commit entries from current term
		}
		replicatedOn := 1 // Count self
		for _, peer := range n.peers {
			if n.leader.MatchIndex[peer] >= idx {
				replicatedOn++
			}
		}
		majority := (len(n.peers)+1)/2 + 1
		if replicatedOn >= majority {
			n.volatile.CommitIndex = idx
		}
	}
	n.applyCommitted()
}

func (n *Node) applyCommitted() {
	for n.volatile.LastApplied < n.volatile.CommitIndex {
		n.volatile.LastApplied++
		entry := n.persist.Log[n.volatile.LastApplied-1]
		if n.applyFn != nil {
			n.applyFn(entry)
		}
	}
}

// ---- RPC Handlers (called when this node receives RPCs) ----

// HandleRequestVote processes an incoming vote request.
func (n *Node) HandleRequestVote(req *VoteRequest) *VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &VoteResponse{Term: n.persist.CurrentTerm}

	if req.Term < n.persist.CurrentTerm {
		resp.VoteGranted = false
		return resp
	}

	if req.Term > n.persist.CurrentTerm {
		n.stepDown(req.Term)
	}

	// Grant vote if we haven't voted yet (or already voted for this candidate)
	// AND candidate's log is at least as up-to-date as ours.
	canVote := n.persist.VotedFor == "" || n.persist.VotedFor == req.CandidateID
	logUpToDate := req.LastLogTerm > n.lastLogTerm() ||
		(req.LastLogTerm == n.lastLogTerm() && req.LastLogIndex >= n.lastLogIndex())

	if canVote && logUpToDate {
		n.persist.VotedFor = req.CandidateID
		resp.VoteGranted = true
		n.resetElectionTimer()
	}

	resp.Term = n.persist.CurrentTerm
	return resp
}

// HandleAppendEntries processes an incoming append entries request.
func (n *Node) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &AppendEntriesResponse{Term: n.persist.CurrentTerm}

	if req.Term < n.persist.CurrentTerm {
		resp.Success = false
		return resp
	}

	if req.Term > n.persist.CurrentTerm || n.role == Candidate {
		n.stepDown(req.Term)
	}

	n.resetElectionTimer()

	// Check if our log contains an entry at PrevLogIndex with PrevLogTerm
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > uint64(len(n.persist.Log)) {
			resp.Success = false
			return resp
		}
		if n.persist.Log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			// Conflict — delete this entry and everything after it
			n.persist.Log = n.persist.Log[:req.PrevLogIndex-1]
			resp.Success = false
			return resp
		}
	}

	// Append new entries (overwrite conflicts)
	for i, entry := range req.Entries {
		logIdx := req.PrevLogIndex + uint64(i) + 1
		if logIdx <= uint64(len(n.persist.Log)) {
			if n.persist.Log[logIdx-1].Term != entry.Term {
				n.persist.Log = n.persist.Log[:logIdx-1]
				n.persist.Log = append(n.persist.Log, entry)
			}
		} else {
			n.persist.Log = append(n.persist.Log, entry)
		}
	}

	// Update commit index
	if req.LeaderCommit > n.volatile.CommitIndex {
		lastNewEntry := req.PrevLogIndex + uint64(len(req.Entries))
		if req.LeaderCommit < lastNewEntry {
			n.volatile.CommitIndex = req.LeaderCommit
		} else {
			n.volatile.CommitIndex = lastNewEntry
		}
		n.applyCommitted()
	}

	resp.Success = true
	resp.Term = n.persist.CurrentTerm
	return resp
}

// ---- Helpers ----

func (n *Node) lastLogIndex() uint64 {
	return uint64(len(n.persist.Log))
}

func (n *Node) lastLogTerm() uint64 {
	if len(n.persist.Log) == 0 {
		return 0
	}
	return n.persist.Log[len(n.persist.Log)-1].Term
}
