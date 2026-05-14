package raft

import (
	"fmt"
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

	// Solo-cluster edge case: with no peers, the AppendEntries response path
	// never fires, so commit/apply must be driven directly from Propose.
	if len(n.peers) == 0 {
		n.advanceCommitIndex()
	}

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

	// Edge case: single-node cluster has needed=1 and we already voted
	// for ourselves. Become leader immediately — there are no peers to ask.
	if votes >= needed {
		n.becomeLeader()
		return
	}

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
	if nextIdx == 0 {
		nextIdx = 1
	}
	prevLogIndex := nextIdx - 1
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 {
		prevLogTerm = n.termAt(prevLogIndex)
	}

	entries := n.entriesFrom(nextIdx)

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
		if n.termAt(idx) != n.persist.CurrentTerm {
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
		entry := n.logEntry(n.volatile.LastApplied)
		if entry == nil {
			// Already covered by snapshot — skip silently. This path is
			// reached when CommitIndex jumps forward after RestoreFromSnapshot.
			continue
		}
		if n.applyFn != nil {
			n.applyFn(*entry)
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

	// Check if our log contains an entry at PrevLogIndex with PrevLogTerm.
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex < n.persist.LastIncludedIndex {
			// Leader sent entries that overlap our snapshot. Drop the overlap
			// and treat the rest as new entries from the snapshot boundary.
			skip := int(n.persist.LastIncludedIndex - req.PrevLogIndex)
			if skip >= len(req.Entries) {
				resp.Success = true
				resp.Term = n.persist.CurrentTerm
				return resp
			}
			req.Entries = req.Entries[skip:]
			req.PrevLogIndex = n.persist.LastIncludedIndex
			req.PrevLogTerm = n.persist.LastIncludedTerm
		} else if req.PrevLogIndex > n.lastLogIndex() {
			resp.Success = false
			return resp
		} else if n.termAt(req.PrevLogIndex) != req.PrevLogTerm {
			// Conflict at PrevLogIndex — truncate at conflict.
			n.truncateLogAt(req.PrevLogIndex)
			resp.Success = false
			return resp
		}
	}

	// Append new entries (overwrite conflicts).
	for _, entry := range req.Entries {
		if entry.Index <= n.persist.LastIncludedIndex {
			continue // already covered by snapshot
		}
		existing := n.logEntry(entry.Index)
		if existing != nil {
			if existing.Term != entry.Term {
				n.truncateLogAt(entry.Index)
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

// ---- Snapshot / Log Compaction ----

// LogStats reports the current state of the log + snapshot boundary.
type LogStats struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	FirstLogIndex     uint64
	LastLogIndex      uint64
	LogEntries        int
}

// LogState returns a snapshot of the log/snapshot metadata for monitoring.
func (n *Node) LogState() LogStats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return LogStats{
		LastIncludedIndex: n.persist.LastIncludedIndex,
		LastIncludedTerm:  n.persist.LastIncludedTerm,
		FirstLogIndex:     n.persist.LastIncludedIndex + 1,
		LastLogIndex:      n.lastLogIndex(),
		LogEntries:        len(n.persist.Log),
	}
}

// CreateSnapshot is called by the state-machine wrapper after it has
// successfully serialized + persisted its state. It truncates this node's
// log up to (and including) lastIncludedIndex, freeing memory.
//
// Returns an error if lastIncludedIndex is in the future (not yet committed)
// or already covered by an earlier snapshot.
func (n *Node) CreateSnapshot(lastIncludedIndex uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if lastIncludedIndex <= n.persist.LastIncludedIndex {
		return nil // no-op
	}
	if lastIncludedIndex > n.volatile.CommitIndex {
		return fmt.Errorf("raft: cannot snapshot uncommitted index %d (commitIndex=%d)",
			lastIncludedIndex, n.volatile.CommitIndex)
	}

	term := n.termAt(lastIncludedIndex)
	if term == 0 {
		return fmt.Errorf("raft: no entry at index %d", lastIncludedIndex)
	}

	relCut := int(lastIncludedIndex - n.persist.LastIncludedIndex)
	if relCut > len(n.persist.Log) {
		relCut = len(n.persist.Log)
	}
	newLog := make([]LogEntry, len(n.persist.Log)-relCut)
	copy(newLog, n.persist.Log[relCut:])
	n.persist.Log = newLog

	n.persist.LastIncludedIndex = lastIncludedIndex
	n.persist.LastIncludedTerm = term

	n.logger.Printf("[%s] log compacted: lastIncludedIndex=%d term=%d entries_remaining=%d",
		n.id, lastIncludedIndex, term, len(n.persist.Log))
	return nil
}

// RestoreFromSnapshot is called during startup if a snapshot was loaded
// from disk. It resets log state so that subsequent AppendEntries behave
// correctly relative to the new baseline.
func (n *Node) RestoreFromSnapshot(lastIncludedIndex, lastIncludedTerm uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.persist.LastIncludedIndex = lastIncludedIndex
	n.persist.LastIncludedTerm = lastIncludedTerm
	n.persist.Log = nil
	if n.volatile.CommitIndex < lastIncludedIndex {
		n.volatile.CommitIndex = lastIncludedIndex
	}
	if n.volatile.LastApplied < lastIncludedIndex {
		n.volatile.LastApplied = lastIncludedIndex
	}
	n.logger.Printf("[%s] restored from snapshot: lastIncludedIndex=%d term=%d",
		n.id, lastIncludedIndex, lastIncludedTerm)
}

// truncateLogAt removes all entries with Index >= absIndex.
func (n *Node) truncateLogAt(absIndex uint64) {
	if absIndex <= n.persist.LastIncludedIndex+1 {
		n.persist.Log = nil
		return
	}
	relIdx := int(absIndex - n.persist.LastIncludedIndex - 1)
	if relIdx < 0 {
		return
	}
	if relIdx > len(n.persist.Log) {
		return
	}
	n.persist.Log = n.persist.Log[:relIdx]
}

// ---- Log access helpers (snapshot-aware) ----
//
// After a snapshot is taken at absolute index K (term T), entries 1..K are
// discarded. The remaining `n.persist.Log` slice's element 0 has Index = K+1.
// All callers must go through these helpers to translate between absolute
// log indices and slice positions.

// lastLogIndex returns the absolute index of the last entry in the log,
// including any entries covered by the snapshot.
func (n *Node) lastLogIndex() uint64 {
	return n.persist.LastIncludedIndex + uint64(len(n.persist.Log))
}

// lastLogTerm returns the term of the last entry. If the log slice is empty
// (everything is in the snapshot), returns the snapshot's term.
func (n *Node) lastLogTerm() uint64 {
	if len(n.persist.Log) == 0 {
		return n.persist.LastIncludedTerm
	}
	return n.persist.Log[len(n.persist.Log)-1].Term
}

// logEntry returns the entry at the given absolute index, or nil if that
// index has been compacted into a snapshot or is beyond the end of the log.
func (n *Node) logEntry(absIndex uint64) *LogEntry {
	if absIndex <= n.persist.LastIncludedIndex {
		return nil
	}
	relIdx := int(absIndex - n.persist.LastIncludedIndex - 1)
	if relIdx < 0 || relIdx >= len(n.persist.Log) {
		return nil
	}
	return &n.persist.Log[relIdx]
}

// termAt returns the term of the entry at the given absolute index.
// Handles the boundary case (index == LastIncludedIndex maps to the snapshot
// term) and returns 0 for absent indices.
func (n *Node) termAt(absIndex uint64) uint64 {
	if absIndex == n.persist.LastIncludedIndex {
		return n.persist.LastIncludedTerm
	}
	if e := n.logEntry(absIndex); e != nil {
		return e.Term
	}
	return 0
}

// entriesFrom returns a copy of all entries with Index >= start. If start is
// covered by the snapshot, returns nil (caller should send a snapshot
// instead, but this implementation falls back to "send everything we have").
func (n *Node) entriesFrom(absStart uint64) []LogEntry {
	if absStart <= n.persist.LastIncludedIndex {
		// Caller wants entries we no longer have. Send what we do have.
		out := make([]LogEntry, len(n.persist.Log))
		copy(out, n.persist.Log)
		return out
	}
	relIdx := int(absStart - n.persist.LastIncludedIndex - 1)
	if relIdx >= len(n.persist.Log) {
		return nil
	}
	out := make([]LogEntry, len(n.persist.Log)-relIdx)
	copy(out, n.persist.Log[relIdx:])
	return out
}
