package raft

// ============================================================
// INSTALLSNAPSHOT RPC
// ============================================================
//
// Problem this solves:
//   When a follower has fallen so far behind that the leader has
//   already compacted away the log entries the follower would need
//   to catch up (i.e., follower.nextIndex <= leader.LastIncludedIndex),
//   AppendEntries can never make progress. The Raft paper §7 prescribes
//   InstallSnapshot: the leader sends its entire state-machine snapshot
//   to the follower in one or more chunks.
//
// Our implementation:
//   - Single-RPC snapshot transfer (not chunked). Fine for snapshots
//     up to a few MB; a chunked version would split Data into 64KB
//     chunks indexed by Offset.
//   - The follower swaps its in-memory state machine for the received
//     snapshot, then truncates its Raft log up to LastIncludedIndex.
//
// Used by: etcd, CockroachDB, every production Raft.
// ============================================================

// InstallSnapshotRequest carries a full state-machine snapshot from
// leader to a severely-lagging follower.
type InstallSnapshotRequest struct {
	Term              uint64 // leader's term
	LeaderID          string
	LastIncludedIndex uint64 // snapshot replaces all entries up through this index
	LastIncludedTerm  uint64
	Offset            uint64 // byte offset of this chunk (always 0 in our single-shot impl)
	Data              []byte // serialized state-machine snapshot
	Done              bool   // true on the final chunk (always true here)
}

// InstallSnapshotResponse is the follower's reply.
type InstallSnapshotResponse struct {
	Term    uint64 // follower's current term (so leader can step down if stale)
	Success bool
}

// SnapshotProvider is supplied by the state-machine wrapper so the
// Raft node can read the current snapshot bytes when it needs to send
// them to a lagging follower.
type SnapshotProvider func() (data []byte, lastIncludedIndex uint64, lastIncludedTerm uint64, err error)

// SnapshotInstaller is supplied by the state-machine wrapper so the
// Raft node can hand a received snapshot to the state machine for
// installation.
type SnapshotInstaller func(data []byte, lastIncludedIndex uint64, lastIncludedTerm uint64) error

// SetSnapshotHandlers wires in the state-machine callbacks for sending
// and receiving snapshots. Must be called before any leader-side
// AppendEntries to a follower that may need a snapshot.
func (n *Node) SetSnapshotHandlers(provider SnapshotProvider, installer SnapshotInstaller) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapProvider = provider
	n.snapInstaller = installer
}

// HandleInstallSnapshot is the follower-side handler.
func (n *Node) HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	n.mu.Lock()
	resp := &InstallSnapshotResponse{Term: n.persist.CurrentTerm}

	// Reject stale snapshots.
	if req.Term < n.persist.CurrentTerm {
		n.mu.Unlock()
		return resp
	}

	// Step down if the leader's term is newer.
	if req.Term > n.persist.CurrentTerm {
		n.stepDown(req.Term)
	}
	n.resetElectionTimer()

	// If the snapshot is older than what we already have, no-op success.
	if req.LastIncludedIndex <= n.persist.LastIncludedIndex {
		resp.Success = true
		resp.Term = n.persist.CurrentTerm
		n.mu.Unlock()
		return resp
	}

	installer := n.snapInstaller
	n.mu.Unlock()

	// Install via the state-machine callback (releases mutex while doing I/O).
	if installer != nil {
		if err := installer(req.Data, req.LastIncludedIndex, req.LastIncludedTerm); err != nil {
			n.logger.Printf("[%s] InstallSnapshot install failed: %v", n.id, err)
			return resp
		}
	}

	// Update Raft state to reflect the new baseline.
	n.RestoreFromSnapshot(req.LastIncludedIndex, req.LastIncludedTerm)

	n.mu.Lock()
	resp.Success = true
	resp.Term = n.persist.CurrentTerm
	n.mu.Unlock()
	return resp
}

// sendSnapshotTo sends the current snapshot to a single peer. Called from
// the leader when it detects that a follower needs a snapshot because
// nextIndex has fallen below LastIncludedIndex. Holds no locks.
func (n *Node) sendSnapshotTo(peer string) {
	n.mu.Lock()
	if n.role != Leader || n.snapProvider == nil {
		n.mu.Unlock()
		return
	}
	provider := n.snapProvider
	term := n.persist.CurrentTerm
	leaderID := n.id
	n.mu.Unlock()

	data, lastIdx, lastTerm, err := provider()
	if err != nil {
		n.logger.Printf("[%s] snapshot provider failed: %v", n.id, err)
		return
	}
	if lastIdx == 0 {
		return // no snapshot yet
	}

	req := &InstallSnapshotRequest{
		Term:              term,
		LeaderID:          leaderID,
		LastIncludedIndex: lastIdx,
		LastIncludedTerm:  lastTerm,
		Offset:            0,
		Data:              data,
		Done:              true,
	}

	resp, err := n.transport.InstallSnapshot(peer, req)
	if err != nil || resp == nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.persist.CurrentTerm {
		n.stepDown(resp.Term)
		return
	}
	if !resp.Success {
		return
	}

	// Mark the follower as caught up through the snapshot boundary.
	if n.leader.NextIndex == nil {
		n.leader.NextIndex = make(map[string]uint64)
	}
	if n.leader.MatchIndex == nil {
		n.leader.MatchIndex = make(map[string]uint64)
	}
	n.leader.NextIndex[peer] = lastIdx + 1
	if n.leader.MatchIndex[peer] < lastIdx {
		n.leader.MatchIndex[peer] = lastIdx
	}
}
