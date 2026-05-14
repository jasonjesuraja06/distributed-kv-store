package raft

import (
	"sync"
	"time"
)

// ============================================================
// PRE-VOTE OPTIMIZATION
// ============================================================
//
// Problem this solves:
//   When a node is partitioned away from the cluster, its election
//   timer keeps firing — each one increments its term. When the
//   partition heals and the node rejoins, its inflated term forces
//   the (perfectly healthy) leader to step down, causing a disruptive
//   re-election even though the rejoining node will lose the vote
//   anyway because its log is stale.
//
// The fix (introduced in the Raft thesis, §9.6):
//   Before starting a real election, the candidate first asks peers
//   "WOULD you vote for me if I started an election?" This is the
//   PreVote phase. Pre-vote responses do NOT cause the responder to
//   change its term. The candidate only increments its term and
//   solicits real votes if a majority of pre-votes succeed.
//
// Used by: etcd, CockroachDB, TiKV — every production-grade Raft.
// ============================================================

// PreVoteRequest probes whether peers would vote for the candidate
// in a hypothetical election with the proposed term. It does NOT
// modify the responder's persistent state.
type PreVoteRequest struct {
	Term         uint64 // proposed term (= currentTerm + 1)
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// PreVoteResponse signals whether the responder *would* vote if a
// real election were held. The responder's term and voted-for are
// not changed by responding.
type PreVoteResponse struct {
	Term        uint64 // responder's current term (informational)
	VoteGranted bool
}

// HandlePreVote is the receiver-side handler for an incoming PreVote.
// It is read-only with respect to persistent state — the responder
// must not update CurrentTerm or VotedFor here.
func (n *Node) HandlePreVote(req *PreVoteRequest) *PreVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &PreVoteResponse{Term: n.persist.CurrentTerm}

	// Reject pre-votes from stale terms.
	if req.Term < n.persist.CurrentTerm {
		return resp
	}

	// Would we vote for them? Only if their log is at least as
	// up-to-date as ours (same up-to-date check as real RequestVote).
	logUpToDate := req.LastLogTerm > n.lastLogTerm() ||
		(req.LastLogTerm == n.lastLogTerm() && req.LastLogIndex >= n.lastLogIndex())

	// Also require that we haven't heard from a current leader recently
	// (i.e., our election timer is close to firing). Without this, a
	// recovered partition could still disrupt a healthy leader.
	// Simple proxy: only grant pre-votes if we are NOT a leader and our
	// term is at most the proposed term.
	if logUpToDate && n.role != Leader {
		resp.VoteGranted = true
	}

	return resp
}

// startPreVote runs the pre-vote phase and returns true if a majority
// of peers (including self) would grant a vote. Called with n.mu held.
func (n *Node) startPreVote() bool {
	proposedTerm := n.persist.CurrentTerm + 1
	lastLogIndex := n.lastLogIndex()
	lastLogTerm := n.lastLogTerm()
	peers := append([]string(nil), n.peers...)
	needed := (len(peers)+1)/2 + 1

	// Solo-cluster fast path: we are our own majority.
	if len(peers) == 0 {
		return true
	}

	n.mu.Unlock()
	defer n.mu.Lock()

	var (
		wg          sync.WaitGroup
		mu          sync.Mutex
		granted     = 1 // self
		highestSeen = uint64(0)
	)

	ctx := time.NewTimer(n.electionTimeoutMin / 2)
	defer ctx.Stop()

	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			resp, err := n.transport.PreVote(peer, &PreVoteRequest{
				Term:         proposedTerm,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil || resp == nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if resp.Term > highestSeen {
				highestSeen = resp.Term
			}
			if resp.VoteGranted {
				granted++
			}
		}(peer)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.C:
		// timeout — proceed with whatever votes we have
	}

	mu.Lock()
	ok := granted >= needed
	mu.Unlock()
	return ok
}
