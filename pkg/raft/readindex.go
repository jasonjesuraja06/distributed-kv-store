package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ============================================================
// LINEARIZABLE READS via READ-INDEX
// ============================================================
//
// Problem this solves:
//   Local-replica reads (the simple "just read from any node") are
//   eventually consistent: a follower may serve stale data if it
//   hasn't yet applied recent commits. Linearizable reads require
//   that any read sees all committed writes prior to it.
//
// Naive solution:
//   Route every read through Raft as a no-op command. Correct but
//   slow — every read is an O(network) round trip.
//
// Read-index (Raft paper §6.4):
//   1. Leader records its current commitIndex as readIndex.
//   2. Leader sends a heartbeat round (AppendEntries with no entries)
//      to a majority of peers. If a majority responds successfully,
//      the leader has confirmed it is still the leader as of NOW.
//   3. Wait for the state machine's lastApplied to catch up to readIndex.
//   4. Serve the read from the local state machine.
//
// This serves linearizable reads at the cost of only ONE heartbeat
// round trip (no log append, no disk write, no replication).
//
// Used by: etcd (with optional lease-read optimization on top),
// TiKV, CockroachDB.
// ============================================================

// ErrNotLeader is returned by ReadIndex when called on a non-leader node.
var ErrNotLeader = errors.New("raft: not leader")

// ErrLeadershipLost is returned by ReadIndex when the heartbeat round
// fails to reach a majority (leadership cannot be confirmed).
var ErrLeadershipLost = errors.New("raft: leadership not confirmed by majority")

// ReadIndex performs the read-index protocol and returns the commit
// index at which a linearizable read is safe to serve. The caller is
// responsible for waiting until the state machine has applied that
// index before serving the read.
//
// Returns ErrNotLeader if this node is not the leader.
// Returns ErrLeadershipLost if a majority heartbeat round fails.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	readIdx := n.volatile.CommitIndex
	term := n.persist.CurrentTerm
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	// Solo-cluster fast path: we are our own majority.
	if len(peers) == 0 {
		return readIdx, nil
	}

	// Send a heartbeat round to confirm we're still the leader.
	needed := (len(peers)+1)/2 + 1
	confirmed := 1 // self

	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	heartbeatDeadline := time.Now().Add(2 * n.heartbeatInterval)
	hbCtx, cancel := context.WithDeadline(ctx, heartbeatDeadline)
	defer cancel()

	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			// Build a heartbeat AppendEntries (no entries, just the leader's
			// current commit index).
			n.mu.Lock()
			prevLogIndex := n.lastLogIndex()
			prevLogTerm := n.lastLogTerm()
			req := &AppendEntriesRequest{
				Term:         term,
				LeaderID:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil,
				LeaderCommit: n.volatile.CommitIndex,
			}
			n.mu.Unlock()

			respCh := make(chan *AppendEntriesResponse, 1)
			go func() {
				resp, err := n.transport.AppendEntries(peer, req)
				if err == nil {
					respCh <- resp
				} else {
					respCh <- nil
				}
			}()

			select {
			case resp := <-respCh:
				if resp == nil || !resp.Success {
					return
				}
				mu.Lock()
				confirmed++
				mu.Unlock()
			case <-hbCtx.Done():
				return
			}
		}(peer)
	}

	wg.Wait()

	mu.Lock()
	ok := confirmed >= needed
	mu.Unlock()
	if !ok {
		return 0, ErrLeadershipLost
	}

	// Re-check we are still the leader for the same term.
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.persist.CurrentTerm != term {
		return 0, ErrLeadershipLost
	}
	return readIdx, nil
}

// WaitForApply blocks until the state machine has applied entries up
// through the given index, or until the context is cancelled.
// Pairs with ReadIndex: caller passes the readIndex returned above.
func (n *Node) WaitForApply(ctx context.Context, index uint64) error {
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		n.mu.Lock()
		applied := n.volatile.LastApplied
		n.mu.Unlock()
		if applied >= index {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("raft: timed out waiting for index %d (applied=%d)", index, applied)
		case <-time.After(2 * time.Millisecond):
			// retry
		}
	}
}
