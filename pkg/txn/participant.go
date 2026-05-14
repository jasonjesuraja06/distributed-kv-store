package txn

import (
	"context"
	"errors"
	"sync"
)

// ============================================================
// SHARD PARTICIPANT (in-memory reference implementation)
// ============================================================
//
// A real participant is backed by a Raft group: Prepare appends a
// "tentative" entry to the log, Commit appends a "finalize" entry,
// and Abort appends a "discard" entry. Locks are held in memory
// between Prepare and Commit/Abort. Replicating the prepared state
// through Raft is what makes the participant's vote durable.
//
// This in-memory implementation is sufficient for the cross-shard
// transaction tests and for demonstrating the 2PC machinery.
// ============================================================

// ShardParticipant is an in-memory key-value participant that locks
// keys between Prepare and Commit/Abort.
type ShardParticipant struct {
	mu        sync.Mutex
	shardID   int
	data      map[string]string
	staged    map[string]Op   // txnID -> staged op
	lockOwner map[string]string // key -> txnID currently holding the lock
}

// NewShardParticipant constructs a fresh in-memory shard participant.
func NewShardParticipant(shardID int) *ShardParticipant {
	return &ShardParticipant{
		shardID:   shardID,
		data:      make(map[string]string),
		staged:    make(map[string]Op),
		lockOwner: make(map[string]string),
	}
}

// Get reads a committed value from the shard.
func (s *ShardParticipant) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok
}

// Prepare attempts to lock the op's key on behalf of txnID.
//
// Returns VoteYes if:
//   - the op targets this shard, AND
//   - the key is not currently locked by a different transaction
//
// Returns VoteNo otherwise. Note that we do NOT mutate s.data during
// Prepare — only Commit applies the staged op.
func (s *ShardParticipant) Prepare(_ context.Context, req *PrepareRequest) (*PrepareResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Op.ShardID != s.shardID {
		return &PrepareResponse{TxnID: req.TxnID, Vote: VoteNo,
			Error: "op targets different shard"}, nil
	}

	if owner, locked := s.lockOwner[req.Op.Key]; locked && owner != req.TxnID {
		return &PrepareResponse{TxnID: req.TxnID, Vote: VoteNo,
			Error: "key locked by another txn"}, nil
	}

	s.lockOwner[req.Op.Key] = req.TxnID
	s.staged[req.TxnID] = req.Op
	return &PrepareResponse{TxnID: req.TxnID, Vote: VoteYes}, nil
}

// Commit applies the staged op and releases the lock.
func (s *ShardParticipant) Commit(_ context.Context, req *CommitRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	op, ok := s.staged[req.TxnID]
	if !ok {
		// Idempotent: committing an unknown (or already-committed) txn
		// is fine — participants must tolerate retries.
		return nil
	}

	switch op.Type {
	case OpPut:
		s.data[op.Key] = op.Value
	case OpDelete:
		delete(s.data, op.Key)
	default:
		return errors.New("txn: unknown op type")
	}

	delete(s.staged, req.TxnID)
	delete(s.lockOwner, op.Key)
	return nil
}

// Abort drops the staged op and releases the lock.
func (s *ShardParticipant) Abort(_ context.Context, req *AbortRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	op, ok := s.staged[req.TxnID]
	if !ok {
		return nil // idempotent
	}
	delete(s.staged, req.TxnID)
	delete(s.lockOwner, op.Key)
	return nil
}

// Snapshot returns a copy of the committed state (for tests).
func (s *ShardParticipant) Snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
