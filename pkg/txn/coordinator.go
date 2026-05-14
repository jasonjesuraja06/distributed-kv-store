package txn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ============================================================
// CROSS-SHARD TRANSACTIONS via TWO-PHASE COMMIT (2PC)
// ============================================================
//
// Problem this solves:
//   A single-shard Raft cluster gives linearizable single-key ops,
//   but cannot atomically update keys that live on DIFFERENT shards.
//   Example: transfer($100) from account "alice" (shard 0) to "bob"
//   (shard 1) requires both updates to apply together or neither.
//
// Protocol (Gray, 1978):
//   Phase 1 — PREPARE:
//     Coordinator sends Prepare(txnID, op) to every participating
//     shard. Each shard locks the affected keys, stages the op, and
//     replies VOTE_YES or VOTE_NO. A shard that votes YES is bound:
//     it has durably remembered that it can commit later.
//
//   Phase 2 — COMMIT/ABORT:
//     If ALL shards voted YES: send Commit; each shard applies the
//     staged op and releases locks.
//     If ANY shard voted NO (or timed out): send Abort; each shard
//     drops its staged op and releases locks.
//
// Failure handling:
//   - Coordinator crash before Phase 2: participants block holding
//     locks (this is the well-known "blocking" weakness of 2PC).
//     Recovery requires either a coordinator restart that reads its
//     log and replays Phase 2, or a presumed-abort timeout.
//   - Participant crash after voting YES: on recovery, the participant
//     re-asks the coordinator for the outcome.
//
// Used by (or evolved-from): early distributed DBs, X/Open XA, MySQL
// XA, and as a primitive inside protocols like Paxos Commit and
// Calvin's deterministic ordering scheme.
// ============================================================

// Op describes a single key-level operation in a transaction.
type Op struct {
	ShardID int    `json:"shard_id"`
	Key     string `json:"key"`
	Type    OpType `json:"type"`
	Value   string `json:"value,omitempty"` // for PUT only
}

// OpType is the kind of operation.
type OpType string

const (
	OpPut    OpType = "put"
	OpDelete OpType = "delete"
)

// Vote is a participant's response to Prepare.
type Vote string

const (
	VoteYes Vote = "yes"
	VoteNo  Vote = "no"
)

// PrepareRequest is sent by the coordinator to each participant.
type PrepareRequest struct {
	TxnID string `json:"txn_id"`
	Op    Op     `json:"op"`
}

// PrepareResponse is the participant's vote.
type PrepareResponse struct {
	TxnID string `json:"txn_id"`
	Vote  Vote   `json:"vote"`
	Error string `json:"error,omitempty"`
}

// CommitRequest tells a participant to commit the previously-prepared op.
type CommitRequest struct {
	TxnID string `json:"txn_id"`
}

// AbortRequest tells a participant to drop the previously-prepared op.
type AbortRequest struct {
	TxnID string `json:"txn_id"`
}

// Participant is the shard-side interface. The real implementation is
// backed by a Raft group; for testing we use an in-memory mock.
type Participant interface {
	Prepare(ctx context.Context, req *PrepareRequest) (*PrepareResponse, error)
	Commit(ctx context.Context, req *CommitRequest) error
	Abort(ctx context.Context, req *AbortRequest) error
}

// TxnStatus is the outcome of a transaction.
type TxnStatus string

const (
	StatusCommitted TxnStatus = "committed"
	StatusAborted   TxnStatus = "aborted"
	StatusPending   TxnStatus = "pending"
)

// TxnRecord is the coordinator's durable record of a transaction's
// outcome. After Commit/Abort, this is what survives a coordinator
// crash so participants can re-ask for the outcome.
type TxnRecord struct {
	TxnID       string    `json:"txn_id"`
	Ops         []Op      `json:"ops"`
	Status      TxnStatus `json:"status"`
	DecidedAt   time.Time `json:"decided_at"`
}

// Coordinator runs the 2PC protocol across one or more participants.
type Coordinator struct {
	mu             sync.Mutex
	participants   map[int]Participant // shardID -> Participant
	txnLog         map[string]*TxnRecord
	prepareTimeout time.Duration
}

// NewCoordinator constructs a Coordinator with the given shard map.
func NewCoordinator(participants map[int]Participant) *Coordinator {
	return &Coordinator{
		participants:   participants,
		txnLog:         make(map[string]*TxnRecord),
		prepareTimeout: 2 * time.Second,
	}
}

// SetPrepareTimeout overrides the default Prepare-phase timeout.
func (c *Coordinator) SetPrepareTimeout(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prepareTimeout = d
}

// Execute runs a 2PC transaction over the given operations.
//
// Atomicity guarantee: either all ops apply (Status=Committed) or none
// do (Status=Aborted). Returns the TxnRecord describing the outcome.
func (c *Coordinator) Execute(ctx context.Context, txnID string, ops []Op) (*TxnRecord, error) {
	if txnID == "" {
		return nil, errors.New("txn: empty transaction ID")
	}
	if len(ops) == 0 {
		return nil, errors.New("txn: empty operation list")
	}

	c.mu.Lock()
	if _, dup := c.txnLog[txnID]; dup {
		c.mu.Unlock()
		return nil, fmt.Errorf("txn: duplicate transaction ID %q", txnID)
	}
	record := &TxnRecord{TxnID: txnID, Ops: ops, Status: StatusPending}
	c.txnLog[txnID] = record
	timeout := c.prepareTimeout
	c.mu.Unlock()

	// Phase 1: PREPARE
	prepareCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type result struct {
		shardID int
		vote    Vote
		err     error
	}
	results := make(chan result, len(ops))

	for _, op := range ops {
		c.mu.Lock()
		p, ok := c.participants[op.ShardID]
		c.mu.Unlock()
		if !ok {
			results <- result{shardID: op.ShardID, vote: VoteNo,
				err: fmt.Errorf("unknown shard %d", op.ShardID)}
			continue
		}
		opCopy := op
		go func() {
			resp, err := p.Prepare(prepareCtx, &PrepareRequest{TxnID: txnID, Op: opCopy})
			if err != nil {
				results <- result{shardID: opCopy.ShardID, vote: VoteNo, err: err}
				return
			}
			results <- result{shardID: opCopy.ShardID, vote: resp.Vote}
		}()
	}

	allYes := true
	for i := 0; i < len(ops); i++ {
		r := <-results
		if r.vote != VoteYes {
			allYes = false
		}
	}

	// Phase 2: COMMIT or ABORT
	c.mu.Lock()
	if allYes {
		record.Status = StatusCommitted
	} else {
		record.Status = StatusAborted
	}
	record.DecidedAt = time.Now()
	c.mu.Unlock()

	c.broadcastPhase2(ctx, record)
	return record, nil
}

func (c *Coordinator) broadcastPhase2(ctx context.Context, record *TxnRecord) {
	var wg sync.WaitGroup
	for _, op := range record.Ops {
		c.mu.Lock()
		p, ok := c.participants[op.ShardID]
		c.mu.Unlock()
		if !ok {
			continue
		}
		wg.Add(1)
		go func(p Participant) {
			defer wg.Done()
			if record.Status == StatusCommitted {
				_ = p.Commit(ctx, &CommitRequest{TxnID: record.TxnID})
			} else {
				_ = p.Abort(ctx, &AbortRequest{TxnID: record.TxnID})
			}
		}(p)
	}
	wg.Wait()
}

// Status returns the recorded outcome of a transaction, or ("",
// false) if no such transaction is known to this coordinator.
func (c *Coordinator) Status(txnID string) (TxnStatus, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.txnLog[txnID]
	if !ok {
		return "", false
	}
	return r.Status, true
}

// Serialize emits the coordinator's transaction log as JSON; useful
// for crash-recovery testing.
func (c *Coordinator) Serialize() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return json.Marshal(c.txnLog)
}
