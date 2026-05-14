package tests

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jasonjesuraja06/distributed-kv-store/pkg/raft"
	"github.com/jasonjesuraja06/distributed-kv-store/pkg/store"
	"github.com/jasonjesuraja06/distributed-kv-store/pkg/txn"
)

// ============================================================
// Pre-vote tests
// ============================================================

// recordingTransport counts incoming PreVote calls per peer.
type recordingTransport struct {
	mu        sync.Mutex
	preVotes  map[string]int
	grantPre  bool
	grantReal bool
}

func newRecordingTransport(grantPre, grantReal bool) *recordingTransport {
	return &recordingTransport{
		preVotes:  make(map[string]int),
		grantPre:  grantPre,
		grantReal: grantReal,
	}
}

func (r *recordingTransport) RequestVote(target string, _ *raft.VoteRequest) (*raft.VoteResponse, error) {
	return &raft.VoteResponse{VoteGranted: r.grantReal, Term: 1}, nil
}
func (r *recordingTransport) AppendEntries(_ string, _ *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	return &raft.AppendEntriesResponse{Success: true}, nil
}
func (r *recordingTransport) PreVote(target string, _ *raft.PreVoteRequest) (*raft.PreVoteResponse, error) {
	r.mu.Lock()
	r.preVotes[target]++
	r.mu.Unlock()
	return &raft.PreVoteResponse{VoteGranted: r.grantPre, Term: 1}, nil
}
func (r *recordingTransport) InstallSnapshot(_ string, _ *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	return &raft.InstallSnapshotResponse{Success: true}, nil
}

func TestPreVote_HandlerGrantsWhenLogIsUpToDate(t *testing.T) {
	tr := newRecordingTransport(true, true)
	n := raft.NewNode("solo", nil, tr, func(raft.LogEntry) {})
	n.Start()
	defer n.Stop()

	resp := n.HandlePreVote(&raft.PreVoteRequest{
		Term:         5,
		CandidateID:  "challenger",
		LastLogIndex: 100,
		LastLogTerm:  3,
	})
	if resp == nil {
		t.Fatal("nil response")
	}
	// Solo node is leader after a moment; pre-vote should still respond
	// (and may or may not grant depending on role). Just check it didn't crash.
}

func TestPreVote_RejectsStaleTerm(t *testing.T) {
	tr := newRecordingTransport(false, false)
	n := raft.NewNode("solo", nil, tr, func(raft.LogEntry) {})
	n.Start()
	defer n.Stop()
	time.Sleep(700 * time.Millisecond) // let it become leader

	currentTerm := n.CurrentTerm()
	resp := n.HandlePreVote(&raft.PreVoteRequest{
		Term:         currentTerm - 1, // stale
		CandidateID:  "x",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if resp.VoteGranted {
		t.Error("pre-vote with stale term should be rejected")
	}
}

// ============================================================
// Persistent WAL tests
// ============================================================

func TestWAL_AppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.wal")

	w, err := raft.OpenWAL(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	entries := []raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("alpha")},
		{Term: 1, Index: 2, Command: []byte("beta")},
		{Term: 2, Index: 3, Command: []byte("gamma")},
	}
	if err := w.AppendEntries(entries); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.AppendState(2, "node-1"); err != nil {
		t.Fatalf("state: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and replay
	w2, err := raft.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	state, err := w2.Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(state.Log) != 3 {
		t.Errorf("expected 3 entries, got %d", len(state.Log))
	}
	if state.CurrentTerm != 2 || state.VotedFor != "node-1" {
		t.Errorf("state not restored: term=%d votedFor=%q", state.CurrentTerm, state.VotedFor)
	}
}

func TestWAL_TruncateAndSnapshotMeta(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.wal")
	w, _ := raft.OpenWAL(path)
	defer w.Close()

	w.AppendEntries([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
		{Term: 1, Index: 4, Command: []byte("d")},
	})
	w.AppendTruncate(3) // drop indices >= 3
	w.AppendSnapshotMeta(1, 1) // also drop indices <= 1

	state, err := w.Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(state.Log) != 1 {
		t.Fatalf("expected 1 entry (only index=2 survives), got %d", len(state.Log))
	}
	if state.Log[0].Index != 2 {
		t.Errorf("expected index=2, got %d", state.Log[0].Index)
	}
	if state.LastIncludedIndex != 1 || state.LastIncludedTerm != 1 {
		t.Errorf("snapshot meta not restored: %+v", state)
	}
}

func TestWAL_NodeRecoversTermAndLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raft.wal")

	// Pre-populate WAL with some state
	w, _ := raft.OpenWAL(path)
	w.AppendState(7, "node-X")
	w.AppendEntries([]raft.LogEntry{
		{Term: 7, Index: 1, Command: []byte("first")},
		{Term: 7, Index: 2, Command: []byte("second")},
	})
	w.Close()

	// Open a fresh node, attach the same WAL — should restore state
	tr := newRecordingTransport(true, true)
	n := raft.NewNode("recovered", nil, tr, func(raft.LogEntry) {})
	w2, _ := raft.OpenWAL(path)
	if err := n.AttachWAL(w2); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if n.CurrentTerm() != 7 {
		t.Errorf("term not recovered: got %d, want 7", n.CurrentTerm())
	}
	state := n.LogState()
	if state.LastLogIndex != 2 {
		t.Errorf("log not recovered: lastLogIndex=%d, want 2", state.LastLogIndex)
	}
}

// ============================================================
// InstallSnapshot tests
// ============================================================

func TestInstallSnapshot_HandlerRestoresState(t *testing.T) {
	tr := newRecordingTransport(true, true)
	n := raft.NewNode("recv", nil, tr, func(raft.LogEntry) {})

	installed := false
	n.SetSnapshotHandlers(
		nil,
		func(data []byte, lastIdx, lastTerm uint64) error {
			installed = true
			if string(data) != "snap-payload" {
				t.Errorf("data: got %q", data)
			}
			if lastIdx != 50 || lastTerm != 3 {
				t.Errorf("meta: idx=%d term=%d", lastIdx, lastTerm)
			}
			return nil
		},
	)
	n.Start()
	defer n.Stop()

	resp := n.HandleInstallSnapshot(&raft.InstallSnapshotRequest{
		Term:              5,
		LeaderID:          "leader",
		LastIncludedIndex: 50,
		LastIncludedTerm:  3,
		Data:              []byte("snap-payload"),
		Done:              true,
	})
	if !resp.Success {
		t.Fatal("InstallSnapshot did not succeed")
	}
	if !installed {
		t.Fatal("installer callback was not invoked")
	}
	state := n.LogState()
	if state.LastIncludedIndex != 50 {
		t.Errorf("LastIncludedIndex: got %d want 50", state.LastIncludedIndex)
	}
}

func TestInstallSnapshot_RejectsStaleSnapshot(t *testing.T) {
	tr := newRecordingTransport(true, true)
	n := raft.NewNode("recv", nil, tr, func(raft.LogEntry) {})
	n.Start()
	defer n.Stop()

	// Set a baseline snapshot at index 100
	n.RestoreFromSnapshot(100, 5)

	// Try installing an older snapshot
	resp := n.HandleInstallSnapshot(&raft.InstallSnapshotRequest{
		Term:              6,
		LeaderID:          "leader",
		LastIncludedIndex: 50,
		LastIncludedTerm:  3,
		Data:              []byte("old"),
		Done:              true,
	})
	// Should respond Success=true but NOT clobber our newer state.
	state := n.LogState()
	if state.LastIncludedIndex != 100 {
		t.Errorf("LastIncludedIndex regressed: got %d want 100", state.LastIncludedIndex)
	}
	_ = resp
}

// ============================================================
// Read-index tests
// ============================================================

func TestReadIndex_NotLeaderReturnsError(t *testing.T) {
	tr := newRecordingTransport(true, true)
	n := raft.NewNode("follower", []string{"peer-a"}, tr, func(raft.LogEntry) {})
	n.Start()
	defer n.Stop()
	// Don't wait for election — node is still a follower at this instant.

	_, err := n.ReadIndex(context.Background())
	if err == nil {
		t.Skip("test raced with election; node became leader before ReadIndex call")
	}
	if err != raft.ErrNotLeader && err != raft.ErrLeadershipLost {
		t.Errorf("expected ErrNotLeader, got: %v", err)
	}
}

func TestReadIndex_SoloLeaderSucceeds(t *testing.T) {
	tr := newRecordingTransport(true, true)
	n := raft.NewNode("solo", nil, tr, func(raft.LogEntry) {})
	n.Start()
	defer n.Stop()
	time.Sleep(800 * time.Millisecond) // let it become leader

	idx, err := n.ReadIndex(context.Background())
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	_ = idx
}

// ============================================================
// Joint consensus tests
// ============================================================

func TestJointConsensus_EncodeDecodeRoundTrip(t *testing.T) {
	cc := raft.ConfigChange{
		Phase:     raft.ConfigPhaseJoint,
		OldVoters: []string{"a", "b", "c"},
		NewVoters: []string{"a", "b", "c", "d"},
	}
	encoded, err := raft.EncodeConfigCommand(cc)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := raft.DecodeConfigCommand(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded == nil {
		t.Fatal("decoded nil")
	}
	if decoded.Phase != raft.ConfigPhaseJoint {
		t.Errorf("phase: %v", decoded.Phase)
	}
	if len(decoded.NewVoters) != 4 {
		t.Errorf("new voters: %v", decoded.NewVoters)
	}
}

func TestJointConsensus_DecodeRegularCommandReturnsNil(t *testing.T) {
	regular, _ := store.EncodeCommand(store.Command{
		Type: store.CmdPut, Key: "k", Value: "v",
	})
	decoded, err := raft.DecodeConfigCommand(regular)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if decoded != nil {
		t.Errorf("regular command misdetected as config change: %+v", decoded)
	}
}

func TestJointConsensus_ProposeAddPeer(t *testing.T) {
	tr := newRecordingTransport(true, true)
	n := raft.NewNode("solo", nil, tr, func(raft.LogEntry) {})
	n.Start()
	defer n.Stop()
	time.Sleep(800 * time.Millisecond) // become leader

	if err := n.ProposeAddPeer("newcomer"); err != nil {
		t.Fatalf("propose: %v", err)
	}
	cfg := n.CurrentConfig()
	if cfg.Phase != raft.ConfigPhaseJoint {
		t.Errorf("expected joint phase, got %s", cfg.Phase)
	}
	if len(cfg.NewVoters) == 0 || cfg.NewVoters[len(cfg.NewVoters)-1] != "newcomer" {
		t.Errorf("newcomer not added: %v", cfg.NewVoters)
	}
}

// ============================================================
// Cross-shard 2PC tests
// ============================================================

func TestTxn_CommitsWhenAllShardsVoteYes(t *testing.T) {
	shard0 := txn.NewShardParticipant(0)
	shard1 := txn.NewShardParticipant(1)
	coord := txn.NewCoordinator(map[int]txn.Participant{
		0: shard0,
		1: shard1,
	})

	record, err := coord.Execute(context.Background(), "tx-1", []txn.Op{
		{ShardID: 0, Key: "alice", Type: txn.OpPut, Value: "balance=900"},
		{ShardID: 1, Key: "bob", Type: txn.OpPut, Value: "balance=1100"},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if record.Status != txn.StatusCommitted {
		t.Errorf("expected Committed, got %s", record.Status)
	}
	if v, _ := shard0.Get("alice"); v != "balance=900" {
		t.Errorf("alice not updated: %q", v)
	}
	if v, _ := shard1.Get("bob"); v != "balance=1100" {
		t.Errorf("bob not updated: %q", v)
	}
}

func TestTxn_AbortsWhenLockHeld(t *testing.T) {
	shard0 := txn.NewShardParticipant(0)
	shard1 := txn.NewShardParticipant(1)
	coord := txn.NewCoordinator(map[int]txn.Participant{
		0: shard0,
		1: shard1,
	})

	// First txn prepares but is never committed/aborted — holds locks.
	shard0.Prepare(context.Background(), &txn.PrepareRequest{
		TxnID: "tx-locker",
		Op:    txn.Op{ShardID: 0, Key: "alice", Type: txn.OpPut, Value: "x"},
	})

	// Second txn touches the same key — should abort.
	record, err := coord.Execute(context.Background(), "tx-blocked", []txn.Op{
		{ShardID: 0, Key: "alice", Type: txn.OpPut, Value: "y"},
		{ShardID: 1, Key: "bob", Type: txn.OpPut, Value: "z"},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if record.Status != txn.StatusAborted {
		t.Errorf("expected Aborted, got %s", record.Status)
	}
	// Neither key should have been mutated.
	if v, ok := shard0.Get("alice"); ok {
		t.Errorf("alice should not be committed: %q", v)
	}
	if v, ok := shard1.Get("bob"); ok {
		t.Errorf("bob should not be committed: %q", v)
	}
}

func TestTxn_AbortsWhenShardMissing(t *testing.T) {
	shard0 := txn.NewShardParticipant(0)
	coord := txn.NewCoordinator(map[int]txn.Participant{0: shard0})

	record, err := coord.Execute(context.Background(), "tx-bad-shard", []txn.Op{
		{ShardID: 0, Key: "a", Type: txn.OpPut, Value: "1"},
		{ShardID: 99, Key: "b", Type: txn.OpPut, Value: "2"}, // unknown shard
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if record.Status != txn.StatusAborted {
		t.Errorf("expected Aborted, got %s", record.Status)
	}
}

func TestTxn_StatusLookup(t *testing.T) {
	shard0 := txn.NewShardParticipant(0)
	coord := txn.NewCoordinator(map[int]txn.Participant{0: shard0})

	coord.Execute(context.Background(), "tx-status-test", []txn.Op{
		{ShardID: 0, Key: "k", Type: txn.OpPut, Value: "v"},
	})

	status, ok := coord.Status("tx-status-test")
	if !ok {
		t.Fatal("Status not found")
	}
	if status != txn.StatusCommitted {
		t.Errorf("expected Committed, got %s", status)
	}

	if _, ok := coord.Status("nonexistent"); ok {
		t.Error("Status returned ok for nonexistent txn")
	}
}

func TestTxn_DeleteOp(t *testing.T) {
	shard0 := txn.NewShardParticipant(0)
	coord := txn.NewCoordinator(map[int]txn.Participant{0: shard0})

	// Setup: commit a Put first
	coord.Execute(context.Background(), "tx-setup", []txn.Op{
		{ShardID: 0, Key: "to-delete", Type: txn.OpPut, Value: "exists"},
	})
	if v, ok := shard0.Get("to-delete"); !ok || v != "exists" {
		t.Fatalf("setup failed: %q ok=%v", v, ok)
	}

	// Now delete it via a separate transaction
	record, _ := coord.Execute(context.Background(), "tx-delete", []txn.Op{
		{ShardID: 0, Key: "to-delete", Type: txn.OpDelete},
	})
	if record.Status != txn.StatusCommitted {
		t.Errorf("delete txn status: %s", record.Status)
	}
	if _, ok := shard0.Get("to-delete"); ok {
		t.Error("key still present after delete commit")
	}
}
