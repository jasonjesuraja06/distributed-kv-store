package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/jasonjesuraja06/distributed-kv-store/pkg/raft"
	"github.com/jasonjesuraja06/distributed-kv-store/pkg/snapshot"
	"github.com/jasonjesuraja06/distributed-kv-store/pkg/store"
)

// noopTransport satisfies the raft.Transport interface without doing anything,
// letting us test single-node compaction logic in isolation.
type noopTransport struct{}

func (noopTransport) RequestVote(string, *raft.VoteRequest) (*raft.VoteResponse, error) {
	return &raft.VoteResponse{}, nil
}
func (noopTransport) AppendEntries(string, *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	return &raft.AppendEntriesResponse{}, nil
}

// applyAllEntries waits until the apply callback has processed at least
// `n` total entries.
func applyAllEntries(t *testing.T, applied *uint64, mu *sync.Mutex, n uint64, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		mu.Lock()
		ok := *applied >= n
		mu.Unlock()
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d applies (saw %d)", n, *applied)
}

// startSoloLeader starts a single-node Raft cluster (peers=[]) and waits
// for it to elect itself.
func startSoloLeader(t *testing.T, applyFn raft.ApplyFunc) *raft.Node {
	t.Helper()
	node := raft.NewNode("solo", nil, noopTransport{}, applyFn)
	node.Start()
	// Wait for election (300-500ms randomized timeout)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if node.CurrentRole() == raft.Leader {
			return node
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("solo node failed to become leader within timeout")
	return nil
}

func TestRaft_CreateSnapshotCompactsLog(t *testing.T) {
	kvStore := store.NewKVStore()
	var mu sync.Mutex
	var applied uint64

	applyFn := func(e raft.LogEntry) {
		_ = kvStore.Apply(e.Command)
		mu.Lock()
		applied++
		mu.Unlock()
	}
	node := startSoloLeader(t, applyFn)
	defer node.Stop()

	// Propose 100 entries
	for i := 0; i < 100; i++ {
		cmd, _ := store.EncodeCommand(store.Command{
			Type: store.CmdPut, Key: "k", Value: "v",
		})
		if !node.Propose(cmd) {
			t.Fatalf("propose %d failed", i)
		}
	}
	applyAllEntries(t, &applied, &mu, 100, 2*time.Second)

	beforeState := node.LogState()
	if beforeState.LogEntries < 100 {
		t.Fatalf("expected ≥100 entries before snapshot, got %d", beforeState.LogEntries)
	}

	// Snapshot at index 50
	if err := node.CreateSnapshot(50); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	afterState := node.LogState()
	if afterState.LastIncludedIndex != 50 {
		t.Errorf("LastIncludedIndex: got %d want 50", afterState.LastIncludedIndex)
	}
	if afterState.LogEntries >= beforeState.LogEntries {
		t.Errorf("log should have shrunk: before=%d after=%d",
			beforeState.LogEntries, afterState.LogEntries)
	}
	if afterState.LastLogIndex < 100 {
		t.Errorf("LastLogIndex should still be ≥100 after compaction, got %d",
			afterState.LastLogIndex)
	}
}

func TestRaft_CreateSnapshotRejectsUncommittedIndex(t *testing.T) {
	node := startSoloLeader(t, func(raft.LogEntry) {})
	defer node.Stop()

	// Index 99 is way beyond what's been proposed.
	if err := node.CreateSnapshot(99); err == nil {
		t.Error("expected error for uncommitted index")
	}
}

func TestRaft_RestoreFromSnapshotAdjustsState(t *testing.T) {
	node := startSoloLeader(t, func(raft.LogEntry) {})
	defer node.Stop()

	node.RestoreFromSnapshot(42, 3)
	state := node.LogState()
	if state.LastIncludedIndex != 42 {
		t.Errorf("LastIncludedIndex: got %d want 42", state.LastIncludedIndex)
	}
	if state.LastIncludedTerm != 3 {
		t.Errorf("LastIncludedTerm: got %d want 3", state.LastIncludedTerm)
	}
	if state.LogEntries != 0 {
		t.Errorf("Log should be empty after restore, got %d entries", state.LogEntries)
	}
}

func TestSnapshotManager_TakesSnapshotPastThreshold(t *testing.T) {
	kvStore := store.NewKVStore()
	var mu sync.Mutex
	var applied uint64

	applyFn := func(e raft.LogEntry) {
		_ = kvStore.Apply(e.Command)
		mu.Lock()
		applied++
		mu.Unlock()
	}
	node := startSoloLeader(t, applyFn)
	defer node.Stop()

	dir := t.TempDir()
	path := dir + "/snap.json"

	// Threshold = 10; interval doesn't matter (we'll call MaybeSnapshot manually)
	mgr := snapshot.NewManager(kvStore, node, path, 10, time.Hour)

	// Propose 50 entries
	for i := 0; i < 50; i++ {
		cmd, _ := store.EncodeCommand(store.Command{
			Type: store.CmdPut, Key: "k", Value: "v",
		})
		if !node.Propose(cmd) {
			t.Fatalf("propose %d failed", i)
		}
	}
	applyAllEntries(t, &applied, &mu, 50, 2*time.Second)

	if err := mgr.MaybeSnapshot(); err != nil {
		t.Fatalf("MaybeSnapshot: %v", err)
	}

	stats := mgr.Stats()
	if stats.SnapshotsTaken != 1 {
		t.Errorf("expected 1 snapshot, got %d", stats.SnapshotsTaken)
	}
	if stats.LastSnapshotIndex == 0 {
		t.Error("LastSnapshotIndex should be set")
	}

	// Verify the file exists on disk
	if _, err := snapshot.Load(path); err != nil {
		t.Errorf("snapshot file not loadable: %v", err)
	}
}

func TestSnapshotManager_RestoreFromDisk(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/snap.json"

	// Pre-populate a snapshot on disk
	s := snapshot.New()
	s.LastIncludedIndex = 100
	s.LastIncludedTerm = 5
	s.Data["preloaded-key"] = "preloaded-value"
	if err := s.Save(path); err != nil {
		t.Fatal(err)
	}

	// Fresh store + raft node — should pick up the snapshot
	kvStore := store.NewKVStore()
	node := startSoloLeader(t, func(raft.LogEntry) {})
	defer node.Stop()

	mgr := snapshot.NewManager(kvStore, node, path, 1000, time.Hour)
	loaded, err := mgr.LoadAndRestore()
	if err != nil {
		t.Fatalf("LoadAndRestore: %v", err)
	}
	if !loaded {
		t.Fatal("expected loaded=true")
	}
	if v, ok := kvStore.Get("preloaded-key"); !ok || v != "preloaded-value" {
		t.Errorf("state machine not restored: %q ok=%v", v, ok)
	}
	state := node.LogState()
	if state.LastIncludedIndex != 100 || state.LastIncludedTerm != 5 {
		t.Errorf("raft state not restored: %+v", state)
	}
}

func TestSnapshotManager_NoOpUnderThreshold(t *testing.T) {
	kvStore := store.NewKVStore()
	var mu sync.Mutex
	var applied uint64
	applyFn := func(e raft.LogEntry) {
		_ = kvStore.Apply(e.Command)
		mu.Lock()
		applied++
		mu.Unlock()
	}
	node := startSoloLeader(t, applyFn)
	defer node.Stop()

	dir := t.TempDir()
	mgr := snapshot.NewManager(kvStore, node, dir+"/s.json", 1000, time.Hour)

	for i := 0; i < 5; i++ {
		cmd, _ := store.EncodeCommand(store.Command{
			Type: store.CmdPut, Key: "k", Value: "v",
		})
		node.Propose(cmd)
	}
	applyAllEntries(t, &applied, &mu, 5, 2*time.Second)

	if err := mgr.MaybeSnapshot(); err != nil {
		t.Fatal(err)
	}
	if got := mgr.Stats().SnapshotsTaken; got != 0 {
		t.Errorf("expected 0 snapshots under threshold, got %d", got)
	}
}
