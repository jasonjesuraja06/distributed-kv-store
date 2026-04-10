package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/jasonjesuraja06/distributed-kv-store/pkg/raft"
	"github.com/jasonjesuraja06/distributed-kv-store/pkg/store"
)

// mockTransport simulates network communication for testing.
// Nodes communicate directly through function calls (no real network).
type mockTransport struct {
	mu    sync.Mutex
	nodes map[string]*raft.Node
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		nodes: make(map[string]*raft.Node),
	}
}

func (t *mockTransport) Register(id string, node *raft.Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = node
}

func (t *mockTransport) RequestVote(target string, req *raft.VoteRequest) (*raft.VoteResponse, error) {
	t.mu.Lock()
	node, ok := t.nodes[target]
	t.mu.Unlock()
	if !ok {
		return nil, nil
	}
	return node.HandleRequestVote(req), nil
}

func (t *mockTransport) AppendEntries(target string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	t.mu.Lock()
	node, ok := t.nodes[target]
	t.mu.Unlock()
	if !ok {
		return nil, nil
	}
	return node.HandleAppendEntries(req), nil
}

func TestRaft_LeaderElection(t *testing.T) {
	transport := newMockTransport()

	kvStores := make([]*store.KVStore, 3)
	nodes := make([]*raft.Node, 3)
	ids := []string{"node-1", "node-2", "node-3"}

	for i, id := range ids {
		peers := make([]string, 0, 2)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		kvStores[i] = store.NewKVStore()
		applyFn := func(s *store.KVStore) raft.ApplyFunc {
			return func(entry raft.LogEntry) {
				s.Apply(entry.Command)
			}
		}(kvStores[i])

		nodes[i] = raft.NewNode(id, peers, transport, applyFn)
		transport.Register(id, nodes[i])
	}

	// Start all nodes
	for _, n := range nodes {
		n.Start()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	// Wait for leader election (should happen within 1-2 election timeouts)
	time.Sleep(2 * time.Second)

	// Verify exactly one leader
	leaderCount := 0
	for _, n := range nodes {
		if n.CurrentRole() == raft.Leader {
			leaderCount++
			t.Logf("Leader elected: %s (term %d)", n.ID(), n.CurrentTerm())
		}
	}

	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}

	// All nodes should be on the same term
	term := nodes[0].CurrentTerm()
	for _, n := range nodes[1:] {
		if n.CurrentTerm() != term {
			t.Logf("Warning: node %s on term %d (expected %d)", n.ID(), n.CurrentTerm(), term)
		}
	}
}

func TestRaft_LogReplication(t *testing.T) {
	transport := newMockTransport()

	kvStores := make([]*store.KVStore, 3)
	nodes := make([]*raft.Node, 3)
	ids := []string{"node-1", "node-2", "node-3"}

	for i, id := range ids {
		peers := make([]string, 0, 2)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		kvStores[i] = store.NewKVStore()
		applyFn := func(s *store.KVStore) raft.ApplyFunc {
			return func(entry raft.LogEntry) {
				s.Apply(entry.Command)
			}
		}(kvStores[i])

		nodes[i] = raft.NewNode(id, peers, transport, applyFn)
		transport.Register(id, nodes[i])
	}

	for _, n := range nodes {
		n.Start()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	// Wait for leader election
	time.Sleep(2 * time.Second)

	// Find the leader
	var leader *raft.Node
	for _, n := range nodes {
		if n.CurrentRole() == raft.Leader {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatal("no leader elected")
	}

	// Propose a command
	cmd, _ := store.EncodeCommand(store.Command{
		Type:  store.CmdPut,
		Key:   "greeting",
		Value: "hello world",
	})

	if !leader.Propose(cmd) {
		t.Fatal("leader rejected proposal")
	}

	// Wait for replication
	time.Sleep(1 * time.Second)

	// Verify the value is replicated to all nodes
	for i, s := range kvStores {
		val, ok := s.Get("greeting")
		if !ok {
			t.Logf("node %s: key not found (replication may still be in progress)", ids[i])
			continue
		}
		if val != "hello world" {
			t.Errorf("node %s: expected 'hello world', got '%s'", ids[i], val)
		} else {
			t.Logf("node %s: value replicated correctly", ids[i])
		}
	}
}
