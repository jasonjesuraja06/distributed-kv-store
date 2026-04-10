package tests

import (
	"fmt"
	"math"
	"testing"

	"github.com/jasonjesuraja06/distributed-kv-store/pkg/consistent"
)

func TestRing_Basic(t *testing.T) {
	ring := consistent.NewRing(100)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	if ring.NodeCount() != 3 {
		t.Fatalf("expected 3 nodes, got %d", ring.NodeCount())
	}

	// Same key should always map to the same node
	node1 := ring.GetNode("user:123")
	node2 := ring.GetNode("user:123")
	if node1 != node2 {
		t.Fatal("same key mapped to different nodes")
	}
}

func TestRing_Distribution(t *testing.T) {
	ring := consistent.NewRing(150)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	// Check that keys are reasonably distributed across nodes
	counts := map[string]int{"node-1": 0, "node-2": 0, "node-3": 0}
	numKeys := 10000

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := ring.GetNode(key)
		counts[node]++
	}

	// Each node should get roughly 1/3 of keys (within 15% tolerance)
	expected := float64(numKeys) / 3.0
	for node, count := range counts {
		deviation := math.Abs(float64(count)-expected) / expected
		if deviation > 0.15 {
			t.Errorf("node %s got %d keys (expected ~%d, deviation %.1f%%)",
				node, count, int(expected), deviation*100)
		}
	}
}

func TestRing_AddNodeMinimalDisruption(t *testing.T) {
	ring := consistent.NewRing(150)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	// Record initial mapping
	numKeys := 10000
	before := make(map[string]string)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		before[key] = ring.GetNode(key)
	}

	// Add a 4th node
	ring.AddNode("node-4")

	// Check how many keys moved
	moved := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		after := ring.GetNode(key)
		if before[key] != after {
			moved++
		}
	}

	// With consistent hashing, ~1/4 of keys should move (the new node's share)
	// Allow 10-40% movement range
	movePct := float64(moved) / float64(numKeys) * 100
	if movePct > 40 {
		t.Errorf("too many keys moved: %.1f%% (expected ~25%%)", movePct)
	}
	t.Logf("Keys moved after adding node-4: %d/%d (%.1f%%)", moved, numKeys, movePct)
}

func TestRing_RemoveNode(t *testing.T) {
	ring := consistent.NewRing(100)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	ring.RemoveNode("node-2")

	if ring.NodeCount() != 2 {
		t.Fatalf("expected 2 nodes, got %d", ring.NodeCount())
	}

	// All keys should now map to node-1 or node-3
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := ring.GetNode(key)
		if node != "node-1" && node != "node-3" {
			t.Fatalf("key mapped to removed node: %s", node)
		}
	}
}

func TestRing_GetNodes(t *testing.T) {
	ring := consistent.NewRing(100)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	nodes := ring.GetNodes("mykey", 2)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}

	// Should be distinct nodes
	if nodes[0] == nodes[1] {
		t.Fatal("GetNodes returned duplicate nodes")
	}
}

func TestRing_EmptyRing(t *testing.T) {
	ring := consistent.NewRing(100)
	node := ring.GetNode("key")
	if node != "" {
		t.Fatalf("expected empty string for empty ring, got '%s'", node)
	}
}
