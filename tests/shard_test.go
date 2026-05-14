package tests

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/jasonjesuraja06/distributed-kv-store/pkg/shard"
)

func sampleCluster() *shard.Cluster {
	return &shard.Cluster{
		Shards: []shard.Shard{
			{ID: 0, Replicas: []shard.Replica{
				{NodeID: "s0-a", RaftAddr: "localhost:6001", ClientAddr: "localhost:8001"},
				{NodeID: "s0-b", RaftAddr: "localhost:6002", ClientAddr: "localhost:8002"},
				{NodeID: "s0-c", RaftAddr: "localhost:6003", ClientAddr: "localhost:8003"},
			}},
			{ID: 1, Replicas: []shard.Replica{
				{NodeID: "s1-a", RaftAddr: "localhost:6101", ClientAddr: "localhost:8101"},
				{NodeID: "s1-b", RaftAddr: "localhost:6102", ClientAddr: "localhost:8102"},
				{NodeID: "s1-c", RaftAddr: "localhost:6103", ClientAddr: "localhost:8103"},
			}},
			{ID: 2, Replicas: []shard.Replica{
				{NodeID: "s2-a", RaftAddr: "localhost:6201", ClientAddr: "localhost:8201"},
				{NodeID: "s2-b", RaftAddr: "localhost:6202", ClientAddr: "localhost:8202"},
				{NodeID: "s2-c", RaftAddr: "localhost:6203", ClientAddr: "localhost:8203"},
			}},
		},
	}
}

func TestCluster_ValidateAcceptsGoodConfig(t *testing.T) {
	if err := sampleCluster().Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestCluster_ValidateRejectsDuplicateNodeID(t *testing.T) {
	c := sampleCluster()
	c.Shards[1].Replicas[0].NodeID = "s0-a"
	if err := c.Validate(); err == nil {
		t.Error("expected duplicate node id error")
	}
}

func TestCluster_ValidateRejectsDuplicateShardID(t *testing.T) {
	c := sampleCluster()
	c.Shards[1].ID = 0
	if err := c.Validate(); err == nil {
		t.Error("expected duplicate shard id error")
	}
}

func TestCluster_ValidateRejectsEmpty(t *testing.T) {
	c := &shard.Cluster{}
	if err := c.Validate(); err == nil {
		t.Error("expected error for empty cluster")
	}
}

func TestCluster_SaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cluster.json")
	if err := sampleCluster().Save(path); err != nil {
		t.Fatalf("save: %v", err)
	}
	loaded, err := shard.LoadCluster(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.NumShards() != 3 {
		t.Errorf("NumShards: got %d want 3", loaded.NumShards())
	}
	if loaded.ShardByID(1).Replicas[0].NodeID != "s1-a" {
		t.Errorf("unexpected replica id: %s", loaded.ShardByID(1).Replicas[0].NodeID)
	}
}

func TestCluster_FindReplica(t *testing.T) {
	c := sampleCluster()
	id, r := c.FindReplica("s1-b")
	if id != 1 || r == nil || r.ClientAddr != "localhost:8102" {
		t.Errorf("FindReplica: got id=%d replica=%+v", id, r)
	}
	id, r = c.FindReplica("nonexistent")
	if id != -1 || r != nil {
		t.Errorf("expected nil for missing node, got id=%d r=%+v", id, r)
	}
}

func TestCluster_LoadInvalidJSONFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	os.WriteFile(path, []byte("{not json"), 0o644)
	if _, err := shard.LoadCluster(path); err == nil {
		t.Error("expected parse error")
	}
}

func TestRouter_DeterministicRouting(t *testing.T) {
	c := sampleCluster()
	r1 := shard.NewRouter(c, 150)
	r2 := shard.NewRouter(c, 150)

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		s1 := r1.ShardFor(key)
		s2 := r2.ShardFor(key)
		if s1 != s2 {
			t.Fatalf("non-deterministic routing for %q: r1=%d r2=%d", key, s1, s2)
		}
		if s1 < 0 || s1 > 2 {
			t.Errorf("invalid shard id for %q: %d", key, s1)
		}
	}
}

func TestRouter_DistributionIsBalanced(t *testing.T) {
	c := sampleCluster()
	r := shard.NewRouter(c, 150)

	keys := make([]string, 100_000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	stats := r.Distribution(keys)

	// With 150 virtual nodes per shard the distribution is good but not
	// perfectly uniform; ±10% is the realistic bound documented in TiKV
	// and DynamoDB design papers.
	expected := float64(len(keys)) / 3.0
	for shID, count := range stats.ShardCounts {
		dev := math.Abs(float64(count)-expected) / expected
		if dev > 0.10 {
			t.Errorf("shard %d: %d keys (%.2f%% deviation from balanced — expected <10%%)",
				shID, count, dev*100)
		}
		t.Logf("shard %d: %d keys (%.2f%% of total, %.2f%% deviation from ideal)",
			shID, count, 100*float64(count)/float64(len(keys)), dev*100)
	}
}

func TestRouter_ReplicasForKeyReturnsShardMembers(t *testing.T) {
	c := sampleCluster()
	r := shard.NewRouter(c, 150)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test-%d", i)
		shID := r.ShardFor(key)
		reps := r.ReplicasFor(key)
		if len(reps) != 3 {
			t.Errorf("expected 3 replicas for key %q, got %d", key, len(reps))
		}
		expected := c.ShardByID(shID)
		for j, rep := range reps {
			if rep.NodeID != expected.Replicas[j].NodeID {
				t.Errorf("replica mismatch shard=%d key=%q j=%d: got %s want %s",
					shID, key, j, rep.NodeID, expected.Replicas[j].NodeID)
			}
		}
	}
}

func TestRouter_AddingShardMovesMinimalKeys(t *testing.T) {
	c1 := sampleCluster()
	r1 := shard.NewRouter(c1, 150)

	// Snapshot routing for 100K keys under the 3-shard topology
	const n = 100_000
	before := make(map[string]int, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k-%d", i)
		before[key] = r1.ShardFor(key)
	}

	// Add a 4th shard
	c2 := sampleCluster()
	c2.Shards = append(c2.Shards, shard.Shard{
		ID: 3, Replicas: []shard.Replica{
			{NodeID: "s3-a", RaftAddr: "localhost:6301", ClientAddr: "localhost:8301"},
		},
	})
	r2 := shard.NewRouter(c2, 150)

	moved := 0
	for k, oldShard := range before {
		if r2.ShardFor(k) != oldShard {
			moved++
		}
	}
	movedPct := 100.0 * float64(moved) / float64(n)
	// Optimum is ~25% (1/4). Consistent hashing should achieve close to this.
	if movedPct > 35.0 || movedPct < 15.0 {
		t.Errorf("expected ~25%% key movement, got %.2f%%", movedPct)
	}
	t.Logf("key movement on shard add (3→4): %.2f%% (theoretical optimum: 25%%)", movedPct)
}

func TestCluster_JSONFormatStable(t *testing.T) {
	c := sampleCluster()
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Errorf("output is not valid JSON: %v", err)
	}
	if _, ok := raw["shards"]; !ok {
		t.Error("expected top-level 'shards' field")
	}
}
