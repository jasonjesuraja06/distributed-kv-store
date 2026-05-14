package shard

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// Replica describes a single node within a shard.
type Replica struct {
	NodeID     string `json:"node_id"`
	RaftAddr   string `json:"raft_addr"`
	ClientAddr string `json:"client_addr"`
}

// Shard is one Raft group responsible for a portion of the keyspace.
type Shard struct {
	ID       int       `json:"id"`
	Replicas []Replica `json:"replicas"`
}

// Cluster is the full topology: N shards, each with M replicas. Stored as
// a JSON file consumed by both servers (to know their shard peers) and
// clients (to know how to route operations).
//
// Example cluster.json:
//
//	{
//	  "shards": [
//	    {"id": 0, "replicas": [
//	      {"node_id": "s0-a", "raft_addr": "localhost:6001", "client_addr": "localhost:8001"},
//	      {"node_id": "s0-b", "raft_addr": "localhost:6002", "client_addr": "localhost:8002"},
//	      {"node_id": "s0-c", "raft_addr": "localhost:6003", "client_addr": "localhost:8003"}
//	    ]},
//	    {"id": 1, "replicas": [
//	      {"node_id": "s1-a", "raft_addr": "localhost:6101", "client_addr": "localhost:8101"},
//	      {"node_id": "s1-b", "raft_addr": "localhost:6102", "client_addr": "localhost:8102"},
//	      {"node_id": "s1-c", "raft_addr": "localhost:6103", "client_addr": "localhost:8103"}
//	    ]}
//	  ]
//	}
type Cluster struct {
	Shards []Shard `json:"shards"`
}

// LoadCluster reads a cluster topology from a JSON file.
func LoadCluster(path string) (*Cluster, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cluster load: %w", err)
	}
	var c Cluster
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("cluster parse: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	c.sortShards()
	return &c, nil
}

// Save writes the cluster topology to JSON.
func (c *Cluster) Save(path string) error {
	c.sortShards()
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// Validate checks for duplicate node IDs, missing shards, and empty shards.
func (c *Cluster) Validate() error {
	if len(c.Shards) == 0 {
		return fmt.Errorf("cluster: at least one shard required")
	}
	seenNodes := map[string]bool{}
	seenShards := map[int]bool{}
	for _, sh := range c.Shards {
		if seenShards[sh.ID] {
			return fmt.Errorf("cluster: duplicate shard id %d", sh.ID)
		}
		seenShards[sh.ID] = true
		if len(sh.Replicas) == 0 {
			return fmt.Errorf("cluster: shard %d has no replicas", sh.ID)
		}
		for _, r := range sh.Replicas {
			if r.NodeID == "" {
				return fmt.Errorf("cluster: shard %d has replica with empty node_id", sh.ID)
			}
			if seenNodes[r.NodeID] {
				return fmt.Errorf("cluster: duplicate node id %s", r.NodeID)
			}
			seenNodes[r.NodeID] = true
		}
	}
	return nil
}

// NumShards returns the number of shards in the cluster.
func (c *Cluster) NumShards() int { return len(c.Shards) }

// ShardByID returns the shard with the given ID, or nil if not found.
func (c *Cluster) ShardByID(id int) *Shard {
	for i := range c.Shards {
		if c.Shards[i].ID == id {
			return &c.Shards[i]
		}
	}
	return nil
}

// FindReplica locates a node across all shards by its NodeID.
func (c *Cluster) FindReplica(nodeID string) (shardID int, replica *Replica) {
	for i := range c.Shards {
		for j := range c.Shards[i].Replicas {
			if c.Shards[i].Replicas[j].NodeID == nodeID {
				return c.Shards[i].ID, &c.Shards[i].Replicas[j]
			}
		}
	}
	return -1, nil
}

func (c *Cluster) sortShards() {
	sort.Slice(c.Shards, func(i, j int) bool {
		return c.Shards[i].ID < c.Shards[j].ID
	})
}
