package config

import "time"

// NodeConfig holds configuration for a single node in the cluster.
type NodeConfig struct {
	ID      string // Unique identifier for this node (e.g., "node-1")
	Address string // gRPC listen address (e.g., "localhost:5001")
}

// ClusterConfig holds configuration for the entire cluster.
type ClusterConfig struct {
	Nodes              []NodeConfig  // All nodes in the cluster
	ElectionTimeoutMin time.Duration // Min election timeout (randomized between min and max)
	ElectionTimeoutMax time.Duration // Max election timeout
	HeartbeatInterval  time.Duration // How often the leader sends heartbeats
	DataDir            string        // Directory for persistent storage
}

// DefaultConfig returns a reasonable default cluster configuration.
func DefaultConfig() ClusterConfig {
	return ClusterConfig{
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 500 * time.Millisecond,
		HeartbeatInterval:  100 * time.Millisecond,
		DataDir:            "./data",
	}
}
