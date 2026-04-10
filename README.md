# Distributed Key-Value Store

A fault-tolerant distributed key-value store built from scratch in Go, featuring Raft consensus for leader election and log replication, consistent hashing for key partitioning, and an HTTP-based transport layer for node-to-node communication.

## Architecture

```
                    ┌─────────────────┐
                    │   Client CLI    │
                    │  put/get/delete │
                    └────────┬────────┘
                             │ HTTP REST
              ┌──────────────┼──────────────┐
              │              │              │
        ┌─────▼─────┐ ┌─────▼─────┐ ┌─────▼─────┐
        │  Node 1   │ │  Node 2   │ │  Node 3   │
        │  (Leader) │ │ (Follower)│ │ (Follower)│
        │           │ │           │ │           │
        │ ┌───────┐ │ │ ┌───────┐ │ │ ┌───────┐ │
        │ │ Raft  │◄├─┤►│ Raft  │◄├─┤►│ Raft  │ │
        │ └───┬───┘ │ │ └───┬───┘ │ │ └───┬───┘ │
        │     │     │ │     │     │ │     │     │
        │ ┌───▼───┐ │ │ ┌───▼───┐ │ │ ┌───▼───┐ │
        │ │  KV   │ │ │ │  KV   │ │ │ │  KV   │ │
        │ │ Store │ │ │ │ Store │ │ │ │ Store │ │
        │ └───────┘ │ │ └───────┘ │ │ └───────┘ │
        └───────────┘ └───────────┘ └───────────┘
              │              │              │
              └──────────────┼──────────────┘
                     HTTP RPC (Raft)
```

## Features

- **Raft Consensus**: Leader election with randomized timeouts, log replication with consistency guarantees, and automatic term management across all nodes
- **Key-Value Store**: Thread-safe in-memory state machine supporting put, get, and delete operations with operation statistics and snapshot capability
- **Consistent Hashing**: SHA-256 based hash ring with configurable virtual nodes per physical node, achieving near-optimal 25% key redistribution on topology changes
- **HTTP Transport**: JSON-over-HTTP RPC layer for Raft communication between nodes, with a separate client-facing REST API for data operations
- **Client CLI**: Command-line interface supporting put, get, delete, status, key listing, and write benchmarking against any node in the cluster
- **Fault Tolerance**: Writes require majority consensus before commitment. Followers can serve reads independently. Leader failure triggers automatic re-election

## Performance

Benchmarked on a 3-node local cluster (Apple Silicon):

| Metric | Value |
|--------|-------|
| Write throughput | 3,800+ ops/sec (with Raft consensus) |
| Average write latency | 263 microseconds |
| Write success rate | 100% (1000/1000) |
| Key redistribution on node add | 24.8% (near-optimal) |
| Leader election time | < 500ms |
| Test coverage | 14 tests passing |

## Getting Started

### Prerequisites

- Go 1.21 or later

### Build

```bash
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
```

### Start a 3-Node Cluster

```bash
./scripts/start-cluster.sh
```

Or manually:

```bash
# Terminal 1
bin/server -id node-1 -raft-addr localhost:6001 -client-addr localhost:8001 \
  -peers "node-2=localhost:6002,node-3=localhost:6003"

# Terminal 2
bin/server -id node-2 -raft-addr localhost:6002 -client-addr localhost:8002 \
  -peers "node-1=localhost:6001,node-3=localhost:6003"

# Terminal 3
bin/server -id node-3 -raft-addr localhost:6003 -client-addr localhost:8003 \
  -peers "node-1=localhost:6001,node-2=localhost:6002"
```

### Client Usage

```bash
# Write a key (must target the leader)
bin/client localhost:8001 put name "Jason"

# Read from any node (reads are local)
bin/client localhost:8002 get name

# Delete a key
bin/client localhost:8001 delete name

# Check node status
bin/client localhost:8001 status

# List all keys
bin/client localhost:8002 keys

# Run write benchmark
bin/client localhost:8001 bench 1000
```

### Run Tests

```bash
go test ./tests/ -v
```

## Project Structure

```
distributed-kv-store/
├── cmd/
│   ├── server/main.go          # Node server with Raft + client API
│   └── client/main.go          # CLI client with benchmarking
├── pkg/
│   ├── raft/
│   │   ├── raft.go             # Core Raft implementation (election, replication)
│   │   └── state.go            # Raft state types (log entries, roles, terms)
│   ├── store/
│   │   └── store.go            # Thread-safe KV state machine
│   ├── transport/
│   │   └── transport.go        # HTTP/JSON RPC transport layer
│   ├── consistent/
│   │   └── hash.go             # Consistent hashing ring
│   └── config/
│       └── config.go           # Cluster configuration
├── tests/
│   ├── raft_test.go            # Leader election and log replication tests
│   ├── store_test.go           # KV store operation tests
│   └── consistent_test.go      # Consistent hashing distribution tests
└── scripts/
    └── start-cluster.sh        # Launch a local 3-node cluster
```

## Technical Details

### Raft Consensus

The Raft implementation follows the protocol described in the [Raft paper](https://raft.github.io/raft.pdf) by Ongaro and Ousterhout. Key behaviors:

- **Leader Election**: Nodes start as followers. If no heartbeat is received within a randomized timeout (300-500ms), a node starts an election by incrementing its term and requesting votes. A candidate wins by receiving votes from a majority.
- **Log Replication**: The leader accepts client writes, appends them to its log, and replicates entries to followers via AppendEntries RPCs. An entry is committed once replicated on a majority of nodes.
- **Safety**: Votes are only granted to candidates with logs at least as up-to-date as the voter's. Leaders only commit entries from their current term.

### Consistent Hashing

Keys are mapped to nodes using a hash ring with virtual nodes. Each physical node occupies multiple positions on the ring (configurable, default 150), ensuring even distribution. When a node is added or removed, only the keys that hash to the affected portion of the ring are redistributed.

## License

MIT
