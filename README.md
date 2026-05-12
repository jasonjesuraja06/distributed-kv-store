# Distributed Key-Value Store

> A fault-tolerant, replicated key-value store in **Go** implementing the **Raft consensus algorithm** for strong consistency across a multi-node cluster. **3,800+ writes/sec at 263 µs average write latency** on a 3-node cluster.

![Go](https://img.shields.io/badge/Go-1.21%2B-00ADD8.svg)
![Raft](https://img.shields.io/badge/Consensus-Raft-blue.svg)
![Tests](https://img.shields.io/badge/tests-14%2F14%20passing-success.svg)
![Architecture](https://img.shields.io/badge/Architecture-Distributed-orange.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## Performance

Benchmarked on a 3-node local cluster (Apple Silicon, single host).

| Metric | Value |
|---|---|
| **Write throughput (with Raft consensus)** | **3,800+ ops/sec** |
| **Average write latency** | **263 µs** |
| Write success rate | 100% (1000/1000) |
| Key redistribution on node add | 24.8% (theoretical optimum: 25%) |
| Leader election time | < 500 ms |
| Test coverage | 14/14 passing |

## Highlights

- **Raft consensus** implemented from scratch — randomized-timeout leader election, log replication with majority-commit, term-based safety, and the up-to-date-log voting rule
- **Linearizable writes** — every write requires acknowledgment from a majority of nodes before it commits
- **Consistent hashing** with SHA-256 and 150 virtual nodes per physical node — achieves near-optimal key redistribution when nodes join or leave the cluster
- **HTTP/JSON RPC transport** — one transport layer handles both Raft RPCs (AppendEntries, RequestVote) and the client-facing REST API
- **Local-replica reads** — followers serve reads directly without leader hop, trading consistency for throughput on the read path
- **Fault tolerance** — survives single-node failures with automatic re-election; manual chaos testing validated split-vote recovery and partition healing

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

## Getting Started

### Prerequisites
- Go 1.21+

### Build

```bash
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
```

### Start a 3-node Cluster

```bash
./scripts/start-cluster.sh
```

Or run nodes manually:

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

### Client Operations

```bash
bin/client localhost:8001 put name "Jason"     # writes must target the leader
bin/client localhost:8002 get name             # reads work from any node
bin/client localhost:8001 delete name
bin/client localhost:8001 status               # node role, term, log index
bin/client localhost:8002 keys                 # list all keys
bin/client localhost:8001 bench 1000           # write benchmark
```

### Run Tests

```bash
go test ./tests/ -v
```

## Technical Details

### Raft Consensus

Implementation follows the [Raft paper](https://raft.github.io/raft.pdf) by Ongaro and Ousterhout (2014). Three sub-protocols:

| Sub-protocol | Mechanism |
|---|---|
| **Leader election** | Followers wait on a randomized timeout (300–500 ms); on timeout they increment term, become candidates, and request votes. A candidate becomes leader on receiving a majority. |
| **Log replication** | The leader appends client writes to its log and replicates entries to followers via `AppendEntries` RPCs. Entries commit once acknowledged by a majority. |
| **Safety** | Votes are only granted to candidates with logs at least as up-to-date as the voter's. Leaders only commit entries from their current term (this prevents the classic "Figure 8" anomaly from the paper). |

### Consistent Hashing

Keys map to nodes via a SHA-256 hash ring. Each physical node occupies 150 virtual positions on the ring (configurable), spreading keys uniformly. When a node is added or removed, only the keys on the affected arcs of the ring move — measured at 24.8% redistribution for a 3-node → 4-node transition (theoretical optimum: 25%).

### Read / Write Semantics

| Operation | Routed to | Consistency |
|---|---|---|
| `put`, `delete` | Leader (write rejected on follower with hint to leader) | Linearizable (majority-commit) |
| `get` | Any node (local) | Eventually consistent (follower may lag) |

Trade-off: linearizable reads would require either round-tripping every read to the leader or implementing read-index / read-lease optimization. The current design prioritizes read throughput; strong-read mode is a planned extension.

### Transport Layer

`pkg/transport` is a thin HTTP/JSON wrapper handling both:

1. **Raft RPCs** between nodes — `POST /raft/append-entries`, `POST /raft/request-vote`
2. **Client REST API** — `PUT /kv/{key}`, `GET /kv/{key}`, `DELETE /kv/{key}`, `GET /status`, `GET /keys`

JSON over HTTP was chosen for debuggability; a binary protocol (gRPC, MessagePack) would lower per-op overhead but would obscure the protocol during development.

## Project Structure

```
distributed-kv-store/
├── cmd/
│   ├── server/main.go          # Node server (Raft + client API)
│   └── client/main.go          # CLI with benchmark mode
├── pkg/
│   ├── raft/
│   │   ├── raft.go             # Election + replication (493 lines)
│   │   └── state.go            # Log entries, roles, terms
│   ├── store/
│   │   └── store.go            # Thread-safe KV state machine
│   ├── transport/
│   │   └── transport.go        # HTTP/JSON RPC
│   ├── consistent/
│   │   └── hash.go             # Consistent hashing ring
│   └── config/
│       └── config.go           # Cluster configuration
├── tests/
│   ├── raft_test.go            #   2 cases (election + replication)
│   ├── store_test.go           #   6 cases (CRUD + snapshot)
│   └── consistent_test.go      #   6 cases (ring distribution)
└── scripts/
    └── start-cluster.sh        # Launch 3-node local cluster
```

## Roadmap / Known Limitations

- [ ] Persistent log (currently in-memory; loss on full-cluster restart)
- [ ] Snapshotting + log compaction
- [ ] Linearizable reads via read-index optimization
- [ ] Membership changes (joint consensus)
- [ ] Pre-vote optimization to prevent disruptive elections on partition healing

## License

MIT
