# Distributed Key-Value Store

> A fault-tolerant, replicated key-value store in **Go** implementing the **Raft consensus algorithm** for strong consistency across a multi-node cluster. **3,800+ writes/sec at 263 µs average write latency** on a 3-node cluster.

![Go](https://img.shields.io/badge/Go-1.21%2B-00ADD8.svg)
![Raft](https://img.shields.io/badge/Consensus-Raft-blue.svg)
![Sharding](https://img.shields.io/badge/Sharding-Consistent%20Hash-orange.svg)
![Snapshots](https://img.shields.io/badge/Log%20Compaction-Snapshots-orange.svg)
![Tests](https://img.shields.io/badge/tests-55%2F55%20passing-success.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## Performance

Benchmarked on a 3-node local cluster (Apple Silicon, single host).

| Metric | Value |
|---|---|
| **Write throughput (with Raft consensus)** | **3,800+ ops/sec** |
| **Average write latency** | **263 µs** |
| Write success rate | 100% (1000/1000) |
| Key redistribution on node add (3→4 shards) | 24.8% (theoretical optimum: 25%) |
| Shard-distribution deviation (100K random keys) | <10% across all shards |
| Leader election time | < 500 ms |
| Test coverage | 55/55 passing |

## Highlights

- **Raft consensus** implemented from scratch — randomized-timeout leader election, log replication with majority-commit, term-based safety, and the up-to-date-log voting rule
- **Linearizable writes** — every write requires acknowledgment from a majority of nodes before it commits
- **Multi-shard routing** — cluster.json describes N shards × M replicas; the client uses consistent hashing over shards (not nodes) so adding a shard moves only ~1/N of keys
- **Snapshots + log compaction** — periodic state-machine snapshots (atomic temp-file + rename) trigger Raft log truncation, bounding storage growth; restored automatically on restart
- **Consistent hashing** with SHA-256 and 150 virtual nodes per shard — verified <10% distribution deviation and 24.8% movement on shard add (matching DynamoDB / TiKV design papers)
- **HTTP/JSON RPC transport** — one transport layer handles both Raft RPCs (AppendEntries, RequestVote) and the client-facing REST API
- **Local-replica reads** — followers serve reads directly without leader hop, trading consistency for throughput on the read path
- **Fault tolerance** — survives single-node failures with automatic re-election; manual chaos testing validated split-vote recovery and partition healing

## Architecture

A cluster is N **shards**, each containing M **replicas**. Each shard is an independent Raft group. The client uses consistent hashing over shard IDs to route any given key to its shard, then targets a replica within that shard (writes hit the leader; reads work from any follower).

```
                            ┌─────────────────┐
                            │   Client CLI    │
                            │  put/get/delete │
                            └────────┬────────┘
                                     │
                          consistent hash(key)
                                     │
                ┌────────────────────┼────────────────────┐
                ▼                                         ▼
       ┌─────── SHARD 0 ───────┐                ┌─────── SHARD 1 ───────┐
       │                       │                │                       │
       │  Leader   Follower    │                │  Leader   Follower    │
       │     │       │         │                │     │       │         │
       │   Raft ◄─► Raft       │                │   Raft ◄─► Raft       │
       │     │       │         │                │     │       │         │
       │  KV-Store KV-Store    │                │  KV-Store KV-Store    │
       │  Snapshot Snapshot    │                │  Snapshot Snapshot    │
       └───────────────────────┘                └───────────────────────┘
```

Adding a new shard moves only ~1/(N+1) of the keys (24.8% measured for 3→4 transition, matching the theoretical 25%). Each replica periodically snapshots its state machine and triggers Raft log compaction, bounding on-disk and in-memory growth.

## Getting Started

### Prerequisites
- Go 1.21+

### Build

```bash
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client
```

### Start a 3-node Single-Shard Cluster

```bash
./scripts/start-cluster.sh
```

### Start a 2-Shard × 3-Replica Cluster (6 nodes)

```bash
./scripts/start-sharded-cluster.sh
# Stop:  ./scripts/stop-sharded-cluster.sh
# Logs:  logs/<node-id>.log
```

Topology is defined in [`scripts/cluster.json`](scripts/cluster.json). Each node loads it via `-cluster` and discovers its shard peers automatically.

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

Single-node mode:

```bash
bin/client localhost:8001 put name "Jason"     # writes must target the leader
bin/client localhost:8002 get name             # reads work from any node
bin/client localhost:8001 delete name
bin/client localhost:8001 status               # node role, term, log index, snapshot stats
bin/client localhost:8002 keys                 # list all keys
bin/client localhost:8001 bench 1000           # write benchmark
```

Cluster mode (key is routed to the right shard automatically):

```bash
bin/client cluster:scripts/cluster.json put name Jason
# [router] key="name" → shard=0 (preferred=localhost:8001)
# OK: name = Jason (via localhost:8001)

bin/client cluster:scripts/cluster.json get name
bin/client cluster:scripts/cluster.json status          # status of every node
bin/client cluster:scripts/cluster.json distribution 10000
#   shard 0: 5063 keys (50.63%)
#   shard 1: 4937 keys (49.37%)
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
│   ├── server/main.go          # Node server (Raft + client API + snapshot mgr)
│   └── client/main.go          # CLI: single-node OR `cluster:` mode with router
├── pkg/
│   ├── raft/
│   │   ├── raft.go             # Election + replication + snapshot/compaction
│   │   └── state.go            # Log entries, roles, terms, snapshot metadata
│   ├── store/
│   │   └── store.go            # Thread-safe KV state machine (Snapshot/Restore)
│   ├── snapshot/
│   │   ├── snapshot.go         # On-disk snapshot (atomic rename)
│   │   └── manager.go          # Periodic threshold-driven snapshot loop
│   ├── shard/
│   │   ├── cluster.go          # cluster.json topology (N shards × M replicas)
│   │   └── router.go           # Consistent-hash-based shard router
│   ├── transport/
│   │   └── transport.go        # HTTP/JSON RPC
│   ├── consistent/
│   │   └── hash.go             # Consistent hashing ring
│   └── config/
│       └── config.go           # Cluster configuration
├── tests/                      # 38 cases
│   ├── raft_test.go            #   2 cases (election + replication)
│   ├── compaction_test.go      #   6 cases (snapshot/compaction/restore)
│   ├── snapshot_test.go        #   6 cases (atomic save/load)
│   ├── shard_test.go           #  11 cases (cluster + router + distribution)
│   ├── store_test.go           #   6 cases (CRUD + snapshot)
│   └── consistent_test.go      #   6 cases (ring distribution)
└── scripts/
    ├── cluster.json                  # 2-shard × 3-replica topology
    ├── start-cluster.sh              # 3-node single-shard cluster
    ├── start-sharded-cluster.sh      # 6-node 2×3 sharded cluster
    └── stop-sharded-cluster.sh       # Stop sharded cluster
```

## Feature Set

- [x] Raft consensus (leader election, log replication, majority-commit, term-based safety)
- [x] Snapshotting + Raft log compaction (atomic temp-file + rename)
- [x] Multi-shard routing (consistent-hash-based; each shard is an independent Raft group)
- [x] **Persistent Raft log** — append-only JSON WAL with `fsync` on every record; on startup, `AttachWAL` replays entries, current term, voted-for, and snapshot metadata
- [x] **Pre-vote optimization** — candidates poll peers for hypothetical support before incrementing term, preventing disruptive re-elections after partition healing
- [x] **InstallSnapshot RPC** — leader sends its full state-machine snapshot to a follower whose `nextIndex` has fallen below `LastIncludedIndex`; receiver swaps in the snapshot via the `SnapshotInstaller` callback
- [x] **Linearizable reads via read-index** — leader records `commitIndex`, broadcasts a heartbeat round to confirm majority leadership, then waits for `LastApplied` to catch up before serving the read locally
- [x] **Membership changes via joint consensus** — `ProposeAddPeer` / `ProposeRemovePeer` append a `C_old,new` config entry, switch the commit-majority calculation to require **both** old AND new majorities during the transition, then auto-append the final `C_new` entry once joint commits
- [x] **Cross-shard transactions (2PC)** — `pkg/txn.Coordinator` runs Prepare/Commit/Abort across multiple shard participants atomically; locks are held on prepared keys until commit or abort

## Roadmap

Reasonable next steps (not currently implemented):

- Lease-based reads as a faster alternative to read-index
- Pre-vote disable knob during cluster boot (currently always on after first election cycle)
- Streaming InstallSnapshot for snapshots above a few MB (currently single-shot)
- Coordinator failover for the 2PC layer (currently single-coordinator)

## License

MIT
