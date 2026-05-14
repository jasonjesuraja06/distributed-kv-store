#!/usr/bin/env bash
# Launches a 2-shard × 3-replica cluster locally (6 nodes total).
# Each shard is an independent Raft group; clients route via consistent
# hashing over the cluster.json topology.

set -euo pipefail

BIN=${BIN:-./bin/server}
CLUSTER=${CLUSTER:-scripts/cluster.json}

if [[ ! -x "$BIN" ]]; then
    echo "Building server binary..."
    go build -o "$BIN" ./cmd/server
fi

mkdir -p snapshots logs

start_node() {
    local node_id=$1
    local raft_addr=$2
    local client_addr=$3
    echo "  → $node_id  raft=$raft_addr  client=$client_addr"
    "$BIN" \
        -id "$node_id" \
        -raft-addr "$raft_addr" \
        -client-addr "$client_addr" \
        -cluster "$CLUSTER" \
        -snapshot-path "snapshots/$node_id.json" \
        > "logs/$node_id.log" 2>&1 &
    echo $! > "logs/$node_id.pid"
}

echo "Starting 2-shard × 3-replica cluster (6 nodes)..."

# Shard 0
start_node s0-a localhost:6001 localhost:8001
start_node s0-b localhost:6002 localhost:8002
start_node s0-c localhost:6003 localhost:8003

# Shard 1
start_node s1-a localhost:6101 localhost:8101
start_node s1-b localhost:6102 localhost:8102
start_node s1-c localhost:6103 localhost:8103

echo
echo "Cluster started. To stop: ./scripts/stop-sharded-cluster.sh"
echo "To use the client:"
echo "    ./bin/client cluster:$CLUSTER put name Jason"
echo "    ./bin/client cluster:$CLUSTER distribution 10000"
