#!/bin/bash
# Start a 3-node distributed KV store cluster locally.
# Each node gets its own Raft port and client API port.

set -e

echo "Building server and client..."
cd "$(dirname "$0")/.."
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client

echo "Starting 3-node cluster..."
echo ""

# Node 1: Raft on 6001, Client API on 8001
bin/server \
  -id node-1 \
  -raft-addr localhost:6001 \
  -client-addr localhost:8001 \
  -peers "node-2=localhost:6002,node-3=localhost:6003" &
PID1=$!

# Node 2: Raft on 6002, Client API on 8002
bin/server \
  -id node-2 \
  -raft-addr localhost:6002 \
  -client-addr localhost:8002 \
  -peers "node-1=localhost:6001,node-3=localhost:6003" &
PID2=$!

# Node 3: Raft on 6003, Client API on 8003
bin/server \
  -id node-3 \
  -raft-addr localhost:6003 \
  -client-addr localhost:8003 \
  -peers "node-1=localhost:6001,node-2=localhost:6002" &
PID3=$!

echo "Cluster started!"
echo "  Node 1: PID=$PID1  Raft=:6001  API=http://localhost:8001"
echo "  Node 2: PID=$PID2  Raft=:6002  API=http://localhost:8002"
echo "  Node 3: PID=$PID3  Raft=:6003  API=http://localhost:8003"
echo ""
echo "Wait 2 seconds for leader election..."
sleep 2
echo ""

# Check which node is leader
for port in 8001 8002 8003; do
  STATUS=$(curl -s http://localhost:$port/status 2>/dev/null || echo '{"role":"unreachable"}')
  echo "  localhost:$port -> $STATUS"
done

echo ""
echo "Try these commands:"
echo "  bin/client localhost:8001 put name Jason"
echo "  bin/client localhost:8002 get name"
echo "  bin/client localhost:8003 status"
echo "  bin/client localhost:8001 bench 1000"
echo ""
echo "Kill a node:  kill $PID3"
echo "Stop cluster: kill $PID1 $PID2 $PID3"
echo ""
echo "Press Ctrl+C to stop all nodes."

wait
