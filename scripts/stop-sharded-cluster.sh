#!/usr/bin/env bash
# Stop the local sharded cluster launched by start-sharded-cluster.sh
set -euo pipefail
for pidfile in logs/*.pid; do
    [[ -f "$pidfile" ]] || continue
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
        echo "Stopping $(basename "$pidfile" .pid) (pid $pid)"
        kill "$pid"
    fi
    rm -f "$pidfile"
done
