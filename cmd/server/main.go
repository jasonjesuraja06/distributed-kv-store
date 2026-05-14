package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jasonjesuraja06/distributed-kv-store/pkg/raft"
	"github.com/jasonjesuraja06/distributed-kv-store/pkg/shard"
	"github.com/jasonjesuraja06/distributed-kv-store/pkg/snapshot"
	"github.com/jasonjesuraja06/distributed-kv-store/pkg/store"
	"github.com/jasonjesuraja06/distributed-kv-store/pkg/transport"
)

func main() {
	nodeID := flag.String("id", "", "Node ID (e.g., node-1)")
	raftAddr := flag.String("raft-addr", "", "Raft transport address (e.g., localhost:6001)")
	clientAddr := flag.String("client-addr", "", "Client API address (e.g., localhost:8001)")
	peersFlag := flag.String("peers", "", "Comma-separated peers: id=addr,id=addr (e.g., node-2=localhost:6002,node-3=localhost:6003)")
	shardID := flag.Int("shard", -1, "Shard ID this node belongs to (informational; ignored in single-shard mode)")
	clusterPath := flag.String("cluster", "", "Path to cluster.json. When provided, peers are derived from the cluster topology for this node's shard.")
	snapshotPath := flag.String("snapshot-path", "", "Path to snapshot file (default: snapshots/<node-id>.json)")
	snapshotInterval := flag.Duration("snapshot-interval", 5*time.Second, "How often to check whether to snapshot")
	snapshotThreshold := flag.Uint64("snapshot-threshold", 1000, "Take a snapshot once this many uncompacted committed entries accumulate")
	disableSnapshots := flag.Bool("disable-snapshots", false, "Disable the periodic snapshot manager")
	flag.Parse()

	if *nodeID == "" || *raftAddr == "" || *clientAddr == "" {
		fmt.Println("Usage: server -id <node-id> -raft-addr <addr> -client-addr <addr> -peers <id=addr,...>")
		fmt.Println("       server -id <node-id> -cluster cluster.json   (cluster mode)")
		os.Exit(1)
	}

	// Resolve peers — either from -peers flag or from cluster.json.
	peerAddrs := make(map[string]string)
	var peerIDs []string

	if *clusterPath != "" {
		cluster, err := shard.LoadCluster(*clusterPath)
		if err != nil {
			log.Fatalf("[%s] cluster load: %v", *nodeID, err)
		}
		shID, replica := cluster.FindReplica(*nodeID)
		if replica == nil {
			log.Fatalf("[%s] node not found in cluster.json", *nodeID)
		}
		if *shardID < 0 {
			*shardID = shID
		}
		sh := cluster.ShardByID(shID)
		for _, r := range sh.Replicas {
			if r.NodeID == *nodeID {
				continue
			}
			peerAddrs[r.NodeID] = r.RaftAddr
			peerIDs = append(peerIDs, r.NodeID)
		}
		log.Printf("[%s] cluster mode: shard=%d peers=%v", *nodeID, shID, peerIDs)
	} else if *peersFlag != "" {
		for _, p := range strings.Split(*peersFlag, ",") {
			parts := strings.SplitN(p, "=", 2)
			if len(parts) == 2 {
				peerAddrs[parts[0]] = parts[1]
				peerIDs = append(peerIDs, parts[0])
			}
		}
	}

	log.Printf("[%s] starting: raft=%s client=%s shard=%d peers=%v",
		*nodeID, *raftAddr, *clientAddr, *shardID, peerIDs)

	// State machine
	kvStore := store.NewKVStore()

	// Transport
	trans := transport.NewHTTPTransport(*raftAddr, peerAddrs)

	// Raft node
	applyFn := func(entry raft.LogEntry) {
		if err := kvStore.Apply(entry.Command); err != nil {
			log.Printf("[%s] apply failed: %v", *nodeID, err)
		}
	}
	node := raft.NewNode(*nodeID, peerIDs, trans, applyFn)
	trans.SetNode(node)

	// Snapshot manager
	if *snapshotPath == "" {
		*snapshotPath = filepath.Join("snapshots", *nodeID+".json")
	}
	snapMgr := snapshot.NewManager(kvStore, node, *snapshotPath,
		*snapshotThreshold, *snapshotInterval)

	// Restore from snapshot if present BEFORE starting Raft.
	if loaded, err := snapMgr.LoadAndRestore(); err != nil {
		log.Fatalf("[%s] snapshot restore: %v", *nodeID, err)
	} else if loaded {
		stats := snapMgr.Stats()
		log.Printf("[%s] snapshot restored: index=%d term=%d bytes=%d keys=%d",
			*nodeID, stats.LastSnapshotIndex, stats.LastSnapshotTerm,
			stats.LastSnapshotBytes, kvStore.Size())
	}

	// Start transport, then Raft, then snapshot manager
	if err := trans.Start(); err != nil {
		log.Fatalf("[%s] transport start: %v", *nodeID, err)
	}
	node.Start()
	if !*disableSnapshots {
		snapMgr.Start()
	}

	// Client-facing HTTP API
	go startClientAPI(*clientAddr, *nodeID, *shardID, node, kvStore, snapMgr)

	log.Printf("[%s] running at http://%s (snapshot-path=%s)",
		*nodeID, *clientAddr, *snapshotPath)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("[%s] shutting down...", *nodeID)
	snapMgr.Stop()
	node.Stop()
	trans.Stop()
}

func startClientAPI(addr, nodeID string, shardID int, node *raft.Node,
	kvStore *store.KVStore, snapMgr *snapshot.Manager) {
	mux := http.NewServeMux()

	mux.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		if key == "" {
			http.Error(w, "key required", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodGet:
			val, ok := kvStore.Get(key)
			if !ok {
				http.Error(w, "key not found", http.StatusNotFound)
				return
			}
			writeJSON(w, map[string]string{"key": key, "value": val})

		case http.MethodPut:
			if node.CurrentRole() != raft.Leader {
				http.Error(w, fmt.Sprintf("not leader (leader: %s)", node.LeaderID()),
					http.StatusTemporaryRedirect)
				return
			}
			body, _ := io.ReadAll(r.Body)
			var req struct{ Value string `json:"value"` }
			if err := json.Unmarshal(body, &req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			cmd, _ := store.EncodeCommand(store.Command{
				Type: store.CmdPut, Key: key, Value: req.Value,
			})
			if !node.Propose(cmd) {
				http.Error(w, "propose failed", http.StatusInternalServerError)
				return
			}
			writeJSON(w, map[string]string{"status": "ok", "key": key})

		case http.MethodDelete:
			if node.CurrentRole() != raft.Leader {
				http.Error(w, fmt.Sprintf("not leader (leader: %s)", node.LeaderID()),
					http.StatusTemporaryRedirect)
				return
			}
			cmd, _ := store.EncodeCommand(store.Command{Type: store.CmdDelete, Key: key})
			if !node.Propose(cmd) {
				http.Error(w, "propose failed", http.StatusInternalServerError)
				return
			}
			writeJSON(w, map[string]string{"status": "deleted", "key": key})

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		logState := node.LogState()
		writeJSON(w, map[string]interface{}{
			"node_id":  nodeID,
			"shard_id": shardID,
			"role":     node.CurrentRole().String(),
			"term":     node.CurrentTerm(),
			"store":    kvStore.Stats(),
			"log": map[string]interface{}{
				"last_included_index": logState.LastIncludedIndex,
				"last_included_term":  logState.LastIncludedTerm,
				"first_log_index":     logState.FirstLogIndex,
				"last_log_index":      logState.LastLogIndex,
				"log_entries":         logState.LogEntries,
			},
			"snapshot": snapMgr.Stats(),
		})
	})

	mux.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]interface{}{
			"keys": kvStore.Keys(), "count": kvStore.Size(),
		})
	})

	mux.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		if err := snapMgr.ForceSnapshot(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]interface{}{
			"status": "snapshot_taken", "stats": snapMgr.Stats(),
		})
	})

	server := &http.Server{Addr: addr, Handler: mux}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("client api: %v", err)
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
