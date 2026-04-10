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
	"strings"
	"syscall"

	"github.com/jasonjesuraja06/distributed-kv-store/pkg/raft"
	"github.com/jasonjesuraja06/distributed-kv-store/pkg/store"
	"github.com/jasonjesuraja06/distributed-kv-store/pkg/transport"
)

func main() {
	// Parse command-line flags
	nodeID := flag.String("id", "", "Node ID (e.g., node-1)")
	raftAddr := flag.String("raft-addr", "", "Raft transport address (e.g., localhost:6001)")
	clientAddr := flag.String("client-addr", "", "Client API address (e.g., localhost:8001)")
	peersFlag := flag.String("peers", "", "Comma-separated peers: id=addr,id=addr (e.g., node-2=localhost:6002,node-3=localhost:6003)")
	flag.Parse()

	if *nodeID == "" || *raftAddr == "" || *clientAddr == "" {
		fmt.Println("Usage: server -id <node-id> -raft-addr <addr> -client-addr <addr> -peers <id=addr,...>")
		os.Exit(1)
	}

	// Parse peers
	peerAddrs := make(map[string]string)
	var peerIDs []string
	if *peersFlag != "" {
		for _, p := range strings.Split(*peersFlag, ",") {
			parts := strings.SplitN(p, "=", 2)
			if len(parts) == 2 {
				peerAddrs[parts[0]] = parts[1]
				peerIDs = append(peerIDs, parts[0])
			}
		}
	}

	log.Printf("[%s] Starting node: raft=%s client=%s peers=%v", *nodeID, *raftAddr, *clientAddr, peerIDs)

	// Create the KV store (state machine)
	kvStore := store.NewKVStore()

	// Create the transport
	trans := transport.NewHTTPTransport(*raftAddr, peerAddrs)

	// Create the Raft node
	applyFn := func(entry raft.LogEntry) {
		if err := kvStore.Apply(entry.Command); err != nil {
			log.Printf("[%s] Failed to apply entry: %v", *nodeID, err)
		}
	}
	node := raft.NewNode(*nodeID, peerIDs, trans, applyFn)
	trans.SetNode(node)

	// Start transport (listen for Raft RPCs)
	if err := trans.Start(); err != nil {
		log.Fatalf("Failed to start transport: %v", err)
	}

	// Start Raft node
	node.Start()

	// Start client-facing HTTP API
	go startClientAPI(*clientAddr, *nodeID, node, kvStore)

	log.Printf("[%s] Node is running. Client API at http://%s", *nodeID, *clientAddr)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("[%s] Shutting down...", *nodeID)
	node.Stop()
	trans.Stop()
}

// startClientAPI starts the HTTP API that clients use to read/write data.
func startClientAPI(addr string, nodeID string, node *raft.Node, kvStore *store.KVStore) {
	mux := http.NewServeMux()

	// PUT /kv/{key} — Store a key-value pair
	// Body: {"value": "some-value"}
	mux.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		if key == "" {
			http.Error(w, "key required", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodGet:
			// GET — read from local store (no Raft needed)
			val, ok := kvStore.Get(key)
			if !ok {
				http.Error(w, "key not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})

		case http.MethodPut:
			// PUT — write goes through Raft consensus
			if node.CurrentRole() != raft.Leader {
				http.Error(w, fmt.Sprintf("not leader (leader: %s)", node.LeaderID()), http.StatusTemporaryRedirect)
				return
			}

			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			var req struct {
				Value string `json:"value"`
			}
			if err := json.Unmarshal(body, &req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			cmd, _ := store.EncodeCommand(store.Command{
				Type:  store.CmdPut,
				Key:   key,
				Value: req.Value,
			})

			if !node.Propose(cmd) {
				http.Error(w, "failed to propose", http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "ok", "key": key})

		case http.MethodDelete:
			// DELETE — delete goes through Raft consensus
			if node.CurrentRole() != raft.Leader {
				http.Error(w, fmt.Sprintf("not leader (leader: %s)", node.LeaderID()), http.StatusTemporaryRedirect)
				return
			}

			cmd, _ := store.EncodeCommand(store.Command{
				Type: store.CmdDelete,
				Key:  key,
			})

			if !node.Propose(cmd) {
				http.Error(w, "failed to propose", http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "deleted", "key": key})

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// GET /status — Node status and stats
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		stats := kvStore.Stats()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"node_id": nodeID,
			"role":    node.CurrentRole().String(),
			"term":    node.CurrentTerm(),
			"store":   stats,
		})
	})

	// GET /keys — List all keys
	mux.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"keys":  kvStore.Keys(),
			"count": kvStore.Size(),
		})
	})

	server := &http.Server{Addr: addr, Handler: mux}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Client API error: %v", err)
	}
}
