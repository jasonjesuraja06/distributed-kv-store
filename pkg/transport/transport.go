package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jasonjesuraja06/distributed-kv-store/pkg/raft"
)

// HTTPTransport implements the raft.Transport interface using HTTP/JSON.
// In production you'd use gRPC for binary encoding and HTTP/2 multiplexing,
// but HTTP/JSON is simpler and has zero build dependencies (no protoc needed).
type HTTPTransport struct {
	localAddr  string
	peers      map[string]string // nodeID -> address
	node       *raft.Node
	client     *http.Client
	listener   net.Listener
	mu         sync.RWMutex
	logger     *log.Logger
}

// NewHTTPTransport creates a new HTTP-based transport.
func NewHTTPTransport(localAddr string, peerAddrs map[string]string) *HTTPTransport {
	return &HTTPTransport{
		localAddr: localAddr,
		peers:     peerAddrs,
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
		logger: log.Default(),
	}
}

// SetNode binds this transport to a Raft node (for handling incoming RPCs).
func (t *HTTPTransport) SetNode(node *raft.Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.node = node
}

// Start begins listening for incoming RPCs.
func (t *HTTPTransport) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/vote", t.handleRequestVote)
	mux.HandleFunc("/raft/append", t.handleAppendEntries)

	var err error
	t.listener, err = net.Listen("tcp", t.localAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", t.localAddr, err)
	}

	server := &http.Server{Handler: mux}
	go func() {
		if err := server.Serve(t.listener); err != nil && err != http.ErrServerClosed {
			t.logger.Printf("transport server error: %v", err)
		}
	}()

	return nil
}

// Stop shuts down the transport.
func (t *HTTPTransport) Stop() {
	if t.listener != nil {
		t.listener.Close()
	}
}

// Address returns the local listen address.
func (t *HTTPTransport) Address() string {
	return t.localAddr
}

// ---- Outgoing RPCs (implements raft.Transport interface) ----

func (t *HTTPTransport) RequestVote(target string, req *raft.VoteRequest) (*raft.VoteResponse, error) {
	t.mu.RLock()
	addr, ok := t.peers[target]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown peer: %s", target)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := t.client.Post(fmt.Sprintf("http://%s/raft/vote", addr), "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var voteResp raft.VoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		return nil, err
	}
	return &voteResp, nil
}

func (t *HTTPTransport) AppendEntries(target string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	t.mu.RLock()
	addr, ok := t.peers[target]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown peer: %s", target)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := t.client.Post(fmt.Sprintf("http://%s/raft/append", addr), "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var appendResp raft.AppendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&appendResp); err != nil {
		return nil, err
	}
	return &appendResp, nil
}

// ---- Incoming RPC Handlers ----

func (t *HTTPTransport) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	t.mu.RLock()
	node := t.node
	t.mu.RUnlock()

	if node == nil {
		http.Error(w, "node not ready", http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req raft.VoteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := node.HandleRequestVote(&req)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (t *HTTPTransport) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	t.mu.RLock()
	node := t.node
	t.mu.RUnlock()

	if node == nil {
		http.Error(w, "node not ready", http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req raft.AppendEntriesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := node.HandleAppendEntries(&req)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
