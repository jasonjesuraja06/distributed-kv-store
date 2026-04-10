package consistent

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// Ring implements consistent hashing for distributing keys across nodes.
// Each physical node gets multiple "virtual nodes" on the ring to ensure
// even distribution. When a node is added/removed, only ~1/n keys move.
type Ring struct {
	mu       sync.RWMutex
	ring     []uint32          // Sorted hash values on the ring
	nodeMap  map[uint32]string // Hash value -> node ID
	replicas int               // Number of virtual nodes per physical node
}

// NewRing creates a consistent hash ring with the specified number
// of virtual nodes per physical node. More replicas = more even
// distribution but slightly more memory.
func NewRing(replicas int) *Ring {
	return &Ring{
		nodeMap:  make(map[uint32]string),
		replicas: replicas,
	}
}

// AddNode adds a physical node to the ring.
// It creates 'replicas' virtual nodes spread around the ring.
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.replicas; i++ {
		key := fmt.Sprintf("%s-vnode-%d", nodeID, i)
		h := hashKey(key)
		r.ring = append(r.ring, h)
		r.nodeMap[h] = nodeID
	}
	sort.Slice(r.ring, func(i, j int) bool {
		return r.ring[i] < r.ring[j]
	})
}

// RemoveNode removes a physical node and all its virtual nodes.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	newRing := make([]uint32, 0, len(r.ring))
	for _, h := range r.ring {
		if r.nodeMap[h] != nodeID {
			newRing = append(newRing, h)
		} else {
			delete(r.nodeMap, h)
		}
	}
	r.ring = newRing
}

// GetNode returns the node responsible for the given key.
// It finds the first virtual node on the ring whose hash is >= the key's hash.
// If it wraps around, it returns the first node on the ring (circular).
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return ""
	}

	h := hashKey(key)

	// Binary search for the first ring position >= h
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i] >= h
	})

	// Wrap around if we've gone past the end
	if idx >= len(r.ring) {
		idx = 0
	}

	return r.nodeMap[r.ring[idx]]
}

// GetNodes returns the N distinct nodes responsible for the given key.
// Used for replication — store the key on N different physical nodes.
func (r *Ring) GetNodes(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return nil
	}

	h := hashKey(key)
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i] >= h
	})

	seen := make(map[string]bool)
	var nodes []string

	for i := 0; i < len(r.ring) && len(nodes) < n; i++ {
		pos := (idx + i) % len(r.ring)
		nodeID := r.nodeMap[r.ring[pos]]
		if !seen[nodeID] {
			seen[nodeID] = true
			nodes = append(nodes, nodeID)
		}
	}

	return nodes
}

// NodeCount returns the number of physical nodes on the ring.
func (r *Ring) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]bool)
	for _, nodeID := range r.nodeMap {
		seen[nodeID] = true
	}
	return len(seen)
}

// hashKey produces a uint32 hash for a given key using SHA-256.
// We use SHA-256 (truncated to 32 bits) for good distribution.
func hashKey(key string) uint32 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}
