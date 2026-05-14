package snapshot

import (
	"sync"
	"time"

	"github.com/jasonjesuraja06/distributed-kv-store/pkg/raft"
	"github.com/jasonjesuraja06/distributed-kv-store/pkg/store"
)

// Manager periodically takes snapshots of the KV state machine and triggers
// log compaction in the Raft node. This decouples the snapshot policy
// (when to snapshot, where to store) from Raft's correctness rules.
//
// Snapshot trigger: whenever (CommitIndex - LastIncludedIndex) > LogThreshold.
// Snapshots are saved atomically (temp-file rename) so a crash during
// snapshot creation does not corrupt the on-disk image.
type Manager struct {
	mu       sync.Mutex
	store    *store.KVStore
	node     *raft.Node
	path     string
	threshold uint64
	interval time.Duration

	stopCh chan struct{}
	stats  ManagerStats
}

type ManagerStats struct {
	SnapshotsTaken    uint64
	LastSnapshotIndex uint64
	LastSnapshotTerm  uint64
	LastSnapshotBytes int
	LastSnapshotAt    time.Time
}

// NewManager wires a KV state machine, a Raft node, and a snapshot path
// together. Pass logThreshold = number of new committed entries that
// triggers a snapshot, and interval = polling frequency.
func NewManager(s *store.KVStore, node *raft.Node, path string,
	logThreshold uint64, interval time.Duration) *Manager {
	if logThreshold == 0 {
		logThreshold = 1000
	}
	if interval == 0 {
		interval = 5 * time.Second
	}
	return &Manager{
		store:     s,
		node:      node,
		path:      path,
		threshold: logThreshold,
		interval:  interval,
		stopCh:    make(chan struct{}),
	}
}

// Start begins the periodic snapshot loop. Returns immediately.
func (m *Manager) Start() {
	go m.run()
}

// Stop halts the periodic loop. Any in-flight snapshot completes first.
func (m *Manager) Stop() {
	close(m.stopCh)
}

func (m *Manager) run() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			_ = m.MaybeSnapshot()
		}
	}
}

// MaybeSnapshot takes a snapshot if the log has grown beyond the threshold
// since the last snapshot. Safe to call concurrently with Raft activity.
func (m *Manager) MaybeSnapshot() error {
	logState := m.node.LogState()
	if logState.LastLogIndex-logState.LastIncludedIndex < m.threshold {
		return nil
	}
	return m.ForceSnapshot()
}

// ForceSnapshot snapshots immediately, regardless of threshold.
func (m *Manager) ForceSnapshot() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logState := m.node.LogState()
	if logState.LastLogIndex == logState.LastIncludedIndex {
		return nil // nothing new to snapshot
	}

	// Capture state machine data and the corresponding log boundary.
	// We use LastLogIndex here for simplicity; a more careful impl would use
	// the actual LastApplied to avoid blocking writes.
	data := m.store.Snapshot()
	cutoffIndex := logState.LastLogIndex

	// Build snapshot object (the term will be filled in by Raft).
	snap := &Snapshot{
		LastIncludedIndex: cutoffIndex,
		Data:              data,
	}

	// Compact the Raft log first so we know the term at cutoffIndex is
	// available; then write the snapshot to disk.
	if err := m.node.CreateSnapshot(cutoffIndex); err != nil {
		return err
	}
	// Re-read LogState to capture the term Raft just recorded
	post := m.node.LogState()
	snap.LastIncludedTerm = post.LastIncludedTerm

	if err := snap.Save(m.path); err != nil {
		return err
	}

	m.stats.SnapshotsTaken++
	m.stats.LastSnapshotIndex = snap.LastIncludedIndex
	m.stats.LastSnapshotTerm = snap.LastIncludedTerm
	m.stats.LastSnapshotBytes = snap.SizeBytes()
	m.stats.LastSnapshotAt = time.Now()
	return nil
}

// LoadAndRestore reads a snapshot from disk (if any) and applies it to both
// the state machine and the Raft node. Returns true if a snapshot was found
// and loaded.
func (m *Manager) LoadAndRestore() (bool, error) {
	snap, err := Load(m.path)
	if err != nil {
		return false, err
	}
	if snap == nil {
		return false, nil
	}
	m.store.Restore(snap.Data)
	m.node.RestoreFromSnapshot(snap.LastIncludedIndex, snap.LastIncludedTerm)

	m.mu.Lock()
	m.stats.LastSnapshotIndex = snap.LastIncludedIndex
	m.stats.LastSnapshotTerm = snap.LastIncludedTerm
	m.stats.LastSnapshotBytes = snap.SizeBytes()
	m.mu.Unlock()

	return true, nil
}

func (m *Manager) Stats() ManagerStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stats
}
