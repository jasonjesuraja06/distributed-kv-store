package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Snapshot captures the state machine at a specific point in the Raft log.
// Once a snapshot is committed, all log entries with Index <= LastIncludedIndex
// can be discarded (log compaction), bounding storage growth.
//
// This is the same data flow used by etcd, CockroachDB, and TiKV: periodically
// snapshot the in-memory state machine to disk, then truncate the log up to that
// point. On restart, load the snapshot first, then replay only the tail of the
// log that's newer than the snapshot.
type Snapshot struct {
	LastIncludedIndex uint64            `json:"last_included_index"`
	LastIncludedTerm  uint64            `json:"last_included_term"`
	Data              map[string]string `json:"data"`
}

// New creates an empty snapshot.
func New() *Snapshot {
	return &Snapshot{Data: make(map[string]string)}
}

// Save writes the snapshot atomically to disk:
//  1. Marshal to JSON
//  2. Write to a temp file in the same directory
//  3. Rename the temp file over the target path (atomic on POSIX)
//
// Atomic rename guarantees we never end up with a half-written snapshot
// if the process is killed mid-write.
func (s *Snapshot) Save(path string) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("snapshot marshal: %w", err)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("snapshot mkdir: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".snapshot.*.tmp")
	if err != nil {
		return fmt.Errorf("snapshot temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op if rename succeeds

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("snapshot write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("snapshot fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("snapshot close: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("snapshot rename: %w", err)
	}
	return nil
}

// Load reads a snapshot from disk. Returns (nil, nil) if the file does
// not exist (treated as no prior snapshot — fresh start).
func Load(path string) (*Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("snapshot read: %w", err)
	}
	s := New()
	if err := json.Unmarshal(data, s); err != nil {
		return nil, fmt.Errorf("snapshot unmarshal: %w", err)
	}
	if s.Data == nil {
		s.Data = make(map[string]string)
	}
	return s, nil
}

// SizeBytes returns the serialized size of the snapshot.
func (s *Snapshot) SizeBytes() int {
	b, _ := json.Marshal(s)
	return len(b)
}
