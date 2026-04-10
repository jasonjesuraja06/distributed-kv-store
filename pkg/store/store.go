package store

import (
	"encoding/json"
	"fmt"
	"sync"
)

// CommandType represents the type of operation on the KV store.
type CommandType string

const (
	CmdPut    CommandType = "PUT"
	CmdDelete CommandType = "DELETE"
)

// Command is a serializable operation on the KV store.
type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value string      `json:"value,omitempty"`
}

// EncodeCommand serializes a Command to bytes for the Raft log.
func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

// DecodeCommand deserializes bytes from the Raft log into a Command.
func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}

// KVStore is a thread-safe in-memory key-value store.
// It serves as the state machine that Raft replicates.
// Every committed log entry is applied to this store.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string

	// Stats
	putCount    uint64
	deleteCount uint64
	getCount    uint64
}

// NewKVStore creates a new empty KV store.
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Get retrieves a value by key. Returns the value and whether the key exists.
// This is a read operation — it does NOT go through Raft (reads are local).
func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.getCount++
	val, ok := s.data[key]
	return val, ok
}

// Apply processes a committed Raft log entry.
// This is called by the Raft module when an entry is committed.
func (s *KVStore) Apply(data []byte) error {
	cmd, err := DecodeCommand(data)
	if err != nil {
		return fmt.Errorf("failed to decode command: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Type {
	case CmdPut:
		s.data[cmd.Key] = cmd.Value
		s.putCount++
	case CmdDelete:
		delete(s.data, cmd.Key)
		s.deleteCount++
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}

	return nil
}

// Size returns the number of keys in the store.
func (s *KVStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Keys returns all keys in the store.
func (s *KVStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// Stats returns operation counts.
type Stats struct {
	Keys       int    `json:"keys"`
	PutCount   uint64 `json:"put_count"`
	DeleteCount uint64 `json:"delete_count"`
	GetCount   uint64 `json:"get_count"`
}

func (s *KVStore) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Stats{
		Keys:        len(s.data),
		PutCount:    s.putCount,
		DeleteCount: s.deleteCount,
		GetCount:    s.getCount,
	}
}

// Snapshot returns a copy of all data (for backup/transfer).
func (s *KVStore) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot := make(map[string]string, len(s.data))
	for k, v := range s.data {
		snapshot[k] = v
	}
	return snapshot
}
