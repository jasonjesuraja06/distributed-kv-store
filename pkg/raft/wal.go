package raft

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// ============================================================
// WRITE-AHEAD LOG (WAL)
// ============================================================
//
// Problem this solves:
//   Without disk persistence, every Raft log entry past LastIncludedIndex
//   is lost when a node restarts. The node can recover by replaying from
//   peers, but that defeats the safety guarantee that an acknowledged
//   write survives a crash.
//
// Design:
//   Append-only file of JSON records, one entry per line. Each Append
//   writes the entry, calls fsync, and returns only after the data is
//   durable. On Replay(), we read the entire file back into memory.
//
// Format (one JSON object per line):
//   {"type":"entry", "term":3, "index":42, "command":"base64..."}
//   {"type":"state", "current_term":3, "voted_for":"node-1"}
//   {"type":"truncate", "from_index":42}
//   {"type":"snapshot_meta", "last_included_index":100, "last_included_term":5}
//
// We also persist CurrentTerm and VotedFor (the other two pieces of
// persistent Raft state the paper requires for safety).
// ============================================================

// walRecordType is the discriminator for each line in the WAL.
type walRecordType string

const (
	walRecordEntry        walRecordType = "entry"
	walRecordState        walRecordType = "state"
	walRecordTruncate     walRecordType = "truncate"
	walRecordSnapshotMeta walRecordType = "snapshot_meta"
)

type walRecord struct {
	Type              walRecordType `json:"type"`
	Term              uint64        `json:"term,omitempty"`
	Index             uint64        `json:"index,omitempty"`
	Command           []byte        `json:"command,omitempty"`
	CurrentTerm       uint64        `json:"current_term,omitempty"`
	VotedFor          string        `json:"voted_for,omitempty"`
	FromIndex         uint64        `json:"from_index,omitempty"`
	LastIncludedIndex uint64        `json:"last_included_index,omitempty"`
	LastIncludedTerm  uint64        `json:"last_included_term,omitempty"`
}

// WAL is a durable append-only log for Raft persistent state.
type WAL struct {
	mu   sync.Mutex
	path string
	file *os.File
	w    *bufio.Writer
}

// OpenWAL opens (or creates) the WAL at the given path. The directory
// is created if necessary.
func OpenWAL(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	return &WAL{path: path, file: f, w: bufio.NewWriter(f)}, nil
}

// AppendEntry durably writes a single LogEntry. Returns only after fsync.
func (w *WAL) AppendEntry(e LogEntry) error {
	return w.writeAndSync(walRecord{
		Type:    walRecordEntry,
		Term:    e.Term,
		Index:   e.Index,
		Command: e.Command,
	})
}

// AppendEntries writes a batch of entries (single fsync at the end).
func (w *WAL) AppendEntries(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range entries {
		rec := walRecord{
			Type:    walRecordEntry,
			Term:    e.Term,
			Index:   e.Index,
			Command: e.Command,
		}
		if err := w.writeRecord(rec); err != nil {
			return err
		}
	}
	return w.sync()
}

// AppendState persists CurrentTerm and VotedFor (one of the three
// pieces of persistent state the Raft paper requires for safety).
func (w *WAL) AppendState(currentTerm uint64, votedFor string) error {
	return w.writeAndSync(walRecord{
		Type:        walRecordState,
		CurrentTerm: currentTerm,
		VotedFor:    votedFor,
	})
}

// AppendTruncate records that all entries with index >= fromIndex
// should be discarded during replay.
func (w *WAL) AppendTruncate(fromIndex uint64) error {
	return w.writeAndSync(walRecord{
		Type:      walRecordTruncate,
		FromIndex: fromIndex,
	})
}

// AppendSnapshotMeta records that a snapshot was taken at the given
// index/term. During replay, entries with index <= lastIncludedIndex
// can be discarded (the state-machine snapshot file holds them).
func (w *WAL) AppendSnapshotMeta(lastIncludedIndex, lastIncludedTerm uint64) error {
	return w.writeAndSync(walRecord{
		Type:              walRecordSnapshotMeta,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
	})
}

// Close flushes and closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.w != nil {
		_ = w.w.Flush()
	}
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			return err
		}
		return w.file.Close()
	}
	return nil
}

// ReplayState is the materialized result of replaying a WAL.
type ReplayState struct {
	Log               []LogEntry
	CurrentTerm       uint64
	VotedFor          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
}

// Replay reads the entire WAL file back, applying records in order to
// reconstruct the persistent state.
func (w *WAL) Replay() (*ReplayState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// We need a separate reader because we opened the file in append-only mode.
	f, err := os.Open(w.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &ReplayState{}, nil
		}
		return nil, err
	}
	defer f.Close()

	state := &ReplayState{Log: []LogEntry{}}
	scanner := bufio.NewScanner(f)
	// Allow large log entries (up to 4 MB per line)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec walRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("wal: corrupt record: %w", err)
		}
		switch rec.Type {
		case walRecordEntry:
			state.Log = append(state.Log, LogEntry{
				Term:    rec.Term,
				Index:   rec.Index,
				Command: rec.Command,
			})
		case walRecordState:
			state.CurrentTerm = rec.CurrentTerm
			state.VotedFor = rec.VotedFor
		case walRecordTruncate:
			// Remove entries with index >= rec.FromIndex
			kept := state.Log[:0]
			for _, e := range state.Log {
				if e.Index < rec.FromIndex {
					kept = append(kept, e)
				}
			}
			state.Log = kept
		case walRecordSnapshotMeta:
			state.LastIncludedIndex = rec.LastIncludedIndex
			state.LastIncludedTerm = rec.LastIncludedTerm
			// Drop entries covered by the snapshot.
			kept := state.Log[:0]
			for _, e := range state.Log {
				if e.Index > rec.LastIncludedIndex {
					kept = append(kept, e)
				}
			}
			state.Log = kept
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return state, nil
}

// writeAndSync writes one record and immediately fsyncs.
func (w *WAL) writeAndSync(rec walRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writeRecord(rec); err != nil {
		return err
	}
	return w.sync()
}

func (w *WAL) writeRecord(rec walRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	if err := w.w.WriteByte('\n'); err != nil {
		return err
	}
	return nil
}

func (w *WAL) sync() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}
