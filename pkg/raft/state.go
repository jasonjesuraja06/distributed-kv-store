package raft

// Role represents the current role of a Raft node.
type Role int

const (
	Follower  Role = iota // Default state — listens to the leader
	Candidate             // Trying to become leader via election
	Leader                // Accepted leader — replicates logs to followers
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry represents a single entry in the replicated log.
// Every state change (put/delete) goes through the log first.
type LogEntry struct {
	Term    uint64 // The term when this entry was created
	Index   uint64 // Position in the log (1-indexed)
	Command []byte // The serialized command (e.g., "PUT key value")
}

// PersistentState is the state that must survive restarts.
// Raft requires this to be written to disk BEFORE responding.
type PersistentState struct {
	CurrentTerm uint64     // Latest term this node has seen
	VotedFor    string     // NodeID this node voted for in current term ("" = none)
	Log         []LogEntry // The replicated log
}

// VolatileState is state that can be reconstructed after restart.
type VolatileState struct {
	CommitIndex uint64 // Highest log entry known to be committed
	LastApplied uint64 // Highest log entry applied to state machine
}

// LeaderState is additional state only the leader tracks.
type LeaderState struct {
	NextIndex  map[string]uint64 // For each follower: next log index to send
	MatchIndex map[string]uint64 // For each follower: highest log index known replicated
}
