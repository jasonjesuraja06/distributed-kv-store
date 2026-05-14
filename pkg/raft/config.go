package raft

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ============================================================
// MEMBERSHIP CHANGES via JOINT CONSENSUS
// ============================================================
//
// Problem this solves:
//   Cluster membership cannot safely change in one step. If two
//   different nodes simultaneously update their config to different
//   new memberships, you can end up with two disjoint majorities
//   electing two different leaders for the same term (split brain).
//
// The Raft paper's solution (§6, "Cluster membership changes"):
//   Use a two-phase transition through a joint configuration:
//
//     C_old  →  C_old,new  →  C_new
//
//   During C_old,new, every commit decision requires a majority
//   from BOTH the old set and the new set. This makes it impossible
//   for a leader of C_old (without knowing about the change) and a
//   leader of C_new to simultaneously commit conflicting entries.
//
// Our implementation:
//   - ConfigChange entries are committed through the regular Raft log
//   - When a node applies a ConfigChange that introduces joint config,
//     it switches `commitMajority` to require both old AND new majorities
//   - When the joint-config entry commits, the leader appends a second
//     entry containing only the new config; once THAT commits, the
//     transition is complete and any node not in C_new can step down
//
// API:
//   ProposeAddPeer(id, addr)    — proposes joining `id` to the cluster
//   ProposeRemovePeer(id)       — proposes ejecting `id` from the cluster
//   CurrentConfig()             — current cluster membership
// ============================================================

// ConfigEntryType marks a log entry as carrying a membership change.
// The Command field is JSON-encoded ConfigChange.
const ConfigEntryType uint8 = 1

// ConfigChange is the payload of a membership-change log entry.
type ConfigChange struct {
	Phase     ConfigPhase `json:"phase"`
	OldVoters []string    `json:"old_voters,omitempty"`
	NewVoters []string    `json:"new_voters,omitempty"`
}

// ConfigPhase is one of the joint-consensus phases.
type ConfigPhase string

const (
	ConfigPhaseJoint ConfigPhase = "joint"  // C_old,new — needs majority from both
	ConfigPhaseFinal ConfigPhase = "final"  // C_new — single-majority again
)

// Config holds the cluster membership as known to this node.
type Config struct {
	OldVoters []string
	NewVoters []string // empty unless we're in the joint phase
	Phase     ConfigPhase
}

// InJoint reports whether the cluster is currently in the joint phase.
func (c *Config) InJoint() bool { return c.Phase == ConfigPhaseJoint }

// Voters returns all distinct voters across old and new configs.
func (c *Config) Voters() []string {
	seen := map[string]bool{}
	var out []string
	for _, v := range c.OldVoters {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	for _, v := range c.NewVoters {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

// ConfigCommand is a tagged command body used for the Raft log.
// Regular state-machine commands are stored as-is in Command; config
// changes are wrapped here so the apply loop can route them correctly.
type ConfigCommand struct {
	Type    uint8        `json:"_t"`        // ConfigEntryType for membership changes
	Payload ConfigChange `json:"payload"`
}

// EncodeConfigCommand serializes a ConfigChange to bytes suitable for
// storage in LogEntry.Command.
func EncodeConfigCommand(cc ConfigChange) ([]byte, error) {
	return json.Marshal(ConfigCommand{Type: ConfigEntryType, Payload: cc})
}

// DecodeConfigCommand attempts to interpret a log entry's command as a
// ConfigChange. Returns (nil, nil) if it isn't a config command.
func DecodeConfigCommand(data []byte) (*ConfigChange, error) {
	var wrap ConfigCommand
	if err := json.Unmarshal(data, &wrap); err != nil {
		return nil, nil
	}
	if wrap.Type != ConfigEntryType {
		return nil, nil
	}
	cc := wrap.Payload
	return &cc, nil
}

// ProposeAddPeer proposes adding `newPeerID` to the cluster. The change
// progresses through C_old → C_old,new → C_new. Returns once the
// initial joint-config entry has been appended (commit happens
// asynchronously via the normal replication flow).
func (n *Node) ProposeAddPeer(newPeerID string) error {
	return n.proposeMembershipChange(func(oldVoters []string) []string {
		for _, v := range oldVoters {
			if v == newPeerID {
				return oldVoters
			}
		}
		return append(append([]string(nil), oldVoters...), newPeerID)
	})
}

// ProposeRemovePeer proposes removing `peerID` from the cluster.
func (n *Node) ProposeRemovePeer(peerID string) error {
	return n.proposeMembershipChange(func(oldVoters []string) []string {
		out := make([]string, 0, len(oldVoters))
		for _, v := range oldVoters {
			if v != peerID {
				out = append(out, v)
			}
		}
		return out
	})
}

// CurrentConfig returns a copy of the active cluster membership.
func (n *Node) CurrentConfig() Config {
	n.mu.Lock()
	defer n.mu.Unlock()
	cfg := n.config
	cfg.OldVoters = append([]string(nil), n.config.OldVoters...)
	cfg.NewVoters = append([]string(nil), n.config.NewVoters...)
	return cfg
}

func (n *Node) proposeMembershipChange(transform func([]string) []string) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if n.config.InJoint() {
		n.mu.Unlock()
		return errors.New("raft: membership change already in progress")
	}

	oldVoters := allVoters(n.id, n.peers)
	newVoters := transform(oldVoters)

	cc := ConfigChange{
		Phase:     ConfigPhaseJoint,
		OldVoters: oldVoters,
		NewVoters: newVoters,
	}
	encoded, err := EncodeConfigCommand(cc)
	if err != nil {
		n.mu.Unlock()
		return err
	}

	entry := LogEntry{
		Term:    n.persist.CurrentTerm,
		Index:   n.lastLogIndex() + 1,
		Command: encoded,
	}
	n.persist.Log = append(n.persist.Log, entry)
	n.leader.MatchIndex[n.id] = entry.Index

	// Switch to joint config immediately (Raft optimization: leader
	// applies config changes as soon as the entry is appended, not
	// when committed). This affects commit-majority calculation right
	// away — necessary for safety during the transition.
	n.config = Config{
		OldVoters: oldVoters,
		NewVoters: newVoters,
		Phase:     ConfigPhaseJoint,
	}
	n.logger.Printf("[%s] entering joint config: old=%v new=%v", n.id, oldVoters, newVoters)
	n.mu.Unlock()

	go n.replicateToAll()
	return nil
}

// onConfigCommitted is invoked by the apply loop when a config-change
// entry is committed. For a joint-phase entry, the leader follows up
// with a final-phase entry pinning the cluster to C_new.
func (n *Node) onConfigCommitted(cc ConfigChange) {
	if cc.Phase == ConfigPhaseJoint && n.role == Leader {
		final := ConfigChange{
			Phase:     ConfigPhaseFinal,
			OldVoters: cc.NewVoters,
		}
		encoded, err := EncodeConfigCommand(final)
		if err != nil {
			return
		}
		entry := LogEntry{
			Term:    n.persist.CurrentTerm,
			Index:   n.lastLogIndex() + 1,
			Command: encoded,
		}
		n.persist.Log = append(n.persist.Log, entry)
		n.leader.MatchIndex[n.id] = entry.Index
		go n.replicateToAll()
		n.logger.Printf("[%s] joint config committed; appending final config: %v",
			n.id, cc.NewVoters)
	} else if cc.Phase == ConfigPhaseFinal {
		n.config = Config{
			OldVoters: cc.OldVoters,
			Phase:     ConfigPhaseFinal,
		}
		// Update peer list to reflect the new membership.
		newPeers := make([]string, 0, len(cc.OldVoters))
		for _, v := range cc.OldVoters {
			if v != n.id {
				newPeers = append(newPeers, v)
			}
		}
		n.peers = newPeers
		n.logger.Printf("[%s] membership change complete: %v", n.id, cc.OldVoters)

		// If this node was removed from the cluster, step down.
		inNew := false
		for _, v := range cc.OldVoters {
			if v == n.id {
				inNew = true
				break
			}
		}
		if !inNew && n.role == Leader {
			n.role = Follower
			n.logger.Printf("[%s] removed from cluster; stepping down", n.id)
		}
	}
}

// commitMajority computes how many distinct nodes need to acknowledge
// a log index for it to commit. In the joint phase, BOTH old-config
// majority AND new-config majority are required.
func (n *Node) configMajorityReached(idx uint64) bool {
	if !n.config.InJoint() {
		// Simple majority: standard Raft.
		needed := (len(n.peers)+1)/2 + 1
		replicated := 1 // self
		for _, p := range n.peers {
			if n.leader.MatchIndex[p] >= idx {
				replicated++
			}
		}
		return replicated >= needed
	}
	// Joint: require both old AND new majorities.
	return n.countQuorum(n.config.OldVoters, idx) &&
		n.countQuorum(n.config.NewVoters, idx)
}

func (n *Node) countQuorum(voters []string, idx uint64) bool {
	if len(voters) == 0 {
		return false
	}
	needed := len(voters)/2 + 1
	count := 0
	for _, v := range voters {
		if v == n.id {
			if n.lastLogIndex() >= idx {
				count++
			}
			continue
		}
		if n.leader.MatchIndex[v] >= idx {
			count++
		}
	}
	return count >= needed
}

// allVoters returns the union of self + peers (for capturing current C_old).
func allVoters(selfID string, peers []string) []string {
	out := make([]string, 0, len(peers)+1)
	out = append(out, selfID)
	out = append(out, peers...)
	return out
}

// describe is a debug helper.
func (c *Config) String() string {
	return fmt.Sprintf("Config{phase=%s old=%v new=%v}", c.Phase, c.OldVoters, c.NewVoters)
}
