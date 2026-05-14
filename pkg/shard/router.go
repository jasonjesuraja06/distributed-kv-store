package shard

import (
	"github.com/jasonjesuraja06/distributed-kv-store/pkg/consistent"
)

// Router maps keys to shards using a consistent-hash ring of shard IDs.
//
// Why a ring of SHARDS rather than nodes? Because each shard owns a
// contiguous portion of the keyspace; replicas within a shard hold
// identical data via Raft. We hash to shards, then choose any replica.
//
// Adding/removing a shard moves only ~1/N of the keys, which matches the
// behavior real production systems (TiKV's region splits, DynamoDB's
// resharding) optimize for.
type Router struct {
	cluster *Cluster
	ring    *consistent.Ring
}

// NewRouter constructs a router over the given cluster. It uses
// `virtualNodes` virtual positions per shard on the ring (default 150 if
// zero), giving smooth distribution.
func NewRouter(cluster *Cluster, virtualNodes int) *Router {
	if virtualNodes == 0 {
		virtualNodes = 150
	}
	r := &Router{cluster: cluster, ring: consistent.NewRing(virtualNodes)}
	for _, sh := range cluster.Shards {
		r.ring.AddNode(shardKey(sh.ID))
	}
	return r
}

// ShardFor returns the shard ID responsible for the given key. Deterministic
// across all clients/servers as long as they use the same cluster config.
func (r *Router) ShardFor(key string) int {
	skey := r.ring.GetNode(key)
	return shardKeyToID(skey)
}

// ReplicasFor returns all replicas that hold the given key.
// The leader is somewhere among these; the client retries with redirect.
func (r *Router) ReplicasFor(key string) []Replica {
	id := r.ShardFor(key)
	sh := r.cluster.ShardByID(id)
	if sh == nil {
		return nil
	}
	out := make([]Replica, len(sh.Replicas))
	copy(out, sh.Replicas)
	return out
}

// PreferredReplica returns one replica responsible for the key (typically
// used as the first attempt before falling back via leader-redirect).
func (r *Router) PreferredReplica(key string) (Replica, bool) {
	reps := r.ReplicasFor(key)
	if len(reps) == 0 {
		return Replica{}, false
	}
	return reps[0], true
}

// Cluster returns the underlying topology.
func (r *Router) Cluster() *Cluster { return r.cluster }

// Stats summarizes routing distribution. Useful for debugging hot shards.
type DistributionStats struct {
	SamplesHashed int
	ShardCounts   map[int]int
}

// Distribution simulates hashing `numSamples` random-ish keys and reports
// how many fall on each shard. A balanced cluster should show roughly
// equal counts.
func (r *Router) Distribution(keys []string) DistributionStats {
	counts := map[int]int{}
	for _, k := range keys {
		counts[r.ShardFor(k)]++
	}
	return DistributionStats{SamplesHashed: len(keys), ShardCounts: counts}
}

// ---- helpers ----

func shardKey(id int) string {
	// Stable string form of shard ID for the hash ring
	return "shard-" + itoa(id)
}

func shardKeyToID(skey string) int {
	const prefix = "shard-"
	if len(skey) <= len(prefix) {
		return -1
	}
	return atoi(skey[len(prefix):])
}

// itoa / atoi: minimal local helpers to avoid pulling fmt/strconv into
// the hot path (the router is on every client request).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func atoi(s string) int {
	if len(s) == 0 {
		return -1
	}
	neg := false
	i := 0
	if s[0] == '-' {
		neg = true
		i = 1
	}
	n := 0
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return -1
		}
		n = n*10 + int(s[i]-'0')
	}
	if neg {
		return -n
	}
	return n
}
