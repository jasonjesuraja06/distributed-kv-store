package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jasonjesuraja06/distributed-kv-store/pkg/shard"
)

// target encapsulates either a single fixed address (legacy mode) or a
// cluster configuration that the client uses to route requests to the
// correct shard's replicas.
type target struct {
	fixed   string
	cluster *shard.Cluster
	router  *shard.Router
}

func parseTarget(arg string) (*target, error) {
	if strings.HasPrefix(arg, "cluster:") {
		path := strings.TrimPrefix(arg, "cluster:")
		c, err := shard.LoadCluster(path)
		if err != nil {
			return nil, err
		}
		return &target{cluster: c, router: shard.NewRouter(c, 0)}, nil
	}
	return &target{fixed: arg}, nil
}

// addrForKey returns the first replica to try for a given key.
func (t *target) addrForKey(key string) (string, int) {
	if t.fixed != "" {
		return t.fixed, -1
	}
	r, ok := t.router.PreferredReplica(key)
	if !ok {
		return "", -1
	}
	return r.ClientAddr, t.router.ShardFor(key)
}

// replicasForKey returns all replicas for the shard owning the key.
// Used to find the leader after a 307 redirect.
func (t *target) replicasForKey(key string) []shard.Replica {
	if t.cluster == nil {
		return nil
	}
	return t.router.ReplicasFor(key)
}

// allShards returns all shards (used for cluster-wide status/keys).
func (t *target) allShards() []shard.Shard {
	if t.cluster == nil {
		return nil
	}
	return t.cluster.Shards
}

func main() {
	if len(os.Args) < 3 {
		printUsage()
		os.Exit(1)
	}
	tgt, err := parseTarget(os.Args[1])
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	command := os.Args[2]
	client := &http.Client{Timeout: 5 * time.Second}

	switch command {
	case "put":
		if len(os.Args) < 5 {
			fmt.Println("Usage: client <addr> put <key> <value>")
			os.Exit(1)
		}
		doPut(client, tgt, os.Args[3], os.Args[4])
	case "get":
		if len(os.Args) < 4 {
			fmt.Println("Usage: client <addr> get <key>")
			os.Exit(1)
		}
		doGet(client, tgt, os.Args[3])
	case "delete":
		if len(os.Args) < 4 {
			fmt.Println("Usage: client <addr> delete <key>")
			os.Exit(1)
		}
		doDelete(client, tgt, os.Args[3])
	case "status":
		doStatus(client, tgt)
	case "keys":
		doKeys(client, tgt)
	case "bench":
		count := 1000
		if len(os.Args) >= 4 {
			fmt.Sscanf(os.Args[3], "%d", &count)
		}
		doBenchmark(client, tgt, count)
	case "distribution":
		// Demo: hash N synthetic keys and report per-shard counts.
		count := 10000
		if len(os.Args) >= 4 {
			fmt.Sscanf(os.Args[3], "%d", &count)
		}
		doDistribution(tgt, count)
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: client <addr-or-cluster> <command> [args...]")
	fmt.Println()
	fmt.Println("  <addr-or-cluster>")
	fmt.Println("     localhost:8001                    single-node mode")
	fmt.Println("     cluster:./cluster.json            multi-shard cluster mode")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  put <key> <value>          Store a key-value pair")
	fmt.Println("  get <key>                  Retrieve a value by key")
	fmt.Println("  delete <key>               Delete a key")
	fmt.Println("  status                     Show node (or per-shard) status")
	fmt.Println("  keys                       List all keys (per-shard in cluster mode)")
	fmt.Println("  bench [count]              Run write benchmark (default: 1000)")
	fmt.Println("  distribution [count]       Show how N keys map to shards (cluster only)")
}

// ---- Operations ----

// doPut writes a key. In cluster mode, routes via shard router; on a 307
// "not leader" response, looks up the leader's client_addr in the cluster
// and retries.
func doPut(client *http.Client, t *target, key, value string) {
	body, _ := json.Marshal(map[string]string{"value": value})

	addr, shID := t.addrForKey(key)
	if shID >= 0 {
		fmt.Printf("[router] key=%q → shard=%d (preferred=%s)\n", key, shID, addr)
	}

	for attempt := 0; attempt < 3; attempt++ {
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/kv/%s", addr, key),
			bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			fmt.Printf("OK: %s = %s (via %s)\n", key, value, addr)
			return
		}
		if resp.StatusCode == http.StatusTemporaryRedirect {
			leader := parseLeaderID(strings.TrimSpace(string(respBody)))
			leaderAddr := t.lookupLeaderAddr(key, leader)
			if leaderAddr == "" || leaderAddr == addr {
				fmt.Printf("Error: not leader, leader unknown (response: %s)\n",
					strings.TrimSpace(string(respBody)))
				return
			}
			fmt.Printf("[router] redirect → leader=%s\n", leaderAddr)
			addr = leaderAddr
			continue
		}
		fmt.Printf("Error (%d): %s\n", resp.StatusCode, strings.TrimSpace(string(respBody)))
		return
	}
	fmt.Println("Error: too many redirects")
}

func doGet(client *http.Client, t *target, key string) {
	addr, shID := t.addrForKey(key)
	if shID >= 0 {
		fmt.Printf("[router] key=%q → shard=%d (read from %s)\n", key, shID, addr)
	}
	resp, err := client.Get(fmt.Sprintf("http://%s/kv/%s", addr, key))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		fmt.Printf("Key '%s' not found\n", key)
		return
	}
	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	fmt.Printf("%s = %s\n", result["key"], result["value"])
}

func doDelete(client *http.Client, t *target, key string) {
	addr, shID := t.addrForKey(key)
	if shID >= 0 {
		fmt.Printf("[router] key=%q → shard=%d\n", key, shID)
	}
	for attempt := 0; attempt < 3; attempt++ {
		req, _ := http.NewRequest(http.MethodDelete,
			fmt.Sprintf("http://%s/kv/%s", addr, key), nil)
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			fmt.Printf("Deleted: %s\n", key)
			return
		}
		if resp.StatusCode == http.StatusTemporaryRedirect {
			leader := parseLeaderID(strings.TrimSpace(string(respBody)))
			la := t.lookupLeaderAddr(key, leader)
			if la == "" || la == addr {
				fmt.Printf("Error: %s\n", strings.TrimSpace(string(respBody)))
				return
			}
			addr = la
			continue
		}
		fmt.Printf("Error (%d): %s\n", resp.StatusCode, strings.TrimSpace(string(respBody)))
		return
	}
}

func doStatus(client *http.Client, t *target) {
	addrs := t.allClientAddrs()
	if len(addrs) == 0 {
		addrs = []string{t.fixed}
	}
	for _, a := range addrs {
		fmt.Printf("--- %s ---\n", a)
		resp, err := client.Get(fmt.Sprintf("http://%s/status", a))
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		out, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(out))
	}
}

func doKeys(client *http.Client, t *target) {
	addrs := t.allClientAddrs()
	if len(addrs) == 0 {
		addrs = []string{t.fixed}
	}
	for _, a := range addrs {
		fmt.Printf("--- %s ---\n", a)
		resp, err := client.Get(fmt.Sprintf("http://%s/keys", a))
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		out, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(out))
	}
}

func doBenchmark(client *http.Client, t *target, count int) {
	fmt.Printf("Benchmarking %d writes...\n", count)
	start := time.Now()
	success, failures := 0, 0

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		addr, _ := t.addrForKey(key)
		body, _ := json.Marshal(map[string]string{"value": value})

		req, _ := http.NewRequest(http.MethodPut,
			fmt.Sprintf("http://%s/kv/%s", addr, key), bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			failures++
			continue
		}
		body2, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// On redirect, retry once against the leader.
		if resp.StatusCode == http.StatusTemporaryRedirect && t.cluster != nil {
			leader := parseLeaderID(strings.TrimSpace(string(body2)))
			la := t.lookupLeaderAddr(key, leader)
			if la != "" && la != addr {
				body, _ = json.Marshal(map[string]string{"value": value})
				req2, _ := http.NewRequest(http.MethodPut,
					fmt.Sprintf("http://%s/kv/%s", la, key), bytes.NewReader(body))
				req2.Header.Set("Content-Type", "application/json")
				r2, err2 := client.Do(req2)
				if err2 == nil {
					if r2.StatusCode == http.StatusOK {
						success++
					} else {
						failures++
					}
					r2.Body.Close()
					continue
				}
			}
			failures++
			continue
		}
		if resp.StatusCode == http.StatusOK {
			success++
		} else {
			failures++
		}
	}

	elapsed := time.Since(start)
	opsPerSec := float64(success) / elapsed.Seconds()
	avgLatency := elapsed / time.Duration(count)

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║         BENCHMARK RESULTS            ║")
	fmt.Println("╠══════════════════════════════════════╣")
	fmt.Printf("║  Total ops:     %8d             ║\n", count)
	fmt.Printf("║  Successful:    %8d             ║\n", success)
	fmt.Printf("║  Failed:        %8d             ║\n", failures)
	fmt.Printf("║  Duration:      %8.2fs            ║\n", elapsed.Seconds())
	fmt.Printf("║  Throughput:    %8.0f ops/sec     ║\n", opsPerSec)
	fmt.Printf("║  Avg latency:   %8s           ║\n", avgLatency.Round(time.Microsecond))
	fmt.Println("╚══════════════════════════════════════╝")
}

func doDistribution(t *target, count int) {
	if t.cluster == nil {
		fmt.Println("distribution requires cluster: mode")
		return
	}
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("key-%d-%d", i, time.Now().UnixNano())
	}
	stats := t.router.Distribution(keys)
	fmt.Printf("Hashed %d keys across %d shards:\n", stats.SamplesHashed, len(stats.ShardCounts))
	for _, sh := range t.cluster.Shards {
		c := stats.ShardCounts[sh.ID]
		pct := 100.0 * float64(c) / float64(stats.SamplesHashed)
		fmt.Printf("  shard %d: %6d keys (%5.2f%%)\n", sh.ID, c, pct)
	}
}

// ---- Helpers ----

func parseLeaderID(body string) string {
	// "not leader (leader: node-2)" → "node-2"
	const prefix = "leader: "
	idx := strings.Index(body, prefix)
	if idx < 0 {
		return ""
	}
	rest := body[idx+len(prefix):]
	end := strings.IndexAny(rest, ")\n ")
	if end < 0 {
		return strings.TrimSpace(rest)
	}
	return strings.TrimSpace(rest[:end])
}

// lookupLeaderAddr finds the client_addr of the given leader node_id within
// the shard responsible for the key. Returns "" if not found or single-node mode.
func (t *target) lookupLeaderAddr(key, leaderID string) string {
	if t.cluster == nil || leaderID == "" {
		return ""
	}
	for _, r := range t.router.ReplicasFor(key) {
		if r.NodeID == leaderID {
			return r.ClientAddr
		}
	}
	return ""
}

// allClientAddrs lists every client_addr across every shard, for fan-out
// status/keys queries. In single-node mode returns just the fixed addr.
func (t *target) allClientAddrs() []string {
	if t.cluster == nil {
		return nil
	}
	var out []string
	for _, sh := range t.cluster.Shards {
		for _, r := range sh.Replicas {
			out = append(out, r.ClientAddr)
		}
	}
	return out
}
