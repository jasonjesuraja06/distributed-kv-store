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
)

func main() {
	if len(os.Args) < 3 {
		printUsage()
		os.Exit(1)
	}

	addr := os.Args[1]
	command := os.Args[2]

	client := &http.Client{Timeout: 5 * time.Second}

	switch command {
	case "put":
		if len(os.Args) < 5 {
			fmt.Println("Usage: client <addr> put <key> <value>")
			os.Exit(1)
		}
		key, value := os.Args[3], os.Args[4]
		doPut(client, addr, key, value)

	case "get":
		if len(os.Args) < 4 {
			fmt.Println("Usage: client <addr> get <key>")
			os.Exit(1)
		}
		key := os.Args[3]
		doGet(client, addr, key)

	case "delete":
		if len(os.Args) < 4 {
			fmt.Println("Usage: client <addr> delete <key>")
			os.Exit(1)
		}
		key := os.Args[3]
		doDelete(client, addr, key)

	case "status":
		doStatus(client, addr)

	case "keys":
		doKeys(client, addr)

	case "bench":
		count := 1000
		if len(os.Args) >= 4 {
			fmt.Sscanf(os.Args[3], "%d", &count)
		}
		doBenchmark(client, addr, count)

	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: client <server-addr> <command> [args...]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  put <key> <value>   Store a key-value pair")
	fmt.Println("  get <key>           Retrieve a value by key")
	fmt.Println("  delete <key>        Delete a key")
	fmt.Println("  status              Show node status")
	fmt.Println("  keys                List all keys")
	fmt.Println("  bench [count]       Run write benchmark (default: 1000)")
	fmt.Println()
	fmt.Println("Example:")
	fmt.Println("  client localhost:8001 put name Jason")
	fmt.Println("  client localhost:8001 get name")
}

func doPut(client *http.Client, addr, key, value string) {
	body, _ := json.Marshal(map[string]string{"value": value})
	req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/kv/%s", addr, key), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		fmt.Printf("OK: %s = %s\n", key, value)
	} else {
		fmt.Printf("Error (%d): %s\n", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
}

func doGet(client *http.Client, addr, key string) {
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

func doDelete(client *http.Client, addr, key string) {
	req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/kv/%s", addr, key), nil)
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		fmt.Printf("Deleted: %s\n", key)
	} else {
		fmt.Printf("Error (%d): %s\n", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
}

func doStatus(client *http.Client, addr string) {
	resp, err := client.Get(fmt.Sprintf("http://%s/status", addr))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	out, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(out))
}

func doKeys(client *http.Client, addr string) {
	resp, err := client.Get(fmt.Sprintf("http://%s/keys", addr))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	out, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(out))
}

func doBenchmark(client *http.Client, addr string, count int) {
	fmt.Printf("Benchmarking %d writes to %s...\n", count, addr)

	start := time.Now()
	success := 0
	failures := 0

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		value := fmt.Sprintf("value-%d", i)

		body, _ := json.Marshal(map[string]string{"value": value})
		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/kv/%s", addr, key), bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			failures++
			continue
		}
		resp.Body.Close()
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
