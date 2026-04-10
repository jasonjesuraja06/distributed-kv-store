package tests

import (
	"testing"

	"github.com/jasonjesuraja06/distributed-kv-store/pkg/store"
)

func TestKVStore_PutAndGet(t *testing.T) {
	s := store.NewKVStore()

	cmd, _ := store.EncodeCommand(store.Command{
		Type:  store.CmdPut,
		Key:   "name",
		Value: "Jason",
	})
	if err := s.Apply(cmd); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	val, ok := s.Get("name")
	if !ok {
		t.Fatal("key 'name' not found")
	}
	if val != "Jason" {
		t.Fatalf("expected 'Jason', got '%s'", val)
	}
}

func TestKVStore_Delete(t *testing.T) {
	s := store.NewKVStore()

	// Put then delete
	cmd, _ := store.EncodeCommand(store.Command{Type: store.CmdPut, Key: "temp", Value: "data"})
	s.Apply(cmd)

	cmd, _ = store.EncodeCommand(store.Command{Type: store.CmdDelete, Key: "temp"})
	s.Apply(cmd)

	_, ok := s.Get("temp")
	if ok {
		t.Fatal("key 'temp' should have been deleted")
	}
}

func TestKVStore_Overwrite(t *testing.T) {
	s := store.NewKVStore()

	cmd, _ := store.EncodeCommand(store.Command{Type: store.CmdPut, Key: "k", Value: "v1"})
	s.Apply(cmd)

	cmd, _ = store.EncodeCommand(store.Command{Type: store.CmdPut, Key: "k", Value: "v2"})
	s.Apply(cmd)

	val, _ := s.Get("k")
	if val != "v2" {
		t.Fatalf("expected 'v2', got '%s'", val)
	}
}

func TestKVStore_GetNonExistent(t *testing.T) {
	s := store.NewKVStore()
	_, ok := s.Get("nonexistent")
	if ok {
		t.Fatal("should not find nonexistent key")
	}
}

func TestKVStore_Stats(t *testing.T) {
	s := store.NewKVStore()

	cmd, _ := store.EncodeCommand(store.Command{Type: store.CmdPut, Key: "a", Value: "1"})
	s.Apply(cmd)
	cmd, _ = store.EncodeCommand(store.Command{Type: store.CmdPut, Key: "b", Value: "2"})
	s.Apply(cmd)
	cmd, _ = store.EncodeCommand(store.Command{Type: store.CmdDelete, Key: "a"})
	s.Apply(cmd)

	stats := s.Stats()
	if stats.PutCount != 2 {
		t.Fatalf("expected 2 puts, got %d", stats.PutCount)
	}
	if stats.DeleteCount != 1 {
		t.Fatalf("expected 1 delete, got %d", stats.DeleteCount)
	}
	if stats.Keys != 1 {
		t.Fatalf("expected 1 key, got %d", stats.Keys)
	}
}

func TestKVStore_Snapshot(t *testing.T) {
	s := store.NewKVStore()

	cmd, _ := store.EncodeCommand(store.Command{Type: store.CmdPut, Key: "x", Value: "1"})
	s.Apply(cmd)
	cmd, _ = store.EncodeCommand(store.Command{Type: store.CmdPut, Key: "y", Value: "2"})
	s.Apply(cmd)

	snap := s.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 keys in snapshot, got %d", len(snap))
	}
	if snap["x"] != "1" || snap["y"] != "2" {
		t.Fatal("snapshot data mismatch")
	}
}
