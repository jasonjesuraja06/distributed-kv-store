package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jasonjesuraja06/distributed-kv-store/pkg/snapshot"
)

func TestSnapshot_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")

	original := &snapshot.Snapshot{
		LastIncludedIndex: 42,
		LastIncludedTerm:  3,
		Data: map[string]string{
			"alice":   "engineer",
			"bob":     "manager",
			"carol":   "founder",
			"unicode": "résumé 日本語 🚀",
		},
	}
	if err := original.Save(path); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := snapshot.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected snapshot, got nil")
	}
	if loaded.LastIncludedIndex != 42 {
		t.Errorf("LastIncludedIndex: got %d want 42", loaded.LastIncludedIndex)
	}
	if loaded.LastIncludedTerm != 3 {
		t.Errorf("LastIncludedTerm: got %d want 3", loaded.LastIncludedTerm)
	}
	if len(loaded.Data) != 4 {
		t.Errorf("Data size: got %d want 4", len(loaded.Data))
	}
	if loaded.Data["unicode"] != "résumé 日本語 🚀" {
		t.Errorf("unicode round-trip failed: got %q", loaded.Data["unicode"])
	}
}

func TestSnapshot_LoadMissingFileReturnsNilNoError(t *testing.T) {
	s, err := snapshot.Load("/tmp/definitely-does-not-exist-snapshot.json")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if s != nil {
		t.Errorf("expected nil snapshot, got %+v", s)
	}
}

func TestSnapshot_AtomicWriteNoTempLeftBehind(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")

	s := snapshot.New()
	s.LastIncludedIndex = 1
	s.Data["x"] = "y"
	if err := s.Save(path); err != nil {
		t.Fatalf("save: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if e.Name() != "snap.json" {
			t.Errorf("leftover temp file: %s", e.Name())
		}
	}
}

func TestSnapshot_OverwriteReplacesAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")

	s1 := snapshot.New()
	s1.LastIncludedIndex = 1
	s1.Data["v"] = "first"
	if err := s1.Save(path); err != nil {
		t.Fatal(err)
	}

	s2 := snapshot.New()
	s2.LastIncludedIndex = 2
	s2.Data["v"] = "second"
	if err := s2.Save(path); err != nil {
		t.Fatal(err)
	}

	loaded, _ := snapshot.Load(path)
	if loaded.LastIncludedIndex != 2 {
		t.Errorf("overwrite didn't take: got %d", loaded.LastIncludedIndex)
	}
	if loaded.Data["v"] != "second" {
		t.Errorf("expected 'second', got %q", loaded.Data["v"])
	}
}

func TestSnapshot_EmptyDataMapIsHandled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")

	s := snapshot.New()
	s.LastIncludedIndex = 7
	s.LastIncludedTerm = 1
	if err := s.Save(path); err != nil {
		t.Fatal(err)
	}
	loaded, _ := snapshot.Load(path)
	if loaded.Data == nil {
		t.Error("Data should never be nil after Load")
	}
	if len(loaded.Data) != 0 {
		t.Errorf("expected 0 entries, got %d", len(loaded.Data))
	}
}

func TestSnapshot_SizeBytesIncreasesWithData(t *testing.T) {
	small := snapshot.New()
	small.Data["a"] = "1"
	big := snapshot.New()
	for i := 0; i < 100; i++ {
		big.Data[string(rune('a'+i%26))+string(rune('a'+(i/26)%26))] = "value"
	}
	if big.SizeBytes() <= small.SizeBytes() {
		t.Errorf("big snapshot should be larger: small=%d big=%d",
			small.SizeBytes(), big.SizeBytes())
	}
}
