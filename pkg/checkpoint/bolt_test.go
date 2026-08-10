package checkpoint

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// TestBoltStore_CRUD tests basic Create, Read, Update, Delete operations.
func TestBoltStore_CRUD(t *testing.T) {
	dir := t.TempDir() // Auto-cleanup after test
	path := filepath.Join(dir, "test_checkpoints.db")
	ctx := context.Background()

	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("NewBoltStore failed: %v", err)
	}
	defer store.Close()

	// Test data: key formats
	tests := []struct {
		componentID string
		token       []byte
	}{
		{"producer/orders", []byte("lsn:12345")},
		{"consumer/orders/analytics_db", []byte("offset:67890")},
		{"consumer/orders/archive", []byte("s3://bucket/key")},
		{"stream/orders", []byte("byte_offset:4096")},
	}

	// 1. Save all checkpoints
	for _, tt := range tests {
		if err := store.Save(ctx, tt.componentID, tt.token); err != nil {
			t.Fatalf("Save(%q) failed: %v", tt.componentID, err)
		}
	}

	// 2. Load and verify all checkpoints
	for _, tt := range tests {
		loaded, err := store.Load(ctx, tt.componentID)
		if err != nil {
			t.Fatalf("Load(%q) failed: %v", tt.componentID, err)
		}
		if string(loaded) != string(tt.token) {
			t.Errorf("Load(%q) = %q, want %q", tt.componentID, loaded, tt.token)
		}
	}

	// 3. Update (overwrite) a checkpoint
	newToken := []byte("lsn:99999")
	if err := store.Save(ctx, "producer/orders", newToken); err != nil {
		t.Fatalf("Save (update) failed: %v", err)
	}
	loaded, err := store.Load(ctx, "producer/orders")
	if err != nil {
		t.Fatalf("Load (after update) failed: %v", err)
	}
	if string(loaded) != string(newToken) {
		t.Errorf("Load after update = %q, want %q", loaded, newToken)
	}

	// 4. Load non-existent key → should return (nil, nil)
	missing, err := store.Load(ctx, "consumer/does-not-exist/sink")
	if err != nil {
		t.Fatalf("Load(non-existent) returned error: %v", err)
	}
	if missing != nil {
		t.Errorf("Load(non-existent) = %v, want nil", missing)
	}

	// 5. Delete a checkpoint
	if err := store.Delete(ctx, "producer/orders"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	deleted, err := store.Load(ctx, "producer/orders")
	if err != nil {
		t.Fatalf("Load (after delete) failed: %v", err)
	}
	if deleted != nil {
		t.Errorf("Load after delete = %v, want nil", deleted)
	}

	// 6. Delete non-existent key → should return nil (idempotent)
	if err := store.Delete(ctx, "producer/does-not-exist"); err != nil {
		t.Fatalf("Delete(non-existent) failed: %v", err)
	}
}

// TestBoltStore_List tests listing all checkpoint keys.
func TestBoltStore_List(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_list.db")
	ctx := context.Background()

	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("NewBoltStore failed: %v", err)
	}
	defer store.Close()

	// Save 3 checkpoints
	_ = store.Save(ctx, "producer/p1", []byte("lsn:1"))
	_ = store.Save(ctx, "consumer/p1/sink-a", []byte("offset:2"))
	_ = store.Save(ctx, "consumer/p1/sink-b", []byte("offset:3"))

	keys, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(keys) != 3 {
		t.Fatalf("List returned %d keys, want 3", len(keys))
	}

	// Verify all expected keys are present
	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}

	expected := []string{"producer/p1", "consumer/p1/sink-a", "consumer/p1/sink-b"}
	for _, e := range expected {
		if !keySet[e] {
			t.Errorf("List missing key %q", e)
		}
	}
}

// TestBoltStore_Check tests database integrity verification (UQ-11).
func TestBoltStore_Check(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_check.db")
	ctx := context.Background()

	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("NewBoltStore failed: %v", err)
	}
	defer store.Close()

	// Save some data
	_ = store.Save(ctx, "producer/test", []byte("lsn:42"))

	// Check should pass on healthy DB
	if err := store.Check(); err != nil {
		t.Fatalf("Check failed on healthy DB: %v", err)
	}
}

// TestBoltStore_Compact tests space reclamation (UQ-18).
func TestBoltStore_Compact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_compact.db")
	ctx := context.Background()

	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("NewBoltStore failed: %v", err)
	}

	// Write and overwrite many times to create garbage
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("producer/pipeline-%d", i%10)
		token := fmt.Sprintf("lsn:%d", i)
		_ = store.Save(ctx, key, []byte(token))
	}

	// Delete some keys to create more garbage
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("producer/pipeline-%d", i)
		_ = store.Delete(ctx, key)
	}

	// Verify data before compact
	before, err := store.Load(ctx, "producer/pipeline-5")
	if err != nil {
		t.Fatalf("Load before compact failed: %v", err)
	}

	// Compact
	if err := store.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify data is preserved after compact
	after, err := store.Load(ctx, "producer/pipeline-5")
	if err != nil {
		t.Fatalf("Load after compact failed: %v", err)
	}
	if string(before) != string(after) {
		t.Errorf("Data changed after compact: before=%q, after=%q", before, after)
	}

	// Verify deleted keys are still deleted
	deleted, err := store.Load(ctx, "producer/pipeline-0")
	if err != nil {
		t.Fatalf("Load deleted key after compact failed: %v", err)
	}
	if deleted != nil {
		t.Errorf("Deleted key reappeared after compact: %v", deleted)
	}

	// Verify List still works after compact
	keys, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List after compact failed: %v", err)
	}
	if len(keys) != 5 { // 10 - 5 deleted = 5
		t.Errorf("List after compact returned %d keys, want 5", len(keys))
	}

	store.Close()
}

// TestBoltStore_MemoryMapSafety proves that the copied slice in Load()
// remains valid even after the BoltDB transaction is closed.
//
// This is the #1 BoltDB pitfall: returning mmap'd slices directly
// causes data corruption when accessed outside the transaction.
func TestBoltStore_MemoryMapSafety(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_mmap.db")
	ctx := context.Background()

	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("NewBoltStore failed: %v", err)
	}
	defer store.Close()

	original := []byte("hello world checkpoint")
	_ = store.Save(ctx, "test/safety", original)

	// Load the data
	loaded, err := store.Load(ctx, "test/safety")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Mutate the loaded slice.
	// If Load() returned the mmap'd slice directly (BUG), this would
	// corrupt BoltDB's internal data structures.
	loaded[0] = 'X'

	// Load again to verify the DB was NOT corrupted
	reloaded, err := store.Load(ctx, "test/safety")
	if err != nil {
		t.Fatalf("Reload failed: %v", err)
	}
	if string(reloaded) != string(original) {
		t.Fatalf("Memory map safety FAILED! DB was corrupted.\nGot: %q\nWant: %q", reloaded, original)
	}
}

// TestBoltStore_ConcurrentAccess tests thread safety under concurrent reads/writes.
// BoltDB allows multiple concurrent readers but only one writer at a time.
func TestBoltStore_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_concurrent.db")
	ctx := context.Background()

	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("NewBoltStore failed: %v", err)
	}
	defer store.Close()

	const numWriters = 5
	const numReaders = 10
	const numOps = 20

	var wg sync.WaitGroup

	// Concurrent writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				key := fmt.Sprintf("producer/pipeline-%d", writerID)
				token := fmt.Sprintf("lsn:%d-%d", writerID, i)
				if err := store.Save(ctx, key, []byte(token)); err != nil {
					t.Errorf("Writer %d Save failed: %v", writerID, err)
					return
				}
			}
		}(w)
	}

	// Concurrent readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				key := fmt.Sprintf("producer/pipeline-%d", readerID%numWriters)
				_, err := store.Load(ctx, key)
				if err != nil {
					t.Errorf("Reader %d Load failed: %v", readerID, err)
					return
				}
			}
		}(r)
	}

	wg.Wait()

	// Verify final state: each writer's last write should be present
	for w := 0; w < numWriters; w++ {
		key := fmt.Sprintf("producer/pipeline-%d", w)
		loaded, err := store.Load(ctx, key)
		if err != nil {
			t.Fatalf("Final Load(%q) failed: %v", key, err)
		}
		if loaded == nil {
			t.Errorf("Final Load(%q) returned nil", key)
		}
	}
}

// TestBoltStore_Validation tests input validation.
func TestBoltStore_Validation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_validation.db")
	ctx := context.Background()

	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("NewBoltStore failed: %v", err)
	}
	defer store.Close()

	// Empty componentID should fail
	if err := store.Save(ctx, "", []byte("token")); err == nil {
		t.Error("Save with empty componentID should fail")
	}

	// Nil token should fail
	if err := store.Save(ctx, "producer/test", nil); err == nil {
		t.Error("Save with nil token should fail")
	}
}

// TestBoltStore_CloseIdempotent tests that Close() is safe to call multiple times.
func TestBoltStore_CloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_close.db")

	store, err := NewBoltStore(path)
	if err != nil {
		t.Fatalf("NewBoltStore failed: %v", err)
	}

	// Close multiple times should not panic or error
	if err := store.Close(); err != nil {
		t.Fatalf("First Close failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Second Close failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Third Close failed: %v", err)
	}
}
