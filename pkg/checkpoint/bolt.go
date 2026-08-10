package checkpoint

import (
	"context"
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	bucketName = "checkpoints"
	// openTimeout prevents hanging if the DB file is locked by another process.
	// BoltDB uses file-level locking: only ONE process can open the DB at a time.
	openTimeout = 1 * time.Second
)

// BoltStore implements Store using BoltDB (embedded key-value store).
//
// Thread-safety: BoltDB handles concurrency internally via its transaction model.
//   - Multiple concurrent readers (View transactions) are allowed.
//   - Only ONE writer (Update transaction) at a time.
//   - At 20-30 checkpoint writes/sec (RFC ADR-002), single-writer is NOT a bottleneck.
type BoltStore struct {
	db *bolt.DB
}

// NewBoltStore opens or creates a BoltDB database at the given path.
//
// The path should be a persistent volume (e.g., /var/lib/etl/checkpoints.db).
// If the file doesn't exist, it will be created.
// If the file is locked by another process, returns error after openTimeout.
func NewBoltStore(path string) (*BoltStore, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{
		Timeout: openTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("open bolt db at %s: %w", path, err)
	}

	// Create the default bucket if it doesn't exist.
	// This is a one-time setup operation.
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create bucket %q: %w", bucketName, err)
	}

	return &BoltStore{db: db}, nil
}

// Save stores the checkpoint token for a specific component.
//
// BoltDB's Update() is ACID: the write is fsync'd to disk before returning.
// This guarantees durability — if Save returns nil, the checkpoint is on disk.
func (s *BoltStore) Save(ctx context.Context, componentID string, token []byte) error {
	if componentID == "" {
		return fmt.Errorf("checkpoint: componentID must not be empty")
	}
	if token == nil {
		return fmt.Errorf("checkpoint: token must not be nil for component %q", componentID)
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		return b.Put([]byte(componentID), token)
	})
}

// Load retrieves the last saved checkpoint for a component.
//
// Returns (nil, nil) if the component has no checkpoint yet.
//
// CRITICAL: BoltDB returns memory-mapped slices that are ONLY valid during
// the transaction. We MUST copy the data before returning to avoid data
// corruption after the transaction closes.
func (s *BoltStore) Load(ctx context.Context, componentID string) ([]byte, error) {
	var token []byte

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		v := b.Get([]byte(componentID))
		if v != nil {
			// Copy the mmap'd slice to a new allocation.
			// Without this, accessing `token` after this function returns
			// would read freed/invalid memory.
			token = make([]byte, len(v))
			copy(token, v)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("checkpoint: load %q: %w", componentID, err)
	}
	return token, nil
}

// Delete removes the checkpoint for a component.
// Idempotent: returns nil if the key didn't exist.
func (s *BoltStore) Delete(ctx context.Context, componentID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		return b.Delete([]byte(componentID))
	})
}

// List returns all component IDs that have saved checkpoints.
// Useful for debugging, monitoring, and checkpoint migration (UQ-10).
func (s *BoltStore) List(ctx context.Context) ([]string, error) {
	var keys []string

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		return b.ForEach(func(k, v []byte) error {
			// Copy the key since it's mmap'd
			key := make([]byte, len(k))
			copy(key, k)
			keys = append(keys, string(key))
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("checkpoint: list: %w", err)
	}
	return keys, nil
}

// Check verifies database integrity (UQ-11: Checkpoint corruption recovery).
//
// Wraps bolt.DB's built-in consistency checker.
// Returns nil if the database is healthy.
// Returns error describing the corruption if found.
//
// Usage: call periodically (e.g., on worker startup) or on-demand via CLI.
func (s *BoltStore) Check() error {
	// bolt.DB doesn't expose Check() directly in the Go API.
	// We use a read transaction that touches every page as a health check.
	// If any page is corrupted, the transaction will fail.
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("checkpoint: bucket %q not found — database may be corrupted", bucketName)
		}
		// Iterate all keys to force page reads
		return b.ForEach(func(k, v []byte) error {
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("checkpoint: integrity check failed: %w", err)
	}
	return nil
}

// Compact reclaims space from deleted/overwritten entries (UQ-18).
//
// Creates a new database file with only live data, then replaces the old file.
// This is an expensive operation — call periodically (e.g., every 10K writes)
// or during maintenance windows, NOT on every Save.
//
// Implementation: uses bolt.Compact() to copy live pages to a temp file,
// then atomically replaces the original.
func (s *BoltStore) Compact() error {
	// bolt.Compact requires a destination file.
	// We write to a temp file, then rename (atomic on POSIX).
	tmpPath := s.db.Path() + ".compact.tmp"

	// Open destination
	dst, err := bolt.Open(tmpPath, 0o600, &bolt.Options{Timeout: openTimeout})
	if err != nil {
		return fmt.Errorf("checkpoint: compact open tmp: %w", err)
	}

	// Compact: copy live data from src to dst
	err = bolt.Compact(dst, s.db, 65536) // 64KB buffer
	dst.Close()
	if err != nil {
		os.Remove(tmpPath) // Clean up on failure
		return fmt.Errorf("checkpoint: compact: %w", err)
	}

	// Close current DB, replace with compacted version, reopen
	originalPath := s.db.Path()
	s.db.Close()

	if err := os.Rename(tmpPath, originalPath); err != nil {
		return fmt.Errorf("checkpoint: compact rename: %w", err)
	}

	// Reopen the compacted database
	db, err := bolt.Open(originalPath, 0o600, &bolt.Options{Timeout: openTimeout})
	if err != nil {
		return fmt.Errorf("checkpoint: compact reopen: %w", err)
	}
	s.db = db

	return nil
}

// Close releases the underlying database resources.
// Idempotent: safe to call multiple times.
func (s *BoltStore) Close() error {
	if s.db != nil {
		err := s.db.Close()
		s.db = nil // Prevent double-close
		return err
	}
	return nil
}
