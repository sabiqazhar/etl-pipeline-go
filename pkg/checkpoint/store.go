package checkpoint

import "context"

// Store is the contract for persisting pipeline checkpoints.
//
// Key format:
//   - "producer/<pipeline_id>"           → source extraction position
//   - "consumer/<pipeline_id>/<sink_id>" → per-sink load position
//   - "stream/<pipeline_id>"             → stream replay offset

type Store interface {
	// Save stores the checkpoint token for a specific component.
	// Overwrites any existing checkpoint for the same componentID.
	// componentID must follow the key format above.
	Save(ctx context.Context, componentID string, token []byte) error

	// Load retrieves the last saved checkpoint for a component.
	// Returns (nil, nil) if the component has no checkpoint yet.
	// Returns (nil, error) only on actual I/O failure.
	Load(ctx context.Context, componentID string) ([]byte, error)

	// Delete removes the checkpoint for a component.
	// Returns nil if the key didn't exist (idempotent).
	Delete(ctx context.Context, componentID string) error

	// List returns all component IDs that have saved checkpoints.
	// Useful for debugging, monitoring, and checkpoint migration (UQ-10).
	List(ctx context.Context) ([]string, error)

	// Check verifies database integrity (UQ-11).
	// Returns nil if the database is healthy.
	// Wraps bolt.DB.Check() internally.
	Check() error

	// Compact reclaims space from deleted/overwritten entries (UQ-18).
	// Should be called periodically (e.g., every 10K writes) or on-demand.
	// Wraps bolt.DB.Compact() internally.
	Compact() error

	// Close releases the underlying database resources.
	// Must be idempotent (safe to call multiple times).
	Close() error
}
