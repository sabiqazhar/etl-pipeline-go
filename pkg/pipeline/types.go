package pipeline

import "time"

// reecord represent a single data record
type Record struct {
	Key       []byte            `json:"key"`
	Payload   []byte            `json:"payload"`
	Headers   map[string]string `json:"headers"`
	Timestamp time.Time         `json:"timestamp"`
}

// CheckpointToken is an opaque maker for source position
type CheckpointToken []byte

// RecordBatch is a group of records processed together.
type RecordBatch struct {
	ID           string          `json:"id"` // for debugging/tracing
	Records      []Record        `json:"records"`
	Checkpoint   CheckpointToken `json:"Checkpoint"`
	StreamOffset int64           `json:"-"`
}
