package stream

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/aula-id/etl-pipeline-go/pkg/checkpoint"
	"github.com/aula-id/etl-pipeline-go/pkg/model"
)

// InProcStream implements Stream using a bounded channel and a spillover file.
type InProcStream struct {
	mu       sync.RWMutex
	ch       chan model.RecordBatch
	readers  []chan model.RecordBatch
	file     *os.File
	dir      string
	capacity int

	// Phase 2: Checkpoint integration
	checkpointStore checkpoint.Store
	pipelineID      string
	logger          *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

// NewInProcStream creates a new stream with a spillover directory and channel capacity.
// checkpointStore and pipelineID are optional (can be nil/"") for Phase 1 backward compatibility.
func NewInProcStream(dir string, capacity int, ckptStore checkpoint.Store, pipelineID string, logger *slog.Logger) (*InProcStream, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create spillover dir: %w", err)
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &InProcStream{
		dir:             dir,
		capacity:        capacity,
		checkpointStore: ckptStore,
		pipelineID:      pipelineID,
		logger:          logger,
	}, nil
}

func (s *InProcStream) Init(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.ch = make(chan model.RecordBatch, s.capacity)

	// Open spillover file (append-only)
	var err error
	s.file, err = os.OpenFile(filepath.Join(s.dir, "spillover.dat"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	// REPLAY LOGIC: Read uncommitted data from file and push to channel
	if err := s.replayUncommitted(); err != nil {
		return fmt.Errorf("replay failed: %w", err)
	}

	return nil
}

func (s *InProcStream) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (s *InProcStream) Drain(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.readers {
		close(r)
	}
	s.readers = nil
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

func (s *InProcStream) Close() error {
	if s.file != nil {
		s.file.Close()
	}
	return nil
}

func (s *InProcStream) Writer() StreamWriter {
	return &inProcWriter{stream: s}
}

func (s *InProcStream) Reader() StreamReader {
	s.mu.Lock()
	defer s.mu.Unlock()

	rCh := make(chan model.RecordBatch, s.capacity)
	s.readers = append(s.readers, rCh)

	return &inProcReader{stream: s, ch: rCh}
}

// --- Internal Writer Logic ---

type inProcWriter struct {
	stream *InProcStream
}

func (w *inProcWriter) Publish(ctx context.Context, batch model.RecordBatch) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return err
	}

	w.stream.mu.Lock()
	defer w.stream.mu.Unlock()

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	if _, err := w.stream.file.Write(lenBuf); err != nil {
		return err
	}
	if _, err := w.stream.file.Write(data); err != nil {
		return err
	}

	if err := w.stream.file.Sync(); err != nil {
		return err
	}

	for _, rCh := range w.stream.readers {
		select {
		case rCh <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (w *inProcWriter) Flush(ctx context.Context) error {
	return w.stream.file.Sync()
}

// --- Internal Reader Logic ---

type inProcReader struct {
	stream *InProcStream
	ch     chan model.RecordBatch
}

func (r *inProcReader) Read(ctx context.Context) (model.RecordBatch, error) {
	select {
	case batch, ok := <-r.ch:
		if !ok {
			return model.RecordBatch{}, io.EOF
		}
		return batch, nil
	case <-ctx.Done():
		return model.RecordBatch{}, ctx.Err()
	}
}

func (r *inProcReader) Commit(ctx context.Context, token model.CheckpointToken) error {
	// Phase 2: Commit is now handled by Consumer directly to CheckpointStore
	// This method is kept for interface compatibility but does nothing
	return nil
}

// --- Replay Logic ---

func (s *InProcStream) replayUncommitted() error {
	// Phase 1 backward compatibility: if no checkpoint store, replay all
	if s.checkpointStore == nil || s.pipelineID == "" {
		return s.replayAll()
	}

	// Phase 2: Smart replay based on consumer checkpoints

	// 1. Read all batches from spillover file
	batches, err := s.readAllBatches()
	if err != nil {
		return fmt.Errorf("read batches: %w", err)
	}

	if len(batches) == 0 {
		return nil
	}

	// 2. Get all consumer checkpoints from BoltDB
	consumerCheckpoints, err := s.getConsumerCheckpoints()
	if err != nil {
		s.logger.Error("failed to get consumer checkpoints, replaying all", "error", err)
		return s.replayBatches(batches)
	}

	// 3. For each batch, check if any consumer needs replay
	replayCount := 0
	for _, batch := range batches {
		if s.needsReplay(batch.Checkpoint, consumerCheckpoints) {
			s.ch <- batch
			replayCount++
		}
	}

	s.logger.Info("replay completed",
		"total_batches", len(batches),
		"replayed", replayCount,
		"skipped", len(batches)-replayCount,
	)

	return nil
}

// replayAll replays all batches from spillover file (Phase 1 behavior)
func (s *InProcStream) replayAll() error {
	info, err := s.file.Stat()
	if err != nil {
		return err
	}

	if info.Size() == 0 {
		return nil
	}

	if _, err := s.file.Seek(0, 0); err != nil {
		return err
	}

	reader := io.LimitReader(s.file, info.Size())
	lenBuf := make([]byte, 4)

	for {
		_, err := io.ReadFull(reader, lenBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return err
		}

		length := binary.BigEndian.Uint32(lenBuf)
		data := make([]byte, length)

		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}

		var batch model.RecordBatch
		if err := json.Unmarshal(data, &batch); err != nil {
			return err
		}

		s.ch <- batch
	}

	return nil
}

// readAllBatches reads all batches from spillover file into memory
func (s *InProcStream) readAllBatches() ([]model.RecordBatch, error) {
	info, err := s.file.Stat()
	if err != nil {
		return nil, err
	}

	if info.Size() == 0 {
		return nil, nil
	}

	if _, err := s.file.Seek(0, 0); err != nil {
		return nil, err
	}

	var batches []model.RecordBatch
	reader := io.LimitReader(s.file, info.Size())
	lenBuf := make([]byte, 4)

	for {
		_, err := io.ReadFull(reader, lenBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}

		length := binary.BigEndian.Uint32(lenBuf)
		data := make([]byte, length)

		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}

		var batch model.RecordBatch
		if err := json.Unmarshal(data, &batch); err != nil {
			return nil, err
		}

		batches = append(batches, batch)
	}

	return batches, nil
}

// replayBatches pushes all batches to the channel
func (s *InProcStream) replayBatches(batches []model.RecordBatch) error {
	for _, batch := range batches {
		s.ch <- batch
	}
	return nil
}

// getConsumerCheckpoints retrieves all consumer checkpoints for this pipeline
// Returns map[sinkID]checkpoint
func (s *InProcStream) getConsumerCheckpoints() (map[string][]byte, error) {
	if s.checkpointStore == nil {
		return nil, nil
	}

	keys, err := s.checkpointStore.List(s.ctx)
	if err != nil {
		return nil, err
	}

	checkpoints := make(map[string][]byte)
	prefix := fmt.Sprintf("consumer/%s/", s.pipelineID)

	for _, key := range keys {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			// Extract sink ID from key: "consumer/pipeline/sink_id" → "sink_id"
			sinkID := key[len(prefix):]
			token, err := s.checkpointStore.Load(s.ctx, key)
			if err != nil {
				s.logger.Error("failed to load checkpoint", "key", key, "error", err)
				continue
			}
			if token != nil {
				checkpoints[sinkID] = token
			}
		}
	}

	return checkpoints, nil
}

// needsReplay checks if a batch needs to be replayed based on consumer checkpoints
// Returns true if at least one consumer has checkpoint < batch checkpoint
func (s *InProcStream) needsReplay(batchCheckpoint []byte, consumerCheckpoints map[string][]byte) bool {
	// If no consumers have checkpoints, all batches need replay
	if len(consumerCheckpoints) == 0 {
		return true
	}

	// Check each consumer's checkpoint
	for sinkID, consumerCheckpoint := range consumerCheckpoints {
		if compareCheckpoints(consumerCheckpoint, batchCheckpoint) < 0 {
			// Consumer checkpoint < batch checkpoint → needs replay
			s.logger.Debug("consumer needs replay",
				"sink", sinkID,
				"consumer_checkpoint", string(consumerCheckpoint),
				"batch_checkpoint", string(batchCheckpoint),
			)
			return true
		}
	}

	// All consumers have checkpoint >= batch checkpoint → skip
	return false
}

// compareCheckpoints compares two checkpoint tokens
// Returns:
//
//	-1 if a < b (a is behind b)
//	 0 if a == b
//	 1 if a > b (a is ahead of b)
//
// For Phase 2, we use lexicographic comparison which works for:
//   - "seq:1", "seq:2", "seq:3" (MemorySource)
//   - LSN strings (Postgres CDC)
//   - Offset strings (Kafka)
//
// TODO RFC 0002: Refine comparison logic per source type
func compareCheckpoints(a, b []byte) int {
	return bytes.Compare(a, b)
}
