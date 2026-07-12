package stream

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/aula-id/etl-pipeline-go/pkg/pipeline"
)

// InProcStream implements Stream using a bounded channel and a spillover file.
type InProcStream struct {
	mu         sync.RWMutex
	ch         chan pipeline.RecordBatch
	readers    []chan pipeline.RecordBatch
	file       *os.File
	offsetFile *os.File
	dir        string
	capacity   int

	ctx    context.Context
	cancel context.CancelFunc
}

// NewInProcStream creates a new stream with a spillover directory and channel capacity.
func NewInProcStream(dir string, capacity int) (*InProcStream, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create spillover dir: %w", err)
	}

	return &InProcStream{
		dir:      dir,
		capacity: capacity,
	}, nil
}

func (s *InProcStream) Init(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.ch = make(chan pipeline.RecordBatch, s.capacity)

	// Open spillover file (append-only)
	var err error
	s.file, err = os.OpenFile(filepath.Join(s.dir, "spillover.dat"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	// Open offset file (tracks committed byte position)
	s.offsetFile, err = os.OpenFile(filepath.Join(s.dir, "committed.offset"), os.O_CREATE|os.O_RDWR, 0o644)
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
	// Close all reader channels so Read() returns io.EOF
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
	if s.offsetFile != nil {
		s.offsetFile.Close()
	}
	return nil
}

func (s *InProcStream) Writer() StreamWriter {
	return &inProcWriter{stream: s}
}

func (s *InProcStream) Reader() StreamReader {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Pub/Sub: Each reader gets its own buffered channel
	rCh := make(chan pipeline.RecordBatch, s.capacity)
	s.readers = append(s.readers, rCh)

	return &inProcReader{stream: s, ch: rCh}
}

// --- Internal Writer Logic ---

type inProcWriter struct {
	stream *InProcStream
}

func (w *inProcWriter) Publish(ctx context.Context, batch pipeline.RecordBatch) error {
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

	// FIX: Get the exact file size AFTER writing this specific batch
	info, err := w.stream.file.Stat()
	if err != nil {
		return err
	}
	// Assign the exact offset to the batch struct
	batch.StreamOffset = info.Size()

	if err := w.stream.file.Sync(); err != nil {
		return err
	}

	for _, rCh := range w.stream.readers {
		select {
		case rCh <- batch: // Send the batch WITH the correct StreamOffset attached
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
	stream     *InProcStream
	ch         chan pipeline.RecordBatch
	lastOffset int64 // FIX: Tracks the offset of the last read batch
}

func (r *inProcReader) Read(ctx context.Context) (pipeline.RecordBatch, error) {
	select {
	case batch, ok := <-r.ch:
		if !ok {
			return pipeline.RecordBatch{}, io.EOF
		}
		// FIX: Save the offset from the batch we just read
		r.lastOffset = batch.StreamOffset
		return batch, nil
	case <-ctx.Done():
		return pipeline.RecordBatch{}, ctx.Err()
	}
}

func (r *inProcReader) Commit(ctx context.Context, token pipeline.CheckpointToken) error {
	r.stream.mu.Lock()
	defer r.stream.mu.Unlock()

	if r.lastOffset == 0 {
		return nil
	}

	r.stream.offsetFile.Truncate(0)
	r.stream.offsetFile.Seek(0, 0)

	// FIX: Write the EXACT offset of the committed batch, not the total file size
	_, err := r.stream.offsetFile.Write([]byte(fmt.Sprintf("%d", r.lastOffset)))
	return err
}

// --- Replay Logic ---

func (s *InProcStream) replayUncommitted() error {
	var committedOffset int64
	offsetData, _ := io.ReadAll(s.offsetFile)
	if len(offsetData) > 0 {
		fmt.Sscanf(string(offsetData), "%d", &committedOffset)
	}

	info, err := s.file.Stat()
	if err != nil {
		return err
	}

	if info.Size() <= committedOffset {
		return nil
	}

	if _, err := s.file.Seek(committedOffset, 0); err != nil {
		return err
	}

	reader := io.LimitReader(s.file, info.Size()-committedOffset)
	lenBuf := make([]byte, 4)

	// FIX: Track the current byte position as we read
	currentOffset := committedOffset

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

		// FIX: Update offset (4 bytes for length prefix + length of data)
		currentOffset += int64(4) + int64(length)

		var batch pipeline.RecordBatch
		if err := json.Unmarshal(data, &batch); err != nil {
			return err
		}

		// FIX: Assign the correct offset to the replayed batch
		batch.StreamOffset = currentOffset

		s.ch <- batch
	}

	return nil
}
