# RFC 0002: Phase 1 — Core Lifecycle, InProcStream & Worker Skeleton

> **TL;DR**: Define the Go package layout, concrete component implementations, YAML config schema, and debug CLI for Phase 1 — a runnable worker that boots a single-pipeline from any `Source` to `io.Writer` via in-memory InProcStream, supervised with panic recovery and exponential-backoff restart.

- **Date**: 2026-06-09
- **Status**: Draft
- **Author**: @sabiqazhar
- **PR**: [Link to GitHub PR]
- **Supersedes**: None
- **Superseded By**: None

---

## 1. Motivation

[RFC 0001](./0001-pipeline-core-architecture.md) defines the high-level architecture and interfaces. Before we can iterate on checkpointing, DLQ, transforms, or clustering, we need a **working binary** that exercises every core abstraction end-to-end.

Phase 1 delivers this skeleton: a `go run cmd/worker/main.go` that reads a YAML config, boots a pipeline with a real `Source`, streams records through an in-memory `InProcStream`, delivers them to a `Sink`, and survives panics via supervised restart. Every interface from RFC 0001 gets a concrete implementation.

This RFC is the **detailed implementation blueprint** for Phase 1. It resolves all open design questions needed to write code — package layout, type definitions, concurrency model, config schema, CLI flags, and implementation ordering.

---

## 2. Proposed Change

### 2.1 Project Layout

```
etl-pipeline-go/
├── cmd/
│   └── worker/
│       └── main.go              # Entry point: parse flags, load config, start Worker
├── internal/
│   ├── lifecycle/
│   │   └── lifecycle.go         # Lifecycle interface, RunFunc adapter, lifecycle states
│   ├── record/
│   │   └── record.go            # Record, RecordBatch, meta types
│   ├── stream/
│   │   ├── stream.go            # Stream, StreamWriter, StreamReader interfaces
│   │   └── inproc.go            # InProcStream — bounded ring-buffer in memory
│   ├── source/
│   │   ├── source.go            # Source interface
│   │   └── stdin.go             # StdinSource — reads newline-delimited JSON from stdin
│   ├── sink/
│   │   ├── sink.go              # Sink interface
│   │   └── stdout.go            # StdoutSink — writes records as JSON to stdout
│   ├── pipeline/
│   │   └── pipeline.go          # Pipeline — wires Producer→Stream→Consumer(s), orchestrates drain order
│   ├── supervisor/
│   │   └── supervisor.go        # Supervisor — manages child goroutines, panic recovery, backoff, watchdog
│   └── config/
│       ├── config.go            # Config struct, LoadConfig(path) function
│       └── testdata/
│           └── simple.yaml      # Sample config for manual testing
├── go.mod
└── go.sum
```

**Rationale**: All production code lives under `internal/` to enforce encapsulation. The `cmd/worker/main.go` is the only public package. Source and sink implementations start with trivial debug adapters (stdin/stdout); real connectors (Kafka, Postgres, filesystem) come in later phases or as external plugins.

### 2.2 Core Types

#### Record & RecordBatch

```go
// package record

// Record is a single data unit flowing through the pipeline.
// For Phase 1, Payload is an opaque []byte. Schema-aware types arrive in a later RFC.
type Record struct {
    ID      string // UUID v7 set by Producer for dedup tracing
    Payload []byte // raw bytes (JSON, protobuf, CSV row, etc.)
    Headers map[string]string // metadata: content-type, origin, retry-count, etc.
}

// RecordBatch is the atomic unit of transport between Producer, Stream, and Consumer.
// All records in a batch share the same fate: either all delivered or all retried.
type RecordBatch struct {
    ID      string    // batch UUID v7
    Records []Record
    Size    int       // total bytes of payloads (pre-computed for backpressure)
}
```

#### Lifecycle (moved from interfaces-only to concrete state machine)

```go
// package lifecycle

// State represents the current phase of a Lifecycle component.
type State int

const (
    StateCreated  State = iota // before Init
    StateInit                  // Init succeeded
    StateRunning               // Run is executing
    StateDraining              // Drain in progress
    StateStopped               // Drain + Close completed
    StateFailed                // irrecoverable error
)

// Lifecycle is the universal contract (identical to RFC 0001 semantics).
type Lifecycle interface {
    Init(context.Context) error
    Run(context.Context) error
    Drain(context.Context) error
    Close() error
}

// RunFunc is a helper adapter: converts a function into a Lifecycle.
// The function is the Run body; Init and Drain are no-ops; Close is no-op.
func RunFunc(fn func(context.Context) error) Lifecycle { ... }

// Component wraps a Lifecycle with state tracking, panic shield, and error recording.
type Component struct {
    Name      string
    state     State
    err       error
    lifecycle Lifecycle
    mu        sync.Mutex
}

func NewComponent(name string, lc Lifecycle) *Component { ... }
func (c *Component) Init(ctx context.Context) error      { ... } // transitions Created→Init
func (c *Component) Run(ctx context.Context) error       { ... } // transitions Init→Running
func (c *Component) Drain(ctx context.Context) error     { ... } // transitions Running→Draining
func (c *Component) Close() error                        { ... } // transitions *→Stopped
func (c *Component) State() State                        { ... }
func (c *Component) Err() error                          { ... }
```

#### Source & StdinSource

```go
// package source

type Source interface {
    lifecycle.Lifecycle
    Records() <-chan record.RecordBatch
}

// StdinSource reads newline-delimited JSON from os.Stdin.
// Each line becomes one RecordBatch with a single Record.
// Sends one batch per line until EOF (graceful drain) or ctx cancel.
// The Run loop selects on ctx.Done() alongside scanner.Scan() so that
// SIGINT/SIGTERM immediately interrupts stdin reads.
type StdinSource struct {
    ch      chan record.RecordBatch
    scanner *bufio.Scanner
    done    chan struct{}
}
```

#### Sink & StdoutSink

```go
// package sink

type Sink interface {
    lifecycle.Lifecycle
    Load(context.Context, record.RecordBatch) error
}

// StdoutSink writes each record as indented JSON to the configured io.Writer (default os.Stdout).
// Load is a simple marshal+write. No batching, no commit — Phase 1 debug only.
type StdoutSink struct {
    writer io.Writer
    enc    *json.Encoder
}
```

#### Stream & InProcStream

```go
// package stream

// Stream interfaces — exact copies of RFC 0001.
type Stream interface {
    lifecycle.Lifecycle
    Writer() StreamWriter
    Reader() StreamReader
}

type StreamWriter interface {
    Publish(context.Context, record.RecordBatch) error
    Flush(context.Context) error
}

type StreamReader interface {
    Read(context.Context) (record.RecordBatch, error)
    Commit(context.Context, uint64) error  // seqnum for Phase 1 (no-op)
}

// InProcStream is a bounded in-memory ring-buffer with explicit reader lifecycle.
// - One writer, N independent readers (fan-out via copy-on-read; Phase 1 returns
//   references — consumers must treat returned batches as immutable).
// - Blocking on full buffer: Publish blocks until space frees (backpressure).
// - io.EOF on Read after Close + all batches consumed.
// - No spillover in Phase 1 (added in Phase 2 alongside checkpointing).
// - Context cancellation is correctly handled via mutex-held Broadcast (see below).
type InProcStream struct {
    cap     int            // max batches in flight
    batches []*batchSlot   // ring buffer
    wp      int            // write cursor
    rp      []*readerState // read cursors per reader (pointer for dynamic removal)
    mu      sync.Mutex
    cond    *sync.Cond
    closed  bool
    ctx     context.Context
}

// readerState tracks one reader's position within the ring buffer.
type readerState struct {
    pos      int
    detached bool
}

var ErrStreamClosed = errors.New("stream: closed")
```

**Fan-out semantics**: When a writer publishes a batch, it is appended to the ring. Each reader maintains its own read cursor (created via `NewReader()`, removed via `RemoveReader()`). A reader sees every batch written after it joined. Readers can lag independently — a slow reader blocks `Publish` once the ring fills, affecting all consumers (backpressure). This is acceptable for Phase 1; spillover (Phase 2) removes the blocking.

**Context-safe blocking**: Publish and Read use `sync.Cond.Wait()` in a loop that checks context cancellation. To prevent the lost-wakeup race (where `cond.Broadcast()` fires before `cond.Wait()` is called), a `context.AfterFunc` holds the mutex during Broadcast:

```go
// In InProcStream.Init or per-blocking-call setup:
context.AfterFunc(ctx, func() {
    s.mu.Lock()
    s.cond.Broadcast()
    s.mu.Unlock()
})
```

With this pattern, the Broadcast is serialized with the wait loop — the mutex guarantees that a wakeup cannot be lost between the context-cancellation check and `cond.Wait()`.

**Reader lifecycle**:
- `NewReader(ctx context.Context) *readerState` — appends a new cursor at current write position. The reader only sees batches published after it joins.
- `RemoveReader(r *readerState)` — marks the reader as detached (its cursor advances to the write position), allowing the ring to reclaim its tracked slot. The Supervisor's panic-recovery handler must call `RemoveReader` before restarting a consumer.
- Cursor advancement in Phase 1 happens on `Read()` (at-most-once delivery). `Commit()` is a no-op until Phase 2, when checkpointing enables at-least-once delivery with cursor advancement on `Commit()`.

**Copy-on-read semantics (Phase 1)**: `Read` returns a pointer to the ring slot's internal `*RecordBatch`. Consumers **must not mutate** the returned batch. Deep copies arrive in Phase 3 alongside the Transformer interface.

### 2.3 Pipeline Orchestration

```go
// package pipeline

// Pipeline wires a single data flow: Producer → Stream → Consumers.
// It owns the drain ordering: Consumers drain first, then Producer, then Stream.
type Pipeline struct {
    name      string
    stream    stream.Stream
    producer  *lifecycle.Component
    consumers []*lifecycle.Component
    sup       supervisor.Supervisor
    closeCh   chan struct{}
}

// New creates a Pipeline, wraps Producer and each Consumer as Components,
// registers them with the Supervisor, and creates the InProcStream.
func New(name string, src source.Source, sinks []sink.Sink, opts ...Option) (*Pipeline, error)

// Run starts the Supervisor, which in turn starts all children.
// Blocks until ctx is cancelled or a child enters permanent failure.
func (p *Pipeline) Run(ctx context.Context) error

// Drain orchestrates graceful shutdown:
//   1. Cancel pipeline context (signals all components)
//   2. Consumers detect cancellation → drain current Load → exit (read cursors advance)
//   3. Producer detects cancellation or gets ErrStreamClosed from Publish → exit
//   4. Stream.Drain() sets closed flag → Publish calls return ErrStreamClosed → readers get io.EOF after consuming remaining batches
//   5. Close all components
func (p *Pipeline) Drain(ctx context.Context) error
```

**Producer loop** (internal goroutine managed by Supervisor). Handles `ErrStreamClosed` from Publish during drain:

```
for {
    select {
    case <-ctx.Done():
        return ctx.Err()
    case batch, ok := <-source.Records():
        if !ok { return nil }  // source exhausted
        if err := stream.Writer().Publish(ctx, batch); err != nil {
            if errors.Is(err, ErrStreamClosed) {
                return nil  // normal drain — stream is shutting down
            }
            return err
        }
    }
}
```

`ErrStreamClosed` is returned by `Publish` when the stream is draining. This allows the producer to exit cleanly during shutdown even when the ring buffer is full and consumers have already stopped.

**Consumer loop** (internal goroutine managed by Supervisor):

```
for {
    batch, err := stream.Reader().Read(ctx)
    if err == io.EOF { return nil }
    if err != nil { return err }
    if err := sink.Load(ctx, batch); err != nil { return err }
    stream.Reader().Commit(ctx, seq)
}
```

### 2.4 Supervisor Design

```go
// package supervisor

// Supervisor manages a set of named children with automatic restart.
// It implements two failure-detection layers:
//   Layer 1 — panic recovery: recover() in each child goroutine, restarts child.
//   Layer 2 — watchdog: periodic health check; if a child has not advanced a monotonic
//             tick counter within the watchdog interval, it is considered hung and restarted.
type Supervisor struct {
    mu         sync.Mutex
    wg         sync.WaitGroup  // tracks all child goroutines for graceful drain
    children   map[string]*managedChild
    backoff    BackoffConfig
    watchdog   time.Duration   // 0 = disabled in Phase 1
    maxRetries int             // consecutive failures before permanent failure (default 5)
}

type BackoffConfig struct {
    Initial time.Duration // 1s
    Max     time.Duration // 30s
    Jitter  float64       // 0.2 (20% jitter)
}

type ChildOption func(*childConfig)

func WithBackoff(initial, max time.Duration, jitter float64) ChildOption { ... }
func WithWatchdog(interval time.Duration) ChildOption                     { ... }
func WithMaxRetries(n int) ChildOption                                    { ... }

func NewSupervisor() *Supervisor { ... }
func (s *Supervisor) AddChild(name string, lc lifecycle.Lifecycle, opts ...ChildOption) { ... }
func (s *Supervisor) Serve(ctx context.Context) error { ... }  // blocks until all children fully drain and exit (via wg.Wait()) or permanent failure
```

**Supervisor goroutine per child** (wrapped with `wg.Add(1)` / `wg.Done()`):

```
go func() {
    defer s.wg.Done()

    for attempts := 0; ; attempts++ {
        comp := lifecycle.NewComponent(name, lc)

        // Init
        if err := comp.Init(ctx); err != nil {
            // backoff + retry or permanent fail
            continue
        }

        // Run in a goroutine with recover()
        runErr := runWithPanicRecover(ctx, comp)

        // Drain
        drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        comp.Drain(drainCtx)
        cancel()
        comp.Close()

        if runErr == nil {
            return // clean exit
        }

        if attempts >= maxRetries {
            markPermanentFailed(name, runErr)
            return
        }

        select {
        case <-ctx.Done():
            return
        case <-time.After(calculateBackoff(attempts)):
        }
    }
}()
```

`Supervisor.Serve()` calls `s.wg.Wait()` after launching all children, ensuring it does not return until every child has fully completed its Drain/Close cycle. This prevents `Pipeline.Run()` from returning to `main()` before cleanup finishes.

### 2.5 Worker & Config

```go
// package config
// (lives in internal/config/)

type Config struct {
    Worker    WorkerConfig     `yaml:"worker"`
    Pipeline  PipelineConfig   `yaml:"pipeline"`
}

// NOTE: yaml.v3 does not honor `default` struct tags. The `default` values shown
// below are documentation-only. A `SetDefaults()` function applies them explicitly
// before unmarshal.

type WorkerConfig struct {
    Name             string  `yaml:"name"`             // default: "worker-1"
    Supervisor       SupervisorConfig `yaml:"supervisor"`
}

type SupervisorConfig struct {
    WatchdogInterval string `yaml:"watchdog_interval"` // default: "0" (disabled)
    MaxRetries       int    `yaml:"max_retries"`       // default: 5
    BackoffInitial   string `yaml:"backoff_initial"`   // default: "1s"
    BackoffMax       string `yaml:"backoff_max"`       // default: "30s"
}

type PipelineConfig struct {
    Name           string            `yaml:"name"`
    Source         SourceConfig      `yaml:"source"`
    Sinks          []SinkConfig      `yaml:"sinks"`
    StreamSize     int               `yaml:"stream_capacity"`   // default: 1000 (max batches in ring)
    MaxRecordBytes int               `yaml:"max_record_bytes"`  // default: 1048576 (1 MB)
    DrainTimeout   string            `yaml:"drain_timeout"`     // default: "10s"
}

type SourceConfig struct {
    Type   string         `yaml:"type"`          // discriminator: "stdin", "generator", etc.
    Config map[string]any `yaml:"config"`        // type-specific settings
}

type SinkConfig struct {
    Type   string         `yaml:"type"`          // discriminator: "stdout", "file", etc.
    Config map[string]any `yaml:"config"`        // type-specific settings
}

func LoadConfig(path string) (*Config, error) {
    cfg := &Config{}
    cfg.SetDefaults()  // apply defaults before unmarshal
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }
    if err := yaml.Unmarshal(data, cfg); err != nil {
        return nil, err
    }
    return cfg, cfg.validate()
}

func (c *Config) SetDefaults() {
    c.Worker.Name = "worker-1"
    c.Worker.Supervisor.MaxRetries = 5
    c.Worker.Supervisor.BackoffInitial = "1s"
    c.Worker.Supervisor.BackoffMax = "30s"
    c.Pipeline.StreamSize = 1000
    c.Pipeline.MaxRecordBytes = 1048576
    c.Pipeline.DrainTimeout = "10s"
}

func (c *Config) validate() error { ... }
```

**Sample config** (`internal/config/testdata/simple.yaml`):

```yaml
worker:
  name: worker-1
  supervisor:
    max_retries: 5
    backoff_initial: 1s
    backoff_max: 30s

pipeline:
  name: demo-pipeline
  stream_capacity: 1000
  source:
    type: stdin
    config:
      batch_size: 1
  sinks:
    - type: stdout
      config:
        pretty: true
```

**Source/Sink registry** (injected `Registry` struct, not plugin-based in Phase 1):

```go
// internal/source/registry.go

type Factory func(config map[string]any) (Source, error)

type Registry struct {
    factories map[string]Factory
}

func NewRegistry() *Registry {
    return &Registry{factories: make(map[string]Factory)}
}

func (r *Registry) Register(name string, fn Factory) {
    r.factories[name] = fn
}

func (r *Registry) Build(cfg SourceConfig) (Source, error) {
    fn, ok := r.factories[cfg.Type]
    if !ok { return nil, fmt.Errorf("unknown source type: %s", cfg.Type) }
    return fn(cfg.Config)
}
```

Similarly, `internal/sink/registry.go` provides an identical `Registry` for sinks. The default registries are populated in `main()`:

```go
srcRegistry := source.NewRegistry()
srcRegistry.Register("stdin", newStdinSource)
sinkRegistry := sink.NewRegistry()
sinkRegistry.Register("stdout", newStdoutSink)
```

The `Pipeline.New` function accepts optional `*source.Registry` / `*sink.Registry` parameters, defaulting to nil (tests inject mock registries without global state).

**CLI entry point** (`cmd/worker/main.go`) — uses `log/slog` for structured logging:

```go
func main() {
    configPath := flag.String("config", "pipeline.yaml", "path to YAML config file")
    flag.Parse()

    cfg, err := config.LoadConfig(*configPath)
    if err != nil {
        slog.Error("config loading failed", "error", err)
        os.Exit(1)
    }

    // Build source & sinks from config via registry
    src, err := source.DefaultRegistry.Build(cfg.Pipeline.Source)
    if err != nil {
        slog.Error("source build failed", "error", err)
        os.Exit(1)
    }
    sinks, err := sink.DefaultRegistry.BuildAll(cfg.Pipeline.Sinks)
    if err != nil {
        slog.Error("sink build failed", "error", err)
        os.Exit(1)
    }

    // Create pipeline
    p, err := pipeline.New(cfg.Pipeline.Name, src, sinks,
        pipeline.WithStreamCapacity(cfg.Pipeline.StreamSize),
        pipeline.WithMaxRecordBytes(cfg.Pipeline.MaxRecordBytes),
        pipeline.WithDrainTimeout(cfg.Pipeline.DrainTimeout),
    )
    if err != nil {
        slog.Error("pipeline creation failed", "error", err)
        os.Exit(1)
    }

    ctx, cancel := signal.NotifyContext(context.Background(),
        syscall.SIGINT, syscall.SIGTERM)
    defer cancel()
    // SIGQUIT intentionally NOT intercepted — Go runtime handles it for stack dumps.
    // SIGHUP reserved for config reload (Phase 4).

    if err := p.Run(ctx); err != nil {
        slog.Info("pipeline exited", "error", err)
    }
}
```

Uses `log/slog` from the standard library (zero dependency cost) instead of `log.Printf`, providing structured key-value logging from day one. This avoids a Phase 3 migration that would touch every file.

### 2.6 Concurrency Model

```
main goroutine
  └── signal.NotifyContext (SIGINT/SIGTERM)
       └── Pipeline.Run(ctx)
            └── Supervisor.Serve(ctx)
                 ├── child: Producer  (owns source.Records() + stream.Writer().Publish)
                 ├── child: Consumer0 (owns stream.Reader().Read + sink.Load)
                 └── child: Consumer1 (same, isolated)
```

- **Producer goroutine**: reads from `source.Records()` chan → `Publish` to stream. Blocked on full ring = backpressure.
- **Consumer goroutines**: `Read` from stream → `Load` to sink. Each has independent read cursor.
- **Supervisor goroutine**: orchestrates Init→Run→Drain→Close for each child. Detects panics via `recover()`.
- All children share the same `context.Context` from `Pipeline.Run`. When ctx is cancelled, all children begin draining.

Key rule: **No goroutine escapes the Supervisor**. Every goroutine is spawned inside `runWithPanicRecover` so that panics are caught and trigger restart.

---

## 3. Implementation Strategy

### 3.1 Implementation Order (within Phase 1)

Each step produces a compilable, testable increment. The binary only works end-to-end after Step 6.

| Step | Package | Deliverable | Testable? |
|------|---------|-------------|-----------|
| **1. Project scaffold** | root | `go.mod`, directory structure, empty `main.go` | `go build ./...` passes |
| **2. Record types** | `internal/record` | `Record`, `RecordBatch` structs with UUID v7 generation | `go test ./internal/record/` |
| **3. Lifecycle** | `internal/lifecycle` | `Lifecycle` interface, `Component` wrapper with state machine, `RunFunc` adapter | Unit tests for state transitions, panic shield |
| **4. Source & Sink** | `internal/source`, `internal/sink` | `StdinSource`, `StdoutSink` implementations | Unit tests with pipe/stdin, bytes.Buffer |
| **5. InProcStream** | `internal/stream` | `InProcStream` with ring buffer, fan-out readers, blocking publish, EOF on close | Unit tests: single reader, multi-reader, backpressure, close semantics |
| **6. Supervisor** | `internal/supervisor` | `Supervisor` with `AddChild`, `Serve`, panic recovery, exponential backoff, permanent failure detection | Unit tests: clean exit, panic restart, retry limit, context cancel |
| **7. Config** | `internal/config` | `LoadConfig`, source/sink factories, sample YAML | Unit tests: valid YAML, missing fields, unknown source type |
| **8. Pipeline** | `internal/pipeline` | `New`, `Run`, `Drain` orchestration with correct drain ordering | Integration test with StdinSource + StdoutSink |
| **9. CLI** | `cmd/worker/main.go` | Flag parsing, signal handling, wiring everything together | Manual: `echo '{"a":1}' \| go run ./cmd/worker --config test.yaml` |
| **10. E2E test** | `pipeline_test.go` (in `internal/pipeline`) | Full end-to-end test: pipe JSON via stdin, capture stdout, verify output | `go test -v ./internal/pipeline/` |

### 3.2 Testing Strategy for Phase 1

| Layer | Approach | Tools |
|-------|----------|-------|
| **Unit** | Table-driven tests for each component in isolation | `testing`, `stdr/testify` (assert) |
| **Integration** | Wire StdinSource + InProcStream + StdoutSink, feed input, assert output | `os.Pipe`, `bytes.Buffer` |
| **E2E** | Spawn `cmd/worker` as subprocess with temp config, pipe stdin, capture stdout | `os/exec`, `testing` |
| **Supervisor** | Inject components that panic or hang; verify restart count, permanent failure, clean exit | `testing`, `context` with timeout |
| **Race** | Run all tests with `-race` | `go test -race ./...` |

**Key test scenarios:**

1. **Happy path**: stdin → producer → stream → consumer → stdout. Verify record arrives.
2. **Multiple sinks**: two stdout sinks, verify both receive copies of each batch.
3. **Producer panic**: inject panic in Records() chan — verify Supervisor restarts producer, stream remains intact, consumers keep processing buffered batches.
4. **Consumer panic**: inject panic in Load() — verify Supervisor restarts consumer, producer continues writing, backlog is consumed after restart.
5. **Backpressure**: fill ring buffer, verify Publish blocks; consume a batch, verify Publish unblocks.
6. **Graceful shutdown**: SIGINT mid-flight — verify all components drain in order, no data loss within the stream.
7. **Permanent failure**: component panics 6 times — verify Supervisor marks it permanently failed and Pipeline.Run returns an error.

### 3.3 Dependencies

| Dependency | Purpose | Phase 1? |
|-----------|---------|----------|
| `github.com/google/uuid` | UUID v7 for Record/Batch IDs | Yes |
| `gopkg.in/yaml.v3` | YAML config parsing | Yes |
| `github.com/stretchr/testify` | Test assertions | Dev only |

**Deliberately excluded from Phase 1:**

- `tinylib/msgp` — MessagePack codegen adds build complexity; JSON is fine for debug sinks. msgp arrives in Phase 3 (transforms) when throughput matters.
- `go.etcd.io/bbolt` — Checkpoint store; arrives in Phase 2.
- `fsnotify/fsnotify` — Hot-reload; arrives in Phase 4.

---

## 4. Alternatives Considered

| Approach | Why Not |
|----------|---------|
| **Full msgp from day one** | Adds codegen step and build complexity for no benefit in debug mode. JSON is human-readable for Phase 1 testing. |
| **External broker (NATS) instead of InProcStream** | Contradicts RFC 0001 decision to keep Phase 1 zero-infra. InProcStream proves the fan-out semantics before adding network. |
| **Plugin-based source/sink registry (go-plugin, wasm)** | Over-engineering for Phase 1. A simple `map[string]Factory` suffices for 2 adapters. Plugin system, if needed, is a separate RFC. |
| **errgroup instead of custom Supervisor** | `errgroup` kills all goroutines on first error — we need per-child restart. Supervisor is intentionally custom. |
| **Context.Background for drain** | Drain must have a timeout to prevent hung components from blocking shutdown forever. Configurable per-component. |
| **Unbuffered chan for source.Records()** | Source may produce faster than stream can accept. A small buffered chan (configurable, default 10) decouples them. |
| **Channel-based fan-out (N chans)** | For 2-5 consumers, copying in the ring is simpler and avoids N channel allocations per batch. Revisit if consumer count grows beyond 10. |

---

## 5. Drawbacks & Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| **Ring buffer contention under high throughput** | Medium | Use `sync.Mutex` (simpler for Phase 1). Profile first; if contention shows in flame graph, switch to `sync.RWMutex` (readers don't contend with each other) or per-reader atomic cursors. |
| **No spillover → OOM on burst** | Medium | Phase 1 targets debug/demo loads. Document `stream_capacity` as the safety valve. Spillover arrives in Phase 2. |
| **`sync.Cond` lost wakeup under context cancellation** | High | Use `context.AfterFunc` with mutex-held `Broadcast` (documented in §2.2). The mutex serializes the wakeup, preventing the lost-wakeup race that would otherwise hang shutdown permanently. |
| **Orphan reader cursors after consumer restart** | High | Supervisor's panic-recovery calls `InProcStream.RemoveReader()` before restarting a consumer. Explicit `NewReader`/`RemoveReader` lifecycle prevents cursor leaks. |
| **Publish blocks during drain** | Medium | Stream.Drain() sets a closed flag; Publish returns `ErrStreamClosed`. Producer loop handles this error as a clean exit. |
| **Panic recovery misses some panics** (e.g., goroutine started outside Supervisor) | Low | Enforce in code review: every long-lived goroutine must be spawned inside a `Supervisor` child or documented exception. |
| **Drain timeout too short** | Low | Make drain timeout configurable in `PipelineConfig` (default 10s). Consumer `Load` should respect ctx cancellation. |
| **UUID v7 generation performance** | Low | `google/uuid` is fast enough for ETL batches (not per-record in Phase 1). Profile and replace with `xid` if bottleneck. |
| **YAML config lacks schema validation** | Medium | Add struct tags + `validate` library in Phase 2. For Phase 1, manual checks in `LoadConfig.validate()` catch common mistakes. |

---

## 6. Unresolved Questions

- [x] **Logging**: Should Phase 1 use `log.Printf` or wire `slog` from day one? Using `slog` adds a dependency but avoids a rewrite in Phase 3. **Decision: use `log/slog` from day one.** `slog` is in the standard library since Go 1.21 (project targets Go 1.26+). Zero dependency cost. Structured logging from Phase 1 avoids a migration that would touch every file. See the updated CLI entry point in §2.5.
- [ ] **Stream capacity units**: Should `stream_capacity` be in batches (current proposal) or individual records? Batches are simpler and map to the atomic transport unit. Document that 1 batch = ~1000 records (configurable per source).
- [ ] **Consumer naming**: Consumers need unique names for Supervisor logging. Auto-generate `{pipeline}-consumer-{idx}` or require explicit names in config? **Proposal: auto-generate with optional override in config.**
- [ ] **Drain timeout**: What is the default drain timeout for Phase 1? **Proposal: 10 seconds, configurable via `pipeline.drain_timeout` in YAML.**
- [ ] **Record ID generation**: UUID v7 requires a clock sequence for full spec compliance. Accept `google/uuid` which does not yet support v7 natively, or use `github.com/gofrs/uuid` v5 which does? **Proposal: use `github.com/google/uuid` with `uuid.NewString()` (UUID v4) for Phase 1; revisit v7 in Phase 2 when checkpointing needs timestamp-ordered IDs.**
- [ ] **go.mod module path**: What should the Go module path be? **Proposal: `github.com/sabiqazhar/etl-pipeline-go`** (adjust to actual org).
- [ ] **Race detection**: All tests must pass with `-race`. The `sync.Mutex` and `sync.Cond` in InProcStream are the main areas of concern — ensure lock ordering is documented.
- [x] **Error types for permanent failure**: Should `Pipeline.Run` return a `multierror` with all permanently failed children, or just the first? **Decision: define a concrete `PermanentFailureError` type** rather than an anonymous interface, so callers can import and assert it cleanly.

```go
type PermanentFailureError struct {
    Names []string  // permanently failed child names
    Err   error     // wrapped error (first failure)
}

func (e *PermanentFailureError) Error() string { ... }
func (e *PermanentFailureError) Unwrap() error { return e.Err }
```
