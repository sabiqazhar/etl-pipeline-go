# RFC 0001: Pipeline Core Architecture — Lifecycle, Isolation & Transport

> **TL;DR**: Define the foundational Go interfaces, lifecycle contracts, and in-process transport for a multi-pipeline, multi-sink ETL framework with autonomous supervision, per-sink isolation, and at-least-once delivery.

- **Date**: 2026-06-09
- **Status**: Draft
- **Author**: @sabiqazhar
- **PR**: [Link to GitHub PR]
- **Supersedes**: None
- **Superseded By**: None

---

## 1. Motivation

Existing ETL tools (Airbyte, Kafka Connect, Debezium) suffer from:

| Problem | Cost |
|---|---|
| **No sink isolation** — a slow sink blocks the entire pipeline | Data freshness SLOs missed |
| **Zombie state on crash** — silent duplicates or gaps | Manual reconciliation |
| **JVM/Python bloat** — 500+ MB per connector | Poor host density, high infra cost |
| **Lifecycle outsourced to orchestrators** — Airflow/Dagster manages drain/restart | Ops debt, slow config deploys |
| **Single-writer bottleneck** — all records through one transform engine | Throughput capped regardless of sink count |

This project targets a **complementary niche**: high-density, self-healing, single-binary pipelines in Go where the team controls both source and sink integrations.

## 2. Proposed Change

### High-Level Architecture

```
Worker (OS Process)
├── Supervision Tree          ── recursive restart (panic → backoff → retry)
├── Config Watcher            ── fsnotify/etcd watch → hot-reload
└── Pipeline Registry
    └── Pipeline (per data flow)
        ├── Producer          ── owns Source, writes to Stream
        ├── InProcStream      ── bounded Go chan + disk spillover
        ├── Consumer[A]       ── reads Stream, applies transforms, loads to Sink A
        ├── Consumer[B]       ── same, isolated from A
        └── DLQ Sink          ── poison pills after retry exhaustion
```

### Data Flow (At-Least-Once)

```
Source → Producer → [InProcStream] → Consumer[A] → Sink A
                                    → Consumer[B] → Sink B
                                    → DLQ Consumer → DLQ Sink
```

Each Consumer reads independently. Failure in one sink does not block others. Producer commits checkpoint *after* batch is written to Stream. Consumer commits checkpoint *after* sink Load succeeds. On crash, uncommitted batches are replayed.

### Lifecycle State Machine

Every managed component follows: **`Init → Run → Drain → Close`**

- `Init`: acquire resources, validate config (idempotent)
- `Run`: block until ctx cancelled or fatal error
- `Drain`: graceful stop — finish in-flight work, commit checkpoint
- `Close`: release resources (idempotent)

A Supervisor catches panics (Layer-1), detects hangs via watchdog (Layer-2), and restarts with exponential backoff (1s→30s max). After 5 consecutive failures, marks the component as permanently failed.

### Core Go Interfaces

```go
// Lifecycle — universal contract for all pipeline components.
type Lifecycle interface {
    Init(context.Context) error
    Run(context.Context) error
    Drain(context.Context) error
    Close() error
}

// Source extracts records from an external system.
type Source interface {
    Lifecycle
    Records() <-chan RecordBatch
}

// Sink loads transformed records to an external target (idempotent Load).
type Sink interface {
    Lifecycle
    Load(context.Context, RecordBatch) error
}

// Transformer mutates a record in-place (pure, no I/O). Return nil to drop record.
type Transformer interface {
    Transform(Record) (*Record, error)
}

// Stream decouples Producer from Consumers. One writer, N independent readers.
type Stream interface {
    Lifecycle
    Writer() StreamWriter
    Reader() StreamReader
}

type StreamWriter interface {
    Publish(context.Context, RecordBatch) error
    Flush(context.Context) error
}

type StreamReader interface {
    Read(context.Context) (RecordBatch, error)  // io.EOF when drained
    Commit(context.Context, CheckpointToken) error
}

// Supervisor monitors children and applies restart policy.
type Supervisor interface {
    AddChild(name string, child Lifecycle, opts ...ChildOption)
    Serve(context.Context) error
}
```

### Key Architecture Decisions

| Decision | Choice | Rationale |
|---|---|---|
| **Transport** | InProcStream (bounded chan + spillover file) | Zero infra dependency for single-node, ~100μs latency. External broker (NATS) opt-in later. |
| **Checkpoint store** | bboltDB (embedded) | ACID, zero deployment deps. NATS KV opt-in for cluster mode. |
| **Serialization** | MessagePack via `tinylib/msgp` | 5-10x faster than JSON, zero-alloc marshal/unmarshal. |
| **Config format** | YAML with type-discriminated blocks | Human-friendly, supports pluggable source/sink via registry. |
| **Supervision** | Dual-layer (panic recovery + hang watchdog) | Catches 99% of unexpected failures in Go. |

## 3. Implementation Strategy

Four phases over 8 weeks. Each delivers a working binary.

| Phase | Weeks | Deliverable |
|---|---|---|
| **1: Core Lifecycle & Stream** | 1-2 | `go run cmd/worker/main.go` runs a pipeline from any Source to stdout. Lifecycle interfaces, InProcStream, Supervisor, Pipeline, Worker, YAML config loading, debug CLI. |
| **2: Checkpointing, DLQ & At-Least-Once** | 3-4 | Kill worker mid-flight → resume from last committed checkpoint. bboltDB checkpoint store, stream replay, DLQ routing, crash-recovery E2E tests. |
| **3: Transforms, Metrics & Observability** | 5-6 | Transformer chain, Prometheus metrics, OpenTelemetry tracing, slog logging, HTTP health endpoints. |
| **4: Live Config Reload & Cluster** | 7-8 | fsnotify hot-reload, NATS KV/JetStream adapters, leader election. |

## 4. Alternatives Considered

| Approach | Why Not |
|---|---|
| **External broker (Kafka/NATS) from day one** | Adds deployment complexity and latency (~2ms vs ~100μs) for the common single-node case. InProcStream is simpler, faster, and zero-infra. External broker is an opt-in adapter. |
| **SQLite instead of bboltDB** | bboltDB is simpler (single-writer model matches our checkpoint pattern) and equally mature. Switch to SQLite WAL if write contention appears at >500 pipelines. |
| **Protobuf/Avro instead of MessagePack** | Protobuf needs `.proto` files and a registry; Avro needs a schema registry. msgp is Go-idiomatic, code-generated, and zero-alloc. |
| **JSON config** | YAML is more readable for ops configs, supports anchors/aliases, and is standard in the Go ecosystem (e.g., Kubernetes). |

## 5. Drawbacks & Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| **bboltDB write contention at scale** | Low | Abstract store behind interface; upgrade to SQLite WAL if bottleneck confirmed. |
| **MessagePack debugging friction** | High | `msgp-tool` CLI for decode; debug mode logs key/size not payload. |
| **Spillover disk full** | Low | Configurable `max_spillover_bytes` (default 1 GB); disk usage metrics + alerting. |
| **Supervisor backoff flap** | Low | Jitter + consecutive-failure window. |
| **Go GC pressure at high throughput** | Medium | Profile with pprof; tune GOGC; buffer pooling via `sync.Pool`. |
| **Drain ordering** | Medium | Consumers drain before Producer; enforced in `Pipeline.Drain()`. |

## 6. Unresolved Questions

- [ ] **Record model**: Expected record size range (bytes to MB)? Affects buffer sizing and spillover throughput. Parametrize `MaxRecordSize` (default 1 MB).
- [ ] **Batch sizing**: Records per batch — configurable per source? Default 1,000 records or 10 MB (whichever first).
- [ ] **Spillover file format**: Append-only binary with `[4-byte length][msgp-encoded RecordBatch]`. Compaction truncates up to oldest uncommitted byte.
- [ ] **Consumer fan-out**: Pub/sub — each consumer gets a copy. Memory cost is N × batch size (acceptable: 2-5 sinks per pipeline).
- [ ] **Drain ordering**: Consumers drain first, then Producer, then Stream. Enforced in `Pipeline.Drain()`.
- [ ] **Config validation on reload**: Reject invalid configs before draining any pipeline. Log error, continue with old config.
- [ ] **DLQ retry policy**: Per-sink `max_retries` (default 3), exponential backoff (1s, 2s, 4s). Retry count tracked in `Record.Headers`.
- [ ] **Secret management**: Support `env:` prefix for DSNs/passwords in config values. Vault/AWS Secrets Manager in future RFC.
- [ ] **Liveness vs readiness**: `/health/live` = always 200; `/health/ready` = 200 only if all pipelines in RUNNING state.
