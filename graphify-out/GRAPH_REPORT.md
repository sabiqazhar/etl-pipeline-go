# Graph Report - .  (2026-08-03)

## Corpus Check
- Corpus is ~9,948 words - fits in a single context window. You may not need a graph.

## Summary
- 132 nodes · 203 edges · 17 communities (11 shown, 6 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 24 edges (avg confidence: 0.81)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Stream & Record Core
- Architecture & RFC Design
- Lifecycle State Machine
- Backoff Policy & Options
- Supervisor Tests
- Supervisor Implementation
- Core Contracts (Source/Sink/Stream)
- Autonomous Supervision Concepts
- Lifecycle State Tests
- Config Reloading
- MessagePack Serialization
- Message Stream Model
- Project Module
- Backpressure
- Load Balancing
- Schema Evolution

## God Nodes (most connected - your core abstractions)
1. `InProcStream` - 16 edges
2. `Supervisor` - 10 edges
3. `New()` - 8 edges
4. `mockComponent` - 8 edges
5. `newMockComponent()` - 8 edges
6. `StateManager` - 7 edges
7. `State` - 7 edges
8. `RecordBatch` - 7 edges
9. `child` - 7 edges
10. `testLogger()` - 7 edges

## Surprising Connections (you probably didn't know these)
- `Distributed KV Store` --semantically_similar_to--> `bboltDB Checkpoint Store`  [INFERRED] [semantically similar]
  README.md → docs/rfcs/0001-pipeline-core-architecture.md
- `Phase 1 Worker` --implements--> `Worker Process`  [INFERRED]
  docs/rfcs/0002-phase1-core-implementation.md → README.md
- `Source and Sink Registry` --conceptually_related_to--> `Producer`  [INFERRED]
  docs/rfcs/0002-phase1-core-implementation.md → README.md
- `InProcStream` --implements--> `Message Stream`  [INFERRED]
  docs/rfcs/0001-pipeline-core-architecture.md → README.md
- `Lifecycle Interface` --implements--> `Zero-Crash Architecture`  [INFERRED]
  docs/rfcs/0001-pipeline-core-architecture.md → README.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **At-Least-Once Delivery Flow** — readme_producer, readme_message_stream, readme_consumer, readme_dlq [EXTRACTED 1.00]
- **Lifecycle Management Contract** — docs_rfcs_0001_pipeline_core_architecture_lifecycle, docs_rfcs_0002_phase1_core_implementation_component, readme_zero_crash [INFERRED 0.85]
- **Autonomous Supervision** — readme_supervision_tree, docs_rfcs_0001_pipeline_core_architecture_supervisor, docs_rfcs_0002_phase1_core_implementation_permanentfailure [INFERRED 0.85]

## Communities (17 total, 6 thin omitted)

### Community 0 - "Stream & Record Core"
Cohesion: 0.13
Nodes (14): CancelFunc, File, CheckpointToken, Record, RecordBatch, Context, RWMutex, NewInProcStream() (+6 more)

### Community 1 - "Architecture & RFC Design"
Cohesion: 0.10
Nodes (23): RFC Template, bboltDB Checkpoint Store, RFC 0001 Pipeline Core Architecture, InProcStream, Transport Decision, RFC 0002 Phase 1 Implementation, Source and Sink Registry, Phase 1 Worker (+15 more)

### Community 2 - "Lifecycle State Machine"
Cohesion: 0.17
Nodes (6): ErrInvalidTransition, State, StateManager, RWMutex, Context, mockComponent

### Community 3 - "Backoff Policy & Options"
Cohesion: 0.26
Nodes (9): DefaultBackoff(), Duration, DefaultChildConfig(), Duration, WithBackoff(), WithDrainTimeout(), BackoffPolicy, ChildConfig (+1 more)

### Community 4 - "Supervisor Tests"
Cohesion: 0.44
Nodes (11): WithMaxFailures(), New(), Logger, T, newMockComponent(), TestBackoffPolicy_Delay(), testLogger(), TestSupervisor_GracefulShutdown() (+3 more)

### Community 5 - "Supervisor Implementation"
Cohesion: 0.42
Nodes (5): Mutex, Context, Logger, child, Supervisor

### Community 6 - "Core Contracts (Source/Sink/Stream)"
Cohesion: 0.22
Nodes (6): Lifecycle, Sink, Source, Stream, StreamReader, StreamWriter

### Community 7 - "Autonomous Supervision Concepts"
Cohesion: 0.33
Nodes (6): Lifecycle Interface, Supervisor, Component State Machine, PermanentFailureError, Autonomous Supervision Tree, Zero-Crash Architecture

### Community 8 - "Lifecycle State Tests"
Cohesion: 0.47
Nodes (4): NewStateManager(), T, TestStateMachine_CloseIdempotency(), TestStateMachine_Transitions()

### Community 9 - "Config Reloading"
Cohesion: 0.67
Nodes (3): Config Watcher, YAML Config Loader, Live Configuration Reloading

## Knowledge Gaps
- **19 isolated node(s):** `github.com/aula-id/etl-pipeline-go`, `Transformer`, `Live Configuration Reloading`, `Schema Evolution Handling`, `Load Balancing` (+14 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **6 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Lifecycle` connect `Core Contracts (Source/Sink/Stream)` to `Lifecycle State Tests`, `Backoff Policy & Options`, `Supervisor Implementation`?**
  _High betweenness centrality (0.256) - this node is a cross-community bridge._
- **Why does `InProcStream` connect `Stream & Record Core` to `Core Contracts (Source/Sink/Stream)`?**
  _High betweenness centrality (0.135) - this node is a cross-community bridge._
- **Are the 5 inferred relationships involving `New()` (e.g. with `testLogger()` and `TestSupervisor_GracefulShutdown()`) actually correct?**
  _`New()` has 5 INFERRED edges - model-reasoned connections that need verification._
- **What connects `github.com/aula-id/etl-pipeline-go`, `Transformer`, `Live Configuration Reloading` to the rest of the system?**
  _19 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Stream & Record Core` be split into smaller, more focused modules?**
  _Cohesion score 0.12923076923076923 - nodes in this community are weakly interconnected._
- **Should `Architecture & RFC Design` be split into smaller, more focused modules?**
  _Cohesion score 0.09881422924901186 - nodes in this community are weakly interconnected._