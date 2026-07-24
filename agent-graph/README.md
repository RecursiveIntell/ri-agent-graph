# ri-agent-graph

**Graph-based agent orchestration for Rust** — a LangGraph-inspired execution engine with checkpointing, parallel fan-out/fan-in, interrupt/resume, and cryptographic execution receipts.

[![Crates.io](https://img.shields.io/crates/v/ri-agent-graph)](https://crates.io/crates/ri-agent-graph)
[![docs.rs](https://img.shields.io/docsrs/ri-agent-graph)](https://docs.rs/ri-agent-graph)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE-MIT)

![Architecture](assets/architecture.svg)

## What it gives you

- **Deterministic graph execution** — define nodes as computational steps, edges as control flow, and execute with typed state
- **8 node types** — `llm`, `router`, `join`, `parallel`, `passthrough`, `state_transform`, `subgraph`, `human_approval`
- **Parallel fan-out/fan-in** — automatic concurrent execution with configurable join policies (`collect_array`, `merge_objects`, `first_non_null`, `all_success`, `quorum`)
- **Checkpointing & interrupt/resume** — SQLite-backed persistence with atomic checkpoint transactions and crash recovery
- **Cryptographic receipts** — HMAC-SHA256 authenticated `GraphExecutionReceiptV1` for every run
- **Event streaming** — lifecycle events (node start/complete/error), token streaming, and state snapshots
- **stack-ids integration** — `TraceCtx`, `AttemptId`, `TrialId` execution tracing at every layer
- **Zero-cost abstractions** — generic over user-defined state `S`, no heap allocation beyond what your nodes require

![Layers](assets/layers.svg)

## Installation

```bash
cargo add ri-agent-graph
```

Or in your `Cargo.toml`:

```toml
[dependencies]
ri-agent-graph = "0.2"
```

### Feature flags

| Flag | Default | Description |
|------|---------|-------------|
| `checkpointing` | ✅ on | SQLite-backed checkpoint persistence via `rusqlite` |

To disable checkpointing (embedded/no_std targets):

```toml
ri-agent-graph = { version = "0.2", default-features = false }
```

## Quick start

```rust
use ri_agent_graph::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Build a two-step graph
    let graph = AgentGraph::builder()
        .add_node("step1", node!(|state| async move {
            state.set("count", 1).await?;
            Ok(())
        }))
        .add_node("step2", node!(|state| async move {
            let count: i32 = state.get("count").await?;
            state.set("count", count + 1).await?;
            Ok(())
        }))
        .add_edge("step1", "step2")
        .build()?;

    // Execute
    let state = AgentState::new();
    let result = graph.execute("step1", state).await?;

    let final_count: i32 = result.get("count").await?;
    assert_eq!(final_count, 2);

    Ok(())
}
```

## Node types

| Type | Description | Status |
|------|-------------|--------|
| `llm` | Invoke an LLM with system/user prompts and optional tool calls. Response merged via configurable reducer. | ✅ |
| `router` | Conditional branching. Evaluate a function/predicate to select the next edge dynamically. | ✅ |
| `join` | Fan-in synchronization. Wait for parallel branches and merge state via a join policy. | ✅ |
| `parallel` | Fan-out dispatch. Spawns concurrent branches from a single node; the engine's `JoinSet` handles real parallelism. | ✅ |
| `passthrough` | No-op state pass. Useful for fan-out distribution points. | ✅ |
| `state_transform` | 10 state mutations: `set`, `copy`, `delete`, `increment`, `append`, `merge`, `merge_object`, `select`, `compare`, `format`. | ✅ |
| `subgraph` | Reference another registered graph as a node. Enables composition and reuse. | ✅ |
| `human_approval` | HITL gate. Emits an `InterruptError` and waits for external approval via checkpoint/resume. | ✅ |

## Router example

```rust
use ri_agent_graph::{AgentGraph, router, node, START, END};
use serde_json::json;

let graph = AgentGraph::builder()
    .add_node("classify", node!(|state| async move {
        // LLM classification returns "bug", "feature", or "question"
        state.set("category", "bug").await?;
        Ok(())
    }))
    .add_node("handle_bug", node!(|state| async move {
        state.set("response", "Bug triaged").await?;
        Ok(())
    }))
    .add_node("handle_feature", node!(|state| async move {
        state.set("response", "Feature scoped").await?;
        Ok(())
    }))
    .add_node("handle_question", node!(|state| async move {
        state.set("response", "Question answered").await?;
        Ok(())
    }))
    .add_edge(START, "classify")
    .add_router("classify", router!(|state| {
        let category: String = state.get("category").await?;
        Ok(match category.as_str() {
            "bug" => vec!["handle_bug"],
            "feature" => vec!["handle_feature"],
            _ => vec!["handle_question"],
        })
    }))
    .add_edge("handle_bug", END)
    .add_edge("handle_feature", END)
    .add_edge("handle_question", END)
    .build()?;
```

## Parallel fan-out with join

```rust
let graph = AgentGraph::builder()
    .add_node("coordinator", node!(|state| async move {
        state.set("workstreams", json!(["A", "B", "C"])).await?;
        Ok(())
    }))
    .add_node("fanout", passthrough_node!())
    .add_node("worker_a", node!(|state| async move {
        state.set("result_a", "done").await?;
        Ok(())
    }))
    .add_node("worker_b", node!(|state| async move {
        state.set("result_b", "done").await?;
        Ok(())
    }))
    .add_node("worker_c", node!(|state| async move {
        state.set("result_c", "done").await?;
        Ok(())
    }))
    .add_node("merger", join_node!(JoinMode::CollectArray, ["result_a", "result_b", "result_c"], "findings"))
    .add_edge("coordinator", "fanout")
    .add_edge("fanout", "worker_a")
    .add_edge("fanout", "worker_b")
    .add_edge("fanout", "worker_c")
    .add_edge("worker_a", "merger")
    .add_edge("worker_b", "merger")
    .add_edge("worker_c", "merger")
    .add_edge("merger", END)
    .with_reducers(Reducers::new().append_to("findings"))
    .build()?;
```

## Checkpointing & interrupt/resume

Enable checkpointing to persist execution state to SQLite:

```rust
use ri_agent_graph::checkpoint_store::SqliteCheckpointStore;

let store = SqliteCheckpointStore::open("executions.db").await?;
let mut executor = GraphExecutor::new(graph)
    .with_checkpoint_store(store);

// Execute with interrupt detection
match executor.execute_with_interrupt(initial_state).await {
    Ok(receipt) => println!("Completed: {:?}", receipt),
    Err(AgentGraphError::Interrupted { checkpoint_id, .. }) => {
        // Resume from checkpoint with injected input
        executor.resume_from(checkpoint_id, injected_input).await?;
    }
}
```

## Execution receipts

Every completed run produces a `GraphExecutionReceiptV1`:

```rust
pub struct GraphExecutionReceiptV1 {
    pub run_id: String,
    pub graph_name: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub steps: Vec<StepExecutionReceiptV1>,
    pub final_state_digest: String,
    pub status: ExecutionOutcome,
}
```

Each step receipt carries:
- Node ID, attempt count, wall-clock duration
- Input/output state digests
- Error details (if any)
- `TraceCtx` / `AttemptId` / `TrialId` from stack-ids

## Ecosystem

| Crate | Description | Status |
|-------|-------------|--------|
| [ri-agent-graph](https://crates.io/crates/ri-agent-graph) | Core graph execution engine | ✅ v0.2.1 |
| [agent-graph-mcp](https://crates.io/crates/agent-graph-mcp) | MCP server — 25 typed tools for graph lifecycle, execution, approval, templates | ✅ v0.2.2 |
| [stack-ids](https://crates.io/crates/stack-ids) | Shared identity, scope, and trace primitives | ✅ v0.1.3 |
| [llm-pipeline](https://crates.io/crates/llm-pipeline) | Reusable LLM node payloads (Ollama, prompt templating, parsing) | ✅ v0.2.0 |

## API overview

```rust
// Core types
AgentGraph<S>           // The graph: nodes + edges + reducers
AgentState              // Key-value state flowing through execution
GraphExecutor<S>        // The runtime engine
GraphExecutionReceiptV1 // Cryptographic execution receipt

// Node construction
node!(|state| async { ... })         // Inline closure node
passthrough_node!()                   // No-op passthrough
router!(|state| { ... })             // Conditional routing
join_node!(mode, inputs, output)     // Fan-in synchronization

// Sentinels
START  // Virtual entry node
END    // Virtual exit node
```

## Claim boundaries

- **This crate provides graph execution semantics** — it does not include LLM provider clients, prompt templating, or response parsing. Those belong in `llm-pipeline` or your application layer.
- **Receipts prove structural execution** — they do not prove that an external LLM call occurred, what model responded, or what the provider's internal state was. Receipts carry cryptographic digests of the local execution trace only.
- **Interrupt/resume is deterministic local resume** — it supports linear chains of deterministic `passthrough` and local `state_transform` nodes with SQLite-bound state. It does not support resuming across LLM calls, network I/O, or external tool invocations.
- **Parallelism is best-effort** — concurrent branch execution uses Tokio's `JoinSet`. Unordered parallel writes to the same state key are rejected unless an explicit `Reducer` is declared.

## Error handling

All fallible operations return `Result<T, AgentGraphError>`:

```rust
pub enum AgentGraphError {
    GraphBuild(String),
    NodeNotFound(String),
    EdgeNotFound(String),
    StateKeyNotFound(String),
    StateTypeMismatch { key: String, expected: String, actual: String },
    ParallelWriteConflict(String),
    CheckpointError(CheckpointStoreOperation),
    Interrupted { checkpoint_id: String, node_id: String },
    ExecutionTimeout { run_id: String, elapsed_ms: u64 },
    IntegrityKeyRequired,
    // ...
}
```

## Verification

```bash
# Build
cargo build --release -p ri-agent-graph

# Test
cargo test -p ri-agent-graph

# Clippy (strict)
cargo clippy -p ri-agent-graph -- -D warnings

# Format check
cargo fmt --check

# Publish dry-run
cargo publish -p ri-agent-graph --dry-run
```

## Roadmap

- [ ] Typed state extractors (derive macro for `StateExtract`)
- [ ] Graph visualization (Mermaid/DOT export)
- [ ] Streaming LLM token passthrough
- [ ] Distributed checkpoint backend (PostgreSQL, S3)
- [ ] Subgraph composition with state isolation
- [ ] WebAssembly target support (`wasm-bindgen`)

## License

MIT — see [LICENSE-MIT](LICENSE-MIT) for details.

---

Built by [RecursiveIntell](https://github.com/RecursiveIntell) — an applied R&D studio building local-first AI infrastructure.
