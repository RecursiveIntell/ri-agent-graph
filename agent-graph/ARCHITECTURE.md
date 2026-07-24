# Architecture — agent-graph

## Overview

`agent-graph` is a **graph orchestrator** that owns control-flow (routing, loops, joins, parallelism, interrupts/resume, checkpointing) and executes node work via a pluggable **Payload** layer. It does NOT implement payload logic (LLM calls, parsing, streaming decode) — only orchestrates their execution and records outcomes.

## State Model

**Decision**: State is `serde_json::Value` (specifically `Value::Object` via `HashMap<String, Value>`).

- `AgentState` wraps `Arc<RwLock<HashMap<String, Value>>>` — the canonical graph state container.
- At the node boundary, state is conceptually a JSON object. Each node receives the state and produces updates.
- **PayloadNode** uses `input_selector: Value → Value` to extract node input from state, and `output_mapper: (state, PayloadOutput) → Value` to fold output back into state.
- **Legacy Node** trait continues to work: nodes read/write `AgentState` directly via `get`/`set`.

### Fan-in Merge Policy

- For parallel branches, state is forked per-branch.
- After all branches complete, a **JoinNode** (or registered Reducers) merges results deterministically.
- **Implicit merge** (via Reducers registered on AgentState) is supported for backward compat.
- **Explicit merge** (via JoinNode) is the recommended approach for new code.
- Do NOT implicitly merge concurrently produced Values without a policy — at minimum `LastWriteWins` reducer is applied.

## Node Types

### PayloadNode
Wraps `Box<dyn Payload + Send + Sync>`. The `Payload` trait is:
```rust
trait Payload: Send + Sync {
    fn invoke(&self, input: Value, ctx: &PayloadContext)
        -> Pin<Box<dyn Future<Output = Result<PayloadOutput, PayloadError>> + Send + '_>>;
}
```
- `PayloadContext` provides a token sink for streaming and metadata.
- `PayloadOutput` contains `value: Value` and `meta: HashMap<String, Value>`.
- `input_selector` extracts what to pass from state (default: entire state).
- `output_mapper` applies result back to state (default: merge output.value into state).

### FnNode (legacy)
Wraps a closure `|state, config| → Result<NodeOutput>`. Nodes mutate `AgentState` directly.

### JoinNode
Explicit fan-in merge node. Reads specified keys from state, applies a merge function, writes result to an output key. Used when parallel branches converge.

### Subgraph
An `AgentGraph` used as a node inside another graph. State is forked for isolation.

## Scheduler Semantics

### Superstep Model (BSP)
Execution proceeds in **supersteps**:
1. Current superstep contains one or more nodes.
2. If multiple nodes: they execute in parallel with forked state.
3. After all complete: state is merged (via reducers or JoinNode), next superstep is computed.
4. Repeat until END or max iterations.

### Conditional Routing
- `RoutingFunction` trait evaluates state and returns `RouterOutput` (single target, fan-out, or end).
- Conditional edges attach a router to a source node.
- Router evaluates after the source node completes.

### Cycles / Loops
- Edges can form cycles (e.g., A → B → router → A).
- Bounded by `max_iterations` (graph-level) and `recursion_limit` (config-level).
- Explicit termination via `Navigation::End` or router returning `None`.

### Fan-out / Fan-in
- **Fan-out**: multiple edges from one node, or `Navigation::Nodes(vec![...])`.
- **Fan-in**: multiple edges converging on one node. That node runs in the next superstep after all predecessors complete.
- **JoinNode** provides explicit merge logic. Without it, reducer-based merge applies.

## Checkpoint Boundaries

### CheckpointStore Trait
Granular per-attempt recording:
- `create_run(graph_name) → RunId`
- `record_attempt(run_id, node_id, attempt, input) → AttemptId`
- `complete_attempt(attempt_id, output, meta)`
- `fail_attempt(attempt_id, error)`
- `record_interrupt(attempt_id, interrupt)`
- `save_state_snapshot(run_id, state)`
- `load_run(run_id) → RunState`

### Legacy CheckpointSaver
The existing `CheckpointSaver` trait (superstep-level snapshots) remains for backward compat.

### Default Implementation
`InMemoryCheckpointStore` — HashMap-based, suitable for tests.

### Checkpoint Timing
- Before each node attempt: `record_attempt`
- After success: `complete_attempt`
- After failure: `fail_attempt`
- On interrupt: `record_interrupt`
- After each superstep: `save_state_snapshot`

## Executor Abstraction

### Executor Trait
```rust
trait Executor: Send + Sync {
    fn execute_node(&self, node: Arc<dyn Node>, state: AgentState, config: GraphConfig)
        -> Pin<Box<dyn Future<Output = Result<NodeOutput>> + Send>>;
}
```

### InProcessExecutor (default)
- Runs nodes directly via `node.execute(&state, &config).await`.
- Used when no external executor is configured.

### Future: TauriQueueExecutor
- Feature-gated behind `cfg(feature = "tauri-queue")`.
- Each node attempt becomes a job record in tauri-queue.
- Worker executes the payload and writes output to CheckpointStore.
- NOT implemented in core — provided as adapter module.

## Event Pipeline

### EventSink Trait
```rust
trait EventSink: Send + Sync {
    fn emit(&self, event: GraphEvent);
}
```
- **Synchronous, non-blocking** — implementations must not block.
- `NoopEventSink` — default, discards events.
- `ChannelEventSink` — wraps `mpsc::Sender<StreamEvent>` for backward compat.
- `CallbackEventSink` — wraps a user-provided closure.

### GraphEvent Variants
- `RunStart`, `RunEnd`
- `NodeStart`, `NodeEnd`
- `Token` — forwarded from Payload layer via PayloadContext token sink
- `CheckpointWritten`
- `InterruptRaised`
- `StateUpdate`
- `SuperstepStart`, `SuperstepEnd`

### Token Streaming Wire-up
1. Runtime creates a token sink callback that captures `(run_id, node_id, event_sink)`.
2. Token sink is passed to Payload via `PayloadContext`.
3. Payload calls `ctx.on_token("...")` during execution.
4. Callback emits `GraphEvent::Token { run_id, node_id, token }` on EventSink.

## Interrupt / Resume

### Interrupt Type
```rust
struct Interrupt {
    kind: InterruptKind,       // AwaitInput, AwaitApproval, Custom
    payload: Value,            // describes what's needed
    correlation_id: String,    // UUID for safe resume matching
}
```

### Flow
1. Node returns `NodeOutcome::Interrupt { interrupt }`.
2. Runtime records interrupt via CheckpointStore.
3. Runtime emits `GraphEvent::InterruptRaised`.
4. Execution pauses, returns `ExecutionResult::Interrupted`.
5. Caller injects response data into state.
6. Caller calls `graph.resume(state, config, checkpoint)`.
7. Runtime loads checkpoint, validates correlation_id, continues from interrupted node.

### Legacy Interrupt
Existing `InterruptConfig` (interrupt_before/after) continues to work via `AgentGraphError::InterruptError`.

## Cancellation

- `Arc<AtomicBool>` cancel flag passed via `GraphExecCtx`.
- Checked at superstep boundaries (before each node execution).
- When set: runtime stops, records final status, returns `AgentGraphError::Cancelled`.
- Exposed to users via `GraphExecCtx::cancel()` method.

## Concurrency

- Parallel branches use `tokio::task::JoinSet` (bounded by superstep size).
- Each branch gets a forked `AgentState` — no shared mutable state during parallel execution.
- Merge happens synchronously after all branches complete.
- No unbounded task spawning.

## Dependencies

- Core crate: NO Tauri dependency.
- `async-trait`: used for legacy traits (`Node`, `RoutingFunction`, `CheckpointSaver`).
- New traits use boxed futures directly (no async-trait).
- `rusqlite`: optional, behind `checkpointing` feature.

## Assumptions

1. Payload implementations are provided by external crates (e.g., `llm-pipeline`).
2. The Payload trait defined here is the canonical interface; external crates implement it.
3. State is always JSON-serializable (for checkpointing).
4. Node IDs are strings (no compile-time safety).
5. The superstep model is sufficient for all current use cases.
