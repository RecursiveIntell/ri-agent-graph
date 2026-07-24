# ri-agent-graph

**Graph-based agent orchestration for Rust** — a LangGraph-inspired execution engine with an MCP server, parallel fan-out/fan-in, checkpointing, interrupt/resume, and cryptographic receipts.

[![Crates.io — engine](https://img.shields.io/crates/v/ri-agent-graph?label=ri-agent-graph)](https://crates.io/crates/ri-agent-graph)
[![Crates.io — mcp](https://img.shields.io/crates/v/agent-graph-mcp?label=agent-graph-mcp)](https://crates.io/crates/agent-graph-mcp)
[![MCP Badge](https://lobehub.com/badge/mcp-full/recursiveintell-agent-graph-mcp?theme=light)](https://lobehub.com/mcp/recursiveintell-agent-graph-mcp)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE-MIT)

---

## What's here

| Crate | crates.io | Description |
|-------|-----------|-------------|
| **[ri-agent-graph](./agent-graph/)** | [![v0.2.2](https://img.shields.io/crates/v/ri-agent-graph)](https://crates.io/crates/ri-agent-graph) | Core engine — `AgentGraph`, `GraphExecutor`, 8 node types, checkpointing, receipts |
| **[agent-graph-mcp](./agent-graph-mcp/)** | [![v0.2.3](https://img.shields.io/crates/v/agent-graph-mcp)](https://crates.io/crates/agent-graph-mcp) | MCP server — 25 typed tools, daemon/proxy architecture, HITL, witnesses |

## Quick start

### Core engine

```bash
cargo add ri-agent-graph
```

```rust
use ri_agent_graph::prelude::*;

let graph = AgentGraph::builder()
    .add_node("greet", node!(|state| async move {
        state.set("message", "hello").await?;
        Ok(())
    }))
    .add_edge(START, "greet")
    .add_edge("greet", END)
    .build()?;

let result = graph.execute(START, AgentState::new()).await?;
```

### MCP server

```bash
# Start the daemon
agent-graph-mcpd --data-dir ~/.local/share/agent-graph --socket /tmp/agent-graph.sock &

# Hermes config
mcp_servers:
  agent_graph:
    command: ~/.cargo/bin/agent-graph-mcp
    args: [--base-url, http://127.0.0.1:11434, --model, glm-5.2:cloud, --data-dir, ~/.agent-graph]
```

## Architecture

```
Hermes ──→ agent-graph-mcp (proxy) ──Unix socket──→ agent-graph-mcpd (daemon) ──→ SQLite
              stdin/stdout                framed             Tokio async I/O
```

The **daemon** owns the SQLite database with a file lock, crash recovery, and startup mode enforcement. The **proxy** is stateless and bridges MCP stdio to the daemon's Unix socket. A `--direct` flag runs everything in-process for simple deployments.

## Node types

| Type | Description |
|------|-------------|
| `llm` | LLM call via Ollama with prompt, JSON mode, tool calls |
| `router` | Conditional branching via `path`+`op`+`value`→`targets` rules |
| `join` | Fan-in merge — `collect_array`, `merge_objects`, `first_non_null`, `all_success`, `quorum` |
| `parallel` | Fan-out dispatch with `JoinSet`-backed real concurrency |
| `passthrough` | No-op passthrough for fan-out distribution points |
| `state_transform` | 10 ops: `set`, `copy`, `delete`, `increment`, `append`, `merge`, `merge_object`, `select`, `compare`, `format` |
| `subgraph` | Compose another registered graph as a node |
| `human_approval` | HITL gate — emits `InterruptError`, resumes via checkpoint injection |

## MCP tools (25)

**Graph lifecycle:** `graph_create`, `graph_list`, `graph_inspect`, `graph_render`
**Execution:** `graph_execute`, `graph_run_start`, `graph_run_wait`, `graph_run_cancel`, `graph_run_get`
**State:** `graph_run_state`, `graph_run_events`, `graph_run_checkpoint`, `graph_run_resume`
**Approval:** `graph_approval_list`, `graph_approval_get`, `graph_approval_request`
**Evidence:** `graph_source_witness_capture`, `graph_source_witness_get`
**Templates:** `graph_template_list`, `graph_template_instantiate`, `graph_template_candidates`, `graph_template_outcomes`
**Policy:** `graph_policy_check`
**Receipt:** `graph_run_receipt`
**Status:** `graph_status`

## Built-in graph templates

| Template | Description |
|----------|-------------|
| `council_deliberation` | 3-analyst parallel council: coordinator → fanout → researchers → join → synthesize |
| `parallel_council` | 2-person debate: optimist vs skeptic → join → judge |
| `plan_critique_refine` | Sequential plan → critique → refine |
| `analysis_pipeline` | planner → researcher → extractor → synthesizer → validator with correction loop |
| `classifier_router` | LLM classifier routes input to bug/feature/question handlers |

## Ecosystem

| Crate | Version | Role |
|-------|---------|------|
| `ri-agent-graph` | v0.2.2 | Core graph execution engine |
| `agent-graph-mcp` | v0.2.3 | MCP protocol server |
| `stack-ids` | v0.1.3 | Trace/identity primitives (`TraceCtx`, `AttemptId`, `TrialId`) |
| `llm-pipeline` | v0.2.0 | Reusable LLM node payloads (Ollama, prompt templating, parsing) |

## Verification

```bash
# Engine
cargo test -p ri-agent-graph              # 149 tests
cargo clippy -p ri-agent-graph -- -D warnings

# MCP server
cargo test -p agent-graph-mcp --lib --test daemon_recovery --test mcp_integration  # 116 tests
cargo clippy -p agent-graph-mcp -- -D warnings
```

## License

MIT © [RecursiveIntell](https://github.com/RecursiveIntell) — applied R&D studio building local-first AI infrastructure.
