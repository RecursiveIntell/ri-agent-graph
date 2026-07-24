# ri-agent-graph

**Graph-based agent orchestration for Rust** — a LangGraph-inspired execution engine with an MCP server, parallel fan-out/fan-in, checkpointing, interrupt/resume, and cryptographic receipts.

[![Crates.io — engine](https://img.shields.io/crates/v/ri-agent-graph?label=ri-agent-graph)](https://crates.io/crates/ri-agent-graph)
[![Crates.io — mcp](https://img.shields.io/crates/v/agent-graph-mcp?label=agent-graph-mcp)](https://crates.io/crates/agent-graph-mcp)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE-MIT)

---

## What's here

| Crate | crates.io | Description |
|-------|-----------|-------------|
| **[ri-agent-graph](./agent-graph/)** | [![v0.2.2](https://img.shields.io/crates/v/ri-agent-graph)](https://crates.io/crates/ri-agent-graph) | Core engine — `AgentGraph`, `GraphExecutor`, 8 node types, checkpointing, receipts, 149 tests |
| **[agent-graph-mcp](./agent-graph-mcp/)** | [![v0.2.4](https://img.shields.io/crates/v/agent-graph-mcp)](https://crates.io/crates/agent-graph-mcp) | MCP server — 25 typed tools, daemon/proxy, HITL, witnesses, templates |

> **The MCP server now has its own dedicated repo at [RecursiveIntell/agent-graph-mcp](https://github.com/RecursiveIntell/agent-graph-mcp)**

## Quick start

### Core engine

```bash
cargo add ri-agent-graph
```

```rust
use ri_agent_graph::prelude::*;

let graph = AgentGraph::builder()
    .add_node("greet", node!(|state| async move {
        state.set("msg", "hello").await?; Ok(())
    }))
    .add_edge(START, "greet")
    .add_edge("greet", END)
    .build()?;

let result = graph.execute(START, AgentState::new()).await?;
```

### MCP server

```bash
npx -y @recursiveintell/agent-graph-mcp --direct --base-url http://127.0.0.1:11434 --model glm-5.2:cloud
```

## Architecture

```
Hermes ──→ agent-graph-mcp (proxy) ──Unix socket──→ agent-graph-mcpd (daemon) ──→ SQLite
              stdin/stdout                framed             Tokio async I/O
```

## Node types

| Type | Description |
|------|-------------|
| `llm` | LLM call via Ollama with prompt, JSON mode, tool calls |
| `router` | Conditional branching via `path`+`op`+`value`→`targets` |
| `join` | Fan-in merge — `collect_array`, `merge_objects`, `first_non_null`, `all_success`, `quorum` |
| `parallel` | Fan-out dispatch with `JoinSet`-backed concurrency |
| `passthrough` | No-op passthrough for fan-out distribution |
| `state_transform` | 10 ops: `set`, `copy`, `delete`, `increment`, `append`, `merge`, `merge_object`, `select`, `compare`, `format` |
| `subgraph` | Compose another graph as a node |
| `human_approval` | HITL gate — emits `InterruptError`, resumes via checkpoint |

## MCP tools (25)

**Graph lifecycle:** `graph_create`, `graph_list`, `graph_inspect`, `graph_render` · **Execution:** `graph_execute`, `graph_run_start`, `graph_run_wait`, `graph_run_cancel`, `graph_run_get` · **State:** `graph_run_state`, `graph_run_events`, `graph_run_checkpoint`, `graph_run_resume` · **Approval:** `graph_approval_list`, `graph_approval_get`, `graph_approval_request` · **Evidence:** `graph_source_witness_capture`, `graph_source_witness_get` · **Templates:** `graph_template_list`, `graph_template_instantiate`, `graph_template_candidates`, `graph_template_outcomes` · **Policy:** `graph_policy_check`, **Receipt:** `graph_run_receipt`, **Status:** `graph_status`

## Built-in templates

| Template | Description |
|----------|-------------|
| `council_deliberation` | 3-analyst parallel: coordinator → fanout → researchers → join → synthesize |
| `parallel_council` | 2-person debate: optimist vs skeptic → join → judge |
| `plan_critique_refine` | plan → critique → refine |
| `analysis_pipeline` | planner → researcher → extractor → synthesizer → validator with correction loop |
| `classifier_router` | LLM classifier routes to bug/feature/question handlers |

## Ecosystem

| Crate | Version | Role |
|-------|---------|------|
| `ri-agent-graph` | v0.2.2 | Core engine |
| `agent-graph-mcp` | v0.2.4 | MCP protocol server |
| `stack-ids` | v0.1.3 | Trace/identity primitives |
| `llm-pipeline` | v0.2.0 | Reusable LLM node payloads |

## Verification

```bash
cargo test -p ri-agent-graph              # 149 tests
cargo test -p agent-graph-mcp --lib       # 116 tests
cargo clippy -- -D warnings
```

## License

MIT © [RecursiveIntell](https://github.com/RecursiveIntell)
