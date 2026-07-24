# agent-graph-mcp

`agent-graph-mcp` is an MCP server that compiles bounded declarative workflow specs to the real `agent_graph::AgentGraph` runtime. It keeps the original `graph_create`, `graph_execute`, and `graph_status` names and accepts the original V1 graph shape.

**Status (2026-07-24): All 28 AG hostile-audit findings closed. 116 tests passing. Daemon + proxy transport verified over Unix socket. Ready for Hermes staging.**

## Quick Start (Hermes)

```bash
# Build and install
cargo build --release -p agent-graph-mcp
cp target/release/agent-graph-mcp ~/.cargo/bin/
cp target/release/agent-graph-mcpd ~/.cargo/bin/

# Start daemon
mkdir -p ~/.local/share/agent-graph
openssl rand -hex 32 > ~/.local/share/agent-graph/integrity.key
agent-graph-mcpd --data-dir ~/.local/share/agent-graph --socket /tmp/agent-graph.sock &

# Configure Hermes (already done: hermes mcp add agent_graph --command ...)
# The proxy binary auto-connects to the daemon socket
```

## Architecture

```
Hermes ──→ agent-graph-mcp (proxy) ──Unix socket──→ agent-graph-mcpd (daemon) ──→ SQLite
              stdin/stdout                framed             Tokio async I/O
```

- **Daemon** (`agent-graph-mcpd`): Single-process owner with file lock, Tokio async Unix socket listener, SQLite-backed persistence, startup mode enforcement, crash recovery
- **Proxy** (`agent-graph-mcp`): Stateless stdin/stdout ↔ framed socket bridge (or `--direct` for legacy in-process mode)
- **Socket**: 0600 perms, 4-byte BE length prefix + JSON-RPC 2.0 framing

## Capability boundary

- Runs are process-local and reported as `volatile` while active. With `--data-dir`, terminal projections and explicitly requested deterministic pre-execution checkpoints are persisted to SQLite; uncheckpointed active rows still become `interrupted_non_resumable` after restart.
- Normal execution is synchronous. `graph_execute {\"mode\":\"async\"}` starts a background run that can be inspected and cancellation-requested by run ID.
- Cancellation is observed while an LLM future is in flight by dropping the local provider future (best effort). The underlying provider request may still be in flight; terminal cancellation is recorded when the graph observes the interruption.
- With SQLite enabled, terminal write failures remain visible in the run record as `storage_class: \"volatile\"` with `persistence_error`; no failed write is reported as durable.
- Durable integrity-sensitive records require `AGENT_GRAPH_INTEGRITY_KEY_PATH` to name a readable external file containing at least 32 secret bytes. The key is never written to SQLite, receipts, or bundles. Without it, checkpoint/resume, durable approval, terminal receipt, and source-witness operations fail closed with `INTEGRITY_KEY_REQUIRED`.
- `graph_run_start {\"checkpoint\":true}` is an intentional pre-execution checkpoint. `graph_run_checkpoint` reads it and `graph_run_resume` reserves execution capacity before atomically consuming it once. Resume is available only for a linear chain of deterministic `passthrough` and local `state_transform` nodes, with SQLite-bound state, budget, graph-version, dependency, cursor, and HMAC-SHA256 checkpoint authentication. This is deterministic local resume, not generic replay.
- Unordered parallel writes to the same state key are rejected unless `GraphSpec.reducers` declares a reducer; sequential repeated writes remain allowed.
- `evidence_required` requires durable SQLite-backed local witness IDs and bounded UTF-8 spans. Witness capture stores caller-supplied content only; locators are never fetched, and source authority is never independently verified.
- Terminal receipts, checkpoints, approvals, and witnesses use HMAC-SHA256 authentication; their redacted bundles remain `integrity_only`. They do not prove an external model call occurred and are not complete replay.
- `graph_run_start` accepts optional positive-integer `max_wall_clock_ms` and `max_nodes` budgets. Requested budgets and observed counters are included in terminal projections and receipts. `max_llm_calls` is rejected with `INVALID_BUDGETS` because this permitted runtime path has no real LLM invocation hook.
- LLM, router, join, parallel, loop, subgraph, external/tool, provider, uncaptured source-witness, and generic replay behavior are excluded from resume. Durable approval is supported only as a SQLite-backed decision over an already-created deterministic-local checkpoint; it cannot execute HumanApproval nodes, arbitrary Hermes tools, shell, filesystem, provider actions, or secret/environment references.
- **Crash recovery**: Interrupted runs report `interrupted` after restart (never `running` or `completed`). Checkpoint transactions are atomic — no partial rows after crash. Uncommitted `graph_create` rolled back on restart.

## Daemon controls

```
agent-graph-mcpd --data-dir PATH --socket PATH

Environment:
  AGENT_GRAPH_INTEGRITY_KEY_PATH  Path to 32+ byte integrity key file
```

- Startup mode (keyed vs keyless) is durable across restarts; flipping modes is rejected.
- Concurrent daemon instances on the same data directory are rejected via file lock.
- Legacy schema variants (missing `executions` table or `owner_instance_id` column) are safely handled.

## Tools (25 available)

`graph_create`, `graph_execute`, `graph_run_start`, `graph_run_wait`, `graph_run_cancel`, `graph_run_get`, `graph_run_state`, `graph_run_events`, `graph_run_receipt`, `graph_run_checkpoint`, `graph_run_resume`, `graph_list`, `graph_inspect`, `graph_render`, `graph_status`, `graph_policy_check`, `graph_approval_list`, `graph_approval_get`, `graph_approval_request`, `graph_template_list`, `graph_template_candidates`, `graph_template_instantiate`, `graph_template_outcomes`, `graph_source_witness_capture`, `graph_source_witness_get`

`graph_approval_decide` and `graph_delete` have been removed from the model-facing tool set (AG-002).

## Audit closure

All 28 hostile-audit findings (AG-001 through AG-028) are closed. See `agent-graph-mcp/docs/remediation/hostile-audit-closure-ledger.md` for the full evidence matrix.

| Gate | Result |
|------|--------|
| `cargo test` (lib + daemon_recovery + integration) | **116 passed, 0 failed** |
| `cargo fmt --all --check` | Clean |
| `cargo clippy -p agent-graph-mcp -- -D warnings` | Clean |
| `cargo audit` adjudication | 8 advisories, all unreachable from binary |
| Daemon MCP lifecycle | `initialize → tools/list` (25 tools) over Unix socket |
| Crash recovery | Interrupted-run detection, checkpoint integrity, graph_create atomicity |
| Startup mode enforcement | Keyed/keyless flip rejection |
| Process multiplicity | File lock + watchdog reacquisition |

## Verification

```bash
cargo fmt --check
cargo test -p agent-graph-mcp --lib --test daemon_recovery --test mcp_integration
cargo clippy -p agent-graph-mcp -- -D warnings
```

Evidence artifacts: `.hermes/evidence/agent-graph-mcp-lifecycle-*.log`, `.hermes/evidence/advisory-adjudication.json`
