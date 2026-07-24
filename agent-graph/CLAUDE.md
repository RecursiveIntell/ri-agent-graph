# CLAUDE.md — Instructions for Claude Code in This LangGraph Repo

## Mission
Refactor this Rust LangGraph-clone into a production-viable **graph orchestrator** that executes node work via a separate **Payload** layer (LangChain clone).

## Non-Negotiables
- Core crate must not depend on Tauri.
- Node I/O boundary is `serde_json::Value`.
- Must support PayloadNode executing `Box<dyn Payload + Send + Sync>`.
- Must implement checkpointing and interrupt/resume.
- Must implement deterministic fan-out/fan-in via explicit join semantics.
- Keep repo green: fmt/test/clippy.

## Architecture Targets
- `EventSink` for structured runtime events (including token streaming).
- `CheckpointStore` trait + in-memory default implementation.
- `Executor` trait + in-process default implementation.
- Scheduler supports branching, loops, and joins with explicit merge policy.

## Scope Discipline
Do NOT implement payload logic (LLM calls, parsing, streaming decode) in this repo.
Only orchestrate payload execution and record outcomes.

## Work Method
- Start by writing/Updating ARCHITECTURE.md with decisions.
- Land changes incrementally; don’t break compilation for long stretches.
- Add tests proving branching, loops, joins, interrupt/resume, cancel.

## If API Breaks
Prefer legacy module/feature and document migration in README.
