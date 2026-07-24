# AGENTS.md — Working Agreement for Refactoring This LangGraph Clone

## Prime Directive
This crate is the **orchestrator/runtime**. It must not become a second “payload library.”
Payload logic (LLM calls, parsing, streaming decoding) lives in the LangChain payload crate.

## Must-Haves
1. **Node boundary is `serde_json::Value`**
   - Heterogenous workflows are first-class.
   - Typed helpers can exist at edges, but runtime wiring is Value.

2. **Core has no Tauri dependency**
   - Any tauri-queue integration is feature-gated and/or in a separate crate.

3. **Checkpointing and interrupts are first-class**
   - Persist node attempts and outcomes.
   - Support interrupt/resume with injected input.

4. **Explicit concurrency semantics**
   - Fan-out/fan-in must be deterministic.
   - No implicit concurrent state merges without a join policy.

5. **Repo stays green**
   - cargo fmt
   - cargo test
   - cargo clippy -- -D warnings

## Allowed Scope
✅ Node types, scheduler/executor, checkpoint store, events, interrupt/resume  
✅ Feature-gated adapter to tauri-queue  
✅ Documentation and examples  
❌ No provider clients, no prompt templating, no JSON parsing logic here (belongs in payload crate)  
❌ No Tauri dependency in core crate  

## Definition of Done Checklist
- [ ] PayloadNode exists and runs `Box<dyn Payload>`
- [ ] Router and Join semantics implemented and tested
- [ ] Looping supported with termination controls
- [ ] CheckpointStore trait + in-memory implementation
- [ ] Interrupt/resume works end-to-end
- [ ] EventSink supports token and lifecycle events
- [ ] Optional tauri-queue executor adapter (feature-gated) if implemented
- [ ] README + ARCHITECTURE.md updated
- [ ] fmt/test/clippy clean
