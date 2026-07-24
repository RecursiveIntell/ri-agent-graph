# Changelog

## [Unreleased]
### Fixed
- [AG-1] Transaction lost-update anomaly: added version counter to `AgentState` with conflict detection on `commit()`
- [PATH-1] Path dependency normalized from `../../Libraries/stack-ids` to `../stack-ids`
### Changed
- [AG-2] Decomposed `graph.rs` monolith (1539 lines) into `graph.rs`, `builder.rs`, and `engine.rs`
- `commit()` now returns `Result<(), AgentGraphError>` instead of `()` (breaking change for callers)
### Added
- Test for concurrent transaction conflict detection
