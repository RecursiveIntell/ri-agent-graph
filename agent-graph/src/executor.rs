//! Executor abstraction for running node attempts.
//!
//! The [`Executor`] trait allows plugging in different execution strategies
//! (in-process, external job queue, etc.) without changing graph logic.

use crate::command::NodeOutput;
use crate::config::GraphConfig;
use crate::node::Node;
use crate::state::AgentState;
use crate::Result;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Structural origin of a graph run. This is observation, never authority.
#[derive(Debug, Clone)]
pub enum RunLineage {
    Root,
    LinkedSubgraph(ParentTrialRef),
    UnlinkedSubgraph,
}

/// Immediate supplied parent trial, without recursive ancestry or liveness proof.
/// Captured contexts may be replayed; consumers must independently match live parents.
#[derive(Debug, Clone)]
pub struct ParentTrialRef {
    run_id: String,
    node_id: String,
    attempt_id: stack_ids::AttemptId,
    trial_id: stack_ids::TrialId,
    root_run_id: Option<String>,
}
impl ParentTrialRef {
    pub fn run_id(&self) -> &str {
        &self.run_id
    }
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
    pub fn attempt_id(&self) -> &stack_ids::AttemptId {
        &self.attempt_id
    }
    pub fn trial_id(&self) -> &stack_ids::TrialId {
        &self.trial_id
    }
    pub fn root_run_id(&self) -> Option<&str> {
        self.root_run_id.as_deref()
    }
}

/// Immutable identity issued by the engine for one graph execution trial.
///
/// Observation API contract v1; this is not a signed or durable wire artifact.
///
/// This observation is not permission, a durable receipt, or proof of a live parent.
/// Structural parent observations may be replayed. Nested graphs have their own run identity. Legacy event sinks may
/// drop events, so executors receive this value directly rather than joining
/// mutable state or event timing. Only the engine constructs it.
#[derive(Debug, Clone)]
pub struct NodeExecutionContext {
    run_id: String,
    node_id: String,
    attempt_id: stack_ids::AttemptId,
    trial_id: stack_ids::TrialId,
    retry_ordinal: usize,
    lineage: RunLineage,
}

impl NodeExecutionContext {
    pub(crate) fn new(
        run_id: String,
        node_id: String,
        attempt_id: stack_ids::AttemptId,
        trial_id: stack_ids::TrialId,
        retry_ordinal: usize,
        lineage: RunLineage,
    ) -> Self {
        Self {
            run_id,
            node_id,
            attempt_id,
            trial_id,
            retry_ordinal,
            lineage,
        }
    }

    pub fn lineage(&self) -> &RunLineage {
        &self.lineage
    }
    pub fn root_run_id(&self) -> Option<&str> {
        match &self.lineage {
            RunLineage::Root => Some(&self.run_id),
            RunLineage::LinkedSubgraph(parent) => parent.root_run_id(),
            RunLineage::UnlinkedSubgraph => None,
        }
    }
    pub(crate) fn parent_ref(&self) -> ParentTrialRef {
        ParentTrialRef {
            run_id: self.run_id.clone(),
            node_id: self.node_id.clone(),
            attempt_id: self.attempt_id.clone(),
            trial_id: self.trial_id.clone(),
            root_run_id: self.root_run_id().map(str::to_owned),
        }
    }

    /// Identity of this graph execution, including a nested graph's own run.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }
    /// Node name within this graph execution.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
    /// Stable identity shared by retries of this node invocation.
    pub fn attempt_id(&self) -> &stack_ids::AttemptId {
        &self.attempt_id
    }
    /// Fresh identity of this concrete graph execution trial.
    pub fn trial_id(&self) -> &stack_ids::TrialId {
        &self.trial_id
    }
    /// Zero-based ordinal within the retry family.
    pub fn retry_ordinal(&self) -> usize {
        self.retry_ordinal
    }
}

/// Trait for executing individual node attempts.
///
/// The default [`InProcessExecutor`] runs nodes directly in the current tokio runtime.
/// Alternative implementations (e.g., tauri-queue) can be provided behind feature flags.
///
/// Uses boxed futures instead of async-trait.
pub trait Executor: Send + Sync {
    /// Execute a single node attempt.
    ///
    /// The executor owns the `Arc<dyn Node>`, `AgentState`, and `GraphConfig`
    /// so the returned future is `'static` and can be spawned on a task.
    fn execute_node(
        &self,
        node: Arc<dyn Node>,
        state: AgentState,
        config: GraphConfig,
    ) -> Pin<Box<dyn Future<Output = Result<NodeOutput>> + Send>>;

    /// Execute with the exact engine-issued trial observation.
    ///
    /// The default preserves legacy executors and intentionally ignores the
    /// context. It does not make them governed executors. An authority-bearing
    /// adapter must override this method and reject its legacy entrypoint;
    /// execution identity alone never supplies effect permission.
    fn execute_node_with_context(
        &self,
        node: Arc<dyn Node>,
        state: AgentState,
        config: GraphConfig,
        _context: NodeExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<NodeOutput>> + Send>> {
        self.execute_node(node, state, config)
    }
}

/// Default executor: runs nodes directly in the current tokio runtime.
pub struct InProcessExecutor;

impl InProcessExecutor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for InProcessExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor for InProcessExecutor {
    fn execute_node(
        &self,
        node: Arc<dyn Node>,
        state: AgentState,
        config: GraphConfig,
    ) -> Pin<Box<dyn Future<Output = Result<NodeOutput>> + Send>> {
        Box::pin(async move { node.execute(&state, &config).await })
    }
    fn execute_node_with_context(
        &self,
        node: Arc<dyn Node>,
        state: AgentState,
        config: GraphConfig,
        context: NodeExecutionContext,
    ) -> Pin<Box<dyn Future<Output = Result<NodeOutput>> + Send>> {
        Box::pin(async move { node.execute_with_context(&state, &config, context).await })
    }
}
