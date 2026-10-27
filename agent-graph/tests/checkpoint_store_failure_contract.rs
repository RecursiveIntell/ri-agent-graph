//! Contract tests for configured checkpoint-store persistence failures.

use ri_agent_graph::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

struct FailingCheckpointStore {
    inner: InMemoryCheckpointStore,
    operation: CheckpointStoreOperation,
}

impl FailingCheckpointStore {
    fn new(operation: CheckpointStoreOperation) -> Self {
        Self {
            inner: InMemoryCheckpointStore::new(),
            operation,
        }
    }

    fn fails(&self, operation: CheckpointStoreOperation) -> bool {
        self.operation == operation
    }

    fn injected_error() -> AgentGraphError {
        AgentGraphError::Other("injected checkpoint-store failure".to_string())
    }
}

impl CheckpointStore for FailingCheckpointStore {
    fn create_run(
        &self,
        graph_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<RunId>> + Send + '_>> {
        if self.fails(CheckpointStoreOperation::CreateRun) {
            return Box::pin(async { Err(Self::injected_error()) });
        }
        self.inner.create_run(graph_name)
    }

    fn record_attempt(
        &self,
        run_id: &str,
        node_id: &str,
        attempt: u32,
        input: &Value,
    ) -> Pin<Box<dyn Future<Output = Result<CheckpointAttemptId>> + Send + '_>> {
        if self.fails(CheckpointStoreOperation::RecordAttempt) {
            return Box::pin(async { Err(Self::injected_error()) });
        }
        self.inner.record_attempt(run_id, node_id, attempt, input)
    }

    fn complete_attempt(
        &self,
        attempt_id: &str,
        output: &Value,
        meta: &HashMap<String, Value>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        if self.fails(CheckpointStoreOperation::CompleteAttempt) {
            return Box::pin(async { Err(Self::injected_error()) });
        }
        self.inner.complete_attempt(attempt_id, output, meta)
    }

    fn fail_attempt(
        &self,
        attempt_id: &str,
        error: &str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        if self.fails(CheckpointStoreOperation::FailAttempt) {
            return Box::pin(async { Err(Self::injected_error()) });
        }
        self.inner.fail_attempt(attempt_id, error)
    }

    fn record_interrupt(
        &self,
        attempt_id: &str,
        interrupt: &Interrupt,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        self.inner.record_interrupt(attempt_id, interrupt)
    }

    fn save_state_snapshot(
        &self,
        run_id: &str,
        state: &HashMap<String, Value>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        if self.fails(CheckpointStoreOperation::SaveStateSnapshot) {
            return Box::pin(async { Err(Self::injected_error()) });
        }
        self.inner.save_state_snapshot(run_id, state)
    }

    fn load_run(
        &self,
        run_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<RunState>>> + Send + '_>> {
        self.inner.load_run(run_id)
    }

    fn complete_run(&self, run_id: &str) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        if self.fails(CheckpointStoreOperation::CompleteRun) {
            return Box::pin(async { Err(Self::injected_error()) });
        }
        self.inner.complete_run(run_id)
    }

    fn fail_run(
        &self,
        run_id: &str,
        error: &str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        if self.fails(CheckpointStoreOperation::FailRun) {
            return Box::pin(async { Err(Self::injected_error()) });
        }
        self.inner.fail_run(run_id, error)
    }
}

fn assert_checkpoint_store_failure(
    result: Result<AgentState>,
    operation: CheckpointStoreOperation,
) {
    assert!(matches!(
        result,
        Err(AgentGraphError::CheckpointStore {
            operation: actual,
            ..
        }) if actual == operation
    ));
}

#[tokio::test]
async fn configured_store_creation_failure_never_executes_a_node() {
    let graph = AgentGraph::builder()
        .with_checkpoint_store(Arc::new(FailingCheckpointStore::new(
            CheckpointStoreOperation::CreateRun,
        )))
        .add_node(
            "step",
            node!(|_state| async move {
                Err::<(), _>(AgentGraphError::ExecutionError(
                    "node executed despite failed run creation".to_string(),
                ))
            }),
        )
        .build()
        .unwrap();

    assert_checkpoint_store_failure(
        graph.execute("step", AgentState::new()).await,
        CheckpointStoreOperation::CreateRun,
    );
}

#[tokio::test]
async fn attempt_and_snapshot_failures_cannot_report_success() {
    for operation in [
        CheckpointStoreOperation::RecordAttempt,
        CheckpointStoreOperation::CompleteAttempt,
        CheckpointStoreOperation::SaveStateSnapshot,
        CheckpointStoreOperation::CompleteRun,
    ] {
        let graph = AgentGraph::builder()
            .with_checkpoint_store(Arc::new(FailingCheckpointStore::new(operation)))
            .add_node(
                "step",
                node!(|state| async move {
                    state.set("completed", true).await?;
                    Ok(())
                }),
            )
            .build()
            .unwrap();

        assert_checkpoint_store_failure(graph.execute("step", AgentState::new()).await, operation);
    }
}

#[tokio::test]
async fn failure_persistence_failure_is_visible() {
    let graph = AgentGraph::builder()
        .with_checkpoint_store(Arc::new(FailingCheckpointStore::new(
            CheckpointStoreOperation::FailAttempt,
        )))
        .add_node(
            "step",
            node!(|_state| async move {
                Err::<(), _>(AgentGraphError::ExecutionError("node failure".to_string()))
            }),
        )
        .build()
        .unwrap();

    assert_checkpoint_store_failure(
        graph.execute("step", AgentState::new()).await,
        CheckpointStoreOperation::FailAttempt,
    );
}

#[tokio::test]
async fn terminal_failure_persistence_failure_is_visible() {
    let graph = AgentGraph::builder()
        .with_checkpoint_store(Arc::new(FailingCheckpointStore::new(
            CheckpointStoreOperation::FailRun,
        )))
        .add_node(
            "step",
            node!(|_state| async move {
                Err::<(), _>(AgentGraphError::ExecutionError("node failure".to_string()))
            }),
        )
        .build()
        .unwrap();

    assert_checkpoint_store_failure(
        graph.execute("step", AgentState::new()).await,
        CheckpointStoreOperation::FailRun,
    );
}

#[tokio::test]
async fn no_store_execution_remains_compatible() {
    let graph = AgentGraph::builder()
        .add_node(
            "step",
            node!(|state| async move {
                state.set("completed", true).await?;
                Ok(())
            }),
        )
        .build()
        .unwrap();

    let result = graph.execute("step", AgentState::new()).await.unwrap();
    let completed: bool = result.get("completed").await.unwrap();
    assert!(completed);
}

#[tokio::test]
async fn record_attempt_failure_prevents_context_executor_invocation() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct CountExecutor(Arc<AtomicUsize>);
        impl Executor for CountExecutor {
            fn execute_node(
                &self,
                _: Arc<dyn Node>,
                _: AgentState,
                _: GraphConfig,
            ) -> Pin<Box<dyn Future<Output = Result<NodeOutput>> + Send>> {
                Box::pin(async { panic!("legacy entrypoint must not be used") })
            }
            fn execute_node_with_context(
                &self,
                _: Arc<dyn Node>,
                _: AgentState,
                _: GraphConfig,
                _: NodeExecutionContext,
            ) -> Pin<Box<dyn Future<Output = Result<NodeOutput>> + Send>> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(NodeOutput::Done) })
            }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let graph = AgentGraph::builder()
            .with_executor(Arc::new(CountExecutor(calls.clone())))
            .with_checkpoint_store(Arc::new(FailingCheckpointStore::new(
                CheckpointStoreOperation::RecordAttempt,
            )))
            .add_node("step", node!(|_state| async move { Ok(()) }))
            .build()
            .unwrap();
        assert_checkpoint_store_failure(
            graph.execute("step", AgentState::new()).await,
            CheckpointStoreOperation::RecordAttempt,
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    })
    .await
    .expect("30-second case limit");
}
