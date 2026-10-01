//! Correlation-only AG2a-core. These observations are not native authority,
//! causal parent links, durable receipts, or evidence of physical cancellation.
#![allow(deprecated)]
use ri_agent_graph::prelude::*;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

type NodeFuture = Pin<Box<dyn Future<Output = Result<NodeOutput>> + Send>>;

#[derive(Default)]
struct Observations {
    contexts: Mutex<Vec<NodeExecutionContext>>,
    events: Mutex<Vec<GraphEvent>>,
}
impl EventSink for Observations {
    fn emit(&self, event: GraphEvent) {
        self.events.lock().unwrap().push(event);
    }
}
struct ObservingExecutor(Arc<Observations>);
impl Executor for ObservingExecutor {
    fn execute_node(&self, _: Arc<dyn Node>, _: AgentState, _: GraphConfig) -> NodeFuture {
        Box::pin(async {
            Err(AgentGraphError::ExecutionError(
                "legacy path rejected".into(),
            ))
        })
    }
    fn execute_node_with_context(
        &self,
        node: Arc<dyn Node>,
        state: AgentState,
        config: GraphConfig,
        context: NodeExecutionContext,
    ) -> NodeFuture {
        self.0.contexts.lock().unwrap().push(context);
        Box::pin(async move { node.execute(&state, &config).await })
    }
}
fn instrumented(observations: &Arc<Observations>) -> AgentGraphBuilder {
    AgentGraph::builder()
        .with_executor(Arc::new(ObservingExecutor(observations.clone())))
        .with_event_sink(observations.clone())
}
fn noop() -> Box<dyn Node> {
    node!(|_state| async move { Ok(()) })
}
enum ActionNode {
    Flaky(Arc<AtomicUsize>),
    Pending(Arc<Semaphore>),
    FailAfter(Arc<Semaphore>),
}
#[async_trait::async_trait]
impl Node for ActionNode {
    async fn execute(&self, _: &AgentState, _: &GraphConfig) -> Result<NodeOutput> {
        match self {
            Self::Flaky(calls) => {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(AgentGraphError::ExecutionError("first fails".into()))
                } else {
                    Ok(NodeOutput::Done)
                }
            }
            Self::Pending(signal) => {
                signal.add_permits(1);
                std::future::pending::<()>().await;
                Ok(NodeOutput::Done)
            }
            Self::FailAfter(started) => {
                started.acquire().await.unwrap().forget();
                Err(AgentGraphError::ExecutionError("sibling fails".into()))
            }
        }
    }
}

fn assert_start(context: &NodeExecutionContext, events: &[GraphEvent]) {
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event,
                GraphEvent::NodeStart { run_id, node_id, attempt_id, trial_id, .. }
                if run_id == context.run_id() && node_id == context.node_id()
                    && attempt_id.as_ref() == Some(context.attempt_id())
                    && trial_id.as_ref() == Some(context.trial_id())
            ))
            .count(),
        1
    );
}
fn assert_end(context: &NodeExecutionContext, events: &[GraphEvent], expected: NodeOutcomeKind) {
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event,
                GraphEvent::NodeEnd { run_id, node_id, attempt_id, trial_id, outcome, .. }
                if run_id == context.run_id() && node_id == context.node_id()
                    && attempt_id.as_ref() == Some(context.attempt_id())
                    && trial_id.as_ref() == Some(context.trial_id()) && std::mem::discriminant(outcome) == std::mem::discriminant(&expected)
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn sequential_context_matches_actual_events() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let seen = Arc::new(Observations::default());
        let graph = instrumented(&seen).add_node("one", noop()).build().unwrap();
        graph.execute("one", AgentState::new()).await.unwrap();
        let contexts = seen.contexts.lock().unwrap();
        let events = seen.events.lock().unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].node_id(), "one");
        assert_eq!(contexts[0].retry_ordinal(), 0);
        assert_start(&contexts[0], &events);
        assert_end(&contexts[0], &events, NodeOutcomeKind::Success);
    })
    .await
    .expect("30-second case limit");
}

#[tokio::test]
async fn retry_family_is_stable_and_each_trial_matches_start_and_end() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let seen = Arc::new(Observations::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let graph = instrumented(&seen)
            .add_node_with_retry(
                "flaky",
                Box::new(ActionNode::Flaky(calls.clone())),
                RetryPolicy::new()
                    .with_max_attempts(2)
                    .with_initial_interval(Duration::from_millis(1))
                    .with_jitter(false),
            )
            .build()
            .unwrap();
        graph.execute("flaky", AgentState::new()).await.unwrap();
        let c = seen.contexts.lock().unwrap();
        let e = seen.events.lock().unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!(c[0].attempt_id(), c[1].attempt_id());
        assert_ne!(c[0].trial_id(), c[1].trial_id());
        assert_eq!((c[0].retry_ordinal(), c[1].retry_ordinal()), (0, 1));
        for context in c.iter() {
            assert_start(context, &e);
        }
        assert_end(&c[0], &e, NodeOutcomeKind::Failed);
        assert_end(&c[1], &e, NodeOutcomeKind::Success);
        let order: Vec<_> = e
            .iter()
            .filter_map(|event| match event {
                GraphEvent::NodeStart { .. } => Some("start"),
                GraphEvent::NodeEnd { .. } => Some("end"),
                _ => None,
            })
            .collect();
        assert_eq!(order, ["start", "end", "start", "end"]);
    })
    .await
    .expect("30-second case limit");
}

#[tokio::test]
async fn parallel_and_simultaneous_runs_do_not_mix_contexts() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let seen = Arc::new(Observations::default());
        let graph = instrumented(&seen)
            .add_node("start", noop())
            .add_node("left", noop())
            .add_node("right", noop())
            .add_node("join", noop())
            .add_edge("start", "left")
            .add_edge("start", "right")
            .add_edge("left", "join")
            .add_edge("right", "join")
            .build()
            .unwrap();
        let (a, b) = tokio::join!(
            graph.execute("start", AgentState::new()),
            graph.execute("start", AgentState::new())
        );
        a.unwrap();
        b.unwrap();
        let c = seen.contexts.lock().unwrap();
        let e = seen.events.lock().unwrap();
        assert_eq!(c.len(), 8);
        let runs: HashSet<_> = c.iter().map(|x| x.run_id()).collect();
        let trials: HashSet<_> = c.iter().map(|x| x.trial_id().to_string()).collect();
        assert_eq!(runs.len(), 2);
        assert_eq!(trials.len(), 8);
        for run in runs {
            let own: Vec<_> = c.iter().filter(|context| context.run_id() == run).collect();
            let names: HashSet<_> = own.iter().map(|context| context.node_id()).collect();
            let families: HashSet<_> = own
                .iter()
                .map(|context| context.attempt_id().to_string())
                .collect();
            assert_eq!(own.len(), 4);
            assert_eq!(names, HashSet::from(["start", "left", "right", "join"]));
            assert_eq!(families.len(), 4);
        }
        for context in c.iter() {
            assert_start(context, &e);
            assert_end(context, &e, NodeOutcomeKind::Success);
        }
    })
    .await
    .expect("30-second case limit");
}

#[tokio::test]
async fn nested_graphs_keep_distinct_run_identity_without_claiming_causal_parent() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let seen = Arc::new(Observations::default());
        let inner = instrumented(&seen)
            .add_node("same", noop())
            .add_edge(START, "same")
            .build()
            .unwrap();
        let outer = instrumented(&seen)
            .add_subgraph("same", inner)
            .build()
            .unwrap();
        outer.execute("same", AgentState::new()).await.unwrap();
        let c = seen.contexts.lock().unwrap();
        let e = seen.events.lock().unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!(c[0].node_id(), c[1].node_id());
        assert_ne!(c[0].run_id(), c[1].run_id());
        assert_ne!(c[0].trial_id(), c[1].trial_id());
        for context in c.iter() {
            assert_start(context, &e);
            assert_end(context, &e, NodeOutcomeKind::Success);
        }
    })
    .await
    .expect("30-second case limit");
}

#[tokio::test]
async fn mutable_state_cannot_spoof_engine_context() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let seen = Arc::new(Observations::default());
        let graph = instrumented(&seen)
            .add_node("real", noop())
            .build()
            .unwrap();
        let state = AgentState::new();
        for key in ["run_id", "node_id", "attempt_id", "trial_id", "context"] {
            state.set(key, "forged").await.unwrap();
        }
        graph.execute("real", state).await.unwrap();
        let c = seen.contexts.lock().unwrap();
        assert_eq!(c[0].node_id(), "real");
        assert_ne!(c[0].run_id(), "forged");
        assert_ne!(c[0].attempt_id().to_string(), "forged");
        assert_ne!(c[0].trial_id().to_string(), "forged");
    })
    .await
    .expect("30-second case limit");
}

struct LegacyExecutor(Arc<AtomicUsize>);
impl Executor for LegacyExecutor {
    fn execute_node(
        &self,
        node: Arc<dyn Node>,
        state: AgentState,
        config: GraphConfig,
    ) -> NodeFuture {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { node.execute(&state, &config).await })
    }
}
#[tokio::test]
async fn legacy_executor_default_remains_compatible() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let calls = Arc::new(AtomicUsize::new(0));
        let graph = AgentGraph::builder()
            .with_executor(Arc::new(LegacyExecutor(calls.clone())))
            .add_node("old", noop())
            .build()
            .unwrap();
        graph.execute("old", AgentState::new()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        AgentGraph::builder()
            .add_node("direct", noop())
            .build()
            .unwrap()
            .execute("direct", AgentState::new())
            .await
            .unwrap();
    })
    .await
    .expect("30-second case limit");
}

#[tokio::test]
async fn cancellation_before_dispatch_creates_no_executor_observation() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let seen = Arc::new(Observations::default());
        let graph = Arc::new(
            instrumented(&seen)
                .add_node("never", noop())
                .build()
                .unwrap(),
        );
        let (handle, cancel) =
            graph.execute_cancellable("never", AgentState::new(), GraphConfig::default());
        cancel.store(true, Ordering::SeqCst);
        assert!(matches!(
            handle.await.unwrap(),
            Err(AgentGraphError::Cancelled)
        ));
        assert!(seen.contexts.lock().unwrap().is_empty());
    })
    .await
    .expect("30-second case limit");
}

struct CancelAfterFailure {
    flag: Arc<Mutex<Option<Arc<AtomicBool>>>>,
}
impl EventSink for CancelAfterFailure {
    fn emit(&self, event: GraphEvent) {
        if matches!(
            event,
            GraphEvent::NodeEnd {
                outcome: NodeOutcomeKind::Failed,
                ..
            }
        ) {
            self.flag
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .store(true, Ordering::SeqCst);
        }
    }
}
#[tokio::test]
async fn cancellation_between_retries_does_not_issue_second_context() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let seen = Arc::new(Observations::default());
        let flag = Arc::new(Mutex::new(None));
        let graph = Arc::new(
            instrumented(&seen)
                .with_event_sink(Arc::new(CancelAfterFailure { flag: flag.clone() }))
                .add_node_with_retry(
                    "fails",
                    node!(|_state| async move {
                        Err::<(), _>(AgentGraphError::ExecutionError("fail".into()))
                    }),
                    RetryPolicy::new()
                        .with_max_attempts(2)
                        .with_initial_interval(Duration::from_millis(1))
                        .with_jitter(false),
                )
                .build()
                .unwrap(),
        );
        let (handle, cancel) =
            graph.execute_cancellable("fails", AgentState::new(), GraphConfig::default());
        *flag.lock().unwrap() = Some(cancel);
        assert!(matches!(
            handle.await.unwrap(),
            Err(AgentGraphError::Cancelled)
        ));
        assert_eq!(seen.contexts.lock().unwrap().len(), 1);
    })
    .await
    .expect("30-second case limit");
}

#[tokio::test]
async fn dropped_events_do_not_drop_direct_context_delivery() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let seen = Arc::new(Observations::default());
        let graph = instrumented(&seen)
            .with_event_sink(Arc::new(NoopEventSink))
            .add_node("one", noop())
            .build()
            .unwrap();
        graph.execute("one", AgentState::new()).await.unwrap();
        assert_eq!(seen.contexts.lock().unwrap().len(), 1);
        assert!(seen.events.lock().unwrap().is_empty());
    })
    .await
    .expect("30-second case limit");
}

#[tokio::test]
async fn retry_exhaustion_has_no_false_success() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let seen = Arc::new(Observations::default());
        let graph = instrumented(&seen)
            .add_node_with_retry(
                "fails",
                node!(|_state| async move {
                    Err::<(), _>(AgentGraphError::ExecutionError("fail".into()))
                }),
                RetryPolicy::new()
                    .with_max_attempts(2)
                    .with_initial_interval(Duration::from_millis(1))
                    .with_jitter(false),
            )
            .build()
            .unwrap();
        assert!(graph.execute("fails", AgentState::new()).await.is_err());
        let c = seen.contexts.lock().unwrap();
        let e = seen.events.lock().unwrap();
        assert_eq!(c.len(), 2);
        for context in c.iter() {
            assert_start(context, &e);
            assert_end(context, &e, NodeOutcomeKind::Failed);
        }
        assert!(!e.iter().any(|event| matches!(
            event,
            GraphEvent::NodeEnd {
                outcome: NodeOutcomeKind::Success,
                ..
            }
        )));
    })
    .await
    .expect("30-second case limit");
}

#[tokio::test]
async fn sibling_failure_preserves_dispatched_context_but_not_terminal_join_claim() {
    tokio::time::timeout(Duration::from_secs(30), async {
    let seen = Arc::new(Observations::default());
    let started = Arc::new(Semaphore::new(0));
    let graph = instrumented(&seen)
        .add_node("root", noop())
        .add_node("pending", Box::new(ActionNode::Pending(started.clone())))
        .add_node("failure", Box::new(ActionNode::FailAfter(started.clone())))
        .add_edge("root", "pending")
        .add_edge("root", "failure")
        .build()
        .unwrap();
    assert!(tokio::time::timeout(
        Duration::from_secs(2),
        graph.execute("root", AgentState::new())
    )
    .await
    .unwrap()
    .is_err());
    let c = seen.contexts.lock().unwrap();
    let e = seen.events.lock().unwrap();
    assert_eq!(c.len(), 3);
    for context in c.iter() {
        assert_start(context, &e);
    }
    assert!(e.iter().any(|event| matches!(event, GraphEvent::NodeEnd { node_id, outcome: NodeOutcomeKind::Interrupted, attempt_id: None, trial_id: None, .. } if node_id == "pending")));
    assert!(!e.iter().any(|event| matches!(event, GraphEvent::NodeEnd { node_id, outcome: NodeOutcomeKind::Success, .. } if node_id == "pending")));
    }).await.expect("30-second case limit");
}

#[tokio::test]
async fn parent_handle_abort_does_not_fabricate_unstarted_context_or_success() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let seen = Arc::new(Observations::default());
        let started = Arc::new(Semaphore::new(0));
        let graph = Arc::new(
            instrumented(&seen)
                .add_node("pending", Box::new(ActionNode::Pending(started.clone())))
                .add_node("never", noop())
                .add_edge("pending", "never")
                .build()
                .unwrap(),
        );
        let (handle, _) =
            graph.execute_cancellable("pending", AgentState::new(), GraphConfig::default());
        tokio::time::timeout(Duration::from_secs(2), started.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
        let c = seen.contexts.lock().unwrap();
        let e = seen.events.lock().unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].node_id(), "pending");
        assert_start(&c[0], &e);
        assert!(!e.iter().any(|event| matches!(
            event,
            GraphEvent::NodeEnd {
                outcome: NodeOutcomeKind::Success,
                ..
            }
        )));
    })
    .await
    .expect("30-second case limit");
}

#[tokio::test]
async fn queued_parallel_branch_gets_no_context_after_first_branch_failure() {
    tokio::time::timeout(Duration::from_secs(30), async {
    let seen = Arc::new(Observations::default());
    let graph = instrumented(&seen).add_node("root", noop())
        .add_node("fails", node!(|_state| async move { Err::<(), _>(AgentGraphError::ExecutionError("first fails".into())) }))
        .add_node("never", noop()).add_edge("root", "fails").add_edge("root", "never")
        .build().unwrap();
    let result = graph.execute_with_config("root", AgentState::new(), GraphConfig::default().with_max_parallelism(1)).await;
    assert!(matches!(result, Err(AgentGraphError::ExecutionError(message)) if message == "first fails"));
    let c = seen.contexts.lock().unwrap(); let e = seen.events.lock().unwrap();
    assert_eq!(c.len(), 2);
    assert!(c.iter().all(|context| context.node_id() != "never"));
    assert!(!e.iter().any(|event| matches!(event, GraphEvent::NodeStart { node_id, .. } if node_id == "never")));
    for context in c.iter() { assert_start(context, &e); }
    }).await.expect("30-second case limit");
}
