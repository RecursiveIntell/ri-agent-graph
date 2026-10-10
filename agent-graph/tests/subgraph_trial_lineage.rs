//! Structural correlation only: supplied parent context is replayable, not authority.
use ri_agent_graph::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

type Seen = Arc<Mutex<Vec<NodeExecutionContext>>>;
type NodeFuture = Pin<Box<dyn Future<Output = Result<NodeOutput>> + Send>>;
async fn bounded(f: impl Future<Output = ()>) {
    tokio::time::timeout(Duration::from_secs(30), f)
        .await
        .expect("30s case bound");
}
struct Tap {
    seen: Seen,
    child: Option<AgentGraph>,
    failures: Option<Arc<AtomicUsize>>,
    started: Option<Arc<Semaphore>>,
    release: Option<Arc<Semaphore>>,
}
#[async_trait::async_trait]
impl Node for Tap {
    async fn execute(&self, state: &AgentState, config: &GraphConfig) -> Result<NodeOutput> {
        if let Some(child) = &self.child {
            return Node::execute(child, state, config).await;
        }
        self.action(state).await
    }
    async fn execute_with_context(
        &self,
        state: &AgentState,
        config: &GraphConfig,
        ctx: NodeExecutionContext,
    ) -> Result<NodeOutput> {
        self.seen.lock().unwrap().push(ctx.clone());
        if let Some(child) = &self.child {
            return child.execute_with_context(state, config, ctx).await;
        }
        self.action(state).await
    }
}
impl Tap {
    async fn action(&self, state: &AgentState) -> Result<NodeOutput> {
        if let Some(s) = &self.started {
            s.add_permits(1);
        }
        if let Some(s) = &self.release {
            s.acquire().await.unwrap().forget();
        }
        state.set("written", true).await?;
        if self
            .failures
            .as_ref()
            .is_some_and(|n| n.fetch_add(1, Ordering::SeqCst) == 0)
        {
            return Err(AgentGraphError::ExecutionError("first trial fails".into()));
        }
        Ok(NodeOutput::Done)
    }
}
fn tap(seen: &Seen, child: Option<AgentGraph>) -> Tap {
    Tap {
        seen: seen.clone(),
        child,
        failures: None,
        started: None,
        release: None,
    }
}
struct Forward {
    calls: Arc<AtomicUsize>,
    legacy: bool,
}
impl Executor for Forward {
    fn execute_node(&self, n: Arc<dyn Node>, s: AgentState, c: GraphConfig) -> NodeFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { n.execute(&s, &c).await })
    }
    fn execute_node_with_context(
        &self,
        n: Arc<dyn Node>,
        s: AgentState,
        c: GraphConfig,
        x: NodeExecutionContext,
    ) -> NodeFuture {
        if self.legacy {
            return self.execute_node(n, s, c);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { n.execute_with_context(&s, &c, x).await })
    }
}
struct Legacy(Arc<AtomicUsize>);
impl Executor for Legacy {
    fn execute_node(&self, n: Arc<dyn Node>, s: AgentState, c: GraphConfig) -> NodeFuture {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { n.execute(&s, &c).await })
    }
}
fn builder(mode: usize, calls: &Arc<AtomicUsize>) -> AgentGraphBuilder {
    match mode {
        0 => AgentGraph::builder(),
        1 => AgentGraph::builder().with_executor(Arc::new(InProcessExecutor::new())),
        2 => AgentGraph::builder().with_executor(Arc::new(Forward {
            calls: calls.clone(),
            legacy: false,
        })),
        _ => AgentGraph::builder().with_executor(Arc::new(Legacy(calls.clone()))),
    }
}
fn chain(seen: &Seen, mode: usize, calls: &Arc<AtomicUsize>) -> AgentGraph {
    let grandchild = builder(mode, calls)
        .add_node("leaf", Box::new(tap(seen, None)))
        .build()
        .unwrap();
    let child = builder(mode, calls)
        .add_node("grandchild", Box::new(tap(seen, Some(grandchild))))
        .build()
        .unwrap();
    builder(mode, calls)
        .add_node("child", Box::new(tap(seen, Some(child))))
        .build()
        .unwrap()
}
fn linked(ctx: &NodeExecutionContext) -> &ParentTrialRef {
    match ctx.lineage() {
        RunLineage::LinkedSubgraph(p) => p,
        other => panic!("expected linked, got {other:?}"),
    }
}
fn assert_link(child: &NodeExecutionContext, parent: &NodeExecutionContext) {
    let p = linked(child);
    assert_eq!(p.run_id(), parent.run_id());
    assert_eq!(p.node_id(), parent.node_id());
    assert_eq!(p.attempt_id(), parent.attempt_id());
    assert_eq!(p.trial_id(), parent.trial_id());
    assert_eq!(child.root_run_id(), parent.root_run_id());
    assert_ne!(child.run_id(), parent.run_id());
    assert_ne!(child.attempt_id(), parent.attempt_id());
    assert_ne!(child.trial_id(), parent.trial_id());
}
async fn nested_mode(mode: usize) {
    let seen = Seen::default();
    let calls = Arc::new(AtomicUsize::new(0));
    chain(&seen, mode, &calls)
        .execute("child", AgentState::new())
        .await
        .unwrap();
    let xs = seen.lock().unwrap();
    assert_eq!(xs.len(), 3);
    assert!(matches!(xs[0].lineage(), RunLineage::Root));
    assert_link(&xs[1], &xs[0]);
    assert_link(&xs[2], &xs[1]);
    if mode == 2 {
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
#[tokio::test]
async fn direct_nested_lineage() {
    bounded(nested_mode(0)).await;
}
#[tokio::test]
async fn in_process_nested_lineage() {
    bounded(nested_mode(1)).await;
}
#[tokio::test]
async fn custom_forward_nested_lineage() {
    bounded(nested_mode(2)).await;
}

#[tokio::test]
async fn legacy_executor_preserves_policy_and_unknown_root() {
    bounded(async {
        let seen = Seen::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let grandchild = AgentGraph::builder()
            .add_node("leaf", Box::new(tap(&seen, None)))
            .build()
            .unwrap();
        let child = AgentGraph::builder()
            .add_node("grandchild", Box::new(tap(&seen, Some(grandchild))))
            .build()
            .unwrap();
        let root = builder(3, &calls)
            .add_node("child", Box::new(child))
            .build()
            .unwrap();
        root.execute("child", AgentState::new()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let xs = seen.lock().unwrap();
        assert_eq!(xs.len(), 2);
        assert!(matches!(xs[0].lineage(), RunLineage::UnlinkedSubgraph));
        assert_eq!(xs[0].root_run_id(), None);
        assert_link(&xs[1], &xs[0]);
        assert_eq!(xs[1].root_run_id(), None);
    })
    .await;
}

#[tokio::test]
async fn four_root_constructors_keep_root_origin() {
    bounded(async {
        for mode in 0..4 {
            let seen = Seen::default();
            let g = Arc::new(
                AgentGraph::builder()
                    .add_node("leaf", Box::new(tap(&seen, None)))
                    .build()
                    .unwrap(),
            );
            let s = AgentState::new();
            let c = GraphConfig::default();
            match mode {
                0 => {
                    g.execute_with_summary("leaf", s, c).await.0.unwrap();
                }
                1 => assert!(matches!(
                    g.execute_with_interrupt("leaf", s, c).await,
                    ExecutionResult::Complete(_)
                )),
                2 => {
                    g.execute_cancellable("leaf", s, c)
                        .0
                        .await
                        .unwrap()
                        .unwrap();
                }
                _ => {
                    let (h, mut rx) = g.stream("leaf", s, c);
                    while rx.recv().await.is_some() {}
                    h.await.unwrap().unwrap();
                }
            }
            let xs = seen.lock().unwrap();
            assert_eq!(xs.len(), 1);
            assert!(matches!(xs[0].lineage(), RunLineage::Root));
            assert_eq!(xs[0].root_run_id(), Some(xs[0].run_id()));
        }
    })
    .await;
}

fn retry() -> RetryPolicy {
    RetryPolicy::default()
        .with_max_attempts(2)
        .with_initial_interval(Duration::from_millis(1))
        .with_jitter(false)
}
#[tokio::test]
async fn parent_retry_creates_new_child_run() {
    bounded(async {
        let seen = Seen::default();
        let mut leaf = tap(&seen, None);
        leaf.failures = Some(Arc::new(AtomicUsize::new(0)));
        let child = AgentGraph::builder()
            .add_node("leaf", Box::new(leaf))
            .build()
            .unwrap();
        let root = AgentGraph::builder()
            .add_node_with_retry("child", Box::new(tap(&seen, Some(child))), retry())
            .build()
            .unwrap();
        root.execute("child", AgentState::new()).await.unwrap();
        let xs = seen.lock().unwrap();
        assert_eq!(xs.len(), 4);
        assert_link(&xs[1], &xs[0]);
        assert_link(&xs[3], &xs[2]);
        assert_eq!(xs[0].attempt_id(), xs[2].attempt_id());
        assert_ne!(xs[0].trial_id(), xs[2].trial_id());
        assert_ne!(xs[1].run_id(), xs[3].run_id());
    })
    .await;
}
#[tokio::test]
async fn inner_retry_retains_parent() {
    bounded(async {
        let seen = Seen::default();
        let mut leaf = tap(&seen, None);
        leaf.failures = Some(Arc::new(AtomicUsize::new(0)));
        let child = AgentGraph::builder()
            .add_node_with_retry("leaf", Box::new(leaf), retry())
            .build()
            .unwrap();
        AgentGraph::builder()
            .add_node("child", Box::new(tap(&seen, Some(child))))
            .build()
            .unwrap()
            .execute("child", AgentState::new())
            .await
            .unwrap();
        let xs = seen.lock().unwrap();
        assert_eq!(xs.len(), 3);
        assert_link(&xs[1], &xs[0]);
        assert_link(&xs[2], &xs[0]);
        assert_eq!(xs[1].attempt_id(), xs[2].attempt_id());
        assert_ne!(xs[1].trial_id(), xs[2].trial_id());
    })
    .await;
}

#[tokio::test]
async fn captured_context_replay_is_only_supplied_correlation() {
    bounded(async {
        let seen = Seen::default();
        AgentGraph::builder()
            .add_node("old", Box::new(tap(&seen, None)))
            .build()
            .unwrap()
            .execute("old", AgentState::new())
            .await
            .unwrap();
        let old = seen.lock().unwrap()[0].clone();
        let different = AgentGraph::builder()
            .add_node("different", Box::new(tap(&seen, None)))
            .build()
            .unwrap();
        Node::execute_with_context(
            &different,
            &AgentState::new(),
            &GraphConfig::default(),
            old.clone(),
        )
        .await
        .unwrap();
        let xs = seen.lock().unwrap();
        assert_eq!(xs.len(), 2);
        assert_link(&xs[1], &old);
        assert_eq!(linked(&xs[1]).node_id(), "old"); // No claim that old is still running.
    })
    .await;
}

#[tokio::test]
async fn subgraph_success_merges_but_failure_does_not() {
    bounded(async {
        for fails in [false, true] {
            let seen = Seen::default();
            let mut leaf = tap(&seen, None);
            if fails {
                leaf.failures = Some(Arc::new(AtomicUsize::new(0)));
            }
            let child = AgentGraph::builder()
                .add_node("leaf", Box::new(leaf))
                .build()
                .unwrap();
            let state = AgentState::new();
            let result = Node::execute(&child, &state, &GraphConfig::default()).await;
            assert_eq!(result.is_err(), fails);
            assert_eq!(state.export().await.contains_key("written"), !fails);
        }
    })
    .await;
}

struct DropEvents;
impl EventSink for DropEvents {
    fn emit(&self, _: GraphEvent) {}
}
#[tokio::test]
async fn parallel_and_simultaneous_roots_do_not_cross_links() {
    bounded(async {
        let seen = Seen::default();
        let child = || {
            AgentGraph::builder()
                .with_event_sink(Arc::new(DropEvents))
                .add_node("same", Box::new(tap(&seen, None)))
                .build()
                .unwrap()
        };
        let root = Arc::new(
            AgentGraph::builder()
                .with_event_sink(Arc::new(DropEvents))
                .add_node("start", node!(|_s| async move { Ok(()) }))
                .add_node("left", Box::new(tap(&seen, Some(child()))))
                .add_node("right", Box::new(tap(&seen, Some(child()))))
                .add_edge("start", "left")
                .add_edge("start", "right")
                .build()
                .unwrap(),
        );
        let state = AgentState::new();
        state.set("run_id", "forged").await.unwrap();
        state.set("parent_trial", "forged").await.unwrap();
        let config =
            GraphConfig::default().with_metadata("parent_run", serde_json::json!("forged"));
        let a = root
            .clone()
            .execute_cancellable("start", state.clone(), config.clone())
            .0;
        let b = root.execute_cancellable("start", state, config).0;
        a.await.unwrap().unwrap();
        b.await.unwrap().unwrap();
        let xs = seen.lock().unwrap();
        assert_eq!(xs.len(), 8);
        let parents: Vec<_> = xs
            .iter()
            .filter(|x| matches!(x.lineage(), RunLineage::Root))
            .collect();
        assert_eq!(parents.len(), 4);
        let roots: std::collections::HashSet<_> = parents.iter().map(|x| x.run_id()).collect();
        assert_eq!(roots.len(), 2);
        for run in roots {
            let names: std::collections::HashSet<_> = parents
                .iter()
                .filter(|x| x.run_id() == run)
                .map(|x| x.node_id())
                .collect();
            assert_eq!(names, std::collections::HashSet::from(["left", "right"]));
        }
        for c in xs
            .iter()
            .filter(|x| matches!(x.lineage(), RunLineage::LinkedSubgraph(_)))
        {
            let p = parents
                .iter()
                .find(|p| p.trial_id() == linked(c).trial_id())
                .unwrap();
            assert_link(c, p);
            assert_eq!(
                xs.iter()
                    .filter(|x| matches!(x.lineage(), RunLineage::LinkedSubgraph(_))
                        && linked(x).trial_id() == p.trial_id())
                    .count(),
                1
            );
        }
    })
    .await;
}

fn pending_child(seen: &Seen, started: &Arc<Semaphore>, release: &Arc<Semaphore>) -> AgentGraph {
    let mut leaf = tap(seen, None);
    leaf.started = Some(started.clone());
    leaf.release = Some(release.clone());
    AgentGraph::builder()
        .add_node("leaf", Box::new(leaf))
        .build()
        .unwrap()
}
#[tokio::test]
async fn parent_handle_abort_keeps_only_historical_link() {
    bounded(async {
        let seen = Seen::default();
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let child = pending_child(&seen, &started, &release);
        let root = Arc::new(
            AgentGraph::builder()
                .add_node("child", Box::new(tap(&seen, Some(child))))
                .add_node("never", Box::new(tap(&seen, None)))
                .add_edge("child", "never")
                .build()
                .unwrap(),
        );
        let (h, _) = root.execute_cancellable("child", AgentState::new(), GraphConfig::default());
        started.acquire().await.unwrap().forget();
        h.abort();
        assert!(h.await.unwrap_err().is_cancelled());
        {
            let xs = seen.lock().unwrap();
            assert_eq!(xs.len(), 2);
            assert_link(&xs[1], &xs[0]);
        }
        let fresh = AgentGraph::builder()
            .add_node("fresh", Box::new(tap(&seen, None)))
            .build()
            .unwrap();
        fresh.execute("fresh", AgentState::new()).await.unwrap();
        let xs = seen.lock().unwrap();
        assert_eq!(xs.len(), 3);
        assert!(matches!(xs[2].lineage(), RunLineage::Root));
        assert_ne!(xs[2].run_id(), xs[0].run_id());
    })
    .await;
}
#[tokio::test]
async fn cooperative_parent_cancel_does_not_cancel_awaited_child() {
    bounded(async {
        let seen = Seen::default();
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let child = pending_child(&seen, &started, &release);
        let state = AgentState::new();
        let root = Arc::new(
            AgentGraph::builder()
                .add_node("child", Box::new(tap(&seen, Some(child))))
                .add_node("never", Box::new(tap(&seen, None)))
                .add_edge("child", "never")
                .build()
                .unwrap(),
        );
        let (h, cancel) = root.execute_cancellable("child", state.clone(), GraphConfig::default());
        started.acquire().await.unwrap().forget();
        cancel.store(true, Ordering::SeqCst);
        assert!(!h.is_finished());
        assert!(!state.export().await.contains_key("written"));
        release.add_permits(1);
        assert!(matches!(h.await.unwrap(), Err(AgentGraphError::Cancelled)));
        assert!(state.export().await.contains_key("written")); // Child completed and merged before parent checked flag.
        let xs = seen.lock().unwrap();
        assert_eq!(xs.len(), 2);
        assert_link(&xs[1], &xs[0]);
    })
    .await;
}
#[tokio::test]
async fn queued_subgraph_after_failure_has_no_link() {
    bounded(async {
        let seen = Seen::default();
        let mut failure = tap(&seen, None);
        failure.failures = Some(Arc::new(AtomicUsize::new(0)));
        let child = AgentGraph::builder()
            .add_node("leaf", Box::new(tap(&seen, None)))
            .build()
            .unwrap();
        let root = AgentGraph::builder()
            .add_node("start", node!(|_s| async move { Ok(()) }))
            .add_node("failure", Box::new(failure))
            .add_node("never", Box::new(tap(&seen, Some(child))))
            .add_edge("start", "failure")
            .add_edge("start", "never")
            .build()
            .unwrap();
        assert!(matches!(
            root.execute_with_config(
                "start",
                AgentState::new(),
                GraphConfig::default().with_max_parallelism(1)
            )
            .await,
            Err(AgentGraphError::ExecutionError(_))
        ));
        let xs = seen.lock().unwrap();
        assert_eq!(xs.len(), 1);
        assert_eq!(xs[0].node_id(), "failure");
    })
    .await;
}

struct FailAfter(Arc<Semaphore>);
#[async_trait::async_trait]
impl Node for FailAfter {
    async fn execute(&self, _: &AgentState, _: &GraphConfig) -> Result<NodeOutput> {
        self.0.acquire().await.unwrap().forget();
        Err(AgentGraphError::ExecutionError("sibling fails".into()))
    }
}
#[tokio::test]
async fn started_subgraph_sibling_abort_has_no_merge() {
    bounded(async {
        let seen = Seen::default();
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let child = pending_child(&seen, &started, &release);
        let state = AgentState::new();
        let root = AgentGraph::builder()
            .add_node("start", node!(|_s| async move { Ok(()) }))
            .add_node("child", Box::new(tap(&seen, Some(child))))
            .add_node("failure", Box::new(FailAfter(started)))
            .add_edge("start", "child")
            .add_edge("start", "failure")
            .build()
            .unwrap();
        assert!(matches!(
            root.execute("start", state.clone()).await,
            Err(AgentGraphError::ExecutionError(_))
        ));
        assert!(!state.export().await.contains_key("written"));
        let xs = seen.lock().unwrap();
        assert_eq!(xs.len(), 2);
        assert_link(&xs[1], &xs[0]);
    })
    .await;
}

#[tokio::test]
async fn predispatch_cancel_has_zero_context_calls() {
    bounded(async {
        let seen = Seen::default();
        let root = Arc::new(
            AgentGraph::builder()
                .add_node("child", Box::new(tap(&seen, None)))
                .build()
                .unwrap(),
        );
        let (h, flag) =
            root.execute_cancellable("child", AgentState::new(), GraphConfig::default());
        flag.store(true, Ordering::SeqCst);
        assert!(matches!(h.await.unwrap(), Err(AgentGraphError::Cancelled)));
        assert!(seen.lock().unwrap().is_empty());
    })
    .await;
}
