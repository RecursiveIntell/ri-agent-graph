//! Public graph consumer regression for join admission and terminal projection.
use ri_agent_graph::prelude::*;
use serde_json::{json, Value};

#[tokio::test]
async fn strategy_join_preserves_quarantine_and_valid_branch_output() {
    let graph = AgentGraph::builder()
        .add_node(
            "join",
            Box::new(JoinNode::strategy(
                vec!["good".into(), "bad".into()],
                "merged",
                Box::new(ProofCarryingJoin::default()),
            )),
        )
        .add_edge(START, "join")
        .build()
        .unwrap();
    let good = json!({
        "evidence": [{"witness_id": "w1", "digest": "sha256:abc"}],
        "checks": [{"status": "passed"}],
        "receipt": "receipt:r1",
    });
    let state = AgentState::new();
    state.set("good", good.clone()).await.unwrap();
    state
        .set(
            "bad",
            json!({"evidence": null, "checks": null, "receipt": "receipt:r2"}),
        )
        .await
        .unwrap();
    let result = graph.execute(START, state).await.unwrap();
    let merged: Value = result.get("merged").await.unwrap();
    assert_eq!(merged["join"], "proof_carrying_join");
    assert_eq!(merged["certification"], "quarantine");
    assert_eq!(merged["value"], json!([good]));
    assert_eq!(merged["contradictions"], json!([]));
    assert_eq!(merged["minority_report"], json!([]));
    assert!(merged["notes"].as_array().unwrap().iter().any(|note| note
        .as_str()
        .unwrap()
        .contains("artifact 'bad' quarantined")));
}

#[tokio::test]
async fn strategy_join_rejects_malformed_configured_identity() {
    let graph = AgentGraph::builder()
        .add_node(
            "join",
            Box::new(JoinNode::strategy(
                vec!["branch".into()],
                "merged",
                Box::new(DedupeByIdentity {
                    identity_path: Some("claim.id".into()),
                }),
            )),
        )
        .add_edge(START, "join")
        .build()
        .unwrap();
    let state = AgentState::new();
    state
        .set("branch", json!({"claim": {"id": null}}))
        .await
        .unwrap();
    let error = graph.execute(START, state).await.unwrap_err();
    assert!(error.to_string().contains("must be a non-empty string"));
}
