//! Common checkpoint data/memory support must not depend on SQLite.
use ri_agent_graph::prelude::*;

async fn sample_checkpoint() -> Checkpoint {
    Checkpoint {
        execution_id: "feature-contract".into(),
        timestamp: chrono::Utc::now(),
        current_node: "node".into(),
        iteration: 2,
        state: AgentState::new().snapshot().await,
        step_number: 3,
        active_nodes: vec!["node".into()],
    }
}

#[tokio::test]
async fn checkpoint_data_roundtrips_without_storage_backend() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let checkpoint: ri_agent_graph::checkpoint::Checkpoint = sample_checkpoint().await;
        let encoded = serde_json::to_value(&checkpoint).unwrap();
        let decoded: Checkpoint = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
        assert_eq!(encoded["step_number"], 3);
        assert_eq!(encoded["active_nodes"], serde_json::json!(["node"]));
    })
    .await
    .expect("30-second case limit");
}

#[tokio::test]
async fn memory_checkpoint_saver_retains_data_in_every_profile() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let saver = MemorySaver::new();
        let checkpoint = sample_checkpoint().await;
        saver.save(&checkpoint).await.unwrap();
        let loaded = saver.load("feature-contract").await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(loaded).unwrap(),
            serde_json::to_value(checkpoint).unwrap()
        );
    })
    .await
    .expect("30-second case limit");
}

#[cfg(feature = "checkpointing")]
#[tokio::test]
async fn sqlite_symbols_and_disposable_backend_remain_available() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let manager = CheckpointManager::new(":memory:").unwrap();
        let checkpoint = sample_checkpoint().await;
        manager.save(&checkpoint).unwrap();
        assert_eq!(
            manager
                .load("feature-contract")
                .unwrap()
                .unwrap()
                .current_node,
            "node"
        );
        let saver = SqliteSaver::new(":memory:").unwrap();
        saver.save(&checkpoint).await.unwrap();
        assert_eq!(
            saver
                .load("feature-contract")
                .await
                .unwrap()
                .unwrap()
                .current_node,
            "node"
        );
        // Preserve existing SQLite behavior; this test does not change its schema.
    })
    .await
    .expect("30-second case limit");
}
