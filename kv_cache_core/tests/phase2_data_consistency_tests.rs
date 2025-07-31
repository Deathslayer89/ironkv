use kv_cache_core::{
    consensus::{RaftConsensus, LogIndex},
    config::{ClusterConfig, MetricsConfig},
    metrics::MetricsCollector,
    store::Key,
    value::Value,
    ttl::TTLStore,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Test node for data consistency testing
struct ConsistencyTestNode {
    node_id: String,
    store: TTLStore,
    consensus: RaftConsensus,
    is_running: bool,
}

impl ConsistencyTestNode {
    async fn new(node_id: String, config: ClusterConfig) -> Self {
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        let consensus = RaftConsensus::new(node_id.clone(), config, metrics);
        let store = TTLStore::new();
        
        Self {
            node_id,
            store,
            consensus,
            is_running: false,
        }
    }

    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.consensus.start().await?;
        self.is_running = true;
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.consensus.stop().await?;
        self.is_running = false;
        Ok(())
    }

    async fn is_leader(&self) -> bool {
        if !self.is_running {
            return false;
        }
        self.consensus.is_leader().await
    }

    async fn set_with_consensus(&mut self, key: Key, value: Value) -> Result<LogIndex, Box<dyn std::error::Error>> {
        if !self.is_leader().await {
            return Err("Not leader".into());
        }
        
        // Submit command to consensus
        let command = format!("SET {} {}", key, value.to_string());
        let log_index = self.consensus.submit_command(command.as_bytes().to_vec()).await?;
        
        // Apply to local store
        self.store.set(key, value, None).await;
        
        Ok(log_index)
    }

    async fn get(&self, key: &Key) -> Option<Value> {
        self.store.get(key).await
    }

    async fn get_committed_value(&self, key: &Key) -> Option<Value> {
        // Only return value if it's been committed in the log
        let state = self.consensus.get_state().await;
        if state.commit_index > LogIndex(0) {
            self.store.get(key).await
        } else {
            None
        }
    }
}

/// Test cluster for data consistency testing
struct ConsistencyTestCluster {
    nodes: HashMap<String, ConsistencyTestNode>,
    config: ClusterConfig,
}

impl ConsistencyTestCluster {
    fn new() -> Self {
        let mut config = ClusterConfig::default();
        config.members.insert("node-0".to_string(), "127.0.0.1:6380".to_string());
        config.members.insert("node-1".to_string(), "127.0.0.1:6381".to_string());
        config.members.insert("node-2".to_string(), "127.0.0.1:6382".to_string());
        
        Self {
            nodes: HashMap::new(),
            config,
        }
    }

    async fn add_node(&mut self, node_id: String) -> Result<(), Box<dyn std::error::Error>> {
        let node = ConsistencyTestNode::new(node_id.clone(), self.config.clone()).await;
        self.nodes.insert(node_id, node);
        Ok(())
    }

    async fn start_node(&mut self, node_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.start().await?;
        }
        Ok(())
    }

    async fn stop_node(&mut self, node_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.stop().await?;
        }
        Ok(())
    }

    async fn get_leader(&self) -> Option<String> {
        for (node_id, node) in &self.nodes {
            if node.is_leader().await {
                return Some(node_id.clone());
            }
        }
        None
    }

    async fn wait_for_leader(&self, timeout_secs: u64) -> Option<String> {
        let start = std::time::Instant::now();
        while start.elapsed().as_secs() < timeout_secs {
            if let Some(leader) = self.get_leader().await {
                return Some(leader);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        None
    }
}

#[tokio::test]
async fn test_strong_consistency_for_writes() {
    println!("=== Starting test_strong_consistency_for_writes ===");
    
    let mut cluster = ConsistencyTestCluster::new();
    
    // Create 3-node cluster
    cluster.add_node("node-0".to_string()).await.unwrap();
    cluster.add_node("node-1".to_string()).await.unwrap();
    cluster.add_node("node-2".to_string()).await.unwrap();
    
    // Start all nodes
    cluster.start_node("node-0").await.unwrap();
    cluster.start_node("node-1").await.unwrap();
    cluster.start_node("node-2").await.unwrap();
    
    // Wait for leader election
    let leader_id = cluster.wait_for_leader(5).await.expect("No leader elected");
    println!("✅ Leader elected: {}", leader_id);
    
    // Test strong consistency: write should be immediately visible to all nodes
    let key = "consistency_test_key".to_string();
    let value = Value::String("strong_consistency_value".to_string());
    
    // Write through consensus
    {
        let leader = cluster.nodes.get_mut(&leader_id).unwrap();
        let log_index = leader.set_with_consensus(key.clone(), value.clone()).await.unwrap();
        println!("✅ Write submitted to consensus with log index: {}", log_index.0);
    }
    
    // Wait a bit for replication
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // Verify all nodes see the same value
    for (node_id, node) in &cluster.nodes {
        let retrieved_value = node.get(&key).await;
        assert_eq!(retrieved_value, Some(value.clone()), 
                   "Node {} should see the same value", node_id);
        println!("✅ Node {} has consistent value", node_id);
    }
    
    println!("=== test_strong_consistency_for_writes completed successfully ===");
}

#[tokio::test]
async fn test_read_after_write_consistency() {
    println!("=== Starting test_read_after_write_consistency ===");
    
    let mut cluster = ConsistencyTestCluster::new();
    
    // Create 3-node cluster
    cluster.add_node("node-0".to_string()).await.unwrap();
    cluster.add_node("node-1".to_string()).await.unwrap();
    cluster.add_node("node-2".to_string()).await.unwrap();
    
    // Start all nodes
    cluster.start_node("node-0").await.unwrap();
    cluster.start_node("node-1").await.unwrap();
    cluster.start_node("node-2").await.unwrap();
    
    // Wait for leader election
    let leader_id = cluster.wait_for_leader(5).await.expect("No leader elected");
    println!("✅ Leader elected: {}", leader_id);
    
    // Test read-after-write consistency
    let key = "read_after_write_key".to_string();
    let value = Value::String("read_after_write_value".to_string());
    
    // Write value
    {
        let leader = cluster.nodes.get_mut(&leader_id).unwrap();
        let log_index = leader.set_with_consensus(key.clone(), value.clone()).await.unwrap();
        println!("✅ Write submitted with log index: {}", log_index.0);
        
        // Immediately read from leader - should see the written value
        let immediate_read = leader.get(&key).await;
        assert_eq!(immediate_read, Some(value.clone()), 
                   "Leader should immediately see written value");
        println!("✅ Leader immediately sees written value");
    }
    
    // Read from all nodes - should all see the same value
    for (node_id, node) in &cluster.nodes {
        let retrieved_value = node.get(&key).await;
        assert_eq!(retrieved_value, Some(value.clone()), 
                   "Node {} should see written value", node_id);
        println!("✅ Node {} has read-after-write consistency", node_id);
    }
    
    println!("=== test_read_after_write_consistency completed successfully ===");
}

#[tokio::test]
async fn test_handle_stale_reads() {
    println!("=== Starting test_handle_stale_reads ===");
    
    let mut cluster = ConsistencyTestCluster::new();
    
    // Create 3-node cluster
    cluster.add_node("node-0".to_string()).await.unwrap();
    cluster.add_node("node-1".to_string()).await.unwrap();
    cluster.add_node("node-2".to_string()).await.unwrap();
    
    // Start all nodes
    cluster.start_node("node-0").await.unwrap();
    cluster.start_node("node-1").await.unwrap();
    cluster.start_node("node-2").await.unwrap();
    
    // Wait for leader election
    let leader_id = cluster.wait_for_leader(5).await.expect("No leader elected");
    println!("✅ Leader elected: {}", leader_id);
    
    // Test handling of stale reads
    let key = "stale_read_key".to_string();
    let initial_value = Value::String("initial_value".to_string());
    let updated_value = Value::String("updated_value".to_string());
    
    // Write initial value
    {
        let leader = cluster.nodes.get_mut(&leader_id).unwrap();
        leader.set_with_consensus(key.clone(), initial_value.clone()).await.unwrap();
        println!("✅ Initial value written");
    }
    
    // Wait for replication
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // Verify all nodes see initial value
    for (node_id, node) in &cluster.nodes {
        let value = node.get(&key).await;
        assert_eq!(value, Some(initial_value.clone()), 
                   "Node {} should see initial value", node_id);
    }
    println!("✅ All nodes see initial value");
    
    // Update value
    {
        let leader = cluster.nodes.get_mut(&leader_id).unwrap();
        leader.set_with_consensus(key.clone(), updated_value.clone()).await.unwrap();
        println!("✅ Value updated");
        
        // Check that leader immediately sees updated value
        let leader_read = leader.get(&key).await;
        assert_eq!(leader_read, Some(updated_value.clone()), 
                   "Leader should see updated value");
        println!("✅ Leader sees updated value");
    }
    
    // Check that followers eventually see updated value (consistency)
    let mut all_consistent = false;
    let start = std::time::Instant::now();
    while start.elapsed().as_secs() < 5 && !all_consistent {
        all_consistent = true;
        for (node_id, node) in &cluster.nodes {
            let value = node.get(&key).await;
            if value != Some(updated_value.clone()) {
                all_consistent = false;
                println!("Node {} still has stale value: {:?}", node_id, value);
                break;
            }
        }
        if !all_consistent {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
    
    assert!(all_consistent, "All nodes should eventually see updated value");
    println!("✅ All nodes eventually see updated value");
    
    println!("=== test_handle_stale_reads completed successfully ===");
}

#[tokio::test]
async fn test_consistency_level_configuration() {
    println!("=== Starting test_consistency_level_configuration ===");
    
    let mut cluster = ConsistencyTestCluster::new();
    
    // Create 3-node cluster
    cluster.add_node("node-0".to_string()).await.unwrap();
    cluster.add_node("node-1".to_string()).await.unwrap();
    cluster.add_node("node-2".to_string()).await.unwrap();
    
    // Start all nodes
    cluster.start_node("node-0").await.unwrap();
    cluster.start_node("node-1").await.unwrap();
    cluster.start_node("node-2").await.unwrap();
    
    // Wait for leader election
    let leader_id = cluster.wait_for_leader(5).await.expect("No leader elected");
    println!("✅ Leader elected: {}", leader_id);
    
    // Test different consistency levels
    let key = "consistency_level_key".to_string();
    let value = Value::String("consistency_level_value".to_string());
    
    // Strong consistency (default): wait for majority
    {
        let leader = cluster.nodes.get_mut(&leader_id).unwrap();
        let log_index = leader.set_with_consensus(key.clone(), value.clone()).await.unwrap();
        println!("✅ Strong consistency write with log index: {}", log_index.0);
    }
    
    // Verify majority consistency (at least 2 out of 3 nodes)
    let mut consistent_nodes = 0;
    for (node_id, node) in &cluster.nodes {
        let retrieved_value = node.get(&key).await;
        if retrieved_value == Some(value.clone()) {
            consistent_nodes += 1;
            println!("✅ Node {} has consistent value", node_id);
        } else {
            println!("⚠️  Node {} has inconsistent value: {:?}", node_id, retrieved_value);
        }
    }
    
    // Should have at least majority (2 out of 3)
    assert!(consistent_nodes >= 2, 
            "Should have majority consistency, got {} consistent nodes", consistent_nodes);
    println!("✅ Majority consistency achieved: {}/3 nodes", consistent_nodes);
    
    println!("=== test_consistency_level_configuration completed successfully ===");
}

#[tokio::test]
async fn test_phase2_3_complete() {
    println!("=== Starting Phase 2.3 Complete Test ===");
    
    let mut cluster = ConsistencyTestCluster::new();
    
    // Create 3-node cluster
    cluster.add_node("node-0".to_string()).await.unwrap();
    cluster.add_node("node-1".to_string()).await.unwrap();
    cluster.add_node("node-2".to_string()).await.unwrap();
    
    // Start all nodes
    cluster.start_node("node-0").await.unwrap();
    cluster.start_node("node-1").await.unwrap();
    cluster.start_node("node-2").await.unwrap();
    
    // Wait for leader election
    let leader_id = cluster.wait_for_leader(5).await.expect("No leader elected");
    println!("✅ Leader elected: {}", leader_id);
    
    // Test comprehensive data consistency scenarios
    let test_cases = vec![
        ("key1", Value::String("value1".to_string())),
        ("key2", Value::String("value2".to_string())),
        ("key3", Value::String("value3".to_string())),
    ];
    
    for (key, value) in test_cases {
        // Write with strong consistency
        {
            let leader = cluster.nodes.get_mut(&leader_id).unwrap();
            let log_index = leader.set_with_consensus(key.to_string(), value.clone()).await.unwrap();
            println!("✅ Wrote {} with log index {}", key, log_index.0);
            
            // Verify read-after-write consistency
            let immediate_read = leader.get(&key.to_string()).await;
            assert_eq!(immediate_read, Some(value.clone()), 
                       "Leader should immediately see written value for {}", key);
        }
        
        // Wait for replication
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
        // Verify all nodes have consistent values
        for (node_id, node) in &cluster.nodes {
            let retrieved_value = node.get(&key.to_string()).await;
            assert_eq!(retrieved_value, Some(value.clone()), 
                       "Node {} should have consistent value for {}", node_id, key);
        }
        println!("✅ All nodes consistent for {}", key);
    }
    
    // Test consistency under node failure
    println!("Testing consistency under node failure...");
    let follower_id = if leader_id == "node-0" { "node-1" } else { "node-0" };
    cluster.stop_node(follower_id).await.unwrap();
    println!("✅ Stopped follower: {}", follower_id);
    
    // Write to remaining nodes
    let key = "failure_test_key".to_string();
    let value = Value::String("failure_test_value".to_string());
    {
        let leader = cluster.nodes.get_mut(&leader_id).unwrap();
        let log_index = leader.set_with_consensus(key.clone(), value.clone()).await.unwrap();
        println!("✅ Wrote during failure with log index: {}", log_index.0);
    }
    
    // Verify remaining nodes are consistent
    for (node_id, node) in &cluster.nodes {
        if node_id != follower_id {
            let retrieved_value = node.get(&key).await;
            assert_eq!(retrieved_value, Some(value.clone()), 
                       "Node {} should have consistent value during failure", node_id);
        }
    }
    println!("✅ Remaining nodes consistent during failure");
    
    // Restart the stopped node
    cluster.start_node(follower_id).await.unwrap();
    println!("✅ Restarted follower: {}", follower_id);
    
    // Wait for recovery
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    
    // Verify all nodes are consistent after recovery
    for (node_id, node) in &cluster.nodes {
        let retrieved_value = node.get(&key).await;
        assert_eq!(retrieved_value, Some(value.clone()), 
                   "Node {} should have consistent value after recovery", node_id);
    }
    println!("✅ All nodes consistent after recovery");
    
    println!("=== Phase 2.3 Complete Test finished successfully ===");
} 