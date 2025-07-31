//! Simple tests for Phase 2.2: Log Replication
//! 
//! This module tests the log replication functionality using the existing consensus system.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use kv_cache_core::{
    consensus::{
        log::{LogEntry, LogIndex, RaftLog},
        state::{RaftState, RaftRole, RaftTerm},
    },
    config::ClusterConfig,
};

// Simple test structures for log replication
#[derive(Debug, Clone)]
pub struct SimpleLogTestNode {
    pub node_id: String,
    pub log: Arc<RwLock<RaftLog>>,
    pub state: Arc<RwLock<RaftState>>,
}

impl SimpleLogTestNode {
    pub async fn new(node_id: String) -> Self {
        let log = Arc::new(RwLock::new(RaftLog::new()));
        let state = Arc::new(RwLock::new(RaftState::new(node_id.clone())));
        
        Self {
            node_id,
            log,
            state,
        }
    }

    pub async fn append_log_entry(&mut self, term: RaftTerm, command: Vec<u8>) -> Result<LogIndex, Box<dyn std::error::Error>> {
        let mut log = self.log.write().await;
        let entry = LogEntry::new(term, log.get_next_index(), command);
        log.append(entry).await
    }

    pub async fn get_log_entry(&self, index: LogIndex) -> Result<Option<LogEntry>, Box<dyn std::error::Error>> {
        let log = self.log.read().await;
        log.get_entry(index).await
    }

    pub async fn get_log_stats(&self) -> (LogIndex, LogIndex, usize) {
        let log = self.log.read().await;
        (log.get_last_index(), log.get_next_index(), log.get_total_size())
    }

    pub async fn is_leader(&self) -> bool {
        let state = self.state.read().await;
        state.role == RaftRole::Leader
    }

    pub async fn set_as_leader(&mut self) {
        let mut state = self.state.write().await;
        state.role = RaftRole::Leader;
    }

    pub async fn set_as_follower(&mut self) {
        let mut state = self.state.write().await;
        state.role = RaftRole::Follower;
    }
}

#[derive(Debug, Clone)]
pub struct SimpleLogTestCluster {
    pub nodes: HashMap<String, SimpleLogTestNode>,
}

impl SimpleLogTestCluster {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    pub async fn add_node(&mut self, node_id: String) -> Result<(), Box<dyn std::error::Error>> {
        let node = SimpleLogTestNode::new(node_id.clone()).await;
        self.nodes.insert(node_id, node);
        Ok(())
    }

    pub async fn get_leader(&self) -> Option<String> {
        for (node_id, node) in &self.nodes {
            if node.is_leader().await {
                return Some(node_id.clone());
            }
        }
        None
    }

    pub async fn set_leader(&mut self, node_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.set_as_leader().await;
        }
        Ok(())
    }

    pub async fn get_cluster_log_stats(&self) -> HashMap<String, (LogIndex, LogIndex, usize)> {
        let mut stats = HashMap::new();
        for (node_id, node) in &self.nodes {
            stats.insert(node_id.clone(), node.get_log_stats().await);
        }
        stats
    }

    pub async fn verify_log_consistency(&self) -> bool {
        let stats = self.get_cluster_log_stats().await;
        if stats.is_empty() {
            return true;
        }

        // All nodes should have the same last index
        let first_node_stats = stats.values().next().unwrap();
        let expected_last_index = first_node_stats.0;

        for (node_id, (last_index, _, _)) in &stats {
            if *last_index != expected_last_index {
                println!("Log inconsistency detected: node {} has last_index {}, expected {}", 
                    node_id, last_index.value(), expected_last_index.value());
                return false;
            }
        }
        true
    }
}

async fn create_simple_log_test_cluster(node_count: usize) -> SimpleLogTestCluster {
    let mut cluster = SimpleLogTestCluster::new();
    
    for i in 0..node_count {
        let node_id = format!("node-{}", i);
        cluster.add_node(node_id).await.unwrap();
    }
    
    cluster
}

#[tokio::test]
async fn test_basic_log_operations() {
    println!("=== Starting test_basic_log_operations ===");
    
    // Create a single node for basic log operations
    let mut cluster = create_simple_log_test_cluster(1).await;
    
    // Set the node as leader
    cluster.set_leader("node-0").await.unwrap();
    println!("✅ Set node-0 as leader");
    
    // Add some log entries
    if let Some(node) = cluster.nodes.get_mut("node-0") {
        node.append_log_entry(RaftTerm(1), b"SET key1 value1".to_vec()).await.unwrap();
        node.append_log_entry(RaftTerm(1), b"SET key2 value2".to_vec()).await.unwrap();
        node.append_log_entry(RaftTerm(1), b"SET key3 value3".to_vec()).await.unwrap();
        
        println!("✅ Added 3 log entries to leader");
    }
    
    // Verify log entries
    if let Some(node) = cluster.nodes.get("node-0") {
        let (last_index, next_index, total_size) = node.get_log_stats().await;
        assert_eq!(last_index, LogIndex(3), "Should have 3 log entries");
        assert_eq!(next_index, LogIndex(4), "Next index should be 4");
        assert!(total_size > 0, "Log should have some size");
        
        println!("✅ Log stats verified: last_index={}, next_index={}, size={}", 
                last_index.value(), next_index.value(), total_size);
        
        // Verify individual entries
        for i in 1..=3 {
            let entry = node.get_log_entry(LogIndex(i)).await.unwrap();
            assert!(entry.is_some(), "Entry {} should exist", i);
            let entry = entry.unwrap();
            assert_eq!(entry.index, LogIndex(i), "Entry {} should have correct index", i);
            assert_eq!(entry.term, RaftTerm(1), "Entry {} should have correct term", i);
        }
        
        println!("✅ All log entries verified");
    }
    
    println!("=== test_basic_log_operations completed successfully ===");
}

#[tokio::test]
async fn test_log_consistency_across_nodes() {
    println!("=== Starting test_log_consistency_across_nodes ===");
    
    // Create a 3-node cluster
    let mut cluster = create_simple_log_test_cluster(3).await;
    
    // Set one node as leader
    cluster.set_leader("node-0").await.unwrap();
    println!("✅ Set node-0 as leader");
    
    // Add log entries to the leader
    if let Some(node) = cluster.nodes.get_mut("node-0") {
        for i in 1..=5 {
            let command = format!("SET key{} value{}", i, i);
            node.append_log_entry(RaftTerm(1), command.as_bytes().to_vec()).await.unwrap();
        }
        println!("✅ Added 5 log entries to leader");
    }
    
    // Manually replicate entries to followers (simulating AppendEntries RPC)
    for i in 1..=5 {
        let command = format!("SET key{} value{}", i, i);
        let entry = LogEntry::new(RaftTerm(1), LogIndex(i), command.as_bytes().to_vec());
        
        // Replicate to node-1
        if let Some(node) = cluster.nodes.get_mut("node-1") {
            let mut log = node.log.write().await;
            log.append(entry.clone()).await.unwrap();
        }
        
        // Replicate to node-2
        if let Some(node) = cluster.nodes.get_mut("node-2") {
            let mut log = node.log.write().await;
            log.append(entry).await.unwrap();
        }
    }
    
    println!("✅ Replicated entries to followers");
    
    // Verify log consistency across all nodes
    let is_consistent = cluster.verify_log_consistency().await;
    assert!(is_consistent, "Log should be consistent across all nodes");
    
    // Verify all nodes have the same log entries
    let stats = cluster.get_cluster_log_stats().await;
    for (node_id, (last_index, _, _)) in &stats {
        assert_eq!(*last_index, LogIndex(5), "Node {} should have 5 log entries", node_id);
        println!("✅ Node {}: last_index = {}", node_id, last_index.value());
    }
    
    println!("=== test_log_consistency_across_nodes completed successfully ===");
}

#[tokio::test]
async fn test_log_compaction() {
    println!("=== Starting test_log_compaction ===");
    
    // Create a single node
    let mut cluster = create_simple_log_test_cluster(1).await;
    cluster.set_leader("node-0").await.unwrap();
    
    // Add log entries (not enough to trigger compaction, but enough to test the functionality)
    if let Some(node) = cluster.nodes.get_mut("node-0") {
        for i in 1..=10 {
            let command = format!("SET key{} value{}", i, i);
            node.append_log_entry(RaftTerm(1), command.as_bytes().to_vec()).await.unwrap();
        }
        println!("✅ Added 10 log entries");
        
        // Verify log stats
        let (last_index, next_index, total_size) = node.get_log_stats().await;
        assert_eq!(last_index, LogIndex(10), "Should have 10 log entries");
        assert_eq!(next_index, LogIndex(11), "Next index should be 11");
        assert!(total_size > 0, "Log should have some size");
        
        println!("✅ Log stats: last_index={}, next_index={}, size={}", 
                last_index.value(), next_index.value(), total_size);
        
        // Test log compaction (should not trigger since we have < 1000 entries)
        let mut log = node.log.write().await;
        let compaction_result = log.compact().await;
        assert!(compaction_result.is_ok(), "Compaction should succeed even with few entries");
        println!("✅ Log compaction test completed (no compaction needed)");
        
        // Verify log is still consistent after compaction attempt
        let (last_index_after, next_index_after, _) = node.get_log_stats().await;
        assert_eq!(last_index_after, LogIndex(10), "Last index should remain 10 after compaction");
        assert_eq!(next_index_after, LogIndex(11), "Next index should remain 11 after compaction");
        
        // Test that all entries are still accessible
        for i in 1..=10 {
            let entry = node.get_log_entry(LogIndex(i)).await.unwrap();
            assert!(entry.is_some(), "Entry {} should still exist after compaction", i);
        }
        println!("✅ All entries still accessible after compaction");
    }
    
    println!("=== test_log_compaction completed successfully ===");
}

#[tokio::test]
async fn test_log_entry_retrieval() {
    println!("=== Starting test_log_entry_retrieval ===");
    
    // Create a single node
    let mut cluster = create_simple_log_test_cluster(1).await;
    cluster.set_leader("node-0").await.unwrap();
    
    // Add log entries
    if let Some(node) = cluster.nodes.get_mut("node-0") {
        node.append_log_entry(RaftTerm(1), b"SET key1 value1".to_vec()).await.unwrap();
        node.append_log_entry(RaftTerm(1), b"SET key2 value2".to_vec()).await.unwrap();
        node.append_log_entry(RaftTerm(1), b"SET key3 value3".to_vec()).await.unwrap();
        
        println!("✅ Added 3 log entries");
        
        // Test retrieving existing entries
        for i in 1..=3 {
            let entry = node.get_log_entry(LogIndex(i)).await.unwrap();
            assert!(entry.is_some(), "Entry {} should exist", i);
            let entry = entry.unwrap();
            assert_eq!(entry.index, LogIndex(i), "Entry {} should have correct index", i);
            assert_eq!(entry.term, RaftTerm(1), "Entry {} should have correct term", i);
            println!("✅ Retrieved entry {}: {:?}", i, entry);
        }
        
        // Test retrieving non-existent entry
        let entry = node.get_log_entry(LogIndex(999)).await.unwrap();
        assert!(entry.is_none(), "Non-existent entry should return None");
        println!("✅ Non-existent entry correctly returns None");
    }
    
    println!("=== test_log_entry_retrieval completed successfully ===");
}

#[tokio::test]
async fn test_phase2_2_simple_complete() {
    println!("=== Starting Phase 2.2 Simple Complete Test ===");
    
    // Create a 3-node cluster
    let mut cluster = create_simple_log_test_cluster(3).await;
    cluster.set_leader("node-0").await.unwrap();
    println!("✅ Created 3-node cluster with leader");
    
    // Test basic log operations
    if let Some(node) = cluster.nodes.get_mut("node-0") {
        node.append_log_entry(RaftTerm(1), b"SET key1 value1".to_vec()).await.unwrap();
        node.append_log_entry(RaftTerm(1), b"SET key2 value2".to_vec()).await.unwrap();
        node.append_log_entry(RaftTerm(1), b"SET key3 value3".to_vec()).await.unwrap();
        println!("✅ Added log entries to leader");
    }
    
    // Simulate replication to followers
    for i in 1..=3 {
        let command = format!("SET key{} value{}", i, i);
        let entry = LogEntry::new(RaftTerm(1), LogIndex(i), command.as_bytes().to_vec());
        
        for node_id in &["node-1", "node-2"] {
            if let Some(node) = cluster.nodes.get_mut(*node_id) {
                let mut log = node.log.write().await;
                log.append(entry.clone()).await.unwrap();
            }
        }
    }
    println!("✅ Replicated entries to followers");
    
    // Verify log consistency
    let is_consistent = cluster.verify_log_consistency().await;
    assert!(is_consistent, "Log should be consistent across all nodes");
    println!("✅ Log consistency verified");
    
    // Verify all nodes have the same log entries
    let stats = cluster.get_cluster_log_stats().await;
    for (node_id, (last_index, _, _)) in &stats {
        assert_eq!(*last_index, LogIndex(3), "Node {} should have 3 log entries", node_id);
        println!("✅ Node {}: last_index = {}", node_id, last_index.value());
    }
    
    println!("✅ All nodes synchronized");
    println!("=== Phase 2.2 Simple Complete Test finished successfully ===");
} 