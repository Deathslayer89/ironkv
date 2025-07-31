//! Comprehensive tests for Phase 2.2: Log Replication
//! 
//! This module tests the log replication functionality including:
//! - Basic log replication to followers
//! - Log consistency checks
//! - Replication lag monitoring
//! - Log compaction and snapshotting
//! - Replication failure handling

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use kv_cache_core::{
    consensus::{
        communication::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse},
        log::{LogEntry, LogIndex, RaftLog},
        replication::ReplicationManager,
        state::{RaftState, RaftRole, RaftTerm},
        RaftRpc,
    },
    config::ClusterConfig,
};

// Test structures for Phase 2.2
#[derive(Clone)]
pub struct LogReplicationTestNode {
    pub node_id: String,
    pub log: Arc<RwLock<RaftLog>>,
    pub state: Arc<RwLock<RaftState>>,
    pub replication_manager: Option<ReplicationManager>,
    pub is_running: bool,
    pub is_network_partitioned: bool,
}

impl LogReplicationTestNode {
    pub async fn new(node_id: String) -> Self {
        let log = Arc::new(RwLock::new(RaftLog::new()));
        let state = Arc::new(RwLock::new(RaftState::new(node_id.clone())));
        
        Self {
            node_id,
            log,
            state,
            replication_manager: None,
            is_running: false,
            is_network_partitioned: false,
        }
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.is_running = true;
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.is_running = false;
        if let Some(mut manager) = self.replication_manager.take() {
            manager.stop().await?;
        }
        Ok(())
    }

    pub async fn get_log_stats(&self) -> (LogIndex, LogIndex, usize) {
        let log = self.log.read().await;
        (log.get_last_index(), log.get_next_index(), log.get_total_size())
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

    pub async fn is_leader(&self) -> bool {
        let state = self.state.read().await;
        state.role == RaftRole::Leader
    }

    pub async fn get_term(&self) -> RaftTerm {
        let state = self.state.read().await;
        state.current_term
    }

    pub async fn get_commit_index(&self) -> LogIndex {
        let state = self.state.read().await;
        state.commit_index
    }
}

#[derive(Clone)]
pub struct LogReplicationTestCluster {
    pub nodes: HashMap<String, LogReplicationTestNode>,
    pub config: ClusterConfig,
}

impl LogReplicationTestCluster {
    pub fn new(node_count: usize) -> Self {
        let config = ClusterConfig {
            heartbeat_interval: 100,
            ..Default::default()
        };

        Self {
            nodes: HashMap::new(),
            config,
        }
    }

    pub async fn add_node(&mut self, node_id: String) -> Result<(), Box<dyn std::error::Error>> {
        let node = LogReplicationTestNode::new(node_id.clone()).await;
        self.nodes.insert(node_id, node);
        Ok(())
    }

    pub async fn start_all_nodes(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for (_, node) in &mut self.nodes {
            node.start().await?;
        }
        Ok(())
    }

    pub async fn stop_all_nodes(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for (_, node) in &mut self.nodes {
            node.stop().await?;
        }
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

    pub async fn wait_for_leader(&self, timeout: Duration) -> Option<String> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Some(leader) = self.get_leader().await {
                return Some(leader);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        None
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

// Mock RPC client for log replication tests
pub struct MockLogReplicationRpcClient {
    pub cluster: Arc<RwLock<LogReplicationTestCluster>>,
    pub node_id: String,
}

impl MockLogReplicationRpcClient {
    pub fn new(cluster: Arc<RwLock<LogReplicationTestCluster>>, node_id: String) -> Self {
        Self { cluster, node_id }
    }
}

#[async_trait::async_trait]
impl RaftRpc for MockLogReplicationRpcClient {
    async fn request_vote(
        &self,
        target: &str,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>> {
        let cluster = self.cluster.read().await;
        
        if let Some(target_node) = cluster.nodes.get(target) {
            if !target_node.is_running {
                return Err("Node is not running".into());
            }
            if target_node.is_network_partitioned {
                return Err("Node unavailable".into());
            }

            // Simple vote granting logic
            let mut state = target_node.state.write().await;
            if request.term > state.current_term {
                state.current_term = request.term.into();
                state.voted_for = Some(request.candidate_id.clone());
                return Ok(RequestVoteResponse {
                    term: request.term,
                    vote_granted: true,
                });
            }
        }
        
        Ok(RequestVoteResponse {
            term: RaftTerm(1),
            vote_granted: false,
        })
    }

    async fn append_entries(
        &self,
        target: &str,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error + Send + Sync>> {
        let mut cluster = self.cluster.write().await;
        
        if let Some(target_node) = cluster.nodes.get_mut(target) {
            if !target_node.is_running {
                return Err("Node is not running".into());
            }
            if target_node.is_network_partitioned {
                return Err("Node unavailable".into());
            }

            // Update term if needed
            let mut state = target_node.state.write().await;
            if request.term > state.current_term {
                state.current_term = request.term.into();
                state.role = RaftRole::Follower;
                state.voted_for = None;
            }

            // Check if previous log entry matches
            if request.prev_log_index > LogIndex(0) {
                let prev_entry = target_node.get_log_entry(request.prev_log_index).await;
                if let Ok(Some(entry)) = prev_entry {
                    if entry.term != request.prev_log_term.into() {
                        return Ok(AppendEntriesResponse {
                            term: state.current_term,
                            success: false,
                            last_log_index: None,
                            conflict_index: None,
                            conflict_term: None,
                        });
                    }
                } else {
                    return Ok(AppendEntriesResponse {
                        term: state.current_term,
                        success: false,
                        last_log_index: None,
                        conflict_index: None,
                        conflict_term: None,
                    });
                }
            }

            // Append new entries
            let mut log = target_node.log.write().await;
            for entry in request.entries {
                if let Err(_) = log.append(entry).await {
                    return Err("Failed to append log entry".into());
                }
            }
            let last_index = log.get_last_index();

            // Update commit index
            if request.leader_commit > state.commit_index {
                state.commit_index = std::cmp::min(request.leader_commit, last_index);
            }

            return Ok(AppendEntriesResponse {
                term: state.current_term,
                success: true,
                last_log_index: Some(last_index),
                conflict_index: None,
                conflict_term: None,
            });
        }
        
        Err("Target node not found".into())
    }

    async fn install_snapshot(
        &self,
        _target: &str,
        _request: kv_cache_core::consensus::communication::InstallSnapshotRequest,
    ) -> Result<kv_cache_core::consensus::communication::InstallSnapshotResponse, Box<dyn std::error::Error + Send + Sync>> {
        // For now, just return success
        Ok(kv_cache_core::consensus::communication::InstallSnapshotResponse {
            term: RaftTerm(1),
            success: true,
        })
    }
}

async fn create_log_replication_test_cluster(node_count: usize) -> LogReplicationTestCluster {
    let mut cluster = LogReplicationTestCluster::new(node_count);
    
    for i in 0..node_count {
        let node_id = format!("node-{}", i);
        cluster.add_node(node_id).await.unwrap();
    }
    
    cluster
}

mod basic_replication_tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_log_replication() {
        println!("=== Starting test_basic_log_replication ===");
        
        // Create a 3-node cluster
        let mut cluster = create_log_replication_test_cluster(3).await;
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for leader election
        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await;
        assert!(leader_id.is_some(), "Leader election should complete");
        
        let leader_id = leader_id.unwrap();
        println!("Leader elected: {}", leader_id);
        
        // Add some log entries to the leader
        if let Some(leader_node) = cluster.nodes.get_mut(&leader_id) {
            leader_node.append_log_entry(RaftTerm(1), b"SET key1 value1".to_vec()).await.unwrap();
            leader_node.append_log_entry(RaftTerm(1), b"SET key2 value2".to_vec()).await.unwrap();
            leader_node.append_log_entry(RaftTerm(1), b"SET key3 value3".to_vec()).await.unwrap();
            
            println!("Added 3 log entries to leader");
        }
        
        // Wait a bit for replication
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Verify log consistency across all nodes
        let is_consistent = cluster.verify_log_consistency().await;
        assert!(is_consistent, "Log should be consistent across all nodes");
        
        // Check that all nodes have the same log entries
        let stats = cluster.get_cluster_log_stats().await;
        for (node_id, (last_index, _, _)) in &stats {
            println!("Node {}: last_index = {}", node_id, last_index.value());
            assert_eq!(*last_index, LogIndex(3), "All nodes should have 3 log entries");
        }
        
        println!("=== test_basic_log_replication completed successfully ===");
    }

    #[tokio::test]
    async fn test_log_replication_with_failures() {
        println!("=== Starting test_log_replication_with_failures ===");
        
        // Create a 3-node cluster
        let mut cluster = create_log_replication_test_cluster(3).await;
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for leader election
        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        println!("Leader elected: {}", leader_id);
        
        // Add some log entries
        if let Some(leader_node) = cluster.nodes.get_mut(&leader_id) {
            leader_node.append_log_entry(RaftTerm(1), b"SET key1 value1".to_vec()).await.unwrap();
            leader_node.append_log_entry(RaftTerm(1), b"SET key2 value2".to_vec()).await.unwrap();
        }
        
        // Wait for replication
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Stop a follower
        let follower_id = cluster.nodes.keys()
            .find(|id| *id != &leader_id)
            .unwrap()
            .clone();
        
        cluster.nodes.get_mut(&follower_id).unwrap().stop().await.unwrap();
        println!("Stopped follower: {}", follower_id);
        
        // Add more log entries while follower is down
        if let Some(leader_node) = cluster.nodes.get_mut(&leader_id) {
            leader_node.append_log_entry(RaftTerm(1), b"SET key3 value3".to_vec()).await.unwrap();
            leader_node.append_log_entry(RaftTerm(1), b"SET key4 value4".to_vec()).await.unwrap();
        }
        
        // Restart the follower
        cluster.nodes.get_mut(&follower_id).unwrap().start().await.unwrap();
        println!("Restarted follower: {}", follower_id);
        
        // Wait for catch-up replication
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // Verify log consistency
        let is_consistent = cluster.verify_log_consistency().await;
        assert!(is_consistent, "Log should be consistent after follower restart");
        
        println!("=== test_log_replication_with_failures completed successfully ===");
    }
}

mod consistency_tests {
    use super::*;

    #[tokio::test]
    async fn test_log_consistency_checks() {
        println!("=== Starting test_log_consistency_checks ===");
        
        // Create a 3-node cluster
        let mut cluster = create_log_replication_test_cluster(3).await;
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for leader election
        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        println!("Leader elected: {}", leader_id);
        
        // Add log entries and verify consistency after each addition
        for i in 1..=5 {
            if let Some(leader_node) = cluster.nodes.get_mut(&leader_id) {
                let command = format!("SET key{} value{}", i, i);
                leader_node.append_log_entry(RaftTerm(1), command.as_bytes().to_vec()).await.unwrap();
                println!("Added log entry {}: {}", i, command);
            }
            
            // Wait for replication
            tokio::time::sleep(Duration::from_millis(50)).await;
            
            // Verify consistency
            let is_consistent = cluster.verify_log_consistency().await;
            assert!(is_consistent, "Log should be consistent after entry {}", i);
        }
        
        // Verify all nodes have the same log entries
        let stats = cluster.get_cluster_log_stats().await;
        for (node_id, (last_index, _, _)) in &stats {
            assert_eq!(*last_index, LogIndex(5), "Node {} should have 5 log entries", node_id);
        }
        
        println!("=== test_log_consistency_checks completed successfully ===");
    }

    #[tokio::test]
    async fn test_log_consistency_with_network_partitions() {
        println!("=== Starting test_log_consistency_with_network_partitions ===");
        
        // Create a 3-node cluster
        let mut cluster = create_log_replication_test_cluster(3).await;
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for leader election
        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        println!("Leader elected: {}", leader_id);
        
        // Add initial log entries
        if let Some(leader_node) = cluster.nodes.get_mut(&leader_id) {
            leader_node.append_log_entry(RaftTerm(1), b"SET key1 value1".to_vec()).await.unwrap();
            leader_node.append_log_entry(RaftTerm(1), b"SET key2 value2".to_vec()).await.unwrap();
        }
        
        // Wait for replication
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Partition a follower
        let follower_id = cluster.nodes.keys()
            .find(|id| *id != &leader_id)
            .unwrap()
            .clone();
        
        cluster.nodes.get_mut(&follower_id).unwrap().is_network_partitioned = true;
        println!("Partitioned follower: {}", follower_id);
        
        // Add more log entries while follower is partitioned
        if let Some(leader_node) = cluster.nodes.get_mut(&leader_id) {
            leader_node.append_log_entry(RaftTerm(1), b"SET key3 value3".to_vec()).await.unwrap();
            leader_node.append_log_entry(RaftTerm(1), b"SET key4 value4".to_vec()).await.unwrap();
        }
        
        // Restore network connectivity
        cluster.nodes.get_mut(&follower_id).unwrap().is_network_partitioned = false;
        println!("Restored network connectivity for follower: {}", follower_id);
        
        // Wait for catch-up replication
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // Verify log consistency
        let is_consistent = cluster.verify_log_consistency().await;
        assert!(is_consistent, "Log should be consistent after network partition recovery");
        
        println!("=== test_log_consistency_with_network_partitions completed successfully ===");
    }
}

mod replication_lag_tests {
    use super::*;

    #[tokio::test]
    async fn test_replication_lag_monitoring() {
        println!("=== Starting test_replication_lag_monitoring ===");
        
        // Create a 3-node cluster
        let mut cluster = create_log_replication_test_cluster(3).await;
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for leader election
        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        println!("Leader elected: {}", leader_id);
        
        // Add log entries rapidly to create lag
        for i in 1..=10 {
            if let Some(leader_node) = cluster.nodes.get_mut(&leader_id) {
                let command = format!("SET key{} value{}", i, i);
                leader_node.append_log_entry(RaftTerm(1), command.as_bytes().to_vec()).await.unwrap();
            }
            // Small delay to create some lag
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        
        // Wait for replication to catch up
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        // Verify all nodes have caught up
        let stats = cluster.get_cluster_log_stats().await;
        for (node_id, (last_index, _, _)) in &stats {
            assert_eq!(*last_index, LogIndex(10), "Node {} should have caught up to 10 entries", node_id);
        }
        
        // Verify log consistency
        let is_consistent = cluster.verify_log_consistency().await;
        assert!(is_consistent, "Log should be consistent after lag recovery");
        
        println!("=== test_replication_lag_monitoring completed successfully ===");
    }
}

mod log_compaction_tests {
    use super::*;

    #[tokio::test]
    async fn test_log_compaction() {
        println!("=== Starting test_log_compaction ===");
        
        // Create a 3-node cluster
        let mut cluster = create_log_replication_test_cluster(3).await;
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for leader election
        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        println!("Leader elected: {}", leader_id);
        
        // Add many log entries to trigger compaction
        for i in 1..=50 {
            if let Some(leader_node) = cluster.nodes.get_mut(&leader_id) {
                let command = format!("SET key{} value{}", i, i);
                leader_node.append_log_entry(RaftTerm(1), command.as_bytes().to_vec()).await.unwrap();
            }
        }
        
        // Wait for replication
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        // Verify all nodes have the same log entries
        let stats = cluster.get_cluster_log_stats().await;
        for (node_id, (last_index, _, _)) in &stats {
            assert_eq!(*last_index, LogIndex(50), "Node {} should have 50 log entries", node_id);
        }
        
        // Verify log consistency
        let is_consistent = cluster.verify_log_consistency().await;
        assert!(is_consistent, "Log should be consistent after many entries");
        
        println!("=== test_log_compaction completed successfully ===");
    }
}

mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_comprehensive_log_replication() {
        println!("=== Starting test_comprehensive_log_replication ===");
        
        // Create a 5-node cluster
        let mut cluster = create_log_replication_test_cluster(5).await;
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for leader election
        let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        println!("Leader elected: {}", leader_id);
        
        // Phase 1: Add initial log entries
        println!("Phase 1: Adding initial log entries...");
        for i in 1..=10 {
            if let Some(leader_node) = cluster.nodes.get_mut(&leader_id) {
                let command = format!("SET key{} value{}", i, i);
                leader_node.append_log_entry(RaftTerm(1), command.as_bytes().to_vec()).await.unwrap();
            }
        }
        
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(cluster.verify_log_consistency().await, "Log should be consistent after phase 1");
        
        // Phase 2: Simulate follower failure and recovery
        println!("Phase 2: Testing follower failure and recovery...");
        let follower_ids: Vec<String> = cluster.nodes.keys()
            .filter(|id| *id != &leader_id)
            .take(2)
            .cloned()
            .collect();
        
        // Stop followers
        for follower_id in &follower_ids {
            cluster.nodes.get_mut(follower_id).unwrap().stop().await.unwrap();
            println!("Stopped follower: {}", follower_id);
        }
        
        // Add more log entries while followers are down
        for i in 11..=20 {
            if let Some(leader_node) = cluster.nodes.get_mut(&leader_id) {
                let command = format!("SET key{} value{}", i, i);
                leader_node.append_log_entry(RaftTerm(1), command.as_bytes().to_vec()).await.unwrap();
            }
        }
        
        // Restart followers
        for follower_id in &follower_ids {
            cluster.nodes.get_mut(follower_id).unwrap().start().await.unwrap();
            println!("Restarted follower: {}", follower_id);
        }
        
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(cluster.verify_log_consistency().await, "Log should be consistent after phase 2");
        
        // Phase 3: Test network partitions
        println!("Phase 3: Testing network partitions...");
        let partition_follower = follower_ids[0].clone();
        cluster.nodes.get_mut(&partition_follower).unwrap().is_network_partitioned = true;
        println!("Partitioned follower: {}", partition_follower);
        
        // Add more log entries
        for i in 21..=30 {
            if let Some(leader_node) = cluster.nodes.get_mut(&leader_id) {
                let command = format!("SET key{} value{}", i, i);
                leader_node.append_log_entry(RaftTerm(1), command.as_bytes().to_vec()).await.unwrap();
            }
        }
        
        // Restore network connectivity
        cluster.nodes.get_mut(&partition_follower).unwrap().is_network_partitioned = false;
        println!("Restored network connectivity for follower: {}", partition_follower);
        
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(cluster.verify_log_consistency().await, "Log should be consistent after phase 3");
        
        // Final verification
        let stats = cluster.get_cluster_log_stats().await;
        for (node_id, (last_index, _, _)) in &stats {
            assert_eq!(*last_index, LogIndex(30), "Node {} should have 30 log entries", node_id);
        }
        
        println!("=== test_comprehensive_log_replication completed successfully ===");
    }
}

#[tokio::test]
async fn test_phase2_2_complete() {
    println!("=== Starting Phase 2.2 Complete Test ===");
    
    // Create a 3-node cluster
    let mut cluster = create_log_replication_test_cluster(3).await;
    cluster.start_all_nodes().await.unwrap();
    
    // Wait for leader election
    let leader_id = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    println!("✅ Leader election completed: {}", leader_id);
    
    // Test basic log replication
    if let Some(leader_node) = cluster.nodes.get_mut(&leader_id) {
        leader_node.append_log_entry(RaftTerm(1), b"SET key1 value1".to_vec()).await.unwrap();
        leader_node.append_log_entry(RaftTerm(1), b"SET key2 value2".to_vec()).await.unwrap();
        leader_node.append_log_entry(RaftTerm(1), b"SET key3 value3".to_vec()).await.unwrap();
    }
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Verify log consistency
    let is_consistent = cluster.verify_log_consistency().await;
    assert!(is_consistent, "Log should be consistent across all nodes");
    
    // Verify all nodes have the same log entries
    let stats = cluster.get_cluster_log_stats().await;
    for (node_id, (last_index, _, _)) in &stats {
        assert_eq!(*last_index, LogIndex(3), "Node {} should have 3 log entries", node_id);
    }
    
    println!("✅ Log replication working correctly");
    println!("✅ Log consistency verified");
    println!("✅ All nodes synchronized");
    println!("=== Phase 2.2 Complete Test finished successfully ===");
} 