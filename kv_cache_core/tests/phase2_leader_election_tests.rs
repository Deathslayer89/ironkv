//! Comprehensive tests for Phase 2.1: Leader Election and Failover
//! 
//! This module tests the leader election and failover functionality including:
//! - Proper leader election with timeouts
//! - Automatic failover when leader goes down
//! - Network partition handling in leader election
//! - Leader election metrics and monitoring

use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio::time::sleep;
use kv_cache_core::{
    consensus::{
        RaftConsensus, RaftState, RaftRole, RaftTerm,
        communication::{RaftRpc, RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse},
        state::RaftStateSummary,
        log::{LogIndex, LogTerm},
    },
    config::{ClusterConfig, MetricsConfig},
    metrics::MetricsCollector,
    log::log_cluster_operation,
};

// Test structures for Phase 2.1
#[derive(Clone)]
pub struct ElectionTestNode {
    pub node_id: String,
    pub consensus: RaftConsensus,
    pub state: Arc<RwLock<RaftState>>,
    pub is_running: bool,
    pub is_network_partitioned: bool,
}

impl ElectionTestNode {
    // Remove the old new method since we're creating nodes directly in the cluster
    
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.consensus.start().await?;
        self.is_running = true;
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.consensus.stop().await?;
        self.is_running = false;
        Ok(())
    }

    pub async fn get_role(&self) -> RaftRole {
        let state = self.state.read().await;
        state.role.clone()
    }

    pub async fn get_term(&self) -> RaftTerm {
        let state = self.state.read().await;
        state.current_term
    }

    pub async fn is_leader(&self) -> bool {
        if !self.is_running {
            return false;
        }
        self.consensus.is_leader().await
    }

    pub async fn get_state_summary(&self) -> RaftStateSummary {
        let state = self.state.read().await;
        state.get_summary()
    }

    pub fn partition_network(&mut self) {
        self.is_network_partitioned = true;
    }

    pub fn restore_network(&mut self) {
        self.is_network_partitioned = false;
    }
}

#[derive(Clone)]
pub struct ElectionTestCluster {
    pub nodes: HashMap<String, ElectionTestNode>,
    pub config: ClusterConfig,
}

impl ElectionTestCluster {
    pub fn new(node_count: usize) -> Self {
        let mut members = HashMap::new();
        let mut nodes = HashMap::new();
        
        for i in 0..node_count {
            let node_id = format!("node-{}", i);
            let address = format!("127.0.0.1:{}", 8000 + i);
            members.insert(node_id.clone(), address);
        }
        
        let config = ClusterConfig {
            members,
            heartbeat_interval: 1, // 1 second
            failure_timeout: 3, // 3 seconds
            ..Default::default()
        };
        
        Self {
            nodes,
            config,
        }
    }

    pub async fn add_node(&mut self, node_id: String, cluster_ref: Arc<RwLock<ElectionTestCluster>>) -> Result<(), Box<dyn std::error::Error>> {
        let metrics_config = MetricsConfig::default();
        let metrics = Arc::new(MetricsCollector::new(metrics_config));
        
        // Create MockRpcClient for this node
        let mock_rpc = Arc::new(MockRpcClient::new(cluster_ref, node_id.clone()));
        
        // Create consensus with mock RPC client
        let consensus = RaftConsensus::with_rpc_client(node_id.clone(), self.config.clone(), metrics, mock_rpc);
        let state = consensus.get_state_arc();
        
        let node = ElectionTestNode {
            node_id: node_id.clone(),
            consensus,
            state,
            is_running: false,
            is_network_partitioned: false,
        };
        
        self.nodes.insert(node_id, node);
        Ok(())
    }

    pub async fn start_all_nodes(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.start_all_nodes_staggered().await
    }

    pub async fn start_all_nodes_staggered(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for (i, (node_id, node)) in self.nodes.iter_mut().enumerate() {
            println!("Starting node {} (index {})", node_id, i);
            println!("Before start: node {} is_running={}", node_id, node.is_running);
            node.start().await?;
            println!("After start: node {} is_running={}", node_id, node.is_running);
            // Add a small delay between starting nodes to ensure different election timeouts
            sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }

    pub async fn stop_all_nodes(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for (_, node) in self.nodes.iter_mut() {
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
            sleep(Duration::from_millis(10)).await;
        }
        None
    }

    pub async fn wait_for_election_completion(&self, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            let mut has_leader = false;
            for (_, node) in &self.nodes {
                if node.is_leader().await {
                    has_leader = true;
                    break;
                }
            }
            
            if has_leader {
                return true;
            }
            
            sleep(Duration::from_millis(10)).await;
        }
        false
    }

    pub async fn get_cluster_state(&self) -> HashMap<String, RaftStateSummary> {
        let mut states = HashMap::new();
        for (node_id, node) in &self.nodes {
            states.insert(node_id.clone(), node.get_state_summary().await);
        }
        states
    }

    pub async fn stop_node(&mut self, node_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.is_running = false;
            // Actually stop the consensus
            node.consensus.stop().await?;
        }
        Ok(())
    }

    pub fn start_node(&mut self, node_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.is_running = true;
        }
        Ok(())
    }
}

// Mock RPC client for testing
#[derive(Clone)]
pub struct MockRpcClient {
    pub cluster: Arc<RwLock<ElectionTestCluster>>,
    pub node_id: String,
}

impl MockRpcClient {
    pub fn new(cluster: Arc<RwLock<ElectionTestCluster>>, node_id: String) -> Self {
        Self { cluster, node_id }
    }
}

#[async_trait::async_trait]
impl RaftRpc for MockRpcClient {
    async fn request_vote(
        &self,
        target: &str,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>> {
        let mut cluster = self.cluster.write().await;
        
        // Find the node ID that corresponds to this address
        let target_node_id = cluster.config.members.iter()
            .find(|(_, addr)| addr.as_str() == target)
            .map(|(node_id, _)| node_id.clone());
        
        let target_node_id = match target_node_id {
            Some(id) => id,
            None => {
                println!("MockRpcClient: Could not find node for address {}", target);
                return Ok(RequestVoteResponse {
                    term: RaftTerm(0),
                    vote_granted: false,
                });
            }
        };
        
        // Check if target node is partitioned (assume nodes are running if they have consensus)
        if let Some(target_node) = cluster.nodes.get_mut(&target_node_id) {
            println!("MockRpcClient: Target node {} is_running={}, is_partitioned={}", target_node_id, target_node.is_running, target_node.is_network_partitioned);
            if target_node.is_network_partitioned {
                return Err("Node unavailable".into());
            }
            
            // Get current state of target node
            let mut target_state = target_node.state.write().await;
            
            // If request term is higher, update our term and clear vote
            if request.term > target_state.current_term {
                target_state.update_term(request.term);
                target_state.voted_for = None;
                target_state.role = crate::RaftRole::Follower;
            }
            
            // Grant vote if term is higher or same and haven't voted yet
            println!("MockRpcClient: Vote check - request.term={}, target_state.current_term={}, target_state.voted_for={:?}", 
                     request.term, target_state.current_term, target_state.voted_for);
            if request.term >= target_state.current_term 
                && target_state.voted_for.is_none() {
                
                // Actually update the target node's state
                target_state.vote_for(request.candidate_id.clone());
                target_state.update_leader_contact(); // Reset election timeout
                
                drop(target_state); // Release lock
                
                println!("MockRpcClient: Node {} granted vote to {}", target_node_id, request.candidate_id);
                
                return Ok(RequestVoteResponse {
                    term: request.term,
                    vote_granted: true,
                });
            }
            
            drop(target_state); // Release lock
        }
        
        // Return the actual term from the target node
        let target_term = if let Some(target_node) = cluster.nodes.get(&target_node_id) {
            let target_state = target_node.state.read().await;
            println!("MockRpcClient: Target node {} has term {}", target_node_id, target_state.current_term);
            target_state.current_term
        } else {
            println!("MockRpcClient: Target node {} not found", target_node_id);
            RaftTerm(0)
        };
        
        println!("MockRpcClient: Returning response with term {} for target {}", target_term, target_node_id);
        
        Ok(RequestVoteResponse {
            term: target_term,
            vote_granted: false,
        })
    }

    async fn append_entries(
        &self,
        target: &str,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error + Send + Sync>> {
        let mut cluster = self.cluster.write().await;
        
        // Find the node ID that corresponds to this address
        let target_node_id = cluster.config.members.iter()
            .find(|(_, addr)| addr.as_str() == target)
            .map(|(node_id, _)| node_id.clone());
        
        let target_node_id = match target_node_id {
            Some(id) => id,
            None => {
                return Ok(AppendEntriesResponse {
                    term: RaftTerm(0),
                    success: false,
                    conflict_index: Some(LogIndex(0)),
                    conflict_term: Some(LogTerm(0)),
                    last_log_index: Some(LogIndex(0)),
                });
            }
        };
        
        // Check if target node is partitioned (ignore is_running in test environment)
        if let Some(target_node) = cluster.nodes.get_mut(&target_node_id) {
            println!("MockRpcClient: Target node {} is_running={}, is_partitioned={}", target_node_id, target_node.is_running, target_node.is_network_partitioned);
            if target_node.is_network_partitioned {
                return Err("Node unavailable".into());
            }
            
            let mut target_state = target_node.state.write().await;
            
            // Accept append entries if term is higher or same
            if request.term >= target_state.current_term {
                // Update the target node's state
                target_state.update_leader_contact(); // Reset election timeout
                target_state.leader_id = Some(request.leader_id.clone());
                
                // If term is higher, become follower
                if request.term > target_state.current_term {
                    target_state.become_follower(request.term, Some(request.leader_id.clone()));
                }
                
                drop(target_state); // Release lock
                
                return Ok(AppendEntriesResponse {
                    term: request.term,
                    success: true,
                    conflict_index: Some(LogIndex(0)),
                    conflict_term: Some(LogTerm(0)),
                    last_log_index: Some(LogIndex(0)),
                });
            }
            
            drop(target_state); // Release lock
        }
        
        Ok(AppendEntriesResponse {
            term: RaftTerm(0),
            success: false,
            conflict_index: Some(LogIndex(0)),
            conflict_term: Some(LogTerm(0)),
            last_log_index: Some(LogIndex(0)),
        })
    }

    async fn install_snapshot(
        &self,
        _target: &str,
        _request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, Box<dyn std::error::Error + Send + Sync>> {
        // Mock implementation - always succeed
        Ok(InstallSnapshotResponse {
            term: RaftTerm(0),
            success: true,
        })
    }
}

// Helper function to create a test cluster with properly connected nodes
async fn create_test_cluster(node_count: usize) -> ElectionTestCluster {
    let mut cluster = ElectionTestCluster::new(node_count);
    
    // Create a shared reference to the cluster
    let cluster_ref = Arc::new(RwLock::new(cluster.clone()));
    
    // Add nodes with the shared reference
    for i in 0..node_count {
        let node_id = format!("node-{}", i);
        cluster.add_node(node_id, Arc::clone(&cluster_ref)).await.unwrap();
    }
    
    // Update the shared reference to point to the cluster with nodes
    {
        let mut cluster_guard = cluster_ref.write().await;
        *cluster_guard = cluster.clone();
    }
    
    // Wait a bit to ensure all nodes are properly initialized
    sleep(Duration::from_millis(10)).await;
    
    cluster
}

mod leader_election_tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_leader_election() {
        // Create a 3-node cluster
        let mut cluster = create_test_cluster(3).await;
        
        // Start nodes with a small delay between them to avoid simultaneous elections
        for (i, (node_id, node)) in cluster.nodes.iter_mut().enumerate() {
            println!("Starting node {} (index {})", node_id, i);
            node.start().await.unwrap();
            // Add a small delay between starting nodes to ensure different election timeouts
            sleep(Duration::from_millis(50)).await;
        }
        
        // Add debug logging to see what's happening
        println!("Cluster started, checking initial state...");
        for (node_id, node) in &cluster.nodes {
            let role = node.get_role().await;
            let term = node.get_term().await;
            let state_summary = node.get_state_summary().await;
            println!("Node {}: role={:?}, term={}, votes={}, voted_for={:?}", 
                     node_id, role, term, state_summary.votes_received, state_summary.voted_for);
        }
        
        // Wait a bit for election loop to start
        sleep(Duration::from_millis(200)).await;
        
        println!("After 200ms, checking state...");
        for (node_id, node) in &cluster.nodes {
            let role = node.get_role().await;
            let term = node.get_term().await;
            let state_summary = node.get_state_summary().await;
            println!("Node {}: role={:?}, term={}, votes={}, voted_for={:?}", 
                     node_id, role, term, state_summary.votes_received, state_summary.voted_for);
        }
        
        // Wait a bit more to see if votes are collected
        sleep(Duration::from_millis(1000)).await;
        
        println!("After 1000ms, checking state...");
        for (node_id, node) in &cluster.nodes {
            let role = node.get_role().await;
            let term = node.get_term().await;
            let state_summary = node.get_state_summary().await;
            println!("Node {}: role={:?}, term={}, votes={}, voted_for={:?}", 
                     node_id, role, term, state_summary.votes_received, state_summary.voted_for);
        }
        
        // Wait for leader election
        let leader = cluster.wait_for_leader(Duration::from_secs(5)).await;
        assert!(leader.is_some(), "Leader election should complete within 5 seconds");
        
        // Wait a bit more for the leader to establish its leadership
        sleep(Duration::from_millis(500)).await;
        
        // Verify only one leader exists
        let mut leader_count = 0;
        for (_, node) in &cluster.nodes {
            if node.is_leader().await {
                leader_count += 1;
            }
        }
        assert_eq!(leader_count, 1, "Only one leader should exist");
        
        // Verify all nodes have the same term
        let states = cluster.get_cluster_state().await;
        let first_term = states.values().next().unwrap().current_term;
        for state in states.values() {
            assert_eq!(state.current_term, first_term, "All nodes should have the same term");
        }
    }

    #[tokio::test]
    async fn test_leader_failover() {
        println!("=== Starting test_leader_failover ===");
        
        // Create a 3-node cluster
        let mut cluster = create_test_cluster(3).await;
        println!("Cluster created with {} nodes", cluster.nodes.len());
        
        println!("Cluster created, about to start nodes...");
        
        // Start all nodes
        cluster.start_all_nodes().await.unwrap();
        
        println!("Nodes started, checking states...");
        
        // Add debugging to see node states
        for (node_id, node) in &cluster.nodes {
            println!("Node {}: is_running={}", node_id, node.is_running);
        }
        
        // Wait a bit more to ensure nodes are fully started
        sleep(Duration::from_millis(100)).await;
        
        // Check states again
        println!("After 100ms delay, checking states...");
        for (node_id, node) in &cluster.nodes {
            println!("Node {}: is_running={}", node_id, node.is_running);
        }
        
        // Wait for initial leader
        let initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await;
        assert!(initial_leader.is_some(), "Initial leader election should complete");
        
        let initial_leader_id = initial_leader.unwrap();
        println!("Initial leader elected: {}", initial_leader_id);
        
        // Stop the leader
        cluster.stop_node(&initial_leader_id).await.unwrap();
        println!("Stopped leader: {}", initial_leader_id);
        
        // Wait for new leader election
        let new_leader = cluster.wait_for_leader(Duration::from_secs(5)).await;
        assert!(new_leader.is_some(), "New leader should be elected after failover");
        
        let new_leader_id = new_leader.unwrap();
        println!("New leader elected: {}", new_leader_id);
        
        // Verify that the new leader is different from the original leader
        assert_ne!(initial_leader_id, new_leader_id, "New leader should be different from the original leader");
        
        // Verify only one leader exists
        let mut leader_count = 0;
        for (_, node) in &cluster.nodes {
            if node.is_leader().await {
                leader_count += 1;
            }
        }
        assert_eq!(leader_count, 1, "Only one leader should exist after failover");
        
        println!("=== test_leader_failover completed successfully ===");
    }

    #[tokio::test]
    async fn test_network_partition_handling() {
        // Create a 5-node cluster
        let mut cluster = create_test_cluster(5).await;
        
        // Start all nodes
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for initial leader
        let initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await;
        assert!(initial_leader.is_some(), "Initial leader election should complete");
        
        // Create network partition (split into 2 groups: 3 nodes and 2 nodes)
        // The 3-node group should be able to elect a leader
        // The 2-node group should not be able to elect a leader (no majority)
        
        // Partition nodes 0,1,2 from nodes 3,4
        for i in 0..3 {
            if let Some(node) = cluster.nodes.get_mut(&format!("node-{}", i)) {
                node.partition_network();
            }
        }
        
        // Wait for partition to be detected and new election
        sleep(Duration::from_millis(500)).await;
        
        // The 3-node partition should still have a leader
        let mut leader_in_partition = false;
        for i in 0..3 {
            if let Some(node) = cluster.nodes.get(&format!("node-{}", i)) {
                if node.is_leader().await {
                    leader_in_partition = true;
                    break;
                }
            }
        }
        assert!(leader_in_partition, "3-node partition should maintain a leader");
        
        // The 2-node partition should not have a leader (no majority)
        let mut leader_in_minority = false;
        for i in 3..5 {
            if let Some(node) = cluster.nodes.get(&format!("node-{}", i)) {
                if node.is_leader().await {
                    leader_in_minority = true;
                    break;
                }
            }
        }
        assert!(!leader_in_minority, "2-node partition should not have a leader (no majority)");
    }

    #[tokio::test]
    async fn test_election_timeout_behavior() {
        // Create a 3-node cluster with longer election timeout
        let mut cluster = create_test_cluster(3).await;
        cluster.config.heartbeat_interval = 2; // 2 second timeout
        
        // Start all nodes
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for leader election (should take longer due to increased timeout)
        let start = Instant::now();
        let leader = cluster.wait_for_leader(Duration::from_secs(10)).await;
        let election_duration = start.elapsed();
        
        assert!(leader.is_some(), "Leader election should complete");
        assert!(election_duration >= Duration::from_millis(1000), 
                "Election should take at least the configured timeout duration");
    }

    #[tokio::test]
    async fn test_concurrent_elections() {
        // Create a 3-node cluster
        let mut cluster = create_test_cluster(3).await;
        
        // Start all nodes simultaneously
        cluster.start_all_nodes().await.unwrap();
        
        // All nodes should start as followers
        for (_, node) in &cluster.nodes {
            assert_eq!(node.get_role().await, RaftRole::Follower, "Nodes should start as followers");
        }
        
        // Wait for leader election
        let leader = cluster.wait_for_leader(Duration::from_secs(5)).await;
        assert!(leader.is_some(), "Leader election should complete");
        
        // Verify exactly one leader and others are followers
        let mut leader_count = 0;
        let mut follower_count = 0;
        
        for (_, node) in &cluster.nodes {
            match node.get_role().await {
                RaftRole::Leader => leader_count += 1,
                RaftRole::Follower => follower_count += 1,
                RaftRole::Candidate => panic!("No nodes should be in candidate state after election"),
            }
        }
        
        assert_eq!(leader_count, 1, "Exactly one leader should exist");
        assert_eq!(follower_count, 2, "Exactly two followers should exist");
    }
}

mod failover_tests {
    use super::*;

    #[tokio::test]
    async fn test_leader_crash_failover() {
        // Create a 3-node cluster
        let mut cluster = create_test_cluster(3).await;
        
        // Start all nodes
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for initial leader
        let initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        
        // Simulate leader crash by stopping the leader node
        cluster.stop_node(&initial_leader).await.unwrap();
        
        // Wait for failover
        let new_leader = cluster.wait_for_leader(Duration::from_secs(10)).await;
        assert!(new_leader.is_some(), "New leader should be elected after leader crash");
        
        let new_leader_id = new_leader.unwrap();
        assert_ne!(new_leader_id, initial_leader, "New leader should be different from crashed leader");
        
        // Verify the crashed leader is not running
        if let Some(crashed_node) = cluster.nodes.get(&initial_leader) {
            assert!(!crashed_node.is_running, "Crashed leader should not be running");
        }
    }

    #[tokio::test]
    async fn test_multiple_leader_failures() {
        // Create a 5-node cluster
        let mut cluster = create_test_cluster(5).await;
        
        // Start all nodes
        cluster.start_all_nodes().await.unwrap();
        
        // Perform multiple leader failures
        for failure_round in 0..3 {
            // Wait for current leader
            let current_leader = cluster.wait_for_leader(Duration::from_secs(5)).await;
            assert!(current_leader.is_some(), "Leader should exist in round {}", failure_round);
            
            let leader_id = current_leader.unwrap();
            let current_term = {
                let node = cluster.nodes.get(&leader_id).unwrap();
                node.get_term().await
            };
            
            // Stop the leader
            cluster.stop_node(&leader_id).await.unwrap();
            
            // Wait for new leader
            let new_leader = cluster.wait_for_leader(Duration::from_secs(10)).await;
            assert!(new_leader.is_some(), "New leader should be elected in round {}", failure_round);
            
            let new_leader_id = new_leader.unwrap();
            let new_term = {
                let node = cluster.nodes.get(&new_leader_id).unwrap();
                node.get_term().await
            };
            
            assert_ne!(new_leader_id, leader_id, "New leader should be different in round {}", failure_round);
            assert!(new_term.value() > current_term.value(), "Term should increase in round {}", failure_round);
        }
    }

    #[tokio::test]
    async fn test_leader_recovery() {
        // Create a 3-node cluster
        let mut cluster = create_test_cluster(3).await;
        
        // Start all nodes
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for initial leader
        let initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        let initial_term = {
            let node = cluster.nodes.get(&initial_leader).unwrap();
            node.get_term().await
        };
        
        // Stop the leader
        cluster.stop_node(&initial_leader).await.unwrap();
        
        // Wait for new leader
        let new_leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
        let new_term = {
            let node = cluster.nodes.get(&new_leader).unwrap();
            node.get_term().await
        };
        
        assert!(new_term.value() > initial_term.value(), "Term should increase after failover");
        
        // Restart the original leader
        cluster.start_node(&initial_leader).unwrap();
        
        // The original leader should become a follower
        sleep(Duration::from_millis(500)).await;
        
        let original_leader_role = {
            let node = cluster.nodes.get(&initial_leader).unwrap();
            node.get_role().await
        };
        
        assert_eq!(original_leader_role, RaftRole::Follower, "Recovered leader should become follower");
        
        // The new leader should remain leader
        let current_leader = cluster.get_leader().await;
        assert_eq!(current_leader, Some(new_leader), "New leader should remain leader after recovery");
    }
}

mod network_partition_tests {
    use super::*;

    #[tokio::test]
    async fn test_minority_partition() {
        // Create a 5-node cluster
        let mut cluster = create_test_cluster(5).await;
        
        // Start all nodes
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for initial leader
        let initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        
        // Create minority partition (2 nodes)
        for i in 0..2 {
            if let Some(node) = cluster.nodes.get_mut(&format!("node-{}", i)) {
                node.partition_network();
            }
        }
        
        // Wait for partition to be detected
        sleep(Duration::from_millis(500)).await;
        
        // Majority partition should maintain a leader
        let majority_leader = cluster.get_leader().await;
        assert!(majority_leader.is_some(), "Majority partition should maintain a leader");
        
        // Minority partition should not have a leader
        let mut minority_has_leader = false;
        for i in 0..2 {
            if let Some(node) = cluster.nodes.get(&format!("node-{}", i)) {
                if node.is_leader().await {
                    minority_has_leader = true;
                    break;
                }
            }
        }
        assert!(!minority_has_leader, "Minority partition should not have a leader");
    }

    #[tokio::test]
    async fn test_partition_recovery() {
        // Create a 3-node cluster
        let mut cluster = create_test_cluster(3).await;
        
        // Start all nodes
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for initial leader
        let initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        
        // Create partition
        for i in 0..2 {
            if let Some(node) = cluster.nodes.get_mut(&format!("node-{}", i)) {
                node.partition_network();
            }
        }
        
        // Wait for partition to be detected
        sleep(Duration::from_millis(500)).await;
        
        // Restore network connectivity
        for i in 0..2 {
            if let Some(node) = cluster.nodes.get_mut(&format!("node-{}", i)) {
                node.restore_network();
            }
        }
        
        // Wait for cluster to stabilize
        sleep(Duration::from_millis(1000)).await;
        
        // Cluster should have a single leader
        let leader = cluster.get_leader().await;
        assert!(leader.is_some(), "Cluster should have a leader after partition recovery");
        
        // All nodes should have the same term
        let states = cluster.get_cluster_state().await;
        let first_term = states.values().next().unwrap().current_term;
        for state in states.values() {
            assert_eq!(state.current_term, first_term, "All nodes should have the same term after recovery");
        }
    }
}

mod metrics_tests {
    use super::*;

    #[tokio::test]
    async fn test_election_metrics() {
        // Create a 3-node cluster
        let mut cluster = create_test_cluster(3).await;
        
        // Start all nodes
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for leader election
        let leader = cluster.wait_for_leader(Duration::from_secs(5)).await;
        assert!(leader.is_some(), "Leader election should complete");
        
        // Verify election metrics are collected
        for (node_id, node) in &cluster.nodes {
            let state = node.get_state_summary().await;
            
            // Verify basic metrics are present
            assert!(state.current_term.value() > 0, "Term should be greater than 0 for node {}", node_id);
            assert!(state.total_nodes > 0, "Total nodes should be greater than 0 for node {}", node_id);
            
            // Verify role is properly set
            assert!(matches!(state.role, RaftRole::Leader | RaftRole::Follower), 
                    "Node {} should be either leader or follower", node_id);
        }
    }

    #[tokio::test]
    async fn test_failover_metrics() {
        // Create a 3-node cluster
        let mut cluster = create_test_cluster(3).await;
        
        // Start all nodes
        cluster.start_all_nodes().await.unwrap();
        
        // Wait for initial leader
        let initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        let initial_term = {
            let node = cluster.nodes.get(&initial_leader).unwrap();
            node.get_term().await
        };
        
        // Stop the leader
        cluster.stop_node(&initial_leader).await.unwrap();
        
        // Wait for new leader
        let new_leader = cluster.wait_for_leader(Duration::from_secs(10)).await.unwrap();
        let new_term = {
            let node = cluster.nodes.get(&new_leader).unwrap();
            node.get_term().await
        };
        
        // Verify term increased
        assert!(new_term.value() > initial_term.value(), "Term should increase after failover");
        
        // Verify new leader metrics
        let new_leader_state = {
            let node = cluster.nodes.get(&new_leader).unwrap();
            node.get_state_summary().await
        };
        
        assert_eq!(new_leader_state.role, RaftRole::Leader, "New leader should have leader role");
        assert_eq!(new_leader_state.current_term, new_term, "New leader term should match");
    }
}

// Integration test for complete Phase 2.1 functionality
#[tokio::test]
async fn test_phase2_1_complete() {
    log_cluster_operation("Starting Phase 2.1 complete integration test", "test-node", true, Duration::from_millis(0), None);
    
    // Create a 5-node cluster
    let mut cluster = create_test_cluster(5).await;
    
    // Start all nodes
    cluster.start_all_nodes().await.unwrap();
    
    // Test 1: Basic leader election
    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await;
    assert!(leader.is_some(), "Basic leader election should work");
    
    // Test 2: Leader failover
    let initial_leader = leader.unwrap();
    cluster.stop_node(&initial_leader).await.unwrap();
    
    let new_leader = cluster.wait_for_leader(Duration::from_secs(10)).await;
    assert!(new_leader.is_some(), "Leader failover should work");
    assert_ne!(new_leader.unwrap(), initial_leader, "New leader should be different");
    
    // Test 3: Network partition handling
    cluster.start_node(&initial_leader).unwrap();
    sleep(Duration::from_millis(500)).await;
    
    // Create partition
    for i in 0..2 {
        if let Some(node) = cluster.nodes.get_mut(&format!("node-{}", i)) {
            node.partition_network();
        }
    }
    
    sleep(Duration::from_millis(500)).await;
    
    // Majority should maintain leader
    let partition_leader = cluster.get_leader().await;
    assert!(partition_leader.is_some(), "Majority partition should maintain leader");
    
    // Test 4: Partition recovery
    for i in 0..2 {
        if let Some(node) = cluster.nodes.get_mut(&format!("node-{}", i)) {
            node.restore_network();
        }
    }
    
    sleep(Duration::from_millis(1000)).await;
    
    // Cluster should stabilize
    let final_leader = cluster.get_leader().await;
    assert!(final_leader.is_some(), "Cluster should stabilize after partition recovery");
    
    // Test 5: Metrics verification
    let states = cluster.get_cluster_state().await;
    let first_term = states.values().next().unwrap().current_term;
    for (node_id, state) in states {
        assert!(state.current_term.value() > 0, "Node {} should have valid term", node_id);
        assert!(matches!(state.role, RaftRole::Leader | RaftRole::Follower), 
                "Node {} should have valid role", node_id);
    }
    
    log_cluster_operation("Phase 2.1 complete integration test passed", "test-node", true, Duration::from_millis(0), None);
} 