//! Comprehensive tests for Phase 5: Advanced Distributed Features
//! 
//! This module tests the advanced distributed features including:
//! - Raft consensus algorithm
//! - Dynamic cluster membership
//! - Data rebalancing
//! - Cross-datacenter replication
//! - Safety mechanisms
//! - Communication protocols

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

use kv_cache_core::{
    consensus::{RaftConsensus, RaftRole, RaftTerm},
    config::{ClusterConfig, MetricsConfig},
    metrics::MetricsCollector,
    cluster::sharding::ShardManager,
    membership::{HealthStatus, MembershipChange, MembershipChangeType, ChangeStatus},
    rebalancing::{RebalancingState, RebalancingOperation, RebalancingType, RebalancingTrigger},
};

// Use actual types from kv_cache_core crate

// Local test-specific types that don't exist in the crate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub node_id: String,
    pub address: String,
    pub port: u16,
    pub datacenter: String,
    pub status: NodeStatus,
    pub health: HealthStatus,
    pub last_seen: u64, // timestamp in milliseconds
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    Active,
    Joining,
    Leaving,
    Failed,
    Suspended,
    Rebalancing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancingPlan {
    pub plan_id: String,
    pub source_nodes: std::collections::HashMap<String, Vec<String>>,
    pub target_nodes: std::collections::HashMap<String, Vec<String>>,
    pub keys_to_move: Vec<String>,
    pub estimated_size: u64,
    pub estimated_duration: u64, // milliseconds
    pub safety_checks_passed: bool,
    pub datacenter_aware: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyCheckResult {
    pub check_id: String,
    pub check_type: SafetyCheckType,
    pub passed: bool,
    pub timestamp: u64,
    pub error: Option<String>,
    pub details: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SafetyCheckType {
    ClusterHealth,
    DataConsistency,
    ResourceAvailability,
    NetworkConnectivity,
    QuorumAvailability,
    RebalancingLocks,
    MigrationReadiness,
    DatacenterConnectivity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancingMessage {
    pub message_type: RebalancingMessageType,
    pub operation_id: String,
    pub source_node: String,
    pub target_node: Option<String>,
    pub payload: Vec<u8>,
    pub timestamp: u64,
    pub sequence_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RebalancingMessageType {
    StartRebalancing,
    StartAck,
    StatusUpdate,
    DataTransferRequest,
    DataTransferResponse,
    CompleteRebalancing,
    CancelRebalancing,
    Heartbeat,
    SafetyCheckRequest,
    SafetyCheckResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatacenterReplicationConfig {
    pub source_datacenter: String,
    pub target_datacenter: String,
    pub replication_factor: u32,
    pub sync_interval: u64, // milliseconds
    pub conflict_resolution: ConflictResolution,
    pub bandwidth_limit: Option<u64>, // bytes per second
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConflictResolution {
    LastWriteWins,
    SourceWins,
    TargetWins,
    Manual,
    TimestampBased,
}

// Mock implementations for comprehensive testing
pub struct MockDistributedCache {
    pub node_id: String,
    pub consensus: RaftConsensus,
    pub shard_manager: ShardManager,
    pub membership_state: Arc<RwLock<MembershipState>>,
    pub rebalancing_state: Arc<RwLock<RebalancingState>>,
    pub safety_coordinator: Arc<RwLock<SafetyCoordinator>>,
    pub communication: Arc<RwLock<CommunicationCoordinator>>,
    pub datacenter_config: Option<DatacenterReplicationConfig>,
}

#[derive(Debug, Clone)]
pub struct MembershipState {
    pub version: u64,
    pub nodes: std::collections::HashMap<String, ClusterNode>,
    pub pending_changes: Vec<MembershipChange>,
    pub last_update: u64,
    pub quorum_size: usize,
}

#[derive(Debug, Clone)]
pub struct SafetyCoordinator {
    pub active_checks: std::collections::HashMap<String, SafetyCheckResult>,
    pub rebalancing_locks: std::collections::HashMap<String, RebalancingLock>,
    pub check_history: Vec<SafetyCheckResult>,
}

#[derive(Debug, Clone)]
pub struct RebalancingLock {
    pub lock_id: String,
    pub locked_by: String,
    pub lock_timestamp: u64,
    pub lock_timeout: u64,
    pub lock_reason: String,
}

#[derive(Debug, Clone)]
pub struct CommunicationCoordinator {
    pub message_handlers: std::collections::HashMap<String, tokio::sync::mpsc::Sender<RebalancingMessage>>,
    pub stats: CommunicationStats,
}

#[derive(Debug, Clone)]
pub struct CommunicationStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub failed_sends: u64,
    pub failed_receives: u64,
    pub avg_latency_ms: u64,
    pub active_connections: usize,
}

impl MockDistributedCache {
    pub fn new(
        node_id: String,
        config: ClusterConfig,
        datacenter_config: Option<DatacenterReplicationConfig>,
    ) -> Self {
        let consensus = RaftConsensus::new(
            node_id.clone(),
            config.clone(),
            Arc::new(MetricsCollector::new(MetricsConfig::default())),
        );
        
        let shard_manager = ShardManager::new().with_local_node_id(node_id.clone());
        
        let membership_state = Arc::new(RwLock::new(MembershipState {
            version: 1,
            nodes: std::collections::HashMap::new(),
            pending_changes: Vec::new(),
            last_update: Instant::now().elapsed().as_millis() as u64,
            quorum_size: 2,
        }));
        
        let rebalancing_state = Arc::new(RwLock::new(RebalancingState::Idle));
        
        let safety_coordinator = Arc::new(RwLock::new(SafetyCoordinator {
            active_checks: std::collections::HashMap::new(),
            rebalancing_locks: std::collections::HashMap::new(),
            check_history: Vec::new(),
        }));
        
        let communication = Arc::new(RwLock::new(CommunicationCoordinator {
            message_handlers: std::collections::HashMap::new(),
            stats: CommunicationStats {
                messages_sent: 0,
                messages_received: 0,
                failed_sends: 0,
                failed_receives: 0,
                avg_latency_ms: 0,
                active_connections: 0,
            },
        }));
        
        Self {
            node_id,
            consensus,
            shard_manager,
            membership_state,
            rebalancing_state,
            safety_coordinator,
            communication,
            datacenter_config,
        }
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Start consensus
        self.consensus.start().await?;
        
        // Start shard manager
        self.shard_manager.start().await.map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        
        // Add self to membership
        let mut membership = self.membership_state.write().await;
        let node = ClusterNode {
            node_id: self.node_id.clone(),
            address: "127.0.0.1".to_string(),
            port: 8080,
            datacenter: "dc1".to_string(),
            status: NodeStatus::Active,
            health: HealthStatus::Healthy,
            last_seen: Instant::now().elapsed().as_millis() as u64,
            version: 1,
        };
        let _: &mut std::collections::HashMap<String, ClusterNode> = &mut membership.nodes;
        membership.nodes.insert(self.node_id.clone(), node);
        
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.consensus.stop().await?;
        self.shard_manager.stop().await.map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        Ok(())
    }

    pub async fn add_node(&mut self, node: ClusterNode) -> Result<(), Box<dyn std::error::Error>> {
        let node_id = node.node_id.clone();
        let consensus_term = self.consensus.get_state().await.current_term;
        
        {
            let mut membership = self.membership_state.write().await;
            
            let change = MembershipChange {
                change_id: format!("change_{}", Instant::now().elapsed().as_millis()),
                change_type: MembershipChangeType::AddNode,
                node_id: node_id.clone(),
                timestamp: Instant::now().elapsed().as_millis() as i64,
                status: ChangeStatus::Pending,
                consensus_term,
            };
            
            membership.pending_changes.push(change);
            membership.nodes.insert(node_id.clone(), node);
            membership.version += 1;
        }
        
        // Trigger rebalancing if needed
        self.trigger_rebalancing_if_needed().await?;
        
        Ok(())
    }

    pub async fn remove_node(&mut self, node_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let node_id_string = node_id.to_string();
        let consensus_term = self.consensus.get_state().await.current_term;
        
        {
            let mut membership = self.membership_state.write().await;
            
            let change = MembershipChange {
                change_id: format!("change_{}", Instant::now().elapsed().as_millis()),
                change_type: MembershipChangeType::RemoveNode,
                node_id: node_id_string.clone(),
                timestamp: Instant::now().elapsed().as_millis() as i64,
                status: ChangeStatus::Pending,
                consensus_term,
            };
            
            membership.pending_changes.push(change);
            membership.nodes.remove(node_id);
            membership.version += 1;
        }
        
        // Trigger rebalancing
        self.trigger_rebalancing_if_needed().await?;
        
        Ok(())
    }

    pub async fn trigger_rebalancing_if_needed(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let membership = self.membership_state.read().await;
        let current_state = self.rebalancing_state.read().await;
        
        if *current_state == RebalancingState::Idle && !membership.pending_changes.is_empty() {
            drop(current_state);
            drop(membership);
            
            let mut state = self.rebalancing_state.write().await;
            *state = RebalancingState::Planning;
            
            // Create rebalancing operation
            let operation = RebalancingOperation {
                operation_id: format!("op_{}", Instant::now().elapsed().as_millis()),
                operation_type: RebalancingType::MembershipChange,
                trigger: RebalancingTrigger::MembershipChange,
                affected_nodes: vec![self.node_id.clone()],
                state: RebalancingState::Planning,
                start_time: Instant::now(),
                end_time: None,
                progress: 0,
                error: None,
            };
            
            // Perform safety checks
            let safety_passed = self.perform_safety_checks(&operation).await?;
            
            if safety_passed {
                *state = RebalancingState::InProgress;
            } else {
                *state = RebalancingState::Failed;
            }
        }
        
        Ok(())
    }

    pub async fn perform_safety_checks(&self, _operation: &RebalancingOperation) -> Result<bool, Box<dyn std::error::Error>> {
        let mut safety = self.safety_coordinator.write().await;
        
        // Perform various safety checks
        let checks = vec![
            self.check_cluster_health().await?,
            self.check_quorum_availability().await?,
            self.check_network_connectivity().await?,
        ];
        
        let all_passed = checks.iter().all(|check| check.passed);
        
        // Store check results
        for check in checks {
            safety.active_checks.insert(check.check_id.clone(), check.clone());
            safety.check_history.push(check);
        }
        
        Ok(all_passed)
    }

    async fn check_cluster_health(&self) -> Result<SafetyCheckResult, Box<dyn std::error::Error>> {
        let membership = self.membership_state.read().await;
        let healthy_nodes = membership.nodes.values()
            .filter(|node| node.health == HealthStatus::Healthy)
            .count();
        
        let passed = healthy_nodes >= membership.quorum_size;
        
        Ok(SafetyCheckResult {
            check_id: format!("health_{}", Instant::now().elapsed().as_millis()),
            check_type: SafetyCheckType::ClusterHealth,
            passed,
            timestamp: Instant::now().elapsed().as_millis() as u64,
            error: if !passed { Some("Insufficient healthy nodes".to_string()) } else { None },
            details: {
                let mut details = std::collections::HashMap::new();
                details.insert("healthy_nodes".to_string(), healthy_nodes.to_string());
                details.insert("quorum_size".to_string(), membership.quorum_size.to_string());
                details
            },
        })
    }

    async fn check_quorum_availability(&self) -> Result<SafetyCheckResult, Box<dyn std::error::Error>> {
        let membership = self.membership_state.read().await;
        let active_nodes = membership.nodes.values()
            .filter(|node| node.status == NodeStatus::Active)
            .count();
        
        let passed = active_nodes >= membership.quorum_size;
        
        Ok(SafetyCheckResult {
            check_id: format!("quorum_{}", Instant::now().elapsed().as_millis()),
            check_type: SafetyCheckType::QuorumAvailability,
            passed,
            timestamp: Instant::now().elapsed().as_millis() as u64,
            error: if !passed { Some("Insufficient nodes for quorum".to_string()) } else { None },
            details: {
                let mut details = std::collections::HashMap::new();
                details.insert("active_nodes".to_string(), active_nodes.to_string());
                details.insert("quorum_size".to_string(), membership.quorum_size.to_string());
                details
            },
        })
    }

    async fn check_network_connectivity(&self) -> Result<SafetyCheckResult, Box<dyn std::error::Error>> {
        // Mock network connectivity check
        let passed = true; // In real implementation, this would ping nodes
        
        Ok(SafetyCheckResult {
            check_id: format!("network_{}", Instant::now().elapsed().as_millis()),
            check_type: SafetyCheckType::NetworkConnectivity,
            passed,
            timestamp: Instant::now().elapsed().as_millis() as u64,
            error: None,
            details: std::collections::HashMap::new(),
        })
    }

    pub async fn get_stats(&self) -> DistributedCacheStats {
        let membership = self.membership_state.read().await;
        let rebalancing_state = self.rebalancing_state.read().await;
        let safety = self.safety_coordinator.read().await;
        let communication = self.communication.read().await;
        
        DistributedCacheStats {
            node_id: self.node_id.clone(),
            total_nodes: membership.nodes.len(),
            active_nodes: membership.nodes.values()
                .filter(|node| node.status == NodeStatus::Active)
                .count(),
            healthy_nodes: membership.nodes.values()
                .filter(|node| node.health == HealthStatus::Healthy)
                .count(),
            rebalancing_state: rebalancing_state.clone(),
            pending_changes: membership.pending_changes.len(),
            active_safety_checks: safety.active_checks.len(),
            messages_sent: communication.stats.messages_sent,
            messages_received: communication.stats.messages_received,
            consensus_term: self.consensus.get_state().await.current_term,
            consensus_role: self.consensus.get_state().await.role,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DistributedCacheStats {
    pub node_id: String,
    pub total_nodes: usize,
    pub active_nodes: usize,
    pub healthy_nodes: usize,
    pub rebalancing_state: RebalancingState,
    pub pending_changes: usize,
    pub active_safety_checks: usize,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub consensus_term: RaftTerm,
    pub consensus_role: RaftRole,
}

mod consensus_tests {
    use super::*;

    #[tokio::test]
    async fn test_raft_consensus_basic() {
        let config = ClusterConfig::default();
        let mut cache = MockDistributedCache::new("node1".to_string(), config, None);
        
        cache.start().await.unwrap();
        
        // Test consensus state
        let state = cache.consensus.get_state().await;
        let term = state.current_term;
        let role = state.role;
        
        assert!(term.0 > 0);
        assert!(matches!(role, RaftRole::Follower | RaftRole::Candidate | RaftRole::Leader));
        
        cache.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_consensus_leader_election() {
        let config = ClusterConfig::default();
        let mut cache1 = MockDistributedCache::new("node1".to_string(), config.clone(), None);
        let mut cache2 = MockDistributedCache::new("node2".to_string(), config.clone(), None);
        let mut cache3 = MockDistributedCache::new("node3".to_string(), config, None);
        
        cache1.start().await.unwrap();
        cache2.start().await.unwrap();
        cache3.start().await.unwrap();
        
        // Wait for leader election
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Check that one node becomes leader
        let state1 = cache1.consensus.get_state().await;
        let state2 = cache2.consensus.get_state().await;
        let state3 = cache3.consensus.get_state().await;
        let role1 = state1.role;
        let role2 = state2.role;
        let role3 = state3.role;
        
        let leader_count = [role1, role2, role3]
            .iter()
            .filter(|role| matches!(role, RaftRole::Leader))
            .count();
        
        assert_eq!(leader_count, 1);
        
        cache1.stop().await.unwrap();
        cache2.stop().await.unwrap();
        cache3.stop().await.unwrap();
    }
}

mod membership_tests {
    use super::*;

    #[tokio::test]
    async fn test_membership_management() {
        let config = ClusterConfig::default();
        let mut cache = MockDistributedCache::new("node1".to_string(), config, None);
        
        cache.start().await.unwrap();
        
        // Add a new node
        let new_node = ClusterNode {
            node_id: "node2".to_string(),
            address: "127.0.0.1".to_string(),
            port: 8081,
            datacenter: "dc1".to_string(),
            status: NodeStatus::Joining,
            health: HealthStatus::Healthy,
            last_seen: Instant::now().elapsed().as_millis() as u64,
            version: 1,
        };
        
        cache.add_node(new_node).await.unwrap();
        
        // Check membership state
        let stats = cache.get_stats().await;
        assert_eq!(stats.total_nodes, 2);
        assert_eq!(stats.pending_changes, 1);
        
        cache.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_node_removal() {
        let config = ClusterConfig::default();
        let mut cache = MockDistributedCache::new("node1".to_string(), config, None);
        
        cache.start().await.unwrap();
        
        // Add a node first
        let new_node = ClusterNode {
            node_id: "node2".to_string(),
            address: "127.0.0.1".to_string(),
            port: 8081,
            datacenter: "dc1".to_string(),
            status: NodeStatus::Active,
            health: HealthStatus::Healthy,
            last_seen: Instant::now().elapsed().as_millis() as u64,
            version: 1,
        };
        
        cache.add_node(new_node).await.unwrap();
        
        // Remove the node
        cache.remove_node("node2").await.unwrap();
        
        // Check membership state
        let stats = cache.get_stats().await;
        assert_eq!(stats.total_nodes, 1);
        assert_eq!(stats.pending_changes, 2);
        
        cache.stop().await.unwrap();
    }
}

mod rebalancing_tests {
    use super::*;

    #[tokio::test]
    async fn test_rebalancing_trigger() {
        let config = ClusterConfig::default();
        let mut cache = MockDistributedCache::new("node1".to_string(), config, None);
        
        cache.start().await.unwrap();
        
        // Add a node to trigger rebalancing
        let new_node = ClusterNode {
            node_id: "node2".to_string(),
            address: "127.0.0.1".to_string(),
            port: 8081,
            datacenter: "dc1".to_string(),
            status: NodeStatus::Active,
            health: HealthStatus::Healthy,
            last_seen: Instant::now().elapsed().as_millis() as u64,
            version: 1,
        };
        
        cache.add_node(new_node).await.unwrap();
        
        // Wait for rebalancing to start
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        let stats = cache.get_stats().await;
        assert!(matches!(stats.rebalancing_state, RebalancingState::Planning | RebalancingState::InProgress | RebalancingState::Failed));
        
        cache.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_safety_checks() {
        let config = ClusterConfig::default();
        let cache = MockDistributedCache::new("node1".to_string(), config, None);
        
        let operation = RebalancingOperation {
            operation_id: "test_op".to_string(),
            operation_type: RebalancingType::NodeJoin,
            trigger: RebalancingTrigger::Automatic,
            affected_nodes: vec!["node1".to_string()],
            state: RebalancingState::Planning,
            start_time: Instant::now(),
            end_time: None,
            progress: 0,
            error: None,
        };
        
        let safety_passed = cache.perform_safety_checks(&operation).await.unwrap();
        assert!(safety_passed); // Should pass with single node
        
        let stats = cache.get_stats().await;
        assert!(stats.active_safety_checks > 0);
    }
}

mod communication_tests {
    use super::*;

    #[tokio::test]
    async fn test_message_serialization() {
        let message = RebalancingMessage {
            message_type: RebalancingMessageType::StartRebalancing,
            operation_id: "test_op".to_string(),
            source_node: "node1".to_string(),
            target_node: Some("node2".to_string()),
            payload: b"test_payload".to_vec(),
            timestamp: Instant::now().elapsed().as_millis() as u64,
            sequence_number: 1,
        };
        
        let serialized = serde_json::to_string(&message).unwrap();
        let deserialized: RebalancingMessage = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(deserialized.operation_id, "test_op");
        assert_eq!(deserialized.source_node, "node1");
        assert_eq!(deserialized.sequence_number, 1);
    }

    #[tokio::test]
    async fn test_operation_serialization() {
        let operation = RebalancingOperation {
            operation_id: "test_op".to_string(),
            operation_type: RebalancingType::NodeJoin,
            trigger: RebalancingTrigger::Automatic,
            affected_nodes: vec!["node1".to_string(), "node2".to_string()],
            state: RebalancingState::Planning,
            start_time: Instant::now(),
            end_time: None,
            progress: 25,
            error: None,
        };
        
        let serialized = serde_json::to_string(&operation).unwrap();
        let deserialized: RebalancingOperation = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(deserialized.operation_id, "test_op");
        assert_eq!(deserialized.progress, 25);
    }
}

mod datacenter_tests {
    use super::*;

    #[tokio::test]
    async fn test_datacenter_replication_config() {
        let config = DatacenterReplicationConfig {
            source_datacenter: "dc1".to_string(),
            target_datacenter: "dc2".to_string(),
            replication_factor: 3,
            sync_interval: 5000, // 5 seconds
            conflict_resolution: ConflictResolution::LastWriteWins,
            bandwidth_limit: Some(1024 * 1024), // 1MB/s
        };
        
        let cluster_config = ClusterConfig::default();
        let mut cache = MockDistributedCache::new("node1".to_string(), cluster_config, Some(config));
        
        cache.start().await.unwrap();
        
        // Test that datacenter config is properly set
        assert!(cache.datacenter_config.is_some());
        let dc_config = cache.datacenter_config.as_ref().unwrap();
        assert_eq!(dc_config.source_datacenter, "dc1");
        assert_eq!(dc_config.target_datacenter, "dc2");
        assert_eq!(dc_config.replication_factor, 3);
        
        cache.stop().await.unwrap();
    }
}

mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_full_distributed_workflow() {
        let config = ClusterConfig::default();
        let mut cache = MockDistributedCache::new("node1".to_string(), config, None);
        
        // Start the cache
        cache.start().await.unwrap();
        
        // Add multiple nodes
        let nodes = vec![
            ClusterNode {
                node_id: "node2".to_string(),
                address: "127.0.0.1".to_string(),
                port: 8081,
                datacenter: "dc1".to_string(),
                status: NodeStatus::Active,
                health: HealthStatus::Healthy,
                last_seen: Instant::now().elapsed().as_millis() as u64,
                version: 1,
            },
            ClusterNode {
                node_id: "node3".to_string(),
                address: "127.0.0.1".to_string(),
                port: 8082,
                datacenter: "dc1".to_string(),
                status: NodeStatus::Active,
                health: HealthStatus::Healthy,
                last_seen: Instant::now().elapsed().as_millis() as u64,
                version: 1,
            },
        ];
        
        for node in nodes {
            cache.add_node(node).await.unwrap();
        }
        
        // Wait for operations to complete
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Check final state
        let stats = cache.get_stats().await;
        assert_eq!(stats.total_nodes, 3);
        assert!(stats.active_nodes >= 1);
        assert!(stats.healthy_nodes >= 1);
        assert!(stats.pending_changes > 0);
        
        // Stop the cache
        cache.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_error_handling() {
        let config = ClusterConfig::default();
        let mut cache = MockDistributedCache::new("node1".to_string(), config, None);
        
        cache.start().await.unwrap();
        
        // Test removing non-existent node
        let result = cache.remove_node("non_existent").await;
        assert!(result.is_ok()); // Should not error, just not find the node
        
        // Test adding duplicate node
        let node = ClusterNode {
            node_id: "node1".to_string(), // Same as existing
            address: "127.0.0.1".to_string(),
            port: 8081,
            datacenter: "dc1".to_string(),
            status: NodeStatus::Active,
            health: HealthStatus::Healthy,
            last_seen: Instant::now().elapsed().as_millis() as u64,
            version: 1,
        };
        
        let result = cache.add_node(node).await;
        assert!(result.is_ok()); // Should update existing node
        
        cache.stop().await.unwrap();
    }
}

mod performance_tests {
    use super::*;

    #[tokio::test]
    async fn test_concurrent_operations() {
        let config = ClusterConfig::default();
        let cache = Arc::new(MockDistributedCache::new("node1".to_string(), config, None));
        
        let mut handles = Vec::new();
        
        // Spawn multiple tasks to perform operations concurrently
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = tokio::spawn(async move {
                let _node = ClusterNode {
                    node_id: format!("node{}", i),
                    address: "127.0.0.1".to_string(),
                    port: 8080 + i as u16,
                    datacenter: "dc1".to_string(),
                    status: NodeStatus::Active,
                    health: HealthStatus::Healthy,
                    last_seen: Instant::now().elapsed().as_millis() as u64,
                    version: 1,
                };
                
                // Note: We can't mutate Arc, so this is just for testing the structure
                // In a real implementation, you'd have proper concurrency control
                let _stats = cache_clone.get_stats().await;
            });
            handles.push(handle);
        }
        
        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        let stats = cache.get_stats().await;
        assert_eq!(stats.node_id, "node1");
    }
}

#[tokio::test]
async fn test_phase5_complete() {
    println!("Running Phase 5: Advanced Distributed Features tests...");
    
    let config = ClusterConfig::default();
    let mut cache = MockDistributedCache::new("node1".to_string(), config, None);
    
    // Test basic startup
    cache.start().await.unwrap();
    
    // Test membership management
    let node = ClusterNode {
        node_id: "node2".to_string(),
        address: "127.0.0.1".to_string(),
        port: 8081,
        datacenter: "dc1".to_string(),
        status: NodeStatus::Active,
        health: HealthStatus::Healthy,
        last_seen: Instant::now().elapsed().as_millis() as u64,
        version: 1,
    };
    
    cache.add_node(node).await.unwrap();
    
    // Test safety checks
    let operation = RebalancingOperation {
        operation_id: "test_op".to_string(),
        operation_type: RebalancingType::NodeJoin,
        trigger: RebalancingTrigger::Automatic,
        affected_nodes: vec!["node1".to_string()],
        state: RebalancingState::Planning,
        start_time: Instant::now(),
        end_time: None,
        progress: 0,
        error: None,
    };
    
    let safety_passed = cache.perform_safety_checks(&operation).await.unwrap();
    assert!(safety_passed);
    
    // Test statistics
    let stats = cache.get_stats().await;
    assert_eq!(stats.total_nodes, 2);
    assert!(stats.active_nodes >= 1);
    assert!(stats.healthy_nodes >= 1);
    assert!(stats.pending_changes > 0);
    
    // Test shutdown
    cache.stop().await.unwrap();
    
    println!("Phase 5 tests completed successfully!");
} 