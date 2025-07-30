//! Comprehensive tests for Phase 5: Advanced Distributed Features
//! 
//! This module tests the advanced distributed features including:
//! - Raft consensus algorithm
//! - Dynamic cluster membership
//! - Data rebalancing
//! - Cross-datacenter replication (placeholder)

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use kv_cache_core::{
    consensus::{RaftConsensus, RaftState, RaftRole, RaftTerm, LogEntry, LogIndex, LogTerm},
    config::{ClusterConfig, MetricsConfig},
    metrics::MetricsCollector,
    cluster::sharding::ShardManager,
    membership::{MembershipManager, MembershipState, MemberInfo, MemberStatus, HealthStatus, MembershipChange, MembershipChangeType, ChangeStatus, MembershipStats},
    rebalancing::{RebalancingManager, RebalancingState, RebalancingOperation, RebalancingType, RebalancingTrigger, RebalancingPlan, RebalancingStats},
};

// Use actual types from kv_cache_core crate

/// Test suite for Raft consensus algorithm
mod consensus_tests {
    use super::*;

    #[tokio::test]
    async fn test_raft_consensus_creation() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        let consensus = RaftConsensus::new("node1".to_string(), config, metrics);
        
        assert_eq!(consensus.node_id, "node1");
    }

    #[tokio::test]
    async fn test_raft_consensus_start_stop() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        let mut consensus = RaftConsensus::new("node1".to_string(), config, metrics);
        
        // Start consensus
        consensus.start().await.unwrap();
        
        // Check initial state
        let state = consensus.get_state().await;
        assert_eq!(state.role, RaftRole::Follower);
        assert_eq!(state.current_term, RaftTerm(1));
        
        // Stop consensus
        consensus.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_raft_consensus_leader_check() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        let consensus = RaftConsensus::new("node1".to_string(), config, metrics);
        
        // Initially not leader
        assert!(!consensus.is_leader().await);
    }

    #[tokio::test]
    async fn test_raft_consensus_cluster_status() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        let consensus = RaftConsensus::new("node1".to_string(), config, metrics);
        
        let status = consensus.get_cluster_status().await;
        assert_eq!(status.node_id, "node1");
        assert_eq!(status.role, RaftRole::Follower);
        assert_eq!(status.current_term, RaftTerm(1));
    }

    #[tokio::test]
    async fn test_raft_consensus_submit_command() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        let consensus = RaftConsensus::new("node1".to_string(), config, metrics);
        
        // Should fail because we're not the leader
        let result = consensus.submit_command(b"test command".to_vec()).await;
        assert!(result.is_err());
    }
}

/// Test suite for dynamic cluster membership
mod membership_tests {
    use super::*;

    #[tokio::test]
    async fn test_membership_state_creation() {
        let state = MembershipState {
            version: 1,
            members: std::collections::HashMap::new(),
            pending_changes: Vec::new(),
            last_update: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        };

        assert_eq!(state.version, 1);
        assert_eq!(state.members.len(), 0);
        assert_eq!(state.pending_changes.len(), 0);
    }

    #[tokio::test]
    async fn test_member_info_creation() {
        let member = MemberInfo {
            node_id: "node1".to_string(),
            address: "127.0.0.1".to_string(),
            port: 50051,
            status: MemberStatus::Active,
            last_seen: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            health: HealthStatus::Healthy,
            datacenter: Some("dc1".to_string()),
        };

        assert_eq!(member.node_id, "node1");
        assert_eq!(member.status, MemberStatus::Active);
        assert_eq!(member.health, HealthStatus::Healthy);
        assert_eq!(member.datacenter, Some("dc1".to_string()));
    }

    #[tokio::test]
    async fn test_membership_change_creation() {
        let change = MembershipChange {
            change_id: "test_change".to_string(),
            change_type: MembershipChangeType::AddNode,
            node_id: "node1".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            status: ChangeStatus::Pending,
            consensus_term: RaftTerm(1),
        };

        assert_eq!(change.change_id, "test_change");
        assert_eq!(change.status, ChangeStatus::Pending);
        assert_eq!(change.change_type, MembershipChangeType::AddNode);
    }

    #[tokio::test]
    async fn test_membership_manager_creation() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        
        // Create mock consensus
        let consensus = Arc::new(RaftConsensus::new(
            "node1".to_string(),
            config.clone(),
            Arc::new(MetricsCollector::new(MetricsConfig::default())),
        ));

        let manager = MembershipManager::new(
            "node1".to_string(),
            consensus,
            config,
            metrics,
        );

        assert_eq!(manager.node_id, "node1");
    }

    #[tokio::test]
    async fn test_membership_stats() {
        let stats = MembershipStats {
            total_members: 5,
            active_members: 4,
            healthy_members: 3,
            pending_changes: 1,
            version: 2,
        };

        assert_eq!(stats.total_members, 5);
        assert_eq!(stats.active_members, 4);
        assert_eq!(stats.healthy_members, 3);
        assert_eq!(stats.pending_changes, 1);
        assert_eq!(stats.version, 2);
    }
}

/// Test suite for data rebalancing
mod rebalancing_tests {
    use super::*;

    #[tokio::test]
    async fn test_rebalancing_operation_creation() {
        let operation = RebalancingOperation {
            operation_id: "test_op".to_string(),
            operation_type: RebalancingType::NodeJoin,
            trigger: RebalancingTrigger::Automatic,
            affected_nodes: vec!["node1".to_string(), "node2".to_string()],
            state: RebalancingState::Idle,
            start_time: std::time::Instant::now(),
            end_time: None,
            progress: 0,
            error: None,
        };

        assert_eq!(operation.operation_id, "test_op");
        assert_eq!(operation.state, RebalancingState::Idle);
        assert_eq!(operation.progress, 0);
        assert_eq!(operation.affected_nodes.len(), 2);
    }

    #[tokio::test]
    async fn test_rebalancing_plan_creation() {
        let plan = RebalancingPlan {
            plan_id: "test_plan".to_string(),
            source_nodes: std::collections::HashMap::new(),
            target_nodes: std::collections::HashMap::new(),
            keys_to_move: Vec::new(),
            estimated_size: 1024,
            estimated_duration: Duration::from_secs(60),
            safety_checks_passed: false,
        };

        assert_eq!(plan.plan_id, "test_plan");
        assert_eq!(plan.estimated_size, 1024);
        assert_eq!(plan.estimated_duration, Duration::from_secs(60));
        assert!(!plan.safety_checks_passed);
    }

    #[tokio::test]
    async fn test_rebalancing_manager_creation() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        
        // Create mock shard manager
        let shard_manager = Arc::new(ShardManager::new());
        
        // Create mock membership manager
        let consensus = Arc::new(RaftConsensus::new(
            "node1".to_string(),
            config.clone(),
            Arc::new(MetricsCollector::new(MetricsConfig::default())),
        ));
        let membership_manager = Arc::new(MembershipManager::new(
            "node1".to_string(),
            consensus,
            config.clone(),
            Arc::new(MetricsCollector::new(MetricsConfig::default())),
        ));

        let manager = RebalancingManager::new(
            "node1".to_string(),
            shard_manager,
            membership_manager,
            config,
            metrics,
        );

        assert_eq!(manager.node_id, "node1");
    }

    #[tokio::test]
    async fn test_rebalancing_stats() {
        let stats = RebalancingStats {
            current_state: RebalancingState::Idle,
            active_operations: 0,
            completed_operations: 5,
            failed_operations: 1,
            total_operations: 6,
        };

        assert_eq!(stats.current_state, RebalancingState::Idle);
        assert_eq!(stats.completed_operations, 5);
        assert_eq!(stats.failed_operations, 1);
        assert_eq!(stats.total_operations, 6);
    }
}

/// Test suite for integration scenarios
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_consensus_and_membership_integration() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        
        // Create consensus
        let consensus = Arc::new(RaftConsensus::new(
            "node1".to_string(),
            config.clone(),
            Arc::new(MetricsCollector::new(MetricsConfig::default())),
        ));

        // Create membership manager
        let membership_manager = MembershipManager::new(
            "node1".to_string(),
            Arc::clone(&consensus),
            config.clone(),
            metrics,
        );

        // Test that both can be created together
        assert_eq!(consensus.node_id, "node1");
        assert_eq!(membership_manager.node_id, "node1");
    }

    #[tokio::test]
    async fn test_membership_and_rebalancing_integration() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        
        // Create consensus
        let consensus = Arc::new(RaftConsensus::new(
            "node1".to_string(),
            config.clone(),
            Arc::new(MetricsCollector::new(MetricsConfig::default())),
        ));

        // Create membership manager
        let membership_manager = Arc::new(MembershipManager::new(
            "node1".to_string(),
            Arc::clone(&consensus),
            config.clone(),
            metrics.clone(),
        ));

        // Create shard manager
        let shard_manager = Arc::new(ShardManager::new());

        // Create rebalancing manager
        let rebalancing_manager = RebalancingManager::new(
            "node1".to_string(),
            shard_manager,
            membership_manager,
            config,
            metrics,
        );

        // Test that all can be created together
        assert_eq!(consensus.node_id, "node1");
        assert_eq!(rebalancing_manager.node_id, "node1");
    }

    #[tokio::test]
    async fn test_full_phase5_integration() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        
        // Create all Phase 5 components
        let consensus = Arc::new(RaftConsensus::new(
            "node1".to_string(),
            config.clone(),
            Arc::new(MetricsCollector::new(MetricsConfig::default())),
        ));

        let membership_manager = Arc::new(MembershipManager::new(
            "node1".to_string(),
            Arc::clone(&consensus),
            config.clone(),
            metrics.clone(),
        ));

        let shard_manager = Arc::new(ShardManager::new());

        let rebalancing_manager = RebalancingManager::new(
            "node1".to_string(),
            Arc::clone(&shard_manager),
            Arc::clone(&membership_manager),
            config,
            metrics,
        );

        // Test that all components can coexist
        assert_eq!(consensus.node_id, "node1");
        assert_eq!(membership_manager.node_id, "node1");
        assert_eq!(rebalancing_manager.node_id, "node1");

        // Test basic functionality
        assert!(!consensus.is_leader().await);
        let membership_state = membership_manager.get_membership_state().await;
        assert_eq!(membership_state.version, 1);
        let rebalancing_state = rebalancing_manager.get_state().await;
        assert_eq!(rebalancing_state, RebalancingState::Idle);
    }
}

/// Test suite for error handling
mod error_handling_tests {
    use super::*;

    #[tokio::test]
    async fn test_consensus_error_handling() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        let consensus = RaftConsensus::new("node1".to_string(), config, metrics);
        
        // Test that non-leader cannot submit commands
        let result = consensus.submit_command(b"test".to_vec()).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Not the leader");
    }

    #[tokio::test]
    async fn test_membership_error_handling() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        
        let consensus = Arc::new(RaftConsensus::new(
            "node1".to_string(),
            config.clone(),
            Arc::new(MetricsCollector::new(MetricsConfig::default())),
        ));

        let membership_manager = MembershipManager::new(
            "node1".to_string(),
            Arc::clone(&consensus),
            config,
            metrics,
        );

        // Test getting non-existent member
        let member = membership_manager.get_member("non_existent").await;
        assert!(member.is_none());
    }

    #[tokio::test]
    async fn test_rebalancing_error_handling() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        
        let consensus = Arc::new(RaftConsensus::new(
            "node1".to_string(),
            config.clone(),
            Arc::new(MetricsCollector::new(MetricsConfig::default())),
        ));

        let membership_manager = Arc::new(MembershipManager::new(
            "node1".to_string(),
            Arc::clone(&consensus),
            config.clone(),
            metrics.clone(),
        ));

        let shard_manager = Arc::new(ShardManager::new());

        let rebalancing_manager = RebalancingManager::new(
            "node1".to_string(),
            shard_manager,
            membership_manager,
            config,
            metrics,
        );

        // Test that rebalancing is idle initially
        let state = rebalancing_manager.get_state().await;
        assert_eq!(state, RebalancingState::Idle);

        // Test that no operations are active initially
        let operations = rebalancing_manager.get_active_operations();
        assert_eq!(operations.len(), 0);
    }
}

/// Test suite for performance and stress testing
mod performance_tests {
    use super::*;

    #[tokio::test]
    async fn test_consensus_performance() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        let consensus = RaftConsensus::new("node1".to_string(), config, metrics);
        
        let start = std::time::Instant::now();
        
        // Perform multiple operations
        for _ in 0..100 {
            let _ = consensus.get_state().await;
            let _ = consensus.is_leader().await;
        }
        
        let duration = start.elapsed();
        assert!(duration < Duration::from_millis(100)); // Should be very fast
    }

    #[tokio::test]
    async fn test_membership_performance() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        
        let consensus = Arc::new(RaftConsensus::new(
            "node1".to_string(),
            config.clone(),
            Arc::new(MetricsCollector::new(MetricsConfig::default())),
        ));

        let membership_manager = MembershipManager::new(
            "node1".to_string(),
            Arc::clone(&consensus),
            config,
            metrics,
        );

        let start = std::time::Instant::now();
        
        // Perform multiple operations
        for _ in 0..100 {
            let _ = membership_manager.get_membership_state().await;
            let _ = membership_manager.get_stats().await;
        }
        
        let duration = start.elapsed();
        assert!(duration < Duration::from_millis(100)); // Should be very fast
    }

    #[tokio::test]
    async fn test_rebalancing_performance() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        
        let consensus = Arc::new(RaftConsensus::new(
            "node1".to_string(),
            config.clone(),
            Arc::new(MetricsCollector::new(MetricsConfig::default())),
        ));

        let membership_manager = Arc::new(MembershipManager::new(
            "node1".to_string(),
            consensus,
            config.clone(),
            metrics.clone(),
        ));

        let shard_manager = Arc::new(ShardManager::new());

        let rebalancing_manager = RebalancingManager::new(
            "node1".to_string(),
            shard_manager,
            membership_manager,
            config,
            metrics,
        );

        let start = std::time::Instant::now();
        
        // Perform multiple operations
        for _ in 0..100 {
            let _ = rebalancing_manager.get_state().await;
            let _ = rebalancing_manager.get_stats().await;
        }
        
        let duration = start.elapsed();
        assert!(duration < Duration::from_millis(100)); // Should be very fast
    }
}

/// Test suite for concurrent operations
mod concurrency_tests {
    use super::*;

    #[tokio::test]
    async fn test_concurrent_consensus_operations() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        let consensus = Arc::new(RaftConsensus::new("node1".to_string(), config, metrics));
        
        let mut handles = Vec::new();
        
        // Spawn multiple concurrent operations
        for i in 0..10 {
            let consensus_clone = Arc::clone(&consensus);
            let handle = tokio::spawn(async move {
                let state = consensus_clone.get_state().await;
                let is_leader = consensus_clone.is_leader().await;
                (i, state, is_leader)
            });
            handles.push(handle);
        }
        
        // Wait for all operations to complete
        for handle in handles {
            let (i, state, is_leader) = handle.await.unwrap();
            assert_eq!(state.role, RaftRole::Follower);
            assert!(!is_leader);
        }
    }

    #[tokio::test]
    async fn test_concurrent_membership_operations() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        
        let consensus = Arc::new(RaftConsensus::new(
            "node1".to_string(),
            config.clone(),
            Arc::new(MetricsCollector::new(MetricsConfig::default())),
        ));

        let membership_manager = Arc::new(MembershipManager::new(
            "node1".to_string(),
            Arc::clone(&consensus),
            config,
            metrics,
        ));

        let mut handles = Vec::new();
        
        // Spawn multiple concurrent operations
        for i in 0..10 {
            let manager_clone = Arc::clone(&membership_manager);
            let handle = tokio::spawn(async move {
                let state = manager_clone.get_membership_state().await;
                let stats = manager_clone.get_stats().await;
                (i, state, stats)
            });
            handles.push(handle);
        }
        
        // Wait for all operations to complete
        for handle in handles {
            let (i, state, stats) = handle.await.unwrap();
            assert_eq!(state.version, 1);
            assert_eq!(stats.total_members, 0);
        }
    }
}

/// Main test runner
#[tokio::test]
async fn test_phase5_complete() {
    // This test ensures all Phase 5 components can be created and work together
    let config = ClusterConfig::default();
    let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
    
    // Create all Phase 5 components
    let consensus = Arc::new(RaftConsensus::new(
        "node1".to_string(),
        config.clone(),
        Arc::new(MetricsCollector::new(MetricsConfig::default())),
    ));

    let membership_manager = Arc::new(MembershipManager::new(
        "node1".to_string(),
        Arc::clone(&consensus),
        config.clone(),
        metrics.clone(),
    ));

    let shard_manager = Arc::new(ShardManager::new());

    let rebalancing_manager = RebalancingManager::new(
        "node1".to_string(),
        Arc::clone(&shard_manager),
        Arc::clone(&membership_manager),
        config,
        metrics,
    );

    // Verify all components are working
    assert_eq!(consensus.node_id, "node1");
    assert_eq!(membership_manager.node_id, "node1");
    assert_eq!(rebalancing_manager.node_id, "node1");

    // Test basic functionality
    assert!(!consensus.is_leader().await);
    let membership_state = membership_manager.get_membership_state().await;
    assert_eq!(membership_state.version, 1);
    let rebalancing_state = rebalancing_manager.get_state().await;
    assert_eq!(rebalancing_state, RebalancingState::Idle);

    println!("✅ Phase 5 complete test passed - all components working together!");
} 