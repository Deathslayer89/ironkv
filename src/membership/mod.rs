//! Dynamic cluster membership management for Phase 5
//! 
//! This module provides advanced membership management with dynamic node
//! discovery, health checking, and membership change coordination.

pub mod discovery;
pub mod coordinator;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

use crate::consensus::{RaftConsensus, RaftState, RaftTerm};
use crate::config::ClusterConfig;
use crate::log::log_cluster_operation;
use crate::metrics::MetricsCollector;

/// Cluster membership state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipState {
    /// Current cluster version
    pub version: u64,
    /// All cluster members
    pub members: HashMap<String, MemberInfo>,
    /// Pending membership changes
    pub pending_changes: Vec<MembershipChange>,
    /// Last membership update timestamp
    pub last_update: i64,
}

/// Member information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    /// Node ID
    pub node_id: String,
    /// Node address
    pub address: String,
    /// Node port
    pub port: u16,
    /// Member status
    pub status: MemberStatus,
    /// Last seen timestamp
    pub last_seen: i64,
    /// Health status
    pub health: HealthStatus,
    /// Datacenter (for multi-DC setups)
    pub datacenter: Option<String>,
}

/// Member status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MemberStatus {
    /// Node is active and healthy
    Active,
    /// Node is joining the cluster
    Joining,
    /// Node is leaving the cluster
    Leaving,
    /// Node has failed
    Failed,
    /// Node is suspended
    Suspended,
}

/// Health status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HealthStatus {
    /// Node is healthy
    Healthy,
    /// Node is degraded
    Degraded,
    /// Node is unhealthy
    Unhealthy,
    /// Health status unknown
    Unknown,
}

/// Membership change operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipChange {
    /// Change ID
    pub change_id: String,
    /// Change type
    pub change_type: MembershipChangeType,
    /// Target node ID
    pub node_id: String,
    /// Change timestamp
    pub timestamp: i64,
    /// Change status
    pub status: ChangeStatus,
    /// Consensus term when change was proposed
    pub consensus_term: RaftTerm,
}

/// Membership change type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MembershipChangeType {
    /// Add a new node
    AddNode,
    /// Remove a node
    RemoveNode,
    /// Update node information
    UpdateNode,
    /// Suspend a node
    SuspendNode,
    /// Resume a suspended node
    ResumeNode,
}

/// Change status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChangeStatus {
    /// Change is pending
    Pending,
    /// Change is in progress
    InProgress,
    /// Change completed successfully
    Completed,
    /// Change failed
    Failed,
    /// Change was rolled back
    RolledBack,
}

/// Main membership manager
pub struct MembershipManager {
    /// Current membership state
    state: Arc<RwLock<MembershipState>>,
    /// Raft consensus for coordination
    consensus: Arc<RaftConsensus>,
    /// Configuration
    config: ClusterConfig,
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
    /// Node ID
    pub node_id: String,
    /// Discovery coordinator
    discovery: discovery::DiscoveryCoordinator,
    /// Change coordinator
    coordinator: coordinator::ChangeCoordinator,
}

impl MembershipManager {
    /// Create a new membership manager
    pub fn new(
        node_id: String,
        consensus: Arc<RaftConsensus>,
        config: ClusterConfig,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        let state = Arc::new(RwLock::new(MembershipState {
            version: 1,
            members: HashMap::new(),
            pending_changes: Vec::new(),
            last_update: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        }));

        let discovery = discovery::DiscoveryCoordinator::new(
            node_id.clone(),
            Arc::clone(&state),
            config.clone(),
        );

        let coordinator = coordinator::ChangeCoordinator::new(
            node_id.clone(),
            Arc::clone(&state),
            Arc::clone(&consensus),
            config.clone(),
        );

        Self {
            state,
            consensus,
            config,
            metrics,
            node_id,
            discovery,
            coordinator,
        }
    }

    /// Start the membership manager
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Starting membership manager for node: {}", self.node_id);

        // Start discovery coordinator
        self.discovery.start().await?;

        // Start change coordinator
        self.coordinator.start().await?;

        tracing::info!("Membership manager started successfully");
        Ok(())
    }

    /// Stop the membership manager
    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Stopping membership manager for node: {}", self.node_id);

        // Stop discovery coordinator
        self.discovery.stop().await?;

        // Stop change coordinator
        self.coordinator.stop().await?;

        tracing::info!("Membership manager stopped successfully");
        Ok(())
    }

    /// Add a new node to the cluster
    pub async fn add_node(
        &self,
        node_id: String,
        address: String,
        port: u16,
        datacenter: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = SystemTime::now();

        let change = MembershipChange {
            change_id: format!("add_{}_{}", node_id, start_time.duration_since(UNIX_EPOCH).unwrap().as_millis()),
            change_type: MembershipChangeType::AddNode,
            node_id: node_id.clone(),
            timestamp: start_time.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            status: ChangeStatus::Pending,
            consensus_term: self.consensus.get_state().await.current_term,
        };

        let result = self.coordinator.propose_change(change).await;

        // Record metrics
        let duration = SystemTime::now().duration_since(start_time).unwrap_or_default();
        self.metrics.record_cluster_operation("membership_add_node", result.is_ok(), duration);

        // Log operation
        log_cluster_operation(
            "membership_add_node",
            &self.node_id,
            result.is_ok(),
            duration,
            Some(vec![("node_id", node_id.clone())]),
        );

        result
    }

    /// Remove a node from the cluster
    pub async fn remove_node(
        &self,
        node_id: String,
        graceful: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = SystemTime::now();

        let change = MembershipChange {
            change_id: format!("remove_{}_{}", node_id, start_time.duration_since(UNIX_EPOCH).unwrap().as_millis()),
            change_type: MembershipChangeType::RemoveNode,
            node_id: node_id.clone(),
            timestamp: start_time.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            status: ChangeStatus::Pending,
            consensus_term: self.consensus.get_state().await.current_term,
        };

        let result = self.coordinator.propose_change(change).await;

        // Record metrics
        let duration = SystemTime::now().duration_since(start_time).unwrap_or_default();
        self.metrics.record_cluster_operation("membership_remove_node", result.is_ok(), duration);

        // Log operation
        log_cluster_operation(
            "membership_remove_node",
            &self.node_id,
            result.is_ok(),
            duration,
            Some(vec![
                ("node_id", node_id.clone()),
                ("graceful", graceful.to_string()),
            ]),
        );

        result
    }

    /// Get current membership state
    pub async fn get_membership_state(&self) -> MembershipState {
        self.state.read().await.clone()
    }

    /// Get member information
    pub async fn get_member(&self, node_id: &str) -> Option<MemberInfo> {
        let state = self.state.read().await;
        state.members.get(node_id).cloned()
    }

    /// Get all active members
    pub async fn get_active_members(&self) -> Vec<MemberInfo> {
        let state = self.state.read().await;
        state
            .members
            .values()
            .filter(|member| member.status == MemberStatus::Active)
            .cloned()
            .collect()
    }

    /// Get membership statistics
    pub async fn get_stats(&self) -> MembershipStats {
        let state = self.state.read().await;
        let total = state.members.len();
        let active = state
            .members
            .values()
            .filter(|m| m.status == MemberStatus::Active)
            .count();
        let healthy = state
            .members
            .values()
            .filter(|m| m.health == HealthStatus::Healthy)
            .count();

        MembershipStats {
            total_members: total,
            active_members: active,
            healthy_members: healthy,
            pending_changes: state.pending_changes.len(),
            version: state.version,
        }
    }
}

/// Membership statistics
#[derive(Debug, Clone)]
pub struct MembershipStats {
    pub total_members: usize,
    pub active_members: usize,
    pub healthy_members: usize,
    pub pending_changes: usize,
    pub version: u64,
}

impl std::fmt::Display for MembershipStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MembershipStats(total={}, active={}, healthy={}, pending={}, version={})",
            self.total_members,
            self.active_members,
            self.healthy_members,
            self.pending_changes,
            self.version
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;

    #[tokio::test]
    async fn test_membership_state_creation() {
        let state = MembershipState {
            version: 1,
            members: HashMap::new(),
            pending_changes: Vec::new(),
            last_update: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
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
            last_seen: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            health: HealthStatus::Healthy,
            datacenter: Some("dc1".to_string()),
        };

        assert_eq!(member.node_id, "node1");
        assert_eq!(member.status, MemberStatus::Active);
        assert_eq!(member.health, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_membership_change_creation() {
        let change = MembershipChange {
            change_id: "test_change".to_string(),
            change_type: MembershipChangeType::AddNode,
            node_id: "node1".to_string(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            status: ChangeStatus::Pending,
            consensus_term: RaftTerm(1),
        };

        assert_eq!(change.change_id, "test_change");
        assert_eq!(change.status, ChangeStatus::Pending);
    }
} 