//! Cluster module for distributed key-value cache functionality
//! 
//! This module provides the foundation for Phase 3: Distributed System Foundation
//! including inter-node communication, membership management, sharding, and replication.

pub mod communication;
pub mod membership;
pub mod sharding;
pub mod replication;

// Re-export main types for easier access
pub use communication::{KvCacheService, KvCacheClient};
pub use communication::kv_cache::{NodeInfo, NodeStatus};
pub use membership::{ClusterMembership, MembershipStatus};
pub use sharding::{ConsistentHashRing, ShardManager, ShardStatus};
pub use replication::{ReplicationManager, ReplicationStrategy, ReplicationStatus};

/// Cluster configuration
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub node_id: String,
    pub bind_address: String,
    pub bind_port: u16,
    pub cluster_members: Vec<NodeInfo>,
    pub replication_factor: usize,
    pub heartbeat_interval_ms: u64,
    pub failure_timeout_ms: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: "node-1".to_string(),
            bind_address: "127.0.0.1".to_string(),
            bind_port: 50051, // Default gRPC port
            cluster_members: Vec::new(),
            replication_factor: 2,
            heartbeat_interval_ms: 1000,
            failure_timeout_ms: 5000,
        }
    }
}

/// Main cluster manager that coordinates all distributed functionality
pub struct ClusterManager {
    config: ClusterConfig,
    membership: ClusterMembership,
    shard_manager: ShardManager,
    replication_manager: ReplicationManager,
    communication_service: KvCacheService,
}

impl ClusterManager {
    pub fn new(config: ClusterConfig) -> Result<Self, String> {
        let membership = ClusterMembership::new(config.node_id.clone());
        let shard_manager = ShardManager::new();
        let replication_manager = ReplicationManager::new(config.replication_factor);
        let communication_service = KvCacheService::new();

        Ok(Self {
            config,
            membership,
            shard_manager,
            replication_manager,
            communication_service,
        })
    }

    /// Start the cluster manager and all its components
    pub async fn start(&mut self) -> Result<(), String> {
        // Start membership management
        self.membership.start().await.map_err(|e| e.to_string())?;
        
        // Start shard management
        self.shard_manager.start().await.map_err(|e| e.to_string())?;
        
        // Start replication management
        self.replication_manager.start().await.map_err(|e| e.to_string())?;
        
        // Start gRPC communication service
        self.communication_service.start(&self.config.bind_address, self.config.bind_port).await.map_err(|e| e.to_string())?;
        
        Ok(())
    }

    /// Stop the cluster manager and all its components
    pub async fn stop(&mut self) -> Result<(), String> {
        // Stop all components gracefully
        self.communication_service.stop().await.map_err(|e| e.to_string())?;
        self.replication_manager.stop().await.map_err(|e| e.to_string())?;
        self.shard_manager.stop().await.map_err(|e| e.to_string())?;
        self.membership.stop().await.map_err(|e| e.to_string())?;
        
        Ok(())
    }

    /// Get cluster status information
    pub fn get_status(&self) -> ClusterStatus {
        ClusterStatus {
            node_id: self.config.node_id.clone(),
            membership_status: self.membership.get_status(),
            shard_status: self.shard_manager.get_status(),
            replication_status: self.replication_manager.get_status(),
        }
    }
}

/// Status information about the cluster
#[derive(Debug, Clone)]
pub struct ClusterStatus {
    pub node_id: String,
    pub membership_status: MembershipStatus,
    pub shard_status: ShardStatus,
    pub replication_status: ReplicationStatus,
} 