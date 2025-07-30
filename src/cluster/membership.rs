//! Cluster membership management
//! 
//! This module handles node discovery, heartbeats, and cluster membership
//! tracking for the distributed cache system.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::interval;
use crate::cluster::communication::{KvCacheClient, kv_cache::{NodeInfo, NodeStatus}};

/// Cluster membership manager
pub struct ClusterMembership {
    node_id: String,
    members: Arc<RwLock<HashMap<String, MemberInfo>>>,
    heartbeat_interval: Duration,
    failure_timeout: Duration,
    membership_task: Option<tokio::task::JoinHandle<()>>,
}

/// Extended member information with health tracking
#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub node_info: NodeInfo,
    pub last_heartbeat: Instant,
    pub health_status: HealthStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Suspicious,
    Failed,
}

/// Status information about membership
#[derive(Debug, Clone)]
pub struct MembershipStatus {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub failed_nodes: usize,
    pub node_id: String,
}

impl ClusterMembership {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            members: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_interval: Duration::from_millis(1000),
            failure_timeout: Duration::from_millis(5000),
            membership_task: None,
        }
    }

    /// Start the membership management system
    pub async fn start(&mut self) -> Result<(), String> {
        // Add self to membership
        self.add_member(self.node_id.clone(), "127.0.0.1".to_string(), 50051).await;
        
        // Start background membership management task
        let members = Arc::clone(&self.members);
        let node_id = self.node_id.clone();
        let heartbeat_interval = self.heartbeat_interval;
        let failure_timeout = self.failure_timeout;
        
        let handle = tokio::spawn(async move {
            let mut interval = interval(heartbeat_interval);
            
            loop {
                interval.tick().await;
                
                // Send heartbeats to all other members
                let mut members_guard = members.write().await;
                let mut to_remove = Vec::new();
                
                for (member_id, member_info) in members_guard.iter_mut() {
                    if *member_id == node_id {
                        continue; // Skip self
                    }
                    
                    // Check if member has failed
                    if member_info.last_heartbeat.elapsed() > failure_timeout {
                        member_info.health_status = HealthStatus::Failed;
                        to_remove.push(member_id.clone());
                    } else if member_info.last_heartbeat.elapsed() > failure_timeout / 2 {
                        member_info.health_status = HealthStatus::Suspicious;
                    } else {
                        member_info.health_status = HealthStatus::Healthy;
                    }
                    
                    // Send heartbeat
                    if let Err(e) = Self::send_heartbeat(member_id, &member_info.node_info).await {
                        eprintln!("Failed to send heartbeat to {}: {}", member_id, e);
                    }
                }
                
                // Remove failed members
                for member_id in to_remove {
                    members_guard.remove(&member_id);
                }
            }
        });
        
        self.membership_task = Some(handle);
        Ok(())
    }

    /// Stop the membership management system
    pub async fn stop(&mut self) -> Result<(), String> {
        if let Some(handle) = self.membership_task.take() {
            handle.abort();
        }
        Ok(())
    }

    /// Add a new member to the cluster
    pub async fn add_member(&self, node_id: String, address: String, port: u32) {
        let node_info = NodeInfo {
            node_id: node_id.clone(),
            address,
            port: port as i32,
            status: NodeStatus::Active as i32,
            last_seen: chrono::Utc::now().timestamp(),
        };
        
        let member_info = MemberInfo {
            node_info,
            last_heartbeat: Instant::now(),
            health_status: HealthStatus::Healthy,
        };
        
        let mut members = self.members.write().await;
        members.insert(node_id, member_info);
    }

    /// Remove a member from the cluster
    pub async fn remove_member(&self, node_id: &str) {
        let mut members = self.members.write().await;
        members.remove(node_id);
    }

    /// Update member heartbeat
    pub async fn update_heartbeat(&self, node_id: &str) {
        let mut members = self.members.write().await;
        if let Some(member_info) = members.get_mut(node_id) {
            member_info.last_heartbeat = Instant::now();
            member_info.health_status = HealthStatus::Healthy;
        }
    }

    /// Get all cluster members
    pub async fn get_members(&self) -> Vec<NodeInfo> {
        let members = self.members.read().await;
        members.values()
            .map(|member_info| member_info.node_info.clone())
            .collect()
    }

    /// Get healthy members only
    pub async fn get_healthy_members(&self) -> Vec<NodeInfo> {
        let members = self.members.read().await;
        members.values()
            .filter(|member_info| member_info.health_status == HealthStatus::Healthy)
            .map(|member_info| member_info.node_info.clone())
            .collect()
    }

    /// Check if a node is healthy
    pub async fn is_healthy(&self, node_id: &str) -> bool {
        let members = self.members.read().await;
        members.get(node_id)
            .map(|member_info| member_info.health_status == HealthStatus::Healthy)
            .unwrap_or(false)
    }

    /// Get membership status
    pub fn get_status(&self) -> MembershipStatus {
        // This is a simplified version - in a real implementation,
        // you'd want to get this asynchronously
        MembershipStatus {
            total_nodes: 0, // TODO: Implement proper counting
            healthy_nodes: 0,
            failed_nodes: 0,
            node_id: self.node_id.clone(),
        }
    }

    /// Send heartbeat to a specific member
    async fn send_heartbeat(node_id: &str, node_info: &NodeInfo) -> Result<(), String> {
        let addr = format!("http://{}:{}", node_info.address, node_info.port);
        let mut client = KvCacheClient::connect(addr).await.map_err(|e| e.to_string())?;
        
        let _response = client.health_check(node_id.to_string()).await.map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Discover cluster members from a seed node
    pub async fn discover_from_seed(&self, seed_address: String, seed_port: u32) -> Result<(), String> {
        let addr = format!("http://{}:{}", seed_address, seed_port);
        let mut client = KvCacheClient::connect(addr).await.map_err(|e| e.to_string())?;
        
        let response = client.get_membership(self.node_id.clone()).await.map_err(|e| e.to_string())?;
        
        // Add discovered members
        for node_info in response.nodes {
            if node_info.node_id != self.node_id {
                self.add_member(
                    node_info.node_id.clone(),
                    node_info.address.clone(),
                    node_info.port as u32,
                ).await;
            }
        }
        
        Ok(())
    }

    /// Join cluster by contacting a seed node
    pub async fn join_cluster(&self, seed_address: String, seed_port: u32) -> Result<(), String> {
        let addr = format!("http://{}:{}", seed_address, seed_port);
        let _client = KvCacheClient::connect(addr).await.map_err(|e| e.to_string())?;
        
        // TODO: Implement proper join request
        // For now, just discover members
        self.discover_from_seed(seed_address, seed_port).await?;
        
        Ok(())
    }
}

impl Default for ClusterMembership {
    fn default() -> Self {
        Self::new("node-1".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_membership_creation() {
        let membership = ClusterMembership::new("test-node".to_string());
        assert_eq!(membership.node_id, "test-node");
    }

    #[tokio::test]
    async fn test_add_remove_member() {
        let membership = ClusterMembership::new("test-node".to_string());
        
        membership.add_member("node-2".to_string(), "127.0.0.1".to_string(), 50052).await;
        let members = membership.get_members().await;
        assert_eq!(members.len(), 1);
        
        membership.remove_member("node-2").await;
        let members = membership.get_members().await;
        assert_eq!(members.len(), 0);
    }
} 