//! Replication management for fault tolerance
//! 
//! This module implements leader-follower replication to ensure data
//! availability and fault tolerance in the distributed cache.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::cluster::communication::KvCacheClient;

/// Replication strategy for different consistency levels
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationStrategy {
    /// Synchronous replication - wait for all replicas
    Synchronous,
    /// Asynchronous replication - fire and forget
    Asynchronous,
    /// Quorum-based replication - wait for majority
    Quorum(usize),
}

/// Replication manager for handling data replication
pub struct ReplicationManager {
    replication_factor: usize,
    strategy: ReplicationStrategy,
    followers: Arc<RwLock<HashMap<String, KvCacheClient>>>,
    local_node_id: String,
    is_leader: bool,
}

/// Status information about replication
#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    pub replication_factor: usize,
    pub active_followers: usize,
    pub strategy: String,
    pub is_leader: bool,
}

impl ReplicationManager {
    pub fn new(replication_factor: usize) -> Self {
        Self {
            replication_factor,
            strategy: ReplicationStrategy::Asynchronous,
            followers: Arc::new(RwLock::new(HashMap::new())),
            local_node_id: "node-1".to_string(),
            is_leader: false,
        }
    }

    pub fn with_strategy(mut self, strategy: ReplicationStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn with_local_node_id(mut self, node_id: String) -> Self {
        self.local_node_id = node_id;
        self
    }

    pub fn set_leader(mut self, is_leader: bool) -> Self {
        self.is_leader = is_leader;
        self
    }

    /// Start the replication manager
    pub async fn start(&mut self) -> Result<(), String> {
        // Initialize replication manager
        Ok(())
    }

    /// Stop the replication manager
    pub async fn stop(&mut self) -> Result<(), String> {
        // Clear all follower connections
        let mut followers = self.followers.write().await;
        followers.clear();
        Ok(())
    }

    /// Add a follower node
    pub async fn add_follower(&self, node_id: String, address: String, port: u32) -> Result<(), String> {
        let addr = format!("http://{}:{}", address, port);
        let client = KvCacheClient::connect(addr).await.map_err(|e| e.to_string())?;
        
        let mut followers = self.followers.write().await;
        followers.insert(node_id, client);
        
        Ok(())
    }

    /// Remove a follower node
    pub async fn remove_follower(&self, node_id: &str) {
        let mut followers = self.followers.write().await;
        followers.remove(node_id);
    }

    /// Replicate a write operation to followers
    pub async fn replicate_write(
        &self,
        operation: String,
        key: String,
        value: String,
        ttl_seconds: i64,
    ) -> Result<(), String> {
        if !self.is_leader {
            return Ok(()); // Only leaders replicate
        }

        let followers = self.followers.read().await;
        if followers.is_empty() {
            return Ok(()); // No followers to replicate to
        }

        match self.strategy {
            ReplicationStrategy::Synchronous => {
                self.replicate_synchronous(operation, key, value, ttl_seconds).await
            }
            ReplicationStrategy::Asynchronous => {
                self.replicate_asynchronous(operation, key, value, ttl_seconds).await
            }
            ReplicationStrategy::Quorum(quorum_size) => {
                self.replicate_quorum(quorum_size, operation, key, value, ttl_seconds).await
            }
        }
    }

    /// Synchronous replication - wait for all replicas
    async fn replicate_synchronous(
        &self,
        operation: String,
        key: String,
        value: String,
        ttl_seconds: i64,
    ) -> Result<(), String> {
        let followers = self.followers.read().await;
        let mut futures = Vec::new();
        
        for (node_id, client) in followers.iter() {
            let mut client_clone = client.clone();
            let operation_clone = operation.clone();
            let key_clone = key.clone();
            let value_clone = value.clone();
            let node_id_clone = node_id.clone();
            
            let future = tokio::spawn(async move {
                client_clone.forward_request(operation_clone, key_clone, value_clone, ttl_seconds).await
                    .map_err(|e| format!("Replication failed to {}: {}", node_id_clone, e))
            });
            
            futures.push((node_id.clone(), future));
        }

        // Wait for all replicas to respond
        for (node_id, future) in futures {
            match future.await {
                Ok(Ok(_)) => {
                    // Replication successful
                }
                Ok(Err(e)) => {
                    return Err(e);
                }
                Err(e) => {
                    return Err(format!("Replication task failed for {}: {}", node_id, e));
                }
            }
        }

        Ok(())
    }

    /// Asynchronous replication - fire and forget
    async fn replicate_asynchronous(
        &self,
        operation: String,
        key: String,
        value: String,
        ttl_seconds: i64,
    ) -> Result<(), String> {
        let followers = self.followers.read().await;
        
        for (node_id, client) in followers.iter() {
            let mut client_clone = client.clone();
            let operation_clone = operation.clone();
            let key_clone = key.clone();
            let value_clone = value.clone();
            let node_id_clone = node_id.clone();
            
            // Spawn replication task without waiting
            tokio::spawn(async move {
                if let Err(e) = client_clone.forward_request(operation_clone, key_clone, value_clone, ttl_seconds).await {
                    eprintln!("Asynchronous replication failed to {}: {}", node_id_clone, e);
                }
            });
        }

        Ok(())
    }

    /// Quorum-based replication - wait for majority
    async fn replicate_quorum(
        &self,
        quorum_size: usize,
        operation: String,
        key: String,
        value: String,
        ttl_seconds: i64,
    ) -> Result<(), String> {
        let followers = self.followers.read().await;
        let mut futures = Vec::new();
        
        for (node_id, client) in followers.iter() {
            let mut client_clone = client.clone();
            let operation_clone = operation.clone();
            let key_clone = key.clone();
            let value_clone = value.clone();
            let node_id_clone = node_id.clone();
            
            let future = tokio::spawn(async move {
                client_clone.forward_request(operation_clone, key_clone, value_clone, ttl_seconds).await
                    .map_err(|e| format!("Quorum replication failed to {}: {}", node_id_clone, e))
            });
            
            futures.push((node_id.clone(), future));
        }

        // Wait for quorum to respond
        let mut successful_replicas = 0;
        for (node_id, future) in futures {
            match future.await {
                Ok(Ok(_)) => {
                    successful_replicas += 1;
                    if successful_replicas >= quorum_size {
                        return Ok(()); // Quorum reached
                    }
                }
                Ok(Err(e)) => {
                    eprintln!("{}", e);
                }
                Err(e) => {
                    eprintln!("Quorum replication task failed for {}: {}", node_id, e);
                }
            }
        }

        if successful_replicas >= quorum_size {
            Ok(())
        } else {
            Err(format!("Failed to reach quorum: {}/{} replicas", successful_replicas, quorum_size))
        }
    }

    /// Get replication status
    pub fn get_status(&self) -> ReplicationStatus {
        // This is a simplified version - in a real implementation,
        // you'd want to get this asynchronously
        ReplicationStatus {
            replication_factor: self.replication_factor,
            active_followers: 0, // TODO: Implement proper counting
            strategy: format!("{:?}", self.strategy),
            is_leader: self.is_leader,
        }
    }

    /// Get the number of active followers
    pub async fn get_follower_count(&self) -> usize {
        let followers = self.followers.read().await;
        followers.len()
    }

    /// Check if we have enough replicas for the replication factor
    pub async fn has_sufficient_replicas(&self) -> bool {
        let follower_count = self.get_follower_count().await;
        follower_count >= self.replication_factor - 1 // -1 because leader doesn't count as follower
    }
}

impl Default for ReplicationManager {
    fn default() -> Self {
        Self::new(2) // Default replication factor of 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replication_manager_creation() {
        let manager = ReplicationManager::new(3)
            .with_strategy(ReplicationStrategy::Synchronous)
            .with_local_node_id("node-1".to_string())
            .set_leader(true);
        
        assert_eq!(manager.replication_factor, 3);
        assert_eq!(manager.strategy, ReplicationStrategy::Synchronous);
        assert_eq!(manager.local_node_id, "node-1");
        assert!(manager.is_leader);
    }

    #[tokio::test]
    async fn test_replication_manager_status() {
        let manager = ReplicationManager::new(2);
        let status = manager.get_status();
        
        assert_eq!(status.replication_factor, 2);
        assert_eq!(status.strategy, "Asynchronous");
        assert!(!status.is_leader);
    }
} 