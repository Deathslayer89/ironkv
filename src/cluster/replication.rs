//! Replication management for fault tolerance
//! 
//! This module implements leader-follower replication to ensure data
//! availability and fault tolerance in the distributed cache.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use crate::cluster::communication::KvCacheClient;
use crate::{TTLStore, Value};

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

/// Replication log entry for state synchronization
#[derive(Debug, Clone)]
pub struct ReplicationLogEntry {
    pub sequence_number: u64,
    pub timestamp: Instant,
    pub operation: String,
    pub key: String,
    pub value: Option<String>,
    pub ttl_seconds: Option<i64>,
}

/// Follower state for tracking synchronization
#[derive(Debug, Clone)]
pub struct FollowerState {
    pub node_id: String,
    pub last_sequence_number: u64,
    pub last_sync_time: Instant,
    pub sync_status: SyncStatus,
    pub client: KvCacheClient,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SyncStatus {
    /// Follower is up to date
    Synchronized,
    /// Follower needs full sync
    NeedsFullSync,
    /// Follower needs log replay
    NeedsLogReplay,
    /// Follower is offline
    Offline,
}

/// Replication manager for handling data replication
pub struct ReplicationManager {
    replication_factor: usize,
    strategy: ReplicationStrategy,
    followers: Arc<RwLock<HashMap<String, FollowerState>>>,
    local_node_id: String,
    is_leader: bool,
    store: Arc<TTLStore>,
    replication_log: Arc<RwLock<VecDeque<ReplicationLogEntry>>>,
    sequence_number: Arc<RwLock<u64>>,
    max_log_size: usize,
}

/// Status information about replication
#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    pub replication_factor: usize,
    pub active_followers: usize,
    pub strategy: String,
    pub is_leader: bool,
    pub log_size: usize,
    pub sequence_number: u64,
}

impl ReplicationManager {
    pub fn new(replication_factor: usize) -> Self {
        Self {
            replication_factor,
            strategy: ReplicationStrategy::Asynchronous,
            followers: Arc::new(RwLock::new(HashMap::new())),
            local_node_id: "node-1".to_string(),
            is_leader: false,
            store: Arc::new(TTLStore::new()),
            replication_log: Arc::new(RwLock::new(VecDeque::new())),
            sequence_number: Arc::new(RwLock::new(0)),
            max_log_size: 10000, // Keep last 10k operations
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

    pub fn with_store(mut self, store: Arc<TTLStore>) -> Self {
        self.store = store;
        self
    }

    /// Start the replication manager
    pub async fn start(&mut self) -> Result<(), String> {
        // Start background log cleanup task
        let replication_log = Arc::clone(&self.replication_log);
        let max_log_size = self.max_log_size;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // Cleanup every minute
            loop {
                interval.tick().await;
                let mut log = replication_log.write().await;
                while log.len() > max_log_size {
                    log.pop_front(); // Remove oldest entries
                }
            }
        });
        
        Ok(())
    }

    /// Stop the replication manager
    pub async fn stop(&mut self) -> Result<(), String> {
        // Clear all follower connections
        let mut followers = self.followers.write().await;
        followers.clear();
        Ok(())
    }

    /// Add a follower node with initial sync
    pub async fn add_follower(&self, node_id: String, address: String, port: u32) -> Result<(), String> {
        let addr = format!("http://{}:{}", address, port);
        let client = KvCacheClient::connect(addr).await.map_err(|e| e.to_string())?;
        
        let follower_state = FollowerState {
            node_id: node_id.clone(),
            last_sequence_number: 0,
            last_sync_time: Instant::now(),
            sync_status: SyncStatus::NeedsFullSync,
            client,
        };
        
        let mut followers = self.followers.write().await;
        followers.insert(node_id.clone(), follower_state);
        
        // Trigger initial full sync
        if self.is_leader {
            self.perform_full_sync(&node_id).await?;
        }
        
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

        // Add to replication log
        let seq_num = {
            let mut seq = self.sequence_number.write().await;
            *seq += 1;
            *seq
        };

        let log_entry = ReplicationLogEntry {
            sequence_number: seq_num,
            timestamp: Instant::now(),
            operation: operation.clone(),
            key: key.clone(),
            value: Some(value.clone()),
            ttl_seconds: if ttl_seconds > 0 { Some(ttl_seconds) } else { None },
        };

        {
            let mut log = self.replication_log.write().await;
            log.push_back(log_entry);
        }

        let followers = self.followers.read().await;
        if followers.is_empty() {
            return Ok(()); // No followers to replicate to
        }

        match self.strategy {
            ReplicationStrategy::Synchronous => {
                self.replicate_synchronous(seq_num, operation, key, value, ttl_seconds).await
            }
            ReplicationStrategy::Asynchronous => {
                self.replicate_asynchronous(seq_num, operation, key, value, ttl_seconds).await
            }
            ReplicationStrategy::Quorum(quorum_size) => {
                self.replicate_quorum(quorum_size, seq_num, operation, key, value, ttl_seconds).await
            }
        }
    }

    /// Perform initial full sync for a new follower
    pub async fn perform_full_sync(&self, follower_id: &str) -> Result<(), String> {
        let mut followers = self.followers.write().await;
        let follower = followers.get_mut(follower_id)
            .ok_or(format!("Follower {} not found", follower_id))?;
        
        follower.sync_status = SyncStatus::NeedsFullSync;
        
        // Get all keys from the store (this is a simplified version)
        // In a real implementation, you'd iterate through the store efficiently
        let store_data = self.get_store_snapshot().await?;
        
        // Send all data to follower
        for (key, value, ttl) in store_data {
            let mut client = follower.client.clone();
            let key_clone = key.clone();
            let _ = client.forward_request(
                "SET".to_string(),
                key,
                value,
                ttl.unwrap_or(0),
            ).await.map_err(|e| format!("Full sync failed for key {}: {}", key_clone, e))?;
        }
        
        // Update follower state
        let current_seq = *self.sequence_number.read().await;
        follower.last_sequence_number = current_seq;
        follower.last_sync_time = Instant::now();
        follower.sync_status = SyncStatus::Synchronized;
        
        Ok(())
    }

    /// Perform log replay for a recovering follower
    pub async fn perform_log_replay(&self, follower_id: &str) -> Result<(), String> {
        let mut followers = self.followers.write().await;
        let follower = followers.get_mut(follower_id)
            .ok_or(format!("Follower {} not found", follower_id))?;
        
        let last_seq = follower.last_sequence_number;
        let log = self.replication_log.read().await;
        
        // Find entries after the follower's last sequence number
        let entries_to_replay: Vec<_> = log.iter()
            .filter(|entry| entry.sequence_number > last_seq)
            .cloned()
            .collect();
        
        if entries_to_replay.is_empty() {
            follower.sync_status = SyncStatus::Synchronized;
            return Ok(());
        }
        
        follower.sync_status = SyncStatus::NeedsLogReplay;
        
        // Replay operations
        for entry in &entries_to_replay {
            let mut client = follower.client.clone();
            let _ = client.forward_request(
                entry.operation.clone(),
                entry.key.clone(),
                entry.value.clone().unwrap_or_default(),
                entry.ttl_seconds.unwrap_or(0),
            ).await.map_err(|e| format!("Log replay failed for seq {}: {}", entry.sequence_number, e))?;
        }
        
        // Update follower state
        if let Some(last_entry) = entries_to_replay.last() {
            follower.last_sequence_number = last_entry.sequence_number;
        }
        follower.last_sync_time = Instant::now();
        follower.sync_status = SyncStatus::Synchronized;
        
        Ok(())
    }

    /// Get a snapshot of the current store state for full sync
    async fn get_store_snapshot(&self) -> Result<Vec<(String, String, Option<i64>)>, String> {
        // This is a simplified implementation
        // In a real system, you'd need to iterate through the store efficiently
        // For now, we'll return an empty vector as a placeholder
        Ok(vec![])
    }

    /// Check and sync followers that need synchronization
    pub async fn sync_followers(&self) -> Result<(), String> {
        if !self.is_leader {
            return Ok(());
        }

        // Collect followers that need sync
        let mut followers_to_sync = Vec::new();
        {
            let followers = self.followers.read().await;
            for (follower_id, follower_state) in followers.iter() {
                match follower_state.sync_status {
                    SyncStatus::NeedsFullSync => {
                        followers_to_sync.push((follower_id.clone(), SyncStatus::NeedsFullSync));
                    }
                    SyncStatus::NeedsLogReplay => {
                        followers_to_sync.push((follower_id.clone(), SyncStatus::NeedsLogReplay));
                    }
                    SyncStatus::Synchronized => {
                        // Check if follower is still up to date
                        let current_seq = *self.sequence_number.read().await;
                        if follower_state.last_sequence_number < current_seq {
                            followers_to_sync.push((follower_id.clone(), SyncStatus::NeedsLogReplay));
                        }
                    }
                    SyncStatus::Offline => {
                        // Try to reconnect or mark for removal
                        // This would involve health checks
                    }
                }
            }
        }

        // Perform sync operations
        for (follower_id, sync_type) in followers_to_sync {
            match sync_type {
                SyncStatus::NeedsFullSync => {
                    self.perform_full_sync(&follower_id).await?;
                }
                SyncStatus::NeedsLogReplay => {
                    self.perform_log_replay(&follower_id).await?;
                }
                _ => {}
            }
        }
        
        Ok(())
    }

    /// Synchronous replication - wait for all replicas
    async fn replicate_synchronous(
        &self,
        sequence_number: u64,
        operation: String,
        key: String,
        value: String,
        ttl_seconds: i64,
    ) -> Result<(), String> {
        let followers = self.followers.read().await;
        let mut futures = Vec::new();
        
        for (node_id, follower_state) in followers.iter() {
            let mut client = follower_state.client.clone();
            let operation_clone = operation.clone();
            let key_clone = key.clone();
            let value_clone = value.clone();
            let node_id_clone = node_id.clone();
            
            let future = tokio::spawn(async move {
                client.forward_request(operation_clone, key_clone, value_clone, ttl_seconds).await
                    .map_err(|e| format!("Replication failed to {}: {}", node_id_clone, e))
            });
            
            futures.push((node_id.clone(), future));
        }

        // Wait for all replicas to respond
        for (node_id, future) in futures {
            match future.await {
                Ok(Ok(_)) => {
                    // Update follower's last sequence number
                    let mut followers = self.followers.write().await;
                    if let Some(follower) = followers.get_mut(&node_id) {
                        follower.last_sequence_number = sequence_number;
                        follower.last_sync_time = Instant::now();
                    }
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
        sequence_number: u64,
        operation: String,
        key: String,
        value: String,
        ttl_seconds: i64,
    ) -> Result<(), String> {
        let followers = self.followers.read().await;
        
        for (node_id, follower_state) in followers.iter() {
            let mut client = follower_state.client.clone();
            let operation_clone = operation.clone();
            let key_clone = key.clone();
            let value_clone = value.clone();
            let node_id_clone = node_id.clone();
            let seq_num = sequence_number;
            
            // Spawn replication task without waiting
            tokio::spawn(async move {
                if let Err(e) = client.forward_request(operation_clone, key_clone, value_clone, ttl_seconds).await {
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
        sequence_number: u64,
        operation: String,
        key: String,
        value: String,
        ttl_seconds: i64,
    ) -> Result<(), String> {
        let followers = self.followers.read().await;
        let mut futures = Vec::new();
        
        for (node_id, follower_state) in followers.iter() {
            let mut client = follower_state.client.clone();
            let operation_clone = operation.clone();
            let key_clone = key.clone();
            let value_clone = value.clone();
            let node_id_clone = node_id.clone();
            
            let future = tokio::spawn(async move {
                client.forward_request(operation_clone, key_clone, value_clone, ttl_seconds).await
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
                    // Update follower's last sequence number
                    let mut followers = self.followers.write().await;
                    if let Some(follower) = followers.get_mut(&node_id) {
                        follower.last_sequence_number = sequence_number;
                        follower.last_sync_time = Instant::now();
                    }
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
        let log_size = self.replication_log.try_read()
            .map(|log| log.len())
            .unwrap_or(0);
        let sequence_number = self.sequence_number.try_read()
            .map(|seq| *seq)
            .unwrap_or(0);
        
        ReplicationStatus {
            replication_factor: self.replication_factor,
            active_followers: 0, // TODO: Implement proper counting
            strategy: format!("{:?}", self.strategy),
            is_leader: self.is_leader,
            log_size,
            sequence_number,
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

    /// Get replication log for debugging
    pub async fn get_replication_log(&self) -> Vec<ReplicationLogEntry> {
        let log = self.replication_log.read().await;
        log.iter().cloned().collect()
    }

    /// Get follower states for monitoring
    pub async fn get_follower_states(&self) -> HashMap<String, (u64, SyncStatus)> {
        let followers = self.followers.read().await;
        followers.iter()
            .map(|(id, state)| (id.clone(), (state.last_sequence_number, state.sync_status.clone())))
            .collect()
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

    #[tokio::test]
    async fn test_replication_log_management() {
        let manager = ReplicationManager::new(2).set_leader(true);
        
        // Add some operations to the log
        manager.replicate_write(
            "SET".to_string(),
            "key1".to_string(),
            "value1".to_string(),
            0,
        ).await.unwrap();
        
        manager.replicate_write(
            "SET".to_string(),
            "key2".to_string(),
            "value2".to_string(),
            60,
        ).await.unwrap();
        
        let log = manager.get_replication_log().await;
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].key, "key1");
        assert_eq!(log[1].key, "key2");
    }
} 