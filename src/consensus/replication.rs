//! Raft log replication management for the consensus system
//! 
//! This module provides log replication logic, follower state tracking,
//! and replication optimization for the Raft consensus algorithm.

use crate::consensus::communication::{AppendEntriesRequest, RaftRpc};
use crate::consensus::log::{LogEntry, LogIndex, LogTerm};
use crate::consensus::state::{RaftState, RaftTerm};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;

/// Replication state for a follower
#[derive(Debug, Clone)]
pub struct ReplicationState {
    /// Follower's node ID
    pub node_id: String,
    /// Next index to send to this follower
    pub next_index: LogIndex,
    /// Highest replicated index for this follower
    pub match_index: LogIndex,
    /// Whether the follower is active
    pub is_active: bool,
    /// Last time we received a response from this follower
    pub last_response_time: Option<Instant>,
    /// Number of consecutive failures
    pub consecutive_failures: u32,
    /// Whether we're currently replicating to this follower
    pub replicating: bool,
}

impl ReplicationState {
    /// Create a new replication state
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            next_index: LogIndex(1),
            match_index: LogIndex(0),
            is_active: true,
            last_response_time: None,
            consecutive_failures: 0,
            replicating: false,
        }
    }

    /// Update the next index
    pub fn update_next_index(&mut self, new_next_index: LogIndex) {
        self.next_index = new_next_index;
    }

    /// Update the match index
    pub fn update_match_index(&mut self, new_match_index: LogIndex) {
        if new_match_index > self.match_index {
            self.match_index = new_match_index;
        }
    }

    /// Mark a successful replication
    pub fn mark_success(&mut self) {
        self.consecutive_failures = 0;
        self.last_response_time = Some(Instant::now());
        self.is_active = true;
    }

    /// Mark a failed replication
    pub fn mark_failure(&mut self) {
        self.consecutive_failures += 1;
        if self.consecutive_failures > 5 {
            self.is_active = false;
        }
    }

    /// Check if the follower is healthy
    pub fn is_healthy(&self) -> bool {
        self.is_active && self.consecutive_failures < 3
    }

    /// Get the lag behind the leader
    pub fn get_lag(&self, leader_last_index: LogIndex) -> u64 {
        if leader_last_index.value() > self.match_index.value() {
            leader_last_index.value() - self.match_index.value()
        } else {
            0
        }
    }
}

/// Replication manager for handling log replication
pub struct ReplicationManager {
    /// RPC client for communication
    rpc_client: Arc<dyn RaftRpc + Send + Sync>,
    /// Cluster members
    members: HashMap<String, String>,
    /// Replication state for each follower
    replication_states: HashMap<String, ReplicationState>,
    /// Heartbeat interval
    heartbeat_interval: Duration,
    /// Control channel for stopping the manager
    stop_tx: Option<mpsc::Sender<()>>,
    /// Background task handle
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ReplicationManager {
    /// Create a new replication manager
    pub fn new(
        rpc_client: Arc<dyn RaftRpc + Send + Sync>,
        members: HashMap<String, String>,
    ) -> Self {
        let mut replication_states = HashMap::new();
        
        // Initialize replication state for each member
        for (node_id, _) in &members {
            replication_states.insert(node_id.clone(), ReplicationState::new(node_id.clone()));
        }

        Self {
            rpc_client,
            members,
            replication_states,
            heartbeat_interval: Duration::from_millis(50), // 50ms default
            stop_tx: None,
            task_handle: None,
        }
    }

    /// Start the replication manager
    pub async fn start(
        &mut self,
        state: Arc<RwLock<RaftState>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (stop_tx, stop_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);

        let rpc_client = Arc::clone(&self.rpc_client);
        let members = self.members.clone();
        let replication_states = self.replication_states.clone();
        let heartbeat_interval = self.heartbeat_interval;

        let task_handle = tokio::spawn(async move {
            Self::run_replication_loop(
                state,
                rpc_client,
                members,
                replication_states,
                heartbeat_interval,
                stop_rx,
            ).await;
        });

        self.task_handle = Some(task_handle);
        Ok(())
    }

    /// Stop the replication manager
    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(()).await;
        }

        if let Some(task_handle) = self.task_handle.take() {
            let _ = task_handle.await;
        }

        Ok(())
    }

    /// Replicate a specific log entry to all followers
    pub async fn replicate_log_entry(
        &self,
        log_index: LogIndex,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // This would be called by the leader when a new entry is added
        // In a real implementation, this would trigger replication to all followers
        tracing::debug!("Replicating log entry {} to followers", log_index);
        Ok(())
    }

    /// Run the main replication loop
    async fn run_replication_loop(
        state: Arc<RwLock<RaftState>>,
        rpc_client: Arc<dyn RaftRpc + Send + Sync>,
        members: HashMap<String, String>,
        mut replication_states: HashMap<String, ReplicationState>,
        heartbeat_interval: Duration,
        mut stop_rx: mpsc::Receiver<()>,
    ) {
        let mut interval = interval(heartbeat_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Check if we're the leader and send heartbeats
                    if let Err(_) = Self::send_heartbeats(
                        &state,
                        &rpc_client,
                        &members,
                        &mut replication_states,
                    ).await {
                        tracing::error!("Error sending heartbeats");
                    }
                }
                _ = stop_rx.recv() => {
                    tracing::info!("Replication manager stopping");
                    break;
                }
            }
        }
    }

    /// Send heartbeats to all followers
    async fn send_heartbeats(
        state: &Arc<RwLock<RaftState>>,
        rpc_client: &Arc<dyn RaftRpc + Send + Sync>,
        members: &HashMap<String, String>,
        replication_states: &mut HashMap<String, ReplicationState>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let state_guard = state.read().await;
        
        // Only send heartbeats if we're the leader
        if !state_guard.is_leader() {
            return Ok(());
        }

        let current_term = state_guard.current_term;
        let leader_id = state_guard.node_id.clone();
        let commit_index = state_guard.commit_index;
        let _last_log_entry = state_guard.log.read().await.get_last_entry().await?;
        drop(state_guard);

        // Send heartbeats to all followers
        let mut heartbeat_tasks = Vec::new();

        for (node_id, address) in members {
            if node_id == &leader_id {
                continue; // Skip self
            }

            let replication_state = replication_states.get(node_id).unwrap();
            
            // Create heartbeat request (empty entries)
            let request = AppendEntriesRequest {
                term: current_term,
                leader_id: leader_id.clone(),
                prev_log_index: LogIndex(replication_state.next_index.value().saturating_sub(1)),
                prev_log_term: if replication_state.next_index.value() > 1 {
                    // Get the term of the previous log entry
                    let prev_entry = state.read().await.log.read().await
                        .get_entry(LogIndex(replication_state.next_index.value() - 1)).await?;
                    LogTerm::from(prev_entry.unwrap_or_else(|| {
                        LogEntry::new(RaftTerm(0), LogIndex(0), vec![])
                    }).term)
                } else {
                    LogTerm(0)
                },
                entries: vec![], // Empty for heartbeat
                leader_commit: commit_index,
            };

            let rpc_client = Arc::clone(rpc_client);
            let node_id = node_id.clone();
            let address = address.clone();

            let task = tokio::spawn(async move {
                rpc_client.append_entries(&address, request).await
            });

            heartbeat_tasks.push((node_id, task));
        }

        // Process responses
        for (node_id, task) in heartbeat_tasks {
            match task.await {
                Ok(Ok(response)) => {
                    if let Some(replication_state) = replication_states.get_mut(&node_id) {
                        if response.term > current_term {
                            // Found higher term, step down
                            let mut state_guard = state.write().await;
                            state_guard.update_term(response.term);
                            tracing::info!("Found higher term {}, stepping down", response.term);
                            return Ok(());
                        } else if response.success {
                            replication_state.mark_success();
                            if let Some(last_log_index) = response.last_log_index {
                                replication_state.update_match_index(last_log_index);
                            }
                        } else {
                            replication_state.mark_failure();
                            // Decrement next_index for optimization
                            if replication_state.next_index.value() > 1 {
                                replication_state.update_next_index(
                                    LogIndex(replication_state.next_index.value() - 1)
                                );
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    if let Some(replication_state) = replication_states.get_mut(&node_id) {
                        replication_state.mark_failure();
                    }
                    tracing::warn!("Failed to send heartbeat to node {}: {}", node_id, e);
                }
                Err(e) => {
                    tracing::warn!("Task failed for node {}: {}", node_id, e);
                }
            }
        }

        // Update commit index based on replication
        Self::update_commit_index(state, replication_states).await?;

        Ok(())
    }

    /// Update the commit index based on replication progress
    async fn update_commit_index(
        state: &Arc<RwLock<RaftState>>,
        replication_states: &HashMap<String, ReplicationState>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut state_guard = state.write().await;
        let current_term = state_guard.current_term;
        let _last_log_index = state_guard.log.read().await.get_last_index();
        
        // Find the highest index that has been replicated to a majority
        let mut match_indices: Vec<u64> = replication_states
            .values()
            .map(|rs| rs.match_index.value())
            .collect();
        
        match_indices.sort();
        
        // Find the median index (majority)
        let majority_index = if !match_indices.is_empty() {
            let median_pos = match_indices.len() / 2;
            LogIndex(match_indices[median_pos])
        } else {
            LogIndex(0)
        };

        // Update commit index if we have a majority and the entry is from current term
        if majority_index > state_guard.commit_index {
            // Check if the entry at majority_index is from current term
            let entry = state_guard.log.read().await.get_entry(majority_index).await?;
            if let Some(entry) = entry {
                if entry.term == current_term {
                    state_guard.update_commit_index(majority_index);
                    tracing::debug!("Updated commit index to {}", majority_index);
                }
            }
        }

        Ok(())
    }

    /// Get replication statistics
    pub fn get_stats(&self) -> ReplicationStats {
        let total_lag = 0;
        let mut active_followers = 0;
        let mut healthy_followers = 0;

        for replication_state in self.replication_states.values() {
            if replication_state.is_active {
                active_followers += 1;
            }
            if replication_state.is_healthy() {
                healthy_followers += 1;
            }
            // Note: We don't have access to leader's last index here
            // total_lag += replication_state.get_lag(leader_last_index);
        }

        ReplicationStats {
            total_followers: self.replication_states.len(),
            active_followers,
            healthy_followers,
            total_lag,
            average_lag: if active_followers > 0 {
                total_lag / active_followers as u64
            } else {
                0
            },
        }
    }

    /// Get replication state for a specific follower
    pub fn get_follower_state(&self, node_id: &str) -> Option<&ReplicationState> {
        self.replication_states.get(node_id)
    }

    /// Check if a follower is caught up
    pub fn is_follower_caught_up(&self, node_id: &str, leader_last_index: LogIndex) -> bool {
        if let Some(replication_state) = self.replication_states.get(node_id) {
            replication_state.match_index >= leader_last_index
        } else {
            false
        }
    }
}

/// Replication statistics
#[derive(Debug, Clone)]
pub struct ReplicationStats {
    pub total_followers: usize,
    pub active_followers: u32,
    pub healthy_followers: u32,
    pub total_lag: u64,
    pub average_lag: u64,
}

impl std::fmt::Display for ReplicationStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Replication Stats: followers={}/{}/{}, lag={}",
            self.healthy_followers,
            self.active_followers,
            self.total_followers,
            self.average_lag
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::communication::{MockRaftClient, RpcTimeout};

    #[test]
    fn test_replication_state_creation() {
        let state = ReplicationState::new("node1".to_string());
        assert_eq!(state.node_id, "node1");
        assert_eq!(state.next_index, LogIndex(1));
        assert_eq!(state.match_index, LogIndex(0));
        assert!(state.is_active);
        assert_eq!(state.consecutive_failures, 0);
    }

    #[test]
    fn test_replication_state_updates() {
        let mut state = ReplicationState::new("node1".to_string());
        
        state.update_next_index(LogIndex(5));
        assert_eq!(state.next_index, LogIndex(5));
        
        state.update_match_index(LogIndex(3));
        assert_eq!(state.match_index, LogIndex(3));
        
        state.update_match_index(LogIndex(2)); // Should not update (lower)
        assert_eq!(state.match_index, LogIndex(3));
    }

    #[test]
    fn test_replication_state_success_failure() {
        let mut state = ReplicationState::new("node1".to_string());
        
        // Mark success
        state.mark_success();
        assert_eq!(state.consecutive_failures, 0);
        assert!(state.is_active);
        assert!(state.last_response_time.is_some());
        
        // Mark failures
        for _ in 0..3 {
            state.mark_failure();
        }
        assert_eq!(state.consecutive_failures, 3);
        assert!(state.is_healthy()); // Still healthy
        
        state.mark_failure();
        assert_eq!(state.consecutive_failures, 4);
        assert!(!state.is_healthy()); // No longer healthy
        
        for _ in 0..2 {
            state.mark_failure();
        }
        assert!(!state.is_active); // No longer active
    }

    #[test]
    fn test_replication_state_lag() {
        let mut state = ReplicationState::new("node1".to_string());
        state.update_match_index(LogIndex(5));
        
        let lag = state.get_lag(LogIndex(10));
        assert_eq!(lag, 5);
        
        let lag = state.get_lag(LogIndex(3));
        assert_eq!(lag, 0); // No lag if leader is behind
    }

    #[tokio::test]
    async fn test_replication_manager_creation() {
        let timeout = RpcTimeout::default();
        let rpc_client = Arc::new(MockRaftClient::new(timeout));
        let members = HashMap::new();
        
        let manager = ReplicationManager::new(rpc_client, members);
        assert_eq!(manager.replication_states.len(), 0);
    }

    #[tokio::test]
    async fn test_replication_stats() {
        let timeout = RpcTimeout::default();
        let rpc_client = Arc::new(MockRaftClient::new(timeout));
        let members = HashMap::new();
        
        let manager = ReplicationManager::new(rpc_client, members);
        let stats = manager.get_stats();
        
        assert_eq!(stats.total_followers, 0);
        assert_eq!(stats.active_followers, 0);
        assert_eq!(stats.healthy_followers, 0);
    }
} 