//! Raft consensus algorithm implementation for the distributed key-value cache
//! 
//! This module provides a complete Raft consensus implementation for ensuring
//! consistency and fault tolerance in the distributed cache system.

pub mod state;
pub mod log;
pub mod communication;
pub mod election;
pub mod replication;
pub mod grpc_client;
pub mod grpc_server;

use crate::config::ClusterConfig;
use crate::log::log_cluster_operation;
use crate::metrics::MetricsCollector;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant};
use tokio::sync::RwLock;


pub use state::{RaftState, RaftRole, RaftTerm};
pub use log::{LogEntry, LogIndex, LogTerm};
pub use communication::{RaftRpc, RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse};
pub use election::{ElectionManager, ElectionTimeout};
pub use replication::{ReplicationManager, ReplicationState};
pub use grpc_client::GrpcRaftClient;
pub use grpc_server::{RaftGrpcServer, create_raft_server};

/// Main Raft consensus manager
#[derive(Clone)]
pub struct RaftConsensus {
    /// Current Raft state
    state: Arc<RwLock<RaftState>>,
    /// Election manager
    election_manager: ElectionManager,
    /// Replication manager
    replication_manager: ReplicationManager,
    /// Configuration
    config: ClusterConfig,
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
    /// Node ID
    pub node_id: String,
    /// Cluster members
    members: HashMap<String, String>,
    /// State machine store (for applying committed commands)
    store: Option<Arc<crate::ttl::TTLStore>>,
}

impl RaftConsensus {
    /// Create a new Raft consensus instance
    pub fn new(
        node_id: String,
        config: ClusterConfig,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        let state = Arc::new(RwLock::new(RaftState::new(node_id.clone())));
        
        // Create real gRPC RPC client
        let timeout = crate::consensus::communication::RpcTimeout::default();
        let rpc_client = Arc::new(GrpcRaftClient::new(timeout));
        
        let members = config.members.clone();
        let election_manager = ElectionManager::new(rpc_client.clone(), members.clone());
        let replication_manager = ReplicationManager::new(rpc_client, members.clone());
        
        Self {
            state,
            election_manager,
            replication_manager,
            config,
            metrics,
            node_id,
            members,
            store: None,
        }
    }

    /// Create a new Raft consensus instance with custom RPC client
    pub fn with_rpc_client(
        node_id: String,
        config: ClusterConfig,
        metrics: Arc<MetricsCollector>,
        rpc_client: Arc<dyn RaftRpc + Send + Sync>,
    ) -> Self {
        let state = Arc::new(RwLock::new(RaftState::new(node_id.clone())));
        
        let members = config.members.clone();
        let election_manager = ElectionManager::new(rpc_client.clone(), members.clone());
        let replication_manager = ReplicationManager::new(rpc_client, members.clone());
        
        Self {
            state,
            election_manager,
            replication_manager,
            config,
            metrics,
            node_id,
            members,
            store: None,
        }
    }

    /// Start the Raft consensus system
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Starting Raft consensus for node: {}", self.node_id);
        
        // Initialize as follower
        {
            let mut state = self.state.write().await;
            state.role = RaftRole::Follower;
            state.current_term = RaftTerm(1);
        }

        // Start election manager
        self.election_manager.start(Arc::clone(&self.state)).await?;
        
        // Start replication manager
        self.replication_manager.start(Arc::clone(&self.state)).await?;

        tracing::info!("Raft consensus started successfully");
        Ok(())
    }

    /// Stop the Raft consensus system
    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Stopping Raft consensus for node: {}", self.node_id);
        
        // Stop election manager
        self.election_manager.stop().await?;
        
        // Stop replication manager
        self.replication_manager.stop().await?;

        tracing::info!("Raft consensus stopped successfully");
        Ok(())
    }

    /// Get the current Raft state
    pub async fn get_state(&self) -> RaftState {
        self.state.read().await.clone()
    }

    /// Get a reference to the state Arc for gRPC server creation
    pub fn get_state_arc(&self) -> Arc<RwLock<RaftState>> {
        Arc::clone(&self.state)
    }

    /// Check if this node is currently the leader
    pub async fn is_leader(&self) -> bool {
        let state = self.state.read().await;
        state.role == RaftRole::Leader && state.leader_id == Some(self.node_id.clone())
    }

    /// Submit a command to the Raft log
    pub async fn submit_command(
        &self,
        command: Vec<u8>,
    ) -> Result<LogIndex, Box<dyn std::error::Error + Send + Sync>> {
        let start_time = Instant::now();
        
        // Check if we're the leader
        if !self.is_leader().await {
            return Err("Not the leader".into());
        }

        // Get current state
        let state = self.state.write().await;
        
        // Create log entry
        let log_entry = LogEntry::new(
            state.current_term,
            state.log.read().await.get_next_index(),
            command,
        );

        // Append to log
        let index = state.log.write().await.append(log_entry).await?;
        
        // Replicate to followers
        self.replication_manager.replicate_log_entry(index).await?;

        let result = Ok(index);

        // Record metrics
        let duration = start_time.elapsed();
        self.metrics.record_cluster_operation("raft_submit_command", result.is_ok(), duration);

        // Log operation
        log_cluster_operation(
            "raft_submit_command",
            &self.node_id,
            result.is_ok(),
            duration,
            None,
        );

        result
    }

    /// Apply committed log entries to the state machine
    pub async fn apply_committed_entries(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get the current commit index and last applied index
        let (commit_index, last_applied) = {
            let state = self.state.read().await;
            (state.commit_index, state.last_applied)
        };
        
        // Apply any new committed entries
        for index in (last_applied.0 + 1)..=commit_index.0 {
            let log_index = LogIndex(index);
            
            // Get the log entry
            let entry = {
                let state = self.state.read().await;
                let log = state.log.read().await;
                log.get_entry(log_index).await?
            };
            
            if let Some(entry) = entry {
                // Apply the command to the state machine
                self.apply_command(&entry.command).await?;
                
                // Update last applied index
                let mut state = self.state.write().await;
                state.last_applied = log_index;
            }
        }
        
        Ok(())
    }

    /// Set the state machine store
    pub fn set_store(&mut self, store: Arc<crate::ttl::TTLStore>) {
        self.store = Some(store);
    }

    /// Apply a command to the state machine
    async fn apply_command(&self, command: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Ok(command_str) = String::from_utf8(command.to_vec()) {
            let parts: Vec<&str> = command_str.split_whitespace().collect();
            if parts.len() >= 3 && parts[0] == "SET" {
                let key = parts[1].to_string();
                let value = parts[2..].join(" ");
                println!("Applying SET command: {} = {}", key, value);
                
                // Apply to the store if available
                if let Some(store) = &self.store {
                    store.set(key, crate::value::Value::String(value), None).await;
                }
            }
        }
        Ok(())
    }

    /// Handle RequestVote RPC
    pub async fn handle_request_vote(
        &self,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>> {
        let start_time = Instant::now();
        
        let mut state = self.state.write().await;
        
        // Check term
        if request.term < state.current_term {
            return Ok(RequestVoteResponse {
                term: state.current_term,
                vote_granted: false,
            });
        }

        // Update term if needed
        if request.term > state.current_term {
            state.current_term = request.term;
            state.role = RaftRole::Follower;
            state.voted_for = None;
        }

        // Check if we can vote
        let can_vote = state.voted_for.is_none() || state.voted_for.as_ref() == Some(&request.candidate_id);
        
        if can_vote {
            // Check if candidate's log is at least as up-to-date as ours
            let last_log_entry = state.log.read().await.get_last_entry().await?;
            let candidate_log_ok = request.last_log_term > last_log_entry.term.into() ||
                (request.last_log_term == last_log_entry.term.into() && 
                 request.last_log_index >= last_log_entry.index);

            if candidate_log_ok {
                state.voted_for = Some(request.candidate_id);
                return Ok(RequestVoteResponse {
                    term: state.current_term,
                    vote_granted: true,
                });
            }
        }

        let result = Ok(RequestVoteResponse {
            term: state.current_term,
            vote_granted: false,
        });

        // Record metrics
        let duration = start_time.elapsed();
        self.metrics.record_cluster_operation("raft_request_vote", result.is_ok(), duration);

        result
    }

    /// Handle AppendEntries RPC
    pub async fn handle_append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error + Send + Sync>> {
        let start_time = Instant::now();
        
        let mut state = self.state.write().await;
        
        // Check term
        if request.term < state.current_term {
            return Ok(AppendEntriesResponse {
                term: state.current_term,
                success: false,
                last_log_index: None,
                conflict_index: None,
                conflict_term: None,
            });
        }

        // Update term and become follower
        if request.term > state.current_term {
            state.current_term = request.term;
            state.role = RaftRole::Follower;
            state.voted_for = None;
        }

        // Update leader ID
        state.leader_id = Some(request.leader_id);

        // Reset election timeout
        self.election_manager.reset_timeout().await;

        // Check if previous log entry matches
        if request.prev_log_index > LogIndex(0) {
            let prev_log_entry = state.log.read().await.get_entry(request.prev_log_index).await?;
            if prev_log_entry.is_none() || prev_log_entry.unwrap().term != request.prev_log_term.into() {
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
        for entry in request.entries {
            state.log.write().await.append(entry).await?;
        }

        // Get last index before updating commit index
        let last_index = state.log.read().await.get_last_index();
        
        // Update commit index
        if request.leader_commit > state.commit_index {
            state.commit_index = std::cmp::min(request.leader_commit, last_index);
        }

        let result = Ok(AppendEntriesResponse {
            term: state.current_term,
            success: true,
            last_log_index: Some(last_index),
            conflict_index: None,
            conflict_term: None,
        });

        // Record metrics
        let duration = start_time.elapsed();
        self.metrics.record_cluster_operation("raft_append_entries", result.is_ok(), duration);

        result
    }

    /// Get cluster status
    pub async fn get_cluster_status(&self) -> ClusterStatus {
        let state = self.state.read().await;
        let log_size = state.log.read().await.get_size().unwrap_or(0);
        
        ClusterStatus {
            node_id: self.node_id.clone(),
            role: state.role.clone(),
            current_term: state.current_term,
            leader_id: state.leader_id.clone(),
            commit_index: state.commit_index,
            last_applied: state.last_applied,
            voted_for: state.voted_for.clone(),
            log_size,
        }
    }
}

/// Cluster status information
#[derive(Debug, Clone)]
pub struct ClusterStatus {
    pub node_id: String,
    pub role: RaftRole,
    pub current_term: RaftTerm,
    pub leader_id: Option<String>,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub voted_for: Option<String>,
    pub log_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;

    #[tokio::test]
    async fn test_raft_consensus_creation() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(crate::config::MetricsConfig::default()));
        let consensus = RaftConsensus::new("node1".to_string(), config, metrics);
        
        assert_eq!(consensus.node_id, "node1");
    }

    #[tokio::test]
    async fn test_raft_consensus_start_stop() {
        let config = ClusterConfig::default();
        let metrics = Arc::new(MetricsCollector::new(crate::config::MetricsConfig::default()));
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
        let metrics = Arc::new(MetricsCollector::new(crate::config::MetricsConfig::default()));
        let consensus = RaftConsensus::new("node1".to_string(), config, metrics);
        
        // Initially not leader
        assert!(!consensus.is_leader().await);
    }
} 