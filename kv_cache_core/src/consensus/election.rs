//! Raft leader election management for the consensus system
//! 
//! This module provides leader election logic, timeout management, and
//! election state tracking for the Raft consensus algorithm.

use crate::consensus::communication::{RaftRpc, RequestVoteRequest};
use crate::consensus::log::LogTerm;
use crate::consensus::state::{RaftState};
use crate::RaftTerm;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;

/// Election timeout configuration
#[derive(Debug, Clone)]
pub struct ElectionTimeout {
    /// Minimum election timeout in milliseconds
    pub min_timeout_ms: u64,
    /// Maximum election timeout in milliseconds
    pub max_timeout_ms: u64,
    /// Current election timeout
    pub current_timeout_ms: u64,
}

impl ElectionTimeout {
    /// Create a new election timeout with default values
    pub fn new() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let min_timeout_ms = 150;
        let max_timeout_ms = 300;
        let current_timeout_ms = rng.gen_range(min_timeout_ms..=max_timeout_ms);
        
        Self {
            min_timeout_ms,
            max_timeout_ms,
            current_timeout_ms,
        }
    }

    /// Create a new election timeout with custom values
    pub fn with_timeouts(min_ms: u64, max_ms: u64) -> Self {
        Self {
            min_timeout_ms: min_ms,
            max_timeout_ms: max_ms,
            current_timeout_ms: min_ms,
        }
    }

    /// Reset the timeout to a random value within the range
    pub fn reset(&mut self) {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        self.current_timeout_ms = rng.gen_range(self.min_timeout_ms..=self.max_timeout_ms);
    }

    /// Get the current timeout duration
    pub fn duration(&self) -> Duration {
        Duration::from_millis(self.current_timeout_ms)
    }
}

impl Default for ElectionTimeout {
    fn default() -> Self {
        Self::new()
    }
}

/// Election manager for handling leader elections
pub struct ElectionManager {
    /// Election timeout configuration
    timeout: ElectionTimeout,
    /// RPC client for communication
    rpc_client: Arc<dyn RaftRpc + Send + Sync>,
    /// Cluster members
    members: HashMap<String, String>,
    /// Control channel for stopping the manager
    stop_tx: Option<mpsc::Sender<()>>,
    /// Background task handle
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Clone for ElectionManager {
    fn clone(&self) -> Self {
        Self {
            timeout: self.timeout.clone(),
            rpc_client: Arc::clone(&self.rpc_client),
            members: self.members.clone(),
            stop_tx: None, // Can't clone sender
            task_handle: None, // Can't clone JoinHandle
        }
    }
}

// Remove ElectionState struct and its impl
// Move any necessary fields (election_round, votes_received, election_start_time, election_in_progress, last_leader_contact) to RaftState if not already present
// Update all logic to use RaftState for election state

impl ElectionManager {
    /// Create a new election manager
    pub fn new(
        rpc_client: Arc<dyn RaftRpc + Send + Sync>,
        members: HashMap<String, String>,
    ) -> Self {
        let total_nodes = members.len() as u32;
        Self {
            timeout: ElectionTimeout::new(),
            rpc_client,
            members,
            stop_tx: None,
            task_handle: None,
        }
    }

    /// Start the election manager
    pub async fn start(
        &mut self,
        state: Arc<RwLock<RaftState>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (stop_tx, stop_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);

        let timeout = self.timeout.clone();
        let rpc_client = Arc::clone(&self.rpc_client);
        let members = self.members.clone();

        let task_handle = tokio::spawn(async move {
            Self::run_election_loop(state, timeout, rpc_client, members, stop_rx).await;
        });

        self.task_handle = Some(task_handle);
        Ok(())
    }

    /// Stop the election manager
    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(()).await;
        }

        if let Some(task_handle) = self.task_handle.take() {
            let _ = task_handle.await;
        }

        Ok(())
    }

    /// Reset the election timeout
    pub async fn reset_timeout(&self) {
        // This would be called when we receive a message from the leader
        // In a real implementation, we would signal the election loop
    }

    /// Run the main election loop
    async fn run_election_loop(
        state: Arc<RwLock<RaftState>>,
        mut timeout: ElectionTimeout,
        rpc_client: Arc<dyn RaftRpc + Send + Sync>,
        members: HashMap<String, String>,
        mut stop_rx: mpsc::Receiver<()>,
    ) {
        let mut interval = interval(Duration::from_millis(10)); // Check every 10ms

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Check if we need to start an election
                    if let Err(_) = Self::check_and_start_election(
                        &state,
                        &mut timeout,
                        &rpc_client,
                        &members,
                    ).await {
                        tracing::error!("Error in election check");
                    }
                }
                _ = stop_rx.recv() => {
                    tracing::info!("Election manager stopping");
                    break;
                }
            }
        }
    }

    /// Check if we need to start an election and handle it
    async fn check_and_start_election(
        state: &Arc<RwLock<RaftState>>,
        timeout: &mut ElectionTimeout,
        rpc_client: &Arc<dyn RaftRpc + Send + Sync>,
        members: &HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut state_guard = state.write().await;

        // Check if we're a follower and timeout has expired
        if state_guard.is_follower() && state_guard.is_election_timeout_expired(timeout.duration()) {
            println!("Election: Timeout expired for node {}, starting election with timeout {:?}", state_guard.node_id, timeout.duration());
            
            // Become candidate
            state_guard.become_candidate();
            state_guard.start_election();
            timeout.reset();
            println!("Election: Reset timeout to {:?} for node {}", timeout.duration(), state_guard.node_id);

            // Start election process
            drop(state_guard); // Release lock before async call
            Self::run_election(state, rpc_client, members).await?;
        }

        Ok(())
    }

    /// Run an election
    async fn run_election(
        state: &Arc<RwLock<RaftState>>,
        rpc_client: &Arc<dyn RaftRpc + Send + Sync>,
        members: &HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let state_guard = state.read().await;
        let current_term = state_guard.current_term;
        let candidate_id = state_guard.node_id.clone();
        let last_log_entry = state_guard.log.read().await.get_last_entry().await?;
        drop(state_guard);

        println!("Election: Node {} starting election for term {}", candidate_id, current_term);

        // Send RequestVote to all other nodes
        let mut vote_tasks = Vec::new();

        for (node_id, address) in members {
            if node_id == &candidate_id {
                continue; // Skip self
            }

            println!("Election: Sending request_vote to {} at {}", node_id, address);

            let request = RequestVoteRequest {
                term: current_term,
                candidate_id: candidate_id.clone(),
                last_log_index: last_log_entry.index,
                last_log_term: LogTerm::from(last_log_entry.term),
            };

            let rpc_client = Arc::clone(rpc_client);
            let node_id_clone = node_id.clone();
            let address = address.clone();

            let task = tokio::spawn(async move {
                let result = rpc_client.request_vote(&address, request).await;
                println!("Election: request_vote result for {}: {:?}", node_id_clone, result);
                result
            });

            vote_tasks.push((node_id, task));
        }

        // Collect votes
        let mut votes_received = 1; // Vote for self
        let mut higher_term = None;

        for (node_id, task) in vote_tasks {
            match task.await {
                Ok(Ok(response)) => {
                    if response.term > current_term {
                        higher_term = Some(response.term);
                        println!("Election: Found higher term {} from {}", response.term, node_id);
                        break;
                    } else if response.vote_granted {
                        votes_received += 1;
                        println!("Election: Received vote from node: {} (total: {})", node_id, votes_received);
                    } else {
                        println!("Election: Vote denied by node: {}", node_id);
                    }
                }
                Ok(Err(e)) => {
                    println!("Election: Failed to get vote from node {}: {}", node_id, e);
                }
                Err(e) => {
                    println!("Election: Task failed for node {}: {}", node_id, e);
                }
            }
        }

        // Check if we found a higher term
        if let Some(higher_term) = higher_term {
            let mut state_guard = state.write().await;
            state_guard.update_term(higher_term);
            state_guard.end_election();
            println!("Election: Found higher term {}, becoming follower", higher_term);
            return Ok(());
        }

        // Check if we won the election
        if votes_received > (members.len() as u32 / 2) {
            let mut state_guard = state.write().await;
            state_guard.become_leader();
            state_guard.end_election();
            println!("Election: Won election with {} votes, becoming leader", votes_received);
        } else {
            // Election failed, increment term and try again
            let mut state_guard = state.write().await;
            state_guard.update_term(RaftTerm(current_term.value() + 1));
            state_guard.end_election();
            println!("Election: Election failed with {} votes, incrementing term to {}", votes_received, current_term.value() + 1);
        }

        Ok(())
    }

    /// Get election statistics
    pub async fn get_stats(&self, state: &Arc<RwLock<RaftState>>) -> ElectionStats {
        let state_guard = state.read().await;
        ElectionStats {
            election_round: state_guard.election_round,
            votes_received: state_guard.votes_received,
            total_nodes: state_guard.total_nodes,
            election_in_progress: state_guard.election_in_progress,
            current_timeout_ms: self.timeout.current_timeout_ms,
        }
    }
}

/// Election statistics
#[derive(Debug, Clone)]
pub struct ElectionStats {
    pub election_round: u64,
    pub votes_received: u32,
    pub total_nodes: u32,
    pub election_in_progress: bool,
    pub current_timeout_ms: u64,
}

impl std::fmt::Display for ElectionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Election Stats: round={}, votes={}/{}, in_progress={}, timeout={}ms",
            self.election_round,
            self.votes_received,
            self.total_nodes,
            self.election_in_progress,
            self.current_timeout_ms
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::communication::{MockRaftClient, RpcTimeout};

    #[test]
    fn test_election_timeout_creation() {
        let timeout = ElectionTimeout::new();
        assert_eq!(timeout.min_timeout_ms, 150);
        assert_eq!(timeout.max_timeout_ms, 300);
        assert_eq!(timeout.current_timeout_ms, 150);
    }

    #[test]
    fn test_election_timeout_reset() {
        let mut timeout = ElectionTimeout::with_timeouts(100, 200);
        let original_timeout = timeout.current_timeout_ms;
        timeout.reset();
        
        // Should be different (random)
        assert_ne!(timeout.current_timeout_ms, original_timeout);
        assert!(timeout.current_timeout_ms >= 100);
        assert!(timeout.current_timeout_ms <= 200);
    }

    #[tokio::test]
    async fn test_election_manager_creation() {
        let timeout = RpcTimeout::default();
        let rpc_client = Arc::new(MockRaftClient::new(timeout));
        let members = HashMap::new();
        
        let manager = ElectionManager::new(rpc_client, members);
        assert_eq!(manager.timeout.current_timeout_ms, 150);
    }

    #[tokio::test]
    async fn test_election_stats() {
        let timeout = RpcTimeout::default();
        let rpc_client = Arc::new(MockRaftClient::new(timeout));
        let members = HashMap::new();
        
        let manager = ElectionManager::new(rpc_client, members);
        let stats = manager.get_stats(&Arc::new(RwLock::new(RaftState::new("node1".to_string())))).await;
        
        assert_eq!(stats.election_round, 0);
        assert_eq!(stats.total_nodes, 0);
        assert!(!stats.election_in_progress);
    }
} 