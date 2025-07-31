//! Raft leader election management for the consensus system
//! 
//! This module provides leader election logic, timeout management, and
//! election state tracking for the Raft consensus algorithm.

use crate::consensus::communication::{RaftRpc, RequestVoteRequest};
use crate::consensus::log::LogTerm;
use crate::consensus::state::{RaftState};
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
        Self {
            min_timeout_ms: 150,
            max_timeout_ms: 300,
            current_timeout_ms: 150,
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
    /// Election state
    election_state: ElectionState,
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
            election_state: self.election_state.clone(),
            stop_tx: None, // Can't clone sender
            task_handle: None, // Can't clone JoinHandle
        }
    }
}

/// Election state tracking
#[derive(Debug, Clone)]
pub struct ElectionState {
    /// Current election round
    pub election_round: u64,
    /// Number of votes received in current election
    pub votes_received: u32,
    /// Total number of nodes in cluster
    pub total_nodes: u32,
    /// Last election start time
    pub election_start_time: Option<Instant>,
    /// Whether an election is currently in progress
    pub election_in_progress: bool,
    /// Last time we received a message from the leader
    pub last_leader_contact: Option<Instant>,
}

impl ElectionState {
    /// Create a new election state
    pub fn new(total_nodes: u32) -> Self {
        Self {
            election_round: 0,
            votes_received: 0,
            total_nodes,
            election_start_time: None,
            election_in_progress: false,
            last_leader_contact: None,
        }
    }

    /// Start a new election
    pub fn start_election(&mut self) {
        self.election_round += 1;
        self.votes_received = 1; // Vote for self
        self.election_start_time = Some(Instant::now());
        self.election_in_progress = true;
    }

    /// End the current election
    pub fn end_election(&mut self) {
        self.election_in_progress = false;
        self.election_start_time = None;
    }

    /// Add a vote
    pub fn add_vote(&mut self) {
        self.votes_received += 1;
    }

    /// Check if we have majority
    pub fn has_majority(&self) -> bool {
        self.votes_received > (self.total_nodes / 2)
    }

    /// Get majority threshold
    pub fn majority_threshold(&self) -> u32 {
        (self.total_nodes / 2) + 1
    }

    /// Update leader contact time
    pub fn update_leader_contact(&mut self) {
        self.last_leader_contact = Some(Instant::now());
    }

    /// Check if election timeout has expired
    pub fn is_timeout_expired(&self, timeout_duration: Duration) -> bool {
        if let Some(last_contact) = self.last_leader_contact {
            last_contact.elapsed() > timeout_duration
        } else {
            true
        }
    }
}

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
            election_state: ElectionState::new(total_nodes),
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
        let election_state = self.election_state.clone();

        let task_handle = tokio::spawn(async move {
            Self::run_election_loop(state, timeout, rpc_client, members, election_state, stop_rx).await;
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
        mut election_state: ElectionState,
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
                        &mut election_state,
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
        election_state: &mut ElectionState,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut state_guard = state.write().await;

        // Check if we're a follower and timeout has expired
        if state_guard.is_follower() && election_state.is_timeout_expired(timeout.duration()) {
            tracing::info!("Election timeout expired, starting election");
            
            // Become candidate
            state_guard.become_candidate();
            election_state.start_election();
            timeout.reset();

            // Start election process
            drop(state_guard); // Release lock before async call
            Self::run_election(state, rpc_client, members, election_state).await?;
        }

        Ok(())
    }

    /// Run an election
    async fn run_election(
        state: &Arc<RwLock<RaftState>>,
        rpc_client: &Arc<dyn RaftRpc + Send + Sync>,
        members: &HashMap<String, String>,
        election_state: &mut ElectionState,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let state_guard = state.read().await;
        let current_term = state_guard.current_term;
        let candidate_id = state_guard.node_id.clone();
        let last_log_entry = state_guard.log.read().await.get_last_entry().await?;
        drop(state_guard);

        // Send RequestVote to all other nodes
        let mut vote_tasks = Vec::new();

        for (node_id, address) in members {
            if node_id == &candidate_id {
                continue; // Skip self
            }

            let request = RequestVoteRequest {
                term: current_term,
                candidate_id: candidate_id.clone(),
                last_log_index: last_log_entry.index,
                last_log_term: LogTerm::from(last_log_entry.term),
            };

            let rpc_client = Arc::clone(rpc_client);
            let node_id = node_id.clone();
            let address = address.clone();

            let task = tokio::spawn(async move {
                rpc_client.request_vote(&address, request).await
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
                        break;
                    } else if response.vote_granted {
                        votes_received += 1;
                        tracing::info!("Received vote from node: {}", node_id);
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!("Failed to get vote from node {}: {}", node_id, e);
                }
                Err(e) => {
                    tracing::warn!("Task failed for node {}: {}", node_id, e);
                }
            }
        }

        // Check if we found a higher term
        if let Some(higher_term) = higher_term {
            let mut state_guard = state.write().await;
            state_guard.update_term(higher_term);
            election_state.end_election();
            tracing::info!("Found higher term {}, becoming follower", higher_term);
            return Ok(());
        }

        // Check if we won the election
        if votes_received > (members.len() as u32 / 2) {
            let mut state_guard = state.write().await;
            state_guard.become_leader();
            election_state.end_election();
            tracing::info!("Won election with {} votes, becoming leader", votes_received);
        } else {
            // Election failed, stay as candidate
            tracing::info!("Election failed with {} votes, remaining candidate", votes_received);
        }

        Ok(())
    }

    /// Get election statistics
    pub fn get_stats(&self) -> ElectionStats {
        ElectionStats {
            election_round: self.election_state.election_round,
            votes_received: self.election_state.votes_received,
            total_nodes: self.election_state.total_nodes,
            election_in_progress: self.election_state.election_in_progress,
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

    #[test]
    fn test_election_state_creation() {
        let state = ElectionState::new(5);
        assert_eq!(state.election_round, 0);
        assert_eq!(state.votes_received, 0);
        assert_eq!(state.total_nodes, 5);
        assert!(!state.election_in_progress);
    }

    #[test]
    fn test_election_state_start_election() {
        let mut state = ElectionState::new(3);
        state.start_election();
        
        assert_eq!(state.election_round, 1);
        assert_eq!(state.votes_received, 1); // Vote for self
        assert!(state.election_in_progress);
        assert!(state.election_start_time.is_some());
    }

    #[test]
    fn test_election_state_majority() {
        let mut state = ElectionState::new(3);
        
        // Need 2 votes for majority (3/2 + 1 = 2)
        assert_eq!(state.majority_threshold(), 2);
        
        state.add_vote();
        assert!(!state.has_majority());
        
        state.add_vote();
        assert!(state.has_majority());
    }

    #[tokio::test]
    async fn test_election_manager_creation() {
        let timeout = RpcTimeout::default();
        let rpc_client = Arc::new(MockRaftClient::new(timeout));
        let members = HashMap::new();
        
        let manager = ElectionManager::new(rpc_client, members);
        assert_eq!(manager.election_state.total_nodes, 0);
    }

    #[tokio::test]
    async fn test_election_stats() {
        let timeout = RpcTimeout::default();
        let rpc_client = Arc::new(MockRaftClient::new(timeout));
        let members = HashMap::new();
        
        let manager = ElectionManager::new(rpc_client, members);
        let stats = manager.get_stats();
        
        assert_eq!(stats.election_round, 0);
        assert_eq!(stats.total_nodes, 0);
        assert!(!stats.election_in_progress);
    }
} 