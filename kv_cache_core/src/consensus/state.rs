//! Raft state management for the consensus system
//! 
//! This module defines the core Raft state structures and types used
//! throughout the consensus implementation.

use crate::consensus::log::RaftLog;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Raft term identifier
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
pub struct RaftTerm(pub u64);

impl RaftTerm {
    /// Increment the term
    pub fn increment(&mut self) {
        self.0 += 1;
    }

    /// Get the term value
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for RaftTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Raft node roles
#[derive(Debug, Clone, PartialEq)]
pub enum RaftRole {
    /// Follower role - receives requests from leaders and candidates
    Follower,
    /// Candidate role - participates in leader election
    Candidate,
    /// Leader role - handles all client requests and log replication
    Leader,
}

impl std::fmt::Display for RaftRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RaftRole::Follower => write!(f, "Follower"),
            RaftRole::Candidate => write!(f, "Candidate"),
            RaftRole::Leader => write!(f, "Leader"),
        }
    }
}

/// Complete Raft state for a node
#[derive(Debug, Clone)]
pub struct RaftState {
    /// Node ID
    pub node_id: String,
    /// Current role (Follower, Candidate, Leader)
    pub role: RaftRole,
    /// Current term number
    pub current_term: RaftTerm,
    /// ID of the current leader (if any)
    pub leader_id: Option<String>,
    /// ID of the candidate that received vote in current term
    pub voted_for: Option<String>,
    /// Index of highest log entry known to be committed
    pub commit_index: crate::consensus::log::LogIndex,
    /// Index of highest log entry applied to state machine
    pub last_applied: crate::consensus::log::LogIndex,
    /// Raft log
    pub log: Arc<RwLock<RaftLog>>,
    /// Election timeout in milliseconds
    pub election_timeout_ms: u64,
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,
    /// Last time we received a message from the leader
    pub last_leader_contact: Option<std::time::Instant>,
    /// Number of votes received in current election
    pub votes_received: u32,
    /// Total number of nodes in the cluster
    pub total_nodes: u32,
    pub election_round: u64,
    pub election_start_time: Option<std::time::Instant>,
    pub election_in_progress: bool,
}

impl RaftState {
    /// Create a new Raft state
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            role: RaftRole::Follower,
            current_term: RaftTerm(0),
            leader_id: None,
            voted_for: None,
            commit_index: crate::consensus::log::LogIndex(0),
            last_applied: crate::consensus::log::LogIndex(0),
            log: Arc::new(RwLock::new(RaftLog::new())),
            election_timeout_ms: 150, // 150ms default
            heartbeat_interval_ms: 50, // 50ms default
            last_leader_contact: None,
            votes_received: 0,
            total_nodes: 1,
            election_round: 0,
            election_start_time: None,
            election_in_progress: false,
        }
    }

    /// Check if the node is a follower
    pub fn is_follower(&self) -> bool {
        matches!(self.role, RaftRole::Follower)
    }

    /// Check if the node is a candidate
    pub fn is_candidate(&self) -> bool {
        matches!(self.role, RaftRole::Candidate)
    }

    /// Check if the node is a leader
    pub fn is_leader(&self) -> bool {
        matches!(self.role, RaftRole::Leader)
    }

    /// Become a follower
    pub fn become_follower(&mut self, term: RaftTerm, leader_id: Option<String>) {
        self.role = RaftRole::Follower;
        self.current_term = term;
        self.leader_id = leader_id;
        self.voted_for = None;
        self.votes_received = 0;
        self.last_leader_contact = Some(std::time::Instant::now());
    }

    /// Become a candidate
    pub fn become_candidate(&mut self) {
        self.role = RaftRole::Candidate;
        self.current_term.increment();
        self.leader_id = None;
        self.voted_for = Some(self.node_id.clone());
        self.votes_received = 1; // Vote for self
        self.last_leader_contact = None;
        self.start_election();
    }

    /// Become a leader
    pub fn become_leader(&mut self) {
        self.role = RaftRole::Leader;
        self.leader_id = Some(self.node_id.clone());
        self.votes_received = 0;
        self.last_leader_contact = Some(std::time::Instant::now());
    }

    /// Check if election timeout has expired
    pub fn is_election_timeout_expired(&self, timeout: std::time::Duration) -> bool {
        if let Some(last_contact) = self.last_leader_contact {
            last_contact.elapsed() > timeout
        } else {
            true
        }
    }

    /// Update leader contact time
    pub fn update_leader_contact(&mut self) {
        self.last_leader_contact = Some(std::time::Instant::now());
    }

    /// Add a vote in current election
    pub fn add_vote(&mut self) {
        self.votes_received += 1;
    }

    /// Check if we have majority of votes
    pub fn has_majority(&self) -> bool {
        self.votes_received > (self.total_nodes / 2)
    }

    /// Get the majority threshold
    pub fn majority_threshold(&self) -> u32 {
        (self.total_nodes / 2) + 1
    }

    /// Check if we can vote for a candidate
    pub fn can_vote_for(&self, candidate_id: &str) -> bool {
        self.voted_for.is_none() || self.voted_for.as_ref() == Some(&candidate_id.to_string())
    }

    /// Vote for a candidate
    pub fn vote_for(&mut self, candidate_id: String) {
        self.voted_for = Some(candidate_id);
    }

    /// Update commit index
    pub fn update_commit_index(&mut self, new_commit_index: crate::consensus::log::LogIndex) {
        if new_commit_index > self.commit_index {
            self.commit_index = new_commit_index;
        }
    }

    /// Update last applied index
    pub fn update_last_applied(&mut self, new_last_applied: crate::consensus::log::LogIndex) {
        if new_last_applied > self.last_applied {
            self.last_applied = new_last_applied;
        }
    }

    /// Get the next index for log entries
    pub fn get_next_log_index(&self) -> crate::consensus::log::LogIndex {
        // This will be implemented when we have access to the log
        crate::consensus::log::LogIndex(0)
    }

    /// Check if a term is stale
    pub fn is_term_stale(&self, term: RaftTerm) -> bool {
        term < self.current_term
    }

    /// Check if a term is newer
    pub fn is_term_newer(&self, term: RaftTerm) -> bool {
        term > self.current_term
    }

    /// Update to a newer term
    pub fn update_term(&mut self, new_term: RaftTerm) {
        if new_term > self.current_term {
            self.current_term = new_term;
            self.role = RaftRole::Follower;
            self.voted_for = None;
            self.leader_id = None;
            self.votes_received = 0;
        }
    }

    /// Get state summary for debugging
    pub fn get_summary(&self) -> RaftStateSummary {
        RaftStateSummary {
            node_id: self.node_id.clone(),
            role: self.role.clone(),
            current_term: self.current_term,
            leader_id: self.leader_id.clone(),
            voted_for: self.voted_for.clone(),
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            votes_received: self.votes_received,
            total_nodes: self.total_nodes,
            election_timeout_ms: self.election_timeout_ms,
            heartbeat_interval_ms: self.heartbeat_interval_ms,
        }
    }

    pub fn start_election(&mut self) {
        self.election_round += 1;
        self.votes_received = 1; // Vote for self
        self.election_start_time = Some(std::time::Instant::now());
        self.election_in_progress = true;
    }
    pub fn end_election(&mut self) {
        self.election_in_progress = false;
        self.election_start_time = None;
    }
}

/// Summary of Raft state for debugging and monitoring
#[derive(Debug, Clone)]
pub struct RaftStateSummary {
    pub node_id: String,
    pub role: RaftRole,
    pub current_term: RaftTerm,
    pub leader_id: Option<String>,
    pub voted_for: Option<String>,
    pub commit_index: crate::consensus::log::LogIndex,
    pub last_applied: crate::consensus::log::LogIndex,
    pub votes_received: u32,
    pub total_nodes: u32,
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
}

impl std::fmt::Display for RaftStateSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Node: {}, Role: {}, Term: {}, Leader: {:?}, Votes: {}/{}, Commit: {}, Applied: {}",
            self.node_id,
            self.role,
            self.current_term,
            self.leader_id,
            self.votes_received,
            self.total_nodes,
            self.commit_index,
            self.last_applied
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_term_increment() {
        let mut term = RaftTerm(1);
        term.increment();
        assert_eq!(term, RaftTerm(2));
    }

    #[test]
    fn test_raft_term_comparison() {
        let term1 = RaftTerm(1);
        let term2 = RaftTerm(2);
        assert!(term1 < term2);
        assert!(term2 > term1);
        assert_eq!(term1, term1);
    }

    #[test]
    fn test_raft_state_creation() {
        let state = RaftState::new("node1".to_string());
        assert_eq!(state.node_id, "node1");
        assert!(state.is_follower());
        assert_eq!(state.current_term, RaftTerm(0));
        assert_eq!(state.votes_received, 0);
    }

    #[test]
    fn test_raft_state_role_transitions() {
        let mut state = RaftState::new("node1".to_string());
        
        // Start as follower
        assert!(state.is_follower());
        
        // Become candidate
        state.become_candidate();
        assert!(state.is_candidate());
        assert_eq!(state.current_term, RaftTerm(1));
        assert_eq!(state.voted_for, Some("node1".to_string()));
        assert_eq!(state.votes_received, 1);
        
        // Become leader
        state.become_leader();
        assert!(state.is_leader());
        assert_eq!(state.leader_id, Some("node1".to_string()));
        
        // Become follower
        state.become_follower(RaftTerm(2), Some("node2".to_string()));
        assert!(state.is_follower());
        assert_eq!(state.current_term, RaftTerm(2));
        assert_eq!(state.leader_id, Some("node2".to_string()));
        assert_eq!(state.voted_for, None);
    }

    #[test]
    fn test_raft_state_voting() {
        let mut state = RaftState::new("node1".to_string());
        state.total_nodes = 3;
        
        // Can vote initially
        assert!(state.can_vote_for("node2"));
        state.vote_for("node2".to_string());
        assert_eq!(state.voted_for, Some("node2".to_string()));
        
        // Can't vote for different candidate
        assert!(!state.can_vote_for("node3"));
        
        // Can vote for same candidate
        assert!(state.can_vote_for("node2"));
    }

    #[test]
    fn test_raft_state_majority() {
        let mut state = RaftState::new("node1".to_string());
        state.total_nodes = 3;
        
        // Need 2 votes for majority (3/2 + 1 = 2)
        assert_eq!(state.majority_threshold(), 2);
        
        state.add_vote();
        assert!(!state.has_majority());
        
        state.add_vote();
        assert!(state.has_majority());
    }

    #[test]
    fn test_raft_state_term_management() {
        let mut state = RaftState::new("node1".to_string());
        
        // Check term staleness - a term is stale if it's less than current term
        assert!(!state.is_term_stale(RaftTerm(0))); // Same term is not stale
        assert!(!state.is_term_stale(RaftTerm(1))); // Higher term is not stale
        
        // Check term newness
        assert!(state.is_term_newer(RaftTerm(1)));
        assert!(!state.is_term_newer(RaftTerm(0)));
        
        // Update to newer term
        state.update_term(RaftTerm(2));
        assert_eq!(state.current_term, RaftTerm(2));
        assert!(state.is_follower());
        assert_eq!(state.voted_for, None);
        
        // Now check staleness with new term
        assert!(state.is_term_stale(RaftTerm(0))); // Term 0 is now stale compared to term 2
        assert!(state.is_term_stale(RaftTerm(1))); // Term 1 is now stale compared to term 2
        assert!(!state.is_term_stale(RaftTerm(2))); // Same term is not stale
        assert!(!state.is_term_stale(RaftTerm(3))); // Higher term is not stale
    }

    #[test]
    fn test_raft_state_summary() {
        let state = RaftState::new("node1".to_string());
        let summary = state.get_summary();
        
        assert_eq!(summary.node_id, "node1");
        assert!(matches!(summary.role, RaftRole::Follower));
        assert_eq!(summary.current_term, RaftTerm(0));
    }
} 