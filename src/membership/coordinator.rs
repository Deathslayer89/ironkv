//! Membership change coordination for Phase 5
//! 
//! This module handles consensus-based membership changes, ensuring
//! safety and consistency during cluster membership modifications.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use serde::{Deserialize, Serialize};

use super::{
    MembershipState, MemberInfo, MemberStatus, HealthStatus,
    MembershipChange, MembershipChangeType, ChangeStatus,
};
use crate::consensus::{RaftConsensus, RaftTerm};
use crate::config::ClusterConfig;
use crate::log::log_cluster_operation;
use crate::metrics::MetricsCollector;

/// Membership change proposal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeProposal {
    /// Proposal ID
    pub proposal_id: String,
    /// Membership change
    pub change: MembershipChange,
    /// Proposer node ID
    pub proposer: String,
    /// Proposal timestamp
    pub timestamp: i64,
    /// Required quorum size
    pub quorum_size: usize,
    /// Current votes
    pub votes: HashMap<String, Vote>,
    /// Proposal status
    pub status: ProposalStatus,
}

/// Vote on a membership change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vote {
    /// Voter node ID
    pub voter: String,
    /// Vote value
    pub value: VoteValue,
    /// Vote timestamp
    pub timestamp: i64,
    /// Vote reason (optional)
    pub reason: Option<String>,
}

/// Vote value
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VoteValue {
    /// Approve the change
    Approve,
    /// Reject the change
    Reject,
    /// Abstain from voting
    Abstain,
}

/// Proposal status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ProposalStatus {
    /// Proposal is pending
    Pending,
    /// Proposal is being voted on
    Voting,
    /// Proposal was approved
    Approved,
    /// Proposal was rejected
    Rejected,
    /// Proposal timed out
    TimedOut,
    /// Proposal is being applied
    Applying,
    /// Proposal was applied successfully
    Applied,
    /// Proposal failed to apply
    Failed,
}

/// Membership change coordinator
pub struct ChangeCoordinator {
    /// Node ID
    node_id: String,
    /// Membership state
    state: Arc<RwLock<MembershipState>>,
    /// Raft consensus for coordination
    consensus: Arc<RaftConsensus>,
    /// Configuration
    config: ClusterConfig,
    /// Active proposals
    proposals: HashMap<String, ChangeProposal>,
    /// Proposal timeout
    proposal_timeout: Duration,
    /// Voting timeout
    voting_timeout: Duration,
    /// Control channel for stopping
    stop_tx: Option<mpsc::Sender<()>>,
    /// Background task handle
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ChangeCoordinator {
    /// Create a new change coordinator
    pub fn new(
        node_id: String,
        state: Arc<RwLock<MembershipState>>,
        consensus: Arc<RaftConsensus>,
        config: ClusterConfig,
    ) -> Self {
        Self {
            node_id,
            state,
            consensus,
            config,
            proposals: HashMap::new(),
            proposal_timeout: Duration::from_secs(300), // 5 minutes
            voting_timeout: Duration::from_secs(60),    // 1 minute
            stop_tx: None,
            task_handle: None,
        }
    }

    /// Start the change coordinator
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Starting change coordinator for node: {}", self.node_id);

        let (stop_tx, stop_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);

        let node_id = self.node_id.clone();
        let state = Arc::clone(&self.state);
        let consensus = Arc::clone(&self.consensus);
        let config = self.config.clone();
        let proposal_timeout = self.proposal_timeout;
        let voting_timeout = self.voting_timeout;

        let handle = tokio::spawn(async move {
            Self::run_coordination_loop(
                node_id,
                state,
                consensus,
                config,
                proposal_timeout,
                voting_timeout,
                stop_rx,
            )
            .await;
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    /// Stop the change coordinator
    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Stopping change coordinator for node: {}", self.node_id);

        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(()).await;
        }

        if let Some(handle) = self.task_handle.take() {
            handle.await?;
        }

        Ok(())
    }

    /// Run the main coordination loop
    async fn run_coordination_loop(
        node_id: String,
        state: Arc<RwLock<MembershipState>>,
        consensus: Arc<RaftConsensus>,
        config: ClusterConfig,
        proposal_timeout: Duration,
        voting_timeout: Duration,
        mut stop_rx: mpsc::Receiver<()>,
    ) {
        let mut timer = interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                _ = timer.tick() => {
                    if let Err(e) = Self::process_proposals(&node_id, &state, &consensus, proposal_timeout, voting_timeout).await {
                        tracing::error!("Proposal processing error: {}", e);
                    }
                }
                _ = stop_rx.recv() => {
                    tracing::info!("Change coordinator stopping");
                    break;
                }
            }
        }
    }

    /// Process active proposals
    async fn process_proposals(
        node_id: &str,
        state: &Arc<RwLock<MembershipState>>,
        consensus: &Arc<RaftConsensus>,
        proposal_timeout: Duration,
        voting_timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut state_guard = state.write().await;
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

        // Process pending changes
        let mut completed_changes = Vec::new();
        for change in &mut state_guard.pending_changes {
            match change.status {
                ChangeStatus::Pending => {
                    // Start voting process
                    change.status = ChangeStatus::InProgress;
                }
                ChangeStatus::InProgress => {
                    // Check if change has timed out
                    let time_since_timestamp = current_time - change.timestamp;
                    if time_since_timestamp > proposal_timeout.as_secs() as i64 {
                        change.status = ChangeStatus::Failed;
                        completed_changes.push(change.change_id.clone());
                    }
                }
                ChangeStatus::Completed => {
                    completed_changes.push(change.change_id.clone());
                }
                ChangeStatus::Failed => {
                    completed_changes.push(change.change_id.clone());
                }
                _ => {}
            }
        }

        // Remove completed changes
        state_guard.pending_changes.retain(|change| {
            !completed_changes.contains(&change.change_id)
        });

        Ok(())
    }

    /// Propose a membership change
    pub async fn propose_change(
        &self,
        change: MembershipChange,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        let current_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let change_id = change.change_id.clone();

        // Create proposal
        let proposal = ChangeProposal {
            proposal_id: format!("proposal_{}_{}", change_id, start_time.elapsed().as_millis()),
            change: change.clone(),
            proposer: self.node_id.clone(),
            timestamp: current_timestamp,
            quorum_size: self.calculate_quorum_size().await,
            votes: HashMap::new(),
            status: ProposalStatus::Pending,
        };

        // Add to pending changes
        {
            let mut state_guard = self.state.write().await;
            state_guard.pending_changes.push(change);
        }

        // Submit to consensus if we're the leader
        if self.consensus.is_leader().await {
            let command = serde_json::to_vec(&proposal)?;
            self.consensus.submit_command(command).await?;
        }

        // Record metrics
        let duration = start_time.elapsed();
        // TODO: Add metrics recording

        // Log operation
        log_cluster_operation(
            "membership_propose_change",
            &self.node_id,
            true,
            duration,
            Some(vec![("change_id", change_id)]),
        );

        Ok(())
    }

    /// Vote on a membership change proposal
    pub async fn vote_on_proposal(
        &self,
        proposal_id: &str,
        vote: VoteValue,
        reason: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let vote_record = Vote {
            voter: self.node_id.clone(),
            value: vote.clone(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            reason,
        };

        // TODO: Send vote to proposal coordinator
        tracing::debug!("Voting {:?} on proposal {}", vote, proposal_id);

        Ok(())
    }

    /// Apply a membership change
    pub async fn apply_change(
        &self,
        change: &MembershipChange,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();

        let mut state_guard = self.state.write().await;

        match change.change_type {
            MembershipChangeType::AddNode => {
                // Add node to membership
                let member_info = MemberInfo {
                    node_id: change.node_id.clone(),
                    address: "".to_string(), // TODO: Get from change
                    port: 0,                 // TODO: Get from change
                    status: MemberStatus::Joining,
                    last_seen: change.timestamp,
                    health: HealthStatus::Unknown,
                    datacenter: None,
                };
                state_guard.members.insert(change.node_id.clone(), member_info);
            }
            MembershipChangeType::RemoveNode => {
                // Mark node as leaving
                if let Some(member) = state_guard.members.get_mut(&change.node_id) {
                    member.status = MemberStatus::Leaving;
                    member.last_seen = change.timestamp;
                }
            }
            MembershipChangeType::UpdateNode => {
                // Update node information
                if let Some(member) = state_guard.members.get_mut(&change.node_id) {
                    member.last_seen = change.timestamp;
                }
            }
            MembershipChangeType::SuspendNode => {
                // Suspend node
                if let Some(member) = state_guard.members.get_mut(&change.node_id) {
                    member.status = MemberStatus::Suspended;
                    member.last_seen = change.timestamp;
                }
            }
            MembershipChangeType::ResumeNode => {
                // Resume suspended node
                if let Some(member) = state_guard.members.get_mut(&change.node_id) {
                    member.status = MemberStatus::Active;
                    member.last_seen = change.timestamp;
                }
            }
        }

        state_guard.version += 1;
        state_guard.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

        // Log operation
        let duration = start_time.elapsed();
        log_cluster_operation(
            "membership_apply_change",
            &self.node_id,
            true,
            duration,
            Some(vec![("change_id", change.change_id.clone())]),
        );

        Ok(())
    }

    /// Rollback a membership change
    pub async fn rollback_change(
        &self,
        change: &MembershipChange,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();

        let mut state_guard = self.state.write().await;

        match change.change_type {
            MembershipChangeType::AddNode => {
                // Remove the added node
                state_guard.members.remove(&change.node_id);
            }
            MembershipChangeType::RemoveNode => {
                // Restore the removed node
                if let Some(member) = state_guard.members.get_mut(&change.node_id) {
                    member.status = MemberStatus::Active;
                    member.last_seen = change.timestamp;
                }
            }
            MembershipChangeType::UpdateNode => {
                // Revert the update
                if let Some(member) = state_guard.members.get_mut(&change.node_id) {
                    member.last_seen = change.timestamp;
                }
            }
            MembershipChangeType::SuspendNode => {
                // Resume the suspended node
                if let Some(member) = state_guard.members.get_mut(&change.node_id) {
                    member.status = MemberStatus::Active;
                    member.last_seen = change.timestamp;
                }
            }
            MembershipChangeType::ResumeNode => {
                // Re-suspend the resumed node
                if let Some(member) = state_guard.members.get_mut(&change.node_id) {
                    member.status = MemberStatus::Suspended;
                    member.last_seen = change.timestamp;
                }
            }
        }

        state_guard.version += 1;
        state_guard.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

        // Log operation
        let duration = start_time.elapsed();
        log_cluster_operation(
            "membership_rollback_change",
            &self.node_id,
            true,
            duration,
            Some(vec![("change_id", change.change_id.clone())]),
        );

        Ok(())
    }

    /// Calculate required quorum size
    async fn calculate_quorum_size(&self) -> usize {
        let state_guard = self.state.read().await;
        let total_members = state_guard.members.len() + 1; // +1 for self
        (total_members / 2) + 1 // Majority
    }

    /// Get active proposals
    pub fn get_active_proposals(&self) -> Vec<&ChangeProposal> {
        self.proposals.values().collect()
    }

    /// Get proposal by ID
    pub fn get_proposal(&self, proposal_id: &str) -> Option<&ChangeProposal> {
        self.proposals.get(proposal_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;

    #[tokio::test]
    async fn test_change_coordinator_creation() {
        let config = ClusterConfig::default();
        let state = Arc::new(RwLock::new(MembershipState {
            version: 1,
            members: HashMap::new(),
            pending_changes: Vec::new(),
            last_update: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        }));

        // Create mock consensus
        let consensus = Arc::new(RaftConsensus::new(
            "node1".to_string(),
            config.clone(),
            Arc::new(MetricsCollector::new(crate::config::MetricsConfig::default())),
        ));

        let coordinator = ChangeCoordinator::new("node1".to_string(), state, consensus, config);
        assert_eq!(coordinator.node_id, "node1");
    }

    #[test]
    fn test_change_proposal_creation() {
        let change = MembershipChange {
            change_id: "test_change".to_string(),
            change_type: MembershipChangeType::AddNode,
            node_id: "node1".to_string(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            status: ChangeStatus::Pending,
            consensus_term: RaftTerm(1),
        };

        let proposal = ChangeProposal {
            proposal_id: "test_proposal".to_string(),
            change,
            proposer: "node1".to_string(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            quorum_size: 3,
            votes: HashMap::new(),
            status: ProposalStatus::Pending,
        };

        assert_eq!(proposal.proposal_id, "test_proposal");
        assert_eq!(proposal.status, ProposalStatus::Pending);
    }

    #[test]
    fn test_vote_creation() {
        let vote = Vote {
            voter: "node1".to_string(),
            value: VoteValue::Approve,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            reason: Some("Node looks healthy".to_string()),
        };

        assert_eq!(vote.voter, "node1");
        assert_eq!(vote.value, VoteValue::Approve);
        assert!(vote.reason.is_some());
    }
} 