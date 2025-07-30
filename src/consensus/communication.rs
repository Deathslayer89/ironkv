//! Raft communication protocol for the consensus system
//! 
//! This module defines the RPC structures and communication protocols
//! used by the Raft consensus algorithm.

use crate::consensus::log::{LogEntry, LogIndex, LogTerm};
use crate::consensus::state::RaftTerm;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Raft RPC trait for communication between nodes
#[async_trait::async_trait]
pub trait RaftRpc {
    /// Send a RequestVote RPC to a follower
    async fn request_vote(
        &self,
        target: &str,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>>;

    /// Send an AppendEntries RPC to a follower
    async fn append_entries(
        &self,
        target: &str,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error + Send + Sync>>;

    /// Send an InstallSnapshot RPC to a follower
    async fn install_snapshot(
        &self,
        target: &str,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, Box<dyn std::error::Error + Send + Sync>>;
}

/// RequestVote RPC request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    /// Candidate's term
    pub term: RaftTerm,
    /// Candidate requesting vote
    pub candidate_id: String,
    /// Index of candidate's last log entry
    pub last_log_index: LogIndex,
    /// Term of candidate's last log entry
    pub last_log_term: LogTerm,
}

/// RequestVote RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// Current term, for candidate to update itself
    pub term: RaftTerm,
    /// True means candidate received vote
    pub vote_granted: bool,
}

/// AppendEntries RPC request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    /// Leader's term
    pub term: RaftTerm,
    /// Leader ID
    pub leader_id: String,
    /// Index of log entry immediately preceding new ones
    pub prev_log_index: LogIndex,
    /// Term of prev_log_index entry
    pub prev_log_term: LogTerm,
    /// Log entries to store (empty for heartbeat)
    pub entries: Vec<LogEntry>,
    /// Leader's commit index
    pub leader_commit: LogIndex,
}

/// AppendEntries RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// Current term, for leader to update itself
    pub term: RaftTerm,
    /// True if follower contained entry matching prev_log_index and prev_log_term
    pub success: bool,
    /// Index of the last log entry that was successfully replicated
    pub last_log_index: Option<LogIndex>,
    /// Conflict information for optimization
    pub conflict_index: Option<LogIndex>,
    pub conflict_term: Option<LogTerm>,
}

/// InstallSnapshot RPC request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    /// Leader's term
    pub term: RaftTerm,
    /// Leader ID
    pub leader_id: String,
    /// Last included index in the snapshot
    pub last_included_index: LogIndex,
    /// Last included term in the snapshot
    pub last_included_term: LogTerm,
    /// Offset of the chunk in the snapshot
    pub offset: u64,
    /// Raw bytes of the snapshot chunk
    pub data: Vec<u8>,
    /// True if this is the last chunk
    pub done: bool,
}

/// InstallSnapshot RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    /// Current term, for leader to update itself
    pub term: RaftTerm,
    /// True if snapshot was successfully installed
    pub success: bool,
}

/// RPC timeout configuration
#[derive(Debug, Clone)]
pub struct RpcTimeout {
    /// Request timeout
    pub request_timeout: Duration,
    /// Heartbeat timeout
    pub heartbeat_timeout: Duration,
    /// Election timeout
    pub election_timeout: Duration,
}

impl Default for RpcTimeout {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_millis(100),
            heartbeat_timeout: Duration::from_millis(50),
            election_timeout: Duration::from_millis(150),
        }
    }
}

/// RPC statistics
#[derive(Debug, Clone, Default)]
pub struct RpcStats {
    /// Number of successful RPCs
    pub successful_rpcs: u64,
    /// Number of failed RPCs
    pub failed_rpcs: u64,
    /// Total RPC latency
    pub total_latency: Duration,
    /// Number of timeout errors
    pub timeout_errors: u64,
    /// Number of network errors
    pub network_errors: u64,
}

impl RpcStats {
    /// Record a successful RPC
    pub fn record_success(&mut self, latency: Duration) {
        self.successful_rpcs += 1;
        self.total_latency += latency;
    }

    /// Record a failed RPC
    pub fn record_failure(&mut self, is_timeout: bool) {
        self.failed_rpcs += 1;
        if is_timeout {
            self.timeout_errors += 1;
        } else {
            self.network_errors += 1;
        }
    }

    /// Get average latency
    pub fn average_latency(&self) -> Duration {
        if self.successful_rpcs > 0 {
            self.total_latency / self.successful_rpcs as u32
        } else {
            Duration::ZERO
        }
    }

    /// Get success rate
    pub fn success_rate(&self) -> f64 {
        let total = self.successful_rpcs + self.failed_rpcs;
        if total > 0 {
            self.successful_rpcs as f64 / total as f64
        } else {
            0.0
        }
    }
}

/// RPC client implementation using gRPC
pub struct GrpcRaftClient {
    /// gRPC client
    client: tonic::transport::Channel,
    /// Timeout configuration
    timeout: RpcTimeout,
    /// Statistics
    stats: RpcStats,
}

impl GrpcRaftClient {
    /// Create a new gRPC Raft client
    pub async fn new(
        endpoint: String,
        timeout: RpcTimeout,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = tonic::transport::Channel::from_shared(endpoint)?
            .timeout(timeout.request_timeout)
            .connect()
            .await?;

        Ok(Self {
            client,
            timeout,
            stats: RpcStats::default(),
        })
    }

    /// Get RPC statistics
    pub fn get_stats(&self) -> RpcStats {
        self.stats.clone()
    }
}

#[async_trait::async_trait]
impl RaftRpc for GrpcRaftClient {
    async fn request_vote(
        &self,
        _target: &str,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>> {
        let start_time = std::time::Instant::now();
        
        // In a real implementation, this would use the actual gRPC service
        // For now, we'll simulate the RPC call
        
        // Simulate network delay
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Simulate response
        let response = RequestVoteResponse {
            term: request.term,
            vote_granted: true, // Simplified for demo
        };
        
        let _latency = start_time.elapsed();
        // Note: We can't mutate self.stats here due to &self, but in real impl we would
        
        Ok(response)
    }

    async fn append_entries(
        &self,
        _target: &str,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error + Send + Sync>> {
        let start_time = std::time::Instant::now();
        
        // Simulate network delay
        tokio::time::sleep(Duration::from_millis(5)).await;
        
        // Simulate response
        let response = AppendEntriesResponse {
            term: request.term,
            success: true,
            last_log_index: Some(LogIndex(0)),
            conflict_index: None,
            conflict_term: None,
        };
        
        let _latency = start_time.elapsed();
        
        Ok(response)
    }

    async fn install_snapshot(
        &self,
        _target: &str,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, Box<dyn std::error::Error + Send + Sync>> {
        let start_time = std::time::Instant::now();
        
        // Simulate network delay
        tokio::time::sleep(Duration::from_millis(20)).await;
        
        // Simulate response
        let response = InstallSnapshotResponse {
            term: request.term,
            success: true,
        };
        
        let _latency = start_time.elapsed();
        
        Ok(response)
    }
}

/// Mock RPC client for testing
pub struct MockRaftClient {
    /// Mock responses
    responses: std::collections::HashMap<String, MockResponse>,
    /// Timeout configuration
    timeout: RpcTimeout,
}

/// Mock response for testing
#[derive(Debug, Clone)]
pub enum MockResponse {
    RequestVote(RequestVoteResponse),
    AppendEntries(AppendEntriesResponse),
    InstallSnapshot(InstallSnapshotResponse),
    Error(String),
    Timeout,
}

impl MockRaftClient {
    /// Create a new mock Raft client
    pub fn new(timeout: RpcTimeout) -> Self {
        Self {
            responses: std::collections::HashMap::new(),
            timeout,
        }
    }

    /// Set a mock response for a specific RPC
    pub fn set_response(&mut self, key: String, response: MockResponse) {
        self.responses.insert(key, response);
    }

    /// Generate a key for the responses map
    fn get_key(&self, rpc_type: &str, target: &str) -> String {
        format!("{}:{}", rpc_type, target)
    }
}

#[async_trait::async_trait]
impl RaftRpc for MockRaftClient {
    async fn request_vote(
        &self,
        target: &str,
        _request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>> {
        let key = self.get_key("request_vote", target);
        
        match self.responses.get(&key) {
            Some(MockResponse::RequestVote(response)) => Ok(response.clone()),
            Some(MockResponse::Error(msg)) => Err(msg.clone().into()),
            Some(MockResponse::Timeout) => {
                tokio::time::sleep(self.timeout.request_timeout).await;
                Err("RPC timeout".into())
            }
            _ => Ok(RequestVoteResponse {
                term: RaftTerm(1),
                vote_granted: true,
            }),
        }
    }

    async fn append_entries(
        &self,
        target: &str,
        _request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error + Send + Sync>> {
        let key = self.get_key("append_entries", target);
        
        match self.responses.get(&key) {
            Some(MockResponse::AppendEntries(response)) => Ok(response.clone()),
            Some(MockResponse::Error(msg)) => Err(msg.clone().into()),
            Some(MockResponse::Timeout) => {
                tokio::time::sleep(self.timeout.request_timeout).await;
                Err("RPC timeout".into())
            }
            _ => Ok(AppendEntriesResponse {
                term: RaftTerm(1),
                success: true,
                last_log_index: Some(LogIndex(0)),
                conflict_index: None,
                conflict_term: None,
            }),
        }
    }

    async fn install_snapshot(
        &self,
        target: &str,
        _request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, Box<dyn std::error::Error + Send + Sync>> {
        let key = self.get_key("install_snapshot", target);
        
        match self.responses.get(&key) {
            Some(MockResponse::InstallSnapshot(response)) => Ok(response.clone()),
            Some(MockResponse::Error(msg)) => Err(msg.clone().into()),
            Some(MockResponse::Timeout) => {
                tokio::time::sleep(self.timeout.request_timeout).await;
                Err("RPC timeout".into())
            }
            _ => Ok(InstallSnapshotResponse {
                term: RaftTerm(1),
                success: true,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_request_vote_request_serialization() {
        let request = RequestVoteRequest {
            term: RaftTerm(1),
            candidate_id: "node1".to_string(),
            last_log_index: LogIndex(5),
            last_log_term: LogTerm(1),
        };

        let serialized = bincode::serialize(&request).unwrap();
        let deserialized: RequestVoteRequest = bincode::deserialize(&serialized).unwrap();

        assert_eq!(request.term, deserialized.term);
        assert_eq!(request.candidate_id, deserialized.candidate_id);
        assert_eq!(request.last_log_index, deserialized.last_log_index);
        assert_eq!(request.last_log_term, deserialized.last_log_term);
    }

    #[tokio::test]
    async fn test_append_entries_request_serialization() {
        let request = AppendEntriesRequest {
            term: RaftTerm(1),
            leader_id: "node1".to_string(),
            prev_log_index: LogIndex(5),
            prev_log_term: LogTerm(1),
            entries: vec![
                LogEntry::new(RaftTerm(1), LogIndex(6), vec![1, 2, 3]),
            ],
            leader_commit: LogIndex(5),
        };

        let serialized = bincode::serialize(&request).unwrap();
        let deserialized: AppendEntriesRequest = bincode::deserialize(&serialized).unwrap();

        assert_eq!(request.term, deserialized.term);
        assert_eq!(request.leader_id, deserialized.leader_id);
        assert_eq!(request.prev_log_index, deserialized.prev_log_index);
        assert_eq!(request.entries.len(), deserialized.entries.len());
    }

    #[tokio::test]
    async fn test_mock_raft_client() {
        let timeout = RpcTimeout::default();
        let mut client = MockRaftClient::new(timeout);

        // Set a mock response
        let response = RequestVoteResponse {
            term: RaftTerm(2),
            vote_granted: false,
        };
        client.set_response(
            "request_vote:node1".to_string(),
            MockResponse::RequestVote(response.clone()),
        );

        // Test the mock response
        let request = RequestVoteRequest {
            term: RaftTerm(1),
            candidate_id: "node2".to_string(),
            last_log_index: LogIndex(5),
            last_log_term: LogTerm(1),
        };

        let result = client.request_vote("node1", request).await.unwrap();
        assert_eq!(result.term, response.term);
        assert_eq!(result.vote_granted, response.vote_granted);
    }

    #[tokio::test]
    async fn test_rpc_stats() {
        let mut stats = RpcStats::default();

        // Record some statistics
        stats.record_success(Duration::from_millis(10));
        stats.record_success(Duration::from_millis(20));
        stats.record_failure(false);
        stats.record_failure(true);

        assert_eq!(stats.successful_rpcs, 2);
        assert_eq!(stats.failed_rpcs, 2);
        assert_eq!(stats.timeout_errors, 1);
        assert_eq!(stats.network_errors, 1);
        assert_eq!(stats.average_latency(), Duration::from_millis(15));
        assert_eq!(stats.success_rate(), 0.5);
    }
} 