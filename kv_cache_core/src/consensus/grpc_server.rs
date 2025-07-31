//! gRPC server implementation for Raft consensus communication
//! 
//! This module provides a gRPC server that handles Raft RPC calls from other nodes
//! in the cluster, including RequestVote, AppendEntries, and InstallSnapshot.

use crate::consensus::communication::{
    RequestVoteRequest, RequestVoteResponse, 
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
};
use crate::consensus::log::{LogEntry, LogIndex, LogTerm};
use crate::consensus::state::{RaftState, RaftTerm};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

// Import generated gRPC types
pub mod raft {
    tonic::include_proto!("raft");
}

use raft::raft_service_server::{RaftService, RaftServiceServer};
use raft::{
    RequestVoteRequest as GrpcRequestVoteRequest,
    RequestVoteResponse as GrpcRequestVoteResponse,
    AppendEntriesRequest as GrpcAppendEntriesRequest,
    AppendEntriesResponse as GrpcAppendEntriesResponse,
    InstallSnapshotRequest as GrpcInstallSnapshotRequest,
    InstallSnapshotResponse as GrpcInstallSnapshotResponse,
    HealthCheckRequest, HealthCheckResponse,
};

/// gRPC server for Raft consensus communication
pub struct RaftGrpcServer {
    /// Current Raft state
    state: Arc<RwLock<RaftState>>,
    /// Node ID
    node_id: String,
}

impl RaftGrpcServer {
    /// Create a new gRPC server
    pub fn new(state: Arc<RwLock<RaftState>>, node_id: String) -> Self {
        Self { state, node_id }
    }

    /// Convert gRPC RequestVoteRequest to internal format
    fn convert_request_vote_request(&self, request: &GrpcRequestVoteRequest) -> RequestVoteRequest {
        RequestVoteRequest {
            term: RaftTerm(request.term),
            candidate_id: request.candidate_id.clone(),
            last_log_index: LogIndex(request.last_log_index),
            last_log_term: LogTerm(request.last_log_term),
        }
    }

    /// Convert internal RequestVoteResponse to gRPC format
    fn convert_request_vote_response(&self, response: RequestVoteResponse) -> GrpcRequestVoteResponse {
        GrpcRequestVoteResponse {
            term: response.term.value(),
            vote_granted: response.vote_granted,
        }
    }

    /// Convert gRPC AppendEntriesRequest to internal format
    fn convert_append_entries_request(&self, request: &GrpcAppendEntriesRequest) -> AppendEntriesRequest {
        let entries: Vec<LogEntry> = request.entries.iter().map(|entry| {
            LogEntry::new(
                RaftTerm(entry.term),
                LogIndex(entry.index),
                entry.command.clone(),
            )
        }).collect();

        AppendEntriesRequest {
            term: RaftTerm(request.term),
            leader_id: request.leader_id.clone(),
            prev_log_index: LogIndex(request.prev_log_index),
            prev_log_term: LogTerm(request.prev_log_term),
            entries,
            leader_commit: LogIndex(request.leader_commit),
        }
    }

    /// Convert internal AppendEntriesResponse to gRPC format
    fn convert_append_entries_response(&self, response: AppendEntriesResponse) -> GrpcAppendEntriesResponse {
        GrpcAppendEntriesResponse {
            term: response.term.value(),
            success: response.success,
            last_log_index: response.last_log_index.map(|idx| idx.value()).unwrap_or(0),
            conflict_index: response.conflict_index.map(|idx| idx.value()).unwrap_or(0),
            conflict_term: response.conflict_term.map(|term| term.value()).unwrap_or(0),
        }
    }

    /// Convert gRPC InstallSnapshotRequest to internal format
    fn convert_install_snapshot_request(&self, request: &GrpcInstallSnapshotRequest) -> InstallSnapshotRequest {
        InstallSnapshotRequest {
            term: RaftTerm(request.term),
            leader_id: request.leader_id.clone(),
            last_included_index: LogIndex(request.last_included_index),
            last_included_term: LogTerm(request.last_included_term),
            offset: request.offset,
            data: request.data.clone(),
            done: request.done,
        }
    }

    /// Convert internal InstallSnapshotResponse to gRPC format
    fn convert_install_snapshot_response(&self, response: InstallSnapshotResponse) -> GrpcInstallSnapshotResponse {
        GrpcInstallSnapshotResponse {
            term: response.term.value(),
            success: response.success,
        }
    }
}

#[tonic::async_trait]
impl RaftService for RaftGrpcServer {
    async fn request_vote(
        &self,
        request: Request<GrpcRequestVoteRequest>,
    ) -> Result<Response<GrpcRequestVoteResponse>, Status> {
        let grpc_request = request.into_inner();
        let internal_request = self.convert_request_vote_request(&grpc_request);
        
        // Handle the request using the consensus system
        let response = self.handle_request_vote(internal_request).await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        let grpc_response = self.convert_request_vote_response(response);
        Ok(Response::new(grpc_response))
    }

    async fn append_entries(
        &self,
        request: Request<GrpcAppendEntriesRequest>,
    ) -> Result<Response<GrpcAppendEntriesResponse>, Status> {
        let grpc_request = request.into_inner();
        let internal_request = self.convert_append_entries_request(&grpc_request);
        
        // Handle the request using the consensus system
        let response = self.handle_append_entries(internal_request).await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        let grpc_response = self.convert_append_entries_response(response);
        Ok(Response::new(grpc_response))
    }

    async fn install_snapshot(
        &self,
        request: Request<GrpcInstallSnapshotRequest>,
    ) -> Result<Response<GrpcInstallSnapshotResponse>, Status> {
        let grpc_request = request.into_inner();
        let internal_request = self.convert_install_snapshot_request(&grpc_request);
        
        // Handle the request using the consensus system
        let response = self.handle_install_snapshot(internal_request).await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        let grpc_response = self.convert_install_snapshot_response(response);
        Ok(Response::new(grpc_response))
    }

    async fn health_check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let _grpc_request = request.into_inner();
        
        let state = self.state.read().await;
        let response = HealthCheckResponse {
            node_id: self.node_id.clone(),
            healthy: true, // TODO: Implement actual health check
            term: state.current_term.value(),
            role: format!("{:?}", state.role),
            timestamp: chrono::Utc::now().timestamp() as u64,
        };
        
        Ok(Response::new(response))
    }
}

impl RaftGrpcServer {
    /// Handle RequestVote RPC
    async fn handle_request_vote(
        &self,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn std::error::Error>> {
        let mut state = self.state.write().await;
        
        // If request term is less than current term, reject
        if request.term < state.current_term {
            return Ok(RequestVoteResponse {
                term: state.current_term,
                vote_granted: false,
            });
        }
        
        // If request term is greater than current term, update term and become follower
        if request.term > state.current_term {
            state.update_term(request.term);
        }
        
        // Check if we can vote for this candidate
        let vote_granted = if state.can_vote_for(&request.candidate_id) {
            // Check if candidate's log is at least as up-to-date as ours
            let last_log_entry = state.log.read().await.get_last_entry().await?;
            let candidate_log_ok = request.last_log_term > LogTerm::from(last_log_entry.term) ||
                (request.last_log_term == LogTerm::from(last_log_entry.term) && 
                 request.last_log_index >= last_log_entry.index);
            
            if candidate_log_ok {
                state.vote_for(request.candidate_id.clone());
                true
            } else {
                false
            }
        } else {
            false
        };
        
        Ok(RequestVoteResponse {
            term: state.current_term,
            vote_granted,
        })
    }

    /// Handle AppendEntries RPC
    async fn handle_append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error>> {
        let mut state = self.state.write().await;
        
        // If request term is less than current term, reject
        if request.term < state.current_term {
            return Ok(AppendEntriesResponse {
                term: state.current_term,
                success: false,
                last_log_index: None,
                conflict_index: None,
                conflict_term: None,
            });
        }
        
        // If request term is greater than current term, update term and become follower
        if request.term > state.current_term {
            state.update_term(request.term);
        }
        
        // Update leader contact time
        state.update_leader_contact();
        
        // If we were candidate or leader, become follower
        if !state.is_follower() {
            state.become_follower(request.term, Some(request.leader_id.clone()));
        }
        
        // Check if previous log entry matches
        let prev_log_ok = {
            let log = state.log.read().await;
            if request.prev_log_index.value() == 0 {
                true
            } else {
                if let Some(entry) = log.get_entry(request.prev_log_index).await? {
                    entry.term == request.prev_log_term.into()
                } else {
                    false
                }
            }
        };
        
        if !prev_log_ok {
            return Ok(AppendEntriesResponse {
                term: state.current_term,
                success: false,
                last_log_index: None,
                conflict_index: Some(request.prev_log_index),
                conflict_term: Some(request.prev_log_term),
            });
        }
        
        // Append new entries and update commit index
        let last_log_index = {
            let mut log = state.log.write().await;
            for entry in request.entries {
                log.append(entry).await?;
            }
            
            log.get_last_index()
        };
        
        // Update commit index outside of the log borrow
        if request.leader_commit > state.commit_index {
            let new_commit_index = std::cmp::min(request.leader_commit, last_log_index);
            state.update_commit_index(new_commit_index);
        }
        
        Ok(AppendEntriesResponse {
            term: state.current_term,
            success: true,
            last_log_index: Some(last_log_index),
            conflict_index: None,
            conflict_term: None,
        })
    }

    /// Handle InstallSnapshot RPC
    async fn handle_install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, Box<dyn std::error::Error>> {
        let mut state = self.state.write().await;
        
        // If request term is less than current term, reject
        if request.term < state.current_term {
            return Ok(InstallSnapshotResponse {
                term: state.current_term,
                success: false,
            });
        }
        
        // If request term is greater than current term, update term and become follower
        if request.term > state.current_term {
            state.update_term(request.term);
        }
        
        // Update leader contact time
        state.update_leader_contact();
        
        // If we were candidate or leader, become follower
        if !state.is_follower() {
            state.become_follower(request.term, Some(request.leader_id.clone()));
        }
        
        // TODO: Implement actual snapshot installation
        // For now, just return success
        Ok(InstallSnapshotResponse {
            term: state.current_term,
            success: true,
        })
    }
}

/// Create a gRPC server instance
pub fn create_raft_server(
    state: Arc<RwLock<RaftState>>,
    node_id: String,
) -> RaftServiceServer<RaftGrpcServer> {
    let server = RaftGrpcServer::new(state, node_id);
    RaftServiceServer::new(server)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::log::RaftLog;

    #[tokio::test]
    async fn test_raft_grpc_server_creation() {
        let state = Arc::new(RwLock::new(RaftState::new("test-node".to_string())));
        let server = RaftGrpcServer::new(state, "test-node".to_string());
        assert_eq!(server.node_id, "test-node");
    }

    #[test]
    fn test_request_vote_conversion() {
        let state = Arc::new(RwLock::new(RaftState::new("test-node".to_string())));
        let server = RaftGrpcServer::new(state, "test-node".to_string());
        
        let grpc_request = GrpcRequestVoteRequest {
            term: 1,
            candidate_id: "node1".to_string(),
            last_log_index: 5,
            last_log_term: 1,
        };
        
        let internal_request = server.convert_request_vote_request(&grpc_request);
        assert_eq!(internal_request.term, RaftTerm(1));
        assert_eq!(internal_request.candidate_id, "node1");
        assert_eq!(internal_request.last_log_index, LogIndex(5));
        assert_eq!(internal_request.last_log_term, LogTerm(1));
    }

    #[test]
    fn test_append_entries_conversion() {
        let state = Arc::new(RwLock::new(RaftState::new("test-node".to_string())));
        let server = RaftGrpcServer::new(state, "test-node".to_string());
        
        let grpc_request = GrpcAppendEntriesRequest {
            term: 1,
            leader_id: "node1".to_string(),
            prev_log_index: 5,
            prev_log_term: 1,
            entries: vec![
                raft::LogEntry {
                    term: 1,
                    index: 6,
                    command: vec![1, 2, 3],
                    timestamp: 1234567890,
                }
            ],
            leader_commit: 5,
        };
        
        let internal_request = server.convert_append_entries_request(&grpc_request);
        assert_eq!(internal_request.term, RaftTerm(1));
        assert_eq!(internal_request.leader_id, "node1");
        assert_eq!(internal_request.entries.len(), 1);
        assert_eq!(internal_request.entries[0].command, vec![1, 2, 3]);
    }
} 