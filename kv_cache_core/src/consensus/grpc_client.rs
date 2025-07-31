//! Real gRPC Raft client implementation for network communication
//! 
//! This module provides a real gRPC client for Raft consensus communication,
//! replacing the mock implementation with actual network calls.

use crate::consensus::communication::{
    RaftRpc, RequestVoteRequest, RequestVoteResponse, 
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    RpcTimeout, RpcStats, NetworkPartitionDetector
};
use crate::consensus::log::{LogIndex, LogTerm};
use crate::consensus::state::RaftTerm;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request};
use tracing::warn;

// Import generated gRPC types
pub mod raft {
    tonic::include_proto!("raft");
}

use raft::raft_service_client::RaftServiceClient;
use raft::{
    RequestVoteRequest as GrpcRequestVoteRequest,
    RequestVoteResponse as GrpcRequestVoteResponse,
    AppendEntriesRequest as GrpcAppendEntriesRequest,
    AppendEntriesResponse as GrpcAppendEntriesResponse,
    InstallSnapshotRequest as GrpcInstallSnapshotRequest,
    InstallSnapshotResponse as GrpcInstallSnapshotResponse,
    LogEntry as GrpcLogEntry,
    HealthCheckRequest,
};

/// Connection health status
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionHealth {
    Healthy,
    Unhealthy,
    Unknown,
}

/// Connection information
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub target: String,
    pub health: ConnectionHealth,
    pub last_heartbeat: Instant,
    pub consecutive_failures: u32,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub average_latency_ms: u64,
    pub last_error: Option<String>,
}

/// Real gRPC Raft client with connection pooling and retry logic
pub struct GrpcRaftClient {
    /// Connection pool for RPC clients
    connections: Arc<RwLock<HashMap<String, RaftServiceClient<Channel>>>>,
    /// Connection health information
    connection_health: Arc<RwLock<HashMap<String, ConnectionInfo>>>,
    /// Network partition detector
    partition_detector: Arc<RwLock<NetworkPartitionDetector>>,
    /// Timeout configuration
    timeout: RpcTimeout,
    /// Statistics tracking
    stats: Arc<RwLock<RpcStats>>,
    /// Connection pool configuration
    max_connections: usize,
    /// Retry configuration
    max_retries: u32,
    /// Retry backoff duration
    retry_backoff: Duration,
    /// Health check interval
    health_check_interval: Duration,
    /// Maximum consecutive failures before marking connection as unhealthy
    max_consecutive_failures: u32,
}

impl GrpcRaftClient {
    /// Create a new gRPC Raft client
    pub fn new(timeout: RpcTimeout) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            connection_health: Arc::new(RwLock::new(HashMap::new())),
            partition_detector: Arc::new(RwLock::new(NetworkPartitionDetector::new(3, Duration::from_secs(60)))),
            timeout,
            stats: Arc::new(RwLock::new(RpcStats::default())),
            max_connections: 100,
            max_retries: 3,
            retry_backoff: Duration::from_millis(100),
            health_check_interval: Duration::from_secs(30),
            max_consecutive_failures: 3,
        }
    }

    /// Get or create a connection to a target node
    async fn get_connection(&self, target: &str) -> Result<RaftServiceClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
        let mut connections = self.connections.write().await;
        
        if let Some(client) = connections.get(target) {
            // Clone the existing connection
            return Ok(client.clone());
        }

        // Create new connection
        let endpoint = Endpoint::from_shared(format!("http://{}", target))?
            .timeout(self.timeout.request_timeout)
            .connect_timeout(Duration::from_secs(5))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(5));

        let channel = endpoint.connect().await?;
        let client = RaftServiceClient::new(channel);
        
        // Store in connection pool
        connections.insert(target.to_string(), client.clone());
        
        // Initialize connection health info
        let mut health_info = self.connection_health.write().await;
        health_info.insert(target.to_string(), ConnectionInfo {
            target: target.to_string(),
            health: ConnectionHealth::Unknown,
            last_heartbeat: Instant::now(),
            consecutive_failures: 0,
            total_requests: 0,
            successful_requests: 0,
            average_latency_ms: 0,
            last_error: None,
        });
        
        // Clean up old connections if pool is full
        if connections.len() > self.max_connections {
            let keys: Vec<String> = connections.keys().cloned().collect();
            for key in keys.iter().take(connections.len() - self.max_connections) {
                connections.remove(key);
            }
        }

        Ok(client)
    }

    /// Convert internal RequestVoteRequest to gRPC format
    fn convert_request_vote_request(&self, request: &RequestVoteRequest) -> GrpcRequestVoteRequest {
        GrpcRequestVoteRequest {
            term: request.term.value(),
            candidate_id: request.candidate_id.clone(),
            last_log_index: request.last_log_index.value(),
            last_log_term: request.last_log_term.value(),
        }
    }

    /// Convert gRPC RequestVoteResponse to internal format
    fn convert_request_vote_response(&self, response: GrpcRequestVoteResponse) -> RequestVoteResponse {
        RequestVoteResponse {
            term: RaftTerm(response.term),
            vote_granted: response.vote_granted,
        }
    }

    /// Convert internal AppendEntriesRequest to gRPC format
    fn convert_append_entries_request(&self, request: &AppendEntriesRequest) -> GrpcAppendEntriesRequest {
        let entries: Vec<GrpcLogEntry> = request.entries.iter().map(|entry| {
            GrpcLogEntry {
                term: entry.term.value(),
                index: entry.index.value(),
                command: entry.command.clone(),
                timestamp: chrono::Utc::now().timestamp() as u64,
            }
        }).collect();

        GrpcAppendEntriesRequest {
            term: request.term.value(),
            leader_id: request.leader_id.clone(),
            prev_log_index: request.prev_log_index.value(),
            prev_log_term: request.prev_log_term.value(),
            entries,
            leader_commit: request.leader_commit.value(),
        }
    }

    /// Convert gRPC AppendEntriesResponse to internal format
    fn convert_append_entries_response(&self, response: GrpcAppendEntriesResponse) -> AppendEntriesResponse {
        AppendEntriesResponse {
            term: RaftTerm(response.term),
            success: response.success,
            last_log_index: Some(LogIndex(response.last_log_index)),
            conflict_index: Some(LogIndex(response.conflict_index)),
            conflict_term: Some(LogTerm(response.conflict_term)),
        }
    }

    /// Convert internal InstallSnapshotRequest to gRPC format
    fn convert_install_snapshot_request(&self, request: &InstallSnapshotRequest) -> GrpcInstallSnapshotRequest {
        GrpcInstallSnapshotRequest {
            term: request.term.value(),
            leader_id: request.leader_id.clone(),
            last_included_index: request.last_included_index.value(),
            last_included_term: request.last_included_term.value(),
            offset: request.offset,
            data: request.data.clone(),
            done: request.done,
        }
    }

    /// Convert gRPC InstallSnapshotResponse to internal format
    fn convert_install_snapshot_response(&self, response: GrpcInstallSnapshotResponse) -> InstallSnapshotResponse {
        InstallSnapshotResponse {
            term: RaftTerm(response.term),
            success: response.success,
        }
    }

    /// Execute RPC with retry logic
    async fn execute_with_retry<F, T, E>(
        &self,
        target: &str,
        operation: F,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut last_error: Option<E> = None;
        let mut attempt = 0;

        while attempt < self.max_retries {
            attempt += 1;
            let start = Instant::now();

            match operation().await {
                Ok(result) => {
                    // Record success
                    let latency = start.elapsed();
                    let mut stats = self.stats.write().await;
                    stats.record_success(latency);
                    
                    // Record success in partition detector
                    let mut detector = self.partition_detector.write().await;
                    detector.record_success(target);
                    
                    return Ok(result);
                }
                Err(e) => {
                    let error_string = e.to_string();
                    last_error = Some(e);
                    let _latency = start.elapsed();
                    let mut stats = self.stats.write().await;
                    stats.record_failure(false); // Not a timeout
                    
                    // Determine error type for partition detection
                    let error_type = if error_string.contains("timeout") {
                        "timeout"
                    } else if error_string.contains("connection refused") {
                        "connection_refused"
                    } else if error_string.contains("unavailable") {
                        "unavailable"
                    } else if error_string.contains("deadline exceeded") {
                        "deadline_exceeded"
                    } else {
                        "unknown"
                    };
                    
                    // Record failure in partition detector
                    let mut detector = self.partition_detector.write().await;
                    detector.record_failure(target, error_type);
                    
                    // Check if we should stop retrying due to partition
                    if detector.is_partitioned(target) {
                        warn!("Stopping retries for {} due to detected network partition", target);
                        break;
                    }
                    
                    // Wait before retry with exponential backoff
                    if attempt < self.max_retries {
                        let backoff = self.retry_backoff * 2_u32.pow(attempt as u32 - 1);
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        }

        // All retries failed
        if let Some(error) = last_error {
            Err(Box::new(error))
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "All retry attempts failed",
            )))
        }
    }
}

#[async_trait]
impl RaftRpc for GrpcRaftClient {
    async fn request_vote(
        &self,
        target: &str,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>> {
        let client = self.get_connection(target).await?;
        let grpc_request = self.convert_request_vote_request(&request);
        
        self.execute_with_retry(target, || {
            let client = client.clone();
            let request = grpc_request.clone();
            Box::pin(async move {
                let response = client
                    .clone()
                    .request_vote(tonic::Request::new(request))
                    .await?;
                Ok::<_, tonic::Status>(response.into_inner())
            })
        })
        .await
        .map(|response| self.convert_request_vote_response(response))
    }

    async fn append_entries(
        &self,
        target: &str,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error + Send + Sync>> {
        let client = self.get_connection(target).await?;
        let grpc_request = self.convert_append_entries_request(&request);
        
        self.execute_with_retry(target, || {
            let client = client.clone();
            let request = grpc_request.clone();
            Box::pin(async move {
                let response = client
                    .clone()
                    .append_entries(tonic::Request::new(request))
                    .await?;
                Ok::<_, tonic::Status>(response.into_inner())
            })
        })
        .await
        .map(|response| self.convert_append_entries_response(response))
    }

    async fn install_snapshot(
        &self,
        target: &str,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, Box<dyn std::error::Error + Send + Sync>> {
        let client = self.get_connection(target).await?;
        let grpc_request = self.convert_install_snapshot_request(&request);
        
        self.execute_with_retry(target, || {
            let client = client.clone();
            let request = grpc_request.clone();
            Box::pin(async move {
                let response = client
                    .clone()
                    .install_snapshot(tonic::Request::new(request))
                    .await?;
                Ok::<_, tonic::Status>(response.into_inner())
            })
        })
        .await
        .map(|response| self.convert_install_snapshot_response(response))
    }
}

impl GrpcRaftClient {
    /// Get RPC statistics
    pub async fn get_stats(&self) -> RpcStats {
        self.stats.read().await.clone()
    }

    /// Clear connection pool
    pub async fn clear_connections(&self) {
        let mut connections = self.connections.write().await;
        connections.clear();
    }

    /// Get connection pool size
    pub async fn connection_count(&self) -> usize {
        self.connections.read().await.len()
    }

    /// Get network partition information
    pub async fn get_partition_info(&self, target: &str) -> Option<crate::consensus::communication::PartitionInfo> {
        let detector = self.partition_detector.read().await;
        detector.get_partition_info(target).cloned()
    }

    /// Get all detected partitions
    pub async fn get_all_partitions(&self) -> Vec<crate::consensus::communication::PartitionInfo> {
        let detector = self.partition_detector.read().await;
        detector.get_all_partitions().into_iter().cloned().collect()
    }

    /// Check if a target is currently partitioned
    pub async fn is_partitioned(&self, target: &str) -> bool {
        let detector = self.partition_detector.read().await;
        detector.is_partitioned(target)
    }

    /// Clean up old partition records
    pub async fn cleanup_partitions(&self) {
        let mut detector = self.partition_detector.write().await;
        detector.cleanup_old_partitions();
    }

    /// Get comprehensive network status
    pub async fn get_network_status(&self) -> NetworkStatus {
        let connections = self.connections.read().await;
        let health_info = self.connection_health.read().await;
        let detector = self.partition_detector.read().await;
        
        let total_connections = connections.len();
        let healthy_connections = health_info.values()
            .filter(|info| info.health == ConnectionHealth::Healthy)
            .count();
        let unhealthy_connections = health_info.values()
            .filter(|info| info.health == ConnectionHealth::Unhealthy)
            .count();
        let partitioned_connections = detector.get_all_partitions().len();
        
        NetworkStatus {
            total_connections,
            healthy_connections,
            unhealthy_connections,
            partitioned_connections,
            partitions: detector.get_all_partitions().into_iter().cloned().collect(),
        }
    }

    /// Perform a health check on a connection
    async fn health_check(&self, target: &str) -> Result<ConnectionHealth, Box<dyn std::error::Error + Send + Sync>> {
        let mut client = self.get_connection(target).await?;
        let start = Instant::now();
        
        let request = Request::new(HealthCheckRequest {
            node_id: "health_check".to_string(),
            timestamp: chrono::Utc::now().timestamp() as u64,
        });

        match tokio::time::timeout(self.timeout.request_timeout, client.health_check(request)).await {
            Ok(Ok(response)) => {
                let health = if response.into_inner().healthy {
                    ConnectionHealth::Healthy
                } else {
                    ConnectionHealth::Unhealthy
                };
                
                // Update connection health info
                self.update_connection_health(target, health.clone(), start.elapsed(), None).await;
                Ok(health)
            }
            Ok(Err(e)) => {
                let error_msg = e.to_string();
                self.update_connection_health(target, ConnectionHealth::Unhealthy, start.elapsed(), Some(error_msg.clone())).await;
                Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, error_msg)))
            }
            Err(_) => {
                self.update_connection_health(target, ConnectionHealth::Unhealthy, start.elapsed(), Some("timeout".to_string())).await;
                Err(Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "health check timeout")))
            }
        }
    }

    /// Update connection health information
    async fn update_connection_health(&self, target: &str, health: ConnectionHealth, latency: Duration, error: Option<String>) {
        let mut health_info = self.connection_health.write().await;
        if let Some(info) = health_info.get_mut(target) {
            info.health = health.clone();
            info.last_heartbeat = Instant::now();
            info.total_requests += 1;
            
            if health == ConnectionHealth::Healthy {
                info.consecutive_failures = 0;
                info.successful_requests += 1;
            } else {
                info.consecutive_failures += 1;
            }
            
            // Update average latency
            let latency_ms = latency.as_millis() as u64;
            if info.successful_requests > 0 {
                info.average_latency_ms = ((info.average_latency_ms * (info.successful_requests - 1)) + latency_ms) / info.successful_requests;
            }
            
            info.last_error = error;
        }
    }

    /// Check if a connection is healthy
    pub async fn is_connection_healthy(&self, target: &str) -> bool {
        let health_info = self.connection_health.read().await;
        if let Some(info) = health_info.get(target) {
            info.health == ConnectionHealth::Healthy && 
            info.consecutive_failures < self.max_consecutive_failures &&
            info.last_heartbeat.elapsed() < self.health_check_interval
        } else {
            false
        }
    }

    /// Get connection health information
    pub async fn get_connection_info(&self, target: &str) -> Option<ConnectionInfo> {
        let health_info = self.connection_health.read().await;
        health_info.get(target).cloned()
    }

    /// Get all connection health information
    pub async fn get_all_connection_info(&self) -> Vec<ConnectionInfo> {
        let health_info = self.connection_health.read().await;
        health_info.values().cloned().collect()
    }

    /// Remove unhealthy connections from the pool
    pub async fn cleanup_unhealthy_connections(&self) {
        let mut connections = self.connections.write().await;
        let mut health_info = self.connection_health.write().await;
        
        let unhealthy_targets: Vec<String> = health_info
            .iter()
            .filter(|(_, info)| {
                info.health == ConnectionHealth::Unhealthy ||
                info.consecutive_failures >= self.max_consecutive_failures ||
                info.last_heartbeat.elapsed() > self.health_check_interval * 2
            })
            .map(|(target, _)| target.clone())
            .collect();

        for target in unhealthy_targets {
            connections.remove(&target);
            health_info.remove(&target);
        }
    }

    /// Start background health checking
    pub async fn start_health_checker(&self) {
        let health_checker = self.clone_for_health_checker();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(health_checker.health_check_interval);
            loop {
                interval.tick().await;
                health_checker.run_health_checks().await;
                health_checker.cleanup_unhealthy_connections().await;
            }
        });
    }

    /// Run health checks on all connections
    async fn run_health_checks(&self) {
        let targets: Vec<String> = {
            let health_info = self.connection_health.read().await;
            health_info.keys().cloned().collect()
        };

        for target in targets {
            if let Err(e) = self.health_check(&target).await {
                warn!("Health check failed for {}: {}", target, e);
            }
        }
    }

    /// Clone the client for background health checking
    fn clone_for_health_checker(&self) -> Self {
        Self {
            connections: Arc::clone(&self.connections),
            connection_health: Arc::clone(&self.connection_health),
            partition_detector: Arc::clone(&self.partition_detector),
            timeout: self.timeout.clone(),
            stats: Arc::clone(&self.stats),
            max_connections: self.max_connections,
            max_retries: self.max_retries,
            retry_backoff: self.retry_backoff,
            health_check_interval: self.health_check_interval,
            max_consecutive_failures: self.max_consecutive_failures,
        }
    }
}

/// Network status information
#[derive(Debug, Clone)]
pub struct NetworkStatus {
    pub total_connections: usize,
    pub healthy_connections: usize,
    pub unhealthy_connections: usize,
    pub partitioned_connections: usize,
    pub partitions: Vec<crate::consensus::communication::PartitionInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::communication::RpcTimeout;

    #[test]
    fn test_request_vote_conversion() {
        let request = RequestVoteRequest {
            term: RaftTerm(1),
            candidate_id: "node1".to_string(),
            last_log_index: LogIndex(10),
            last_log_term: LogTerm(1),
        };

        let client = GrpcRaftClient::new(RpcTimeout::default());
        let grpc_request = client.convert_request_vote_request(&request);

        assert_eq!(grpc_request.term, 1);
        assert_eq!(grpc_request.candidate_id, "node1");
        assert_eq!(grpc_request.last_log_index, 10);
        assert_eq!(grpc_request.last_log_term, 1);
    }

    #[test]
    fn test_append_entries_conversion() {
        let request = AppendEntriesRequest {
            term: RaftTerm(1),
            leader_id: "node1".to_string(),
            prev_log_index: LogIndex(10),
            prev_log_term: LogTerm(1),
            entries: vec![],
            leader_commit: LogIndex(10),
        };

        let client = GrpcRaftClient::new(RpcTimeout::default());
        let grpc_request = client.convert_append_entries_request(&request);

        assert_eq!(grpc_request.term, 1);
        assert_eq!(grpc_request.leader_id, "node1");
        assert_eq!(grpc_request.prev_log_index, 10);
        assert_eq!(grpc_request.prev_log_term, 1);
        assert_eq!(grpc_request.leader_commit, 10);
    }

    #[tokio::test]
    async fn test_connection_health_tracking() {
        let client = GrpcRaftClient::new(RpcTimeout::default());
        
        // Test initial state
        assert!(!client.is_connection_healthy("test-node").await);
        assert!(client.get_connection_info("test-node").await.is_none());
        
        // Test network status
        let status = client.get_network_status().await;
        assert_eq!(status.total_connections, 0);
        assert_eq!(status.healthy_connections, 0);
        assert_eq!(status.unhealthy_connections, 0);
        assert_eq!(status.partitioned_connections, 0);
    }

    #[tokio::test]
    async fn test_partition_detection() {
        let client = GrpcRaftClient::new(RpcTimeout::default());
        
        // Test initial state
        assert!(!client.is_partitioned("test-node").await);
        assert!(client.get_partition_info("test-node").await.is_none());
        
        // Test getting all partitions
        let partitions = client.get_all_partitions().await;
        assert_eq!(partitions.len(), 0);
    }

    #[tokio::test]
    async fn test_network_status() {
        let client = GrpcRaftClient::new(RpcTimeout::default());
        
        let status = client.get_network_status().await;
        assert_eq!(status.total_connections, 0);
        assert_eq!(status.healthy_connections, 0);
        assert_eq!(status.unhealthy_connections, 0);
        assert_eq!(status.partitioned_connections, 0);
        assert_eq!(status.partitions.len(), 0);
    }

    #[tokio::test]
    async fn test_connection_pool_management() {
        let client = GrpcRaftClient::new(RpcTimeout::default());
        
        // Test initial connection count
        assert_eq!(client.connection_count().await, 0);
        
        // Test clearing connections
        client.clear_connections().await;
        assert_eq!(client.connection_count().await, 0);
    }
} 