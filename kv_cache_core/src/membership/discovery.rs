//! Dynamic node discovery for cluster membership
//! 
//! This module implements service discovery mechanisms including
//! health checking, automatic node detection, and gossip-based discovery.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use serde::{Deserialize, Serialize};

use super::{MembershipState, MemberInfo, MemberStatus, HealthStatus};
use crate::config::ClusterConfig;
use crate::cluster::communication::KvCacheClient;
use crate::log::log_cluster_operation;


/// Discovery message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscoveryMessage {
    /// Node announcement
    Announce {
        node_id: String,
        address: String,
        port: u16,
        datacenter: Option<String>,
        timestamp: u64,
    },
    /// Health check request
    HealthCheck {
        from_node: String,
        timestamp: u64,
    },
    /// Health check response
    HealthResponse {
        node_id: String,
        status: HealthStatus,
        timestamp: u64,
    },
    /// Node departure notification
    Departure {
        node_id: String,
        graceful: bool,
        timestamp: u64,
    },
}

/// Discovery coordinator for managing node discovery
pub struct DiscoveryCoordinator {
    /// Node ID
    node_id: String,
    /// Membership state
    state: Arc<RwLock<MembershipState>>,
    /// Configuration
    config: ClusterConfig,
    /// Discovery interval
    discovery_interval: Duration,
    /// Health check interval
    health_check_interval: Duration,
    /// Failure timeout
    failure_timeout: Duration,
    /// Control channel for stopping
    stop_tx: Option<mpsc::Sender<()>>,
    /// Background task handle
    task_handle: Option<tokio::task::JoinHandle<()>>,
    /// Known nodes for health checking
    #[allow(dead_code)]
    known_nodes: HashMap<String, NodeHealthInfo>,
}

/// Node health information
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct NodeHealthInfo {
    /// Node address
    address: String,
    /// Node port
    port: u16,
    /// Last successful health check
    last_healthy: Option<u64>,
    /// Last health check attempt
    last_check: Option<u64>,
    /// Consecutive failures
    consecutive_failures: u32,
    /// Health status
    status: HealthStatus,
}

impl DiscoveryCoordinator {
    /// Create a new discovery coordinator
    pub fn new(
        node_id: String,
        state: Arc<RwLock<MembershipState>>,
        config: ClusterConfig,
    ) -> Self {
        Self {
            node_id,
            state,
            config,
            discovery_interval: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(10),
            failure_timeout: Duration::from_secs(60),
            stop_tx: None,
            task_handle: None,
            known_nodes: HashMap::new(),
        }
    }

    /// Start the discovery coordinator
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Starting discovery coordinator for node: {}", self.node_id);

        let (stop_tx, stop_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);

        let node_id = self.node_id.clone();
        let state = Arc::clone(&self.state);
        let config = self.config.clone();
        let discovery_interval = self.discovery_interval;
        let health_check_interval = self.health_check_interval;
        let failure_timeout = self.failure_timeout;

        let handle = tokio::spawn(async move {
            Self::run_discovery_loop(
                node_id,
                state,
                config,
                discovery_interval,
                health_check_interval,
                failure_timeout,
                stop_rx,
            )
            .await;
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    /// Stop the discovery coordinator
    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Stopping discovery coordinator for node: {}", self.node_id);

        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(()).await;
        }

        if let Some(handle) = self.task_handle.take() {
            handle.await?;
        }

        Ok(())
    }

    /// Run the main discovery loop
    async fn run_discovery_loop(
        node_id: String,
        state: Arc<RwLock<MembershipState>>,
        config: ClusterConfig,
        discovery_interval: Duration,
        health_check_interval: Duration,
        failure_timeout: Duration,
        mut stop_rx: mpsc::Receiver<()>,
    ) {
        let mut discovery_timer = interval(discovery_interval);
        let mut health_timer = interval(health_check_interval);

        loop {
            tokio::select! {
                _ = discovery_timer.tick() => {
                    if let Err(e) = Self::perform_discovery(&node_id, &state, &config).await {
                        tracing::error!("Discovery error: {}", e);
                    }
                }
                _ = health_timer.tick() => {
                    if let Err(e) = Self::perform_health_checks(&node_id, &state, &config, failure_timeout).await {
                        tracing::error!("Health check error: {}", e);
                    }
                }
                _ = stop_rx.recv() => {
                    tracing::info!("Discovery coordinator stopping");
                    break;
                }
            }
        }
    }

    /// Perform service discovery
    async fn perform_discovery(
        node_id: &str,
        state: &Arc<RwLock<MembershipState>>,
        config: &ClusterConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start_time = Instant::now();

        // Check configured members
        for (member_id, address) in &config.members {
            if member_id == node_id {
                continue; // Skip self
            }

            // Parse address:port
            let (host, port) = if let Some(colon_pos) = address.rfind(':') {
                let host = &address[..colon_pos];
                let port = address[colon_pos + 1..].parse::<u16>()?;
                (host, port)
            } else {
                (address.as_str(), 50051) // Default port
            };

            // Try to connect and get health info
            if let Ok(mut client) = KvCacheClient::connect(format!("http://{}:{}", host, port)).await {
                if let Ok(health_response) = client.health_check(node_id.to_string()).await {
                    let member_info = MemberInfo {
                        node_id: member_id.clone(),
                        address: host.to_string(),
                        port,
                        status: MemberStatus::Active,
                        last_seen: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                        health: if health_response.healthy {
                            HealthStatus::Healthy
                        } else {
                            HealthStatus::Unhealthy
                        },
                        datacenter: None, // TODO: Add datacenter info
                    };

                    // Update membership state
                    let mut state_guard = state.write().await;
                    state_guard.members.insert(member_id.clone(), member_info);
                    state_guard.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
                }
            }
        }

        // Log operation
        let duration = start_time.elapsed();
        log_cluster_operation(
            "discovery_perform",
            node_id,
            true,
            duration,
            None,
        );

        Ok(())
    }

    /// Perform health checks on known nodes
    async fn perform_health_checks(
        node_id: &str,
        state: &Arc<RwLock<MembershipState>>,
        _config: &ClusterConfig,
        failure_timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start_time = Instant::now();
        let mut state_guard = state.write().await;
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

        for (member_id, member_info) in &mut state_guard.members {
            if member_id == node_id {
                continue; // Skip self
            }

            // Check if node has been seen recently
            let time_since_last_seen = current_time - member_info.last_seen;
            if time_since_last_seen > failure_timeout.as_secs() as i64 {
                member_info.status = MemberStatus::Failed;
                member_info.health = HealthStatus::Unhealthy;
                continue;
            }

            // Try to perform health check
            let address = format!("{}:{}", member_info.address, member_info.port);
            if let Ok(mut client) = KvCacheClient::connect(format!("http://{}", address)).await {
                match client.health_check(node_id.to_string()).await {
                    Ok(health_response) => {
                        member_info.last_seen = current_time;
                        member_info.health = if health_response.healthy {
                            HealthStatus::Healthy
                        } else {
                            HealthStatus::Unhealthy
                        };
                        member_info.status = if health_response.healthy {
                            MemberStatus::Active
                        } else {
                            MemberStatus::Failed
                        };
                    }
                    Err(_) => {
                        member_info.health = HealthStatus::Unhealthy;
                        let time_since_last_seen = current_time - member_info.last_seen;
                        if time_since_last_seen > failure_timeout.as_secs() as i64 {
                            member_info.status = MemberStatus::Failed;
                        }
                    }
                }
            } else {
                member_info.health = HealthStatus::Unhealthy;
                let time_since_last_seen = current_time - member_info.last_seen;
                if time_since_last_seen > failure_timeout.as_secs() as i64 {
                    member_info.status = MemberStatus::Failed;
                }
            }
        }

        state_guard.last_update = current_time;

        // Log operation
        let duration = start_time.elapsed();
        log_cluster_operation(
            "discovery_health_check",
            node_id,
            true,
            duration,
            None,
        );

        Ok(())
    }

    /// Announce this node to the cluster
    pub async fn announce_node(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _message = DiscoveryMessage::Announce {
            node_id: self.node_id.clone(),
            address: self.config.bind_address.clone(),
            port: self.config.port,
            datacenter: None, // TODO: Add datacenter support
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        };

        // Send announcement to all known members
        for (member_id, address) in &self.config.members {
            if member_id == &self.node_id {
                continue; // Skip self
            }

            if let Ok(_client) = KvCacheClient::connect(format!("http://{}", address)).await {
                // TODO: Implement announcement RPC
                tracing::debug!("Announcing to node: {}", member_id);
            }
        }

        Ok(())
    }

    /// Handle incoming discovery message
    pub async fn handle_message(&self, message: DiscoveryMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match message {
            DiscoveryMessage::Announce { node_id, address, port, datacenter, timestamp } => {
                let mut state_guard = self.state.write().await;
                let member_info = MemberInfo {
                    node_id: node_id.clone(),
                    address,
                    port,
                    status: MemberStatus::Active,
                    last_seen: timestamp as i64,
                    health: HealthStatus::Healthy,
                    datacenter,
                };
                state_guard.members.insert(node_id, member_info);
                state_guard.last_update = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
            }
            DiscoveryMessage::HealthCheck { from_node: _, timestamp } => {
                // Respond with health status
                let _response = DiscoveryMessage::HealthResponse {
                    node_id: self.node_id.clone(),
                    status: HealthStatus::Healthy, // TODO: Implement actual health check
                    timestamp,
                };
                // TODO: Send response back to from_node
            }
            DiscoveryMessage::HealthResponse { node_id, status, timestamp } => {
                let mut state_guard = self.state.write().await;
                if let Some(member) = state_guard.members.get_mut(&node_id) {
                    member.last_seen = timestamp as i64;
                    member.health = status;
                }
            }
            DiscoveryMessage::Departure { node_id, graceful, timestamp } => {
                let mut state_guard = self.state.write().await;
                if let Some(member) = state_guard.members.get_mut(&node_id) {
                    member.status = if graceful {
                        MemberStatus::Leaving
                    } else {
                        MemberStatus::Failed
                    };
                    member.last_seen = timestamp as i64;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;

    #[tokio::test]
    async fn test_discovery_coordinator_creation() {
        let config = ClusterConfig::default();
        let state = Arc::new(RwLock::new(MembershipState {
            version: 1,
            members: HashMap::new(),
            pending_changes: Vec::new(),
            last_update: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        }));

        let coordinator = DiscoveryCoordinator::new("node1".to_string(), state, config);
        assert_eq!(coordinator.node_id, "node1");
    }

    #[test]
    fn test_discovery_message_creation() {
        let message = DiscoveryMessage::Announce {
            node_id: "node1".to_string(),
            address: "127.0.0.1".to_string(),
            port: 50051,
            datacenter: Some("dc1".to_string()),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        };

        match message {
            DiscoveryMessage::Announce { node_id, address, port, datacenter, .. } => {
                assert_eq!(node_id, "node1");
                assert_eq!(address, "127.0.0.1");
                assert_eq!(port, 50051);
                assert_eq!(datacenter, Some("dc1".to_string()));
            }
            _ => panic!("Expected Announce message"),
        }
    }

    #[test]
    fn test_node_health_info_creation() {
        let health_info = NodeHealthInfo {
            address: "127.0.0.1".to_string(),
            port: 50051,
            last_healthy: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()),
            last_check: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()),
            consecutive_failures: 0,
            status: HealthStatus::Healthy,
        };

        assert_eq!(health_info.address, "127.0.0.1");
        assert_eq!(health_info.port, 50051);
        assert_eq!(health_info.status, HealthStatus::Healthy);
    }
} 