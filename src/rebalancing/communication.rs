//! Communication protocols for rebalancing operations
//! 
//! This module handles inter-node communication during rebalancing,
//! including coordination, status updates, and data transfer coordination.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant};
use tokio::sync::{mpsc};
use serde::{Deserialize, Serialize};

use super::{RebalancingPlan, RebalancingState};
use crate::config::ClusterConfig;
use crate::membership::MembershipManager;
use crate::log::log_cluster_operation;
use crate::metrics::MetricsCollector;

/// Rebalancing message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RebalancingMessage {
    /// Start rebalancing operation
    StartRebalancing {
        operation_id: String,
        plan: RebalancingPlan,
        source_node: String,
    },
    /// Acknowledge rebalancing start
    StartAck {
        operation_id: String,
        node_id: String,
        success: bool,
        error: Option<String>,
    },
    /// Status update during rebalancing
    StatusUpdate {
        operation_id: String,
        node_id: String,
        progress: u8,
        state: RebalancingState,
        error: Option<String>,
    },
    /// Data transfer request
    DataTransferRequest {
        operation_id: String,
        keys: Vec<String>,
        source_node: String,
        target_node: String,
        sequence_number: u64,
    },
    /// Data transfer response
    DataTransferResponse {
        operation_id: String,
        sequence_number: u64,
        success: bool,
        data: Option<Vec<u8>>,
        error: Option<String>,
    },
    /// Complete rebalancing operation
    CompleteRebalancing {
        operation_id: String,
        node_id: String,
        success: bool,
        error: Option<String>,
    },
    /// Cancel rebalancing operation
    CancelRebalancing {
        operation_id: String,
        node_id: String,
        reason: String,
    },
    /// Heartbeat during rebalancing
    Heartbeat {
        operation_id: String,
        node_id: String,
        timestamp: u64,
    },
}

/// Communication coordinator for rebalancing
pub struct RebalancingCommunication {
    /// Node ID
    node_id: String,
    /// Configuration
    config: ClusterConfig,
    /// Membership manager
    membership_manager: Arc<MembershipManager>,
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
    /// Active message handlers
    message_handlers: HashMap<String, mpsc::Sender<RebalancingMessage>>,
    /// Control channel for stopping
    stop_tx: Option<mpsc::Sender<()>>,
    /// Background task handle
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Communication statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommunicationStats {
    /// Total messages sent
    pub messages_sent: u64,
    /// Total messages received
    pub messages_received: u64,
    /// Failed message sends
    pub failed_sends: u64,
    /// Failed message receives
    pub failed_receives: u64,
    /// Average message latency (ms)
    pub avg_latency_ms: u64,
    /// Active connections
    pub active_connections: usize,
}

impl RebalancingCommunication {
    /// Create a new communication coordinator
    pub fn new(
        node_id: String,
        config: ClusterConfig,
        membership_manager: Arc<MembershipManager>,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        Self {
            node_id,
            config,
            membership_manager,
            metrics,
            message_handlers: HashMap::new(),
            stop_tx: None,
            task_handle: None,
        }
    }

    /// Start the communication coordinator
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Starting rebalancing communication for node: {}", self.node_id);

        let (stop_tx, stop_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);

        let node_id = self.node_id.clone();
        let config = self.config.clone();
        let membership_manager = Arc::clone(&self.membership_manager);
        let metrics = Arc::clone(&self.metrics);

        let handle = tokio::spawn(async move {
            Self::run_communication_loop(
                node_id,
                config,
                membership_manager,
                metrics,
                stop_rx,
            )
            .await;
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    /// Stop the communication coordinator
    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Stopping rebalancing communication for node: {}", self.node_id);

        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(()).await;
        }

        if let Some(handle) = self.task_handle.take() {
            handle.await?;
        }

        Ok(())
    }

    /// Run the main communication loop
    async fn run_communication_loop(
        node_id: String,
        _config: ClusterConfig,
        _membership_manager: Arc<MembershipManager>,
        _metrics: Arc<MetricsCollector>,
        mut stop_rx: mpsc::Receiver<()>,
    ) {
        tracing::info!("Communication loop started for node: {}", node_id);

        loop {
            tokio::select! {
                _ = stop_rx.recv() => {
                    tracing::info!("Communication coordinator stopping");
                    break;
                }
            }
        }
    }

    /// Send a rebalancing message to a specific node
    pub async fn send_message(
        &self,
        target_node: &str,
        message: RebalancingMessage,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let start_time = Instant::now();

        // TODO: Implement actual message sending
        // This would involve:
        // 1. Serializing the message
        // 2. Sending via gRPC or TCP
        // 3. Handling timeouts and retries

        let success = true; // Placeholder
        let duration = start_time.elapsed();

        // Log operation
        let details = vec![
            ("target_node", target_node.to_string()),
            ("message_type", format!("{:?}", message)),
        ];
        log_cluster_operation(
            "rebalancing_message_send",
            &self.node_id,
            success,
            duration,
            Some(details),
        );

        Ok(success)
    }

    /// Broadcast a message to all active nodes
    pub async fn broadcast_message(
        &self,
        message: RebalancingMessage,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        let active_members = self.membership_manager.get_active_members().await;
        let mut failed_nodes = Vec::new();

        for member in &active_members {
            if member.node_id != self.node_id {
                if let Err(_) = self.send_message(&member.node_id, message.clone()).await {
                    failed_nodes.push(member.node_id.clone());
                }
            }
        }

        let duration = start_time.elapsed();
        let details = vec![
            ("total_nodes", active_members.len().to_string()),
            ("failed_nodes", failed_nodes.len().to_string()),
        ];
        log_cluster_operation(
            "rebalancing_broadcast",
            &self.node_id,
            failed_nodes.is_empty(),
            duration,
            Some(details),
        );

        Ok(failed_nodes)
    }

    /// Register a message handler for a specific operation
    pub async fn register_handler(
        &mut self,
        operation_id: String,
        handler: mpsc::Sender<RebalancingMessage>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.message_handlers.insert(operation_id, handler);
        Ok(())
    }

    /// Unregister a message handler
    pub async fn unregister_handler(&mut self, operation_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.message_handlers.remove(operation_id);
        Ok(())
    }

    /// Get communication statistics
    pub async fn get_stats(&self) -> CommunicationStats {
        CommunicationStats {
            messages_sent: 0, // TODO: Implement actual tracking
            messages_received: 0,
            failed_sends: 0,
            failed_receives: 0,
            avg_latency_ms: 0,
            active_connections: self.message_handlers.len(),
        }
    }

    /// Send heartbeat to all nodes
    pub async fn send_heartbeat(&self, operation_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let message = RebalancingMessage::Heartbeat {
            operation_id: operation_id.to_string(),
            node_id: self.node_id.clone(),
            timestamp: Instant::now().elapsed().as_millis() as u64,
        };

        self.broadcast_message(message).await?;
        Ok(())
    }

    /// Handle incoming message
    pub async fn handle_message(&self, message: RebalancingMessage) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        let message_type = format!("{:?}", message);

        match &message {
            RebalancingMessage::StartRebalancing { operation_id, .. } => {
                if let Some(handler) = self.message_handlers.get(operation_id) {
                    let _ = handler.send(message.clone()).await;
                }
            }
            RebalancingMessage::StatusUpdate { operation_id, .. } => {
                if let Some(handler) = self.message_handlers.get(operation_id) {
                    let _ = handler.send(message.clone()).await;
                }
            }
            RebalancingMessage::DataTransferRequest { operation_id, .. } => {
                if let Some(handler) = self.message_handlers.get(operation_id) {
                    let _ = handler.send(message.clone()).await;
                }
            }
            RebalancingMessage::CompleteRebalancing { operation_id, .. } => {
                if let Some(handler) = self.message_handlers.get(operation_id) {
                    let _ = handler.send(message.clone()).await;
                }
            }
            RebalancingMessage::CancelRebalancing { operation_id, .. } => {
                if let Some(handler) = self.message_handlers.get(operation_id) {
                    let _ = handler.send(message.clone()).await;
                }
            }
            RebalancingMessage::Heartbeat { .. } => {
                // Handle heartbeat - could be used for health checking
            }
            _ => {
                tracing::warn!("Unhandled rebalancing message: {}", message_type);
            }
        }

        let duration = start_time.elapsed();
        let details = vec![
            ("message_type", message_type),
        ];
        log_cluster_operation(
            "rebalancing_message_handle",
            &self.node_id,
            true,
            duration,
            Some(details),
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;

    #[tokio::test]
    async fn test_rebalancing_communication_creation() {
        let config = ClusterConfig::default();
        let membership_manager = Arc::new(MembershipManager::new(
            "node1".to_string(),
            Arc::new(crate::consensus::RaftConsensus::new(
                "node1".to_string(),
                config.clone(),
                Arc::new(MetricsCollector::new(crate::config::MetricsConfig::default())),
            )),
            config.clone(),
            Arc::new(MetricsCollector::new(crate::config::MetricsConfig::default())),
        ));
        let metrics = Arc::new(MetricsCollector::new(crate::config::MetricsConfig::default()));

        let communication = RebalancingCommunication::new(
            "node1".to_string(),
            config,
            membership_manager,
            metrics,
        );

        assert_eq!(communication.node_id, "node1");
    }

    #[test]
    fn test_rebalancing_message_serialization() {
        let message = RebalancingMessage::Heartbeat {
            operation_id: "test_op".to_string(),
            node_id: "node1".to_string(),
            timestamp: 123456789,
        };

        let serialized = serde_json::to_string(&message).unwrap();
        let deserialized: RebalancingMessage = serde_json::from_str(&serialized).unwrap();

        match deserialized {
            RebalancingMessage::Heartbeat { operation_id, node_id, timestamp } => {
                assert_eq!(operation_id, "test_op");
                assert_eq!(node_id, "node1");
                assert_eq!(timestamp, 123456789);
            }
            _ => panic!("Expected Heartbeat message"),
        }
    }

    #[test]
    fn test_communication_stats_creation() {
        let stats = CommunicationStats {
            messages_sent: 100,
            messages_received: 95,
            failed_sends: 5,
            failed_receives: 0,
            avg_latency_ms: 50,
            active_connections: 3,
        };

        assert_eq!(stats.messages_sent, 100);
        assert_eq!(stats.messages_received, 95);
        assert_eq!(stats.failed_sends, 5);
        assert_eq!(stats.avg_latency_ms, 50);
        assert_eq!(stats.active_connections, 3);
    }
} 