//! Data rebalancing for dynamic cluster membership
//! 
//! This module handles data redistribution when nodes join or leave
//! the cluster, ensuring consistent hashing and minimal data movement.

pub mod migration;
pub mod safety;
pub mod communication;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use serde::{Deserialize, Serialize};

use crate::cluster::sharding::{ConsistentHashRing, ShardManager};
use crate::membership::{MembershipManager, MemberInfo, MemberStatus};
use crate::config::ClusterConfig;
use crate::log::log_cluster_operation;
use crate::metrics::MetricsCollector;

// Serialization helpers for Instant and Duration
mod timestamp_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(timestamp: &Instant, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let millis = timestamp.elapsed().as_millis() as u64;
        serializer.serialize_u64(millis)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Instant, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(Instant::now() - Duration::from_millis(millis))
    }
}

mod timestamp_serde_opt {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(timestamp: &Option<Instant>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match timestamp {
            Some(ts) => {
                let millis = ts.elapsed().as_millis() as u64;
                serializer.serialize_some(&millis)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Instant>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt_millis = Option::<u64>::deserialize(deserializer)?;
        match opt_millis {
            Some(millis) => Ok(Some(Instant::now() - Duration::from_millis(millis))),
            None => Ok(None),
        }
    }
}

mod duration_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let millis = duration.as_millis() as u64;
        serializer.serialize_u64(millis)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}

/// Rebalancing state machine
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RebalancingState {
    /// No rebalancing in progress
    Idle,
    /// Planning rebalancing operation
    Planning,
    /// Rebalancing is in progress
    InProgress,
    /// Rebalancing completed successfully
    Completed,
    /// Rebalancing failed
    Failed,
    /// Rebalancing was cancelled
    Cancelled,
}

/// Rebalancing operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancingOperation {
    /// Operation ID
    pub operation_id: String,
    /// Operation type
    pub operation_type: RebalancingType,
    /// Triggering event
    pub trigger: RebalancingTrigger,
    /// Affected nodes
    pub affected_nodes: Vec<String>,
    /// Operation state
    pub state: RebalancingState,
    /// Start timestamp (as u64 milliseconds since epoch)
    #[serde(with = "timestamp_serde")]
    pub start_time: Instant,
    /// End timestamp (as u64 milliseconds since epoch, if completed)
    #[serde(with = "timestamp_serde_opt")]
    pub end_time: Option<Instant>,
    /// Progress percentage (0-100)
    pub progress: u8,
    /// Error message (if failed)
    pub error: Option<String>,
}

/// Rebalancing operation type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RebalancingType {
    /// Node joined the cluster
    NodeJoin,
    /// Node left the cluster
    NodeLeave,
    /// Node failed
    NodeFailure,
    /// Manual rebalancing
    Manual,
    /// Periodic rebalancing
    Periodic,
    /// Membership change triggered rebalancing
    MembershipChange,
}

/// Rebalancing trigger
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RebalancingTrigger {
    /// Automatic trigger
    Automatic,
    /// Manual trigger by user
    Manual,
    /// Triggered by membership change
    MembershipChange,
    /// Triggered by health check
    HealthCheck,
}

/// Rebalancing plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancingPlan {
    /// Plan ID
    pub plan_id: String,
    /// Source nodes and their data
    pub source_nodes: HashMap<String, Vec<String>>,
    /// Target nodes and their data
    pub target_nodes: HashMap<String, Vec<String>>,
    /// Keys to be moved
    pub keys_to_move: Vec<String>,
    /// Estimated data size to move
    pub estimated_size: u64,
    /// Estimated duration (as u64 milliseconds)
    #[serde(with = "duration_serde")]
    pub estimated_duration: Duration,
    /// Safety checks passed
    pub safety_checks_passed: bool,
}

/// Main rebalancing manager
pub struct RebalancingManager {
    /// Current rebalancing state
    state: Arc<RwLock<RebalancingState>>,
    /// Active rebalancing operations
    operations: HashMap<String, RebalancingOperation>,
    /// Shard manager
    shard_manager: Arc<ShardManager>,
    /// Membership manager
    membership_manager: Arc<MembershipManager>,
    /// Configuration
    config: ClusterConfig,
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
    /// Node ID
    pub node_id: String,
    /// Migration coordinator
    migration: migration::MigrationCoordinator,
    /// Safety coordinator
    safety: safety::SafetyCoordinator,
    /// Control channel for stopping
    stop_tx: Option<mpsc::Sender<()>>,
    /// Background task handle
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl RebalancingManager {
    /// Create a new rebalancing manager
    pub fn new(
        node_id: String,
        shard_manager: Arc<ShardManager>,
        membership_manager: Arc<MembershipManager>,
        config: ClusterConfig,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        let state = Arc::new(RwLock::new(RebalancingState::Idle));

        let migration = migration::MigrationCoordinator::new(
            node_id.clone(),
            Arc::clone(&state),
            config.clone(),
        );

        let safety = safety::SafetyCoordinator::new(
            node_id.clone(),
            Arc::clone(&state),
            config.clone(),
        );

        Self {
            state,
            operations: HashMap::new(),
            shard_manager,
            membership_manager,
            config,
            metrics,
            node_id,
            migration,
            safety,
            stop_tx: None,
            task_handle: None,
        }
    }

    /// Start the rebalancing manager
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Starting rebalancing manager for node: {}", self.node_id);

        let (stop_tx, stop_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);

        // Start migration coordinator
        self.migration.start().await?;

        // Start safety coordinator
        self.safety.start().await?;

        let node_id = self.node_id.clone();
        let state = Arc::clone(&self.state);
        let shard_manager = Arc::clone(&self.shard_manager);
        let membership_manager = Arc::clone(&self.membership_manager);

        let handle = tokio::spawn(async move {
            Self::run_rebalancing_loop(
                node_id,
                state,
                shard_manager,
                membership_manager,
                stop_rx,
            )
            .await;
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    /// Stop the rebalancing manager
    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Stopping rebalancing manager for node: {}", self.node_id);

        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(()).await;
        }

        // Stop migration coordinator
        self.migration.stop().await?;

        // Stop safety coordinator
        self.safety.stop().await?;

        if let Some(handle) = self.task_handle.take() {
            handle.await?;
        }

        Ok(())
    }

    /// Run the main rebalancing loop
    async fn run_rebalancing_loop(
        node_id: String,
        state: Arc<RwLock<RebalancingState>>,
        shard_manager: Arc<ShardManager>,
        membership_manager: Arc<MembershipManager>,
        mut stop_rx: mpsc::Receiver<()>,
    ) {
        let mut timer = interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = timer.tick() => {
                    if let Err(e) = Self::check_rebalancing_needed(&node_id, &state, &shard_manager, &membership_manager).await {
                        tracing::error!("Rebalancing check error: {}", e);
                    }
                }
                _ = stop_rx.recv() => {
                    tracing::info!("Rebalancing manager stopping");
                    break;
                }
            }
        }
    }

    /// Check if rebalancing is needed
    async fn check_rebalancing_needed(
        node_id: &str,
        state: &Arc<RwLock<RebalancingState>>,
        shard_manager: &Arc<ShardManager>,
        membership_manager: &Arc<MembershipManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let current_state = state.read().await;
        if *current_state != RebalancingState::Idle {
            return Ok(()); // Already rebalancing
        }

        // Check membership changes
        let membership_state = membership_manager.get_membership_state().await;
        let active_members = membership_manager.get_active_members().await;

        // Check if hash ring needs updating
        let ring_nodes = shard_manager.get_nodes().await;

        if active_members.len() != ring_nodes.len() {
            // Membership changed, trigger rebalancing
            drop(current_state);
            // Note: This would need to be called on a RebalancingManager instance
            // For now, we'll just log the need for rebalancing
            tracing::info!("Membership change detected, rebalancing needed");
        }

        Ok(())
    }

    /// Trigger a rebalancing operation
    async fn trigger_rebalancing(
        &mut self,
        operation_type: RebalancingType,
        trigger: RebalancingTrigger,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();

        // Update state to planning
        {
            let mut state_guard = self.state.write().await;
            *state_guard = RebalancingState::Planning;
        }

        // Create rebalancing plan
        let plan = self.create_rebalancing_plan(operation_type, trigger).await?;

        // Perform safety checks
        if !self.safety.perform_safety_checks(&plan).await? {
            tracing::warn!("Safety checks failed for rebalancing plan: {}", plan.plan_id);
            let mut state_guard = self.state.write().await;
            *state_guard = RebalancingState::Idle;
            return Ok(());
        }

        // Start migration
        self.migration.start_migration(plan).await?;

        // Update state to in progress
        {
            let mut state_guard = self.state.write().await;
            *state_guard = RebalancingState::InProgress;
        }

        // Log operation
        let duration = start_time.elapsed();
        log_cluster_operation(
            "rebalancing_trigger",
            &self.node_id,
            true,
            duration,
            None,
        );

        Ok(())
    }

    /// Create a rebalancing plan
    async fn create_rebalancing_plan(
        &self,
        operation_type: RebalancingType,
        trigger: RebalancingTrigger,
    ) -> Result<RebalancingPlan, Box<dyn std::error::Error>> {
        let start_time = Instant::now();

        // Get current nodes
        let current_nodes = self.shard_manager.get_nodes().await;

        // Get active members
        let active_members = self.membership_manager.get_active_members().await;
        let active_node_ids: Vec<String> = active_members.iter().map(|m| m.node_id.clone()).collect();

        // Calculate new hash ring
        let new_hash_ring = ConsistentHashRing::new(self.config.virtual_nodes_per_physical);

        // Calculate key movements
        let mut source_nodes = HashMap::new();
        let mut target_nodes = HashMap::new();
        let mut keys_to_move = Vec::new();

        // TODO: Implement actual key movement calculation
        // This is a simplified version - in practice, you'd need to:
        // 1. Get all keys from the current hash ring
        // 2. Calculate which keys need to move to new nodes
        // 3. Group keys by source and target nodes

        let plan = RebalancingPlan {
            plan_id: format!("plan_{:?}_{}", operation_type, start_time.elapsed().as_millis()),
            source_nodes,
            target_nodes,
            keys_to_move,
            estimated_size: 0, // TODO: Calculate actual size
            estimated_duration: Duration::from_secs(60), // TODO: Estimate based on data size
            safety_checks_passed: false, // Will be set by safety coordinator
        };

        Ok(plan)
    }

    /// Get current rebalancing state
    pub async fn get_state(&self) -> RebalancingState {
        self.state.read().await.clone()
    }

    /// Get active rebalancing operations
    pub fn get_active_operations(&self) -> Vec<&RebalancingOperation> {
        self.operations.values().collect()
    }

    /// Get rebalancing statistics
    pub async fn get_stats(&self) -> RebalancingStats {
        let state = self.get_state().await;
        let active_operations = self.operations.len();
        let completed_operations = self.operations.values()
            .filter(|op| op.state == RebalancingState::Completed)
            .count();
        let failed_operations = self.operations.values()
            .filter(|op| op.state == RebalancingState::Failed)
            .count();

        RebalancingStats {
            current_state: state,
            active_operations,
            completed_operations,
            failed_operations,
            total_operations: self.operations.len(),
        }
    }

    /// Manually trigger rebalancing
    pub async fn trigger_manual_rebalancing(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();

        let result = self.trigger_rebalancing(
            RebalancingType::Manual,
            RebalancingTrigger::Manual,
        ).await;

        // Record metrics
        let duration = start_time.elapsed();
        self.metrics.record_cluster_operation("rebalancing_manual_trigger", result.is_ok(), duration);

        result
    }

    /// Cancel ongoing rebalancing
    pub async fn cancel_rebalancing(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();

        // Update state to cancelled
        {
            let mut state_guard = self.state.write().await;
            if *state_guard == RebalancingState::InProgress || *state_guard == RebalancingState::Planning {
                *state_guard = RebalancingState::Cancelled;
            }
        }

        // Stop migration
        self.migration.cancel_migration().await?;

        // Record metrics
        let duration = start_time.elapsed();
        self.metrics.record_cluster_operation("rebalancing_cancel", true, duration);

        Ok(())
    }
}

/// Rebalancing statistics
#[derive(Debug, Clone)]
pub struct RebalancingStats {
    pub current_state: RebalancingState,
    pub active_operations: usize,
    pub completed_operations: usize,
    pub failed_operations: usize,
    pub total_operations: usize,
}

impl std::fmt::Display for RebalancingStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RebalancingStats(state={:?}, active={}, completed={}, failed={}, total={})",
            self.current_state,
            self.active_operations,
            self.completed_operations,
            self.failed_operations,
            self.total_operations
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;

    #[tokio::test]
    async fn test_rebalancing_operation_creation() {
        let operation = RebalancingOperation {
            operation_id: "test_op".to_string(),
            operation_type: RebalancingType::NodeJoin,
            trigger: RebalancingTrigger::Automatic,
            affected_nodes: vec!["node1".to_string(), "node2".to_string()],
            state: RebalancingState::Idle,
            start_time: Instant::now(),
            end_time: None,
            progress: 0,
            error: None,
        };

        assert_eq!(operation.operation_id, "test_op");
        assert_eq!(operation.state, RebalancingState::Idle);
        assert_eq!(operation.progress, 0);
    }

    #[test]
    fn test_rebalancing_plan_creation() {
        let plan = RebalancingPlan {
            plan_id: "test_plan".to_string(),
            source_nodes: HashMap::new(),
            target_nodes: HashMap::new(),
            keys_to_move: Vec::new(),
            estimated_size: 1024,
            estimated_duration: Duration::from_secs(60),
            safety_checks_passed: false,
        };

        assert_eq!(plan.plan_id, "test_plan");
        assert_eq!(plan.estimated_size, 1024);
        assert_eq!(plan.estimated_duration, Duration::from_secs(60));
    }

    #[test]
    fn test_rebalancing_stats_creation() {
        let stats = RebalancingStats {
            current_state: RebalancingState::Idle,
            active_operations: 0,
            completed_operations: 5,
            failed_operations: 1,
            total_operations: 6,
        };

        assert_eq!(stats.current_state, RebalancingState::Idle);
        assert_eq!(stats.completed_operations, 5);
        assert_eq!(stats.failed_operations, 1);
    }
} 