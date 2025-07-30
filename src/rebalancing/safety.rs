//! Safety checks for rebalancing operations
//! 
//! This module provides safety mechanisms to ensure data consistency
//! and system stability during rebalancing operations.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use serde::{Deserialize, Serialize};

use super::{RebalancingPlan, RebalancingState};
use crate::config::ClusterConfig;
use crate::membership::MembershipManager;
use crate::log::log_cluster_operation;
use crate::metrics::MetricsCollector;

/// Safety check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyCheckResult {
    /// Check ID
    pub check_id: String,
    /// Check type
    pub check_type: SafetyCheckType,
    /// Check passed
    pub passed: bool,
    /// Check timestamp (as u64 milliseconds since epoch)
    #[serde(with = "timestamp_serde")]
    pub timestamp: Instant,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Check details
    pub details: HashMap<String, String>,
}

/// Safety check type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SafetyCheckType {
    /// Check cluster health
    ClusterHealth,
    /// Check data consistency
    DataConsistency,
    /// Check resource availability
    ResourceAvailability,
    /// Check network connectivity
    NetworkConnectivity,
    /// Check quorum availability
    QuorumAvailability,
    /// Check rebalancing locks
    RebalancingLocks,
    /// Check migration readiness
    MigrationReadiness,
}

/// Safety check status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SafetyCheckStatus {
    /// Check not performed
    NotPerformed,
    /// Check in progress
    InProgress,
    /// Check passed
    Passed,
    /// Check failed
    Failed,
    /// Check skipped
    Skipped,
}

/// Safety check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyCheckConfig {
    /// Minimum healthy nodes required
    pub min_healthy_nodes: usize,
    /// Maximum concurrent rebalancing operations
    pub max_concurrent_rebalancing: usize,
    /// Minimum quorum size
    pub min_quorum_size: usize,
    /// Maximum data loss tolerance (bytes)
    pub max_data_loss_tolerance: u64,
    /// Network timeout for checks (as u64 milliseconds)
    #[serde(with = "duration_serde")]
    pub network_timeout: Duration,
    /// Check retry count
    pub check_retry_count: u32,
    /// Check retry delay (as u64 milliseconds)
    #[serde(with = "duration_serde")]
    pub check_retry_delay: Duration,
}

/// Safety coordinator
pub struct SafetyCoordinator {
    /// Node ID
    node_id: String,
    /// Rebalancing state
    state: Arc<RwLock<RebalancingState>>,
    /// Configuration
    config: ClusterConfig,
    /// Safety check configuration
    safety_config: SafetyCheckConfig,
    /// Membership manager
    membership_manager: Arc<MembershipManager>,
    /// Active safety checks
    active_checks: HashMap<String, SafetyCheckResult>,
    /// Rebalancing locks
    rebalancing_locks: HashMap<String, RebalancingLock>,
    /// Control channel for stopping
    stop_tx: Option<mpsc::Sender<()>>,
    /// Background task handle
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Rebalancing lock
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancingLock {
    /// Lock ID
    pub lock_id: String,
    /// Locked by node
    pub locked_by: String,
    /// Lock timestamp (as u64 milliseconds since epoch)
    #[serde(with = "timestamp_serde")]
    pub lock_timestamp: Instant,
    /// Lock timeout (as u64 milliseconds)
    #[serde(with = "duration_serde")]
    pub lock_timeout: Duration,
    /// Lock reason
    pub lock_reason: String,
}

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

impl SafetyCoordinator {
    /// Create a new safety coordinator
    pub fn new(
        node_id: String,
        state: Arc<RwLock<RebalancingState>>,
        config: ClusterConfig,
    ) -> Self {
        let safety_config = SafetyCheckConfig {
            min_healthy_nodes: 2,
            max_concurrent_rebalancing: 1,
            min_quorum_size: 2,
            max_data_loss_tolerance: 0, // No data loss tolerance
            network_timeout: Duration::from_secs(10),
            check_retry_count: 3,
            check_retry_delay: Duration::from_secs(5),
        };

        let node_id_clone1 = node_id.clone();
        let node_id_clone2 = node_id.clone();
        let config_clone1 = config.clone();
        let config_clone2 = config.clone();

        Self {
            node_id,
            state,
            config,
            safety_config,
            membership_manager: Arc::new(MembershipManager::new(
                node_id_clone1,
                Arc::new(crate::consensus::RaftConsensus::new(
                    node_id_clone2,
                    config_clone1,
                    Arc::new(MetricsCollector::new(crate::config::MetricsConfig::default())),
                )),
                config_clone2,
                Arc::new(MetricsCollector::new(crate::config::MetricsConfig::default())),
            )),
            active_checks: HashMap::new(),
            rebalancing_locks: HashMap::new(),
            stop_tx: None,
            task_handle: None,
        }
    }

    /// Start the safety coordinator
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Starting safety coordinator for node: {}", self.node_id);

        let (stop_tx, stop_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);

        let node_id = self.node_id.clone();
        let state = Arc::clone(&self.state);
        let config = self.config.clone();
        let safety_config = self.safety_config.clone();

        let handle = tokio::spawn(async move {
            Self::run_safety_loop(
                node_id,
                state,
                config,
                safety_config,
                stop_rx,
            )
            .await;
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    /// Stop the safety coordinator
    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Stopping safety coordinator for node: {}", self.node_id);

        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(()).await;
        }

        if let Some(handle) = self.task_handle.take() {
            handle.await?;
        }

        Ok(())
    }

    /// Run the main safety loop
    async fn run_safety_loop(
        node_id: String,
        state: Arc<RwLock<RebalancingState>>,
        config: ClusterConfig,
        safety_config: SafetyCheckConfig,
        mut stop_rx: mpsc::Receiver<()>,
    ) {
        let mut timer = interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = timer.tick() => {
                    if let Err(e) = Self::perform_periodic_checks(&node_id, &state, &config, &safety_config).await {
                        tracing::error!("Periodic safety check error: {}", e);
                    }
                }
                _ = stop_rx.recv() => {
                    tracing::info!("Safety coordinator stopping");
                    break;
                }
            }
        }
    }

    /// Perform periodic safety checks
    async fn perform_periodic_checks(
        node_id: &str,
        state: &Arc<RwLock<RebalancingState>>,
        config: &ClusterConfig,
        safety_config: &SafetyCheckConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let current_state = state.read().await;
        if *current_state == RebalancingState::Idle {
            // Only perform checks when not rebalancing
            // TODO: Implement periodic health checks
        }

        Ok(())
    }

    /// Perform safety checks for a rebalancing plan
    pub async fn perform_safety_checks(
        &self,
        plan: &RebalancingPlan,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let start_time = Instant::now();

        tracing::info!("Performing safety checks for plan: {}", plan.plan_id);

        // Perform all safety checks
        let checks = vec![
            self.check_cluster_health().await?,
            self.check_data_consistency(plan).await?,
            self.check_resource_availability(plan).await?,
            self.check_network_connectivity(plan).await?,
            self.check_quorum_availability().await?,
            self.check_rebalancing_locks(plan).await?,
            self.check_migration_readiness(plan).await?,
        ];

        // All checks must pass
        let all_passed = checks.iter().all(|check| check.passed);

        // Log operation
        let duration = start_time.elapsed();
        let details = vec![
            ("plan_id", plan.plan_id.clone()),
            ("checks_passed", checks.iter().filter(|c| c.passed).count().to_string()),
            ("total_checks", checks.len().to_string()),
        ];
        log_cluster_operation(
            "safety_checks_perform",
            &self.node_id,
            all_passed,
            duration,
            Some(details),
        );

        Ok(all_passed)
    }

    /// Check cluster health
    async fn check_cluster_health(&self) -> Result<SafetyCheckResult, Box<dyn std::error::Error>> {
        let check_id = format!("cluster_health_{}", Instant::now().elapsed().as_millis());
        let start_time = Instant::now();

        let membership_stats = self.membership_manager.get_stats().await;
        let healthy_nodes = membership_stats.healthy_members;
        let total_nodes = membership_stats.total_members;

        let passed = healthy_nodes >= self.safety_config.min_healthy_nodes;
        let error = if !passed {
            Some(format!("Insufficient healthy nodes: {} < {}", healthy_nodes, self.safety_config.min_healthy_nodes))
        } else {
            None
        };

        let mut details = HashMap::new();
        details.insert("healthy_nodes".to_string(), healthy_nodes.to_string());
        details.insert("total_nodes".to_string(), total_nodes.to_string());
        details.insert("min_required".to_string(), self.safety_config.min_healthy_nodes.to_string());

        Ok(SafetyCheckResult {
            check_id,
            check_type: SafetyCheckType::ClusterHealth,
            passed,
            timestamp: start_time,
            error,
            details,
        })
    }

    /// Check data consistency
    async fn check_data_consistency(
        &self,
        plan: &RebalancingPlan,
    ) -> Result<SafetyCheckResult, Box<dyn std::error::Error>> {
        let check_id = format!("data_consistency_{}", Instant::now().elapsed().as_millis());
        let start_time = Instant::now();

        // TODO: Implement actual data consistency checks
        // This would involve:
        // 1. Checking that source data exists
        // 2. Verifying data integrity
        // 3. Ensuring no data corruption

        let passed = true; // Placeholder
        let error = None;
        let mut details = HashMap::new();
        details.insert("keys_to_move".to_string(), plan.keys_to_move.len().to_string());
        details.insert("estimated_size".to_string(), plan.estimated_size.to_string());

        Ok(SafetyCheckResult {
            check_id,
            check_type: SafetyCheckType::DataConsistency,
            passed,
            timestamp: start_time,
            error,
            details,
        })
    }

    /// Check resource availability
    async fn check_resource_availability(
        &self,
        plan: &RebalancingPlan,
    ) -> Result<SafetyCheckResult, Box<dyn std::error::Error>> {
        let check_id = format!("resource_availability_{}", Instant::now().elapsed().as_millis());
        let start_time = Instant::now();

        // TODO: Implement resource availability checks
        // This would involve:
        // 1. Checking available disk space
        // 2. Checking available memory
        // 3. Checking network bandwidth
        // 4. Checking CPU availability

        let passed = true; // Placeholder
        let error = None;
        let mut details = HashMap::new();
        details.insert("estimated_size".to_string(), plan.estimated_size.to_string());

        Ok(SafetyCheckResult {
            check_id,
            check_type: SafetyCheckType::ResourceAvailability,
            passed,
            timestamp: start_time,
            error,
            details,
        })
    }

    /// Check network connectivity
    async fn check_network_connectivity(
        &self,
        plan: &RebalancingPlan,
    ) -> Result<SafetyCheckResult, Box<dyn std::error::Error>> {
        let check_id = format!("network_connectivity_{}", Instant::now().elapsed().as_millis());
        let start_time = Instant::now();

        // Check connectivity to all nodes involved in the plan
        let mut all_nodes = Vec::new();
        all_nodes.extend(plan.source_nodes.keys().cloned());
        all_nodes.extend(plan.target_nodes.keys().cloned());
        all_nodes.sort();
        all_nodes.dedup();

        let mut failed_nodes: Vec<String> = Vec::new();
        for node in &all_nodes {
            // TODO: Implement actual connectivity check
            // This would involve:
            // 1. Pinging the node
            // 2. Checking gRPC connectivity
            // 3. Measuring latency
        }

        let passed = failed_nodes.is_empty();
        let error = if !passed {
            Some(format!("Failed to connect to nodes: {:?}", failed_nodes))
        } else {
            None
        };

        let mut details = HashMap::new();
        details.insert("total_nodes".to_string(), all_nodes.len().to_string());
        details.insert("failed_nodes".to_string(), failed_nodes.len().to_string());

        Ok(SafetyCheckResult {
            check_id,
            check_type: SafetyCheckType::NetworkConnectivity,
            passed,
            timestamp: start_time,
            error,
            details,
        })
    }

    /// Check quorum availability
    async fn check_quorum_availability(&self) -> Result<SafetyCheckResult, Box<dyn std::error::Error>> {
        let check_id = format!("quorum_availability_{}", Instant::now().elapsed().as_millis());
        let start_time = Instant::now();

        let membership_stats = self.membership_manager.get_stats().await;
        let active_nodes = membership_stats.active_members;
        let required_quorum = self.safety_config.min_quorum_size;

        let passed = active_nodes >= required_quorum;
        let error = if !passed {
            Some(format!("Insufficient nodes for quorum: {} < {}", active_nodes, required_quorum))
        } else {
            None
        };

        let mut details = HashMap::new();
        details.insert("active_nodes".to_string(), active_nodes.to_string());
        details.insert("required_quorum".to_string(), required_quorum.to_string());

        Ok(SafetyCheckResult {
            check_id,
            check_type: SafetyCheckType::QuorumAvailability,
            passed,
            timestamp: start_time,
            error,
            details,
        })
    }

    /// Check rebalancing locks
    async fn check_rebalancing_locks(
        &self,
        plan: &RebalancingPlan,
    ) -> Result<SafetyCheckResult, Box<dyn std::error::Error>> {
        let check_id = format!("rebalancing_locks_{}", Instant::now().elapsed().as_millis());
        let start_time = Instant::now();

        // Check if any nodes involved in the plan are locked
        let mut all_nodes = Vec::new();
        all_nodes.extend(plan.source_nodes.keys().cloned());
        all_nodes.extend(plan.target_nodes.keys().cloned());

        let mut locked_nodes = Vec::new();
        for node in &all_nodes {
            if let Some(lock) = self.rebalancing_locks.get(node) {
                if lock.lock_timestamp.elapsed() < lock.lock_timeout {
                    locked_nodes.push(node.clone());
                }
            }
        }

        let passed = locked_nodes.is_empty();
        let error = if !passed {
            Some(format!("Nodes are locked: {:?}", locked_nodes))
        } else {
            None
        };

        let mut details = HashMap::new();
        details.insert("total_nodes".to_string(), all_nodes.len().to_string());
        details.insert("locked_nodes".to_string(), locked_nodes.len().to_string());

        Ok(SafetyCheckResult {
            check_id,
            check_type: SafetyCheckType::RebalancingLocks,
            passed,
            timestamp: start_time,
            error,
            details,
        })
    }

    /// Check migration readiness
    async fn check_migration_readiness(
        &self,
        plan: &RebalancingPlan,
    ) -> Result<SafetyCheckResult, Box<dyn std::error::Error>> {
        let check_id = format!("migration_readiness_{}", Instant::now().elapsed().as_millis());
        let start_time = Instant::now();

        // TODO: Implement migration readiness checks
        // This would involve:
        // 1. Checking if target nodes can accept data
        // 2. Verifying migration protocols are available
        // 3. Checking migration bandwidth

        let passed = true; // Placeholder
        let error = None;
        let mut details = HashMap::new();
        details.insert("source_nodes".to_string(), plan.source_nodes.len().to_string());
        details.insert("target_nodes".to_string(), plan.target_nodes.len().to_string());

        Ok(SafetyCheckResult {
            check_id,
            check_type: SafetyCheckType::MigrationReadiness,
            passed,
            timestamp: start_time,
            error,
            details,
        })
    }

    /// Acquire a rebalancing lock
    pub async fn acquire_rebalancing_lock(
        &mut self,
        node_id: &str,
        reason: String,
        timeout: Duration,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let lock_id = format!("lock_{}_{}", node_id, Instant::now().elapsed().as_millis());

        // Check if node is already locked
        if let Some(existing_lock) = self.rebalancing_locks.get(node_id) {
            if existing_lock.lock_timestamp.elapsed() < existing_lock.lock_timeout {
                return Ok(false); // Already locked
            }
        }

        // Create new lock
        let lock = RebalancingLock {
            lock_id: lock_id.clone(),
            locked_by: self.node_id.clone(),
            lock_timestamp: Instant::now(),
            lock_timeout: timeout,
            lock_reason: reason,
        };

        self.rebalancing_locks.insert(node_id.to_string(), lock);
        Ok(true)
    }

    /// Release a rebalancing lock
    pub async fn release_rebalancing_lock(&mut self, node_id: &str) -> Result<bool, Box<dyn std::error::Error>> {
        if let Some(lock) = self.rebalancing_locks.remove(node_id) {
            tracing::info!("Released rebalancing lock for node {}: {}", node_id, lock.lock_reason);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get active safety checks
    pub fn get_active_checks(&self) -> Vec<&SafetyCheckResult> {
        self.active_checks.values().collect()
    }

    /// Get rebalancing locks
    pub fn get_rebalancing_locks(&self) -> Vec<&RebalancingLock> {
        self.rebalancing_locks.values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;

    #[tokio::test]
    async fn test_safety_coordinator_creation() {
        let config = ClusterConfig::default();
        let state = Arc::new(RwLock::new(RebalancingState::Idle));

        let coordinator = SafetyCoordinator::new("node1".to_string(), state, config);
        assert_eq!(coordinator.node_id, "node1");
        assert_eq!(coordinator.safety_config.min_healthy_nodes, 2);
    }

    #[test]
    fn test_safety_check_result_creation() {
        let result = SafetyCheckResult {
            check_id: "test_check".to_string(),
            check_type: SafetyCheckType::ClusterHealth,
            passed: true,
            timestamp: Instant::now(),
            error: None,
            details: HashMap::new(),
        };

        assert_eq!(result.check_id, "test_check");
        assert!(result.passed);
        assert!(result.error.is_none());
    }

    #[test]
    fn test_rebalancing_lock_creation() {
        let lock = RebalancingLock {
            lock_id: "test_lock".to_string(),
            locked_by: "node1".to_string(),
            lock_timestamp: Instant::now(),
            lock_timeout: Duration::from_secs(60),
            lock_reason: "Testing".to_string(),
        };

        assert_eq!(lock.lock_id, "test_lock");
        assert_eq!(lock.locked_by, "node1");
        assert_eq!(lock.lock_reason, "Testing");
    }
} 