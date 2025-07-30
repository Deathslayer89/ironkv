//! Data migration for rebalancing operations
//! 
//! This module handles the actual data transfer between nodes during
//! rebalancing operations, including batching, progress tracking, and rollback.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use serde::{Deserialize, Serialize};

use super::{RebalancingPlan, RebalancingState};
use crate::config::ClusterConfig;
use crate::cluster::communication::KvCacheClient;
use crate::log::log_cluster_operation;
use crate::metrics::MetricsCollector;

// Serialization helpers for SystemTime and Duration
mod systemtime_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let duration = time.duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| serde::ser::Error::custom("Invalid SystemTime"))?;
        let millis = duration.as_millis() as u64;
        serializer.serialize_u64(millis)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        let duration = Duration::from_millis(millis);
        Ok(SystemTime::UNIX_EPOCH + duration)
    }
}

mod systemtime_serde_opt {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(time: &Option<SystemTime>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match time {
            Some(ts) => {
                let duration = ts.duration_since(SystemTime::UNIX_EPOCH)
                    .map_err(|_| serde::ser::Error::custom("Invalid SystemTime"))?;
                let millis = duration.as_millis() as u64;
                serializer.serialize_some(&millis)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<SystemTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt_millis = Option::<u64>::deserialize(deserializer)?;
        match opt_millis {
            Some(millis) => {
                let duration = Duration::from_millis(millis);
                Ok(Some(SystemTime::UNIX_EPOCH + duration))
            }
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

mod duration_serde_opt {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match duration {
            Some(dur) => {
                let millis = dur.as_millis() as u64;
                serializer.serialize_some(&millis)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt_millis = Option::<u64>::deserialize(deserializer)?;
        match opt_millis {
            Some(millis) => Ok(Some(Duration::from_millis(millis))),
            None => Ok(None),
        }
    }
}

/// Migration state
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MigrationState {
    /// Migration not started
    NotStarted,
    /// Migration is in progress
    InProgress,
    /// Migration completed successfully
    Completed,
    /// Migration failed
    Failed,
    /// Migration was cancelled
    Cancelled,
    /// Migration is rolling back
    RollingBack,
}

/// Migration batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationBatch {
    /// Batch ID
    pub batch_id: String,
    /// Source node
    pub source_node: String,
    /// Target node
    pub target_node: String,
    /// Keys in this batch
    pub keys: Vec<String>,
    /// Batch size in bytes
    pub size_bytes: u64,
    /// Batch state
    pub state: BatchState,
    /// Start time (as u64 milliseconds since epoch)
    #[serde(with = "systemtime_serde_opt")]
    pub start_time: Option<SystemTime>,
    /// End time (as u64 milliseconds since epoch)
    #[serde(with = "systemtime_serde_opt")]
    pub end_time: Option<SystemTime>,
    /// Error message (if failed)
    pub error: Option<String>,
}

/// Batch state
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BatchState {
    /// Batch is pending
    Pending,
    /// Batch is in progress
    InProgress,
    /// Batch completed successfully
    Completed,
    /// Batch failed
    Failed,
    /// Batch was cancelled
    Cancelled,
}

/// Migration progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationProgress {
    /// Total keys to migrate
    pub total_keys: usize,
    /// Keys migrated so far
    pub migrated_keys: usize,
    /// Total size to migrate
    pub total_size: u64,
    /// Size migrated so far
    pub migrated_size: u64,
    /// Progress percentage (0-100)
    pub progress_percentage: u8,
    /// Estimated time remaining (as u64 milliseconds)
    #[serde(with = "duration_serde_opt")]
    pub estimated_time_remaining: Option<Duration>,
    /// Current batch being processed
    pub current_batch: Option<String>,
    /// Failed batches
    pub failed_batches: Vec<String>,
}

/// Migration coordinator
pub struct MigrationCoordinator {
    /// Node ID
    node_id: String,
    /// Rebalancing state
    state: Arc<RwLock<RebalancingState>>,
    /// Configuration
    config: ClusterConfig,
    /// Current migration plan
    current_plan: Option<RebalancingPlan>,
    /// Migration batches
    batches: HashMap<String, MigrationBatch>,
    /// Migration progress
    progress: MigrationProgress,
    /// Batch size for migration
    batch_size: usize,
    /// Migration timeout
    migration_timeout: Duration,
    /// Control channel for stopping
    stop_tx: Option<mpsc::Sender<()>>,
    /// Background task handle
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl MigrationCoordinator {
    /// Create a new migration coordinator
    pub fn new(
        node_id: String,
        state: Arc<RwLock<RebalancingState>>,
        config: ClusterConfig,
    ) -> Self {
        Self {
            node_id,
            state,
            config,
            current_plan: None,
            batches: HashMap::new(),
            progress: MigrationProgress {
                total_keys: 0,
                migrated_keys: 0,
                total_size: 0,
                migrated_size: 0,
                progress_percentage: 0,
                estimated_time_remaining: None,
                current_batch: None,
                failed_batches: Vec::new(),
            },
            batch_size: 100, // Default batch size
            migration_timeout: Duration::from_secs(300), // 5 minutes
            stop_tx: None,
            task_handle: None,
        }
    }

    /// Start the migration coordinator
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Starting migration coordinator for node: {}", self.node_id);

        let (stop_tx, stop_rx) = mpsc::channel(1);
        self.stop_tx = Some(stop_tx);

        let node_id = self.node_id.clone();
        let state = Arc::clone(&self.state);
        let config = self.config.clone();
        let migration_timeout = self.migration_timeout;

        let handle = tokio::spawn(async move {
            Self::run_migration_loop(
                node_id,
                state,
                config,
                migration_timeout,
                stop_rx,
            )
            .await;
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    /// Stop the migration coordinator
    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Stopping migration coordinator for node: {}", self.node_id);

        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(()).await;
        }

        if let Some(handle) = self.task_handle.take() {
            handle.await?;
        }

        Ok(())
    }

    /// Run the main migration loop
    async fn run_migration_loop(
        node_id: String,
        state: Arc<RwLock<RebalancingState>>,
        config: ClusterConfig,
        migration_timeout: Duration,
        mut stop_rx: mpsc::Receiver<()>,
    ) {
        let mut timer = interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = timer.tick() => {
                    if let Err(e) = Self::process_migration(&node_id, &state, &config, migration_timeout).await {
                        tracing::error!("Migration processing error: {}", e);
                    }
                }
                _ = stop_rx.recv() => {
                    tracing::info!("Migration coordinator stopping");
                    break;
                }
            }
        }
    }

    /// Process migration batches
    async fn process_migration(
        node_id: &str,
        state: &Arc<RwLock<RebalancingState>>,
        config: &ClusterConfig,
        migration_timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let current_state = state.read().await;
        if *current_state != RebalancingState::InProgress {
            return Ok(()); // Not currently migrating
        }

        // TODO: Implement actual batch processing
        // This would involve:
        // 1. Getting pending batches
        // 2. Processing each batch
        // 3. Updating progress
        // 4. Handling failures

        Ok(())
    }

    /// Start migration for a rebalancing plan
    pub async fn start_migration(
        &mut self,
        plan: RebalancingPlan,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = SystemTime::now();

        tracing::info!("Starting migration for plan: {}", plan.plan_id);

        // Store the plan
        self.current_plan = Some(plan.clone());

        // Create migration batches
        self.create_migration_batches(&plan).await?;

        // Update progress
        self.update_progress(&plan).await;

        // Log operation
        let duration = SystemTime::now().duration_since(start_time).unwrap_or_default();
        let details = vec![
            ("plan_id", plan.plan_id.clone()),
        ];
        log_cluster_operation(
            "migration_start",
            &self.node_id,
            true,
            duration,
            Some(details),
        );

        Ok(())
    }

    /// Create migration batches from a plan
    async fn create_migration_batches(
        &mut self,
        plan: &RebalancingPlan,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut batch_id_counter = 0;

        for (source_node, keys) in &plan.source_nodes {
            for (target_node, target_keys) in &plan.target_nodes {
                // Find keys that need to move from source to target
                let keys_to_move: Vec<String> = keys
                    .iter()
                    .filter(|key| target_keys.contains(key))
                    .cloned()
                    .collect();

                if keys_to_move.is_empty() {
                    continue;
                }

                // Split keys into batches
                for chunk in keys_to_move.chunks(self.batch_size) {
                    let batch_id = format!("batch_{}_{}", batch_id_counter, SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_millis());
                    batch_id_counter += 1;

                    let batch = MigrationBatch {
                        batch_id: batch_id.clone(),
                        source_node: source_node.clone(),
                        target_node: target_node.clone(),
                        keys: chunk.to_vec(),
                        size_bytes: 0, // TODO: Calculate actual size
                        state: BatchState::Pending,
                        start_time: None,
                        end_time: None,
                        error: None,
                    };

                    self.batches.insert(batch_id, batch);
                }
            }
        }

        Ok(())
    }

    /// Update migration progress
    async fn update_progress(&mut self, plan: &RebalancingPlan) {
        let total_keys = plan.keys_to_move.len();
        let total_size = plan.estimated_size;

        let completed_batches = self.batches.values()
            .filter(|batch| batch.state == BatchState::Completed)
            .count();

        let failed_batches = self.batches.values()
            .filter(|batch| batch.state == BatchState::Failed)
            .collect::<Vec<_>>();

        let migrated_keys = completed_batches * self.batch_size;
        let migrated_size = (migrated_keys as u64) * (total_size / total_keys as u64);

        let progress_percentage = if total_keys > 0 {
            ((migrated_keys as f64 / total_keys as f64) * 100.0) as u8
        } else {
            0
        };

        self.progress = MigrationProgress {
            total_keys,
            migrated_keys,
            total_size,
            migrated_size,
            progress_percentage,
            estimated_time_remaining: None, // TODO: Calculate based on rate
            current_batch: None, // TODO: Track current batch
            failed_batches: failed_batches.iter().map(|b| b.batch_id.clone()).collect(),
        };
    }

    /// Process a migration batch
    async fn process_batch(
        &mut self,
        batch: &mut MigrationBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start_time = SystemTime::now();

        // Update batch state
        batch.state = BatchState::InProgress;
        batch.start_time = Some(start_time);

        // Connect to source node
        let source_address = format!("http://{}:{}", batch.source_node, 50051);
        let mut source_client = match KvCacheClient::connect(source_address).await {
            Ok(client) => client,
            Err(e) => {
                batch.state = BatchState::Failed;
                batch.error = Some(format!("Failed to connect to source: {}", e));
                return Err(e);
            }
        };

        // Connect to target node
        let target_address = format!("http://{}:{}", batch.target_node, 50051);
        let mut target_client = match KvCacheClient::connect(target_address).await {
            Ok(client) => client,
            Err(e) => {
                batch.state = BatchState::Failed;
                batch.error = Some(format!("Failed to connect to target: {}", e));
                return Err(e);
            }
        };

        // Migrate each key in the batch
        for key in &batch.keys {
            // Get value from source
            let source_response = match source_client.forward_request(
                "GET".to_string(),
                key.clone(),
                String::new(),
                0,
            ).await {
                Ok(response) => response,
                Err(e) => {
                    batch.state = BatchState::Failed;
                    batch.error = Some(format!("Failed to get key {} from source: {}", key, e));
                    return Err(e);
                }
            };

            if source_response.success && !source_response.data.is_empty() {
                // Set value on target
                let target_response = match target_client.forward_request(
                    "SET".to_string(),
                    key.clone(),
                    source_response.data,
                    0,
                ).await {
                    Ok(response) => response,
                    Err(e) => {
                        batch.state = BatchState::Failed;
                        batch.error = Some(format!("Failed to set key {} on target: {}", key, e));
                        return Err(e);
                    }
                };

                if !target_response.success {
                    batch.state = BatchState::Failed;
                    batch.error = Some(format!("Failed to set key {} on target", key));
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Target write failed for key {}", key)
                    )));
                }

                // Delete from source (optional, depending on strategy)
                // source_client.forward_request("DEL".to_string(), key.clone(), String::new(), 0).await?;
            }
        }

        // Mark batch as completed
        batch.state = BatchState::Completed;
        batch.end_time = Some(SystemTime::now());

        // Update progress
        if let Some(plan) = &self.current_plan {
            let plan_clone = plan.clone();
            self.update_progress(&plan_clone).await;
        }

        Ok(())
    }

    /// Cancel ongoing migration
    pub async fn cancel_migration(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Cancelling migration for node: {}", self.node_id);

        // Cancel all pending and in-progress batches
        for batch in self.batches.values_mut() {
            if batch.state == BatchState::Pending || batch.state == BatchState::InProgress {
                batch.state = BatchState::Cancelled;
                batch.end_time = Some(SystemTime::now());
            }
        }

        // Clear current plan
        self.current_plan = None;

        Ok(())
    }

    /// Get migration progress
    pub fn get_progress(&self) -> &MigrationProgress {
        &self.progress
    }

    /// Get migration batches
    pub fn get_batches(&self) -> Vec<&MigrationBatch> {
        self.batches.values().collect()
    }

    /// Get batch by ID
    pub fn get_batch(&self, batch_id: &str) -> Option<&MigrationBatch> {
        self.batches.get(batch_id)
    }

    /// Set batch size
    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size;
    }

    /// Set migration timeout
    pub fn set_migration_timeout(&mut self, timeout: Duration) {
        self.migration_timeout = timeout;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;

    #[tokio::test]
    async fn test_migration_coordinator_creation() {
        let config = ClusterConfig::default();
        let state = Arc::new(RwLock::new(RebalancingState::Idle));

        let coordinator = MigrationCoordinator::new("node1".to_string(), state, config);
        assert_eq!(coordinator.node_id, "node1");
        assert_eq!(coordinator.batch_size, 100);
    }

    #[test]
    fn test_migration_batch_creation() {
        let batch = MigrationBatch {
            batch_id: "test_batch".to_string(),
            source_node: "node1".to_string(),
            target_node: "node2".to_string(),
            keys: vec!["key1".to_string(), "key2".to_string()],
            size_bytes: 1024,
            state: BatchState::Pending,
            start_time: None,
            end_time: None,
            error: None,
        };

        assert_eq!(batch.batch_id, "test_batch");
        assert_eq!(batch.state, BatchState::Pending);
        assert_eq!(batch.keys.len(), 2);
    }

    #[test]
    fn test_migration_progress_creation() {
        let progress = MigrationProgress {
            total_keys: 1000,
            migrated_keys: 500,
            total_size: 1024 * 1024,
            migrated_size: 512 * 1024,
            progress_percentage: 50,
            estimated_time_remaining: Some(Duration::from_secs(60)),
            current_batch: Some("batch1".to_string()),
            failed_batches: Vec::new(),
        };

        assert_eq!(progress.total_keys, 1000);
        assert_eq!(progress.migrated_keys, 500);
        assert_eq!(progress.progress_percentage, 50);
    }
} 