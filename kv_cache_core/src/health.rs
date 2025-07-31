//! Health check system for the distributed key-value cache
//! 
//! This module provides comprehensive health monitoring including:
//! - Service health checks
//! - Cluster health monitoring
//! - Resource usage monitoring
//! - Dependency health checks
//! - Health check endpoints for external monitoring

use crate::config::HealthConfig;
use crate::metrics::MetricsCollector;
use crate::store::Store;
use crate::consensus::state::{RaftState, RaftRole};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};

/// Health check status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HealthStatus {
    /// Service is healthy
    Healthy,
    /// Service is degraded but functional
    Degraded,
    /// Service is unhealthy
    Unhealthy,
}

/// Health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    /// Overall health status
    pub status: HealthStatus,
    /// Health check name
    pub name: String,
    /// Health check description
    pub description: String,
    /// Last check timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Check duration in milliseconds
    pub duration_ms: u64,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
    /// Error message if unhealthy
    pub error: Option<String>,
}

/// Comprehensive health check response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Overall health status
    pub status: HealthStatus,
    /// Service name
    pub service: String,
    /// Service version
    pub version: String,
    /// Timestamp of health check
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Individual health check results
    pub checks: HashMap<String, HealthCheckResult>,
    /// Summary information
    pub summary: HealthSummary,
}

/// Health summary information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthSummary {
    /// Total number of checks
    pub total_checks: usize,
    /// Number of healthy checks
    pub healthy_checks: usize,
    /// Number of degraded checks
    pub degraded_checks: usize,
    /// Number of unhealthy checks
    pub unhealthy_checks: usize,
    /// Uptime in seconds
    pub uptime_seconds: u64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// Active connections
    pub active_connections: usize,
}

/// Health check system
pub struct HealthChecker {
    config: HealthConfig,
    store: Arc<Store>,
    metrics: Arc<MetricsCollector>,
    raft_state: Arc<RwLock<RaftState>>,
    startup_time: Instant,
    last_health_check: Arc<RwLock<Option<HealthResponse>>>,
}

/// Health check trait for different components
#[async_trait::async_trait]
pub trait HealthCheck {
    /// Perform the health check
    async fn check(&self) -> HealthCheckResult;
    
    /// Get the name of this health check
    fn name(&self) -> &str;
    
    /// Get the description of this health check
    fn description(&self) -> &str;
}

/// Store health check
pub struct StoreHealthCheck {
    store: Arc<Store>,
}

/// Cluster health check
pub struct ClusterHealthCheck {
    raft_state: Arc<RwLock<RaftState>>,
}

/// Memory health check
pub struct MemoryHealthCheck {
    metrics: Arc<MetricsCollector>,
}

/// Network health check
pub struct NetworkHealthCheck {
    metrics: Arc<MetricsCollector>,
}

/// Persistence health check
pub struct PersistenceHealthCheck {
    store: Arc<Store>,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new(
        config: HealthConfig,
        store: Arc<Store>,
        metrics: Arc<MetricsCollector>,
        raft_state: Arc<RwLock<RaftState>>,
    ) -> Self {
        Self {
            config,
            store,
            metrics,
            raft_state,
            startup_time: Instant::now(),
            last_health_check: Arc::new(RwLock::new(None)),
        }
    }

    /// Perform comprehensive health check
    pub async fn check_health(&self) -> HealthResponse {
        let start_time = Instant::now();
        let mut checks = HashMap::new();

        // Perform individual health checks
        let store_check = StoreHealthCheck {
            store: self.store.clone(),
        };
        checks.insert(store_check.name().to_string(), store_check.check().await);

        let cluster_check = ClusterHealthCheck {
            raft_state: self.raft_state.clone(),
        };
        checks.insert(cluster_check.name().to_string(), cluster_check.check().await);

        let memory_check = MemoryHealthCheck {
            metrics: self.metrics.clone(),
        };
        checks.insert(memory_check.name().to_string(), memory_check.check().await);

        let network_check = NetworkHealthCheck {
            metrics: self.metrics.clone(),
        };
        checks.insert(network_check.name().to_string(), network_check.check().await);

        let persistence_check = PersistenceHealthCheck {
            store: self.store.clone(),
        };
        checks.insert(persistence_check.name().to_string(), persistence_check.check().await);

        // Determine overall status
        let overall_status = self.determine_overall_status(&checks);
        
        // Create summary
        let summary = self.create_summary(&checks);

        let response = HealthResponse {
            status: overall_status,
            service: "kv-cache".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            timestamp: chrono::Utc::now(),
            checks,
            summary,
        };

        // Cache the result
        {
            let mut last_check = self.last_health_check.write().await;
            *last_check = Some(response.clone());
        }

        tracing::info!(
            "Health check completed: status={:?}, duration_ms={}",
            response.status,
            start_time.elapsed().as_millis()
        );

        response
    }

    /// Get the last health check result
    pub async fn get_last_health_check(&self) -> Option<HealthResponse> {
        self.last_health_check.read().await.clone()
    }

    /// Perform a quick health check (for load balancers)
    pub async fn quick_health_check(&self) -> HealthStatus {
        let store_check = StoreHealthCheck {
            store: self.store.clone(),
        };
        
        let result = store_check.check().await;
        result.status
    }

    /// Determine overall health status based on individual checks
    fn determine_overall_status(&self, checks: &HashMap<String, HealthCheckResult>) -> HealthStatus {
        let mut unhealthy_count = 0;
        let mut degraded_count = 0;

        for check in checks.values() {
            match check.status {
                HealthStatus::Unhealthy => unhealthy_count += 1,
                HealthStatus::Degraded => degraded_count += 1,
                HealthStatus::Healthy => {}
            }
        }

        if unhealthy_count > 0 {
            HealthStatus::Unhealthy
        } else if degraded_count > 0 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        }
    }

    /// Create health summary
    fn create_summary(&self, checks: &HashMap<String, HealthCheckResult>) -> HealthSummary {
        let mut healthy_count = 0;
        let mut degraded_count = 0;
        let mut unhealthy_count = 0;

        for check in checks.values() {
            match check.status {
                HealthStatus::Healthy => healthy_count += 1,
                HealthStatus::Degraded => degraded_count += 1,
                HealthStatus::Unhealthy => unhealthy_count += 1,
            }
        }

        let uptime = self.startup_time.elapsed().as_secs();
        
        // Get memory usage from metrics
        let memory_usage = self.metrics.get_metrics_summary().cache.memory_usage_bytes;
        let active_connections = self.metrics.get_metrics_summary().server.active_connections;

        HealthSummary {
            total_checks: checks.len(),
            healthy_checks: healthy_count,
            degraded_checks: degraded_count,
            unhealthy_checks: unhealthy_count,
            uptime_seconds: uptime,
            memory_usage_bytes: memory_usage,
            active_connections,
        }
    }

    /// Start health check monitoring
    pub async fn start_monitoring(&self) -> Result<(), Box<dyn std::error::Error>> {
        let config = self.config.clone();
        let health_checker = self.clone_for_monitoring();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(config.check_interval));
            
            loop {
                interval.tick().await;
                
                let _ = health_checker.check_health().await;
            }
        });

        Ok(())
    }

    /// Clone for monitoring (without Arc fields)
    fn clone_for_monitoring(&self) -> Self {
        Self {
            config: self.config.clone(),
            store: self.store.clone(),
            metrics: self.metrics.clone(),
            raft_state: self.raft_state.clone(),
            startup_time: self.startup_time,
            last_health_check: self.last_health_check.clone(),
        }
    }
}

#[async_trait::async_trait]
impl HealthCheck for StoreHealthCheck {
    async fn check(&self) -> HealthCheckResult {
        let start_time = Instant::now();
        let mut metadata = HashMap::new();
        let mut error = None;

        // Check if store is accessible
        let store_status = match self.store.get_stats().await {
            Ok(stats) => {
                metadata.insert("total_keys".to_string(), stats.total_keys.to_string());
                metadata.insert("memory_usage_bytes".to_string(), stats.memory_usage_bytes.to_string());
                HealthStatus::Healthy
            }
            Err(e) => {
                error = Some(format!("Store access failed: {}", e));
                HealthStatus::Unhealthy
            }
        };

        HealthCheckResult {
            status: store_status,
            name: "store".to_string(),
            description: "Key-value store health check".to_string(),
            timestamp: chrono::Utc::now(),
            duration_ms: start_time.elapsed().as_millis() as u64,
            metadata,
            error,
        }
    }

    fn name(&self) -> &str {
        "store"
    }

    fn description(&self) -> &str {
        "Key-value store health check"
    }
}

#[async_trait::async_trait]
impl HealthCheck for ClusterHealthCheck {
    async fn check(&self) -> HealthCheckResult {
        let start_time = Instant::now();
        let mut metadata = HashMap::new();
        let mut error = None;

        let raft_state = self.raft_state.read().await;
        
        // Check cluster state
        let cluster_status = match raft_state.role {
            RaftRole::Leader => {
                metadata.insert("role".to_string(), "leader".to_string());
                metadata.insert("term".to_string(), raft_state.current_term.to_string());
                HealthStatus::Healthy
            }
            RaftRole::Follower => {
                metadata.insert("role".to_string(), "follower".to_string());
                metadata.insert("term".to_string(), raft_state.current_term.to_string());
                HealthStatus::Healthy
            }
            RaftRole::Candidate => {
                metadata.insert("role".to_string(), "candidate".to_string());
                metadata.insert("term".to_string(), raft_state.current_term.to_string());
                HealthStatus::Degraded
            }
        };

        HealthCheckResult {
            status: cluster_status,
            name: "cluster".to_string(),
            description: "Cluster consensus health check".to_string(),
            timestamp: chrono::Utc::now(),
            duration_ms: start_time.elapsed().as_millis() as u64,
            metadata,
            error,
        }
    }

    fn name(&self) -> &str {
        "cluster"
    }

    fn description(&self) -> &str {
        "Cluster consensus health check"
    }
}

#[async_trait::async_trait]
impl HealthCheck for MemoryHealthCheck {
    async fn check(&self) -> HealthCheckResult {
        let start_time = Instant::now();
        let mut metadata = HashMap::new();
        let mut error = None;

        let metrics = self.metrics.get_metrics_summary();
        let memory_usage = metrics.cache.memory_usage_bytes;
        
        // Check memory usage thresholds
        let memory_status = if memory_usage > 1_000_000_000 { // 1GB
            error = Some("Memory usage is high".to_string());
            HealthStatus::Degraded
        } else if memory_usage > 2_000_000_000 { // 2GB
            error = Some("Memory usage is very high".to_string());
            HealthStatus::Unhealthy
        } else {
            HealthStatus::Healthy
        };

        metadata.insert("memory_usage_bytes".to_string(), memory_usage.to_string());
        metadata.insert("cache_hit_ratio".to_string(), format!("{:.2}", metrics.cache.hit_ratio));

        HealthCheckResult {
            status: memory_status,
            name: "memory".to_string(),
            description: "Memory usage health check".to_string(),
            timestamp: chrono::Utc::now(),
            duration_ms: start_time.elapsed().as_millis() as u64,
            metadata,
            error,
        }
    }

    fn name(&self) -> &str {
        "memory"
    }

    fn description(&self) -> &str {
        "Memory usage health check"
    }
}

#[async_trait::async_trait]
impl HealthCheck for NetworkHealthCheck {
    async fn check(&self) -> HealthCheckResult {
        let start_time = Instant::now();
        let mut metadata = HashMap::new();
        let mut error = None;

        let metrics = self.metrics.get_metrics_summary();
        let network_metrics = &metrics.network;
        
        // Check network health
        let network_status = if network_metrics.network_errors > 100 {
            error = Some("High network error rate".to_string());
            HealthStatus::Degraded
        } else if network_metrics.network_errors > 1000 {
            error = Some("Very high network error rate".to_string());
            HealthStatus::Unhealthy
        } else {
            HealthStatus::Healthy
        };

        metadata.insert("network_errors".to_string(), network_metrics.network_errors.to_string());
        metadata.insert("healthy_connections".to_string(), network_metrics.healthy_connections.to_string());
        metadata.insert("unhealthy_connections".to_string(), network_metrics.unhealthy_connections.to_string());

        HealthCheckResult {
            status: network_status,
            name: "network".to_string(),
            description: "Network connectivity health check".to_string(),
            timestamp: chrono::Utc::now(),
            duration_ms: start_time.elapsed().as_millis() as u64,
            metadata,
            error,
        }
    }

    fn name(&self) -> &str {
        "network"
    }

    fn description(&self) -> &str {
        "Network connectivity health check"
    }
}

#[async_trait::async_trait]
impl HealthCheck for PersistenceHealthCheck {
    async fn check(&self) -> HealthCheckResult {
        let start_time = Instant::now();
        let mut metadata = HashMap::new();
        let mut error = None;

        // Check persistence status
        let persistence_status = match self.store.get_persistence_status().await {
            Ok(status) => {
                metadata.insert("aof_enabled".to_string(), status.aof_enabled.to_string());
                metadata.insert("snapshot_enabled".to_string(), status.snapshot_enabled.to_string());
                if let Some(last_snapshot) = status.last_snapshot_time {
                    metadata.insert("last_snapshot".to_string(), last_snapshot.to_string());
                }
                HealthStatus::Healthy
            }
            Err(e) => {
                error = Some(format!("Persistence check failed: {}", e));
                HealthStatus::Degraded
            }
        };

        HealthCheckResult {
            status: persistence_status,
            name: "persistence".to_string(),
            description: "Data persistence health check".to_string(),
            timestamp: chrono::Utc::now(),
            duration_ms: start_time.elapsed().as_millis() as u64,
            metadata,
            error,
        }
    }

    fn name(&self) -> &str {
        "persistence"
    }

    fn description(&self) -> &str {
        "Data persistence health check"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use crate::metrics::MetricsCollector;
    use crate::config::{CacheConfig, HealthConfig};

    #[tokio::test]
    async fn test_health_checker_creation() {
        let config = HealthConfig::default();
        let store = Arc::new(Store::new());
        let metrics = Arc::new(MetricsCollector::new(crate::config::MetricsConfig::default()));
        let raft_state = Arc::new(RwLock::new(RaftState::new("test-node".to_string())));

        let health_checker = HealthChecker::new(config, store, metrics, raft_state);
        let health_response = health_checker.check_health().await;
        assert_eq!(health_response.service, "kv-cache");
    }

    #[tokio::test]
    async fn test_store_health_check() {
        let store = Arc::new(Store::new());
        let health_check = StoreHealthCheck { store };

        let result = health_check.check().await;
        assert_eq!(result.name, "store");
        assert_eq!(result.status, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_memory_health_check() {
        let metrics = Arc::new(MetricsCollector::new(crate::config::MetricsConfig::default()));
        let health_check = MemoryHealthCheck { metrics };

        let result = health_check.check().await;
        assert_eq!(result.name, "memory");
        assert!(result.metadata.contains_key("memory_usage_bytes"));
    }
} 