//! Metrics collection for the distributed key-value cache
//! 
//! This module provides metrics collection using the `metrics` crate with
//! Prometheus-compatible output for monitoring and observability.

use crate::config::MetricsConfig;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Metrics collector for the cache system
pub struct MetricsCollector {
    config: MetricsConfig,
    server: Arc<ServerMetrics>,
    cache: Arc<CacheMetrics>,
    cluster: Arc<ClusterMetrics>,
    network: Arc<NetworkMetrics>,
    persistence: Arc<PersistenceMetrics>,
    histograms: Arc<RwLock<HashMap<String, HistogramMetrics>>>,
}

/// Server-related metrics
#[derive(Debug)]
pub struct ServerMetrics {
    pub active_connections: AtomicUsize,
    pub total_connections: AtomicU64,
    pub requests_per_second: AtomicU64,
    pub uptime_seconds: AtomicU64,
    pub startup_time: Instant,
}

/// Cache-related metrics
#[derive(Debug)]
pub struct CacheMetrics {
    pub total_keys: AtomicUsize,
    pub memory_usage_bytes: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub evictions: AtomicU64,
    pub expired_keys: AtomicU64,
    pub set_operations: AtomicU64,
    pub get_operations: AtomicU64,
    pub delete_operations: AtomicU64,
    pub set_latency_ms: AtomicU64,
    pub get_latency_ms: AtomicU64,
    pub delete_latency_ms: AtomicU64,
}

/// Cluster-related metrics
#[derive(Debug)]
pub struct ClusterMetrics {
    pub total_nodes: AtomicUsize,
    pub healthy_nodes: AtomicUsize,
    pub failed_nodes: AtomicUsize,
    pub replication_factor: AtomicUsize,
    pub replication_lag_ms: AtomicU64,
    pub cluster_operations: AtomicU64,
    pub cluster_errors: AtomicU64,
    pub heartbeat_interval_ms: AtomicU64,
    pub membership_changes: AtomicU64,
}

/// Network-related metrics
#[derive(Debug)]
pub struct NetworkMetrics {
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub connections_established: AtomicU64,
    pub connections_closed: AtomicU64,
    pub network_errors: AtomicU64,
    pub request_timeout_errors: AtomicU64,
    pub network_latency_ms: AtomicU64,
    // Enhanced network metrics for Phase 1.2
    pub healthy_connections: AtomicUsize,
    pub unhealthy_connections: AtomicUsize,
    pub connection_health_checks: AtomicU64,
    pub failed_health_checks: AtomicU64,
    pub network_partitions_detected: AtomicU64,
    pub connection_pool_size: AtomicUsize,
    pub connection_pool_evictions: AtomicU64,
    pub retry_attempts: AtomicU64,
    pub retry_successes: AtomicU64,
    pub retry_failures: AtomicU64,
    pub average_connection_latency_ms: AtomicU64,
    pub max_connection_latency_ms: AtomicU64,
    pub min_connection_latency_ms: AtomicU64,
}

/// Persistence-related metrics
#[derive(Debug)]
pub struct PersistenceMetrics {
    pub aof_size_bytes: AtomicU64,
    pub snapshot_size_bytes: AtomicU64,
    pub aof_rewrites: AtomicU64,
    pub snapshots_created: AtomicU64,
    pub persistence_errors: AtomicU64,
    pub last_snapshot_time: AtomicU64,
    pub last_aof_rewrite_time: AtomicU64,
}

/// Histogram metrics for detailed latency tracking
#[derive(Debug)]
pub struct HistogramMetrics {
    pub name: String,
    pub buckets: Vec<f64>,
    pub counts: Vec<AtomicU64>,
    pub sum: AtomicU64,
    pub count: AtomicU64,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(config: MetricsConfig) -> Self {
        let startup_time = Instant::now();
        
        Self {
            server: Arc::new(ServerMetrics {
                active_connections: AtomicUsize::new(0),
                total_connections: AtomicU64::new(0),
                requests_per_second: AtomicU64::new(0),
                uptime_seconds: AtomicU64::new(0),
                startup_time,
            }),
            cache: Arc::new(CacheMetrics {
                total_keys: AtomicUsize::new(0),
                memory_usage_bytes: AtomicU64::new(0),
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                evictions: AtomicU64::new(0),
                expired_keys: AtomicU64::new(0),
                set_operations: AtomicU64::new(0),
                get_operations: AtomicU64::new(0),
                delete_operations: AtomicU64::new(0),
                set_latency_ms: AtomicU64::new(0),
                get_latency_ms: AtomicU64::new(0),
                delete_latency_ms: AtomicU64::new(0),
            }),
            cluster: Arc::new(ClusterMetrics {
                total_nodes: AtomicUsize::new(0),
                healthy_nodes: AtomicUsize::new(0),
                failed_nodes: AtomicUsize::new(0),
                replication_factor: AtomicUsize::new(0),
                replication_lag_ms: AtomicU64::new(0),
                cluster_operations: AtomicU64::new(0),
                cluster_errors: AtomicU64::new(0),
                heartbeat_interval_ms: AtomicU64::new(0),
                membership_changes: AtomicU64::new(0),
            }),
            network: Arc::new(NetworkMetrics {
                bytes_sent: AtomicU64::new(0),
                bytes_received: AtomicU64::new(0),
                connections_established: AtomicU64::new(0),
                connections_closed: AtomicU64::new(0),
                network_errors: AtomicU64::new(0),
                request_timeout_errors: AtomicU64::new(0),
                network_latency_ms: AtomicU64::new(0),
                healthy_connections: AtomicUsize::new(0),
                unhealthy_connections: AtomicUsize::new(0),
                connection_health_checks: AtomicU64::new(0),
                failed_health_checks: AtomicU64::new(0),
                network_partitions_detected: AtomicU64::new(0),
                connection_pool_size: AtomicUsize::new(0),
                connection_pool_evictions: AtomicU64::new(0),
                retry_attempts: AtomicU64::new(0),
                retry_successes: AtomicU64::new(0),
                retry_failures: AtomicU64::new(0),
                average_connection_latency_ms: AtomicU64::new(0),
                max_connection_latency_ms: AtomicU64::new(0),
                min_connection_latency_ms: AtomicU64::new(0),
            }),
            persistence: Arc::new(PersistenceMetrics {
                aof_size_bytes: AtomicU64::new(0),
                snapshot_size_bytes: AtomicU64::new(0),
                aof_rewrites: AtomicU64::new(0),
                snapshots_created: AtomicU64::new(0),
                persistence_errors: AtomicU64::new(0),
                last_snapshot_time: AtomicU64::new(0),
                last_aof_rewrite_time: AtomicU64::new(0),
            }),
            histograms: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Start the metrics collection
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.config.enabled {
            return Ok(());
        }

        // Initialize metrics with custom labels
        self.initialize_metrics();

        // Start background collection task
        let server = Arc::clone(&self.server);
        let collection_interval = self.config.collection_interval;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(collection_interval));
            loop {
                interval.tick().await;
                Self::update_uptime(&server);
            }
        });

        Ok(())
    }

    /// Initialize metrics with custom labels
    fn initialize_metrics(&self) {
        // Set custom labels
        for (key, value) in &self.config.labels {
            metrics::gauge!("cache_labels", 1.0, "key" => key.clone(), "value" => value.clone());
        }
    }

    /// Update uptime metric
    fn update_uptime(server: &ServerMetrics) {
        let uptime = server.startup_time.elapsed().as_secs();
        server.uptime_seconds.store(uptime, Ordering::Relaxed);
        metrics::gauge!("cache_uptime_seconds", uptime as f64);
    }

    /// Record a cache operation
    pub fn record_cache_operation(&self, operation: &str, duration: Duration, success: bool) {
        let duration_ms = duration.as_millis() as u64;
        
        match operation {
            "SET" => {
                self.cache.set_operations.fetch_add(1, Ordering::Relaxed);
                self.cache.set_latency_ms.store(duration_ms, Ordering::Relaxed);
                metrics::counter!("cache_set_operations_total", 1);
                metrics::gauge!("cache_set_latency_ms", duration_ms as f64);
            }
            "GET" => {
                self.cache.get_operations.fetch_add(1, Ordering::Relaxed);
                self.cache.get_latency_ms.store(duration_ms, Ordering::Relaxed);
                metrics::counter!("cache_get_operations_total", 1);
                metrics::gauge!("cache_get_latency_ms", duration_ms as f64);
            }
            "DELETE" => {
                self.cache.delete_operations.fetch_add(1, Ordering::Relaxed);
                self.cache.delete_latency_ms.store(duration_ms, Ordering::Relaxed);
                metrics::counter!("cache_delete_operations_total", 1);
                metrics::gauge!("cache_delete_latency_ms", duration_ms as f64);
            }
            _ => {}
        }

        if success {
            metrics::counter!("cache_operations_success_total", 1, "operation" => operation.to_string());
        } else {
            metrics::counter!("cache_operations_error_total", 1, "operation" => operation.to_string());
        }

        // Record histogram if enabled
        if self.config.histograms {
            self.record_histogram(&format!("cache_{}_latency", operation.to_lowercase()), duration_ms);
        }
    }

    /// Record a cache hit or miss
    pub fn record_cache_access(&self, hit: bool) {
        if hit {
            self.cache.cache_hits.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("cache_hits_total", 1);
        } else {
            self.cache.cache_misses.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("cache_misses_total", 1);
        }
    }

    /// Update memory usage
    pub fn update_memory_usage(&self, bytes: u64, key_count: usize) {
        self.cache.memory_usage_bytes.store(bytes, Ordering::Relaxed);
        self.cache.total_keys.store(key_count, Ordering::Relaxed);
        
        metrics::gauge!("cache_memory_usage_bytes", bytes as f64);
        metrics::gauge!("cache_total_keys", key_count as f64);
    }

    /// Record an eviction
    pub fn record_eviction(&self, reason: &str) {
        self.cache.evictions.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("cache_evictions_total", 1, "reason" => reason.to_string());
    }

    /// Record an expired key
    pub fn record_expired_key(&self) {
        self.cache.expired_keys.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("cache_expired_keys_total", 1);
    }

    /// Record a connection event
    pub fn record_connection_event(&self, event: &str) {
        match event {
            "established" => {
                self.network.connections_established.fetch_add(1, Ordering::Relaxed);
                self.server.total_connections.fetch_add(1, Ordering::Relaxed);
                metrics::counter!("cache_connections_established_total", 1);
            }
            "closed" => {
                self.network.connections_closed.fetch_add(1, Ordering::Relaxed);
                metrics::counter!("cache_connections_closed_total", 1);
            }
            _ => {}
        }
    }

    /// Update active connections count
    pub fn update_active_connections(&self, count: usize) {
        self.server.active_connections.store(count, Ordering::Relaxed);
        metrics::gauge!("cache_active_connections", count as f64);
    }

    /// Record network I/O
    pub fn record_network_io(&self, bytes_sent: u64, bytes_received: u64) {
        self.network.bytes_sent.fetch_add(bytes_sent, Ordering::Relaxed);
        self.network.bytes_received.fetch_add(bytes_received, Ordering::Relaxed);
        
        metrics::counter!("cache_network_bytes_sent_total", bytes_sent);
        metrics::counter!("cache_network_bytes_received_total", bytes_received);
    }

    /// Record connection health check results
    pub fn record_connection_health_check(&self, healthy: bool, latency_ms: u64) {
        self.network.connection_health_checks.fetch_add(1, Ordering::Relaxed);
        
        if !healthy {
            self.network.failed_health_checks.fetch_add(1, Ordering::Relaxed);
        }
        
        // Update latency statistics
        let current_avg = self.network.average_connection_latency_ms.load(Ordering::Relaxed);
        let total_checks = self.network.connection_health_checks.load(Ordering::Relaxed);
        
        if total_checks > 0 {
            let new_avg = ((current_avg * (total_checks - 1)) + latency_ms) / total_checks;
            self.network.average_connection_latency_ms.store(new_avg, Ordering::Relaxed);
        }
        
        // Update min/max latency
        let current_min = self.network.min_connection_latency_ms.load(Ordering::Relaxed);
        let current_max = self.network.max_connection_latency_ms.load(Ordering::Relaxed);
        
        if current_min == 0 || latency_ms < current_min {
            self.network.min_connection_latency_ms.store(latency_ms, Ordering::Relaxed);
        }
        
        if latency_ms > current_max {
            self.network.max_connection_latency_ms.store(latency_ms, Ordering::Relaxed);
        }
    }

    /// Update connection health counts
    pub fn update_connection_health_counts(&self, healthy_count: usize, unhealthy_count: usize) {
        self.network.healthy_connections.store(healthy_count, Ordering::Relaxed);
        self.network.unhealthy_connections.store(unhealthy_count, Ordering::Relaxed);
    }

    /// Record connection pool events
    pub fn record_connection_pool_event(&self, pool_size: usize, evicted: bool) {
        self.network.connection_pool_size.store(pool_size, Ordering::Relaxed);
        if evicted {
            self.network.connection_pool_evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record retry attempts
    pub fn record_retry_attempt(&self, success: bool) {
        self.network.retry_attempts.fetch_add(1, Ordering::Relaxed);
        if success {
            self.network.retry_successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.network.retry_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record network partition detection
    pub fn record_network_partition(&self) {
        self.network.network_partitions_detected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cluster operation
    pub fn record_cluster_operation(&self, operation: &str, success: bool, duration: Duration) {
        let duration_ms = duration.as_millis() as u64;
        
        self.cluster.cluster_operations.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.cluster.cluster_errors.fetch_add(1, Ordering::Relaxed);
        }
        
        metrics::counter!("cache_cluster_operations_total", 1, "operation" => operation.to_string());
        if !success {
            metrics::counter!("cache_cluster_errors_total", 1, "operation" => operation.to_string());
        }
        metrics::histogram!("cache_cluster_operation_duration_ms", duration_ms as f64);
    }

    /// Update cluster membership
    pub fn update_cluster_membership(&self, total: usize, healthy: usize, failed: usize) {
        self.cluster.total_nodes.store(total, Ordering::Relaxed);
        self.cluster.healthy_nodes.store(healthy, Ordering::Relaxed);
        self.cluster.failed_nodes.store(failed, Ordering::Relaxed);
        
        metrics::gauge!("cache_cluster_total_nodes", total as f64);
        metrics::gauge!("cache_cluster_healthy_nodes", healthy as f64);
        metrics::gauge!("cache_cluster_failed_nodes", failed as f64);
    }

    /// Record a persistence operation
    pub fn record_persistence_operation(&self, operation: &str, success: bool, bytes: Option<u64>) {
        if success {
            metrics::counter!("cache_persistence_operations_success_total", 1, "operation" => operation.to_string());
        } else {
            self.persistence.persistence_errors.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("cache_persistence_operations_error_total", 1, "operation" => operation.to_string());
        }
        
        if let Some(bytes) = bytes {
            match operation {
                "aof_write" => {
                    self.persistence.aof_size_bytes.fetch_add(bytes, Ordering::Relaxed);
                    metrics::gauge!("cache_aof_size_bytes", self.persistence.aof_size_bytes.load(Ordering::Relaxed) as f64);
                }
                "snapshot_write" => {
                    self.persistence.snapshot_size_bytes.store(bytes, Ordering::Relaxed);
                    metrics::gauge!("cache_snapshot_size_bytes", bytes as f64);
                }
                _ => {}
            }
        }
    }

    /// Record histogram data
    pub async fn record_histogram(&self, name: &str, value: u64) {
        let mut histograms = self.histograms.write().await;
        
        if !histograms.contains_key(name) {
            let buckets = vec![0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0];
            let counts: Vec<AtomicU64> = (0..buckets.len()).map(|_| AtomicU64::new(0)).collect();
            
            histograms.insert(name.to_string(), HistogramMetrics {
                name: name.to_string(),
                buckets,
                counts,
                sum: AtomicU64::new(0),
                count: AtomicU64::new(0),
            });
        }
        
        if let Some(histogram) = histograms.get(name) {
            histogram.sum.fetch_add(value, Ordering::Relaxed);
            histogram.count.fetch_add(1, Ordering::Relaxed);
            
            // Update bucket counts
            for (i, bucket) in histogram.buckets.iter().enumerate() {
                if value as f64 <= *bucket {
                    histogram.counts[i].fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
        }
    }

    /// Get cache hit ratio
    pub fn get_cache_hit_ratio(&self) -> f64 {
        let hits = self.cache.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get all metrics as a structured format
    pub fn get_metrics_summary(&self) -> MetricsSummary {
        MetricsSummary {
            server: ServerMetricsSummary {
                active_connections: self.server.active_connections.load(Ordering::Relaxed),
                total_connections: self.server.total_connections.load(Ordering::Relaxed),
                uptime_seconds: self.server.uptime_seconds.load(Ordering::Relaxed),
            },
            cache: CacheMetricsSummary {
                total_keys: self.cache.total_keys.load(Ordering::Relaxed),
                memory_usage_bytes: self.cache.memory_usage_bytes.load(Ordering::Relaxed),
                cache_hits: self.cache.cache_hits.load(Ordering::Relaxed),
                cache_misses: self.cache.cache_misses.load(Ordering::Relaxed),
                hit_ratio: self.get_cache_hit_ratio(),
                evictions: self.cache.evictions.load(Ordering::Relaxed),
                expired_keys: self.cache.expired_keys.load(Ordering::Relaxed),
            },
            cluster: ClusterMetricsSummary {
                total_nodes: self.cluster.total_nodes.load(Ordering::Relaxed),
                healthy_nodes: self.cluster.healthy_nodes.load(Ordering::Relaxed),
                failed_nodes: self.cluster.failed_nodes.load(Ordering::Relaxed),
                cluster_operations: self.cluster.cluster_operations.load(Ordering::Relaxed),
                cluster_errors: self.cluster.cluster_errors.load(Ordering::Relaxed),
            },
            network: NetworkMetricsSummary {
                bytes_sent: self.network.bytes_sent.load(Ordering::Relaxed),
                bytes_received: self.network.bytes_received.load(Ordering::Relaxed),
                connections_established: self.network.connections_established.load(Ordering::Relaxed),
                connections_closed: self.network.connections_closed.load(Ordering::Relaxed),
                network_errors: self.network.network_errors.load(Ordering::Relaxed),
                healthy_connections: self.network.healthy_connections.load(Ordering::Relaxed),
                unhealthy_connections: self.network.unhealthy_connections.load(Ordering::Relaxed),
                connection_health_checks: self.network.connection_health_checks.load(Ordering::Relaxed),
                failed_health_checks: self.network.failed_health_checks.load(Ordering::Relaxed),
                network_partitions_detected: self.network.network_partitions_detected.load(Ordering::Relaxed),
                connection_pool_size: self.network.connection_pool_size.load(Ordering::Relaxed),
                connection_pool_evictions: self.network.connection_pool_evictions.load(Ordering::Relaxed),
                retry_attempts: self.network.retry_attempts.load(Ordering::Relaxed),
                retry_successes: self.network.retry_successes.load(Ordering::Relaxed),
                retry_failures: self.network.retry_failures.load(Ordering::Relaxed),
                average_connection_latency_ms: self.network.average_connection_latency_ms.load(Ordering::Relaxed),
                max_connection_latency_ms: self.network.max_connection_latency_ms.load(Ordering::Relaxed),
                min_connection_latency_ms: self.network.min_connection_latency_ms.load(Ordering::Relaxed),
            },
        }
    }
}

/// Summary of all metrics
#[derive(Debug, Clone)]
pub struct MetricsSummary {
    pub server: ServerMetricsSummary,
    pub cache: CacheMetricsSummary,
    pub cluster: ClusterMetricsSummary,
    pub network: NetworkMetricsSummary,
}

#[derive(Debug, Clone)]
pub struct ServerMetricsSummary {
    pub active_connections: usize,
    pub total_connections: u64,
    pub uptime_seconds: u64,
}

#[derive(Debug, Clone)]
pub struct CacheMetricsSummary {
    pub total_keys: usize,
    pub memory_usage_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub hit_ratio: f64,
    pub evictions: u64,
    pub expired_keys: u64,
}

#[derive(Debug, Clone)]
pub struct ClusterMetricsSummary {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub failed_nodes: usize,
    pub cluster_operations: u64,
    pub cluster_errors: u64,
}

#[derive(Debug, Clone)]
pub struct NetworkMetricsSummary {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub connections_established: u64,
    pub connections_closed: u64,
    pub network_errors: u64,
    pub healthy_connections: usize,
    pub unhealthy_connections: usize,
    pub connection_health_checks: u64,
    pub failed_health_checks: u64,
    pub network_partitions_detected: u64,
    pub connection_pool_size: usize,
    pub connection_pool_evictions: u64,
    pub retry_attempts: u64,
    pub retry_successes: u64,
    pub retry_failures: u64,
    pub average_connection_latency_ms: u64,
    pub max_connection_latency_ms: u64,
    pub min_connection_latency_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MetricsConfig;

    #[test]
    fn test_metrics_collector_creation() {
        let config = MetricsConfig::default();
        let collector = MetricsCollector::new(config);
        assert_eq!(collector.cache.total_keys.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_cache_operation_recording() {
        let config = MetricsConfig::default();
        let collector = MetricsCollector::new(config);
        
        let duration = Duration::from_millis(10);
        collector.record_cache_operation("SET", duration, true);
        
        assert_eq!(collector.cache.set_operations.load(Ordering::Relaxed), 1);
        assert_eq!(collector.cache.set_latency_ms.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_cache_hit_ratio() {
        let config = MetricsConfig::default();
        let collector = MetricsCollector::new(config);
        
        // No hits or misses
        assert_eq!(collector.get_cache_hit_ratio(), 0.0);
        
        // 2 hits, 1 miss
        collector.record_cache_access(true);
        collector.record_cache_access(true);
        collector.record_cache_access(false);
        
        assert_eq!(collector.get_cache_hit_ratio(), 2.0 / 3.0);
    }

    #[tokio::test]
    async fn test_metrics_summary() {
        let config = MetricsConfig::default();
        let collector = MetricsCollector::new(config);
        
        collector.record_cache_operation("GET", Duration::from_millis(5), true);
        collector.record_cache_access(true);
        
        let summary = collector.get_metrics_summary();
        assert_eq!(summary.cache.cache_hits, 1);
        assert_eq!(summary.cache.hit_ratio, 1.0);
    }
} 