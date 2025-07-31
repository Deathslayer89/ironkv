//! Performance optimization module for the distributed key-value cache
//! 
//! This module provides performance optimization features including:
//! - Connection pooling
//! - Request batching
//! - Performance benchmarks
//! - Performance monitoring
//! - Optimization strategies

use crate::config::PerformanceConfig;
use crate::metrics::MetricsCollector;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};
use serde::{Deserialize, Serialize};

/// Connection pool for managing network connections
pub struct ConnectionPool {
    config: PerformanceConfig,
    connections: Arc<RwLock<HashMap<String, PooledConnection>>>,
    semaphore: Arc<Semaphore>,
    metrics: Arc<MetricsCollector>,
}

/// Pooled connection
#[derive(Debug, Clone)]
pub struct PooledConnection {
    /// Connection ID
    pub id: String,
    /// Remote address
    pub remote_addr: String,
    /// Connection creation time
    pub created_at: Instant,
    /// Last used time
    pub last_used: Instant,
    /// Connection health status
    pub healthy: bool,
    /// Connection latency in milliseconds
    pub latency_ms: u64,
    /// Number of requests handled
    pub request_count: u64,
}

/// Request batch for optimizing multiple operations
pub struct RequestBatch {
    config: PerformanceConfig,
    operations: Vec<BatchOperation>,
    created_at: Instant,
    max_wait_time: Duration,
}

/// Batch operation
#[derive(Debug, Clone)]
pub struct BatchOperation {
    /// Operation type
    pub operation_type: String,
    /// Operation key
    pub key: String,
    /// Operation value
    pub value: Option<Vec<u8>>,
    /// Operation timestamp
    pub timestamp: Instant,
    /// Operation priority
    pub priority: u8,
}

/// Batch result
#[derive(Debug, Clone)]
pub struct BatchResult {
    /// Batch ID
    pub batch_id: String,
    /// Number of operations
    pub operation_count: usize,
    /// Processing time in milliseconds
    pub processing_time_ms: u64,
    /// Success count
    pub success_count: usize,
    /// Failure count
    pub failure_count: usize,
    /// Results for each operation
    pub results: Vec<OperationResult>,
}

/// Operation result
#[derive(Debug, Clone)]
pub struct OperationResult {
    /// Operation index
    pub index: usize,
    /// Success status
    pub success: bool,
    /// Result value
    pub value: Option<Vec<u8>>,
    /// Error message
    pub error: Option<String>,
    /// Processing time in milliseconds
    pub processing_time_ms: u64,
}

/// Performance benchmark results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    /// Benchmark name
    pub name: String,
    /// Total operations
    pub total_operations: usize,
    /// Total time in milliseconds
    pub total_time_ms: u64,
    /// Operations per second
    pub ops_per_second: f64,
    /// Average latency in milliseconds
    pub avg_latency_ms: f64,
    /// 95th percentile latency
    pub p95_latency_ms: f64,
    /// 99th percentile latency
    pub p99_latency_ms: f64,
    /// Error rate
    pub error_rate: f64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// CPU usage percentage
    pub cpu_usage_percent: f64,
}

/// Performance monitor
pub struct PerformanceMonitor {
    config: PerformanceConfig,
    metrics: Arc<MetricsCollector>,
    benchmarks: Arc<RwLock<HashMap<String, BenchmarkResult>>>,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(config: PerformanceConfig, metrics: Arc<MetricsCollector>) -> Self {
        let max_connections = config.max_connections;
        Self {
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(max_connections)),
            metrics,
        }
    }

    /// Get a connection from the pool
    pub async fn get_connection(&self, remote_addr: &str) -> Result<PooledConnection, Box<dyn std::error::Error>> {
        let _permit = self.semaphore.acquire().await?;
        
        let mut connections = self.connections.write().await;
        
        // Check if we have an existing healthy connection
        if let Some(conn) = connections.get(remote_addr) {
            if conn.healthy && self.is_connection_fresh(conn) {
                // Update last used time
                let mut updated_conn = conn.clone();
                updated_conn.last_used = Instant::now();
                connections.insert(remote_addr.to_string(), updated_conn.clone());
                
                self.metrics.record_connection_pool_event(
                    connections.len(),
                    false,
                );
                
                return Ok(updated_conn);
            }
        }

        // Create new connection
        let new_conn = self.create_connection(remote_addr).await?;
        connections.insert(remote_addr.to_string(), new_conn.clone());
        
        self.metrics.record_connection_pool_event(
            connections.len(),
            false,
        );
        
        Ok(new_conn)
    }

    /// Return a connection to the pool
    pub async fn return_connection(&self, connection: PooledConnection) {
        let mut connections = self.connections.write().await;
        
        // Update connection stats
        let mut updated_conn = connection.clone();
        updated_conn.last_used = Instant::now();
        updated_conn.request_count += 1;
        
        connections.insert(connection.remote_addr.clone(), updated_conn);
        
        self.metrics.record_connection_pool_event(
            connections.len(),
            false,
        );
    }

    /// Mark connection as unhealthy
    pub async fn mark_connection_unhealthy(&self, remote_addr: &str) {
        let mut connections = self.connections.write().await;
        
        if let Some(conn) = connections.get_mut(remote_addr) {
            conn.healthy = false;
        }
        
        self.metrics.record_connection_pool_event(
            connections.len(),
            true,
        );
    }

    /// Clean up expired connections
    pub async fn cleanup_expired_connections(&self) {
        let mut connections = self.connections.write().await;
        let now = Instant::now();
        let max_idle_time = Duration::from_secs(self.config.connection_idle_timeout);
        
        connections.retain(|_, conn| {
            let idle_time = now.duration_since(conn.last_used);
            idle_time < max_idle_time
        });
        
        self.metrics.record_connection_pool_event(
            connections.len(),
            false,
        );
    }

    /// Get pool statistics
    pub async fn get_pool_stats(&self) -> PoolStats {
        let connections = self.connections.read().await;
        let total_connections = connections.len();
        let healthy_connections = connections.values().filter(|c| c.healthy).count();
        let unhealthy_connections = total_connections - healthy_connections;
        
        PoolStats {
            total_connections,
            healthy_connections,
            unhealthy_connections,
            available_permits: self.semaphore.available_permits(),
        }
    }

    /// Check if connection is fresh
    fn is_connection_fresh(&self, conn: &PooledConnection) -> bool {
        let now = Instant::now();
        let max_age = Duration::from_secs(self.config.connection_max_age);
        now.duration_since(conn.created_at) < max_age
    }

    /// Create a new connection
    async fn create_connection(&self, remote_addr: &str) -> Result<PooledConnection, Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        
        // Simulate connection creation
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        let latency = start_time.elapsed().as_millis() as u64;
        
        Ok(PooledConnection {
            id: format!("conn-{}", remote_addr),
            remote_addr: remote_addr.to_string(),
            created_at: Instant::now(),
            last_used: Instant::now(),
            healthy: true,
            latency_ms: latency,
            request_count: 0,
        })
    }
}

/// Connection pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_connections: usize,
    pub healthy_connections: usize,
    pub unhealthy_connections: usize,
    pub available_permits: usize,
}

impl RequestBatch {
    /// Create a new request batch
    pub fn new(config: PerformanceConfig) -> Self {
        Self {
            config: config.clone(),
            operations: Vec::new(),
            created_at: Instant::now(),
            max_wait_time: Duration::from_millis(config.batch_max_wait_time_ms),
        }
    }

    /// Add operation to batch
    pub fn add_operation(&mut self, operation: BatchOperation) -> bool {
        if self.operations.len() >= self.config.batch_max_size {
            return false;
        }
        
        self.operations.push(operation);
        true
    }

    /// Check if batch is ready to process
    pub fn is_ready(&self) -> bool {
        let elapsed = self.created_at.elapsed();
        self.operations.len() >= self.config.batch_min_size || elapsed >= self.max_wait_time
    }

    /// Get operations from batch
    pub fn take_operations(&mut self) -> Vec<BatchOperation> {
        std::mem::take(&mut self.operations)
    }

    /// Get batch size
    pub fn size(&self) -> usize {
        self.operations.len()
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

impl PerformanceMonitor {
    /// Create a new performance monitor
    pub fn new(config: PerformanceConfig, metrics: Arc<MetricsCollector>) -> Self {
        Self {
            config,
            metrics,
            benchmarks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Run a performance benchmark
    pub async fn run_benchmark<F, R>(
        &self,
        name: &str,
        operation_count: usize,
        operation: F,
    ) -> BenchmarkResult
    where
        F: Fn() -> R + Send + Sync + std::panic::RefUnwindSafe,
        R: Send,
    {
        let start_time = Instant::now();
        let mut latencies = Vec::with_capacity(operation_count);
        let mut error_count = 0;

        // Warm up
        for _ in 0..self.config.benchmark_warmup_operations {
            let _ = operation();
        }

        // Run benchmark
        for _ in 0..operation_count {
            let op_start = Instant::now();
            
            let result = std::panic::catch_unwind(|| operation());
            if result.is_err() {
                error_count += 1;
            }
            
            let latency = op_start.elapsed().as_millis() as u64;
            latencies.push(latency);
        }

        let total_time = start_time.elapsed().as_millis() as u64;
        let ops_per_second = (operation_count as f64) / (total_time as f64 / 1000.0);
        
        // Calculate percentiles
        latencies.sort();
        let avg_latency = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
        let p95_index = (latencies.len() as f64 * 0.95) as usize;
        let p99_index = (latencies.len() as f64 * 0.99) as usize;
        
        let p95_latency = if p95_index < latencies.len() {
            latencies[p95_index] as f64
        } else {
            avg_latency
        };
        
        let p99_latency = if p99_index < latencies.len() {
            latencies[p99_index] as f64
        } else {
            avg_latency
        };

        let error_rate = error_count as f64 / operation_count as f64;

        let result = BenchmarkResult {
            name: name.to_string(),
            total_operations: operation_count,
            total_time_ms: total_time,
            ops_per_second,
            avg_latency_ms: avg_latency,
            p95_latency_ms: p95_latency,
            p99_latency_ms: p99_latency,
            error_rate,
            memory_usage_bytes: 0, // Would need system metrics
            cpu_usage_percent: 0.0, // Would need system metrics
        };

        // Store benchmark result
        {
            let mut benchmarks = self.benchmarks.write().await;
            benchmarks.insert(name.to_string(), result.clone());
        }

        result
    }

    /// Get benchmark results
    pub async fn get_benchmark_results(&self) -> HashMap<String, BenchmarkResult> {
        self.benchmarks.read().await.clone()
    }

    /// Get specific benchmark result
    pub async fn get_benchmark_result(&self, name: &str) -> Option<BenchmarkResult> {
        self.benchmarks.read().await.get(name).cloned()
    }

    /// Run comprehensive performance tests
    pub async fn run_comprehensive_benchmarks(&self) -> HashMap<String, BenchmarkResult> {
        let mut results = HashMap::new();

        // Test 1: Simple get operations
        let get_result = self.run_benchmark("get_operations", 10000, || {
            // Simulate get operation
            std::thread::sleep(Duration::from_micros(100));
        }).await;
        results.insert("get_operations".to_string(), get_result);

        // Test 2: Simple set operations
        let set_result = self.run_benchmark("set_operations", 10000, || {
            // Simulate set operation
            std::thread::sleep(Duration::from_micros(150));
        }).await;
        results.insert("set_operations".to_string(), set_result);

        // Test 3: Mixed operations
        let mixed_result = self.run_benchmark("mixed_operations", 10000, || {
            // Simulate mixed operations
            std::thread::sleep(Duration::from_micros(125));
        }).await;
        results.insert("mixed_operations".to_string(), mixed_result);

        // Test 4: High concurrency
        let concurrency_result = self.run_benchmark("high_concurrency", 1000, || {
            // Simulate high concurrency operation
            std::thread::sleep(Duration::from_millis(1));
        }).await;
        results.insert("high_concurrency".to_string(), concurrency_result);

        results
    }

    /// Generate performance report
    pub async fn generate_performance_report(&self) -> PerformanceReport {
        let benchmarks = self.run_comprehensive_benchmarks().await;
        let metrics = self.metrics.get_metrics_summary();
        
        PerformanceReport {
            timestamp: chrono::Utc::now(),
            benchmarks: benchmarks.clone(),
            metrics_summary: metrics,
            recommendations: self.generate_recommendations(&benchmarks),
        }
    }

    /// Generate performance recommendations
    fn generate_recommendations(&self, benchmarks: &HashMap<String, BenchmarkResult>) -> Vec<String> {
        let mut recommendations = Vec::new();

        for (name, result) in benchmarks {
            if result.avg_latency_ms > 10.0 {
                recommendations.push(format!("High latency detected in {}: {:.2}ms average", name, result.avg_latency_ms));
            }
            
            if result.error_rate > 0.01 {
                recommendations.push(format!("High error rate in {}: {:.2}%", name, result.error_rate * 100.0));
            }
            
            if result.ops_per_second < 1000.0 {
                recommendations.push(format!("Low throughput in {}: {:.0} ops/sec", name, result.ops_per_second));
            }
        }

        if recommendations.is_empty() {
            recommendations.push("Performance is within acceptable ranges".to_string());
        }

        recommendations
    }
}

/// Performance report
#[derive(Debug, Clone)]
pub struct PerformanceReport {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub benchmarks: HashMap<String, BenchmarkResult>,
    pub metrics_summary: crate::metrics::MetricsSummary,
    pub recommendations: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::MetricsCollector;
    use crate::config::MetricsConfig;

    #[tokio::test]
    async fn test_connection_pool_creation() {
        let config = PerformanceConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        let pool = ConnectionPool::new(config, metrics);
        
        let stats = pool.get_pool_stats().await;
        assert_eq!(stats.total_connections, 0);
    }

    #[tokio::test]
    async fn test_request_batch() {
        let config = PerformanceConfig::default();
        let mut batch = RequestBatch::new(config);
        
        assert!(batch.is_empty());
        
        let operation = BatchOperation {
            operation_type: "GET".to_string(),
            key: "test-key".to_string(),
            value: None,
            timestamp: Instant::now(),
            priority: 1,
        };
        
        assert!(batch.add_operation(operation));
        assert_eq!(batch.size(), 1);
        assert!(!batch.is_empty());
    }

    #[tokio::test]
    async fn test_performance_monitor() {
        let config = PerformanceConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        let monitor = PerformanceMonitor::new(config, metrics);
        
        let result = monitor.run_benchmark("test", 100, || {
            std::thread::sleep(Duration::from_micros(100));
        }).await;
        
        assert_eq!(result.name, "test");
        assert_eq!(result.total_operations, 100);
        assert!(result.ops_per_second > 0.0);
    }

    #[tokio::test]
    async fn test_comprehensive_benchmarks() {
        let config = PerformanceConfig::default();
        let metrics = Arc::new(MetricsCollector::new(MetricsConfig::default()));
        let monitor = PerformanceMonitor::new(config, metrics);
        
        let results = monitor.run_comprehensive_benchmarks().await;
        assert!(!results.is_empty());
        assert!(results.contains_key("get_operations"));
        assert!(results.contains_key("set_operations"));
    }
} 