//! Configuration management for the distributed key-value cache
//! 
//! This module provides structured configuration management using TOML/YAML
//! files with serde for serialization and deserialization.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

/// Main configuration structure for the cache system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Server configuration
    pub server: ServerConfig,
    /// Storage configuration
    pub storage: StorageConfig,
    /// Cluster configuration
    pub cluster: ClusterConfig,
    /// Persistence configuration
    pub persistence: PersistenceConfig,
    /// Logging configuration
    pub logging: LoggingConfig,
    /// Metrics configuration
    pub metrics: MetricsConfig,
    /// Tracing configuration
    pub tracing: TracingConfig,
    /// Health check configuration
    pub health: HealthConfig,
    /// Security configuration
    pub security: SecurityConfig,
    /// Performance configuration
    pub performance: PerformanceConfig,
}

/// Server-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Server bind address
    pub bind_address: String,
    /// Server port
    pub port: u16,
    /// Maximum number of concurrent connections
    pub max_connections: usize,
    /// Connection timeout in seconds
    pub connection_timeout: u64,
    /// Request timeout in seconds
    pub request_timeout: u64,
    /// Enable graceful shutdown
    pub graceful_shutdown: bool,
    /// Graceful shutdown timeout in seconds
    pub shutdown_timeout: u64,
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Maximum memory usage in bytes
    pub max_memory_bytes: Option<u64>,
    /// Maximum number of keys
    pub max_keys: Option<usize>,
    /// Eviction policy
    pub eviction_policy: EvictionPolicy,
    /// Default TTL in seconds (0 = no TTL)
    pub default_ttl_seconds: u64,
    /// Enable TTL cleanup
    pub enable_ttl_cleanup: bool,
    /// TTL cleanup interval in seconds
    pub ttl_cleanup_interval: u64,
    /// TTL cleanup sample size
    pub ttl_cleanup_sample_size: usize,
}

/// Eviction policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvictionPolicy {
    /// No eviction (return error when full)
    NoEviction,
    /// Least Recently Used
    LRU,
    /// Least Frequently Used
    LFU,
    /// Random eviction
    Random,
}

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Node ID
    pub node_id: String,
    /// Cluster bind address
    pub bind_address: String,
    /// Cluster port
    pub port: u16,
    /// Heartbeat interval in seconds
    pub heartbeat_interval: u64,
    /// Failure timeout in seconds
    pub failure_timeout: u64,
    /// Replication factor
    pub replication_factor: usize,
    /// Virtual nodes per physical node
    pub virtual_nodes_per_physical: usize,
    /// Cluster members (node_id -> address:port)
    pub members: HashMap<String, String>,
    /// Replication strategy
    pub replication_strategy: ReplicationStrategy,
    /// Enable cluster mode
    pub enabled: bool,
}

/// Replication strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationStrategy {
    /// Synchronous replication
    Synchronous,
    /// Asynchronous replication
    Asynchronous,
    /// Quorum-based replication
    Quorum(usize),
}

/// Persistence configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    /// Enable persistence
    pub enabled: bool,
    /// Data directory path
    pub data_dir: String,
    /// AOF file path
    pub aof_path: String,
    /// Snapshot file path
    pub snapshot_path: String,
    /// Enable AOF
    pub enable_aof: bool,
    /// AOF sync policy
    pub aof_sync_policy: AofSyncPolicy,
    /// Snapshot interval in seconds
    pub snapshot_interval: u64,
    /// Maximum AOF file size in bytes
    pub max_aof_size: u64,
    /// Enable AOF rewriting
    pub enable_aof_rewrite: bool,
}

/// AOF sync policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AofSyncPolicy {
    /// Sync on every write
    Always,
    /// Sync every N seconds
    Every(u64),
    /// Sync on shutdown only
    Shutdown,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level
    pub level: LogLevel,
    /// Log format
    pub format: LogFormat,
    /// Log file path (optional)
    pub file_path: Option<String>,
    /// Enable console output
    pub console: bool,
    /// Enable structured logging
    pub structured: bool,
    /// Log rotation settings
    pub rotation: LogRotation,
}

/// Log level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

/// Log format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogFormat {
    /// Simple text format
    Simple,
    /// JSON format
    Json,
    /// Pretty format with colors
    Pretty,
}

/// Log rotation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRotation {
    /// Enable log rotation
    pub enabled: bool,
    /// Maximum file size in bytes
    pub max_size: u64,
    /// Maximum number of files to keep
    pub max_files: usize,
    /// Rotation interval
    pub interval: LogRotationInterval,
}

/// Log rotation interval
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogRotationInterval {
    /// Daily rotation
    Daily,
    /// Hourly rotation
    Hourly,
    /// Weekly rotation
    Weekly,
    /// Never (size-based only)
    Never,
}

/// Metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable metrics
    pub enabled: bool,
    /// Metrics bind address
    pub bind_address: String,
    /// Metrics port
    pub port: u16,
    /// Metrics path
    pub path: String,
    /// Enable Prometheus exporter
    pub prometheus: bool,
    /// Metrics collection interval in seconds
    pub collection_interval: u64,
    /// Enable histogram metrics
    pub histograms: bool,
    /// Custom labels
    pub labels: HashMap<String, String>,
}

/// Tracing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
    /// Enable tracing
    pub enabled: bool,
    /// Service name
    pub service_name: String,
    /// Service version
    pub service_version: String,
    /// Sampling rate (0.0 to 1.0)
    pub sampling_rate: f64,
    /// Enable OpenTelemetry export
    pub opentelemetry: bool,
    /// OpenTelemetry endpoint
    pub otel_endpoint: Option<String>,
    /// Enable local spans
    pub local_spans: bool,
    /// Maximum span duration in seconds
    pub max_span_duration: u64,
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    /// Enable health checks
    pub enabled: bool,
    /// Health check interval in seconds
    pub check_interval: u64,
    /// Health check timeout in seconds
    pub timeout: u64,
    /// Enable health check endpoints
    pub enable_endpoints: bool,
    /// Health check endpoint path
    pub endpoint_path: String,
    /// Enable detailed health checks
    pub detailed: bool,
    /// Health check failure threshold
    pub failure_threshold: usize,
    /// Health check success threshold
    pub success_threshold: usize,
}

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Enable authentication
    pub enable_auth: bool,
    /// Enable authorization
    pub enable_authz: bool,
    /// Enable TLS/SSL
    pub enable_tls: bool,
    /// TLS certificate file path
    pub tls_cert_path: Option<String>,
    /// TLS private key file path
    pub tls_key_path: Option<String>,
    /// Enable audit logging
    pub enable_audit_log: bool,
    /// Maximum audit log entries
    pub max_audit_log_entries: usize,
    /// Session timeout in seconds
    pub session_timeout: u64,
    /// Create default admin user
    pub create_default_admin: bool,
    /// Default admin password
    pub default_admin_password: String,
    /// Password policy
    pub password_policy: PasswordPolicy,
    /// Rate limiting
    pub rate_limiting: RateLimitingConfig,
}

/// Password policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordPolicy {
    /// Minimum password length
    pub min_length: usize,
    /// Require uppercase letters
    pub require_uppercase: bool,
    /// Require lowercase letters
    pub require_lowercase: bool,
    /// Require numbers
    pub require_numbers: bool,
    /// Require special characters
    pub require_special: bool,
    /// Maximum password age in days
    pub max_age_days: Option<u32>,
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitingConfig {
    /// Enable rate limiting
    pub enabled: bool,
    /// Maximum requests per minute
    pub max_requests_per_minute: usize,
    /// Maximum failed login attempts
    pub max_failed_logins: usize,
    /// Account lockout duration in minutes
    pub lockout_duration_minutes: u64,
}

/// Performance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Maximum number of connections in pool
    pub max_connections: usize,
    /// Connection idle timeout in seconds
    pub connection_idle_timeout: u64,
    /// Connection maximum age in seconds
    pub connection_max_age: u64,
    /// Enable request batching
    pub enable_batching: bool,
    /// Batch minimum size
    pub batch_min_size: usize,
    /// Batch maximum size
    pub batch_max_size: usize,
    /// Batch maximum wait time in milliseconds
    pub batch_max_wait_time_ms: u64,
    /// Enable performance monitoring
    pub enable_monitoring: bool,
    /// Benchmark warmup operations
    pub benchmark_warmup_operations: usize,
    /// Performance monitoring interval in seconds
    pub monitoring_interval: u64,
}

impl CacheConfig {
    /// Load configuration from a file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: CacheConfig = toml::from_str(&content)?;
        Ok(config)
    }

    /// Load configuration from a YAML file
    pub fn from_yaml_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: CacheConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// Save configuration to a file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Save configuration to a YAML file
    pub fn save_to_yaml_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let content = serde_yaml::to_string(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // Validate server config
        if self.server.port == 0 {
            errors.push("Server port cannot be 0".to_string());
        }
        if self.server.max_connections == 0 {
            errors.push("Max connections cannot be 0".to_string());
        }

        // Validate storage config
        if let Some(max_memory) = self.storage.max_memory_bytes {
            if max_memory == 0 {
                errors.push("Max memory cannot be 0".to_string());
            }
        }
        if let Some(max_keys) = self.storage.max_keys {
            if max_keys == 0 {
                errors.push("Max keys cannot be 0".to_string());
            }
        }

        // Validate cluster config
        if self.cluster.enabled {
            if self.cluster.node_id.is_empty() {
                errors.push("Node ID cannot be empty".to_string());
            }
            if self.cluster.port == 0 {
                errors.push("Cluster port cannot be 0".to_string());
            }
            if self.cluster.replication_factor == 0 {
                errors.push("Replication factor cannot be 0".to_string());
            }
        }

        // Validate persistence config
        if self.persistence.enabled {
            if self.persistence.data_dir.is_empty() {
                errors.push("Data directory cannot be empty".to_string());
            }
        }

        // Validate metrics config
        if self.metrics.enabled {
            if self.metrics.port == 0 {
                errors.push("Metrics port cannot be 0".to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Get duration for heartbeat interval
    pub fn heartbeat_duration(&self) -> Duration {
        Duration::from_secs(self.cluster.heartbeat_interval)
    }

    /// Get duration for failure timeout
    pub fn failure_duration(&self) -> Duration {
        Duration::from_secs(self.cluster.failure_timeout)
    }

    /// Get duration for TTL cleanup interval
    pub fn ttl_cleanup_duration(&self) -> Duration {
        Duration::from_secs(self.storage.ttl_cleanup_interval)
    }

    /// Get duration for snapshot interval
    pub fn snapshot_duration(&self) -> Duration {
        Duration::from_secs(self.persistence.snapshot_interval)
    }

    /// Get duration for metrics collection interval
    pub fn metrics_collection_duration(&self) -> Duration {
        Duration::from_secs(self.metrics.collection_interval)
    }

    /// Get duration for graceful shutdown timeout
    pub fn shutdown_duration(&self) -> Duration {
        Duration::from_secs(self.server.shutdown_timeout)
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            cluster: ClusterConfig::default(),
            persistence: PersistenceConfig::default(),
            logging: LoggingConfig::default(),
            metrics: MetricsConfig::default(),
            tracing: TracingConfig::default(),
            health: HealthConfig::default(),
            security: SecurityConfig::default(),
            performance: PerformanceConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1".to_string(),
            port: 6379,
            max_connections: 1000,
            connection_timeout: 30,
            request_timeout: 60,
            graceful_shutdown: true,
            shutdown_timeout: 30,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: Some(1024 * 1024 * 1024), // 1GB
            max_keys: Some(1_000_000),
            eviction_policy: EvictionPolicy::LRU,
            default_ttl_seconds: 0,
            enable_ttl_cleanup: true,
            ttl_cleanup_interval: 60,
            ttl_cleanup_sample_size: 1000,
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: "node-1".to_string(),
            bind_address: "127.0.0.1".to_string(),
            port: 50051,
            heartbeat_interval: 5,
            failure_timeout: 15,
            replication_factor: 2,
            virtual_nodes_per_physical: 150,
            members: HashMap::new(),
            replication_strategy: ReplicationStrategy::Asynchronous,
            enabled: false,
        }
    }
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            data_dir: "./data".to_string(),
            aof_path: "./data/appendonly.aof".to_string(),
            snapshot_path: "./data/snapshot.rdb".to_string(),
            enable_aof: true,
            aof_sync_policy: AofSyncPolicy::Every(1),
            snapshot_interval: 3600, // 1 hour
            max_aof_size: 100 * 1024 * 1024, // 100MB
            enable_aof_rewrite: true,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            format: LogFormat::Simple,
            file_path: None,
            console: true,
            structured: true,
            rotation: LogRotation::default(),
        }
    }
}

impl Default for LogRotation {
    fn default() -> Self {
        Self {
            enabled: true,
            max_size: 100 * 1024 * 1024, // 100MB
            max_files: 5,
            interval: LogRotationInterval::Daily,
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_address: "127.0.0.1".to_string(),
            port: 9090,
            path: "/metrics".to_string(),
            prometheus: true,
            collection_interval: 15,
            histograms: true,
            labels: HashMap::new(),
        }
    }
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            service_name: "kv-cache".to_string(),
            service_version: "0.1.0".to_string(),
            sampling_rate: 0.1,
            opentelemetry: false,
            otel_endpoint: None,
            local_spans: true,
            max_span_duration: 60,
        }
    }
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval: 30,
            timeout: 5,
            enable_endpoints: true,
            endpoint_path: "/health".to_string(),
            detailed: true,
            failure_threshold: 3,
            success_threshold: 2,
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            enable_auth: false,
            enable_authz: false,
            enable_tls: false,
            tls_cert_path: None,
            tls_key_path: None,
            enable_audit_log: true,
            max_audit_log_entries: 10000,
            session_timeout: 3600, // 1 hour
            create_default_admin: true,
            default_admin_password: "admin123".to_string(),
            password_policy: PasswordPolicy::default(),
            rate_limiting: RateLimitingConfig::default(),
        }
    }
}

impl Default for PasswordPolicy {
    fn default() -> Self {
        Self {
            min_length: 8,
            require_uppercase: true,
            require_lowercase: true,
            require_numbers: true,
            require_special: false,
            max_age_days: Some(90),
        }
    }
}

impl Default for RateLimitingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_requests_per_minute: 1000,
            max_failed_logins: 5,
            lockout_duration_minutes: 15,
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            max_connections: 100,
            connection_idle_timeout: 300, // 5 minutes
            connection_max_age: 3600, // 1 hour
            enable_batching: true,
            batch_min_size: 10,
            batch_max_size: 1000,
            batch_max_wait_time_ms: 50, // 50ms
            enable_monitoring: true,
            benchmark_warmup_operations: 1000,
            monitoring_interval: 60, // 1 minute
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CacheConfig::default();
        assert_eq!(config.server.port, 6379);
        assert_eq!(config.cluster.node_id, "node-1");
        assert!(!config.cluster.enabled);
    }

    #[test]
    fn test_config_validation() {
        let mut config = CacheConfig::default();
        assert!(config.validate().is_ok());

        // Test invalid config
        config.server.port = 0;
        let errors = config.validate().unwrap_err();
        assert!(errors.contains(&"Server port cannot be 0".to_string()));
    }

    #[test]
    fn test_config_serialization() {
        let config = CacheConfig::default();
        let toml_str = toml::to_string(&config).unwrap();
        let deserialized: CacheConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(config.server.port, deserialized.server.port);
    }

    #[test]
    fn test_duration_conversions() {
        let config = CacheConfig::default();
        assert_eq!(config.heartbeat_duration(), Duration::from_secs(5));
        assert_eq!(config.failure_duration(), Duration::from_secs(15));
    }
} 