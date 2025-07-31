use kv_cache_core::{
    config_manager::{ConfigManager, SecretsManager},
};
use std::env;
use tempfile::NamedTempFile;
use std::fs;

/// Test helper to create a minimal test config file
fn create_minimal_config() -> NamedTempFile {
    let temp_file = NamedTempFile::new().unwrap();
    let config_content = r#"
server:
  port: 6379
  bind_address: "127.0.0.1"
  max_connections: 100
  connection_timeout: 30
  request_timeout: 60
  graceful_shutdown: true
  shutdown_timeout: 30
storage:
  max_memory_bytes: null
  max_keys: null
  eviction_policy: "NoEviction"
  default_ttl_seconds: 0
  enable_ttl_cleanup: true
  ttl_cleanup_interval: 60
  ttl_cleanup_sample_size: 100
cluster:
  node_id: "test-node"
  bind_address: "127.0.0.1"
  port: 6380
  heartbeat_interval: 5
  failure_timeout: 15
  replication_factor: 3
  virtual_nodes_per_physical: 150
  members: {}
  replication_strategy: "Synchronous"
  enabled: true
persistence:
  enabled: false
  data_dir: "./data"
  aof_path: "./data/appendonly.aof"
  snapshot_path: "./data/dump.rdb"
  enable_aof: false
  aof_sync_policy: "Shutdown"
  snapshot_interval: 3600
  max_aof_size: 1073741824
  enable_aof_rewrite: false
logging:
  level: "Info"
  format: "Json"
  file_path: null
  console: true
  structured: true
  rotation:
    enabled: false
    max_size: 10485760
    max_files: 5
    interval: "Daily"
metrics:
  enabled: true
  bind_address: "127.0.0.1"
  port: 9090
  path: "/metrics"
  prometheus: true
  collection_interval: 15
  histograms: true
  labels: {}
tracing:
  enabled: false
  service_name: "kv-cache"
  service_version: "0.1.0"
  sampling_rate: 0.1
  opentelemetry: false
  otel_endpoint: null
  local_spans: true
  max_span_duration: 30
health:
  enabled: true
  check_interval: 30
  timeout: 5
  enable_endpoints: true
  endpoint_path: "/health"
  detailed: true
  failure_threshold: 3
  success_threshold: 1
security:
  enable_auth: false
  enable_authz: false
  enable_tls: false
  tls_cert_path: null
  tls_key_path: null
  enable_audit_log: true
  max_audit_log_entries: 1000
  session_timeout: 3600
  create_default_admin: false
  default_admin_password: ""
  password_policy:
    min_length: 8
    require_uppercase: false
    require_lowercase: false
    require_numbers: false
    require_special: false
    max_age_days: null
  rate_limiting:
    enabled: false
    max_requests_per_minute: 100
    max_failed_logins: 5
    lockout_duration_minutes: 30
performance:
  max_connections: 100
  enable_batching: true
  connection_idle_timeout: 300
  connection_max_age: 3600
  batch_min_size: 10
  batch_max_size: 1000
  batch_max_wait_time_ms: 50
  enable_monitoring: true
  benchmark_warmup_operations: 1000
  monitoring_interval: 60
"#;
    fs::write(&temp_file, config_content).unwrap();
    temp_file
}

#[tokio::test]
async fn test_minimal_config_loading() {
    let mut config_manager = ConfigManager::new();
    let temp_file = create_minimal_config();
    
    // Load configuration from file
    config_manager.load_from_file(temp_file.path().to_str().unwrap()).await.unwrap();
    
    let config = config_manager.get_config().await;
    
    // Verify basic configuration was loaded
    assert_eq!(config.server.port, 6379);
    assert_eq!(config.server.bind_address, "127.0.0.1");
    assert_eq!(config.cluster.node_id, "test-node");
    assert_eq!(config.security.enable_auth, false);
}

#[tokio::test]
async fn test_minimal_secrets_manager() {
    let mut secrets_manager = SecretsManager::new();
    
    // Test basic secret operations
    secrets_manager.store_secret("test_key", "test_value").await.unwrap();
    let value = secrets_manager.get_secret("test_key").await;
    assert_eq!(value, Some("test_value".to_string()));
}

#[tokio::test]
async fn test_minimal_environment_config() {
    let mut config_manager = ConfigManager::new();
    
    // Set a simple environment variable
    env::set_var("IRONKV_SERVER_PORT", "8080");
    
    // Load configuration from environment
    config_manager.load_from_env().await.unwrap();
    
    let config = config_manager.get_config().await;
    assert_eq!(config.server.port, 8080);
    
    // Clean up
    env::remove_var("IRONKV_SERVER_PORT");
} 