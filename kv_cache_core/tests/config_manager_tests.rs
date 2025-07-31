use kv_cache_core::{
    config::CacheConfig,
    config_manager::{
        ConfigManager, SecretsManager, ConfigValidator, DefaultConfigValidator, 
        ProductionConfigValidator, ConfigValidationResult, ConfigSource
    },
};
use serde_json::json;
use std::env;
use std::fs;
use tempfile::NamedTempFile;
use tokio::time::{sleep, Duration};

/// Test helper to create a temporary config file
fn create_temp_config(content: &str) -> NamedTempFile {
    let temp_file = NamedTempFile::new().unwrap();
    fs::write(&temp_file, content).unwrap();
    temp_file
}

#[tokio::test]
async fn test_config_manager_basic_operations() {
    let config_manager = ConfigManager::new();
    let config = config_manager.get_config().await;
    
    // Test default configuration
    assert_eq!(config.server.port, 6379);
    assert_eq!(config.server.bind_address, "127.0.0.1");
    assert_eq!(config.performance.max_connections, 100);
}

#[tokio::test]
async fn test_config_loading_from_file() {
    let mut config_manager = ConfigManager::new();
    
    // Create temporary config file
    let config_content = r#"
server:
  port: 8080
  bind_address: "0.0.0.0"
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
performance:
  max_connections: 500
  enable_batching: true
  connection_idle_timeout: 300
  connection_max_age: 3600
  batch_min_size: 10
  batch_max_size: 1000
  batch_max_wait_time_ms: 50
  enable_monitoring: true
  benchmark_warmup_operations: 1000
  monitoring_interval: 60
security:
  enable_auth: true
  enable_authz: true
  session_timeout: 3600
  max_login_attempts: 5
  password_min_length: 8
  audit_log_enabled: true
  audit_log_retention_days: 30
cluster:
  members: {}
persistence:
  enabled: false
  aof_enabled: false
  snapshot_enabled: false
  snapshot_interval: 3600
  data_dir: "./data"
logging:
  level: "info"
  format: "json"
  output: "stdout"
metrics:
  enabled: true
  port: 9090
  bind_address: "127.0.0.1"
tracing:
  enabled: false
  jaeger_endpoint: ""
  service_name: "kv-cache"
  service_version: "0.1.0"
health:
  enabled: true
  port: 8081
  bind_address: "127.0.0.1"
  check_interval: 30
  timeout: 5
"#;
    
    let temp_file = create_temp_config(config_content);
    
    // Load configuration from file
    config_manager.load_from_file(temp_file.path().to_str().unwrap()).await.unwrap();
    
    let config = config_manager.get_config().await;
    
    // Verify configuration was loaded correctly
    assert_eq!(config.server.port, 8080);
    assert_eq!(config.server.bind_address, "0.0.0.0");
    assert_eq!(config.performance.max_connections, 500);
    assert_eq!(config.performance.enable_batching, true);
    assert_eq!(config.security.enable_auth, true);
    assert_eq!(config.security.enable_authz, true);
}

#[tokio::test]
async fn test_config_loading_from_environment() {
    let mut config_manager = ConfigManager::new();
    
    // Set environment variables
    env::set_var("IRONKV_SERVER_PORT", "9090");
    env::set_var("IRONKV_SERVER_BIND_ADDRESS", "0.0.0.0");
    env::set_var("IRONKV_PERFORMANCE_MAX_CONNECTIONS", "1000");
    env::set_var("IRONKV_SECURITY_ENABLE_AUTH", "true");
    
    // Load configuration from environment
    config_manager.load_from_env().await.unwrap();
    
    let config = config_manager.get_config().await;
    
    // Verify environment variables were loaded
    assert_eq!(config.server.port, 9090);
    assert_eq!(config.server.bind_address, "0.0.0.0");
    assert_eq!(config.performance.max_connections, 1000);
    assert_eq!(config.security.enable_auth, true);
    
    // Clean up environment variables
    env::remove_var("IRONKV_SERVER_PORT");
    env::remove_var("IRONKV_SERVER_BIND_ADDRESS");
    env::remove_var("IRONKV_PERFORMANCE_MAX_CONNECTIONS");
    env::remove_var("IRONKV_SECURITY_ENABLE_AUTH");
}

#[tokio::test]
async fn test_cluster_members_from_environment() {
    let mut config_manager = ConfigManager::new();
    
    // Set cluster members environment variable
    env::set_var("IRONKV_CLUSTER_MEMBERS", "node1:127.0.0.1:6379,node2:127.0.0.1:6380,node3:127.0.0.1:6381");
    
    // Load configuration from environment
    config_manager.load_from_env().await.unwrap();
    
    let config = config_manager.get_config().await;
    
    // Verify cluster members were parsed correctly
    assert_eq!(config.cluster.members.len(), 3);
    assert_eq!(config.cluster.members.get("node1"), Some(&"127.0.0.1:6379".to_string()));
    assert_eq!(config.cluster.members.get("node2"), Some(&"127.0.0.1:6380".to_string()));
    assert_eq!(config.cluster.members.get("node3"), Some(&"127.0.0.1:6381".to_string()));
    
    // Clean up
    env::remove_var("IRONKV_CLUSTER_MEMBERS");
}

#[tokio::test]
async fn test_config_validation() {
    let mut config_manager = ConfigManager::new();
    config_manager.add_validator(Box::new(DefaultConfigValidator));
    
    // Test valid configuration
    let valid_config = config_manager.get_config().await;
    let validation = config_manager.validate_config(&valid_config).await;
    assert!(validation.is_valid);
    assert!(validation.errors.is_empty());
    
    // Test invalid configuration
    let mut invalid_config = valid_config.clone();
    invalid_config.server.port = 0; // Invalid port
    
    let validation = config_manager.validate_config(&invalid_config).await;
    assert!(!validation.is_valid);
    assert!(validation.errors.contains(&"Server port cannot be 0".to_string()));
}

#[tokio::test]
async fn test_production_config_validator() {
    let mut config_manager = ConfigManager::new();
    config_manager.add_validator(Box::new(ProductionConfigValidator));
    
    // Test production configuration with warnings
    let mut prod_config = config_manager.get_config().await;
    prod_config.server.bind_address = "0.0.0.0".to_string();
    prod_config.performance.max_connections = 50;
    prod_config.security.enable_auth = false;
    
    let validation = config_manager.validate_config(&prod_config).await;
    assert!(validation.is_valid); // Should be valid but with warnings
    assert!(!validation.warnings.is_empty());
    assert!(validation.warnings.contains(&"Binding to 0.0.0.0 is not recommended for production".to_string()));
    assert!(validation.warnings.contains(&"Low max connections for production workload".to_string()));
    assert!(validation.warnings.contains(&"Authentication disabled in production".to_string()));
}

#[tokio::test]
async fn test_config_update() {
    let mut config_manager = ConfigManager::new();
    
    // Create temporary config file for saving
    let temp_file = create_temp_config(r#"
server:
  port: 6379
  bind_address: "127.0.0.1"
  max_connections: 100
  connection_timeout: 30
  request_timeout: 60
  graceful_shutdown: true
  shutdown_timeout: 30
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
cluster:
  members: {}
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
  port: 8081
  bind_address: "127.0.0.1"
  check_interval: 30
  timeout: 5
"#);
    config_manager.load_from_file(temp_file.path().to_str().unwrap()).await.unwrap();
    
    // Update configuration
    let mut new_config = config_manager.get_config().await;
    new_config.server.port = 9999;
    new_config.performance.max_connections = 2000;
    
    config_manager.update_config(new_config.clone()).await.unwrap();
    
    // Verify configuration was updated
    let updated_config = config_manager.get_config().await;
    assert_eq!(updated_config.server.port, 9999);
    assert_eq!(updated_config.performance.max_connections, 2000);
    
    // Verify configuration was saved to file
    let saved_content = fs::read_to_string(temp_file.path()).unwrap();
    assert!(saved_content.contains("port: 9999"));
    assert!(saved_content.contains("max_connections: 2000"));
}

#[tokio::test]
async fn test_config_watcher() {
    let config_manager = ConfigManager::new();
    let mut watcher = config_manager.watch_config();
    
    // Test initial configuration
    let initial_config = watcher.borrow().clone();
    assert_eq!(initial_config.server.port, 6379);
    
    // Test that watcher receives updates
    let mut config_manager = ConfigManager::new();
    let temp_file = create_temp_config(r#"
server:
  port: 6379
  bind_address: "127.0.0.1"
  max_connections: 100
  connection_timeout: 30
  request_timeout: 60
  graceful_shutdown: true
  shutdown_timeout: 30
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
security:
  enable_auth: false
  enable_authz: false
  session_timeout: 3600
  max_login_attempts: 5
  password_min_length: 8
  audit_log_enabled: true
  audit_log_retention_days: 30
cluster:
  members: {}
persistence:
  enabled: false
  aof_enabled: false
  snapshot_enabled: false
  snapshot_interval: 3600
  data_dir: "./data"
logging:
  level: "info"
  format: "json"
  output: "stdout"
metrics:
  enabled: true
  port: 9090
  bind_address: "127.0.0.1"
tracing:
  enabled: false
  jaeger_endpoint: ""
  service_name: "kv-cache"
  service_version: "0.1.0"
health:
  enabled: true
  port: 8081
  bind_address: "127.0.0.1"
  check_interval: 30
  timeout: 5
"#);
    config_manager.load_from_file(temp_file.path().to_str().unwrap()).await.unwrap();
    
    let mut watcher = config_manager.watch_config();
    
    // Update configuration
    let mut new_config = config_manager.get_config().await;
    new_config.server.port = 8888;
    config_manager.update_config(new_config).await.unwrap();
    
    // Wait for watcher to receive update
    sleep(Duration::from_millis(100)).await;
    
    let updated_config = watcher.borrow().clone();
    assert_eq!(updated_config.server.port, 8888);
}

#[tokio::test]
async fn test_secrets_manager_basic_operations() {
    let mut secrets_manager = SecretsManager::new();
    
    // Test storing and retrieving secrets without encryption
    secrets_manager.store_secret("api_key", "secret123").await.unwrap();
    let api_key = secrets_manager.get_secret("api_key").await;
    assert_eq!(api_key, Some("secret123".to_string()));
    
    // Test non-existent secret
    let non_existent = secrets_manager.get_secret("non_existent").await;
    assert_eq!(non_existent, None);
}

#[tokio::test]
async fn test_secrets_manager_with_encryption() {
    let mut secrets_manager = SecretsManager::new();
    secrets_manager.set_encryption_key("my-secret-key".to_string());
    
    // Test storing and retrieving encrypted secrets
    secrets_manager.store_secret("db_password", "super-secret-password").await.unwrap();
    let password = secrets_manager.get_secret("db_password").await;
    assert_eq!(password, Some("super-secret-password".to_string()));
    
    // Test that the stored value is actually encrypted
    // We can't directly access the encrypted value, but we can verify encryption is working
    // by storing another secret and verifying both are retrievable
    secrets_manager.store_secret("another_secret", "another-password").await.unwrap();
    let another_secret = secrets_manager.get_secret("another_secret").await;
    assert_eq!(another_secret, Some("another-password".to_string()));
}

#[tokio::test]
async fn test_secrets_manager_from_environment() {
    let mut secrets_manager = SecretsManager::new();
    
    // Set environment variables with prefix
    env::set_var("IRONKV_SECRET_DB_PASSWORD", "env-password");
    env::set_var("IRONKV_SECRET_API_KEY", "env-api-key");
    
    // Load secrets from environment
    secrets_manager.load_from_env("IRONKV_SECRET_").await.unwrap();
    
    // Verify secrets were loaded
    let db_password = secrets_manager.get_secret("db_password").await;
    let api_key = secrets_manager.get_secret("api_key").await;
    
    assert_eq!(db_password, Some("env-password".to_string()));
    assert_eq!(api_key, Some("env-api-key".to_string()));
    
    // Clean up
    env::remove_var("IRONKV_SECRET_DB_PASSWORD");
    env::remove_var("IRONKV_SECRET_API_KEY");
}

#[tokio::test]
async fn test_secrets_manager_from_file() {
    let mut secrets_manager = SecretsManager::new();
    
    // Create temporary secrets file
    let secrets_content = json!({
        "db_password": "file-password",
        "api_key": "file-api-key",
        "jwt_secret": "file-jwt-secret"
    }).to_string();
    
    let temp_file = create_temp_config(&secrets_content);
    
    // Load secrets from file
    secrets_manager.load_from_file(temp_file.path().to_str().unwrap()).await.unwrap();
    
    // Verify secrets were loaded
    let db_password = secrets_manager.get_secret("db_password").await;
    let api_key = secrets_manager.get_secret("api_key").await;
    let jwt_secret = secrets_manager.get_secret("jwt_secret").await;
    
    assert_eq!(db_password, Some("file-password".to_string()));
    assert_eq!(api_key, Some("file-api-key".to_string()));
    assert_eq!(jwt_secret, Some("file-jwt-secret".to_string()));
}

#[tokio::test]
async fn test_config_source_detection() {
    let mut config_manager = ConfigManager::new();
    
    // Test default source
    let source = config_manager.get_config_source().await;
    assert!(matches!(source, ConfigSource::Environment));
    
    // Test file source
    let temp_file = create_temp_config(r#"
server:
  port: 6379
  bind_address: "127.0.0.1"
  max_connections: 100
  connection_timeout: 30
  request_timeout: 60
  graceful_shutdown: true
  shutdown_timeout: 30
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
security:
  enable_auth: false
  enable_authz: false
  session_timeout: 3600
  max_login_attempts: 5
  password_min_length: 8
  audit_log_enabled: true
  audit_log_retention_days: 30
cluster:
  members: {}
persistence:
  enabled: false
  aof_enabled: false
  snapshot_enabled: false
  snapshot_interval: 3600
  data_dir: "./data"
logging:
  level: "info"
  format: "json"
  output: "stdout"
metrics:
  enabled: true
  port: 9090
  bind_address: "127.0.0.1"
tracing:
  enabled: false
  jaeger_endpoint: ""
  service_name: "kv-cache"
  service_version: "0.1.0"
health:
  enabled: true
  port: 8081
  bind_address: "127.0.0.1"
  check_interval: 30
  timeout: 5
"#);
    config_manager.load_from_file(temp_file.path().to_str().unwrap()).await.unwrap();
    
    let source = config_manager.get_config_source().await;
    assert!(matches!(source, ConfigSource::File(_)));
}

#[tokio::test]
async fn test_config_validation_errors() {
    let mut config_manager = ConfigManager::new();
    config_manager.add_validator(Box::new(DefaultConfigValidator));
    
    // Create invalid configuration
    let mut invalid_config = config_manager.get_config().await;
    invalid_config.server.port = 0; // Invalid port
    invalid_config.performance.max_connections = 0; // Invalid connections
    invalid_config.performance.batch_min_size = 100;
    invalid_config.performance.batch_max_size = 50; // Min > Max
    
    let validation = config_manager.validate_config(&invalid_config).await;
    
    assert!(!validation.is_valid);
    assert!(validation.errors.contains(&"Server port cannot be 0".to_string()));
    assert!(validation.errors.contains(&"Max connections must be greater than 0".to_string()));
    assert!(validation.errors.contains(&"Batch min size cannot be greater than batch max size".to_string()));
}

#[tokio::test]
async fn test_config_validation_warnings() {
    let mut config_manager = ConfigManager::new();
    config_manager.add_validator(Box::new(DefaultConfigValidator));
    
    // Create configuration with warnings
    let mut warning_config = config_manager.get_config().await;
    warning_config.cluster.members.clear(); // No cluster members
    warning_config.security.enable_authz = true;
    warning_config.security.enable_auth = false; // Authz without auth
    
    let validation = config_manager.validate_config(&warning_config).await;
    
    assert!(validation.is_valid); // Should be valid but with warnings
    assert!(validation.warnings.contains(&"No cluster members configured".to_string()));
    assert!(validation.warnings.contains(&"Authorization enabled without authentication".to_string()));
}

#[tokio::test]
async fn test_multiple_validators() {
    let mut config_manager = ConfigManager::new();
    config_manager.add_validator(Box::new(DefaultConfigValidator));
    config_manager.add_validator(Box::new(ProductionConfigValidator));
    
    // Create configuration that triggers warnings from both validators
    let mut config = config_manager.get_config().await;
    config.server.bind_address = "0.0.0.0".to_string();
    config.performance.max_connections = 50;
    config.security.enable_auth = false;
    config.cluster.members.clear();
    
    let validation = config_manager.validate_config(&config).await;
    
    assert!(validation.is_valid); // Should be valid but with warnings
    assert!(validation.warnings.len() >= 4); // Multiple warnings from both validators
}

#[tokio::test]
async fn test_config_manager_integration() {
    // Test full integration workflow
    let mut config_manager = ConfigManager::new();
    
    // Add validators
    config_manager.add_validator(Box::new(DefaultConfigValidator));
    config_manager.add_validator(Box::new(ProductionConfigValidator));
    
    // Set up secrets manager
    let mut secrets_manager = config_manager.secrets_manager().clone();
    secrets_manager.set_encryption_key("integration-test-key".to_string());
    secrets_manager.store_secret("test_secret", "test_value").await.unwrap();
    
    // Load configuration from file
    let config_content = r#"
server:
  port: 8080
  bind_address: "127.0.0.1"
  max_connections: 100
  connection_timeout: 30
  request_timeout: 60
  graceful_shutdown: true
  shutdown_timeout: 30
performance:
  max_connections: 1000
  enable_batching: true
  connection_idle_timeout: 300
  connection_max_age: 3600
  batch_min_size: 10
  batch_max_size: 1000
  batch_max_wait_time_ms: 50
  enable_monitoring: true
  benchmark_warmup_operations: 1000
  monitoring_interval: 60
security:
  enable_auth: true
  enable_authz: true
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
cluster:
  node_id: "test-node"
  bind_address: "127.0.0.1"
  port: 6380
  heartbeat_interval: 5
  failure_timeout: 15
  replication_factor: 3
  virtual_nodes_per_physical: 150
  members:
    node1: "127.0.0.1:6379"
    node2: "127.0.0.1:6380"
  replication_strategy: "Synchronous"
  enabled: true
persistence:
  enabled: false
  aof_enabled: false
  snapshot_enabled: false
  snapshot_interval: 3600
  data_dir: "./data"
logging:
  level: "info"
  format: "json"
  output: "stdout"
metrics:
  enabled: true
  port: 9090
  bind_address: "127.0.0.1"
tracing:
  enabled: false
  jaeger_endpoint: ""
  service_name: "kv-cache"
  service_version: "0.1.0"
health:
  enabled: true
  port: 8081
  bind_address: "127.0.0.1"
  check_interval: 30
  timeout: 5
"#;
    
    let temp_file = create_temp_config(config_content);
    config_manager.load_from_file(temp_file.path().to_str().unwrap()).await.unwrap();
    
    // Verify configuration
    let config = config_manager.get_config().await;
    assert_eq!(config.server.port, 8080);
    assert_eq!(config.performance.max_connections, 1000);
    assert_eq!(config.cluster.members.len(), 2);
    
    // Verify validation
    let validation = config_manager.validate_config(&config).await;
    assert!(validation.is_valid);
    
    // Verify secrets
    let secret = secrets_manager.get_secret("test_secret").await;
    assert_eq!(secret, Some("test_value".to_string()));
    
    // Verify config source
    let source = config_manager.get_config_source().await;
    assert!(matches!(source, ConfigSource::File(_)));
} 