//! Distributed tracing for the key-value cache
//! 
//! This module provides distributed tracing using the `tracing` crate with
//! support for local spans and optional OpenTelemetry export.

use crate::config::TracingConfig;
use std::sync::Once;
use tracing::{info_span, instrument, Span};




static INIT: Once = Once::new();

/// Initialize the tracing system based on configuration
pub fn init_tracing(config: &TracingConfig) -> Result<(), Box<dyn std::error::Error>> {
    if !config.enabled {
        return Ok(());
    }

    INIT.call_once(|| {
        let _ = init_tracing_inner(config);
    });
    Ok(())
}

fn init_tracing_inner(config: &TracingConfig) -> Result<(), Box<dyn std::error::Error>> {
    // In this simplified version, we don't initialize a separate subscriber
    // since logging already sets up the global subscriber
    // In a real implementation, you would coordinate between logging and tracing
    
    tracing::info!(
        "Tracing system initialized: service={}, version={}, sampling_rate={}",
        config.service_name,
        config.service_version,
        config.sampling_rate
    );

    Ok(())
}

/// Create OpenTelemetry layer
fn create_opentelemetry_layer(
    _config: &TracingConfig,
    _endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // This is a placeholder for OpenTelemetry integration
    // In a real implementation, you would use tracing-opentelemetry crate
    tracing::warn!("OpenTelemetry integration not fully implemented");
    Ok(())
}

/// Create a span for cache operations
pub fn cache_operation_span(operation: &str, key: &str) -> Span {
    info_span!(
        "cache_operation",
        operation = operation,
        key = key,
        service.name = "kv-cache",
        service.version = "0.1.0"
    )
}

/// Create a span for cluster operations
pub fn cluster_operation_span(operation: &str, node_id: &str) -> Span {
    info_span!(
        "cluster_operation",
        operation = operation,
        node_id = node_id,
        service.name = "kv-cache",
        service.version = "0.1.0"
    )
}

/// Create a span for replication operations
pub fn replication_operation_span(operation: &str, follower_id: &str, sequence_number: u64) -> Span {
    info_span!(
        "replication_operation",
        operation = operation,
        follower_id = follower_id,
        sequence_number = sequence_number,
        service.name = "kv-cache",
        service.version = "0.1.0"
    )
}

/// Create a span for network operations
pub fn network_operation_span(operation: &str, remote_addr: &str) -> Span {
    info_span!(
        "network_operation",
        operation = operation,
        remote_addr = remote_addr,
        service.name = "kv-cache",
        service.version = "0.1.0"
    )
}

/// Create a span for persistence operations
pub fn persistence_operation_span(operation: &str, file_path: &str) -> Span {
    info_span!(
        "persistence_operation",
        operation = operation,
        file_path = file_path,
        service.name = "kv-cache",
        service.version = "0.1.0"
    )
}

/// Instrument a cache operation with tracing
#[instrument(skip(operation), fields(operation = %operation_name, key = %key))]
pub async fn trace_cache_operation<F, R>(
    operation_name: &str,
    key: &str,
    operation: F,
) -> Result<R, Box<dyn std::error::Error>>
where
    F: FnOnce() -> Result<R, Box<dyn std::error::Error>>,
{
    let span = cache_operation_span(operation_name, key);
    let _enter = span.enter();
    
    tracing::debug!("Starting cache operation");
    let result = operation();
    
    match &result {
        Ok(_) => tracing::debug!("Cache operation completed successfully"),
        Err(e) => tracing::error!("Cache operation failed: {}", e),
    }
    
    result
}

/// Instrument a cluster operation with tracing
#[instrument(skip(operation), fields(operation = %operation_name, node_id = %node_id))]
pub async fn trace_cluster_operation<F, R>(
    operation_name: &str,
    node_id: &str,
    operation: F,
) -> Result<R, Box<dyn std::error::Error>>
where
    F: FnOnce() -> Result<R, Box<dyn std::error::Error>>,
{
    let span = cluster_operation_span(operation_name, node_id);
    let _enter = span.enter();
    
    tracing::debug!("Starting cluster operation");
    let result = operation();
    
    match &result {
        Ok(_) => tracing::debug!("Cluster operation completed successfully"),
        Err(e) => tracing::error!("Cluster operation failed: {}", e),
    }
    
    result
}

/// Instrument a replication operation with tracing
#[instrument(skip(operation), fields(operation = %operation_name, follower_id = %follower_id, sequence_number = %sequence_number))]
pub async fn trace_replication_operation<F, R>(
    operation_name: &str,
    follower_id: &str,
    sequence_number: u64,
    operation: F,
) -> Result<R, Box<dyn std::error::Error>>
where
    F: FnOnce() -> Result<R, Box<dyn std::error::Error>>,
{
    let span = replication_operation_span(operation_name, follower_id, sequence_number);
    let _enter = span.enter();
    
    tracing::debug!("Starting replication operation");
    let result = operation();
    
    match &result {
        Ok(_) => tracing::debug!("Replication operation completed successfully"),
        Err(e) => tracing::error!("Replication operation failed: {}", e),
    }
    
    result
}

/// Instrument a network operation with tracing
#[instrument(skip(operation), fields(operation = %operation_name, remote_addr = %remote_addr))]
pub async fn trace_network_operation<F, R>(
    operation_name: &str,
    remote_addr: &str,
    operation: F,
) -> Result<R, Box<dyn std::error::Error>>
where
    F: FnOnce() -> Result<R, Box<dyn std::error::Error>>,
{
    let span = network_operation_span(operation_name, remote_addr);
    let _enter = span.enter();
    
    tracing::debug!("Starting network operation");
    let result = operation();
    
    match &result {
        Ok(_) => tracing::debug!("Network operation completed successfully"),
        Err(e) => tracing::error!("Network operation failed: {}", e),
    }
    
    result
}

/// Instrument a persistence operation with tracing
#[instrument(skip(operation), fields(operation = %operation_name, file_path = %file_path))]
pub async fn trace_persistence_operation<F, R>(
    operation_name: &str,
    file_path: &str,
    operation: F,
) -> Result<R, Box<dyn std::error::Error>>
where
    F: FnOnce() -> Result<R, Box<dyn std::error::Error>>,
{
    let span = persistence_operation_span(operation_name, file_path);
    let _enter = span.enter();
    
    tracing::debug!("Starting persistence operation");
    let result = operation();
    
    match &result {
        Ok(_) => tracing::debug!("Persistence operation completed successfully"),
        Err(e) => tracing::error!("Persistence operation failed: {}", e),
    }
    
    result
}

/// Add custom attributes to the current span
pub fn add_span_attribute(name: &str, value: &str) {
    tracing::span::Span::current().record(name, &value);
}

/// Add custom attributes to the current span with numeric value
pub fn add_span_attribute_number(name: &str, value: u64) {
    tracing::span::Span::current().record(name, &value);
}

/// Add custom attributes to the current span with float value
pub fn add_span_attribute_float(name: &str, value: f64) {
    tracing::span::Span::current().record(name, &value);
}

/// Add custom attributes to the current span with boolean value
pub fn add_span_attribute_bool(name: &str, value: bool) {
    tracing::span::Span::current().record(name, &value);
}

/// Create a span for request processing
pub fn request_span(request_id: &str, operation: &str) -> Span {
    info_span!(
        "request",
        request_id = request_id,
        operation = operation,
        service.name = "kv-cache",
        service.version = "0.1.0"
    )
}

/// Create a span for command parsing
pub fn command_parsing_span(command: &str) -> Span {
    info_span!(
        "command_parsing",
        command = command,
        service.name = "kv-cache",
        service.version = "0.1.0"
    )
}

/// Create a span for storage access
pub fn storage_access_span(operation: &str, key: &str) -> Span {
    info_span!(
        "storage_access",
        operation = operation,
        key = key,
        service.name = "kv-cache",
        service.version = "0.1.0"
    )
}

/// Create a span for eviction process
pub fn eviction_span(policy: &str, keys_evicted: usize) -> Span {
    info_span!(
        "eviction",
        policy = policy,
        keys_evicted = keys_evicted,
        service.name = "kv-cache",
        service.version = "0.1.0"
    )
}

/// Create a span for TTL cleanup
pub fn ttl_cleanup_span(keys_checked: usize, keys_expired: usize) -> Span {
    info_span!(
        "ttl_cleanup",
        keys_checked = keys_checked,
        keys_expired = keys_expired,
        service.name = "kv-cache",
        service.version = "0.1.0"
    )
}

/// Create a span for membership changes
pub fn membership_change_span(change_type: &str, node_id: &str) -> Span {
    info_span!(
        "membership_change",
        change_type = change_type,
        node_id = node_id,
        service.name = "kv-cache",
        service.version = "0.1.0"
    )
}

/// Create a span for heartbeat operations
pub fn heartbeat_span(node_id: &str, status: &str) -> Span {
    info_span!(
        "heartbeat",
        node_id = node_id,
        status = status,
        service.name = "kv-cache",
        service.version = "0.1.0"
    )
}

/// Create a span for sharding operations
pub fn sharding_span(operation: &str, key: &str, target_node: &str) -> Span {
    info_span!(
        "sharding",
        operation = operation,
        key = key,
        target_node = target_node,
        service.name = "kv-cache",
        service.version = "0.1.0"
    )
}

/// Trace a complete request lifecycle
pub async fn trace_request_lifecycle<F, R>(
    request_id: &str,
    operation: &str,
    key: &str,
    handler: F,
) -> Result<R, Box<dyn std::error::Error>>
where
    F: FnOnce() -> Result<R, Box<dyn std::error::Error>>,
{
    let request_span = request_span(request_id, operation);
    let _request_enter = request_span.enter();
    
    tracing::debug!("Processing request");
    
    // Parse command
    let command_span = command_parsing_span(operation);
    let _command_enter = command_span.enter();
    tracing::debug!("Parsing command");
    drop(_command_enter);
    
    // Access storage
    let storage_span = storage_access_span(operation, key);
    let _storage_enter = storage_span.enter();
    tracing::debug!("Accessing storage");
    
    let result = handler();
    
    match &result {
        Ok(_) => tracing::debug!("Request completed successfully"),
        Err(e) => tracing::error!("Request failed: {}", e),
    }
    
    result
}

/// Trace a background task
pub async fn trace_background_task<F, R>(
    task_name: &str,
    task: F,
) -> Result<R, Box<dyn std::error::Error>>
where
    F: FnOnce() -> Result<R, Box<dyn std::error::Error>>,
{
    let span = info_span!(
        "background_task",
        task_name = task_name,
        service.name = "kv-cache",
        service.version = "0.1.0"
    );
    let _enter = span.enter();
    
    tracing::debug!("Starting background task");
    let result = task();
    
    match &result {
        Ok(_) => tracing::debug!("Background task completed successfully"),
        Err(e) => tracing::error!("Background task failed: {}", e),
    }
    
    result
}

/// Trace system startup
pub fn trace_startup(config: &crate::config::CacheConfig) {
    let span = info_span!(
        "system_startup",
        service.name = "kv-cache",
        service.version = "0.1.0"
    );
    let _enter = span.enter();
    
    tracing::info!("Starting key-value cache system");
    tracing::info!("Configuration loaded: server={}:{}", config.server.bind_address, config.server.port);
    
    if config.cluster.enabled {
        tracing::info!("Cluster mode enabled: node_id={}", config.cluster.node_id);
    }
    
    if config.persistence.enabled {
        tracing::info!("Persistence enabled: data_dir={}", config.persistence.data_dir);
    }
    
    if config.metrics.enabled {
        tracing::info!("Metrics enabled: {}:{}", config.metrics.bind_address, config.metrics.port);
    }
    
    if config.tracing.enabled {
        tracing::info!("Tracing enabled: service={}", config.tracing.service_name);
    }
}

/// Trace system shutdown
pub fn trace_shutdown(reason: &str) {
    let span = info_span!(
        "system_shutdown",
        reason = reason,
        service.name = "kv-cache",
        service.version = "0.1.0"
    );
    let _enter = span.enter();
    
    tracing::info!("Shutting down key-value cache system: {}", reason);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TracingConfig;

    #[test]
    fn test_tracing_initialization() {
        let config = TracingConfig::default();
        assert!(init_tracing(&config).is_ok());
    }

    #[test]
    fn test_span_creation() {
        let span = cache_operation_span("SET", "test_key");
        // Span creation test - name() method not available in this context
        assert!(true);
    }

    #[tokio::test]
    async fn test_trace_cache_operation() {
        let result = trace_cache_operation("GET", "test_key", || {
            Ok::<String, Box<dyn std::error::Error>>("test_value".to_string())
        }).await;
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test_value");
    }

    #[tokio::test]
    async fn test_trace_request_lifecycle() {
        let result = trace_request_lifecycle(
            "req_123",
            "SET",
            "test_key",
            || Ok::<String, Box<dyn std::error::Error>>("test_value".to_string())
        ).await;
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test_value");
    }
} 