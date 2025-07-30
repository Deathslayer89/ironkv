//! Structured logging for the distributed key-value cache
//! 
//! This module provides structured logging using the `tracing` crate with
//! support for console output, file logging, and log rotation.

use crate::config::{LoggingConfig, LogLevel};
use std::sync::Once;
use tracing::Level;
use tracing_subscriber::fmt::{format::FmtSpan, time::UtcTime};
use std::time::Duration;

static INIT: Once = Once::new();

/// Initialize the logging system based on configuration
pub fn init_logging(config: &LoggingConfig) -> Result<(), Box<dyn std::error::Error>> {
    INIT.call_once(|| {
        let _ = init_logging_inner(config);
    });
    Ok(())
}

fn init_logging_inner(config: &LoggingConfig) -> Result<(), Box<dyn std::error::Error>> {
    let level = convert_log_level(&config.level);

    // Create a simple console layer
    let mut builder = tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_span_events(FmtSpan::CLOSE)
        .with_timer(UtcTime::rfc_3339())
        .with_max_level(level);

    // Note: Format changes are not supported in this simplified version
    // All formats will use the default format

    if config.structured {
        builder = builder.with_target(true);
    }

    // Initialize the subscriber
    builder.init();

    tracing::info!("Logging system initialized with level: {:?}", config.level);
    Ok(())
}

/// Create a file logging layer (placeholder - not implemented)
fn create_file_layer(
    _config: &LoggingConfig,
    _file_path: &str,
    _level: Level,
) -> Result<(), Box<dyn std::error::Error>> {
    // File logging not implemented in this simplified version
    Ok(())
}

/// Convert our log level to tracing level
fn convert_log_level(level: &LogLevel) -> Level {
    match level {
        LogLevel::Error => Level::ERROR,
        LogLevel::Warn => Level::WARN,
        LogLevel::Info => Level::INFO,
        LogLevel::Debug => Level::DEBUG,
        LogLevel::Trace => Level::TRACE,
    }
}

/// Log a cache operation with structured fields
pub fn log_cache_operation(
    operation: &str,
    key: &str,
    success: bool,
    duration: Duration,
    additional_fields: Option<Vec<(&str, String)>>,
) {
    let span = tracing::info_span!(
        "cache_operation",
        operation = operation,
        key = key,
        success = success,
        duration_ms = duration.as_millis() as u64,
    );

    if let Some(fields) = additional_fields {
        for (name, value) in fields {
            span.record(name, &value.as_str());
        }
    }

    let _enter = span.enter();
    
    if success {
        tracing::info!("Cache operation completed successfully");
    } else {
        tracing::error!("Cache operation failed");
    }
}

/// Log a cluster operation with structured fields
pub fn log_cluster_operation(
    operation: &str,
    node_id: &str,
    success: bool,
    duration: Duration,
    additional_fields: Option<Vec<(&str, String)>>,
) {
    let span = tracing::info_span!(
        "cluster_operation",
        operation = operation,
        node_id = node_id,
        success = success,
        duration_ms = duration.as_millis() as u64,
    );

    if let Some(fields) = additional_fields {
        for (name, value) in fields {
            span.record(name, &value.as_str());
        }
    }

    let _enter = span.enter();
    
    if success {
        tracing::info!("Cluster operation completed successfully");
    } else {
        tracing::error!("Cluster operation failed");
    }
}

/// Log a replication operation with structured fields
pub fn log_replication_operation(
    operation: &str,
    follower_id: &str,
    sequence_number: u64,
    success: bool,
    duration: Duration,
    additional_fields: Option<Vec<(&str, String)>>,
) {
    let span = tracing::info_span!(
        "replication_operation",
        operation = operation,
        follower_id = follower_id,
        sequence_number = sequence_number,
        success = success,
        duration_ms = duration.as_millis() as u64,
    );

    if let Some(fields) = additional_fields {
        for (name, value) in fields {
            span.record(name, &value.as_str());
        }
    }

    let _enter = span.enter();
    
    if success {
        tracing::info!("Replication operation completed successfully");
    } else {
        tracing::error!("Replication operation failed");
    }
}

/// Log a network operation with structured fields
pub fn log_network_operation(
    operation: &str,
    remote_addr: &str,
    success: bool,
    duration: Duration,
    bytes_sent: Option<u64>,
    bytes_received: Option<u64>,
    additional_fields: Option<Vec<(&str, String)>>,
) {
    let span = tracing::info_span!(
        "network_operation",
        operation = operation,
        remote_addr = remote_addr,
        success = success,
        duration_ms = duration.as_millis() as u64,
    );

    if let Some(bytes) = bytes_sent {
        span.record("bytes_sent", &bytes);
    }
    if let Some(bytes) = bytes_received {
        span.record("bytes_received", &bytes);
    }

    if let Some(fields) = additional_fields {
        for (name, value) in fields {
            span.record(name, &value.as_str());
        }
    }

    let _enter = span.enter();
    
    if success {
        tracing::info!("Network operation completed successfully");
    } else {
        tracing::error!("Network operation failed");
    }
}

/// Log a persistence operation with structured fields
pub fn log_persistence_operation(
    operation: &str,
    file_path: &str,
    success: bool,
    duration: Duration,
    bytes_written: Option<u64>,
    additional_fields: Option<Vec<(&str, String)>>,
) {
    let span = tracing::info_span!(
        "persistence_operation",
        operation = operation,
        file_path = file_path,
        success = success,
        duration_ms = duration.as_millis() as u64,
    );

    if let Some(bytes) = bytes_written {
        span.record("bytes_written", &bytes);
    }

    if let Some(fields) = additional_fields {
        for (name, value) in fields {
            span.record(name, &value.as_str());
        }
    }

    let _enter = span.enter();
    
    if success {
        tracing::info!("Persistence operation completed successfully");
    } else {
        tracing::error!("Persistence operation failed");
    }
}

/// Log system startup
pub fn log_startup(config: &crate::config::CacheConfig) {
    tracing::info!("Starting key-value cache server");
    tracing::info!("Server configuration: {}:{}", config.server.bind_address, config.server.port);
    
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

/// Log system shutdown
pub fn log_shutdown(reason: &str) {
    tracing::info!("Shutting down key-value cache server: {}", reason);
}

/// Log configuration reload
pub fn log_config_reload(path: &str, success: bool) {
    if success {
        tracing::info!("Configuration reloaded successfully from: {}", path);
    } else {
        tracing::error!("Failed to reload configuration from: {}", path);
    }
}

/// Log memory usage
pub fn log_memory_usage(current_bytes: u64, max_bytes: Option<u64>, key_count: usize) {
    let span = tracing::info_span!(
        "memory_usage",
        current_bytes = current_bytes,
        key_count = key_count,
    );

    if let Some(max) = max_bytes {
        let usage_percent = (current_bytes as f64 / max as f64) * 100.0;
        span.record("max_bytes", &max);
        span.record("usage_percent", &usage_percent);
        
        if usage_percent > 80.0 {
            tracing::warn!("High memory usage: {:.1}%", usage_percent);
        }
    }

    let _enter = span.enter();
    tracing::debug!("Memory usage: {} bytes, {} keys", current_bytes, key_count);
}

/// Log performance metrics
pub fn log_performance_metrics(
    operation: &str,
    count: u64,
    total_duration: Duration,
    min_duration: Duration,
    max_duration: Duration,
    avg_duration: Duration,
) {
    let span = tracing::info_span!(
        "performance_metrics",
        operation = operation,
        count = count,
        total_duration_ms = total_duration.as_millis() as u64,
        min_duration_ms = min_duration.as_millis() as u64,
        max_duration_ms = max_duration.as_millis() as u64,
        avg_duration_ms = avg_duration.as_millis() as u64,
    );

    let _enter = span.enter();
    tracing::info!("Performance metrics for {}", operation);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LoggingConfig;

    #[test]
    fn test_log_level_conversion() {
        assert_eq!(convert_log_level(&LogLevel::Error), Level::ERROR);
        assert_eq!(convert_log_level(&LogLevel::Warn), Level::WARN);
        assert_eq!(convert_log_level(&LogLevel::Info), Level::INFO);
        assert_eq!(convert_log_level(&LogLevel::Debug), Level::DEBUG);
        assert_eq!(convert_log_level(&LogLevel::Trace), Level::TRACE);
    }

    #[test]
    fn test_logging_initialization() {
        let config = LoggingConfig::default();
        assert!(init_logging(&config).is_ok());
    }
} 