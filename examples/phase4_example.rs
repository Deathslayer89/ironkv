//! Example demonstrating Phase 4: Production Readiness & Observability
//! 
//! This example shows:
//! 1. Configuration management with TOML/YAML
//! 2. Structured logging with tracing
//! 3. Metrics collection and monitoring
//! 4. Distributed tracing with spans
//! 5. Graceful shutdown handling

use kv_cache_core::{
    config::{CacheConfig, LoggingConfig, MetricsConfig, TracingConfig},
    log::{init_logging, log_startup, log_cache_operation, log_memory_usage},
    metrics::{MetricsCollector, MetricsSummary},
    trace::{init_tracing, trace_startup, trace_cache_operation, trace_request_lifecycle},
    shutdown::{GracefulShutdownManager, ShutdownHandle, ShutdownSignal},
    TTLStore, Value,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Phase 4: Production Readiness & Observability Example");
    println!("========================================================\n");

    // 1. Configuration Management
    println!("📋 1. Configuration Management");
    println!("-----------------------------");
    
    // Create default configuration
    let mut config = CacheConfig::default();
    
    // Customize configuration
    config.server.port = 6380;
    config.server.bind_address = "127.0.0.1".to_string();
    config.server.max_connections = 500;
    
    config.logging.level = kv_cache_core::config::LogLevel::Debug;
    config.logging.console = true;
    config.logging.structured = true;
    
    config.metrics.enabled = true;
    config.metrics.port = 9091;
    config.metrics.prometheus = true;
    config.metrics.histograms = true;
    
    config.tracing.enabled = true;
    config.tracing.service_name = "kv-cache-example".to_string();
    config.tracing.service_version = "1.0.0".to_string();
    config.tracing.sampling_rate = 0.5;
    config.tracing.local_spans = true;
    
    // Validate configuration
    match config.validate() {
        Ok(_) => println!("✅ Configuration validation passed"),
        Err(errors) => {
            println!("❌ Configuration validation failed:");
            for error in errors {
                println!("   - {}", error);
            }
            return Err("Configuration validation failed".into());
        }
    }
    
    // Save configuration to file
    config.save_to_file("config.toml")?;
    println!("✅ Configuration saved to config.toml");
    
    // Load configuration from file
    let loaded_config = CacheConfig::from_file("config.toml")?;
    println!("✅ Configuration loaded from config.toml");
    assert_eq!(loaded_config.server.port, 6380);
    
    println!();

    // 2. Initialize Logging
    println!("📝 2. Structured Logging");
    println!("------------------------");
    
    init_logging(&config.logging)?;
    println!("✅ Logging system initialized");
    
    // Log startup information
    log_startup(&config);
    
    // Demonstrate structured logging
    log_cache_operation("SET", "user:1", true, Duration::from_millis(5), None);
    log_cache_operation("GET", "user:1", true, Duration::from_millis(2), None);
    log_cache_operation("GET", "user:2", false, Duration::from_millis(1), None);
    
    println!();

    // 3. Initialize Tracing
    println!("🔍 3. Distributed Tracing");
    println!("-------------------------");
    
    init_tracing(&config.tracing)?;
    println!("✅ Tracing system initialized");
    
    // Trace startup
    trace_startup(&config);
    
    println!();

    // 4. Initialize Metrics
    println!("📊 4. Metrics Collection");
    println!("------------------------");
    
    let metrics_collector = MetricsCollector::new(config.metrics.clone());
    metrics_collector.start().await?;
    println!("✅ Metrics collector started");
    
    // Record some metrics
    metrics_collector.record_cache_operation("SET", Duration::from_millis(10), true);
    metrics_collector.record_cache_operation("GET", Duration::from_millis(5), true);
    metrics_collector.record_cache_operation("DELETE", Duration::from_millis(3), true);
    metrics_collector.record_cache_access(true);
    metrics_collector.record_cache_access(true);
    metrics_collector.record_cache_access(false);
    
    // Update memory usage
    metrics_collector.update_memory_usage(1024 * 1024, 1000); // 1MB, 1000 keys
    
    // Get metrics summary
    let summary = metrics_collector.get_metrics_summary();
    println!("📈 Cache Hit Ratio: {:.2}%", summary.cache.hit_ratio * 100.0);
    println!("📈 Total Keys: {}", summary.cache.total_keys);
    println!("📈 Memory Usage: {} bytes", summary.cache.memory_usage_bytes);
    
    println!();

    // 5. Graceful Shutdown
    println!("🛑 5. Graceful Shutdown");
    println!("----------------------");
    
    let mut shutdown_manager = GracefulShutdownManager::new(config.clone());
    let shutdown_handle = shutdown_manager.get_shutdown_handle();
    
    println!("✅ Shutdown manager initialized");
    
    // Simulate some work with tracing
    let store = Arc::new(TTLStore::new());
    
    // Demonstrate traced operations
    let result = trace_request_lifecycle(
        "req_001",
        "SET",
        "config:timeout",
        || {
            store.set("config:timeout".to_string(), Value::String("30".to_string()), None);
            Ok("OK".to_string())
        }
    ).await?;
    
    let result = trace_request_lifecycle(
        "req_002", 
        "GET",
        "config:timeout",
        || {
            let value = store.get("config:timeout");
            Ok(value.map(|v| v.to_string()).unwrap_or_else(|| "NOT_FOUND".to_string()))
        }
    ).await?;
    
    println!("✅ Traced operations completed");
    
    // Simulate some background work
    let shutdown_handle_clone = shutdown_handle;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut counter = 0;
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    counter += 1;
                    tracing::debug!("Background task iteration: {}", counter);
                    
                    if counter >= 5 {
                        break;
                    }
                }
                signal = shutdown_handle_clone.wait_for_shutdown() => {
                    tracing::info!("Background task received shutdown signal: {:?}", signal);
                    break;
                }
            }
        }
        
        tracing::info!("Background task completed");
    });
    
    // Wait a bit to see the background task in action
    sleep(Duration::from_secs(3)).await;
    
    // Demonstrate graceful shutdown
    println!("🔄 Initiating graceful shutdown...");
    
    // In a real application, this would be triggered by SIGINT/SIGTERM
    // For this example, we'll simulate it
    let shutdown_result = shutdown_manager.run(|| {
        tracing::info!("Main application logic completed");
        Ok(())
    }).await;
    
    match shutdown_result {
        Ok(_) => println!("✅ Graceful shutdown completed successfully"),
        Err(e) => println!("❌ Shutdown error: {}", e),
    }
    
    println!();

    // 6. Final Metrics Summary
    println!("📊 6. Final Metrics Summary");
    println!("---------------------------");
    
    let final_summary = metrics_collector.get_metrics_summary();
    
    println!("Server Metrics:");
    println!("  - Active Connections: {}", final_summary.server.active_connections);
    println!("  - Total Connections: {}", final_summary.server.total_connections);
    println!("  - Uptime: {} seconds", final_summary.server.uptime_seconds);
    
    println!("Cache Metrics:");
    println!("  - Total Keys: {}", final_summary.cache.total_keys);
    println!("  - Memory Usage: {} bytes", final_summary.cache.memory_usage_bytes);
    println!("  - Cache Hits: {}", final_summary.cache.cache_hits);
    println!("  - Cache Misses: {}", final_summary.cache.cache_misses);
    println!("  - Hit Ratio: {:.2}%", final_summary.cache.hit_ratio * 100.0);
    println!("  - Evictions: {}", final_summary.cache.evictions);
    println!("  - Expired Keys: {}", final_summary.cache.expired_keys);
    
    println!("Network Metrics:");
    println!("  - Bytes Sent: {}", final_summary.network.bytes_sent);
    println!("  - Bytes Received: {}", final_summary.network.bytes_received);
    println!("  - Connections Established: {}", final_summary.network.connections_established);
    println!("  - Connections Closed: {}", final_summary.network.connections_closed);
    println!("  - Network Errors: {}", final_summary.network.network_errors);
    
    println!();
    println!("🎉 Phase 4 Example Completed Successfully!");
    println!();
    println!("Key Features Demonstrated:");
    println!("✅ Configuration management with TOML");
    println!("✅ Structured logging with tracing");
    println!("✅ Metrics collection and monitoring");
    println!("✅ Distributed tracing with spans");
    println!("✅ Graceful shutdown handling");
    println!("✅ Production-ready observability");
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_phase4_example_config() {
        let config = CacheConfig::default();
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn test_phase4_example_metrics() {
        let config = CacheConfig::default();
        let collector = MetricsCollector::new(config.metrics);
        let summary = collector.get_metrics_summary();
        assert_eq!(summary.cache.total_keys, 0);
    }

    #[tokio::test]
    async fn test_phase4_example_shutdown() {
        let config = CacheConfig::default();
        let manager = GracefulShutdownManager::new(config);
        assert!(!manager.is_shutting_down());
    }
} 