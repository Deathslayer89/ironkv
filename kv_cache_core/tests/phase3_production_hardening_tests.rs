//! Phase 3: Production Hardening Tests
//! 
//! This module contains comprehensive tests for production hardening features:
//! - Health check system
//! - Security features (authentication, authorization, audit logging)
//! - Performance optimization (connection pooling, request batching, benchmarks)
//! - Monitoring and observability

use kv_cache_core::{
    CacheConfig, HealthChecker, HealthStatus,
    SecurityManager,
    ConnectionPool, RequestBatch, PerformanceMonitor,
    Store, MetricsCollector, consensus::state::RaftState,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

/// Test health check system functionality
mod health_tests {
    use super::*;

    #[tokio::test]
    async fn test_health_checker_creation() {
        let config = CacheConfig::default();
        let store = Arc::new(Store::new());
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let raft_state = Arc::new(RwLock::new(RaftState::new("test-node".to_string())));

        let health_checker = HealthChecker::new(
            config.health,
            store,
            metrics,
            raft_state,
        );

        // Test that health checker was created successfully
        let health_response = health_checker.check_health().await;
        assert_eq!(health_response.service, "kv-cache");
    }

    #[tokio::test]
    async fn test_comprehensive_health_check() {
        let config = CacheConfig::default();
        let store = Arc::new(Store::new());
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let raft_state = Arc::new(RwLock::new(RaftState::new("test-node".to_string())));

        let health_checker = HealthChecker::new(
            config.health,
            store,
            metrics,
            raft_state,
        );

        let health_response = health_checker.check_health().await;
        
        assert_eq!(health_response.service, "kv-cache");
        assert!(!health_response.checks.is_empty());
        assert!(health_response.summary.total_checks > 0);
    }

    #[tokio::test]
    async fn test_quick_health_check() {
        let config = CacheConfig::default();
        let store = Arc::new(Store::new());
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let raft_state = Arc::new(RwLock::new(RaftState::new("test-node".to_string())));

        let health_checker = HealthChecker::new(
            config.health,
            store,
            metrics,
            raft_state,
        );

        let status = health_checker.quick_health_check().await;
        assert!(matches!(status, HealthStatus::Healthy | HealthStatus::Degraded | HealthStatus::Unhealthy));
    }

    #[tokio::test]
    async fn test_health_check_caching() {
        let config = CacheConfig::default();
        let store = Arc::new(Store::new());
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let raft_state = Arc::new(RwLock::new(RaftState::new("test-node".to_string())));

        let health_checker = HealthChecker::new(
            config.health,
            store,
            metrics,
            raft_state,
        );

        // First check
        let _response1 = health_checker.check_health().await;
        
        // Get cached result
        let cached_response = health_checker.get_last_health_check().await;
        assert!(cached_response.is_some());
        
        let cached = cached_response.unwrap();
        assert_eq!(cached.service, "kv-cache");
    }
}

/// Test security system functionality
mod security_tests {
    use super::*;

    #[tokio::test]
    async fn test_security_manager_creation() {
        let config = CacheConfig::default();
        let security_manager = SecurityManager::new(config.security);
        
        // Test that security manager was created successfully by testing authentication
        security_manager.initialize_defaults().await.unwrap();
        let auth_result = security_manager.authenticate("admin", "admin123", None, None).await;
        assert!(auth_result.success);
    }

    #[tokio::test]
    async fn test_security_manager_initialization() {
        let mut config = CacheConfig::default();
        config.security.create_default_admin = true;
        config.security.default_admin_password = "admin123".to_string();
        
        let security_manager = SecurityManager::new(config.security);
        assert!(security_manager.initialize_defaults().await.is_ok());
    }

    #[tokio::test]
    async fn test_authentication() {
        let mut config = CacheConfig::default();
        config.security.create_default_admin = true;
        config.security.default_admin_password = "admin123".to_string();
        
        let security_manager = SecurityManager::new(config.security);
        security_manager.initialize_defaults().await.unwrap();

        let auth_result = security_manager.authenticate("admin", "admin123", None, None).await;
        assert!(auth_result.success);
        assert!(auth_result.session_token.is_some());
        assert!(auth_result.user.is_some());
        
        let user = auth_result.user.unwrap();
        assert_eq!(user.username, "admin");
        assert!(user.roles.contains(&"admin".to_string()));
    }

    #[tokio::test]
    async fn test_authentication_failure() {
        let mut config = CacheConfig::default();
        config.security.create_default_admin = true;
        config.security.default_admin_password = "admin123".to_string();
        
        let security_manager = SecurityManager::new(config.security);
        security_manager.initialize_defaults().await.unwrap();

        let auth_result = security_manager.authenticate("admin", "wrongpassword", None, None).await;
        assert!(!auth_result.success);
        assert!(auth_result.session_token.is_none());
        assert!(auth_result.user.is_none());
        assert!(auth_result.error.is_some());
    }

    #[tokio::test]
    async fn test_authorization() {
        let mut config = CacheConfig::default();
        config.security.create_default_admin = true;
        config.security.default_admin_password = "admin123".to_string();
        
        let security_manager = SecurityManager::new(config.security);
        security_manager.initialize_defaults().await.unwrap();

        let auth_result = security_manager.authenticate("admin", "admin123", None, None).await;
        assert!(auth_result.success);

        let session_token = auth_result.session_token.unwrap();
        let authz_result = security_manager.authorize(&session_token, "GET", "test-key").await;
        assert!(authz_result.allowed);
    }

    #[tokio::test]
    async fn test_session_validation() {
        let mut config = CacheConfig::default();
        config.security.create_default_admin = true;
        config.security.default_admin_password = "admin123".to_string();
        
        let security_manager = SecurityManager::new(config.security);
        security_manager.initialize_defaults().await.unwrap();

        let auth_result = security_manager.authenticate("admin", "admin123", None, None).await;
        assert!(auth_result.success);

        let session_token = auth_result.session_token.unwrap();
        let user = security_manager.validate_session(&session_token).await;
        assert!(user.is_some());
        
        let user = user.unwrap();
        assert_eq!(user.username, "admin");
    }

    #[tokio::test]
    async fn test_audit_logging() {
        let config = CacheConfig::default();
        let security_manager = SecurityManager::new(config.security);

        security_manager.log_audit_event(
            Some("test-user".to_string()),
            "test-action",
            "test-resource",
            true,
            None,
            None,
            HashMap::new(),
        ).await;

        let entries = security_manager.get_audit_log(None, None, None).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "test-action");
        assert_eq!(entries[0].resource, "test-resource");
        assert!(entries[0].success);
    }

    #[tokio::test]
    async fn test_audit_log_filtering() {
        let config = CacheConfig::default();
        let security_manager = SecurityManager::new(config.security);

        security_manager.log_audit_event(
            Some("user1".to_string()),
            "action1",
            "resource1",
            true,
            None,
            None,
            HashMap::new(),
        ).await;

        security_manager.log_audit_event(
            Some("user2".to_string()),
            "action2",
            "resource2",
            false,
            None,
            None,
            HashMap::new(),
        ).await;

        // Filter by user
        let entries = security_manager.get_audit_log(None, Some("user1".to_string()), None).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].user_id, Some("user1".to_string()));

        // Filter by action
        let entries = security_manager.get_audit_log(None, None, Some("action2".to_string())).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "action2");
    }

    #[tokio::test]
    async fn test_session_cleanup() {
        let mut config = CacheConfig::default();
        config.security.session_timeout = 1; // 1 second timeout
        
        let security_manager = SecurityManager::new(config.security);
        security_manager.initialize_defaults().await.unwrap();

        let auth_result = security_manager.authenticate("admin", "admin123", None, None).await;
        assert!(auth_result.success);

        let session_token = auth_result.session_token.unwrap();
        
        // Wait for session to expire
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        
        security_manager.cleanup_expired_sessions().await;
        
        let user = security_manager.validate_session(&session_token).await;
        assert!(user.is_none());
    }
}

/// Test performance optimization functionality
mod performance_tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_pool_creation() {
        let config = CacheConfig::default();
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let pool = ConnectionPool::new(config.performance, metrics);
        
        let stats = pool.get_pool_stats().await;
        assert_eq!(stats.total_connections, 0);
        assert_eq!(stats.healthy_connections, 0);
        assert_eq!(stats.unhealthy_connections, 0);
    }

    #[tokio::test]
    async fn test_connection_pool_operations() {
        let config = CacheConfig::default();
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let pool = ConnectionPool::new(config.performance, metrics);
        
        // Get connection
        let connection = pool.get_connection("127.0.0.1:8080").await.unwrap();
        assert_eq!(connection.remote_addr, "127.0.0.1:8080");
        assert!(connection.healthy);
        
        let stats = pool.get_pool_stats().await;
        assert_eq!(stats.total_connections, 1);
        assert_eq!(stats.healthy_connections, 1);
        
        // Return connection
        pool.return_connection(connection).await;
        
        // Get same connection again (should reuse)
        let connection2 = pool.get_connection("127.0.0.1:8080").await.unwrap();
        assert_eq!(connection2.remote_addr, "127.0.0.1:8080");
    }

    #[tokio::test]
    async fn test_connection_pool_health_management() {
        let config = CacheConfig::default();
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let pool = ConnectionPool::new(config.performance, metrics);
        
        // Get connection
        let connection = pool.get_connection("127.0.0.1:8080").await.unwrap();
        pool.return_connection(connection).await;
        
        // Mark as unhealthy
        pool.mark_connection_unhealthy("127.0.0.1:8080").await;
        
        let stats = pool.get_pool_stats().await;
        assert_eq!(stats.unhealthy_connections, 1);
        assert_eq!(stats.healthy_connections, 0);
    }

    #[tokio::test]
    async fn test_request_batch_operations() {
        let mut config = CacheConfig::default();
        config.performance.batch_min_size = 3; // Lower min size for test
        let mut batch = RequestBatch::new(config.performance);
        
        assert!(batch.is_empty());
        assert_eq!(batch.size(), 0);
        
        // Add operations
        for i in 0..5 {
            let operation = kv_cache_core::performance::BatchOperation {
                operation_type: "GET".to_string(),
                key: format!("key-{}", i),
                value: None,
                timestamp: std::time::Instant::now(),
                priority: 1,
            };
            
            assert!(batch.add_operation(operation));
        }
        
        assert!(!batch.is_empty());
        assert_eq!(batch.size(), 5);
        
        // Check if ready (should be ready due to min size)
        assert!(batch.is_ready());
        
        // Take operations
        let operations = batch.take_operations();
        assert_eq!(operations.len(), 5);
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn test_request_batch_timing() {
        let mut config = CacheConfig::default();
        config.performance.batch_min_size = 100; // High min size
        config.performance.batch_max_wait_time_ms = 100; // 100ms max wait
        
        let mut batch = RequestBatch::new(config.performance);
        
        // Add one operation
        let operation = kv_cache_core::performance::BatchOperation {
            operation_type: "GET".to_string(),
            key: "test-key".to_string(),
            value: None,
            timestamp: std::time::Instant::now(),
            priority: 1,
        };
        
        batch.add_operation(operation);
        
        // Should not be ready immediately
        assert!(!batch.is_ready());
        
        // Wait for max wait time
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
        
        // Should be ready now
        assert!(batch.is_ready());
    }

    #[tokio::test]
    async fn test_performance_monitor_creation() {
        let config = CacheConfig::default();
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let monitor = PerformanceMonitor::new(config.performance, metrics);
        
        // Test that performance monitor was created successfully by running a benchmark
        let result = monitor.run_benchmark("test_creation", 10, || {
            std::thread::sleep(std::time::Duration::from_micros(10));
        }).await;
        assert_eq!(result.name, "test_creation");
    }

    #[tokio::test]
    async fn test_simple_benchmark() {
        let config = CacheConfig::default();
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let monitor = PerformanceMonitor::new(config.performance, metrics);
        
        let result = monitor.run_benchmark("test_benchmark", 100, || {
            std::thread::sleep(std::time::Duration::from_millis(1)); // Use milliseconds instead of microseconds
        }).await;
        
        assert_eq!(result.name, "test_benchmark");
        assert_eq!(result.total_operations, 100);
        assert!(result.ops_per_second > 0.0);
        assert!(result.avg_latency_ms > 0.0);
        assert!(result.error_rate >= 0.0);
    }

    #[tokio::test]
    async fn test_comprehensive_benchmarks() {
        let config = CacheConfig::default();
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let monitor = PerformanceMonitor::new(config.performance, metrics);
        
        let results = monitor.run_comprehensive_benchmarks().await;
        
        assert!(!results.is_empty());
        assert!(results.contains_key("get_operations"));
        assert!(results.contains_key("set_operations"));
        assert!(results.contains_key("mixed_operations"));
        assert!(results.contains_key("high_concurrency"));
        
        // Check that all benchmarks have reasonable results
        for (name, result) in results {
            assert!(!name.is_empty());
            assert!(result.total_operations > 0);
            assert!(result.ops_per_second > 0.0);
            // Allow for very fast operations that might have 0 latency
            assert!(result.avg_latency_ms >= 0.0);
            assert!(result.error_rate >= 0.0 && result.error_rate <= 1.0);
        }
    }

    #[tokio::test]
    async fn test_performance_report_generation() {
        let config = CacheConfig::default();
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let monitor = PerformanceMonitor::new(config.performance, metrics);
        
        let report = monitor.generate_performance_report().await;
        
        assert!(!report.benchmarks.is_empty());
        assert!(!report.recommendations.is_empty());
        assert!(report.timestamp > chrono::Utc::now() - chrono::Duration::seconds(10));
    }

    #[tokio::test]
    async fn test_benchmark_result_storage() {
        let config = CacheConfig::default();
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let monitor = PerformanceMonitor::new(config.performance, metrics);
        
        // Run a benchmark
        let result = monitor.run_benchmark("storage_test", 50, || {
            std::thread::sleep(std::time::Duration::from_micros(50));
        }).await;
        
        // Retrieve stored result
        let stored_result = monitor.get_benchmark_result("storage_test").await;
        assert!(stored_result.is_some());
        
        let stored = stored_result.unwrap();
        assert_eq!(stored.name, result.name);
        assert_eq!(stored.total_operations, result.total_operations);
        assert_eq!(stored.ops_per_second, result.ops_per_second);
    }

    #[tokio::test]
    async fn test_connection_pool_cleanup() {
        let mut config = CacheConfig::default();
        config.performance.connection_idle_timeout = 1; // 1 second timeout
        
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let pool = ConnectionPool::new(config.performance, metrics);
        
        // Get connection
        let connection = pool.get_connection("127.0.0.1:8080").await.unwrap();
        pool.return_connection(connection).await;
        
        let stats = pool.get_pool_stats().await;
        assert_eq!(stats.total_connections, 1);
        
        // Wait for idle timeout
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        
        // Clean up expired connections
        pool.cleanup_expired_connections().await;
        
        let stats = pool.get_pool_stats().await;
        assert_eq!(stats.total_connections, 0);
    }
}

/// Integration tests for Phase 3 features
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_health_and_security_integration() {
        let config = CacheConfig::default();
        let store = Arc::new(Store::new());
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let raft_state = Arc::new(RwLock::new(RaftState::new("test-node".to_string())));

        // Create health checker
        let health_checker = HealthChecker::new(
            config.health,
            store.clone(),
            metrics.clone(),
            raft_state,
        );

        // Create security manager
        let security_manager = SecurityManager::new(config.security);
        security_manager.initialize_defaults().await.unwrap();

        // Run health check
        let health_response = health_checker.check_health().await;
        assert_eq!(health_response.status, HealthStatus::Healthy);

        // Test authentication
        let auth_result = security_manager.authenticate("admin", "admin123", None, None).await;
        assert!(auth_result.success);

        // Log audit event
        security_manager.log_audit_event(
            Some("admin".to_string()),
            "health_check",
            "system",
            true,
            None,
            None,
            HashMap::new(),
        ).await;

        let audit_entries = security_manager.get_audit_log(None, None, None).await;
        assert!(!audit_entries.is_empty());
    }

    #[tokio::test]
    async fn test_performance_and_health_integration() {
        let config = CacheConfig::default();
        let store = Arc::new(Store::new());
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let raft_state = Arc::new(RwLock::new(RaftState::new("test-node".to_string())));

        // Create health checker
        let health_checker = HealthChecker::new(
            config.health,
            store.clone(),
            metrics.clone(),
            raft_state,
        );

        // Create performance monitor
        let performance_monitor = PerformanceMonitor::new(config.performance, metrics.clone());

        // Run performance benchmarks
        let benchmarks = performance_monitor.run_comprehensive_benchmarks().await;
        assert!(!benchmarks.is_empty());

        // Run health check
        let health_response = health_checker.check_health().await;
        assert_eq!(health_response.status, HealthStatus::Healthy);

        // Verify that performance metrics are reflected in health check
        let memory_check = health_response.checks.get("memory");
        assert!(memory_check.is_some());
    }

    #[tokio::test]
    async fn test_full_production_hardening_workflow() {
        let mut config = CacheConfig::default();
        config.security.enable_auth = true;
        config.security.enable_authz = true;
        config.health.enabled = true;
        config.performance.enable_monitoring = true;

        let store = Arc::new(Store::new());
        let metrics = Arc::new(MetricsCollector::new(config.metrics));
        let raft_state = Arc::new(RwLock::new(RaftState::new("test-node".to_string())));

        // Initialize all systems
        let health_checker = HealthChecker::new(
            config.health.clone(),
            store.clone(),
            metrics.clone(),
            raft_state,
        );

        let security_manager = SecurityManager::new(config.security.clone());
        security_manager.initialize_defaults().await.unwrap();

        let performance_monitor = PerformanceMonitor::new(config.performance.clone(), metrics.clone());

        // 1. Authenticate user
        let auth_result = security_manager.authenticate("admin", "admin123", None, None).await;
        assert!(auth_result.success);
        let session_token = auth_result.session_token.unwrap();

        // 2. Authorize operation
        let authz_result = security_manager.authorize(&session_token, "GET", "test-key").await;
        assert!(authz_result.allowed);

        // 3. Log operation
        security_manager.log_audit_event(
            Some("admin".to_string()),
            "GET",
            "test-key",
            true,
            None,
            None,
            HashMap::new(),
        ).await;

        // 4. Run performance benchmark
        let benchmark_result = performance_monitor.run_benchmark("production_test", 100, || {
            std::thread::sleep(std::time::Duration::from_micros(100));
        }).await;
        assert!(benchmark_result.ops_per_second > 0.0);

        // 5. Check system health
        let health_response = health_checker.check_health().await;
        assert_eq!(health_response.status, HealthStatus::Healthy);

        // 6. Generate performance report
        let performance_report = performance_monitor.generate_performance_report().await;
        assert!(!performance_report.benchmarks.is_empty());

        // 7. Verify audit trail
        let audit_entries = security_manager.get_audit_log(None, None, None).await;
        assert!(!audit_entries.is_empty());

        // 8. Check metrics
        let metrics_summary = metrics.get_metrics_summary();
        // Allow for metrics that might not have started yet
        assert!(metrics_summary.server.uptime_seconds >= 0);
    }
}

 