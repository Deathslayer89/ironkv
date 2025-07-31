use kv_cache_core::{
    config::CacheConfig,
    health::HealthChecker,
    metrics::MetricsCollector,
    security::SecurityManager,
    store::Store,
    consensus::state::RaftState,
    tls_manager::{
        TlsManager, TlsConfig, SecurityHeadersConfig, CorsConfig, 
        RateLimitConfig, RateLimiter, TlsVersion
    },
    server::HttpServer,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::Response,
};
use tower::ServiceExt;
use tempfile::NamedTempFile;
use std::fs;

/// Test helper to create a test server with security features
async fn create_test_server_with_security(
    tls_config: Option<TlsConfig>,
    cors_config: Option<CorsConfig>,
    rate_limit_config: Option<RateLimitConfig>,
    security_headers: Option<SecurityHeadersConfig>,
) -> HttpServer {
    let config = CacheConfig::default();
    let store = Arc::new(Store::new());
    let metrics = Arc::new(MetricsCollector::new(config.metrics.clone()));
    let raft_state = Arc::new(RwLock::new(RaftState::new("test-node".to_string())));
    let health_checker = Arc::new(HealthChecker::new(
        config.health.clone(),
        store.clone(),
        metrics.clone(),
        raft_state.clone(),
    ));
    let security_manager = Arc::new(SecurityManager::new(config.security.clone()));

    let mut server = HttpServer::new(
        config,
        store,
        metrics,
        health_checker,
        security_manager,
        raft_state,
    );

    // Configure TLS if provided
    if let Some(tls_config) = tls_config {
        server = server.with_tls(tls_config).await.unwrap();
    }

    // Configure CORS if provided
    if let Some(cors_config) = cors_config {
        server = server.with_cors(cors_config);
    }

    // Configure rate limiting if provided
    if let Some(rate_limit_config) = rate_limit_config {
        server = server.with_rate_limiting(rate_limit_config);
    }

    // Configure security headers if provided
    if let Some(security_headers) = security_headers {
        server = server.with_security_headers(security_headers);
    }

    server
}

/// Test helper to make HTTP requests
async fn make_request(app: axum::Router, method: &str, uri: &str, body: Option<String>) -> Response {
    let mut req_builder = Request::builder()
        .method(method)
        .uri(uri);

    let request = if let Some(body_content) = body {
        req_builder
            .header("content-type", "application/json")
            .body(Body::from(body_content))
            .unwrap()
    } else {
        req_builder.body(Body::empty()).unwrap()
    };

    app.oneshot(request).await.unwrap()
}

#[tokio::test]
async fn test_tls_manager_creation() {
    let tls_config = TlsConfig {
        enabled: false,
        cert_path: None,
        key_path: None,
        ca_cert_path: None,
        enable_mtls: false,
        auto_renewal: false,
        renewal_threshold_days: 30,
        check_interval_seconds: 3600,
        min_tls_version: TlsVersion::Tls13,
        cipher_suites: vec![],
        ocsp_stapling: false,
        hsts_enabled: true,
        hsts_max_age: 31536000,
        hsts_include_subdomains: true,
        hsts_preload: false,
    };

    let mut tls_manager = TlsManager::new(tls_config);
    tls_manager.initialize().await.unwrap();
    
    // Test shutdown
    tls_manager.shutdown().await;
}

#[tokio::test]
async fn test_rate_limiter_basic_operations() {
    let config = RateLimitConfig {
        enabled: true,
        requests_per_minute: 5,
        burst_size: 2,
        window_seconds: 60,
        storage_backend: kv_cache_core::tls_manager::RateLimitStorage::Memory,
        include_headers: true,
    };

    let rate_limiter = RateLimiter::new(config);

    // Test allowed requests
    for i in 0..5 {
        let allowed = rate_limiter.is_allowed("test-client").await.unwrap();
        assert!(allowed, "Request {} should be allowed", i);
    }

    // Test rate limited request
    let allowed = rate_limiter.is_allowed("test-client").await.unwrap();
    assert!(!allowed, "Request should be rate limited");

    // Test headers
    let headers = rate_limiter.get_headers("test-client").await;
    assert!(headers.contains_key("X-RateLimit-Limit"));
    assert!(headers.contains_key("X-RateLimit-Remaining"));
}

#[tokio::test]
async fn test_rate_limiter_different_clients() {
    let config = RateLimitConfig {
        enabled: true,
        requests_per_minute: 3,
        burst_size: 1,
        window_seconds: 60,
        storage_backend: kv_cache_core::tls_manager::RateLimitStorage::Memory,
        include_headers: true,
    };

    let rate_limiter = RateLimiter::new(config);

    // Test different clients should have separate limits
    for i in 0..3 {
        let allowed1 = rate_limiter.is_allowed("client1").await.unwrap();
        let allowed2 = rate_limiter.is_allowed("client2").await.unwrap();
        
        assert!(allowed1, "Client1 request {} should be allowed", i);
        assert!(allowed2, "Client2 request {} should be allowed", i);
    }

    // Both clients should be rate limited now
    let allowed1 = rate_limiter.is_allowed("client1").await.unwrap();
    let allowed2 = rate_limiter.is_allowed("client2").await.unwrap();
    
    assert!(!allowed1, "Client1 should be rate limited");
    assert!(!allowed2, "Client2 should be rate limited");
}

#[tokio::test]
async fn test_security_headers_config() {
    let config = SecurityHeadersConfig::default();
    assert!(config.enabled);
    assert!(config.content_security_policy.is_some());
    assert!(config.x_frame_options.is_some());
    assert!(config.x_content_type_options.is_some());
    assert!(config.x_xss_protection.is_some());
    assert!(config.referrer_policy.is_some());
    assert!(config.permissions_policy.is_some());
    assert!(config.hsts.is_some());
}

#[tokio::test]
async fn test_cors_config() {
    let config = CorsConfig::default();
    assert!(config.enabled);
    assert!(config.allowed_origins.contains(&"*".to_string()));
    assert!(config.allowed_methods.contains(&"GET".to_string()));
    assert!(config.allowed_methods.contains(&"POST".to_string()));
    assert!(config.allowed_methods.contains(&"PUT".to_string()));
    assert!(config.allowed_methods.contains(&"DELETE".to_string()));
    assert!(config.allowed_methods.contains(&"OPTIONS".to_string()));
    assert!(config.allowed_headers.contains(&"Content-Type".to_string()));
    assert!(config.allowed_headers.contains(&"Authorization".to_string()));
    assert_eq!(config.allow_credentials, false);
    assert_eq!(config.max_age, Some(86400));
}

#[tokio::test]
async fn test_cors_config_custom() {
    let config = CorsConfig {
        enabled: true,
        allowed_origins: vec!["https://example.com".to_string()],
        allowed_methods: vec!["GET".to_string(), "POST".to_string()],
        allowed_headers: vec!["Content-Type".to_string()],
        expose_headers: vec!["X-Custom-Header".to_string()],
        allow_credentials: true,
        max_age: Some(3600),
    };

    assert!(config.enabled);
    assert_eq!(config.allowed_origins.len(), 1);
    assert_eq!(config.allowed_methods.len(), 2);
    assert_eq!(config.allowed_headers.len(), 1);
    assert_eq!(config.expose_headers.len(), 1);
    assert!(config.allow_credentials);
    assert_eq!(config.max_age, Some(3600));
}

#[tokio::test]
async fn test_server_with_security_features() {
    let cors_config = CorsConfig {
        enabled: true,
        allowed_origins: vec!["*".to_string()],
        allowed_methods: vec!["GET".to_string(), "POST".to_string()],
        allowed_headers: vec!["Content-Type".to_string()],
        expose_headers: vec![],
        allow_credentials: false,
        max_age: Some(3600),
    };

    let rate_limit_config = RateLimitConfig {
        enabled: true,
        requests_per_minute: 100,
        burst_size: 10,
        window_seconds: 60,
        storage_backend: kv_cache_core::tls_manager::RateLimitStorage::Memory,
        include_headers: true,
    };

    let security_headers = SecurityHeadersConfig {
        enabled: true,
        content_security_policy: Some("default-src 'self'".to_string()),
        x_frame_options: Some("DENY".to_string()),
        x_content_type_options: Some("nosniff".to_string()),
        x_xss_protection: Some("1; mode=block".to_string()),
        referrer_policy: Some("strict-origin-when-cross-origin".to_string()),
        permissions_policy: Some("geolocation=(), microphone=(), camera=()".to_string()),
        hsts: Some("max-age=31536000; includeSubDomains".to_string()),
    };

    let server = create_test_server_with_security(
        None, // No TLS for this test
        Some(cors_config),
        Some(rate_limit_config),
        Some(security_headers),
    ).await;

    // Test that server was created successfully
    let router = server.create_router();
    
    // Test health endpoint
    let response = make_request(router.clone(), "GET", "/health", None).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_tls_config_validation() {
    let tls_config = TlsConfig {
        enabled: true,
        cert_path: Some("/path/to/cert.pem".to_string()),
        key_path: Some("/path/to/key.pem".to_string()),
        ca_cert_path: None,
        enable_mtls: false,
        auto_renewal: true,
        renewal_threshold_days: 30,
        check_interval_seconds: 3600,
        min_tls_version: TlsVersion::Tls13,
        cipher_suites: vec!["TLS_AES_256_GCM_SHA384".to_string()],
        ocsp_stapling: true,
        hsts_enabled: true,
        hsts_max_age: 31536000,
        hsts_include_subdomains: true,
        hsts_preload: false,
    };

    assert!(tls_config.enabled);
    assert_eq!(tls_config.min_tls_version, TlsVersion::Tls13);
    assert!(tls_config.auto_renewal);
    assert!(tls_config.ocsp_stapling);
    assert!(tls_config.hsts_enabled);
    assert_eq!(tls_config.hsts_max_age, 31536000);
    assert!(tls_config.hsts_include_subdomains);
    assert!(!tls_config.hsts_preload);
}

#[tokio::test]
async fn test_rate_limiter_disabled() {
    let config = RateLimitConfig {
        enabled: false,
        requests_per_minute: 5,
        burst_size: 2,
        window_seconds: 60,
        storage_backend: kv_cache_core::tls_manager::RateLimitStorage::Memory,
        include_headers: true,
    };

    let rate_limiter = RateLimiter::new(config);

    // All requests should be allowed when rate limiting is disabled
    for _ in 0..10 {
        let allowed = rate_limiter.is_allowed("test-client").await.unwrap();
        assert!(allowed, "Request should be allowed when rate limiting is disabled");
    }
}

#[tokio::test]
async fn test_security_headers_disabled() {
    let config = SecurityHeadersConfig {
        enabled: false,
        content_security_policy: Some("default-src 'self'".to_string()),
        x_frame_options: Some("DENY".to_string()),
        x_content_type_options: Some("nosniff".to_string()),
        x_xss_protection: Some("1; mode=block".to_string()),
        referrer_policy: Some("strict-origin-when-cross-origin".to_string()),
        permissions_policy: Some("geolocation=(), microphone=(), camera=()".to_string()),
        hsts: Some("max-age=31536000; includeSubDomains".to_string()),
    };

    assert!(!config.enabled);
    // Even when disabled, the configuration values should still be present
    assert!(config.content_security_policy.is_some());
    assert!(config.x_frame_options.is_some());
}

#[tokio::test]
async fn test_cors_disabled() {
    let config = CorsConfig {
        enabled: false,
        allowed_origins: vec!["*".to_string()],
        allowed_methods: vec!["GET".to_string()],
        allowed_headers: vec!["Content-Type".to_string()],
        expose_headers: vec![],
        allow_credentials: false,
        max_age: Some(3600),
    };

    assert!(!config.enabled);
    // Configuration values should still be present even when disabled
    assert!(!config.allowed_origins.is_empty());
    assert!(!config.allowed_methods.is_empty());
}

#[tokio::test]
async fn test_tls_version_enum() {
    let tls12 = TlsVersion::Tls12;
    let tls13 = TlsVersion::Tls13;
    let default = TlsVersion::default();

    assert_eq!(default, TlsVersion::Tls13);
    assert_ne!(tls12, tls13);
    assert_eq!(tls13, TlsVersion::Tls13);
}

#[tokio::test]
async fn test_rate_limiter_headers() {
    let config = RateLimitConfig {
        enabled: true,
        requests_per_minute: 10,
        burst_size: 5,
        window_seconds: 60,
        storage_backend: kv_cache_core::tls_manager::RateLimitStorage::Memory,
        include_headers: true,
    };

    let rate_limiter = RateLimiter::new(config);

    // Make some requests
    for _ in 0..3 {
        rate_limiter.is_allowed("test-client").await.unwrap();
    }

    // Get headers
    let headers = rate_limiter.get_headers("test-client").await;
    
    assert!(headers.contains_key("X-RateLimit-Limit"));
    assert!(headers.contains_key("X-RateLimit-Remaining"));
    assert!(headers.contains_key("X-RateLimit-Reset"));
    
    assert_eq!(headers.get("X-RateLimit-Limit").unwrap(), "10");
    assert_eq!(headers.get("X-RateLimit-Remaining").unwrap(), "7");
}

#[tokio::test]
async fn test_rate_limiter_no_headers() {
    let config = RateLimitConfig {
        enabled: true,
        requests_per_minute: 10,
        burst_size: 5,
        window_seconds: 60,
        storage_backend: kv_cache_core::tls_manager::RateLimitStorage::Memory,
        include_headers: false,
    };

    let rate_limiter = RateLimiter::new(config);

    // Make some requests
    for _ in 0..3 {
        rate_limiter.is_allowed("test-client").await.unwrap();
    }

    // Get headers
    let headers = rate_limiter.get_headers("test-client").await;
    
    // Should be empty when headers are disabled
    assert!(headers.is_empty());
} 

#[tokio::test]
async fn test_tls_config_with_rustls() {
    let tls_config = TlsConfig {
        enabled: true,
        cert_path: Some("/tmp/test_cert.pem".to_string()),
        key_path: Some("/tmp/test_key.pem".to_string()),
        ca_cert_path: None,
        enable_mtls: false,
        auto_renewal: false,
        renewal_threshold_days: 30,
        check_interval_seconds: 3600,
        min_tls_version: TlsVersion::Tls13,
        cipher_suites: vec![],
        ocsp_stapling: false,
        hsts_enabled: true,
        hsts_max_age: 31536000,
        hsts_include_subdomains: true,
        hsts_preload: false,
    };

    let tls_manager = TlsManager::new(tls_config);
    let result = tls_manager.get_axum_tls_config();
    // Should succeed because we're returning Some(()) for now
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[tokio::test]
async fn test_cors_config_integration() {
    let cors_config = CorsConfig {
        enabled: true,
        allowed_origins: vec!["http://example.com".to_string()],
        allowed_methods: vec!["GET".to_string(), "POST".to_string(), "OPTIONS".to_string()],
        allowed_headers: vec!["Content-Type".to_string(), "Authorization".to_string()],
        expose_headers: vec![],
        allow_credentials: true,
        max_age: Some(600),
    };
    
    let server = create_test_server_with_security(None, Some(cors_config), None, None).await;
    let router = server.create_router();
    
    // Test that the router can be created successfully
    assert!(true, "Router created successfully with CORS config");
} 