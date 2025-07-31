use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::Response,
};
use kv_cache_core::{
    config::CacheConfig,
    health::HealthChecker,
    metrics::MetricsCollector,
    security::SecurityManager,
    server::HttpServer,
    store::Store,
    consensus::state::RaftState,
};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

/// Test helper to create a test server
async fn create_test_server() -> HttpServer {
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

    HttpServer::new(
        config,
        store,
        metrics,
        health_checker,
        security_manager,
        raft_state,
    )
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
async fn test_health_endpoint() {
    let server = create_test_server().await;
    let app = server.create_router();

    let response = make_request(app, "GET", "/health", None).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let health_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(health_response["service"], "kv-cache");
    assert!(health_response["status"].is_string());
    assert!(health_response["checks"].is_object());
    assert!(health_response["summary"].is_object());
}

#[tokio::test]
async fn test_ready_endpoint() {
    let server = create_test_server().await;
    let app = server.create_router();

    let response = make_request(app, "GET", "/ready", None).await;

    // Should return 200 OK for ready status
    assert_eq!(response.status(), StatusCode::OK);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let ready_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(ready_response["status"].is_string());
    assert!(ready_response["timestamp"].is_string());
    
    let status = ready_response["status"].as_str().unwrap();
    assert!(matches!(status, "ready" | "degraded" | "not_ready"));
}

#[tokio::test]
async fn test_metrics_endpoint() {
    let server = create_test_server().await;
    let app = server.create_router();

    let response = make_request(app, "GET", "/metrics", None).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let metrics_text = String::from_utf8(body.to_vec()).unwrap();

    // Check that it's in Prometheus format
    assert!(metrics_text.contains("# HELP"));
    assert!(metrics_text.contains("# TYPE"));
    assert!(metrics_text.contains("ironkv_server_uptime_seconds"));
    assert!(metrics_text.contains("ironkv_active_connections"));
    assert!(metrics_text.contains("ironkv_total_connections"));
}

#[tokio::test]
async fn test_get_key_not_found() {
    let server = create_test_server().await;
    let app = server.create_router();

    let response = make_request(app, "GET", "/kv/nonexistent-key", None).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let kv_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(kv_response["success"], false);
    assert_eq!(kv_response["message"], "Key not found");
    assert!(kv_response["value"].is_null());
}

#[tokio::test]
async fn test_set_and_get_key() {
    let server = create_test_server().await;
    let app = server.create_router();

    let test_key = "test-key-123";
    let test_value = "Hello, IRONKV!";
    let set_body = json!({
        "value": test_value.as_bytes().to_vec(),
        "ttl": null
    }).to_string();

    // Set the key
    let set_response = make_request(
        app.clone(),
        "PUT",
        &format!("/kv/{}", test_key),
        Some(set_body)
    ).await;

    assert_eq!(set_response.status(), StatusCode::OK);

    let set_body = hyper::body::to_bytes(set_response.into_body()).await.unwrap();
    let set_kv_response: serde_json::Value = serde_json::from_slice(&set_body).unwrap();

    assert_eq!(set_kv_response["success"], true);
    assert_eq!(set_kv_response["message"], "Key set successfully");

    // Get the key
    let get_response = make_request(app, "GET", &format!("/kv/{}", test_key), None).await;

    assert_eq!(get_response.status(), StatusCode::OK);

    let get_body = hyper::body::to_bytes(get_response.into_body()).await.unwrap();
    let get_kv_response: serde_json::Value = serde_json::from_slice(&get_body).unwrap();

    assert_eq!(get_kv_response["success"], true);
    assert_eq!(get_kv_response["message"], "Key found");
    
    // Verify the value (it will be base64 encoded bytes)
    let value_bytes = get_kv_response["value"].as_array().unwrap();
    let value_string = String::from_utf8(
        value_bytes.iter()
            .map(|v| v.as_u64().unwrap() as u8)
            .collect::<Vec<u8>>()
    ).unwrap();
    assert_eq!(value_string, test_value);
}

#[tokio::test]
async fn test_delete_key() {
    let server = create_test_server().await;
    let app = server.create_router();

    let test_key = "delete-test-key";
    let test_value = "Value to delete";
    let set_body = json!({
        "value": test_value.as_bytes().to_vec(),
        "ttl": null
    }).to_string();

    // Set the key first
    let _set_response = make_request(
        app.clone(),
        "PUT",
        &format!("/kv/{}", test_key),
        Some(set_body)
    ).await;

    // Delete the key
    let delete_response = make_request(
        app.clone(),
        "DELETE",
        &format!("/kv/{}", test_key),
        None
    ).await;

    assert_eq!(delete_response.status(), StatusCode::OK);

    let delete_body = hyper::body::to_bytes(delete_response.into_body()).await.unwrap();
    let delete_kv_response: serde_json::Value = serde_json::from_slice(&delete_body).unwrap();

    assert_eq!(delete_kv_response["success"], true);
    assert_eq!(delete_kv_response["message"], "Key deleted successfully");

    // Verify the key is actually deleted
    let get_response = make_request(app, "GET", &format!("/kv/{}", test_key), None).await;
    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_nonexistent_key() {
    let server = create_test_server().await;
    let app = server.create_router();

    let response = make_request(app, "DELETE", "/kv/nonexistent-key", None).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let kv_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(kv_response["success"], false);
    assert_eq!(kv_response["message"], "Key not found");
}

#[tokio::test]
async fn test_cluster_status_endpoint() {
    let server = create_test_server().await;
    let app = server.create_router();

    let response = make_request(app, "GET", "/cluster/status", None).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let cluster_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(cluster_response["node_id"], "test-node");
    assert!(cluster_response["role"].is_string());
    assert!(cluster_response["term"].is_number());
    assert!(cluster_response["committed_index"].is_number());
    assert!(cluster_response["last_applied"].is_number());
    assert!(cluster_response["leader_id"].is_null() || cluster_response["leader_id"].is_string());
}

#[tokio::test]
async fn test_cluster_nodes_endpoint() {
    let server = create_test_server().await;
    let app = server.create_router();

    let response = make_request(app, "GET", "/cluster/nodes", None).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let nodes_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(nodes_response["nodes"].is_object());
    assert!(nodes_response["total_nodes"].is_number());
    assert_eq!(nodes_response["total_nodes"], 0); // Default config has no nodes
}

#[tokio::test]
async fn test_admin_stats_endpoint() {
    let server = create_test_server().await;
    let app = server.create_router();

    let response = make_request(app, "GET", "/admin/stats", None).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let stats_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(stats_response["total_keys"].is_number());
    assert!(stats_response["memory_usage_bytes"].is_number());
    assert!(stats_response["uptime_seconds"].is_number());
    
    // Should start with 0 keys
    assert_eq!(stats_response["total_keys"], 0);
}

#[tokio::test]
async fn test_admin_audit_endpoint() {
    let server = create_test_server().await;
    let app = server.create_router();

    let response = make_request(app, "GET", "/admin/audit", None).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let audit_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(audit_response["entries"].is_array());
    assert!(audit_response["total_entries"].is_number());
    
    // Should start with 0 audit entries
    assert_eq!(audit_response["total_entries"], 0);
}

#[tokio::test]
async fn test_admin_audit_with_limit() {
    let server = create_test_server().await;
    let app = server.create_router();

    let response = make_request(app, "GET", "/admin/audit?limit=10", None).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let audit_response: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(audit_response["entries"].is_array());
    assert!(audit_response["total_entries"].is_number());
}

#[tokio::test]
async fn test_invalid_endpoint() {
    let server = create_test_server().await;
    let app = server.create_router();

    let response = make_request(app, "GET", "/invalid/endpoint", None).await;

    // Should return 404 for invalid endpoints
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_multiple_key_operations() {
    let server = create_test_server().await;
    let app = server.create_router();

    // Test multiple keys
    let keys = vec!["key1", "key2", "key3"];
    let values = vec!["value1", "value2", "value3"];

    // Set multiple keys
    for (key, value) in keys.iter().zip(values.iter()) {
        let set_body = json!({
            "value": value.as_bytes().to_vec(),
            "ttl": null
        }).to_string();

        let set_response = make_request(
            app.clone(),
            "PUT",
            &format!("/kv/{}", key),
            Some(set_body)
        ).await;

        assert_eq!(set_response.status(), StatusCode::OK);
    }

    // Verify all keys exist
    for (key, expected_value) in keys.iter().zip(values.iter()) {
        let get_response = make_request(app.clone(), "GET", &format!("/kv/{}", key), None).await;
        assert_eq!(get_response.status(), StatusCode::OK);

        let get_body = hyper::body::to_bytes(get_response.into_body()).await.unwrap();
        let get_kv_response: serde_json::Value = serde_json::from_slice(&get_body).unwrap();

        assert_eq!(get_kv_response["success"], true);
        
        // Verify the value
        let value_bytes = get_kv_response["value"].as_array().unwrap();
        let value_string = String::from_utf8(
            value_bytes.iter()
                .map(|v| v.as_u64().unwrap() as u8)
                .collect::<Vec<u8>>()
        ).unwrap();
        assert_eq!(value_string, *expected_value);
    }

    // Check admin stats reflect the changes
    let stats_response = make_request(app.clone(), "GET", "/admin/stats", None).await;
    assert_eq!(stats_response.status(), StatusCode::OK);

    let stats_body = hyper::body::to_bytes(stats_response.into_body()).await.unwrap();
    let stats: serde_json::Value = serde_json::from_slice(&stats_body).unwrap();

    assert_eq!(stats["total_keys"], 3);
}

#[tokio::test]
async fn test_concurrent_requests() {
    let server = create_test_server().await;
    let app = server.create_router();

    // Test concurrent health checks
    let mut handles = vec![];
    
    for i in 0..10 {
        let app_clone = app.clone();
        let handle = tokio::spawn(async move {
            let response = make_request(app_clone, "GET", "/health", None).await;
            (i, response.status())
        });
        handles.push(handle);
    }

    let results = futures::future::join_all(handles).await;
    
    for (i, result) in results.into_iter().enumerate() {
        let (id, status) = result.unwrap();
        assert_eq!(status, StatusCode::OK, "Request {} failed with status {}", i, status);
    }
} 