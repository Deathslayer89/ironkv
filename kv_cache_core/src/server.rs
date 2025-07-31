use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, put, delete},
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    config::CacheConfig,
    health::{HealthChecker, HealthResponse},
    metrics::MetricsCollector,
    security::SecurityManager,
    store::{Store, StoreStats},
    consensus::state::RaftState,
    value::Value,
};

/// HTTP server for IRONKV
pub struct HttpServer {
    config: CacheConfig,
    store: Arc<Store>,
    metrics: Arc<MetricsCollector>,
    health_checker: Arc<HealthChecker>,
    security_manager: Arc<SecurityManager>,
    raft_state: Arc<RwLock<RaftState>>,
}

impl HttpServer {
    /// Create a new HTTP server
    pub fn new(
        config: CacheConfig,
        store: Arc<Store>,
        metrics: Arc<MetricsCollector>,
        health_checker: Arc<HealthChecker>,
        security_manager: Arc<SecurityManager>,
        raft_state: Arc<RwLock<RaftState>>,
    ) -> Self {
        Self {
            config,
            store,
            metrics,
            health_checker,
            security_manager,
            raft_state,
        }
    }

    /// Start the HTTP server
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let app = self.create_router();

        let addr = format!("{}:{}", self.config.server.bind_address, self.config.server.port)
            .parse()?;

        println!("🚀 IRONKV HTTP server starting on {}", addr);

        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await?;

        Ok(())
    }

    /// Create the router with all endpoints
    pub fn create_router(&self) -> Router {
        let state = Arc::new(self.clone_state());

        Router::new()
            // Health check endpoints
            .route("/health", get(health_check_handler))
            .route("/ready", get(ready_check_handler))
            
            // Metrics endpoint
            .route("/metrics", get(metrics_handler))
            
            // Key-value store endpoints
            .route("/kv/:key", get(get_key_handler))
            .route("/kv/:key", put(set_key_handler))
            .route("/kv/:key", delete(delete_key_handler))
            
            // Cluster endpoints
            .route("/cluster/status", get(cluster_status_handler))
            .route("/cluster/nodes", get(cluster_nodes_handler))
            
            // Admin endpoints
            .route("/admin/stats", get(admin_stats_handler))
            .route("/admin/audit", get(admin_audit_handler))
            
            .with_state(state)
    }

    /// Clone the server state for handlers
    fn clone_state(&self) -> ServerState {
        ServerState {
            store: self.store.clone(),
            metrics: self.metrics.clone(),
            health_checker: self.health_checker.clone(),
            security_manager: self.security_manager.clone(),
            raft_state: self.raft_state.clone(),
            config: self.config.clone(),
        }
    }
}

/// Server state shared across handlers
#[derive(Clone)]
struct ServerState {
    store: Arc<Store>,
    metrics: Arc<MetricsCollector>,
    health_checker: Arc<HealthChecker>,
    security_manager: Arc<SecurityManager>,
    raft_state: Arc<RwLock<RaftState>>,
    config: CacheConfig,
}

// Request/Response types
#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct LoginResponse {
    success: bool,
    token: Option<String>,
    message: String,
}

#[derive(Serialize)]
struct LogoutResponse {
    success: bool,
    message: String,
}

#[derive(Deserialize)]
struct SetKeyRequest {
    value: Vec<u8>,
    ttl: Option<u64>,
}

#[derive(Serialize)]
struct KeyValueResponse {
    success: bool,
    value: Option<Vec<u8>>,
    message: String,
}

#[derive(Serialize)]
struct ClusterStatusResponse {
    node_id: String,
    role: String,
    term: u64,
    committed_index: u64,
    last_applied: u64,
    leader_id: Option<String>,
}

#[derive(Serialize)]
struct ClusterNodesResponse {
    nodes: HashMap<String, String>,
    total_nodes: usize,
}

#[derive(Serialize)]
struct AdminStatsResponse {
    total_keys: usize,
    memory_usage_bytes: u64,
    uptime_seconds: u64,
}

#[derive(Serialize)]
struct AdminAuditResponse {
    entries: Vec<crate::security::AuditLogEntry>,
    total_entries: usize,
}

// Handler functions
async fn health_check_handler(
    State(state): State<Arc<ServerState>>,
) -> Json<HealthResponse> {
    let health_response = state.health_checker.check_health().await;
    Json(health_response)
}

async fn ready_check_handler(
    State(state): State<Arc<ServerState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    let status = state.health_checker.quick_health_check().await;
    
    let response = serde_json::json!({
        "status": match status {
            crate::health::HealthStatus::Healthy => "ready",
            crate::health::HealthStatus::Degraded => "degraded",
            crate::health::HealthStatus::Unhealthy => "not_ready",
        },
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    let status_code = match status {
        crate::health::HealthStatus::Healthy => StatusCode::OK,
        crate::health::HealthStatus::Degraded => StatusCode::OK,
        crate::health::HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };

    (status_code, Json(response))
}

async fn metrics_handler(
    State(state): State<Arc<ServerState>>,
) -> (StatusCode, String) {
    let metrics_summary = state.metrics.get_metrics_summary();
    let prometheus_metrics = format!(
        "# HELP ironkv_server_uptime_seconds Server uptime in seconds\n\
         # TYPE ironkv_server_uptime_seconds counter\n\
         ironkv_server_uptime_seconds {}\n\
         \n\
         # HELP ironkv_active_connections Active connections\n\
         # TYPE ironkv_active_connections gauge\n\
         ironkv_active_connections {}\n\
         \n\
         # HELP ironkv_total_connections Total connections\n\
         # TYPE ironkv_total_connections counter\n\
         ironkv_total_connections {}\n",
        metrics_summary.server.uptime_seconds,
        metrics_summary.server.active_connections,
        metrics_summary.server.total_connections,
    );

    (StatusCode::OK, prometheus_metrics)
}

async fn login_handler(
    State(state): State<Arc<ServerState>>,
    Json(login_request): Json<LoginRequest>,
) -> (StatusCode, Json<LoginResponse>) {
    let auth_result = state
        .security_manager
        .authenticate(&login_request.username, &login_request.password, None, None)
        .await;

    if auth_result.success {
        let response = LoginResponse {
            success: true,
            token: auth_result.session_token,
            message: "Login successful".to_string(),
        };
        (StatusCode::OK, Json(response))
    } else {
        let response = LoginResponse {
            success: false,
            token: None,
            message: "Invalid credentials".to_string(),
        };
        (StatusCode::UNAUTHORIZED, Json(response))
    }
}

async fn logout_handler(
    State(_state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> (StatusCode, Json<LogoutResponse>) {
    if let Some(auth_header) = headers.get("Authorization") {
        if let Ok(token) = auth_header.to_str() {
            if token.starts_with("Bearer ") {
                let _token = token[7..].to_string();
                // In a real implementation, you'd invalidate the session
                let response = LogoutResponse {
                    success: true,
                    message: "Logout successful".to_string(),
                };
                return (StatusCode::OK, Json(response));
            }
        }
    }

    let response = LogoutResponse {
        success: false,
        message: "Invalid token".to_string(),
    };
    (StatusCode::BAD_REQUEST, Json(response))
}

async fn get_key_handler(
    State(state): State<Arc<ServerState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> (StatusCode, Json<KeyValueResponse>) {
    // Check authentication if enabled
    if state.config.security.enable_auth {
        if let Err(_) = authenticate_request(&state, &headers).await {
            let response = KeyValueResponse {
                success: false,
                value: None,
                message: "Unauthorized".to_string(),
            };
            return (StatusCode::UNAUTHORIZED, Json(response));
        }
    }

    match state.store.get(&key).await {
        Some(value) => {
            // Convert Value enum to bytes for response
            let value_bytes = match &value {
                Value::String(s) => s.as_bytes().to_vec(),
                Value::List(list) => serde_json::to_vec(&list).unwrap_or_default(),
                Value::Hash(hash) => serde_json::to_vec(&hash).unwrap_or_default(),
                Value::Set(set) => serde_json::to_vec(&set).unwrap_or_default(),
            };
            
            let response = KeyValueResponse {
                success: true,
                value: Some(value_bytes),
                message: "Key found".to_string(),
            };
            (StatusCode::OK, Json(response))
        }
        None => {
            let response = KeyValueResponse {
                success: false,
                value: None,
                message: "Key not found".to_string(),
            };
            (StatusCode::NOT_FOUND, Json(response))
        }
    }
}

async fn set_key_handler(
    State(state): State<Arc<ServerState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
    Json(set_request): Json<SetKeyRequest>,
) -> (StatusCode, Json<KeyValueResponse>) {
    // Check authentication if enabled
    if state.config.security.enable_auth {
        if let Err(_) = authenticate_request(&state, &headers).await {
            let response = KeyValueResponse {
                success: false,
                value: None,
                message: "Unauthorized".to_string(),
            };
            return (StatusCode::UNAUTHORIZED, Json(response));
        }
    }

    // Convert bytes to string for storage (simplified approach)
    let value_string = String::from_utf8(set_request.value.clone()).unwrap_or_default();
    let value = Value::String(value_string);
    state.store.set(key, value).await;
    
    let response = KeyValueResponse {
        success: true,
        value: Some(set_request.value),
        message: "Key set successfully".to_string(),
    };
    (StatusCode::OK, Json(response))
}

async fn delete_key_handler(
    State(state): State<Arc<ServerState>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> (StatusCode, Json<KeyValueResponse>) {
    // Check authentication if enabled
    if state.config.security.enable_auth {
        if let Err(_) = authenticate_request(&state, &headers).await {
            let response = KeyValueResponse {
                success: false,
                value: None,
                message: "Unauthorized".to_string(),
            };
            return (StatusCode::UNAUTHORIZED, Json(response));
        }
    }

    let deleted = state.store.delete(&key).await;
    if deleted {
        let response = KeyValueResponse {
            success: true,
            value: None,
            message: "Key deleted successfully".to_string(),
        };
        (StatusCode::OK, Json(response))
    } else {
        let response = KeyValueResponse {
            success: false,
            value: None,
            message: "Key not found".to_string(),
        };
        (StatusCode::NOT_FOUND, Json(response))
    }
}

async fn cluster_status_handler(
    State(state): State<Arc<ServerState>>,
) -> Json<ClusterStatusResponse> {
    let raft_state = state.raft_state.read().await;
    let response = ClusterStatusResponse {
        node_id: raft_state.node_id.clone(),
        role: format!("{:?}", raft_state.role),
        term: raft_state.current_term.0,
        committed_index: raft_state.commit_index.0,
        last_applied: raft_state.last_applied.0,
        leader_id: raft_state.leader_id.clone(),
    };
    Json(response)
}

async fn cluster_nodes_handler(
    State(state): State<Arc<ServerState>>,
) -> Json<ClusterNodesResponse> {
    let response = ClusterNodesResponse {
        nodes: state.config.cluster.members.clone(),
        total_nodes: state.config.cluster.members.len(),
    };
    Json(response)
}

async fn admin_stats_handler(
    State(state): State<Arc<ServerState>>,
) -> Json<AdminStatsResponse> {
    let store_stats = state.store.get_stats().await.unwrap_or(StoreStats {
        total_keys: 0,
        memory_usage_bytes: 0,
    });
    let response = AdminStatsResponse {
        total_keys: store_stats.total_keys,
        memory_usage_bytes: store_stats.memory_usage_bytes,
        uptime_seconds: state.metrics.get_metrics_summary().server.uptime_seconds,
    };
    Json(response)
}

async fn admin_audit_handler(
    State(state): State<Arc<ServerState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<AdminAuditResponse> {
    let limit = params.get("limit").and_then(|s| s.parse::<usize>().ok()).map(|l| l.to_string());
    let audit_entries = state.security_manager.get_audit_log(None, None, limit).await;
    
    let total_entries = audit_entries.len();
    let response = AdminAuditResponse {
        entries: audit_entries,
        total_entries,
    };
    Json(response)
}

// Helper function for authentication
async fn authenticate_request(
    state: &ServerState,
    headers: &HeaderMap,
) -> Result<(), ()> {
    if let Some(auth_header) = headers.get("Authorization") {
        if let Ok(token) = auth_header.to_str() {
            if token.starts_with("Bearer ") {
                let token = token[7..].to_string();
                let authz_result = state
                    .security_manager
                    .authorize(&token, "GET", "any")
                    .await;
                if authz_result.allowed {
                    return Ok(());
                }
            }
        }
    }
    Err(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CacheConfig;

    #[tokio::test]
    async fn test_server_creation() {
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

        let server = HttpServer::new(
            config,
            store,
            metrics,
            health_checker,
            security_manager,
            raft_state,
        );

        // Test that server was created successfully
        assert_eq!(server.config.server.port, 6379);
    }

    #[tokio::test]
    async fn test_router_creation() {
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

        let server = HttpServer::new(
            config,
            store,
            metrics,
            health_checker,
            security_manager,
            raft_state,
        );

        // Test that router can be created without errors
        let _router = server.create_router();
        // If we get here, the router was created successfully
    }
} 