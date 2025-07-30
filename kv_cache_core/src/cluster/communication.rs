//! gRPC communication module for inter-node communication
//! 
//! This module implements the KvCacheService using tonic for high-performance
//! RPC communication between cache nodes.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{transport::Server, Request, Response, Status};
use crate::{TTLStore, Value};

// Include the generated protobuf code
pub mod kv_cache {
    tonic::include_proto!("kv_cache");
}

use kv_cache::kv_cache_service_server::{KvCacheService as KvCacheServiceTrait, KvCacheServiceServer};
use kv_cache::{
    Request as GrpcRequest, Response as GrpcResponse,
    ReplicateRequest, ReplicateResponse,
    HealthRequest, HealthResponse,
    MembershipRequest, MembershipResponse,
    JoinRequest, JoinResponse,
    LeaveRequest, LeaveResponse,
    NodeInfo, NodeStatus,
};

/// gRPC service implementation for inter-node communication
pub struct KvCacheService {
    store: Arc<TTLStore>,
    node_id: String,
    cluster_members: Arc<RwLock<HashMap<String, NodeInfo>>>,
    server_handle: Option<tokio::task::JoinHandle<()>>,
}

impl KvCacheService {
    pub fn new() -> Self {
        Self {
            store: Arc::new(TTLStore::new()),
            node_id: "node-1".to_string(),
            cluster_members: Arc::new(RwLock::new(HashMap::new())),
            server_handle: None,
        }
    }

    pub fn with_store(mut self, store: Arc<TTLStore>) -> Self {
        self.store = store;
        self
    }

    pub fn with_node_id(mut self, node_id: String) -> Self {
        self.node_id = node_id;
        self
    }

    /// Start the gRPC server
    pub async fn start(&mut self, bind_address: &str, bind_port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = format!("{}:{}", bind_address, bind_port).parse()?;
        let service = KvCacheServiceServer::new(self.clone());
        
        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve(addr)
                .await
                .unwrap();
        });
        
        self.server_handle = Some(handle);
        Ok(())
    }

    /// Stop the gRPC server
    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }
        Ok(())
    }
}

impl Clone for KvCacheService {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            node_id: self.node_id.clone(),
            cluster_members: Arc::clone(&self.cluster_members),
            server_handle: None,
        }
    }
}

#[tonic::async_trait]
impl KvCacheServiceTrait for KvCacheService {
    /// Forward a request to this node
    async fn forward_request(
        &self,
        request: Request<GrpcRequest>,
    ) -> Result<Response<GrpcResponse>, Status> {
        let req = request.into_inner();
        
        match req.operation.to_uppercase().as_str() {
            "GET" => {
                let key = req.key;
                match self.store.get(&key).await {
                    Some(value) => {
                        let data = match value {
                            Value::String(s) => s,
                            Value::List(l) => format!("{:?}", l),
                            Value::Hash(h) => format!("{:?}", h),
                            Value::Set(s) => format!("{:?}", s),
                        };
                        Ok(Response::new(GrpcResponse {
                            success: true,
                            data,
                            error: String::new(),
                        }))
                    }
                    None => Ok(Response::new(GrpcResponse {
                        success: true,
                        data: String::new(),
                        error: String::new(),
                    }))
                }
            }
            "SET" => {
                let key = req.key;
                let value = Value::String(req.value);
                
                if req.ttl_seconds > 0 {
                    self.store.set(key, value, Some(req.ttl_seconds as u64)).await;
                } else {
                    self.store.set(key, value, None).await;
                }
                
                Ok(Response::new(GrpcResponse {
                    success: true,
                    data: "OK".to_string(),
                    error: String::new(),
                }))
            }
            "DEL" => {
                let key = req.key;
                let deleted = self.store.delete(&key).await;
                
                Ok(Response::new(GrpcResponse {
                    success: true,
                    data: if deleted { "1".to_string() } else { "0".to_string() },
                    error: String::new(),
                }))
            }
            _ => Ok(Response::new(GrpcResponse {
                success: false,
                data: String::new(),
                error: format!("Unknown operation: {}", req.operation),
            }))
        }
    }

    /// Handle data replication from leader to follower
    async fn replicate_data(
        &self,
        request: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        let req = request.into_inner();
        
        match req.operation.to_uppercase().as_str() {
            "SET" => {
                let key = req.key;
                let value = Value::String(req.value);
                
                if req.ttl_seconds > 0 {
                    self.store.set(key, value, Some(req.ttl_seconds as u64)).await;
                } else {
                    self.store.set(key, value, None).await;
                }
                
                Ok(Response::new(ReplicateResponse {
                    success: true,
                    error: String::new(),
                }))
            }
            "DEL" => {
                let key = req.key;
                self.store.delete(&key).await;
                
                Ok(Response::new(ReplicateResponse {
                    success: true,
                    error: String::new(),
                }))
            }
            _ => Ok(Response::new(ReplicateResponse {
                success: false,
                error: format!("Unknown replication operation: {}", req.operation),
            }))
        }
    }

    /// Health check endpoint
    async fn health_check(
        &self,
        request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let req = request.into_inner();
        
        let mut metadata = HashMap::new();
        metadata.insert("node_id".to_string(), self.node_id.clone());
        metadata.insert("store_size".to_string(), self.store.len().await.to_string());
        
        Ok(Response::new(HealthResponse {
            healthy: true,
            node_id: self.node_id.clone(),
            timestamp: req.timestamp,
            metadata,
        }))
    }

    /// Get cluster membership information
    async fn get_membership(
        &self,
        _request: Request<MembershipRequest>,
    ) -> Result<Response<MembershipResponse>, Status> {
        let members = self.cluster_members.read().await;
        let nodes: Vec<NodeInfo> = members.values().cloned().collect();
        
        Ok(Response::new(MembershipResponse {
            nodes,
            cluster_version: 1, // TODO: Implement proper versioning
        }))
    }

    /// Handle node joining the cluster
    async fn join_cluster(
        &self,
        request: Request<JoinRequest>,
    ) -> Result<Response<JoinResponse>, Status> {
        let req = request.into_inner();
        
        let node_info = NodeInfo {
            node_id: req.node_id.clone(),
            address: req.address,
            port: req.port,
            status: NodeStatus::Joining as i32,
            last_seen: chrono::Utc::now().timestamp(),
        };
        
        {
            let mut members = self.cluster_members.write().await;
            members.insert(req.node_id.clone(), node_info.clone());
        }
        
        let members = self.cluster_members.read().await;
        let cluster_members: Vec<NodeInfo> = members.values().cloned().collect();
        
        Ok(Response::new(JoinResponse {
            accepted: true,
            error: String::new(),
            cluster_members,
        }))
    }

    /// Handle node leaving the cluster
    async fn leave_cluster(
        &self,
        request: Request<LeaveRequest>,
    ) -> Result<Response<LeaveResponse>, Status> {
        let req = request.into_inner();
        
        {
            let mut members = self.cluster_members.write().await;
            if let Some(node_info) = members.get_mut(&req.node_id) {
                node_info.status = NodeStatus::Leaving as i32;
            }
        }
        
        // TODO: Implement graceful data migration if req.graceful is true
        
        Ok(Response::new(LeaveResponse {
            accepted: true,
            error: String::new(),
        }))
    }
}

/// gRPC client for communicating with other nodes
#[derive(Clone)]
pub struct KvCacheClient {
    client: kv_cache::kv_cache_service_client::KvCacheServiceClient<tonic::transport::Channel>,
}

impl std::fmt::Debug for KvCacheClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvCacheClient")
            .field("client", &"<tonic_client>")
            .finish()
    }
}

impl KvCacheClient {
    /// Create a new client connected to a specific node
    pub async fn connect(addr: String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = kv_cache::kv_cache_service_client::KvCacheServiceClient::connect(addr).await?;
        Ok(Self { client })
    }

    /// Forward a request to another node
    pub async fn forward_request(&mut self, operation: String, key: String, value: String, ttl_seconds: i64) -> Result<GrpcResponse, Box<dyn std::error::Error + Send + Sync>> {
        let request = GrpcRequest {
            operation,
            key,
            value,
            ttl_seconds,
        };
        
        let response = self.client.clone().forward_request(request).await?;
        Ok(response.into_inner())
    }

    /// Send health check to another node
    pub async fn health_check(&mut self, node_id: String) -> Result<HealthResponse, Box<dyn std::error::Error + Send + Sync>> {
        let request = HealthRequest {
            node_id,
            timestamp: chrono::Utc::now().timestamp(),
        };
        
        let response = self.client.clone().health_check(request).await?;
        Ok(response.into_inner())
    }

    /// Get membership information from another node
    pub async fn get_membership(&mut self, requesting_node_id: String) -> Result<MembershipResponse, Box<dyn std::error::Error + Send + Sync>> {
        let request = MembershipRequest {
            requesting_node_id,
        };
        
        let response = self.client.clone().get_membership(request).await?;
        Ok(response.into_inner())
    }
} 