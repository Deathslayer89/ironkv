use std::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use kv_cache_core::{TTLStore, Value};
use kv_cache_core::consensus::RaftConsensus;
use kv_cache_core::config::{CacheConfig, MetricsConfig};
use kv_cache_core::metrics::MetricsCollector;
use tokio::sync::RwLock;

mod client;
mod protocol;

// Include test modules
#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod protocol_tests;
#[cfg(test)]
mod simple_tests;
#[cfg(test)]
mod simple_protocol_test;
#[cfg(test)]
mod test_runner;

const DEFAULT_PORT: u16 = 6379;
const DEFAULT_CLUSTER_PORT: u16 = 6380;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    
    // Parse command line arguments
    let mut cluster_mode = false;
    let mut config_path = None;
    let mut node_id = None;
    let mut cluster_port = DEFAULT_CLUSTER_PORT;
    
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "client" => {
                // Run as client
                return client::run_client().await;
            }
            "--cluster" => {
                cluster_mode = true;
            }
            "--config" => {
                if i + 1 < args.len() {
                    config_path = Some(args[i + 1].clone());
                    i += 1;
                } else {
                    eprintln!("Error: --config requires a path");
                    return Err("Missing config path".into());
                }
            }
            "--node-id" => {
                if i + 1 < args.len() {
                    node_id = Some(args[i + 1].clone());
                    i += 1;
                } else {
                    eprintln!("Error: --node-id requires a value");
                    return Err("Missing node-id".into());
                }
            }
            "--cluster-port" => {
                if i + 1 < args.len() {
                    cluster_port = args[i + 1].parse().unwrap_or(DEFAULT_CLUSTER_PORT);
                    i += 1;
                } else {
                    eprintln!("Error: --cluster-port requires a value");
                    return Err("Missing cluster-port".into());
                }
            }
            "--help" => {
                print_usage();
                return Ok(());
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_usage();
                return Err("Unknown argument".into());
            }
        }
        i += 1;
    }
    
    if cluster_mode {
        // Run as cluster server
        run_cluster_server(config_path, node_id, cluster_port).await?;
    } else {
        // Run as single node server (default)
        run_server().await?;
    }
    
    Ok(())
}

fn print_usage() {
    println!("IRONKV - High-Performance Distributed Key-Value Cache");
    println!();
    println!("Usage:");
    println!("  kv_cache_server                    # Run in single-node mode");
    println!("  kv_cache_server --cluster          # Run in cluster mode");
    println!("  kv_cache_server client             # Run as client");
    println!();
    println!("Cluster Mode Options:");
    println!("  --config <path>                    # Configuration file path");
    println!("  --node-id <id>                     # Node identifier");
    println!("  --cluster-port <port>              # Cluster communication port (default: 6380)");
    println!();
    println!("Examples:");
    println!("  kv_cache_server --cluster --node-id node1 --cluster-port 6380");
    println!("  kv_cache_server --cluster --config cluster.toml");
}

async fn run_cluster_server(
    config_path: Option<String>,
    node_id: Option<String>,
    cluster_port: u16,
) -> Result<(), Box<dyn Error>> {
    println!("🚀 Starting IRONKV in CLUSTER mode");
    
    // Load configuration
    let config = if let Some(path) = config_path {
        CacheConfig::from_file(&path)?
    } else {
        create_default_cluster_config(node_id, cluster_port)
    };
    
    println!("📋 Configuration loaded:");
    println!("   Node ID: {}", config.cluster.node_id);
    println!("   Cluster Port: {}", config.cluster.port);
    println!("   Redis Port: {}", config.server.port);
    println!("   Cluster Members: {}", config.cluster.members.len());
    
    // Initialize metrics
    let metrics = Arc::new(MetricsCollector::new(config.metrics.clone()));
    
    // Create data store
    let store = Arc::new(TTLStore::new().with_expiration_cleanup());
    
    // Create consensus system (not Arc)
    let mut consensus = RaftConsensus::new(
        config.cluster.node_id.clone(),
        config.cluster.clone(),
        metrics.clone(),
    );
    
    // Set the store in consensus for applying committed commands
    consensus.set_store(Arc::clone(&store));
    
    // Start consensus system
    consensus.start().await?;
    println!("✅ Consensus system started");
    
    // Start Redis protocol server
    let redis_listener = TcpListener::bind(format!("{}:{}", config.server.bind_address, config.server.port)).await?;
    println!("✅ Redis server listening on {}:{}", config.server.bind_address, config.server.port);
    
    // Start cluster communication server (gRPC)
    let grpc_addr = format!("{}:{}", config.cluster.bind_address, config.cluster.port).parse()?;
    println!("✅ Cluster server (gRPC) listening on {}", grpc_addr);
    
    // Spawn gRPC server in background
    let raft_server = kv_cache_core::consensus::create_raft_server(
        consensus.get_state_arc(),
        consensus.node_id.clone(),
    );
    let grpc_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(raft_server)
            .serve(grpc_addr)
            .await
            .expect("gRPC server failed");
    });
    
    // Handle Redis connections (main task)
    handle_redis_connections(redis_listener, store, &mut consensus).await?;
    
    // Stop consensus system
    consensus.stop().await?;
    println!("✅ Consensus system stopped");
    
    // Wait for gRPC server to finish (should not happen in normal operation)
    let _ = grpc_handle.await;
    
    Ok(())
}

async fn handle_redis_connections(
    listener: TcpListener,
    store: Arc<TTLStore>,
    consensus: &mut RaftConsensus,
) -> Result<(), Box<dyn Error>> {
    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New Redis connection from: {}", addr);
        
        let store_clone = Arc::clone(&store);
        let consensus_clone = consensus.clone();
        
        tokio::spawn(async move {
            if let Err(e) = handle_redis_client(socket, store_clone, &consensus_clone).await {
                eprintln!("Error handling Redis client {}: {}", addr, e);
            }
        });
    }
}

async fn handle_redis_client(
    mut socket: TcpStream,
    store: Arc<TTLStore>,
    consensus: &RaftConsensus,
) -> Result<(), Box<dyn Error>> {
    let (reader, mut writer) = socket.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        
        if bytes_read == 0 {
            // Client disconnected
            break;
        }
        
        let response = process_command_with_consensus(&line.trim(), &store, consensus).await;
        writer.write_all(response.as_bytes()).await?;
        writer.flush().await?;
    }
    
    Ok(())
}

async fn process_command_with_consensus(
    command: &str,
    store: &TTLStore,
    consensus: &RaftConsensus,
) -> String {
    let parts: Vec<&str> = command.split_whitespace().collect();
    
    if parts.is_empty() {
        return "-ERR empty command\r\n".to_string();
    }
    
    let operation = parts[0].to_uppercase();
    let is_write_operation = matches!(
        operation.as_str(),
        "SET" | "DEL" | "EXPIRE" | "PERSIST" | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | 
        "HSET" | "HDEL" | "SADD" | "SREM"
    );
    
    if is_write_operation {
        // Strong consistency for writes: must go through consensus
        if !consensus.is_leader().await {
            return "-ERR not leader\r\n".to_string();
        }
        
        // Submit command to consensus for replication
        match consensus.submit_command(command.as_bytes().to_vec()).await {
            Ok(log_index) => {
                println!("Command submitted to consensus with log index: {}", log_index.0);
                
                // Wait for the command to be committed (strong consistency)
                // In a real implementation, we'd wait for commit_index to advance
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                
                // Apply committed entries to the store
                if let Err(e) = consensus.apply_committed_entries().await {
                    println!("Warning: Failed to apply committed entries: {}", e);
                }
                
                // Now process the command locally
                process_command(command, store).await
            }
            Err(e) => {
                format!("-ERR consensus error: {}\r\n", e)
            }
        }
    } else {
        // Read operations: can be served from any node
        // For read-after-write consistency, we could check commit_index
        // but for now, we'll serve reads immediately
        process_command(command, store).await
    }
}

fn create_default_cluster_config(node_id: Option<String>, cluster_port: u16) -> CacheConfig {
    let mut config = CacheConfig::default();
    
    // Set cluster mode
    config.cluster.enabled = true;
    config.cluster.node_id = node_id.unwrap_or_else(|| "node1".to_string());
    config.cluster.bind_address = "127.0.0.1".to_string();
    config.cluster.port = cluster_port;
    config.cluster.heartbeat_interval = 50;
    config.cluster.failure_timeout = 150;
    config.cluster.replication_factor = 3;
    
    // Check for custom Redis port from environment variable
    if let Ok(port_str) = std::env::var("IRONKV_SERVER_PORT") {
        if let Ok(port) = port_str.parse::<u16>() {
            config.server.port = port;
        }
    }
    
    // Add default cluster members
    config.cluster.members.insert("node1".to_string(), "127.0.0.1:6380".to_string());
    config.cluster.members.insert("node2".to_string(), "127.0.0.1:6381".to_string());
    config.cluster.members.insert("node3".to_string(), "127.0.0.1:6382".to_string());
    
    config
}

async fn run_server() -> Result<(), Box<dyn Error>> {
    println!("Starting IRONKV server on port {}", DEFAULT_PORT);
    
    let store = Arc::new(TTLStore::new().with_expiration_cleanup());
    let listener = TcpListener::bind(format!("127.0.0.1:{}", DEFAULT_PORT)).await?;
    
    println!("Server listening on 127.0.0.1:{}", DEFAULT_PORT);
    
    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from: {}", addr);
        
        let store_clone = Arc::clone(&store);
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, store_clone).await {
                eprintln!("Error handling client {}: {}", addr, e);
            }
        });
    }
}

async fn handle_client(mut socket: TcpStream, store: Arc<TTLStore>) -> Result<(), Box<dyn Error>> {
    let (reader, mut writer) = socket.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        
        if bytes_read == 0 {
            // Client disconnected
            break;
        }
        
        let response = process_command(&line.trim(), &store).await;
        writer.write_all(response.as_bytes()).await?;
        writer.flush().await?;
    }
    
    Ok(())
}

async fn process_command(command: &str, store: &TTLStore) -> String {
    let parts: Vec<&str> = command.split_whitespace().collect();
    
    if parts.is_empty() {
        return "-ERR empty command\r\n".to_string();
    }
    
    match parts[0].to_uppercase().as_str() {
        // Basic operations
        "SET" => {
            if parts.len() < 3 || parts.len() > 5 {
                return "-ERR wrong number of arguments for SET\r\n".to_string();
            }
            let key = parts[1].to_string();
            let value = Value::String(parts[2].to_string());
            
            // Check for TTL options: SET key value [EX seconds] [PX milliseconds]
            let mut ttl_seconds = None;
            let mut i = 3;
            while i < parts.len() {
                match parts[i].to_uppercase().as_str() {
                    "EX" => {
                        if i + 1 < parts.len() {
                            if let Ok(seconds) = parts[i + 1].parse::<u64>() {
                                ttl_seconds = Some(seconds);
                                i += 2;
                            } else {
                                return "-ERR invalid time value\r\n".to_string();
                            }
                        } else {
                            return "-ERR wrong number of arguments for SET\r\n".to_string();
                        }
                    }
                    "PX" => {
                        if i + 1 < parts.len() {
                            if let Ok(milliseconds) = parts[i + 1].parse::<u64>() {
                                ttl_seconds = Some(milliseconds / 1000);
                                i += 2;
                            } else {
                                return "-ERR invalid time value\r\n".to_string();
                            }
                        } else {
                            return "-ERR wrong number of arguments for SET\r\n".to_string();
                        }
                    }
                    _ => {
                        return "-ERR syntax error\r\n".to_string();
                    }
                }
            }
            
            store.set(key, value, ttl_seconds).await;
            "+OK\r\n".to_string()
        }
        
        "GET" => {
            if parts.len() != 2 {
                return "-ERR wrong number of arguments for GET\r\n".to_string();
            }
            let key = parts[1].to_string();
            match store.get(&key).await {
                Some(value) => {
                    let value_str = value.to_string();
                    format!("${}\r\n{}\r\n", value_str.len(), value_str)
                }
                None => "$-1\r\n".to_string(),
            }
        }
        
        "DEL" => {
            if parts.len() != 2 {
                return "-ERR wrong number of arguments for DEL\r\n".to_string();
            }
            let key = parts[1].to_string();
            let deleted = store.delete(&key).await;
            if deleted {
                ":1\r\n".to_string()
            } else {
                ":0\r\n".to_string()
            }
        }
        
        "EXISTS" => {
            if parts.len() != 2 {
                return "-ERR wrong number of arguments for EXISTS\r\n".to_string();
            }
            let key = parts[1].to_string();
            let exists = store.exists(&key).await;
            if exists {
                ":1\r\n".to_string()
            } else {
                ":0\r\n".to_string()
            }
        }
        
        "TTL" => {
            if parts.len() != 2 {
                return "-ERR wrong number of arguments for TTL\r\n".to_string();
            }
            let key = parts[1].to_string();
            match store.ttl(&key).await {
                Some(ttl) => format!(":{}\r\n", ttl),
                None => ":-2\r\n".to_string(), // Key doesn't exist
            }
        }
        
        "EXPIRE" => {
            if parts.len() != 3 {
                return "-ERR wrong number of arguments for EXPIRE\r\n".to_string();
            }
            let key = parts[1].to_string();
            let ttl_seconds: u64 = match parts[2].parse() {
                Ok(seconds) => seconds,
                Err(_) => return "-ERR invalid time value\r\n".to_string(),
            };
            let success = store.expire(&key, ttl_seconds).await;
            if success {
                ":1\r\n".to_string()
            } else {
                ":0\r\n".to_string()
            }
        }
        
        "PERSIST" => {
            if parts.len() != 2 {
                return "-ERR wrong number of arguments for PERSIST\r\n".to_string();
            }
            let key = parts[1].to_string();
            let success = store.persist(&key).await;
            if success {
                ":1\r\n".to_string()
            } else {
                ":0\r\n".to_string()
            }
        }
        
        // List operations
        "LPUSH" => {
            if parts.len() != 3 {
                return "-ERR wrong number of arguments for LPUSH\r\n".to_string();
            }
            let key = parts[1].to_string();
            let value = parts[2].to_string();
            match store.lpush(key, value).await {
                Ok(len) => format!(":{}\r\n", len),
                Err(e) => format!("-ERR {}\r\n", e),
            }
        }
        
        "RPUSH" => {
            if parts.len() != 3 {
                return "-ERR wrong number of arguments for RPUSH\r\n".to_string();
            }
            let key = parts[1].to_string();
            let value = parts[2].to_string();
            match store.rpush(key, value).await {
                Ok(len) => format!(":{}\r\n", len),
                Err(e) => format!("-ERR {}\r\n", e),
            }
        }
        
        "LPOP" => {
            if parts.len() != 2 {
                return "-ERR wrong number of arguments for LPOP\r\n".to_string();
            }
            let key = parts[1].to_string();
            match store.lpop(&key).await {
                Ok(Some(value)) => format!("${}\r\n{}\r\n", value.len(), value),
                Ok(None) => "$-1\r\n".to_string(),
                Err(e) => format!("-ERR {}\r\n", e),
            }
        }
        
        "RPOP" => {
            if parts.len() != 2 {
                return "-ERR wrong number of arguments for RPOP\r\n".to_string();
            }
            let key = parts[1].to_string();
            match store.rpop(&key).await {
                Ok(Some(value)) => format!("${}\r\n{}\r\n", value.len(), value),
                Ok(None) => "$-1\r\n".to_string(),
                Err(e) => format!("-ERR {}\r\n", e),
            }
        }
        
        "LRANGE" => {
            if parts.len() != 4 {
                return "-ERR wrong number of arguments for LRANGE\r\n".to_string();
            }
            let key = parts[1].to_string();
            let start: i64 = parts[2].parse().unwrap_or(0);
            let stop: i64 = parts[3].parse().unwrap_or(-1);
            
            match store.lrange(&key, start, stop).await {
                Ok(items) => {
                    let mut response = format!("*{}\r\n", items.len());
                    for item in items {
                        response.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
                    }
                    response
                }
                Err(e) => format!("-ERR {}\r\n", e),
            }
        }
        
        // Hash operations
        "HSET" => {
            if parts.len() != 4 {
                return "-ERR wrong number of arguments for HSET\r\n".to_string();
            }
            let key = parts[1].to_string();
            let field = parts[2].to_string();
            let value = parts[3].to_string();
            match store.hset(key, field, value).await {
                Ok(is_new) => format!(":{}\r\n", if is_new { 1 } else { 0 }),
                Err(e) => format!("-ERR {}\r\n", e),
            }
        }
        
        "HGET" => {
            if parts.len() != 3 {
                return "-ERR wrong number of arguments for HGET\r\n".to_string();
            }
            let key = parts[1].to_string();
            let field = parts[2];
            match store.hget(&key, field).await {
                Ok(Some(value)) => format!("${}\r\n{}\r\n", value.len(), value),
                Ok(None) => "$-1\r\n".to_string(),
                Err(e) => format!("-ERR {}\r\n", e),
            }
        }
        
        "HDEL" => {
            if parts.len() != 3 {
                return "-ERR wrong number of arguments for HDEL\r\n".to_string();
            }
            let key = parts[1].to_string();
            let field = parts[2];
            match store.hdel(&key, field).await {
                Ok(deleted) => format!(":{}\r\n", if deleted { 1 } else { 0 }),
                Err(e) => format!("-ERR {}\r\n", e),
            }
        }
        
        "HGETALL" => {
            if parts.len() != 2 {
                return "-ERR wrong number of arguments for HGETALL\r\n".to_string();
            }
            let key = parts[1].to_string();
            match store.hgetall(&key).await {
                Ok(hash_map) => {
                    let mut response = format!("*{}\r\n", hash_map.len() * 2);
                    for (field, value) in hash_map {
                        response.push_str(&format!("${}\r\n{}\r\n", field.len(), field));
                        response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                    }
                    response
                }
                Err(e) => format!("-ERR {}\r\n", e),
            }
        }
        
        // Set operations
        "SADD" => {
            if parts.len() != 3 {
                return "-ERR wrong number of arguments for SADD\r\n".to_string();
            }
            let key = parts[1].to_string();
            let member = parts[2].to_string();
            match store.sadd(key, member).await {
                Ok(added) => format!(":{}\r\n", if added { 1 } else { 0 }),
                Err(e) => format!("-ERR {}\r\n", e),
            }
        }
        
        "SREM" => {
            if parts.len() != 3 {
                return "-ERR wrong number of arguments for SREM\r\n".to_string();
            }
            let key = parts[1].to_string();
            let member = parts[2];
            match store.srem(&key, member).await {
                Ok(removed) => format!(":{}\r\n", if removed { 1 } else { 0 }),
                Err(e) => format!("-ERR {}\r\n", e),
            }
        }
        
        "SISMEMBER" => {
            if parts.len() != 3 {
                return "-ERR wrong number of arguments for SISMEMBER\r\n".to_string();
            }
            let key = parts[1].to_string();
            let member = parts[2];
            match store.sismember(&key, member).await {
                Ok(is_member) => format!(":{}\r\n", if is_member { 1 } else { 0 }),
                Err(e) => format!("-ERR {}\r\n", e),
            }
        }
        
        "SMEMBERS" => {
            if parts.len() != 2 {
                return "-ERR wrong number of arguments for SMEMBERS\r\n".to_string();
            }
            let key = parts[1].to_string();
            match store.smembers(&key).await {
                Ok(members) => {
                    let mut response = format!("*{}\r\n", members.len());
                    for member in members {
                        response.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                    }
                    response
                }
                Err(e) => format!("-ERR {}\r\n", e),
            }
        }
        
        // Utility commands
        "PING" => {
            "+PONG\r\n".to_string()
        }
        
        "QUIT" => {
            "+OK\r\n".to_string()
        }
        
        _ => {
            format!("-ERR unknown command '{}'\r\n", parts[0])
        }
    }
}
