use std::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use kv_cache_core::{TTLStore, Value};

mod client;
mod protocol;

#[cfg(test)]
mod simple_tests;

#[cfg(test)]
mod protocol_tests;

#[cfg(test)]
mod simple_protocol_test;

const DEFAULT_PORT: u16 = 6379;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    
    if args.len() > 1 && args[1] == "client" {
        // Run as client
        client::run_client().await?;
    } else {
        // Run as server (default)
        run_server().await?;
    }
    
    Ok(())
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
