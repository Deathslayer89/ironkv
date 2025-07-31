use std::sync::Arc;
use tokio::sync::Barrier;
use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use kv_cache_core::{Store, Value};

pub async fn run_server_for_tests(barrier: Arc<Barrier>, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::new();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    
    // Signal that server is ready
    barrier.wait().await;
    
    loop {
        let (socket, _addr) = listener.accept().await?;
        let store_clone = store.clone();
        
        tokio::spawn(async move {
            if let Err(e) = handle_client_for_tests(socket, store_clone).await {
                eprintln!("Error handling client: {}", e);
            }
        });
    }
}

async fn handle_client_for_tests(mut socket: tokio::net::TcpStream, store: Store) -> Result<(), Box<dyn std::error::Error>> {
    let (reader, mut writer) = socket.split();
    let mut reader = tokio::io::BufReader::new(reader);
    let mut line = String::new();
    
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        
        if bytes_read == 0 {
            break;
        }
        
        let response = process_command_for_tests(&line.trim(), &store).await;
        writer.write_all(response.as_bytes()).await?;
        writer.flush().await?;
    }
    
    Ok(())
}

async fn process_command_for_tests(command: &str, store: &Store) -> String {
    let parts: Vec<&str> = command.split_whitespace().collect();
    
    if parts.is_empty() {
        return "-ERR empty command\r\n".to_string();
    }
    
    match parts[0].to_uppercase().as_str() {
        // Basic operations
        "SET" => {
            if parts.len() != 3 {
                return "-ERR wrong number of arguments for SET\r\n".to_string();
            }
            let key = parts[1].to_string();
            let value = Value::String(parts[2].to_string());
            store.set(key, value).await;
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