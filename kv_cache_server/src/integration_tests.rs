use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::Barrier;
use crate::test_runner::run_server_for_tests;

async fn send_command(stream: &mut TcpStream, command: &str) -> String {
    let command_with_newline = format!("{}\r\n", command);
    stream.write_all(command_with_newline.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    
    let mut buffer = [0; 1024];
    let n = stream.read(&mut buffer).await.unwrap();
    String::from_utf8_lossy(&buffer[..n]).to_string()
}

async fn connect_to_server() -> TcpStream {
    TcpStream::connect("127.0.0.1:6379").await.unwrap()
}

async fn setup_server() {
    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = Arc::clone(&barrier);
    
    // Start server in background
    tokio::spawn(async move {
        if let Err(e) = run_server_for_tests(barrier_clone).await {
            eprintln!("Server error: {}", e);
        }
    });
    
    // Wait for server to be ready
    barrier.wait().await;
    
    // Give server a moment to fully start
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_basic_operations() {
    setup_server().await;
    let mut stream = connect_to_server().await;
    
    // Test SET and GET
    let response = send_command(&mut stream, "SET test_key test_value").await;
    assert!(response.contains("+OK"));
    
    let response = send_command(&mut stream, "GET test_key").await;
    assert!(response.contains("test_value"));
    
    // Test EXISTS
    let response = send_command(&mut stream, "EXISTS test_key").await;
    assert!(response.contains(":1"));
    
    // Test non-existent key
    let response = send_command(&mut stream, "GET nonexistent_key").await;
    assert!(response.contains("$-1"));
    
    // Test DELETE
    let response = send_command(&mut stream, "DEL test_key").await;
    assert!(response.contains(":1"));
    
    let response = send_command(&mut stream, "EXISTS test_key").await;
    assert!(response.contains(":0"));
}

#[tokio::test]
async fn test_list_operations() {
    setup_server().await;
    let mut stream = connect_to_server().await;
    
    // Test LPUSH
    let response = send_command(&mut stream, "LPUSH mylist item1").await;
    assert!(response.contains(":1"));
    
    let response = send_command(&mut stream, "LPUSH mylist item2").await;
    assert!(response.contains(":2"));
    
    // Test RPUSH
    let response = send_command(&mut stream, "RPUSH mylist item3").await;
    assert!(response.contains(":3"));
    
    // Test LRANGE
    let response = send_command(&mut stream, "LRANGE mylist 0 -1").await;
    assert!(response.contains("item2"));
    assert!(response.contains("item1"));
    assert!(response.contains("item3"));
    
    // Test LPOP
    let response = send_command(&mut stream, "LPOP mylist").await;
    assert!(response.contains("item2"));
    
    // Test RPOP
    let response = send_command(&mut stream, "RPOP mylist").await;
    assert!(response.contains("item3"));
    
    // Test empty list
    let response = send_command(&mut stream, "LPOP mylist").await;
    assert!(response.contains("item1"));
    
    let response = send_command(&mut stream, "LPOP mylist").await;
    assert!(response.contains("$-1")); // Empty list
}

#[tokio::test]
async fn test_hash_operations() {
    setup_server().await;
    let mut stream = connect_to_server().await;
    
    // Test HSET
    let response = send_command(&mut stream, "HSET myhash field1 value1").await;
    assert!(response.contains(":1")); // New field
    
    let response = send_command(&mut stream, "HSET myhash field2 value2").await;
    assert!(response.contains(":1")); // New field
    
    let response = send_command(&mut stream, "HSET myhash field1 updated_value").await;
    assert!(response.contains(":0")); // Existing field
    
    // Test HGET
    let response = send_command(&mut stream, "HGET myhash field1").await;
    assert!(response.contains("updated_value"));
    
    let response = send_command(&mut stream, "HGET myhash field2").await;
    assert!(response.contains("value2"));
    
    let response = send_command(&mut stream, "HGET myhash nonexistent_field").await;
    assert!(response.contains("$-1"));
    
    // Test HGETALL
    let response = send_command(&mut stream, "HGETALL myhash").await;
    assert!(response.contains("field1"));
    assert!(response.contains("field2"));
    assert!(response.contains("updated_value"));
    assert!(response.contains("value2"));
    
    // Test HDEL
    let response = send_command(&mut stream, "HDEL myhash field1").await;
    assert!(response.contains(":1"));
    
    let response = send_command(&mut stream, "HGET myhash field1").await;
    assert!(response.contains("$-1"));
    
    let response = send_command(&mut stream, "HDEL myhash field1").await;
    assert!(response.contains(":0")); // Already deleted
}

#[tokio::test]
async fn test_set_operations() {
    setup_server().await;
    let mut stream = connect_to_server().await;
    
    // Test SADD
    let response = send_command(&mut stream, "SADD myset member1").await;
    assert!(response.contains(":1")); // New member
    
    let response = send_command(&mut stream, "SADD myset member2").await;
    assert!(response.contains(":1")); // New member
    
    let response = send_command(&mut stream, "SADD myset member1").await;
    assert!(response.contains(":0")); // Duplicate member
    
    // Test SISMEMBER
    let response = send_command(&mut stream, "SISMEMBER myset member1").await;
    assert!(response.contains(":1")); // Member exists
    
    let response = send_command(&mut stream, "SISMEMBER myset member3").await;
    assert!(response.contains(":0")); // Member doesn't exist
    
    // Test SMEMBERS
    let response = send_command(&mut stream, "SMEMBERS myset").await;
    assert!(response.contains("member1"));
    assert!(response.contains("member2"));
    
    // Test SREM
    let response = send_command(&mut stream, "SREM myset member1").await;
    assert!(response.contains(":1"));
    
    let response = send_command(&mut stream, "SISMEMBER myset member1").await;
    assert!(response.contains(":0")); // No longer a member
    
    let response = send_command(&mut stream, "SREM myset member1").await;
    assert!(response.contains(":0")); // Already removed
}

#[tokio::test]
async fn test_type_conflicts() {
    setup_server().await;
    let mut stream = connect_to_server().await;
    
    // Create a string key
    let response = send_command(&mut stream, "SET conflict_key string_value").await;
    assert!(response.contains("+OK"));
    
    // Try to use list operations on a string key
    let response = send_command(&mut stream, "LPUSH conflict_key list_item").await;
    assert!(response.contains("-ERR"));
    
    let response = send_command(&mut stream, "HSET conflict_key field value").await;
    assert!(response.contains("-ERR"));
    
    let response = send_command(&mut stream, "SADD conflict_key member").await;
    assert!(response.contains("-ERR"));
    
    // Verify the original value is still intact
    let response = send_command(&mut stream, "GET conflict_key").await;
    assert!(response.contains("string_value"));
}

#[tokio::test]
async fn test_concurrent_access() {
    setup_server().await;
    let mut handles = vec![];
    
    for i in 0..5 {
        let handle = tokio::spawn(async move {
            let mut stream = connect_to_server().await;
            
            // Each task performs different operations
            match i % 4 {
                0 => {
                    // String operations
                    let key = format!("concurrent_key_{}", i);
                    let value = format!("value_{}", i);
                    let response = send_command(&mut stream, &format!("SET {} {}", key, value)).await;
                    assert!(response.contains("+OK"));
                }
                1 => {
                    // List operations
                    let key = format!("concurrent_list_{}", i);
                    let item = format!("item_{}", i);
                    let response = send_command(&mut stream, &format!("LPUSH {} {}", key, item)).await;
                    assert!(response.contains(":1"));
                }
                2 => {
                    // Hash operations
                    let key = format!("concurrent_hash_{}", i);
                    let field = format!("field_{}", i);
                    let value = format!("value_{}", i);
                    let response = send_command(&mut stream, &format!("HSET {} {} {}", key, field, value)).await;
                    assert!(response.contains(":1"));
                }
                3 => {
                    // Set operations
                    let key = format!("concurrent_set_{}", i);
                    let member = format!("member_{}", i);
                    let response = send_command(&mut stream, &format!("SADD {} {}", key, member)).await;
                    assert!(response.contains(":1"));
                }
                _ => unreachable!(),
            }
        });
        handles.push(handle);
    }
    
    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_edge_cases() {
    setup_server().await;
    let mut stream = connect_to_server().await;
    
    // Test empty commands
    let response = send_command(&mut stream, "").await;
    assert!(response.contains("-ERR"));
    
    // Test unknown commands
    let response = send_command(&mut stream, "UNKNOWN_COMMAND").await;
    assert!(response.contains("-ERR"));
    
    // Test wrong number of arguments
    let response = send_command(&mut stream, "SET key").await;
    assert!(response.contains("-ERR"));
    
    let response = send_command(&mut stream, "GET key extra_arg").await;
    assert!(response.contains("-ERR"));
    
    // Test PING
    let response = send_command(&mut stream, "PING").await;
    assert!(response.contains("+PONG"));
    
    // Test operations on non-existent keys
    let response = send_command(&mut stream, "LRANGE nonexistent_list 0 -1").await;
    assert!(response.contains("*0")); // Empty array
    
    let response = send_command(&mut stream, "HGETALL nonexistent_hash").await;
    assert!(response.contains("*0")); // Empty array
    
    let response = send_command(&mut stream, "SMEMBERS nonexistent_set").await;
    assert!(response.contains("*0")); // Empty array
}

#[tokio::test]
async fn test_lrange_edge_cases() {
    setup_server().await;
    let mut stream = connect_to_server().await;
    
    // Create a list with 5 items
    for i in 1..=5 {
        send_command(&mut stream, &format!("RPUSH testlist item{}", i)).await;
    }
    
    // Test various range scenarios
    let response = send_command(&mut stream, "LRANGE testlist 0 2").await;
    assert!(response.contains("item1"));
    assert!(response.contains("item2"));
    assert!(response.contains("item3"));
    
    let response = send_command(&mut stream, "LRANGE testlist -3 -1").await;
    assert!(response.contains("item3"));
    assert!(response.contains("item4"));
    assert!(response.contains("item5"));
    
    let response = send_command(&mut stream, "LRANGE testlist 10 20").await;
    assert!(response.contains("*0")); // Out of bounds
    
    let response = send_command(&mut stream, "LRANGE testlist -10 -5").await;
    assert!(response.contains("*0")); // Out of bounds
} 