use kv_cache_core::{Store, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

#[tokio::test]
async fn test_advanced_value_types() {
    let store = Store::new();
    
    // Test String operations
    store.set("string_key".to_string(), Value::String("hello world".to_string())).await;
    let result = store.get(&"string_key".to_string()).await;
    assert_eq!(result, Some(Value::String("hello world".to_string())));
    
    // Test List operations
    assert_eq!(store.lpush("list_key".to_string(), "item1".to_string()).await, Ok(1));
    assert_eq!(store.lpush("list_key".to_string(), "item2".to_string()).await, Ok(2));
    assert_eq!(store.rpush("list_key".to_string(), "item3".to_string()).await, Ok(3));
    
    let range = store.lrange(&"list_key".to_string(), 0, -1).await.unwrap();
    assert_eq!(range, vec!["item2".to_string(), "item1".to_string(), "item3".to_string()]);
    
    assert_eq!(store.lpop(&"list_key".to_string()).await, Ok(Some("item2".to_string())));
    assert_eq!(store.rpop(&"list_key".to_string()).await, Ok(Some("item3".to_string())));
    
    // Test Hash operations
    assert_eq!(store.hset("hash_key".to_string(), "field1".to_string(), "value1".to_string()).await, Ok(true));
    assert_eq!(store.hset("hash_key".to_string(), "field2".to_string(), "value2".to_string()).await, Ok(true));
    assert_eq!(store.hset("hash_key".to_string(), "field1".to_string(), "updated_value".to_string()).await, Ok(false));
    
    assert_eq!(store.hget(&"hash_key".to_string(), "field1").await, Ok(Some("updated_value".to_string())));
    assert_eq!(store.hget(&"hash_key".to_string(), "field2").await, Ok(Some("value2".to_string())));
    assert_eq!(store.hget(&"hash_key".to_string(), "nonexistent").await, Ok(None));
    
    let all_fields = store.hgetall(&"hash_key".to_string()).await.unwrap();
    assert_eq!(all_fields.len(), 2);
    assert_eq!(all_fields.get("field1"), Some(&"updated_value".to_string()));
    assert_eq!(all_fields.get("field2"), Some(&"value2".to_string()));
    
    assert_eq!(store.hdel(&"hash_key".to_string(), "field1").await, Ok(true));
    assert_eq!(store.hget(&"hash_key".to_string(), "field1").await, Ok(None));
    
    // Test Set operations
    assert_eq!(store.sadd("set_key".to_string(), "member1".to_string()).await, Ok(true));
    assert_eq!(store.sadd("set_key".to_string(), "member2".to_string()).await, Ok(true));
    assert_eq!(store.sadd("set_key".to_string(), "member1".to_string()).await, Ok(false)); // Duplicate
    
    assert_eq!(store.sismember(&"set_key".to_string(), "member1").await, Ok(true));
    assert_eq!(store.sismember(&"set_key".to_string(), "member3").await, Ok(false));
    
    let members = store.smembers(&"set_key".to_string()).await.unwrap();
    assert_eq!(members.len(), 2);
    assert!(members.contains(&"member1".to_string()));
    assert!(members.contains(&"member2".to_string()));
    
    assert_eq!(store.srem(&"set_key".to_string(), "member1").await, Ok(true));
    assert_eq!(store.sismember(&"set_key".to_string(), "member1").await, Ok(false));
}

#[tokio::test]
async fn test_type_conflicts() {
    let store = Store::new();
    
    // Create a string key
    store.set("conflict_key".to_string(), Value::String("string_value".to_string())).await;
    
    // Try to use list operations on a string key
    assert!(store.lpush("conflict_key".to_string(), "list_item".to_string()).await.is_err());
    assert!(store.hset("conflict_key".to_string(), "field".to_string(), "value".to_string()).await.is_err());
    assert!(store.sadd("conflict_key".to_string(), "member".to_string()).await.is_err());
    
    // Verify the original value is still intact
    let result = store.get(&"conflict_key".to_string()).await;
    assert_eq!(result, Some(Value::String("string_value".to_string())));
}

#[tokio::test]
async fn test_concurrent_operations() {
    let store = Arc::new(Store::new());
    let mut handles = vec![];
    
    // Spawn multiple tasks performing different operations
    for i in 0..10 {
        let store_clone = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            match i % 4 {
                0 => {
                    // String operations
                    let key = format!("concurrent_key_{}", i);
                    let value = Value::String(format!("value_{}", i));
                    store_clone.set(key, value).await;
                }
                1 => {
                    // List operations
                    let key = format!("concurrent_list_{}", i);
                    let _ = store_clone.lpush(key, format!("item_{}", i)).await;
                }
                2 => {
                    // Hash operations
                    let key = format!("concurrent_hash_{}", i);
                    let field = format!("field_{}", i);
                    let value = format!("value_{}", i);
                    let _ = store_clone.hset(key, field, value).await;
                }
                3 => {
                    // Set operations
                    let key = format!("concurrent_set_{}", i);
                    let member = format!("member_{}", i);
                    let _ = store_clone.sadd(key, member).await;
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
    
    // Verify some results
    assert_eq!(store.len().await, 10);
}

#[tokio::test]
async fn test_value_enum_operations() {
    // Test Value enum creation and conversion
    let string_val = Value::String("hello".to_string());
    assert!(string_val.is_string());
    assert_eq!(string_val.as_string(), Some(&"hello".to_string()));
    
    let mut list = VecDeque::new();
    list.push_back("item1".to_string());
    list.push_back("item2".to_string());
    let list_val = Value::List(list.clone());
    assert!(list_val.is_list());
    assert_eq!(list_val.as_list(), Some(&list));
    
    let mut hash = HashMap::new();
    hash.insert("key1".to_string(), "value1".to_string());
    let hash_val = Value::Hash(hash.clone());
    assert!(hash_val.is_hash());
    assert_eq!(hash_val.as_hash(), Some(&hash));
    
    let mut set = HashSet::new();
    set.insert("member1".to_string());
    let set_val = Value::Set(set.clone());
    assert!(set_val.is_set());
    assert_eq!(set_val.as_set(), Some(&set));
    
    // Test From implementations
    let from_string: Value = "test".to_string().into();
    assert!(from_string.is_string());
    
    let from_list: Value = list.into();
    assert!(from_list.is_list());
    
    let from_hash: Value = hash.into();
    assert!(from_hash.is_hash());
    
    let from_set: Value = set.into();
    assert!(from_set.is_set());
}

#[tokio::test]
async fn test_edge_cases() {
    let store = Store::new();
    
    // Test operations on non-existent keys
    assert_eq!(store.lrange(&"nonexistent".to_string(), 0, -1).await, Ok(vec![]));
    assert_eq!(store.hgetall(&"nonexistent".to_string()).await, Ok(HashMap::new()));
    assert_eq!(store.smembers(&"nonexistent".to_string()).await, Ok(vec![]));
    
    // Test empty list operations
    assert_eq!(store.lpop(&"empty_list".to_string()).await, Ok(None));
    assert_eq!(store.rpop(&"empty_list".to_string()).await, Ok(None));
    
    // Test LRANGE edge cases
    store.rpush("testlist".to_string(), "item1".to_string()).await.unwrap();
    store.rpush("testlist".to_string(), "item2".to_string()).await.unwrap();
    store.rpush("testlist".to_string(), "item3".to_string()).await.unwrap();
    
    // Test out of bounds ranges
    assert_eq!(store.lrange(&"testlist".to_string(), 10, 20).await, Ok(vec![]));
    assert_eq!(store.lrange(&"testlist".to_string(), -10, -5).await, Ok(vec![]));
    
    // Test negative indices
    let result = store.lrange(&"testlist".to_string(), -2, -1).await.unwrap();
    assert_eq!(result, vec!["item2".to_string(), "item3".to_string()]);
} 