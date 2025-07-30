use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};
use crate::value::Value;

#[derive(Debug)]
pub struct TTLStore {
    data: Arc<RwLock<HashMap<String, (Value, Option<Instant>)>>>,
    expiration_task: Option<tokio::task::JoinHandle<()>>,
    cleanup_interval: Duration,
    sample_size: usize,
}

impl TTLStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            expiration_task: None,
            cleanup_interval: Duration::from_secs(10),
            sample_size: 20, // Sample 20 keys per cleanup cycle
        }
    }

    pub fn with_expiration_cleanup(mut self) -> Self {
        let data_clone = Arc::clone(&self.data);
        let interval = self.cleanup_interval;
        let sample_size = self.sample_size;
        
        self.expiration_task = Some(tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;
                Self::cleanup_expired_keys_sample(&data_clone, sample_size).await;
            }
        }));
        self
    }

    pub fn with_cleanup_config(mut self, interval: Duration, sample_size: usize) -> Self {
        self.cleanup_interval = interval;
        self.sample_size = sample_size;
        self
    }

    async fn cleanup_expired_keys_sample(
        data: &Arc<RwLock<HashMap<String, (Value, Option<Instant>)>>>,
        sample_size: usize,
    ) {
        let mut data = data.write().await;
        let now = Instant::now();
        
        // Sample keys for cleanup to avoid blocking for too long
        let keys_to_check: Vec<String> = data
            .keys()
            .take(sample_size)
            .cloned()
            .collect();

        let mut expired_keys = Vec::new();
        for key in keys_to_check {
            if let Some((_, expiration)) = data.get(&key) {
                if let Some(exp) = expiration {
                    if *exp <= now {
                        expired_keys.push(key);
                    }
                }
            }
        }

        let expired_count = expired_keys.len();
        for key in expired_keys {
            data.remove(&key);
        }

        if expired_count > 0 {
            println!("Cleaned up {} expired keys (sampled {})", expired_count, sample_size);
        }
    }



    pub async fn set(&self, key: String, value: Value, ttl_seconds: Option<u64>) {
        let mut data = self.data.write().await;
        let expiration = ttl_seconds.map(|seconds| Instant::now() + Duration::from_secs(seconds));
        data.insert(key, (value, expiration));
    }

    pub async fn get(&self, key: &str) -> Option<Value> {
        let mut data = self.data.write().await;
        
        if let Some((value, expiration)) = data.get(key) {
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    // Key has expired, remove it
                    data.remove(key);
                    return None;
                }
            }
            Some(value.clone())
        } else {
            None
        }
    }

    pub async fn delete(&self, key: &str) -> bool {
        let mut data = self.data.write().await;
        data.remove(key).is_some()
    }

    pub async fn exists(&self, key: &str) -> bool {
        let mut data = self.data.write().await;
        
        if let Some((_, expiration)) = data.get(key) {
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    // Key has expired, remove it
                    data.remove(key);
                    return false;
                }
            }
            true
        } else {
            false
        }
    }

    pub async fn ttl(&self, key: &str) -> Option<i64> {
        let mut data = self.data.write().await;
        
        if let Some((_, expiration)) = data.get(key) {
            if let Some(exp) = expiration {
                let now = Instant::now();
                if *exp <= now {
                    // Key has expired, remove it
                    data.remove(key);
                    return None;
                }
                let ttl_seconds = (exp.duration_since(now).as_secs_f64() + 0.5) as i64;
                Some(ttl_seconds)
            } else {
                Some(-1) // No expiration
            }
        } else {
            None // Key doesn't exist
        }
    }

    // Redis-like TTL commands
    pub async fn expire(&self, key: &str, ttl_seconds: u64) -> bool {
        let mut data = self.data.write().await;
        
        if let Some((value, _)) = data.get_mut(key) {
            let expiration = Instant::now() + Duration::from_secs(ttl_seconds);
            *data.get_mut(key).unwrap() = (value.clone(), Some(expiration));
            true
        } else {
            false
        }
    }

    pub async fn expire_at(&self, key: &str, timestamp: u64) -> bool {
        let mut data = self.data.write().await;
        
        if let Some((value, _)) = data.get_mut(key) {
            let expiration = Instant::now() + Duration::from_secs(timestamp.saturating_sub(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            ));
            *data.get_mut(key).unwrap() = (value.clone(), Some(expiration));
            true
        } else {
            false
        }
    }

    pub async fn persist(&self, key: &str) -> bool {
        let mut data = self.data.write().await;
        
        if let Some((value, _)) = data.get_mut(key) {
            *data.get_mut(key).unwrap() = (value.clone(), None);
            true
        } else {
            false
        }
    }

    pub async fn pttl(&self, key: &str) -> Option<i64> {
        let mut data = self.data.write().await;
        
        if let Some((_, expiration)) = data.get(key) {
            if let Some(exp) = expiration {
                let now = Instant::now();
                if *exp <= now {
                    // Key has expired, remove it
                    data.remove(key);
                    return None;
                }
                let ttl_millis = exp.duration_since(now).as_millis() as i64;
                Some(ttl_millis)
            } else {
                Some(-1) // No expiration
            }
        } else {
            None // Key doesn't exist
        }
    }

    pub async fn pexpire(&self, key: &str, ttl_milliseconds: u64) -> bool {
        let mut data = self.data.write().await;
        
        if let Some((value, _)) = data.get_mut(key) {
            let expiration = Instant::now() + Duration::from_millis(ttl_milliseconds);
            *data.get_mut(key).unwrap() = (value.clone(), Some(expiration));
            true
        } else {
            false
        }
    }

    pub async fn len(&self) -> usize {
        let data = self.data.read().await;
        data.len()
    }

    pub async fn clear(&self) {
        let mut data = self.data.write().await;
        data.clear();
    }

    // List operations with TTL support
    pub async fn lpush(&self, key: String, value: String) -> Result<usize, String> {
        let mut data = self.data.write().await;
        
        // Check if key exists and has expired
        if let Some((_, expiration)) = data.get(&key) {
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    data.remove(&key);
                }
            }
        }
        
        let entry = data.entry(key).or_insert_with(|| (Value::List(VecDeque::new()), None));
        match &mut entry.0 {
            Value::List(list) => {
                list.push_front(value);
                Ok(list.len())
            }
            _ => Err("Key exists and is not a list".to_string()),
        }
    }

    pub async fn rpush(&self, key: String, value: String) -> Result<usize, String> {
        let mut data = self.data.write().await;
        
        // Check if key exists and has expired
        if let Some((_, expiration)) = data.get(&key) {
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    data.remove(&key);
                }
            }
        }
        
        let entry = data.entry(key).or_insert_with(|| (Value::List(VecDeque::new()), None));
        match &mut entry.0 {
            Value::List(list) => {
                list.push_back(value);
                Ok(list.len())
            }
            _ => Err("Key exists and is not a list".to_string()),
        }
    }

    pub async fn lpop(&self, key: &str) -> Result<Option<String>, String> {
        let mut data = self.data.write().await;
        
        if let Some((value, expiration)) = data.get_mut(key) {
            // Check expiration
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    data.remove(key);
                    return Ok(None);
                }
            }
            
            match value {
                Value::List(list) => {
                    if list.is_empty() {
                        data.remove(key);
                        Ok(None)
                    } else {
                        Ok(list.pop_front())
                    }
                }
                _ => Err("Key exists and is not a list".to_string()),
            }
        } else {
            Ok(None)
        }
    }

    pub async fn rpop(&self, key: &str) -> Result<Option<String>, String> {
        let mut data = self.data.write().await;
        
        if let Some((value, expiration)) = data.get_mut(key) {
            // Check expiration
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    data.remove(key);
                    return Ok(None);
                }
            }
            
            match value {
                Value::List(list) => {
                    if list.is_empty() {
                        data.remove(key);
                        Ok(None)
                    } else {
                        Ok(list.pop_back())
                    }
                }
                _ => Err("Key exists and is not a list".to_string()),
            }
        } else {
            Ok(None)
        }
    }

    pub async fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, String> {
        let mut data = self.data.write().await;
        
        if let Some((value, expiration)) = data.get(key) {
            // Check expiration
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    data.remove(key);
                    return Ok(vec![]);
                }
            }
            
            match value {
                Value::List(list) => {
                    let len = list.len() as i64;
                    let start_idx = if start < 0 { len + start } else { start };
                    let stop_idx = if stop < 0 { len + stop + 1 } else { stop + 1 };
                    
                    let start_idx = start_idx.max(0).min(len) as usize;
                    let stop_idx = stop_idx.max(0).min(len) as usize;
                    
                    let result: Vec<String> = list.iter()
                        .skip(start_idx)
                        .take(stop_idx - start_idx)
                        .cloned()
                        .collect();
                    
                    Ok(result)
                }
                _ => Err("Key exists and is not a list".to_string()),
            }
        } else {
            Ok(vec![])
        }
    }

    // Hash operations with TTL support
    pub async fn hset(&self, key: String, field: String, value: String) -> Result<bool, String> {
        let mut data = self.data.write().await;
        
        // Check if key exists and has expired
        if let Some((_, expiration)) = data.get(&key) {
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    data.remove(&key);
                }
            }
        }
        
        let entry = data.entry(key).or_insert_with(|| (Value::Hash(HashMap::new()), None));
        match &mut entry.0 {
            Value::Hash(hash_map) => {
                let is_new = !hash_map.contains_key(&field);
                hash_map.insert(field, value);
                Ok(is_new)
            }
            _ => Err("Key exists and is not a hash".to_string()),
        }
    }

    pub async fn hget(&self, key: &str, field: &str) -> Result<Option<String>, String> {
        let mut data = self.data.write().await;
        
        if let Some((value, expiration)) = data.get(key) {
            // Check expiration
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    data.remove(key);
                    return Ok(None);
                }
            }
            
            match value {
                Value::Hash(hash_map) => {
                    Ok(hash_map.get(field).cloned())
                }
                _ => Err("Key exists and is not a hash".to_string()),
            }
        } else {
            Ok(None)
        }
    }

    pub async fn hdel(&self, key: &str, field: &str) -> Result<bool, String> {
        let mut data = self.data.write().await;
        
        if let Some((value, expiration)) = data.get_mut(key) {
            // Check expiration
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    data.remove(key);
                    return Ok(false);
                }
            }
            
            match value {
                Value::Hash(hash_map) => {
                    let removed = hash_map.remove(field).is_some();
                    if hash_map.is_empty() {
                        data.remove(key);
                    }
                    Ok(removed)
                }
                _ => Err("Key exists and is not a hash".to_string()),
            }
        } else {
            Ok(false)
        }
    }

    pub async fn hgetall(&self, key: &str) -> Result<HashMap<String, String>, String> {
        let mut data = self.data.write().await;
        
        if let Some((value, expiration)) = data.get(key) {
            // Check expiration
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    data.remove(key);
                    return Ok(HashMap::new());
                }
            }
            
            match value {
                Value::Hash(hash_map) => {
                    Ok(hash_map.clone())
                }
                _ => Err("Key exists and is not a hash".to_string()),
            }
        } else {
            Ok(HashMap::new())
        }
    }

    // Set operations with TTL support
    pub async fn sadd(&self, key: String, member: String) -> Result<bool, String> {
        let mut data = self.data.write().await;
        
        // Check if key exists and has expired
        if let Some((_, expiration)) = data.get(&key) {
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    data.remove(&key);
                }
            }
        }
        
        let entry = data.entry(key).or_insert_with(|| (Value::Set(HashSet::new()), None));
        match &mut entry.0 {
            Value::Set(set) => {
                Ok(set.insert(member))
            }
            _ => Err("Key exists and is not a set".to_string()),
        }
    }

    pub async fn srem(&self, key: &str, member: &str) -> Result<bool, String> {
        let mut data = self.data.write().await;
        
        if let Some((value, expiration)) = data.get_mut(key) {
            // Check expiration
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    data.remove(key);
                    return Ok(false);
                }
            }
            
            match value {
                Value::Set(set) => {
                    let removed = set.remove(member);
                    if set.is_empty() {
                        data.remove(key);
                    }
                    Ok(removed)
                }
                _ => Err("Key exists and is not a set".to_string()),
            }
        } else {
            Ok(false)
        }
    }

    pub async fn sismember(&self, key: &str, member: &str) -> Result<bool, String> {
        let mut data = self.data.write().await;
        
        if let Some((value, expiration)) = data.get(key) {
            // Check expiration
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    data.remove(key);
                    return Ok(false);
                }
            }
            
            match value {
                Value::Set(set) => {
                    Ok(set.contains(member))
                }
                _ => Err("Key exists and is not a set".to_string()),
            }
        } else {
            Ok(false)
        }
    }

    pub async fn smembers(&self, key: &str) -> Result<Vec<String>, String> {
        let mut data = self.data.write().await;
        
        if let Some((value, expiration)) = data.get(key) {
            // Check expiration
            if let Some(exp) = expiration {
                if *exp <= Instant::now() {
                    data.remove(key);
                    return Ok(vec![]);
                }
            }
            
            match value {
                Value::Set(set) => {
                    Ok(set.iter().cloned().collect())
                }
                _ => Err("Key exists and is not a set".to_string()),
            }
        } else {
            Ok(vec![])
        }
    }
}

impl Default for TTLStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TTLStore {
    fn drop(&mut self) {
        if let Some(task) = self.expiration_task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_ttl_operations() {
        let store = TTLStore::new();
        
        // Test SET with TTL
        store.set("key1".to_string(), Value::String("value1".to_string()), Some(1)).await;
        store.set("key2".to_string(), Value::String("value2".to_string()), None).await;
        
        // Both keys should exist initially
        assert!(store.exists("key1").await);
        assert!(store.exists("key2").await);
        
        // Check TTL values
        assert!(store.ttl("key1").await.unwrap() > 0);
        assert_eq!(store.ttl("key2").await, Some(-1)); // No expiration
        
        // Wait for key1 to expire
        tokio::time::sleep(Duration::from_secs(2)).await;
        
        // key1 should be expired, key2 should still exist
        assert!(!store.exists("key1").await);
        assert!(store.exists("key2").await);
        assert_eq!(store.get("key1").await, None);
        assert_eq!(store.get("key2").await, Some(Value::String("value2".to_string())));
    }

    #[tokio::test]
    async fn test_expire_command() {
        let store = TTLStore::new();
        
        // Set a key without TTL
        store.set("key1".to_string(), Value::String("value1".to_string()), None).await;
        assert_eq!(store.ttl("key1").await, Some(-1));
        
        // Set TTL using expire command
        assert!(store.expire("key1", 1).await);
        assert!(store.ttl("key1").await.unwrap() > 0);
        
        // Wait for expiration
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(!store.exists("key1").await);
    }

    #[tokio::test]
    async fn test_persist_command() {
        let store = TTLStore::new();
        
        // Set a key with TTL
        store.set("key1".to_string(), Value::String("value1".to_string()), Some(10)).await;
        assert!(store.ttl("key1").await.unwrap() > 0);
        
        // Remove TTL using persist command
        assert!(store.persist("key1").await);
        assert_eq!(store.ttl("key1").await, Some(-1));
        assert!(store.exists("key1").await);
    }

    #[tokio::test]
    async fn test_list_operations_with_ttl() {
        let store = TTLStore::new();
        
        // Create a list with TTL
        store.set("mylist".to_string(), Value::List(VecDeque::new()), Some(1)).await;
        
        // Add items to the list
        assert_eq!(store.lpush("mylist".to_string(), "item1".to_string()).await, Ok(1));
        assert_eq!(store.lpush("mylist".to_string(), "item2".to_string()).await, Ok(2));
        
        // List should exist
        assert!(store.exists("mylist").await);
        
        // Wait for expiration
        tokio::time::sleep(Duration::from_secs(2)).await;
        
        // List should be expired
        assert!(!store.exists("mylist").await);
        assert_eq!(store.lpop("mylist").await, Ok(None));
    }

    #[tokio::test]
    async fn test_hash_operations_with_ttl() {
        let store = TTLStore::new();
        
        // Create a hash with TTL
        store.set("myhash".to_string(), Value::Hash(HashMap::new()), Some(1)).await;
        
        // Add fields to the hash
        assert_eq!(store.hset("myhash".to_string(), "field1".to_string(), "value1".to_string()).await, Ok(true));
        assert_eq!(store.hset("myhash".to_string(), "field2".to_string(), "value2".to_string()).await, Ok(true));
        
        // Hash should exist
        assert!(store.exists("myhash").await);
        
        // Wait for expiration
        tokio::time::sleep(Duration::from_secs(2)).await;
        
        // Hash should be expired
        assert!(!store.exists("myhash").await);
        assert_eq!(store.hget("myhash", "field1").await, Ok(None));
    }

    #[tokio::test]
    async fn test_set_operations_with_ttl() {
        let store = TTLStore::new();
        
        // Create a set with TTL
        store.set("myset".to_string(), Value::Set(HashSet::new()), Some(1)).await;
        
        // Add members to the set
        assert_eq!(store.sadd("myset".to_string(), "member1".to_string()).await, Ok(true));
        assert_eq!(store.sadd("myset".to_string(), "member2".to_string()).await, Ok(true));
        
        // Set should exist
        assert!(store.exists("myset").await);
        
        // Wait for expiration
        tokio::time::sleep(Duration::from_secs(2)).await;
        
        // Set should be expired
        assert!(!store.exists("myset").await);
        assert_eq!(store.sismember("myset", "member1").await, Ok(false));
    }

    #[tokio::test]
    async fn test_concurrent_access_with_ttl() {
        let store = Arc::new(TTLStore::new());
        let mut handles = vec![];
        
        // Spawn multiple tasks that set and get values with TTL
        for i in 0..5 {
            let store_clone = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                let key = format!("key{}", i);
                let value = Value::String(format!("value{}", i));
                
                store_clone.set(key.clone(), value.clone(), Some(5)).await;
                let retrieved = store_clone.get(&key).await;
                assert_eq!(retrieved, Some(value));
            });
            handles.push(handle);
        }
        
        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        assert_eq!(store.len().await, 5);
    }

    // Additional test cases for edge cases and Redis-like behavior
    #[tokio::test]
    async fn test_ttl_edge_cases() {
        let store = TTLStore::new();
        
        // Test TTL on non-existent key
        assert_eq!(store.ttl("nonexistent").await, None);
        
        // Test expire on non-existent key
        assert!(!store.expire("nonexistent", 10).await);
        
        // Test persist on non-existent key
        assert!(!store.persist("nonexistent").await);
        
        // Test very short TTL
        store.set("short_ttl".to_string(), Value::String("value".to_string()), Some(0)).await;
        assert_eq!(store.get("short_ttl").await, None); // Should be immediately expired
        
        // Test TTL precision
        store.set("precision_test".to_string(), Value::String("value".to_string()), Some(1)).await;
        let ttl_before = store.ttl("precision_test").await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let ttl_after = store.ttl("precision_test").await.unwrap();
        assert!(ttl_after < ttl_before);
    }

    #[tokio::test]
    async fn test_ttl_with_data_structures() {
        let store = TTLStore::new();
        
        // Test TTL on list operations
        store.set("list_key".to_string(), Value::List(VecDeque::new()), Some(1)).await;
        assert_eq!(store.lpush("list_key".to_string(), "item".to_string()).await, Ok(1));
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(store.lpop("list_key").await, Ok(None));
        
        // Test TTL on hash operations
        store.set("hash_key".to_string(), Value::Hash(HashMap::new()), Some(1)).await;
        assert_eq!(store.hset("hash_key".to_string(), "field".to_string(), "value".to_string()).await, Ok(true));
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(store.hget("hash_key", "field").await, Ok(None));
        
        // Test TTL on set operations
        store.set("set_key".to_string(), Value::Set(HashSet::new()), Some(1)).await;
        assert_eq!(store.sadd("set_key".to_string(), "member".to_string()).await, Ok(true));
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(store.sismember("set_key", "member").await, Ok(false));
    }

    #[tokio::test]
    async fn test_ttl_extension() {
        let store = TTLStore::new();
        
        // Set a key with short TTL
        store.set("extend_key".to_string(), Value::String("value".to_string()), Some(1)).await;
        
        // Extend the TTL before it expires
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(store.expire("extend_key", 2).await);
        
        // Key should still exist after original TTL
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(store.exists("extend_key").await);
        
        // But should expire after extended TTL
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(!store.exists("extend_key").await);
    }

    #[tokio::test]
    async fn test_ttl_removal() {
        let store = TTLStore::new();
        
        // Set a key with TTL
        store.set("remove_ttl".to_string(), Value::String("value".to_string()), Some(10)).await;
        assert!(store.ttl("remove_ttl").await.unwrap() > 0);
        
        // Remove TTL
        assert!(store.persist("remove_ttl").await);
        assert_eq!(store.ttl("remove_ttl").await, Some(-1));
        
        // Key should still exist and be accessible
        assert!(store.exists("remove_ttl").await);
        assert_eq!(store.get("remove_ttl").await, Some(Value::String("value".to_string())));
    }

    #[tokio::test]
    async fn test_active_expiration_cleanup() {
        let store = TTLStore::new().with_expiration_cleanup();
        
        // Add multiple keys with different TTLs
        for i in 0..10 {
            let key = format!("cleanup_key_{}", i);
            let ttl = if i % 2 == 0 { 1 } else { 10 };
            store.set(key, Value::String(format!("value_{}", i)), Some(ttl)).await;
        }
        
        // Wait for some keys to expire and cleanup task to run
        tokio::time::sleep(Duration::from_secs(12)).await;
        
        // Check that expired keys are cleaned up
        let len = store.len().await;
        assert!(len < 10); // Some keys should have been cleaned up
        
        // Add more keys with longer TTLs
        for i in 10..15 {
            let key = format!("cleanup_key_{}", i);
            store.set(key, Value::String(format!("value_{}", i)), Some(5)).await;
        }
        
        // Wait for these new keys to expire and be cleaned up
        tokio::time::sleep(Duration::from_secs(15)).await;
        
        // Check final state - should have fewer keys than after first cleanup
        let final_len = store.len().await;
        assert!(final_len < len + 5); // Should have cleaned up the new keys too
    }

    #[tokio::test]
    async fn test_redis_like_ttl_commands() {
        let store = TTLStore::new();
        
        // Test PTTL (millisecond precision TTL)
        store.set("pttl_key".to_string(), Value::String("value".to_string()), Some(1)).await;
        let pttl = store.pttl("pttl_key").await.unwrap();
        assert!(pttl > 0 && pttl <= 1000); // Should be around 1000ms
        
        // Test PEXPIRE (set TTL in milliseconds)
        store.set("pexpire_key".to_string(), Value::String("value".to_string()), None).await;
        assert!(store.pexpire("pexpire_key", 1000).await); // 1 second
        let pttl_after = store.pttl("pexpire_key").await.unwrap();
        assert!(pttl_after > 0 && pttl_after <= 1000);
        
        // Test EXPIREAT (set expiration at specific timestamp)
        let future_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() + 2; // 2 seconds from now
        
        store.set("expireat_key".to_string(), Value::String("value".to_string()), None).await;
        assert!(store.expire_at("expireat_key", future_timestamp).await);
        
        // Key should exist initially
        assert!(store.exists("expireat_key").await);
        
        // Wait for expiration
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!store.exists("expireat_key").await);
    }

    #[tokio::test]
    async fn test_cleanup_configuration() {
        // Test custom cleanup configuration
        let store = TTLStore::new()
            .with_cleanup_config(Duration::from_secs(5), 5) // 5 second interval, 5 keys per sample
            .with_expiration_cleanup();
        
        // Add keys with short TTL
        for i in 0..10 {
            let key = format!("config_key_{}", i);
            store.set(key, Value::String(format!("value_{}", i)), Some(1)).await;
        }
        
        // Wait for cleanup to run with custom interval
        tokio::time::sleep(Duration::from_secs(7)).await;
        
        // Check that some keys were cleaned up
        let len = store.len().await;
        assert!(len < 10);
    }

    #[tokio::test]
    async fn test_ttl_precision_and_edge_cases() {
        let store = TTLStore::new();
        
        // Test millisecond precision
        store.set("ms_precision".to_string(), Value::String("value".to_string()), None).await;
        assert!(store.pexpire("ms_precision", 100).await); // 100ms
        
        let pttl = store.pttl("ms_precision").await.unwrap();
        assert!(pttl > 0 && pttl <= 100);
        
        // Test zero TTL (should expire immediately)
        store.set("zero_ttl".to_string(), Value::String("value".to_string()), Some(0)).await;
        assert_eq!(store.get("zero_ttl").await, None);
        
        // Test very short TTL in milliseconds
        store.set("short_ms".to_string(), Value::String("value".to_string()), None).await;
        assert!(store.pexpire("short_ms", 50).await); // 50ms
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(store.get("short_ms").await, None);
    }

    #[tokio::test]
    async fn test_ttl_commands_on_nonexistent_keys() {
        let store = TTLStore::new();
        
        // All TTL commands should return appropriate values for non-existent keys
        assert_eq!(store.ttl("nonexistent").await, None);
        assert_eq!(store.pttl("nonexistent").await, None);
        assert!(!store.expire("nonexistent", 10).await);
        assert!(!store.pexpire("nonexistent", 10000).await);
        assert!(!store.expire_at("nonexistent", 1234567890).await);
        assert!(!store.persist("nonexistent").await);
    }

    #[tokio::test]
    async fn test_ttl_consistency_across_operations() {
        let store = TTLStore::new();
        
        // Set a key with TTL
        store.set("consistency_key".to_string(), Value::String("value".to_string()), Some(5)).await;
        
        // Check TTL values are consistent
        let ttl1 = store.ttl("consistency_key").await.unwrap();
        let pttl1 = store.pttl("consistency_key").await.unwrap();
        
        // TTL should be approximately PTTL / 1000 (within rounding error)
        assert!((ttl1 as f64 - pttl1 as f64 / 1000.0).abs() < 1.0);
        
        // Wait a bit and check again
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        let ttl2 = store.ttl("consistency_key").await.unwrap();
        let pttl2 = store.pttl("consistency_key").await.unwrap();
        
        // Values should have decreased
        assert!(ttl2 < ttl1);
        assert!(pttl2 < pttl1);
        
        // Ratio should still be approximately correct
        assert!((ttl2 as f64 - pttl2 as f64 / 1000.0).abs() < 1.0);
    }
} 