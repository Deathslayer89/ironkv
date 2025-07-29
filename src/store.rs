use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::value::Value;

pub type Key = String;

#[derive(Debug, Clone)]
pub struct Store {
    data: Arc<RwLock<HashMap<Key, Value>>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // Basic operations
    pub async fn set(&self, key: Key, value: Value) {
        let mut data = self.data.write().await;
        data.insert(key, value);
    }

    pub async fn get(&self, key: &Key) -> Option<Value> {
        let data = self.data.read().await;
        data.get(key).cloned()
    }

    pub async fn delete(&self, key: &Key) -> bool {
        let mut data = self.data.write().await;
        data.remove(key).is_some()
    }

    pub async fn exists(&self, key: &Key) -> bool {
        let data = self.data.read().await;
        data.contains_key(key)
    }

    pub async fn len(&self) -> usize {
        let data = self.data.read().await;
        data.len()
    }

    pub async fn clear(&self) {
        let mut data = self.data.write().await;
        data.clear();
    }

    // List operations
    pub async fn lpush(&self, key: Key, value: String) -> Result<usize, String> {
        let mut data = self.data.write().await;
        let list = data.entry(key).or_insert_with(|| Value::List(VecDeque::new()));
        
        match list {
            Value::List(list) => {
                list.push_front(value);
                Ok(list.len())
            }
            _ => Err("Key exists and is not a list".to_string()),
        }
    }

    pub async fn rpush(&self, key: Key, value: String) -> Result<usize, String> {
        let mut data = self.data.write().await;
        let list = data.entry(key).or_insert_with(|| Value::List(VecDeque::new()));
        
        match list {
            Value::List(list) => {
                list.push_back(value);
                Ok(list.len())
            }
            _ => Err("Key exists and is not a list".to_string()),
        }
    }

    pub async fn lpop(&self, key: &Key) -> Result<Option<String>, String> {
        let mut data = self.data.write().await;
        
        match data.get_mut(key) {
            Some(Value::List(list)) => {
                if list.is_empty() {
                    data.remove(key);
                    Ok(None)
                } else {
                    Ok(list.pop_front())
                }
            }
            Some(_) => Err("Key exists and is not a list".to_string()),
            None => Ok(None),
        }
    }

    pub async fn rpop(&self, key: &Key) -> Result<Option<String>, String> {
        let mut data = self.data.write().await;
        
        match data.get_mut(key) {
            Some(Value::List(list)) => {
                if list.is_empty() {
                    data.remove(key);
                    Ok(None)
                } else {
                    Ok(list.pop_back())
                }
            }
            Some(_) => Err("Key exists and is not a list".to_string()),
            None => Ok(None),
        }
    }

    pub async fn lrange(&self, key: &Key, start: i64, stop: i64) -> Result<Vec<String>, String> {
        let data = self.data.read().await;
        
        match data.get(key) {
            Some(Value::List(list)) => {
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
            Some(_) => Err("Key exists and is not a list".to_string()),
            None => Ok(vec![]),
        }
    }

    // Hash operations
    pub async fn hset(&self, key: Key, field: String, value: String) -> Result<bool, String> {
        let mut data = self.data.write().await;
        let hash = data.entry(key).or_insert_with(|| Value::Hash(HashMap::new()));
        
        match hash {
            Value::Hash(hash_map) => {
                let is_new = !hash_map.contains_key(&field);
                hash_map.insert(field, value);
                Ok(is_new)
            }
            _ => Err("Key exists and is not a hash".to_string()),
        }
    }

    pub async fn hget(&self, key: &Key, field: &str) -> Result<Option<String>, String> {
        let data = self.data.read().await;
        
        match data.get(key) {
            Some(Value::Hash(hash_map)) => {
                Ok(hash_map.get(field).cloned())
            }
            Some(_) => Err("Key exists and is not a hash".to_string()),
            None => Ok(None),
        }
    }

    pub async fn hdel(&self, key: &Key, field: &str) -> Result<bool, String> {
        let mut data = self.data.write().await;
        
        match data.get_mut(key) {
            Some(Value::Hash(hash_map)) => {
                let removed = hash_map.remove(field).is_some();
                if hash_map.is_empty() {
                    data.remove(key);
                }
                Ok(removed)
            }
            Some(_) => Err("Key exists and is not a hash".to_string()),
            None => Ok(false),
        }
    }

    pub async fn hgetall(&self, key: &Key) -> Result<HashMap<String, String>, String> {
        let data = self.data.read().await;
        
        match data.get(key) {
            Some(Value::Hash(hash_map)) => {
                Ok(hash_map.clone())
            }
            Some(_) => Err("Key exists and is not a hash".to_string()),
            None => Ok(HashMap::new()),
        }
    }

    // Set operations
    pub async fn sadd(&self, key: Key, member: String) -> Result<bool, String> {
        let mut data = self.data.write().await;
        let set = data.entry(key).or_insert_with(|| Value::Set(HashSet::new()));
        
        match set {
            Value::Set(set) => {
                Ok(set.insert(member))
            }
            _ => Err("Key exists and is not a set".to_string()),
        }
    }

    pub async fn srem(&self, key: &Key, member: &str) -> Result<bool, String> {
        let mut data = self.data.write().await;
        
        match data.get_mut(key) {
            Some(Value::Set(set)) => {
                let removed = set.remove(member);
                if set.is_empty() {
                    data.remove(key);
                }
                Ok(removed)
            }
            Some(_) => Err("Key exists and is not a set".to_string()),
            None => Ok(false),
        }
    }

    pub async fn sismember(&self, key: &Key, member: &str) -> Result<bool, String> {
        let data = self.data.read().await;
        
        match data.get(key) {
            Some(Value::Set(set)) => {
                Ok(set.contains(member))
            }
            Some(_) => Err("Key exists and is not a set".to_string()),
            None => Ok(false),
        }
    }

    pub async fn smembers(&self, key: &Key) -> Result<Vec<String>, String> {
        let data = self.data.read().await;
        
        match data.get(key) {
            Some(Value::Set(set)) => {
                Ok(set.iter().cloned().collect())
            }
            Some(_) => Err("Key exists and is not a set".to_string()),
            None => Ok(vec![]),
        }
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_operations() {
        let store = Store::new();
        
        // Test set and get
        store.set("key1".to_string(), Value::String("value1".to_string())).await;
        assert_eq!(store.get(&"key1".to_string()).await, Some(Value::String("value1".to_string())));
        
        // Test non-existent key
        assert_eq!(store.get(&"nonexistent".to_string()).await, None);
        
        // Test delete
        assert!(store.delete(&"key1".to_string()).await);
        assert_eq!(store.get(&"key1".to_string()).await, None);
        
        // Test delete non-existent key
        assert!(!store.delete(&"nonexistent".to_string()).await);
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let store = Arc::new(Store::new());
        let mut handles = vec![];
        
        // Spawn multiple tasks that set and get values concurrently
        for i in 0..10 {
            let store_clone = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                let key = format!("key{}", i);
                let value = Value::String(format!("value{}", i));
                
                store_clone.set(key.clone(), value.clone()).await;
                let retrieved = store_clone.get(&key).await;
                assert_eq!(retrieved, Some(value));
            });
            handles.push(handle);
        }
        
        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        assert_eq!(store.len().await, 10);
    }

    #[tokio::test]
    async fn test_exists_and_clear() {
        let store = Store::new();
        
        store.set("key1".to_string(), Value::String("value1".to_string())).await;
        assert!(store.exists(&"key1".to_string()).await);
        assert!(!store.exists(&"nonexistent".to_string()).await);
        
        store.clear().await;
        assert_eq!(store.len().await, 0);
        assert!(!store.exists(&"key1".to_string()).await);
    }

    #[tokio::test]
    async fn test_list_operations() {
        let store = Store::new();
        
        // Test lpush
        assert_eq!(store.lpush("mylist".to_string(), "item1".to_string()).await, Ok(1));
        assert_eq!(store.lpush("mylist".to_string(), "item2".to_string()).await, Ok(2));
        
        // Test lrange
        assert_eq!(store.lrange(&"mylist".to_string(), 0, -1).await, Ok(vec!["item2".to_string(), "item1".to_string()]));
        
        // Test lpop
        assert_eq!(store.lpop(&"mylist".to_string()).await, Ok(Some("item2".to_string())));
        assert_eq!(store.lpop(&"mylist".to_string()).await, Ok(Some("item1".to_string())));
        assert_eq!(store.lpop(&"mylist".to_string()).await, Ok(None));
        
        // Test rpush
        assert_eq!(store.rpush("mylist2".to_string(), "item1".to_string()).await, Ok(1));
        assert_eq!(store.rpush("mylist2".to_string(), "item2".to_string()).await, Ok(2));
        
        // Test rpop
        assert_eq!(store.rpop(&"mylist2".to_string()).await, Ok(Some("item2".to_string())));
        assert_eq!(store.rpop(&"mylist2".to_string()).await, Ok(Some("item1".to_string())));
        assert_eq!(store.rpop(&"mylist2".to_string()).await, Ok(None));
    }

    #[tokio::test]
    async fn test_hash_operations() {
        let store = Store::new();
        
        // Test hset
        assert_eq!(store.hset("myhash".to_string(), "field1".to_string(), "value1".to_string()).await, Ok(true));
        assert_eq!(store.hset("myhash".to_string(), "field1".to_string(), "value2".to_string()).await, Ok(false));
        
        // Test hget
        assert_eq!(store.hget(&"myhash".to_string(), "field1").await, Ok(Some("value2".to_string())));
        assert_eq!(store.hget(&"myhash".to_string(), "nonexistent").await, Ok(None));
        
        // Test hgetall
        let all_fields = store.hgetall(&"myhash".to_string()).await.unwrap();
        assert_eq!(all_fields.len(), 1);
        assert_eq!(all_fields.get("field1"), Some(&"value2".to_string()));
        
        // Test hdel
        assert_eq!(store.hdel(&"myhash".to_string(), "field1").await, Ok(true));
        assert_eq!(store.hdel(&"myhash".to_string(), "field1").await, Ok(false));
        assert_eq!(store.hget(&"myhash".to_string(), "field1").await, Ok(None));
    }

    #[tokio::test]
    async fn test_set_operations() {
        let store = Store::new();
        
        // Test sadd
        assert_eq!(store.sadd("myset".to_string(), "member1".to_string()).await, Ok(true));
        assert_eq!(store.sadd("myset".to_string(), "member1".to_string()).await, Ok(false));
        assert_eq!(store.sadd("myset".to_string(), "member2".to_string()).await, Ok(true));
        
        // Test sismember
        assert_eq!(store.sismember(&"myset".to_string(), "member1").await, Ok(true));
        assert_eq!(store.sismember(&"myset".to_string(), "nonexistent").await, Ok(false));
        
        // Test smembers
        let members = store.smembers(&"myset".to_string()).await.unwrap();
        assert_eq!(members.len(), 2);
        assert!(members.contains(&"member1".to_string()));
        assert!(members.contains(&"member2".to_string()));
        
        // Test srem
        assert_eq!(store.srem(&"myset".to_string(), "member1").await, Ok(true));
        assert_eq!(store.srem(&"myset".to_string(), "member1").await, Ok(false));
        assert_eq!(store.sismember(&"myset".to_string(), "member1").await, Ok(false));
    }

    #[tokio::test]
    async fn test_type_conflicts() {
        let store = Store::new();
        
        // Create a string key
        store.set("mykey".to_string(), Value::String("value".to_string())).await;
        
        // Try to use list operations on a string key
        assert!(store.lpush("mykey".to_string(), "item".to_string()).await.is_err());
        assert!(store.hset("mykey".to_string(), "field".to_string(), "value".to_string()).await.is_err());
        assert!(store.sadd("mykey".to_string(), "member".to_string()).await.is_err());
    }
} 