use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type Key = String;
pub type Value = String;

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
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_basic_operations() {
        let store = Store::new();
        
        // Test set and get
        store.set("key1".to_string(), "value1".to_string()).await;
        assert_eq!(store.get(&"key1".to_string()).await, Some("value1".to_string()));
        
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
                let value = format!("value{}", i);
                
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
        
        store.set("key1".to_string(), "value1".to_string()).await;
        assert!(store.exists(&"key1".to_string()).await);
        assert!(!store.exists(&"nonexistent".to_string()).await);
        
        store.clear().await;
        assert_eq!(store.len().await, 0);
        assert!(!store.exists(&"key1".to_string()).await);
    }
} 