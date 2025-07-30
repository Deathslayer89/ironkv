use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::value::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum EvictionPolicy {
    NoEviction,
    LRU,
    LFU,
    Random,
}

#[derive(Debug)]
pub struct EvictionConfig {
    pub policy: EvictionPolicy,
    pub max_memory_bytes: Option<usize>,
    pub max_keys: Option<usize>,
    pub max_memory_percentage: Option<f64>, // Percentage of system memory
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            policy: EvictionPolicy::NoEviction,
            max_memory_bytes: None,
            max_keys: None,
            max_memory_percentage: None,
        }
    }
}

#[derive(Debug)]
pub struct EvictionStore {
    data: Arc<RwLock<HashMap<String, (Value, Option<std::time::Instant>)>>>,
    lru_queue: Arc<RwLock<VecDeque<String>>>,
    access_times: Arc<RwLock<HashMap<String, std::time::Instant>>>,
    config: EvictionConfig,
    current_memory_usage: Arc<RwLock<usize>>,
}

impl EvictionStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            lru_queue: Arc::new(RwLock::new(VecDeque::new())),
            access_times: Arc::new(RwLock::new(HashMap::new())),
            config: EvictionConfig::default(),
            current_memory_usage: Arc::new(RwLock::new(0)),
        }
    }

    pub fn with_config(mut self, config: EvictionConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn set(&self, key: String, value: Value, ttl_seconds: Option<u64>) -> bool {
        // Calculate memory usage of the new entry first
        let entry_size = self.estimate_entry_size(&key, &value);
        
        // Check if we need to evict before adding
        if !self.should_allow_entry(entry_size).await {
            if !self.evict_entries(entry_size).await {
                return false; // Could not make space
            }
        }

        // Now acquire all locks in a consistent order to prevent deadlock
        let mut data = self.data.write().await;
        let mut lru_queue = self.lru_queue.write().await;
        let mut access_times = self.access_times.write().await;
        let mut memory_usage = self.current_memory_usage.write().await;

        // Remove old entry if it exists
        if let Some((old_value, _)) = data.get(&key) {
            let old_size = self.estimate_entry_size(&key, old_value);
            *memory_usage = memory_usage.saturating_sub(old_size);
            
            // Remove from LRU queue
            if let Some(pos) = lru_queue.iter().position(|k| k == &key) {
                lru_queue.remove(pos);
            }
        }

        // Add new entry
        let expiration = ttl_seconds.map(|seconds| {
            std::time::Instant::now() + std::time::Duration::from_secs(seconds)
        });
        
        data.insert(key.clone(), (value, expiration));
        lru_queue.push_back(key.clone());
        access_times.insert(key, std::time::Instant::now());
        *memory_usage += entry_size;

        true
    }

    pub async fn get(&self, key: &str) -> Option<Value> {
        // Acquire locks in consistent order
        let mut data = self.data.write().await;
        let mut lru_queue = self.lru_queue.write().await;
        let mut access_times = self.access_times.write().await;

        if let Some((value, expiration)) = data.get(key) {
            // Check TTL
            if let Some(exp) = expiration {
                if *exp <= std::time::Instant::now() {
                    // Key has expired, remove it
                    data.remove(key);
                    if let Some(pos) = lru_queue.iter().position(|k| k == key) {
                        lru_queue.remove(pos);
                    }
                    access_times.remove(key);
                    return None;
                }
            }

            // Update LRU access time
            if let Some(pos) = lru_queue.iter().position(|k| k == key) {
                lru_queue.remove(pos);
                lru_queue.push_back(key.to_string());
            }
            access_times.insert(key.to_string(), std::time::Instant::now());

            Some(value.clone())
        } else {
            None
        }
    }

    pub async fn delete(&self, key: &str) -> bool {
        // Acquire locks in consistent order
        let mut data = self.data.write().await;
        let mut lru_queue = self.lru_queue.write().await;
        let mut access_times = self.access_times.write().await;
        let mut memory_usage = self.current_memory_usage.write().await;

        if let Some((value, _)) = data.remove(key) {
            let entry_size = self.estimate_entry_size(key, &value);
            *memory_usage = memory_usage.saturating_sub(entry_size);
            
            // Remove from LRU queue
            if let Some(pos) = lru_queue.iter().position(|k| k == key) {
                lru_queue.remove(pos);
            }
            access_times.remove(key);
            true
        } else {
            false
        }
    }

    pub async fn exists(&self, key: &str) -> bool {
        // Acquire locks in consistent order
        let mut data = self.data.write().await;
        let mut lru_queue = self.lru_queue.write().await;
        let mut access_times = self.access_times.write().await;

        if let Some((_, expiration)) = data.get(key) {
            // Check TTL
            if let Some(exp) = expiration {
                if *exp <= std::time::Instant::now() {
                    // Key has expired, remove it
                    data.remove(key);
                    if let Some(pos) = lru_queue.iter().position(|k| k == key) {
                        lru_queue.remove(pos);
                    }
                    access_times.remove(key);
                    return false;
                }
            }

            // Update LRU access time
            if let Some(pos) = lru_queue.iter().position(|k| k == key) {
                lru_queue.remove(pos);
                lru_queue.push_back(key.to_string());
            }
            access_times.insert(key.to_string(), std::time::Instant::now());

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
        // Acquire locks in consistent order
        let mut data = self.data.write().await;
        let mut lru_queue = self.lru_queue.write().await;
        let mut access_times = self.access_times.write().await;
        let mut memory_usage = self.current_memory_usage.write().await;

        data.clear();
        lru_queue.clear();
        access_times.clear();
        *memory_usage = 0;
    }

    pub async fn get_memory_usage(&self) -> usize {
        let memory_usage = self.current_memory_usage.read().await;
        *memory_usage
    }

    pub async fn get_max_memory(&self) -> Option<usize> {
        if let Some(max_bytes) = self.config.max_memory_bytes {
            return Some(max_bytes);
        }

        if let Some(percentage) = self.config.max_memory_percentage {
            // Get system memory info
            if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
                for line in meminfo.lines() {
                    if line.starts_with("MemTotal:") {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<usize>() {
                                return Some((kb * 1024) as usize * percentage as usize / 100);
                            }
                        }
                    }
                }
            }
        }

        None
    }

    pub fn set_max_memory(&mut self, max_memory_bytes: usize) {
        self.config.max_memory_bytes = Some(max_memory_bytes);
    }

    pub fn set_max_keys(&mut self, max_keys: usize) {
        self.config.max_keys = Some(max_keys);
    }

    pub fn set_eviction_policy(&mut self, policy: EvictionPolicy) {
        self.config.policy = policy;
    }

    async fn should_allow_entry(&self, entry_size: usize) -> bool {
        let current_usage = self.current_memory_usage.read().await;
        let new_usage = *current_usage + entry_size;

        // Check memory limit
        if let Some(max_memory) = self.get_max_memory().await {
            if new_usage > max_memory {
                return false;
            }
        }

        // Check key limit
        if let Some(max_keys) = self.config.max_keys {
            let current_keys = self.len().await;
            if current_keys >= max_keys {
                return false;
            }
        }

        true
    }

    async fn evict_entries(&self, required_space: usize) -> bool {
        match self.config.policy {
            EvictionPolicy::NoEviction => false,
            EvictionPolicy::LRU => self.evict_lru(required_space).await,
            EvictionPolicy::LFU => self.evict_lfu(required_space).await,
            EvictionPolicy::Random => self.evict_random(required_space).await,
        }
    }

    async fn evict_lru(&self, required_space: usize) -> bool {
        // Acquire locks in consistent order
        let mut data = self.data.write().await;
        let mut lru_queue = self.lru_queue.write().await;
        let mut access_times = self.access_times.write().await;
        let mut memory_usage = self.current_memory_usage.write().await;

        let mut freed_space = 0;
        let mut keys_to_remove = Vec::new();

        // Remove least recently used keys until we have enough space
        while freed_space < required_space && !lru_queue.is_empty() {
            if let Some(key) = lru_queue.pop_front() {
                if let Some((value, _)) = data.get(&key) {
                    let entry_size = self.estimate_entry_size(&key, value);
                    freed_space += entry_size;
                    keys_to_remove.push(key);
                }
            }
        }

        // Remove the keys
        for key in keys_to_remove {
            if let Some((value, _)) = data.remove(&key) {
                let entry_size = self.estimate_entry_size(&key, &value);
                *memory_usage = memory_usage.saturating_sub(entry_size);
                access_times.remove(&key);
            }
        }

        freed_space >= required_space
    }

    async fn evict_lfu(&self, required_space: usize) -> bool {
        // For now, implement LFU as random eviction
        // A proper LFU implementation would require frequency counters
        self.evict_random(required_space).await
    }

    async fn evict_random(&self, required_space: usize) -> bool {
        // Acquire locks in consistent order
        let mut data = self.data.write().await;
        let mut lru_queue = self.lru_queue.write().await;
        let mut access_times = self.access_times.write().await;
        let mut memory_usage = self.current_memory_usage.write().await;

        let mut keys: Vec<String> = data.keys().cloned().collect();
        if keys.is_empty() {
            return false;
        }

        let mut freed_space = 0;
        let mut keys_to_remove = Vec::new();

        // Randomly select keys to evict
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        std::time::Instant::now().hash(&mut hasher);
        let mut seed = hasher.finish();

        while freed_space < required_space && !keys.is_empty() {
            // Simple pseudo-random selection
            seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
            let index = (seed as usize) % keys.len();
            let key = keys.remove(index);

            if let Some((value, _)) = data.get(&key) {
                let entry_size = self.estimate_entry_size(&key, value);
                freed_space += entry_size;
                keys_to_remove.push(key);
            }
        }

        // Remove the keys
        for key in keys_to_remove {
            if let Some((value, _)) = data.remove(&key) {
                let entry_size = self.estimate_entry_size(&key, &value);
                *memory_usage = memory_usage.saturating_sub(entry_size);
                
                // Remove from LRU queue
                if let Some(pos) = lru_queue.iter().position(|k| k == &key) {
                    lru_queue.remove(pos);
                }
                access_times.remove(&key);
            }
        }

        freed_space >= required_space
    }

    fn estimate_entry_size(&self, key: &str, value: &Value) -> usize {
        let key_size = key.len();
        let value_size = match value {
            Value::String(s) => s.len(),
            Value::List(list) => list.iter().map(|s| s.len()).sum::<usize>(),
            Value::Hash(hash) => hash.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>(),
            Value::Set(set) => set.iter().map(|s| s.len()).sum::<usize>(),
        };
        
        // Add overhead for data structures
        let overhead = match value {
            Value::String(_) => 0,
            Value::List(list) => list.len() * 8, // Approximate pointer overhead
            Value::Hash(hash) => hash.len() * 16, // Approximate hash table overhead
            Value::Set(set) => set.len() * 8, // Approximate set overhead
        };

        key_size + value_size + overhead + 32 // Additional overhead for HashMap entry
    }
}

impl Default for EvictionStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_basic_eviction_operations() {
        let store = EvictionStore::new();
        
        // Test basic set/get
        assert!(store.set("key1".to_string(), Value::String("value1".to_string()), None).await);
        assert_eq!(store.get("key1").await, Some(Value::String("value1".to_string())));
        
        // Test delete
        assert!(store.delete("key1").await);
        assert_eq!(store.get("key1").await, None);
    }

    #[tokio::test]
    async fn test_memory_limit_eviction() {
        let mut store = EvictionStore::new();
        store.set_max_memory(1000); // 1KB limit
        store.set_eviction_policy(EvictionPolicy::LRU); // Enable eviction
        
        // Add keys until we hit the limit
        let mut i = 0;
        while store.set(
            format!("key{}", i),
            Value::String("a".repeat(100)), // ~100 bytes per entry
            None
        ).await {
            i += 1;
            if i > 20 { break; } // Safety break
        }
        
        // Should have evicted some keys
        assert!(store.len().await < i);
        assert!(store.get_memory_usage().await <= 1000);
    }

    #[tokio::test]
    async fn test_key_limit_eviction() {
        let mut store = EvictionStore::new();
        store.set_max_keys(5);
        store.set_eviction_policy(EvictionPolicy::LRU);
        
        // Add 10 keys
        for i in 0..10 {
            store.set(
                format!("key{}", i),
                Value::String(format!("value{}", i)),
                None
            ).await;
        }
        
        // Should have evicted some keys due to LRU
        assert_eq!(store.len().await, 5);
        
        // Most recent keys should still exist
        assert!(store.exists("key9").await);
        assert!(store.exists("key8").await);
        assert!(store.exists("key7").await);
        assert!(store.exists("key6").await);
        assert!(store.exists("key5").await);
        
        // Oldest keys should be evicted
        assert!(!store.exists("key0").await);
        assert!(!store.exists("key1").await);
        assert!(!store.exists("key2").await);
        assert!(!store.exists("key3").await);
        assert!(!store.exists("key4").await);
    }

    #[tokio::test]
    async fn test_lru_eviction() {
        let mut store = EvictionStore::new();
        store.set_max_keys(3);
        store.set_eviction_policy(EvictionPolicy::LRU);
        
        // Add 3 keys
        store.set("key1".to_string(), Value::String("value1".to_string()), None).await;
        store.set("key2".to_string(), Value::String("value2".to_string()), None).await;
        store.set("key3".to_string(), Value::String("value3".to_string()), None).await;
        
        // Access key1 to make it most recently used
        store.get("key1").await;
        
        // Add a new key, should evict key2 (least recently used)
        store.set("key4".to_string(), Value::String("value4".to_string()), None).await;
        
        assert_eq!(store.len().await, 3);
        assert!(store.exists("key1").await); // Most recently used
        assert!(!store.exists("key2").await); // Least recently used, evicted
        assert!(store.exists("key3").await);
        assert!(store.exists("key4").await);
    }

    #[tokio::test]
    async fn test_no_eviction_policy() {
        let mut store = EvictionStore::new();
        store.set_max_keys(1);
        store.set_eviction_policy(EvictionPolicy::NoEviction);
        
        // Add first key
        assert!(store.set("key1".to_string(), Value::String("value1".to_string()), None).await);
        
        // Try to add second key, should fail
        assert!(!store.set("key2".to_string(), Value::String("value2".to_string()), None).await);
        
        // First key should still exist
        assert!(store.exists("key1").await);
        assert!(!store.exists("key2").await);
    }

    #[tokio::test]
    async fn test_memory_usage_tracking() {
        let store = EvictionStore::new();
        
        // Initial memory usage should be 0
        assert_eq!(store.get_memory_usage().await, 0);
        
        // Add a key and check memory usage
        store.set("key1".to_string(), Value::String("value1".to_string()), None).await;
        let usage_after_set = store.get_memory_usage().await;
        assert!(usage_after_set > 0);
        
        // Delete the key and check memory usage
        store.delete("key1").await;
        assert_eq!(store.get_memory_usage().await, 0);
    }

    #[tokio::test]
    async fn test_ttl_with_eviction() {
        let mut store = EvictionStore::new();
        store.set_max_keys(5);
        store.set_eviction_policy(EvictionPolicy::LRU);
        
        // Add keys with TTL
        store.set("key1".to_string(), Value::String("value1".to_string()), Some(1)).await;
        store.set("key2".to_string(), Value::String("value2".to_string()), None).await;
        
        // Wait for key1 to expire
        tokio::time::sleep(Duration::from_secs(2)).await;
        
        // key1 should be expired and removed
        assert!(!store.exists("key1").await);
        assert!(store.exists("key2").await);
        
        // Memory usage should be reduced
        let usage = store.get_memory_usage().await;
        assert!(usage > 0); // key2 should still exist
    }

    #[tokio::test]
    async fn test_concurrent_access_with_eviction() {
        let store = Arc::new(EvictionStore::new());
        let mut handles = vec![];
        
        // Spawn multiple tasks that set and get values
        for i in 0..10 {
            let store_clone = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                let key = format!("key{}", i);
                let value = Value::String(format!("value{}", i));
                
                store_clone.set(key.clone(), value.clone(), None).await;
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
} 