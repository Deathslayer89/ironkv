use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncBufReadExt, BufReader, BufWriter};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use crate::value::Value;

#[derive(Debug, Serialize, Deserialize)]
pub enum PersistenceCommand {
    Set { key: String, value: Value, ttl: Option<u64> },
    Delete { key: String },
    Lpush { key: String, value: String },
    Rpush { key: String, value: String },
    Lpop { key: String },
    Rpop { key: String },
    Hset { key: String, field: String, value: String },
    Hdel { key: String, field: String },
    Sadd { key: String, member: String },
    Srem { key: String, member: String },
}

#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    pub aof_enabled: bool,
    pub aof_path: PathBuf,
    pub aof_rewrite_percentage: f64, // Rewrite when AOF is X% larger than snapshot
    pub snapshot_enabled: bool,
    pub snapshot_path: PathBuf,
    pub snapshot_interval_seconds: u64,
    pub snapshot_compression: bool,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            aof_enabled: true,
            aof_path: PathBuf::from("cache.aof"),
            aof_rewrite_percentage: 100.0, // Rewrite when AOF is 100% larger
            snapshot_enabled: true,
            snapshot_path: PathBuf::from("cache.rdb"),
            snapshot_interval_seconds: 300, // 5 minutes
            snapshot_compression: false,
        }
    }
}

// Custom serializable data structure for persistence
#[derive(Debug, Serialize, Deserialize)]
struct SerializableData {
    key: String,
    value: Value,
    ttl_seconds: Option<u64>,
}

#[derive(Debug)]
pub struct PersistenceStore {
    data: Arc<RwLock<HashMap<String, (Value, Option<std::time::Instant>)>>>,
    config: PersistenceConfig,
    aof_writer: Arc<RwLock<Option<BufWriter<File>>>>,
    aof_rewrite_task: Option<tokio::task::JoinHandle<()>>,
    snapshot_task: Option<tokio::task::JoinHandle<()>>,
}

impl PersistenceStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            config: PersistenceConfig::default(),
            aof_writer: Arc::new(RwLock::new(None)),
            aof_rewrite_task: None,
            snapshot_task: None,
        }
    }

    pub fn with_config(mut self, config: PersistenceConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Load from snapshot first if available
        if self.config.snapshot_enabled && self.config.snapshot_path.exists() {
            self.load_snapshot().await?;
        }

        // Then replay AOF if available
        if self.config.aof_enabled && self.config.aof_path.exists() {
            self.replay_aof().await?;
        }

        // Initialize AOF writer
        if self.config.aof_enabled {
            self.initialize_aof_writer().await?;
        }

        // Start background tasks
        self.start_background_tasks().await;

        Ok(())
    }

    async fn initialize_aof_writer(&self) -> Result<(), Box<dyn std::error::Error>> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.config.aof_path)
            .await?;
        
        let writer = BufWriter::new(file);
        let mut aof_writer = self.aof_writer.write().await;
        *aof_writer = Some(writer);
        
        Ok(())
    }

    async fn start_background_tasks(&mut self) {
        // Start AOF rewrite task
        if self.config.aof_enabled {
            let data_clone = Arc::clone(&self.data);
            let config = self.config.clone();
            let aof_writer = Arc::clone(&self.aof_writer);
            
            self.aof_rewrite_task = Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(
                    std::time::Duration::from_secs(60) // Check every minute
                );
                
                loop {
                    interval.tick().await;
                    if let Err(e) = Self::check_and_rewrite_aof(&data_clone, &config, &aof_writer).await {
                        eprintln!("AOF rewrite error: {}", e);
                    }
                }
            }));
        }

        // Start snapshot task
        if self.config.snapshot_enabled {
            let data_clone = Arc::clone(&self.data);
            let config = self.config.clone();
            
            self.snapshot_task = Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(
                    std::time::Duration::from_secs(config.snapshot_interval_seconds)
                );
                
                loop {
                    interval.tick().await;
                    if let Err(e) = Self::create_snapshot(&data_clone, &config).await {
                        eprintln!("Snapshot error: {}", e);
                    }
                }
            }));
        }
    }

    pub async fn set(&self, key: String, value: Value, ttl_seconds: Option<u64>) -> Result<(), Box<dyn std::error::Error>> {
        let mut data = self.data.write().await;
        let expiration = ttl_seconds.map(|seconds| {
            std::time::Instant::now() + std::time::Duration::from_secs(seconds)
        });
        data.insert(key.clone(), (value.clone(), expiration));

        // Write to AOF
        if self.config.aof_enabled {
            let command = PersistenceCommand::Set {
                key: key.clone(),
                value: value.clone(),
                ttl: ttl_seconds,
            };
            self.write_aof_command(&command).await?;
        }

        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let mut data = self.data.write().await;
        let existed = data.remove(key).is_some();

        if existed && self.config.aof_enabled {
            let command = PersistenceCommand::Delete {
                key: key.to_string(),
            };
            self.write_aof_command(&command).await?;
        }

        Ok(existed)
    }

    pub async fn get(&self, key: &str) -> Option<Value> {
        let mut data = self.data.write().await;
        
        if let Some((value, expiration)) = data.get(key) {
            if let Some(exp) = expiration {
                if *exp <= std::time::Instant::now() {
                    data.remove(key);
                    return None;
                }
            }
            Some(value.clone())
        } else {
            None
        }
    }

    pub async fn exists(&self, key: &str) -> bool {
        let mut data = self.data.write().await;
        
        if let Some((_, expiration)) = data.get(key) {
            if let Some(exp) = expiration {
                if *exp <= std::time::Instant::now() {
                    data.remove(key);
                    return false;
                }
            }
            true
        } else {
            false
        }
    }

    pub async fn len(&self) -> usize {
        let data = self.data.read().await;
        data.len()
    }

    pub async fn clear(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut data = self.data.write().await;
        data.clear();

        if self.config.aof_enabled {
            // Write a special clear command or truncate the file
            // For simplicity, we'll just truncate the AOF file
            self.truncate_aof().await?;
        }

        Ok(())
    }

    async fn write_aof_command(&self, command: &PersistenceCommand) -> Result<(), Box<dyn std::error::Error>> {
        let serialized = bincode::serialize(command)?;
        let mut aof_writer = self.aof_writer.write().await;
        
        if let Some(writer) = aof_writer.as_mut() {
            writer.write_all(&serialized).await?;
            writer.write_all(b"\n").await?; // Add newline for readability
            writer.flush().await?;
        }
        
        Ok(())
    }

    async fn truncate_aof(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Close current writer
        let mut aof_writer = self.aof_writer.write().await;
        *aof_writer = None;
        
        // Truncate the file
        tokio::fs::write(&self.config.aof_path, b"").await?;
        
        // Reinitialize writer
        self.initialize_aof_writer().await?;
        
        Ok(())
    }

    async fn replay_aof(&self) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::open(&self.config.aof_path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }

            let command: PersistenceCommand = bincode::deserialize(line.as_bytes())?;
            self.replay_command(&command).await?;
        }

        Ok(())
    }

    async fn replay_command(&self, command: &PersistenceCommand) -> Result<(), Box<dyn std::error::Error>> {
        let mut data = self.data.write().await;
        
        match command {
            PersistenceCommand::Set { key, value, ttl } => {
                let expiration = ttl.map(|seconds| {
                    std::time::Instant::now() + std::time::Duration::from_secs(seconds)
                });
                data.insert(key.clone(), (value.clone(), expiration));
            }
            PersistenceCommand::Delete { key } => {
                data.remove(key);
            }
            PersistenceCommand::Lpush { key, value } => {
                let entry = data.entry(key.clone()).or_insert_with(|| {
                    (Value::List(std::collections::VecDeque::new()), None)
                });
                if let Value::List(list) = &mut entry.0 {
                    list.push_front(value.clone());
                }
            }
            PersistenceCommand::Rpush { key, value } => {
                let entry = data.entry(key.clone()).or_insert_with(|| {
                    (Value::List(std::collections::VecDeque::new()), None)
                });
                if let Value::List(list) = &mut entry.0 {
                    list.push_back(value.clone());
                }
            }
            PersistenceCommand::Lpop { key } => {
                if let Some((Value::List(list), _)) = data.get_mut(key) {
                    list.pop_front();
                    if list.is_empty() {
                        data.remove(key);
                    }
                }
            }
            PersistenceCommand::Rpop { key } => {
                if let Some((Value::List(list), _)) = data.get_mut(key) {
                    list.pop_back();
                    if list.is_empty() {
                        data.remove(key);
                    }
                }
            }
            PersistenceCommand::Hset { key, field, value } => {
                let entry = data.entry(key.clone()).or_insert_with(|| {
                    (Value::Hash(HashMap::new()), None)
                });
                if let Value::Hash(hash) = &mut entry.0 {
                    hash.insert(field.clone(), value.clone());
                }
            }
            PersistenceCommand::Hdel { key, field } => {
                if let Some((Value::Hash(hash), _)) = data.get_mut(key) {
                    hash.remove(field);
                    if hash.is_empty() {
                        data.remove(key);
                    }
                }
            }
            PersistenceCommand::Sadd { key, member } => {
                let entry = data.entry(key.clone()).or_insert_with(|| {
                    (Value::Set(std::collections::HashSet::new()), None)
                });
                if let Value::Set(set) = &mut entry.0 {
                    set.insert(member.clone());
                }
            }
            PersistenceCommand::Srem { key, member } => {
                if let Some((Value::Set(set), _)) = data.get_mut(key) {
                    set.remove(member);
                    if set.is_empty() {
                        data.remove(key);
                    }
                }
            }
        }
        
        Ok(())
    }

    async fn create_snapshot(data: &Arc<RwLock<HashMap<String, (Value, Option<std::time::Instant>)>>>, 
                           config: &PersistenceConfig) -> Result<(), Box<dyn std::error::Error>> {
        let data = data.read().await;
        
        // Create a temporary file first
        let mut temp_path = config.snapshot_path.clone();
        temp_path.set_extension("tmp");
        let file = File::create(&temp_path).await?;
        let mut writer = BufWriter::new(file);
        
        // Convert to serializable format
        let serializable_data: Vec<SerializableData> = data.iter().map(|(key, (value, ttl))| {
            let ttl_seconds = ttl.map(|exp| {
                exp.duration_since(std::time::Instant::now()).as_secs()
            });
            SerializableData {
                key: key.clone(),
                value: value.clone(),
                ttl_seconds,
            }
        }).collect();
        
        // Serialize the data
        let serialized = bincode::serialize(&serializable_data)?;
        writer.write_all(&serialized).await?;
        writer.flush().await?;
        
        // Atomically replace the old snapshot
        tokio::fs::rename(&temp_path, &config.snapshot_path).await?;
        
        println!("Snapshot created: {} keys", data.len());
        Ok(())
    }

    async fn load_snapshot(&self) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::open(&self.config.snapshot_path).await?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;
        
        let serializable_data: Vec<SerializableData> = bincode::deserialize(&buffer)?;
        
        let mut store_data = self.data.write().await;
        store_data.clear();
        
        for item in serializable_data {
            let expiration = item.ttl_seconds.map(|seconds| {
                std::time::Instant::now() + std::time::Duration::from_secs(seconds)
            });
            store_data.insert(item.key, (item.value, expiration));
        }
        
        println!("Snapshot loaded: {} keys", store_data.len());
        Ok(())
    }

    async fn check_and_rewrite_aof(data: &Arc<RwLock<HashMap<String, (Value, Option<std::time::Instant>)>>>,
                                  config: &PersistenceConfig,
                                  aof_writer: &Arc<RwLock<Option<BufWriter<File>>>>) -> Result<(), Box<dyn std::error::Error>> {
        // Check if AOF needs rewriting
        if !config.aof_path.exists() {
            return Ok(());
        }

        let aof_size = tokio::fs::metadata(&config.aof_path).await?.len() as usize;
        let data = data.read().await;
        
        // Convert to serializable format for size estimation
        let serializable_data: Vec<SerializableData> = data.iter().map(|(key, (value, ttl))| {
            let ttl_seconds = ttl.map(|exp| {
                exp.duration_since(std::time::Instant::now()).as_secs()
            });
            SerializableData {
                key: key.clone(),
                value: value.clone(),
                ttl_seconds,
            }
        }).collect();
        
        let estimated_snapshot_size = bincode::serialized_size(&serializable_data)? as usize;

        // If AOF is significantly larger than snapshot, rewrite it
        let threshold = estimated_snapshot_size as f64 * (1.0 + config.aof_rewrite_percentage / 100.0);
        if aof_size as f64 > threshold {
            Self::rewrite_aof(&data, config, aof_writer).await?;
        }

        Ok(())
    }

    async fn rewrite_aof(data: &HashMap<String, (Value, Option<std::time::Instant>)>,
                        config: &PersistenceConfig,
                        aof_writer: &Arc<RwLock<Option<BufWriter<File>>>>) -> Result<(), Box<dyn std::error::Error>> {
        // Create a new AOF file
        let mut new_aof_path = config.aof_path.clone();
        new_aof_path.set_extension("new");
        let file = File::create(&new_aof_path).await?;
        let mut writer = BufWriter::new(file);

        // Write current state as commands
        for (key, (value, ttl)) in data {
            let ttl_seconds = ttl.map(|exp| {
                exp.duration_since(std::time::Instant::now()).as_secs()
            });

            let command = PersistenceCommand::Set {
                key: key.clone(),
                value: value.clone(),
                ttl: ttl_seconds,
            };

            let serialized = bincode::serialize(&command)?;
            writer.write_all(&serialized).await?;
            writer.write_all(b"\n").await?;
        }

        writer.flush().await?;

        // Atomically replace the old AOF
        tokio::fs::rename(&new_aof_path, &config.aof_path).await?;

        // Reinitialize the AOF writer
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.aof_path)
            .await?;
        
        let writer = BufWriter::new(file);
        let mut aof_writer = aof_writer.write().await;
        *aof_writer = Some(writer);

        println!("AOF rewritten: {} keys", data.len());
        Ok(())
    }

    pub async fn flush(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.config.aof_enabled {
            let mut aof_writer = self.aof_writer.write().await;
            if let Some(writer) = aof_writer.as_mut() {
                writer.flush().await?;
            }
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Flush any pending writes
        self.flush().await?;

        // Cancel background tasks
        if let Some(task) = self.aof_rewrite_task.take() {
            task.abort();
        }
        if let Some(task) = self.snapshot_task.take() {
            task.abort();
        }

        Ok(())
    }
}

impl Default for PersistenceStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for PersistenceStore {
    fn drop(&mut self) {
        if let Some(task) = self.aof_rewrite_task.take() {
            task.abort();
        }
        if let Some(task) = self.snapshot_task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_basic_persistence_operations() {
        let temp_dir = tempdir().unwrap();
        let mut config = PersistenceConfig::default();
        config.aof_path = temp_dir.path().join("test.aof");
        config.snapshot_path = temp_dir.path().join("test.rdb");
        
        let mut store = PersistenceStore::new().with_config(config);
        store.initialize().await.unwrap();

        // Test basic operations
        store.set("key1".to_string(), Value::String("value1".to_string()), None).await.unwrap();
        store.set("key2".to_string(), Value::String("value2".to_string()), Some(10)).await.unwrap();
        
        assert_eq!(store.get("key1").await, Some(Value::String("value1".to_string())));
        assert_eq!(store.get("key2").await, Some(Value::String("value2".to_string())));
        
        // Test delete
        assert!(store.delete("key1").await.unwrap());
        assert_eq!(store.get("key1").await, None);
    }

    #[tokio::test]
    async fn test_aof_replay() {
        let temp_dir = tempdir().unwrap();
        let mut config = PersistenceConfig::default();
        config.aof_path = temp_dir.path().join("test.aof");
        config.snapshot_path = temp_dir.path().join("test.rdb");
        
        let mut store = PersistenceStore::new().with_config(config.clone());
        store.initialize().await.unwrap();

        // Add some data
        store.set("key1".to_string(), Value::String("value1".to_string()), None).await.unwrap();
        store.set("key2".to_string(), Value::String("value2".to_string()), None).await.unwrap();
        store.delete("key1").await.unwrap();

        // Create a new store and replay AOF
        let mut new_store = PersistenceStore::new().with_config(config);
        new_store.initialize().await.unwrap();

        // Check that data was replayed correctly
        assert_eq!(new_store.get("key1").await, None); // Was deleted
        assert_eq!(new_store.get("key2").await, Some(Value::String("value2".to_string())));
    }

    #[tokio::test]
    async fn test_snapshot_creation_and_loading() {
        let temp_dir = tempdir().unwrap();
        let mut config = PersistenceConfig::default();
        config.aof_path = temp_dir.path().join("test.aof");
        config.snapshot_path = temp_dir.path().join("test.rdb");
        
        let mut store = PersistenceStore::new().with_config(config.clone());
        store.initialize().await.unwrap();

        // Add some data
        store.set("key1".to_string(), Value::String("value1".to_string()), None).await.unwrap();
        store.set("key2".to_string(), Value::String("value2".to_string()), None).await.unwrap();

        // Manually create a snapshot
        PersistenceStore::create_snapshot(&store.data, &config).await.unwrap();

        // Create a new store and load snapshot
        let mut new_store = PersistenceStore::new().with_config(config);
        new_store.initialize().await.unwrap();

        // Check that data was loaded correctly
        assert_eq!(new_store.get("key1").await, Some(Value::String("value1".to_string())));
        assert_eq!(new_store.get("key2").await, Some(Value::String("value2".to_string())));
    }

    #[tokio::test]
    async fn test_complex_data_structures() {
        let temp_dir = tempdir().unwrap();
        let mut config = PersistenceConfig::default();
        config.aof_path = temp_dir.path().join("test.aof");
        config.snapshot_path = temp_dir.path().join("test.rdb");
        
        let mut store = PersistenceStore::new().with_config(config);
        store.initialize().await.unwrap();

        // Test list operations
        let command = PersistenceCommand::Lpush {
            key: "mylist".to_string(),
            value: "item1".to_string(),
        };
        store.replay_command(&command).await.unwrap();

        let command = PersistenceCommand::Rpush {
            key: "mylist".to_string(),
            value: "item2".to_string(),
        };
        store.replay_command(&command).await.unwrap();

        // Test hash operations
        let command = PersistenceCommand::Hset {
            key: "myhash".to_string(),
            field: "field1".to_string(),
            value: "value1".to_string(),
        };
        store.replay_command(&command).await.unwrap();

        // Test set operations
        let command = PersistenceCommand::Sadd {
            key: "myset".to_string(),
            member: "member1".to_string(),
        };
        store.replay_command(&command).await.unwrap();

        // Verify data
        let data = store.data.read().await;
        assert!(data.contains_key("mylist"));
        assert!(data.contains_key("myhash"));
        assert!(data.contains_key("myset"));
    }

    #[tokio::test]
    async fn test_ttl_persistence() {
        let temp_dir = tempdir().unwrap();
        let mut config = PersistenceConfig::default();
        config.aof_path = temp_dir.path().join("test.aof");
        config.snapshot_path = temp_dir.path().join("test.rdb");
        
        let mut store = PersistenceStore::new().with_config(config);
        store.initialize().await.unwrap();

        // Add data with TTL
        store.set("key1".to_string(), Value::String("value1".to_string()), Some(1)).await.unwrap();
        store.set("key2".to_string(), Value::String("value2".to_string()), None).await.unwrap();

        // Wait for key1 to expire
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Check that expired key is not returned
        assert_eq!(store.get("key1").await, None);
        assert_eq!(store.get("key2").await, Some(Value::String("value2".to_string())));
    }

    #[tokio::test]
    async fn test_concurrent_access_with_persistence() {
        let temp_dir = tempdir().unwrap();
        let mut config = PersistenceConfig::default();
        config.aof_path = temp_dir.path().join("test.aof");
        config.snapshot_path = temp_dir.path().join("test.rdb");
        
        let mut store = PersistenceStore::new().with_config(config);
        store.initialize().await.unwrap();

        let store = Arc::new(store);
        let mut handles = vec![];

        // Spawn multiple tasks
        for i in 0..10 {
            let store_clone = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                let key = format!("key{}", i);
                let value = Value::String(format!("value{}", i));
                
                store_clone.set(key.clone(), value.clone(), None).await.unwrap();
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