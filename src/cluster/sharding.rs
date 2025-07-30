//! Consistent hashing for sharding
//! 
//! This module implements consistent hashing to distribute keys across
//! cluster nodes, enabling horizontal scaling and load balancing.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::cluster::communication::{KvCacheClient, kv_cache::NodeInfo};

/// Virtual node representation for better distribution
#[derive(Debug, Clone)]
pub struct VirtualNode {
    pub node_id: String,
    pub virtual_id: u32,
    pub hash: u64,
}

/// Consistent hash ring for key distribution
pub struct ConsistentHashRing {
    ring: BTreeMap<u64, VirtualNode>,
    virtual_nodes_per_physical: usize,
}

/// Shard manager that handles key-to-node mapping
pub struct ShardManager {
    hash_ring: Arc<RwLock<ConsistentHashRing>>,
    node_clients: Arc<RwLock<HashMap<String, KvCacheClient>>>,
    local_node_id: String,
}

/// Status information about sharding
#[derive(Debug, Clone)]
pub struct ShardStatus {
    pub total_virtual_nodes: usize,
    pub physical_nodes: usize,
    pub local_node_id: String,
}

impl ConsistentHashRing {
    pub fn new(virtual_nodes_per_physical: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            virtual_nodes_per_physical,
        }
    }

    /// Add a physical node to the hash ring
    pub fn add_node(&mut self, node_id: &str) {
        for i in 0..self.virtual_nodes_per_physical {
            let virtual_node = VirtualNode {
                node_id: node_id.to_string(),
                virtual_id: i as u32,
                hash: self.hash(&format!("{}:{}", node_id, i)),
            };
            self.ring.insert(virtual_node.hash, virtual_node);
        }
    }

    /// Remove a physical node from the hash ring
    pub fn remove_node(&mut self, node_id: &str) {
        let hashes_to_remove: Vec<u64> = self.ring
            .values()
            .filter(|vn| vn.node_id == node_id)
            .map(|vn| vn.hash)
            .collect();
        
        for hash in hashes_to_remove {
            self.ring.remove(&hash);
        }
    }

    /// Find the node responsible for a given key
    pub fn get_node_for_key(&self, key: &str) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }

        let key_hash = self.hash(key);
        
        // Find the first virtual node with hash >= key_hash
        let node = self.ring.range(key_hash..).next()
            .or_else(|| self.ring.iter().next()) // Wrap around to the beginning
            .map(|(_, vn)| vn.node_id.as_str());
        
        node
    }

    /// Get all nodes in the ring
    pub fn get_nodes(&self) -> Vec<String> {
        self.ring.values()
            .map(|vn| vn.node_id.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Get the number of virtual nodes
    pub fn virtual_node_count(&self) -> usize {
        self.ring.len()
    }

    /// Get the number of physical nodes
    pub fn physical_node_count(&self) -> usize {
        self.get_nodes().len()
    }

    /// Hash function for consistent hashing
    fn hash(&self, key: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

impl ShardManager {
    pub fn new() -> Self {
        Self {
            hash_ring: Arc::new(RwLock::new(ConsistentHashRing::new(150))), // 150 virtual nodes per physical node
            node_clients: Arc::new(RwLock::new(HashMap::new())),
            local_node_id: "node-1".to_string(),
        }
    }

    pub fn with_local_node_id(mut self, node_id: String) -> Self {
        self.local_node_id = node_id;
        self
    }

    /// Start the shard manager
    pub async fn start(&mut self) -> Result<(), String> {
        // Add local node to the hash ring
        let mut ring = self.hash_ring.write().await;
        ring.add_node(&self.local_node_id);
        Ok(())
    }

    /// Stop the shard manager
    pub async fn stop(&mut self) -> Result<(), String> {
        // Clear all clients
        let mut clients = self.node_clients.write().await;
        clients.clear();
        Ok(())
    }

    /// Add a node to the cluster
    pub async fn add_node(&self, node_info: &NodeInfo) -> Result<(), String> {
        // Add to hash ring
        let mut ring = self.hash_ring.write().await;
        ring.add_node(&node_info.node_id);
        
        // Create client connection
        let addr = format!("http://{}:{}", node_info.address, node_info.port);
        let client = KvCacheClient::connect(addr).await.map_err(|e| e.to_string())?;
        
        let mut clients = self.node_clients.write().await;
        clients.insert(node_info.node_id.clone(), client);
        
        Ok(())
    }

    /// Remove a node from the cluster
    pub async fn remove_node(&self, node_id: &str) {
        // Remove from hash ring
        let mut ring = self.hash_ring.write().await;
        ring.remove_node(node_id);
        
        // Remove client
        let mut clients = self.node_clients.write().await;
        clients.remove(node_id);
    }

    /// Determine which node should handle a given key
    pub async fn get_node_for_key(&self, key: &str) -> Option<String> {
        let ring = self.hash_ring.read().await;
        ring.get_node_for_key(key).map(|s| s.to_string())
    }

    /// Check if a key belongs to the local node
    pub async fn is_local_key(&self, key: &str) -> bool {
        self.get_node_for_key(key).await
            .map(|node_id| node_id == self.local_node_id)
            .unwrap_or(false)
    }

    /// Forward a request to the appropriate node
    pub async fn forward_request(
        &self,
        operation: String,
        key: String,
        value: String,
        ttl_seconds: i64,
    ) -> Result<crate::cluster::communication::kv_cache::Response, String> {
        let target_node = self.get_node_for_key(&key).await
            .ok_or("No nodes available")?;
        
        if target_node == self.local_node_id {
            // This should be handled locally, not forwarded
            return Err("Key belongs to local node".into());
        }
        
        let clients = self.node_clients.read().await;
        let client = clients.get(&target_node)
            .ok_or(format!("No client for node {}", target_node))?;
        
        // Clone the client for the request
        let mut client_clone = client.clone();
        client_clone.forward_request(operation, key, value, ttl_seconds).await.map_err(|e| e.to_string())
    }

    /// Get shard status
    pub fn get_status(&self) -> ShardStatus {
        // This is a simplified version - in a real implementation,
        // you'd want to get this asynchronously
        ShardStatus {
            total_virtual_nodes: 0, // TODO: Implement proper counting
            physical_nodes: 0,
            local_node_id: self.local_node_id.clone(),
        }
    }

    /// Get all nodes in the cluster
    pub async fn get_nodes(&self) -> Vec<String> {
        let ring = self.hash_ring.read().await;
        ring.get_nodes()
    }

    /// Get the distribution of keys across nodes
    pub async fn get_key_distribution(&self, sample_keys: &[String]) -> HashMap<String, usize> {
        let mut distribution = HashMap::new();
        let ring = self.hash_ring.read().await;
        
        for key in sample_keys {
            if let Some(node_id) = ring.get_node_for_key(key) {
                *distribution.entry(node_id.to_string()).or_insert(0) += 1;
            }
        }
        
        distribution
    }
}

impl Default for ShardManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consistent_hash_ring() {
        let mut ring = ConsistentHashRing::new(3);
        
        // Add nodes
        ring.add_node("node-1");
        ring.add_node("node-2");
        ring.add_node("node-3");
        
        assert_eq!(ring.physical_node_count(), 3);
        assert_eq!(ring.virtual_node_count(), 9); // 3 nodes * 3 virtual nodes each
        
        // Test key distribution
        let key1 = "user:123";
        let key2 = "user:456";
        let key3 = "user:789";
        
        let node1 = ring.get_node_for_key(key1);
        let node2 = ring.get_node_for_key(key2);
        let node3 = ring.get_node_for_key(key3);
        
        assert!(node1.is_some());
        assert!(node2.is_some());
        assert!(node3.is_some());
        
        // Remove a node
        ring.remove_node("node-2");
        assert_eq!(ring.physical_node_count(), 2);
        assert_eq!(ring.virtual_node_count(), 6); // 2 nodes * 3 virtual nodes each
    }

    #[tokio::test]
    async fn test_shard_manager() {
        let mut shard_manager = ShardManager::new().with_local_node_id("node-1".to_string());
        
        // Start the shard manager to add local node to hash ring
        shard_manager.start().await.unwrap();
        
        // Test local key check
        let is_local = shard_manager.is_local_key("test-key").await;
        assert!(is_local); // Should be local since we're the only node
    }
} 