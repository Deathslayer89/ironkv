//! Example demonstrating Phase 3: Distributed System Foundation
//! 
//! This example shows how to use the cluster functionality including:
//! - gRPC inter-node communication
//! - Cluster membership management
//! - Consistent hashing for sharding
//! - Leader-follower replication

use kv_cache_core::{
    cluster::{
        ClusterManager, ClusterConfig, ClusterStatus,
        ReplicationStrategy, ConsistentHashRing, ShardManager,
        ClusterMembership, ReplicationManager
    },
    TTLStore, Value
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting IRONKV Phase 3: Distributed System Foundation Demo");
    println!("================================================================");

    // Example 1: Basic Cluster Manager
    println!("\n1️⃣ Creating a basic cluster manager...");
    let config = ClusterConfig {
        node_id: "node-1".to_string(),
        bind_address: "127.0.0.1".to_string(),
        bind_port: 50051,
        cluster_members: vec![],
        replication_factor: 2,
        heartbeat_interval_ms: 1000,
        failure_timeout_ms: 5000,
    };

    let mut cluster_manager = ClusterManager::new(config)?;
    println!("✅ Cluster manager created successfully");

    // Example 2: Consistent Hashing
    println!("\n2️⃣ Demonstrating consistent hashing...");
    let mut hash_ring = ConsistentHashRing::new(150);
    
    // Add nodes to the hash ring
    hash_ring.add_node("node-1");
    hash_ring.add_node("node-2");
    hash_ring.add_node("node-3");
    
    println!("   Added 3 nodes to hash ring");
    println!("   Virtual nodes: {}", hash_ring.virtual_node_count());
    println!("   Physical nodes: {}", hash_ring.physical_node_count());
    
    // Test key distribution
    let test_keys = vec!["user:123", "user:456", "user:789", "session:abc", "cache:xyz"];
    for key in &test_keys {
        if let Some(node) = hash_ring.get_node_for_key(key) {
            println!("   Key '{}' → Node '{}'", key, node);
        }
    }

    // Example 3: Shard Manager
    println!("\n3️⃣ Testing shard manager...");
    let shard_manager = ShardManager::new().with_local_node_id("node-1".to_string());
    
    // Test local key detection
    let test_key = "user:123";
    let is_local = shard_manager.is_local_key(test_key).await;
    println!("   Key '{}' is local: {}", test_key, is_local);

    // Example 4: Replication Manager
    println!("\n4️⃣ Testing replication manager...");
    let replication_manager = ReplicationManager::new(3)
        .with_strategy(ReplicationStrategy::Synchronous)
        .with_local_node_id("node-1".to_string())
        .set_leader(true);
    
    let status = replication_manager.get_status();
    println!("   Replication factor: {}", status.replication_factor);
    println!("   Strategy: {}", status.strategy);
    println!("   Is leader: {}", status.is_leader);

    // Example 5: Membership Management
    println!("\n5️⃣ Testing membership management...");
    let membership = ClusterMembership::new("node-1".to_string());
    
    // Add some members
    membership.add_member("node-2".to_string(), "127.0.0.1".to_string(), 50052).await;
    membership.add_member("node-3".to_string(), "127.0.0.1".to_string(), 50053).await;
    
    let members = membership.get_members().await;
    println!("   Cluster members: {}", members.len());
    for member in members {
        println!("   - {}:{}:{}", member.node_id, member.address, member.port);
    }

    // Example 6: Cluster Status
    println!("\n6️⃣ Getting cluster status...");
    let cluster_status = cluster_manager.get_status();
    println!("   Node ID: {}", cluster_status.node_id);
    println!("   Membership status: {:?}", cluster_status.membership_status);
    println!("   Shard status: {:?}", cluster_status.shard_status);
    println!("   Replication status: {:?}", cluster_status.replication_status);

    // Example 7: Simulating cluster operations
    println!("\n7️⃣ Simulating cluster operations...");
    
    // Create a store for testing
    let store = Arc::new(TTLStore::new());
    
    // Simulate some operations
    println!("   Setting key 'user:123' with value 'John Doe'");
    store.set("user:123".to_string(), Value::String("John Doe".to_string()), None).await;
    
    println!("   Setting key 'user:456' with value 'Jane Smith' and TTL");
    store.set("user:456".to_string(), Value::String("Jane Smith".to_string()), Some(60)).await;
    
    println!("   Getting key 'user:123'");
    if let Some(value) = store.get("user:123").await {
        println!("   Value: {:?}", value);
    }
    
    println!("   Store size: {}", store.len().await);

    // Example 8: Key distribution analysis
    println!("\n8️⃣ Analyzing key distribution...");
    let sample_keys = vec![
        "user:123".to_string(), "user:456".to_string(), "user:789".to_string(),
        "session:abc".to_string(), "cache:xyz".to_string(), "temp:123".to_string(),
        "config:db".to_string(), "log:error".to_string(), "stats:count".to_string(),
    ];
    
    let distribution = shard_manager.get_key_distribution(&sample_keys).await;
    println!("   Key distribution across nodes:");
    for (node_id, count) in distribution {
        println!("   - {}: {} keys", node_id, count);
    }

    println!("\n🎉 Phase 3 Demo completed successfully!");
    println!("========================================");
    println!("This demonstrates the foundation for distributed key-value caching");
    println!("including gRPC communication, consistent hashing, and replication.");

    Ok(())
} 