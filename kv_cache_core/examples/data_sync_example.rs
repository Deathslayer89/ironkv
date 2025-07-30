//! Example demonstrating data synchronization features in the distributed cache
//! 
//! This example shows:
//! 1. Initial full sync when a new follower joins
//! 2. Log replay for recovering followers
//! 3. State tracking and synchronization

use kv_cache_core::{
    cluster::{
        ReplicationManager, ReplicationStrategy,
        ClusterManager, ClusterConfig,
    },
    cluster::replication::SyncStatus,
    TTLStore, Value,
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Data Synchronization Example");
    println!("================================\n");

    // Create a leader node with replication manager
    let leader_store = Arc::new(TTLStore::new());
    let mut leader_replication = ReplicationManager::new(2)
        .with_strategy(ReplicationStrategy::Synchronous)
        .with_local_node_id("leader-1".to_string())
        .set_leader(true)
        .with_store(Arc::clone(&leader_store));

    // Start the replication manager
    leader_replication.start().await?;
    println!("✅ Leader replication manager started");

    // Add some initial data to the leader
    leader_store.set("user:1".to_string(), Value::String("Alice".to_string()), None).await;
    leader_store.set("user:2".to_string(), Value::String("Bob".to_string()), None).await;
    leader_store.set("config:timeout".to_string(), Value::String("30".to_string()), None).await;
    
    println!("📝 Added initial data to leader:");
    println!("   - user:1 = Alice");
    println!("   - user:2 = Bob");
    println!("   - config:timeout = 30");

    // Simulate adding a new follower (this would normally connect to a real node)
    println!("\n🔄 Adding new follower (simulated)...");
    
    // In a real scenario, this would connect to an actual follower node
    // For this example, we'll simulate the process
    let follower_id = "follower-1".to_string();
    
    // The follower would be in NeedsFullSync status initially
    println!("   - Follower status: NeedsFullSync");
    println!("   - Performing initial full sync...");
    
    // Simulate full sync by showing what would be sent
    let store_data: Vec<(String, String, Option<i64>)> = vec![
        ("user:1".to_string(), "Alice".to_string(), None),
        ("user:2".to_string(), "Bob".to_string(), None),
        ("config:timeout".to_string(), "30".to_string(), None),
    ];
    
    println!("   - Sending {} keys to follower", store_data.len());
    for (key, value, _) in &store_data {
        println!("     SET {} = {}", key, value);
    }
    
    // Simulate follower becoming synchronized
    println!("   - Follower status: Synchronized");
    println!("✅ New follower added and synchronized");

    // Add more data while follower is synchronized
    println!("\n📝 Adding more data to leader...");
    leader_replication.replicate_write(
        "SET".to_string(),
        "user:3".to_string(),
        "Charlie".to_string(),
        0,
    ).await?;
    
    leader_replication.replicate_write(
        "SET".to_string(),
        "user:4".to_string(),
        "Diana".to_string(),
        0,
    ).await?;
    
    println!("   - user:3 = Charlie");
    println!("   - user:4 = Diana");

    // Simulate follower going offline and coming back
    println!("\n⚠️  Simulating follower going offline...");
    println!("   - Follower status: Offline");
    
    // Add more data while follower is offline
    println!("\n📝 Adding data while follower is offline...");
    leader_replication.replicate_write(
        "SET".to_string(),
        "user:5".to_string(),
        "Eve".to_string(),
        0,
    ).await?;
    
    leader_replication.replicate_write(
        "SET".to_string(),
        "user:6".to_string(),
        "Frank".to_string(),
        0,
    ).await?;
    
    println!("   - user:5 = Eve");
    println!("   - user:6 = Frank");

    // Simulate follower coming back online
    println!("\n🔄 Follower coming back online...");
    println!("   - Follower status: NeedsLogReplay");
    println!("   - Performing log replay...");
    
    // Show what would be replayed
    let replay_entries = vec![
        ("user:3".to_string(), "Charlie".to_string()),
        ("user:4".to_string(), "Diana".to_string()),
        ("user:5".to_string(), "Eve".to_string()),
        ("user:6".to_string(), "Frank".to_string()),
    ];
    
    println!("   - Replaying {} operations:", replay_entries.len());
    for (key, value) in &replay_entries {
        println!("     SET {} = {}", key, value);
    }
    
    println!("   - Follower status: Synchronized");
    println!("✅ Follower recovered and synchronized");

    // Show replication log status
    println!("\n📊 Replication Status:");
    let status = leader_replication.get_status();
    println!("   - Replication Factor: {}", status.replication_factor);
    println!("   - Strategy: {}", status.strategy);
    println!("   - Is Leader: {}", status.is_leader);
    println!("   - Log Size: {}", status.log_size);
    println!("   - Sequence Number: {}", status.sequence_number);

    // Show replication log contents
    println!("\n📋 Replication Log Contents:");
    let log = leader_replication.get_replication_log().await;
    for entry in log {
        println!("   - Seq {}: {} {} = {}", 
            entry.sequence_number,
            entry.operation,
            entry.key,
            entry.value.unwrap_or_default()
        );
    }

    // Demonstrate different replication strategies
    println!("\n🔄 Demonstrating different replication strategies:");
    
    // Asynchronous replication
    let mut async_replication = ReplicationManager::new(2)
        .with_strategy(ReplicationStrategy::Asynchronous)
        .with_local_node_id("async-leader".to_string())
        .set_leader(true);
    
    println!("   - Asynchronous: Fire and forget");
    async_replication.replicate_write(
        "SET".to_string(),
        "async:key".to_string(),
        "async_value".to_string(),
        0,
    ).await?;
    
    // Quorum replication
    let mut quorum_replication = ReplicationManager::new(3)
        .with_strategy(ReplicationStrategy::Quorum(2))
        .with_local_node_id("quorum-leader".to_string())
        .set_leader(true);
    
    println!("   - Quorum: Wait for majority (2/3)");
    quorum_replication.replicate_write(
        "SET".to_string(),
        "quorum:key".to_string(),
        "quorum_value".to_string(),
        0,
    ).await?;

    println!("\n✅ Data synchronization example completed successfully!");
    println!("\nKey Features Demonstrated:");
    println!("  • Initial full sync for new followers");
    println!("  • Log replay for recovering followers");
    println!("  • State tracking and synchronization");
    println!("  • Multiple replication strategies");
    println!("  • Replication log management");

    Ok(())
} 