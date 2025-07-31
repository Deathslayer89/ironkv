fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile kv_cache.proto
    tonic_build::compile_protos("proto/kv_cache.proto")?;
    
    // Compile raft.proto
    tonic_build::compile_protos("proto/raft.proto")?;
    
    Ok(())
} 