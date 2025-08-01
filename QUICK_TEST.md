# IRONKV Quick Test Guide

## 🚀 Start 3-Node Cluster

### Simple One-Command Start (Recommended)
```bash
# Start all nodes
./start_cluster.sh start

# Check status
./start_cluster.sh status

# Run test
./start_cluster.sh test
```

### Manual Start (Multiple Terminals)
```bash
# Terminal 1
RUST_LOG=info ./kv_cache_server/target/release/kv_cache_server --cluster --node-id node1 --cluster-port 6380

# Terminal 2  
RUST_LOG=info ./kv_cache_server/target/release/kv_cache_server --cluster --node-id node2 --cluster-port 6382

# Terminal 3
RUST_LOG=info ./kv_cache_server/target/release/kv_cache_server --cluster --node-id node3 --cluster-port 6384
```

## ✅ Verify Cluster

```bash
# Check processes
ps aux | grep kv_cache_server | grep -v grep

# Check ports
netstat -tlnp | grep -E "(6379|6380|6381|6382|6383|6384)"
```

## 🧪 Basic Tests

```bash
# Test PING (all nodes)
echo "PING" | nc localhost 6379
echo "PING" | nc localhost 6381  
echo "PING" | nc localhost 6383

# Find leader (only one will accept SET)
echo "SET testkey hello" | nc localhost 6379
echo "SET testkey hello" | nc localhost 6381
echo "SET testkey hello" | nc localhost 6383

# Test GET (on leader)
echo "GET testkey" | nc localhost 6383  # Replace with actual leader port
```

## 🔄 Failover Test

```bash
# 1. Kill current leader
pkill -f "node-id node3"  # Replace with actual leader

# 2. Wait 5 seconds
sleep 5

# 3. Test new leader
echo "SET failover_test value" | nc localhost 6379
echo "SET failover_test value" | nc localhost 6381
echo "SET failover_test value" | nc localhost 6383

# 4. Restart failed node
RUST_LOG=info ./kv_cache_server/target/release/kv_cache_server --cluster --node-id node3 --cluster-port 6384
```

## 📊 Expected Results

| Test | Expected Response |
|------|------------------|
| PING | `+PONG` |
| SET (leader) | `+OK` |
| SET (follower) | `-ERR not leader` |
| GET (leader) | `$5` + `hello` |
| Failover | New leader accepts writes |

## 🎯 Success Indicators

- ✅ All 3 nodes running
- ✅ Leader election completed
- ✅ Redis commands work on leader
- ✅ Followers reject writes
- ✅ Automatic failover works
- ✅ Cluster recovers after node restart 