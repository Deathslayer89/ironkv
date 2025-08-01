# IRONKV Distributed Key-Value Store - Deployment & Testing Guide

## 🚀 Overview

IRONKV is a production-ready distributed key-value store with Redis protocol compatibility and Raft consensus for strong consistency, automatic failover, and high availability.

## 📋 Prerequisites

- **Rust 1.75+**: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- **Docker & Docker Compose** (optional, for containerized deployment)
- **netcat** (for testing): `sudo apt install netcat-openbsd`

## 🏗️ Building IRONKV

```bash
# Clone and build
git clone <your-repo>
cd IRONKV
cd kv_cache_server
cargo build --release
cd ..
```

## 🎯 Quick Start - 3-Node Cluster

### Option 1: Simple One-Command Start (Recommended)

```bash
# Start all 3 nodes with one command
./start_cluster.sh start

# Check cluster status
./start_cluster.sh status

# Run quick test
./start_cluster.sh test

# Stop cluster
./start_cluster.sh stop
```

### Option 2: Manual Start (Multiple Terminals)

```bash
# Terminal 1 - Node 1 (Leader candidate)
RUST_LOG=info ./kv_cache_server/target/release/kv_cache_server --cluster --node-id node1 --cluster-port 6380

# Terminal 2 - Node 2 (Follower)
RUST_LOG=info ./kv_cache_server/target/release/kv_cache_server --cluster --node-id node2 --cluster-port 6382

# Terminal 3 - Node 3 (Follower)
RUST_LOG=info ./kv_cache_server/target/release/kv_cache_server --cluster --node-id node3 --cluster-port 6384
```

### Step 2: Verify Cluster Status

```bash
# Check all nodes are running
ps aux | grep kv_cache_server | grep -v grep

# Check ports are listening
netstat -tlnp | grep -E "(6379|6380|6381|6382|6383|6384)"
```

**Expected Output:**
```
tcp        0      0 127.0.0.1:6380         0.0.0.0:*               LISTEN      [node1-grpc]
tcp        0      0 127.0.0.1:6379         0.0.0.0:*               LISTEN      [node1-redis]
tcp        0      0 127.0.0.1:6382         0.0.0.0:*               LISTEN      [node2-grpc]
tcp        0      0 127.0.0.1:6381         0.0.0.0:*               LISTEN      [node2-redis]
tcp        0      0 127.0.0.1:6384         0.0.0.0:*               LISTEN      [node3-grpc]
tcp        0      0 127.0.0.1:6383         0.0.0.0:*               LISTEN      [node3-redis]
```

## 🧪 Testing the Cluster

### Basic Redis Operations

```bash
# Test PING (should work on any node)
echo "PING" | nc localhost 6379
echo "PING" | nc localhost 6381
echo "PING" | nc localhost 6383

# Expected: +PONG
```

### Find the Current Leader

```bash
# Try SET on each node - only leader will accept writes
echo "SET testkey hello" | nc localhost 6379
echo "SET testkey hello" | nc localhost 6381
echo "SET testkey hello" | nc localhost 6383

# Leader responds: +OK
# Followers respond: -ERR not leader
```

### Test Data Operations

```bash
# Assuming Node3 (6383) is leader, test full CRUD operations
echo "SET user:1 'John Doe'" | nc localhost 6383
echo "GET user:1" | nc localhost 6383
echo "SET counter 0" | nc localhost 6383
echo "INCR counter" | nc localhost 6383
echo "GET counter" | nc localhost 6383
echo "DEL user:1" | nc localhost 6383
```

### Test TTL (Time To Live)

```bash
# Set key with 10 second expiration
echo "SET tempkey 'expires soon' EX 10" | nc localhost 6383
echo "TTL tempkey" | nc localhost 6383
echo "GET tempkey" | nc localhost 6383

# Wait 10 seconds, then check again
sleep 11
echo "GET tempkey" | nc localhost 6383
```

## 🔄 Testing Failover (Production Feature)

### Simulate Leader Failure

```bash
# 1. Identify current leader (the one that accepts SET commands)
echo "SET failover_test value" | nc localhost 6379
echo "SET failover_test value" | nc localhost 6381
echo "SET failover_test value" | nc localhost 6383

# 2. Kill the current leader (replace with actual leader port)
pkill -f "node-id node3"  # If node3 is leader

# 3. Wait for failover (3-5 seconds)
sleep 5

# 4. Test new leader
echo "SET new_leader_test value" | nc localhost 6379
echo "SET new_leader_test value" | nc localhost 6381
echo "SET new_leader_test value" | nc localhost 6383

# One of the remaining nodes should now be leader and accept writes
```

### Restart Failed Node

```bash
# Restart the failed node
RUST_LOG=info ./kv_cache_server/target/release/kv_cache_server --cluster --node-id node3 --cluster-port 6384

# Wait for rejoin
sleep 5

# Test cluster is fully operational
echo "SET cluster_healthy true" | nc localhost 6383
```

## 🐳 Docker Deployment

### Using Docker Compose

```bash
# Start the entire cluster with monitoring
docker-compose up -d --build

# Check status
docker-compose ps

# View logs
docker-compose logs -f ironkv-node1
docker-compose logs -f ironkv-node2
docker-compose logs -f ironkv-node3

# Test the cluster
echo "PING" | nc localhost 6379
echo "SET docker_test value" | nc localhost 6379
```

### Docker Compose Configuration

The `docker-compose.yml` includes:
- **3 IRONKV nodes** with proper port mapping
- **Prometheus** for metrics collection
- **Grafana** for monitoring dashboards
- **Persistent volumes** for data storage
- **Health checks** for automatic recovery

## ⚙️ Configuration

### Environment Variables

```bash
# Node identification
IRONKV_NODE_ID=node1
IRONKV_CLUSTER_PORT=6380
IRONKV_SERVER_PORT=6379

# Logging
RUST_LOG=info
RUST_BACKTRACE=1

# Cluster members (comma-separated)
IRONKV_CLUSTER_MEMBERS=node1:127.0.0.1:6380,node2:127.0.0.1:6382,node3:127.0.0.1:6384
```

### Configuration Files

Create `config/node1.toml`:
```toml
[server]
port = 6379
bind_address = "127.0.0.1"

[cluster]
enabled = true
node_id = "node1"
port = 6380
bind_address = "127.0.0.1"
members = { "node1" = "127.0.0.1:6380", "node2" = "127.0.0.1:6382", "node3" = "127.0.0.1:6384" }

[metrics]
enabled = true
port = 9090
```

## 📊 Monitoring

### Prometheus Metrics

```bash
# Access metrics endpoint
curl http://localhost:9090/metrics

# Key metrics to monitor:
# - raft_election_count
# - raft_leader_changes
# - cache_operations_total
# - cache_hit_ratio
# - network_connections
```

### Grafana Dashboard

1. Access Grafana: `http://localhost:3000`
2. Default credentials: `admin/admin`
3. Import dashboard from `monitoring/grafana/dashboards/`

## 🔧 Troubleshooting

### Common Issues

**1. Port Already in Use**
```bash
# Check what's using the port
sudo netstat -tlnp | grep 6379
sudo lsof -i :6379

# Kill conflicting processes
sudo pkill -f redis-server
```

**2. Election Failures**
```bash
# Check cluster connectivity
nc -zv localhost 6380
nc -zv localhost 6382
nc -zv localhost 6384

# Restart nodes in order
pkill -f kv_cache_server
# Wait 5 seconds, then restart nodes
```

**3. Data Not Persisting**
```bash
# Check data directory permissions
ls -la /var/lib/ironkv/
sudo chown -R ironkv:ironkv /var/lib/ironkv/
```

### Log Analysis

```bash
# Enable debug logging
RUST_LOG=debug ./kv_cache_server/target/release/kv_cache_server --cluster --node-id node1 --cluster-port 6380

# Look for key log messages:
# - "Won election with X votes, becoming leader"
# - "Found higher term X, becoming follower"
# - "New Redis connection from"
# - "Election failed with X votes"
```

## 🚀 Production Deployment

### Systemd Service

```bash
# Install as system service
sudo cp scripts/deploy.sh /usr/local/bin/ironkv-deploy
sudo chmod +x /usr/local/bin/ironkv-deploy

# Deploy cluster
sudo ironkv-deploy deploy

# Check status
sudo ironkv-deploy status

# View logs
sudo journalctl -u ironkv-node1 -f
```

### Security Considerations

1. **Network Security**: Use firewalls to restrict access
2. **TLS/SSL**: Enable TLS for production (see `tls_manager.rs`)
3. **Authentication**: Implement Redis AUTH if needed
4. **Monitoring**: Set up alerting for cluster health

## 📈 Performance Testing

### Load Testing

```bash
# Install redis-benchmark
sudo apt install redis-tools

# Basic performance test
redis-benchmark -h localhost -p 6379 -n 10000 -c 10

# Test specific operations
redis-benchmark -h localhost -p 6379 -n 10000 -c 10 -t set,get
```

### Cluster Performance

```bash
# Test with multiple clients
for i in {1..5}; do
  redis-benchmark -h localhost -p 6379 -n 1000 -c 5 &
done
wait
```

## ✅ Success Criteria

Your IRONKV cluster is working correctly when:

1. **✅ All nodes start without errors**
2. **✅ Leader election completes successfully**
3. **✅ Redis commands work on leader node**
4. **✅ Followers reject write commands with "not leader"**
5. **✅ Automatic failover works when leader fails**
6. **✅ Data persists across node restarts**
7. **✅ Cluster recovers when failed nodes rejoin**

## 🎯 Next Steps

1. **Scale Testing**: Test with larger datasets
2. **Network Partitioning**: Test split-brain scenarios
3. **Backup & Recovery**: Implement data backup strategies
4. **Integration**: Connect your applications to IRONKV
5. **Monitoring**: Set up comprehensive monitoring and alerting

---

**🎉 Congratulations! You now have a production-ready distributed key-value store with Redis compatibility and Raft consensus!** 