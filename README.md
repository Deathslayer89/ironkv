# IRONKV - High-Performance Distributed Key-Value Cache

A production-ready, fault-tolerant, scalable distributed key-value store implemented in Rust. IRONKV provides Redis-compatible protocol with advanced features including distributed consensus, automatic failover, and horizontal scaling.

## Features

### Core Features
- **Redis-Compatible Protocol**: Drop-in replacement for Redis with RESP protocol support
- **Distributed Consensus**: Raft-based consensus for strong consistency
- **Automatic Failover**: Leader election and automatic recovery from node failures
- **Horizontal Scaling**: Sharding and data rebalancing across cluster nodes
- **TTL Support**: Automatic expiration with configurable cleanup strategies
- **Persistence**: AOF (Append-Only File) and snapshot-based persistence
- **High Performance**: Async I/O with concurrent access patterns

### Advanced Features
- **Multiple Data Types**: Strings, Lists, Hashes, Sets with full CRUD operations
- **Eviction Policies**: LRU, LFU, Random, and No-Eviction policies
- **Metrics & Monitoring**: Prometheus-compatible metrics with comprehensive observability
- **Configuration Management**: TOML/YAML configuration with hot-reloading
- **Graceful Shutdown**: Coordinated shutdown with data persistence
- **Health Checks**: Built-in health endpoints and cluster status monitoring

## Quick Start

### Prerequisites
- Rust 1.75+ 
- Linux/macOS (Windows support coming soon)

### Installation

```bash
# Clone the repository
git clone https://github.com/Deathslayer89/ironkv.git
cd ironkv

# Build the project
cd kv_cache_server
cargo build --release
```

### Running IRONKV

#### Single Node Mode
```bash
# Start the server
./target/release/kv_cache_server

# Server will start on 127.0.0.1:6379
```

#### Cluster Mode
```bash
# Start with cluster configuration
./target/release/kv_cache_server --config cluster.toml
```

### Using IRONKV

#### Command Line Interface
```bash
# Connect using netcat
echo -e "SET mykey myvalue\r\nGET mykey\r\n" | nc localhost 6379

# Or use any Redis client
redis-cli -p 6379
```

#### Python Example
```python
import socket

def send_command(command):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 6379))
    sock.send(command.encode('utf-8'))
    response = sock.recv(1024).decode('utf-8')
    sock.close()
    return response

# Basic operations
send_command("SET user:123 'John Doe'\r\n")
send_command("GET user:123\r\n")
send_command("EXPIRE user:123 3600\r\n")  # TTL in seconds
```

#### Node.js Example
```javascript
const net = require('net');

function sendCommand(command) {
    return new Promise((resolve, reject) => {
        const client = net.createConnection(6379, 'localhost');
        client.write(command);
        client.on('data', (data) => {
            resolve(data.toString());
            client.end();
        });
    });
}

// Usage
sendCommand("SET session:abc 'user data'\r\n")
    .then(response => console.log(response));
```

## Configuration

### Basic Configuration
Create `config.toml`:

```toml
[server]
bind_address = "0.0.0.0"
port = 6379
max_connections = 10000
connection_timeout = 30
request_timeout = 5

[storage]
max_memory_bytes = 1073741824  # 1GB
max_keys = 1000000
eviction_policy = "LRU"
default_ttl_seconds = 0
enable_ttl_cleanup = true

[cluster]
enabled = true
node_id = "node-1"
bind_address = "0.0.0.0"
port = 6380
heartbeat_interval = 1
failure_timeout = 5
replication_factor = 3

[persistence]
enabled = true
data_dir = "/var/lib/ironkv"
aof_path = "/var/lib/ironkv/ironkv.aof"
snapshot_path = "/var/lib/ironkv/ironkv.rdb"
enable_aof = true
snapshot_interval = 300

[metrics]
enabled = true
bind_address = "0.0.0.0"
port = 9090
path = "/metrics"
```

### Environment Variables
```bash
export IRONKV_CONFIG_PATH="/etc/ironkv/config.toml"
export IRONKV_LOG_LEVEL="info"
export IRONKV_NODE_ID="node-1"
```

## API Reference

### Supported Commands

#### String Operations
- `SET key value [EX seconds] [PX milliseconds]` - Set key with optional TTL
- `GET key` - Get value by key
- `DEL key` - Delete key
- `EXISTS key` - Check if key exists
- `EXPIRE key seconds` - Set expiration time
- `TTL key` - Get time to live

#### List Operations
- `LPUSH key value` - Push to left of list
- `RPUSH key value` - Push to right of list
- `LPOP key` - Pop from left of list
- `RPOP key` - Pop from right of list
- `LRANGE key start stop` - Get range of list elements

#### Hash Operations
- `HSET key field value` - Set hash field
- `HGET key field` - Get hash field
- `HDEL key field` - Delete hash field
- `HGETALL key` - Get all hash fields

#### Set Operations
- `SADD key member` - Add member to set
- `SREM key member` - Remove member from set
- `SISMEMBER key member` - Check if member exists in set
- `SMEMBERS key` - Get all set members

#### Cluster Operations
- `CLUSTER INFO` - Get cluster information
- `CLUSTER NODES` - List cluster nodes
- `CLUSTER MEET ip port` - Add node to cluster

## Deployment

### Docker Deployment
```dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bullseye-slim
COPY --from=builder /app/target/release/kv_cache_server /usr/local/bin/
EXPOSE 6379 6380 9090
CMD ["kv_cache_server"]
```

### Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ironkv
spec:
  replicas: 3
  selector:
    matchLabels:
      app: ironkv
  template:
    metadata:
      labels:
        app: ironkv
    spec:
      containers:
      - name: ironkv
        image: ironkv:latest
        ports:
        - containerPort: 6379
        - containerPort: 6380
        - containerPort: 9090
        volumeMounts:
        - name: data
          mountPath: /var/lib/ironkv
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: ironkv-data
```

### Systemd Service
```ini
[Unit]
Description=IRONKV Distributed Cache
After=network.target

[Service]
Type=simple
User=ironkv
Group=ironkv
ExecStart=/usr/local/bin/kv_cache_server --config /etc/ironkv/config.toml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Monitoring & Observability

### Metrics Endpoint
IRONKV exposes Prometheus-compatible metrics at `http://localhost:9090/metrics`:

- `ironkv_operations_total` - Total operations by type
- `ironkv_cache_hits_total` - Cache hit count
- `ironkv_cache_misses_total` - Cache miss count
- `ironkv_memory_usage_bytes` - Current memory usage
- `ironkv_connections_active` - Active connections
- `ironkv_cluster_nodes_total` - Total cluster nodes

### Health Checks
```bash
# Health endpoint
curl http://localhost:9090/health

# Cluster status
curl http://localhost:9090/cluster/status
```

### Logging
IRONKV supports structured logging with configurable levels:
```bash
# Set log level
export RUST_LOG=ironkv=info

# View logs
journalctl -u ironkv -f
```

## Performance

### Benchmarks
- **Throughput**: 100,000+ operations/second on single node
- **Latency**: <1ms for GET operations, <5ms for SET operations
- **Memory**: Efficient memory usage with configurable limits
- **Scalability**: Linear scaling with cluster size

### Tuning
```toml
[performance]
# Increase worker threads
worker_threads = 8

# Adjust buffer sizes
read_buffer_size = 8192
write_buffer_size = 8192

# Connection pooling
max_connections_per_worker = 1000
```

## Security

### Network Security
- TLS/SSL support (coming in v1.1)
- Authentication and authorization (coming in v1.1)
- Network isolation and firewall rules

### Data Security
- Encryption at rest (coming in v1.1)
- Secure configuration management
- Audit logging

## Troubleshooting

### Common Issues

#### Connection Refused
```bash
# Check if server is running
ps aux | grep kv_cache_server

# Check port availability
netstat -tlnp | grep 6379
```

#### High Memory Usage
```bash
# Check memory configuration
grep max_memory config.toml

# Monitor memory usage
curl http://localhost:9090/metrics | grep memory
```

#### Cluster Issues
```bash
# Check cluster status
curl http://localhost:9090/cluster/status

# View cluster logs
journalctl -u ironkv | grep cluster
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup
```bash
git clone https://github.com/yourusername/ironkv.git
cd ironkv
cargo test
cargo run --bin kv_cache_server
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/deathslayer89/ironkv/issues)
- **Discussions**: [GitHub Discussions](https://github.com/deathslayer89/ironkv/discussions)

## Roadmap

- [ ] TLS/SSL support
- [ ] Authentication and authorization
- [ ] Encryption at rest
- [ ] Cross-datacenter replication
- [ ] Lua scripting support
- [ ] Stream data type
- [ ] Pub/Sub improvements
- [ ] Backup and restore tools