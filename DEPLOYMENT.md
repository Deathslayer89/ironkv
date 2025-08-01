# IRONKV Deployment Guide

IRONKV can be deployed in two ways: **Docker (Recommended)** or **System Installation**.

## 🐳 Docker Deployment (Recommended)

**No TOML files needed!** Configuration is handled via environment variables.

### Quick Start

```bash
# Clone the repository
git clone <your-repo-url>
cd IRONKV

# Build and deploy
./scripts/docker-deploy.sh

# Or manually
docker-compose up -d --build
```

### What you get:
- ✅ 3-node IRONKV cluster
- ✅ Prometheus metrics collection
- ✅ Grafana dashboards
- ✅ Persistent data storage
- ✅ Health checks
- ✅ Automatic restarts

### Access Points:
- **Node 1**: `localhost:6379` (Redis protocol)
- **Node 2**: `localhost:6381` (Redis protocol)
- **Node 3**: `localhost:6383` (Redis protocol)
- **Prometheus**: `localhost:9093`
- **Grafana**: `localhost:3000` (admin/admin)

### Useful Commands:
```bash
# View logs
docker-compose logs -f

# Stop cluster
docker-compose down

# Restart cluster
docker-compose restart

# Check status
docker-compose ps
```

## 🖥️ System Installation

**Requires TOML configuration files** for production deployment.

### Prerequisites:
- Linux system
- Root access
- Rust 1.75+

### Installation:

```bash
# Build the project
cargo build --release

# Deploy (requires root)
sudo ./scripts/deploy.sh

# Or manually
sudo ./scripts/deploy.sh deploy
```

### Configuration Files:
- `config/production.toml` - Production settings
- `config/node1.toml` - Node 1 configuration
- `config/node2.toml` - Node 2 configuration
- `config/node3.toml` - Node 3 configuration

### System Services:
- `ironkv-node1.service`
- `ironkv-node2.service`
- `ironkv-node3.service`

### Useful Commands:
```bash
# Check status
systemctl status ironkv-node*

# View logs
journalctl -u ironkv-node* -f

# Stop cluster
systemctl stop ironkv-node*

# Restart cluster
systemctl restart ironkv-node*
```

## 🔧 Configuration Options

### Environment Variables (Docker):
```bash
IRONKV_NODE_ID=node1
IRONKV_CLUSTER_PORT=6380
IRONKV_SERVER_PORT=6379
IRONKV_METRICS_PORT=9090
IRONKV_CLUSTER_MEMBERS=node1:host:port,node2:host:port
```

### TOML Configuration (System):
```toml
[server]
bind_address = "0.0.0.0"
port = 6379
max_connections = 10000

[cluster]
enabled = true
node_id = "node1"
port = 6380

[storage]
max_memory_bytes = 2147483648  # 2GB
eviction_policy = "LRU"
```

## 🚀 Production Considerations

### Docker (Recommended for most cases):
- ✅ Easy deployment and scaling
- ✅ Consistent environments
- ✅ Built-in monitoring
- ✅ Resource isolation
- ✅ Easy backup/restore

### System Installation:
- ✅ Better performance
- ✅ More control over resources
- ✅ Traditional service management
- ✅ Direct hardware access

## 📊 Monitoring

Both deployments include:
- **Prometheus** metrics collection
- **Grafana** dashboards
- **Health checks**
- **Structured logging**

## 🔒 Security

- Non-root user execution
- Network isolation
- Resource limits
- Secure defaults

## 📝 Testing

Test your deployment:
```bash
# Test Redis connections
redis-cli -p 6379 ping
redis-cli -p 6379 set test "hello"
redis-cli -p 6379 get test

# Test metrics
curl http://localhost:9090/metrics

# Test Grafana
open http://localhost:3000
```

## 🆘 Troubleshooting

### Common Issues:

1. **Port conflicts**: Change ports in docker-compose.yml or config files
2. **Permission errors**: Ensure proper user/group setup
3. **Memory issues**: Adjust `max_memory_bytes` in configuration
4. **Network issues**: Check firewall settings and network connectivity

### Logs:
- **Docker**: `docker-compose logs -f`
- **System**: `journalctl -u ironkv-node* -f`

### Health Checks:
- **Docker**: `docker-compose ps`
- **System**: `systemctl status ironkv-node*` 