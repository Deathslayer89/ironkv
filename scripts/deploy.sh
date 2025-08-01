#!/bin/bash

# IRONKV Production Deployment Script
# This script deploys IRONKV in cluster mode with proper configuration

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BINARY_PATH="$PROJECT_ROOT/kv_cache_server/target/release/kv_cache_server"
CONFIG_DIR="$PROJECT_ROOT/config"
DATA_DIR="/var/lib/ironkv"
LOG_DIR="/var/log/ironkv"
USER="ironkv"
GROUP="ironkv"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

# Check if running as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        error "This script must be run as root"
        exit 1
    fi
}

# Create system user and group
create_user() {
    log "Creating system user and group..."
    
    if ! getent group $GROUP > /dev/null 2>&1; then
        groupadd $GROUP
        log "Created group: $GROUP"
    fi
    
    if ! getent passwd $USER > /dev/null 2>&1; then
        useradd -r -g $GROUP -s /bin/false -d /var/lib/ironkv $USER
        log "Created user: $USER"
    fi
}

# Create directories
create_directories() {
    log "Creating directories..."
    
    mkdir -p "$DATA_DIR"/{node1,node2,node3}
    mkdir -p "$LOG_DIR"
    mkdir -p /etc/ironkv
    
    # Set permissions
    chown -R $USER:$GROUP "$DATA_DIR"
    chown -R $USER:$GROUP "$LOG_DIR"
    chmod 755 "$DATA_DIR"
    chmod 755 "$LOG_DIR"
    
    log "Created data directory: $DATA_DIR"
    log "Created log directory: $LOG_DIR"
}

# Install binary
install_binary() {
    log "Installing IRONKV binary..."
    
    if [[ ! -f "$BINARY_PATH" ]]; then
        error "Binary not found at $BINARY_PATH"
        error "Please run 'cargo build --release' first"
        exit 1
    fi
    
    cp "$BINARY_PATH" /usr/local/bin/ironkv
    chown $USER:$GROUP /usr/local/bin/ironkv
    chmod +x /usr/local/bin/ironkv
    
    log "Installed binary to /usr/local/bin/ironkv"
}

# Install configuration files
install_config() {
    log "Installing configuration files..."
    
    cp "$CONFIG_DIR"/*.toml /etc/ironkv/
    chown -R $USER:$GROUP /etc/ironkv
    chmod 644 /etc/ironkv/*.toml
    
    log "Installed configuration files to /etc/ironkv/"
}

# Create systemd service files
create_systemd_services() {
    log "Creating systemd service files..."
    
    # Node 1 service
    cat > /etc/systemd/system/ironkv-node1.service << EOF
[Unit]
Description=IRONKV Node 1 - Distributed Key-Value Cache
After=network.target
Wants=network.target

[Service]
Type=simple
User=$USER
Group=$GROUP
ExecStart=/usr/local/bin/ironkv --cluster --config /etc/ironkv/node1.toml
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ironkv-node1

# Security settings
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$DATA_DIR/node1 $LOG_DIR

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

[Install]
WantedBy=multi-user.target
EOF

    # Node 2 service
    cat > /etc/systemd/system/ironkv-node2.service << EOF
[Unit]
Description=IRONKV Node 2 - Distributed Key-Value Cache
After=network.target
Wants=network.target

[Service]
Type=simple
User=$USER
Group=$GROUP
ExecStart=/usr/local/bin/ironkv --cluster --config /etc/ironkv/node2.toml
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ironkv-node2

# Security settings
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$DATA_DIR/node2 $LOG_DIR

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

[Install]
WantedBy=multi-user.target
EOF

    # Node 3 service
    cat > /etc/systemd/system/ironkv-node3.service << EOF
[Unit]
Description=IRONKV Node 3 - Distributed Key-Value Cache
After=network.target
Wants=network.target

[Service]
Type=simple
User=$USER
Group=$GROUP
ExecStart=/usr/local/bin/ironkv --cluster --config /etc/ironkv/node3.toml
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ironkv-node3

# Security settings
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$DATA_DIR/node3 $LOG_DIR

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

[Install]
WantedBy=multi-user.target
EOF

    # Reload systemd
    systemctl daemon-reload
    
    log "Created systemd service files"
}

# Start services
start_services() {
    log "Starting IRONKV cluster services..."
    
    # Enable and start services
    systemctl enable ironkv-node1.service
    systemctl enable ironkv-node2.service
    systemctl enable ironkv-node3.service
    
    # Start services with delay to allow cluster formation
    systemctl start ironkv-node1.service
    sleep 5
    systemctl start ironkv-node2.service
    sleep 5
    systemctl start ironkv-node3.service
    
    log "Started IRONKV cluster services"
}

# Check service status
check_status() {
    log "Checking service status..."
    
    echo -e "\n${BLUE}Service Status:${NC}"
    systemctl status ironkv-node1.service --no-pager -l
    echo
    systemctl status ironkv-node2.service --no-pager -l
    echo
    systemctl status ironkv-node3.service --no-pager -l
    
    echo -e "\n${BLUE}Cluster Health Check:${NC}"
    sleep 10
    if command -v redis-cli > /dev/null 2>&1; then
        echo "Testing connection to node1..."
        redis-cli -p 6379 ping
        echo "Testing connection to node2..."
        redis-cli -p 6379 ping
        echo "Testing connection to node3..."
        redis-cli -p 6379 ping
    else
        echo "redis-cli not found. You can test connections manually:"
        echo "  redis-cli -p 6379 ping"
    fi
}

# Main deployment function
deploy() {
    log "Starting IRONKV deployment..."
    
    check_root
    create_user
    create_directories
    install_binary
    install_config
    create_systemd_services
    start_services
    check_status
    
    log "IRONKV deployment completed successfully!"
    echo -e "\n${GREEN}IRONKV is now running on:${NC}"
    echo "  - Node 1: localhost:6379 (cluster port: 6380)"
    echo "  - Node 2: localhost:6379 (cluster port: 6381)"
    echo "  - Node 3: localhost:6379 (cluster port: 6382)"
    echo "  - Metrics: localhost:9090, 9091, 9092"
    echo
    echo "Useful commands:"
    echo "  - Check status: systemctl status ironkv-node*"
    echo "  - View logs: journalctl -u ironkv-node* -f"
    echo "  - Stop cluster: systemctl stop ironkv-node*"
    echo "  - Restart cluster: systemctl restart ironkv-node*"
}

# Usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  deploy     Deploy IRONKV cluster (default)"
    echo "  status     Check service status"
    echo "  stop       Stop all services"
    echo "  restart    Restart all services"
    echo "  logs       Show logs"
    echo "  help       Show this help message"
    echo
}

# Handle command line arguments
case "${1:-deploy}" in
    deploy)
        deploy
        ;;
    status)
        check_status
        ;;
    stop)
        log "Stopping IRONKV cluster..."
        systemctl stop ironkv-node1.service ironkv-node2.service ironkv-node3.service
        log "Stopped IRONKV cluster"
        ;;
    restart)
        log "Restarting IRONKV cluster..."
        systemctl restart ironkv-node1.service ironkv-node2.service ironkv-node3.service
        log "Restarted IRONKV cluster"
        ;;
    logs)
        journalctl -u ironkv-node* -f
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        error "Unknown command: $1"
        usage
        exit 1
        ;;
esac 