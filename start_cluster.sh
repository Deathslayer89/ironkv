#!/bin/bash

# IRONKV Cluster Startup Script
# Starts all 3 nodes automatically

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

info() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $1${NC}"
}

# Configuration
NODES=(
    "node1:6379:6380"
    "node2:6381:6382"
    "node3:6383:6384"
)

# Check if binary exists
if [ ! -f "./kv_cache_server/target/release/kv_cache_server" ]; then
    error "IRONKV binary not found! Please build first:"
    echo "  cd kv_cache_server && cargo build --release"
    exit 1
fi

# Function to stop existing processes
stop_cluster() {
    log "Stopping existing IRONKV processes..."
    pkill -f kv_cache_server || true
    sleep 2
}

# Function to check if port is available
check_port() {
    local port=$1
    if netstat -tlnp 2>/dev/null | grep -q ":$port "; then
        return 1
    fi
    return 0
}

# Function to wait for node to be ready
wait_for_node() {
    local port=$1
    local node_id=$2
    local max_attempts=30
    local attempt=1
    
    log "Waiting for $node_id to be ready on port $port..."
    
    while [ $attempt -le $max_attempts ]; do
        if echo "PING" | nc -w 1 localhost $port 2>/dev/null | grep -q "+PONG"; then
            log "✅ $node_id is ready!"
            return 0
        fi
        
        echo -n "."
        sleep 1
        attempt=$((attempt + 1))
    done
    
    error "$node_id failed to start within $max_attempts seconds"
    return 1
}

# Function to find leader
find_leader() {
    log "🔍 Finding cluster leader..."
    
    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id redis_port cluster_port <<< "$node_info"
        
        if echo "SET leader_test value" | nc -w 1 localhost $redis_port 2>/dev/null | grep -q "+OK"; then
            log "✅ Leader found: $node_id (port $redis_port)"
            return 0
        fi
    done
    
    error "No leader found!"
    return 1
}

# Main startup function
start_cluster() {
    log "🚀 Starting IRONKV 3-node cluster..."
    
    # Stop any existing processes
    stop_cluster
    
    # Check ports
    log "Checking port availability..."
    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id redis_port cluster_port <<< "$node_info"
        
        if ! check_port $redis_port; then
            error "Port $redis_port is already in use!"
            exit 1
        fi
        
        if ! check_port $cluster_port; then
            error "Port $cluster_port is already in use!"
            exit 1
        fi
    done
    
    # Start nodes
    log "Starting nodes..."
    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id redis_port cluster_port <<< "$node_info"
        
        log "Starting $node_id (Redis: $redis_port, Cluster: $cluster_port)..."
        
        # Start node in background
        RUST_LOG=info ./kv_cache_server/target/release/kv_cache_server \
            --cluster \
            --node-id $node_id \
            --cluster-port $cluster_port > "ironkv_${node_id}.log" 2>&1 &
        
        # Store PID
        echo $! > "ironkv_${node_id}.pid"
        
        # Wait a bit before starting next node
        sleep 2
    done
    
    # Wait for all nodes to be ready
    log "Waiting for all nodes to be ready..."
    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id redis_port cluster_port <<< "$node_info"
        wait_for_node $redis_port $node_id
    done
    
    # Wait for leader election
    log "Waiting for leader election..."
    sleep 5
    
    # Find and display leader
    if find_leader; then
        log "🎉 Cluster is ready!"
        show_status
    else
        error "Cluster failed to elect a leader!"
        show_logs
        exit 1
    fi
}

# Function to show cluster status
show_status() {
    log "📊 Cluster Status:"
    echo "----------------------------------------"
    
    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id redis_port cluster_port <<< "$node_info"
        
        # Check if process is running
        if [ -f "ironkv_${node_id}.pid" ]; then
            pid=$(cat "ironkv_${node_id}.pid")
            if kill -0 $pid 2>/dev/null; then
                status="✅ RUNNING"
            else
                status="❌ STOPPED"
            fi
        else
            status="❌ NOT STARTED"
        fi
        
        # Check if port is listening
        if netstat -tlnp 2>/dev/null | grep -q ":$redis_port "; then
            port_status="✅ LISTENING"
        else
            port_status="❌ NOT LISTENING"
        fi
        
        # Check if it's leader
        if echo "SET leader_test value" | nc -w 1 localhost $redis_port 2>/dev/null | grep -q "+OK"; then
            leader_status="👑 LEADER"
        else
            leader_status="📋 FOLLOWER"
        fi
        
        printf "%-8s | %-12s | %-15s | %-10s\n" "$node_id" "$status" "$port_status" "$leader_status"
    done
    
    echo "----------------------------------------"
}

# Function to show logs
show_logs() {
    log "📝 Recent logs from all nodes:"
    echo "----------------------------------------"
    
    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id redis_port cluster_port <<< "$node_info"
        
        if [ -f "ironkv_${node_id}.log" ]; then
            echo "=== $node_id logs ==="
            tail -10 "ironkv_${node_id}.log"
            echo ""
        fi
    done
}

# Function to stop cluster
stop_cluster_cmd() {
    log "🛑 Stopping IRONKV cluster..."
    
    # Kill processes
    pkill -f kv_cache_server || true
    
    # Remove PID files
    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id redis_port cluster_port <<< "$node_info"
        rm -f "ironkv_${node_id}.pid"
    done
    
    log "✅ Cluster stopped"
}

# Function to restart cluster
restart_cluster() {
    log "🔄 Restarting IRONKV cluster..."
    stop_cluster_cmd
    sleep 2
    start_cluster
}

# Function to show usage
show_usage() {
    echo "IRONKV Cluster Management Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start     Start the 3-node cluster (default)"
    echo "  stop      Stop the cluster"
    echo "  restart   Restart the cluster"
    echo "  status    Show cluster status"
    echo "  logs      Show recent logs"
    echo "  test      Run quick cluster test"
    echo "  help      Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 start      # Start cluster"
    echo "  $0 status     # Check status"
    echo "  $0 test       # Test cluster"
    echo "  $0 stop       # Stop cluster"
}

# Function to run quick test
run_test() {
    log "🧪 Running quick cluster test..."
    
    # Find leader
    if ! find_leader; then
        error "No leader found - cluster may not be running"
        return 1
    fi
    
    # Test basic operations
    for node_info in "${NODES[@]}"; do
        IFS=':' read -r node_id redis_port cluster_port <<< "$node_info"
        
        log "Testing $node_id (port $redis_port)..."
        
        # Test PING
        if echo "PING" | nc -w 1 localhost $redis_port 2>/dev/null | grep -q "+PONG"; then
            echo "  ✅ PING: OK"
        else
            echo "  ❌ PING: FAILED"
        fi
        
        # Test SET (only leader should accept)
        if echo "SET test_${node_id} value" | nc -w 1 localhost $redis_port 2>/dev/null | grep -q "+OK"; then
            echo "  ✅ SET: OK (LEADER)"
        elif echo "SET test_${node_id} value" | nc -w 1 localhost $redis_port 2>/dev/null | grep -q "not leader"; then
            echo "  ✅ SET: Rejected (FOLLOWER)"
        else
            echo "  ❌ SET: FAILED"
        fi
    done
    
    log "✅ Quick test completed!"
}

# Main script logic
case "${1:-start}" in
    start)
        start_cluster
        ;;
    stop)
        stop_cluster_cmd
        ;;
    restart)
        restart_cluster
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    test)
        run_test
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        error "Unknown command: $1"
        show_usage
        exit 1
        ;;
esac 