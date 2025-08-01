#!/bin/bash

# IRONKV Docker Deployment Script
# Simple deployment using Docker Compose - no TOML files needed!

set -e

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

# Check if Docker is installed
check_docker() {
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    log "Docker and Docker Compose are available"
}

# Build and start services
deploy() {
    log "Starting IRONKV Docker deployment..."
    
    check_docker
    
    # Build and start services
    log "Building and starting IRONKV cluster..."
    docker-compose up -d --build
    
    log "Waiting for services to start..."
    sleep 30
    
    # Check service status
    log "Checking service status..."
    docker-compose ps
    
    # Test connections
    log "Testing cluster connections..."
    sleep 10
    
    if command -v redis-cli > /dev/null 2>&1; then
        echo -e "\n${BLUE}Testing Redis connections:${NC}"
        echo "Testing node1 (localhost:6379)..."
        redis-cli -p 6379 ping
        echo "Testing node2 (localhost:6381)..."
        redis-cli -p 6381 ping
        echo "Testing node3 (localhost:6383)..."
        redis-cli -p 6383 ping
    else
        echo -e "\n${YELLOW}redis-cli not found. You can test connections manually:${NC}"
        echo "  redis-cli -p 6379 ping  # Node 1"
        echo "  redis-cli -p 6381 ping  # Node 2"
        echo "  redis-cli -p 6383 ping  # Node 3"
    fi
    
    log "IRONKV Docker deployment completed successfully!"
    echo -e "\n${GREEN}IRONKV is now running on:${NC}"
    echo "  - Node 1: localhost:6379 (cluster: 6380, metrics: 9090)"
    echo "  - Node 2: localhost:6381 (cluster: 6382, metrics: 9091)"
    echo "  - Node 3: localhost:6383 (cluster: 6384, metrics: 9092)"
    echo "  - Prometheus: localhost:9093"
    echo "  - Grafana: localhost:3000 (admin/admin)"
    echo
    echo "Useful commands:"
    echo "  - View logs: docker-compose logs -f"
    echo "  - Stop cluster: docker-compose down"
    echo "  - Restart cluster: docker-compose restart"
    echo "  - Scale nodes: docker-compose up -d --scale ironkv-node1=1 --scale ironkv-node2=1 --scale ironkv-node3=1"
}

# Stop services
stop() {
    log "Stopping IRONKV cluster..."
    docker-compose down
    log "Stopped IRONKV cluster"
}

# Restart services
restart() {
    log "Restarting IRONKV cluster..."
    docker-compose restart
    log "Restarted IRONKV cluster"
}

# Show logs
logs() {
    docker-compose logs -f
}

# Show status
status() {
    log "IRONKV cluster status:"
    docker-compose ps
    
    echo -e "\n${BLUE}Service logs (last 20 lines):${NC}"
    docker-compose logs --tail=20
}

# Clean up
cleanup() {
    log "Cleaning up IRONKV cluster..."
    docker-compose down -v
    docker system prune -f
    log "Cleanup completed"
}

# Usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  deploy     Deploy IRONKV cluster (default)"
    echo "  stop       Stop all services"
    echo "  restart    Restart all services"
    echo "  logs       Show logs"
    echo "  status     Show status"
    echo "  cleanup    Stop and remove all data"
    echo "  help       Show this help message"
    echo
}

# Handle command line arguments
case "${1:-deploy}" in
    deploy)
        deploy
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    logs)
        logs
        ;;
    status)
        status
        ;;
    cleanup)
        cleanup
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