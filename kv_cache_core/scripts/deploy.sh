#!/bin/bash

# IRONKV Deployment Script
# Supports both Docker Compose and Kubernetes deployments

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKER_IMAGE="ironkv:latest"
NAMESPACE="ironkv"
DEPLOYMENT_TYPE="${1:-docker}"

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

info() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    case $DEPLOYMENT_TYPE in
        docker|docker-compose)
            if ! command -v docker &> /dev/null; then
                error "Docker is not installed"
                exit 1
            fi
            
            if ! command -v docker-compose &> /dev/null; then
                error "Docker Compose is not installed"
                exit 1
            fi
            
            log "Docker and Docker Compose are available"
            ;;
        k8s|kubernetes)
            if ! command -v kubectl &> /dev/null; then
                error "kubectl is not installed"
                exit 1
            fi
            
            if ! kubectl cluster-info &> /dev/null; then
                error "Kubernetes cluster is not accessible"
                exit 1
            fi
            
            log "Kubernetes cluster is accessible"
            ;;
        *)
            error "Invalid deployment type: $DEPLOYMENT_TYPE"
            echo "Supported types: docker, k8s"
            exit 1
            ;;
    esac
}

# Build Docker image
build_image() {
    log "Building Docker image: $DOCKER_IMAGE"
    
    cd "$PROJECT_ROOT"
    
    if docker build -t "$DOCKER_IMAGE" .; then
        log "Docker image built successfully"
    else
        error "Failed to build Docker image"
        exit 1
    fi
}

# Deploy with Docker Compose
deploy_docker() {
    log "Deploying with Docker Compose..."
    
    cd "$PROJECT_ROOT"
    
    # Create necessary directories
    mkdir -p config certs monitoring/grafana/dashboards monitoring/grafana/datasources
    
    # Create node configurations
    create_node_configs
    
    # Start services
    if docker-compose up -d; then
        log "Docker Compose deployment started successfully"
        show_docker_status
    else
        error "Docker Compose deployment failed"
        exit 1
    fi
}

# Create node configurations for Docker Compose
create_node_configs() {
    log "Creating node configurations..."
    
    # Node 1 (Leader)
    cat > config/node1.toml << EOF
[server]
bind_address = "0.0.0.0"
port = 8080
enable_tls = false

[cluster]
enabled = true
node_id = "node1"
cluster_port = 8081

[storage]
engine = "memory"
max_memory_mb = 512

[metrics]
enabled = true
port = 9090
EOF

    # Node 2 (Follower)
    cat > config/node2.toml << EOF
[server]
bind_address = "0.0.0.0"
port = 8080
enable_tls = false

[cluster]
enabled = true
node_id = "node2"
cluster_port = 8081

[storage]
engine = "memory"
max_memory_mb = 512

[metrics]
enabled = true
port = 9090
EOF

    # Node 3 (Follower)
    cat > config/node3.toml << EOF
[server]
bind_address = "0.0.0.0"
port = 8080
enable_tls = false

[cluster]
enabled = true
node_id = "node3"
cluster_port = 8081

[storage]
engine = "memory"
max_memory_mb = 512

[metrics]
enabled = true
port = 9090
EOF

    log "Node configurations created"
}

# Deploy with Kubernetes
deploy_kubernetes() {
    log "Deploying with Kubernetes..."
    
    cd "$PROJECT_ROOT/k8s"
    
    # Apply namespace
    kubectl apply -f namespace.yaml
    
    # Apply ConfigMap
    kubectl apply -f configmap.yaml
    
    # Apply PVC
    kubectl apply -f pvc.yaml
    
    # Apply Deployment
    kubectl apply -f deployment.yaml
    
    # Apply Services
    kubectl apply -f service.yaml
    
    # Wait for deployment to be ready
    log "Waiting for deployment to be ready..."
    kubectl wait --for=condition=available --timeout=300s deployment/ironkv -n "$NAMESPACE"
    
    log "Kubernetes deployment completed successfully"
    show_k8s_status
}

# Show Docker Compose status
show_docker_status() {
    log "Docker Compose services status:"
    docker-compose ps
    
    log "Service endpoints:"
    echo "  IRONKV Node 1: http://localhost:8080"
    echo "  IRONKV Node 2: http://localhost:8082"
    echo "  IRONKV Node 3: http://localhost:8084"
    echo "  Prometheus: http://localhost:9093"
    echo "  Grafana: http://localhost:3000 (admin/admin)"
    echo "  Redis: localhost:6379"
}

# Show Kubernetes status
show_k8s_status() {
    log "Kubernetes deployment status:"
    kubectl get pods -n "$NAMESPACE"
    kubectl get services -n "$NAMESPACE"
    
    log "Service endpoints:"
    echo "  Internal Service: ironkv-service.ironkv.svc.cluster.local:8080"
    echo "  External Service: Check LoadBalancer IP with 'kubectl get svc ironkv-external -n ironkv'"
}

# Undeploy
undeploy() {
    case $DEPLOYMENT_TYPE in
        docker|docker-compose)
            log "Undeploying Docker Compose services..."
            cd "$PROJECT_ROOT"
            docker-compose down -v
            log "Docker Compose services stopped"
            ;;
        k8s|kubernetes)
            log "Undeploying Kubernetes resources..."
            cd "$PROJECT_ROOT/k8s"
            kubectl delete -f service.yaml --ignore-not-found
            kubectl delete -f deployment.yaml --ignore-not-found
            kubectl delete -f pvc.yaml --ignore-not-found
            kubectl delete -f configmap.yaml --ignore-not-found
            kubectl delete -f namespace.yaml --ignore-not-found
            log "Kubernetes resources deleted"
            ;;
    esac
}

# Show help
show_help() {
    echo "Usage: $0 [DEPLOYMENT_TYPE] [COMMAND]"
    echo ""
    echo "Deployment Types:"
    echo "  docker, docker-compose  Deploy using Docker Compose"
    echo "  k8s, kubernetes        Deploy using Kubernetes"
    echo ""
    echo "Commands:"
    echo "  deploy                 Deploy IRONKV (default)"
    echo "  undeploy               Undeploy IRONKV"
    echo "  status                 Show deployment status"
    echo "  build                  Build Docker image only"
    echo "  help                   Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 docker deploy       Deploy with Docker Compose"
    echo "  $0 k8s deploy          Deploy with Kubernetes"
    echo "  $0 docker undeploy     Undeploy Docker Compose"
    echo "  $0 k8s status          Show Kubernetes status"
}

# Main execution
main() {
    # Handle help command first
    if [ "$1" = "help" ] || [ "$1" = "--help" ] || [ "$1" = "-h" ] || [ "$2" = "help" ] || [ "$2" = "--help" ] || [ "$2" = "-h" ]; then
        show_help
        exit 0
    fi
    
    case "${2:-deploy}" in
        deploy)
            check_prerequisites
            build_image
            case $DEPLOYMENT_TYPE in
                docker|docker-compose)
                    deploy_docker
                    ;;
                k8s|kubernetes)
                    deploy_kubernetes
                    ;;
            esac
            ;;
        undeploy)
            undeploy
            ;;
        status)
            case $DEPLOYMENT_TYPE in
                docker|docker-compose)
                    cd "$PROJECT_ROOT"
                    show_docker_status
                    ;;
                k8s|kubernetes)
                    show_k8s_status
                    ;;
            esac
            ;;
        build)
            check_prerequisites
            build_image
            ;;
        *)
            error "Unknown command: $2"
            show_help
            exit 1
            ;;
    esac
}

# Run main function
main "$@" 