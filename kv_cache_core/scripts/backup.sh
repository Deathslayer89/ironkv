#!/bin/bash

# IRONKV Backup Script
# This script creates backups of IRONKV data and configuration

set -e

# Configuration
BACKUP_DIR="/backups/ironkv"
DATA_DIR="/app/data"
CONFIG_DIR="/app/config"
LOG_DIR="/app/logs"
RETENTION_DAYS=30
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_NAME="ironkv_backup_${TIMESTAMP}.tar.gz"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

# Create backup directory if it doesn't exist
create_backup_dir() {
    log "Creating backup directory: $BACKUP_DIR"
    mkdir -p "$BACKUP_DIR"
}

# Create backup
create_backup() {
    log "Starting backup: $BACKUP_NAME"
    
    # Check if data directory exists
    if [ ! -d "$DATA_DIR" ]; then
        error "Data directory $DATA_DIR does not exist"
        exit 1
    fi
    
    # Create backup archive
    tar -czf "$BACKUP_DIR/$BACKUP_NAME" \
        -C /app \
        data config logs \
        --exclude='*.tmp' \
        --exclude='*.log' \
        --exclude='*.pid'
    
    if [ $? -eq 0 ]; then
        log "Backup created successfully: $BACKUP_NAME"
        log "Backup size: $(du -h "$BACKUP_DIR/$BACKUP_NAME" | cut -f1)"
    else
        error "Backup failed"
        exit 1
    fi
}

# Clean old backups
cleanup_old_backups() {
    log "Cleaning up backups older than $RETENTION_DAYS days"
    
    find "$BACKUP_DIR" -name "ironkv_backup_*.tar.gz" -type f -mtime +$RETENTION_DAYS -delete
    
    log "Cleanup completed"
}

# Verify backup
verify_backup() {
    log "Verifying backup integrity"
    
    if tar -tzf "$BACKUP_DIR/$BACKUP_NAME" > /dev/null 2>&1; then
        log "Backup verification successful"
    else
        error "Backup verification failed"
        exit 1
    fi
}

# Main execution
main() {
    log "Starting IRONKV backup process"
    
    create_backup_dir
    create_backup
    verify_backup
    cleanup_old_backups
    
    log "Backup process completed successfully"
}

# Handle script arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [OPTIONS]"
        echo "Options:"
        echo "  --help, -h     Show this help message"
        echo "  --verify       Only verify existing backups"
        echo "  --cleanup      Only cleanup old backups"
        echo "  --restore FILE Restore from backup file"
        exit 0
        ;;
    --verify)
        verify_backup
        exit 0
        ;;
    --cleanup)
        cleanup_old_backups
        exit 0
        ;;
    --restore)
        if [ -z "$2" ]; then
            error "Please specify backup file to restore from"
            exit 1
        fi
        log "Restoring from backup: $2"
        tar -xzf "$2" -C /app
        log "Restore completed"
        exit 0
        ;;
    "")
        main
        ;;
    *)
        error "Unknown option: $1"
        echo "Use --help for usage information"
        exit 1
        ;;
esac 