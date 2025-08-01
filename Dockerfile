# Multi-stage build for IRONKV
FROM rust:1.75-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy Cargo files
COPY kv_cache_core/Cargo.toml kv_cache_core/
COPY kv_cache_server/Cargo.toml kv_cache_server/

# Create dummy main.rs files to cache dependencies
RUN mkdir -p kv_cache_core/src kv_cache_server/src
RUN echo "fn main() {}" > kv_cache_core/src/lib.rs
RUN echo "fn main() {}" > kv_cache_server/src/main.rs

# Build dependencies
RUN cargo build --release --bin kv_cache_server

# Copy source code
COPY kv_cache_core/src/ kv_cache_core/src/
COPY kv_cache_server/src/ kv_cache_server/src/

# Build the application
RUN cargo build --release --bin kv_cache_server

# Runtime stage
FROM debian:bullseye-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    redis-tools \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create ironkv user
RUN groupadd -r ironkv && useradd -r -g ironkv ironkv

# Create directories
RUN mkdir -p /var/lib/ironkv /var/log/ironkv /etc/ironkv
RUN chown -R ironkv:ironkv /var/lib/ironkv /var/log/ironkv /etc/ironkv

# Copy binary from builder
COPY --from=builder /app/target/release/kv_cache_server /usr/local/bin/ironkv
RUN chown ironkv:ironkv /usr/local/bin/ironkv
RUN chmod +x /usr/local/bin/ironkv

# Switch to ironkv user
USER ironkv

# Expose ports
EXPOSE 6379 6380 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD redis-cli -p 6379 ping || exit 1

# Default command
CMD ["ironkv", "--cluster", "--node-id", "${IRONKV_NODE_ID:-node1}", "--cluster-port", "${IRONKV_CLUSTER_PORT:-6380}"] 