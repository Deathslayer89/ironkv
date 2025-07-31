# Multi-stage build for IRONKV distributed key-value store
FROM rust:1.75-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy Cargo files
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./

# Create dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies (this layer will be cached)
RUN cargo build --release

# Remove dummy main.rs and copy actual source code
RUN rm src/main.rs
COPY src/ ./src/
COPY proto/ ./proto/

# Build the application
RUN cargo build --release

# Production stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r ironkv && useradd -r -g ironkv ironkv

# Create necessary directories
RUN mkdir -p /app/data /app/logs /app/certs && \
    chown -R ironkv:ironkv /app

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/target/release/kv_cache_server /app/ironkv

# Copy configuration files
COPY config.toml /app/config.toml

# Switch to non-root user
USER ironkv

# Expose ports
EXPOSE 8080 8081 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Default command
CMD ["/app/ironkv"] 