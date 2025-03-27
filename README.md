# ClickHouse Connection Pool

A connection pooling library for ClickHouse in Rust, built on top of deadpool.

## Features

- **Connection Pooling**: Efficiently reuse existing connections for improved performance
- **Auto-Retry**: Automatic retries with exponential backoff
- **Health Checks**: Periodic health checks to ensure connections are healthy
- **Metrics Integration**: Prometheus-compatible metrics for monitoring
- **Graceful Shutdown**: Graceful shutting down and clean up of existing connections
- **Thread Safety**: Support for concurrency

## Installation

Add this to your Cargo.toml:

```toml
[dependencies]
clickhouse-pool = "0.1.0"
```

## Quickstart

```rust
use clickhouse_pool::{ClickHouseConfig, ClickHousePool, RetryConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration
    let config = ClickHouseConfig::new(
        "localhost".to_string(),
        9000,
        "default".to_string(),
        "default".to_string(),
        "".to_string(),
    );
    
    // Configure retry policy
    let retry_config = RetryConfig::default();
    
    // Create pool
    let pool = ClickHousePool::new(
        config,
        retry_config,
        None, // Optional metrics
    );
    
    // Initialize pool (creates initial connections)
    pool.initialize().await?;
    
    // Execute a query with automatic retries
    let result = pool.execute_with_retry("SELECT 1").await?;
    
    // Process result
    let mut stream = result.into_stream();
    while let Some(row) = stream.try_next().await? {
        println!("Value: {}", row.get::<i32, _>(0)?);
    }
    
    // Graceful shutdown (waits for in-use connections)
    pool.shutdown().await?;
    
    Ok(())
}
```

## Configuration

### ClickHouseConfig

```rust
let config = ClickHouseConfig::new(
    "localhost".to_string(),  // Host
    9000,                    // Port
    "my_database".to_string(), // Database name
    "username".to_string(),   // Username
    "password".to_string(),   // Password
);

// Set timeouts
config.connect_timeout_seconds = 10;
config.query_timeout_seconds = 30;
```

### RetryConfig

```rust
let retry_config = RetryConfig {
    max_retries: 3,
    initial_backoff_ms: 100,
    max_backoff_ms: 5000,
    backoff_multiplier: 2.0,
};
```

### Connection Health and Statistics

```rust
// Get current pool status
let status = pool.status();
println!("Active connections: {}", status.size);
println!("Available connections: {}", status.available);

// Get detailed statistics
let stats = pool.stats();
println!("Active: {}, In use: {}, Waiters: {}", 
    stats.size, stats.in_use, stats.waiters);
```