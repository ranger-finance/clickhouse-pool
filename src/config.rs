use serde_derive::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use url::Url;

pub const CONNECTION_TIMEOUT_SECONDS_DEFAULT: u64 = 3;
pub const QUERY_TIMEOUT_SECONDS_DEFAULT: u64 = 10;
pub const POOL_MAX_CONNECTIONS_DEFAULT: u32 = 256;
pub const PORT_DEFAULT: u16 = 9000;

pub const MAX_RETRIES_DEFAULT: u32 = 5;
pub const INITIAL_BACKOFF_MS_DEFAULT: u64 = 100;
pub const MAX_BACKOFF_MS_DEFAULT: u64 = 10_000;
pub const BACKOFF_MULTIPLIER_DEFAULT: f64 = 2.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickhouseConfig {
    /// Database host
    pub host: String,

    /// Database port
    pub port: u16,

    /// Database name
    pub database: String,

    /// Username for authentication
    pub username: String,

    /// Password for authentication
    pub password: String,

    /// Connection timeout in seconds
    #[serde(default)]
    pub connect_timeout_seconds: u64,

    /// Query timeout in seconds
    #[serde(default)]
    pub query_timeout_seconds: u64,

    /// Max. number of connections in the pool
    #[serde(default)]
    pub max_connections: u32,
}

impl Default for ClickhouseConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: PORT_DEFAULT,
            database: "default".to_string(),
            username: "default".to_string(),
            password: String::new(),
            connect_timeout_seconds: CONNECTION_TIMEOUT_SECONDS_DEFAULT,
            query_timeout_seconds: QUERY_TIMEOUT_SECONDS_DEFAULT,
            max_connections: POOL_MAX_CONNECTIONS_DEFAULT,
        }
    }
}

impl ClickhouseConfig {
    pub fn new(
        host: String,
        port: u16,
        database: String,
        username: String,
        password: String,
        connect_timeout_seconds: u64,
        query_timeout_seconds: u64,
        max_connections: u32,
    ) -> Self {
        Self {
            host,
            port,
            database,
            username,
            password,
            connect_timeout_seconds,
            query_timeout_seconds,
            max_connections,
        }
    }

    pub fn connection_url(&self) -> Url {
        let url_str = format!("https://{}:{}", self.host, self.port,);

        Url::parse(&url_str).expect("Failed to parse Clickhouse URL")
    }

    pub fn authenticated_connection_url(&self) -> Url {
        let url_str = format!(
            "https://{}:{}@{}:{}",
            self.username, self.password, self.host, self.port,
        );

        Url::parse(&url_str).expect("Failed to parse authenticated Clickhouse URL")
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_seconds)
    }

    pub fn query_timeout(&self) -> Duration {
        Duration::from_secs(self.query_timeout_seconds)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Max number of retry attempts
    pub max_retries: u32,

    /// Initial backoff duration in milliseconds
    pub initial_backoff_ms: u64,

    /// Max backoff duration in milliseconds
    pub max_backoff_ms: u64,

    /// Backoff multiplier for exponential backoff
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: MAX_RETRIES_DEFAULT,
            initial_backoff_ms: INITIAL_BACKOFF_MS_DEFAULT,
            max_backoff_ms: MAX_BACKOFF_MS_DEFAULT,
            backoff_multiplier: BACKOFF_MULTIPLIER_DEFAULT,
        }
    }
}

impl RetryConfig {
    pub fn backoff_duration(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::from_millis(0);
        }

        let backoff_ms = (self.initial_backoff_ms as f64
            * self.backoff_multiplier.powi(attempt as i32 - 1)) as u64;
        let capped_backoff_ms = std::cmp::min(backoff_ms, self.max_backoff_ms);

        Duration::from_millis(capped_backoff_ms)
    }
}

#[derive(Debug, Clone)]
pub struct DatalakeConfig {
    pub clickhouse: Arc<ClickhouseConfig>,
    pub retry: Arc<RetryConfig>,
}

impl Default for DatalakeConfig {
    fn default() -> Self {
        Self {
            clickhouse: Arc::new(ClickhouseConfig::default()),
            retry: Arc::new(RetryConfig::default()),
        }
    }
}

impl DatalakeConfig {
    pub fn new(clickhouse_config: ClickhouseConfig, retry: RetryConfig) -> Self {
        Self {
            clickhouse: Arc::new(clickhouse_config),
            retry: Arc::new(retry),
        }
    }
}
