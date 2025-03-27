use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse::Client;
use deadpool::managed::{Manager, Metrics, Object, PoolError, RecycleError};
use thiserror::Error;
use tokio::task;
use tokio::time::timeout;

use crate::config::{ClickhouseConfig, DatalakeConfig};
use crate::metrics::{Kind, MetricConfig, Registry, SharedRegistrar};

#[derive(Debug, Error)]
pub enum ClickhouseError {
    #[error("Clickhouse client error: {0}")]
    Client(#[from] clickhouse::error::Error),

    #[error("Connection validation failed: {0}")]
    Validation(String),

    #[error("Connection timed out")]
    Timeout,

    #[error("Pool error: {0}")]
    Pool(String),

    #[error("Shutdown in progress")]
    ShuttingDown,
}

impl From<tokio::time::error::Elapsed> for ClickhouseError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}

impl<T: std::fmt::Display> From<PoolError<T>> for ClickhouseError {
    fn from(value: PoolError<T>) -> Self {
        Self::Pool(value.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct PoolMetrics {
    pub size: usize,
    pub available: usize,
    pub in_use: usize,
    pub max_size: usize,
    pub min_size: usize,
    pub waiters: usize,
}

pub struct ClickhouseConnection {
    client: Client,
    last_used: Instant,
    id: u64,
    query_count: AtomicU64,
    created_at: Instant,
}

impl ClickhouseConnection {
    pub fn new(client: Client, id: u64) -> Self {
        Self {
            client,
            last_used: Instant::now(),
            id,
            query_count: AtomicU64::new(0),
            created_at: Instant::now(),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    pub fn idle_time(&self) -> Duration {
        self.last_used.elapsed()
    }

    pub fn query_count(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }

    pub async fn health_check(&self) -> Result<(), ClickhouseError> {
        match self.client.query("SELECT 1").execute().await {
            Ok(_) => Ok(()),
            Err(e) => Err(ClickhouseError::Client(e)),
        }
    }
}

impl Deref for ClickhouseConnection {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for ClickhouseConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl fmt::Debug for ClickhouseConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickhouseConnection")
            .field("id", &self.id)
            .field("created_at", &self.created_at)
            .field("query_count", &self.query_count)
            .field("last_used", &self.last_used)
            .finish()
    }
}

pub fn get_query_type(query: &str) -> &'static str {
    let query = query.trim_start().to_uppercase();

    if query.starts_with("SELECT") {
        "select"
    } else if query.starts_with("INSERT") {
        "insert"
    } else if query.starts_with("CREATE") {
        "create"
    } else if query.starts_with("ALTER") {
        "alter"
    } else if query.starts_with("DROP") {
        "drop"
    } else {
        "other"
    }
}

#[derive(Debug)]
pub struct ClickhouseConnectionManager {
    config: Arc<ClickhouseConfig>,
    next_connection_id: AtomicU64,
    is_shutting_down: Arc<AtomicBool>,
    metrics: Option<SharedRegistrar>,
}

impl ClickhouseConnectionManager {
    pub fn new(config: Arc<ClickhouseConfig>, metrics: Option<SharedRegistrar>) -> Self {
        Self {
            config,
            next_connection_id: AtomicU64::new(1),
            is_shutting_down: Arc::new(AtomicBool::new(false)),
            metrics,
        }
    }

    pub fn initiate_shutdown(&self) {
        self.is_shutting_down.store(true, Ordering::SeqCst);
        log::info!("Clickhouse connection manager shutdown in progress");
    }

    pub fn create_client(&self) -> Result<Client, ClickhouseError> {
        let url = self.config.authenticated_connection_url();

        let client = Client::default()
            .with_url(url)
            .with_user(&self.config.username)
            .with_password(&self.config.password)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "1");

        Ok(client)
    }
}

impl Manager for ClickhouseConnectionManager {
    type Type = ClickhouseConnection;
    type Error = ClickhouseError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        if self.is_shutting_down.load(Ordering::SeqCst) {
            return Err(ClickhouseError::ShuttingDown);
        }

        let connection_id = self.next_connection_id.fetch_add(1, Ordering::SeqCst);

        let start = Instant::now();

        let config = &self.config.clone();
        log::debug!(
            "Creating new Clickhouse connection [id: {}] to: {}:{}",
            connection_id,
            config.host,
            config.port
        );

        let client = self.create_client()?;

        let validation_timeout = Duration::from_secs(config.connect_timeout_seconds);

        let validation = match timeout(validation_timeout, client.query("SELECT 1").execute()).await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(ClickhouseError::Client(e)),
            Err(_) => Err(ClickhouseError::Timeout),
        };

        let duration = start.elapsed();
        if let Some(metrics) = &self.metrics {
            metrics.set_gauge_vec_mut(
                "clickhouse_connection_creation_second",
                &["create"],
                duration.as_secs_f64(),
            );
        }

        match validation {
            Ok(()) => {
                log::debug!(
                    "Connection established: [id: {}] in {:?}",
                    connection_id,
                    duration
                );

                if let Some(metrics) = &self.metrics {
                    metrics.inc_int_counter_vec_mut(
                        "clickhouse_connections_created_total",
                        &["success"],
                    );
                }

                Ok(ClickhouseConnection::new(client, connection_id))
            }
            Err(e) => {
                log::error!(
                    "Failed to validate ClickHouse connection (id: {}): {}",
                    connection_id,
                    e
                );

                if let Some(metrics) = &self.metrics {
                    metrics.inc_int_counter_vec_mut(
                        "clickhouse_connections_created_total",
                        &["failure"],
                    );
                }

                Err(e)
            }
        }
    }

    async fn recycle(
        &self,
        conn: &mut Self::Type,
        _: &Metrics,
    ) -> Result<(), RecycleError<Self::Error>> {
        if self.is_shutting_down.load(Ordering::SeqCst) {
            return Err(RecycleError::Message("Shutting down".into()));
        }

        log::debug!("Testing health of connection: [id: {}]", conn.id());

        let validation_timeout = Duration::from_secs(self.config.connect_timeout_seconds);

        match timeout(validation_timeout, conn.query("SELECT 1").execute()).await {
            Ok(Ok(_)) => {
                log::debug!("Connection [id: {}] health check passed", conn.id());

                if let Some(metrics) = &self.metrics {
                    metrics.inc_int_counter_vec_mut(
                        "clickhouse_connections_health_checks_total",
                        &["success"],
                    );
                }

                Ok(())
            }
            Ok(Err(e)) => {
                log::warn!("Connection [id: {}] health check failed: {}", conn.id(), e);

                if let Some(metrics) = &self.metrics {
                    metrics.inc_int_counter_vec_mut(
                        "clickhouse_connections_health_checks_total",
                        &["failure"],
                    );
                }

                Err(RecycleError::Message(
                    format!("Health check failed: {}", e).into(),
                ))
            }
            Err(_) => {
                log::warn!(
                    "Connection [id: {}] health check timed out after: {:?}",
                    conn.id(),
                    validation_timeout
                );

                if let Some(metrics) = &self.metrics {
                    metrics.inc_int_counter_vec_mut(
                        "clickhouse_connections_health_checks_total",
                        &["timeout"],
                    );
                }

                Err(RecycleError::Message("Health check timed out".into()))
            }
        }
    }
}

pub type Pool = deadpool::managed::Pool<ClickhouseConnectionManager>;
pub type PooledConnection = Object<ClickhouseConnectionManager>;

pub struct ClickhouseConnectionPool {
    pool: Pool,
    config: Arc<DatalakeConfig>,
    metrics: Option<SharedRegistrar>,
    is_initialized: AtomicBool,
}

impl ClickhouseConnectionPool {
    pub fn new(config: Arc<DatalakeConfig>, metrics: Option<SharedRegistrar>) -> Self {
        if let Some(metrics_ref) = &metrics {
            Self::register_metrics(metrics_ref);
        }

        let initial_size = config.clickhouse.max_connections as usize;

        let manager = ClickhouseConnectionManager::new(config.clickhouse.clone(), metrics.clone());

        let pool = deadpool::managed::Pool::<ClickhouseConnectionManager>::builder(manager)
            .max_size(initial_size)
            .build()
            .expect("Failed to build connection pool");

        Self {
            pool,
            config,
            metrics,
            is_initialized: AtomicBool::new(false),
        }
    }

    pub async fn initialize(&self) -> Result<(), ClickhouseError> {
        if self.is_initialized.load(Ordering::SeqCst) {
            return Ok(());
        }

        log::info!("Initializing Clickhouse connection pool");

        let warmup_count = self.config.clickhouse.max_connections as usize;

        let mut warmup_handles = Vec::with_capacity(warmup_count);

        for i in 0..warmup_count {
            let pool = self.pool.clone();

            let handle = task::spawn(async move {
                match pool.get().await {
                    Ok(conn) => match conn.health_check().await {
                        Ok(_) => {
                            log::debug!("Warm-up connection {} initialized successfully", i);
                            Ok(())
                        }
                        Err(e) => {
                            log::error!("Warm-up connection {} health check failed: {}", i, e);
                            Err(e)
                        }
                    },
                    Err(e) => {
                        log::error!("Failed to get warm-up connection {}: {}", i, e);
                        Err(ClickhouseError::Pool(e.to_string()))
                    }
                }
            });
            warmup_handles.push(handle);
        }

        let mut warmup_success_count = 0;
        for (i, handle) in warmup_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(_)) => {
                    warmup_success_count += 1;
                }
                Ok(Err(e)) => {
                    log::warn!("Warm-up connection {} failed: {}", i, e);
                }
                Err(e) => {
                    log::error!("Warm-up task {} panicked: {}", i, e);
                }
            }
        }

        log::info!(
            "Connection pool warm-up complete: {}/{} successful",
            warmup_success_count,
            warmup_count
        );

        self.is_initialized.store(true, Ordering::SeqCst);

        if let Some(metrics) = &self.metrics {
            let status = self.pool.status();
            metrics.set_int_gauge_vec_mut(
                "clickhouse_pool_connections",
                &["available"],
                status.available as i64,
            );
            metrics.set_int_gauge_vec_mut(
                "clickhouse_pool_connections",
                &["size"],
                status.size as i64,
            );
        }

        Ok(())
    }

    pub async fn get_connection(&self) -> Result<PooledConnection, ClickhouseError> {
        if !self.is_initialized.load(Ordering::SeqCst) {
            log::warn!("Attempting to get connection from uninitialized pool");
        }

        let start = Instant::now();

        let timeout_duration = Duration::from_secs(self.config.clickhouse.connect_timeout_seconds);

        match tokio::time::timeout(timeout_duration, self.pool.get()).await {
            Ok(Ok(conn)) => {
                let duration = start.elapsed();

                if let Some(metrics) = &self.metrics {
                    metrics.set_gauge_vec_mut(
                        "clickhouse_connection_acquisition_seconds",
                        &["success"],
                        duration.as_secs_f64(),
                    );
                    metrics.inc_int_counter_vec_mut(
                        "clickhouse_connection_acquisition_total",
                        &["success"],
                    );
                }

                log::debug!("Connection acquired in {:?}", duration);
                Ok(conn)
            }
            Ok(Err(e)) => {
                if let Some(metrics) = &self.metrics {
                    metrics.inc_int_counter_vec_mut(
                        "clickhouse_connection_acquisition_total",
                        &["failure"],
                    );
                }

                log::warn!("Failed to get connection from pool: {}", e);
                Err(ClickhouseError::Pool(e.to_string()))
            }
            Err(_) => {
                if let Some(metrics) = &self.metrics {
                    metrics.inc_int_counter_vec_mut(
                        "clickhouse_connection_acquisition_total",
                        &["timeout"],
                    );
                }

                log::warn!(
                    "Timed out waiting for connection after {:?}",
                    timeout_duration
                );
                Err(ClickhouseError::Timeout)
            }
        }
    }

    pub async fn shutdown(&self) -> Result<(), ClickhouseError> {
        log::info!("Initiating graceful shutdown of ClickHouse connection pool");

        let pool_manager = self.pool.manager();
        pool_manager.initiate_shutdown();

        let status = self.pool.status();
        log::info!(
            "Connection pool status before shutdown: size={}, available={}, in_use={}",
            status.size,
            status.available,
            status.size - status.available
        );

        let drain_timeout = Duration::from_secs(30);
        let drain_start = Instant::now();

        loop {
            let status = self.pool.status();
            let in_use = status.size - status.available;

            if in_use == 0 {
                log::info!("All connections returned to pool, proceeding with shutdown");
                break;
            }

            if drain_start.elapsed() > drain_timeout {
                log::warn!(
                    "Shutdown drain timeout exceeded, {} connections still in use",
                    in_use
                );

                break;
            }

            log::info!("Waiting for {} connections to be returned to pool", in_use);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Close all connections
        self.pool.close();
        log::info!("All connections closed");

        log::info!("ClickHouse connection pool shutdown complete");

        Ok(())
    }

    fn register_metrics(metrics: &SharedRegistrar) {
        let metric_configs = [
            // Connection
            MetricConfig {
                kind: Kind::IntCounterVec,
                name: "clickhouse_connections_created_total",
                help: "Total no. of connections created",
                label_names: &["status"],
            },
            MetricConfig {
                kind: Kind::IntGaugeVec,
                name: "clickhouse_pool_connections",
                help: "Current no. of connections in the pool",
                label_names: &["state"],
            },
            MetricConfig {
                kind: Kind::IntCounterVec,
                name: "clickhouse_connetion_health_checks_total",
                help: "Total no. of connection health checks",
                label_names: &["status"],
            },
            MetricConfig {
                kind: Kind::GaugeVec,
                name: "clickhouse_connection_creation_seconds",
                help: "Time taken to create connections",
                label_names: &["operation"],
            },
            // Queries
            MetricConfig {
                kind: Kind::IntCounterVec,
                name: "clickhouse_queries_total",
                help: "Total no. of queries executed",
                label_names: &["type"],
            },
            MetricConfig {
                kind: Kind::IntCounterVec,
                name: "clickhouse_query_errors_total",
                help: "Total no. of query errors",
                label_names: &["type"],
            },
            MetricConfig {
                kind: Kind::GaugeVec,
                name: "clickhouse_query_duration_seconds",
                help: "Query execution time in seconds",
                label_names: &["type"],
            },
            // Batch queries
            MetricConfig {
                kind: Kind::IntCounterVec,
                name: "clickhouse_batch_query_errors_total",
                help: "Total number of batch query errors",
                label_names: &["type"],
            },
            MetricConfig {
                kind: Kind::GaugeVec,
                name: "clickhouse_batch_query_duration_seconds",
                help: "Batch query execution time in seconds",
                label_names: &["type"],
            },
            // Connection acquisition
            MetricConfig {
                kind: Kind::GaugeVec,
                name: "clickhouse_connection_acquisition_seconds",
                help: "Time taken to acquire a connection from the pool",
                label_names: &["status"],
            },
            MetricConfig {
                kind: Kind::IntCounterVec,
                name: "clickhouse_connection_acquisition_total",
                help: "Total number of connection acquisition attempts",
                label_names: &["status"],
            },
        ];

        metrics.with_metric_configs(&metric_configs).ok();
    }
}
