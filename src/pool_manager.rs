use std::sync::Arc;

use crate::config::DatalakeConfig;
use crate::metrics::SharedRegistrar;
use crate::pool::{get_query_type, ClickhouseConnectionPool, ClickhouseError};
use anyhow::Result;
use serde::de::DeserializeOwned;

pub struct PoolManager {
    pool: Arc<ClickhouseConnectionPool>,
    config: Arc<DatalakeConfig>,
    metrics: Option<SharedRegistrar>,
    last_recycle_time: std::sync::atomic::AtomicU64,
}

impl PoolManager {
    pub async fn new(config: Arc<DatalakeConfig>, metrics: Option<SharedRegistrar>) -> Self {
        let pool = Arc::new(ClickhouseConnectionPool::new(
            config.clone(),
            metrics.clone(),
        ));

        let _ = match pool.initialize().await {
            Ok(_) => {
                log::debug!("Pool warmed up and initialized successfully");
            }
            Err(e) => {
                log::error!("Error warming up and initializing pool: {:?}", e);
            }
        };

        Self {
            pool,
            config,
            metrics,
            last_recycle_time: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn seconds_since_last_recycle(&self) -> u64 {
        let last = self.last_recycle_time.load(std::sync::atomic::Ordering::SeqCst);
        if last == 0 {
            return u64::MAX;
        }
        
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        now.saturating_sub(last)
    }

    pub async fn recycle_idle_connections(&self, max_to_recycle: usize) -> Result<usize, ClickhouseError> {
        log::info!("Starting connection recycling - checking up to {} connections", max_to_recycle);
        let start = std::time::Instant::now();
        
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_recycle_time.store(now, std::sync::atomic::Ordering::SeqCst);
        
        let status = self.pool.status();
        
        let to_check = std::cmp::min(max_to_recycle, status.available);
        
        if to_check == 0 {
            log::info!("No connections available for recycling");
            return Ok(0);
        }
        
        log::info!("Checking {} connections out of {} available", to_check, status.available);
        
        let mut recycled = 0;
        
        for _ in 0..to_check {
            match self.pool.get_connection().await {
                Ok(conn) => {
                    match conn.health_check().await {
                        Ok(_) => {
                            log::debug!("Connection [id: {}] is healthy, returning to pool", conn.id());
                        }
                        Err(e) => {
                            log::warn!(
                                "Connection [id: {}] failed health check: {}, will be recycled",
                                conn.id(),
                                e
                            );
                            recycled += 1;
                            
                            if let Some(metrics) = &self.metrics {
                                metrics.inc_int_counter_vec_mut(
                                    "clickhouse_connections_recycled_total",
                                    &["health_check_failed"],
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to get connection for health check: {}", e);
                }
            }
        }
        
        let duration = start.elapsed();
        log::info!(
            "Connection recycling complete: recycled={} in {:?}",
            recycled,
            duration
        );
        
        if let Some(metrics) = &self.metrics {
            metrics.set_gauge_vec_mut(
                "clickhouse_connection_recycling_seconds",
                &["total"],
                duration.as_secs_f64(),
            );
        }
        
        Ok(recycled)
    }

    pub async fn execute_with_retry(&self, query: &str) -> Result<(), ClickhouseError> {
        let mut attempt = 0;
        let max_retries = self.config.retry.max_retries;

        loop {
            attempt += 1;

            let conn = match self.pool.get_connection().await {
                Ok(conn) => conn,
                Err(e) => {
                    if attempt > max_retries {
                        log::error!("Failed to get connection after {} attempts: {}", attempt, e);
                        return Err(e);
                    }

                    let backoff = self.config.retry.backoff_duration(attempt);
                    log::warn!(
                        "Failed to get connection (attempt {}/{}), retrying in {:?}: {}",
                        attempt,
                        max_retries,
                        backoff,
                        e
                    );

                    tokio::time::sleep(backoff).await;
                    continue;
                }
            };

            match conn.query(query).execute().await {
                Ok(response) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_int_counter_vec_mut(
                            "clickhouse_query_success_total",
                            &[get_query_type(query)],
                        );
                    }

                    return Ok(response);
                }
                Err(e) => {
                    if attempt > max_retries {
                        log::error!("Query failed after {} attempts: {}", attempt, e);
                        return Err(ClickhouseError::Client(e));
                    }

                    let backoff = self.config.retry.backoff_duration(attempt);
                    log::warn!(
                        "Query failed (attempt {}/{}), retrying in {:?}: {}\nQuery: {}",
                        attempt,
                        max_retries,
                        backoff,
                        e,
                        query
                    );

                    if let Some(metrics) = &self.metrics {
                        metrics.inc_int_counter_vec_mut(
                            "clickhouse_query_retries_total",
                            &[get_query_type(query)],
                        );
                    }

                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    pub async fn execute_select_with_retry<T>(&self, query: &str) -> Result<Vec<T>, ClickhouseError>
    where
        T: clickhouse::Row + DeserializeOwned + Send + 'static,
    {
        let mut attempt = 0;
        let max_retries = self.config.retry.max_retries;

        loop {
            attempt += 1;

            let conn = match self.pool.get_connection().await {
                Ok(conn) => conn,
                Err(e) => {
                    if attempt > max_retries {
                        log::error!("Failed to get connection after {} attempts: {}", attempt, e);
                        return Err(e);
                    }

                    let backoff = self.config.retry.backoff_duration(attempt);
                    log::warn!(
                        "Failed to get connection (attempt {}/{}), retrying in {:?}: {}",
                        attempt,
                        max_retries,
                        backoff,
                        e
                    );

                    tokio::time::sleep(backoff).await;
                    continue;
                }
            };

            match conn.query(query).fetch_all::<T>().await {
                Ok(response) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_int_counter_vec_mut(
                            "clickhouse_query_success_total",
                            &[get_query_type(query)],
                        );
                    }

                    return Ok(response);
                }
                Err(e) => {
                    if attempt > max_retries {
                        log::error!("Query failed after {} attempts: {}", attempt, e);
                        return Err(ClickhouseError::Client(e));
                    }

                    let backoff = self.config.retry.backoff_duration(attempt);
                    log::warn!(
                        "Query failed (attempt {}/{}), retrying in {:?}: {}\nQuery: {}",
                        attempt,
                        max_retries,
                        backoff,
                        e,
                        query
                    );

                    if let Some(metrics) = &self.metrics {
                        metrics.inc_int_counter_vec_mut(
                            "clickhouse_query_retries_total",
                            &[get_query_type(query)],
                        );
                    }

                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}
