use std::sync::Arc;

use anyhow::Result;
use crate::metrics::SharedRegistrar;
use crate::config::DatalakeConfig;
use crate::pool::{get_query_type, ClickhouseConnectionPool, ClickhouseError};

pub struct PoolManager {
    pool: Arc<ClickhouseConnectionPool>,
    config: Arc<DatalakeConfig>,
    metrics: Option<SharedRegistrar>,
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
        }
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
}
