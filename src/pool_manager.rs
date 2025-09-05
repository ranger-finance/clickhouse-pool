use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::batch_processor::{BatchCommand, BatchSender};
use crate::config::DatalakeConfig;
use crate::metrics::SharedRegistrar;
use crate::pool::{get_query_type, ClickhouseConnectionPool, ClickhouseError};
use crate::traits::{Model, PartitionKey};
use anyhow::Result;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;
use tokio::time::interval;

#[derive(Clone)]
pub struct PoolManager {
    pool: Arc<ClickhouseConnectionPool>,
    config: Arc<DatalakeConfig>,
    metrics: Option<SharedRegistrar>,
    last_recycle_time: u64,
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
            last_recycle_time: 0,
        }
    }

    pub fn get_pool(&self) -> Arc<ClickhouseConnectionPool> {
        self.pool.clone()
    }

    pub fn seconds_since_last_recycle(&self) -> u64 {
        let last = self.last_recycle_time;

        if last == 0 {
            return u64::MAX;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now.saturating_sub(last)
    }

    pub async fn refill_connection_pool(&self) -> Result<usize, ClickhouseError> {
        let pool = self.get_pool();
        let status = pool.status();

        let current_total = status.size;
        let target_total = self.config.clickhouse.max_connections as usize;
        let deficit = target_total.saturating_sub(current_total);

        if deficit == 0 {
            log::info!("Deficit = 0");
            return Ok(0);
        }

        let to_add = deficit;
        log::info!("Attempting to add {} new connections to pool", to_add);

        let mut added = 0;

        for i in 0..to_add {
            match pool.get_connection().await {
                Ok(conn) => match conn.health_check().await {
                    Ok(_) => {
                        added += 1;
                    }
                    Err(e) => {
                        log::warn!("New connection failed health check: {}", e);
                    }
                },
                Err(e) => {
                    log::error!(
                        "Failed to create new connection {}/{}: {}",
                        i + 1,
                        to_add,
                        e
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }

        log::info!("Added {}/{} new connections to pool", added, to_add);
        Ok(added)
    }

    pub async fn recycle_idle_connections(
        &mut self,
        max_to_recycle: usize,
    ) -> Result<usize, ClickhouseError> {
        log::info!(
            "Starting connection recycling - checking up to {} connections",
            max_to_recycle
        );
        let start = std::time::Instant::now();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_recycle_time = now;

        let status = self.pool.status();
        log::info!("Clickhouse pool metrics: {:?}", status);

        let to_check = std::cmp::min(max_to_recycle, status.available);

        if to_check == 0 {
            log::info!("No connections available for recycling");
            return Ok(0);
        }

        log::info!(
            "Checking {} connections out of {} available",
            to_check,
            status.available
        );

        let mut recycled = 0;

        for _ in 0..to_check {
            match self.pool.get_connection().await {
                Ok(conn) => match conn.health_check().await {
                    Ok(_) => {
                        log::debug!(
                            "Connection [id: {}] is healthy, returning to pool",
                            conn.id()
                        );
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
                },
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

    pub fn create_batch_processor<M>(
        &self,
        batch_size: usize,
        max_wait_ms: u64,
    ) -> BatchSender<M::T>
    where
        M: Model + Send + Sync + 'static,
        M::T: Clone + Send + 'static,
    {
        let (tx, mut rx) = mpsc::channel(1000);

        let pool_manager = self.clone();

        tokio::spawn(async move {
            let mut batch = Vec::with_capacity(batch_size);
            let mut last_flush = Instant::now();
            let mut flush_interval = interval(Duration::from_millis(100));

            loop {
                tokio::select! {
                    cmd = rx.recv() => match cmd {
                        Some(BatchCommand::Add(item)) => {
                            batch.push(item);

                            if batch.len() >= batch_size {
                                Self::process_batch::<M>(&pool_manager, &mut batch).await;
                                last_flush = Instant::now();
                            }
                        },
                        Some(BatchCommand::Flush) => {
                            if !batch.is_empty() {
                                Self::process_batch::<M>(&pool_manager, &mut batch).await;
                                last_flush = Instant::now();
                            }
                        },
                        None => break,
                    },

                    _ = flush_interval.tick() => {
                        if !batch.is_empty() && last_flush.elapsed() >= Duration::from_millis(max_wait_ms) {
                            Self::process_batch::<M>(&pool_manager, &mut batch).await;
                            last_flush = Instant::now();
                        }
                    }
                }
            }

            if !batch.is_empty() {
                Self::process_batch::<M>(&pool_manager, &mut batch).await;
            }
        });

        BatchSender { tx }
    }

    async fn process_batch<M>(pool_manager: &PoolManager, batch: &mut Vec<M::T>)
    where
        M: Model,
        M::T: Clone,
    {
        if batch.is_empty() {
            return;
        }

        let items = std::mem::take(batch);

        let query = M::batch_insert_query(&items);

        match pool_manager.execute_with_retry(&query).await {
            Ok(_) => {
                log::info!(
                    "Successfully inserted {} items into {}",
                    items.len(),
                    M::table_name()
                );
            }
            Err(e) => {
                log::error!("Error inserting batch into {}: {}", M::table_name(), e);
                batch.extend(items);
            }
        }
    }

    pub fn create_partition_aware_batch_processor<M>(
        &self,
        batch_size: usize,
        max_wait_ms: u64,
        max_partitions_per_batch: usize,
    ) -> BatchSender<M::T>
    where
        M: Model + Send + Sync + 'static,
        M::T: Clone + Send + PartitionKey + 'static,
    {
        let (tx, mut rx) = mpsc::channel::<BatchCommand<M::T>>(1000);
        let pool_manager = self.clone();

        tokio::spawn(async move {
            let mut partition_batches: HashMap<String, Vec<M::T>> = HashMap::new();
            let mut last_flush = Instant::now();
            let mut flush_interval = interval(Duration::from_millis(100));

            loop {
                tokio::select! {
                    cmd = rx.recv() => match cmd {
                        Some(BatchCommand::Add(item)) => {
                            let partition_key = item.partition_key();
                            let partition_batch = partition_batches.entry(partition_key).or_insert_with(Vec::new);
                            partition_batch.push(item);

                            let should_flush_size = partition_batches.values().any(|batch| batch.len() >= batch_size);
                            
                            let should_flush_partitions = partition_batches.len() >= max_partitions_per_batch;

                            if should_flush_size || should_flush_partitions {
                                Self::process_partition_batches::<M>(&pool_manager, &mut partition_batches).await;
                                last_flush = Instant::now();
                            }
                        },
                        Some(BatchCommand::Flush) => {
                            if !partition_batches.is_empty() {
                                Self::process_partition_batches::<M>(&pool_manager, &mut partition_batches).await;
                                last_flush = Instant::now();
                            }
                        },
                        None => break,
                    },

                    _ = flush_interval.tick() => {
                        if !partition_batches.is_empty() && last_flush.elapsed() >= Duration::from_millis(max_wait_ms) {
                            Self::process_partition_batches::<M>(&pool_manager, &mut partition_batches).await;
                            last_flush = Instant::now();
                        }
                    }
                }
            }

            if !partition_batches.is_empty() {
                Self::process_partition_batches::<M>(&pool_manager, &mut partition_batches).await;
            }
        });

        BatchSender { tx }
    }

    async fn process_partition_batches<M>(
        pool_manager: &PoolManager, 
        partition_batches: &mut HashMap<String, Vec<M::T>>
    )
    where
        M: Model,
        M::T: Clone,
    {
        if partition_batches.is_empty() {
            return;
        }

        for (partition_key, batch) in partition_batches.drain() {
            if batch.is_empty() {
                continue;
            }

            let query = M::batch_insert_query(&batch);

            match pool_manager.execute_with_retry(&query).await {
                Ok(_) => {
                    log::info!(
                        "Successfully inserted {} items into {} for partition {}",
                        batch.len(),
                        M::table_name(),
                        partition_key
                    );
                }
                Err(e) => {
                    log::error!(
                        "Error inserting batch into {} for partition {}: {}", 
                        M::table_name(), 
                        partition_key, 
                        e
                    );
                }
            }
        }
    }
}
