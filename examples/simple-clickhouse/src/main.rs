use std::error::Error;
use std::sync::Arc;
use uuid::Uuid;
use tokio::signal;
use tokio::time::sleep;
use clickhouse_connection_pool::{
    pool_manager::PoolManager,
    config::{DatalakeConfig, ClickhouseConfig, RetryConfig},
    traits::Model
};
use anyhow::Result;
use std::time::Duration;

mod model;

use crate::model::Price;


#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let datalake_config = load_config().unwrap();

    let pool_manager = create_manager_and_instantiate_table(datalake_config).await.unwrap();

    println!("Sleeping...");
    sleep(Duration::from_secs(3)).await;
    println!("Awake...");
        
    let _ = insert_test_data(&pool_manager).await;
    sleep(Duration::from_secs(3)).await;

    let prices = get_all_prices(&pool_manager, Some(20), None).await?; 
    println!("Retrieved 20 prices: {:?}", prices);
    
    println!("Service is running. Press Ctrl+C to shutdown.");
    match signal::ctrl_c().await {
        Ok(()) => println!("Shutdown signal received"),
        Err(e) => println!("Failed to listen for shutdown signal: {}", e),
    }
        
    println!("Service shutdown complete");
    Ok(())
}

fn load_config() -> Result<Arc<DatalakeConfig>, Box<dyn Error>> {
    let host = std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("CLICKHOUSE_PORT")
        .map(|p| p.parse::<u16>().unwrap_or(9000))
        .unwrap_or(9000);
    let database = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "test".to_string());
    let username = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
    
    let config = ClickhouseConfig::new(host, port, database, username, password, 10, 30, 5);
        
    let retry_config = RetryConfig::default();

    let datalake_config = Arc::new(DatalakeConfig::new(config, retry_config));
    
    Ok(datalake_config)
}

async fn create_manager_and_instantiate_table(
    config: Arc<DatalakeConfig>,
) -> Result<Arc<PoolManager>, Box<dyn Error>> {
    let manager = PoolManager::new(
        config,
        None,
    ).await;
    
    let _ = match manager
        .execute_with_retry(Price::create_table_sql())
        .await {
            Ok(_) => {
                println!("DB init successful");
                Ok(())
            },
            Err(e) => {
                println!("Error initing db: {:?}", e);
                Err(e)
            }
        };

    Ok(Arc::new(manager))
}

async fn insert_test_data(pool_manager: &Arc<PoolManager>) -> Result<(), Box<dyn Error>> {
    println!("Inserting test data");
    
    let mut prices = Vec::new();
    for i in 0..100 {
        for asset in ["SOL", "BTC"] {
            prices.push(Price::new(
                Uuid::new_v4(),
                asset.to_string(),
                42.58 + (i as f64 * 0.01),
            ));
        }
    }

    if prices.is_empty() {
        return Ok(());
    }

    for chunk in prices.chunks(20) {
        let blob = chunk.to_vec();

        let query = Price::batch_insert_query(blob.as_slice());

        pool_manager.execute_with_retry(&query)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to insert price: {}", e))?;
    }

    println!("Inserted {} test prices", prices.len());
    
    Ok(())
}

async fn get_all_prices(pool_manager: &Arc<PoolManager>, limit: Option<u64>, offset: Option<u64>) -> Result<Vec<Price>> {
    let query = Price::build_query(None, limit, offset);
    
    let result = pool_manager.execute_select_with_retry(&query).await?;
        
    Ok(result)
}