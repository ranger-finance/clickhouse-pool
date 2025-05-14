use tokio::sync::mpsc;

use crate::pool::ClickhouseError;

pub enum BatchCommand<T> {
    Add(T),
    Flush,
}

pub struct BatchSender<T> {
    pub tx: mpsc::Sender<BatchCommand<T>>,
}

impl<T> BatchSender<T> {
    pub async fn add(&self, item: T) -> Result<(), ClickhouseError> {
        self.tx
            .send(BatchCommand::Add(item))
            .await
            .map_err(|_| ClickhouseError::BatchInsertionError("Channel closed".to_string()))
    }

    pub async fn flush(&self) -> Result<(), ClickhouseError> {
        self.tx
            .send(BatchCommand::Flush)
            .await
            .map_err(|_| ClickhouseError::BatchInsertionError("Channel closed".to_string()))
    }
}

impl<T> Clone for BatchSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}
