//! Projection consumer - async event processing for a single projection.
//!
//! The consumer manages event consumption for one projection using JS-driven mode:
//! JavaScript polls for events, processes them, and sends operations back.
//!
//! The consumer uses a `ProjectionInstance` for SQLite operations.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::Mutex;

use spitedb::{GlobalPos, SpiteDB};

use super::error::ProjectionError;
use super::instance::ProjectionInstance;
use super::operation::ProjectionOp;
use super::schema::ProjectionSchema;

/// Default batch size for catch-up reads.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// Configuration for a projection consumer.
#[derive(Debug, Clone)]
pub struct ProjectionConsumerConfig {
    /// Name of the projection.
    pub name: String,

    /// Base directory for projection databases.
    pub db_dir: PathBuf,

    /// Schema definition.
    pub schema: ProjectionSchema,

    /// Batch size for processing.
    pub batch_size: usize,
}

impl ProjectionConsumerConfig {
    /// Creates a new consumer config.
    pub fn new(name: impl Into<String>, db_dir: PathBuf, schema: ProjectionSchema) -> Self {
        Self {
            name: name.into(),
            db_dir,
            schema,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Sets the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

/// A projection consumer manages event consumption for a single projection.
///
/// Each consumer has:
/// - Its own `ProjectionInstance` (separate SQLite database)
/// - Methods for JS-driven consumption
pub struct ProjectionConsumer {
    /// The projection instance (owns the SQLite database).
    instance: Arc<Mutex<ProjectionInstance>>,

    /// Configuration.
    config: ProjectionConsumerConfig,

    /// Reference to the event store (for reading events).
    event_store: Arc<SpiteDB>,
}

impl ProjectionConsumer {
    /// Creates a new projection consumer.
    ///
    /// Opens or creates the projection's SQLite database.
    pub fn new(
        config: ProjectionConsumerConfig,
        event_store: Arc<SpiteDB>,
    ) -> Result<Self, ProjectionError> {
        let instance = ProjectionInstance::new(&config.name, &config.db_dir, config.schema.clone())?;

        Ok(Self {
            instance: Arc::new(Mutex::new(instance)),
            config,
            event_store,
        })
    }

    /// Returns the projection name.
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Returns a reference to the projection instance.
    pub fn instance(&self) -> Arc<Mutex<ProjectionInstance>> {
        Arc::clone(&self.instance)
    }

    // =========================================================================
    // JS-Driven Mode
    // =========================================================================

    /// Gets the current checkpoint (for JS-driven mode).
    ///
    /// Use `spawn_blocking` when calling from an async context.
    pub async fn get_checkpoint(&self) -> Result<Option<i64>, ProjectionError> {
        let instance = Arc::clone(&self.instance);

        tokio::task::spawn_blocking(move || {
            let guard = instance.blocking_lock();
            guard.get_checkpoint()
        })
        .await
        .map_err(|e| ProjectionError::Internal(format!("Task join error: {}", e)))?
    }

    /// Reads a row by primary key (for JS-driven mode).
    pub async fn read_row(
        &self,
        key: &str,
    ) -> Result<Option<serde_json::Value>, ProjectionError> {
        let instance = Arc::clone(&self.instance);
        let key = key.to_string();

        tokio::task::spawn_blocking(move || {
            let guard = instance.blocking_lock();
            guard.read_row(&key)
        })
        .await
        .map_err(|e| ProjectionError::Internal(format!("Task join error: {}", e)))?
    }

    /// Applies a batch of operations (for JS-driven mode).
    ///
    /// The checkpoint is updated atomically with the data changes.
    pub async fn apply_batch(
        &self,
        operations: Vec<ProjectionOp>,
        checkpoint: i64,
    ) -> Result<(), ProjectionError> {
        let instance = Arc::clone(&self.instance);

        tokio::task::spawn_blocking(move || {
            let mut guard = instance.blocking_lock();
            guard.apply_batch(operations, checkpoint)
        })
        .await
        .map_err(|e| ProjectionError::Internal(format!("Task join error: {}", e)))?
    }

    /// Gets events for JS processing.
    ///
    /// Reads events from the global log starting after the current checkpoint.
    /// Returns None if there are no new events.
    pub async fn get_events(
        &self,
        batch_size: usize,
    ) -> Result<Option<(Vec<spitedb::Event>, i64)>, ProjectionError> {
        let checkpoint = self.get_checkpoint().await?;

        let from_pos = checkpoint
            .map(|p| GlobalPos::from_raw((p + 1) as u64))
            .unwrap_or(GlobalPos::FIRST);

        let events = self
            .event_store
            .read_global(from_pos, batch_size)
            .await
            .map_err(|e| ProjectionError::Internal(format!("Read error: {}", e)))?;

        if events.is_empty() {
            return Ok(None);
        }

        let batch_id = from_pos.as_raw() as i64;
        Ok(Some((events, batch_id)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::operation::OpType;
    use crate::projection::schema::{ColumnDef, ColumnType};
    use spitedb::crypto::{BatchCryptor, EnvKeyProvider};
    use tempfile::TempDir;

    fn test_cryptor() -> BatchCryptor {
        let key = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
        ];
        BatchCryptor::new(EnvKeyProvider::from_key(key))
    }

    fn create_test_schema() -> ProjectionSchema {
        ProjectionSchema {
            table_name: "test_projection".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::Text,
                    primary_key: true,
                    nullable: false,
                    default_value: None,
                },
                ColumnDef {
                    name: "count".to_string(),
                    col_type: ColumnType::Integer,
                    primary_key: false,
                    nullable: false,
                    default_value: None,
                },
            ],
        }
    }

    #[tokio::test]
    async fn test_consumer_js_driven_mode() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("events.db");
        let event_store = Arc::new(SpiteDB::open_with_cryptor(&db_path, test_cryptor()).await.unwrap());

        let config = ProjectionConsumerConfig::new("test", temp_dir.path().to_path_buf(), create_test_schema());
        let consumer = ProjectionConsumer::new(config, event_store).unwrap();

        // Initially no checkpoint
        let checkpoint = consumer.get_checkpoint().await.unwrap();
        assert!(checkpoint.is_none());

        // Apply a batch
        let ops = vec![ProjectionOp {
            op_type: OpType::Upsert,
            key: "key1".to_string(),
            value: Some(serde_json::json!({"count": 42})),
        }];
        consumer.apply_batch(ops, 100).await.unwrap();

        // Check checkpoint updated
        let checkpoint = consumer.get_checkpoint().await.unwrap();
        assert_eq!(checkpoint, Some(100));

        // Read the row
        let row = consumer.read_row("key1").await.unwrap();
        assert!(row.is_some());
        assert_eq!(row.unwrap()["count"], 42);
    }
}
