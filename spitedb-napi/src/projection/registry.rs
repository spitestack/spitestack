//! Projection registry - manages all projection consumers.
//!
//! The registry provides a central point for:
//! - Registering projections
//! - Starting/stopping consumers
//! - Getting projection instances for direct operations

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use spitedb::SpiteDB;

use super::consumer::{ProjectionConsumer, ProjectionConsumerConfig};
use super::error::ProjectionError;
use super::instance::ProjectionInstance;
use super::operation::ProjectionOp;
use super::schema::ProjectionSchema;
use tokio::sync::Mutex;

/// Registry for all projection consumers.
///
/// Each projection has:
/// - Its own SQLite database file (`{db_dir}/{name}.db`)
/// - Its own consumer for event processing
/// - Independent checkpoint tracking
pub struct ProjectionRegistry {
    /// Base directory for projection databases.
    db_dir: PathBuf,

    /// Reference to the event store.
    event_store: Arc<SpiteDB>,

    /// Registered consumers.
    consumers: HashMap<String, ProjectionConsumer>,
}

impl ProjectionRegistry {
    /// Creates a new projection registry.
    ///
    /// # Arguments
    ///
    /// * `db_dir` - Directory where projection databases will be stored
    /// * `event_store` - Reference to the SpiteDB event store
    pub fn new(db_dir: PathBuf, event_store: Arc<SpiteDB>) -> Result<Self, ProjectionError> {
        // Ensure the directory exists
        std::fs::create_dir_all(&db_dir)?;

        Ok(Self {
            db_dir,
            event_store,
            consumers: HashMap::new(),
        })
    }

    /// Returns the database directory.
    pub fn db_dir(&self) -> &PathBuf {
        &self.db_dir
    }

    /// Returns the number of registered projections.
    pub fn len(&self) -> usize {
        self.consumers.len()
    }

    /// Returns true if no projections are registered.
    pub fn is_empty(&self) -> bool {
        self.consumers.is_empty()
    }

    /// Checks if a projection is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.consumers.contains_key(name)
    }

    /// Returns a list of registered projection names.
    pub fn projection_names(&self) -> Vec<&str> {
        self.consumers.keys().map(|s| s.as_str()).collect()
    }

    // =========================================================================
    // Registration
    // =========================================================================

    /// Registers a new projection.
    ///
    /// Creates the database file and consumer but doesn't start processing.
    pub fn register(
        &mut self,
        name: &str,
        schema: ProjectionSchema,
    ) -> Result<(), ProjectionError> {
        if self.consumers.contains_key(name) {
            return Err(ProjectionError::AlreadyExists(name.to_string()));
        }

        let config =
            ProjectionConsumerConfig::new(name, self.db_dir.clone(), schema);

        let consumer = ProjectionConsumer::new(config, Arc::clone(&self.event_store))?;

        self.consumers.insert(name.to_string(), consumer);

        Ok(())
    }

    /// Unregisters a projection.
    ///
    /// Stops the consumer if running and removes it from the registry.
    /// Does NOT delete the database file.
    pub async fn unregister(&mut self, name: &str) -> Result<(), ProjectionError> {
        let mut consumer = self
            .consumers
            .remove(name)
            .ok_or_else(|| ProjectionError::NotFound(name.to_string()))?;

        // Stop if running
        if consumer.is_running() {
            consumer.stop().await;
        }

        Ok(())
    }

    // =========================================================================
    // Consumer Lifecycle (Rust-native mode)
    // =========================================================================

    /// Starts a specific projection consumer with a Rust apply function.
    pub async fn start<F>(&mut self, name: &str, apply_fn: F) -> Result<(), ProjectionError>
    where
        F: Fn(&[spitedb::Event]) -> Vec<ProjectionOp> + Send + Sync + 'static,
    {
        let consumer = self
            .consumers
            .get_mut(name)
            .ok_or_else(|| ProjectionError::NotFound(name.to_string()))?;

        consumer.start(apply_fn).await
    }

    /// Stops a specific projection consumer.
    pub async fn stop(&mut self, name: &str) -> Result<(), ProjectionError> {
        let consumer = self
            .consumers
            .get_mut(name)
            .ok_or_else(|| ProjectionError::NotFound(name.to_string()))?;

        consumer.stop().await;
        Ok(())
    }

    /// Stops all consumers.
    pub async fn stop_all(&mut self) {
        for consumer in self.consumers.values_mut() {
            consumer.stop().await;
        }
    }

    /// Returns whether a consumer is running.
    pub fn is_running(&self, name: &str) -> Option<bool> {
        self.consumers.get(name).map(|c| c.is_running())
    }

    // =========================================================================
    // JS-Driven Mode Operations
    // =========================================================================

    /// Gets the checkpoint for a projection.
    pub async fn get_checkpoint(&self, name: &str) -> Result<Option<i64>, ProjectionError> {
        let consumer = self
            .consumers
            .get(name)
            .ok_or_else(|| ProjectionError::NotFound(name.to_string()))?;

        consumer.get_checkpoint().await
    }

    /// Reads a row from a projection by primary key.
    pub async fn read_row(
        &self,
        name: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>, ProjectionError> {
        let consumer = self
            .consumers
            .get(name)
            .ok_or_else(|| ProjectionError::NotFound(name.to_string()))?;

        consumer.read_row(key).await
    }

    /// Gets events for JS processing.
    pub async fn get_events(
        &self,
        name: &str,
        batch_size: usize,
    ) -> Result<Option<(Vec<spitedb::Event>, i64)>, ProjectionError> {
        let consumer = self
            .consumers
            .get(name)
            .ok_or_else(|| ProjectionError::NotFound(name.to_string()))?;

        consumer.get_events(batch_size).await
    }

    /// Applies a batch of operations for JS-driven mode.
    pub async fn apply_batch(
        &self,
        name: &str,
        operations: Vec<ProjectionOp>,
        checkpoint: i64,
    ) -> Result<(), ProjectionError> {
        let consumer = self
            .consumers
            .get(name)
            .ok_or_else(|| ProjectionError::NotFound(name.to_string()))?;

        consumer.apply_batch(operations, checkpoint).await
    }

    /// Gets a projection instance for direct operations.
    ///
    /// Returns None if the projection doesn't exist.
    pub fn get_instance(&self, name: &str) -> Option<Arc<Mutex<ProjectionInstance>>> {
        self.consumers.get(name).map(|c| c.instance())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::operation::OpType;
    use crate::projection::schema::{ColumnDef, ColumnType};
    use tempfile::TempDir;

    fn create_test_schema(name: &str) -> ProjectionSchema {
        ProjectionSchema {
            table_name: name.to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::Text,
                    primary_key: true,
                    nullable: false,
                    default_value: None,
                },
                ColumnDef {
                    name: "value".to_string(),
                    col_type: ColumnType::Integer,
                    primary_key: false,
                    nullable: false,
                    default_value: None,
                },
            ],
        }
    }

    #[tokio::test]
    async fn test_registry_lifecycle() {
        let temp_dir = TempDir::new().unwrap();
        let event_store = Arc::new(SpiteDB::open_in_memory().await.unwrap());

        let mut registry =
            ProjectionRegistry::new(temp_dir.path().to_path_buf(), event_store).unwrap();

        // Register projections
        registry
            .register("proj_a", create_test_schema("proj_a"))
            .unwrap();
        registry
            .register("proj_b", create_test_schema("proj_b"))
            .unwrap();

        assert_eq!(registry.len(), 2);
        assert!(registry.contains("proj_a"));
        assert!(registry.contains("proj_b"));

        // Check database files created
        assert!(temp_dir.path().join("proj_a.db").exists());
        assert!(temp_dir.path().join("proj_b.db").exists());

        // Unregister
        registry.unregister("proj_a").await.unwrap();
        assert_eq!(registry.len(), 1);
        assert!(!registry.contains("proj_a"));

        // Database file still exists (we don't delete it)
        assert!(temp_dir.path().join("proj_a.db").exists());
    }

    #[tokio::test]
    async fn test_registry_js_driven_mode() {
        let temp_dir = TempDir::new().unwrap();
        let event_store = Arc::new(SpiteDB::open_in_memory().await.unwrap());

        let mut registry =
            ProjectionRegistry::new(temp_dir.path().to_path_buf(), event_store).unwrap();

        registry
            .register("test", create_test_schema("test"))
            .unwrap();

        // Apply batch
        let ops = vec![ProjectionOp {
            op_type: OpType::Upsert,
            key: "key1".to_string(),
            value: Some(serde_json::json!({"value": 123})),
        }];
        registry.apply_batch("test", ops, 50).await.unwrap();

        // Check checkpoint
        let checkpoint = registry.get_checkpoint("test").await.unwrap();
        assert_eq!(checkpoint, Some(50));

        // Read row
        let row = registry.read_row("test", "key1").await.unwrap();
        assert!(row.is_some());
        assert_eq!(row.unwrap()["value"], 123);
    }

    #[tokio::test]
    async fn test_registry_multiple_projections_independent() {
        let temp_dir = TempDir::new().unwrap();
        let event_store = Arc::new(SpiteDB::open_in_memory().await.unwrap());

        let mut registry =
            ProjectionRegistry::new(temp_dir.path().to_path_buf(), event_store).unwrap();

        registry
            .register("proj_a", create_test_schema("proj_a"))
            .unwrap();
        registry
            .register("proj_b", create_test_schema("proj_b"))
            .unwrap();

        // Apply to proj_a at checkpoint 100
        let ops_a = vec![ProjectionOp {
            op_type: OpType::Upsert,
            key: "key_a".to_string(),
            value: Some(serde_json::json!({"value": 1})),
        }];
        registry.apply_batch("proj_a", ops_a, 100).await.unwrap();

        // Apply to proj_b at checkpoint 50 (different from proj_a)
        let ops_b = vec![ProjectionOp {
            op_type: OpType::Upsert,
            key: "key_b".to_string(),
            value: Some(serde_json::json!({"value": 2})),
        }];
        registry.apply_batch("proj_b", ops_b, 50).await.unwrap();

        // Checkpoints are independent
        assert_eq!(registry.get_checkpoint("proj_a").await.unwrap(), Some(100));
        assert_eq!(registry.get_checkpoint("proj_b").await.unwrap(), Some(50));

        // Data is independent
        let row_a = registry.read_row("proj_a", "key_a").await.unwrap();
        let row_b = registry.read_row("proj_b", "key_b").await.unwrap();
        assert!(row_a.is_some());
        assert!(row_b.is_some());
        assert!(registry.read_row("proj_a", "key_b").await.unwrap().is_none());
        assert!(registry.read_row("proj_b", "key_a").await.unwrap().is_none());
    }
}
