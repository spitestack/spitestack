//! Projection consumer - async event processing for a single projection.
//!
//! The consumer manages event consumption for one projection. It can operate in two modes:
//!
//! 1. **JS-driven mode**: JavaScript polls for events, processes them, and sends operations back.
//!    This is simpler and matches the existing pattern.
//!
//! 2. **Rust-native mode**: A Rust async task consumes events and applies them via a callback.
//!    This provides true push semantics with better latency.
//!
//! Both modes use the same underlying `ProjectionInstance` for SQLite operations.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

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
/// - An optional async task for Rust-native consumption
/// - Methods for JS-driven consumption
pub struct ProjectionConsumer {
    /// The projection instance (owns the SQLite database).
    instance: Arc<Mutex<ProjectionInstance>>,

    /// Configuration.
    config: ProjectionConsumerConfig,

    /// Reference to the event store (for reading events and subscribing).
    event_store: Arc<SpiteDB>,

    /// Whether the Rust consumer task is running.
    running: Arc<AtomicBool>,

    /// Handle to the consumer task (if started in Rust-native mode).
    task_handle: Option<JoinHandle<()>>,
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
            running: Arc::new(AtomicBool::new(false)),
            task_handle: None,
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

    // =========================================================================
    // Rust-Native Mode
    // =========================================================================

    /// Returns whether the Rust consumer task is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Starts the Rust-native consumer task.
    ///
    /// The consumer will:
    /// 1. Load the checkpoint from the projection database
    /// 2. Create a catch-up subscription from that position
    /// 3. Process events using the provided apply function
    /// 4. Write results to the projection database via spawn_blocking
    ///
    /// The `apply_fn` receives events and returns operations to apply.
    pub async fn start<F>(&mut self, apply_fn: F) -> Result<(), ProjectionError>
    where
        F: Fn(&[spitedb::Event]) -> Vec<ProjectionOp> + Send + Sync + 'static,
    {
        if self.running.load(Ordering::SeqCst) {
            return Err(ProjectionError::AlreadyRunning(self.config.name.clone()));
        }

        // Get starting checkpoint
        let checkpoint = self.get_checkpoint().await?;
        let start_pos = checkpoint
            .map(|p| GlobalPos::from_raw((p + 1) as u64))
            .unwrap_or(GlobalPos::FIRST);

        // Create catch-up subscription
        let subscription = self
            .event_store
            .subscribe_from(start_pos)
            .await
            .map_err(|e| ProjectionError::Internal(format!("Subscribe error: {}", e)))?;

        // Spawn consumer task
        let instance = Arc::clone(&self.instance);
        let running = Arc::clone(&self.running);
        let batch_size = self.config.batch_size;
        let name = self.config.name.clone();

        running.store(true, Ordering::SeqCst);

        let handle = tokio::spawn(async move {
            run_consumer_loop(instance, subscription, apply_fn, running, batch_size, &name).await;
        });

        self.task_handle = Some(handle);

        Ok(())
    }

    /// Stops the Rust-native consumer task.
    pub async fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);

        if let Some(handle) = self.task_handle.take() {
            // The task should exit on its own when running becomes false
            // but we can abort if needed
            handle.abort();
            let _ = handle.await;
        }
    }
}

/// Internal consumer loop for Rust-native mode.
async fn run_consumer_loop<F>(
    instance: Arc<Mutex<ProjectionInstance>>,
    mut subscription: spitedb::subscription::CatchUpSubscription,
    apply_fn: F,
    running: Arc<AtomicBool>,
    batch_size: usize,
    name: &str,
) where
    F: Fn(&[spitedb::Event]) -> Vec<ProjectionOp> + Send + Sync + 'static,
{
    let mut batch: Vec<spitedb::Event> = Vec::with_capacity(batch_size);

    while running.load(Ordering::SeqCst) {
        match subscription.next().await {
            Some(Ok(event)) => {
                batch.push(event);

                // Process when batch is full
                if batch.len() >= batch_size {
                    if let Err(e) = process_batch(&instance, &batch, &apply_fn).await {
                        eprintln!("Projection {} batch error: {}", name, e);
                        // On error, we stop - could add retry logic here
                        break;
                    }
                    batch.clear();
                }
            }
            Some(Err(spitedb::error::Error::SubscriptionLagged(n))) => {
                eprintln!("Projection {} subscription lagged by {} events", name, n);
                // Could restart subscription from checkpoint here
                break;
            }
            Some(Err(e)) => {
                eprintln!("Projection {} subscription error: {}", name, e);
                break;
            }
            None => {
                // Subscription closed
                break;
            }
        }
    }

    // Process any remaining events
    if !batch.is_empty() && running.load(Ordering::SeqCst) {
        if let Err(e) = process_batch(&instance, &batch, &apply_fn).await {
            eprintln!("Projection {} final batch error: {}", name, e);
        }
    }

    running.store(false, Ordering::SeqCst);
}

/// Processes a batch of events through the apply function and writes to SQLite.
async fn process_batch<F>(
    instance: &Arc<Mutex<ProjectionInstance>>,
    events: &[spitedb::Event],
    apply_fn: &F,
) -> Result<(), ProjectionError>
where
    F: Fn(&[spitedb::Event]) -> Vec<ProjectionOp>,
{
    if events.is_empty() {
        return Ok(());
    }

    // Apply function to get operations
    let operations = apply_fn(events);

    // Get checkpoint from last event
    let checkpoint = events
        .last()
        .map(|e| e.global_pos.as_raw() as i64)
        .unwrap_or(0);

    // Write to SQLite via spawn_blocking
    let instance = Arc::clone(instance);
    tokio::task::spawn_blocking(move || {
        let mut guard = instance.blocking_lock();
        guard.apply_batch(operations, checkpoint)
    })
    .await
    .map_err(|e| ProjectionError::Internal(format!("Task join error: {}", e)))??;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::operation::OpType;
    use crate::projection::schema::{ColumnDef, ColumnType};
    use tempfile::TempDir;

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
        let event_store = Arc::new(SpiteDB::open_in_memory().await.unwrap());

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
