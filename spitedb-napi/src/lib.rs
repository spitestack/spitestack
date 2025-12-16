//! # SpiteDB NAPI Bindings
//!
//! This crate provides Node.js/Bun bindings for SpiteDB, enabling JavaScript
//! applications to use the event store with native performance.

use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio::sync::Mutex;

use spitedb::{
    AppendCommand, AppendResult, CommandId, Event, EventData, GlobalPos, SpiteDB, StreamId,
    StreamRev, Tenant,
};

mod projection;

pub use projection::{
    BatchResult, ColumnDef, ColumnType, OpType, ProjectionError, ProjectionOp,
    ProjectionRegistry, ProjectionSchema,
};

// =============================================================================
// SpiteDB NAPI Wrapper
// =============================================================================

/// NAPI wrapper for SpiteDB.
#[napi]
pub struct SpiteDBNapi {
    inner: Arc<SpiteDB>,
    projection_registry: Arc<Mutex<Option<ProjectionRegistry>>>,
}

#[napi]
impl SpiteDBNapi {
    /// Opens a SpiteDB database at the given path.
    #[napi(factory)]
    pub async fn open(path: String) -> Result<Self> {
        let db = SpiteDB::open(&path)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to open database: {}", e)))?;

        Ok(Self {
            inner: Arc::new(db),
            projection_registry: Arc::new(Mutex::new(None)),
        })
    }

    /// Opens an in-memory SpiteDB database.
    #[napi(factory)]
    pub async fn open_in_memory() -> Result<Self> {
        let db = SpiteDB::open_in_memory()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to open database: {}", e)))?;

        Ok(Self {
            inner: Arc::new(db),
            projection_registry: Arc::new(Mutex::new(None)),
        })
    }

    /// Appends events to a stream.
    ///
    /// @param streamId - The stream to append to
    /// @param commandId - Unique command ID for idempotency
    /// @param expectedRev - Expected revision: -1 for "any", 0 for "stream must not exist", >0 for exact revision
    /// @param events - Array of event data buffers
    /// @param tenant - Optional tenant ID (defaults to "default" for single-tenant apps)
    #[napi]
    pub async fn append(
        &self,
        stream_id: String,
        command_id: String,
        expected_rev: i64,
        events: Vec<Buffer>,
        tenant: Option<String>,
    ) -> Result<AppendResultNapi> {
        // Convert expected_rev:
        // -1 = any revision is ok
        // 0 = stream must not exist (StreamRev::NONE)
        // >0 = exact revision
        let expected = if expected_rev < 0 {
            // For "any", we use a very high revision that will always pass
            // Actually, we need to handle this in the core - for now use NONE as workaround
            // TODO: Add proper "any" support to core API
            StreamRev::NONE
        } else {
            StreamRev::from_raw(expected_rev as u64)
        };

        let event_data: Vec<EventData> = events
            .into_iter()
            .map(|buf| EventData::new(buf.to_vec()))
            .collect();

        // Use provided tenant or default to "default"
        let tenant_obj = tenant.map(Tenant::new).unwrap_or_else(Tenant::default_tenant);

        let command = AppendCommand::new_with_tenant(
            CommandId::new(command_id),
            StreamId::new(stream_id),
            tenant_obj,
            expected,
            event_data,
        );

        let result = self
            .inner
            .append(command)
            .await
            .map_err(|e| Error::from_reason(format!("Append failed: {}", e)))?;

        Ok(AppendResultNapi::from(result))
    }

    /// Reads events from a stream.
    #[napi]
    pub async fn read_stream(
        &self,
        stream_id: String,
        from_rev: i64,
        limit: i64,
    ) -> Result<Vec<EventNapi>> {
        let events = self
            .inner
            .read_stream(stream_id, StreamRev::from_raw(from_rev as u64), limit as usize)
            .await
            .map_err(|e| Error::from_reason(format!("Read failed: {}", e)))?;

        Ok(events.into_iter().map(EventNapi::from).collect())
    }

    /// Reads events from the global log.
    #[napi]
    pub async fn read_global(&self, from_pos: i64, limit: i64) -> Result<Vec<EventNapi>> {
        let events = self
            .inner
            .read_global(GlobalPos::from_raw(from_pos as u64), limit as usize)
            .await
            .map_err(|e| Error::from_reason(format!("Read failed: {}", e)))?;

        Ok(events.into_iter().map(EventNapi::from).collect())
    }

    /// Gets the current revision of a stream.
    #[napi]
    pub async fn get_stream_revision(&self, stream_id: String) -> Result<i64> {
        let rev = self
            .inner
            .get_stream_revision(stream_id)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to get revision: {}", e)))?;

        Ok(rev.as_raw() as i64)
    }

    /// Initializes the projection registry.
    ///
    /// @param projectionsDir - Directory where projection databases will be stored.
    ///                         Each projection will have its own .db file in this directory.
    #[napi]
    pub async fn init_projections(&self, projections_dir: String) -> Result<()> {
        let registry =
            ProjectionRegistry::new(std::path::PathBuf::from(projections_dir), self.inner.clone())
                .map_err(|e| Error::from_reason(format!("Failed to init projections: {}", e)))?;

        let mut guard = self.projection_registry.lock().await;
        *guard = Some(registry);

        Ok(())
    }

    /// Registers a projection with the given schema.
    ///
    /// Creates the projection's database file at `{projectionsDir}/{name}.db`.
    #[napi]
    pub async fn register_projection(
        &self,
        name: String,
        schema: Vec<ColumnDefNapi>,
    ) -> Result<()> {
        let mut guard = self.projection_registry.lock().await;
        let registry = guard.as_mut().ok_or_else(|| {
            Error::from_reason("Projections not initialized. Call initProjections() first.")
        })?;

        let columns: Vec<ColumnDef> = schema.into_iter().map(ColumnDef::from).collect();
        let proj_schema = ProjectionSchema {
            table_name: name.clone(),
            columns,
        };

        registry
            .register(&name, proj_schema)
            .map_err(|e| Error::from_reason(format!("Failed to register projection: {}", e)))?;

        Ok(())
    }

    /// Reads a row from a projection table by primary key (synchronous for proxy support).
    ///
    /// This method is synchronous because the magic proxy syntax (`table[key]`) requires
    /// synchronous property access. The read uses blocking_lock internally.
    #[napi]
    pub fn read_projection_row(
        &self,
        projection_name: String,
        key: String,
    ) -> Result<Option<String>> {
        let guard = self.projection_registry.blocking_lock();
        let registry = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Projections not initialized"))?;

        // Get the consumer and read synchronously
        let instance = registry
            .get_instance(&projection_name)
            .ok_or_else(|| Error::from_reason(format!("Projection '{}' not found", projection_name)))?;

        let inst_guard = instance.blocking_lock();
        let result = inst_guard
            .read_row(&key)
            .map_err(|e| Error::from_reason(format!("Read failed: {}", e)))?;

        // Return as JSON string
        Ok(result.map(|v| serde_json::to_string(&v).unwrap_or_default()))
    }

    /// Applies a batch of operations to a projection and updates the checkpoint.
    #[napi]
    pub async fn apply_projection_batch(&self, batch: BatchResultNapi) -> Result<()> {
        let guard = self.projection_registry.lock().await;
        let registry = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Projections not initialized"))?;

        let result = BatchResult::from(batch);

        registry
            .apply_batch(
                &result.projection_name,
                result.operations,
                result.last_global_pos,
            )
            .await
            .map_err(|e| Error::from_reason(format!("Apply batch failed: {}", e)))?;

        Ok(())
    }

    /// Gets the next batch of events for a projection.
    #[napi]
    pub async fn get_projection_events(
        &self,
        projection_name: String,
        batch_size: i64,
    ) -> Result<Option<EventBatchNapi>> {
        let guard = self.projection_registry.lock().await;
        let registry = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Projections not initialized"))?;

        let result = registry
            .get_events(&projection_name, batch_size as usize)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to get events: {}", e)))?;

        match result {
            Some((events, batch_id)) => Ok(Some(EventBatchNapi {
                projection_name,
                events: events.into_iter().map(EventNapi::from).collect(),
                batch_id,
            })),
            None => Ok(None),
        }
    }

    /// Gets the current checkpoint for a projection.
    #[napi]
    pub async fn get_projection_checkpoint(&self, projection_name: String) -> Result<Option<i64>> {
        let guard = self.projection_registry.lock().await;
        let registry = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Projections not initialized"))?;

        registry
            .get_checkpoint(&projection_name)
            .await
            .map_err(|e| Error::from_reason(format!("Failed to get checkpoint: {}", e)))
    }
}

// =============================================================================
// NAPI Types
// =============================================================================

/// Result of an append operation.
#[napi(object)]
pub struct AppendResultNapi {
    /// First global position assigned
    pub first_pos: i64,
    /// Last global position assigned
    pub last_pos: i64,
    /// First stream revision assigned
    pub first_rev: i64,
    /// Last stream revision assigned
    pub last_rev: i64,
}

impl From<AppendResult> for AppendResultNapi {
    fn from(result: AppendResult) -> Self {
        Self {
            first_pos: result.first_pos.as_raw() as i64,
            last_pos: result.last_pos.as_raw() as i64,
            first_rev: result.first_rev.as_raw() as i64,
            last_rev: result.last_rev.as_raw() as i64,
        }
    }
}

/// An event read from the store.
#[napi(object)]
pub struct EventNapi {
    /// Global position in the log
    pub global_pos: i64,
    /// Stream this event belongs to
    pub stream_id: String,
    /// Revision within the stream
    pub stream_rev: i64,
    /// Timestamp when stored (Unix milliseconds)
    pub timestamp_ms: i64,
    /// Event payload
    pub data: Buffer,
}

impl From<Event> for EventNapi {
    fn from(event: Event) -> Self {
        Self {
            global_pos: event.global_pos.as_raw() as i64,
            stream_id: event.stream_id.to_string(),
            stream_rev: event.stream_rev.as_raw() as i64,
            timestamp_ms: event.timestamp_ms as i64,
            data: Buffer::from(event.data),
        }
    }
}

/// A batch of events for projection processing.
#[napi(object)]
pub struct EventBatchNapi {
    /// Name of the projection this batch is for
    pub projection_name: String,
    /// Events in the batch
    pub events: Vec<EventNapi>,
    /// Batch ID for acknowledgment
    pub batch_id: i64,
}

/// Column definition for a projection schema.
#[napi(object)]
pub struct ColumnDefNapi {
    /// Column name
    pub name: String,
    /// Column type: "text", "integer", "real", "blob", "boolean"
    pub col_type: String,
    /// Whether this column is part of the primary key
    pub primary_key: bool,
    /// Whether this column allows NULL values
    pub nullable: bool,
    /// Default value (as JSON string)
    pub default_value: Option<String>,
}

/// A single projection operation.
#[napi(object)]
pub struct ProjectionOpNapi {
    /// Operation type: "upsert" or "delete"
    pub op_type: String,
    /// Primary key value
    pub key: String,
    /// Row values for upsert (JSON string)
    pub value: Option<String>,
}

/// Result of processing a batch - operations to apply.
#[napi(object)]
pub struct BatchResultNapi {
    /// Name of the projection
    pub projection_name: String,
    /// Operations to apply
    pub operations: Vec<ProjectionOpNapi>,
    /// Last global position processed (for checkpoint)
    pub last_global_pos: i64,
}
