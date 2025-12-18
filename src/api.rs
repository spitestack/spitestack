//! # Async API for SpiteDB
//!
//! This module provides the public async interface for SpiteDB. It wraps the
//! synchronous storage layer with Tokio's async primitives, enabling non-blocking
//! usage from async applications.
//!
//! ## Why Async?
//!
//! Modern applications often need to handle many concurrent connections efficiently.
//! Async/await allows thousands of concurrent operations without thousands of threads:
//!
//! ```text
//! Sync (threads):          Async (tasks):
//! ┌──────┐ ┌──────┐        ┌──────────────────┐
//! │Thread│ │Thread│  ...   │  Single Thread   │
//! │  1   │ │  2   │        │  Many Tasks      │
//! │ 😴  │ │ 😴  │        │  ⚡ ⚡ ⚡ ⚡   │
//! └──────┘ └──────┘        └──────────────────┘
//!   Waiting = blocked       Waiting = yielded
//!   (wastes resources)      (runs other tasks)
//! ```
//!
//! ## The SQLite Challenge
//!
//! SQLite's `Connection` type uses `RefCell` internally, making it `!Sync`.
//! This means we can't share a connection across threads, even with `Arc`.
//!
//! Our solution: **Dedicated threads with async channels**
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Tokio Runtime                             │
//! │                                                             │
//! │  ┌─────────────────────────────────────────────────────┐   │
//! │  │              Async Tasks (clients)                   │   │
//! │  │  task1 ─┐                               ┌─ task4     │   │
//! │  │  task2 ─┼─ tokio::sync::mpsc ──────────┼─ task5     │   │
//! │  │  task3 ─┘    (async send)              └─ task6     │   │
//! │  └─────────────────────┬───────────────────────────────┘   │
//! │                        │                                    │
//! └────────────────────────┼────────────────────────────────────┘
//!                          │
//!                          ▼
//!              ┌───────────────────────┐
//!              │  Dedicated OS Thread  │  ← std::thread::spawn
//!              │                       │
//!              │  ┌─────────────────┐  │
//!              │  │     Storage     │  │  ← Not shared, owned by thread
//!              │  │   (Connection)  │  │
//!              │  └─────────────────┘  │
//!              └───────────────────────┘
//! ```
//!
//! ## Read Path Architecture
//!
//! For file-based databases, the reader uses a **read-only connection** with direct
//! SQL queries (not the Storage abstraction). This ensures:
//! - Reader always sees the latest committed data from the writer
//! - No stale in-memory cache issues
//! - SQLite's WAL mode handles isolation correctly
//!
//! For in-memory databases, we use a shared Storage instance for simplicity
//! (primarily used in testing).
//!
//! ## Rust Concepts
//!
//! - **async/await**: Cooperative multitasking without callbacks
//! - **tokio::sync::mpsc**: Async channel (can await send/recv)
//! - **tokio::sync::oneshot**: Single-use channel for request/response
//! - **!Sync types**: Types that can't be shared between threads via &T
//! - **Dedicated threads**: When async won't work, use OS threads

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, available_parallelism, JoinHandle};

use rusqlite::{Connection, OpenFlags};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::crypto::BatchCryptor;
use crate::error::{Error, Result};
use crate::reader::{self, ReadRequest};
use crate::schema::Database;
use crate::types::{
    AppendCommand, AppendResult, CommandId, DeleteStreamCommand, DeleteStreamResult,
    DeleteTenantCommand, DeleteTenantResult, Event, GlobalPos, StreamId, StreamRev, Tenant,
};
use crate::writer::{BatchWriterHandle, TransactionBuilder, WriterConfig, spawn_batch_writer};

// =============================================================================
// Configuration
// =============================================================================

/// Size of the read request channel.
const READ_CHANNEL_SIZE: usize = 4096;

/// Minimum number of reader threads.
const MIN_READ_THREADS: usize = 1;

/// Maximum number of reader threads.
const MAX_READ_THREADS: usize = 16;

/// Background compaction interval (5 minutes).
const COMPACTION_INTERVAL_SECS: u64 = 5 * 60;

/// Maximum batches to process per compaction cycle.
/// Keeps compaction incremental to avoid blocking writes for too long.
const COMPACTION_MAX_BATCHES_PER_CYCLE: usize = 100;

// =============================================================================
// SpiteDB - The Main Async Handle
// =============================================================================

/// The main async handle for SpiteDB operations.
///
/// # Thread Safety
///
/// `SpiteDB` is `Clone`, `Send`, and `Sync`. You can share it across tasks
/// and threads freely. All clones share the same underlying storage.
///
/// # Architecture
///
/// Internally, SpiteDB maintains:
/// - One dedicated OS thread for writes (single-writer invariant)
/// - A pool of reader threads (based on CPU count, 1-16 threads)
/// - Tokio channels for async communication
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                            Async Tasks                                   │
/// └────────────────────────────────┬────────────────────────────────────────┘
///                    ┌─────────────┴─────────────┐
///                    │                           │
///                    ▼                           ▼
///            ┌───────────────┐         ┌─────────────────────┐
///            │  WriteActor   │         │     ReadPool        │
///            │  (1 thread)   │         │   (N threads)       │
///            │               │         │                     │
///            │  ┌─────────┐  │         │  ┌───┐ ┌───┐ ┌───┐ │
///            │  │ Storage │  │         │  │ R │ │ R │ │ R │ │
///            │  │  (R/W)  │  │         │  └───┘ └───┘ └───┘ │
///            │  └─────────┘  │         │  Read-only conns    │
///            └───────┬───────┘         └──────────┬──────────┘
///                    │                            │
///                    └─────────────┬──────────────┘
///                                  ▼
///                          ┌─────────────┐
///                          │   SQLite    │
///                          │   (WAL)     │
///                          └─────────────┘
/// ```
///
/// # Example
///
/// ```rust,ignore
/// use spitedb::{SpiteDB, AppendCommand, EventData, StreamRev};
///
/// #[tokio::main]
/// async fn main() -> spitedb::Result<()> {
///     let db = SpiteDB::open("my_events.db").await?;
///
///     // Append events
///     let cmd = AppendCommand::new(
///         "cmd-123",
///         "user-456",
///         StreamRev::NONE,
///         vec![EventData::new(b"user created".to_vec())],
///     );
///     let result = db.append(cmd).await?;
///     println!("Wrote at position {}", result.first_pos);
///
///     // Read events
///     let events = db.read_stream("user-456", StreamRev::FIRST, 100).await?;
///     for event in events {
///         println!("Event: {:?}", event.data);
///     }
///
///     db.shutdown().await;
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct SpiteDB {
    /// Batch writer handle for write operations with group commit.
    writer: BatchWriterHandle,

    /// Channel to send read requests.
    read_tx: mpsc::Sender<ReadRequest>,

    /// Handles to the reader threads (for shutdown).
    reader_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,

    /// Number of reader threads in the pool.
    reader_count: usize,
}

impl SpiteDB {
    /// Opens or creates a SpiteDB database at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the SQLite database file. Created if it doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let db = SpiteDB::open("events.db").await?;
    /// ```
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let cryptor = BatchCryptor::from_env()?;
        let path = path.as_ref().to_path_buf();
        Self::open_internal(path, cryptor).await
    }

    /// Opens a file-based database with a custom cryptor.
    ///
    /// # Use Case
    ///
    /// For dependency injection or testing with custom key providers.
    pub async fn open_with_cryptor<P: AsRef<Path>>(path: P, cryptor: BatchCryptor) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        Self::open_internal(path, cryptor).await
    }

    /// Internal implementation for file-based databases.
    async fn open_internal(path: PathBuf, cryptor: BatchCryptor) -> Result<Self> {
        // Clone the cryptor for readers (they need their own instance)
        let reader_cryptor = Arc::new(cryptor.clone_with_same_key());

        // Create read channel
        let (read_tx, read_rx) = mpsc::channel(READ_CHANNEL_SIZE);

        // Initialize the database and spawn the batch writer
        let db = Database::open(&path)?;

        // Spawn batch writer with default config (10ms batch timeout)
        let writer = spawn_batch_writer(db.into_connection(), cryptor, WriterConfig::default())?;

        // Determine reader thread count based on available CPUs
        let reader_count = available_parallelism()
            .map(|n| n.get())
            .unwrap_or(MIN_READ_THREADS)
            .clamp(MIN_READ_THREADS, MAX_READ_THREADS);

        // Wrap the receiver in Arc<std::sync::Mutex> for sharing across threads
        // Threads compete to receive from the channel (simple load balancing)
        let read_rx = Arc::new(std::sync::Mutex::new(read_rx));

        // Spawn reader thread pool
        // Each thread opens its own read-only connection.
        // This ensures readers always see the latest committed data via WAL mode.
        let mut reader_handles = Vec::with_capacity(reader_count);

        for i in 0..reader_count {
            let rx = Arc::clone(&read_rx);
            let reader_path = path.clone();
            let cryptor = Arc::clone(&reader_cryptor);

            let handle = thread::Builder::new()
                .name(format!("spitedb-async-reader-{}", i))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to create reader runtime");

                    rt.block_on(async {
                        // Each thread opens its own read-only connection
                        let conn = Connection::open_with_flags(
                            &reader_path,
                            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                        )
                        .expect("failed to open read-only connection");
                        reader::run_reader_pooled(conn, cryptor, rx).await;
                    });
                })
                .map_err(|e| Error::Schema(format!("failed to spawn reader thread: {}", e)))?;

            reader_handles.push(handle);
        }

        let db = Self {
            writer,
            read_tx,
            reader_handles: Arc::new(Mutex::new(reader_handles)),
            reader_count,
        };

        // Spawn background compaction task
        db.spawn_compaction_task();

        Ok(db)
    }

    /// Spawns the background compaction task.
    ///
    /// This task runs periodically (every 5 minutes) to physically delete
    /// events that have been logically deleted via tombstones.
    fn spawn_compaction_task(&self) {
        let writer = self.writer.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                std::time::Duration::from_secs(COMPACTION_INTERVAL_SECS)
            );

            // Skip the first immediate tick
            interval.tick().await;

            loop {
                interval.tick().await;

                match writer.compact(Some(COMPACTION_MAX_BATCHES_PER_CYCLE)).await {
                    Ok(stats) => {
                        if stats.batches_rewritten > 0 || stats.tombstones_removed > 0 {
                            eprintln!(
                                "[spitedb] background compaction: {} batches rewritten, {} events deleted, {} tombstones removed, {} bytes reclaimed",
                                stats.batches_rewritten,
                                stats.events_deleted,
                                stats.tombstones_removed,
                                stats.bytes_reclaimed,
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("[spitedb] background compaction failed: {}", e);
                    }
                }
            }
        });
    }

    /// Returns the number of reader threads in the pool.
    pub fn reader_count(&self) -> usize {
        self.reader_count
    }

    /// Appends events to a stream.
    ///
    /// # Group Commit
    ///
    /// Writes are batched with other concurrent writes for efficiency. The batch
    /// timeout (default 10ms) ensures low latency while maximizing throughput.
    ///
    /// # Exactly-Once Semantics
    ///
    /// If `command.command_id` was already processed, returns the cached result
    /// without creating duplicate events. Safe to retry on timeout.
    ///
    /// # Errors
    ///
    /// - `Error::Conflict` if `expected_rev` doesn't match current revision
    /// - `Error::Schema` if the database connection is closed
    pub async fn append(&self, command: AppendCommand) -> Result<AppendResult> {
        self.writer.append(command).await
    }

    /// Begins a transaction for guaranteed same-batch writes.
    ///
    /// # Use Case
    ///
    /// When you need multiple append operations to be written atomically in the
    /// same SQLite transaction. This is useful for:
    ///
    /// - Cross-stream operations that must succeed or fail together
    /// - Ensuring related events have contiguous global positions
    /// - Reducing latency by bundling multiple writes
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut tx = db.begin_transaction();
    /// tx.append(AppendCommand::new("cmd-1", "stream-a", StreamRev::NONE, events1));
    /// tx.append(AppendCommand::new("cmd-2", "stream-b", StreamRev::NONE, events2));
    /// let results = tx.submit().await?;
    /// // Both writes are in the same batch - contiguous positions guaranteed
    /// ```
    ///
    /// # Per-Command Isolation
    ///
    /// Each command in the transaction runs in its own SAVEPOINT. If one command
    /// fails (e.g., conflict), others can still succeed. The results vector
    /// contains a result for each command in submission order.
    pub fn begin_transaction(&self) -> TransactionBuilder {
        self.writer.begin_transaction()
    }

    /// Reads events from a specific stream.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The stream to read from (accepts &str, String, or StreamId)
    /// * `from_rev` - Starting revision (inclusive), use `StreamRev::FIRST` for beginning
    /// * `limit` - Maximum number of events to return
    ///
    /// # Returns
    ///
    /// Events in revision order. Empty vec if stream doesn't exist.
    pub async fn read_stream(
        &self,
        stream_id: impl Into<StreamId>,
        from_rev: StreamRev,
        limit: usize,
    ) -> Result<Vec<Event>> {
        self.read_stream_tenant(stream_id, Tenant::default_tenant(), from_rev, limit).await
    }

    /// Reads events from a specific stream within a specific tenant.
    pub async fn read_stream_tenant(
        &self,
        stream_id: impl Into<StreamId>,
        tenant: impl Into<Tenant>,
        from_rev: StreamRev,
        limit: usize,
    ) -> Result<Vec<Event>> {
        let (response_tx, response_rx) = oneshot::channel();

        let stream_id = stream_id.into();
        let tenant_hash = tenant.into().hash();

        self.read_tx
            .send(ReadRequest::ReadStream {
                stream_id,
                tenant_hash,
                from_rev,
                limit,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::Schema("reader thread has shut down".to_string()))?;

        response_rx
            .await
            .map_err(|_| Error::Schema("reader dropped response channel".to_string()))?
    }

    /// Reads events from the global log.
    ///
    /// # Arguments
    ///
    /// * `from_pos` - Starting global position (inclusive)
    /// * `limit` - Maximum number of events to return
    ///
    /// # Returns
    ///
    /// Events in global position order, across all streams.
    pub async fn read_global(&self, from_pos: GlobalPos, limit: usize) -> Result<Vec<Event>> {
        let (response_tx, response_rx) = oneshot::channel();

        self.read_tx
            .send(ReadRequest::ReadGlobal {
                from_pos,
                limit,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::Schema("reader thread has shut down".to_string()))?;

        response_rx
            .await
            .map_err(|_| Error::Schema("reader dropped response channel".to_string()))?
    }

    /// Gets the current revision of a stream.
    ///
    /// Returns `StreamRev::NONE` if the stream doesn't exist.
    pub async fn get_stream_revision(&self, stream_id: impl Into<StreamId>) -> Result<StreamRev> {
        self.get_stream_revision_tenant(stream_id, Tenant::default_tenant()).await
    }

    /// Gets the current revision of a stream within a specific tenant.
    pub async fn get_stream_revision_tenant(
        &self,
        stream_id: impl Into<StreamId>,
        tenant: impl Into<Tenant>,
    ) -> Result<StreamRev> {
        let (response_tx, response_rx) = oneshot::channel();

        let stream_id = stream_id.into();
        let tenant_hash = tenant.into().hash();

        self.read_tx
            .send(ReadRequest::GetStreamRevision {
                stream_id,
                tenant_hash,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::Schema("reader thread has shut down".to_string()))?;

        response_rx
            .await
            .map_err(|_| Error::Schema("reader dropped response channel".to_string()))
    }

    // =========================================================================
    // Deletion (GDPR Compliance)
    // =========================================================================

    /// Deletes all events from a stream.
    ///
    /// # GDPR Compliance
    ///
    /// Creates a tombstone record that immediately filters all events in the
    /// stream from reads. Physical deletion happens during offline compaction.
    ///
    /// # Arguments
    ///
    /// * `command_id` - Unique ID for idempotency (safe to retry)
    /// * `stream_id` - The stream to delete
    ///
    /// # Returns
    ///
    /// The tombstone information (revision range deleted, timestamp).
    ///
    /// # Errors
    ///
    /// Returns an error if the stream doesn't exist.
    pub async fn delete_stream(
        &self,
        command_id: impl Into<CommandId>,
        stream_id: impl Into<StreamId>,
        tenant: impl Into<Tenant>,
    ) -> Result<DeleteStreamResult> {
        let command = DeleteStreamCommand::delete_all(command_id, stream_id, tenant);
        self.writer.delete_stream(command).await
    }

    /// Deletes a range of events from a stream.
    ///
    /// # GDPR Compliance
    ///
    /// Creates a tombstone record for the specified revision range. Events in
    /// that range are immediately filtered from all reads.
    ///
    /// # Arguments
    ///
    /// * `command_id` - Unique ID for idempotency (safe to retry)
    /// * `stream_id` - The stream to delete from
    /// * `tenant` - The tenant that owns this stream
    /// * `from_rev` - First revision to delete (inclusive)
    /// * `to_rev` - Last revision to delete (inclusive)
    ///
    /// # Returns
    ///
    /// The tombstone information (revision range deleted, timestamp).
    ///
    /// # Errors
    ///
    /// Returns an error if the stream doesn't exist or the revision range
    /// is invalid.
    pub async fn delete_stream_range(
        &self,
        command_id: impl Into<CommandId>,
        stream_id: impl Into<StreamId>,
        tenant: impl Into<Tenant>,
        from_rev: StreamRev,
        to_rev: StreamRev,
    ) -> Result<DeleteStreamResult> {
        let command = DeleteStreamCommand::delete_range(command_id, stream_id, tenant, from_rev, to_rev);
        self.writer.delete_stream(command).await
    }

    /// Deletes all events for a tenant.
    ///
    /// # GDPR "Right to be Forgotten"
    ///
    /// This is the nuclear option for GDPR compliance. All events belonging
    /// to the specified tenant are immediately filtered from all reads.
    ///
    /// # Arguments
    ///
    /// * `command_id` - Unique ID for idempotency (safe to retry)
    /// * `tenant` - The tenant to delete
    ///
    /// # Returns
    ///
    /// The tombstone information (tenant hash, timestamp).
    ///
    /// # Warning
    ///
    /// This operation cannot be undone (without database restoration).
    /// Ensure you have proper authorization before calling this.
    pub async fn delete_tenant(
        &self,
        command_id: impl Into<CommandId>,
        tenant: impl Into<Tenant>,
    ) -> Result<DeleteTenantResult> {
        let command = DeleteTenantCommand::new(command_id, tenant);
        self.writer.delete_tenant(command).await
    }

    /// Gets current admission control metrics.
    ///
    /// # Observability
    ///
    /// Returns a snapshot of the adaptive admission control system's state:
    /// - `current_limit`: Current max in-flight events (auto-adjusted)
    /// - `observed_p99_ms`: Observed p99 latency in milliseconds
    /// - `target_p99_ms`: Target p99 latency (default 60ms)
    /// - `requests_accepted`: Total requests that completed successfully
    /// - `requests_rejected`: Total requests that timed out
    /// - `rejection_rate`: Ratio of rejected to total requests
    /// - `adjustments`: Number of times the controller adjusted the limit
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let metrics = db.admission_metrics();
    /// println!("Current limit: {}", metrics.current_limit);
    /// println!("Observed p99: {:.2}ms", metrics.observed_p99_ms);
    /// if metrics.rejection_rate > 0.05 {
    ///     println!("Warning: High rejection rate!");
    /// }
    /// ```
    pub fn admission_metrics(&self) -> crate::writer::MetricsSnapshot {
        self.writer.metrics()
    }

    /// Shuts down the database gracefully.
    ///
    /// # Graceful Shutdown
    ///
    /// 1. Sends shutdown signal to all reader threads
    /// 2. Waits for threads to complete (they'll finish current work first)
    /// 3. SQLite connections are closed when Storage is dropped
    ///
    /// Note: The batch writer thread will shut down when the last handle is dropped.
    ///
    /// # Important
    ///
    /// After shutdown, all operations will fail. Create a new SpiteDB instance
    /// if you need to reopen the database.
    pub async fn shutdown(self) {
        // Send shutdown signals to all reader threads
        for _ in 0..self.reader_count {
            let _ = self.read_tx.send(ReadRequest::Shutdown).await;
        }

        // Wait for all reader threads to complete
        let handles = std::mem::take(&mut *self.reader_handles.lock().await);
        for handle in handles {
            let _ = handle.join();
        }

        // Batch writer shuts down when the last handle is dropped
        // (which happens when self is dropped at end of this function)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::{BatchCryptor, EnvKeyProvider};
    use crate::types::EventData;

    /// Creates a test cryptor with a fixed key (no env var needed).
    fn test_cryptor() -> BatchCryptor {
        let key = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
        ];
        BatchCryptor::new(EnvKeyProvider::from_key(key))
    }

    /// Creates a test database in a temporary directory.
    /// Returns (db, temp_dir) - temp_dir must be kept alive for the db to work.
    async fn test_db() -> (SpiteDB, tempfile::TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = SpiteDB::open_with_cryptor(&db_path, test_cryptor()).await.unwrap();
        (db, temp_dir)
    }

    #[tokio::test]
    async fn test_open() {
        let (db, _temp_dir) = test_db().await;
        db.shutdown().await;
    }

    #[tokio::test]
    async fn test_append_single() {
        let (db, _temp_dir) = test_db().await;

        let cmd = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"hello async".to_vec())],
        );

        let result = db.append(cmd).await.unwrap();
        assert_eq!(result.first_pos, GlobalPos::FIRST);

        db.shutdown().await;
    }

    #[tokio::test]
    async fn test_append_multiple() {
        let (db, _temp_dir) = test_db().await;

        for i in 0..10 {
            let cmd = AppendCommand::new(
                format!("cmd-{}", i),
                format!("stream-{}", i),
                StreamRev::NONE,
                vec![EventData::new(format!("event {}", i).into_bytes())],
            );

            let result = db.append(cmd).await.unwrap();
            assert_eq!(result.first_pos.as_raw(), (i + 1) as u64);
        }

        db.shutdown().await;
    }

    #[tokio::test]
    async fn test_conflict_detection() {
        let (db, _temp_dir) = test_db().await;

        // First append
        let cmd1 = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"first".to_vec())],
        );
        db.append(cmd1).await.unwrap();

        // Conflicting append
        let cmd2 = AppendCommand::new(
            "cmd-2",
            "stream-1",
            StreamRev::NONE, // Wrong - should be rev 1
            vec![EventData::new(b"conflict".to_vec())],
        );

        let result = db.append(cmd2).await;
        assert!(matches!(result, Err(Error::Conflict { .. })));

        db.shutdown().await;
    }

    #[tokio::test]
    async fn test_idempotency() {
        let (db, _temp_dir) = test_db().await;

        let cmd = AppendCommand::new(
            "cmd-idem",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"hello".to_vec())],
        );

        let result1 = db.append(cmd.clone()).await.unwrap();
        let result2 = db.append(cmd).await.unwrap();

        // Same result for duplicate command
        assert_eq!(result1.first_pos.as_raw(), result2.first_pos.as_raw());

        db.shutdown().await;
    }

    #[tokio::test]
    async fn test_concurrent_appends() {
        let (db, _temp_dir) = test_db().await;

        // Spawn multiple tasks that append concurrently
        let mut handles = vec![];

        for i in 0..10 {
            let db = db.clone();
            let handle = tokio::spawn(async move {
                let cmd = AppendCommand::new(
                    format!("cmd-{}", i),
                    format!("stream-{}", i),
                    StreamRev::NONE,
                    vec![EventData::new(format!("event {}", i).into_bytes())],
                );
                db.append(cmd).await.unwrap()
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        db.shutdown().await;
    }

    #[tokio::test]
    async fn test_file_based_read_write() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        // Create database file first
        {
            let _db = Database::open(&db_path).unwrap();
        }

        let db = SpiteDB::open_with_cryptor(&db_path, test_cryptor()).await.unwrap();

        // Write
        let cmd = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![
                EventData::new(b"event1".to_vec()),
                EventData::new(b"event2".to_vec()),
            ],
        );
        let result = db.append(cmd).await.unwrap();
        assert_eq!(result.event_count(), 2);

        // Small delay for WAL sync between connections
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Read
        let events = db
            .read_stream("stream-1", StreamRev::FIRST, 100)
            .await
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data, b"event1");
        assert_eq!(events[1].data, b"event2");

        // Read global
        let global_events = db.read_global(GlobalPos::FIRST, 100).await.unwrap();
        assert_eq!(global_events.len(), 2);

        // Get revision
        let rev = db.get_stream_revision("stream-1").await.unwrap();
        assert_eq!(rev.as_raw(), 2);

        db.shutdown().await;
    }

    #[tokio::test]
    async fn test_clone_and_share() {
        let (db, _temp_dir) = test_db().await;

        // Clone for multiple tasks
        let db1 = db.clone();
        let db2 = db.clone();

        let h1 = tokio::spawn(async move {
            let cmd = AppendCommand::new(
                "cmd-from-task-1",
                "stream-1",
                StreamRev::NONE,
                vec![EventData::new(b"from task 1".to_vec())],
            );
            db1.append(cmd).await.unwrap()
        });

        let h2 = tokio::spawn(async move {
            let cmd = AppendCommand::new(
                "cmd-from-task-2",
                "stream-2",
                StreamRev::NONE,
                vec![EventData::new(b"from task 2".to_vec())],
            );
            db2.append(cmd).await.unwrap()
        });

        h1.await.unwrap();
        h2.await.unwrap();

        db.shutdown().await;
    }

    // =========================================================================
    // Tombstone / Deletion Tests
    // =========================================================================

    #[tokio::test]
    async fn test_delete_stream() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("delete_test.db");

        {
            let _db = Database::open(&db_path).unwrap();
        }

        let db = SpiteDB::open_with_cryptor(&db_path, test_cryptor()).await.unwrap();

        // Create a stream with events
        let cmd = AppendCommand::new(
            "cmd-1",
            "user-123",
            StreamRev::NONE,
            vec![
                EventData::new(b"event1".to_vec()),
                EventData::new(b"event2".to_vec()),
            ],
        );
        db.append(cmd).await.unwrap();

        // Verify events exist
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let events = db
            .read_stream("user-123", StreamRev::FIRST, 100)
            .await
            .unwrap();
        assert_eq!(events.len(), 2);

        // Delete the stream
        let result = db.delete_stream("delete-cmd-1", "user-123", "default").await.unwrap();
        assert_eq!(result.stream_id.as_str(), "user-123");

        // Verify events are filtered out
        let events_after = db
            .read_stream("user-123", StreamRev::FIRST, 100)
            .await
            .unwrap();
        assert!(events_after.is_empty(), "events should be filtered after delete");

        db.shutdown().await;
    }

    #[tokio::test]
    async fn test_delete_stream_range() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("delete_range_test.db");

        {
            let _db = Database::open(&db_path).unwrap();
        }

        let db = SpiteDB::open_with_cryptor(&db_path, test_cryptor()).await.unwrap();

        // Create a stream with events
        let cmd = AppendCommand::new(
            "cmd-1",
            "user-456",
            StreamRev::NONE,
            vec![
                EventData::new(b"event1".to_vec()),
                EventData::new(b"event2".to_vec()),
                EventData::new(b"event3".to_vec()),
                EventData::new(b"event4".to_vec()),
            ],
        );
        db.append(cmd).await.unwrap();

        // Wait for WAL sync
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Delete only revisions 2-3
        let result = db
            .delete_stream_range(
                "delete-cmd-2",
                "user-456",
                "default",
                StreamRev::from_raw(2),
                StreamRev::from_raw(3),
            )
            .await
            .unwrap();
        assert_eq!(result.from_rev.as_raw(), 2);
        assert_eq!(result.to_rev.as_raw(), 3);

        // Verify only revisions 1 and 4 remain
        let events_after = db
            .read_stream("user-456", StreamRev::FIRST, 100)
            .await
            .unwrap();
        assert_eq!(events_after.len(), 2);
        assert_eq!(events_after[0].stream_rev.as_raw(), 1);
        assert_eq!(events_after[1].stream_rev.as_raw(), 4);

        db.shutdown().await;
    }

    #[tokio::test]
    async fn test_delete_stream_nonexistent() {
        let (db, _temp_dir) = test_db().await;

        // Try to delete a non-existent stream
        let result = db.delete_stream("delete-cmd", "nonexistent", "default").await;
        assert!(result.is_err());

        db.shutdown().await;
    }

    #[tokio::test]
    async fn test_delete_tenant() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("delete_tenant_test.db");

        {
            let _db = Database::open(&db_path).unwrap();
        }

        let db = SpiteDB::open_with_cryptor(&db_path, test_cryptor()).await.unwrap();

        // Create streams for a tenant (using default tenant for simplicity)
        let cmd1 = AppendCommand::new(
            "cmd-1",
            "stream-a",
            StreamRev::NONE,
            vec![EventData::new(b"event1".to_vec())],
        );
        let cmd2 = AppendCommand::new(
            "cmd-2",
            "stream-b",
            StreamRev::NONE,
            vec![EventData::new(b"event2".to_vec())],
        );
        db.append(cmd1).await.unwrap();
        db.append(cmd2).await.unwrap();

        // Wait for WAL sync
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Verify events exist
        let events_before = db.read_global(GlobalPos::FIRST, 100).await.unwrap();
        assert_eq!(events_before.len(), 2);

        // Delete the default tenant
        let result = db.delete_tenant("tenant-delete-cmd", "default").await.unwrap();
        assert!(result.deleted_ms > 0);

        // Verify all events are filtered
        let events_after = db.read_global(GlobalPos::FIRST, 100).await.unwrap();
        assert!(events_after.is_empty(), "all tenant events should be filtered");

        db.shutdown().await;
    }
}
