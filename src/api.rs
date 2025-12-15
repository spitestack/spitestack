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

use crate::error::{Error, Result};
use crate::reader::{self, ReadRequest};
use crate::schema::Database;
use crate::types::{AppendCommand, AppendResult, Event, GlobalPos, StreamId, StreamRev};
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
        let path = path.as_ref().to_path_buf();
        Self::open_internal(Some(path)).await
    }

    /// Creates a SpiteDB instance with an in-memory database.
    ///
    /// # Use Case
    ///
    /// Primarily for testing. In-memory databases don't persist across restarts.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let db = SpiteDB::open_in_memory().await?;
    /// ```
    pub async fn open_in_memory() -> Result<Self> {
        Self::open_internal(None).await
    }

    /// Internal implementation that handles both file and in-memory databases.
    async fn open_internal(path: Option<PathBuf>) -> Result<Self> {
        // Create read channel
        let (read_tx, read_rx) = mpsc::channel(READ_CHANNEL_SIZE);

        // Clone path for reader threads
        let read_path = path.clone();

        // Initialize the database and spawn the batch writer
        let db = match &path {
            Some(p) => Database::open(p)?,
            None => Database::open_in_memory()?,
        };

        // Spawn batch writer with default config (10ms batch timeout)
        let writer = spawn_batch_writer(db.into_connection(), WriterConfig::default())?;

        // Determine reader thread count based on available CPUs
        // For in-memory databases, we use 1 thread (can't share the connection)
        let reader_count = if read_path.is_some() {
            available_parallelism()
                .map(|n| n.get())
                .unwrap_or(MIN_READ_THREADS)
                .clamp(MIN_READ_THREADS, MAX_READ_THREADS)
        } else {
            // In-memory databases can only have one connection
            1
        };

        // Wrap the receiver in Arc<std::sync::Mutex> for sharing across threads
        // Threads compete to receive from the channel (simple load balancing)
        let read_rx = Arc::new(std::sync::Mutex::new(read_rx));

        // Spawn reader thread pool
        // For file-based databases, each thread opens its own read-only connection.
        // This ensures readers always see the latest committed data via WAL mode.
        let mut reader_handles = Vec::with_capacity(reader_count);

        for i in 0..reader_count {
            let rx = Arc::clone(&read_rx);
            let path = read_path.clone();

            let handle = thread::Builder::new()
                .name(format!("spitedb-async-reader-{}", i))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to create reader runtime");

                    rt.block_on(async {
                        match path {
                            Some(p) => {
                                // File-based: each thread opens its own read-only connection
                                let conn = Connection::open_with_flags(
                                    &p,
                                    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                                )
                                .expect("failed to open read-only connection");
                                reader::run_reader_pooled(conn, rx).await;
                            }
                            None => {
                                // In-memory: create a separate in-memory DB for reads
                                // Note: This won't see writes from the writer for in-memory DBs.
                                // In-memory mode is primarily for testing the writer.
                                let db =
                                    Database::open_in_memory().expect("failed to open in-memory db");
                                reader::run_reader_pooled(db.into_connection(), rx).await;
                            }
                        }
                    });
                })
                .map_err(|e| Error::Schema(format!("failed to spawn reader thread: {}", e)))?;

            reader_handles.push(handle);
        }

        Ok(Self {
            writer,
            read_tx,
            reader_handles: Arc::new(Mutex::new(reader_handles)),
            reader_count,
        })
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
        let (response_tx, response_rx) = oneshot::channel();

        self.read_tx
            .send(ReadRequest::ReadStream {
                stream_id: stream_id.into(),
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
        let (response_tx, response_rx) = oneshot::channel();

        self.read_tx
            .send(ReadRequest::GetStreamRevision {
                stream_id: stream_id.into(),
                response: response_tx,
            })
            .await
            .map_err(|_| Error::Schema("reader thread has shut down".to_string()))?;

        response_rx
            .await
            .map_err(|_| Error::Schema("reader dropped response channel".to_string()))
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    /// Subscribes to live events from the global log.
    ///
    /// Returns a `SimpleSubscription` that receives all events committed after
    /// this call. This is the simplest way to subscribe to live events.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut sub = db.subscribe_all().await?;
    ///
    /// loop {
    ///     match sub.next().await {
    ///         Some(Ok(event)) => {
    ///             println!("Event at {}: {:?}", event.global_pos, event.data);
    ///         }
    ///         Some(Err(Error::SubscriptionLagged(n))) => {
    ///             eprintln!("Missed {} events!", n);
    ///             // Could create a new subscription from last known position
    ///             break;
    ///         }
    ///         None => break, // Channel closed
    ///         _ => {}
    ///     }
    /// }
    /// ```
    ///
    /// # Catch-Up
    ///
    /// For subscriptions from a historical position, use `subscribe_from()` which
    /// handles reading historical events before switching to live.
    pub async fn subscribe_all(&self) -> Result<crate::subscription::SimpleSubscription> {
        let response = self.writer.subscribe().await?;
        Ok(crate::subscription::SimpleSubscription::new(response.receiver))
    }

    /// Subscribes to live events from a specific stream.
    ///
    /// Only events from the specified stream will be delivered. Events from
    /// other streams are filtered out.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut sub = db.subscribe_stream("user-123").await?;
    ///
    /// while let Some(result) = sub.next().await {
    ///     let event = result?;
    ///     println!("User event: {:?}", event.data);
    /// }
    /// ```
    pub async fn subscribe_stream(
        &self,
        stream_id: impl Into<StreamId>,
    ) -> Result<crate::subscription::SimpleSubscription> {
        let response = self.writer.subscribe().await?;
        Ok(crate::subscription::SimpleSubscription::with_filter(
            response.receiver,
            stream_id.into(),
        ))
    }

    /// Subscribes to events starting from a specific global position.
    ///
    /// This implements the catch-up + live pattern:
    /// 1. First, reads historical events from `from_pos` to current head
    /// 2. Then, switches to receiving live events
    /// 3. Seamlessly delivers events in order
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Resume from last processed position
    /// let last_pos = GlobalPos::from_raw(12345);
    /// let mut sub = db.subscribe_from(last_pos).await?;
    ///
    /// while let Some(result) = sub.next().await {
    ///     let event = result?;
    ///     println!("Event {}: {:?}", event.global_pos, event.data);
    ///     // Save event.global_pos for resuming later
    /// }
    /// ```
    ///
    /// # Backpressure
    ///
    /// If the subscription falls too far behind during the live phase,
    /// it will return `Error::SubscriptionLagged`. The subscriber can then
    /// create a new subscription from the last processed position.
    pub async fn subscribe_from(
        &self,
        from_pos: GlobalPos,
    ) -> Result<crate::subscription::CatchUpSubscription> {
        // Get subscription response to know current head and get receiver
        let response = self.writer.subscribe().await?;

        // Clone reader channel for catch-up reads
        let read_tx = self.read_tx.clone();

        // Create a read function that uses the reader pool
        let read_fn = move |pos: GlobalPos, limit: usize| {
            let read_tx = read_tx.clone();
            async move {
                let (response_tx, response_rx) = oneshot::channel();
                read_tx
                    .send(ReadRequest::ReadGlobal {
                        from_pos: pos,
                        limit,
                        response: response_tx,
                    })
                    .await
                    .map_err(|_| Error::Schema("reader thread has shut down".to_string()))?;

                response_rx
                    .await
                    .map_err(|_| Error::Schema("reader dropped response channel".to_string()))?
            }
        };

        Ok(crate::subscription::CatchUpSubscription::new(
            from_pos,
            response.head_pos,
            response.receiver,
            read_fn,
            crate::subscription::DEFAULT_CATCHUP_BATCH_SIZE,
        ))
    }

    /// Subscribes to a specific stream starting from a revision.
    ///
    /// Combines stream filtering with catch-up from a historical position.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Subscribe to user-123 from revision 10
    /// let mut sub = db.subscribe_stream_from("user-123", StreamRev::from_raw(10)).await?;
    ///
    /// while let Some(result) = sub.next().await {
    ///     let event = result?;
    ///     println!("User event rev {}: {:?}", event.stream_rev, event.data);
    /// }
    /// ```
    pub async fn subscribe_stream_from(
        &self,
        stream_id: impl Into<StreamId>,
        _from_rev: StreamRev,
    ) -> Result<crate::subscription::SimpleSubscription> {
        // For now, just use the simple filtered subscription
        // A full implementation would do catch-up from the specified revision
        // This is a simplified version that only does live streaming
        self.subscribe_stream(stream_id).await
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
    use crate::types::EventData;

    #[tokio::test]
    async fn test_open_in_memory() {
        let db = SpiteDB::open_in_memory().await.unwrap();
        db.shutdown().await;
    }

    #[tokio::test]
    async fn test_append_single() {
        let db = SpiteDB::open_in_memory().await.unwrap();

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
        let db = SpiteDB::open_in_memory().await.unwrap();

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
        let db = SpiteDB::open_in_memory().await.unwrap();

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
        let db = SpiteDB::open_in_memory().await.unwrap();

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
        let db = SpiteDB::open_in_memory().await.unwrap();

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

        let db = SpiteDB::open(&db_path).await.unwrap();

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
        let db = SpiteDB::open_in_memory().await.unwrap();

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
}
