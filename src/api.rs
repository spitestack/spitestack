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

use rusqlite::{params, Connection, OpenFlags};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::error::{Error, Result};
use crate::schema::Database;
use crate::storage::Storage;
use crate::types::{AppendCommand, AppendResult, Event, GlobalPos, StreamId, StreamRev};

// =============================================================================
// Configuration
// =============================================================================

/// Size of the write request channel.
const WRITE_CHANNEL_SIZE: usize = 1024;

/// Size of the read request channel.
const READ_CHANNEL_SIZE: usize = 4096;

/// Minimum number of reader threads.
const MIN_READ_THREADS: usize = 1;

/// Maximum number of reader threads.
const MAX_READ_THREADS: usize = 16;

// =============================================================================
// Request Types
// =============================================================================

/// Internal request type for write operations.
enum WriteRequest {
    Append {
        command: AppendCommand,
        response: oneshot::Sender<Result<AppendResult>>,
    },
    Shutdown,
}

/// Internal request type for read operations.
enum ReadRequest {
    ReadStream {
        stream_id: StreamId,
        from_rev: StreamRev,
        limit: usize,
        response: oneshot::Sender<Result<Vec<Event>>>,
    },
    ReadGlobal {
        from_pos: GlobalPos,
        limit: usize,
        response: oneshot::Sender<Result<Vec<Event>>>,
    },
    GetStreamRevision {
        stream_id: StreamId,
        response: oneshot::Sender<StreamRev>,
    },
    Shutdown,
}

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
    /// Channel to send write requests.
    write_tx: mpsc::Sender<WriteRequest>,

    /// Channel to send read requests.
    read_tx: mpsc::Sender<ReadRequest>,

    /// Handle to the writer thread (for shutdown).
    writer_handle: Arc<Mutex<Option<JoinHandle<()>>>>,

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
        // Create channels
        // Note: mpsc::channel creates the async Tokio channel
        let (write_tx, write_rx) = mpsc::channel(WRITE_CHANNEL_SIZE);
        let (read_tx, read_rx) = mpsc::channel(READ_CHANNEL_SIZE);

        // Clone path for reader threads
        let read_path = path.clone();

        // Spawn writer thread
        // We use std::thread because Storage isn't Send+Sync
        // The thread owns Storage and communicates via channels
        let writer_handle = thread::Builder::new()
            .name("spitedb-async-writer".to_string())
            .spawn(move || {
                // Create runtime for receiving from async channel
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create writer runtime");

                rt.block_on(async {
                    // Initialize storage
                    let storage = match &path {
                        Some(p) => {
                            let db = Database::open(p).expect("failed to open database");
                            Storage::new(db.into_connection()).expect("failed to create storage")
                        }
                        None => {
                            let db =
                                Database::open_in_memory().expect("failed to open in-memory db");
                            Storage::new(db.into_connection()).expect("failed to create storage")
                        }
                    };

                    run_writer(storage, write_rx).await;
                });
            })
            .map_err(|e| Error::Schema(format!("failed to spawn writer thread: {}", e)))?;

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
                                run_reader_direct_pooled(conn, rx).await;
                            }
                            None => {
                                // In-memory: use Storage (single thread only)
                                let db =
                                    Database::open_in_memory().expect("failed to open in-memory db");
                                let storage =
                                    Storage::new(db.into_connection()).expect("failed to create storage");
                                run_reader_pooled(storage, rx).await;
                            }
                        }
                    });
                })
                .map_err(|e| Error::Schema(format!("failed to spawn reader thread: {}", e)))?;

            reader_handles.push(handle);
        }

        Ok(Self {
            write_tx,
            read_tx,
            writer_handle: Arc::new(Mutex::new(Some(writer_handle))),
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
        let (response_tx, response_rx) = oneshot::channel();

        self.write_tx
            .send(WriteRequest::Append {
                command,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::Schema("writer thread has shut down".to_string()))?;

        response_rx
            .await
            .map_err(|_| Error::Schema("writer dropped response channel".to_string()))?
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

    /// Shuts down the database gracefully.
    ///
    /// # Graceful Shutdown
    ///
    /// 1. Sends shutdown signal to writer and all reader threads
    /// 2. Waits for threads to complete (they'll finish current work first)
    /// 3. SQLite connections are closed when Storage is dropped
    ///
    /// # Important
    ///
    /// After shutdown, all operations will fail. Create a new SpiteDB instance
    /// if you need to reopen the database.
    pub async fn shutdown(self) {
        // Send shutdown signal to writer
        let _ = self.write_tx.send(WriteRequest::Shutdown).await;

        // Send shutdown signals to all reader threads
        for _ in 0..self.reader_count {
            let _ = self.read_tx.send(ReadRequest::Shutdown).await;
        }

        // Wait for writer to complete
        if let Some(handle) = self.writer_handle.lock().await.take() {
            let _ = handle.join();
        }

        // Wait for all reader threads to complete
        let handles = std::mem::take(&mut *self.reader_handles.lock().await);
        for handle in handles {
            let _ = handle.join();
        }
    }
}

// =============================================================================
// Worker Functions
// =============================================================================

/// The writer loop that processes write requests.
///
/// Runs on a dedicated thread, receives requests via async channel,
/// executes synchronously on Storage.
async fn run_writer(mut storage: Storage, mut rx: mpsc::Receiver<WriteRequest>) {
    while let Some(request) = rx.recv().await {
        match request {
            WriteRequest::Append { command, response } => {
                let result = storage.append(command);
                let _ = response.send(result);
            }
            WriteRequest::Shutdown => break,
        }
    }
}

/// The reader loop that processes read requests using Storage (for in-memory DBs).
/// This version is for single-thread use (takes ownership of receiver).
#[allow(dead_code)]
async fn run_reader(storage: Storage, mut rx: mpsc::Receiver<ReadRequest>) {
    while let Some(request) = rx.recv().await {
        match request {
            ReadRequest::ReadStream {
                stream_id,
                from_rev,
                limit,
                response,
            } => {
                let result = storage.read_stream(&stream_id, from_rev, limit);
                let _ = response.send(result);
            }
            ReadRequest::ReadGlobal {
                from_pos,
                limit,
                response,
            } => {
                let result = storage.read_global(from_pos, limit);
                let _ = response.send(result);
            }
            ReadRequest::GetStreamRevision {
                stream_id,
                response,
            } => {
                let result = storage.get_stream_revision(&stream_id);
                let _ = response.send(result);
            }
            ReadRequest::Shutdown => break,
        }
    }
}

/// The reader loop using direct SQL queries (for file-based DBs).
/// This version is for single-thread use (takes ownership of receiver).
#[allow(dead_code)]
async fn run_reader_direct(conn: Connection, mut rx: mpsc::Receiver<ReadRequest>) {
    while let Some(request) = rx.recv().await {
        match request {
            ReadRequest::ReadStream {
                stream_id,
                from_rev,
                limit,
                response,
            } => {
                let result = read_stream_direct(&conn, &stream_id, from_rev, limit);
                let _ = response.send(result);
            }
            ReadRequest::ReadGlobal {
                from_pos,
                limit,
                response,
            } => {
                let result = read_global_direct(&conn, from_pos, limit);
                let _ = response.send(result);
            }
            ReadRequest::GetStreamRevision {
                stream_id,
                response,
            } => {
                let result = get_stream_revision_direct(&conn, &stream_id);
                let _ = response.send(result);
            }
            ReadRequest::Shutdown => break,
        }
    }
}

/// Pooled reader using Storage (for in-memory DBs).
///
/// # Load Balancing
///
/// Multiple threads share the channel via Arc<Mutex>. Threads compete to
/// acquire the lock and receive the next request. This provides simple but
/// effective load balancing - whichever thread is free picks up the next request.
async fn run_reader_pooled(
    storage: Storage,
    rx: Arc<std::sync::Mutex<mpsc::Receiver<ReadRequest>>>,
) {
    loop {
        // Lock the receiver to get the next request
        // This provides simple load balancing - threads compete for work
        let request = {
            let mut guard = rx.lock().expect("receiver mutex poisoned");
            guard.recv().await
        };

        match request {
            Some(ReadRequest::ReadStream {
                stream_id,
                from_rev,
                limit,
                response,
            }) => {
                let result = storage.read_stream(&stream_id, from_rev, limit);
                let _ = response.send(result);
            }
            Some(ReadRequest::ReadGlobal {
                from_pos,
                limit,
                response,
            }) => {
                let result = storage.read_global(from_pos, limit);
                let _ = response.send(result);
            }
            Some(ReadRequest::GetStreamRevision {
                stream_id,
                response,
            }) => {
                let result = storage.get_stream_revision(&stream_id);
                let _ = response.send(result);
            }
            Some(ReadRequest::Shutdown) | None => break,
        }
    }
}

/// Pooled reader using direct SQL queries (for file-based DBs).
///
/// # Why Direct Queries?
///
/// For file-based databases with separate reader/writer connections, using
/// Storage would cache stream_heads at startup. Since the reader doesn't
/// see the writer's in-memory updates, it would have stale data.
///
/// By using direct SQL queries, we always read the latest committed data
/// from the database file (via SQLite's WAL mode).
///
/// # Load Balancing
///
/// Multiple threads share the channel via Arc<Mutex>. Each thread has its own
/// read-only SQLite connection, so they can execute queries in parallel.
async fn run_reader_direct_pooled(
    conn: Connection,
    rx: Arc<std::sync::Mutex<mpsc::Receiver<ReadRequest>>>,
) {
    loop {
        // Lock the receiver to get the next request
        let request = {
            let mut guard = rx.lock().expect("receiver mutex poisoned");
            guard.recv().await
        };

        match request {
            Some(ReadRequest::ReadStream {
                stream_id,
                from_rev,
                limit,
                response,
            }) => {
                let result = read_stream_direct(&conn, &stream_id, from_rev, limit);
                let _ = response.send(result);
            }
            Some(ReadRequest::ReadGlobal {
                from_pos,
                limit,
                response,
            }) => {
                let result = read_global_direct(&conn, from_pos, limit);
                let _ = response.send(result);
            }
            Some(ReadRequest::GetStreamRevision {
                stream_id,
                response,
            }) => {
                let result = get_stream_revision_direct(&conn, &stream_id);
                let _ = response.send(result);
            }
            Some(ReadRequest::Shutdown) | None => break,
        }
    }
}

// =============================================================================
// Direct Read Functions (for read-only connections)
// =============================================================================

/// Reads events from a stream using direct SQL queries.
fn read_stream_direct(
    conn: &Connection,
    stream_id: &StreamId,
    from_rev: StreamRev,
    limit: usize,
) -> Result<Vec<Event>> {
    let stream_hash = stream_id.hash();

    // Get collision slot from stream_heads
    let collision_slot: i64 = conn
        .query_row(
            "SELECT collision_slot FROM stream_heads WHERE stream_id = ?",
            [stream_id.as_str()],
            |row| row.get(0),
        )
        .unwrap_or(0);

    let mut stmt = conn.prepare(
        "SELECT e.global_pos, e.byte_offset, e.byte_len, e.stream_rev, b.data
         FROM event_index e
         JOIN batches b ON e.batch_id = b.batch_id
         WHERE e.stream_hash = ? AND e.collision_slot = ? AND e.stream_rev >= ?
         ORDER BY e.stream_rev
         LIMIT ?",
    )?;

    let events = stmt.query_map(
        params![
            stream_hash.as_raw(),
            collision_slot,
            from_rev.as_raw() as i64,
            limit as i64,
        ],
        |row| {
            let global_pos: i64 = row.get(0)?;
            let byte_offset: i64 = row.get(1)?;
            let byte_len: i64 = row.get(2)?;
            let stream_rev: i64 = row.get(3)?;
            let batch_data: Vec<u8> = row.get(4)?;

            Ok((global_pos, byte_offset, byte_len, stream_rev, batch_data))
        },
    )?;

    let mut result = Vec::new();
    for event_data in events {
        let (global_pos, byte_offset, byte_len, stream_rev, batch_data) = event_data?;

        let event = decode_event(
            &batch_data,
            byte_offset as usize,
            byte_len as usize,
            GlobalPos::from_raw_unchecked(global_pos as u64),
            stream_id.clone(),
            StreamRev::from_raw(stream_rev as u64),
        )?;

        result.push(event);
    }

    Ok(result)
}

/// Reads events from the global log using direct SQL queries.
fn read_global_direct(conn: &Connection, from_pos: GlobalPos, limit: usize) -> Result<Vec<Event>> {
    let mut stmt = conn.prepare(
        "SELECT e.global_pos, e.byte_offset, e.byte_len, e.stream_hash, e.collision_slot, e.stream_rev, b.data
         FROM event_index e
         JOIN batches b ON e.batch_id = b.batch_id
         WHERE e.global_pos >= ?
         ORDER BY e.global_pos
         LIMIT ?",
    )?;

    let events = stmt.query_map(params![from_pos.as_raw() as i64, limit as i64], |row| {
        let global_pos: i64 = row.get(0)?;
        let byte_offset: i64 = row.get(1)?;
        let byte_len: i64 = row.get(2)?;
        let _stream_hash: i64 = row.get(3)?;
        let _collision_slot: i64 = row.get(4)?;
        let stream_rev: i64 = row.get(5)?;
        let batch_data: Vec<u8> = row.get(6)?;

        Ok((global_pos, byte_offset, byte_len, stream_rev, batch_data))
    })?;

    let mut result = Vec::new();
    for event_data in events {
        let (global_pos, byte_offset, byte_len, stream_rev, batch_data) = event_data?;

        let event = decode_event_with_stream_id(
            &batch_data,
            byte_offset as usize,
            byte_len as usize,
            GlobalPos::from_raw_unchecked(global_pos as u64),
            StreamRev::from_raw(stream_rev as u64),
        )?;

        result.push(event);
    }

    Ok(result)
}

/// Gets stream revision using direct SQL queries.
fn get_stream_revision_direct(conn: &Connection, stream_id: &StreamId) -> StreamRev {
    conn.query_row(
        "SELECT last_rev FROM stream_heads WHERE stream_id = ?",
        [stream_id.as_str()],
        |row| {
            let rev: i64 = row.get(0)?;
            Ok(StreamRev::from_raw(rev as u64))
        },
    )
    .unwrap_or(StreamRev::NONE)
}

// =============================================================================
// Event Decoding
// =============================================================================

/// Decodes an event from batch data when stream_id is known.
fn decode_event(
    batch_data: &[u8],
    offset: usize,
    len: usize,
    global_pos: GlobalPos,
    stream_id: StreamId,
    stream_rev: StreamRev,
) -> Result<Event> {
    let event_data = &batch_data[offset..offset + len];
    let mut cursor = 0;

    // Skip stream_id (already provided)
    let stream_id_len = read_u32(event_data, &mut cursor) as usize;
    cursor += stream_id_len;

    // Event type
    let event_type_len = read_u32(event_data, &mut cursor) as usize;
    let event_type = if event_type_len > 0 {
        let s = std::str::from_utf8(&event_data[cursor..cursor + event_type_len])
            .map_err(|e| Error::Schema(format!("invalid event_type UTF-8: {}", e)))?;
        cursor += event_type_len;
        Some(s.to_string())
    } else {
        None
    };

    // Timestamp
    let timestamp_ms = read_u64(event_data, &mut cursor);

    // Metadata
    let metadata_len = read_u32(event_data, &mut cursor) as usize;
    let metadata = if metadata_len > 0 {
        let m = event_data[cursor..cursor + metadata_len].to_vec();
        cursor += metadata_len;
        Some(m)
    } else {
        None
    };

    // Data
    let data_len = read_u32(event_data, &mut cursor) as usize;
    let data = event_data[cursor..cursor + data_len].to_vec();

    Ok(Event {
        global_pos,
        stream_id,
        stream_rev,
        timestamp_ms,
        event_type,
        data,
        metadata,
    })
}

/// Decodes an event from batch data, extracting stream_id from the data.
fn decode_event_with_stream_id(
    batch_data: &[u8],
    offset: usize,
    len: usize,
    global_pos: GlobalPos,
    stream_rev: StreamRev,
) -> Result<Event> {
    let event_data = &batch_data[offset..offset + len];
    let mut cursor = 0;

    // Stream ID
    let stream_id_len = read_u32(event_data, &mut cursor) as usize;
    let stream_id_str = std::str::from_utf8(&event_data[cursor..cursor + stream_id_len])
        .map_err(|e| Error::Schema(format!("invalid stream_id UTF-8: {}", e)))?;
    cursor += stream_id_len;
    let stream_id = StreamId::new(stream_id_str);

    // Event type
    let event_type_len = read_u32(event_data, &mut cursor) as usize;
    let event_type = if event_type_len > 0 {
        let s = std::str::from_utf8(&event_data[cursor..cursor + event_type_len])
            .map_err(|e| Error::Schema(format!("invalid event_type UTF-8: {}", e)))?;
        cursor += event_type_len;
        Some(s.to_string())
    } else {
        None
    };

    // Timestamp
    let timestamp_ms = read_u64(event_data, &mut cursor);

    // Metadata
    let metadata_len = read_u32(event_data, &mut cursor) as usize;
    let metadata = if metadata_len > 0 {
        let m = event_data[cursor..cursor + metadata_len].to_vec();
        cursor += metadata_len;
        Some(m)
    } else {
        None
    };

    // Data
    let data_len = read_u32(event_data, &mut cursor) as usize;
    let data = event_data[cursor..cursor + data_len].to_vec();

    Ok(Event {
        global_pos,
        stream_id,
        stream_rev,
        timestamp_ms,
        event_type,
        data,
        metadata,
    })
}

/// Reads a u32 from bytes in little-endian format.
fn read_u32(data: &[u8], cursor: &mut usize) -> u32 {
    let bytes: [u8; 4] = data[*cursor..*cursor + 4].try_into().unwrap();
    *cursor += 4;
    u32::from_le_bytes(bytes)
}

/// Reads a u64 from bytes in little-endian format.
fn read_u64(data: &[u8], cursor: &mut usize) -> u64 {
    let bytes: [u8; 8] = data[*cursor..*cursor + 8].try_into().unwrap();
    *cursor += 8;
    u64::from_le_bytes(bytes)
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
