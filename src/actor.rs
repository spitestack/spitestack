//! # Actor Pattern for Thread-Safe Storage Access
//!
//! This module implements the actor pattern to provide safe concurrent access
//! to the storage layer. The architecture enforces CLAUDE.md invariants:
//!
//! > **Single logical writer**: At any time, only one node may write.
//!
//! We enforce this with two separate components:
//!
//! 1. **WriteActor**: Single thread, owns the write connection
//! 2. **ReadPool**: Multiple threads, each with a read-only connection
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                            Clients                                       │
//! └────────────────────────────────┬────────────────────────────────────────┘
//!                                  │
//!                    ┌─────────────┴─────────────┐
//!                    │                           │
//!                    ▼                           ▼
//!            ┌───────────────┐         ┌─────────────────────┐
//!            │  WriteActor   │         │     ReadPool        │
//!            │  (1 thread)   │         │   (N threads)       │
//!            │               │         │                     │
//!            │  ┌─────────┐  │         │  ┌───┐ ┌───┐ ┌───┐ │
//!            │  │ Storage │  │         │  │ R │ │ R │ │ R │ │
//!            │  │  (R/W)  │  │         │  └───┘ └───┘ └───┘ │
//!            │  └─────────┘  │         │  Read-only conns    │
//!            └───────┬───────┘         └──────────┬──────────┘
//!                    │                            │
//!                    └─────────────┬──────────────┘
//!                                  ▼
//!                          ┌─────────────┐
//!                          │   SQLite    │
//!                          │   (WAL)     │
//!                          └─────────────┘
//! ```
//!
//! ## Why Separate Read and Write?
//!
//! SQLite with WAL (Write-Ahead Logging) mode supports:
//! - One writer at a time (exclusive)
//! - Multiple concurrent readers
//!
//! By separating reads and writes, we can:
//! - Parallelize read operations across multiple threads
//! - Keep the write path simple and single-threaded
//! - Avoid read operations blocking writes (and vice versa)
//!
//! ## Rust Concepts
//!
//! - **Thread pools**: Multiple worker threads processing from a shared queue
//! - **Work stealing**: Not implemented here, but a future optimization
//! - **Read-only connections**: SQLite connections opened with `SQLITE_OPEN_READONLY`

use std::path::Path;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::thread::{self, available_parallelism, JoinHandle};

use rusqlite::{Connection, OpenFlags};

use crate::error::{Error, Result};
use crate::storage::Storage;
use crate::types::{AppendCommand, AppendResult, Event, GlobalPos, StreamId, StreamRev};

// =============================================================================
// Configuration
// =============================================================================

/// Maximum number of pending requests in write channel.
const WRITE_CHANNEL_BOUND: usize = 1024;

/// Maximum number of pending requests in read channel.
const READ_CHANNEL_BOUND: usize = 4096;

/// Minimum number of read threads.
const MIN_READ_THREADS: usize = 1;

/// Maximum number of read threads.
const MAX_READ_THREADS: usize = 16;

// =============================================================================
// Write Actor
// =============================================================================

/// Request types for the write actor.
///
/// Only write operations go through this channel.
pub enum WriteRequest {
    /// Append events to a stream.
    Append {
        command: AppendCommand,
        response_tx: Sender<Result<AppendResult>>,
    },

    /// Shutdown the writer gracefully.
    Shutdown,
}

/// The single write actor that owns the Storage instance.
///
/// # Single Writer Invariant
///
/// There must only ever be ONE WriteActor for a database. This is enforced
/// by ownership - creating a WriteActor consumes the Storage, and Storage
/// can only be created from a Database connection.
///
/// # Why Not Clone?
///
/// WriteActor intentionally does NOT implement Clone. You can only get
/// additional senders via `sender()`, which share the same underlying
/// channel and thread.
#[derive(Debug)]
pub struct WriteActor {
    request_tx: SyncSender<WriteRequest>,
    thread_handle: Option<JoinHandle<()>>,
}

impl WriteActor {
    /// Spawns the write actor on a dedicated thread.
    ///
    /// # Arguments
    ///
    /// * `storage` - The storage instance (ownership transferred to actor thread)
    ///
    /// # Single Writer
    ///
    /// This consumes `storage`, ensuring only one writer exists.
    pub fn spawn(storage: Storage) -> Self {
        let (request_tx, request_rx) = mpsc::sync_channel(WRITE_CHANNEL_BOUND);

        let thread_handle = thread::Builder::new()
            .name("spitedb-writer".to_string())
            .spawn(move || {
                run_write_actor(storage, request_rx);
            })
            .expect("failed to spawn writer thread");

        Self {
            request_tx,
            thread_handle: Some(thread_handle),
        }
    }

    /// Appends events to a stream.
    pub fn append(&self, command: AppendCommand) -> Result<AppendResult> {
        let (response_tx, response_rx) = mpsc::channel();

        self.request_tx
            .send(WriteRequest::Append {
                command,
                response_tx,
            })
            .map_err(|_| Error::Schema("write actor has shut down".to_string()))?;

        response_rx
            .recv()
            .map_err(|_| Error::Schema("write actor dropped response channel".to_string()))?
    }

    /// Returns a clone of the request sender for sharing.
    pub fn sender(&self) -> SyncSender<WriteRequest> {
        self.request_tx.clone()
    }

    /// Shuts down the write actor gracefully.
    pub fn shutdown(mut self) {
        let _ = self.request_tx.send(WriteRequest::Shutdown);

        if let Some(handle) = self.thread_handle.take() {
            handle.join().expect("write actor thread panicked");
        }
    }
}

/// The write actor's main loop.
fn run_write_actor(mut storage: Storage, request_rx: Receiver<WriteRequest>) {
    while let Ok(request) = request_rx.recv() {
        match request {
            WriteRequest::Append {
                command,
                response_tx,
            } => {
                let result = storage.append(command);
                let _ = response_tx.send(result);
            }
            WriteRequest::Shutdown => break,
        }
    }
}

// =============================================================================
// Read Pool
// =============================================================================

/// Request types for the read pool.
pub enum ReadRequest {
    /// Read events from a specific stream.
    ReadStream {
        stream_id: StreamId,
        from_rev: StreamRev,
        limit: usize,
        response_tx: Sender<Result<Vec<Event>>>,
    },

    /// Read events from the global log.
    ReadGlobal {
        from_pos: GlobalPos,
        limit: usize,
        response_tx: Sender<Result<Vec<Event>>>,
    },

    /// Get the current revision of a stream.
    GetStreamRevision {
        stream_id: StreamId,
        response_tx: Sender<StreamRev>,
    },

    /// Shutdown this reader thread.
    Shutdown,
}

/// A pool of reader threads for parallel read operations.
///
/// # Thread Count
///
/// By default, uses `available_parallelism()` (number of CPU cores),
/// clamped to `[MIN_READ_THREADS, MAX_READ_THREADS]`.
///
/// # Load Balancing
///
/// All reader threads share the same channel. When a request arrives,
/// whichever thread is free picks it up. This provides simple but
/// effective load balancing.
///
/// # Read-Only Connections
///
/// Each reader thread opens its own read-only SQLite connection.
/// This is safe because:
/// - WAL mode allows concurrent readers
/// - Read-only connections can't modify data
/// - Each thread owns its connection (no sharing)
#[derive(Debug)]
pub struct ReadPool {
    request_tx: SyncSender<ReadRequest>,
    thread_handles: Vec<JoinHandle<()>>,
}

impl ReadPool {
    /// Creates a new read pool for an in-memory database.
    ///
    /// # In-Memory Limitation
    ///
    /// In-memory databases can't have multiple connections. We use a single
    /// reader thread that shares the connection via the write actor's Storage.
    /// This is mainly for testing.
    ///
    /// For production, use `ReadPool::open()` with a file path.
    pub fn spawn_in_memory(storage: Storage) -> Self {
        let (request_tx, request_rx) = mpsc::sync_channel(READ_CHANNEL_BOUND);

        // For in-memory, we can only have one connection, so one reader thread
        let handle = thread::Builder::new()
            .name("spitedb-reader-0".to_string())
            .spawn(move || {
                run_read_worker_with_storage(storage, request_rx);
            })
            .expect("failed to spawn reader thread");

        Self {
            request_tx,
            thread_handles: vec![handle],
        }
    }

    /// Creates a new read pool with file-based read-only connections.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the SQLite database file
    /// * `thread_count` - Number of reader threads (None = auto-detect)
    ///
    /// # Auto Thread Count
    ///
    /// If `thread_count` is None, uses `available_parallelism()` (CPU cores),
    /// clamped to reasonable bounds.
    pub fn open<P: AsRef<Path>>(path: P, thread_count: Option<usize>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Determine thread count
        let count = thread_count.unwrap_or_else(|| {
            available_parallelism()
                .map(|n| n.get())
                .unwrap_or(MIN_READ_THREADS)
        });
        let count = count.clamp(MIN_READ_THREADS, MAX_READ_THREADS);

        let (request_tx, request_rx) = mpsc::sync_channel(READ_CHANNEL_BOUND);

        // Wrap receiver in Arc for sharing across threads
        // We use a simple approach: clone the receiver for each thread
        // Actually, Receiver is not Clone, so we need a different approach
        // We'll use a shared channel where threads compete to receive
        let request_rx = std::sync::Arc::new(std::sync::Mutex::new(request_rx));

        let mut thread_handles = Vec::with_capacity(count);

        for i in 0..count {
            let path = path.clone();
            let rx = std::sync::Arc::clone(&request_rx);

            let handle = thread::Builder::new()
                .name(format!("spitedb-reader-{}", i))
                .spawn(move || {
                    // Each thread opens its own read-only connection
                    let conn = Connection::open_with_flags(
                        &path,
                        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                    )
                    .expect("failed to open read-only connection");

                    run_read_worker(conn, rx);
                })
                .expect("failed to spawn reader thread");

            thread_handles.push(handle);
        }

        Ok(Self {
            request_tx,
            thread_handles,
        })
    }

    /// Number of reader threads in the pool.
    pub fn thread_count(&self) -> usize {
        self.thread_handles.len()
    }

    /// Reads events from a specific stream.
    pub fn read_stream(
        &self,
        stream_id: StreamId,
        from_rev: StreamRev,
        limit: usize,
    ) -> Result<Vec<Event>> {
        let (response_tx, response_rx) = mpsc::channel();

        self.request_tx
            .send(ReadRequest::ReadStream {
                stream_id,
                from_rev,
                limit,
                response_tx,
            })
            .map_err(|_| Error::Schema("read pool has shut down".to_string()))?;

        response_rx
            .recv()
            .map_err(|_| Error::Schema("read pool dropped response channel".to_string()))?
    }

    /// Reads events from the global log.
    pub fn read_global(&self, from_pos: GlobalPos, limit: usize) -> Result<Vec<Event>> {
        let (response_tx, response_rx) = mpsc::channel();

        self.request_tx
            .send(ReadRequest::ReadGlobal {
                from_pos,
                limit,
                response_tx,
            })
            .map_err(|_| Error::Schema("read pool has shut down".to_string()))?;

        response_rx
            .recv()
            .map_err(|_| Error::Schema("read pool dropped response channel".to_string()))?
    }

    /// Gets the current revision of a stream.
    pub fn get_stream_revision(&self, stream_id: StreamId) -> Result<StreamRev> {
        let (response_tx, response_rx) = mpsc::channel();

        self.request_tx
            .send(ReadRequest::GetStreamRevision {
                stream_id,
                response_tx,
            })
            .map_err(|_| Error::Schema("read pool has shut down".to_string()))?;

        response_rx
            .recv()
            .map_err(|_| Error::Schema("read pool dropped response channel".to_string()))
    }

    /// Returns a clone of the request sender.
    pub fn sender(&self) -> SyncSender<ReadRequest> {
        self.request_tx.clone()
    }

    /// Shuts down all reader threads gracefully.
    pub fn shutdown(self) {
        // Send shutdown to all threads
        for _ in 0..self.thread_handles.len() {
            let _ = self.request_tx.send(ReadRequest::Shutdown);
        }

        // Wait for all threads to complete
        for handle in self.thread_handles {
            handle.join().expect("reader thread panicked");
        }
    }
}

/// Read worker loop using a shared Storage instance (for in-memory).
fn run_read_worker_with_storage(storage: Storage, request_rx: Receiver<ReadRequest>) {
    while let Ok(request) = request_rx.recv() {
        match request {
            ReadRequest::ReadStream {
                stream_id,
                from_rev,
                limit,
                response_tx,
            } => {
                let result = storage.read_stream(&stream_id, from_rev, limit);
                let _ = response_tx.send(result);
            }
            ReadRequest::ReadGlobal {
                from_pos,
                limit,
                response_tx,
            } => {
                let result = storage.read_global(from_pos, limit);
                let _ = response_tx.send(result);
            }
            ReadRequest::GetStreamRevision {
                stream_id,
                response_tx,
            } => {
                let result = storage.get_stream_revision(&stream_id);
                let _ = response_tx.send(result);
            }
            ReadRequest::Shutdown => break,
        }
    }
}

/// Read worker loop using a read-only connection (for file-based DBs).
fn run_read_worker(
    conn: Connection,
    request_rx: std::sync::Arc<std::sync::Mutex<Receiver<ReadRequest>>>,
) {
    loop {
        // Lock the receiver to get the next request
        // This provides simple load balancing - threads compete for work
        let request = {
            let rx = request_rx.lock().expect("receiver mutex poisoned");
            rx.recv()
        };

        match request {
            Ok(ReadRequest::ReadStream {
                stream_id,
                from_rev,
                limit,
                response_tx,
            }) => {
                let result = read_stream_direct(&conn, &stream_id, from_rev, limit);
                let _ = response_tx.send(result);
            }
            Ok(ReadRequest::ReadGlobal {
                from_pos,
                limit,
                response_tx,
            }) => {
                let result = read_global_direct(&conn, from_pos, limit);
                let _ = response_tx.send(result);
            }
            Ok(ReadRequest::GetStreamRevision {
                stream_id,
                response_tx,
            }) => {
                let result = get_stream_revision_direct(&conn, &stream_id);
                let _ = response_tx.send(result);
            }
            Ok(ReadRequest::Shutdown) | Err(_) => break,
        }
    }
}

// =============================================================================
// Direct Read Functions (for read-only connections)
// =============================================================================

/// Reads events from a stream using a direct connection.
///
/// This duplicates some logic from Storage, but avoids needing the full
/// Storage struct with its write-focused caches.
fn read_stream_direct(
    conn: &Connection,
    stream_id: &StreamId,
    from_rev: StreamRev,
    limit: usize,
) -> Result<Vec<Event>> {
    use rusqlite::params;

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

/// Reads events from the global log using a direct connection.
fn read_global_direct(conn: &Connection, from_pos: GlobalPos, limit: usize) -> Result<Vec<Event>> {
    use rusqlite::params;

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

/// Gets stream revision using a direct connection.
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
// Event Decoding (duplicated from storage for read-only use)
// =============================================================================

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

    // Skip stream_id
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

fn read_u32(data: &[u8], cursor: &mut usize) -> u32 {
    let bytes: [u8; 4] = data[*cursor..*cursor + 4].try_into().unwrap();
    *cursor += 4;
    u32::from_le_bytes(bytes)
}

fn read_u64(data: &[u8], cursor: &mut usize) -> u64 {
    let bytes: [u8; 8] = data[*cursor..*cursor + 8].try_into().unwrap();
    *cursor += 8;
    u64::from_le_bytes(bytes)
}

// =============================================================================
// Combined Handle (for convenience)
// =============================================================================

/// A combined handle providing both read and write access.
///
/// This is a convenience wrapper that holds both a WriteActor and ReadPool,
/// routing operations appropriately.
#[derive(Debug)]
pub struct StorageHandle {
    writer: WriteActor,
    readers: ReadPool,
}

impl StorageHandle {
    /// Creates a new storage handle for an in-memory database.
    ///
    /// # In-Memory Limitation
    ///
    /// In-memory databases are single-connection. We use one Storage for
    /// writes and a separate Storage (sharing the same in-memory DB via
    /// shared cache) for reads. For testing purposes, this works well.
    pub fn open_in_memory() -> Result<Self> {
        use crate::schema::Database;

        // Create the write connection
        let write_db = Database::open_in_memory()?;
        let write_storage = Storage::new(write_db.into_connection())?;
        let writer = WriteActor::spawn(write_storage);

        // For in-memory, create a second storage for reads
        // This uses shared cache mode so they see the same data
        let read_db = Database::open_in_memory()?;
        let read_storage = Storage::new(read_db.into_connection())?;
        let readers = ReadPool::spawn_in_memory(read_storage);

        Ok(Self { writer, readers })
    }

    /// Creates a new storage handle for a file-based database.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the SQLite database file
    /// * `read_threads` - Number of reader threads (None = auto-detect)
    pub fn open<P: AsRef<Path>>(path: P, read_threads: Option<usize>) -> Result<Self> {
        use crate::schema::Database;

        let path = path.as_ref();

        // Create the write connection
        let write_db = Database::open(path)?;
        let write_storage = Storage::new(write_db.into_connection())?;
        let writer = WriteActor::spawn(write_storage);

        // Create read pool with read-only connections
        let readers = ReadPool::open(path, read_threads)?;

        Ok(Self { writer, readers })
    }

    /// Appends events to a stream.
    pub fn append(&self, command: AppendCommand) -> Result<AppendResult> {
        self.writer.append(command)
    }

    /// Reads events from a specific stream.
    pub fn read_stream(
        &self,
        stream_id: StreamId,
        from_rev: StreamRev,
        limit: usize,
    ) -> Result<Vec<Event>> {
        self.readers.read_stream(stream_id, from_rev, limit)
    }

    /// Reads events from the global log.
    pub fn read_global(&self, from_pos: GlobalPos, limit: usize) -> Result<Vec<Event>> {
        self.readers.read_global(from_pos, limit)
    }

    /// Gets the current revision of a stream.
    pub fn get_stream_revision(&self, stream_id: StreamId) -> Result<StreamRev> {
        self.readers.get_stream_revision(stream_id)
    }

    /// Returns the number of reader threads.
    pub fn read_thread_count(&self) -> usize {
        self.readers.thread_count()
    }

    /// Shuts down all actors gracefully.
    pub fn shutdown(self) {
        self.writer.shutdown();
        self.readers.shutdown();
    }
}

// =============================================================================
// Legacy Compatibility
// =============================================================================

/// Legacy alias for backwards compatibility.
///
/// # Deprecated
///
/// Use `StorageHandle` instead for the combined read/write interface,
/// or use `WriteActor` and `ReadPool` separately for more control.
pub type StorageActor = StorageHandle;

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Database;
    use crate::types::EventData;
    use std::sync::Arc;

    /// Creates a test write actor with an in-memory database.
    fn create_test_write_actor() -> (WriteActor, Storage) {
        let write_db = Database::open_in_memory().unwrap();
        let write_storage = Storage::new(write_db.into_connection()).unwrap();
        let writer = WriteActor::spawn(write_storage);

        let read_db = Database::open_in_memory().unwrap();
        let read_storage = Storage::new(read_db.into_connection()).unwrap();

        (writer, read_storage)
    }

    #[test]
    fn test_write_actor_append() {
        let (writer, _read_storage) = create_test_write_actor();

        let cmd = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"hello".to_vec())],
        );

        let result = writer.append(cmd).unwrap();
        assert_eq!(result.first_pos, GlobalPos::FIRST);

        writer.shutdown();
    }

    #[test]
    fn test_write_actor_conflict() {
        let (writer, _read_storage) = create_test_write_actor();

        // First append
        let cmd1 = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"first".to_vec())],
        );
        writer.append(cmd1).unwrap();

        // Conflicting append
        let cmd2 = AppendCommand::new(
            "cmd-2",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"conflict".to_vec())],
        );

        let result = writer.append(cmd2);
        assert!(matches!(result, Err(Error::Conflict { .. })));

        writer.shutdown();
    }

    #[test]
    fn test_write_actor_idempotency() {
        let (writer, _read_storage) = create_test_write_actor();

        let cmd = AppendCommand::new(
            "cmd-idem",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"hello".to_vec())],
        );

        let result1 = writer.append(cmd.clone()).unwrap();
        let result2 = writer.append(cmd).unwrap();

        assert_eq!(result1.first_pos.as_raw(), result2.first_pos.as_raw());

        writer.shutdown();
    }

    #[test]
    fn test_read_pool_in_memory() {
        let db = Database::open_in_memory().unwrap();
        let mut storage = Storage::new(db.into_connection()).unwrap();

        // Write some events first
        let cmd = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![
                EventData::new(b"event1".to_vec()),
                EventData::new(b"event2".to_vec()),
            ],
        );
        storage.append(cmd).unwrap();

        // Now create read pool with this storage
        let read_pool = ReadPool::spawn_in_memory(storage);

        // Read should work
        let events = read_pool
            .read_stream(StreamId::new("stream-1"), StreamRev::FIRST, 100)
            .unwrap();
        assert_eq!(events.len(), 2);

        let rev = read_pool
            .get_stream_revision(StreamId::new("stream-1"))
            .unwrap();
        assert_eq!(rev.as_raw(), 2);

        read_pool.shutdown();
    }

    #[test]
    fn test_concurrent_writes() {
        let (writer, _read_storage) = create_test_write_actor();
        let writer = Arc::new(writer);

        let mut handles = vec![];

        for i in 0..10 {
            let w = Arc::clone(&writer);
            let handle = thread::spawn(move || {
                let cmd = AppendCommand::new(
                    format!("cmd-{}", i),
                    format!("stream-{}", i),
                    StreamRev::NONE,
                    vec![EventData::new(format!("event-{}", i).into_bytes())],
                );
                w.append(cmd).unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Extract from Arc and shutdown
        Arc::try_unwrap(writer)
            .expect("other refs exist")
            .shutdown();
    }

    #[test]
    fn test_file_based_read_pool() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        // Create and populate database
        {
            let db = Database::open(&db_path).unwrap();
            let mut storage = Storage::new(db.into_connection()).unwrap();

            for i in 0..5 {
                let cmd = AppendCommand::new(
                    format!("cmd-{}", i),
                    format!("stream-{}", i),
                    StreamRev::NONE,
                    vec![EventData::new(format!("event-{}", i).into_bytes())],
                );
                storage.append(cmd).unwrap();
            }
        }

        // Create read pool
        let read_pool = ReadPool::open(&db_path, Some(4)).unwrap();
        assert_eq!(read_pool.thread_count(), 4);

        // Read from multiple threads concurrently
        let pool = Arc::new(read_pool);
        let mut handles = vec![];

        for i in 0..5 {
            let p = Arc::clone(&pool);
            let handle = thread::spawn(move || {
                let events = p
                    .read_stream(
                        StreamId::new(format!("stream-{}", i)),
                        StreamRev::FIRST,
                        100,
                    )
                    .unwrap();
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].data, format!("event-{}", i).into_bytes());
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        Arc::try_unwrap(pool).expect("other refs exist").shutdown();
    }

    #[test]
    fn test_storage_handle_file_based() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        // Create the database first
        {
            let _db = Database::open(&db_path).unwrap();
        }

        let handle = StorageHandle::open(&db_path, Some(2)).unwrap();
        assert_eq!(handle.read_thread_count(), 2);

        // Write
        let cmd = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"hello".to_vec())],
        );
        let result = handle.append(cmd).unwrap();
        assert_eq!(result.first_pos, GlobalPos::FIRST);

        // Give a moment for read-only connections to see the write
        thread::sleep(std::time::Duration::from_millis(10));

        // Read
        let events = handle
            .read_stream(StreamId::new("stream-1"), StreamRev::FIRST, 100)
            .unwrap();
        assert_eq!(events.len(), 1);

        handle.shutdown();
    }
}
