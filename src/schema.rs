//! # SQLite Schema for SpiteDB
//!
//! This module defines the database schema and handles initialization. The schema
//! is carefully designed to support event sourcing with high throughput.
//!
//! ## Table Overview
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                           Schema Overview                               │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                         │
//! │  batches            event_index              stream_heads               │
//! │  ┌──────────┐       ┌─────────────────┐      ┌─────────────────┐        │
//! │  │ batch_id │◄──────│ batch_id (FK)   │      │ stream_hash (PK)│        │
//! │  │ base_pos │       │ global_pos (PK) │      │ last_rev        │        │
//! │  │ data BLOB│       │ stream_hash ────┼─────►│ last_pos        │        │
//! │  └──────────┘       │ stream_rev      │      └─────────────────┘        │
//! │                     └─────────────────┘                                 │
//! │                                                                         │
//! │  commands                        tombstones                             │
//! │  ┌─────────────────┐             ┌─────────────────┐                    │
//! │  │ command_id (PK) │             │ stream_hash     │                    │
//! │  │ stream_hash     │             │ from_rev        │                    │
//! │  │ first_pos       │             │ to_rev          │                    │
//! │  │ last_pos        │             └─────────────────┘                    │
//! │  └─────────────────┘                                                    │
//! │                                                                         │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Design Decisions
//!
//! ### Why `stream_hash` instead of `stream_id` as FK?
//!
//! Stream IDs can be arbitrarily long strings (e.g., "user-12345-orders-2024").
//! Using a hash (i64) as the primary identifier:
//! - Saves space in indexes
//! - Faster comparisons (integer vs string)
//! - Still store full `stream_id` in `event_index` for human readability
//!
//! ### Why separate `batches` and `event_index`?
//!
//! Events are written in batches for throughput. Storing them as:
//! - **Batch blob**: Efficient sequential writes, good compression
//! - **Event index**: Fast random access by global_pos or stream
//!
//! This is a classic trade-off: write amplification (two inserts per batch)
//! for read flexibility (index supports multiple access patterns).

use rusqlite::Connection;

use crate::{Error, Result};

// =============================================================================
// Schema Version
// =============================================================================

/// Current schema version. Increment when making breaking schema changes.
///
/// # Migration Strategy
///
/// For v1, we don't implement migrations - if the version doesn't match,
/// we return an error. Future versions may add migration support.
const SCHEMA_VERSION: i32 = 1;

// =============================================================================
// DDL Statements
// =============================================================================
// Each table has its own constant for readability. Comments explain the
// purpose of each column and any non-obvious design decisions.

/// The `batches` table stores event data as compressed blobs.
///
/// # Columns
///
/// - `batch_id`: Auto-increment primary key, used by event_index FK
/// - `base_pos`: First global_pos in this batch (for quick range queries)
/// - `event_count`: Number of events in batch (for iteration without decoding)
/// - `created_ms`: Unix timestamp in milliseconds
/// - `codec`: Compression algorithm (0 = none, 1 = lz4, etc.)
/// - `cipher`: Encryption algorithm (0 = none, 1 = AES-GCM, etc.)
/// - `nonce`: Encryption nonce/IV if cipher != 0
/// - `checksum`: Integrity checksum (e.g., CRC32, XXH3)
/// - `data`: The actual event payloads, possibly compressed and/or encrypted
///
/// # Why BLOB for data?
///
/// SQLite is optimized for BLOB storage. By packing multiple events into one
/// blob, we reduce:
/// - Row overhead (SQLite has ~50 bytes overhead per row)
/// - Index entries (one batch = one row, not N events = N rows)
/// - Transaction overhead (fewer rows to lock and journal)
const CREATE_BATCHES: &str = r#"
CREATE TABLE IF NOT EXISTS batches (
    batch_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    base_pos    INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    created_ms  INTEGER NOT NULL,
    codec       INTEGER NOT NULL DEFAULT 0,
    cipher      INTEGER NOT NULL DEFAULT 0,
    nonce       BLOB,
    checksum    BLOB NOT NULL,
    data        BLOB NOT NULL
)
"#;

/// The `event_index` table provides fast lookups by position or stream.
///
/// # Design Philosophy: Minimal Index
///
/// This table is intentionally slim - only 7 integer columns.
/// All other metadata (stream_id, event_type, ts_ms) lives in the batch blob.
/// This keeps the B-tree small and fast.
///
/// # Columns
///
/// - `global_pos`: Unique, strictly increasing position in the global log (PK)
/// - `batch_id`: FK to batches table (which blob contains this event)
/// - `byte_offset`: Offset within the batch's data blob (where event starts)
/// - `byte_len`: Length of this event's data within the blob
/// - `stream_hash`: XXH3-64 hash of stream_id for fast lookups
/// - `collision_slot`: Disambiguator for hash collisions (usually 0)
/// - `stream_rev`: Revision within the stream (1, 2, 3, ...)
///
/// # Storage Cost
///
/// SQLite stores small integers efficiently:
/// - collision_slot = 0: ~1 byte (not 8!)
/// - For 10M events with no collisions: ~10MB overhead
///
/// # What's NOT Here (lives in blob instead)
///
/// - `stream_id`: Stored in blob, retrieved when decoding. Client usually knows it anyway.
/// - `event_type`: Stored in blob. Filtering by type requires decoding, but that's rare.
/// - `ts_ms`: Stored in blob. Time-based queries use batch's created_ms as approximation.
///
/// # Invariants (from CLAUDE.md)
///
/// - `global_pos` strictly increases, is never reused
/// - `stream_rev` strictly increases per stream, no gaps or duplicates
/// - Events in a single append are contiguous in global_pos
///
/// # Indexes
///
/// - PRIMARY KEY on global_pos: O(log n) reads from global log
/// - UNIQUE INDEX on (stream_hash, collision_slot, stream_rev): O(log n) stream reads
const CREATE_EVENT_INDEX: &str = r#"
CREATE TABLE IF NOT EXISTS event_index (
    global_pos     INTEGER PRIMARY KEY,
    batch_id       INTEGER NOT NULL,
    byte_offset    INTEGER NOT NULL,
    byte_len       INTEGER NOT NULL,
    stream_hash    INTEGER NOT NULL,
    collision_slot INTEGER NOT NULL DEFAULT 0,
    stream_rev     INTEGER NOT NULL
)
"#;

/// Unique index enforcing stream revision uniqueness.
///
/// # Why (stream_hash, collision_slot, stream_rev)?
///
/// - `stream_hash`: Fast integer lookup (vs string comparison)
/// - `collision_slot`: Disambiguates rare hash collisions (usually 0)
/// - `stream_rev`: Enforces no duplicate revisions within a stream
///
/// This index supports:
/// - `WHERE stream_hash = ? AND collision_slot = ? ORDER BY stream_rev`
/// - Uniqueness constraint preventing duplicate events
const CREATE_EVENT_STREAM_INDEX: &str = r#"
CREATE UNIQUE INDEX IF NOT EXISTS event_stream_rev
ON event_index(stream_hash, collision_slot, stream_rev)
"#;

/// The `stream_heads` table caches the latest revision for each stream.
///
/// # Purpose
///
/// When appending events, we need to:
/// 1. Check the current stream revision (for conflict detection)
/// 2. Determine the next revision number
/// 3. Handle hash collisions (two different stream_ids with same hash)
///
/// # Columns
///
/// - `stream_id`: Original stream ID string (primary key for uniqueness)
/// - `stream_hash`: XXH3-64 hash of stream_id (for fast indexed lookups)
/// - `collision_slot`: Disambiguator for hash collisions (0 for first stream, 1+ for collisions)
/// - `last_rev`: Most recent stream_rev for this stream
/// - `last_pos`: global_pos of the most recent event
///
/// # Hash Collision Handling
///
/// With 64-bit hashes, collisions are rare (~1 in 37M at 1M streams) but possible.
/// When detected, we auto-assign collision slots:
///
/// ```text
/// Stream "user-123"  → hash X, slot 0
/// Stream "order-abc" → hash X, slot 1  (collision, auto-assigned)
/// ```
///
/// The event_index uses (stream_hash, collision_slot) to distinguish streams.
/// Users never see collision_slot - it's handled transparently.
///
/// # Query Pattern
///
/// On append, we query by stream_hash (not stream_id):
/// ```sql
/// SELECT stream_id, collision_slot, last_rev, last_pos
/// FROM stream_heads WHERE stream_hash = ?
/// ```
/// This returns all streams with this hash (usually 1, rarely 2+).
/// We match stream_id in application code to find our stream or detect collision.
///
/// # Memory Caching
///
/// In production, this table is cached in memory as a HashMap<StreamHash, Vec<Entry>>.
/// Collision checks become O(1) HashMap lookup + iterate 1-2 entries (~50ns).
/// The table serves as the source of truth; memory cache can lag but never lead.
const CREATE_STREAM_HEADS: &str = r#"
CREATE TABLE IF NOT EXISTS stream_heads (
    stream_id      TEXT PRIMARY KEY,
    stream_hash    INTEGER NOT NULL,
    collision_slot INTEGER NOT NULL DEFAULT 0,
    last_rev       INTEGER NOT NULL,
    last_pos       INTEGER NOT NULL
)
"#;

/// Index for efficient stream_hash lookups.
///
/// We query by stream_hash to find all streams with a given hash,
/// then filter by stream_id in application code. This supports both:
/// - Normal lookups (find stream by hash)
/// - Collision detection (find other streams with same hash)
const CREATE_STREAM_HEADS_HASH_INDEX: &str = r#"
CREATE INDEX IF NOT EXISTS stream_heads_hash
ON stream_heads(stream_hash)
"#;

/// The `commands` table tracks processed commands for idempotency.
///
/// # Exactly-Once Semantics
///
/// Every append includes a client-supplied `command_id`. If we see the same
/// command_id again, we return the stored result instead of re-processing.
///
/// # Columns
///
/// - `command_id`: Client-supplied unique identifier (UUID recommended)
/// - `stream_hash`: Which stream this command wrote to
/// - `first_pos`: First global_pos written by this command
/// - `last_pos`: Last global_pos written by this command
/// - `created_ms`: When the command was processed
///
/// # Why Store Position Range?
///
/// When returning a cached result, we need to tell the client what events
/// were created. The position range lets us reconstruct the AppendResult.
///
/// # Cleanup
///
/// Commands can be garbage collected after some retention period (e.g., 7 days).
/// Clients retrying after that period will fail, but that's expected behavior.
const CREATE_COMMANDS: &str = r#"
CREATE TABLE IF NOT EXISTS commands (
    command_id  TEXT PRIMARY KEY,
    stream_hash INTEGER NOT NULL,
    first_pos   INTEGER NOT NULL,
    last_pos    INTEGER NOT NULL,
    created_ms  INTEGER NOT NULL
)
"#;

/// The `tombstones` table marks events for logical deletion.
///
/// # GDPR Compliance
///
/// When a user requests data deletion, we can't immediately remove events:
/// - Other processes may be reading them
/// - Subscribers may not have processed them yet
/// - Compaction is expensive and shouldn't be synchronous
///
/// Instead, we:
/// 1. Insert a tombstone record (fast, synchronous)
/// 2. Filter out tombstoned events on read (immediate effect)
/// 3. Physically delete during offline compaction (eventual)
///
/// # Columns
///
/// - `stream_hash`: Which stream contains the deleted events
/// - `from_rev`: First revision to delete (inclusive)
/// - `to_rev`: Last revision to delete (inclusive)
/// - `deleted_ms`: When the deletion was requested
///
/// # Why Revision Ranges?
///
/// GDPR often requires deleting all data for a user, which means all events
/// in their stream. Storing a range (from_rev, to_rev) is more efficient
/// than one row per deleted event.
const CREATE_TOMBSTONES: &str = r#"
CREATE TABLE IF NOT EXISTS tombstones (
    stream_hash INTEGER NOT NULL,
    from_rev    INTEGER NOT NULL,
    to_rev      INTEGER NOT NULL,
    deleted_ms  INTEGER NOT NULL
)
"#;

/// Index for efficient tombstone lookups during reads.
///
/// When reading a stream, we need to quickly find any tombstones that apply.
/// This index supports: `WHERE stream_hash = ? AND from_rev <= ? AND to_rev >= ?`
const CREATE_TOMBSTONES_INDEX: &str = r#"
CREATE INDEX IF NOT EXISTS tombstones_stream
ON tombstones(stream_hash, from_rev, to_rev)
"#;

/// Metadata table for schema versioning.
///
/// # Why a Separate Table?
///
/// SQLite has a `user_version` pragma, but we want richer metadata:
/// - Multiple key-value pairs
/// - Timestamps for when values were set
/// - Ability to store migration history
const CREATE_METADATA: &str = r#"
CREATE TABLE IF NOT EXISTS spitedb_metadata (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
"#;

// =============================================================================
// Database Wrapper
// =============================================================================

/// A wrapper around a SQLite connection with SpiteDB schema.
///
/// # Rust Pattern: Newtype Wrapper
///
/// Wrapping `Connection` in our own struct provides:
/// - Type safety: Can't accidentally pass a raw connection where Database is expected
/// - Encapsulation: We control what operations are exposed
/// - Extension point: Can add fields later (e.g., prepared statements cache)
///
/// # Ownership
///
/// `Database` owns its `Connection`. When `Database` is dropped, the connection
/// is closed. This is Rust's RAII (Resource Acquisition Is Initialization) pattern.
///
/// # Rust Pattern: Debug Derive
///
/// We derive `Debug` so the type can be used with `unwrap_err()` and similar
/// methods that require the Ok type to implement Debug. The actual debug output
/// just shows "Database { ... }" since Connection doesn't have useful debug info.
#[derive(Debug)]
pub struct Database {
    /// The underlying SQLite connection.
    ///
    /// We keep this private to control access. All operations go through
    /// Database's methods, ensuring invariants are maintained.
    conn: Connection,
}

impl Database {
    /// Opens a database file, creating and initializing it if necessary.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the SQLite database file. Created if it doesn't exist.
    ///
    /// # Returns
    ///
    /// A `Database` instance with all tables created and schema version verified.
    ///
    /// # Errors
    ///
    /// - `Error::Sqlite` if the file can't be opened or created
    /// - `Error::Schema` if the schema version doesn't match
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use spitedb::Database;
    ///
    /// let db = Database::open("events.db")?;
    /// # Ok::<(), spitedb::Error>(())
    /// ```
    ///
    /// # Rust Pattern: impl AsRef<Path>
    ///
    /// Accepting `impl AsRef<Path>` means callers can pass:
    /// - `&str`: `Database::open("events.db")`
    /// - `String`: `Database::open(path_string)`
    /// - `&Path`: `Database::open(Path::new("events.db"))`
    /// - `PathBuf`: `Database::open(path_buf)`
    ///
    /// This is more ergonomic than requiring a specific type.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        let mut db = Self { conn };
        db.initialize()?;
        Ok(db)
    }

    /// Creates an in-memory database for testing.
    ///
    /// # When to Use
    ///
    /// - Unit tests that don't need persistence
    /// - Integration tests that want isolation
    /// - Benchmarks measuring pure performance (no disk I/O)
    ///
    /// # Note
    ///
    /// In-memory databases are lost when the connection closes. They're faster
    /// but unsuitable for production use.
    ///
    /// # Example
    ///
    /// ```rust
    /// use spitedb::Database;
    ///
    /// let db = Database::open_in_memory()?;
    /// // Use db for testing...
    /// // Automatically cleaned up when db goes out of scope
    /// # Ok::<(), spitedb::Error>(())
    /// ```
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let mut db = Self { conn };
        db.initialize()?;
        Ok(db)
    }

    /// Initializes the database schema.
    ///
    /// This is called automatically by `open` and `open_in_memory`. It:
    /// 1. Creates all tables if they don't exist
    /// 2. Verifies the schema version matches
    /// 3. Sets pragmas for optimal performance
    ///
    /// # Rust Pattern: &mut self
    ///
    /// Taking `&mut self` signals that this method modifies the database.
    /// The borrow checker ensures no other references exist during this call.
    fn initialize(&mut self) -> Result<()> {
        // =====================================================================
        // SQLite Pragmas for Performance
        // =====================================================================
        // These must be set before any other operations.

        // WAL mode: Better concurrency, reads don't block writes
        // Systems concept: WAL = Write-Ahead Logging. Changes are written to a
        // separate log file first, then checkpointed to the main DB later.
        // This allows readers to see a consistent snapshot while writes happen.
        self.conn.execute_batch("PRAGMA journal_mode = WAL")?;

        // Synchronous = NORMAL: Sync WAL on commit, but not every write
        // Trade-off: Slightly less durable (could lose last transaction on OS crash)
        // but significantly faster. For event stores, this is usually acceptable
        // since the client can retry on failure.
        self.conn.execute_batch("PRAGMA synchronous = NORMAL")?;

        // Foreign keys: We don't use FK constraints (for performance), but
        // enable them anyway for safety during development.
        self.conn.execute_batch("PRAGMA foreign_keys = ON")?;

        // =====================================================================
        // Create Tables
        // =====================================================================
        // Using IF NOT EXISTS makes this idempotent - safe to call multiple times.

        self.conn.execute_batch(CREATE_METADATA)?;
        self.conn.execute_batch(CREATE_BATCHES)?;
        self.conn.execute_batch(CREATE_EVENT_INDEX)?;
        self.conn.execute_batch(CREATE_EVENT_STREAM_INDEX)?;
        self.conn.execute_batch(CREATE_STREAM_HEADS)?;
        self.conn.execute_batch(CREATE_STREAM_HEADS_HASH_INDEX)?;
        self.conn.execute_batch(CREATE_COMMANDS)?;
        self.conn.execute_batch(CREATE_TOMBSTONES)?;
        self.conn.execute_batch(CREATE_TOMBSTONES_INDEX)?;

        // =====================================================================
        // Schema Versioning
        // =====================================================================

        self.verify_or_set_version()?;

        Ok(())
    }

    /// Verifies the schema version, or sets it if this is a new database.
    ///
    /// # Logic
    ///
    /// 1. Try to read the schema_version from metadata
    /// 2. If not found, this is a new database - set it
    /// 3. If found but doesn't match, error (migration needed)
    /// 4. If found and matches, we're good
    fn verify_or_set_version(&mut self) -> Result<()> {
        let existing: Option<i32> = self
            .conn
            .query_row(
                "SELECT value FROM spitedb_metadata WHERE key = 'schema_version'",
                [],
                |row| {
                    let s: String = row.get(0)?;
                    // Parse the string to i32, defaulting to 0 if parsing fails
                    Ok(s.parse().unwrap_or(0))
                },
            )
            // query_row returns Err if no rows found; convert to None
            .ok();

        match existing {
            None => {
                // New database - set the version
                self.conn.execute(
                    "INSERT INTO spitedb_metadata (key, value) VALUES ('schema_version', ?)",
                    [SCHEMA_VERSION.to_string()],
                )?;
            }
            Some(v) if v == SCHEMA_VERSION => {
                // Version matches, nothing to do
            }
            Some(v) => {
                // Version mismatch - this is an error for v1 (no migrations)
                return Err(Error::Schema(format!(
                    "schema version mismatch: database has version {v}, but this SpiteDB version requires {SCHEMA_VERSION}"
                )));
            }
        }

        Ok(())
    }

    /// Returns a reference to the underlying SQLite connection.
    ///
    /// # When to Use
    ///
    /// This is primarily for testing and advanced use cases. Normal operations
    /// should use Database's methods instead.
    ///
    /// # Safety Note
    ///
    /// Callers must not modify the schema or violate SpiteDB's invariants.
    /// This method exists for flexibility, not as the primary API.
    #[cfg(test)]
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that in-memory database creation works.
    #[test]
    fn test_open_in_memory() {
        let db = Database::open_in_memory().expect("should create in-memory db");

        // Verify tables exist by querying sqlite_master
        let count: i32 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                [],
                |row| row.get(0),
            )
            .expect("should query tables");

        // We expect 6 tables: metadata, batches, event_index, stream_heads, commands, tombstones
        assert_eq!(count, 6, "expected 6 tables");
    }

    /// Verify that indexes are created.
    #[test]
    fn test_indexes_created() {
        let db = Database::open_in_memory().expect("should create db");

        // Query for our custom indexes (excluding SQLite's auto-created ones)
        let indexes: Vec<String> = {
            let mut stmt = db
                .conn
                .prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%'")
                .expect("should prepare");

            stmt.query_map([], |row| row.get(0))
                .expect("should query")
                .collect::<std::result::Result<Vec<_>, _>>()
                .expect("should collect")
        };

        assert!(
            indexes.contains(&"event_stream_rev".to_string()),
            "should have event_stream_rev index"
        );
        assert!(
            indexes.contains(&"tombstones_stream".to_string()),
            "should have tombstones_stream index"
        );
    }

    /// Verify schema version is stored correctly.
    #[test]
    fn test_schema_version_stored() {
        let db = Database::open_in_memory().expect("should create db");

        let version: String = db
            .conn
            .query_row(
                "SELECT value FROM spitedb_metadata WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .expect("should query version");

        assert_eq!(version, SCHEMA_VERSION.to_string());
    }

    /// Verify that opening the same database twice works (idempotent).
    #[test]
    fn test_double_initialization() {
        // Use a temp file so we can open it twice
        let dir = tempfile::tempdir().expect("should create temp dir");
        let path = dir.path().join("test.db");

        // First open - creates tables
        {
            let _db = Database::open(&path).expect("first open should work");
        }

        // Second open - tables already exist
        {
            let db = Database::open(&path).expect("second open should work");

            // Verify it still works
            let count: i32 = db
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                    [],
                    |row| row.get(0),
                )
                .expect("should query");

            assert_eq!(count, 6);
        }
    }
}
