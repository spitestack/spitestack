//! # Synchronous Storage Layer
//!
//! This module provides the core storage operations for SpiteDB. It wraps a SQLite
//! connection and provides methods for appending events and reading streams.
//!
//! ## Design: Single-Writer Architecture
//!
//! All write operations go through a single `Storage` instance. This enforces the
//! "single logical writer" invariant from CLAUDE.md. The benefits:
//!
//! - No write contention or locking complexity
//! - In-memory cache is always consistent (only one writer updates it)
//! - SQLite performs best with single-writer workloads
//!
//! ## In-Memory Caching
//!
//! Stream heads are cached in memory for fast conflict detection:
//!
//! ```text
//! Client: append to "user-123" with expected_rev=5
//!         │
//!         ▼
//! ┌───────────────────┐
//! │  In-Memory Cache  │  ← O(1) lookup: "user-123" at rev 5? ✓
//! │  HashMap by hash  │
//! └───────────────────┘
//!         │
//!         ▼ (only on cache miss)
//! ┌───────────────────┐
//! │     SQLite        │
//! └───────────────────┘
//! ```
//!
//! ## Invariant: Memory Lags Disk
//!
//! The in-memory cache may be behind the database (e.g., after crash recovery),
//! but it must NEVER be ahead. We update the cache only AFTER successful disk writes.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::{SystemTime, UNIX_EPOCH};

use lru::LruCache;
use rusqlite::{params, Connection};

use crate::error::{Error, Result};
use crate::types::{
    AppendCommand, AppendResult, CollisionSlot, CommandId, Event, EventData, GlobalPos,
    StreamHash, StreamId, StreamRev,
};

// =============================================================================
// Batch Encoding
// =============================================================================

/// Codec identifier for batch compression.
///
/// Currently only uncompressed (0) is supported. Future versions may add:
/// - 1: LZ4
/// - 2: Zstd
const CODEC_NONE: i32 = 0;

/// Cipher identifier for batch encryption.
///
/// Currently only plaintext (0) is supported. Future versions may add:
/// - 1: AES-256-GCM
const CIPHER_NONE: i32 = 0;

// =============================================================================
// Command Cache Configuration
// =============================================================================

/// Maximum number of commands to keep in the LRU cache.
///
/// # Sizing Rationale
///
/// At 100 bytes per entry (command_id + positions + timestamp), 100K entries
/// uses ~10MB of memory. This should cover several hours of high-throughput
/// operation while keeping memory bounded.
///
/// Adjust based on your workload:
/// - High-throughput (10K commands/sec): Consider 500K entries
/// - Low-throughput (100 commands/sec): 10K entries is plenty
const COMMAND_CACHE_MAX_ENTRIES: usize = 100_000;

/// Time-to-live for cached commands in milliseconds (12 hours).
///
/// # Why 12 Hours?
///
/// Client retries typically happen within seconds to minutes. A 12-hour window
/// provides generous margin for:
/// - Network partitions that eventually heal
/// - Client applications that queue commands during outages
/// - Manual retries after investigating issues
///
/// Commands older than this are unlikely to be retried and can be evicted
/// to make room for newer entries.
const COMMAND_CACHE_TTL_MS: u64 = 12 * 60 * 60 * 1000; // 12 hours in milliseconds

// =============================================================================
// Stream Head Cache Entry
// =============================================================================

/// Cached information about a stream's current state.
///
/// We cache this in memory to avoid hitting SQLite for every conflict check.
#[derive(Debug, Clone)]
struct StreamHeadEntry {
    /// The original stream ID string.
    stream_id: StreamId,
    /// Collision slot (usually 0).
    collision_slot: CollisionSlot,
    /// Current revision (number of events in stream).
    last_rev: StreamRev,
    /// Global position of the last event.
    last_pos: GlobalPos,
}

// =============================================================================
// Command Cache Entry
// =============================================================================

/// Cached result of a processed command.
///
/// # Exactly-Once Semantics
///
/// When a client retries a command (same command_id), we return the cached result
/// instead of re-processing. This ensures:
/// - Retries are safe (no duplicate events)
/// - Client gets the same response as the original
/// - Network issues don't cause data inconsistency
///
/// # Time-Based Eviction
///
/// Each entry includes a `created_ms` timestamp. Entries older than
/// `COMMAND_CACHE_TTL_MS` (12 hours) are considered expired and can be evicted.
/// The LRU eviction policy handles capacity limits, while time-based eviction
/// handles entries that are too old to be useful.
#[derive(Debug, Clone)]
struct CommandCacheEntry {
    /// First global position written by this command.
    first_pos: GlobalPos,
    /// Last global position written by this command.
    last_pos: GlobalPos,
    /// First stream revision written.
    first_rev: StreamRev,
    /// Last stream revision written.
    last_rev: StreamRev,
    /// When this command was processed (Unix milliseconds).
    ///
    /// Used for time-based eviction. If `current_time - created_ms > TTL`,
    /// this entry is considered expired and will be evicted on next access.
    created_ms: u64,
}

impl CommandCacheEntry {
    fn to_append_result(&self) -> AppendResult {
        AppendResult::new(self.first_pos, self.last_pos, self.first_rev, self.last_rev)
    }
}

// =============================================================================
// Storage
// =============================================================================

/// The main storage interface for SpiteDB.
///
/// # Ownership
///
/// `Storage` owns its SQLite connection and in-memory caches. When `Storage` is
/// dropped, the connection is closed and caches are cleared.
///
/// # Thread Safety
///
/// `Storage` is NOT thread-safe. It's designed for single-threaded use within
/// the writer actor. For multi-threaded access, wrap in appropriate synchronization
/// or use message passing.
///
/// # Rust Pattern: Interior State
///
/// We use `&mut self` for write operations to signal they modify state.
/// Read operations use `&self` and don't modify the cache (cache misses
/// don't populate - that's done lazily on writes or explicitly on startup).
pub struct Storage {
    /// The underlying SQLite connection.
    conn: Connection,

    /// In-memory cache of stream heads, indexed by stream_hash.
    ///
    /// Maps stream_hash → Vec<StreamHeadEntry> to handle collisions.
    /// In the common case (no collision), the Vec has exactly one entry.
    ///
    /// # Why Vec?
    ///
    /// Hash collisions are rare (~1 in 37M), but when they happen, multiple
    /// streams share the same hash. We store all of them and filter by stream_id.
    stream_heads: HashMap<StreamHash, Vec<StreamHeadEntry>>,

    /// In-memory LRU cache of processed commands for idempotency.
    ///
    /// Maps command_id → result. When a duplicate command is received,
    /// we return the cached result instead of re-processing.
    ///
    /// # Cache Strategy: LRU + Time-Based Eviction
    ///
    /// This cache uses two eviction strategies:
    ///
    /// 1. **LRU (Least Recently Used)**: When the cache reaches `COMMAND_CACHE_MAX_ENTRIES`,
    ///    the least recently accessed entry is evicted to make room.
    ///
    /// 2. **Time-based**: Entries older than `COMMAND_CACHE_TTL_MS` (12 hours) are
    ///    considered expired and evicted on access.
    ///
    /// # Why Both?
    ///
    /// - LRU alone might keep very old entries if the cache isn't full
    /// - Time-based alone might evict frequently-used entries
    /// - Together they provide bounded memory AND bounded staleness
    ///
    /// # After Eviction
    ///
    /// If an entry is evicted and the command is retried after 12+ hours,
    /// we'll process it as a new command. This is acceptable because:
    /// - 12 hours is far beyond typical retry windows (seconds to minutes)
    /// - Clients retrying after 12h likely have stale state anyway
    /// - The alternative (DB lookup for every command) is too expensive
    ///
    /// The commands table remains the durable record for auditing and
    /// potential future recovery scenarios, but we don't query it on
    /// every operation.
    commands: LruCache<String, CommandCacheEntry>,

    /// The next global position to assign.
    ///
    /// This is always MAX(global_pos) + 1 from the database.
    /// Invariant: This value is only updated AFTER successful disk writes.
    next_global_pos: GlobalPos,
}

impl Storage {
    /// Opens a storage instance from an existing database connection.
    ///
    /// # Initialization
    ///
    /// This loads the stream heads cache and determines the next global position
    /// by scanning the database. For large databases, this may take a moment.
    ///
    /// # Arguments
    ///
    /// * `conn` - An initialized SQLite connection (schema already created)
    pub fn new(conn: Connection) -> Result<Self> {
        // NonZeroUsize is required by LruCache to ensure capacity > 0.
        // This is a Rust pattern: using the type system to prevent invalid states.
        let cache_capacity = NonZeroUsize::new(COMMAND_CACHE_MAX_ENTRIES)
            .expect("COMMAND_CACHE_MAX_ENTRIES must be > 0");

        let mut storage = Self {
            conn,
            stream_heads: HashMap::new(),
            commands: LruCache::new(cache_capacity),
            next_global_pos: GlobalPos::FIRST,
        };

        storage.load_stream_heads()?;
        storage.load_commands()?;
        storage.load_next_global_pos()?;

        Ok(storage)
    }

    /// Loads all stream heads from the database into the in-memory cache.
    ///
    /// Called once during initialization. After this, the cache is updated
    /// incrementally as events are appended.
    fn load_stream_heads(&mut self) -> Result<()> {
        let mut stmt = self.conn.prepare(
            "SELECT stream_id, stream_hash, collision_slot, last_rev, last_pos
             FROM stream_heads"
        )?;

        let entries = stmt.query_map([], |row| {
            let stream_id: String = row.get(0)?;
            let _stream_hash: i64 = row.get(1)?;
            let collision_slot: i64 = row.get(2)?;
            let last_rev: i64 = row.get(3)?;
            let last_pos: i64 = row.get(4)?;

            Ok(StreamHeadEntry {
                stream_id: StreamId::new(stream_id),
                collision_slot: CollisionSlot::from_raw(collision_slot as u16),
                last_rev: StreamRev::from_raw(last_rev as u64),
                last_pos: GlobalPos::from_raw_unchecked(last_pos as u64),
            })
        })?;

        for entry in entries {
            let entry = entry?;
            let hash = entry.stream_id.hash();
            self.stream_heads
                .entry(hash)
                .or_insert_with(Vec::new)
                .push(entry);
        }

        Ok(())
    }

    /// Loads recent commands from the database into the in-memory LRU cache.
    ///
    /// # Only Recent Commands
    ///
    /// We only load commands within the TTL window (12 hours). Older commands
    /// are unlikely to be retried and would just waste cache space.
    ///
    /// # Startup Recovery
    ///
    /// This is critical for crash recovery: if we crash after committing events
    /// but before responding to the client, the client will retry. Loading recent
    /// commands ensures we return the cached result instead of creating duplicates.
    fn load_commands(&mut self) -> Result<()> {
        let now_ms = current_time_ms();
        let cutoff_ms = now_ms.saturating_sub(COMMAND_CACHE_TTL_MS);

        // Load commands created within the TTL window
        // We join with event_index to get stream revisions for the result
        let mut stmt = self.conn.prepare(
            "SELECT c.command_id, c.first_pos, c.last_pos, c.created_ms,
                    e_first.stream_rev, e_last.stream_rev
             FROM commands c
             JOIN event_index e_first ON e_first.global_pos = c.first_pos
             JOIN event_index e_last ON e_last.global_pos = c.last_pos
             WHERE c.created_ms >= ?
             ORDER BY c.created_ms DESC"
        )?;

        let entries = stmt.query_map([cutoff_ms as i64], |row| {
            let command_id: String = row.get(0)?;
            let first_pos: i64 = row.get(1)?;
            let last_pos: i64 = row.get(2)?;
            let created_ms: i64 = row.get(3)?;
            let first_rev: i64 = row.get(4)?;
            let last_rev: i64 = row.get(5)?;

            Ok((command_id, first_pos, last_pos, created_ms, first_rev, last_rev))
        })?;

        for entry in entries {
            let (command_id, first_pos, last_pos, created_ms, first_rev, last_rev) = entry?;
            // LruCache::put() inserts and returns the old value if key existed
            self.commands.put(
                command_id,
                CommandCacheEntry {
                    first_pos: GlobalPos::from_raw_unchecked(first_pos as u64),
                    last_pos: GlobalPos::from_raw_unchecked(last_pos as u64),
                    first_rev: StreamRev::from_raw(first_rev as u64),
                    last_rev: StreamRev::from_raw(last_rev as u64),
                    created_ms: created_ms as u64,
                },
            );
        }

        Ok(())
    }

    /// Determines the next global position by finding MAX(global_pos) + 1.
    fn load_next_global_pos(&mut self) -> Result<()> {
        let max_pos: Option<i64> = self.conn.query_row(
            "SELECT MAX(global_pos) FROM event_index",
            [],
            |row| row.get(0),
        )?;

        self.next_global_pos = match max_pos {
            Some(pos) if pos > 0 => GlobalPos::from_raw_unchecked(pos as u64 + 1),
            _ => GlobalPos::FIRST,
        };

        Ok(())
    }

    /// Returns the current next global position (for testing/debugging).
    pub fn next_global_pos(&self) -> GlobalPos {
        self.next_global_pos
    }

    // =========================================================================
    // Stream Head Lookups
    // =========================================================================

    /// Looks up a stream's current state by stream ID.
    ///
    /// Returns `None` if the stream doesn't exist yet.
    fn get_stream_head(&self, stream_id: &StreamId) -> Option<&StreamHeadEntry> {
        let hash = stream_id.hash();
        self.stream_heads
            .get(&hash)
            .and_then(|entries| entries.iter().find(|e| &e.stream_id == stream_id))
    }

    /// Finds or assigns a collision slot for a stream.
    ///
    /// - If stream exists: returns its existing slot
    /// - If stream is new with no collision: returns slot 0
    /// - If stream is new with collision: returns next available slot
    fn get_or_assign_collision_slot(&self, stream_id: &StreamId) -> CollisionSlot {
        let hash = stream_id.hash();

        match self.stream_heads.get(&hash) {
            None => CollisionSlot::FIRST,
            Some(entries) => {
                // Check if this exact stream_id already exists
                if let Some(entry) = entries.iter().find(|e| &e.stream_id == stream_id) {
                    return entry.collision_slot;
                }
                // New stream with hash collision - assign next slot
                let max_slot = entries
                    .iter()
                    .map(|e| e.collision_slot.as_raw())
                    .max()
                    .unwrap_or(0);
                CollisionSlot::from_raw(max_slot + 1)
            }
        }
    }

    // =========================================================================
    // Append Operations
    // =========================================================================

    /// Appends events to a stream.
    ///
    /// # Conflict Detection
    ///
    /// Uses optimistic concurrency control via `expected_rev`:
    /// - If the stream's current revision matches `expected_rev`, append succeeds
    /// - If it doesn't match, returns `Error::Conflict`
    /// - For new streams, use `StreamRev::NONE` (0) as expected_rev
    ///
    /// # Idempotency (Exactly-Once Semantics)
    ///
    /// If `command_id` was already processed, returns the **same result** as the
    /// original call. This enables safe client retries - the client can retry
    /// after a timeout without risking duplicate events.
    ///
    /// # Atomicity
    ///
    /// The entire append is wrapped in a transaction. Either all events are
    /// written, or none are (on conflict or error).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let cmd = AppendCommand::new(
    ///     "cmd-123",
    ///     "user-456",
    ///     StreamRev::NONE,  // Expect stream doesn't exist
    ///     vec![EventData::new(b"first event".to_vec())],
    /// );
    /// let result = storage.append(cmd)?;
    /// println!("Wrote events at positions {} to {}", result.first_pos, result.last_pos);
    /// ```
    pub fn append(&mut self, cmd: AppendCommand) -> Result<AppendResult> {
        // Check for duplicate command first (before starting transaction)
        // If duplicate, return the cached result (exactly-once semantics)
        if let Some(cached_result) = self.get_cached_command_result(&cmd.command_id) {
            return Ok(cached_result);
        }

        // Check conflict using in-memory cache
        let current_rev = self
            .get_stream_head(&cmd.stream_id)
            .map(|h| h.last_rev)
            .unwrap_or(StreamRev::NONE);

        if current_rev != cmd.expected_rev {
            return Err(Error::Conflict {
                stream_id: cmd.stream_id.to_string(),
                expected: cmd.expected_rev.as_raw(),
                actual: current_rev.as_raw(),
            });
        }

        // Get or assign collision slot
        let collision_slot = self.get_or_assign_collision_slot(&cmd.stream_id);
        let stream_hash = cmd.stream_id.hash();

        // Calculate positions
        let first_pos = self.next_global_pos;
        let event_count = cmd.events.len() as u64;
        let last_pos = first_pos.add(event_count - 1);

        let first_rev = current_rev.next();
        let last_rev = first_rev.add(event_count - 1);

        let now_ms = current_time_ms();

        // Encode events into a batch blob
        let (batch_data, event_offsets) = encode_batch(&cmd.events, &cmd.stream_id, first_rev, now_ms);
        let checksum = compute_checksum(&batch_data);

        // Execute in a transaction
        let tx = self.conn.transaction()?;

        // Insert batch
        tx.execute(
            "INSERT INTO batches (base_pos, event_count, created_ms, codec, cipher, checksum, data)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                first_pos.as_raw() as i64,
                event_count as i64,
                now_ms as i64,
                CODEC_NONE,
                CIPHER_NONE,
                checksum.as_slice(),
                batch_data.as_slice(),
            ],
        )?;

        let batch_id = tx.last_insert_rowid();

        // Insert event index entries
        let mut rev = first_rev;
        let mut pos = first_pos;
        for (offset, len) in &event_offsets {
            tx.execute(
                "INSERT INTO event_index (global_pos, batch_id, byte_offset, byte_len, stream_hash, collision_slot, stream_rev)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![
                    pos.as_raw() as i64,
                    batch_id,
                    *offset as i64,
                    *len as i64,
                    stream_hash.as_raw(),
                    collision_slot.as_raw() as i64,
                    rev.as_raw() as i64,
                ],
            )?;
            pos = pos.next();
            rev = rev.next();
        }

        // Upsert stream head
        tx.execute(
            "INSERT INTO stream_heads (stream_id, stream_hash, collision_slot, last_rev, last_pos)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(stream_id) DO UPDATE SET
                 last_rev = excluded.last_rev,
                 last_pos = excluded.last_pos",
            params![
                cmd.stream_id.as_str(),
                stream_hash.as_raw(),
                collision_slot.as_raw() as i64,
                last_rev.as_raw() as i64,
                last_pos.as_raw() as i64,
            ],
        )?;

        // Record command for idempotency
        tx.execute(
            "INSERT INTO commands (command_id, stream_hash, first_pos, last_pos, created_ms)
             VALUES (?, ?, ?, ?, ?)",
            params![
                cmd.command_id.as_str(),
                stream_hash.as_raw(),
                first_pos.as_raw() as i64,
                last_pos.as_raw() as i64,
                now_ms as i64,
            ],
        )?;

        tx.commit()?;

        // Update in-memory caches AFTER successful commit
        // Invariant: Memory never leads disk
        self.update_stream_head_cache(
            cmd.stream_id.clone(),
            stream_hash,
            collision_slot,
            last_rev,
            last_pos,
        );

        // Cache the command result for exactly-once semantics
        // LruCache::put() inserts the entry and returns any evicted value
        self.commands.put(
            cmd.command_id.to_string(),
            CommandCacheEntry {
                first_pos,
                last_pos,
                first_rev,
                last_rev,
                created_ms: now_ms,
            },
        );

        self.next_global_pos = last_pos.next();

        Ok(AppendResult::new(first_pos, last_pos, first_rev, last_rev))
    }

    /// Looks up a cached command result for idempotency.
    ///
    /// # Returns
    ///
    /// - `Some(result)` if the command was already processed and is not expired
    /// - `None` if this is a new command or the cached entry has expired
    ///
    /// # Time-Based Expiry
    ///
    /// Even if an entry exists in the cache, we check its timestamp. Entries
    /// older than `COMMAND_CACHE_TTL_MS` (12 hours) are treated as expired.
    /// This prevents the cache from serving very stale entries that might
    /// have been accessed recently but created long ago.
    ///
    /// # Why &mut self?
    ///
    /// LruCache::get() updates the access order (moves item to front),
    /// which requires mutable access. This is the LRU "promotion" behavior.
    fn get_cached_command_result(&mut self, command_id: &CommandId) -> Option<AppendResult> {
        let now_ms = current_time_ms();

        // LruCache::get() returns Option<&V> and promotes the entry
        if let Some(entry) = self.commands.get(command_id.as_str()) {
            // Check time-based expiry
            let age_ms = now_ms.saturating_sub(entry.created_ms);
            if age_ms <= COMMAND_CACHE_TTL_MS {
                return Some(entry.to_append_result());
            }
            // Entry is expired - we'll treat it as a cache miss
            // The LRU will eventually evict it, or we could explicitly pop it
        }

        // Not in cache (or expired) - this is likely a new command
        // We intentionally do NOT fall back to the database here because:
        // - 99.99% of cache misses are new commands (no DB entry exists)
        // - DB lookup on every new command would be expensive
        // - Commands older than 12h being retried is extremely rare
        None
    }

    /// Updates the in-memory stream head cache.
    fn update_stream_head_cache(
        &mut self,
        stream_id: StreamId,
        stream_hash: StreamHash,
        collision_slot: CollisionSlot,
        last_rev: StreamRev,
        last_pos: GlobalPos,
    ) {
        let entries = self.stream_heads.entry(stream_hash).or_insert_with(Vec::new);

        // Find existing entry or add new one
        if let Some(entry) = entries.iter_mut().find(|e| e.stream_id == stream_id) {
            entry.last_rev = last_rev;
            entry.last_pos = last_pos;
        } else {
            entries.push(StreamHeadEntry {
                stream_id,
                collision_slot,
                last_rev,
                last_pos,
            });
        }
    }

    // =========================================================================
    // Read Operations
    // =========================================================================

    /// Reads events from a stream.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The stream to read from
    /// * `from_rev` - Starting revision (inclusive), use `StreamRev::FIRST` for beginning
    /// * `limit` - Maximum number of events to return
    ///
    /// # Returns
    ///
    /// Events in revision order. Empty vec if stream doesn't exist.
    pub fn read_stream(
        &self,
        stream_id: &StreamId,
        from_rev: StreamRev,
        limit: usize,
    ) -> Result<Vec<Event>> {
        let stream_hash = stream_id.hash();

        // Get collision slot (needed for query)
        let collision_slot = self
            .get_stream_head(stream_id)
            .map(|h| h.collision_slot)
            .unwrap_or(CollisionSlot::FIRST);

        let mut stmt = self.conn.prepare(
            "SELECT e.global_pos, e.batch_id, e.byte_offset, e.byte_len, e.stream_rev,
                    b.data
             FROM event_index e
             JOIN batches b ON e.batch_id = b.batch_id
             WHERE e.stream_hash = ? AND e.collision_slot = ? AND e.stream_rev >= ?
             ORDER BY e.stream_rev
             LIMIT ?"
        )?;

        let events = stmt.query_map(
            params![
                stream_hash.as_raw(),
                collision_slot.as_raw() as i64,
                from_rev.as_raw() as i64,
                limit as i64,
            ],
            |row| {
                let global_pos: i64 = row.get(0)?;
                let _batch_id: i64 = row.get(1)?;
                let byte_offset: i64 = row.get(2)?;
                let byte_len: i64 = row.get(3)?;
                let stream_rev: i64 = row.get(4)?;
                let batch_data: Vec<u8> = row.get(5)?;

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

    /// Reads events from the global log.
    ///
    /// # Arguments
    ///
    /// * `from_pos` - Starting position (inclusive)
    /// * `limit` - Maximum number of events to return
    ///
    /// # Returns
    ///
    /// Events in global position order.
    pub fn read_global(&self, from_pos: GlobalPos, limit: usize) -> Result<Vec<Event>> {
        let mut stmt = self.conn.prepare(
            "SELECT e.global_pos, e.byte_offset, e.byte_len, e.stream_hash, e.collision_slot, e.stream_rev,
                    b.data
             FROM event_index e
             JOIN batches b ON e.batch_id = b.batch_id
             WHERE e.global_pos >= ?
             ORDER BY e.global_pos
             LIMIT ?"
        )?;

        let events = stmt.query_map(
            params![from_pos.as_raw() as i64, limit as i64],
            |row| {
                let global_pos: i64 = row.get(0)?;
                let byte_offset: i64 = row.get(1)?;
                let byte_len: i64 = row.get(2)?;
                let stream_hash: i64 = row.get(3)?;
                let _collision_slot: i64 = row.get(4)?;
                let stream_rev: i64 = row.get(5)?;
                let batch_data: Vec<u8> = row.get(6)?;

                Ok((global_pos, byte_offset, byte_len, stream_hash, stream_rev, batch_data))
            },
        )?;

        let mut result = Vec::new();
        for event_data in events {
            let (global_pos, byte_offset, byte_len, _stream_hash, stream_rev, batch_data) = event_data?;

            // For global reads, we need to decode stream_id from the blob
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

    /// Gets the current revision of a stream.
    ///
    /// Returns `StreamRev::NONE` if the stream doesn't exist.
    pub fn get_stream_revision(&self, stream_id: &StreamId) -> StreamRev {
        self.get_stream_head(stream_id)
            .map(|h| h.last_rev)
            .unwrap_or(StreamRev::NONE)
    }
}

// =============================================================================
// Batch Encoding/Decoding
// =============================================================================
//
// Batch format (v1, uncompressed, unencrypted):
//
// For each event:
//   [stream_id_len: u32][stream_id: bytes]
//   [event_type_len: u32][event_type: bytes]  (0 if none)
//   [timestamp_ms: u64]
//   [metadata_len: u32][metadata: bytes]  (0 if none)
//   [data_len: u32][data: bytes]

/// Encodes events into a batch blob.
///
/// Returns the blob data and a vec of (offset, length) for each event.
fn encode_batch(
    events: &[EventData],
    stream_id: &StreamId,
    _first_rev: StreamRev,
    timestamp_ms: u64,
) -> (Vec<u8>, Vec<(usize, usize)>) {
    let mut data = Vec::new();
    let mut offsets = Vec::new();

    let stream_id_bytes = stream_id.as_str().as_bytes();

    for (_i, event) in events.iter().enumerate() {
        let start_offset = data.len();

        // Stream ID
        data.extend_from_slice(&(stream_id_bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(stream_id_bytes);

        // Event type
        if let Some(ref event_type) = event.event_type {
            let type_bytes = event_type.as_bytes();
            data.extend_from_slice(&(type_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(type_bytes);
        } else {
            data.extend_from_slice(&0u32.to_le_bytes());
        }

        // Timestamp
        data.extend_from_slice(&timestamp_ms.to_le_bytes());

        // Metadata
        if let Some(ref metadata) = event.metadata {
            data.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
            data.extend_from_slice(metadata);
        } else {
            data.extend_from_slice(&0u32.to_le_bytes());
        }

        // Data
        data.extend_from_slice(&(event.data.len() as u32).to_le_bytes());
        data.extend_from_slice(&event.data);

        let end_offset = data.len();
        offsets.push((start_offset, end_offset - start_offset));
    }

    (data, offsets)
}

/// Decodes a single event from a batch blob (when stream_id is known).
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

    // Skip stream_id (we already know it)
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

/// Decodes a single event from a batch blob, including stream_id.
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

/// Reads a little-endian u32 and advances the cursor.
fn read_u32(data: &[u8], cursor: &mut usize) -> u32 {
    let bytes: [u8; 4] = data[*cursor..*cursor + 4].try_into().unwrap();
    *cursor += 4;
    u32::from_le_bytes(bytes)
}

/// Reads a little-endian u64 and advances the cursor.
fn read_u64(data: &[u8], cursor: &mut usize) -> u64 {
    let bytes: [u8; 8] = data[*cursor..*cursor + 8].try_into().unwrap();
    *cursor += 8;
    u64::from_le_bytes(bytes)
}

/// Computes a checksum for batch data.
///
/// Currently uses XXH3-64 for consistency with stream hashing.
fn compute_checksum(data: &[u8]) -> Vec<u8> {
    let hash = xxhash_rust::xxh3::xxh3_64(data);
    hash.to_le_bytes().to_vec()
}

/// Returns the current time in milliseconds since Unix epoch.
fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_storage() -> Storage {
        // Create a fresh in-memory connection and initialize schema
        // (In production, we'd reuse Database, but tests need isolated connections)
        let conn = Connection::open_in_memory().unwrap();

        // Initialize schema
        conn.execute_batch("PRAGMA journal_mode = WAL").unwrap();
        conn.execute_batch("PRAGMA synchronous = NORMAL").unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON").unwrap();

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS spitedb_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )"
        ).unwrap();

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS batches (
                batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                base_pos INTEGER NOT NULL,
                event_count INTEGER NOT NULL,
                created_ms INTEGER NOT NULL,
                codec INTEGER NOT NULL DEFAULT 0,
                cipher INTEGER NOT NULL DEFAULT 0,
                nonce BLOB,
                checksum BLOB NOT NULL,
                data BLOB NOT NULL
            )"
        ).unwrap();

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS event_index (
                global_pos INTEGER PRIMARY KEY,
                batch_id INTEGER NOT NULL,
                byte_offset INTEGER NOT NULL,
                byte_len INTEGER NOT NULL,
                stream_hash INTEGER NOT NULL,
                collision_slot INTEGER NOT NULL DEFAULT 0,
                stream_rev INTEGER NOT NULL
            )"
        ).unwrap();

        conn.execute_batch(
            "CREATE UNIQUE INDEX IF NOT EXISTS event_stream_rev
             ON event_index(stream_hash, collision_slot, stream_rev)"
        ).unwrap();

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS stream_heads (
                stream_id TEXT PRIMARY KEY,
                stream_hash INTEGER NOT NULL,
                collision_slot INTEGER NOT NULL DEFAULT 0,
                last_rev INTEGER NOT NULL,
                last_pos INTEGER NOT NULL
            )"
        ).unwrap();

        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS stream_heads_hash
             ON stream_heads(stream_hash)"
        ).unwrap();

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS commands (
                command_id TEXT PRIMARY KEY,
                stream_hash INTEGER NOT NULL,
                first_pos INTEGER NOT NULL,
                last_pos INTEGER NOT NULL,
                created_ms INTEGER NOT NULL
            )"
        ).unwrap();

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tombstones (
                stream_hash INTEGER NOT NULL,
                from_rev INTEGER NOT NULL,
                to_rev INTEGER NOT NULL,
                deleted_ms INTEGER NOT NULL
            )"
        ).unwrap();

        Storage::new(conn).unwrap()
    }

    #[test]
    fn test_append_single_event() {
        let mut storage = create_test_storage();

        let cmd = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"hello world".to_vec())],
        );

        let result = storage.append(cmd).unwrap();

        assert_eq!(result.first_pos, GlobalPos::FIRST);
        assert_eq!(result.last_pos, GlobalPos::FIRST);
        assert_eq!(result.first_rev, StreamRev::FIRST);
        assert_eq!(result.last_rev, StreamRev::FIRST);
    }

    #[test]
    fn test_append_multiple_events() {
        let mut storage = create_test_storage();

        let cmd = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![
                EventData::new(b"event 1".to_vec()),
                EventData::new(b"event 2".to_vec()),
                EventData::new(b"event 3".to_vec()),
            ],
        );

        let result = storage.append(cmd).unwrap();

        assert_eq!(result.first_pos.as_raw(), 1);
        assert_eq!(result.last_pos.as_raw(), 3);
        assert_eq!(result.first_rev.as_raw(), 1);
        assert_eq!(result.last_rev.as_raw(), 3);
        assert_eq!(result.event_count(), 3);
    }

    #[test]
    fn test_append_to_existing_stream() {
        let mut storage = create_test_storage();

        // First append
        let cmd1 = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"first".to_vec())],
        );
        let result1 = storage.append(cmd1).unwrap();

        // Second append
        let cmd2 = AppendCommand::new(
            "cmd-2",
            "stream-1",
            result1.last_rev,
            vec![EventData::new(b"second".to_vec())],
        );
        let result2 = storage.append(cmd2).unwrap();

        assert_eq!(result2.first_rev.as_raw(), 2);
        assert_eq!(result2.first_pos.as_raw(), 2);
    }

    #[test]
    fn test_conflict_detection() {
        let mut storage = create_test_storage();

        // First append
        let cmd1 = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"first".to_vec())],
        );
        storage.append(cmd1).unwrap();

        // Conflicting append (wrong expected_rev)
        let cmd2 = AppendCommand::new(
            "cmd-2",
            "stream-1",
            StreamRev::NONE, // Should be rev 1, not NONE
            vec![EventData::new(b"conflict".to_vec())],
        );

        let result = storage.append(cmd2);
        assert!(matches!(result, Err(Error::Conflict { .. })));
    }

    #[test]
    fn test_duplicate_command_returns_same_result() {
        let mut storage = create_test_storage();

        let cmd = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"hello".to_vec())],
        );

        // First time succeeds
        let result1 = storage.append(cmd.clone()).unwrap();

        // Second time returns the SAME result (exactly-once semantics)
        let result2 = storage.append(cmd).unwrap();

        // Results should be identical
        assert_eq!(result1.first_pos.as_raw(), result2.first_pos.as_raw());
        assert_eq!(result1.last_pos.as_raw(), result2.last_pos.as_raw());
        assert_eq!(result1.first_rev.as_raw(), result2.first_rev.as_raw());
        assert_eq!(result1.last_rev.as_raw(), result2.last_rev.as_raw());

        // Only one event should exist (not two)
        let events = storage.read_stream(&StreamId::new("stream-1"), StreamRev::FIRST, 100).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_retry_after_simulated_timeout() {
        let mut storage = create_test_storage();

        // Simulate: client sends command, gets response, but response is lost
        let cmd = AppendCommand::new(
            "cmd-retry-test",
            "stream-1",
            StreamRev::NONE,
            vec![
                EventData::with_type("Created", b"event 1".to_vec()),
                EventData::with_type("Updated", b"event 2".to_vec()),
            ],
        );

        // Original request succeeds (but pretend client didn't get response)
        let original_result = storage.append(cmd.clone()).unwrap();
        assert_eq!(original_result.event_count(), 2);

        // Client retries with same command_id
        let retry_result = storage.append(cmd).unwrap();

        // Should get exact same result
        assert_eq!(original_result.first_pos.as_raw(), retry_result.first_pos.as_raw());
        assert_eq!(original_result.last_pos.as_raw(), retry_result.last_pos.as_raw());

        // Database should have exactly 2 events, not 4
        let events = storage.read_global(GlobalPos::FIRST, 100).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_read_stream() {
        let mut storage = create_test_storage();

        let cmd = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![
                EventData::with_type("TypeA", b"event 1".to_vec()),
                EventData::with_type("TypeB", b"event 2".to_vec()),
            ],
        );
        storage.append(cmd).unwrap();

        let events = storage
            .read_stream(&StreamId::new("stream-1"), StreamRev::FIRST, 100)
            .unwrap();

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data, b"event 1");
        assert_eq!(events[0].event_type, Some("TypeA".to_string()));
        assert_eq!(events[0].stream_rev.as_raw(), 1);
        assert_eq!(events[1].data, b"event 2");
        assert_eq!(events[1].stream_rev.as_raw(), 2);
    }

    #[test]
    fn test_read_global() {
        let mut storage = create_test_storage();

        // Append to two different streams
        let cmd1 = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"stream1-event1".to_vec())],
        );
        storage.append(cmd1).unwrap();

        let cmd2 = AppendCommand::new(
            "cmd-2",
            "stream-2",
            StreamRev::NONE,
            vec![EventData::new(b"stream2-event1".to_vec())],
        );
        storage.append(cmd2).unwrap();

        let events = storage.read_global(GlobalPos::FIRST, 100).unwrap();

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].stream_id.as_str(), "stream-1");
        assert_eq!(events[0].global_pos.as_raw(), 1);
        assert_eq!(events[1].stream_id.as_str(), "stream-2");
        assert_eq!(events[1].global_pos.as_raw(), 2);
    }

    #[test]
    fn test_get_stream_revision() {
        let mut storage = create_test_storage();

        let stream_id = StreamId::new("stream-1");

        // Non-existent stream
        assert_eq!(storage.get_stream_revision(&stream_id), StreamRev::NONE);

        // After append
        let cmd = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![
                EventData::new(b"e1".to_vec()),
                EventData::new(b"e2".to_vec()),
            ],
        );
        storage.append(cmd).unwrap();

        assert_eq!(storage.get_stream_revision(&stream_id).as_raw(), 2);
    }

    #[test]
    fn test_multiple_streams() {
        let mut storage = create_test_storage();

        // Append to multiple streams
        for i in 0..10 {
            let cmd = AppendCommand::new(
                format!("cmd-{}", i),
                format!("stream-{}", i),
                StreamRev::NONE,
                vec![EventData::new(format!("event-{}", i).into_bytes())],
            );
            storage.append(cmd).unwrap();
        }

        // Verify global positions
        assert_eq!(storage.next_global_pos().as_raw(), 11);

        // Verify each stream
        for i in 0..10 {
            let stream_id = StreamId::new(format!("stream-{}", i));
            assert_eq!(storage.get_stream_revision(&stream_id).as_raw(), 1);
        }
    }
}
