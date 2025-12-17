//! # Batch Writer with Group Commit
//!
//! This module implements the batched write strategy for high-throughput event storage.
//! It collects multiple append commands over a time window and executes them in a single
//! SQLite transaction using SAVEPOINTs for per-command isolation.
//!
//! ## Why Group Commit?
//!
//! SQLite's performance is limited by fsync() calls - each transaction commit requires
//! waiting for the disk to confirm the write. By batching multiple commands into one
//! transaction, we amortize this cost:
//!
//! ```text
//! Without batching:                With batching:
//! ┌──────────────────────────┐     ┌──────────────────────────┐
//! │ cmd1 → BEGIN → COMMIT    │     │ BEGIN                    │
//! │ cmd2 → BEGIN → COMMIT    │     │   SAVEPOINT cmd1         │
//! │ cmd3 → BEGIN → COMMIT    │     │   SAVEPOINT cmd2         │
//! │ ...                      │     │   SAVEPOINT cmd3         │
//! │ 100 fsyncs               │     │ COMMIT                   │
//! └──────────────────────────┘     │ 1 fsync                  │
//!                                  └──────────────────────────┘
//! ```
//!
//! ## SAVEPOINT Semantics
//!
//! Each command executes in its own SAVEPOINT within the outer transaction:
//!
//! - If a command succeeds: SAVEPOINT is released (changes preserved)
//! - If a command fails (conflict): SAVEPOINT is rolled back (changes discarded)
//! - Other commands in the batch are unaffected
//!
//! This provides isolation: a conflict in cmd2 doesn't affect cmd1 or cmd3.
//!
//! ## Staged vs Committed State
//!
//! During a batch, we maintain two layers of in-memory state:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     In-Memory State                          │
//! │                                                              │
//! │  ┌─────────────────────┐    ┌─────────────────────────────┐│
//! │  │   heads_committed   │    │       heads_staged          ││
//! │  │   (mirrors disk)    │    │  (batch-local changes)      ││
//! │  └─────────────────────┘    └─────────────────────────────┘│
//! │                                                              │
//! │  Conflict check: staged → committed → (disk if miss)        │
//! │                                                              │
//! │  After COMMIT: staged → committed, clear staged             │
//! │  After ROLLBACK: discard staged                             │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Transaction API
//!
//! The `Transaction` type guarantees that multiple append commands will be
//! executed in the same batch. This is useful when you need atomicity across
//! multiple streams:
//!
//! ```rust,ignore
//! let tx = writer.begin_transaction();
//! tx.append(cmd1)?;  // To stream A
//! tx.append(cmd2)?;  // To stream B
//! tx.submit().await?;  // Both in same batch
//! ```
//!
//! ## Invariants
//!
//! - Memory never leads disk (staged/committed separation)
//! - Global position strictly increases, no gaps
//! - Stream revisions strictly increase per stream, no gaps
//! - Commands are processed in order within a batch
//! - SAVEPOINTs provide per-command isolation

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use lru::LruCache;
use rusqlite::{params, Connection};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::codec::{compute_checksum, current_time_ms, encode_batch};
use crate::crypto::{BatchCryptor, CIPHER_AES256GCM, CODEC_ZSTD_L1};
use crate::error::{Error, Result};
use crate::types::{
    AppendCommand, AppendResult, CollisionSlot, CommandCacheEntry, CommandId,
    DeleteStreamCommand, DeleteStreamResult, DeleteTenantCommand, DeleteTenantResult, GlobalPos,
    StreamHash, StreamHeadEntry, StreamId, StreamRev, TenantHash, COMMAND_CACHE_MAX_ENTRIES,
    COMMAND_CACHE_TTL_MS,
};

// =============================================================================
// Configuration
// =============================================================================

/// Default batch timeout in milliseconds.
///
/// Commands are collected for up to this duration before being executed.
/// Shorter = lower latency, longer = higher throughput.
pub const DEFAULT_BATCH_TIMEOUT_MS: u64 = 10;

/// Maximum commands per batch.
///
/// If this many commands accumulate before the timeout, execute immediately.
pub const DEFAULT_BATCH_MAX_SIZE: usize = 1000;

/// Size of the command channel.
const COMMAND_CHANNEL_SIZE: usize = 4096;

// =============================================================================
// Writer Configuration
// =============================================================================

/// Configuration for the batch writer.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Maximum time to wait for commands before executing a batch.
    pub batch_timeout: Duration,

    /// Maximum commands per batch.
    pub batch_max_size: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            batch_timeout: Duration::from_millis(DEFAULT_BATCH_TIMEOUT_MS),
            batch_max_size: DEFAULT_BATCH_MAX_SIZE,
        }
    }
}

// =============================================================================
// Request Types
// =============================================================================

/// A write request sent to the batch writer.
pub enum WriteRequest {
    /// Single append command.
    Append {
        command: AppendCommand,
        response: oneshot::Sender<Result<AppendResult>>,
    },

    /// Transaction containing multiple commands (guaranteed same batch).
    Transaction {
        commands: Vec<AppendCommand>,
        response: oneshot::Sender<Result<Vec<Result<AppendResult>>>>,
    },

    /// Delete events from a stream (create tombstone).
    DeleteStream {
        command: DeleteStreamCommand,
        response: oneshot::Sender<Result<DeleteStreamResult>>,
    },

    /// Delete all events for a tenant.
    DeleteTenant {
        command: DeleteTenantCommand,
        response: oneshot::Sender<Result<DeleteTenantResult>>,
    },

    /// Shutdown the writer.
    Shutdown,
}

/// Internal representation of a pending command.
struct PendingCommand {
    command: AppendCommand,
    response: oneshot::Sender<Result<AppendResult>>,
}

/// Internal representation of a pending transaction.
struct PendingTransaction {
    commands: Vec<AppendCommand>,
    response: oneshot::Sender<Result<Vec<Result<AppendResult>>>>,
}

/// Batch item - either a single command or part of a transaction.
enum BatchItem {
    Single(PendingCommand),
    Transaction(PendingTransaction),
}

/// Command extracted from batch item (without response channel).
enum BatchCommand {
    Single(AppendCommand),
    Transaction(Vec<AppendCommand>),
}

/// Response channel extracted from batch item.
enum BatchResponse {
    Single(oneshot::Sender<Result<AppendResult>>),
    Transaction(oneshot::Sender<Result<Vec<Result<AppendResult>>>>),
}

/// Result for a batch item.
enum BatchResult {
    Single(Result<AppendResult>),
    Transaction(Vec<Result<AppendResult>>),
}

// =============================================================================
// Staged State
// =============================================================================

/// Staged changes within a batch (not yet committed to disk).
///
/// This tracks changes made by SAVEPOINTs within the current transaction.
/// If the transaction commits, these become the new committed state.
/// If it rolls back, these are discarded.
struct StagedState {
    /// Stream heads modified in this batch.
    /// Key: (stream_hash, collision_slot)
    heads: HashMap<(StreamHash, CollisionSlot), StreamHeadEntry>,

    /// Next global position to assign.
    next_pos: GlobalPos,
}

impl StagedState {
    fn new(next_pos: GlobalPos) -> Self {
        Self {
            heads: HashMap::new(),
            next_pos,
        }
    }

    fn clear(&mut self, next_pos: GlobalPos) {
        self.heads.clear();
        self.next_pos = next_pos;
    }
}

// =============================================================================
// Batch Writer
// =============================================================================

/// The batch writer that handles group commit.
///
/// This is the internal implementation that runs on a dedicated thread.
/// Use [`BatchWriterHandle`] to interact with it from async code.
pub struct BatchWriter {
    /// SQLite connection (owned, single writer).
    conn: Connection,

    /// Cryptor for compression and encryption.
    cryptor: BatchCryptor,

    /// Committed stream heads (mirrors disk).
    heads_committed: HashMap<StreamHash, Vec<StreamHeadEntry>>,

    /// Staged stream heads (batch-local overlay).
    staged: StagedState,

    /// Next global position (committed).
    next_pos_committed: GlobalPos,

    /// Command cache for idempotency.
    commands: LruCache<String, CommandCacheEntry>,

    /// Configuration (stored for potential future use like dynamic reconfiguration).
    #[allow(dead_code)]
    config: WriterConfig,
}

impl BatchWriter {
    /// Creates a new batch writer.
    ///
    /// # Arguments
    ///
    /// * `conn` - SQLite connection (schema must be initialized)
    /// * `cryptor` - Cryptor for compression and encryption
    /// * `config` - Writer configuration
    pub fn new(conn: Connection, cryptor: BatchCryptor, config: WriterConfig) -> Result<Self> {
        let cache_capacity = NonZeroUsize::new(COMMAND_CACHE_MAX_ENTRIES)
            .expect("COMMAND_CACHE_MAX_ENTRIES must be > 0");

        let mut writer = Self {
            conn,
            cryptor,
            heads_committed: HashMap::new(),
            staged: StagedState::new(GlobalPos::FIRST),
            next_pos_committed: GlobalPos::FIRST,
            commands: LruCache::new(cache_capacity),
            config,
        };

        writer.load_stream_heads()?;
        writer.load_commands()?;
        writer.load_next_global_pos()?;

        // Initialize staged state
        writer.staged = StagedState::new(writer.next_pos_committed);

        Ok(writer)
    }

    /// Loads stream heads from database.
    fn load_stream_heads(&mut self) -> Result<()> {
        let mut stmt = self.conn.prepare(
            "SELECT stream_id, stream_hash, tenant_hash, collision_slot, last_rev, last_pos
             FROM stream_heads",
        )?;

        let entries = stmt.query_map([], |row| {
            let stream_id: String = row.get(0)?;
            let _stream_hash: i64 = row.get(1)?;
            let tenant_hash: i64 = row.get(2)?;
            let collision_slot: i64 = row.get(3)?;
            let last_rev: i64 = row.get(4)?;
            let last_pos: i64 = row.get(5)?;

            Ok(StreamHeadEntry {
                stream_id: StreamId::new(stream_id),
                tenant_hash: TenantHash::from_raw(tenant_hash),
                collision_slot: CollisionSlot::from_raw(collision_slot as u16),
                last_rev: StreamRev::from_raw(last_rev as u64),
                last_pos: GlobalPos::from_raw_unchecked(last_pos as u64),
            })
        })?;

        for entry in entries {
            let entry = entry?;
            let hash = entry.stream_id.hash();
            self.heads_committed
                .entry(hash)
                .or_insert_with(Vec::new)
                .push(entry);
        }

        Ok(())
    }

    /// Loads recent commands from database.
    fn load_commands(&mut self) -> Result<()> {
        let now_ms = current_time_ms();
        let cutoff_ms = now_ms.saturating_sub(COMMAND_CACHE_TTL_MS);

        let mut stmt = self.conn.prepare(
            "SELECT c.command_id, c.first_pos, c.last_pos, c.created_ms,
                    e_first.stream_rev, e_last.stream_rev
             FROM commands c
             JOIN event_index e_first ON e_first.global_pos = c.first_pos
             JOIN event_index e_last ON e_last.global_pos = c.last_pos
             WHERE c.created_ms >= ?
             ORDER BY c.created_ms DESC",
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

    /// Loads next global position from database.
    fn load_next_global_pos(&mut self) -> Result<()> {
        let max_pos: Option<i64> = self.conn.query_row(
            "SELECT MAX(global_pos) FROM event_index",
            [],
            |row| row.get(0),
        )?;

        self.next_pos_committed = match max_pos {
            Some(pos) if pos > 0 => GlobalPos::from_raw_unchecked(pos as u64 + 1),
            _ => GlobalPos::FIRST,
        };

        Ok(())
    }

    // =========================================================================
    // Stream Head Lookups (Staged → Committed)
    // =========================================================================

    /// Gets stream head, checking staged first, then committed.
    fn get_stream_head(&self, stream_id: &StreamId) -> Option<StreamHeadEntry> {
        let hash = stream_id.hash();

        // Check staged first
        for entry in self.staged.heads.values() {
            if &entry.stream_id == stream_id {
                return Some(entry.clone());
            }
        }

        // Fall back to committed
        self.heads_committed
            .get(&hash)
            .and_then(|entries| entries.iter().find(|e| &e.stream_id == stream_id))
            .cloned()
    }

    /// Gets current revision for a stream.
    fn get_current_rev(&self, stream_id: &StreamId) -> StreamRev {
        self.get_stream_head(stream_id)
            .map(|h| h.last_rev)
            .unwrap_or(StreamRev::NONE)
    }

    /// Gets or assigns collision slot for a stream.
    fn get_or_assign_collision_slot(&self, stream_id: &StreamId) -> CollisionSlot {
        let hash = stream_id.hash();

        // Check if we already have this stream in staged
        for entry in self.staged.heads.values() {
            if &entry.stream_id == stream_id {
                return entry.collision_slot;
            }
        }

        // Check committed
        match self.heads_committed.get(&hash) {
            None => CollisionSlot::FIRST,
            Some(entries) => {
                if let Some(entry) = entries.iter().find(|e| &e.stream_id == stream_id) {
                    return entry.collision_slot;
                }
                // New stream with collision
                let max_slot = entries
                    .iter()
                    .map(|e| e.collision_slot.as_raw())
                    .max()
                    .unwrap_or(0);

                // Also check staged for this hash
                let max_staged = self
                    .staged
                    .heads
                    .iter()
                    .filter(|((h, _), _)| h == &hash)
                    .map(|((_, s), _)| s.as_raw())
                    .max()
                    .unwrap_or(0);

                CollisionSlot::from_raw(max_slot.max(max_staged) + 1)
            }
        }
    }

    /// Looks up cached command result.
    fn get_cached_command_result(&mut self, command_id: &CommandId) -> Option<AppendResult> {
        let now_ms = current_time_ms();

        if let Some(entry) = self.commands.get(command_id.as_str()) {
            let age_ms = now_ms.saturating_sub(entry.created_ms);
            if age_ms <= COMMAND_CACHE_TTL_MS {
                return Some(entry.to_append_result());
            }
        }

        None
    }

    // =========================================================================
    // Batch Execution
    // =========================================================================

    /// Executes a batch of commands.
    ///
    /// This is the core group commit logic:
    /// 1. Begin outer transaction
    /// 2. For each command, create a SAVEPOINT and execute
    /// 3. On success: release SAVEPOINT
    /// 4. On failure: rollback SAVEPOINT (others continue)
    /// 5. Commit outer transaction
    /// 6. Send responses
    fn execute_batch(&mut self, items: Vec<BatchItem>) {
        if items.is_empty() {
            return;
        }

        // Reset staged state for this batch
        self.staged.clear(self.next_pos_committed);

        // Prepare command list and response channels separately
        // This avoids unsafe code by consuming items properly
        let mut commands: Vec<BatchCommand> = Vec::new();
        let mut responses: Vec<BatchResponse> = Vec::new();

        for item in items {
            match item {
                BatchItem::Single(pending) => {
                    commands.push(BatchCommand::Single(pending.command));
                    responses.push(BatchResponse::Single(pending.response));
                }
                BatchItem::Transaction(pending) => {
                    commands.push(BatchCommand::Transaction(pending.commands));
                    responses.push(BatchResponse::Transaction(pending.response));
                }
            }
        }

        // Execute the batch and collect results
        let results = self.execute_batch_inner(&commands);

        match results {
            Ok(batch_results) => {
                // Commit succeeded - merge staged into committed
                self.commit_staged_state();

                // Send responses
                for (response, result) in responses.into_iter().zip(batch_results) {
                    match (response, result) {
                        (BatchResponse::Single(sender), BatchResult::Single(r)) => {
                            let _ = sender.send(r);
                        }
                        (BatchResponse::Transaction(sender), BatchResult::Transaction(r)) => {
                            let _ = sender.send(Ok(r));
                        }
                        _ => unreachable!("mismatched response/result types"),
                    }
                }
            }
            Err(e) => {
                // Batch commit failed - all commands fail
                let err_msg = format!("batch commit failed: {}", e);

                for response in responses {
                    match response {
                        BatchResponse::Single(sender) => {
                            let _ = sender.send(Err(Error::Schema(err_msg.clone())));
                        }
                        BatchResponse::Transaction(sender) => {
                            let _ = sender.send(Err(Error::Schema(err_msg.clone())));
                        }
                    }
                }

                // Discard staged state
                self.staged.clear(self.next_pos_committed);
            }
        }
    }

    /// Inner batch execution with transaction.
    ///
    /// Uses raw SQL for transaction management to avoid borrow checker issues
    /// with the Transaction type holding a mutable borrow of the connection.
    fn execute_batch_inner(&mut self, commands: &[BatchCommand]) -> Result<Vec<BatchResult>> {
        // Begin outer transaction using raw SQL
        self.conn.execute("BEGIN IMMEDIATE", [])?;

        let mut savepoint_counter = 0;
        let mut results = Vec::with_capacity(commands.len());

        for cmd in commands {
            match cmd {
                BatchCommand::Single(command) => {
                    let result = self.execute_command_in_savepoint(
                        command,
                        &mut savepoint_counter,
                    );
                    results.push(BatchResult::Single(result));
                }
                BatchCommand::Transaction(commands) => {
                    let mut tx_results = Vec::with_capacity(commands.len());
                    for command in commands {
                        let result = self.execute_command_in_savepoint(
                            command,
                            &mut savepoint_counter,
                        );
                        tx_results.push(result);
                    }
                    results.push(BatchResult::Transaction(tx_results));
                }
            }
        }

        // Commit the outer transaction
        match self.conn.execute("COMMIT", []) {
            Ok(_) => Ok(results),
            Err(e) => {
                // Try to rollback on commit failure
                let _ = self.conn.execute("ROLLBACK", []);
                Err(e.into())
            }
        }
    }

    /// Executes a single command within a SAVEPOINT.
    fn execute_command_in_savepoint(
        &mut self,
        cmd: &AppendCommand,
        savepoint_counter: &mut usize,
    ) -> Result<AppendResult> {
        // Check for duplicate command first (before SAVEPOINT)
        if let Some(cached_result) = self.get_cached_command_result(&cmd.command_id) {
            return Ok(cached_result);
        }

        // Check conflict using staged → committed state
        let current_rev = self.get_current_rev(&cmd.stream_id);
        if cmd.expected_rev != StreamRev::ANY && current_rev != cmd.expected_rev {
            return Err(Error::Conflict {
                stream_id: cmd.stream_id.to_string(),
                expected: cmd.expected_rev.as_raw(),
                actual: current_rev.as_raw(),
            });
        }

        // Create SAVEPOINT
        let sp_name = format!("cmd_{}", *savepoint_counter);
        *savepoint_counter += 1;

        self.conn.execute(&format!("SAVEPOINT {}", sp_name), [])?;

        // Try to execute the command
        match self.execute_command_inner(cmd) {
            Ok(result) => {
                // Release SAVEPOINT (keep changes)
                self.conn.execute(&format!("RELEASE {}", sp_name), [])?;
                Ok(result)
            }
            Err(e) => {
                // Rollback SAVEPOINT (discard changes)
                self.conn.execute(&format!("ROLLBACK TO {}", sp_name), [])?;
                self.conn.execute(&format!("RELEASE {}", sp_name), [])?;
                Err(e)
            }
        }
    }

    /// Executes a command's database operations.
    fn execute_command_inner(&mut self, cmd: &AppendCommand) -> Result<AppendResult> {
        let collision_slot = self.get_or_assign_collision_slot(&cmd.stream_id);
        let stream_hash = cmd.stream_id.hash();
        let tenant_hash = cmd.tenant.hash();

        // Calculate positions using staged state
        let first_pos = self.staged.next_pos;
        let event_count = cmd.events.len() as u64;
        let last_pos = first_pos.add(event_count - 1);

        let current_rev = self.get_current_rev(&cmd.stream_id);
        let first_rev = current_rev.next();
        let last_rev = first_rev.add(event_count - 1);

        let now_ms = current_time_ms();

        // Encode events (compress and encrypt)
        let batch_id = first_pos.as_raw() as i64;
        let (batch_data, nonce, event_offsets) = encode_batch(&cmd.events, batch_id, &self.cryptor)?;
        let checksum = compute_checksum(&batch_data);

        // Insert batch
        self.conn.execute(
            "INSERT INTO batches (base_pos, event_count, created_ms, codec, cipher, nonce, checksum, data)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                first_pos.as_raw() as i64,
                event_count as i64,
                now_ms as i64,
                CODEC_ZSTD_L1,
                CIPHER_AES256GCM,
                nonce.as_slice(),
                checksum.as_slice(),
                batch_data.as_slice(),
            ],
        )?;

        let batch_id = self.conn.last_insert_rowid();

        // Insert event index entries
        let mut rev = first_rev;
        let mut pos = first_pos;
        for (offset, len) in event_offsets.iter() {
            self.conn.execute(
                "INSERT INTO event_index (global_pos, batch_id, byte_offset, byte_len, stream_hash, tenant_hash, collision_slot, stream_rev)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    pos.as_raw() as i64,
                    batch_id,
                    *offset as i64,
                    *len as i64,
                    stream_hash.as_raw(),
                    tenant_hash.as_raw(),
                    collision_slot.as_raw() as i64,
                    rev.as_raw() as i64,
                ],
            )?;

            pos = pos.next();
            rev = rev.next();
        }

        // Upsert stream head
        self.conn.execute(
            "INSERT INTO stream_heads (stream_id, stream_hash, tenant_hash, collision_slot, last_rev, last_pos)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(stream_id) DO UPDATE SET
                 last_rev = excluded.last_rev,
                 last_pos = excluded.last_pos",
            params![
                cmd.stream_id.as_str(),
                stream_hash.as_raw(),
                tenant_hash.as_raw(),
                collision_slot.as_raw() as i64,
                last_rev.as_raw() as i64,
                last_pos.as_raw() as i64,
            ],
        )?;

        // Record command
        self.conn.execute(
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

        // Update staged state
        self.staged.next_pos = last_pos.next();
        self.staged.heads.insert(
            (stream_hash, collision_slot),
            StreamHeadEntry {
                stream_id: cmd.stream_id.clone(),
                tenant_hash,
                collision_slot,
                last_rev,
                last_pos,
            },
        );

        // Cache command result (will be available for duplicates)
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

        Ok(AppendResult::new(first_pos, last_pos, first_rev, last_rev))
    }

    /// Commits staged state to committed state.
    ///
    /// This:
    /// 1. Updates committed position counter
    /// 2. Merges staged stream heads into committed state
    /// 3. Broadcasts all staged events to subscribers
    fn commit_staged_state(&mut self) {
        // Update committed position
        self.next_pos_committed = self.staged.next_pos;

        // Merge staged heads into committed
        for ((hash, _slot), entry) in self.staged.heads.drain() {
            let entries = self.heads_committed.entry(hash).or_insert_with(Vec::new);

            // Update or add
            if let Some(existing) = entries.iter_mut().find(|e| e.stream_id == entry.stream_id) {
                existing.last_rev = entry.last_rev;
                existing.last_pos = entry.last_pos;
            } else {
                entries.push(entry);
            }
        }
    }

    // =========================================================================
    // Delete Operations (Tombstones)
    // =========================================================================

    /// Executes a stream delete command (creates tombstone).
    ///
    /// Delete commands are processed immediately (not batched) because:
    /// 1. They're infrequent compared to appends
    /// 2. Users expect immediate effect
    /// 3. They don't need group commit optimization
    pub fn execute_delete_stream(&mut self, cmd: DeleteStreamCommand) -> Result<DeleteStreamResult> {
        let stream_hash = cmd.stream_id.hash();

        // Get stream head to determine collision slot and current revision
        let head = self.get_stream_head(&cmd.stream_id);

        let collision_slot = match &head {
            Some(h) => h.collision_slot,
            None => {
                // Stream doesn't exist - nothing to delete
                return Err(Error::Schema(format!(
                    "stream '{}' does not exist",
                    cmd.stream_id
                )));
            }
        };

        let head = head.unwrap();

        // Resolve from_rev and to_rev
        let from_rev = cmd.from_rev.unwrap_or(StreamRev::FIRST);
        let to_rev = cmd.to_rev.unwrap_or(head.last_rev);

        // Validate revision range
        if from_rev.as_raw() > to_rev.as_raw() {
            return Err(Error::Schema(format!(
                "invalid revision range: from_rev ({}) > to_rev ({})",
                from_rev.as_raw(),
                to_rev.as_raw()
            )));
        }

        if to_rev.as_raw() > head.last_rev.as_raw() {
            return Err(Error::Schema(format!(
                "to_rev ({}) exceeds stream head ({})",
                to_rev.as_raw(),
                head.last_rev.as_raw()
            )));
        }

        let now_ms = current_time_ms();

        // Insert tombstone record
        // Use INSERT to create the tombstone (we allow overlapping tombstones)
        self.conn.execute(
            "INSERT INTO tombstones (stream_hash, collision_slot, from_rev, to_rev, deleted_ms)
             VALUES (?, ?, ?, ?, ?)",
            params![
                stream_hash.as_raw(),
                collision_slot.as_raw() as i64,
                from_rev.as_raw() as i64,
                to_rev.as_raw() as i64,
                now_ms as i64,
            ],
        )?;

        Ok(DeleteStreamResult {
            stream_id: cmd.stream_id,
            from_rev,
            to_rev,
            deleted_ms: now_ms,
        })
    }

    /// Executes a tenant delete command (creates tenant tombstone).
    pub fn execute_delete_tenant(&mut self, cmd: DeleteTenantCommand) -> Result<DeleteTenantResult> {
        let tenant_hash = cmd.tenant.hash();
        let now_ms = current_time_ms();

        // Use INSERT OR IGNORE for idempotency
        self.conn.execute(
            "INSERT OR IGNORE INTO tenant_tombstones (tenant_hash, deleted_ms)
             VALUES (?, ?)",
            params![tenant_hash.as_raw(), now_ms as i64],
        )?;

        // If the insert was ignored (already exists), get the existing deleted_ms
        let deleted_ms: i64 = self
            .conn
            .query_row(
                "SELECT deleted_ms FROM tenant_tombstones WHERE tenant_hash = ?",
                params![tenant_hash.as_raw()],
                |row| row.get(0),
            )
            .unwrap_or(now_ms as i64);

        Ok(DeleteTenantResult {
            tenant_hash,
            deleted_ms: deleted_ms as u64,
        })
    }
}

// =============================================================================
// Batch Writer Handle (Async Interface)
// =============================================================================

/// Async handle to the batch writer.
///
/// This is the public interface for submitting commands. It sends requests
/// to the writer thread and awaits responses.
#[derive(Clone)]
pub struct BatchWriterHandle {
    tx: mpsc::Sender<WriteRequest>,
}

impl BatchWriterHandle {
    /// Appends events to a stream.
    pub async fn append(&self, command: AppendCommand) -> Result<AppendResult> {
        let (response_tx, response_rx) = oneshot::channel();

        self.tx
            .send(WriteRequest::Append {
                command,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::Schema("writer has shut down".to_string()))?;

        response_rx
            .await
            .map_err(|_| Error::Schema("writer dropped response".to_string()))?
    }

    /// Creates a transaction for guaranteed same-batch writes.
    ///
    /// All commands added to the transaction will be executed in the same
    /// SQLite transaction, ensuring atomicity.
    pub fn begin_transaction(&self) -> TransactionBuilder {
        TransactionBuilder {
            commands: Vec::new(),
            handle: self.clone(),
        }
    }

    /// Submits a transaction.
    pub async fn submit_transaction(
        &self,
        commands: Vec<AppendCommand>,
    ) -> Result<Vec<Result<AppendResult>>> {
        if commands.is_empty() {
            return Ok(Vec::new());
        }

        let (response_tx, response_rx) = oneshot::channel();

        self.tx
            .send(WriteRequest::Transaction {
                commands,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::Schema("writer has shut down".to_string()))?;

        response_rx
            .await
            .map_err(|_| Error::Schema("writer dropped response".to_string()))?
    }

    /// Deletes events from a stream (creates tombstone).
    ///
    /// # GDPR Compliance
    ///
    /// Creates a tombstone record that immediately filters the specified
    /// events from all reads. Physical deletion happens during compaction.
    pub async fn delete_stream(&self, command: DeleteStreamCommand) -> Result<DeleteStreamResult> {
        let (response_tx, response_rx) = oneshot::channel();

        self.tx
            .send(WriteRequest::DeleteStream {
                command,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::Schema("writer has shut down".to_string()))?;

        response_rx
            .await
            .map_err(|_| Error::Schema("writer dropped response".to_string()))?
    }

    /// Deletes all events for a tenant.
    ///
    /// # GDPR "Right to be Forgotten"
    ///
    /// Creates a tenant tombstone that immediately filters all events
    /// belonging to that tenant from all reads.
    pub async fn delete_tenant(&self, command: DeleteTenantCommand) -> Result<DeleteTenantResult> {
        let (response_tx, response_rx) = oneshot::channel();

        self.tx
            .send(WriteRequest::DeleteTenant {
                command,
                response: response_tx,
            })
            .await
            .map_err(|_| Error::Schema("writer has shut down".to_string()))?;

        response_rx
            .await
            .map_err(|_| Error::Schema("writer dropped response".to_string()))?
    }
}

// =============================================================================
// Transaction Builder
// =============================================================================

/// Builder for multi-command transactions.
///
/// Commands added to a transaction are guaranteed to be written in the same
/// batch, providing atomicity across streams.
pub struct TransactionBuilder {
    commands: Vec<AppendCommand>,
    handle: BatchWriterHandle,
}

impl TransactionBuilder {
    /// Adds a command to the transaction.
    pub fn append(&mut self, command: AppendCommand) -> &mut Self {
        self.commands.push(command);
        self
    }

    /// Submits the transaction.
    ///
    /// Returns results for each command in order. If any command fails
    /// (e.g., conflict), its result will be an error, but other commands
    /// may still succeed.
    pub async fn submit(self) -> Result<Vec<Result<AppendResult>>> {
        self.handle.submit_transaction(self.commands).await
    }

    /// Returns the number of commands in the transaction.
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Returns true if the transaction is empty.
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

// =============================================================================
// Writer Loop
// =============================================================================

/// Runs the batch writer loop.
///
/// This function runs on a dedicated thread, collecting commands and
/// executing them in batches.
pub async fn run_batch_writer(
    mut writer: BatchWriter,
    mut rx: mpsc::Receiver<WriteRequest>,
    config: WriterConfig,
) {
    let mut batch: Vec<BatchItem> = Vec::new();
    let mut batch_start: Option<Instant> = None;

    loop {
        // Calculate timeout for batch
        let wait_timeout = if batch.is_empty() {
            // No pending commands - wait indefinitely
            Duration::from_secs(3600) // 1 hour (effectively forever)
        } else {
            // Have pending commands - wait for remaining batch time
            let elapsed = batch_start.unwrap().elapsed();
            config.batch_timeout.saturating_sub(elapsed)
        };

        // Try to receive next request
        let recv_result = timeout(wait_timeout, rx.recv()).await;

        match recv_result {
            Ok(Some(WriteRequest::Append { command, response })) => {
                if batch.is_empty() {
                    batch_start = Some(Instant::now());
                }

                batch.push(BatchItem::Single(PendingCommand { command, response }));

                // Check if batch is full
                if batch.len() >= config.batch_max_size {
                    writer.execute_batch(std::mem::take(&mut batch));
                    batch_start = None;
                }
            }
            Ok(Some(WriteRequest::Transaction { commands, response })) => {
                if batch.is_empty() {
                    batch_start = Some(Instant::now());
                }

                batch.push(BatchItem::Transaction(PendingTransaction {
                    commands,
                    response,
                }));

                // Check if batch is full
                if batch.len() >= config.batch_max_size {
                    writer.execute_batch(std::mem::take(&mut batch));
                    batch_start = None;
                }
            }
            Ok(Some(WriteRequest::DeleteStream { command, response })) => {
                // Delete requests are handled immediately (not batched)
                // They're infrequent and users expect immediate effect
                let result = writer.execute_delete_stream(command);
                let _ = response.send(result);
            }
            Ok(Some(WriteRequest::DeleteTenant { command, response })) => {
                // Delete tenant requests are handled immediately
                let result = writer.execute_delete_tenant(command);
                let _ = response.send(result);
            }
            Ok(Some(WriteRequest::Shutdown)) => {
                // Execute any remaining batch before shutdown
                if !batch.is_empty() {
                    writer.execute_batch(std::mem::take(&mut batch));
                }
                break;
            }
            Ok(None) => {
                // Channel closed
                if !batch.is_empty() {
                    writer.execute_batch(std::mem::take(&mut batch));
                }
                break;
            }
            Err(_) => {
                // Timeout - execute current batch
                if !batch.is_empty() {
                    writer.execute_batch(std::mem::take(&mut batch));
                    batch_start = None;
                }
            }
        }
    }
}

/// Spawns the batch writer on a dedicated thread.
///
/// Returns a handle for submitting commands.
pub fn spawn_batch_writer(conn: Connection, cryptor: BatchCryptor, config: WriterConfig) -> Result<BatchWriterHandle> {
    let (tx, rx) = mpsc::channel(COMMAND_CHANNEL_SIZE);

    let writer = BatchWriter::new(conn, cryptor, config.clone())?;

    std::thread::Builder::new()
        .name("spitedb-batch-writer".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create writer runtime");

            rt.block_on(run_batch_writer(writer, rx, config));
        })
        .map_err(|e| Error::Schema(format!("failed to spawn writer thread: {}", e)))?;

    Ok(BatchWriterHandle { tx })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::EnvKeyProvider;
    use crate::schema::Database;
    use crate::types::EventData;

    fn test_key() -> [u8; 32] {
        [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
        ]
    }

    fn test_cryptor() -> BatchCryptor {
        BatchCryptor::new(EnvKeyProvider::from_key(test_key()))
    }

    fn create_test_writer() -> BatchWriter {
        let db = Database::open_in_memory().unwrap();
        BatchWriter::new(db.into_connection(), test_cryptor(), WriterConfig::default()).unwrap()
    }

    #[test]
    fn test_batch_writer_creation() {
        let writer = create_test_writer();
        assert_eq!(writer.next_pos_committed.as_raw(), 1);
    }

    #[test]
    fn test_staged_state_isolation() {
        let mut writer = create_test_writer();

        // Initial state
        assert_eq!(writer.staged.next_pos.as_raw(), 1);
        assert!(writer.staged.heads.is_empty());

        // After clear
        writer.staged.clear(GlobalPos::from_raw(10));
        assert_eq!(writer.staged.next_pos.as_raw(), 10);
        assert!(writer.staged.heads.is_empty());
    }

    #[tokio::test]
    async fn test_batch_writer_single_command() {
        let db = Database::open_in_memory().unwrap();
        let handle = spawn_batch_writer(db.into_connection(), test_cryptor(), WriterConfig::default()).unwrap();

        let cmd = AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"hello".to_vec())],
        );

        let result = handle.append(cmd).await.unwrap();
        assert_eq!(result.first_pos.as_raw(), 1);
    }

    #[tokio::test]
    async fn test_batch_writer_transaction() {
        let db = Database::open_in_memory().unwrap();
        let handle = spawn_batch_writer(db.into_connection(), test_cryptor(), WriterConfig::default()).unwrap();

        let mut tx = handle.begin_transaction();
        tx.append(AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"event1".to_vec())],
        ));
        tx.append(AppendCommand::new(
            "cmd-2",
            "stream-2",
            StreamRev::NONE,
            vec![EventData::new(b"event2".to_vec())],
        ));
        let results = tx.submit().await.unwrap();

        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());

        // Verify positions are contiguous (same batch)
        let pos1 = results[0].as_ref().unwrap().first_pos.as_raw();
        let pos2 = results[1].as_ref().unwrap().first_pos.as_raw();
        assert_eq!(pos2, pos1 + 1);
    }

    #[tokio::test]
    async fn test_batch_writer_conflict_isolation() {
        let db = Database::open_in_memory().unwrap();
        let handle = spawn_batch_writer(db.into_connection(), test_cryptor(), WriterConfig::default()).unwrap();

        // First write to stream-1
        handle
            .append(AppendCommand::new(
                "cmd-0",
                "stream-1",
                StreamRev::NONE,
                vec![EventData::new(b"first".to_vec())],
            ))
            .await
            .unwrap();

        // Transaction with a conflict in the middle
        let mut tx = handle.begin_transaction();
        tx.append(AppendCommand::new(
            "cmd-1",
            "stream-2",
            StreamRev::NONE,
            vec![EventData::new(b"event1".to_vec())],
        ));
        tx.append(AppendCommand::new(
            "cmd-2",
            "stream-1", // Will conflict - expected NONE but has rev 1
            StreamRev::NONE,
            vec![EventData::new(b"conflict".to_vec())],
        ));
        tx.append(AppendCommand::new(
            "cmd-3",
            "stream-3",
            StreamRev::NONE,
            vec![EventData::new(b"event3".to_vec())],
        ));
        let results = tx.submit().await.unwrap();

        assert_eq!(results.len(), 3);
        assert!(results[0].is_ok()); // stream-2: success
        assert!(results[1].is_err()); // stream-1: conflict
        assert!(results[2].is_ok()); // stream-3: success
    }

    #[tokio::test]
    async fn test_batch_writer_idempotency() {
        let db = Database::open_in_memory().unwrap();
        let handle = spawn_batch_writer(db.into_connection(), test_cryptor(), WriterConfig::default()).unwrap();

        let cmd = AppendCommand::new(
            "cmd-idem",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"hello".to_vec())],
        );

        let result1 = handle.append(cmd.clone()).await.unwrap();
        let result2 = handle.append(cmd).await.unwrap();

        assert_eq!(result1.first_pos.as_raw(), result2.first_pos.as_raw());
    }

    #[tokio::test]
    async fn test_batch_writer_multiple_appends_same_stream() {
        // Test that multiple appends to the same stream within a transaction
        // use staged state correctly
        let db = Database::open_in_memory().unwrap();
        let handle = spawn_batch_writer(db.into_connection(), test_cryptor(), WriterConfig::default()).unwrap();

        let mut tx = handle.begin_transaction();
        tx.append(AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE, // First append: expect stream doesn't exist
            vec![EventData::new(b"event1".to_vec())],
        ));
        tx.append(AppendCommand::new(
            "cmd-2",
            "stream-1",
            StreamRev::from_raw(1), // Second append: expect rev 1 (from staged state)
            vec![EventData::new(b"event2".to_vec())],
        ));
        tx.append(AppendCommand::new(
            "cmd-3",
            "stream-1",
            StreamRev::from_raw(2), // Third append: expect rev 2 (from staged state)
            vec![EventData::new(b"event3".to_vec())],
        ));
        let results = tx.submit().await.unwrap();

        assert_eq!(results.len(), 3);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
        assert!(results[2].is_ok());

        // Verify positions are contiguous
        let pos1 = results[0].as_ref().unwrap().first_pos.as_raw();
        let pos2 = results[1].as_ref().unwrap().first_pos.as_raw();
        let pos3 = results[2].as_ref().unwrap().first_pos.as_raw();
        assert_eq!(pos2, pos1 + 1);
        assert_eq!(pos3, pos2 + 1);

        // Verify stream revisions
        let rev1 = results[0].as_ref().unwrap().first_rev.as_raw();
        let rev2 = results[1].as_ref().unwrap().first_rev.as_raw();
        let rev3 = results[2].as_ref().unwrap().first_rev.as_raw();
        assert_eq!(rev1, 1);
        assert_eq!(rev2, 2);
        assert_eq!(rev3, 3);
    }

    #[tokio::test]
    async fn test_batch_writer_conflict_does_not_affect_others() {
        // Verify that a conflict in the middle of a batch doesn't prevent
        // other commands from being committed
        let db = Database::open_in_memory().unwrap();
        let handle = spawn_batch_writer(db.into_connection(), test_cryptor(), WriterConfig::default()).unwrap();

        // Setup: create stream-1 with one event
        handle
            .append(AppendCommand::new(
                "cmd-setup",
                "stream-1",
                StreamRev::NONE,
                vec![EventData::new(b"setup".to_vec())],
            ))
            .await
            .unwrap();

        // Transaction: first and third should succeed, second should fail
        let mut tx = handle.begin_transaction();
        tx.append(AppendCommand::new(
            "cmd-a",
            "stream-a",
            StreamRev::NONE,
            vec![EventData::new(b"success-a".to_vec())],
        ));
        tx.append(AppendCommand::new(
            "cmd-conflict",
            "stream-1",
            StreamRev::NONE, // Wrong! stream-1 is at rev 1
            vec![EventData::new(b"will-fail".to_vec())],
        ));
        tx.append(AppendCommand::new(
            "cmd-b",
            "stream-b",
            StreamRev::NONE,
            vec![EventData::new(b"success-b".to_vec())],
        ));
        let results = tx.submit().await.unwrap();

        // Verify results
        assert!(results[0].is_ok(), "stream-a should succeed");
        assert!(results[1].is_err(), "stream-1 conflict should fail");
        assert!(results[2].is_ok(), "stream-b should succeed despite earlier conflict");

        // Verify the successful events were actually written
        // We'd need a read connection for this, but the positions should be valid
        let pos_a = results[0].as_ref().unwrap().first_pos.as_raw();
        let pos_b = results[2].as_ref().unwrap().first_pos.as_raw();
        assert!(pos_a > 0);
        assert!(pos_b > 0);
        // Positions should be contiguous (conflict didn't waste a position)
        assert_eq!(pos_b, pos_a + 1);
    }

    #[tokio::test]
    async fn test_batch_writer_concurrent_requests() {
        // Test that concurrent requests get batched together
        let db = Database::open_in_memory().unwrap();
        // Use a longer timeout to ensure batching happens
        let config = WriterConfig {
            batch_timeout: Duration::from_millis(100),
            batch_max_size: 100,
        };
        let handle = spawn_batch_writer(db.into_connection(), test_cryptor(), config).unwrap();

        // Spawn multiple concurrent append tasks
        let mut handles = Vec::new();
        for i in 0..10 {
            let h = handle.clone();
            let task = tokio::spawn(async move {
                h.append(AppendCommand::new(
                    format!("cmd-{}", i),
                    format!("stream-{}", i),
                    StreamRev::NONE,
                    vec![EventData::new(format!("event-{}", i).into_bytes())],
                ))
                .await
            });
            handles.push(task);
        }

        // Wait for all to complete
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap().unwrap());
        }

        // All should succeed
        assert_eq!(results.len(), 10);
        for result in &results {
            assert!(result.first_pos.as_raw() > 0);
        }
    }

    #[tokio::test]
    async fn test_batch_writer_empty_transaction() {
        let db = Database::open_in_memory().unwrap();
        let handle = spawn_batch_writer(db.into_connection(), test_cryptor(), WriterConfig::default()).unwrap();

        let tx = handle.begin_transaction();
        assert!(tx.is_empty());

        let results = tx.submit().await.unwrap();
        assert!(results.is_empty());
    }
}
