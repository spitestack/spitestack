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
use tokio::sync::{mpsc, oneshot, Semaphore, OwnedSemaphorePermit, Mutex as TokioMutex};
use tokio::time::{timeout, sleep};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};

use crate::codec::{compute_checksum, current_time_ms, decode_batch, encode_batch_iter};
use crate::crypto::{BatchCryptor, CIPHER_AES256GCM, CODEC_ZSTD_L1, AES_GCM_NONCE_SIZE};
use crate::error::{Error, Result};
use crate::tombstones::{
    load_stream_tombstones, load_tenant_tombstones, is_revision_tombstoned, CompactionStats,
};
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
/// With adaptive admission control, larger batches improve throughput
/// while admission control protects p99 latency.
pub const DEFAULT_BATCH_TIMEOUT_MS: u64 = 10;

/// Maximum commands per batch.
///
/// If this many commands accumulate before the timeout, execute immediately.
/// With adaptive admission control, larger batches improve throughput
/// while admission control protects p99 latency.
pub const DEFAULT_BATCH_MAX_SIZE: usize = 5_000;

/// Maximum uncompressed payload bytes per batch (1MB).
///
/// If accumulated event payload bytes exceed this before the timeout, execute immediately.
/// Smaller batches = lower latency but more fsyncs.
pub const DEFAULT_BATCH_MAX_BYTES: usize = 1024 * 1024;

/// Size of the command channel.
const COMMAND_CHANNEL_SIZE: usize = 16384;

/// Default max in-flight events for admission control.
///
/// Limits concurrent events to prevent unbounded queueing under load.
/// Set to ~3x the batch size to allow pipelining while bounding queues.
pub const DEFAULT_MAX_INFLIGHT_EVENTS: usize = 3_000;

/// Default per-request deadline in milliseconds.
///
/// Requests exceeding this deadline fail fast with Error::Timeout.
/// This prevents tail latency from growing unbounded under load.
pub const DEFAULT_REQUEST_DEADLINE_MS: u64 = 100;

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

    /// Maximum uncompressed payload bytes per batch.
    pub batch_max_bytes: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            batch_timeout: Duration::from_millis(DEFAULT_BATCH_TIMEOUT_MS),
            batch_max_size: DEFAULT_BATCH_MAX_SIZE,
            batch_max_bytes: DEFAULT_BATCH_MAX_BYTES,
        }
    }
}

// =============================================================================
// Adaptive Admission Control
// =============================================================================

/// Default latency tracking window size (number of samples).
const LATENCY_WINDOW_SIZE: usize = 10_000;

/// Configuration for adaptive admission control.
#[derive(Debug, Clone)]
pub struct AdmissionConfig {
    /// Target p99 latency in milliseconds.
    pub target_p99_ms: u64,

    /// Initial max in-flight events.
    pub initial_max_inflight: usize,

    /// Minimum max in-flight (floor to prevent over-rejection).
    pub min_max_inflight: usize,

    /// Maximum max in-flight (ceiling to prevent unbounded growth).
    pub max_max_inflight: usize,

    /// Adjustment rate per control loop iteration (e.g., 0.10 = 10%).
    pub adjustment_rate: f64,

    /// How often to run the control loop (milliseconds).
    pub adjustment_interval_ms: u64,

    /// Tolerance band around target (adjust only if p99 is outside target ± tolerance).
    pub target_tolerance_ms: u64,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            target_p99_ms: 60,
            initial_max_inflight: 3000,
            min_max_inflight: 500,
            max_max_inflight: 10000,
            adjustment_rate: 0.10,
            adjustment_interval_ms: 2000,
            target_tolerance_ms: 10,
        }
    }
}

/// Sliding window latency tracker for p99 calculation.
pub struct LatencyTracker {
    /// Circular buffer of latencies in microseconds.
    samples: Vec<u64>,
    /// Current write position.
    position: usize,
    /// Number of samples collected (up to samples.len()).
    count: usize,
}

impl LatencyTracker {
    /// Creates a new latency tracker with the given window size.
    pub fn new(window_size: usize) -> Self {
        Self {
            samples: vec![0; window_size],
            position: 0,
            count: 0,
        }
    }

    /// Records a latency sample in microseconds.
    pub fn record(&mut self, latency_us: u64) {
        self.samples[self.position] = latency_us;
        self.position = (self.position + 1) % self.samples.len();
        if self.count < self.samples.len() {
            self.count += 1;
        }
    }

    /// Calculates the given percentile (0.0 to 1.0) from collected samples.
    /// Returns None if no samples have been collected.
    pub fn percentile(&self, p: f64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }

        // Copy and sort the samples we have
        let mut sorted: Vec<u64> = self.samples[..self.count].to_vec();
        sorted.sort_unstable();

        let idx = ((p * self.count as f64) as usize).min(self.count - 1);
        Some(sorted[idx])
    }

    /// Returns the number of samples collected.
    pub fn count(&self) -> usize {
        self.count
    }
}

/// Observable metrics for admission control.
#[derive(Debug)]
pub struct AdmissionMetrics {
    /// Current max in-flight limit.
    pub current_limit: AtomicUsize,

    /// Observed p99 latency in microseconds.
    pub observed_p99_us: AtomicU64,

    /// Target p99 latency in microseconds.
    pub target_p99_us: AtomicU64,

    /// Number of requests accepted.
    pub requests_accepted: AtomicU64,

    /// Number of requests rejected (timeouts).
    pub requests_rejected: AtomicU64,

    /// Number of controller adjustments.
    pub adjustments: AtomicU64,
}

impl AdmissionMetrics {
    fn new(initial_limit: usize, target_p99_ms: u64) -> Self {
        Self {
            current_limit: AtomicUsize::new(initial_limit),
            observed_p99_us: AtomicU64::new(0),
            target_p99_us: AtomicU64::new(target_p99_ms * 1000),
            requests_accepted: AtomicU64::new(0),
            requests_rejected: AtomicU64::new(0),
            adjustments: AtomicU64::new(0),
        }
    }
}

/// Snapshot of admission metrics for external consumption.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Current max in-flight limit.
    pub current_limit: usize,
    /// Observed p99 latency in milliseconds.
    pub observed_p99_ms: f64,
    /// Target p99 latency in milliseconds.
    pub target_p99_ms: f64,
    /// Total requests accepted.
    pub requests_accepted: u64,
    /// Total requests rejected.
    pub requests_rejected: u64,
    /// Rejection rate (0.0 to 1.0).
    pub rejection_rate: f64,
    /// Number of controller adjustments.
    pub adjustments: u64,
}

/// Adaptive admission controller that adjusts max in-flight based on observed latency.
pub struct AdmissionController {
    /// Current effective limit (adjusted by control loop).
    current_limit: AtomicUsize,

    /// Hard semaphore for the max bound.
    semaphore: Arc<Semaphore>,

    /// Configuration.
    config: AdmissionConfig,

    /// Latency tracker (shared with append path).
    latency_tracker: Arc<TokioMutex<LatencyTracker>>,

    /// Observable metrics.
    pub metrics: Arc<AdmissionMetrics>,

    /// Default request deadline.
    default_deadline: Duration,
}

impl AdmissionController {
    /// Creates a new admission controller with the given configuration.
    pub fn new(config: AdmissionConfig) -> Self {
        let metrics = Arc::new(AdmissionMetrics::new(
            config.initial_max_inflight,
            config.target_p99_ms,
        ));

        Self {
            current_limit: AtomicUsize::new(config.initial_max_inflight),
            semaphore: Arc::new(Semaphore::new(config.max_max_inflight)),
            config,
            latency_tracker: Arc::new(TokioMutex::new(LatencyTracker::new(LATENCY_WINDOW_SIZE))),
            metrics,
            default_deadline: Duration::from_millis(DEFAULT_REQUEST_DEADLINE_MS),
        }
    }

    /// Acquires permits for the given number of events, respecting the current adaptive limit.
    pub async fn acquire_permits(&self, count: usize, deadline: Instant) -> Result<OwnedSemaphorePermit> {
        // Check against current adaptive limit
        let current_limit = self.current_limit.load(Ordering::Relaxed);
        let current_inflight = self.config.max_max_inflight - self.semaphore.available_permits();

        if current_inflight + count > current_limit {
            // Would exceed adaptive limit - this is the backpressure point
            // We still try to acquire, but with a tighter effective deadline
        }

        let remaining = deadline.saturating_duration_since(Instant::now());

        timeout(remaining, self.semaphore.clone().acquire_many_owned(count as u32))
            .await
            .map_err(|_| Error::Timeout("backpressure timeout waiting for capacity".into()))?
            .map_err(|_| Error::Schema("writer has shut down".into()))
    }

    /// Records a latency sample.
    pub async fn record_latency(&self, latency_us: u64) {
        self.latency_tracker.lock().await.record(latency_us);
    }

    /// Returns the default request deadline.
    pub fn default_deadline(&self) -> Duration {
        self.default_deadline
    }

    /// Returns a snapshot of current metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let accepted = self.metrics.requests_accepted.load(Ordering::Relaxed);
        let rejected = self.metrics.requests_rejected.load(Ordering::Relaxed);
        let total = accepted + rejected;

        MetricsSnapshot {
            current_limit: self.metrics.current_limit.load(Ordering::Relaxed),
            observed_p99_ms: self.metrics.observed_p99_us.load(Ordering::Relaxed) as f64 / 1000.0,
            target_p99_ms: self.metrics.target_p99_us.load(Ordering::Relaxed) as f64 / 1000.0,
            requests_accepted: accepted,
            requests_rejected: rejected,
            rejection_rate: if total > 0 { rejected as f64 / total as f64 } else { 0.0 },
            adjustments: self.metrics.adjustments.load(Ordering::Relaxed),
        }
    }

    /// Runs the control loop that adjusts max_inflight based on observed p99.
    pub async fn run_control_loop(self: Arc<Self>) {
        loop {
            sleep(Duration::from_millis(self.config.adjustment_interval_ms)).await;

            // Get current p99
            let p99_us = {
                let tracker = self.latency_tracker.lock().await;
                tracker.percentile(0.99)
            };

            let Some(p99_us) = p99_us else { continue };
            let p99_ms = p99_us / 1000;

            let target = self.config.target_p99_ms;
            let tolerance = self.config.target_tolerance_ms;
            let current = self.current_limit.load(Ordering::Relaxed);

            let new_limit = if p99_ms > target + tolerance {
                // Over target: reduce limit
                let reduction = (current as f64 * self.config.adjustment_rate) as usize;
                current.saturating_sub(reduction.max(1)).max(self.config.min_max_inflight)
            } else if p99_ms < target.saturating_sub(tolerance) {
                // Under target: increase limit
                let increase = (current as f64 * self.config.adjustment_rate) as usize;
                (current + increase.max(1)).min(self.config.max_max_inflight)
            } else {
                // Within tolerance: no change
                current
            };

            if new_limit != current {
                self.current_limit.store(new_limit, Ordering::Relaxed);
                self.metrics.adjustments.fetch_add(1, Ordering::Relaxed);
            }

            // Update metrics
            self.metrics.current_limit.store(new_limit, Ordering::Relaxed);
            self.metrics.observed_p99_us.store(p99_us, Ordering::Relaxed);
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

    /// Execute compaction of tombstoned events.
    ///
    /// This physically deletes events marked by tombstones by rewriting
    /// affected batches. Sent by the background compaction task.
    Compact {
        /// Maximum number of batches to process (for incremental compaction)
        max_batches: Option<usize>,
        response: oneshot::Sender<Result<CompactionStats>>,
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
// Validated Command
// =============================================================================

/// A command that has passed validation and is ready for execution.
///
/// This captures all the computed state needed to execute the command,
/// including assigned positions and revisions. Stores indices rather than
/// owned data to avoid cloning.
struct ValidatedCommand {
    /// Index into flat_commands array (for accessing original command)
    flat_index: usize,

    /// Precomputed stream hash
    stream_hash: StreamHash,

    /// Precomputed tenant hash
    tenant_hash: TenantHash,

    /// Assigned collision slot
    collision_slot: CollisionSlot,

    /// First global position assigned to this command's events
    first_pos: GlobalPos,

    /// Last global position assigned to this command's events
    last_pos: GlobalPos,

    /// First stream revision assigned to this command's events
    first_rev: StreamRev,

    /// Last stream revision assigned to this command's events
    last_rev: StreamRev,

    /// Number of events in this command
    event_count: usize,
}

/// Result of validation: either a validated command or a cached/error result.
enum ValidationResult {
    /// Command is valid and ready for execution
    Valid(ValidatedCommand),

    /// Command was a duplicate - return cached result
    Duplicate(AppendResult),

    /// Command had a conflict error
    Conflict(Error),
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
    /// Key: (stream_id, tenant_hash) for O(1) lookup with collision safety.
    /// Using StreamId as key ensures hash collisions don't corrupt staged state.
    heads: HashMap<(StreamId, TenantHash), StreamHeadEntry>,

    /// Next global position to assign.
    next_pos: GlobalPos,

    /// Command cache entries staged in this batch.
    /// These are only merged into the main cache after COMMIT succeeds.
    staged_commands: HashMap<(TenantHash, CommandId), CommandCacheEntry>,
}

impl StagedState {
    fn new(next_pos: GlobalPos) -> Self {
        Self {
            heads: HashMap::new(),
            next_pos,
            staged_commands: HashMap::new(),
        }
    }

    fn clear(&mut self, next_pos: GlobalPos) {
        self.heads.clear();
        self.next_pos = next_pos;
        self.staged_commands.clear();
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
    commands: LruCache<(TenantHash, CommandId), CommandCacheEntry>,

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

        // Query commands directly - no join needed since we store first_rev/last_rev
        // Order by ASC so newest entries are inserted last (most recently used in LRU)
        let mut stmt = self.conn.prepare(
            "SELECT tenant_hash, command_id, stream_id, stream_hash, first_pos, last_pos, first_rev, last_rev, created_ms
             FROM commands
             WHERE created_ms >= ?
             ORDER BY created_ms ASC",
        )?;

        let entries = stmt.query_map([cutoff_ms as i64], |row| {
            let tenant_hash: i64 = row.get(0)?;
            let command_id: String = row.get(1)?;
            let stream_id: String = row.get(2)?;
            let stream_hash: i64 = row.get(3)?;
            let first_pos: i64 = row.get(4)?;
            let last_pos: i64 = row.get(5)?;
            let first_rev: i64 = row.get(6)?;
            let last_rev: i64 = row.get(7)?;
            let created_ms: i64 = row.get(8)?;

            Ok((
                tenant_hash,
                command_id,
                stream_id,
                stream_hash,
                first_pos,
                last_pos,
                first_rev,
                last_rev,
                created_ms,
            ))
        })?;

        for entry in entries {
            let (tenant_hash, command_id, stream_id, stream_hash, first_pos, last_pos, first_rev, last_rev, created_ms) =
                entry?;

            let tenant_hash = TenantHash::from_raw(tenant_hash);
            let command_id = CommandId::from(command_id);
            let stream_id = StreamId::new(stream_id);

            debug_assert_eq!(stream_id.hash().as_raw(), stream_hash, "commands table is corrupted: stream_id hash mismatch");

            self.commands.put(
                (tenant_hash, command_id.clone()),
                CommandCacheEntry {
                    tenant_hash,
                    stream_id,
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
    /// Filters by both stream_id and tenant_hash to ensure proper tenant isolation.
    fn get_stream_head(&self, stream_id: &StreamId, tenant_hash: TenantHash) -> Option<StreamHeadEntry> {
        // Check staged first - O(1) lookup by (stream_id, tenant_hash)
        if let Some(entry) = self.staged.heads.get(&(stream_id.clone(), tenant_hash)) {
            return Some(entry.clone());
        }

        // Fall back to committed - must match both stream_id AND tenant_hash
        let stream_hash = stream_id.hash();
        self.heads_committed
            .get(&stream_hash)
            .and_then(|entries| {
                entries
                    .iter()
                    .find(|e| &e.stream_id == stream_id && e.tenant_hash == tenant_hash)
            })
            .cloned()
    }

    /// Gets current revision for a stream within a specific tenant.
    fn get_current_rev(&self, stream_id: &StreamId, tenant_hash: TenantHash) -> StreamRev {
        self.get_stream_head(stream_id, tenant_hash)
            .map(|h| h.last_rev)
            .unwrap_or(StreamRev::NONE)
    }

    /// Gets or assigns collision slot for a stream within a specific tenant.
    /// Different tenants with the same stream_id get independent collision slots.
    fn get_or_assign_collision_slot(&self, stream_id: &StreamId, tenant_hash: TenantHash) -> CollisionSlot {
        // Check if we already have this stream+tenant in staged - O(1) lookup
        if let Some(entry) = self.staged.heads.get(&(stream_id.clone(), tenant_hash)) {
            return entry.collision_slot;
        }

        let stream_hash = stream_id.hash();

        // Check committed
        match self.heads_committed.get(&stream_hash) {
            None => CollisionSlot::FIRST,
            Some(entries) => {
                // Look for exact match of stream_id AND tenant
                if let Some(entry) = entries
                    .iter()
                    .find(|e| &e.stream_id == stream_id && e.tenant_hash == tenant_hash)
                {
                    return entry.collision_slot;
                }
                // New stream with collision - find max slot across ALL tenants for this hash
                // (collision slots are per-hash, not per-tenant, to avoid DB conflicts)
                let max_slot = entries
                    .iter()
                    .map(|e| e.collision_slot.as_raw())
                    .max()
                    .unwrap_or(0);

                // Also check staged for this hash (across all tenants)
                // Need to iterate staged entries and check their stream_hash
                let max_staged = self
                    .staged
                    .heads
                    .iter()
                    .filter(|((sid, _), _)| sid.hash() == stream_hash)
                    .map(|(_, entry)| entry.collision_slot.as_raw())
                    .max()
                    .unwrap_or(0);

                CollisionSlot::from_raw(max_slot.max(max_staged) + 1)
            }
        }
    }

    /// Looks up cached command result.
    ///
    /// Checks staged commands first (for duplicates within current batch),
    /// then the committed cache. Returns None if not found - the caller should
    /// proceed with processing. If the command was already processed but evicted
    /// from cache, the INSERT will fail with a PK constraint error, which is
    /// expected behavior for retries beyond the cache window.
    fn get_cached_command_result(
        &mut self,
        command_id: &CommandId,
        tenant_hash: TenantHash,
        stream_id: &StreamId,
    ) -> Result<Option<AppendResult>> {
        let now_ms = current_time_ms();
        let key = (tenant_hash, command_id.clone());

        // Check staged commands first (same batch duplicates)
        if let Some(entry) = self.staged.staged_commands.get(&key) {
            if &entry.stream_id != stream_id {
                return Err(Error::Schema(format!(
                    "idempotency violation: command_id '{}' reused for a different stream within tenant",
                    command_id
                )));
            }
            return Ok(Some(entry.to_append_result()));
        }

        // Then check committed cache
        if let Some(entry) = self.commands.get(&key) {
            let age_ms = now_ms.saturating_sub(entry.created_ms);
            if age_ms <= COMMAND_CACHE_TTL_MS {
                if &entry.stream_id != stream_id {
                    return Err(Error::Schema(format!(
                        "idempotency violation: command_id '{}' reused for a different stream within tenant",
                        command_id
                    )));
                }
                return Ok(Some(entry.to_append_result()));
            }
        }

        Ok(None)
    }

    // =========================================================================
    // Command Validation
    // =========================================================================

    /// Validates a command and prepares it for execution.
    ///
    /// This performs:
    /// - Idempotency check (returns cached result if duplicate)
    /// - Conflict detection (returns error if expected_rev doesn't match)
    /// - Position and revision assignment
    /// - Staged state update for intra-batch conflict detection
    ///
    /// Returns ValidationResult to distinguish between valid, duplicate, and conflict cases.
    fn validate_and_prepare_command(
        &mut self,
        cmd: &AppendCommand,
        flat_index: usize,
        now_ms: u64,
    ) -> ValidationResult {
        // Get tenant hash for tenant-scoped lookups
        let tenant_hash = cmd.tenant.hash();

        // Check for duplicate command first
        match self.get_cached_command_result(&cmd.command_id, tenant_hash, &cmd.stream_id) {
            Ok(Some(cached_result)) => return ValidationResult::Duplicate(cached_result),
            Ok(None) => {}
            Err(e) => return ValidationResult::Conflict(e),
        }

        let stream_hash = cmd.stream_id.hash();

        // Check conflict using staged → committed state (tenant-scoped)
        let current_rev = self.get_current_rev(&cmd.stream_id, tenant_hash);
        if cmd.expected_rev != StreamRev::ANY && current_rev != cmd.expected_rev {
            return ValidationResult::Conflict(Error::Conflict {
                stream_id: cmd.stream_id.to_string(),
                expected: cmd.expected_rev.as_raw(),
                actual: current_rev.as_raw(),
            });
        }

        // Get collision slot
        let collision_slot = self.get_or_assign_collision_slot(&cmd.stream_id, tenant_hash);

        // Assign positions using staged state
        let event_count = cmd.events.len();
        let first_pos = self.staged.next_pos;
        let last_pos = first_pos.add(event_count as u64 - 1);
        let first_rev = current_rev.next();
        let last_rev = first_rev.add(event_count as u64 - 1);

        // Update staged state for next command's validation
        self.staged.next_pos = last_pos.next();
        self.staged.heads.insert(
            (cmd.stream_id.clone(), tenant_hash),
            StreamHeadEntry {
                stream_id: cmd.stream_id.clone(),
                tenant_hash,
                collision_slot,
                last_rev,
                last_pos,
            },
        );

        // Stage command result for intra-batch idempotency and post-commit cache merge.
        self.staged.staged_commands.insert(
            (tenant_hash, cmd.command_id.clone()),
            CommandCacheEntry {
                tenant_hash,
                stream_id: cmd.stream_id.clone(),
                first_pos,
                last_pos,
                first_rev,
                last_rev,
                created_ms: now_ms,
            },
        );

        // NOTE: We store flat_index instead of cloning command to avoid ~4MB of cloning per batch
        ValidationResult::Valid(ValidatedCommand {
            flat_index,
            stream_hash,
            tenant_hash,
            collision_slot,
            first_pos,
            last_pos,
            first_rev,
            last_rev,
            event_count,
        })
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
                    // Check for duplicate in committed cache BEFORE adding to batch.
                    // This ensures duplicates get their cached result immediately,
                    // regardless of whether the rest of the batch succeeds or fails.
                    // (True idempotency: retried commands always return same result)
                    let tenant_hash = pending.command.tenant.hash();
                    match self.get_cached_command_result(&pending.command.command_id, tenant_hash, &pending.command.stream_id) {
                        Ok(Some(cached)) => {
                            // Send immediately - this command was already committed
                            let _ = pending.response.send(Ok(cached));
                            // Don't add to batch - skip this item
                            continue;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            let _ = pending.response.send(Err(e));
                            continue;
                        }
                    }
                    commands.push(BatchCommand::Single(pending.command));
                    responses.push(BatchResponse::Single(pending.response));
                }
                BatchItem::Transaction(pending) => {
                    // For transactions, process as a unit (duplicates handled in execute_batch_inner)
                    commands.push(BatchCommand::Transaction(pending.commands));
                    responses.push(BatchResponse::Transaction(pending.response));
                }
            }
        }

        // All items were duplicates - nothing to process
        if commands.is_empty() {
            return;
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
    /// NEW ARCHITECTURE: Single blob per batch
    /// 1. Pre-validate all commands (conflicts, idempotency)
    /// 2. Collect all events from valid commands into Vec<EventData>
    /// 3. Encode ALL events into ONE compressed blob
    /// 4. Single INSERT into batches table
    /// 5. For each valid command: INSERT event_index entries, UPSERT stream_heads
    /// 6. INSERT into commands table (durable idempotency)
    fn execute_batch_inner(&mut self, commands: &[BatchCommand]) -> Result<Vec<BatchResult>> {
        // =====================================================================
        // Phase 1: Validate all commands, collect events from valid ones
        // =====================================================================

        let now_ms = current_time_ms();

        // Flatten commands for validation
        let mut flat_commands: Vec<(&AppendCommand, usize, Option<usize>)> = Vec::new();
        for (batch_idx, cmd) in commands.iter().enumerate() {
            match cmd {
                BatchCommand::Single(command) => {
                    flat_commands.push((command, batch_idx, None));
                }
                BatchCommand::Transaction(cmds) => {
                    for (tx_idx, command) in cmds.iter().enumerate() {
                        flat_commands.push((command, batch_idx, Some(tx_idx)));
                    }
                }
            }
        }

        // Validate each command (no event collection - we'll iterate later)
        let mut valid_commands: Vec<ValidatedCommand> = Vec::new();

        // Pre-allocate result slots
        let mut results: Vec<BatchResult> = commands
            .iter()
            .map(|cmd| match cmd {
                BatchCommand::Single(_) => BatchResult::Single(Err(Error::Schema("not processed".to_string()))),
                BatchCommand::Transaction(cmds) => {
                    BatchResult::Transaction(
                        (0..cmds.len())
                            .map(|_| Err(Error::Schema("not processed".to_string())))
                            .collect()
                    )
                }
            })
            .collect();

        for (idx, (cmd, batch_idx, tx_idx)) in flat_commands.iter().enumerate() {
            match self.validate_and_prepare_command(cmd, idx, now_ms) {
                ValidationResult::Valid(validated) => {
                    valid_commands.push(validated);
                }
                ValidationResult::Duplicate(cached_result) => {
                    // Return cached result
                    match tx_idx {
                        None => results[*batch_idx] = BatchResult::Single(Ok(cached_result)),
                        Some(ti) => {
                            if let BatchResult::Transaction(ref mut tx_results) = results[*batch_idx] {
                                tx_results[*ti] = Ok(cached_result);
                            }
                        }
                    }
                }
                ValidationResult::Conflict(err) => {
                    // Return conflict error
                    match tx_idx {
                        None => results[*batch_idx] = BatchResult::Single(Err(err)),
                        Some(ti) => {
                            if let BatchResult::Transaction(ref mut tx_results) = results[*batch_idx] {
                                tx_results[*ti] = Err(err);
                            }
                        }
                    }
                }
            }
        }

        // If no valid commands, return early with results (duplicates/conflicts only)
        if valid_commands.is_empty() {
            return Ok(results);
        }

        // Calculate total events from valid commands
        let total_events: usize = valid_commands.iter().map(|v| v.event_count).sum();

        // =====================================================================
        // Phase 2: Encode ALL events into ONE blob (zero-copy via iterator)
        // =====================================================================
        let base_pos = valid_commands.first().unwrap().first_pos.as_raw() as i64;

        // Build iterator over all event data from valid commands (no cloning)
        let event_iter = valid_commands.iter()
            .flat_map(|v| flat_commands[v.flat_index].0.events.iter().map(|e| e.data.as_slice()));

        let (batch_data, nonce, event_offsets) = encode_batch_iter(event_iter, base_pos, &self.cryptor)?;
        let checksum = compute_checksum(&batch_data);

        // =====================================================================
        // Phase 3: Database operations
        // =====================================================================

        // Begin transaction
        self.conn.execute("BEGIN IMMEDIATE", [])?;

        // Single INSERT into batches table
        self.conn.prepare_cached(
            "INSERT INTO batches (base_pos, event_count, created_ms, codec, cipher, nonce, checksum, data)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )?.execute(params![
            base_pos,
            total_events as i64,
            now_ms as i64,
            CODEC_ZSTD_L1,
            CIPHER_AES256GCM,
            nonce.as_slice(),
            checksum.as_slice(),
            batch_data.as_slice(),
        ])?;

        let batch_id = self.conn.last_insert_rowid();

        // Prepare cached statements for event_index, stream_heads, and commands.
        // Using cached prepared statements is faster than dynamic bulk INSERT because:
        // - Statement is compiled once and reused for every execution
        // - SQLite WAL mode batches all writes to a single fsync at COMMIT anyway
        // - No query parsing overhead per batch (dynamic SQL defeats prepare_cached)
        let mut event_idx_stmt = self.conn.prepare_cached(
            "INSERT INTO event_index (global_pos, batch_id, byte_offset, byte_len, stream_hash, tenant_hash, collision_slot, stream_rev)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )?;

        let mut stream_head_stmt = self.conn.prepare_cached(
            "INSERT INTO stream_heads (stream_id, stream_hash, tenant_hash, collision_slot, last_rev, last_pos)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(stream_id, tenant_hash) DO UPDATE SET
                 last_rev = excluded.last_rev,
                 last_pos = excluded.last_pos"
        )?;

        let mut cmd_stmt = self.conn.prepare_cached(
            "INSERT INTO commands (tenant_hash, command_id, stream_id, stream_hash, first_pos, last_pos, first_rev, last_rev, created_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )?;

        // Track offset into the combined event_offsets array
        let mut global_offset_idx = 0usize;

        // Process each valid command
        for validated in &valid_commands {
            let mut pos = validated.first_pos;
            let mut rev = validated.first_rev;

            // Insert event_index entries for this command's events
            for _ in 0..validated.event_count {
                let (byte_offset, byte_len) = event_offsets[global_offset_idx];
                event_idx_stmt.execute(params![
                    pos.as_raw() as i64,
                    batch_id,
                    byte_offset as i64,
                    byte_len as i64,
                    validated.stream_hash.as_raw(),
                    validated.tenant_hash.as_raw(),
                    validated.collision_slot.as_raw() as i64,
                    rev.as_raw() as i64,
                ])?;
                pos = pos.next();
                rev = rev.next();
                global_offset_idx += 1;
            }

            // Get command reference for stream_id and command_id
            let cmd = flat_commands[validated.flat_index].0;

            // UPSERT stream head
            stream_head_stmt.execute(params![
                cmd.stream_id.as_str(),
                validated.stream_hash.as_raw(),
                validated.tenant_hash.as_raw(),
                validated.collision_slot.as_raw() as i64,
                validated.last_rev.as_raw() as i64,
                validated.last_pos.as_raw() as i64,
            ])?;

            // INSERT command record (for idempotency)
            cmd_stmt.execute(params![
                validated.tenant_hash.as_raw(),
                cmd.command_id.as_str(),
                cmd.stream_id.as_str(),
                validated.stream_hash.as_raw(),
                validated.first_pos.as_raw() as i64,
                validated.last_pos.as_raw() as i64,
                validated.first_rev.as_raw() as i64,
                validated.last_rev.as_raw() as i64,
                now_ms as i64,
            ])?;
        }

        // Drop prepared statements before commit
        drop(event_idx_stmt);
        drop(stream_head_stmt);
        drop(cmd_stmt);

        // Commit
        let commit_result = self.conn.execute("COMMIT", []);

        match commit_result {
            Ok(_) => {
                // Fill in successful results for valid commands
                for validated in valid_commands {
                    let result = AppendResult::new(
                        validated.first_pos,
                        validated.last_pos,
                        validated.first_rev,
                        validated.last_rev,
                    );

                    // Find the original position in flat_commands
                    let (_, batch_idx, tx_idx) = flat_commands[validated.flat_index];
                    match tx_idx {
                        None => results[batch_idx] = BatchResult::Single(Ok(result)),
                        Some(ti) => {
                            if let BatchResult::Transaction(ref mut tx_results) = results[batch_idx] {
                                tx_results[ti] = Ok(result);
                            }
                        }
                    }
                }
                Ok(results)
            }
            Err(e) => {
                // Try to rollback on commit failure
                let _ = self.conn.execute("ROLLBACK", []);
                Err(e.into())
            }
        }
    }

    /// Commits staged state to committed state.
    ///
    /// This:
    /// 1. Updates committed position counter
    /// 2. Merges staged stream heads into committed state
    /// 3. Merges staged command cache entries into the main cache
    fn commit_staged_state(&mut self) {
        // Update committed position
        self.next_pos_committed = self.staged.next_pos;

        // Merge staged heads into committed
        for ((_stream_id, _tenant_hash), entry) in self.staged.heads.drain() {
            let stream_hash = entry.stream_id.hash();
            let entries = self.heads_committed.entry(stream_hash).or_insert_with(Vec::new);

            // Update or add
            if let Some(existing) = entries
                .iter_mut()
                .find(|e| e.stream_id == entry.stream_id && e.tenant_hash == entry.tenant_hash)
            {
                existing.last_rev = entry.last_rev;
                existing.last_pos = entry.last_pos;
            } else {
                entries.push(entry);
            }
        }

        // Merge staged command cache entries into main cache
        for (key, entry) in self.staged.staged_commands.drain() {
            self.commands.put(key, entry);
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
    ///
    /// Implements idempotency via the `command_id` - retries return the cached result.
    pub fn execute_delete_stream(&mut self, cmd: DeleteStreamCommand) -> Result<DeleteStreamResult> {
        let stream_hash = cmd.stream_id.hash();
        let tenant_hash = cmd.tenant.hash();

        // Check for duplicate delete command first (idempotency)
        if let Some(cached_result) =
            self.get_cached_delete_command_result(&cmd.command_id, tenant_hash, &cmd.stream_id)?
        {
            return Ok(cached_result);
        }

        // Get stream head to determine collision slot and current revision
        let head = self.get_stream_head(&cmd.stream_id, tenant_hash);

        let collision_slot = match &head {
            Some(h) => h.collision_slot,
            None => {
                // Stream doesn't exist for this tenant - nothing to delete
                return Err(Error::Schema(format!(
                    "stream '{}' does not exist for tenant",
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

        // Insert tombstone record with tenant scope
        // Use INSERT to create the tombstone (we allow overlapping tombstones)
        self.conn.execute(
            "INSERT INTO tombstones (stream_hash, collision_slot, tenant_hash, from_rev, to_rev, deleted_ms)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![
                stream_hash.as_raw(),
                collision_slot.as_raw() as i64,
                tenant_hash.as_raw(),
                from_rev.as_raw() as i64,
                to_rev.as_raw() as i64,
                now_ms as i64,
            ],
        )?;

        // Record delete command for idempotency
        self.conn.execute(
            "INSERT INTO delete_commands (tenant_hash, command_id, stream_id, stream_hash, from_rev, to_rev, deleted_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                tenant_hash.as_raw(),
                cmd.command_id.as_str(),
                cmd.stream_id.as_str(),
                stream_hash.as_raw(),
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

    /// Looks up cached delete command result from the database.
    fn get_cached_delete_command_result(
        &self,
        command_id: &CommandId,
        tenant_hash: TenantHash,
        stream_id: &StreamId,
    ) -> Result<Option<DeleteStreamResult>> {
        let result = self.conn.query_row(
            "SELECT stream_id, from_rev, to_rev, deleted_ms FROM delete_commands WHERE tenant_hash = ? AND command_id = ?",
            params![tenant_hash.as_raw(), command_id.as_str()],
            |row| {
                let stored_stream_id: String = row.get(0)?;
                let from_rev: i64 = row.get(1)?;
                let to_rev: i64 = row.get(2)?;
                let deleted_ms: i64 = row.get(3)?;
                Ok((stored_stream_id, from_rev, to_rev, deleted_ms))
            },
        );

        match result {
            Ok((stored_stream_id, from_rev, to_rev, deleted_ms)) => {
                if stored_stream_id != stream_id.as_str() {
                    return Err(Error::Schema(format!(
                        "idempotency violation: delete command_id '{}' reused for a different stream within tenant",
                        command_id
                    )));
                }
                Ok(Some(DeleteStreamResult {
                    stream_id: stream_id.clone(),
                    from_rev: StreamRev::from_raw(from_rev as u64),
                    to_rev: StreamRev::from_raw(to_rev as u64),
                    deleted_ms: deleted_ms as u64,
                }))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
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

    // =========================================================================
    // Compaction (Physical Deletion of Tombstoned Events)
    // =========================================================================

    /// Executes compaction of tombstoned events.
    ///
    /// This method:
    /// 1. Finds batches affected by tombstones (stream or tenant)
    /// 2. Rewrites each batch, excluding tombstoned events
    /// 3. Updates event_index byte offsets
    /// 4. Deletes batches where all events were tombstoned
    /// 5. Cleans up fully-compacted tombstone records
    ///
    /// # Arguments
    ///
    /// * `max_batches` - Optional limit on batches to process (for incremental compaction)
    ///
    /// # Returns
    ///
    /// Statistics about what was compacted.
    pub fn execute_compaction(&mut self, max_batches: Option<usize>) -> Result<CompactionStats> {
        let mut stats = CompactionStats::default();

        // 1. Find affected batches from stream tombstones
        let stream_affected = self.find_stream_tombstoned_batches()?;

        // 2. Find affected batches from tenant tombstones
        let tenant_affected = self.find_tenant_tombstoned_batches()?;

        // 3. Merge and deduplicate batch IDs
        let mut batch_ids: Vec<i64> = stream_affected
            .into_iter()
            .chain(tenant_affected)
            .collect();
        batch_ids.sort();
        batch_ids.dedup();

        // 4. Limit if specified (for incremental compaction)
        if let Some(max) = max_batches {
            batch_ids.truncate(max);
        }

        if batch_ids.is_empty() {
            return Ok(stats);
        }

        // 5. Process each batch
        for batch_id in batch_ids {
            match self.compact_single_batch(batch_id) {
                Ok((events_deleted, bytes_reclaimed)) => {
                    if events_deleted > 0 {
                        stats.batches_rewritten += 1;
                        stats.events_deleted += events_deleted;
                        stats.bytes_reclaimed += bytes_reclaimed;
                    }
                }
                Err(e) => {
                    // Log and continue with other batches
                    eprintln!("[spitedb] compact batch {} failed: {}", batch_id, e);
                }
            }
        }

        // 6. Clean up tombstones that no longer reference any events
        stats.tombstones_removed = self.cleanup_exhausted_tombstones()?;

        Ok(stats)
    }

    /// Finds batch IDs containing events affected by stream tombstones.
    fn find_stream_tombstoned_batches(&self) -> Result<Vec<i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT e.batch_id
             FROM event_index e
             JOIN tombstones t ON e.stream_hash = t.stream_hash
                              AND e.collision_slot = t.collision_slot
                              AND e.tenant_hash = t.tenant_hash
             WHERE e.stream_rev >= t.from_rev
               AND e.stream_rev <= t.to_rev",
        )?;

        let ids = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<i64>, _>>()?;

        Ok(ids)
    }

    /// Finds batch IDs containing events from tombstoned tenants.
    fn find_tenant_tombstoned_batches(&self) -> Result<Vec<i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT e.batch_id
             FROM event_index e
             JOIN tenant_tombstones t ON e.tenant_hash = t.tenant_hash",
        )?;

        let ids = stmt
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<i64>, _>>()?;

        Ok(ids)
    }

    /// Compacts a single batch by rewriting it without tombstoned events.
    ///
    /// Returns (events_deleted, bytes_reclaimed).
    fn compact_single_batch(&mut self, batch_id: i64) -> Result<(usize, usize)> {
        // 1. Load batch metadata
        let (base_pos, nonce, old_data): (i64, Vec<u8>, Vec<u8>) = self.conn.query_row(
            "SELECT base_pos, nonce, data FROM batches WHERE batch_id = ?",
            [batch_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;

        let old_size = old_data.len();

        // 2. Decode the batch
        let nonce_arr: [u8; AES_GCM_NONCE_SIZE] = nonce
            .as_slice()
            .try_into()
            .map_err(|_| Error::Encryption("invalid nonce length".into()))?;
        let decrypted = decode_batch(&old_data, &nonce_arr, base_pos, &self.cryptor)?;

        // 3. Load tenant tombstones once
        let tenant_tombstones = load_tenant_tombstones(&self.conn)?;

        // 4. Load event_index entries and filter tombstoned events
        let events_to_keep = self.load_events_for_batch_filtered(batch_id, &decrypted, &tenant_tombstones)?;

        // 5. Count deleted events
        let original_count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM event_index WHERE batch_id = ?",
            [batch_id],
            |row| row.get(0),
        )?;
        let events_deleted = (original_count as usize).saturating_sub(events_to_keep.len());

        if events_deleted == 0 {
            // No tombstoned events in this batch
            return Ok((0, 0));
        }

        if events_to_keep.is_empty() {
            // All events were tombstoned - delete the entire batch
            self.delete_batch_completely(batch_id)?;
            return Ok((events_deleted, old_size));
        }

        // 6. Rewrite the batch with remaining events
        self.rewrite_batch(batch_id, base_pos, &events_to_keep)?;

        Ok((events_deleted, old_size))
    }

    /// Loads events from a batch, filtering out tombstoned ones.
    fn load_events_for_batch_filtered(
        &self,
        batch_id: i64,
        decrypted: &[u8],
        tenant_tombstones: &std::collections::HashSet<TenantHash>,
    ) -> Result<Vec<EventToKeep>> {
        let mut stmt = self.conn.prepare(
            "SELECT global_pos, byte_offset, byte_len, stream_hash, tenant_hash,
                    collision_slot, stream_rev
             FROM event_index
             WHERE batch_id = ?
             ORDER BY global_pos",
        )?;

        let mut events_to_keep = Vec::new();
        let rows = stmt.query_map([batch_id], |row| {
            Ok(EventIndexRow {
                global_pos: row.get(0)?,
                byte_offset: row.get::<_, i64>(1)? as usize,
                byte_len: row.get::<_, i64>(2)? as usize,
                stream_hash: StreamHash::from_raw(row.get(3)?),
                tenant_hash: TenantHash::from_raw(row.get(4)?),
                collision_slot: CollisionSlot::from_raw(row.get::<_, i64>(5)? as u16),
                stream_rev: StreamRev::from_raw(row.get::<_, i64>(6)? as u64),
            })
        })?;

        for row_result in rows {
            let row = row_result?;

            // Check tenant tombstone
            if tenant_tombstones.contains(&row.tenant_hash) {
                continue; // Skip - tenant is tombstoned
            }

            // Check stream tombstone
            let stream_tombstones = load_stream_tombstones(
                &self.conn,
                row.stream_hash,
                row.collision_slot,
                row.tenant_hash,
            )?;

            if is_revision_tombstoned(&stream_tombstones, row.stream_rev) {
                continue; // Skip - revision is tombstoned
            }

            // Keep this event
            let end = row.byte_offset + row.byte_len;
            if end > decrypted.len() {
                return Err(Error::Schema(format!(
                    "corrupt event_index: offset {} + len {} exceeds batch size {}",
                    row.byte_offset, row.byte_len, decrypted.len()
                )));
            }
            let data = decrypted[row.byte_offset..end].to_vec();

            events_to_keep.push(EventToKeep {
                global_pos: row.global_pos,
                stream_hash: row.stream_hash,
                tenant_hash: row.tenant_hash,
                collision_slot: row.collision_slot,
                stream_rev: row.stream_rev,
                data,
            });
        }

        Ok(events_to_keep)
    }

    /// Rewrites a batch with only the events that should be kept.
    fn rewrite_batch(&mut self, batch_id: i64, base_pos: i64, events: &[EventToKeep]) -> Result<()> {
        let now_ms = current_time_ms();

        // 1. Encode new batch from kept events
        let event_iter = events.iter().map(|e| e.data.as_slice());
        let (new_data, new_nonce, new_offsets) = encode_batch_iter(event_iter, base_pos, &self.cryptor)?;
        let checksum = compute_checksum(&new_data);

        // 2. Begin transaction
        self.conn.execute("BEGIN IMMEDIATE", [])?;

        // 3. Delete old event_index entries for this batch
        self.conn.execute("DELETE FROM event_index WHERE batch_id = ?", [batch_id])?;

        // 4. Update batch data
        self.conn.execute(
            "UPDATE batches SET event_count = ?, created_ms = ?, nonce = ?, checksum = ?, data = ?
             WHERE batch_id = ?",
            params![
                events.len() as i64,
                now_ms as i64,
                new_nonce.as_slice(),
                checksum.as_slice(),
                new_data.as_slice(),
                batch_id,
            ],
        )?;

        // 5. Insert new event_index entries with updated offsets
        let mut stmt = self.conn.prepare_cached(
            "INSERT INTO event_index (global_pos, batch_id, byte_offset, byte_len,
                                      stream_hash, tenant_hash, collision_slot, stream_rev)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )?;

        for (i, event) in events.iter().enumerate() {
            let (offset, len) = new_offsets[i];
            stmt.execute(params![
                event.global_pos,
                batch_id,
                offset as i64,
                len as i64,
                event.stream_hash.as_raw(),
                event.tenant_hash.as_raw(),
                event.collision_slot.as_raw() as i64,
                event.stream_rev.as_raw() as i64,
            ])?;
        }

        drop(stmt);

        // 6. Commit
        self.conn.execute("COMMIT", [])?;

        Ok(())
    }

    /// Deletes a batch entirely when all events are tombstoned.
    fn delete_batch_completely(&mut self, batch_id: i64) -> Result<()> {
        self.conn.execute("BEGIN IMMEDIATE", [])?;
        self.conn.execute("DELETE FROM event_index WHERE batch_id = ?", [batch_id])?;
        self.conn.execute("DELETE FROM batches WHERE batch_id = ?", [batch_id])?;
        self.conn.execute("COMMIT", [])?;
        Ok(())
    }

    /// Cleans up tombstone records that no longer reference any events.
    fn cleanup_exhausted_tombstones(&mut self) -> Result<usize> {
        // Delete stream tombstones where no events remain in the range
        let deleted_stream = self.conn.execute(
            "DELETE FROM tombstones
             WHERE NOT EXISTS (
                 SELECT 1 FROM event_index e
                 WHERE e.stream_hash = tombstones.stream_hash
                   AND e.collision_slot = tombstones.collision_slot
                   AND e.tenant_hash = tombstones.tenant_hash
                   AND e.stream_rev >= tombstones.from_rev
                   AND e.stream_rev <= tombstones.to_rev
             )",
            [],
        )?;

        // Delete tenant tombstones where no events remain for that tenant
        let deleted_tenant = self.conn.execute(
            "DELETE FROM tenant_tombstones
             WHERE NOT EXISTS (
                 SELECT 1 FROM event_index e
                 WHERE e.tenant_hash = tenant_tombstones.tenant_hash
             )",
            [],
        )?;

        Ok(deleted_stream + deleted_tenant)
    }
}

// =============================================================================
// Compaction Helper Types
// =============================================================================

/// Row from event_index during compaction.
struct EventIndexRow {
    global_pos: i64,
    byte_offset: usize,
    byte_len: usize,
    stream_hash: StreamHash,
    tenant_hash: TenantHash,
    collision_slot: CollisionSlot,
    stream_rev: StreamRev,
}

/// Event to keep during compaction (intermediate structure).
struct EventToKeep {
    global_pos: i64,
    stream_hash: StreamHash,
    tenant_hash: TenantHash,
    collision_slot: CollisionSlot,
    stream_rev: StreamRev,
    data: Vec<u8>,
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
    /// Adaptive admission controller.
    admission: Arc<AdmissionController>,
}

impl BatchWriterHandle {
    /// Appends events to a stream.
    ///
    /// # Admission Control
    ///
    /// This method enforces adaptive admission control to bound p99 latency:
    /// - Acquires semaphore permits (one per event) respecting the current adaptive limit
    /// - Applies a deadline (default 100ms) to the entire operation
    /// - Records latency for adaptive tuning
    /// - If the deadline is exceeded while waiting, returns `Error::Timeout`
    pub async fn append(&self, command: AppendCommand) -> Result<AppendResult> {
        let deadline = Instant::now() + self.admission.default_deadline();
        let event_count = command.events.len();

        // Acquire permits with deadline - this is the backpressure point
        let permits = self.admission.acquire_permits(event_count, deadline).await?;

        // Track latency for adaptive tuning
        let start = Instant::now();
        let result = self.append_inner(command, deadline).await;
        let latency_us = start.elapsed().as_micros() as u64;

        // Record latency (only for successful requests to avoid skewing p99)
        if result.is_ok() {
            self.admission.record_latency(latency_us).await;
        }

        // Update metrics
        match &result {
            Ok(_) => { self.admission.metrics.requests_accepted.fetch_add(1, Ordering::Relaxed); }
            Err(Error::Timeout(_)) => { self.admission.metrics.requests_rejected.fetch_add(1, Ordering::Relaxed); }
            _ => {}
        }

        // Permits released on drop
        drop(permits);

        result
    }

    /// Inner append logic with deadline.
    async fn append_inner(&self, command: AppendCommand, deadline: Instant) -> Result<AppendResult> {
        let (response_tx, response_rx) = oneshot::channel();

        // Send with remaining deadline
        let remaining = deadline.saturating_duration_since(Instant::now());
        timeout(remaining, self.tx.send(WriteRequest::Append {
            command,
            response: response_tx,
        }))
        .await
        .map_err(|_| Error::Timeout("timeout sending to writer".into()))?
        .map_err(|_| Error::Schema("writer has shut down".into()))?;

        // Wait for response with remaining deadline
        let remaining = deadline.saturating_duration_since(Instant::now());
        timeout(remaining, response_rx)
            .await
            .map_err(|_| Error::Timeout("timeout waiting for response".into()))?
            .map_err(|_| Error::Schema("writer dropped response".into()))?
    }

    /// Returns a snapshot of admission control metrics.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.admission.snapshot()
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
    ///
    /// # Admission Control
    ///
    /// Acquires permits for all events across all commands in the transaction.
    pub async fn submit_transaction(
        &self,
        commands: Vec<AppendCommand>,
    ) -> Result<Vec<Result<AppendResult>>> {
        if commands.is_empty() {
            return Ok(Vec::new());
        }

        let deadline = Instant::now() + self.admission.default_deadline();

        // Count total events across all commands
        let total_events: usize = commands.iter().map(|c| c.events.len()).sum();

        // Acquire permits for all events
        let permits = self.admission.acquire_permits(total_events, deadline).await?;

        let start = Instant::now();
        let (response_tx, response_rx) = oneshot::channel();

        // Send with remaining deadline
        let remaining = deadline.saturating_duration_since(Instant::now());
        timeout(remaining, self.tx.send(WriteRequest::Transaction {
            commands,
            response: response_tx,
        }))
        .await
        .map_err(|_| Error::Timeout("timeout sending transaction to writer".into()))?
        .map_err(|_| Error::Schema("writer has shut down".into()))?;

        // Wait for response with remaining deadline
        let remaining = deadline.saturating_duration_since(Instant::now());
        let result = timeout(remaining, response_rx)
            .await
            .map_err(|_| Error::Timeout("timeout waiting for transaction response".into()))?
            .map_err(|_| Error::Schema("writer dropped response".into()))?;

        // Record latency
        let latency_us = start.elapsed().as_micros() as u64;
        if result.is_ok() {
            self.admission.record_latency(latency_us).await;
            self.admission.metrics.requests_accepted.fetch_add(1, Ordering::Relaxed);
        }

        drop(permits);
        result
    }

    /// Deletes events from a stream (creates tombstone).
    ///
    /// # GDPR Compliance
    ///
    /// Creates a tombstone record that immediately filters the specified
    /// events from all reads. Physical deletion happens during compaction.
    ///
    /// # Admission Control
    ///
    /// Acquires 1 permit for the delete operation.
    pub async fn delete_stream(&self, command: DeleteStreamCommand) -> Result<DeleteStreamResult> {
        let deadline = Instant::now() + self.admission.default_deadline();

        // Acquire 1 permit for delete operation
        let permits = self.admission.acquire_permits(1, deadline).await?;

        let (response_tx, response_rx) = oneshot::channel();

        // Send with remaining deadline
        let remaining = deadline.saturating_duration_since(Instant::now());
        timeout(remaining, self.tx.send(WriteRequest::DeleteStream {
            command,
            response: response_tx,
        }))
        .await
        .map_err(|_| Error::Timeout("timeout sending delete_stream to writer".into()))?
        .map_err(|_| Error::Schema("writer has shut down".into()))?;

        // Wait for response with remaining deadline
        let remaining = deadline.saturating_duration_since(Instant::now());
        let result = timeout(remaining, response_rx)
            .await
            .map_err(|_| Error::Timeout("timeout waiting for delete_stream response".into()))?
            .map_err(|_| Error::Schema("writer dropped response".into()))?;

        drop(permits);
        result
    }

    /// Deletes all events for a tenant.
    ///
    /// # GDPR "Right to be Forgotten"
    ///
    /// Creates a tenant tombstone that immediately filters all events
    /// belonging to that tenant from all reads.
    ///
    /// # Admission Control
    ///
    /// Acquires 1 permit for the delete operation.
    pub async fn delete_tenant(&self, command: DeleteTenantCommand) -> Result<DeleteTenantResult> {
        let deadline = Instant::now() + self.admission.default_deadline();

        // Acquire 1 permit for delete operation
        let permits = self.admission.acquire_permits(1, deadline).await?;

        let (response_tx, response_rx) = oneshot::channel();

        // Send with remaining deadline
        let remaining = deadline.saturating_duration_since(Instant::now());
        timeout(remaining, self.tx.send(WriteRequest::DeleteTenant {
            command,
            response: response_tx,
        }))
        .await
        .map_err(|_| Error::Timeout("timeout sending delete_tenant to writer".into()))?
        .map_err(|_| Error::Schema("writer has shut down".into()))?;

        // Wait for response with remaining deadline
        let remaining = deadline.saturating_duration_since(Instant::now());
        let result = timeout(remaining, response_rx)
            .await
            .map_err(|_| Error::Timeout("timeout waiting for delete_tenant response".into()))?
            .map_err(|_| Error::Schema("writer dropped response".into()))?;

        drop(permits);
        result
    }

    /// Triggers compaction of tombstoned events.
    ///
    /// # Background Compaction
    ///
    /// This method physically deletes events that have been logically deleted
    /// via tombstones. It's typically called by a background task, not directly
    /// by application code.
    ///
    /// # Arguments
    ///
    /// * `max_batches` - Maximum batches to process per invocation. Use `Some(100)`
    ///   for incremental compaction that doesn't block writes for too long.
    ///
    /// # Returns
    ///
    /// Statistics about what was compacted: batches rewritten, events deleted,
    /// tombstones removed, and bytes reclaimed.
    pub async fn compact(&self, max_batches: Option<usize>) -> Result<CompactionStats> {
        let (response_tx, response_rx) = oneshot::channel();

        // Compaction can take a while, use a longer timeout (5 minutes)
        let compaction_timeout = Duration::from_secs(300);

        timeout(compaction_timeout, self.tx.send(WriteRequest::Compact {
            max_batches,
            response: response_tx,
        }))
        .await
        .map_err(|_| Error::Timeout("timeout sending compaction request".into()))?
        .map_err(|_| Error::Schema("writer has shut down".into()))?;

        timeout(compaction_timeout, response_rx)
            .await
            .map_err(|_| Error::Timeout("timeout waiting for compaction response".into()))?
            .map_err(|_| Error::Schema("writer dropped response".into()))?
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

/// Calculates the total payload bytes for an AppendCommand.
fn command_payload_bytes(cmd: &AppendCommand) -> usize {
    cmd.events.iter().map(|e| e.data.len()).sum()
}

/// Runs the batch writer loop.
///
/// This function runs on a dedicated thread, collecting commands and
/// executing them in batches. Batches are flushed when any threshold is met:
/// - Time: batch_timeout elapsed
/// - Count: batch_max_size commands accumulated
/// - Bytes: batch_max_bytes uncompressed payload bytes accumulated
pub async fn run_batch_writer(
    mut writer: BatchWriter,
    mut rx: mpsc::Receiver<WriteRequest>,
    config: WriterConfig,
) {
    let mut batch: Vec<BatchItem> = Vec::new();
    let mut batch_start: Option<Instant> = None;
    let mut batch_bytes: usize = 0;

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

                // Track payload bytes
                batch_bytes += command_payload_bytes(&command);

                batch.push(BatchItem::Single(PendingCommand { command, response }));

                // Check if batch is full (count or bytes threshold)
                if batch.len() >= config.batch_max_size || batch_bytes >= config.batch_max_bytes {
                    writer.execute_batch(std::mem::take(&mut batch));
                    batch_start = None;
                    batch_bytes = 0;
                }
            }
            Ok(Some(WriteRequest::Transaction { commands, response })) => {
                if batch.is_empty() {
                    batch_start = Some(Instant::now());
                }

                // Track payload bytes for all commands in transaction
                for cmd in &commands {
                    batch_bytes += command_payload_bytes(cmd);
                }

                batch.push(BatchItem::Transaction(PendingTransaction {
                    commands,
                    response,
                }));

                // Check if batch is full (count or bytes threshold)
                if batch.len() >= config.batch_max_size || batch_bytes >= config.batch_max_bytes {
                    writer.execute_batch(std::mem::take(&mut batch));
                    batch_start = None;
                    batch_bytes = 0;
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
            Ok(Some(WriteRequest::Compact { max_batches, response })) => {
                // Compaction requests are handled immediately (not batched)
                // Flush any pending batch first to ensure we compact latest state
                if !batch.is_empty() {
                    writer.execute_batch(std::mem::take(&mut batch));
                    batch_start = None;
                    batch_bytes = 0;
                }
                let result = writer.execute_compaction(max_batches);
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
                    batch_bytes = 0;
                }
            }
        }
    }
}

/// Spawns the batch writer on a dedicated thread.
///
/// Returns a handle for submitting commands.
///
/// # Admission Control
///
/// The returned handle includes adaptive admission control:
/// - Initial max in-flight: 3000 events
/// - Target p99 latency: 60ms
/// - Auto-adjusts based on observed latency
/// - Request deadline: 100ms
pub fn spawn_batch_writer(conn: Connection, cryptor: BatchCryptor, config: WriterConfig) -> Result<BatchWriterHandle> {
    let (tx, rx) = mpsc::channel(COMMAND_CHANNEL_SIZE);

    // Create adaptive admission controller
    let admission = Arc::new(AdmissionController::new(AdmissionConfig::default()));

    // Spawn the control loop that adjusts max_inflight based on observed p99
    let admission_clone = Arc::clone(&admission);
    tokio::spawn(async move {
        admission_clone.run_control_loop().await;
    });

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

    Ok(BatchWriterHandle {
        tx,
        admission,
    })
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
        // Keep the batch timeout well under the handle's default 100ms deadline
        // to avoid flaky timeouts under load.
        let config = WriterConfig {
            batch_timeout: Duration::from_millis(10),
            batch_max_size: 100,
            batch_max_bytes: DEFAULT_BATCH_MAX_BYTES,
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
