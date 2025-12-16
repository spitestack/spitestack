//! # Domain Types for SpiteDB
//!
//! This module defines the core types used throughout SpiteDB. These types
//! model the event sourcing domain: streams, events, positions, and commands.
//!
//! ## Design Philosophy: Newtypes for Safety
//!
//! We use the "newtype pattern" extensively - wrapping primitive types in
//! single-field structs. This provides:
//!
//! - **Type safety**: Can't accidentally pass a `GlobalPos` where `StreamRev` is expected
//! - **Self-documenting code**: Function signatures tell you what they expect
//! - **Encapsulation**: Can add validation or change representation later
//!
//! ## Example
//!
//! ```rust
//! use spitedb::types::{GlobalPos, StreamRev};
//!
//! fn example(pos: GlobalPos, rev: StreamRev) {
//!     // These are different types - can't mix them up!
//!     // pos == rev  // Won't compile
//! }
//! ```
//!
//! ## Invariants
//!
//! These types encode important invariants from CLAUDE.md:
//!
//! - [`GlobalPos`]: Strictly increasing, never reused, never zero
//! - [`StreamRev`]: Strictly increasing per stream, starts at 1, no gaps
//! - [`StreamHash`]: Deterministic hash of [`StreamId`], used for fast lookups

use std::fmt;

// =============================================================================
// Stream Identification
// =============================================================================

/// A human-readable identifier for an event stream.
///
/// # What is a Stream?
///
/// In event sourcing, a stream is a sequence of events for a single entity.
/// For example:
/// - `"user-12345"` - all events for user 12345
/// - `"order-abc-123"` - all events for order abc-123
/// - `"account-checking-999"` - all events for checking account 999
///
/// # Rust Pattern: Newtype
///
/// `StreamId` wraps a `String`. This means:
/// - Type safety: `fn read_stream(id: StreamId)` won't accept a random string
/// - We can add validation (e.g., max length, allowed characters)
/// - We control the API surface
///
/// # Example
///
/// ```rust
/// use spitedb::types::StreamId;
///
/// let stream = StreamId::new("user-12345");
/// println!("Stream: {}", stream);  // "user-12345"
///
/// // Get the hash for database lookups
/// let hash = stream.hash();
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamId(String);

impl StreamId {
    /// Creates a new stream ID from a string.
    ///
    /// # Arguments
    ///
    /// * `id` - The stream identifier. Can be any string, but shorter is better
    ///   for storage efficiency.
    ///
    /// # Rust Pattern: impl Into<String>
    ///
    /// Accepting `impl Into<String>` means callers can pass:
    /// - `&str`: `StreamId::new("user-123")`
    /// - `String`: `StreamId::new(my_string)`
    ///
    /// The conversion happens automatically.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the string representation of this stream ID.
    ///
    /// # Rust Pattern: &str vs String
    ///
    /// Returns `&str` (borrowed) instead of `String` (owned) because:
    /// - No allocation needed
    /// - Caller can clone if they need ownership
    /// - More flexible (can be used with functions expecting &str)
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Computes the hash of this stream ID for database lookups.
    ///
    /// # Why Hash?
    ///
    /// The database stores `stream_hash` (integer) instead of `stream_id` (string)
    /// in the event_index for efficiency:
    /// - Integers are faster to compare than strings
    /// - Integers take fixed space (8 bytes) vs variable-length strings
    /// - B-tree operations are faster with fixed-size keys
    ///
    /// # Algorithm: XXH3
    ///
    /// We use XXH3-64, the fastest hash algorithm for modern CPUs:
    /// - Uses SIMD (SSE2/AVX2/NEON) when available
    /// - ~30 GB/s on modern hardware
    /// - Stable specification - guaranteed consistent across versions/platforms
    /// - Excellent distribution (passes SMHasher tests)
    ///
    /// # Stability Guarantee
    ///
    /// The xxHash algorithm is formally specified. The same stream_id will
    /// produce the same hash forever, across all platforms and versions.
    /// This is critical for database compatibility.
    pub fn hash(&self) -> StreamHash {
        // xxh3_64 produces a u64, we cast to i64 for SQLite INTEGER storage
        StreamHash(xxhash_rust::xxh3::xxh3_64(self.0.as_bytes()) as i64)
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for StreamId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for StreamId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// A hash of a [`StreamId`] for efficient database lookups.
///
/// # Why a Separate Type?
///
/// We could just use `i64`, but a newtype:
/// - Documents intent: this is specifically a stream hash
/// - Prevents mixing up with other integers (global_pos, stream_rev, etc.)
/// - Allows us to change the hash algorithm later
///
/// # Storage
///
/// Stored as INTEGER in SQLite (8 bytes, signed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamHash(i64);

// =============================================================================
// Tenant Identification (Multi-Tenancy)
// =============================================================================

/// A tenant identifier for multi-tenant isolation.
///
/// # What is a Tenant?
///
/// In multi-tenant systems, a tenant represents an isolated customer or organization.
/// Each stream belongs to exactly one tenant. Example tenant IDs:
/// - `"acme-corp"` - all streams for Acme Corporation
/// - `"tenant-123"` - all streams for tenant 123
/// - `"default"` - the default tenant for single-tenant applications
///
/// # Storage
///
/// Like [`StreamId`], tenants are hashed to i64 for efficient storage and comparison.
/// The original tenant string is not stored - clients must track their own mapping
/// if needed.
///
/// # Default Tenant
///
/// Single-tenant applications can omit the tenant parameter, which defaults to
/// "default". This ensures backward compatibility while enabling multi-tenancy.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Tenant(String);

impl Tenant {
    /// The default tenant string for single-tenant applications.
    pub const DEFAULT_STR: &'static str = "default";

    /// Creates a new tenant from a string.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Creates the default tenant for single-tenant applications.
    pub fn default_tenant() -> Self {
        Self(Self::DEFAULT_STR.to_string())
    }

    /// Returns the string representation of this tenant.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Computes the hash of this tenant for database storage.
    ///
    /// Uses the same XXH3-64 algorithm as [`StreamId::hash`] for consistency.
    pub fn hash(&self) -> TenantHash {
        TenantHash(xxhash_rust::xxh3::xxh3_64(self.0.as_bytes()) as i64)
    }
}

impl fmt::Display for Tenant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for Tenant {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for Tenant {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl Default for Tenant {
    fn default() -> Self {
        Self::default_tenant()
    }
}

/// A hash of a [`Tenant`] for efficient database storage.
///
/// # Why Hash Tenants?
///
/// Same rationale as [`StreamHash`]:
/// - Fixed 8 bytes regardless of tenant string length
/// - Fast integer comparisons in queries
/// - Consistent with stream_hash pattern
///
/// # Collision Handling
///
/// Unlike streams (which can number in millions), tenants are typically
/// few (dozens to thousands). With 64-bit hashes, collisions are negligible
/// and we don't implement collision slots for tenants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct TenantHash(i64);

impl TenantHash {
    /// Returns the hash of the default tenant.
    pub fn default_hash() -> Self {
        Tenant::default_tenant().hash()
    }

    /// Creates a TenantHash from a raw integer value.
    pub fn from_raw(value: i64) -> Self {
        Self(value)
    }

    /// Returns the raw integer value for database storage.
    pub fn as_raw(&self) -> i64 {
        self.0
    }
}

impl fmt::Display for TenantHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// A slot for disambiguating hash collisions.
///
/// # Purpose
///
/// When two different stream IDs hash to the same value (rare, ~1 in 37M at 1M streams),
/// we assign different collision slots to distinguish them:
///
/// ```text
/// "user-123"  → hash X, slot 0
/// "order-abc" → hash X, slot 1  (collision)
/// ```
///
/// # Storage
///
/// Stored as INTEGER in SQLite. SQLite stores small integers efficiently:
/// - Value 0: ~1 byte
/// - Value 1-127: ~2 bytes
///
/// Since 99.99%+ of streams have slot 0, storage overhead is minimal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct CollisionSlot(u16);

impl CollisionSlot {
    /// The default slot (0) for streams with no hash collision.
    pub const FIRST: CollisionSlot = CollisionSlot(0);

    /// Creates a CollisionSlot from a raw value.
    pub fn from_raw(value: u16) -> Self {
        Self(value)
    }

    /// Returns the raw value for database storage.
    pub fn as_raw(&self) -> u16 {
        self.0
    }

    /// Returns the next slot (for assigning to a colliding stream).
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for CollisionSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl StreamHash {
    /// Creates a StreamHash from a raw integer value.
    ///
    /// # When to Use
    ///
    /// Primarily for reading from the database. Normally you'd compute the hash
    /// via [`StreamId::hash()`].
    pub fn from_raw(value: i64) -> Self {
        Self(value)
    }

    /// Returns the raw integer value for database storage.
    pub fn as_raw(&self) -> i64 {
        self.0
    }
}

impl fmt::Display for StreamHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

// =============================================================================
// Positions and Revisions
// =============================================================================

/// A position in the global event log.
///
/// # Invariants (from CLAUDE.md)
///
/// - Strictly increases with each event written
/// - Never reused, even after deletes
/// - Events in a single append are contiguous
/// - Starts at 1 (zero is invalid/sentinel)
///
/// # Use Cases
///
/// - Subscriptions: "give me events starting at position X"
/// - Idempotency: "this command wrote events at positions X through Y"
/// - Ordering: global total order across all streams
///
/// # Rust Pattern: Copy
///
/// We derive `Copy` because `GlobalPos` is small (8 bytes) and frequently
/// passed around. `Copy` means it's copied by value instead of moved,
/// which is more ergonomic for small types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GlobalPos(u64);

impl GlobalPos {
    /// The first valid position (1, not 0).
    ///
    /// # Why Start at 1?
    ///
    /// Starting at 1 lets us use 0 as a sentinel value meaning "no position"
    /// or "start from beginning" in APIs without needing Option<GlobalPos>.
    pub const FIRST: GlobalPos = GlobalPos(1);

    /// Creates a GlobalPos from a raw value.
    ///
    /// # Panics
    ///
    /// Panics if `value` is 0. Use [`GlobalPos::from_raw_unchecked`] if you
    /// need to bypass this check (e.g., reading from trusted database).
    pub fn from_raw(value: u64) -> Self {
        assert!(value > 0, "GlobalPos cannot be zero");
        Self(value)
    }

    /// Creates a GlobalPos without checking for zero.
    ///
    /// # Safety (Logical, not Memory)
    ///
    /// Caller must ensure `value > 0`. Using zero will violate invariants.
    /// This exists for performance-critical paths reading from the database.
    pub fn from_raw_unchecked(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw u64 value for database storage.
    pub fn as_raw(&self) -> u64 {
        self.0
    }

    /// Returns the next position.
    ///
    /// # Overflow
    ///
    /// At u64::MAX events, you've stored 18 quintillion events. If you're
    /// writing 1 million events per second, this takes 584,000 years to overflow.
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    /// Adds an offset to this position.
    ///
    /// Useful for calculating the end position after appending N events.
    pub fn add(&self, count: u64) -> Self {
        Self(self.0 + count)
    }
}

impl fmt::Display for GlobalPos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A revision number within a stream.
///
/// # Invariants (from CLAUDE.md)
///
/// - Strictly increases within a stream
/// - No gaps (1, 2, 3, ... not 1, 2, 5)
/// - Starts at 1 for the first event
/// - Used for optimistic concurrency (expected_rev)
///
/// # Relationship to GlobalPos
///
/// - `GlobalPos`: Position in the total order across ALL streams
/// - `StreamRev`: Position within a SINGLE stream
///
/// An event has both: "event at global position 1000 is revision 5 of stream X"
///
/// # Rust Pattern: Distinct from GlobalPos
///
/// Even though both are u64 wrappers, they're different types. This prevents
/// bugs like `read_stream(stream_id, global_pos)` when you meant `stream_rev`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamRev(u64);

impl StreamRev {
    /// The first revision in a stream (1).
    pub const FIRST: StreamRev = StreamRev(1);

    /// Sentinel value meaning "stream does not exist" or "no events yet".
    ///
    /// When checking expected_rev:
    /// - `NONE` means "stream should not exist, I'm creating it"
    /// - Any other value means "stream should be at this exact revision"
    pub const NONE: StreamRev = StreamRev(0);

    /// Creates a StreamRev from a raw value.
    pub fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw u64 value.
    pub fn as_raw(&self) -> u64 {
        self.0
    }

    /// Returns the next revision.
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    /// Adds an offset to this revision.
    pub fn add(&self, count: u64) -> Self {
        Self(self.0 + count)
    }

    /// Returns true if this represents "no events" (revision 0).
    pub fn is_none(&self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for StreamRev {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 == 0 {
            write!(f, "none")
        } else {
            write!(f, "{}", self.0)
        }
    }
}

// =============================================================================
// Command Identification (for Idempotency)
// =============================================================================

/// A unique identifier for a client command.
///
/// # Exactly-Once Semantics
///
/// Every append command includes a `CommandId`. If the same command_id is seen
/// twice, we return the cached result instead of re-processing. This handles:
///
/// - Client retries after timeout (original may have succeeded)
/// - Network duplicates
/// - Client bugs
///
/// # Format
///
/// Typically a UUID, but any unique string works. The client is responsible
/// for generating unique IDs.
///
/// # Example
///
/// ```rust
/// use spitedb::types::CommandId;
///
/// // Client generates a unique ID per command
/// let cmd_id = CommandId::new("550e8400-e29b-41d4-a716-446655440000");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommandId(String);

impl CommandId {
    /// Creates a new command ID.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CommandId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for CommandId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for CommandId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

// =============================================================================
// Events
// =============================================================================

/// An event to be appended to a stream.
///
/// This is the "input" form - what the client provides when appending.
/// It doesn't have position/revision yet (those are assigned during append).
#[derive(Debug, Clone)]
pub struct EventData {
    /// The event payload.
    ///
    /// SpiteDB is payload-agnostic - it's just bytes. The client chooses
    /// the serialization format (JSON, protobuf, messagepack, etc.).
    pub data: Vec<u8>,
}

impl EventData {
    /// Creates a new event with the given data.
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self { data: data.into() }
    }
}

/// A stored event with full position information.
///
/// This is the "output" form - what you get when reading events.
/// It includes all the position/revision info assigned during append.
#[derive(Debug, Clone)]
pub struct Event {
    /// Position in the global log.
    pub global_pos: GlobalPos,

    /// The stream this event belongs to.
    pub stream_id: StreamId,

    /// The tenant this event belongs to (hashed).
    pub tenant_hash: TenantHash,

    /// Revision within the stream.
    pub stream_rev: StreamRev,

    /// When the event was stored (Unix milliseconds).
    /// This is the batch creation time.
    pub timestamp_ms: u64,

    /// The event payload.
    pub data: Vec<u8>,
}

// =============================================================================
// Commands and Results
// =============================================================================

/// A command to append events to a stream.
///
/// # Optimistic Concurrency
///
/// The `expected_rev` field implements optimistic concurrency control:
/// - If `expected_rev` matches the current stream revision, append succeeds
/// - If it doesn't match, append fails with a conflict error
///
/// This prevents lost updates when multiple clients append to the same stream.
///
/// # Multi-Tenancy
///
/// The `tenant` field specifies which tenant this stream belongs to. If not
/// specified, it defaults to the "default" tenant for backward compatibility.
#[derive(Debug, Clone)]
pub struct AppendCommand {
    /// Unique identifier for idempotency.
    ///
    /// If this command was already processed, we return the cached result.
    pub command_id: CommandId,

    /// The stream to append to.
    pub stream_id: StreamId,

    /// The tenant this stream belongs to.
    ///
    /// Defaults to "default" for single-tenant applications.
    pub tenant: Tenant,

    /// Expected current revision of the stream.
    ///
    /// - `StreamRev::NONE` (0): Expect stream doesn't exist (creating it)
    /// - Any other value: Expect stream is at exactly this revision
    pub expected_rev: StreamRev,

    /// Events to append.
    ///
    /// Must contain at least one event.
    pub events: Vec<EventData>,
}

impl AppendCommand {
    /// Creates a new append command with the default tenant.
    ///
    /// # Arguments
    ///
    /// * `command_id` - Unique ID for idempotency
    /// * `stream_id` - Target stream
    /// * `expected_rev` - Expected current revision (for optimistic concurrency)
    /// * `events` - Events to append (must be non-empty)
    ///
    /// # Panics
    ///
    /// Panics if `events` is empty.
    pub fn new(
        command_id: impl Into<CommandId>,
        stream_id: impl Into<StreamId>,
        expected_rev: StreamRev,
        events: Vec<EventData>,
    ) -> Self {
        Self::new_with_tenant(command_id, stream_id, Tenant::default_tenant(), expected_rev, events)
    }

    /// Creates a new append command with a specific tenant.
    ///
    /// # Arguments
    ///
    /// * `command_id` - Unique ID for idempotency
    /// * `stream_id` - Target stream
    /// * `tenant` - The tenant this stream belongs to
    /// * `expected_rev` - Expected current revision (for optimistic concurrency)
    /// * `events` - Events to append (must be non-empty)
    ///
    /// # Panics
    ///
    /// Panics if `events` is empty.
    pub fn new_with_tenant(
        command_id: impl Into<CommandId>,
        stream_id: impl Into<StreamId>,
        tenant: impl Into<Tenant>,
        expected_rev: StreamRev,
        events: Vec<EventData>,
    ) -> Self {
        assert!(!events.is_empty(), "AppendCommand must have at least one event");
        Self {
            command_id: command_id.into(),
            stream_id: stream_id.into(),
            tenant: tenant.into(),
            expected_rev,
            events,
        }
    }
}

/// The result of a successful append operation.
///
/// Contains the positions assigned to the appended events.
#[derive(Debug, Clone)]
pub struct AppendResult {
    /// First global position assigned.
    pub first_pos: GlobalPos,

    /// Last global position assigned (first_pos + event_count - 1).
    pub last_pos: GlobalPos,

    /// First stream revision assigned.
    pub first_rev: StreamRev,

    /// Last stream revision assigned.
    pub last_rev: StreamRev,
}

impl AppendResult {
    /// Creates a new append result.
    pub fn new(
        first_pos: GlobalPos,
        last_pos: GlobalPos,
        first_rev: StreamRev,
        last_rev: StreamRev,
    ) -> Self {
        Self {
            first_pos,
            last_pos,
            first_rev,
            last_rev,
        }
    }

    /// Returns the number of events that were appended.
    pub fn event_count(&self) -> u64 {
        self.last_pos.as_raw() - self.first_pos.as_raw() + 1
    }
}

// =============================================================================
// Cache Types
// =============================================================================

/// Maximum number of commands to keep in the LRU cache.
///
/// At 100 bytes per entry (command_id + positions + timestamp), 100K entries
/// uses ~10MB of memory. This covers several hours of high-throughput operation.
pub const COMMAND_CACHE_MAX_ENTRIES: usize = 100_000;

/// Time-to-live for cached commands in milliseconds (12 hours).
///
/// Commands older than this are considered expired and will be evicted.
/// 12 hours provides generous margin for network partitions and retries.
pub const COMMAND_CACHE_TTL_MS: u64 = 12 * 60 * 60 * 1000;

/// Cached information about a stream's current state.
///
/// Used by the writer and reader to track stream heads in memory,
/// avoiding database lookups for conflict detection.
#[derive(Debug, Clone)]
pub struct StreamHeadEntry {
    /// The original stream ID string.
    pub stream_id: StreamId,
    /// The tenant this stream belongs to (hashed).
    pub tenant_hash: TenantHash,
    /// Collision slot (usually 0).
    pub collision_slot: CollisionSlot,
    /// Current revision (number of events in stream).
    pub last_rev: StreamRev,
    /// Global position of the last event.
    pub last_pos: GlobalPos,
}

/// Cached result of a processed command for idempotency.
///
/// When a client retries a command (same command_id), we return the cached
/// result instead of re-processing. This ensures exactly-once semantics.
#[derive(Debug, Clone)]
pub struct CommandCacheEntry {
    /// First global position written by this command.
    pub first_pos: GlobalPos,
    /// Last global position written by this command.
    pub last_pos: GlobalPos,
    /// First stream revision written.
    pub first_rev: StreamRev,
    /// Last stream revision written.
    pub last_rev: StreamRev,
    /// When this command was processed (Unix milliseconds).
    pub created_ms: u64,
}

impl CommandCacheEntry {
    /// Converts the cache entry to an AppendResult.
    pub fn to_append_result(&self) -> AppendResult {
        AppendResult::new(self.first_pos, self.last_pos, self.first_rev, self.last_rev)
    }
}

// =============================================================================
// Tombstone Types (GDPR Deletion)
// =============================================================================

/// Command to delete events from a stream (create tombstone).
///
/// # GDPR Compliance
///
/// This command creates a tombstone record that marks events as logically deleted.
/// Events in the tombstoned range are immediately filtered from all reads.
///
/// # Idempotency
///
/// Like append commands, delete commands use a `command_id` for idempotency.
/// Retrying the same delete command returns the original result.
#[derive(Debug, Clone)]
pub struct DeleteStreamCommand {
    /// Unique identifier for idempotency.
    pub command_id: CommandId,

    /// The stream to delete events from.
    pub stream_id: StreamId,

    /// First revision to delete (inclusive). None = from the beginning.
    pub from_rev: Option<StreamRev>,

    /// Last revision to delete (inclusive). None = to the current head.
    pub to_rev: Option<StreamRev>,
}

impl DeleteStreamCommand {
    /// Creates a command to delete all events from a stream.
    pub fn delete_all(command_id: impl Into<CommandId>, stream_id: impl Into<StreamId>) -> Self {
        Self {
            command_id: command_id.into(),
            stream_id: stream_id.into(),
            from_rev: None,
            to_rev: None,
        }
    }

    /// Creates a command to delete a range of events from a stream.
    pub fn delete_range(
        command_id: impl Into<CommandId>,
        stream_id: impl Into<StreamId>,
        from_rev: StreamRev,
        to_rev: StreamRev,
    ) -> Self {
        Self {
            command_id: command_id.into(),
            stream_id: stream_id.into(),
            from_rev: Some(from_rev),
            to_rev: Some(to_rev),
        }
    }
}

/// Command to delete all events for a tenant.
///
/// # GDPR "Right to be Forgotten"
///
/// When an entire organization requests data deletion, this command marks
/// the tenant as deleted. All events belonging to that tenant are immediately
/// filtered from all reads.
#[derive(Debug, Clone)]
pub struct DeleteTenantCommand {
    /// Unique identifier for idempotency.
    pub command_id: CommandId,

    /// The tenant to delete.
    pub tenant: Tenant,
}

impl DeleteTenantCommand {
    /// Creates a new tenant delete command.
    pub fn new(command_id: impl Into<CommandId>, tenant: impl Into<Tenant>) -> Self {
        Self {
            command_id: command_id.into(),
            tenant: tenant.into(),
        }
    }
}

/// Result of a successful stream delete operation.
#[derive(Debug, Clone)]
pub struct DeleteStreamResult {
    /// The stream that was deleted from.
    pub stream_id: StreamId,

    /// First revision that was tombstoned.
    pub from_rev: StreamRev,

    /// Last revision that was tombstoned.
    pub to_rev: StreamRev,

    /// When the deletion was recorded (Unix milliseconds).
    pub deleted_ms: u64,
}

/// Result of a successful tenant delete operation.
#[derive(Debug, Clone)]
pub struct DeleteTenantResult {
    /// The hash of the deleted tenant.
    pub tenant_hash: TenantHash,

    /// When the deletion was recorded (Unix milliseconds).
    pub deleted_ms: u64,
}

/// A tombstone record marking deleted stream events.
///
/// Used internally for filtering events during reads.
#[derive(Debug, Clone)]
pub struct StreamTombstone {
    /// Hash of the stream.
    pub stream_hash: StreamHash,

    /// Collision slot (matches event_index).
    pub collision_slot: CollisionSlot,

    /// First revision that is tombstoned (inclusive).
    pub from_rev: StreamRev,

    /// Last revision that is tombstoned (inclusive).
    pub to_rev: StreamRev,

    /// When the deletion was recorded.
    pub deleted_ms: u64,
}

impl StreamTombstone {
    /// Checks if a given revision falls within this tombstone's range.
    pub fn contains(&self, rev: StreamRev) -> bool {
        rev.as_raw() >= self.from_rev.as_raw() && rev.as_raw() <= self.to_rev.as_raw()
    }
}

/// A tombstone record marking a deleted tenant.
#[derive(Debug, Clone)]
pub struct TenantTombstone {
    /// Hash of the deleted tenant.
    pub tenant_hash: TenantHash,

    /// When the deletion was recorded.
    pub deleted_ms: u64,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_id_creation() {
        let id = StreamId::new("user-123");
        assert_eq!(id.as_str(), "user-123");
        assert_eq!(id.to_string(), "user-123");
    }

    #[test]
    fn test_stream_id_from_conversions() {
        let from_str: StreamId = "test".into();
        let from_string: StreamId = String::from("test").into();
        assert_eq!(from_str, from_string);
    }

    #[test]
    fn test_stream_hash_deterministic() {
        let id = StreamId::new("user-123");
        let hash1 = id.hash();
        let hash2 = id.hash();
        assert_eq!(hash1, hash2, "hash should be deterministic");
    }

    #[test]
    fn test_stream_hash_different_for_different_ids() {
        let id1 = StreamId::new("user-123");
        let id2 = StreamId::new("user-456");
        assert_ne!(id1.hash(), id2.hash(), "different IDs should have different hashes");
    }

    #[test]
    fn test_global_pos_ordering() {
        let pos1 = GlobalPos::from_raw(1);
        let pos2 = GlobalPos::from_raw(2);
        assert!(pos1 < pos2);
        assert_eq!(pos1.next(), pos2);
    }

    #[test]
    #[should_panic(expected = "GlobalPos cannot be zero")]
    fn test_global_pos_zero_panics() {
        GlobalPos::from_raw(0);
    }

    #[test]
    fn test_global_pos_first() {
        assert_eq!(GlobalPos::FIRST.as_raw(), 1);
    }

    #[test]
    fn test_stream_rev_none() {
        assert!(StreamRev::NONE.is_none());
        assert!(!StreamRev::FIRST.is_none());
        assert_eq!(StreamRev::NONE.to_string(), "none");
    }

    #[test]
    fn test_stream_rev_ordering() {
        let rev1 = StreamRev::from_raw(1);
        let rev2 = StreamRev::from_raw(2);
        assert!(rev1 < rev2);
        assert_eq!(rev1.next(), rev2);
    }

    #[test]
    fn test_event_data_new() {
        let event = EventData::new(b"hello".to_vec());
        assert_eq!(event.data, b"hello");

        // Also works with Into<Vec<u8>>
        let event2 = EventData::new(vec![1, 2, 3]);
        assert_eq!(event2.data, vec![1, 2, 3]);
    }

    #[test]
    fn test_append_result_count() {
        let result = AppendResult::new(
            GlobalPos::from_raw(100),
            GlobalPos::from_raw(104),
            StreamRev::from_raw(1),
            StreamRev::from_raw(5),
        );
        assert_eq!(result.event_count(), 5);
    }

    #[test]
    #[should_panic(expected = "must have at least one event")]
    fn test_append_command_empty_events_panics() {
        AppendCommand::new("cmd-1", "stream-1", StreamRev::NONE, vec![]);
    }
}
