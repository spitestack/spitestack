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
///
/// # Fields
///
/// - `event_type`: Optional classification (e.g., "OrderCreated", "UserUpdated")
/// - `data`: The event payload as bytes (JSON, protobuf, etc.)
/// - `metadata`: Optional additional context (correlation IDs, user info, etc.)
#[derive(Debug, Clone)]
pub struct EventData {
    /// The type of event, for filtering and routing.
    ///
    /// Optional but recommended. Examples:
    /// - "OrderCreated"
    /// - "user.updated"
    /// - "inventory/item-reserved"
    pub event_type: Option<String>,

    /// The event payload.
    ///
    /// SpiteDB is payload-agnostic - it's just bytes. The client chooses
    /// the serialization format (JSON, protobuf, messagepack, etc.).
    pub data: Vec<u8>,

    /// Optional metadata about the event.
    ///
    /// Useful for cross-cutting concerns that shouldn't be in the domain event:
    /// - Correlation ID for distributed tracing
    /// - User ID who triggered the event
    /// - Causation ID (which event caused this one)
    pub metadata: Option<Vec<u8>>,
}

impl EventData {
    /// Creates a new event with just data, no type or metadata.
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self {
            event_type: None,
            data: data.into(),
            metadata: None,
        }
    }

    /// Creates a new event with a type.
    pub fn with_type(event_type: impl Into<String>, data: impl Into<Vec<u8>>) -> Self {
        Self {
            event_type: Some(event_type.into()),
            data: data.into(),
            metadata: None,
        }
    }

    /// Adds metadata to this event (builder pattern).
    pub fn with_metadata(mut self, metadata: impl Into<Vec<u8>>) -> Self {
        self.metadata = Some(metadata.into());
        self
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

    /// Revision within the stream.
    pub stream_rev: StreamRev,

    /// When the event was stored (Unix milliseconds).
    pub timestamp_ms: u64,

    /// The event type, if provided.
    pub event_type: Option<String>,

    /// The event payload.
    pub data: Vec<u8>,

    /// The event metadata, if provided.
    pub metadata: Option<Vec<u8>>,
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
#[derive(Debug, Clone)]
pub struct AppendCommand {
    /// Unique identifier for idempotency.
    ///
    /// If this command was already processed, we return the cached result.
    pub command_id: CommandId,

    /// The stream to append to.
    pub stream_id: StreamId,

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
    /// Creates a new append command.
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
        assert!(!events.is_empty(), "AppendCommand must have at least one event");
        Self {
            command_id: command_id.into(),
            stream_id: stream_id.into(),
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
    fn test_event_data_builders() {
        let simple = EventData::new(b"hello".to_vec());
        assert!(simple.event_type.is_none());
        assert_eq!(simple.data, b"hello");

        let typed = EventData::with_type("Greeting", b"hello".to_vec());
        assert_eq!(typed.event_type, Some("Greeting".to_string()));

        let with_meta = EventData::new(b"hello".to_vec())
            .with_metadata(b"meta".to_vec());
        assert_eq!(with_meta.metadata, Some(b"meta".to_vec()));
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
