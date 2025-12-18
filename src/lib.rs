//! # SpiteDB - Event Store Runtime
//!
//! SpiteDB is a production-grade event store built on SQLite. It provides:
//!
//! - **Event sourcing primitives**: streams, revisions, ordered global log
//! - **High-throughput writes**: via group commit and batching
//! - **Exactly-once semantics**: through command idempotency
//! - **High availability**: with automatic failover and epoch-based fencing
//! - **GDPR compliance**: tombstones and compaction for data deletion
//!
//! ## Architecture Overview
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        Async API Layer                          │
//! │                    (append, read, subscribe)                    │
//! └─────────────────────────────┬───────────────────────────────────┘
//!                               │
//!                               ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                       Writer Actor                              │
//! │              (single thread, owns write connection)             │
//! │                                                                 │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
//! │  │ Group Commit│  │  SAVEPOINT  │  │  In-Memory State        │ │
//! │  │   Batcher   │  │  Isolation  │  │  (stream heads, pos)    │ │
//! │  └─────────────┘  └─────────────┘  └─────────────────────────┘ │
//! └─────────────────────────────┬───────────────────────────────────┘
//!                               │
//!                               ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                         SQLite                                  │
//! │                   (durable storage)                             │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Core Invariants
//!
//! These invariants are enforced throughout the codebase and must never be violated:
//!
//! 1. **Single logical writer**: Only one node may write at any time
//! 2. **Durable ordering**: `global_pos` strictly increases, never reused
//! 3. **Stream correctness**: `stream_rev` strictly increases per stream, no gaps
//! 4. **Memory safety**: In-memory state may lag disk, but never leads disk
//! 5. **Exactly-once**: Duplicate commands return cached results, never duplicate events
//!
//! ## Module Organization
//!
//! - [`error`]: Custom error types for all failure modes
//! - [`schema`]: SQLite DDL and database initialization
//! - [`types`]: Domain types (StreamId, Event, GlobalPos, etc.)
//! - [`codec`]: Event batch encoding/decoding and checksums
//! - [`writer`]: Batch writer with group commit and SAVEPOINT isolation
//! - [`reader`]: Read operations with pooled connections
//! - [`api`]: Async API (main entry point)
//!
//! Future modules (not yet implemented):
//! - `ha`: High availability and epoch fencing

// =============================================================================
// Module Declarations
// =============================================================================
// Rust pattern: `mod` declares a module, making its contents available.
// Public modules (`pub mod`) are part of the library's API.
// Private modules are internal implementation details.

/// Error types for SpiteDB operations.
///
/// This module defines all error variants that can occur during database
/// operations. Using a single error enum simplifies error handling for callers.
pub mod error;

/// SQLite schema definitions and database initialization.
///
/// This module contains the DDL statements for all tables and the logic
/// to initialize a new database or verify an existing one.
pub mod schema;

/// Domain types for event sourcing.
///
/// This module defines the core types: streams, events, positions, revisions,
/// and commands. Uses the newtype pattern for type safety.
pub mod types;

/// Event batch encoding and decoding.
///
/// This module provides the codec for encoding events into batches and decoding
/// them back. The batch format is designed for efficient storage and retrieval.
pub mod codec;

/// Cryptographic primitives for batch encryption.
///
/// This module provides compression (Zstd level 1) and encryption (AES-256-GCM)
/// for event batches. All batches are compressed and encrypted by default.
pub mod crypto;

/// Read operations for SpiteDB.
///
/// This module provides read operations using direct SQL queries. It ensures
/// readers always see the latest committed data via WAL mode.
pub mod reader;

/// Async API for SpiteDB.
///
/// This module provides the public async interface using Tokio. It wraps the
/// synchronous storage layer with async primitives, enabling non-blocking
/// usage from async applications.
///
/// The main entry point is [`SpiteDB`](api::SpiteDB).
pub mod api;

/// Batch writer with group commit.
///
/// This module implements high-throughput batched writes using SQLite's
/// SAVEPOINT mechanism for per-command isolation within a single transaction.
/// Commands are collected over a configurable time window (default 10ms) and
/// executed together, amortizing the cost of fsync.
///
/// Key features:
/// - Group commit for throughput (many commands, one fsync)
/// - SAVEPOINT isolation (conflict in cmd2 doesn't affect cmd1 or cmd3)
/// - Transaction API for guaranteed same-batch writes across streams
/// - Staged/committed state separation for memory safety
pub mod writer;


/// Tombstones for GDPR-compliant deletion.
///
/// This module implements logical deletion via tombstones. When events are
/// "deleted", a tombstone record is created that causes those events to be
/// filtered from all reads immediately. Physical deletion happens during
/// offline compaction.
///
/// Key features:
/// - Stream tombstones (delete revision ranges within a stream)
/// - Tenant tombstones (delete all data for an organization)
/// - Immediate read filtering (events disappear from reads instantly)
/// - Deferred physical deletion (compaction job interface)
///
/// See [`tombstones::CompactionJob`] for the compaction interface.
pub mod tombstones;

// =============================================================================
// Re-exports
// =============================================================================
// Rust pattern: Re-export commonly used types at the crate root for convenience.
// Users can write `use spitedb::Error` instead of `use spitedb::error::Error`.

pub use api::SpiteDB;
pub use error::{Error, Result};
pub use schema::Database;
pub use writer::{
    BatchWriterHandle, TransactionBuilder, WriterConfig, spawn_batch_writer,
    // Admission control types
    AdmissionConfig, AdmissionController, AdmissionMetrics, MetricsSnapshot, LatencyTracker,
};

// Re-export commonly used types from the types module
pub use types::{
    AppendCommand, AppendResult, CollisionSlot, CommandId, Event, EventData, GlobalPos,
    StreamHash, StreamId, StreamRev, Tenant, TenantHash,
    // Tombstone types
    DeleteStreamCommand, DeleteStreamResult, DeleteTenantCommand, DeleteTenantResult,
    StreamTombstone, TenantTombstone,
};


// Re-export tombstone utilities
pub use tombstones::{CompactionJob, CompactionStats};
