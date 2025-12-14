//! # Error Handling for SpiteDB
//!
//! This module defines the error types used throughout SpiteDB. We use a single
//! error enum ([`Error`]) to represent all possible failure modes, which simplifies
//! error handling for library users.
//!
//! ## Rust Pattern: thiserror
//!
//! We use the `thiserror` crate to derive `std::error::Error` implementations.
//! This provides:
//! - Automatic `Display` implementation from the `#[error(...)]` attributes
//! - Automatic `From` implementations from the `#[from]` attributes
//! - Proper error source chaining via `#[source]`
//!
//! ## Why a Single Error Type?
//!
//! Libraries commonly choose between:
//! 1. **Single enum** (our choice): Easy to match on, simple function signatures
//! 2. **Separate error types per module**: More precise, but verbose
//!
//! For SpiteDB, a single enum works well because:
//! - Most operations can fail in similar ways (SQLite errors, conflicts)
//! - Users typically want to handle errors uniformly (log and retry, or propagate)
//! - It keeps the API surface simple
//!
//! ## Error Categories
//!
//! Errors fall into these categories:
//!
//! | Category | Examples | Typical Response |
//! |----------|----------|------------------|
//! | Conflict | Version mismatch | Retry with fresh data |
//! | Duplicate | Command already processed | Return cached result |
//! | Internal | SQLite error, I/O error | Log and investigate |
//! | Fencing | Epoch expired | Stop writing, failover |

use thiserror::Error;

// =============================================================================
// Error Type
// =============================================================================

/// All errors that can occur in SpiteDB operations.
///
/// # Rust Pattern: Enum Variants
///
/// Each variant represents a distinct failure mode. The `#[error(...)]` attribute
/// defines the `Display` message shown when the error is printed.
///
/// # Example
///
/// ```rust,ignore
/// use spitedb::{Error, Result};
///
/// fn example() -> Result<()> {
///     // Errors can be created directly
///     let err = Error::Conflict {
///         stream_id: "user-123".to_string(),
///         expected: 5,
///         actual: 7,
///     };
///
///     // Or propagated with ?
///     some_operation()?;
///
///     Ok(())
/// }
/// ```
#[derive(Error, Debug)]
pub enum Error {
    // =========================================================================
    // Conflict Errors (Client can retry with updated data)
    // =========================================================================

    /// Optimistic concurrency conflict: the stream was modified since last read.
    ///
    /// # When This Happens
    ///
    /// When appending events, the client provides an `expected_rev` (the revision
    /// they believe is current). If another append happened in between, the actual
    /// revision won't match, and this error is returned.
    ///
    /// # Systems Concept: Optimistic Concurrency Control
    ///
    /// Rather than locking streams during reads (pessimistic), we allow concurrent
    /// reads and detect conflicts at write time (optimistic). This is more scalable
    /// but requires clients to handle conflicts.
    ///
    /// # Recovery
    ///
    /// 1. Re-read the stream to get current state
    /// 2. Re-apply business logic with new data
    /// 3. Retry the append with updated `expected_rev`
    #[error("conflict on stream '{stream_id}': expected revision {expected}, but found {actual}")]
    Conflict {
        /// The stream where the conflict occurred
        stream_id: String,
        /// The revision the client expected
        expected: u64,
        /// The actual current revision
        actual: u64,
    },

    // =========================================================================
    // Idempotency (Not an error, but a signal to return cached result)
    // =========================================================================

    /// Command was already processed; result should be retrieved from cache.
    ///
    /// # When This Happens
    ///
    /// Every append includes a `command_id`. If we see the same `command_id` twice,
    /// the command was already processed. This happens when:
    /// - Client retried after a timeout (but original succeeded)
    /// - Network delivered the request twice
    /// - Client bug sent duplicate requests
    ///
    /// # Systems Concept: Exactly-Once Semantics
    ///
    /// In distributed systems, "exactly once" delivery is impossible to guarantee
    /// at the network level. Instead, we achieve exactly-once *processing* by:
    /// 1. Making operations idempotent (same input → same output)
    /// 2. Storing the result keyed by `command_id`
    /// 3. Returning stored result for duplicates
    ///
    /// # Recovery
    ///
    /// This isn't really an error - look up the cached result and return it.
    #[error("duplicate command '{command_id}': already processed")]
    DuplicateCommand {
        /// The command ID that was already processed
        command_id: String,
    },

    // =========================================================================
    // Internal Errors (Investigate and fix)
    // =========================================================================

    /// SQLite operation failed.
    ///
    /// # When This Happens
    ///
    /// This wraps any error from the `rusqlite` crate:
    /// - Database file is locked by another process
    /// - Disk is full
    /// - Database file is corrupted
    /// - SQL syntax error (indicates a bug in SpiteDB)
    ///
    /// # Rust Pattern: #[from]
    ///
    /// The `#[from]` attribute automatically generates:
    /// ```rust,ignore
    /// impl From<rusqlite::Error> for Error {
    ///     fn from(err: rusqlite::Error) -> Self {
    ///         Error::Sqlite(err)
    ///     }
    /// }
    /// ```
    ///
    /// This enables the `?` operator to convert rusqlite errors automatically.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// Schema version mismatch or corruption detected.
    ///
    /// # When This Happens
    ///
    /// - Opening a database created by a newer SpiteDB version
    /// - Database file was modified outside SpiteDB
    /// - Migration failed partway through
    ///
    /// # Recovery
    ///
    /// May require manual intervention or restore from backup.
    #[error("schema error: {0}")]
    Schema(String),

    // =========================================================================
    // High Availability Errors (Stop writing, trigger failover)
    // =========================================================================

    /// This node's epoch has been superseded; it must stop writing.
    ///
    /// # When This Happens
    ///
    /// In HA mode, each leader holds an "epoch" number. When a new leader is
    /// promoted, it gets a higher epoch. The old leader discovers this when:
    /// - It tries to write and checks its epoch
    /// - A health check reveals a newer epoch exists
    ///
    /// # Systems Concept: Fencing
    ///
    /// Epochs prevent "split brain" where two nodes both think they're leader.
    /// The fencing token (epoch) is checked before every write. If superseded,
    /// the old leader must immediately stop to prevent divergent writes.
    ///
    /// # Recovery
    ///
    /// This node should stop accepting writes and let the new leader take over.
    #[error("epoch fenced: held epoch {held}, but current epoch is {current}")]
    EpochFenced {
        /// The epoch this node thought it held
        held: u64,
        /// The actual current epoch
        current: u64,
    },
}

// =============================================================================
// Result Type Alias
// =============================================================================

/// A `Result` type alias using [`Error`] as the error type.
///
/// # Rust Pattern: Type Aliases
///
/// Defining `type Result<T> = std::result::Result<T, Error>` means:
/// - Functions return `Result<Foo>` instead of `Result<Foo, Error>`
/// - Less typing, clearer intent
/// - Standard pattern used by most Rust libraries
///
/// # Example
///
/// ```rust,ignore
/// use spitedb::Result;
///
/// fn do_something() -> Result<String> {
///     // On success:
///     Ok("done".to_string())
///
///     // On failure (Error::Schema variant):
///     // Err(Error::Schema("bad version".to_string()))
/// }
/// ```
pub type Result<T> = std::result::Result<T, Error>;

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify error messages are formatted correctly.
    ///
    /// # Why Test Display?
    ///
    /// Error messages appear in logs and user output. Testing ensures they're
    /// readable and contain the relevant information.
    #[test]
    fn test_error_display() {
        let conflict = Error::Conflict {
            stream_id: "user-42".to_string(),
            expected: 5,
            actual: 7,
        };
        assert_eq!(
            conflict.to_string(),
            "conflict on stream 'user-42': expected revision 5, but found 7"
        );

        let duplicate = Error::DuplicateCommand {
            command_id: "cmd-abc".to_string(),
        };
        assert_eq!(
            duplicate.to_string(),
            "duplicate command 'cmd-abc': already processed"
        );

        let fenced = Error::EpochFenced {
            held: 3,
            current: 5,
        };
        assert_eq!(
            fenced.to_string(),
            "epoch fenced: held epoch 3, but current epoch is 5"
        );
    }

    /// Verify that rusqlite errors convert automatically.
    ///
    /// # Rust Pattern: From Trait
    ///
    /// The `#[from]` attribute on `Error::Sqlite` generates a `From` impl,
    /// allowing `?` to convert rusqlite errors to our Error type.
    #[test]
    fn test_sqlite_error_conversion() {
        // Create a rusqlite error (using a method that doesn't need a connection)
        let sqlite_err = rusqlite::Error::InvalidParameterName("test".to_string());

        // Convert to our error type
        let our_err: Error = sqlite_err.into();

        // Verify it's the right variant
        assert!(matches!(our_err, Error::Sqlite(_)));
        assert!(our_err.to_string().contains("sqlite error"));
    }
}
