//! Integration tests for SpiteDB database initialization.
//!
//! # Rust Pattern: Integration Tests
//!
//! Files in the `tests/` directory are compiled as separate crates that depend
//! on the library. This means:
//! - They can only access public API (`pub` items)
//! - They test the library as external users would
//! - They're compiled separately from unit tests
//!
//! # Running Tests
//!
//! ```bash
//! # Run all tests (unit + integration)
//! cargo test
//!
//! # Run only integration tests
//! cargo test --test database_test
//!
//! # Run with output visible
//! cargo test -- --nocapture
//! ```

use spitedb::{Database, Error};
use tempfile::tempdir;

/// Test that we can create a new database file.
///
/// This is the most basic test - does the library work at all?
#[test]
fn test_create_new_database() {
    let dir = tempdir().expect("should create temp dir");
    let db_path = dir.path().join("test.db");

    // Should succeed
    let result = Database::open(&db_path);
    assert!(result.is_ok(), "should create new database");

    // File should exist
    assert!(db_path.exists(), "database file should be created");
}

/// Test that we can reopen an existing database.
///
/// This verifies idempotent initialization - opening twice should work.
#[test]
fn test_reopen_existing_database() {
    let dir = tempdir().expect("should create temp dir");
    let db_path = dir.path().join("reopen.db");

    // Create database
    {
        let _db = Database::open(&db_path).expect("first open should work");
    } // Connection closes here (RAII)

    // Reopen should succeed
    {
        let _db = Database::open(&db_path).expect("second open should work");
    }
}

/// Test that in-memory databases work for testing.
///
/// In-memory databases are faster and automatically cleaned up,
/// making them ideal for unit tests.
#[test]
fn test_in_memory_database() {
    let result = Database::open_in_memory();
    assert!(result.is_ok(), "should create in-memory database");
}

/// Test that multiple in-memory databases are independent.
///
/// Each call to `open_in_memory()` creates a separate database.
/// This is important for test isolation.
#[test]
fn test_multiple_in_memory_databases_are_independent() {
    let _db1 = Database::open_in_memory().expect("should create first db");
    let _db2 = Database::open_in_memory().expect("should create second db");

    // If we were writing events, they'd be separate.
    // For now, just verify both exist.
}

/// Test error handling for invalid paths.
///
/// Attempting to open a database in a non-existent directory should fail
/// with a meaningful error.
#[test]
fn test_invalid_path_returns_error() {
    // Try to create a database in a directory that doesn't exist
    let result = Database::open("/nonexistent/directory/test.db");

    assert!(result.is_err(), "should fail for invalid path");

    // Verify it's a SQLite error
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::Sqlite(_)),
        "should be a SQLite error, got: {err}"
    );
}

/// Test that the Result type alias works correctly.
///
/// This is mostly a compile-time check that our type alias is set up right.
#[test]
fn test_result_type_usage() {
    fn returns_result() -> spitedb::Result<String> {
        Ok("success".to_string())
    }

    let result = returns_result();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "success");
}

/// Test error Display implementations.
///
/// Error messages should be human-readable and contain useful information.
#[test]
fn test_error_display() {
    let conflict = Error::Conflict {
        stream_id: "test-stream".to_string(),
        expected: 5,
        actual: 10,
    };

    let message = conflict.to_string();
    assert!(message.contains("test-stream"));
    assert!(message.contains("5"));
    assert!(message.contains("10"));
}

/// Test that database files persist across opens.
///
/// This verifies durability - the database file is created and can be reopened.
/// More detailed persistence tests will be added when we implement event storage.
#[test]
fn test_persistence() {
    let dir = tempdir().expect("should create temp dir");
    let db_path = dir.path().join("persist.db");

    // First open - just create the database
    {
        let _db = Database::open(&db_path).expect("should create db");
        // Schema tables are created automatically
    }

    // Verify the file exists and has some content
    let metadata = std::fs::metadata(&db_path).expect("should have file metadata");
    assert!(metadata.len() > 0, "database file should have content");

    // Second open - verify it still works
    {
        let _db = Database::open(&db_path).expect("should reopen db");
        // If schema version was wrong, this would fail
    }
}
