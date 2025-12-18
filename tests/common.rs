#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};
use spitedb::crypto::{BatchCryptor, EnvKeyProvider};
use spitedb::{Database, WriterConfig};

pub fn test_key() -> [u8; 32] {
    [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
        0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    ]
}

pub fn test_cryptor() -> BatchCryptor {
    BatchCryptor::new(EnvKeyProvider::from_key(test_key()))
}

pub fn create_temp_db_file(name: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let path = dir.path().join(name);
    let _ = Database::open(&path).expect("initialize database");
    (dir, path)
}

pub fn open_read_only(path: &Path) -> Connection {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("open read-only connection")
}

pub fn open_read_write(path: &Path) -> Connection {
    Connection::open(path).expect("open read-write connection")
}

pub async fn eventually<T>(
    timeout: Duration,
    interval: Duration,
    mut f: impl FnMut() -> Option<T>,
) -> T {
    let start = std::time::Instant::now();
    loop {
        if let Some(v) = f() {
            return v;
        }
        if start.elapsed() > timeout {
            panic!("condition not met within {:?}", timeout);
        }
        tokio::time::sleep(interval).await;
    }
}

pub fn writer_config_with_batch_timeout(batch_timeout: Duration) -> WriterConfig {
    WriterConfig {
        batch_timeout,
        ..WriterConfig::default()
    }
}

/// Creates a WriterConfig with small batch settings for faster test execution.
pub fn fast_writer_config() -> WriterConfig {
    WriterConfig {
        batch_timeout: Duration::from_millis(5),
        batch_max_size: 100,
        batch_max_bytes: 1024 * 1024,
    }
}

// =============================================================================
// Database Query Helpers
// =============================================================================

/// Counts the total number of batches in the database.
pub fn count_batches(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM batches", [], |row| row.get(0))
        .unwrap()
}

/// Counts the total number of events in the event_index.
pub fn count_events(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM event_index", [], |row| row.get(0))
        .unwrap()
}

/// Counts events for a specific stream.
pub fn count_stream_events(conn: &Connection, stream_hash: i64) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM event_index WHERE stream_hash = ?",
        [stream_hash],
        |row| row.get(0),
    )
    .unwrap()
}

/// Raw event index row for verification.
#[derive(Debug, Clone)]
pub struct RawEventIndexRow {
    pub global_pos: i64,
    pub batch_id: i64,
    pub byte_offset: i64,
    pub byte_len: i64,
    pub stream_hash: i64,
    pub tenant_hash: i64,
    pub collision_slot: i64,
    pub stream_rev: i64,
}

/// Reads all event_index rows ordered by global_pos.
pub fn read_raw_event_index(conn: &Connection) -> Vec<RawEventIndexRow> {
    let mut stmt = conn
        .prepare(
            "SELECT global_pos, batch_id, byte_offset, byte_len, stream_hash, tenant_hash, collision_slot, stream_rev
             FROM event_index ORDER BY global_pos",
        )
        .unwrap();

    stmt.query_map([], |row| {
        Ok(RawEventIndexRow {
            global_pos: row.get(0)?,
            batch_id: row.get(1)?,
            byte_offset: row.get(2)?,
            byte_len: row.get(3)?,
            stream_hash: row.get(4)?,
            tenant_hash: row.get(5)?,
            collision_slot: row.get(6)?,
            stream_rev: row.get(7)?,
        })
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// Raw batch row for verification.
#[derive(Debug, Clone)]
pub struct RawBatchRow {
    pub batch_id: i64,
    pub base_pos: i64,
    pub event_count: i64,
    pub created_ms: i64,
    pub data_len: usize,
}

/// Reads all batch metadata (without blob data).
pub fn read_raw_batches(conn: &Connection) -> Vec<RawBatchRow> {
    let mut stmt = conn
        .prepare(
            "SELECT batch_id, base_pos, event_count, created_ms, LENGTH(data)
             FROM batches ORDER BY batch_id",
        )
        .unwrap();

    stmt.query_map([], |row| {
        Ok(RawBatchRow {
            batch_id: row.get(0)?,
            base_pos: row.get(1)?,
            event_count: row.get(2)?,
            created_ms: row.get(3)?,
            data_len: row.get::<_, i64>(4)? as usize,
        })
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// Raw stream head row for verification.
#[derive(Debug, Clone)]
pub struct RawStreamHeadRow {
    pub stream_id: String,
    pub stream_hash: i64,
    pub tenant_hash: i64,
    pub collision_slot: i64,
    pub last_rev: i64,
    pub last_pos: i64,
}

/// Reads all stream_heads from the database.
pub fn read_raw_stream_heads(conn: &Connection) -> Vec<RawStreamHeadRow> {
    let mut stmt = conn
        .prepare(
            "SELECT stream_id, stream_hash, tenant_hash, collision_slot, last_rev, last_pos
             FROM stream_heads ORDER BY stream_hash, collision_slot",
        )
        .unwrap();

    stmt.query_map([], |row| {
        Ok(RawStreamHeadRow {
            stream_id: row.get(0)?,
            stream_hash: row.get(1)?,
            tenant_hash: row.get(2)?,
            collision_slot: row.get(3)?,
            last_rev: row.get(4)?,
            last_pos: row.get(5)?,
        })
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// Gets the maximum global_pos in the database, or 0 if empty.
pub fn get_max_global_pos(conn: &Connection) -> i64 {
    conn.query_row("SELECT COALESCE(MAX(global_pos), 0) FROM event_index", [], |row| row.get(0))
        .unwrap()
}

/// Counts tombstones in the database.
pub fn count_tombstones(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM tombstones", [], |row| row.get(0))
        .unwrap()
}

/// Counts tenant tombstones in the database.
pub fn count_tenant_tombstones(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM tenant_tombstones", [], |row| row.get(0))
        .unwrap()
}

// =============================================================================
// Hash Collision Test Helpers
// =============================================================================

/// Generates pairs of stream IDs that hash to the same value.
/// This is useful for testing hash collision handling.
///
/// Finding true collisions is computationally expensive, so we use
/// a pre-computed list of known colliding pairs.
///
/// Note: These are synthetic test values. In practice, 64-bit hash
/// collisions are extremely rare (~1 in 37M at 1M streams).
pub fn get_known_colliding_stream_ids() -> Vec<(String, String)> {
    // Since finding XXH3 collisions is computationally infeasible,
    // we test the collision slot mechanism by directly manipulating
    // the database or by mocking. For now, return empty.
    // The actual collision tests will use database manipulation.
    vec![]
}

/// Creates a stream_heads entry with a specific collision slot for testing.
/// This allows testing collision slot logic without finding actual hash collisions.
pub fn insert_fake_collision_stream_head(
    conn: &Connection,
    stream_id: &str,
    stream_hash: i64,
    tenant_hash: i64,
    collision_slot: i64,
    last_rev: i64,
    last_pos: i64,
) {
    conn.execute(
        "INSERT INTO stream_heads (stream_id, stream_hash, tenant_hash, collision_slot, last_rev, last_pos)
         VALUES (?, ?, ?, ?, ?, ?)",
        rusqlite::params![stream_id, stream_hash, tenant_hash, collision_slot, last_rev, last_pos],
    )
    .unwrap();
}

// =============================================================================
// Verification Helpers
// =============================================================================

/// Verifies that global positions are strictly increasing with no gaps.
pub fn verify_global_pos_gapless(events: &[RawEventIndexRow]) {
    if events.is_empty() {
        return;
    }

    let first_pos = events[0].global_pos;
    for (i, event) in events.iter().enumerate() {
        let expected = first_pos + i as i64;
        assert_eq!(
            event.global_pos, expected,
            "Gap in global_pos at index {}: expected {}, got {}",
            i, expected, event.global_pos
        );
    }
}

/// Verifies that stream revisions for a specific stream are strictly increasing with no gaps.
pub fn verify_stream_rev_gapless(events: &[RawEventIndexRow], stream_hash: i64) {
    let stream_events: Vec<_> = events
        .iter()
        .filter(|e| e.stream_hash == stream_hash)
        .collect();

    if stream_events.is_empty() {
        return;
    }

    for (i, event) in stream_events.iter().enumerate() {
        let expected = (i + 1) as i64;
        assert_eq!(
            event.stream_rev, expected,
            "Gap in stream_rev for stream_hash {}: expected {}, got {}",
            stream_hash, expected, event.stream_rev
        );
    }
}

/// Verifies that all global positions are strictly increasing (no duplicates, no gaps).
pub fn verify_global_ordering(events: &[RawEventIndexRow]) {
    for window in events.windows(2) {
        assert!(
            window[1].global_pos > window[0].global_pos,
            "Global positions not strictly increasing: {} >= {}",
            window[0].global_pos,
            window[1].global_pos
        );
        assert_eq!(
            window[1].global_pos,
            window[0].global_pos + 1,
            "Gap in global positions: {} to {}",
            window[0].global_pos,
            window[1].global_pos
        );
    }
}
