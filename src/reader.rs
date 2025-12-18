//! # Event Reader
//!
//! This module provides read operations for SpiteDB. It reads events from
//! SQLite using direct queries, ensuring readers always see the latest
//! committed data via WAL mode.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                       Reader Pool                                │
//! │                                                                  │
//! │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
//! │  │Reader 1 │  │Reader 2 │  │Reader 3 │  │Reader N │            │
//! │  │(thread) │  │(thread) │  │(thread) │  │(thread) │            │
//! │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘            │
//! │       │            │            │            │                   │
//! │       └────────────┴────────────┴────────────┘                  │
//! │                            │                                     │
//! │                     Read-only SQLite                             │
//! │                      connections                                 │
//! └─────────────────────────────┬───────────────────────────────────┘
//!                               │
//!                               ▼
//!                       ┌─────────────┐
//!                       │   SQLite    │
//!                       │   (WAL)     │
//!                       └─────────────┘
//! ```
//!
//! ## Why Direct SQL?
//!
//! For file-based databases with separate reader/writer connections, using
//! cached stream_heads would return stale data. By using direct SQL queries,
//! we always read the latest committed data from the database file.

use std::collections::HashMap;
use std::sync::Arc;

use rusqlite::{params, Connection};
use tokio::sync::{mpsc, oneshot};

use crate::codec::decode_batch;
use crate::crypto::{BatchCryptor, AES_GCM_NONCE_SIZE};
use crate::error::Result;
use crate::tombstones::{
    filter_stream_events, filter_tenant_events, is_tenant_tombstoned, load_stream_tombstones,
    load_tenant_tombstones,
};
use crate::types::{CollisionSlot, Event, GlobalPos, StreamId, StreamRev, TenantHash};
use crate::Error;

// =============================================================================
// Helper Functions
// =============================================================================

/// Extracts event data from a decrypted batch with bounds checking.
///
/// Returns an error if the offset/length would go out of bounds, which indicates
/// either database corruption or a bug in the event indexing logic.
fn extract_event_data(decrypted: &[u8], offset: usize, len: usize, global_pos: u64) -> Result<Vec<u8>> {
    if offset.checked_add(len).map_or(true, |end| end > decrypted.len()) {
        return Err(Error::Schema(format!(
            "corrupted event index: global_pos={} has invalid bounds (offset={}, len={}, batch_len={})",
            global_pos, offset, len, decrypted.len()
        )));
    }
    Ok(decrypted[offset..offset + len].to_vec())
}

// =============================================================================
// Request Types
// =============================================================================

/// Request type for read operations.
pub enum ReadRequest {
    /// Read events from a specific stream.
    ReadStream {
        stream_id: StreamId,
        tenant_hash: TenantHash,
        from_rev: StreamRev,
        limit: usize,
        response: oneshot::Sender<Result<Vec<Event>>>,
    },
    /// Read events from the global log.
    ReadGlobal {
        from_pos: GlobalPos,
        limit: usize,
        response: oneshot::Sender<Result<Vec<Event>>>,
    },
    /// Read events for a specific tenant from the global log.
    ReadTenantEvents {
        tenant_hash: TenantHash,
        from_pos: GlobalPos,
        limit: usize,
        response: oneshot::Sender<Result<Vec<Event>>>,
    },
    /// Get the current revision of a stream.
    GetStreamRevision {
        stream_id: StreamId,
        tenant_hash: TenantHash,
        response: oneshot::Sender<StreamRev>,
    },
    /// Shutdown the reader.
    Shutdown,
}

// =============================================================================
// Direct Read Functions
// =============================================================================

/// Reads events from a stream using direct SQL queries.
///
/// # Arguments
///
/// * `conn` - Read-only SQLite connection
/// * `stream_id` - The stream to read from
/// * `from_rev` - Starting revision (inclusive)
/// * `limit` - Maximum number of events to return
///
/// # Returns
///
/// Events in revision order. Empty vec if stream doesn't exist.
pub fn read_stream(
    conn: &Connection,
    stream_id: &StreamId,
    from_rev: StreamRev,
    limit: usize,
    cryptor: &BatchCryptor,
) -> Result<Vec<Event>> {
    read_stream_tenant(conn, stream_id, TenantHash::default_hash(), from_rev, limit, cryptor)
}

/// Reads events from a stream within a specific tenant using direct SQL queries.
pub fn read_stream_tenant(
    conn: &Connection,
    stream_id: &StreamId,
    tenant_hash: TenantHash,
    from_rev: StreamRev,
    limit: usize,
    cryptor: &BatchCryptor,
) -> Result<Vec<Event>> {
    let stream_hash = stream_id.hash();

    // Get collision slot from stream_heads for this tenant.
    let collision_slot: Option<i64> = match conn.query_row(
        "SELECT collision_slot FROM stream_heads WHERE stream_id = ? AND tenant_hash = ?",
        params![stream_id.as_str(), tenant_hash.as_raw()],
        |row| row.get(0),
    ) {
        Ok(v) => Some(v),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e.into()),
    };

    let collision_slot = match collision_slot {
        Some(slot) => slot,
        None => return Ok(Vec::new()), // Stream doesn't exist in this tenant
    };

    let collision_slot_typed = CollisionSlot::from_raw(collision_slot as u16);

    // Check if tenant is tombstoned - if so, return empty
    if is_tenant_tombstoned(conn, tenant_hash)? {
        return Ok(Vec::new());
    }

    // Load tombstones for this stream (tenant-scoped)
    let tombstones = load_stream_tombstones(conn, stream_hash, collision_slot_typed, tenant_hash)?;

    let mut stmt = conn.prepare(
        "SELECT e.global_pos, e.byte_offset, e.byte_len, e.stream_rev, b.base_pos, b.created_ms, b.nonce, b.data
         FROM event_index e
         JOIN batches b ON e.batch_id = b.batch_id
         WHERE e.stream_hash = ? AND e.collision_slot = ? AND e.stream_rev >= ?
         ORDER BY e.stream_rev
         LIMIT ?",
    )?;

    let events = stmt.query_map(
        params![
            stream_hash.as_raw(),
            collision_slot,
            from_rev.as_raw() as i64,
            limit as i64,
        ],
        |row| {
            let global_pos: i64 = row.get(0)?;
            let byte_offset: i64 = row.get(1)?;
            let byte_len: i64 = row.get(2)?;
            let stream_rev: i64 = row.get(3)?;
            let base_pos: i64 = row.get(4)?;
            let timestamp_ms: i64 = row.get(5)?;
            let nonce: Vec<u8> = row.get(6)?;
            let batch_data: Vec<u8> = row.get(7)?;

            Ok((global_pos, byte_offset, byte_len, stream_rev, base_pos, timestamp_ms, nonce, batch_data))
        },
    )?;

    // Cache decrypted batches to avoid O(n) decryption per batch
    let mut batch_cache: HashMap<i64, Vec<u8>> = HashMap::new();
    let mut result = Vec::new();
    for event_data in events {
        let (global_pos, byte_offset, byte_len, stream_rev, base_pos, timestamp_ms, nonce, batch_data) = event_data?;

        // Check cache or decrypt the batch once
        let decrypted = if let Some(cached) = batch_cache.get(&base_pos) {
            cached
        } else {
            let nonce_arr: [u8; AES_GCM_NONCE_SIZE] = nonce.as_slice().try_into()
                .map_err(|_| crate::error::Error::Encryption("invalid nonce length".into()))?;
            let decrypted = decode_batch(&batch_data, &nonce_arr, base_pos, cryptor)?;
            batch_cache.insert(base_pos, decrypted);
            batch_cache.get(&base_pos).unwrap()
        };

        // Extract event data from cached decrypted batch with bounds checking
        let data = extract_event_data(decrypted, byte_offset as usize, byte_len as usize, global_pos as u64)?;

        result.push(Event {
            global_pos: GlobalPos::from_raw_unchecked(global_pos as u64),
            stream_id: stream_id.clone(),
            tenant_hash,
            collision_slot: collision_slot_typed,
            stream_rev: StreamRev::from_raw(stream_rev as u64),
            timestamp_ms: timestamp_ms as u64,
            data,
        });
    }

    // Filter out tombstoned events
    let result = filter_stream_events(result, &tombstones);

    Ok(result)
}

/// Reads events from the global log using direct SQL queries.
///
/// # Arguments
///
/// * `conn` - Read-only SQLite connection
/// * `from_pos` - Starting global position (inclusive)
/// * `limit` - Maximum number of events to return
///
/// # Returns
///
/// Events in global position order, across all streams.
/// Events from tombstoned tenants or tombstoned stream revisions are filtered out.
pub fn read_global(conn: &Connection, from_pos: GlobalPos, limit: usize, cryptor: &BatchCryptor) -> Result<Vec<Event>> {
    // Load tenant tombstones first (usually a small set)
    let deleted_tenants = load_tenant_tombstones(conn)?;

    // Join with stream_heads to get stream_id and tenant_hash
    let mut stmt = conn.prepare(
        "SELECT e.global_pos, e.byte_offset, e.byte_len, e.stream_rev,
                b.base_pos, b.created_ms, b.nonce, b.data, s.stream_id, s.tenant_hash, s.collision_slot
         FROM event_index e
         JOIN batches b ON e.batch_id = b.batch_id
         JOIN stream_heads s ON e.stream_hash = s.stream_hash AND e.collision_slot = s.collision_slot AND e.tenant_hash = s.tenant_hash
         WHERE e.global_pos >= ?
         ORDER BY e.global_pos
         LIMIT ?",
    )?;

    let events = stmt.query_map(params![from_pos.as_raw() as i64, limit as i64], |row| {
        let global_pos: i64 = row.get(0)?;
        let byte_offset: i64 = row.get(1)?;
        let byte_len: i64 = row.get(2)?;
        let stream_rev: i64 = row.get(3)?;
        let base_pos: i64 = row.get(4)?;
        let timestamp_ms: i64 = row.get(5)?;
        let nonce: Vec<u8> = row.get(6)?;
        let batch_data: Vec<u8> = row.get(7)?;
        let stream_id: String = row.get(8)?;
        let tenant_hash: i64 = row.get(9)?;
        let collision_slot: i64 = row.get(10)?;

        Ok((global_pos, byte_offset, byte_len, stream_rev, base_pos, timestamp_ms, nonce, batch_data, stream_id, tenant_hash, collision_slot))
    })?;

    // Cache decrypted batches to avoid O(n) decryption per batch
    let mut batch_cache: HashMap<i64, Vec<u8>> = HashMap::new();
    let mut result = Vec::new();
    for event_data in events {
        let (global_pos, byte_offset, byte_len, stream_rev, base_pos, timestamp_ms, nonce, batch_data, stream_id, tenant_hash, collision_slot) = event_data?;

        // Check cache or decrypt the batch once
        let decrypted = if let Some(cached) = batch_cache.get(&base_pos) {
            cached
        } else {
            let nonce_arr: [u8; AES_GCM_NONCE_SIZE] = nonce.as_slice().try_into()
                .map_err(|_| crate::error::Error::Encryption("invalid nonce length".into()))?;
            let decrypted = decode_batch(&batch_data, &nonce_arr, base_pos, cryptor)?;
            batch_cache.insert(base_pos, decrypted);
            batch_cache.get(&base_pos).unwrap()
        };

        // Extract event data from cached decrypted batch with bounds checking
        let data = extract_event_data(decrypted, byte_offset as usize, byte_len as usize, global_pos as u64)?;

        result.push(Event {
            global_pos: GlobalPos::from_raw_unchecked(global_pos as u64),
            stream_id: StreamId::new(stream_id),
            tenant_hash: TenantHash::from_raw(tenant_hash),
            collision_slot: CollisionSlot::from_raw(collision_slot as u16),
            stream_rev: StreamRev::from_raw(stream_rev as u64),
            timestamp_ms: timestamp_ms as u64,
            data,
        });
    }

    // Filter tenant tombstones first (fast - just a HashSet lookup)
    let result = filter_tenant_events(result, &deleted_tenants);

    // For stream tombstones, we use the combined filter function
    // which caches tombstones per stream
    let result = crate::tombstones::filter_all_tombstones(conn, result, &deleted_tenants)?;

    Ok(result)
}

/// Reads events for a specific tenant from the event_index.
///
/// # Arguments
///
/// * `conn` - Read-only SQLite connection
/// * `tenant_hash` - Hash of the tenant to filter by
/// * `from_pos` - Starting global position (inclusive)
/// * `limit` - Maximum number of events to return
///
/// # Returns
///
/// Events in global position order, filtered by tenant.
/// Returns empty if the tenant has been tombstoned.
/// Events from tombstoned stream revisions are filtered out.
///
/// # Note
///
/// This reads directly from event_index (not the projection table), which
/// may be slower for large datasets. For production use with many events,
/// consider using the tenant_event_index projection table instead.
pub fn read_tenant_events(
    conn: &Connection,
    tenant_hash: TenantHash,
    from_pos: GlobalPos,
    limit: usize,
    cryptor: &BatchCryptor,
) -> Result<Vec<Event>> {
    // Check if tenant is tombstoned - if so, return empty
    if is_tenant_tombstoned(conn, tenant_hash)? {
        return Ok(Vec::new());
    }

    let mut stmt = conn.prepare(
        "SELECT e.global_pos, e.byte_offset, e.byte_len, e.stream_rev,
                b.base_pos, b.created_ms, b.nonce, b.data, s.stream_id, s.collision_slot
         FROM event_index e
         JOIN batches b ON e.batch_id = b.batch_id
         JOIN stream_heads s ON e.stream_hash = s.stream_hash AND e.collision_slot = s.collision_slot AND e.tenant_hash = s.tenant_hash
         WHERE e.tenant_hash = ? AND e.global_pos >= ?
         ORDER BY e.global_pos
         LIMIT ?",
    )?;

    let events = stmt.query_map(
        params![tenant_hash.as_raw(), from_pos.as_raw() as i64, limit as i64],
        |row| {
            let global_pos: i64 = row.get(0)?;
            let byte_offset: i64 = row.get(1)?;
            let byte_len: i64 = row.get(2)?;
            let stream_rev: i64 = row.get(3)?;
            let base_pos: i64 = row.get(4)?;
            let timestamp_ms: i64 = row.get(5)?;
            let nonce: Vec<u8> = row.get(6)?;
            let batch_data: Vec<u8> = row.get(7)?;
            let stream_id: String = row.get(8)?;
            let collision_slot: i64 = row.get(9)?;

            Ok((global_pos, byte_offset, byte_len, stream_rev, base_pos, timestamp_ms, nonce, batch_data, stream_id, collision_slot))
        },
    )?;

    // Cache decrypted batches to avoid O(n) decryption per batch
    let mut batch_cache: HashMap<i64, Vec<u8>> = HashMap::new();
    let mut result = Vec::new();
    for event_data in events {
        let (global_pos, byte_offset, byte_len, stream_rev, base_pos, timestamp_ms, nonce, batch_data, stream_id, collision_slot) = event_data?;

        // Check cache or decrypt the batch once
        let decrypted = if let Some(cached) = batch_cache.get(&base_pos) {
            cached
        } else {
            let nonce_arr: [u8; AES_GCM_NONCE_SIZE] = nonce.as_slice().try_into()
                .map_err(|_| crate::error::Error::Encryption("invalid nonce length".into()))?;
            let decrypted = decode_batch(&batch_data, &nonce_arr, base_pos, cryptor)?;
            batch_cache.insert(base_pos, decrypted);
            batch_cache.get(&base_pos).unwrap()
        };

        // Extract event data from cached decrypted batch with bounds checking
        let data = extract_event_data(decrypted, byte_offset as usize, byte_len as usize, global_pos as u64)?;

        result.push(Event {
            global_pos: GlobalPos::from_raw_unchecked(global_pos as u64),
            stream_id: StreamId::new(stream_id),
            tenant_hash,
            collision_slot: CollisionSlot::from_raw(collision_slot as u16),
            stream_rev: StreamRev::from_raw(stream_rev as u64),
            timestamp_ms: timestamp_ms as u64,
            data,
        });
    }

    // Filter stream tombstones using the combined filter
    // Note: deleted_tenants is empty since we already checked this tenant isn't tombstoned
    let empty_deleted_tenants = std::collections::HashSet::new();
    let result = crate::tombstones::filter_all_tombstones(conn, result, &empty_deleted_tenants)?;

    Ok(result)
}

/// Gets stream revision using direct SQL queries.
///
/// # Returns
///
/// The current revision of the stream, or `StreamRev::NONE` if the stream
/// doesn't exist.
pub fn get_stream_revision(conn: &Connection, stream_id: &StreamId) -> StreamRev {
    get_stream_revision_tenant(conn, stream_id, TenantHash::default_hash())
}

/// Gets stream revision for a specific tenant.
pub fn get_stream_revision_tenant(conn: &Connection, stream_id: &StreamId, tenant_hash: TenantHash) -> StreamRev {
    conn.query_row(
        "SELECT last_rev FROM stream_heads WHERE stream_id = ? AND tenant_hash = ?",
        params![stream_id.as_str(), tenant_hash.as_raw()],
        |row| {
            let rev: i64 = row.get(0)?;
            Ok(StreamRev::from_raw(rev as u64))
        },
    )
    .unwrap_or(StreamRev::NONE)
}

// =============================================================================
// Reader Loop
// =============================================================================

/// Pooled reader loop using direct SQL queries.
///
/// Multiple threads share the channel via Arc<Mutex>. Each thread has its own
/// read-only SQLite connection, so they can execute queries in parallel.
///
/// # Load Balancing
///
/// Threads compete to acquire the lock and receive the next request. This
/// provides simple but effective load balancing - whichever thread is free
/// picks up the next request.
pub async fn run_reader_pooled(
    conn: Connection,
    cryptor: Arc<BatchCryptor>,
    rx: Arc<std::sync::Mutex<mpsc::Receiver<ReadRequest>>>,
) {
    loop {
        // Lock the receiver to get the next request
        let request = {
            let mut guard = rx.lock().expect("receiver mutex poisoned");
            guard.recv().await
        };

        match request {
            Some(ReadRequest::ReadStream {
                stream_id,
                tenant_hash,
                from_rev,
                limit,
                response,
            }) => {
                let result = read_stream_tenant(&conn, &stream_id, tenant_hash, from_rev, limit, &cryptor);
                let _ = response.send(result);
            }
            Some(ReadRequest::ReadGlobal {
                from_pos,
                limit,
                response,
            }) => {
                let result = read_global(&conn, from_pos, limit, &cryptor);
                let _ = response.send(result);
            }
            Some(ReadRequest::ReadTenantEvents {
                tenant_hash,
                from_pos,
                limit,
                response,
            }) => {
                let result = read_tenant_events(&conn, tenant_hash, from_pos, limit, &cryptor);
                let _ = response.send(result);
            }
            Some(ReadRequest::GetStreamRevision {
                stream_id,
                tenant_hash,
                response,
            }) => {
                let result = get_stream_revision_tenant(&conn, &stream_id, tenant_hash);
                let _ = response.send(result);
            }
            Some(ReadRequest::Shutdown) | None => break,
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_batch;
    use crate::crypto::{EnvKeyProvider, CODEC_ZSTD_L1, CIPHER_AES256GCM};
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

    fn setup_test_db() -> Connection {
        let db = Database::open_in_memory().unwrap();
        db.into_connection()
    }

    fn insert_test_events(conn: &Connection, stream_id: &str, count: usize, cryptor: &BatchCryptor) {
        insert_test_events_at(conn, stream_id, count, 1, cryptor);
    }

    fn insert_test_events_at(conn: &Connection, stream_id: &str, count: usize, start_pos: usize, cryptor: &BatchCryptor) {
        let stream = StreamId::new(stream_id);
        let stream_hash = stream.hash();

        for i in 0..count {
            let global_pos = start_pos + i;
            let stream_rev = i + 1;
            let events = vec![EventData::new(format!("event-{}", i).into_bytes())];
            let batch_id = global_pos as i64;
            let (blob, nonce, offsets) = encode_batch(&events, batch_id, cryptor).unwrap();

            let checksum = crate::codec::compute_checksum(&blob);

            conn.execute(
                "INSERT INTO batches (base_pos, event_count, created_ms, codec, cipher, nonce, checksum, data)
                 VALUES (?, 1, ?, ?, ?, ?, ?, ?)",
                params![global_pos as i64, 12345i64, CODEC_ZSTD_L1, CIPHER_AES256GCM, nonce.as_slice(), checksum.as_slice(), blob.as_slice()],
            )
            .unwrap();

            let inserted_batch_id = conn.last_insert_rowid();

            conn.execute(
                "INSERT INTO event_index (global_pos, batch_id, byte_offset, byte_len, stream_hash, tenant_hash, collision_slot, stream_rev)
                 VALUES (?, ?, ?, ?, ?, ?, 0, ?)",
                params![
                    global_pos as i64,
                    inserted_batch_id,
                    offsets[0].0 as i64,
                    offsets[0].1 as i64,
                    stream_hash.as_raw(),
                    crate::types::TenantHash::default_hash().as_raw(),
                    stream_rev as i64,
                ],
            )
            .unwrap();
        }

        let last_pos = start_pos + count - 1;
        conn.execute(
            "INSERT OR REPLACE INTO stream_heads (stream_id, stream_hash, tenant_hash, collision_slot, last_rev, last_pos)
             VALUES (?, ?, ?, 0, ?, ?)",
            params![stream_id, stream_hash.as_raw(), crate::types::TenantHash::default_hash().as_raw(), count as i64, last_pos as i64],
        )
        .unwrap();
    }

    #[test]
    fn test_read_stream() {
        let conn = setup_test_db();
        let cryptor = test_cryptor();
        insert_test_events(&conn, "test-stream", 5, &cryptor);

        let events = read_stream(&conn, &StreamId::new("test-stream"), StreamRev::FIRST, 10, &cryptor).unwrap();

        assert_eq!(events.len(), 5);
        assert_eq!(events[0].stream_rev.as_raw(), 1);
        assert_eq!(events[4].stream_rev.as_raw(), 5);
        assert_eq!(events[0].data, b"event-0");
    }

    #[test]
    fn test_read_stream_with_offset() {
        let conn = setup_test_db();
        let cryptor = test_cryptor();
        insert_test_events(&conn, "test-stream", 10, &cryptor);

        let events = read_stream(&conn, &StreamId::new("test-stream"), StreamRev::from_raw(5), 10, &cryptor).unwrap();

        assert_eq!(events.len(), 6); // revisions 5-10
        assert_eq!(events[0].stream_rev.as_raw(), 5);
    }

    #[test]
    fn test_read_stream_empty() {
        let conn = setup_test_db();
        let cryptor = test_cryptor();

        let events = read_stream(&conn, &StreamId::new("nonexistent"), StreamRev::FIRST, 10, &cryptor).unwrap();

        assert!(events.is_empty());
    }

    #[test]
    fn test_read_global() {
        let conn = setup_test_db();
        let cryptor = test_cryptor();
        insert_test_events_at(&conn, "stream-1", 3, 1, &cryptor);  // positions 1, 2, 3
        insert_test_events_at(&conn, "stream-2", 2, 4, &cryptor);  // positions 4, 5

        let events = read_global(&conn, GlobalPos::FIRST, 10, &cryptor).unwrap();

        // Should have all 5 events across both streams
        assert_eq!(events.len(), 5);
        // First 3 from stream-1, last 2 from stream-2
        assert_eq!(events[0].stream_id.as_str(), "stream-1");
        assert_eq!(events[3].stream_id.as_str(), "stream-2");
    }

    #[test]
    fn test_get_stream_revision() {
        let conn = setup_test_db();
        let cryptor = test_cryptor();
        insert_test_events(&conn, "test-stream", 5, &cryptor);

        let rev = get_stream_revision(&conn, &StreamId::new("test-stream"));
        assert_eq!(rev.as_raw(), 5);

        let rev_none = get_stream_revision(&conn, &StreamId::new("nonexistent"));
        assert!(rev_none.is_none());
    }
}
