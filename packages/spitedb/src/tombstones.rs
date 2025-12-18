//! # Tombstone Logic for GDPR-Compliant Deletion
//!
//! This module provides the core functionality for logical deletion in SpiteDB.
//! It handles loading tombstone records from the database and filtering events
//! based on those tombstones.
//!
//! ## Why Logical Deletion?
//!
//! GDPR and similar regulations require the ability to delete user data. However,
//! in an event-sourced system, we can't simply delete events without careful
//! consideration:
//!
//! - Other processes may be reading them
//! - Subscribers may not have processed them yet
//! - Compaction (physical deletion) is expensive
//!
//! Logical deletion (tombstones) provides:
//!
//! 1. **Immediate effect**: Events are filtered from all reads instantly
//! 2. **Deferred physical deletion**: Actual data removal happens during compaction
//! 3. **Audit trail**: `deleted_ms` timestamps record when deletions occurred
//!
//! ## Deletion Scopes
//!
//! - **Stream tombstones**: Delete specific revision ranges within a stream
//! - **Tenant tombstones**: Delete all data for an entire tenant/organization

use std::collections::HashSet;

use rusqlite::{params, Connection};

use crate::error::Result;
use crate::types::{
    CollisionSlot, Event, StreamHash, StreamRev, StreamTombstone, TenantHash, TenantTombstone,
};

// =============================================================================
// Stream Tombstones
// =============================================================================

/// Loads all tombstones for a given stream within a specific tenant.
///
/// # Arguments
///
/// * `conn` - Database connection
/// * `stream_hash` - Hash of the stream
/// * `collision_slot` - Collision slot for the stream
/// * `tenant_hash` - Hash of the tenant (for multi-tenant isolation)
///
/// # Returns
///
/// Vector of tombstone records for the stream. Empty if no tombstones exist.
pub fn load_stream_tombstones(
    conn: &Connection,
    stream_hash: StreamHash,
    collision_slot: CollisionSlot,
    tenant_hash: TenantHash,
) -> Result<Vec<StreamTombstone>> {
    let mut stmt = conn.prepare(
        "SELECT from_rev, to_rev, deleted_ms
         FROM tombstones
         WHERE stream_hash = ? AND collision_slot = ? AND tenant_hash = ?",
    )?;

    let tombstones = stmt.query_map(
        params![stream_hash.as_raw(), collision_slot.as_raw() as i64, tenant_hash.as_raw()],
        |row| {
            let from_rev: i64 = row.get(0)?;
            let to_rev: i64 = row.get(1)?;
            let deleted_ms: i64 = row.get(2)?;

            Ok(StreamTombstone {
                stream_hash,
                collision_slot,
                tenant_hash,
                from_rev: StreamRev::from_raw(from_rev as u64),
                to_rev: StreamRev::from_raw(to_rev as u64),
                deleted_ms: deleted_ms as u64,
            })
        },
    )?;

    tombstones.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
}

/// Checks if a specific revision is tombstoned.
///
/// # Arguments
///
/// * `tombstones` - List of tombstones to check against
/// * `rev` - The revision to check
///
/// # Returns
///
/// `true` if the revision falls within any tombstone range.
pub fn is_revision_tombstoned(tombstones: &[StreamTombstone], rev: StreamRev) -> bool {
    tombstones.iter().any(|t| t.contains(rev))
}

/// Filters tombstoned events from a list of events (same stream).
///
/// Use this when all events are from the same stream and you have the
/// tombstones for that stream.
///
/// # Arguments
///
/// * `events` - Events to filter (must all be from the same stream)
/// * `tombstones` - Tombstones for that stream
///
/// # Returns
///
/// Events that are not tombstoned.
pub fn filter_stream_events(events: Vec<Event>, tombstones: &[StreamTombstone]) -> Vec<Event> {
    if tombstones.is_empty() {
        return events;
    }

    events
        .into_iter()
        .filter(|e| !is_revision_tombstoned(tombstones, e.stream_rev))
        .collect()
}

// =============================================================================
// Tenant Tombstones
// =============================================================================

/// Loads all tenant tombstones.
///
/// # Returns
///
/// Set of tenant hashes that have been tombstoned (deleted).
pub fn load_tenant_tombstones(conn: &Connection) -> Result<HashSet<TenantHash>> {
    let mut stmt = conn.prepare("SELECT tenant_hash FROM tenant_tombstones")?;

    let hashes = stmt.query_map([], |row| {
        let hash: i64 = row.get(0)?;
        Ok(TenantHash::from_raw(hash))
    })?;

    hashes.collect::<std::result::Result<HashSet<_>, _>>().map_err(Into::into)
}

/// Loads a single tenant tombstone if it exists.
///
/// # Returns
///
/// `Some(TenantTombstone)` if the tenant is tombstoned, `None` otherwise.
pub fn load_tenant_tombstone(
    conn: &Connection,
    tenant_hash: TenantHash,
) -> Result<Option<TenantTombstone>> {
    let result = conn.query_row(
        "SELECT deleted_ms FROM tenant_tombstones WHERE tenant_hash = ?",
        params![tenant_hash.as_raw()],
        |row| {
            let deleted_ms: i64 = row.get(0)?;
            Ok(TenantTombstone {
                tenant_hash,
                deleted_ms: deleted_ms as u64,
            })
        },
    );

    match result {
        Ok(tombstone) => Ok(Some(tombstone)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Checks if a tenant is tombstoned.
///
/// # Arguments
///
/// * `conn` - Database connection
/// * `tenant_hash` - Hash of the tenant to check
///
/// # Returns
///
/// `true` if the tenant has been deleted.
pub fn is_tenant_tombstoned(conn: &Connection, tenant_hash: TenantHash) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM tenant_tombstones WHERE tenant_hash = ?",
        params![tenant_hash.as_raw()],
        |row| row.get(0),
    )?;

    Ok(count > 0)
}

/// Filters events belonging to tombstoned tenants.
///
/// # Arguments
///
/// * `events` - Events to filter
/// * `deleted_tenants` - Set of tenant hashes that have been deleted
///
/// # Returns
///
/// Events that don't belong to deleted tenants.
pub fn filter_tenant_events(events: Vec<Event>, deleted_tenants: &HashSet<TenantHash>) -> Vec<Event> {
    if deleted_tenants.is_empty() {
        return events;
    }

    events
        .into_iter()
        .filter(|e| !deleted_tenants.contains(&e.tenant_hash))
        .collect()
}

// =============================================================================
// Combined Filtering
// =============================================================================

/// Filters events based on both tenant and stream tombstones.
///
/// This is the main filtering function used by readers. It:
/// 1. First filters out events from deleted tenants
/// 2. Then loads stream tombstones for remaining events
/// 3. Filters out events that are stream-tombstoned
///
/// # Arguments
///
/// * `conn` - Database connection (for loading stream tombstones)
/// * `events` - Events to filter
/// * `deleted_tenants` - Pre-loaded set of deleted tenant hashes
///
/// # Returns
///
/// Events that are not tombstoned (neither tenant nor stream level).
pub fn filter_all_tombstones(
    conn: &Connection,
    events: Vec<Event>,
    deleted_tenants: &HashSet<TenantHash>,
) -> Result<Vec<Event>> {
    // First filter deleted tenants
    let events = filter_tenant_events(events, deleted_tenants);

    if events.is_empty() {
        return Ok(events);
    }

    // Group events by stream for efficient tombstone loading
    // Note: This is a simplified implementation. For better performance with
    // many streams, consider caching tombstones or batch loading.
    let mut filtered = Vec::with_capacity(events.len());

    // Track which stream tombstones we've already loaded (keyed by stream+slot+tenant)
    let mut stream_tombstones_cache: std::collections::HashMap<
        (StreamHash, CollisionSlot, TenantHash),
        Vec<StreamTombstone>,
    > = std::collections::HashMap::new();

    for event in events {
        // Get stream hash, collision slot, and tenant for this event
        let stream_hash = event.stream_id.hash();
        let collision_slot = event.collision_slot;
        let tenant_hash = event.tenant_hash;

        // Include tenant in cache key for proper multi-tenant isolation
        let key = (stream_hash, collision_slot, tenant_hash);

        let tombstones = if let Some(cached) = stream_tombstones_cache.get(&key) {
            cached
        } else {
            let loaded = load_stream_tombstones(conn, stream_hash, collision_slot, tenant_hash)?;
            stream_tombstones_cache.insert(key, loaded);
            stream_tombstones_cache.get(&key).unwrap()
        };

        if !is_revision_tombstoned(tombstones, event.stream_rev) {
            filtered.push(event);
        }
    }

    Ok(filtered)
}

// =============================================================================
// Compaction Interface (Deferred Execution)
// =============================================================================

/// Statistics from a compaction run.
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Number of batches that were rewritten.
    pub batches_rewritten: usize,

    /// Number of events physically deleted.
    pub events_deleted: usize,

    /// Number of tombstone records removed after compaction.
    pub tombstones_removed: usize,

    /// Bytes reclaimed (approximate, before VACUUM).
    pub bytes_reclaimed: usize,
}

/// Compaction job for physically deleting tombstoned events.
///
/// # Compaction Process (for future implementation)
///
/// 1. Find batches containing tombstoned events
/// 2. Rewrite batches without the tombstoned events
/// 3. Update event_index byte offsets
/// 4. Delete old batches
/// 5. Delete processed tombstone records
/// 6. VACUUM to reclaim space
///
/// # Important Notes
///
/// - Compaction is an offline/maintenance operation
/// - It should not run during normal operation
/// - It requires exclusive write access
/// - Global positions are preserved (never reused)
#[derive(Debug, Clone)]
pub struct CompactionJob {
    /// Hash of the stream to compact (or None for all streams).
    pub stream_hash: Option<StreamHash>,

    /// Collision slot (if stream_hash is Some).
    pub collision_slot: Option<CollisionSlot>,
}

impl CompactionJob {
    /// Creates a compaction job for a specific stream.
    pub fn for_stream(stream_hash: StreamHash, collision_slot: CollisionSlot) -> Self {
        Self {
            stream_hash: Some(stream_hash),
            collision_slot: Some(collision_slot),
        }
    }

    /// Creates a compaction job for all tombstoned data.
    pub fn for_all() -> Self {
        Self {
            stream_hash: None,
            collision_slot: None,
        }
    }

    /// Returns batch IDs that contain tombstoned events.
    ///
    /// This identifies which batches need to be rewritten during compaction.
    pub fn find_affected_batches(&self, conn: &Connection) -> Result<Vec<i64>> {
        let query = match &self.stream_hash {
            Some(hash) => {
                let collision_slot = self.collision_slot.unwrap_or(CollisionSlot::FIRST);
                conn.prepare(
                    "SELECT DISTINCT e.batch_id
                     FROM event_index e
                     JOIN tombstones t ON e.stream_hash = t.stream_hash
                                      AND e.collision_slot = t.collision_slot
                     WHERE e.stream_hash = ?
                       AND e.collision_slot = ?
                       AND e.stream_rev >= t.from_rev
                       AND e.stream_rev <= t.to_rev",
                )?
                .query_map(params![hash.as_raw(), collision_slot.as_raw() as i64], |row| {
                    row.get(0)
                })?
                .collect::<std::result::Result<Vec<i64>, _>>()?
            }
            None => {
                // Find all affected batches across all tombstoned streams
                conn.prepare(
                    "SELECT DISTINCT e.batch_id
                     FROM event_index e
                     JOIN tombstones t ON e.stream_hash = t.stream_hash
                                      AND e.collision_slot = t.collision_slot
                     WHERE e.stream_rev >= t.from_rev
                       AND e.stream_rev <= t.to_rev",
                )?
                .query_map([], |row| row.get(0))?
                .collect::<std::result::Result<Vec<i64>, _>>()?
            }
        };

        Ok(query)
    }

    /// Placeholder for future compaction implementation.
    ///
    /// # Panics
    ///
    /// Always panics - compaction is deferred to a future implementation.
    pub fn execute(&self, _conn: &Connection) -> Result<CompactionStats> {
        unimplemented!(
            "Compaction is deferred to offline maintenance job. \
             Use find_affected_batches() to identify what needs compaction."
        )
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Database;

    fn setup_test_db() -> Connection {
        let db = Database::open_in_memory().unwrap();
        db.into_connection()
    }

    #[test]
    fn test_load_stream_tombstones_empty() {
        let conn = setup_test_db();
        let stream_hash = StreamHash::from_raw(12345);
        let tenant_hash = TenantHash::default_hash();
        let tombstones =
            load_stream_tombstones(&conn, stream_hash, CollisionSlot::FIRST, tenant_hash).unwrap();
        assert!(tombstones.is_empty());
    }

    #[test]
    fn test_load_stream_tombstones() {
        let conn = setup_test_db();
        let stream_hash = StreamHash::from_raw(12345);
        let tenant_hash = TenantHash::default_hash();

        // Insert a tombstone
        conn.execute(
            "INSERT INTO tombstones (stream_hash, collision_slot, tenant_hash, from_rev, to_rev, deleted_ms)
             VALUES (?, 0, ?, 1, 10, 1000)",
            params![stream_hash.as_raw(), tenant_hash.as_raw()],
        )
        .unwrap();

        let tombstones =
            load_stream_tombstones(&conn, stream_hash, CollisionSlot::FIRST, tenant_hash).unwrap();
        assert_eq!(tombstones.len(), 1);
        assert_eq!(tombstones[0].from_rev.as_raw(), 1);
        assert_eq!(tombstones[0].to_rev.as_raw(), 10);
    }

    #[test]
    fn test_is_revision_tombstoned() {
        let tombstones = vec![StreamTombstone {
            stream_hash: StreamHash::from_raw(1),
            collision_slot: CollisionSlot::FIRST,
            tenant_hash: TenantHash::default_hash(),
            from_rev: StreamRev::from_raw(5),
            to_rev: StreamRev::from_raw(10),
            deleted_ms: 1000,
        }];

        // Before range
        assert!(!is_revision_tombstoned(&tombstones, StreamRev::from_raw(4)));

        // In range
        assert!(is_revision_tombstoned(&tombstones, StreamRev::from_raw(5)));
        assert!(is_revision_tombstoned(&tombstones, StreamRev::from_raw(7)));
        assert!(is_revision_tombstoned(&tombstones, StreamRev::from_raw(10)));

        // After range
        assert!(!is_revision_tombstoned(&tombstones, StreamRev::from_raw(11)));
    }

    #[test]
    fn test_load_tenant_tombstones_empty() {
        let conn = setup_test_db();
        let deleted = load_tenant_tombstones(&conn).unwrap();
        assert!(deleted.is_empty());
    }

    #[test]
    fn test_load_tenant_tombstones() {
        let conn = setup_test_db();

        // Insert tenant tombstones
        conn.execute(
            "INSERT INTO tenant_tombstones (tenant_hash, deleted_ms) VALUES (100, 1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tenant_tombstones (tenant_hash, deleted_ms) VALUES (200, 2000)",
            [],
        )
        .unwrap();

        let deleted = load_tenant_tombstones(&conn).unwrap();
        assert_eq!(deleted.len(), 2);
        assert!(deleted.contains(&TenantHash::from_raw(100)));
        assert!(deleted.contains(&TenantHash::from_raw(200)));
    }

    #[test]
    fn test_is_tenant_tombstoned() {
        let conn = setup_test_db();

        let tenant_hash = TenantHash::from_raw(12345);

        // Not tombstoned initially
        assert!(!is_tenant_tombstoned(&conn, tenant_hash).unwrap());

        // Insert tombstone
        conn.execute(
            "INSERT INTO tenant_tombstones (tenant_hash, deleted_ms) VALUES (?, 1000)",
            params![tenant_hash.as_raw()],
        )
        .unwrap();

        // Now tombstoned
        assert!(is_tenant_tombstoned(&conn, tenant_hash).unwrap());
    }

    #[test]
    fn test_filter_tenant_events() {
        use crate::types::{GlobalPos, StreamId};

        let deleted_tenants: HashSet<TenantHash> =
            [TenantHash::from_raw(100)].into_iter().collect();

        let events = vec![
            Event {
                global_pos: GlobalPos::FIRST,
                stream_id: StreamId::new("stream-1"),
                tenant_hash: TenantHash::from_raw(100), // deleted
                collision_slot: CollisionSlot::FIRST,
                stream_rev: StreamRev::from_raw(1),
                timestamp_ms: 1000,
                data: vec![1],
            },
            Event {
                global_pos: GlobalPos::from_raw(2),
                stream_id: StreamId::new("stream-2"),
                tenant_hash: TenantHash::from_raw(200), // not deleted
                collision_slot: CollisionSlot::FIRST,
                stream_rev: StreamRev::from_raw(1),
                timestamp_ms: 1000,
                data: vec![2],
            },
        ];

        let filtered = filter_tenant_events(events, &deleted_tenants);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].data, vec![2]);
    }

    #[test]
    fn test_stream_tombstone_contains() {
        let tombstone = StreamTombstone {
            stream_hash: StreamHash::from_raw(1),
            collision_slot: CollisionSlot::FIRST,
            tenant_hash: TenantHash::default_hash(),
            from_rev: StreamRev::from_raw(5),
            to_rev: StreamRev::from_raw(10),
            deleted_ms: 1000,
        };

        assert!(!tombstone.contains(StreamRev::from_raw(4)));
        assert!(tombstone.contains(StreamRev::from_raw(5)));
        assert!(tombstone.contains(StreamRev::from_raw(7)));
        assert!(tombstone.contains(StreamRev::from_raw(10)));
        assert!(!tombstone.contains(StreamRev::from_raw(11)));
    }

    #[test]
    fn test_compaction_job_find_affected_batches_empty() {
        let conn = setup_test_db();
        let job = CompactionJob::for_all();
        let batches = job.find_affected_batches(&conn).unwrap();
        assert!(batches.is_empty());
    }
}
