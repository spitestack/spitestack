mod common;

use spitedb::reader;
use spitedb::types::{
    AppendCommand, DeleteStreamCommand, DeleteTenantCommand, EventData, GlobalPos, StreamId,
    StreamRev, Tenant,
};
use spitedb::{spawn_batch_writer, Database};

/// Verifies that compaction physically deletes tombstoned events and cleans up tombstone records.
#[tokio::test]
async fn compaction_removes_tombstoned_events_and_cleans_up_tombstones() {
    let (_dir, path) = common::create_temp_db_file("compaction.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let stream = StreamId::new("stream-to-compact");

    // Append some events
    writer
        .append(AppendCommand::new(
            "cmd-1",
            stream.clone(),
            StreamRev::NONE,
            vec![
                EventData::new(b"event1".to_vec()),
                EventData::new(b"event2".to_vec()),
                EventData::new(b"event3".to_vec()),
            ],
        ))
        .await
        .unwrap();

    // Wait for WAL sync
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let read_conn = common::open_read_only(&path);

    // Verify events exist before delete
    let before = reader::read_stream(&read_conn, &stream, StreamRev::FIRST, 100, &cryptor).unwrap();
    assert_eq!(before.len(), 3);

    // Count batches and event_index entries before compaction
    let batches_before: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM batches", [], |row| row.get(0))
        .unwrap();
    let events_before: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM event_index", [], |row| row.get(0))
        .unwrap();
    assert_eq!(batches_before, 1);
    assert_eq!(events_before, 3);

    // Delete the entire stream (creates tombstone)
    let delete_cmd = DeleteStreamCommand::delete_all("cmd-delete", stream.clone(), Tenant::default_tenant());
    writer.delete_stream(delete_cmd).await.unwrap();

    // Verify tombstone was created
    let tombstones_count: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM tombstones", [], |row| row.get(0))
        .unwrap();
    assert_eq!(tombstones_count, 1, "tombstone should be created");

    // Events should be filtered from reads (logical delete)
    let after_delete = reader::read_stream(&read_conn, &stream, StreamRev::FIRST, 100, &cryptor).unwrap();
    assert_eq!(after_delete.len(), 0, "events should be filtered after tombstone");

    // But physical data still exists
    let events_after_delete: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM event_index", [], |row| row.get(0))
        .unwrap();
    assert_eq!(events_after_delete, 3, "event_index entries should still exist before compaction");

    // Trigger compaction
    let stats = writer.compact(None).await.unwrap();

    // Verify compaction stats
    assert_eq!(stats.batches_rewritten, 1, "one batch should be rewritten (deleted)");
    assert_eq!(stats.events_deleted, 3, "three events should be deleted");
    assert_eq!(stats.tombstones_removed, 1, "tombstone should be removed after compaction");

    // Verify physical deletion - batch should be deleted entirely since all events were tombstoned
    drop(read_conn);
    let read_conn = common::open_read_only(&path);

    let batches_after: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM batches", [], |row| row.get(0))
        .unwrap();
    let events_after: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM event_index", [], |row| row.get(0))
        .unwrap();
    let tombstones_after: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM tombstones", [], |row| row.get(0))
        .unwrap();

    assert_eq!(batches_after, 0, "batch should be deleted when all events are tombstoned");
    assert_eq!(events_after, 0, "event_index entries should be deleted");
    assert_eq!(tombstones_after, 0, "tombstone should be cleaned up");
}

/// Verifies that compaction rewrites batches with partial tombstones (some events kept).
#[tokio::test]
async fn compaction_rewrites_batch_with_partial_tombstones() {
    let (_dir, path) = common::create_temp_db_file("compaction_partial.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let stream = StreamId::new("stream-partial");

    // Append events
    writer
        .append(AppendCommand::new(
            "cmd-1",
            stream.clone(),
            StreamRev::NONE,
            vec![
                EventData::new(b"keep1".to_vec()),
                EventData::new(b"delete2".to_vec()),
                EventData::new(b"delete3".to_vec()),
                EventData::new(b"keep4".to_vec()),
            ],
        ))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let read_conn = common::open_read_only(&path);

    // Delete only revisions 2-3
    let delete_cmd = DeleteStreamCommand::delete_range(
        "cmd-delete-range",
        stream.clone(),
        Tenant::default_tenant(),
        StreamRev::from_raw(2),
        StreamRev::from_raw(3),
    );
    writer.delete_stream(delete_cmd).await.unwrap();

    // Verify reads show only kept events
    let after_delete = reader::read_stream(&read_conn, &stream, StreamRev::FIRST, 100, &cryptor).unwrap();
    assert_eq!(after_delete.len(), 2);
    assert_eq!(after_delete[0].data, b"keep1");
    assert_eq!(after_delete[1].data, b"keep4");

    // Get batch size before compaction
    let batch_size_before: i64 = read_conn
        .query_row("SELECT LENGTH(data) FROM batches", [], |row| row.get(0))
        .unwrap();

    // Trigger compaction
    let stats = writer.compact(None).await.unwrap();

    assert_eq!(stats.batches_rewritten, 1);
    assert_eq!(stats.events_deleted, 2, "two events should be deleted");

    // Verify batch still exists but was rewritten (smaller)
    drop(read_conn);
    let read_conn = common::open_read_only(&path);

    let batches_after: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM batches", [], |row| row.get(0))
        .unwrap();
    let events_after: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM event_index", [], |row| row.get(0))
        .unwrap();
    let batch_size_after: i64 = read_conn
        .query_row("SELECT LENGTH(data) FROM batches", [], |row| row.get(0))
        .unwrap();

    assert_eq!(batches_after, 1, "batch should still exist");
    assert_eq!(events_after, 2, "only kept events should remain in index");
    assert!(
        batch_size_after < batch_size_before,
        "batch should be smaller after removing events: before={} after={}",
        batch_size_before,
        batch_size_after
    );

    // Verify reads still work correctly after compaction
    let final_events = reader::read_stream(&read_conn, &stream, StreamRev::FIRST, 100, &cryptor).unwrap();
    assert_eq!(final_events.len(), 2);
    assert_eq!(final_events[0].data, b"keep1");
    assert_eq!(final_events[0].stream_rev.as_raw(), 1);
    assert_eq!(final_events[1].data, b"keep4");
    assert_eq!(final_events[1].stream_rev.as_raw(), 4);
}

/// Verifies that tenant tombstones are compacted correctly.
#[tokio::test]
async fn compaction_removes_tenant_tombstoned_events() {
    let (_dir, path) = common::create_temp_db_file("compaction_tenant.db");
    let cryptor = common::test_cryptor();

    let tenant_a = Tenant::new("tenant-to-delete");
    let tenant_b = Tenant::new("tenant-to-keep");

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create events for both tenants
    writer
        .append(AppendCommand::new_with_tenant(
            "cmd-a",
            "stream-a",
            tenant_a.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"tenant-a-data".to_vec())],
        ))
        .await
        .unwrap();

    writer
        .append(AppendCommand::new_with_tenant(
            "cmd-b",
            "stream-b",
            tenant_b.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"tenant-b-data".to_vec())],
        ))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let read_conn = common::open_read_only(&path);

    // Verify both events exist
    let before = reader::read_global(&read_conn, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert_eq!(before.len(), 2);

    // Delete tenant A
    writer
        .delete_tenant(DeleteTenantCommand::new("cmd-delete-tenant", tenant_a.clone()))
        .await
        .unwrap();

    // Verify tenant_tombstones was created
    let tenant_tombstones: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM tenant_tombstones", [], |row| row.get(0))
        .unwrap();
    assert_eq!(tenant_tombstones, 1);

    // Events from tenant A should be filtered
    let after_delete = reader::read_global(&read_conn, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert_eq!(after_delete.len(), 1);
    assert_eq!(after_delete[0].data, b"tenant-b-data");

    // Trigger compaction
    let stats = writer.compact(None).await.unwrap();

    assert!(stats.events_deleted >= 1, "at least one event should be deleted");

    // Verify tenant tombstone is cleaned up if no events remain
    drop(read_conn);
    let read_conn = common::open_read_only(&path);

    // Tenant B's event should still be readable
    let final_events = reader::read_global(&read_conn, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert_eq!(final_events.len(), 1);
    assert_eq!(final_events[0].data, b"tenant-b-data");
}

/// Verifies that compaction with no tombstones does nothing.
#[tokio::test]
async fn compaction_with_no_tombstones_is_noop() {
    let (_dir, path) = common::create_temp_db_file("compaction_noop.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Append some events (no tombstones)
    writer
        .append(AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"data".to_vec())],
        ))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Trigger compaction
    let stats = writer.compact(None).await.unwrap();

    // Nothing should happen
    assert_eq!(stats.batches_rewritten, 0);
    assert_eq!(stats.events_deleted, 0);
    assert_eq!(stats.tombstones_removed, 0);
    assert_eq!(stats.bytes_reclaimed, 0);

    // Events should still be readable
    let read_conn = common::open_read_only(&path);
    let events = reader::read_global(&read_conn, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert_eq!(events.len(), 1);
}

/// Verifies that incremental compaction respects max_batches limit.
#[tokio::test]
async fn compaction_respects_max_batches_limit() {
    let (_dir, path) = common::create_temp_db_file("compaction_limit.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create multiple batches by using small batch timeout
    for i in 0..5 {
        writer
            .append(AppendCommand::new(
                format!("cmd-{}", i),
                "stream-multi",
                if i == 0 { StreamRev::NONE } else { StreamRev::from_raw(i as u64) },
                vec![EventData::new(format!("event-{}", i).into_bytes())],
            ))
            .await
            .unwrap();
        // Small delay to potentially create separate batches (depends on batch timeout)
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    }

    let read_conn = common::open_read_only(&path);

    // Delete all events
    let delete_cmd = DeleteStreamCommand::delete_all(
        "cmd-delete-all",
        "stream-multi",
        Tenant::default_tenant(),
    );
    writer.delete_stream(delete_cmd).await.unwrap();

    // Count affected batches
    let batches_before: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM batches", [], |row| row.get(0))
        .unwrap();

    // Compact with limit of 2 batches
    let stats = writer.compact(Some(2)).await.unwrap();

    // Should have processed at most 2 batches
    assert!(
        stats.batches_rewritten <= 2,
        "should respect max_batches limit: got {}",
        stats.batches_rewritten
    );

    // If there were more batches, some should remain
    drop(read_conn);
    let read_conn = common::open_read_only(&path);
    let batches_after: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM batches", [], |row| row.get(0))
        .unwrap();

    if batches_before > 2 {
        assert!(
            batches_after > 0 || stats.batches_rewritten < batches_before as usize,
            "incremental compaction should leave some batches for next run"
        );
    }
}
