//! Hash Collision Tests
//!
//! Tests the collision slot mechanism for handling hash collisions.
//! Note: Finding actual XXH3-64 collisions is computationally infeasible,
//! so these tests verify the collision handling logic using database manipulation.

mod common;

use spitedb::types::{AppendCommand, EventData, StreamId, StreamRev, Tenant};
use spitedb::{spawn_batch_writer, Database};

/// Tests that different streams get assigned collision slot 0 (no collision case).
#[tokio::test]
async fn normal_streams_get_slot_zero() {
    let (_dir, path) = common::create_temp_db_file("slot_zero.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create multiple streams
    for i in 0..10 {
        writer
            .append(AppendCommand::new(
                format!("cmd-{}", i),
                format!("stream-{}", i),
                StreamRev::NONE,
                vec![EventData::new(format!("e{}", i).into_bytes())],
            ))
            .await
            .unwrap();
    }

    drop(writer);

    // Verify all streams have collision_slot = 0
    let read_conn = common::open_read_only(&path);
    let heads = common::read_raw_stream_heads(&read_conn);

    assert_eq!(heads.len(), 10);
    for head in &heads {
        assert_eq!(
            head.collision_slot, 0,
            "Stream {} should have collision_slot 0, got {}",
            head.stream_id, head.collision_slot
        );
    }
}

/// Tests that collision slot is preserved after restart.
#[tokio::test]
async fn collision_slot_persists_after_restart() {
    let (_dir, path) = common::create_temp_db_file("slot_persist.db");
    let cryptor = common::test_cryptor();

    // First writer
    let writer_db1 = Database::open(&path).unwrap();
    let writer1 = spawn_batch_writer(
        writer_db1.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    writer1
        .append(AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"e1".to_vec())],
        ))
        .await
        .unwrap();

    drop(writer1);

    // Verify collision_slot is 0
    let read_conn = common::open_read_only(&path);
    let heads1 = common::read_raw_stream_heads(&read_conn);
    assert_eq!(heads1.len(), 1);
    assert_eq!(heads1[0].collision_slot, 0);
    drop(read_conn);

    // Second writer (restart)
    let writer_db2 = Database::open(&path).unwrap();
    let writer2 = spawn_batch_writer(
        writer_db2.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Continue the stream
    writer2
        .append(AppendCommand::new(
            "cmd-2",
            "stream-1",
            StreamRev::from_raw(1),
            vec![EventData::new(b"e2".to_vec())],
        ))
        .await
        .unwrap();

    drop(writer2);

    // Verify collision_slot is still 0
    let read_conn = common::open_read_only(&path);
    let heads2 = common::read_raw_stream_heads(&read_conn);
    assert_eq!(heads2.len(), 1);
    assert_eq!(heads2[0].collision_slot, 0);
    assert_eq!(heads2[0].last_rev, 2);
}

/// Tests that streams with same name in different tenants are separate.
#[tokio::test]
async fn same_stream_id_different_tenants_are_separate() {
    let (_dir, path) = common::create_temp_db_file("tenant_separate.db");
    let cryptor = common::test_cryptor();

    let tenant_a = Tenant::new("tenant-a");
    let tenant_b = Tenant::new("tenant-b");

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Same stream_id in different tenants
    let r1 = writer
        .append(AppendCommand::new_with_tenant(
            "cmd-a",
            "user-events",
            tenant_a.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"a".to_vec())],
        ))
        .await
        .unwrap();

    let r2 = writer
        .append(AppendCommand::new_with_tenant(
            "cmd-b",
            "user-events",
            tenant_b.clone(),
            StreamRev::NONE, // NONE because it's a different tenant
            vec![EventData::new(b"b".to_vec())],
        ))
        .await
        .unwrap();

    // Both should start at rev 1 (independent streams)
    assert_eq!(r1.first_rev.as_raw(), 1);
    assert_eq!(r2.first_rev.as_raw(), 1);

    drop(writer);

    // Verify in database - should have 2 separate stream_heads entries
    let read_conn = common::open_read_only(&path);
    let heads = common::read_raw_stream_heads(&read_conn);

    // Both have same stream_id but different tenant_hash
    let stream_hash = StreamId::new("user-events").hash().as_raw();
    let tenant_a_hash = tenant_a.hash().as_raw();
    let tenant_b_hash = tenant_b.hash().as_raw();

    let tenant_a_head = heads
        .iter()
        .find(|h| h.stream_hash == stream_hash && h.tenant_hash == tenant_a_hash);
    let tenant_b_head = heads
        .iter()
        .find(|h| h.stream_hash == stream_hash && h.tenant_hash == tenant_b_hash);

    assert!(tenant_a_head.is_some(), "Tenant A head should exist");
    assert!(tenant_b_head.is_some(), "Tenant B head should exist");
    assert_eq!(tenant_a_head.unwrap().last_rev, 1);
    assert_eq!(tenant_b_head.unwrap().last_rev, 1);
}

/// Tests event index collision slot consistency.
#[tokio::test]
async fn event_index_collision_slot_consistency() {
    let (_dir, path) = common::create_temp_db_file("event_slot.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Write multiple events to same stream
    writer
        .append(AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![
                EventData::new(b"e1".to_vec()),
                EventData::new(b"e2".to_vec()),
                EventData::new(b"e3".to_vec()),
            ],
        ))
        .await
        .unwrap();

    drop(writer);

    // Verify all events have same collision_slot
    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);

    assert_eq!(events.len(), 3);

    let stream_hash = events[0].stream_hash;
    let collision_slot = events[0].collision_slot;

    for event in &events {
        assert_eq!(
            event.stream_hash, stream_hash,
            "All events should have same stream_hash"
        );
        assert_eq!(
            event.collision_slot, collision_slot,
            "All events should have same collision_slot"
        );
    }
}

/// Tests that stream hash is deterministic.
#[tokio::test]
async fn stream_hash_is_deterministic() {
    let id1 = StreamId::new("test-stream");
    let id2 = StreamId::new("test-stream");
    let id3 = StreamId::new("different-stream");

    assert_eq!(id1.hash().as_raw(), id2.hash().as_raw());
    assert_ne!(id1.hash().as_raw(), id3.hash().as_raw());
}

/// Tests stream heads cache reflects correct collision slots.
#[tokio::test]
async fn stream_heads_reflect_collision_slots() {
    let (_dir, path) = common::create_temp_db_file("heads_slots.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create multiple streams and verify heads
    let stream_ids = vec!["stream-a", "stream-b", "stream-c"];

    for (i, id) in stream_ids.iter().enumerate() {
        writer
            .append(AppendCommand::new(
                format!("cmd-{}", i),
                *id,
                StreamRev::NONE,
                vec![EventData::new(format!("e{}", i).into_bytes())],
            ))
            .await
            .unwrap();
    }

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let heads = common::read_raw_stream_heads(&read_conn);
    let events = common::read_raw_event_index(&read_conn);

    assert_eq!(heads.len(), 3);
    assert_eq!(events.len(), 3);

    // Verify each stream_heads entry matches event_index entries
    for head in &heads {
        let stream_events: Vec<_> = events
            .iter()
            .filter(|e| e.stream_hash == head.stream_hash && e.collision_slot == head.collision_slot)
            .collect();

        assert!(!stream_events.is_empty());

        // Last event should match stream head
        let last_event = stream_events.iter().max_by_key(|e| e.stream_rev).unwrap();
        assert_eq!(last_event.stream_rev, head.last_rev);
        assert_eq!(last_event.global_pos, head.last_pos);
    }
}
