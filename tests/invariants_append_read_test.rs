mod common;

use spitedb::reader;
use spitedb::types::{
    AppendCommand, DeleteStreamCommand, EventData, GlobalPos, StreamId, StreamRev, Tenant,
};
use spitedb::{spawn_batch_writer, Database, Error};

#[tokio::test]
async fn global_positions_and_stream_revisions_are_gapless() {
    let (_dir, path) = common::create_temp_db_file("invariants.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let s1 = StreamId::new("stream-1");
    let s2 = StreamId::new("stream-2");

    let r1 = writer
        .append(AppendCommand::new(
            "cmd-1",
            s1.clone(),
            StreamRev::NONE,
            vec![
                EventData::new(b"s1-e1".to_vec()),
                EventData::new(b"s1-e2".to_vec()),
                EventData::new(b"s1-e3".to_vec()),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(r1.first_pos.as_raw(), 1);
    assert_eq!(r1.last_pos.as_raw(), 3);
    assert_eq!(r1.first_rev.as_raw(), 1);
    assert_eq!(r1.last_rev.as_raw(), 3);

    let r2 = writer
        .append(AppendCommand::new(
            "cmd-2",
            s1.clone(),
            StreamRev::from_raw(3),
            vec![EventData::new(b"s1-e4".to_vec()), EventData::new(b"s1-e5".to_vec())],
        ))
        .await
        .unwrap();
    assert_eq!(r2.first_pos.as_raw(), 4);
    assert_eq!(r2.last_pos.as_raw(), 5);
    assert_eq!(r2.first_rev.as_raw(), 4);
    assert_eq!(r2.last_rev.as_raw(), 5);

    let r3 = writer
        .append(AppendCommand::new(
            "cmd-3",
            s2.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"s2-e1".to_vec())],
        ))
        .await
        .unwrap();
    assert_eq!(r3.first_pos.as_raw(), 6);
    assert_eq!(r3.last_pos.as_raw(), 6);

    drop(writer);

    let read_conn = common::open_read_only(&path);

    let global = reader::read_global(&read_conn, GlobalPos::FIRST, 1000, &cryptor).unwrap();
    assert_eq!(global.len(), 6);
    for (i, e) in global.iter().enumerate() {
        assert_eq!(e.global_pos.as_raw(), (i + 1) as u64);
    }

    let s1_events = reader::read_stream(&read_conn, &s1, StreamRev::FIRST, 1000, &cryptor).unwrap();
    assert_eq!(s1_events.len(), 5);
    for (i, e) in s1_events.iter().enumerate() {
        assert_eq!(e.stream_rev.as_raw(), (i + 1) as u64);
    }
    assert_eq!(s1_events[0].data, b"s1-e1");
    assert_eq!(s1_events[4].data, b"s1-e5");

    let global_slice = reader::read_global(&read_conn, GlobalPos::from_raw_unchecked(3), 2, &cryptor).unwrap();
    assert_eq!(global_slice.len(), 2);
    assert_eq!(global_slice[0].global_pos.as_raw(), 3);
    assert_eq!(global_slice[1].global_pos.as_raw(), 4);

    let s1_slice = reader::read_stream(&read_conn, &s1, StreamRev::from_raw(4), 10, &cryptor).unwrap();
    assert_eq!(s1_slice.len(), 2);
    assert_eq!(s1_slice[0].stream_rev.as_raw(), 4);
    assert_eq!(s1_slice[1].stream_rev.as_raw(), 5);

    let missing = reader::read_stream(
        &read_conn,
        &StreamId::new("does-not-exist"),
        StreamRev::FIRST,
        10,
        &cryptor,
    )
    .unwrap();
    assert!(missing.is_empty());
}

// =============================================================================
// Global Position Invariant Tests
// =============================================================================

/// Tests that global_pos strictly increases across concurrent appends.
/// Multiple concurrent tasks should all get unique, strictly increasing positions.
#[tokio::test]
async fn global_pos_strictly_increases_across_concurrent_appends() {
    let (_dir, path) = common::create_temp_db_file("concurrent_pos.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        common::fast_writer_config(),
    )
    .unwrap();

    // Spawn 20 concurrent append tasks
    let mut handles = Vec::new();
    for i in 0..20 {
        let w = writer.clone();
        let handle = tokio::spawn(async move {
            w.append(AppendCommand::new(
                format!("cmd-{}", i),
                format!("stream-{}", i),
                StreamRev::NONE,
                vec![EventData::new(format!("event-{}", i).into_bytes())],
            ))
            .await
        });
        handles.push(handle);
    }

    // Collect all results
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap().unwrap());
    }

    drop(writer);

    // Verify via direct database query
    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);

    assert_eq!(events.len(), 20);
    common::verify_global_pos_gapless(&events);
}

/// Tests that global_pos is never reused after a restart.
#[tokio::test]
async fn global_pos_never_reused_after_restart() {
    let (_dir, path) = common::create_temp_db_file("restart_pos.db");
    let cryptor = common::test_cryptor();

    // First writer instance
    let writer_db1 = Database::open(&path).unwrap();
    let writer1 = spawn_batch_writer(
        writer_db1.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let r1 = writer1
        .append(AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"before-restart".to_vec())],
        ))
        .await
        .unwrap();

    let max_pos_before = r1.last_pos.as_raw();
    drop(writer1);

    // Second writer instance (restart)
    let writer_db2 = Database::open(&path).unwrap();
    let writer2 = spawn_batch_writer(
        writer_db2.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let r2 = writer2
        .append(AppendCommand::new(
            "cmd-2",
            "stream-1",
            StreamRev::from_raw(1),
            vec![EventData::new(b"after-restart".to_vec())],
        ))
        .await
        .unwrap();

    // New position must be strictly greater than max before restart
    assert!(
        r2.first_pos.as_raw() > max_pos_before,
        "global_pos reused after restart: {} should be > {}",
        r2.first_pos.as_raw(),
        max_pos_before
    );

    drop(writer2);

    // Verify no gaps in the database
    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    common::verify_global_pos_gapless(&events);
}

/// Tests that global_pos is never reused after compaction.
#[tokio::test]
async fn global_pos_never_reused_after_compaction() {
    let (_dir, path) = common::create_temp_db_file("compaction_pos.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Write events
    let r1 = writer
        .append(AppendCommand::new(
            "cmd-1",
            "stream-to-delete",
            StreamRev::NONE,
            vec![
                EventData::new(b"e1".to_vec()),
                EventData::new(b"e2".to_vec()),
            ],
        ))
        .await
        .unwrap();

    let max_pos_before_delete = r1.last_pos.as_raw();

    // Delete the stream
    writer
        .delete_stream(DeleteStreamCommand::delete_all(
            "cmd-delete",
            "stream-to-delete",
            Tenant::default_tenant(),
        ))
        .await
        .unwrap();

    // Compact to physically remove
    writer.compact(None).await.unwrap();

    // Write more events
    let r2 = writer
        .append(AppendCommand::new(
            "cmd-2",
            "stream-new",
            StreamRev::NONE,
            vec![EventData::new(b"after-compaction".to_vec())],
        ))
        .await
        .unwrap();

    // New position must be strictly greater - positions are never reused
    assert!(
        r2.first_pos.as_raw() > max_pos_before_delete,
        "global_pos reused after compaction: {} should be > {}",
        r2.first_pos.as_raw(),
        max_pos_before_delete
    );
}

/// Tests that events in a single append are always contiguous.
#[tokio::test]
async fn global_pos_contiguous_within_single_append() {
    let (_dir, path) = common::create_temp_db_file("contiguous.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Single append with many events
    let result = writer
        .append(AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            (0..100)
                .map(|i| EventData::new(format!("event-{}", i).into_bytes()))
                .collect(),
        ))
        .await
        .unwrap();

    // Result should report contiguous range
    assert_eq!(result.event_count(), 100);
    assert_eq!(
        result.last_pos.as_raw() - result.first_pos.as_raw() + 1,
        100
    );

    drop(writer);

    // Verify in database
    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);

    assert_eq!(events.len(), 100);
    common::verify_global_pos_gapless(&events);
}

/// Tests that conflicts don't waste global positions.
#[tokio::test]
async fn global_pos_no_gaps_after_conflict_in_batch() {
    let (_dir, path) = common::create_temp_db_file("conflict_gaps.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create stream-1 first
    writer
        .append(AppendCommand::new(
            "cmd-setup",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"setup".to_vec())],
        ))
        .await
        .unwrap();

    // Transaction with a conflict in the middle
    let mut tx = writer.begin_transaction();
    tx.append(AppendCommand::new(
        "cmd-a",
        "stream-a",
        StreamRev::NONE,
        vec![EventData::new(b"a".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-conflict",
        "stream-1",
        StreamRev::NONE, // Conflict - stream-1 is at rev 1
        vec![EventData::new(b"conflict".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-b",
        "stream-b",
        StreamRev::NONE,
        vec![EventData::new(b"b".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok());

    drop(writer);

    // Verify no gaps in global positions (conflict shouldn't waste positions)
    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);

    assert_eq!(events.len(), 3); // setup + a + b
    common::verify_global_pos_gapless(&events);
}

// =============================================================================
// Stream Revision Invariant Tests
// =============================================================================

/// Tests that stream_rev conflict is detected when expected_rev doesn't match.
#[tokio::test]
async fn stream_rev_conflict_detected_wrong_expected_rev() {
    let (_dir, path) = common::create_temp_db_file("conflict_rev.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create stream with 3 events
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

    // Try to append expecting rev 2 (wrong - actual is 3)
    let err = writer
        .append(AppendCommand::new(
            "cmd-2",
            "stream-1",
            StreamRev::from_raw(2), // Wrong!
            vec![EventData::new(b"should-fail".to_vec())],
        ))
        .await
        .unwrap_err();

    // Verify we get a conflict error with correct info
    match err {
        Error::Conflict {
            stream_id,
            expected,
            actual,
        } => {
            assert_eq!(stream_id, "stream-1");
            assert_eq!(expected, 2);
            assert_eq!(actual, 3);
        }
        other => panic!("Expected Conflict error, got: {:?}", other),
    }
}

/// Tests conflict when using NONE on existing stream.
#[tokio::test]
async fn stream_rev_conflict_with_none_on_existing_stream() {
    let (_dir, path) = common::create_temp_db_file("conflict_none.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create stream
    writer
        .append(AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"first".to_vec())],
        ))
        .await
        .unwrap();

    // Try to "create" again with NONE
    let err = writer
        .append(AppendCommand::new(
            "cmd-2",
            "stream-1",
            StreamRev::NONE, // Says stream shouldn't exist
            vec![EventData::new(b"should-fail".to_vec())],
        ))
        .await
        .unwrap_err();

    assert!(
        matches!(err, Error::Conflict { expected: 0, actual: 1, .. }),
        "Expected conflict with expected=0, actual=1"
    );
}

/// Tests conflict when expecting non-zero rev on new stream.
#[tokio::test]
async fn stream_rev_conflict_with_value_on_new_stream() {
    let (_dir, path) = common::create_temp_db_file("conflict_value.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Try to append to non-existent stream expecting rev 5
    let err = writer
        .append(AppendCommand::new(
            "cmd-1",
            "stream-new",
            StreamRev::from_raw(5), // Stream doesn't exist
            vec![EventData::new(b"should-fail".to_vec())],
        ))
        .await
        .unwrap_err();

    assert!(
        matches!(err, Error::Conflict { expected: 5, actual: 0, .. }),
        "Expected conflict with expected=5, actual=0"
    );
}

/// Tests that StreamRev::ANY bypasses conflict checking.
#[tokio::test]
async fn stream_rev_any_bypasses_conflict_check() {
    let (_dir, path) = common::create_temp_db_file("any_rev.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create stream
    writer
        .append(AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"first".to_vec())],
        ))
        .await
        .unwrap();

    // Append with ANY - should succeed regardless of current rev
    let result = writer
        .append(AppendCommand::new(
            "cmd-2",
            "stream-1",
            StreamRev::ANY,
            vec![EventData::new(b"second".to_vec())],
        ))
        .await
        .unwrap();

    assert_eq!(result.first_rev.as_raw(), 2);

    // Also works on new streams
    let result2 = writer
        .append(AppendCommand::new(
            "cmd-3",
            "stream-new",
            StreamRev::ANY,
            vec![EventData::new(b"new".to_vec())],
        ))
        .await
        .unwrap();

    assert_eq!(result2.first_rev.as_raw(), 1);
}

/// Tests that stream revisions are independent per stream.
#[tokio::test]
async fn stream_rev_independent_per_stream() {
    let (_dir, path) = common::create_temp_db_file("independent_streams.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Write to stream-1
    let r1 = writer
        .append(AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![
                EventData::new(b"s1-e1".to_vec()),
                EventData::new(b"s1-e2".to_vec()),
            ],
        ))
        .await
        .unwrap();

    // Write to stream-2
    let r2 = writer
        .append(AppendCommand::new(
            "cmd-2",
            "stream-2",
            StreamRev::NONE,
            vec![EventData::new(b"s2-e1".to_vec())],
        ))
        .await
        .unwrap();

    // Stream-1 should be at rev 2, stream-2 at rev 1
    assert_eq!(r1.last_rev.as_raw(), 2);
    assert_eq!(r2.last_rev.as_raw(), 1);

    // Can append to stream-2 expecting rev 1
    let r3 = writer
        .append(AppendCommand::new(
            "cmd-3",
            "stream-2",
            StreamRev::from_raw(1),
            vec![EventData::new(b"s2-e2".to_vec())],
        ))
        .await
        .unwrap();

    assert_eq!(r3.first_rev.as_raw(), 2);

    drop(writer);

    // Verify in database
    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);

    let s1 = StreamId::new("stream-1");
    let s2 = StreamId::new("stream-2");

    common::verify_stream_rev_gapless(&events, s1.hash().as_raw());
    common::verify_stream_rev_gapless(&events, s2.hash().as_raw());
}

/// Tests that the same stream_id in different tenants have independent revisions.
#[tokio::test]
async fn stream_rev_independent_per_tenant_same_stream_id() {
    let (_dir, path) = common::create_temp_db_file("tenant_streams.db");
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

    // Write to "user-1" in tenant A
    let r1 = writer
        .append(AppendCommand::new_with_tenant(
            "cmd-a-1",
            "user-1",
            tenant_a.clone(),
            StreamRev::NONE,
            vec![
                EventData::new(b"a1".to_vec()),
                EventData::new(b"a2".to_vec()),
                EventData::new(b"a3".to_vec()),
            ],
        ))
        .await
        .unwrap();

    // Write to "user-1" in tenant B (same stream_id, different tenant)
    let r2 = writer
        .append(AppendCommand::new_with_tenant(
            "cmd-b-1",
            "user-1",
            tenant_b.clone(),
            StreamRev::NONE, // NONE because it's a new stream for this tenant
            vec![EventData::new(b"b1".to_vec())],
        ))
        .await
        .unwrap();

    // Each tenant's stream should have independent revisions
    assert_eq!(r1.last_rev.as_raw(), 3);
    assert_eq!(r2.last_rev.as_raw(), 1);

    // Can continue each stream independently
    let r3 = writer
        .append(AppendCommand::new_with_tenant(
            "cmd-a-2",
            "user-1",
            tenant_a.clone(),
            StreamRev::from_raw(3), // Tenant A at rev 3
            vec![EventData::new(b"a4".to_vec())],
        ))
        .await
        .unwrap();

    let r4 = writer
        .append(AppendCommand::new_with_tenant(
            "cmd-b-2",
            "user-1",
            tenant_b.clone(),
            StreamRev::from_raw(1), // Tenant B at rev 1
            vec![EventData::new(b"b2".to_vec())],
        ))
        .await
        .unwrap();

    assert_eq!(r3.first_rev.as_raw(), 4);
    assert_eq!(r4.first_rev.as_raw(), 2);

    drop(writer);

    // Verify by reading tenant-specific events
    let read_conn = common::open_read_only(&path);
    let tenant_a_events =
        reader::read_tenant_events(&read_conn, tenant_a.hash(), GlobalPos::FIRST, 100, &cryptor)
            .unwrap();
    let tenant_b_events =
        reader::read_tenant_events(&read_conn, tenant_b.hash(), GlobalPos::FIRST, 100, &cryptor)
            .unwrap();

    assert_eq!(tenant_a_events.len(), 4);
    assert_eq!(tenant_b_events.len(), 2);

    // Verify revisions are gapless for each tenant's view
    for (i, e) in tenant_a_events.iter().enumerate() {
        assert_eq!(e.stream_rev.as_raw(), (i + 1) as u64);
    }
    for (i, e) in tenant_b_events.iter().enumerate() {
        assert_eq!(e.stream_rev.as_raw(), (i + 1) as u64);
    }
}

