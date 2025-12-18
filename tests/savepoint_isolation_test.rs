//! SAVEPOINT Isolation Tests
//!
//! Tests the invariant that each command executes in its own SAVEPOINT,
//! providing isolation within a batch:
//! - Failure in one command doesn't affect others
//! - Rollback only affects the failing command
//! - Global positions are not wasted on conflicts

mod common;

use spitedb::types::{AppendCommand, EventData, StreamRev};
use spitedb::{spawn_batch_writer, Database};

/// Tests that a conflict in one command doesn't affect other commands in the same batch.
#[tokio::test]
async fn savepoint_conflict_does_not_affect_others() {
    let (_dir, path) = common::create_temp_db_file("savepoint_conflict.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create a stream first so we can cause a conflict
    writer
        .append(AppendCommand::new(
            "cmd-setup",
            "existing-stream",
            StreamRev::NONE,
            vec![EventData::new(b"setup".to_vec())],
        ))
        .await
        .unwrap();

    // Submit a transaction with one conflict in the middle
    let mut tx = writer.begin_transaction();
    tx.append(AppendCommand::new(
        "cmd-1",
        "stream-a",
        StreamRev::NONE,
        vec![EventData::new(b"a".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-conflict",
        "existing-stream",
        StreamRev::NONE, // Wrong - stream exists at rev 1
        vec![EventData::new(b"should-fail".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-2",
        "stream-b",
        StreamRev::NONE,
        vec![EventData::new(b"b".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    // First and third should succeed, second should fail
    assert!(results[0].is_ok(), "First command should succeed");
    assert!(results[1].is_err(), "Second command should fail (conflict)");
    assert!(results[2].is_ok(), "Third command should succeed");

    drop(writer);

    // Verify the successful commands were actually written
    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);

    // Should have: setup + a + b = 3 events
    assert_eq!(events.len(), 3);
}

/// Tests that when the first command fails, the rest still succeed.
#[tokio::test]
async fn savepoint_first_command_fails_rest_succeed() {
    let (_dir, path) = common::create_temp_db_file("savepoint_first.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create a stream to cause conflict
    writer
        .append(AppendCommand::new(
            "cmd-setup",
            "existing",
            StreamRev::NONE,
            vec![EventData::new(b"setup".to_vec())],
        ))
        .await
        .unwrap();

    let mut tx = writer.begin_transaction();
    tx.append(AppendCommand::new(
        "cmd-fail",
        "existing",
        StreamRev::NONE, // Conflict - first command
        vec![EventData::new(b"fail".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-ok-1",
        "stream-1",
        StreamRev::NONE,
        vec![EventData::new(b"ok1".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-ok-2",
        "stream-2",
        StreamRev::NONE,
        vec![EventData::new(b"ok2".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    assert!(results[0].is_err(), "First command should fail");
    assert!(results[1].is_ok(), "Second command should succeed");
    assert!(results[2].is_ok(), "Third command should succeed");

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 3); // setup + ok1 + ok2
}

/// Tests that when the middle command fails, surrounding commands succeed.
#[tokio::test]
async fn savepoint_middle_command_fails_rest_succeed() {
    let (_dir, path) = common::create_temp_db_file("savepoint_middle.db");
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
            "cmd-setup",
            "existing",
            StreamRev::NONE,
            vec![EventData::new(b"setup".to_vec())],
        ))
        .await
        .unwrap();

    let mut tx = writer.begin_transaction();
    tx.append(AppendCommand::new(
        "cmd-ok-1",
        "stream-1",
        StreamRev::NONE,
        vec![EventData::new(b"ok1".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-fail",
        "existing",
        StreamRev::NONE, // Conflict - middle
        vec![EventData::new(b"fail".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-ok-2",
        "stream-2",
        StreamRev::NONE,
        vec![EventData::new(b"ok2".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok());

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 3);
}

/// Tests that when the last command fails, earlier commands succeed.
#[tokio::test]
async fn savepoint_last_command_fails_rest_succeed() {
    let (_dir, path) = common::create_temp_db_file("savepoint_last.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    writer
        .append(AppendCommand::new(
            "cmd-setup",
            "existing",
            StreamRev::NONE,
            vec![EventData::new(b"setup".to_vec())],
        ))
        .await
        .unwrap();

    let mut tx = writer.begin_transaction();
    tx.append(AppendCommand::new(
        "cmd-ok-1",
        "stream-1",
        StreamRev::NONE,
        vec![EventData::new(b"ok1".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-ok-2",
        "stream-2",
        StreamRev::NONE,
        vec![EventData::new(b"ok2".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-fail",
        "existing",
        StreamRev::NONE, // Conflict - last
        vec![EventData::new(b"fail".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    assert!(results[0].is_ok());
    assert!(results[1].is_ok());
    assert!(results[2].is_err());

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 3);
}

/// Tests that multiple failures in a batch are handled correctly.
#[tokio::test]
async fn savepoint_multiple_failures_in_batch() {
    let (_dir, path) = common::create_temp_db_file("savepoint_multi_fail.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create two streams to cause conflicts
    writer
        .append(AppendCommand::new(
            "cmd-setup-1",
            "existing-1",
            StreamRev::NONE,
            vec![EventData::new(b"setup1".to_vec())],
        ))
        .await
        .unwrap();
    writer
        .append(AppendCommand::new(
            "cmd-setup-2",
            "existing-2",
            StreamRev::NONE,
            vec![EventData::new(b"setup2".to_vec())],
        ))
        .await
        .unwrap();

    let mut tx = writer.begin_transaction();
    tx.append(AppendCommand::new(
        "cmd-fail-1",
        "existing-1",
        StreamRev::NONE,
        vec![EventData::new(b"fail1".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-ok",
        "stream-new",
        StreamRev::NONE,
        vec![EventData::new(b"ok".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-fail-2",
        "existing-2",
        StreamRev::NONE,
        vec![EventData::new(b"fail2".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    assert!(results[0].is_err(), "First conflict should fail");
    assert!(results[1].is_ok(), "Middle command should succeed");
    assert!(results[2].is_err(), "Second conflict should fail");

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    // setup1 + setup2 + ok = 3 events
    assert_eq!(events.len(), 3);
}

/// Tests that global positions are not wasted on conflicting commands.
#[tokio::test]
async fn savepoint_global_pos_not_wasted_on_conflict() {
    let (_dir, path) = common::create_temp_db_file("savepoint_pos.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Setup
    writer
        .append(AppendCommand::new(
            "cmd-setup",
            "existing",
            StreamRev::NONE,
            vec![EventData::new(b"setup".to_vec())],
        ))
        .await
        .unwrap();

    // Transaction with conflicts
    let mut tx = writer.begin_transaction();
    tx.append(AppendCommand::new(
        "cmd-ok-1",
        "stream-1",
        StreamRev::NONE,
        vec![EventData::new(b"a".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-fail-1",
        "existing",
        StreamRev::NONE,
        vec![EventData::new(b"fail".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-fail-2",
        "existing",
        StreamRev::NONE,
        vec![EventData::new(b"fail".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-ok-2",
        "stream-2",
        StreamRev::NONE,
        vec![EventData::new(b"b".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_err());
    assert!(results[3].is_ok());

    // Verify positions are contiguous (1, 2, 3 - no gaps for failed commands)
    let pos_1 = results[0].as_ref().unwrap().first_pos.as_raw();
    let pos_2 = results[3].as_ref().unwrap().first_pos.as_raw();

    // Positions should be: setup=1, ok1=2, ok2=3
    assert_eq!(pos_1, 2, "First successful command should be at pos 2");
    assert_eq!(pos_2, 3, "Second successful command should be at pos 3 (no gap)");

    drop(writer);

    // Verify in database
    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);

    assert_eq!(events.len(), 3);
    common::verify_global_pos_gapless(&events);
}

/// Tests that intra-batch conflict detection works via staged state.
#[tokio::test]
async fn savepoint_intra_batch_conflict_detection() {
    let (_dir, path) = common::create_temp_db_file("savepoint_intra.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Transaction where second command conflicts with first (within same batch)
    let mut tx = writer.begin_transaction();
    tx.append(AppendCommand::new(
        "cmd-1",
        "same-stream",
        StreamRev::NONE, // Creates stream at rev 1
        vec![EventData::new(b"first".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-2",
        "same-stream",
        StreamRev::NONE, // Conflict! Should expect rev 1 now
        vec![EventData::new(b"conflict".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-3",
        "same-stream",
        StreamRev::from_raw(1), // Correct - expects rev 1 from first command
        vec![EventData::new(b"third".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    assert!(results[0].is_ok(), "First create should succeed");
    assert!(results[1].is_err(), "Second create should conflict");
    assert!(results[2].is_ok(), "Third with correct expected_rev should succeed");

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 2); // first + third
}

/// Tests that multiple appends to the same stream within a batch work correctly.
#[tokio::test]
async fn savepoint_multiple_appends_same_stream() {
    let (_dir, path) = common::create_temp_db_file("savepoint_same_stream.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Multiple appends to same stream with correct expected_rev chaining
    let mut tx = writer.begin_transaction();
    tx.append(AppendCommand::new(
        "cmd-1",
        "stream-1",
        StreamRev::NONE,
        vec![EventData::new(b"e1".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-2",
        "stream-1",
        StreamRev::from_raw(1),
        vec![EventData::new(b"e2".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-3",
        "stream-1",
        StreamRev::from_raw(2),
        vec![EventData::new(b"e3".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-4",
        "stream-1",
        StreamRev::from_raw(3),
        vec![EventData::new(b"e4".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    for (i, r) in results.iter().enumerate() {
        assert!(r.is_ok(), "Command {} should succeed", i);
    }

    // Verify revisions
    assert_eq!(results[0].as_ref().unwrap().first_rev.as_raw(), 1);
    assert_eq!(results[1].as_ref().unwrap().first_rev.as_raw(), 2);
    assert_eq!(results[2].as_ref().unwrap().first_rev.as_raw(), 3);
    assert_eq!(results[3].as_ref().unwrap().first_rev.as_raw(), 4);

    // Verify positions are contiguous
    assert_eq!(results[0].as_ref().unwrap().first_pos.as_raw(), 1);
    assert_eq!(results[1].as_ref().unwrap().first_pos.as_raw(), 2);
    assert_eq!(results[2].as_ref().unwrap().first_pos.as_raw(), 3);
    assert_eq!(results[3].as_ref().unwrap().first_pos.as_raw(), 4);

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 4);
    common::verify_global_pos_gapless(&events);
}
