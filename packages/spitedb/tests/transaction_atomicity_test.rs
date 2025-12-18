//! Transaction Atomicity Tests
//!
//! Tests the invariants around transactions:
//! - Transaction commands are guaranteed to be in the same batch
//! - Positions across transaction commands are contiguous

mod common;

use spitedb::types::{AppendCommand, EventData, StreamRev};
use spitedb::{spawn_batch_writer, Database};

/// Tests that all commands in a transaction are processed in the same batch.
#[tokio::test]
async fn transaction_all_commands_same_batch() {
    let (_dir, path) = common::create_temp_db_file("tx_same_batch.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create a transaction with multiple commands
    let mut tx = writer.begin_transaction();
    tx.append(AppendCommand::new(
        "cmd-1",
        "stream-1",
        StreamRev::NONE,
        vec![EventData::new(b"e1".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-2",
        "stream-2",
        StreamRev::NONE,
        vec![EventData::new(b"e2".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-3",
        "stream-3",
        StreamRev::NONE,
        vec![EventData::new(b"e3".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    for r in &results {
        assert!(r.is_ok());
    }

    drop(writer);

    // All events should be in the same batch
    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);

    assert_eq!(events.len(), 3);

    let batch_ids: Vec<i64> = events.iter().map(|e| e.batch_id).collect();
    assert!(
        batch_ids.iter().all(|&id| id == batch_ids[0]),
        "All events should be in the same batch: {:?}",
        batch_ids
    );
}

/// Tests that positions are contiguous across transaction commands.
#[tokio::test]
async fn transaction_positions_contiguous() {
    let (_dir, path) = common::create_temp_db_file("tx_contiguous.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let mut tx = writer.begin_transaction();
    tx.append(AppendCommand::new(
        "cmd-1",
        "stream-1",
        StreamRev::NONE,
        vec![
            EventData::new(b"a1".to_vec()),
            EventData::new(b"a2".to_vec()),
        ],
    ));
    tx.append(AppendCommand::new(
        "cmd-2",
        "stream-2",
        StreamRev::NONE,
        vec![
            EventData::new(b"b1".to_vec()),
            EventData::new(b"b2".to_vec()),
            EventData::new(b"b3".to_vec()),
        ],
    ));
    tx.append(AppendCommand::new(
        "cmd-3",
        "stream-3",
        StreamRev::NONE,
        vec![EventData::new(b"c1".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    // Verify positions are contiguous
    // cmd-1: 2 events at pos 1-2
    // cmd-2: 3 events at pos 3-5
    // cmd-3: 1 event at pos 6
    let r1 = results[0].as_ref().unwrap();
    let r2 = results[1].as_ref().unwrap();
    let r3 = results[2].as_ref().unwrap();

    assert_eq!(r1.first_pos.as_raw(), 1);
    assert_eq!(r1.last_pos.as_raw(), 2);
    assert_eq!(r2.first_pos.as_raw(), 3);
    assert_eq!(r2.last_pos.as_raw(), 5);
    assert_eq!(r3.first_pos.as_raw(), 6);
    assert_eq!(r3.last_pos.as_raw(), 6);

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 6);
    common::verify_global_pos_gapless(&events);
}

/// Tests that conflicts in a transaction are isolated - they don't abort other commands.
#[tokio::test]
async fn transaction_partial_failures_isolated() {
    let (_dir, path) = common::create_temp_db_file("tx_partial.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Setup a stream to cause conflict
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
        "cmd-1",
        "stream-1",
        StreamRev::NONE,
        vec![EventData::new(b"a".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-conflict",
        "existing",
        StreamRev::NONE, // Conflict!
        vec![EventData::new(b"fail".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-2",
        "stream-2",
        StreamRev::NONE,
        vec![EventData::new(b"b".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    // First and third succeed, second fails
    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok());

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 3); // setup + a + b
}

/// Tests that a conflict in one command doesn't abort other commands.
#[tokio::test]
async fn transaction_conflict_in_one_does_not_abort_others() {
    let (_dir, path) = common::create_temp_db_file("tx_no_abort.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create multiple streams
    for i in 0..3 {
        writer
            .append(AppendCommand::new(
                format!("setup-{}", i),
                format!("stream-{}", i),
                StreamRev::NONE,
                vec![EventData::new(format!("setup-{}", i).into_bytes())],
            ))
            .await
            .unwrap();
    }

    // Transaction with multiple conflicts
    let mut tx = writer.begin_transaction();
    tx.append(AppendCommand::new(
        "cmd-ok-1",
        "new-stream-1",
        StreamRev::NONE,
        vec![EventData::new(b"ok1".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-conflict-1",
        "stream-0",
        StreamRev::NONE,
        vec![EventData::new(b"fail".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-ok-2",
        "new-stream-2",
        StreamRev::NONE,
        vec![EventData::new(b"ok2".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-conflict-2",
        "stream-1",
        StreamRev::NONE,
        vec![EventData::new(b"fail".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-ok-3",
        "new-stream-3",
        StreamRev::NONE,
        vec![EventData::new(b"ok3".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok());
    assert!(results[3].is_err());
    assert!(results[4].is_ok());

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    // 3 setup + 3 ok = 6
    assert_eq!(events.len(), 6);
}

/// Tests that an empty transaction is a no-op.
#[tokio::test]
async fn transaction_empty_is_noop() {
    let (_dir, path) = common::create_temp_db_file("tx_empty.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Empty transaction
    let tx = writer.begin_transaction();
    let results = tx.submit().await.unwrap();

    assert!(results.is_empty());

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 0);
}

/// Tests large transactions with many commands.
#[tokio::test]
async fn transaction_large_batch() {
    let (_dir, path) = common::create_temp_db_file("tx_large.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let mut tx = writer.begin_transaction();
    for i in 0..50 {
        tx.append(AppendCommand::new(
            format!("cmd-{}", i),
            format!("stream-{}", i),
            StreamRev::NONE,
            vec![EventData::new(format!("event-{}", i).into_bytes())],
        ));
    }

    let results = tx.submit().await.unwrap();

    assert_eq!(results.len(), 50);
    for r in &results {
        assert!(r.is_ok());
    }

    drop(writer);

    // All should be in the same batch
    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    let batches = common::read_raw_batches(&read_conn);

    assert_eq!(events.len(), 50);
    assert_eq!(batches.len(), 1, "All events should be in one batch");
    assert_eq!(batches[0].event_count, 50);
}

/// Tests that transaction positions continue from previous writes.
#[tokio::test]
async fn transaction_continues_from_previous_position() {
    let (_dir, path) = common::create_temp_db_file("tx_continue.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Write some events first
    let setup_result = writer
        .append(AppendCommand::new(
            "cmd-setup",
            "stream-setup",
            StreamRev::NONE,
            vec![
                EventData::new(b"s1".to_vec()),
                EventData::new(b"s2".to_vec()),
                EventData::new(b"s3".to_vec()),
            ],
        ))
        .await
        .unwrap();

    assert_eq!(setup_result.last_pos.as_raw(), 3);

    // Now transaction should continue from position 4
    let mut tx = writer.begin_transaction();
    tx.append(AppendCommand::new(
        "cmd-1",
        "stream-1",
        StreamRev::NONE,
        vec![EventData::new(b"a".to_vec())],
    ));
    tx.append(AppendCommand::new(
        "cmd-2",
        "stream-2",
        StreamRev::NONE,
        vec![EventData::new(b"b".to_vec())],
    ));

    let results = tx.submit().await.unwrap();

    assert_eq!(results[0].as_ref().unwrap().first_pos.as_raw(), 4);
    assert_eq!(results[1].as_ref().unwrap().first_pos.as_raw(), 5);

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 5);
    common::verify_global_pos_gapless(&events);
}

/// Tests multiple sequential transactions.
#[tokio::test]
async fn transaction_multiple_sequential() {
    let (_dir, path) = common::create_temp_db_file("tx_sequential.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // First transaction
    let mut tx1 = writer.begin_transaction();
    tx1.append(AppendCommand::new(
        "cmd-1",
        "stream-1",
        StreamRev::NONE,
        vec![EventData::new(b"t1-a".to_vec())],
    ));
    tx1.append(AppendCommand::new(
        "cmd-2",
        "stream-2",
        StreamRev::NONE,
        vec![EventData::new(b"t1-b".to_vec())],
    ));
    let results1 = tx1.submit().await.unwrap();

    assert!(results1[0].is_ok());
    assert!(results1[1].is_ok());
    let last_pos_tx1 = results1[1].as_ref().unwrap().last_pos.as_raw();

    // Second transaction
    let mut tx2 = writer.begin_transaction();
    tx2.append(AppendCommand::new(
        "cmd-3",
        "stream-3",
        StreamRev::NONE,
        vec![EventData::new(b"t2-a".to_vec())],
    ));
    tx2.append(AppendCommand::new(
        "cmd-4",
        "stream-4",
        StreamRev::NONE,
        vec![EventData::new(b"t2-b".to_vec())],
    ));
    let results2 = tx2.submit().await.unwrap();

    assert!(results2[0].is_ok());
    assert!(results2[1].is_ok());

    // Positions should continue
    assert_eq!(
        results2[0].as_ref().unwrap().first_pos.as_raw(),
        last_pos_tx1 + 1
    );

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 4);
    common::verify_global_pos_gapless(&events);
}
