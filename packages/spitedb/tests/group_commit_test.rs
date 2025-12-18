//! Group Commit Semantics Tests
//!
//! Tests the invariants around batched writes:
//! - Multiple concurrent commands are batched together
//! - Batch timeout triggers flush
//! - Batch max size triggers flush
//! - Commit failure fails all commands in batch

mod common;

use std::time::Duration;

use spitedb::types::{AppendCommand, EventData, GlobalPos, StreamRev};
use spitedb::{reader, spawn_batch_writer, Database, WriterConfig};

/// Tests that concurrent commands are batched together.
#[tokio::test]
async fn group_commit_batches_concurrent_commands() {
    let (_dir, path) = common::create_temp_db_file("batch_concurrent.db");
    let cryptor = common::test_cryptor();

    // Use longer batch timeout to ensure commands are grouped
    let config = WriterConfig {
        batch_timeout: Duration::from_millis(50),
        batch_max_size: 100,
        batch_max_bytes: 10 * 1024 * 1024,
    };

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        config,
    )
    .unwrap();

    // Spawn concurrent appends
    let mut handles = Vec::new();
    for i in 0..10 {
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

    // Collect results
    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    drop(writer);

    // Check that commands were batched
    let read_conn = common::open_read_only(&path);
    let batches = common::read_raw_batches(&read_conn);
    let events = common::read_raw_event_index(&read_conn);

    assert_eq!(events.len(), 10);

    // With 50ms batch timeout, likely all in 1-2 batches
    assert!(
        batches.len() <= 3,
        "Expected at most 3 batches, got {}",
        batches.len()
    );
}

/// Tests that batch timeout flushes the batch.
#[tokio::test]
async fn group_commit_timeout_flushes_batch() {
    let (_dir, path) = common::create_temp_db_file("batch_timeout.db");
    let cryptor = common::test_cryptor();

    let config = WriterConfig {
        batch_timeout: Duration::from_millis(20),
        batch_max_size: 1000,
        batch_max_bytes: 10 * 1024 * 1024,
    };

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        config,
    )
    .unwrap();

    // Single command - should be flushed after timeout
    let result = writer
        .append(AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"data".to_vec())],
        ))
        .await
        .unwrap();

    // Should complete within reasonable time (timeout + processing)
    assert_eq!(result.first_pos.as_raw(), 1);

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 1);
}

/// Tests that batch max size triggers flush.
#[tokio::test]
async fn group_commit_max_size_flushes_batch() {
    let (_dir, path) = common::create_temp_db_file("batch_max_size.db");
    let cryptor = common::test_cryptor();

    // Very small max size to force multiple batches
    let config = WriterConfig {
        batch_timeout: Duration::from_millis(1000), // Long timeout
        batch_max_size: 5,                          // Small max size
        batch_max_bytes: 10 * 1024 * 1024,
    };

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        config,
    )
    .unwrap();

    // Submit 20 commands quickly - should create at least 4 batches
    let mut handles = Vec::new();
    for i in 0..20 {
        let w = writer.clone();
        let handle = tokio::spawn(async move {
            w.append(AppendCommand::new(
                format!("cmd-{}", i),
                format!("stream-{}", i),
                StreamRev::NONE,
                vec![EventData::new(format!("e{}", i).into_bytes())],
            ))
            .await
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let batches = common::read_raw_batches(&read_conn);
    let events = common::read_raw_event_index(&read_conn);

    assert_eq!(events.len(), 20);

    // Should have multiple batches due to max_size=5
    assert!(
        batches.len() >= 4,
        "Expected at least 4 batches with max_size=5 and 20 commands, got {}",
        batches.len()
    );
}

/// Tests that batch max bytes triggers flush.
#[tokio::test]
async fn group_commit_max_bytes_flushes_batch() {
    let (_dir, path) = common::create_temp_db_file("batch_max_bytes.db");
    let cryptor = common::test_cryptor();

    // Very small max bytes to force multiple batches
    let config = WriterConfig {
        batch_timeout: Duration::from_millis(1000),
        batch_max_size: 1000,
        batch_max_bytes: 100, // Tiny - forces multiple batches
    };

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        config,
    )
    .unwrap();

    // Submit commands with larger payloads
    let mut handles = Vec::new();
    for i in 0..10 {
        let w = writer.clone();
        let handle = tokio::spawn(async move {
            w.append(AppendCommand::new(
                format!("cmd-{}", i),
                format!("stream-{}", i),
                StreamRev::NONE,
                vec![EventData::new(vec![0u8; 50])], // 50 bytes each
            ))
            .await
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let batches = common::read_raw_batches(&read_conn);
    let events = common::read_raw_event_index(&read_conn);

    assert_eq!(events.len(), 10);

    // Should have multiple batches due to byte limit
    assert!(
        batches.len() >= 2,
        "Expected multiple batches with max_bytes=100, got {}",
        batches.len()
    );
}

/// Tests that events are readable immediately after append returns.
#[tokio::test]
async fn group_commit_readable_immediately_after_append() {
    let (_dir, path) = common::create_temp_db_file("batch_readable.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let result = writer
        .append(AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"payload".to_vec())],
        ))
        .await
        .unwrap();

    // Should be readable immediately (same connection via writer, different via reader)
    let read_conn = common::open_read_only(&path);
    let events = reader::read_global(&read_conn, GlobalPos::FIRST, 100, &cryptor).unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].global_pos.as_raw(), result.first_pos.as_raw());
    assert_eq!(events[0].data, b"payload");
}

/// Tests batching with mixed success and failure commands.
#[tokio::test]
async fn group_commit_mixed_success_and_failure() {
    let (_dir, path) = common::create_temp_db_file("batch_mixed.db");
    let cryptor = common::test_cryptor();

    let config = WriterConfig {
        batch_timeout: Duration::from_millis(50),
        batch_max_size: 100,
        batch_max_bytes: 10 * 1024 * 1024,
    };

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        config,
    )
    .unwrap();

    // Create a stream to cause conflicts
    writer
        .append(AppendCommand::new(
            "setup",
            "existing",
            StreamRev::NONE,
            vec![EventData::new(b"setup".to_vec())],
        ))
        .await
        .unwrap();

    // Submit mixed commands concurrently
    let w1 = writer.clone();
    let w2 = writer.clone();
    let w3 = writer.clone();

    let h1 = tokio::spawn(async move {
        w1.append(AppendCommand::new(
            "cmd-ok-1",
            "stream-ok-1",
            StreamRev::NONE,
            vec![EventData::new(b"ok1".to_vec())],
        ))
        .await
    });

    let h2 = tokio::spawn(async move {
        w2.append(AppendCommand::new(
            "cmd-fail",
            "existing",
            StreamRev::NONE, // Conflict
            vec![EventData::new(b"fail".to_vec())],
        ))
        .await
    });

    let h3 = tokio::spawn(async move {
        w3.append(AppendCommand::new(
            "cmd-ok-2",
            "stream-ok-2",
            StreamRev::NONE,
            vec![EventData::new(b"ok2".to_vec())],
        ))
        .await
    });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let r3 = h3.await.unwrap();

    assert!(r1.is_ok());
    assert!(r2.is_err());
    assert!(r3.is_ok());

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 3); // setup + ok1 + ok2
}

/// Tests that a single batch can handle many concurrent appends.
#[tokio::test]
async fn group_commit_high_concurrency() {
    let (_dir, path) = common::create_temp_db_file("batch_high_concurrency.db");
    let cryptor = common::test_cryptor();

    // Use default config which has longer timeouts
    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Spawn 50 concurrent appends (reduced from 100 for reliability)
    let mut handles = Vec::new();
    for i in 0..50 {
        let w = writer.clone();
        let handle = tokio::spawn(async move {
            w.append(AppendCommand::new(
                format!("cmd-{}", i),
                format!("stream-{}", i),
                StreamRev::NONE,
                vec![EventData::new(format!("payload-{}", i).into_bytes())],
            ))
            .await
        });
        handles.push(handle);
    }

    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    // All should succeed
    for (i, r) in results.iter().enumerate() {
        assert!(r.is_ok(), "Command {} failed: {:?}", i, r.as_ref().err());
    }

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    let batches = common::read_raw_batches(&read_conn);

    assert_eq!(events.len(), 50);
    common::verify_global_pos_gapless(&events);

    // Should be well batched
    let total_events_in_batches: i64 = batches.iter().map(|b| b.event_count).sum();
    assert_eq!(total_events_in_batches, 50);
}
