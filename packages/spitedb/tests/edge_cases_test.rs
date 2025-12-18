//! Edge Cases and Boundary Condition Tests
//!
//! Tests for:
//! - Large event payloads
//! - Many events in single append
//! - Many streams
//! - Special characters in identifiers
//! - Error semantics

mod common;

use spitedb::reader;
use spitedb::types::{AppendCommand, EventData, GlobalPos, StreamId, StreamRev};
use spitedb::{spawn_batch_writer, Database, Error};

/// Tests handling of large event payloads (1MB).
#[tokio::test]
async fn large_event_payload_1mb() {
    let (_dir, path) = common::create_temp_db_file("large_payload.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create a 1MB payload
    let large_payload = vec![0xAB; 1024 * 1024];

    let result = writer
        .append(AppendCommand::new(
            "cmd-large",
            "stream-large",
            StreamRev::NONE,
            vec![EventData::new(large_payload.clone())],
        ))
        .await
        .unwrap();

    assert_eq!(result.first_pos.as_raw(), 1);

    drop(writer);

    // Verify readback
    let read_conn = common::open_read_only(&path);
    let events =
        reader::read_stream(&read_conn, &StreamId::new("stream-large"), StreamRev::FIRST, 10, &cryptor)
            .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].data.len(), 1024 * 1024);
    assert_eq!(events[0].data, large_payload);
}

/// Tests many events in a single append.
#[tokio::test]
async fn many_events_single_append_1000() {
    let (_dir, path) = common::create_temp_db_file("many_events.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let events: Vec<EventData> = (0..1000)
        .map(|i| EventData::new(format!("event-{:04}", i).into_bytes()))
        .collect();

    let result = writer
        .append(AppendCommand::new(
            "cmd-many",
            "stream-many",
            StreamRev::NONE,
            events,
        ))
        .await
        .unwrap();

    assert_eq!(result.first_pos.as_raw(), 1);
    assert_eq!(result.last_pos.as_raw(), 1000);
    assert_eq!(result.first_rev.as_raw(), 1);
    assert_eq!(result.last_rev.as_raw(), 1000);

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 1000);
    common::verify_global_pos_gapless(&events);
}

/// Tests creating many streams.
#[tokio::test]
async fn many_streams_1000() {
    let (_dir, path) = common::create_temp_db_file("many_streams.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    for i in 0..1000 {
        writer
            .append(AppendCommand::new(
                format!("cmd-{}", i),
                format!("stream-{:04}", i),
                StreamRev::NONE,
                vec![EventData::new(format!("e-{}", i).into_bytes())],
            ))
            .await
            .unwrap();
    }

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    let heads = common::read_raw_stream_heads(&read_conn);

    assert_eq!(events.len(), 1000);
    assert_eq!(heads.len(), 1000);
    common::verify_global_pos_gapless(&events);
}

/// Tests long stream IDs.
#[tokio::test]
async fn long_stream_id() {
    let (_dir, path) = common::create_temp_db_file("long_id.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // 1000 character stream ID
    let long_id: String = (0..1000).map(|i| ((i % 26) as u8 + b'a') as char).collect();

    let result = writer
        .append(AppendCommand::new(
            "cmd-long",
            long_id.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"data".to_vec())],
        ))
        .await
        .unwrap();

    assert_eq!(result.first_pos.as_raw(), 1);

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = reader::read_stream(&read_conn, &StreamId::new(&long_id), StreamRev::FIRST, 10, &cryptor)
        .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].stream_id.as_str(), long_id);
}

/// Tests special characters in stream IDs.
#[tokio::test]
async fn special_characters_in_stream_id() {
    let (_dir, path) = common::create_temp_db_file("special_chars.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let special_ids = vec![
        "stream with spaces",
        "stream-with-dashes",
        "stream_with_underscores",
        "stream.with.dots",
        "stream/with/slashes",
        "stream:with:colons",
        "stream@with@at",
        "stream#with#hash",
        "日本語ストリーム",       // Japanese
        "поток",                // Russian
        "🚀emoji🎉stream",
        "",                     // Empty string
        "stream\nwith\nnewlines",
        "stream\twith\ttabs",
        "stream\"with\"quotes",
        "stream'with'quotes",
    ];

    for (i, id) in special_ids.iter().enumerate() {
        let result = writer
            .append(AppendCommand::new(
                format!("cmd-{}", i),
                *id,
                StreamRev::NONE,
                vec![EventData::new(format!("data-{}", i).into_bytes())],
            ))
            .await
            .unwrap();

        assert_eq!(result.first_rev.as_raw(), 1);
    }

    drop(writer);

    // Verify all can be read back
    let read_conn = common::open_read_only(&path);
    for id in &special_ids {
        let events = reader::read_stream(&read_conn, &StreamId::new(*id), StreamRev::FIRST, 10, &cryptor)
            .unwrap();
        assert_eq!(events.len(), 1, "Failed for stream_id: {:?}", id);
        assert_eq!(events[0].stream_id.as_str(), *id);
    }
}

/// Tests binary payload with all byte values.
#[tokio::test]
async fn binary_payload_all_bytes() {
    let (_dir, path) = common::create_temp_db_file("binary_payload.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Payload containing all 256 byte values
    let all_bytes: Vec<u8> = (0..=255u8).collect();

    let result = writer
        .append(AppendCommand::new(
            "cmd-binary",
            "stream-binary",
            StreamRev::NONE,
            vec![EventData::new(all_bytes.clone())],
        ))
        .await
        .unwrap();

    assert_eq!(result.first_pos.as_raw(), 1);

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = reader::read_stream(
        &read_conn,
        &StreamId::new("stream-binary"),
        StreamRev::FIRST,
        10,
        &cryptor,
    )
    .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].data, all_bytes);
}

// =============================================================================
// Error Semantics Tests
// =============================================================================

/// Tests that conflict errors contain expected and actual revision.
#[tokio::test]
async fn conflict_error_contains_expected_and_actual() {
    let (_dir, path) = common::create_temp_db_file("error_conflict.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Create stream with 5 events
    writer
        .append(AppendCommand::new(
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![
                EventData::new(b"e1".to_vec()),
                EventData::new(b"e2".to_vec()),
                EventData::new(b"e3".to_vec()),
                EventData::new(b"e4".to_vec()),
                EventData::new(b"e5".to_vec()),
            ],
        ))
        .await
        .unwrap();

    // Try with wrong expected_rev
    let err = writer
        .append(AppendCommand::new(
            "cmd-2",
            "stream-1",
            StreamRev::from_raw(3), // Wrong - actual is 5
            vec![EventData::new(b"fail".to_vec())],
        ))
        .await
        .unwrap_err();

    match err {
        Error::Conflict {
            stream_id,
            expected,
            actual,
        } => {
            assert_eq!(stream_id, "stream-1");
            assert_eq!(expected, 3);
            assert_eq!(actual, 5);
        }
        other => panic!("Expected Conflict error, got: {:?}", other),
    }
}

/// Tests that duplicate command returns success (not error) with same result.
#[tokio::test]
async fn duplicate_returns_success_not_error() {
    let (_dir, path) = common::create_temp_db_file("duplicate_success.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let cmd = AppendCommand::new(
        "cmd-duplicate",
        "stream-1",
        StreamRev::NONE,
        vec![EventData::new(b"data".to_vec())],
    );

    let result1 = writer.append(cmd.clone()).await.unwrap();
    let result2 = writer.append(cmd.clone()).await.unwrap();

    // Both should succeed with same positions
    assert_eq!(result1.first_pos.as_raw(), result2.first_pos.as_raw());
    assert_eq!(result1.last_pos.as_raw(), result2.last_pos.as_raw());
    assert_eq!(result1.first_rev.as_raw(), result2.first_rev.as_raw());
    assert_eq!(result1.last_rev.as_raw(), result2.last_rev.as_raw());

    drop(writer);

    // Only one event should exist
    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);
    assert_eq!(events.len(), 1);
}

/// Tests that zero-length payloads are handled.
#[tokio::test]
async fn zero_length_payload() {
    let (_dir, path) = common::create_temp_db_file("zero_payload.db");
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
            "cmd-zero",
            "stream-zero",
            StreamRev::NONE,
            vec![EventData::new(vec![])], // Empty payload
        ))
        .await
        .unwrap();

    assert_eq!(result.first_pos.as_raw(), 1);

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = reader::read_stream(
        &read_conn,
        &StreamId::new("stream-zero"),
        StreamRev::FIRST,
        10,
        &cryptor,
    )
    .unwrap();

    assert_eq!(events.len(), 1);
    assert!(events[0].data.is_empty());
}

/// Tests rapid sequential appends to same stream.
#[tokio::test]
async fn rapid_sequential_appends_same_stream() {
    let (_dir, path) = common::create_temp_db_file("rapid_sequential.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        common::fast_writer_config(),
    )
    .unwrap();

    let mut expected_rev = 0u64;
    for i in 0..100 {
        let result = writer
            .append(AppendCommand::new(
                format!("cmd-{}", i),
                "same-stream",
                if expected_rev == 0 {
                    StreamRev::NONE
                } else {
                    StreamRev::from_raw(expected_rev)
                },
                vec![EventData::new(format!("e{}", i).into_bytes())],
            ))
            .await
            .unwrap();

        expected_rev = result.last_rev.as_raw();
    }

    drop(writer);

    let read_conn = common::open_read_only(&path);
    let events = common::read_raw_event_index(&read_conn);

    assert_eq!(events.len(), 100);
    common::verify_global_pos_gapless(&events);
}

/// Tests reading from non-existent positions returns empty.
#[tokio::test]
async fn reading_nonexistent_positions() {
    let (_dir, path) = common::create_temp_db_file("nonexistent.db");
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
            "cmd-1",
            "stream-1",
            StreamRev::NONE,
            vec![EventData::new(b"data".to_vec())],
        ))
        .await
        .unwrap();

    drop(writer);

    let read_conn = common::open_read_only(&path);

    // Read from position 100 (doesn't exist)
    let events = reader::read_global(&read_conn, GlobalPos::from_raw(100), 10, &cryptor).unwrap();
    assert!(events.is_empty());

    // Read from non-existent stream
    let events = reader::read_stream(
        &read_conn,
        &StreamId::new("nonexistent"),
        StreamRev::FIRST,
        10,
        &cryptor,
    )
    .unwrap();
    assert!(events.is_empty());

    // Read from high revision
    let events = reader::read_stream(
        &read_conn,
        &StreamId::new("stream-1"),
        StreamRev::from_raw(100),
        10,
        &cryptor,
    )
    .unwrap();
    assert!(events.is_empty());
}
