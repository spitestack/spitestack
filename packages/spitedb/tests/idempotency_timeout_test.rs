mod common;

use std::time::Duration;

use spitedb::reader;
use spitedb::types::{AppendCommand, EventData, GlobalPos, StreamId, StreamRev};
use spitedb::{spawn_batch_writer, Database};

#[tokio::test]
async fn retry_after_client_timeout_returns_original_result() {
    let (_dir, path) = common::create_temp_db_file("timeout.db");
    let cryptor = common::test_cryptor();

    // Make batching slow enough that a client-side timeout can happen, but still
    // below the writer handle's default 100ms deadline.
    let config = common::writer_config_with_batch_timeout(Duration::from_millis(40));

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(writer_db.into_connection(), cryptor.clone_with_same_key(), config).unwrap();

    let stream = StreamId::new("stream-timeout");
    let cmd = AppendCommand::new(
        "cmd-timeout",
        stream.clone(),
        StreamRev::NONE,
        vec![EventData::new(b"payload".to_vec())],
    );

    // Simulate "crash after COMMIT before reply" from the client's perspective:
    // the writer commits, but the client gives up waiting.
    let timed_out = tokio::time::timeout(Duration::from_millis(5), writer.append(cmd.clone())).await;
    assert!(timed_out.is_err(), "expected client-side timeout");

    // Prove the command actually committed (otherwise this wouldn't test the crash window).
    let read_conn = common::open_read_only(&path);
    common::eventually(Duration::from_millis(250), Duration::from_millis(5), || {
        let events = reader::read_stream(&read_conn, &stream, StreamRev::FIRST, 10, &cryptor).ok()?;
        (events.len() == 1).then_some(())
    })
    .await;

    // Drop the writer and "restart" the writer actor to ensure durable idempotency.
    drop(writer);
    let writer_db2 = Database::open(&path).unwrap();
    let writer2 = spawn_batch_writer(
        writer_db2.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Retry must return the original result and must not duplicate events.
    let result_retry = writer2.append(cmd).await.unwrap();
    assert_eq!(result_retry.first_pos.as_raw(), 1);
    assert_eq!(result_retry.last_pos.as_raw(), 1);

    drop(writer2);

    let global = reader::read_global(&read_conn, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert_eq!(global.len(), 1);
    assert_eq!(global[0].stream_id.as_str(), stream.as_str());
    assert_eq!(global[0].data, b"payload");
}

