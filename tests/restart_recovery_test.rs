mod common;

use spitedb::reader;
use spitedb::types::{AppendCommand, EventData, GlobalPos, StreamId, StreamRev};
use spitedb::{spawn_batch_writer, Database};

#[tokio::test]
async fn restart_recovers_positions_and_revisions() {
    let (_dir, path) = common::create_temp_db_file("restart.db");
    let cryptor = common::test_cryptor();

    let stream = StreamId::new("stream-restart");

    // First writer instance.
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
            stream.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"a".to_vec()), EventData::new(b"b".to_vec())],
        ))
        .await
        .unwrap();
    assert_eq!(r1.first_pos.as_raw(), 1);
    assert_eq!(r1.last_pos.as_raw(), 2);
    assert_eq!(r1.last_rev.as_raw(), 2);

    drop(writer1);

    // Second writer instance (simulates process restart).
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
            stream.clone(),
            StreamRev::from_raw(2),
            vec![EventData::new(b"c".to_vec())],
        ))
        .await
        .unwrap();
    assert_eq!(r2.first_pos.as_raw(), 3);
    assert_eq!(r2.last_pos.as_raw(), 3);
    assert_eq!(r2.first_rev.as_raw(), 3);
    assert_eq!(r2.last_rev.as_raw(), 3);

    drop(writer2);

    let read_conn = common::open_read_only(&path);

    let global = reader::read_global(&read_conn, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert_eq!(global.len(), 3);
    for (i, e) in global.iter().enumerate() {
        assert_eq!(e.global_pos.as_raw(), (i + 1) as u64);
    }

    let stream_events = reader::read_stream(&read_conn, &stream, StreamRev::FIRST, 10, &cryptor).unwrap();
    assert_eq!(stream_events.len(), 3);
    assert_eq!(stream_events[0].data, b"a");
    assert_eq!(stream_events[1].data, b"b");
    assert_eq!(stream_events[2].data, b"c");
    assert_eq!(stream_events[2].stream_rev.as_raw(), 3);
}

