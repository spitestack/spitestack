mod common;

use spitedb::reader;
use spitedb::types::{AppendCommand, EventData, GlobalPos, StreamId, StreamRev};
use spitedb::{spawn_batch_writer, Database};

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

