mod common;

use spitedb::reader;
use spitedb::types::{AppendCommand, EventData, GlobalPos, StreamId, StreamRev};
use spitedb::{spawn_batch_writer, Database, Error};

#[tokio::test]
async fn corrupted_event_index_bounds_return_schema_error() {
    let (_dir, path) = common::create_temp_db_file("corrupt_index.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let stream = StreamId::new("stream-corrupt");
    writer
        .append(AppendCommand::new(
            "cmd-1",
            stream.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"hello".to_vec())],
        ))
        .await
        .unwrap();

    drop(writer);

    // Corrupt the index: make byte_len absurdly large.
    let rw = common::open_read_write(&path);
    rw.execute("UPDATE event_index SET byte_len = 999999 WHERE global_pos = 1", [])
        .unwrap();
    drop(rw);

    let read_conn = common::open_read_only(&path);
    let err = reader::read_global(&read_conn, GlobalPos::FIRST, 10, &cryptor).unwrap_err();
    assert!(
        matches!(err, Error::Schema(ref msg) if msg.contains("corrupted event index")),
        "expected schema corruption error, got: {err:?}"
    );
}

#[tokio::test]
async fn tampered_ciphertext_returns_encryption_error() {
    let (_dir, path) = common::create_temp_db_file("tamper_blob.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let stream = StreamId::new("stream-tamper");
    writer
        .append(AppendCommand::new(
            "cmd-1",
            stream,
            StreamRev::NONE,
            vec![EventData::new(b"secret".to_vec())],
        ))
        .await
        .unwrap();
    drop(writer);

    let rw = common::open_read_write(&path);
    let (batch_id, mut data): (i64, Vec<u8>) = rw
        .query_row("SELECT batch_id, data FROM batches ORDER BY batch_id LIMIT 1", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap();
    assert!(!data.is_empty());
    let idx = data.len() / 2;
    data[idx] ^= 0xff;
    rw.execute("UPDATE batches SET data = ? WHERE batch_id = ?", rusqlite::params![data, batch_id])
        .unwrap();
    drop(rw);

    let read_conn = common::open_read_only(&path);
    let err = reader::read_global(&read_conn, GlobalPos::FIRST, 10, &cryptor).unwrap_err();
    assert!(
        matches!(err, Error::Encryption(_)),
        "expected encryption error, got: {err:?}"
    );
}
