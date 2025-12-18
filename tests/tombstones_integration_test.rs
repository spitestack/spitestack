mod common;

use spitedb::reader;
use spitedb::types::{
    AppendCommand, DeleteStreamCommand, DeleteTenantCommand, EventData, GlobalPos, StreamId, StreamRev, Tenant,
};
use spitedb::{spawn_batch_writer, Database};

#[tokio::test]
async fn stream_tombstones_filter_reads_without_rewriting_history() {
    let (_dir, path) = common::create_temp_db_file("tombstones.db");
    let cryptor = common::test_cryptor();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    let stream = StreamId::new("stream-gdpr");
    writer
        .append(AppendCommand::new(
            "cmd-append",
            stream.clone(),
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

    let read_conn = common::open_read_only(&path);
    common::eventually(std::time::Duration::from_millis(200), std::time::Duration::from_millis(5), || {
        let all = reader::read_stream(&read_conn, &stream, StreamRev::FIRST, 100, &cryptor).ok()?;
        (all.len() == 5).then_some(())
    }).await;

    // Delete revisions 2..=4.
    let delete_cmd = DeleteStreamCommand::delete_range(
        "cmd-delete-range",
        stream.clone(),
        Tenant::default_tenant(),
        StreamRev::from_raw(2),
        StreamRev::from_raw(4),
    );
    let delete_result_1 = writer.delete_stream(delete_cmd.clone()).await.unwrap();
    assert_eq!(delete_result_1.from_rev.as_raw(), 2);
    assert_eq!(delete_result_1.to_rev.as_raw(), 4);

    // Idempotent replay of the delete command returns the original result.
    let delete_result_2 = writer.delete_stream(delete_cmd).await.unwrap();
    assert_eq!(delete_result_2.from_rev.as_raw(), 2);
    assert_eq!(delete_result_2.to_rev.as_raw(), 4);
    assert_eq!(delete_result_2.deleted_ms, delete_result_1.deleted_ms);

    let after = reader::read_stream(&read_conn, &stream, StreamRev::FIRST, 100, &cryptor).unwrap();
    assert_eq!(after.len(), 2);
    assert_eq!(after[0].stream_rev.as_raw(), 1);
    assert_eq!(after[0].data, b"e1");
    assert_eq!(after[1].stream_rev.as_raw(), 5);
    assert_eq!(after[1].data, b"e5");

    // Appending new events after deletion should remain readable (tombstones are ranges).
    let r = writer
        .append(AppendCommand::new(
            "cmd-append-more",
            stream.clone(),
            StreamRev::from_raw(5),
            vec![EventData::new(b"e6".to_vec())],
        ))
        .await
        .unwrap();
    assert_eq!(r.first_rev.as_raw(), 6);

    let after2 = reader::read_stream(&read_conn, &stream, StreamRev::FIRST, 100, &cryptor).unwrap();
    assert_eq!(after2.len(), 3);
    assert_eq!(after2[2].stream_rev.as_raw(), 6);
    assert_eq!(after2[2].data, b"e6");
}

#[tokio::test]
async fn tenant_tombstones_filter_only_that_tenant() {
    let (_dir, path) = common::create_temp_db_file("tenant_tombstones.db");
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

    writer
        .append(AppendCommand::new_with_tenant(
            "cmd-a-1",
            "s-a",
            tenant_a.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"a1".to_vec())],
        ))
        .await
        .unwrap();
    writer
        .append(AppendCommand::new_with_tenant(
            "cmd-b-1",
            "s-b",
            tenant_b.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"b1".to_vec())],
        ))
        .await
        .unwrap();

    let read_conn = common::open_read_only(&path);
    let before = reader::read_global(&read_conn, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert_eq!(before.len(), 2);

    writer
        .delete_tenant(DeleteTenantCommand::new("cmd-delete-tenant-a", tenant_a.clone()))
        .await
        .unwrap();

    let after = reader::read_global(&read_conn, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].data, b"b1");
    assert_eq!(after[0].tenant_hash.as_raw(), tenant_b.hash().as_raw());
}

