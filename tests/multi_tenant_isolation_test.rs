mod common;

use spitedb::reader;
use spitedb::types::{AppendCommand, DeleteStreamCommand, DeleteTenantCommand, EventData, GlobalPos, StreamRev, Tenant};
use spitedb::{spawn_batch_writer, Database};

#[tokio::test]
async fn tenant_scoped_streams_do_not_interfere() {
    let (_dir, path) = common::create_temp_db_file("multi_tenant.db");
    let cryptor = common::test_cryptor();

    let tenant_a = Tenant::new("tenant-a");
    let tenant_b = Tenant::new("tenant-b");
    let tenant_a_hash = tenant_a.hash();
    let tenant_b_hash = tenant_b.hash();

    let writer_db = Database::open(&path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        spitedb::WriterConfig::default(),
    )
    .unwrap();

    // Same stream_id string used in two tenants must behave like two independent streams.
    writer
        .append(AppendCommand::new_with_tenant(
            "cmd-a-1",
            "user-1",
            tenant_a.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"a1".to_vec()), EventData::new(b"a2".to_vec())],
        ))
        .await
        .unwrap();
    writer
        .append(AppendCommand::new_with_tenant(
            "cmd-b-1",
            "user-1",
            tenant_b.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"b1".to_vec())],
        ))
        .await
        .unwrap();

    let read_conn = common::open_read_only(&path);

    let a_events =
        reader::read_tenant_events(&read_conn, tenant_a_hash, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert_eq!(a_events.len(), 2);
    assert_eq!(a_events[0].data, b"a1");
    assert_eq!(a_events[0].stream_rev.as_raw(), 1);
    assert_eq!(a_events[1].data, b"a2");
    assert_eq!(a_events[1].stream_rev.as_raw(), 2);

    let b_events =
        reader::read_tenant_events(&read_conn, tenant_b_hash, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert_eq!(b_events.len(), 1);
    assert_eq!(b_events[0].data, b"b1");
    assert_eq!(b_events[0].stream_rev.as_raw(), 1);

    // Deleting the stream in tenant A must not delete tenant B's data.
    writer
        .delete_stream(DeleteStreamCommand::delete_all(
            "cmd-delete-stream-a",
            "user-1",
            tenant_a.clone(),
        ))
        .await
        .unwrap();

    let a_after =
        reader::read_tenant_events(&read_conn, tenant_a_hash, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert!(a_after.is_empty(), "tenant A should be filtered after stream delete");

    let b_after =
        reader::read_tenant_events(&read_conn, tenant_b_hash, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert_eq!(b_after.len(), 1, "tenant B must be unaffected");

    // Deleting tenant A must not delete tenant B.
    writer
        .delete_tenant(DeleteTenantCommand::new("cmd-delete-tenant-a", tenant_a))
        .await
        .unwrap();

    let global_after = reader::read_global(&read_conn, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert_eq!(global_after.len(), 1);
    assert_eq!(global_after[0].data, b"b1");
}

