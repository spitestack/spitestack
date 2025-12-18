mod common;

use spitedb::reader;
use spitedb::types::{AppendCommand, EventData, GlobalPos, StreamRev, Tenant};
use spitedb::{spawn_batch_writer, Database, Error};

#[tokio::test]
async fn command_id_is_tenant_scoped_and_cannot_be_reused_for_different_streams_within_tenant() {
    let (_dir, path) = common::create_temp_db_file("idempotency_scope.db");
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

    // Same command_id used in two different tenants should not collide.
    writer
        .append(AppendCommand::new_with_tenant(
            "cmd-shared",
            "stream-a",
            tenant_a.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"a1".to_vec())],
        ))
        .await
        .unwrap();
    writer
        .append(AppendCommand::new_with_tenant(
            "cmd-shared",
            "stream-b",
            tenant_b.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"b1".to_vec())],
        ))
        .await
        .unwrap();

    // Reusing the same (tenant, command_id) for a different stream must error.
    let err = writer
        .append(AppendCommand::new_with_tenant(
            "cmd-shared",
            "different-stream",
            tenant_a.clone(),
            StreamRev::NONE,
            vec![EventData::new(b"should-not-write".to_vec())],
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Schema(ref msg) if msg.contains("idempotency violation")),
        "expected idempotency violation, got: {err:?}"
    );

    drop(writer);

    // Ensure the violating append didn't write anything.
    let read_conn = common::open_read_only(&path);
    let global = reader::read_global(&read_conn, GlobalPos::FIRST, 100, &cryptor).unwrap();
    assert_eq!(global.len(), 2);
    assert_eq!(global[0].data, b"a1");
    assert_eq!(global[1].data, b"b1");
}
