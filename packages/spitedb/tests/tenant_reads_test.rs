mod common;

use spitedb::types::{AppendCommand, EventData, StreamRev};
use spitedb::SpiteDB;

#[tokio::test]
async fn tenant_scoped_reads_return_correct_tenant() {
    let (_dir, path) = common::create_temp_db_file("tenant_reads.db");
    let cryptor = common::test_cryptor();

    let db = SpiteDB::open_with_cryptor(&path, cryptor.clone_with_same_key())
        .await
        .unwrap();

    db.append(AppendCommand::new_with_tenant(
        "cmd-a",
        "user-1",
        "tenant-a",
        StreamRev::NONE,
        vec![EventData::new(b"a1".to_vec())],
    ))
    .await
    .unwrap();

    db.append(AppendCommand::new_with_tenant(
        "cmd-b",
        "user-1",
        "tenant-b",
        StreamRev::NONE,
        vec![EventData::new(b"b1".to_vec())],
    ))
    .await
    .unwrap();

    // Give WAL a moment to become visible to read-only connections.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let a = db
        .read_stream_tenant("user-1", "tenant-a", StreamRev::FIRST, 10)
        .await
        .unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].data, b"a1");

    let b = db
        .read_stream_tenant("user-1", "tenant-b", StreamRev::FIRST, 10)
        .await
        .unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].data, b"b1");

    // Default-tenant reads must not accidentally return a different tenant.
    let default_tenant = db.read_stream("user-1", StreamRev::FIRST, 10).await.unwrap();
    assert!(
        default_tenant.is_empty(),
        "default tenant read must not leak other tenants"
    );

    db.shutdown().await;
}

