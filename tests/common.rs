#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};
use spitedb::crypto::{BatchCryptor, EnvKeyProvider};
use spitedb::{Database, WriterConfig};

pub fn test_key() -> [u8; 32] {
    [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
        0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    ]
}

pub fn test_cryptor() -> BatchCryptor {
    BatchCryptor::new(EnvKeyProvider::from_key(test_key()))
}

pub fn create_temp_db_file(name: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let path = dir.path().join(name);
    let _ = Database::open(&path).expect("initialize database");
    (dir, path)
}

pub fn open_read_only(path: &Path) -> Connection {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("open read-only connection")
}

pub fn open_read_write(path: &Path) -> Connection {
    Connection::open(path).expect("open read-write connection")
}

pub async fn eventually<T>(
    timeout: Duration,
    interval: Duration,
    mut f: impl FnMut() -> Option<T>,
) -> T {
    let start = std::time::Instant::now();
    loop {
        if let Some(v) = f() {
            return v;
        }
        if start.elapsed() > timeout {
            panic!("condition not met within {:?}", timeout);
        }
        tokio::time::sleep(interval).await;
    }
}

pub fn writer_config_with_batch_timeout(batch_timeout: Duration) -> WriterConfig {
    WriterConfig {
        batch_timeout,
        ..WriterConfig::default()
    }
}
