//! SpiteDB Stress Test Binary
//!
//! A standalone binary for stress testing SpiteDB under high concurrency.
//! Run with: `cargo run --bin stress_test -- [OPTIONS]`
//!
//! This is separate from the regular test suite because:
//! 1. It can take a long time to run
//! 2. It's configurable via command-line arguments
//! 3. It reports detailed metrics
//!
//! # Examples
//!
//! ```bash
//! # Default test: 100 streams, 1000 events, 10 concurrent tasks
//! cargo run --release --bin stress_test
//!
//! # High concurrency test
//! cargo run --release --bin stress_test -- --streams 500 --events 10000 --concurrency 50
//!
//! # Same-stream contention test
//! cargo run --release --bin stress_test -- --same-stream --events 1000 --concurrency 20
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use spitedb::crypto::{BatchCryptor, EnvKeyProvider};
use spitedb::types::{AppendCommand, EventData, GlobalPos, StreamRev};
use spitedb::{reader, spawn_batch_writer, Database, WriterConfig};

/// Stress test configuration
struct Config {
    /// Number of unique streams to write to
    num_streams: usize,
    /// Total number of events to write
    num_events: usize,
    /// Number of concurrent writer tasks
    concurrency: usize,
    /// Whether to test same-stream contention
    same_stream: bool,
    /// Path to database file (or temp if None)
    db_path: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            num_streams: 100,
            num_events: 1000,
            concurrency: 10,
            same_stream: false,
            db_path: None,
        }
    }
}

fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut config = Config::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--streams" | "-s" => {
                i += 1;
                config.num_streams = args[i].parse().expect("Invalid --streams value");
            }
            "--events" | "-e" => {
                i += 1;
                config.num_events = args[i].parse().expect("Invalid --events value");
            }
            "--concurrency" | "-c" => {
                i += 1;
                config.concurrency = args[i].parse().expect("Invalid --concurrency value");
            }
            "--same-stream" => {
                config.same_stream = true;
            }
            "--db" | "-d" => {
                i += 1;
                config.db_path = Some(args[i].clone());
            }
            "--help" | "-h" => {
                println!(
                    r#"SpiteDB Stress Test

Usage: stress_test [OPTIONS]

Options:
  -s, --streams <N>     Number of unique streams (default: 100)
  -e, --events <N>      Total events to write (default: 1000)
  -c, --concurrency <N> Concurrent writer tasks (default: 10)
  --same-stream         Test same-stream contention
  -d, --db <PATH>       Database path (default: temp file)
  -h, --help            Show this help
"#
                );
                std::process::exit(0);
            }
            arg => {
                eprintln!("Unknown argument: {}", arg);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    config
}

fn test_key() -> [u8; 32] {
    [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ]
}

#[tokio::main]
async fn main() {
    let config = parse_args();

    println!("SpiteDB Stress Test");
    println!("==================");
    println!("Streams:     {}", config.num_streams);
    println!("Events:      {}", config.num_events);
    println!("Concurrency: {}", config.concurrency);
    println!(
        "Mode:        {}",
        if config.same_stream {
            "Same-stream contention"
        } else {
            "Multi-stream"
        }
    );
    println!();

    // Setup database
    let temp_dir = std::env::temp_dir().join(format!("spitedb-stress-{}", std::process::id()));
    std::fs::create_dir_all(&temp_dir).expect("create temp dir");
    let db_path = config
        .db_path
        .clone()
        .unwrap_or_else(|| temp_dir.join("stress.db").to_string_lossy().to_string());

    println!("Database:    {}", db_path);
    println!();

    let cryptor = BatchCryptor::new(EnvKeyProvider::from_key(test_key()));

    let writer_db = Database::open(&db_path).unwrap();
    let writer = spawn_batch_writer(
        writer_db.into_connection(),
        cryptor.clone_with_same_key(),
        WriterConfig::default(),
    )
    .unwrap();

    // Metrics
    let events_written = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    // Stream revision tracking for same-stream mode
    let stream_rev = Arc::new(AtomicU64::new(0));

    println!("Starting stress test...");
    let start = Instant::now();

    // Spawn concurrent tasks
    let mut handles = Vec::new();
    let events_per_task = config.num_events / config.concurrency;

    for task_id in 0..config.concurrency {
        let w = writer.clone();
        let events_written = events_written.clone();
        let conflicts = conflicts.clone();
        let errors = errors.clone();
        let stream_rev = stream_rev.clone();
        let num_streams = config.num_streams;
        let same_stream = config.same_stream;

        let handle = tokio::spawn(async move {
            for i in 0..events_per_task {
                let stream_id = if same_stream {
                    "stress-stream".to_string()
                } else {
                    format!("stream-{}", (task_id * events_per_task + i) % num_streams)
                };

                let expected_rev = if same_stream {
                    // Use ANY for same-stream mode to avoid tracking revisions
                    StreamRev::ANY
                } else {
                    StreamRev::ANY
                };

                let cmd = AppendCommand::new(
                    format!("cmd-{}-{}", task_id, i),
                    stream_id,
                    expected_rev,
                    vec![EventData::new(format!("stress-event-{}-{}", task_id, i).into_bytes())],
                );

                match w.append(cmd).await {
                    Ok(_result) => {
                        events_written.fetch_add(1, Ordering::Relaxed);
                        if same_stream {
                            stream_rev.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(spitedb::Error::Conflict { .. }) => {
                        conflicts.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    drop(writer);

    // Report metrics
    let written = events_written.load(Ordering::Relaxed);
    let conflict_count = conflicts.load(Ordering::Relaxed);
    let error_count = errors.load(Ordering::Relaxed);

    println!();
    println!("Results");
    println!("-------");
    println!("Events written:  {}", written);
    println!("Conflicts:       {}", conflict_count);
    println!("Errors:          {}", error_count);
    println!("Duration:        {:?}", elapsed);
    println!(
        "Throughput:      {:.2} events/sec",
        written as f64 / elapsed.as_secs_f64()
    );
    println!();

    // Verify invariants
    println!("Verifying invariants...");

    let read_conn =
        rusqlite::Connection::open_with_flags(&db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();

    // Count events
    let event_count: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM event_index", [], |row| row.get(0))
        .unwrap();

    println!("  Events in database: {}", event_count);
    assert_eq!(
        event_count as u64, written,
        "Event count mismatch: {} in DB, {} written",
        event_count, written
    );

    // Verify global positions are gapless
    let positions: Vec<i64> = {
        let mut stmt = read_conn
            .prepare("SELECT global_pos FROM event_index ORDER BY global_pos")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };

    if !positions.is_empty() {
        for (i, pos) in positions.iter().enumerate() {
            let expected = (i + 1) as i64;
            assert_eq!(
                *pos, expected,
                "Gap in global_pos: expected {}, got {}",
                expected, pos
            );
        }
        println!("  Global positions: gapless ✓");
    }

    // Verify stream revisions are gapless per stream
    let stream_hashes: Vec<i64> = {
        let mut stmt = read_conn
            .prepare("SELECT DISTINCT stream_hash FROM event_index")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };

    for hash in &stream_hashes {
        let revs: Vec<i64> = {
            let mut stmt = read_conn
                .prepare("SELECT stream_rev FROM event_index WHERE stream_hash = ? ORDER BY stream_rev")
                .unwrap();
            stmt.query_map([hash], |row| row.get(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };

        for (i, rev) in revs.iter().enumerate() {
            let expected = (i + 1) as i64;
            assert_eq!(
                *rev, expected,
                "Gap in stream_rev for hash {}: expected {}, got {}",
                hash, expected, rev
            );
        }
    }
    println!(
        "  Stream revisions:  gapless across {} streams ✓",
        stream_hashes.len()
    );

    // Verify batch integrity
    let batch_count: i64 = read_conn
        .query_row("SELECT COUNT(*) FROM batches", [], |row| row.get(0))
        .unwrap();
    let batch_events: i64 = read_conn
        .query_row("SELECT SUM(event_count) FROM batches", [], |row| row.get(0))
        .unwrap();

    assert_eq!(
        batch_events, event_count,
        "Batch event count mismatch: {} in batches, {} in index",
        batch_events, event_count
    );
    println!(
        "  Batch integrity:   {} batches, {} events ✓",
        batch_count, batch_events
    );

    // Read events to verify data integrity
    let global_events =
        reader::read_global(&read_conn, GlobalPos::FIRST, event_count as usize, &cryptor).unwrap();
    assert_eq!(global_events.len(), event_count as usize);
    println!("  Data integrity:    all events readable ✓");

    println!();
    println!("Stress test PASSED ✓");
}
