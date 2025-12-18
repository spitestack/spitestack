//! SpiteDB CLI
//!
//! This binary provides a command-line interface to SpiteDB.
//! Currently it just demonstrates that the library works.

use spitedb::Database;

fn main() {
    println!("SpiteDB - Event Store Runtime");
    println!("==============================\n");

    // Create an in-memory database to demonstrate the library works
    match Database::open_in_memory() {
        Ok(_db) => {
            println!("Successfully initialized in-memory database!");
            println!("Schema version 1 with the following tables:");
            println!("  - batches: Stores event data as compressed blobs");
            println!("  - event_index: Fast lookups by position or stream");
            println!("  - stream_heads: Caches latest revision per stream");
            println!("  - commands: Tracks processed commands for idempotency");
            println!("  - tombstones: Marks events for logical deletion");
            println!("  - spitedb_metadata: Schema versioning");
        }
        Err(e) => {
            eprintln!("Failed to initialize database: {e}");
            std::process::exit(1);
        }
    }
}
