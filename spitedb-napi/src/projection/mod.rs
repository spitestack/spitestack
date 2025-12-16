//! # Projection Infrastructure
//!
//! This module manages projection tables and checkpoints. Each projection is stored
//! in its own SQLite database file, ensuring complete isolation between projections.
//!
//! ## Architecture
//!
//! ```text
//! ProjectionRegistry
//!     │
//!     ├── {projections_dir}/user_stats.db
//!     │   ├── user_stats (projection table)
//!     │   └── _checkpoint (position tracking)
//!     │
//!     ├── {projections_dir}/order_totals.db
//!     │   ├── order_totals (projection table)
//!     │   └── _checkpoint (position tracking)
//!     │
//!     └── SpiteDB reference (for reading events)
//! ```
//!
//! ## Key Benefits
//!
//! - **Isolation**: One slow projection doesn't affect others
//! - **Independent checkpoints**: Each projection tracks its own position
//! - **Independent failure**: A failing projection doesn't crash others
//! - **Independent rebuild**: Can rebuild one projection without affecting others
//! - **Multi-core**: SQLite operations distributed via spawn_blocking

mod consumer;
mod error;
mod instance;
mod operation;
mod registry;
mod schema;

// Re-export main types
pub use consumer::{ProjectionConsumer, ProjectionConsumerConfig, DEFAULT_BATCH_SIZE};
pub use error::ProjectionError;
pub use instance::ProjectionInstance;
pub use operation::{BatchResult, OpType, ProjectionOp};
pub use registry::ProjectionRegistry;
pub use schema::{ColumnDef, ColumnType, ProjectionSchema};
