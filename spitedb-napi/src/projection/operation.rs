//! Projection operation types.

use serde_json::Value as JsonValue;

use crate::{BatchResultNapi, ProjectionOpNapi};

/// Operation type for projection updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    Upsert,
    Delete,
}

/// A single projection operation.
#[derive(Debug, Clone)]
pub struct ProjectionOp {
    pub op_type: OpType,
    pub key: String,
    pub value: Option<JsonValue>,
}

impl From<ProjectionOpNapi> for ProjectionOp {
    fn from(napi: ProjectionOpNapi) -> Self {
        Self {
            op_type: match napi.op_type.as_str() {
                "delete" => OpType::Delete,
                _ => OpType::Upsert, // Default to upsert
            },
            key: napi.key,
            value: napi.value.and_then(|s| serde_json::from_str(&s).ok()),
        }
    }
}

/// Result of processing a batch - operations to apply.
#[derive(Debug)]
pub struct BatchResult {
    pub projection_name: String,
    pub operations: Vec<ProjectionOp>,
    pub last_global_pos: i64,
}

impl From<BatchResultNapi> for BatchResult {
    fn from(napi: BatchResultNapi) -> Self {
        Self {
            projection_name: napi.projection_name,
            operations: napi.operations.into_iter().map(ProjectionOp::from).collect(),
            last_global_pos: napi.last_global_pos,
        }
    }
}
