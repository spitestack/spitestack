//! Projection schema types.

use serde_json::Value as JsonValue;

use crate::ColumnDefNapi;

/// Column type for projection schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Text,
    Integer,
    Real,
    Blob,
    Boolean,
}

impl ColumnType {
    /// Converts to SQLite type string.
    pub fn to_sql(&self) -> &'static str {
        match self {
            ColumnType::Text => "TEXT",
            ColumnType::Integer => "INTEGER",
            ColumnType::Real => "REAL",
            ColumnType::Blob => "BLOB",
            ColumnType::Boolean => "INTEGER", // SQLite stores booleans as integers
        }
    }

    /// Parses from string (case-insensitive).
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "text" | "string" => Some(ColumnType::Text),
            "integer" | "int" | "bigint" => Some(ColumnType::Integer),
            "real" | "float" | "double" | "decimal" => Some(ColumnType::Real),
            "blob" | "bytes" => Some(ColumnType::Blob),
            "boolean" | "bool" => Some(ColumnType::Boolean),
            _ => None,
        }
    }
}

/// Definition of a column in a projection table.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnType,
    pub primary_key: bool,
    pub nullable: bool,
    pub default_value: Option<JsonValue>,
}

impl From<ColumnDefNapi> for ColumnDef {
    fn from(napi: ColumnDefNapi) -> Self {
        Self {
            name: napi.name,
            col_type: ColumnType::from_str(&napi.col_type).unwrap_or(ColumnType::Text),
            primary_key: napi.primary_key,
            nullable: napi.nullable,
            default_value: napi
                .default_value
                .and_then(|s| serde_json::from_str(&s).ok()),
        }
    }
}

/// Schema definition for a projection table.
#[derive(Debug, Clone)]
pub struct ProjectionSchema {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

impl ProjectionSchema {
    /// Gets the primary key column(s).
    pub fn primary_key_columns(&self) -> Vec<&ColumnDef> {
        self.columns.iter().filter(|c| c.primary_key).collect()
    }

    /// Gets the primary key column name (assumes single PK).
    pub fn primary_key_name(&self) -> Option<&str> {
        self.columns
            .iter()
            .find(|c| c.primary_key)
            .map(|c| c.name.as_str())
    }
}

/// Converts a JSON value to a SQL literal string.
pub fn json_to_sql_literal(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => format!("'{}'", s.replace('\'', "''")),
        _ => "NULL".to_string(),
    }
}

/// Converts a JSON value to a boxed SQL parameter.
pub fn json_value_to_sql(value: &JsonValue) -> Box<dyn rusqlite::ToSql> {
    match value {
        JsonValue::Null => Box::new(Option::<String>::None),
        JsonValue::Bool(b) => Box::new(if *b { 1i64 } else { 0i64 }),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Box::new(i)
            } else if let Some(f) = n.as_f64() {
                Box::new(f)
            } else {
                Box::new(n.to_string())
            }
        }
        JsonValue::String(s) => Box::new(s.clone()),
        _ => Box::new(serde_json::to_string(value).unwrap_or_default()),
    }
}

/// Converts a row value to JSON based on column type.
pub fn row_value_to_json(
    row: &rusqlite::Row,
    idx: usize,
    col_type: ColumnType,
) -> rusqlite::Result<JsonValue> {
    match col_type {
        ColumnType::Text => {
            let v: Option<String> = row.get(idx)?;
            Ok(v.map(JsonValue::String).unwrap_or(JsonValue::Null))
        }
        ColumnType::Integer => {
            let v: Option<i64> = row.get(idx)?;
            Ok(v.map(|n| JsonValue::Number(n.into()))
                .unwrap_or(JsonValue::Null))
        }
        ColumnType::Real => {
            let v: Option<f64> = row.get(idx)?;
            Ok(v.and_then(|n| serde_json::Number::from_f64(n).map(JsonValue::Number))
                .unwrap_or(JsonValue::Null))
        }
        ColumnType::Boolean => {
            let v: Option<i64> = row.get(idx)?;
            Ok(v.map(|n| JsonValue::Bool(n != 0))
                .unwrap_or(JsonValue::Null))
        }
        ColumnType::Blob => {
            let v: Option<Vec<u8>> = row.get(idx)?;
            // Return as base64 string for JSON compatibility
            Ok(v.map(|bytes| {
                use base64::Engine;
                JsonValue::String(base64::engine::general_purpose::STANDARD.encode(bytes))
            })
            .unwrap_or(JsonValue::Null))
        }
    }
}
