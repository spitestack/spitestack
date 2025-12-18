//! Single projection database instance.
//!
//! Each projection gets its own SQLite database file with:
//! - A projection table for the read model
//! - A checkpoint table for tracking processed position

use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value as JsonValue;

use super::error::ProjectionError;
use super::operation::{OpType, ProjectionOp};
use super::schema::{json_to_sql_literal, json_value_to_sql, row_value_to_json, ProjectionSchema};

/// Manages a single projection's SQLite database.
///
/// Each projection has its own database file at `{base_dir}/{name}.db`.
/// The checkpoint is stored in the same database, ensuring atomicity
/// between data updates and checkpoint advancement.
pub struct ProjectionInstance {
    /// Name of the projection
    name: String,

    /// SQLite connection
    conn: Connection,

    /// Schema for the projection table
    schema: ProjectionSchema,

    /// Database file path
    db_path: PathBuf,
}

impl ProjectionInstance {
    /// Creates a new projection instance.
    ///
    /// Opens or creates the SQLite database file at `{base_dir}/{name}.db`,
    /// creates the projection table and checkpoint table if they don't exist.
    pub fn new(name: &str, base_dir: &Path, schema: ProjectionSchema) -> Result<Self, ProjectionError> {
        // Ensure base directory exists
        std::fs::create_dir_all(base_dir)?;

        let db_path = base_dir.join(format!("{}.db", name));
        let conn = Connection::open(&db_path)?;

        // Configure SQLite for performance
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;",
        )?;

        let mut instance = Self {
            name: name.to_string(),
            conn,
            schema,
            db_path,
        };

        instance.ensure_tables()?;

        Ok(instance)
    }

    /// Returns the projection name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the database path.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Ensures the projection table and checkpoint table exist.
    fn ensure_tables(&mut self) -> Result<(), ProjectionError> {
        self.create_projection_table()?;
        self.ensure_checkpoint_table()?;
        Ok(())
    }

    /// Creates the projection table from its schema.
    fn create_projection_table(&self) -> Result<(), ProjectionError> {
        let mut sql = format!("CREATE TABLE IF NOT EXISTS {} (\n", self.schema.table_name);

        let mut primary_keys = Vec::new();

        for (i, col) in self.schema.columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(",\n");
            }

            sql.push_str(&format!("    {} {}", col.name, col.col_type.to_sql()));

            if !col.nullable {
                sql.push_str(" NOT NULL");
            }

            if let Some(ref default) = col.default_value {
                sql.push_str(&format!(" DEFAULT {}", json_to_sql_literal(default)));
            }

            if col.primary_key {
                primary_keys.push(col.name.clone());
            }
        }

        if !primary_keys.is_empty() {
            sql.push_str(&format!(",\n    PRIMARY KEY ({})", primary_keys.join(", ")));
        }

        sql.push_str("\n)");

        self.conn.execute(&sql, [])?;

        Ok(())
    }

    /// Ensures the checkpoint table exists.
    fn ensure_checkpoint_table(&self) -> Result<(), ProjectionError> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS _checkpoint (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_global_pos INTEGER NOT NULL,
                last_processed_ms INTEGER NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    /// Gets the current checkpoint (last processed global_pos).
    pub fn get_checkpoint(&self) -> Result<Option<i64>, ProjectionError> {
        let result: Option<i64> = self
            .conn
            .query_row(
                "SELECT last_global_pos FROM _checkpoint WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;

        Ok(result)
    }

    /// Reads a row by primary key.
    ///
    /// Returns the row as a JSON object, or None if not found.
    pub fn read_row(&self, key: &str) -> Result<Option<JsonValue>, ProjectionError> {
        let pk_name = self
            .schema
            .primary_key_name()
            .ok_or_else(|| ProjectionError::NoPrimaryKey(self.name.clone()))?;

        let sql = format!(
            "SELECT * FROM {} WHERE {} = ?",
            self.schema.table_name, pk_name
        );

        let mut stmt = self.conn.prepare(&sql)?;

        let result = stmt
            .query_row([key], |row| {
                let mut obj = serde_json::Map::new();

                for (i, col) in self.schema.columns.iter().enumerate() {
                    let value = row_value_to_json(row, i, col.col_type)?;
                    obj.insert(col.name.clone(), value);
                }

                Ok(JsonValue::Object(obj))
            })
            .optional()?;

        Ok(result)
    }

    /// Applies a batch of operations atomically with checkpoint update.
    ///
    /// All operations and the checkpoint update happen in a single transaction.
    pub fn apply_batch(
        &mut self,
        operations: Vec<ProjectionOp>,
        checkpoint: i64,
    ) -> Result<(), ProjectionError> {
        // Start transaction
        self.conn.execute("BEGIN IMMEDIATE", [])?;

        let result = self.apply_batch_inner(&operations, checkpoint);

        match result {
            Ok(()) => {
                self.conn.execute("COMMIT", [])?;
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute("ROLLBACK", []);
                Err(e)
            }
        }
    }

    fn apply_batch_inner(
        &self,
        operations: &[ProjectionOp],
        checkpoint: i64,
    ) -> Result<(), ProjectionError> {
        let pk_name = self
            .schema
            .primary_key_name()
            .ok_or_else(|| ProjectionError::NoPrimaryKey(self.name.clone()))?;

        for op in operations {
            match op.op_type {
                OpType::Upsert => {
                    self.execute_upsert(pk_name, &op.key, op.value.as_ref())?;
                }
                OpType::Delete => {
                    self.execute_delete(pk_name, &op.key)?;
                }
            }
        }

        // Update checkpoint
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        self.conn.execute(
            "INSERT OR REPLACE INTO _checkpoint (id, last_global_pos, last_processed_ms)
             VALUES (1, ?, ?)",
            params![checkpoint, now_ms],
        )?;

        Ok(())
    }

    fn execute_upsert(
        &self,
        pk_name: &str,
        key: &str,
        value: Option<&JsonValue>,
    ) -> Result<(), ProjectionError> {
        let value = value.ok_or(ProjectionError::MissingValue)?;

        // Build column list and values
        let mut columns = vec![pk_name.to_string()];
        let mut placeholders = vec!["?".to_string()];
        let mut values: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(key.to_string())];

        if let JsonValue::Object(obj) = value {
            for col in &self.schema.columns {
                if col.name == pk_name {
                    continue; // Already handled
                }

                if let Some(v) = obj.get(&col.name) {
                    columns.push(col.name.clone());
                    placeholders.push("?".to_string());
                    values.push(json_value_to_sql(v));
                }
            }
        }

        let sql = format!(
            "INSERT OR REPLACE INTO {} ({}) VALUES ({})",
            self.schema.table_name,
            columns.join(", "),
            placeholders.join(", ")
        );

        let params: Vec<&dyn rusqlite::ToSql> = values.iter().map(|v| v.as_ref()).collect();
        self.conn.execute(&sql, params.as_slice())?;

        Ok(())
    }

    fn execute_delete(&self, pk_name: &str, key: &str) -> Result<(), ProjectionError> {
        let sql = format!(
            "DELETE FROM {} WHERE {} = ?",
            self.schema.table_name, pk_name
        );
        self.conn.execute(&sql, [key])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::schema::{ColumnDef, ColumnType};
    use tempfile::TempDir;

    fn create_test_schema() -> ProjectionSchema {
        ProjectionSchema {
            table_name: "test_projection".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::Text,
                    primary_key: true,
                    nullable: false,
                    default_value: None,
                },
                ColumnDef {
                    name: "count".to_string(),
                    col_type: ColumnType::Integer,
                    primary_key: false,
                    nullable: false,
                    default_value: None,
                },
            ],
        }
    }

    #[test]
    fn test_create_instance() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let instance = ProjectionInstance::new("test", temp_dir.path(), schema).unwrap();

        assert_eq!(instance.name(), "test");
        assert!(instance.db_path().exists());
    }

    #[test]
    fn test_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut instance = ProjectionInstance::new("test", temp_dir.path(), schema).unwrap();

        // Initially no checkpoint
        assert!(instance.get_checkpoint().unwrap().is_none());

        // Apply empty batch with checkpoint
        instance.apply_batch(vec![], 100).unwrap();

        // Now checkpoint should be set
        assert_eq!(instance.get_checkpoint().unwrap(), Some(100));
    }

    #[test]
    fn test_upsert_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut instance = ProjectionInstance::new("test", temp_dir.path(), schema).unwrap();

        let ops = vec![ProjectionOp {
            op_type: OpType::Upsert,
            key: "key1".to_string(),
            value: Some(serde_json::json!({"count": 42})),
        }];

        instance.apply_batch(ops, 1).unwrap();

        let row = instance.read_row("key1").unwrap();
        assert!(row.is_some());

        let obj = row.unwrap();
        assert_eq!(obj["id"], "key1");
        assert_eq!(obj["count"], 42);
    }

    #[test]
    fn test_delete() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut instance = ProjectionInstance::new("test", temp_dir.path(), schema).unwrap();

        // Insert a row
        let ops = vec![ProjectionOp {
            op_type: OpType::Upsert,
            key: "key1".to_string(),
            value: Some(serde_json::json!({"count": 42})),
        }];
        instance.apply_batch(ops, 1).unwrap();

        // Verify it exists
        assert!(instance.read_row("key1").unwrap().is_some());

        // Delete it
        let ops = vec![ProjectionOp {
            op_type: OpType::Delete,
            key: "key1".to_string(),
            value: None,
        }];
        instance.apply_batch(ops, 2).unwrap();

        // Verify it's gone
        assert!(instance.read_row("key1").unwrap().is_none());
    }
}
