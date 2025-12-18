//! Projection error types.

/// Errors that can occur in projection operations.
#[derive(Debug)]
pub enum ProjectionError {
    /// SQLite error
    Sqlite(rusqlite::Error),
    /// Projection not found
    NotFound(String),
    /// Projection has no primary key defined
    NoPrimaryKey(String),
    /// Upsert operation missing value
    MissingValue,
    /// Projection already exists
    AlreadyExists(String),
    /// Consumer is not running
    NotRunning(String),
    /// Consumer is already running
    AlreadyRunning(String),
    /// IO error (e.g., creating directory)
    Io(std::io::Error),
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProjectionError::Sqlite(e) => write!(f, "SQLite error: {}", e),
            ProjectionError::NotFound(name) => write!(f, "Projection '{}' not found", name),
            ProjectionError::NoPrimaryKey(name) => {
                write!(f, "Projection '{}' has no primary key", name)
            }
            ProjectionError::MissingValue => write!(f, "Upsert operation missing value"),
            ProjectionError::AlreadyExists(name) => {
                write!(f, "Projection '{}' already exists", name)
            }
            ProjectionError::NotRunning(name) => {
                write!(f, "Projection '{}' consumer is not running", name)
            }
            ProjectionError::AlreadyRunning(name) => {
                write!(f, "Projection '{}' consumer is already running", name)
            }
            ProjectionError::Io(e) => write!(f, "IO error: {}", e),
            ProjectionError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for ProjectionError {}

impl From<rusqlite::Error> for ProjectionError {
    fn from(e: rusqlite::Error) -> Self {
        ProjectionError::Sqlite(e)
    }
}

impl From<std::io::Error> for ProjectionError {
    fn from(e: std::io::Error) -> Self {
        ProjectionError::Io(e)
    }
}
