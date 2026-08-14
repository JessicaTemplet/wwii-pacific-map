//! Crate-wide error type.
//!
//! One `LeadIntelError` enum holds every kind of error this app can produce,
//! so every function in this crate can return `Result<T, LeadIntelError>`
//! (or the shorthand `anyhow::Result<T>`).

use std::fmt;

/// All error kinds this application can produce.
#[derive(Debug)]
pub enum LeadIntelError {
    /// A database operation failed.
    Database(rusqlite::Error),

    /// A Redis operation failed.
    Redis(redis::RedisError),

    /// JSON encode/decode failed (job payloads).
    Json(serde_json::Error),

    /// YAML parse failed (pipeline.yaml).
    Yaml(serde_yaml::Error),

    /// Something was not found (lead_id not in DB, etc.)
    NotFound(String),

    /// A required field was missing or invalid.
    InvalidState(String),

    /// File I/O error (reading pipeline.yaml, CSV ingestion, etc.)
    Io(std::io::Error),
}

// ── Automatic conversions from lower-level errors ────────────────────────────

impl From<rusqlite::Error> for LeadIntelError {
    fn from(e: rusqlite::Error) -> Self {
        LeadIntelError::Database(e)
    }
}

impl From<redis::RedisError> for LeadIntelError {
    fn from(e: redis::RedisError) -> Self {
        LeadIntelError::Redis(e)
    }
}

impl From<serde_json::Error> for LeadIntelError {
    fn from(e: serde_json::Error) -> Self {
        LeadIntelError::Json(e)
    }
}

impl From<serde_yaml::Error> for LeadIntelError {
    fn from(e: serde_yaml::Error) -> Self {
        LeadIntelError::Yaml(e)
    }
}

impl From<std::io::Error> for LeadIntelError {
    fn from(e: std::io::Error) -> Self {
        LeadIntelError::Io(e)
    }
}

// ── Display — what gets printed when the error is shown ──────────────────────

impl fmt::Display for LeadIntelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LeadIntelError::Database(e)    => write!(f, "database error: {e}"),
            LeadIntelError::Redis(e)       => write!(f, "redis error: {e}"),
            LeadIntelError::Json(e)        => write!(f, "json error: {e}"),
            LeadIntelError::Yaml(e)        => write!(f, "yaml error: {e}"),
            LeadIntelError::NotFound(msg)  => write!(f, "not found: {msg}"),
            LeadIntelError::InvalidState(msg) => write!(f, "invalid state: {msg}"),
            LeadIntelError::Io(e)          => write!(f, "I/O error: {e}"),
        }
    }
}

/// Required for `anyhow::Error` and other error-handling crates to accept our type.
impl std::error::Error for LeadIntelError {}
