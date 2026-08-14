//! Database connection and schema initialisation.
//!
//! Design
//! ------
//! We use `rusqlite` (synchronous) wrapped in `Arc<Mutex<Connection>>`, since
//! only one task should access the DB at a time — the same guarantee SQLite
//! requires. Async call sites acquire the lock inside `spawn_blocking` so the
//! sync rusqlite calls don't block the tokio runtime:
//!
//!     let db2 = db.clone();
//!     spawn_blocking(move || {
//!         let conn = db2.lock().unwrap();
//!         // ... sync rusqlite calls ...
//!     }).await?;

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use rusqlite::Connection;

/// Shared DB handle — clone this cheaply to hand it to other tasks/threads.
pub type Db = Arc<Mutex<Connection>>;

/// Open (or create) the SQLite database and return a shared handle.
///
/// `path` is the SQLite file path, e.g. "leadintel.db", "/tmp/test.db".
pub fn open(path: &str) -> Result<Db> {
    let conn = Connection::open(path)
        .with_context(|| format!("could not open database at {path}"))?;

    // WAL mode lets readers and one writer operate concurrently without
    // blocking each other — important when the worker and scheduler both
    // access the DB.
    conn.execute_batch("PRAGMA journal_mode=WAL;")?;

    Ok(Arc::new(Mutex::new(conn)))
}

/// Create all tables if they don't already exist.
///
/// We use `IF NOT EXISTS` so it's safe to call on an existing database.
pub fn init_schema(db: &Db) -> Result<()> {
    let conn = db.lock().unwrap();

    conn.execute_batch("
        CREATE TABLE IF NOT EXISTS leads (
            id              TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            company         TEXT NOT NULL,
            state           TEXT NOT NULL DEFAULT 'RAW',
            current_doubt   REAL NOT NULL DEFAULT 1.0,
            budget_cents    INTEGER NOT NULL DEFAULT 25,
            spent_cents     INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT
        );

        CREATE TABLE IF NOT EXISTS observations (
            id          TEXT PRIMARY KEY,
            lead_id     TEXT NOT NULL REFERENCES leads(id),
            field_name  TEXT NOT NULL,
            value       TEXT NOT NULL,
            source      TEXT NOT NULL,
            confidence  REAL NOT NULL,
            run_id      TEXT NOT NULL REFERENCES enrichment_runs(id),
            created_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS enrichment_runs (
            id               TEXT PRIMARY KEY,
            lead_id          TEXT NOT NULL REFERENCES leads(id),
            stage            TEXT NOT NULL,
            idempotency_key  TEXT NOT NULL UNIQUE,
            cost_cents       INTEGER NOT NULL DEFAULT 0,
            success          INTEGER NOT NULL DEFAULT 0,
            started_at       TEXT,
            finished_at      TEXT
        );

        CREATE TABLE IF NOT EXISTS signals (
            id           TEXT PRIMARY KEY,
            lead_id      TEXT NOT NULL REFERENCES leads(id),
            signal_type  TEXT NOT NULL,
            score        REAL NOT NULL,
            explanation  TEXT NOT NULL,
            created_at   TEXT
        );
    ").context("failed to create schema")?;

    Ok(())
}
