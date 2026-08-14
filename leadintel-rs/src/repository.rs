//! Database CRUD operations.
//!
//! Design notes
//! ------------
//! We write SQL directly with rusqlite instead of using an ORM — lower-level,
//! but full control and no hidden N+1 query surprises.
//!
//! Every function takes `&rusqlite::Connection` — a shared reference to the
//! already-open connection.  The caller (in async code) is responsible for
//! acquiring the Mutex lock before calling these functions:
//!
//!     let db2 = db.clone();
//!     spawn_blocking(move || {
//!         let conn = db2.lock().unwrap();
//!         lead_repo::create(&conn, "Alice", "Acme")
//!     }).await??;

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

use crate::models::{EnrichmentRun, Lead, Observation, Signal};

// ─────────────────────────────────────────────────────────────────────────────
// Lead repository
// ─────────────────────────────────────────────────────────────────────────────

/// Insert a new lead and return it.
pub fn lead_create(conn: &Connection, name: &str, company: &str) -> Result<Lead> {
    let lead = Lead::new(name, company);
    conn.execute(
        "INSERT INTO leads (id, name, company, state, current_doubt, budget_cents, spent_cents, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            lead.id,
            lead.name,
            lead.company,
            lead.state,
            lead.current_doubt,
            lead.budget_cents,
            lead.spent_cents,
            lead.created_at,
        ],
    ).context("lead_create INSERT failed")?;
    Ok(lead)
}

/// Fetch a single lead by primary key.  Returns `None` if not found.
pub fn lead_get(conn: &Connection, lead_id: &str) -> Result<Option<Lead>> {
    let mut stmt = conn.prepare(
        "SELECT id, name, company, state, current_doubt, budget_cents, spent_cents, created_at
         FROM leads WHERE id = ?1"
    )?;

    // `.optional()` converts "no rows" from an error into `Ok(None)`.
    let result = stmt.query_row(params![lead_id], row_to_lead).optional()?;
    Ok(result)
}

/// Fetch all leads.
pub fn lead_list_all(conn: &Connection) -> Result<Vec<Lead>> {
    let mut stmt = conn.prepare(
        "SELECT id, name, company, state, current_doubt, budget_cents, spent_cents, created_at
         FROM leads"
    )?;

    let leads = stmt
        .query_map([], row_to_lead)?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(leads)
}

/// Persist changes to an existing lead (state, doubt, spent_cents).
/// Writes exactly these columns — nothing else.
pub fn lead_update(conn: &Connection, lead: &Lead) -> Result<()> {
    conn.execute(
        "UPDATE leads SET state=?1, current_doubt=?2, spent_cents=?3 WHERE id=?4",
        params![lead.state, lead.current_doubt, lead.spent_cents, lead.id],
    ).context("lead_update failed")?;
    Ok(())
}

/// Map a rusqlite row to a Lead struct.
/// This is a private helper used by the query functions above.
fn row_to_lead(row: &rusqlite::Row<'_>) -> rusqlite::Result<Lead> {
    Ok(Lead {
        id:            row.get(0)?,
        name:          row.get(1)?,
        company:       row.get(2)?,
        state:         row.get(3)?,
        current_doubt: row.get(4)?,
        budget_cents:  row.get(5)?,
        spent_cents:   row.get(6)?,
        created_at:    row.get(7)?,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Observation repository
// ─────────────────────────────────────────────────────────────────────────────

/// Insert a new observation.
pub fn observation_add(
    conn:       &Connection,
    lead_id:    &str,
    field_name: &str,
    value:      &str,
    source:     &str,
    confidence: f64,
    run_id:     &str,
) -> Result<Observation> {
    let obs = Observation::new(lead_id, field_name, value, source, confidence, run_id);
    conn.execute(
        "INSERT INTO observations (id, lead_id, field_name, value, source, confidence, run_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            obs.id,
            obs.lead_id,
            obs.field_name,
            obs.value,
            obs.source,
            obs.confidence,
            obs.run_id,
            obs.created_at,
        ],
    ).context("observation_add INSERT failed")?;
    Ok(obs)
}

/// Return all observations for a lead.
pub fn observations_for_lead(conn: &Connection, lead_id: &str) -> Result<Vec<Observation>> {
    let mut stmt = conn.prepare(
        "SELECT id, lead_id, field_name, value, source, confidence, run_id, created_at
         FROM observations WHERE lead_id = ?1"
    )?;

    let obs = stmt
        .query_map(params![lead_id], |row| {
            Ok(Observation {
                id:         row.get(0)?,
                lead_id:    row.get(1)?,
                field_name: row.get(2)?,
                value:      row.get(3)?,
                source:     row.get(4)?,
                confidence: row.get(5)?,
                run_id:     row.get(6)?,
                created_at: row.get(7)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(obs)
}

/// Return the set of distinct field names that have at least one observation.
pub fn observed_fields(conn: &Connection, lead_id: &str) -> Result<std::collections::HashSet<String>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT field_name FROM observations WHERE lead_id = ?1"
    )?;

    let fields = stmt
        .query_map(params![lead_id], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<std::collections::HashSet<_>, _>>()?;

    Ok(fields)
}

// ─────────────────────────────────────────────────────────────────────────────
// EnrichmentRun repository
// ─────────────────────────────────────────────────────────────────────────────

/// Insert a new enrichment run record.
pub fn run_create(conn: &Connection, lead_id: &str, stage: &str, key: &str) -> Result<EnrichmentRun> {
    let run = EnrichmentRun::new(lead_id, stage, key);
    conn.execute(
        "INSERT INTO enrichment_runs (id, lead_id, stage, idempotency_key, cost_cents, success, started_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            run.id,
            run.lead_id,
            run.stage,
            run.idempotency_key,
            run.cost_cents,
            run.success as i32,
            run.started_at,
        ],
    ).context("run_create INSERT failed")?;
    Ok(run)
}

/// Look up a run by idempotency key.  Returns None if not found.
/// Used to skip stages that already ran (idempotency guard).
pub fn run_get_by_key(conn: &Connection, key: &str) -> Result<Option<EnrichmentRun>> {
    let mut stmt = conn.prepare(
        "SELECT id, lead_id, stage, idempotency_key, cost_cents, success, started_at, finished_at
         FROM enrichment_runs WHERE idempotency_key = ?1"
    )?;

    let result = stmt.query_row(params![key], |row| {
        Ok(EnrichmentRun {
            id:               row.get(0)?,
            lead_id:          row.get(1)?,
            stage:            row.get(2)?,
            idempotency_key:  row.get(3)?,
            cost_cents:       row.get(4)?,
            success:          row.get::<_, i32>(5)? != 0, // SQLite stores bool as 0/1
            started_at:       row.get(6)?,
            finished_at:      row.get(7)?,
        })
    }).optional()?;

    Ok(result)
}

/// Mark a run as succeeded or failed.
pub fn run_complete(conn: &Connection, run_id: &str, success: bool) -> Result<()> {
    use chrono::Utc;
    conn.execute(
        "UPDATE enrichment_runs SET success=?1, finished_at=?2 WHERE id=?3",
        params![success as i32, Utc::now().to_rfc3339(), run_id],
    ).context("run_complete UPDATE failed")?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Signal repository
// ─────────────────────────────────────────────────────────────────────────────

/// Insert a signal.
pub fn signal_insert(conn: &Connection, signal: &Signal) -> Result<()> {
    conn.execute(
        "INSERT INTO signals (id, lead_id, signal_type, score, explanation, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            signal.id,
            signal.lead_id,
            signal.signal_type,
            signal.score,
            signal.explanation,
            signal.created_at,
        ],
    ).context("signal_insert failed")?;
    Ok(())
}
