//! Data models.
//!
//! We separate concerns across three files:
//!   - This file defines plain Rust structs (the data)
//!   - `db.rs` defines the SQL schema (CREATE TABLE ...)
//!   - `repository.rs` defines the queries (SELECT, INSERT, UPDATE ...)

use chrono::{DateTime, Utc};

// ─────────────────────────────────────────────────────────────────────────────
// Lead
// ─────────────────────────────────────────────────────────────────────────────

/// A person to be enriched.
///
/// Field notes:
///   - `id` is a UUID string
///   - `state` starts as "RAW", transitions to "DONE" when pipeline finishes
///   - `current_doubt` tracks how uncertain we are about this lead (0.0–1.0)
///   - `budget_cents` / `spent_cents` control how much we're allowed to spend
///     finding info for this lead
#[derive(Debug, Clone)]
pub struct Lead {
    pub id:             String,
    pub name:           String,
    pub company:        String,
    pub state:          String,         // "RAW" | "DONE"
    pub current_doubt:  f64,            // 0.0 = fully resolved, 1.0 = no data
    pub budget_cents:   i64,            // max allowed spend
    pub spent_cents:    i64,            // cumulative spend so far
    pub created_at:     Option<String>, // stored as ISO-8601 text in SQLite
}

impl Lead {
    /// Create a new raw lead ready for enrichment.
    pub fn new(name: impl Into<String>, company: impl Into<String>) -> Self {
        Lead {
            id:            uuid::Uuid::new_v4().to_string(),
            name:          name.into(),
            company:       company.into(),
            state:         "RAW".to_owned(),
            current_doubt: 1.0,
            budget_cents:  25,
            spent_cents:   0,
            created_at:    Some(Utc::now().to_rfc3339()),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Observation
// ─────────────────────────────────────────────────────────────────────────────

/// A single piece of evidence about a lead from one source.
///
/// Each call to an enrichment source produces one Observation per field it
/// returns.  If two sources both return a title, you get two Observations —
/// which is how we detect conflicts (doubt score increases when titles differ).
#[derive(Debug, Clone)]
pub struct Observation {
    pub id:          String,
    pub lead_id:     String,
    pub field_name:  String,  // "title" or "email"
    pub value:       String,
    pub source:      String,  // "mock_apollo", "mock_hunter", etc.
    pub confidence:  f64,     // 0.0–1.0
    pub run_id:      String,  // which EnrichmentRun produced this
    pub created_at:  Option<String>,
}

impl Observation {
    pub fn new(
        lead_id:    impl Into<String>,
        field_name: impl Into<String>,
        value:      impl Into<String>,
        source:     impl Into<String>,
        confidence: f64,
        run_id:     impl Into<String>,
    ) -> Self {
        Observation {
            id:         uuid::Uuid::new_v4().to_string(),
            lead_id:    lead_id.into(),
            field_name: field_name.into(),
            value:      value.into(),
            source:     source.into(),
            confidence,
            run_id:     run_id.into(),
            created_at: Some(Utc::now().to_rfc3339()),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EnrichmentRun
// ─────────────────────────────────────────────────────────────────────────────

/// One execution of one pipeline stage for one lead.
///
/// The `idempotency_key` is a unique string like "{lead_id}-shallow-v1".
/// Before running a stage, we check whether a run with that key already exists.
/// If it does, we skip — this is idempotency: running twice produces the same
/// result as running once.
#[derive(Debug, Clone)]
pub struct EnrichmentRun {
    pub id:               String,
    pub lead_id:          String,
    pub stage:            String,
    pub idempotency_key:  String,
    pub cost_cents:       i64,
    pub success:          bool,
    pub started_at:       Option<String>,
    pub finished_at:      Option<String>,
}

impl EnrichmentRun {
    pub fn new(
        lead_id: impl Into<String>,
        stage:   impl Into<String>,
        key:     impl Into<String>,
    ) -> Self {
        EnrichmentRun {
            id:              uuid::Uuid::new_v4().to_string(),
            lead_id:         lead_id.into(),
            stage:           stage.into(),
            idempotency_key: key.into(),
            cost_cents:      0,
            success:         false,
            started_at:      Some(Utc::now().to_rfc3339()),
            finished_at:     None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Signal
// ─────────────────────────────────────────────────────────────────────────────

/// A derived insight about a lead, generated after enrichment.
///
/// Examples: "title_conflict" (two sources disagree on title),
/// "missing_title" (no source found a title at all).
#[derive(Debug, Clone)]
pub struct Signal {
    pub id:           String,
    pub lead_id:      String,
    pub signal_type:  String,
    pub score:        f64,
    pub explanation:  String,
    pub created_at:   Option<String>,
}

impl Signal {
    pub fn new(
        lead_id:     impl Into<String>,
        signal_type: impl Into<String>,
        score:       f64,
        explanation: impl Into<String>,
    ) -> Self {
        Signal {
            id:          uuid::Uuid::new_v4().to_string(),
            lead_id:     lead_id.into(),
            signal_type: signal_type.into(),
            score,
            explanation: explanation.into(),
            created_at:  Some(Utc::now().to_rfc3339()),
        }
    }
}
