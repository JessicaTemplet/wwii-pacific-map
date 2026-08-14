//! Enrichment tasks — the three pipeline stages.
//!
//! Each task simulates calling a vendor API by sleeping and returning mock
//! data.

use std::time::Duration;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use rand::seq::SliceRandom;
use rusqlite::Connection;
use tokio::task::spawn_blocking;
use tokio::time::sleep;

use crate::{db::Db, repository};

// ─────────────────────────────────────────────────────────────────────────────
// Mock source definitions
// ─────────────────────────────────────────────────────────────────────────────

struct MockSource {
    name:              &'static str,
    latency_secs:      f64,
    titles:            &'static [Option<&'static str>],
    emails:            &'static [Option<&'static str>],
    title_confidence:  f64,
    email_confidence:  f64,
}

// Static list of mock vendor sources tried in order by waterfall_enrichment.
static MOCK_SOURCES: &[MockSource] = &[
    MockSource {
        name:             "mock_apollo",
        latency_secs:     0.3,
        titles:           &[Some("VP Growth"), Some("Head of Marketing"), Some("Director of Sales"), None],
        emails:           &[Some("person@company.com"), None],
        title_confidence: 0.60,
        email_confidence: 0.55,
    },
    MockSource {
        name:             "mock_hunter",
        latency_secs:     0.4,
        titles:           &[None],
        emails:           &[Some("person@company.com"), Some("alt@company.io"), None],
        title_confidence: 0.0,
        email_confidence: 0.75,
    },
    MockSource {
        name:             "mock_clearbit",
        latency_secs:     0.5,
        titles:           &[Some("Chief Marketing Officer"), Some("VP Marketing"), None],
        emails:           &[Some("person@company.com"), None],
        title_confidence: 0.80,
        email_confidence: 0.70,
    },
    MockSource {
        name:             "mock_linkedin_scraper",
        latency_secs:     0.8,
        titles:           &[Some("Head of Growth"), Some("Senior Marketing Manager"), Some("Growth Lead"), None],
        emails:           &[None],
        title_confidence: 0.85,
        email_confidence: 0.0,
    },
];

// Pool of titles the agent uses for its deep-research pass.
static AGENT_TITLE_POOL: &[&str] = &[
    "Chief Revenue Officer",
    "VP of Demand Generation",
    "Head of Growth Marketing",
    "Senior Director of Sales",
];

// ─────────────────────────────────────────────────────────────────────────────
// Stage 1 — shallow_enrichment
// ─────────────────────────────────────────────────────────────────────────────

/// Single fast source (mock_apollo), records whatever it returns.
///
/// `lead_id: String` — we take an owned String rather than a reference because
/// this value needs to outlive the `spawn_blocking` closure.  A reference
/// can't cross the async/thread boundary, but an owned String can.
pub async fn shallow_enrichment(db: Db, lead_id: String) -> Result<()> {
    let key = format!("{lead_id}-shallow-v1");

    // Create the run record synchronously (spawn_blocking moves sync work off the async thread).
    let run_id = {
        let db2 = Arc::clone(&db);
        let key2 = key.clone();
        let lid2 = lead_id.clone();
        spawn_blocking(move || {
            let conn = db2.lock().unwrap();
            repository::run_create(&conn, &lid2, "shallow", &key2)
                .map(|r| r.id)
        }).await??
    };

    // Simulate API latency
    sleep(Duration::from_secs_f64(0.4)).await;

    // Random mock results
    let mut rng = rand::thread_rng();
    let title: Option<&str> = *[Some("VP Growth"), Some("Head of Marketing"), None]
        .choose(&mut rng).unwrap();
    let email: Option<&str> = *[Some("person@company.com"), None]
        .choose(&mut rng).unwrap();

    // Write observations + mark run complete
    let db2 = Arc::clone(&db);
    let lid = lead_id.clone();
    let rid = run_id.clone();
    spawn_blocking(move || -> Result<()> {
        let conn = db2.lock().unwrap();
        if let Some(t) = title {
            repository::observation_add(&conn, &lid, "title", t, "mock_apollo", 0.6, &rid)?;
        }
        if let Some(e) = email {
            repository::observation_add(&conn, &lid, "email", e, "mock_apollo", 0.55, &rid)?;
        }
        repository::run_complete(&conn, &rid, true)?;
        Ok(())
    }).await??;

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 2 — waterfall_enrichment
// ─────────────────────────────────────────────────────────────────────────────

/// Tries each mock source in priority order, stops when all fields are filled.
pub async fn waterfall_enrichment(db: Db, lead_id: String) -> Result<()> {
    let target_fields = ["title", "email"];
    let mut rng = rand::thread_rng();

    for source in MOCK_SOURCES {
        // Check which fields still need filling (sync DB read)
        let db2 = Arc::clone(&db);
        let lid = lead_id.clone();
        let filled = spawn_blocking(move || {
            let conn = db2.lock().unwrap();
            repository::observed_fields(&conn, &lid)
        }).await??;

        // Collect what we still need as a Vec of &str
        let still_needed: Vec<&str> = target_fields
            .iter()
            .filter(|f| !filled.contains(**f))
            .copied()
            .collect();

        // All fields filled — stop trying sources
        if still_needed.is_empty() {
            break;
        }

        // Idempotency check: skip if this source already ran for this lead
        let key = format!("{lead_id}-waterfall-{}-v1", source.name);
        let db2 = Arc::clone(&db);
        let key2 = key.clone();
        let already_ran = spawn_blocking(move || {
            let conn = db2.lock().unwrap();
            repository::run_get_by_key(&conn, &key2).map(|r| r.is_some())
        }).await??;

        if already_ran {
            continue;
        }

        // Create run record
        let run_id = {
            let db2 = Arc::clone(&db);
            let lid = lead_id.clone();
            let key2 = key.clone();
            spawn_blocking(move || {
                let conn = db2.lock().unwrap();
                repository::run_create(&conn, &lid, "waterfall", &key2).map(|r| r.id)
            }).await??
        };

        // Simulate vendor API latency
        sleep(Duration::from_secs_f64(source.latency_secs)).await;

        // Pick random values from this source's pools
        let maybe_title: Option<&&str> = if still_needed.contains(&"title") && source.title_confidence > 0.0 {
            source.titles.choose(&mut rng).and_then(|o| o.as_ref())
        } else {
            None
        };
        let maybe_email: Option<&&str> = if still_needed.contains(&"email") && source.email_confidence > 0.0 {
            source.emails.choose(&mut rng).and_then(|o| o.as_ref())
        } else {
            None
        };

        // Write observations
        let name       = source.name;
        let t_conf     = source.title_confidence;
        let e_conf     = source.email_confidence;
        let added_any  = maybe_title.is_some() || maybe_email.is_some();

        let db2  = Arc::clone(&db);
        let lid  = lead_id.clone();
        let rid  = run_id.clone();
        // Converts &&str → String so we can move into the spawn_blocking closure
        let title_val = maybe_title.map(|t| t.to_string());
        let email_val = maybe_email.map(|e| e.to_string());

        spawn_blocking(move || -> Result<()> {
            let conn = db2.lock().unwrap();
            if let Some(ref t) = title_val {
                repository::observation_add(&conn, &lid, "title", t, name, t_conf, &rid)?;
            }
            if let Some(ref e) = email_val {
                repository::observation_add(&conn, &lid, "email", e, name, e_conf, &rid)?;
            }
            repository::run_complete(&conn, &rid, added_any)?;
            Ok(())
        }).await??;

        // Diagnostic print
        println!("[waterfall] {} → needed={:?} added={}", name, still_needed, added_any);
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage 3 — agent_enrichment
// ─────────────────────────────────────────────────────────────────────────────

/// Expensive deep-research pass that always fills remaining fields at high confidence.
pub async fn agent_enrichment(db: Db, lead_id: String, lead_name: String, lead_company: String) -> Result<()> {
    let key = format!("{lead_id}-agent-v1");

    // Idempotency check
    let db2 = Arc::clone(&db);
    let key2 = key.clone();
    let already_ran = spawn_blocking(move || {
        let conn = db2.lock().unwrap();
        repository::run_get_by_key(&conn, &key2).map(|r| r.is_some())
    }).await??;

    if already_ran {
        return Ok(());
    }

    let run_id = {
        let db2 = Arc::clone(&db);
        let lid = lead_id.clone();
        let key2 = key.clone();
        spawn_blocking(move || {
            let conn = db2.lock().unwrap();
            repository::run_create(&conn, &lid, "agent", &key2).map(|r| r.id)
        }).await??
    };

    // Agent takes longer — simulates web search + LinkedIn parsing
    sleep(Duration::from_secs_f64(1.2)).await;

    // Check which fields are still missing
    let db2 = Arc::clone(&db);
    let lid = lead_id.clone();
    let filled = spawn_blocking(move || {
        let conn = db2.lock().unwrap();
        repository::observed_fields(&conn, &lid)
    }).await??;

    let need_title = !filled.contains("title");
    let need_email = !filled.contains("email");

    let mut rng = rand::thread_rng();
    let title_val = if need_title {
        Some(AGENT_TITLE_POOL.choose(&mut rng).unwrap().to_string())
    } else { None };

    let email_val = if need_email {
        Some(format!(
            "{}@{}.com",
            lead_name.to_lowercase().replace(' ', "."),
            lead_company.to_lowercase().replace(' ', "")
        ))
    } else { None };

    let db2 = Arc::clone(&db);
    let lid = lead_id.clone();
    let rid = run_id.clone();
    spawn_blocking(move || -> Result<()> {
        let conn = db2.lock().unwrap();
        if let Some(ref t) = title_val {
            repository::observation_add(&conn, &lid, "title", t, "mock_agent", 0.90, &rid)?;
        }
        if let Some(ref e) = email_val {
            repository::observation_add(&conn, &lid, "email", e, "mock_agent", 0.70, &rid)?;
        }
        repository::run_complete(&conn, &rid, true)?;
        Ok(())
    }).await??;

    Ok(())
}
