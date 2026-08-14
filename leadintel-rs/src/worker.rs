//! Core pipeline logic — processes one lead through one pipeline stage.
//!
//! This is the heart of the system.  The job consumer (job/consumer.rs) pulls
//! a job from Redis and calls `process_lead()` for each one.
//!
//! Flow:
//!   1. Load lead from DB
//!   2. Compute current doubt score
//!   3. Walk pipeline stages in order; run the first stage whose threshold
//!      is exceeded AND the budget allows
//!   4. Recompute doubt
//!   5. Generate signals
//!   6. If done, mark lead DONE; otherwise re-enqueue for another pass

use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;

use crate::{
    budget,
    config::PipelineStage,
    db::Db,
    doubt,
    repository,
    signals,
    stages,
};

// ─────────────────────────────────────────────────────────────────────────────
// Job payload
// ─────────────────────────────────────────────────────────────────────────────

/// The data that travels with each job in the Redis queue.
///
/// Derive `Serialize` / `Deserialize` so serde_json can convert this to/from
/// the JSON string stored in the Redis hash.
#[derive(Debug, Serialize, Deserialize)]
pub struct LeadPayload {
    pub lead_id: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// process_lead
// ─────────────────────────────────────────────────────────────────────────────

/// Process one lead through one pipeline stage.
///
/// Why `pipeline: Arc<Vec<PipelineStage>>`?
///   The pipeline config is loaded once at startup and shared across all
///   concurrent `process_lead` calls.  `Arc` gives shared read-only access
///   without copying the Vec each time.
pub async fn process_lead(
    db:              Db,
    pipeline:        Arc<Vec<PipelineStage>>,
    payload:         LeadPayload,
    enqueue_fn:      impl Fn(LeadPayload) -> Result<()>,
) -> Result<()> {
    let lead_id = payload.lead_id.clone();

    // ── Step 1: Load lead ────────────────────────────────────────────────────
    let db2 = Arc::clone(&db);
    let lid = lead_id.clone();
    let mut lead = spawn_blocking(move || {
        let conn = db2.lock().unwrap();
        repository::lead_get(&conn, &lid)
    })
    .await?
    .context("spawn_blocking join error")?
    .with_context(|| format!("lead not found: {lead_id}"))?;

    // ── Step 2: Compute current doubt ────────────────────────────────────────
    let db2 = Arc::clone(&db);
    let lid = lead.id.clone();
    let observations = spawn_blocking(move || {
        let conn = db2.lock().unwrap();
        repository::observations_for_lead(&conn, &lid)
    }).await??;

    lead.current_doubt = doubt::compute_doubt(&observations);

    // ── Step 3: Walk stages — run the first applicable one ───────────────────
    let mut executed = false;

    for stage in pipeline.iter() {
        if lead.current_doubt > stage.doubt_threshold {
            if !budget::can_spend(&lead, stage.cost) {
                // Over budget — stop the pipeline entirely for this lead
                break;
            }

            // Run the stage (may take a while — it awaits mock API latency)
            stages::run_stage(&stage.stage, Arc::clone(&db), &lead).await
                .with_context(|| format!("stage {} failed for lead {}", stage.stage, lead.id))?;

            lead.spent_cents += stage.cost;
            executed = true;

            // Only one stage per job execution
            break;
        }
    }

    // ── Step 4: Recompute doubt after the stage ran ──────────────────────────
    let db2 = Arc::clone(&db);
    let lid = lead.id.clone();
    let updated_obs = spawn_blocking(move || {
        let conn = db2.lock().unwrap();
        repository::observations_for_lead(&conn, &lid)
    }).await??;

    lead.current_doubt = doubt::compute_doubt(&updated_obs);

    // ── Step 5: Generate and persist signals ─────────────────────────────────
    let new_signals = signals::generate_signals(&lead.id, &updated_obs);
    if !new_signals.is_empty() {
        let db2 = Arc::clone(&db);
        let sigs = new_signals.clone();
        spawn_blocking(move || -> Result<()> {
            let conn = db2.lock().unwrap();
            for sig in &sigs {
                repository::signal_insert(&conn, sig)?;
            }
            Ok(())
        }).await??;
    }

    // ── Step 6: Decide next action ───────────────────────────────────────────
    if !executed || lead.current_doubt < 0.2 {
        lead.state = "DONE".to_owned();
        println!("[OK] Lead {} done (doubt={:.2})", lead.id, lead.current_doubt);
    } else {
        // Re-enqueue for another pass — the next execution will pick up
        // where we left off (higher stages in the pipeline)
        println!(
            "[->] Lead {} re-enqueued (doubt={:.2})",
            lead.id, lead.current_doubt
        );
        enqueue_fn(LeadPayload { lead_id: lead.id.clone() })?;
    }

    // ── Persist final lead state ─────────────────────────────────────────────
    let db2 = Arc::clone(&db);
    let lead_copy = lead.clone();
    spawn_blocking(move || {
        let conn = db2.lock().unwrap();
        repository::lead_update(&conn, &lead_copy)
    }).await??;

    Ok(())
}
