//! Pipeline entry point — enqueues all raw leads and starts the job system.
//!
//! Starts the job consumer as a tokio task before enqueuing, then waits for
//! all leads to reach the DONE state.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::task::spawn_blocking;
use tokio::time::sleep;

use crate::{config::PipelineStage, db::Db, job, repository};

/// Enqueue all RAW leads and block until every lead reaches DONE or times out.
///
/// `redis_url`     — e.g. "redis://127.0.0.1:6379"
/// `pipeline_path` — path to pipeline.yaml
pub async fn run_pipeline(db: Db, redis_url: String, pipeline_path: String) -> Result<()> {
    // Load pipeline config
    let pipeline = Arc::new(crate::config::load_pipeline_config(&pipeline_path)?);

    // Load all leads
    let db2 = Arc::clone(&db);
    let leads = spawn_blocking(move || {
        let conn = db2.lock().unwrap();
        repository::lead_list_all(&conn)
    }).await??;

    if leads.is_empty() {
        println!("[!] No leads found. Run `leadintel ingest <csv>` first.");
        return Ok(());
    }

    println!("[*] Enqueueing {} leads...", leads.len());

    // Start job consumer in the background
    let redis_url2 = redis_url.clone();
    let db_for_consumer = Arc::clone(&db);
    let pipeline_for_consumer = Arc::clone(&pipeline);
    tokio::spawn(async move {
        if let Err(e) = job::consumer::run(db_for_consumer, pipeline_for_consumer, &redis_url2).await {
            eprintln!("[!] Consumer error: {e}");
        }
    });

    // Start retry scheduler in the background
    let redis_url3 = redis_url.clone();
    tokio::spawn(async move {
        if let Err(e) = job::scheduler::run(&redis_url3).await {
            eprintln!("[!] Scheduler error: {e}");
        }
    });

    // Enqueue every lead
    let producer = job::producer::JobProducer::new(&redis_url).await?;
    for lead in &leads {
        producer.enqueue("process_lead", &serde_json::json!({"lead_id": lead.id})).await?;
    }

    // Poll until all leads are DONE
    loop {
        sleep(Duration::from_secs(2)).await;

        let db2 = Arc::clone(&db);
        let pending_count = spawn_blocking(move || {
            let conn = db2.lock().unwrap();
            repository::lead_list_all(&conn).map(|leads| {
                leads.iter().filter(|l| l.state != "DONE").count()
            })
        }).await??;

        if pending_count == 0 {
            println!("[OK] All leads processed.");
            break;
        }

        println!("[*] {pending_count} lead(s) still pending...");
    }

    Ok(())
}
