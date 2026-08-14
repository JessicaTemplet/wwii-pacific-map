//! Job consumer — pulls jobs from Redis and executes them.
//!
//! Redis commands used:
//!   BRPOPLPUSH  src dst timeout  — atomically pop from src + push to dst.
//!                                   Blocks up to `timeout` seconds waiting
//!                                   for a job.  This is the reliable queue
//!                                   pattern: jobs stay in the processing list
//!                                   until we explicitly remove them, so a
//!                                   crash doesn't lose jobs.
//!   HGETALL     job:<id>         — fetch all fields of the job hash
//!   HSET        job:<id> status  — update status
//!   LREM        queue 1 <id>     — remove job from processing list on success
//!   ZADD        retry_set <score> <id> — schedule a retry

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;

use crate::{
    config::PipelineStage,
    db::Db,
    job::producer::JobProducer,
    worker::{process_lead, LeadPayload},
};

const QUEUE_NAME: &str = "default_queue";
const PROCESSING_QUEUE: &str = "default_queue:processing";
const RETRY_SET: &str = "retry_set";

/// Run the consumer loop forever, processing jobs as they arrive.
pub async fn run(
    db:       Db,
    pipeline: Arc<Vec<PipelineStage>>,
    redis_url: &str,
) -> Result<()> {
    let client = redis::Client::open(redis_url)?;
    let mut conn = ConnectionManager::new(client.clone()).await?;

    // We also need a producer to re-enqueue leads that need more passes.
    let producer = Arc::new(JobProducer::new(redis_url).await?);

    println!("[*] Consumer started. Listening on {QUEUE_NAME}...");

    loop {
        // BRPOPLPUSH: atomically pop from queue → push to processing list.
        // The `5` is a timeout in seconds — if the queue is empty, this
        // returns None after 5s.
        let job_id: Option<String> = redis::cmd("BRPOPLPUSH")
            .arg(QUEUE_NAME)
            .arg(PROCESSING_QUEUE)
            .arg(5_u32)
            .query_async(&mut conn)
            .await?;

        let job_id = match job_id {
            Some(id) => id,
            None     => continue, // timeout — loop again
        };

        println!("[*] Picking up job: {job_id}");

        // Execute the job; handle failure with retries
        if let Err(e) = execute_job(
            &mut conn,
            Arc::clone(&db),
            Arc::clone(&pipeline),
            Arc::clone(&producer),
            &job_id,
        ).await {
            eprintln!("[!] Job {job_id} failed: {e}");
            handle_failure(&mut conn, &job_id).await?;
        }
    }
}

/// Execute one job.
async fn execute_job(
    conn:     &mut ConnectionManager,
    db:       Db,
    pipeline: Arc<Vec<PipelineStage>>,
    producer: Arc<JobProducer>,
    job_id:   &str,
) -> Result<()> {
    let job_key = format!("job:{job_id}");

    // Mark running
    conn.hset::<_, _, _, ()>(&job_key, "status", "running").await?;

    // Fetch job metadata from the hash.
    // HGETALL returns a flat [field, value, field, value, ...] list.
    let job_data: HashMap<String, String> = conn.hgetall(&job_key).await?;

    let func_name = job_data.get("func")
        .ok_or_else(|| anyhow::anyhow!("missing func field for job {job_id}"))?
        .clone();

    let args_str = job_data.get("args").map(String::as_str).unwrap_or("{}");

    // Parse payload — we only handle "process_lead" here
    match func_name.as_str() {
        "process_lead" => {
            let payload: LeadPayload = serde_json::from_str(args_str)?;

            // Build the enqueue closure that re-queues this lead if needed.
            //
            // worker::process_lead expects a SYNC `Fn(LeadPayload) -> Result<()>`,
            // but JobProducer::enqueue is async.  We bridge them with
            // `tokio::task::block_in_place`, which is safe to call from inside
            // a tokio multi-thread runtime (the default for `#[tokio::main]`).
            //
            // block_in_place temporarily moves the current thread off the
            // async scheduler so it can run blocking code — in this case, an
            // async future driven synchronously by `Handle::block_on`.
            //
            // Why not `Handle::block_on` directly?
            //   block_on panics if called on a thread that already owns an
            //   async executor context.  block_in_place exits that context
            //   first, then block_on works fine.
            let producer2 = Arc::clone(&producer);
            let handle = tokio::runtime::Handle::current();
            let enqueue_fn = move |p: LeadPayload| -> Result<()> {
                let producer3 = Arc::clone(&producer2);
                tokio::task::block_in_place(|| {
                    handle.block_on(async move {
                        producer3
                            .enqueue("process_lead", &serde_json::json!({"lead_id": p.lead_id}))
                            .await
                            .map(|_| ())
                    })
                })?;
                Ok(())
            };

            process_lead(db, pipeline, payload, enqueue_fn).await?;
        }
        other => {
            anyhow::bail!("no handler registered for task '{other}'");
        }
    }

    // Mark completed and remove from processing list
    conn.hset::<_, _, _, ()>(&job_key, "status", "completed").await?;
    conn.lrem::<_, _, ()>(PROCESSING_QUEUE, 1, job_id).await?;
    println!("[OK] Job {job_id} completed.");

    Ok(())
}

/// Handle a failed job — schedule retry or send to dead-letter queue.
///
/// Retry delay uses exponential backoff: 2^attempt seconds.
async fn handle_failure(conn: &mut ConnectionManager, job_id: &str) -> Result<()> {
    let job_key = format!("job:{job_id}");

    // Fetch retry counters from the hash
    let retries: i32 = conn.hget(&job_key, "retries_left").await.unwrap_or(0);
    let max_ret: i32 = conn.hget(&job_key, "max_retries").await.unwrap_or(3);

    if retries > 0 {
        let attempt = max_ret - retries;
        // Exponential backoff: 2^attempt seconds (1s, 2s, 4s, ...)
        let delay_secs = 2_i32.pow(attempt as u32);

        println!("[!] Scheduling retry #{} in {delay_secs}s", attempt + 1);

        conn.hset::<_, _, _, ()>(&job_key, "retries_left", retries - 1).await?;
        conn.hset::<_, _, _, ()>(&job_key, "status", "scheduled").await?;

        // Add to the retry sorted set with the future timestamp as the score.
        // The scheduler (scheduler.rs) polls this set and re-queues jobs
        // whose score (retry time) has passed.
        let retry_time = now_f64() + delay_secs as f64;
        conn.zadd::<_, _, _, ()>(RETRY_SET, job_id, retry_time).await?;

        println!("[->] Job {job_id} moved to retry set");
    } else {
        // No retries left — permanent failure
        println!("[FAIL] Job {job_id} failed permanently");
        conn.hset::<_, _, _, ()>(&job_key, "status", "failed").await?;
        conn.lpush::<_, _, ()>("dead_letter_queue", job_id).await?;
        conn.lrem::<_, _, ()>(PROCESSING_QUEUE, 1, job_id).await?;
    }

    Ok(())
}

fn now_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}
