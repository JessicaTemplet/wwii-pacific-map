//! Job producer — pushes jobs onto the Redis queue.
//!
//! The producer stores job metadata in a Redis hash (`job:<uuid>`) and pushes
//! the UUID onto a Redis list (`default_queue`).  The consumer pops from the
//! list and looks up the hash for the actual job details.
//!
//! Redis data structures used:
//!   - HSET  job:<id> func "process_lead" args "[...]" status "queued" ...
//!   - LPUSH default_queue <id>
//!
//! Uses a `ConnectionManager`, which automatically reconnects on failure.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde_json::Value;

/// Pushes jobs to Redis.
pub struct JobProducer {
    /// Async Redis connection — automatically reconnects on failure.
    conn: ConnectionManager,
}

impl JobProducer {
    /// Connect to Redis and return a producer.
    pub async fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let conn = ConnectionManager::new(client).await?;
        Ok(JobProducer { conn })
    }

    /// Push one job onto the default queue.
    ///
    /// `payload` is a JSON value — serialized to a string for storage.
    pub async fn enqueue(&self, func_name: &str, payload: &Value) -> Result<String> {
        let job_id   = uuid::Uuid::new_v4().to_string();
        let job_key  = format!("job:{job_id}");
        let args_str = serde_json::to_string(payload)?;
        let now      = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        // HSET job:<id> field1 val1 field2 val2 ...
        let mut conn = self.conn.clone();
        redis::cmd("HSET")
            .arg(&job_key)
            .arg("id")          .arg(&job_id)
            .arg("func")        .arg(func_name)
            .arg("args")        .arg(&args_str)
            .arg("retries_left").arg(3_i32)
            .arg("max_retries") .arg(3_i32)
            .arg("status")      .arg("queued")
            .arg("created_at")  .arg(now)
            .exec_async(&mut conn)
            .await?;

        // LPUSH default_queue <job_id>
        // `lpush` pushes to the left (head) of the list.
        // The consumer uses BRPOPLPUSH which pops from the right (tail),
        // giving FIFO order.
        conn.lpush::<_, _, ()>("default_queue", &job_id).await?;

        println!("[+] Enqueued: {func_name} ({job_id})");
        Ok(job_id)
    }
}
