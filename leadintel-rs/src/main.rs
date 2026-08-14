//! LeadIntel CLI — entry point for all commands.
//!
//! Three commands:
//!   initdb           — create SQLite tables (idempotent)
//!   ingest <path>    — read a CSV of leads and insert them into the DB
//!   run              — start the enrichment pipeline (consumer + scheduler + lead enqueue)

use clap::{Parser, Subcommand};
use anyhow::Result;

// ─────────────────────────────────────────────────────────────────────────────
// Module declarations
// ─────────────────────────────────────────────────────────────────────────────

mod budget;
mod config;
mod db;
mod doubt;
mod error;
mod models;
mod pipeline;
mod repository;
mod signals;
mod stages;
mod tasks;
mod worker;

// job contains the producer / consumer / scheduler sub-modules.
mod job;

// ─────────────────────────────────────────────────────────────────────────────
// CLI definition
// ─────────────────────────────────────────────────────────────────────────────

/// LeadIntel enrichment pipeline.
#[derive(Parser)]
#[command(name = "leadintel", about = "AI lead enrichment pipeline")]
struct Cli {
    /// Path to the SQLite database file.
    /// Defaults to "leadintel.db" in the current directory.
    #[arg(long, default_value = "leadintel.db")]
    db: String,

    /// Redis connection URL.
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    redis: String,

    /// Path to pipeline.yaml.
    #[arg(long, default_value = "pipeline.yaml")]
    pipeline: String,

    #[command(subcommand)]
    command: Commands,
}

/// The three available subcommands.
#[derive(Subcommand)]
enum Commands {
    /// Initialize the database schema (idempotent — safe to run multiple times).
    Initdb,

    /// Ingest leads from a CSV file (columns: name, company).
    Ingest {
        /// Path to the CSV file.
        path: String,
    },

    /// Run the enrichment pipeline until all leads are DONE.
    Run,
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Open (or create) the SQLite database.
    // All commands need the DB handle.
    let db = db::open(&cli.db)?;

    match cli.command {
        Commands::Initdb => {
            cmd_initdb(&db)?;
        }
        Commands::Ingest { path } => {
            cmd_ingest(&db, &path)?;
        }
        Commands::Run => {
            cmd_run(db, cli.redis, cli.pipeline).await?;
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Command implementations
// ─────────────────────────────────────────────────────────────────────────────

/// Create all database tables.  Safe to call multiple times.
fn cmd_initdb(db: &db::Db) -> Result<()> {
    db::init_schema(db)?;
    println!("[OK] Database schema initialized.");
    Ok(())
}

/// Read a CSV and insert each row as a new Lead.
///
/// Expected CSV columns: name, company
/// Any extra columns are silently ignored.
fn cmd_ingest(db: &db::Db, csv_path: &str) -> Result<()> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(csv_path)?;

    let conn = db.lock().unwrap();
    let mut count = 0_usize;

    for result in reader.records() {
        let record = result?;

        // We access columns by position (0 = name, 1 = company) rather than
        // by header name, since `reader.headers()` would need a second
        // borrow of `reader` while it's already borrowed mutably above.
        let name    = record.get(0).unwrap_or("").trim().to_owned();
        let company = record.get(1).unwrap_or("").trim().to_owned();

        if name.is_empty() {
            continue; // skip blank rows
        }

        let lead = models::Lead::new(name.clone(), company.clone());
        repository::lead_create(&conn, &lead)?;
        println!("[+] Ingested lead: {} @ {} ({})", name, company, lead.id);
        count += 1;
    }

    println!("[OK] Ingested {count} lead(s) from {csv_path}");
    Ok(())
}

/// Start the enrichment pipeline.
///
/// This is async because it spawns tokio tasks (consumer + scheduler) and
/// then awaits the completion poll loop.
async fn cmd_run(db: db::Db, redis_url: String, pipeline_path: String) -> Result<()> {
    // Initialize schema in case the user forgot to run initdb.
    // Idempotent — no harm if tables already exist.
    db::init_schema(&db)?;

    pipeline::run_pipeline(db, redis_url, pipeline_path).await?;
    Ok(())
}
