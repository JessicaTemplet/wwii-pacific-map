//! Pipeline configuration — reads pipeline.yaml.
//!
//! Rust structs derive `Deserialize` so serde fills them from YAML directly —
//! typos in pipeline.yaml are caught at startup rather than at runtime
//! mid-pipeline.

use serde::Deserialize;
use anyhow::{Context, Result};

// ─────────────────────────────────────────────────────────────────────────────
// Structs that mirror the YAML shape
// ─────────────────────────────────────────────────────────────────────────────

/// One stage entry from pipeline.yaml.
#[derive(Debug, Clone, Deserialize)]
pub struct PipelineStage {
    /// Stage name: "shallow", "waterfall", or "agent"
    pub stage: String,

    /// Run this stage if the lead's current_doubt is above this threshold.
    pub doubt_threshold: f64,

    /// How many cents to charge the lead's budget when this stage runs.
    pub cost: i64,
}

/// Top-level wrapper that matches the YAML structure:
///     pipeline:
///       - stage: shallow
///         ...
#[derive(Debug, Deserialize)]
struct PipelineConfig {
    pipeline: Vec<PipelineStage>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Loader
// ─────────────────────────────────────────────────────────────────────────────

/// Load and parse pipeline.yaml from the given path.
///
/// Returns a Vec<PipelineStage> in the same order as the YAML.
/// The caller (worker.rs) iterates this list to decide which stage to run.
pub fn load_pipeline_config(path: &str) -> Result<Vec<PipelineStage>> {
    let yaml_text = std::fs::read_to_string(path)
        .with_context(|| format!("could not read pipeline config from {path}"))?;

    let cfg: PipelineConfig = serde_yaml::from_str(&yaml_text)
        .with_context(|| "pipeline.yaml is not valid YAML or missing required fields")?;

    Ok(cfg.pipeline)
}
