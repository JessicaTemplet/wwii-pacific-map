//! Stage dispatcher — maps a stage name to its task function.
//!
//! Uses `match`, which is exhaustive — if a new stage is added, the compiler
//! flags this function until it's handled.

use anyhow::{bail, Result};

use crate::{db::Db, models::Lead, tasks};

/// Dispatch to the appropriate enrichment task for a given stage name.
pub async fn run_stage(stage_name: &str, db: Db, lead: &Lead) -> Result<()> {
    match stage_name {
        "shallow" => {
            tasks::shallow_enrichment(db, lead.id.clone()).await
        }
        "waterfall" => {
            tasks::waterfall_enrichment(db, lead.id.clone()).await
        }
        "agent" => {
            tasks::agent_enrichment(
                db,
                lead.id.clone(),
                lead.name.clone(),
                lead.company.clone(),
            ).await
        }
        other => bail!("unknown stage: {other:?}"),
    }
}
