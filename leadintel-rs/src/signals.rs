//! Signal generation — derives insights from a lead's observations.
//!
//! A "signal" is a derived flag about data quality:
//!   - "title_conflict"  — two different sources gave different titles
//!   - "missing_title"   — no source found any title at all
//!
//! Like doubt.rs this is pure logic — no async, no DB queries here.
//! The caller fetches the observations and persists the resulting signals.

use crate::models::{Observation, Signal};

/// Generate signals from a lead's observations.
///
/// Returns a Vec of Signal structs to be inserted into the DB by the caller.
///
/// We return Vec<Signal> instead of writing to DB directly — this keeps the
/// function pure and testable.  The caller (worker.rs) writes them.
pub fn generate_signals(lead_id: &str, observations: &[Observation]) -> Vec<Signal> {
    // Collect all title values.
    let titles: Vec<&str> = observations
        .iter()
        .filter(|o| o.field_name == "title")
        .map(|o| o.value.as_str())
        .collect();

    let mut signals = Vec::new();

    // Conflicting titles: more than one distinct title seen
    let unique_titles: std::collections::HashSet<&&str> = titles.iter().collect();
    if unique_titles.len() > 1 {
        signals.push(Signal::new(
            lead_id,
            "title_conflict",
            0.8,
            "Multiple conflicting titles observed",
        ));
    }

    // No title at all
    if titles.is_empty() {
        signals.push(Signal::new(
            lead_id,
            "missing_title",
            0.9,
            "No title found across sources",
        ));
    }

    signals
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Observation;

    fn obs(field: &str, value: &str) -> Observation {
        Observation::new("lead-1", field, value, "test", 1.0, "run-1")
    }

    #[test]
    fn no_signals_when_consistent() {
        let observations = vec![obs("title", "VP"), obs("email", "a@b.com")];
        assert!(generate_signals("lead-1", &observations).is_empty());
    }

    #[test]
    fn title_conflict_signal() {
        let observations = vec![obs("title", "VP"), obs("title", "Director")];
        let sigs = generate_signals("lead-1", &observations);
        assert!(sigs.iter().any(|s| s.signal_type == "title_conflict"));
    }

    #[test]
    fn missing_title_signal() {
        let observations = vec![obs("email", "a@b.com")];
        let sigs = generate_signals("lead-1", &observations);
        assert!(sigs.iter().any(|s| s.signal_type == "missing_title"));
    }
}
