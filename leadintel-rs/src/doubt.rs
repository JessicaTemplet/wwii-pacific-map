//! Doubt score computation.
//!
//! The doubt score is a number from 0.0 to 1.0 that represents how uncertain
//! we are about a lead's information:
//!
//!   1.0 — no observations at all
//!   0.7 — has title OR email, but not both
//!   0.3 — has title AND email, no conflict
//!   0.0 — would require zero missing fields AND zero conflicts (theoretical min)
//!
//! This function is pure logic — it just looks at a list of observations and
//! does arithmetic.  No async, no side effects.  Easy to test in isolation.

use crate::models::Observation;

/// Compute the doubt score for a lead given its current observations.
///
/// Here we accept the already-fetched observations so the function stays pure
/// (no DB access) — the caller fetches them before calling this.
/// This makes it trivial to unit-test without a real database.
pub fn compute_doubt(observations: &[Observation]) -> f64 {
    // No observations at all → maximum uncertainty
    if observations.is_empty() {
        return 1.0;
    }

    let mut doubt = 0.0_f64;

    // Collect all title values we've seen.
    let titles: Vec<&str> = observations
        .iter()
        .filter(|o| o.field_name == "title")
        .map(|o| o.value.as_str())
        .collect();

    // Collect all email values.
    let emails: Vec<&str> = observations
        .iter()
        .filter(|o| o.field_name == "email")
        .map(|o| o.value.as_str())
        .collect();

    // Missing title adds 0.4 to doubt.
    if titles.is_empty() {
        doubt += 0.4;
    }

    // Conflicting titles (more than one distinct value) adds 0.3.
    let unique_titles: std::collections::HashSet<&&str> = titles.iter().collect();
    if unique_titles.len() > 1 {
        doubt += 0.3;
    }

    // Missing email adds 0.3.
    if emails.is_empty() {
        doubt += 0.3;
    }

    // Clamp to [0.0, 1.0].
    doubt.min(1.0)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Observation;

    fn obs(field: &str, value: &str) -> Observation {
        Observation::new("lead-1", field, value, "test", 1.0, "run-1")
    }

    #[test]
    fn no_observations_returns_1() {
        assert_eq!(compute_doubt(&[]), 1.0);
    }

    #[test]
    fn both_fields_present_no_conflict() {
        let observations = vec![obs("title", "VP Growth"), obs("email", "a@b.com")];
        assert_eq!(compute_doubt(&observations), 0.0);
    }

    #[test]
    fn missing_email_adds_0_3() {
        let observations = vec![obs("title", "VP Growth")];
        // Missing email: +0.3
        assert!((compute_doubt(&observations) - 0.3).abs() < 1e-9);
    }

    #[test]
    fn conflicting_titles_add_0_3() {
        let observations = vec![
            obs("title", "VP Growth"),
            obs("title", "Head of Marketing"), // conflict
            obs("email", "a@b.com"),
        ];
        // conflict: +0.3
        assert!((compute_doubt(&observations) - 0.3).abs() < 1e-9);
    }

    #[test]
    fn capped_at_1() {
        // Missing title (+0.4), conflict (+0.3), missing email (+0.3) = 1.0
        let observations = vec![
            obs("title", "A"),
            obs("title", "B"), // conflict
        ];
        // title present (no +0.4), conflict (+0.3), missing email (+0.3) = 0.6
        assert!((compute_doubt(&observations) - 0.6).abs() < 1e-9);
    }
}
