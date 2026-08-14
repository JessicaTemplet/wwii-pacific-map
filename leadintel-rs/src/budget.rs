//! Budget guard — checks whether a lead can afford a stage's cost.
//!
//! This is intentionally tiny — one pure function, easy to test.

use crate::models::Lead;

/// Return true if spending `cost_cents` would not exceed the lead's budget.
pub fn can_spend(lead: &Lead, cost_cents: i64) -> bool {
    lead.spent_cents + cost_cents <= lead.budget_cents
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lead_with_budget(budget: i64, spent: i64) -> Lead {
        let mut l = Lead::new("Test", "Corp");
        l.budget_cents = budget;
        l.spent_cents  = spent;
        l
    }

    #[test]
    fn within_budget() {
        let lead = lead_with_budget(25, 10);
        assert!(can_spend(&lead, 10));  // 10 + 10 = 20 <= 25
    }

    #[test]
    fn exactly_at_budget() {
        let lead = lead_with_budget(25, 17);
        assert!(can_spend(&lead, 8));   // 17 + 8 = 25 <= 25
    }

    #[test]
    fn over_budget() {
        let lead = lead_with_budget(25, 20);
        assert!(!can_spend(&lead, 8)); // 20 + 8 = 28 > 25
    }
}
