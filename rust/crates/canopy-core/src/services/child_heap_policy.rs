//! Observation-only child heap policy port of
//! `packages/acp-bridge/src/child-heap-policy.ts`.
//!
//! This policy reports a fixed partition and counts over-capacity admissions;
//! it never applies a V8 heap flag or refuses a child. A future enforcement
//! path needs separate evidence that the modeled ceiling fits real workloads.

use serde::{Deserialize, Serialize};

use super::daemon_memory_budget::{DaemonMemoryBudget, MIN_CHILD_HEAP_MB};

/// Mirrors `MAX_DAEMON_WORKSPACES` in `channel-control-timeouts.ts`.
pub const MAX_DAEMON_WORKSPACES: u64 = 25;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ChildHeapMode {
    Off,
    Observe,
}

impl ChildHeapMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Observe => "observe",
        }
    }
}

/// Snapshot of the modelled per-child heap partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildHeapPolicySnapshot {
    pub mode: ChildHeapMode,
    pub child_pool_mb: u64,
    pub min_child_heap_mb: u64,
    /// `None` under `off`; `Some(0)` when an enabled policy cannot model one
    /// child within the documented floor.
    pub max_concurrent_children: Option<u64>,
    /// `None` when disabled or when the available pool/cap cannot model the
    /// documented floor. A zero V8 heap flag would mean V8's default heap.
    pub per_child_ceiling_mb: Option<u64>,
    /// Number of decisions over the modeled concurrent-child limit.
    pub refusals: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildHeapDecision {
    pub refuse: bool,
}

/// Observation-only child heap capacity policy.
#[derive(Clone, Debug)]
pub struct ChildHeapPolicy {
    budget: DaemonMemoryBudget,
    mode: ChildHeapMode,
    max_concurrent_children: u64,
    per_child_ceiling_mb: Option<u64>,
    refusals: u64,
}

impl ChildHeapPolicy {
    /// `concurrent_children` is the already-reserved child count, including
    /// the child being considered, matching the source's `decide` contract.
    pub fn decide(&mut self, concurrent_children: u64) -> ChildHeapDecision {
        if self.mode == ChildHeapMode::Off {
            return ChildHeapDecision { refuse: false };
        }
        let refuse = concurrent_children > self.max_concurrent_children;
        if refuse {
            self.refusals = self.refusals.saturating_add(1);
        }
        ChildHeapDecision { refuse }
    }

    pub fn snapshot(&self) -> ChildHeapPolicySnapshot {
        let modeled = self.mode != ChildHeapMode::Off;
        ChildHeapPolicySnapshot {
            mode: self.mode,
            child_pool_mb: self.budget.child_pool_mb,
            min_child_heap_mb: MIN_CHILD_HEAP_MB,
            max_concurrent_children: modeled.then_some(self.max_concurrent_children),
            per_child_ceiling_mb: modeled.then_some(self.per_child_ceiling_mb).flatten(),
            refusals: self.refusals,
        }
    }
}

pub fn create_child_heap_policy(
    budget: DaemonMemoryBudget,
    mode: ChildHeapMode,
) -> ChildHeapPolicy {
    // Keep a fixed partition so the sum of all admitted child ceilings cannot
    // exceed the pool. Dividing by the instantaneous child count at each spawn
    // would create cumulative grants that V8 cannot later lower.
    let admissible = (budget.child_pool_mb / MIN_CHILD_HEAP_MB).min(MAX_DAEMON_WORKSPACES);
    let raw_ceiling_mb = budget
        .child_pool_mb
        .checked_div(admissible)
        .map(|ceiling| ceiling.min(budget.legacy_child_ceiling_mb));
    let modelable = raw_ceiling_mb.is_some_and(|ceiling| ceiling >= MIN_CHILD_HEAP_MB);
    let max_concurrent_children = if modelable { admissible } else { 0 };
    let per_child_ceiling_mb = if modelable { raw_ceiling_mb } else { None };

    ChildHeapPolicy {
        budget,
        mode,
        max_concurrent_children,
        per_child_ceiling_mb,
        refusals: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::daemon_memory_budget::{
        AvailableMemorySource, MemoryBudgetInput, resolve_daemon_memory_budget,
    };

    fn budget(available_memory_mb: u64, budget_mb: Option<f64>) -> DaemonMemoryBudget {
        resolve_daemon_memory_budget(MemoryBudgetInput {
            budget_mb,
            available_memory_mb,
            available_memory_source: AvailableMemorySource::Host,
        })
        .unwrap()
    }

    #[test]
    fn modeled_partition_never_exceeds_pool() {
        for available in [2_048, 8_192, 32_768, 262_144] {
            let b = budget(available, None);
            let snapshot = create_child_heap_policy(b, ChildHeapMode::Observe).snapshot();
            let children = snapshot.max_concurrent_children.unwrap();
            let ceiling = snapshot.per_child_ceiling_mb.unwrap();
            assert!(children > 0);
            assert!(ceiling >= MIN_CHILD_HEAP_MB);
            assert!(children * ceiling <= b.child_pool_mb);
        }
    }

    #[test]
    fn pool_below_floor_admits_no_child_and_has_no_ceiling() {
        let empty = budget(512, None);
        assert_eq!(empty.child_pool_mb, 0);
        let snapshot = create_child_heap_policy(empty, ChildHeapMode::Observe).snapshot();
        assert_eq!(snapshot.max_concurrent_children, Some(0));
        assert_eq!(snapshot.per_child_ceiling_mb, None);

        let small = budget(1_024, None);
        assert!(small.child_pool_mb < MIN_CHILD_HEAP_MB);
        let snapshot = create_child_heap_policy(small, ChildHeapMode::Observe).snapshot();
        assert_eq!(snapshot.max_concurrent_children, Some(0));
        assert_eq!(snapshot.per_child_ceiling_mb, None);
    }

    #[test]
    fn refuses_model_when_legacy_cap_is_under_documented_floor() {
        for available in [768, 900, 1_000, 1_023] {
            let b = budget(available, Some(1_024.0));
            assert!(b.child_pool_mb >= MIN_CHILD_HEAP_MB);
            assert!(b.legacy_child_ceiling_mb < MIN_CHILD_HEAP_MB);
            let snapshot = create_child_heap_policy(b, ChildHeapMode::Observe).snapshot();
            assert_eq!(snapshot.per_child_ceiling_mb, None);
            assert_eq!(snapshot.max_concurrent_children, Some(0));
            assert_eq!(snapshot.min_child_heap_mb, MIN_CHILD_HEAP_MB);
        }
    }

    #[test]
    fn boundary_and_reference_capacity_figures_match_typescript() {
        let boundary =
            create_child_heap_policy(budget(1_024, Some(1_024.0)), ChildHeapMode::Observe)
                .snapshot();
        assert_eq!(boundary.max_concurrent_children, Some(1));
        assert_eq!(boundary.per_child_ceiling_mb, Some(512));

        let eight_gb =
            create_child_heap_policy(budget(8_192, None), ChildHeapMode::Observe).snapshot();
        assert_eq!(eight_gb.max_concurrent_children, Some(7));
        assert_eq!(eight_gb.per_child_ceiling_mb, Some(526));

        let thirty_two_gb =
            create_child_heap_policy(budget(32_768, None), ChildHeapMode::Observe).snapshot();
        assert_eq!(thirty_two_gb.max_concurrent_children, Some(25));
        assert_eq!(thirty_two_gb.per_child_ceiling_mb, Some(614));
    }

    #[test]
    fn counts_over_limit_decisions_but_not_at_the_limit() {
        let mut policy = create_child_heap_policy(budget(8_192, None), ChildHeapMode::Observe);
        let limit = policy.snapshot().max_concurrent_children.unwrap();
        assert!(!policy.decide(1).refuse);
        assert!(!policy.decide(limit).refuse);
        assert_eq!(policy.snapshot().refusals, 0);
        assert!(policy.decide(limit + 1).refuse);
        assert!(policy.decide(limit + 9).refuse);
        assert_eq!(policy.snapshot().refusals, 2);
    }

    #[test]
    fn off_mode_never_counts_and_publishes_null_partition() {
        let mut off = create_child_heap_policy(budget(8_192, None), ChildHeapMode::Off);
        assert!(!off.decide(9_999).refuse);
        let snapshot = off.snapshot();
        assert_eq!(snapshot.refusals, 0);
        assert_eq!(snapshot.max_concurrent_children, None);
        assert_eq!(snapshot.per_child_ceiling_mb, None);

        let observe =
            create_child_heap_policy(budget(8_192, None), ChildHeapMode::Observe).snapshot();
        assert_eq!(observe.max_concurrent_children, Some(7));
        assert_eq!(observe.per_child_ceiling_mb, Some(526));
    }

    #[test]
    fn snapshots_serialize_with_source_field_names_and_nulls() {
        let off = create_child_heap_policy(budget(8_192, None), ChildHeapMode::Off).snapshot();
        let json = serde_json::to_value(off).unwrap();
        assert_eq!(json["mode"], "off");
        assert_eq!(json["childPoolMb"], 3_687);
        assert_eq!(json["minChildHeapMb"], 512);
        assert!(json["maxConcurrentChildren"].is_null());
        assert!(json["perChildCeilingMb"].is_null());
        assert_eq!(json["refusals"], 0);
    }
}
