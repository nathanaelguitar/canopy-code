//! Daemon memory budget port of `packages/acp-bridge/src/daemon-memory-budget.ts`.
//!
//! Host/cgroup probing is kept outside this module. [`detect_available_memory_mb`]
//! accepts OS-reported byte counts as inputs, so callers can inject measurements
//! from a platform-specific API without putting unsafe or platform-specific
//! probing in this portable policy module. The budget arithmetic and source
//! selection match the TypeScript implementation.

use std::fmt;

use serde::{Deserialize, Serialize};

pub const LEGACY_CHILD_HEAP_FRACTION: f64 = 0.5;
pub const DEFAULT_MEMORY_BUDGET_FRACTION: f64 = 0.5;
pub const MIN_MEMORY_BUDGET_MB: u64 = 1_024;
pub const MAX_MEMORY_BUDGET_MB: u64 = 1_048_576;

pub const MIN_CHILD_HEAP_MB: u64 = 512;
pub const MAX_CHILD_HEAP_MB: u64 = 16_384;

pub const ROOT_RESERVE_FRACTION: f64 = 0.1;
pub const MIN_ROOT_RESERVE_MB: u64 = 256;
pub const MAX_ROOT_RESERVE_MB: u64 = 1_024;

pub const JOURNAL_GROWTH_POOL_FRACTION: f64 = 0.05;
pub const MAX_JOURNAL_GROWTH_POOL_MB: u64 = 1_024;

const BYTES_PER_MB: u64 = 1024 * 1024;
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryBudgetSource {
    Flag,
    Derived,
}

impl MemoryBudgetSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Flag => "flag",
            Self::Derived => "derived",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AvailableMemorySource {
    Constrained,
    Host,
}

impl AvailableMemorySource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Constrained => "constrained",
            Self::Host => "host",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AvailableMemory {
    pub memory_mb: u64,
    pub source: AvailableMemorySource,
}

/// Mirrors the source's host/cgroup decision from already measured values.
/// `constrained_memory_bytes` is the result of the platform/runtime's cgroup
/// query, or `None` if unavailable. Zero and values at/above host memory are
/// treated as unconstrained (including cgroup v1's large "unlimited" sentinel).
pub fn detect_available_memory_mb(
    total_memory_bytes: u64,
    constrained_memory_bytes: Option<u64>,
) -> AvailableMemory {
    if let Some(constrained) =
        constrained_memory_bytes.filter(|value| *value > 0 && *value < total_memory_bytes)
    {
        AvailableMemory {
            memory_mb: constrained / BYTES_PER_MB,
            source: AvailableMemorySource::Constrained,
        }
    } else {
        AvailableMemory {
            memory_mb: total_memory_bytes / BYTES_PER_MB,
            source: AvailableMemorySource::Host,
        }
    }
}

/// The daemon's resolved memory figures. This is descriptive; it does not
/// change child spawning or apply a heap ceiling.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DaemonMemoryBudget {
    /// The explicit flag value or half of available memory.
    pub configured_budget_mb: u64,
    /// Configured budget capped to resolved host/cgroup memory.
    pub effective_budget_mb: u64,
    pub budget_source: MemoryBudgetSource,
    /// Cgroup limit when lower than host memory, otherwise host total.
    pub available_memory_mb: u64,
    pub available_memory_source: AvailableMemorySource,
    pub root_reserve_mb: u64,
    /// `effective_budget_mb` minus the root reserve.
    pub child_pool_mb: u64,
    /// Current conservative child ceiling: half of available memory capped at
    /// `MAX_CHILD_HEAP_MB`. It is reported, not applied.
    pub legacy_child_ceiling_mb: u64,
    /// True when the effective budget is below the documented minimum.
    pub insufficient_memory: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct MemoryBudgetInput {
    /// A JavaScript-number-shaped value preserves range validation for callers
    /// that parse flags before converting them to integers.
    pub budget_mb: Option<f64>,
    /// Caller-provided platform measurement in whole MiB.
    pub available_memory_mb: u64,
    pub available_memory_source: AvailableMemorySource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidMemoryBudgetError {
    value: String,
}

impl fmt::Display for InvalidMemoryBudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Invalid memoryBudgetMb: {}. Must be a safe integer in [{MIN_MEMORY_BUDGET_MB}, {MAX_MEMORY_BUDGET_MB}].",
            self.value
        )
    }
}

impl std::error::Error for InvalidMemoryBudgetError {}

pub fn is_valid_memory_budget_mb(value: f64) -> bool {
    value.is_finite()
        && value.fract() == 0.0
        && value.abs() <= MAX_SAFE_INTEGER
        && value >= MIN_MEMORY_BUDGET_MB as f64
        && value <= MAX_MEMORY_BUDGET_MB as f64
}

pub fn normalize_memory_budget_mb(value: f64) -> Result<u64, InvalidMemoryBudgetError> {
    if !is_valid_memory_budget_mb(value) {
        return Err(InvalidMemoryBudgetError {
            value: js_number_string(value),
        });
    }
    // The accepted range is positive and far below u64's limit.
    Ok(value as u64)
}

pub fn memory_budget_range_error() -> String {
    format!(
        "qwen serve: --memory-budget-mb must be an integer in [{MIN_MEMORY_BUDGET_MB}, {MAX_MEMORY_BUDGET_MB}]."
    )
}

pub fn legacy_child_ceiling_mb(available_memory_mb: u64) -> u64 {
    // All accepted inputs are nonnegative, so integer division has the same
    // floor semantics as Math.floor(memoryMb * 0.5).
    (available_memory_mb / 2).min(MAX_CHILD_HEAP_MB)
}

fn clamp(value: u64, low: u64, high: u64) -> u64 {
    value.max(low).min(high)
}

pub fn resolve_daemon_memory_budget(
    input: MemoryBudgetInput,
) -> Result<DaemonMemoryBudget, InvalidMemoryBudgetError> {
    let configured_budget_mb = match input.budget_mb {
        Some(value) => normalize_memory_budget_mb(value)?,
        None => ((input.available_memory_mb as f64 * DEFAULT_MEMORY_BUDGET_FRACTION).floor()
            as u64)
            .min(MAX_MEMORY_BUDGET_MB),
    };
    // Do not report a denominator the host cannot back, and do not clamp a
    // below-minimum derived budget upward.
    let effective_budget_mb = configured_budget_mb.min(input.available_memory_mb);
    let fractional_reserve = (effective_budget_mb as f64 * ROOT_RESERVE_FRACTION).floor() as u64;
    let root_reserve_mb = clamp(fractional_reserve, MIN_ROOT_RESERVE_MB, MAX_ROOT_RESERVE_MB)
        .min(effective_budget_mb);

    Ok(DaemonMemoryBudget {
        configured_budget_mb,
        effective_budget_mb,
        budget_source: if input.budget_mb.is_some() {
            MemoryBudgetSource::Flag
        } else {
            MemoryBudgetSource::Derived
        },
        available_memory_mb: input.available_memory_mb,
        available_memory_source: input.available_memory_source,
        root_reserve_mb,
        child_pool_mb: effective_budget_mb - root_reserve_mb,
        legacy_child_ceiling_mb: legacy_child_ceiling_mb(input.available_memory_mb),
        insufficient_memory: effective_budget_mb < MIN_MEMORY_BUDGET_MB,
    })
}

/// A reported per-child share, never a spawning decision.
pub fn recommended_child_share_mb(budget: &DaemonMemoryBudget, children: u64) -> u64 {
    let share =
        (budget.child_pool_mb / children.max(1)).clamp(MIN_CHILD_HEAP_MB, MAX_CHILD_HEAP_MB);
    share.min(budget.legacy_child_ceiling_mb)
}

/// Aggregate MB ceiling from which live journals may grow.
pub fn journal_growth_pool_mb(budget: &DaemonMemoryBudget) -> u64 {
    if budget.insufficient_memory {
        return 0;
    }
    let fraction =
        (budget.effective_budget_mb as f64 * JOURNAL_GROWTH_POOL_FRACTION).floor() as u64;
    fraction
        .min(MAX_JOURNAL_GROWTH_POOL_MB)
        .min(budget.child_pool_mb)
}

/// Growth is disabled if either journal limit was explicitly configured.
pub fn serve_journal_growth_pool_mb(
    budget: &DaemonMemoryBudget,
    max_journal_events: Option<u64>,
    max_journal_bytes: Option<u64>,
) -> u64 {
    if max_journal_events.is_some() || max_journal_bytes.is_some() {
        return 0;
    }
    journal_growth_pool_mb(budget)
}

/// Derived daemon-wide journal growth capacity in bytes.
///
/// `None` means adaptive growth is disabled, either because an operator pinned
/// either journal cap or because the resolved memory budget provides no pool.
/// The byte unit is the one consumed by ACP journal policies; keeping the
/// conversion here avoids each runtime reconstructing the serve-layer
/// precedence or mixing MiB with serialized-byte limits.
pub fn serve_journal_growth_pool_bytes(
    budget: &DaemonMemoryBudget,
    max_journal_events: Option<u64>,
    max_journal_bytes: Option<u64>,
) -> Option<u64> {
    let pool_mb = serve_journal_growth_pool_mb(budget, max_journal_events, max_journal_bytes);
    (pool_mb > 0).then(|| pool_mb.saturating_mul(BYTES_PER_MB))
}

pub fn format_memory_budget_stderr(budget: &DaemonMemoryBudget) -> String {
    let mut message = format!(
        "qwen serve: memory budget {} MB ({}, {} MB available via {})",
        budget.effective_budget_mb,
        budget.budget_source.as_str(),
        budget.available_memory_mb,
        budget.available_memory_source.as_str()
    );
    if budget.effective_budget_mb < budget.configured_budget_mb {
        message.push_str(&format!(
            "; capped down from the configured {} MB",
            budget.configured_budget_mb
        ));
    }
    if budget.insufficient_memory {
        message.push_str(&format!(
            "; below the {MIN_MEMORY_BUDGET_MB} MB minimum budget"
        ));
        if budget.budget_source == MemoryBudgetSource::Derived {
            let min_host_mb =
                (MIN_MEMORY_BUDGET_MB as f64 / DEFAULT_MEMORY_BUDGET_FRACTION).ceil() as u64;
            message.push_str(&format!(
                " (a derived budget needs a host with at least ~{min_host_mb} MB; pass --memory-budget-mb to override — requires at least {MIN_MEMORY_BUDGET_MB} MB available)"
            ));
        }
    }
    message
}

fn js_number_string(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "Infinity".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_owned()
    } else if value == 0.0 {
        "0".to_owned()
    } else if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(available_memory_mb: u64) -> DaemonMemoryBudget {
        resolve_daemon_memory_budget(MemoryBudgetInput {
            budget_mb: None,
            available_memory_mb,
            available_memory_source: AvailableMemorySource::Host,
        })
        .unwrap()
    }

    #[test]
    fn derives_reference_budget_figures() {
        for (available, configured, effective, reserve, pool, legacy) in [
            (32_768, 16_384, 16_384, 1_024, 15_360, 16_384),
            (16_384, 8_192, 8_192, 819, 7_373, 8_192),
            (8_192, 4_096, 4_096, 409, 3_687, 4_096),
            (3_494, 1_747, 1_747, 256, 1_491, 1_747),
            (2_048, 1_024, 1_024, 256, 768, 1_024),
        ] {
            assert_eq!(
                budget(available),
                DaemonMemoryBudget {
                    configured_budget_mb: configured,
                    effective_budget_mb: effective,
                    budget_source: MemoryBudgetSource::Derived,
                    available_memory_mb: available,
                    available_memory_source: AvailableMemorySource::Host,
                    root_reserve_mb: reserve,
                    child_pool_mb: pool,
                    legacy_child_ceiling_mb: legacy,
                    insufficient_memory: effective < MIN_MEMORY_BUDGET_MB,
                }
            );
        }
    }

    #[test]
    fn injected_host_and_cgroup_measurements_match_source_policy() {
        let mb = 1024 * 1024;
        assert_eq!(
            detect_available_memory_mb(32_768 * mb, Some(4_096 * mb)),
            AvailableMemory {
                memory_mb: 4_096,
                source: AvailableMemorySource::Constrained,
            }
        );
        assert_eq!(
            detect_available_memory_mb(32_768 * mb, Some(u64::MAX)),
            AvailableMemory {
                memory_mb: 32_768,
                source: AvailableMemorySource::Host,
            }
        );
        assert_eq!(
            detect_available_memory_mb(8_192 * mb, Some(0)).source,
            AvailableMemorySource::Host
        );
        assert_eq!(
            detect_available_memory_mb(16_384 * mb, Some(16_384 * mb)).source,
            AvailableMemorySource::Host
        );
    }

    #[test]
    fn caps_explicit_budget_at_available_memory_and_large_derived_budget() {
        let explicit = resolve_daemon_memory_budget(MemoryBudgetInput {
            budget_mb: Some(MAX_MEMORY_BUDGET_MB as f64),
            available_memory_mb: 2_048,
            available_memory_source: AvailableMemorySource::Host,
        })
        .unwrap();
        assert_eq!(explicit.configured_budget_mb, MAX_MEMORY_BUDGET_MB);
        assert_eq!(explicit.effective_budget_mb, 2_048);
        assert_eq!(explicit.budget_source, MemoryBudgetSource::Flag);

        assert_eq!(
            budget(3 * 1024 * 1024).configured_budget_mb,
            MAX_MEMORY_BUDGET_MB
        );
    }

    #[test]
    fn insufficient_hosts_are_not_clamped_up_and_reserve_never_exceeds_budget() {
        let small = budget(768);
        assert_eq!(small.configured_budget_mb, 384);
        assert_eq!(small.effective_budget_mb, 384);
        assert!(small.insufficient_memory);
        for available in [128, 256, 512, 768] {
            let current = budget(available);
            assert!(current.root_reserve_mb <= current.effective_budget_mb);
            assert!(current.child_pool_mb <= current.effective_budget_mb);
        }
    }

    #[test]
    fn validates_safe_integer_flag_range() {
        for value in [MIN_MEMORY_BUDGET_MB as f64, MAX_MEMORY_BUDGET_MB as f64] {
            assert!(is_valid_memory_budget_mb(value));
            assert_eq!(normalize_memory_budget_mb(value).unwrap() as f64, value);
        }
        for value in [
            0.0,
            -1.0,
            1.5,
            f64::NAN,
            (MIN_MEMORY_BUDGET_MB - 1) as f64,
            (MAX_MEMORY_BUDGET_MB + 1) as f64,
        ] {
            assert!(!is_valid_memory_budget_mb(value));
            assert!(normalize_memory_budget_mb(value).is_err());
        }
        assert!(memory_budget_range_error().contains("[1024, 1048576]"));
    }

    #[test]
    fn recommended_child_share_matches_reference_and_small_host_floor_behavior() {
        let large = budget(32_768);
        assert_eq!(recommended_child_share_mb(&large, 1), 15_360);
        assert_eq!(recommended_child_share_mb(&large, 25), 614);
        assert_eq!(
            recommended_child_share_mb(&large, 10_000),
            MIN_CHILD_HEAP_MB
        );
        assert_eq!(
            recommended_child_share_mb(&budget(128 * 1024), 1),
            MAX_CHILD_HEAP_MB
        );
        let small = budget(768);
        assert_eq!(
            recommended_child_share_mb(&small, 1),
            small.legacy_child_ceiling_mb
        );
        assert!(recommended_child_share_mb(&small, 1) < MIN_CHILD_HEAP_MB);
    }

    #[test]
    fn legacy_ceiling_keeps_the_spawn_path_fraction_and_cap() {
        let unsaturated = 8_192;
        assert_eq!(
            legacy_child_ceiling_mb(unsaturated),
            (unsaturated as f64 * LEGACY_CHILD_HEAP_FRACTION).floor() as u64
        );
        assert_eq!(legacy_child_ceiling_mb(65_536), MAX_CHILD_HEAP_MB);
    }

    #[test]
    fn journal_growth_pool_matches_reference_limits_and_pins() {
        for (available, expected) in [(32_768, 819), (16_384, 409), (8_192, 204), (2_048, 51)] {
            assert_eq!(journal_growth_pool_mb(&budget(available)), expected);
        }
        assert_eq!(journal_growth_pool_mb(&budget(512)), 0);

        let mut limited = budget(8_192);
        limited.child_pool_mb = 16;
        assert_eq!(journal_growth_pool_mb(&limited), 16);

        let huge = resolve_daemon_memory_budget(MemoryBudgetInput {
            budget_mb: Some(MAX_MEMORY_BUDGET_MB as f64),
            available_memory_mb: MAX_MEMORY_BUDGET_MB,
            available_memory_source: AvailableMemorySource::Host,
        })
        .unwrap();
        assert_eq!(journal_growth_pool_mb(&huge), MAX_JOURNAL_GROWTH_POOL_MB);

        let normal = budget(8_192);
        assert_eq!(serve_journal_growth_pool_mb(&normal, None, None), 204);
        assert_eq!(serve_journal_growth_pool_mb(&normal, Some(5_000), None), 0);
        assert_eq!(serve_journal_growth_pool_mb(&normal, None, Some(1)), 0);
    }

    #[test]
    fn formats_budget_diagnostics_like_source() {
        assert_eq!(
            format_memory_budget_stderr(&budget(32_768)),
            "qwen serve: memory budget 16384 MB (derived, 32768 MB available via host)"
        );
        let small = budget(768);
        let message = format_memory_budget_stderr(&small);
        assert!(message.contains("below the 1024 MB minimum budget"));
        assert!(message.contains("host with at least ~2048 MB"));
        assert!(message.contains("requires at least 1024 MB available"));

        let flag_small = resolve_daemon_memory_budget(MemoryBudgetInput {
            budget_mb: Some(1_024.0),
            available_memory_mb: 512,
            available_memory_source: AvailableMemorySource::Host,
        })
        .unwrap();
        let flagged_message = format_memory_budget_stderr(&flag_small);
        assert!(flagged_message.contains("capped down from the configured 1024 MB"));
        assert!(!flagged_message.contains("a derived budget needs"));
    }

    #[test]
    fn share_is_monotone_and_never_exceeds_legacy_ceiling() {
        let value = budget(32_768);
        let mut previous = u64::MAX;
        for children in 1..=25 {
            let share = recommended_child_share_mb(&value, children);
            assert!(share <= previous);
            previous = share;
        }
        for available in [768, 1_024, 2_048, 8_192, 32_768] {
            let scoped = budget(available);
            assert!(recommended_child_share_mb(&scoped, 1) <= scoped.legacy_child_ceiling_mb);
        }
    }
}
