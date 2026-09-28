//! Pure policy port of `packages/core/src/services/memoryPressureMonitor.ts`.
//!
//! This module only makes decisions from caller-supplied measurements and
//! counters. OS memory-limit discovery, sampling, cleanup execution, and event
//! delivery stay at the service boundary.

/// RSS and V8 heap utilization thresholds and cleanup timing.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MemoryPressureConfig {
    pub soft_pressure_ratio: f64,
    pub hard_pressure_ratio: f64,
    pub critical_ratio: f64,
    pub cleanup_cooldown_ms: f64,
    pub enable_explicit_gc: bool,
}

pub const DEFAULT_PRESSURE_CONFIG: MemoryPressureConfig = MemoryPressureConfig {
    soft_pressure_ratio: 0.5,
    hard_pressure_ratio: 0.65,
    critical_ratio: 0.8,
    cleanup_cooldown_ms: 5_000.0,
    enable_explicit_gc: true,
};

impl Default for MemoryPressureConfig {
    fn default() -> Self {
        DEFAULT_PRESSURE_CONFIG
    }
}

/// Validate thresholds with the same bounds and precedence as the TS source.
///
/// The returned messages intentionally match `validateMemoryPressureConfig`.
pub fn validate_memory_pressure_config(config: &MemoryPressureConfig) -> Result<(), &'static str> {
    for (name, ratio) in [
        ("softPressureRatio", config.soft_pressure_ratio),
        ("hardPressureRatio", config.hard_pressure_ratio),
        ("criticalRatio", config.critical_ratio),
    ] {
        if !ratio.is_finite() || !(0.3..=0.98).contains(&ratio) {
            return Err(match name {
                "softPressureRatio" => "softPressureRatio must be a finite ratio in [0.3, 0.98]",
                "hardPressureRatio" => "hardPressureRatio must be a finite ratio in [0.3, 0.98]",
                _ => "criticalRatio must be a finite ratio in [0.3, 0.98]",
            });
        }
    }
    if config.soft_pressure_ratio >= config.hard_pressure_ratio {
        return Err("softPressureRatio must be < hardPressureRatio");
    }
    if config.hard_pressure_ratio >= config.critical_ratio {
        return Err("hardPressureRatio must be < criticalRatio");
    }
    if !config.cleanup_cooldown_ms.is_finite() || config.cleanup_cooldown_ms < 0.0 {
        return Err("cleanupCooldownMs must be a non-negative number");
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PressureLevel {
    Normal,
    Soft,
    Hard,
    Critical,
}

impl PressureLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Soft => "soft",
            Self::Hard => "hard",
            Self::Critical => "critical",
        }
    }
}

/// Determine pressure using the greater of RSS/effective-limit and
/// heap-used/heap-limit. Zero limits disable their corresponding ratio, as in
/// the source monitor. Callers should map failed OS/runtime samples to zero or
/// handle them before calling this function.
pub fn pressure_level(
    config: &MemoryPressureConfig,
    rss_bytes: u64,
    effective_memory_limit_bytes: u64,
    heap_used_bytes: u64,
    heap_size_limit_bytes: u64,
) -> PressureLevel {
    let rss_ratio = if effective_memory_limit_bytes > 0 {
        rss_bytes as f64 / effective_memory_limit_bytes as f64
    } else {
        0.0
    };
    let heap_ratio = if heap_size_limit_bytes > 0 {
        heap_used_bytes as f64 / heap_size_limit_bytes as f64
    } else {
        0.0
    };
    let ratio = rss_ratio.max(heap_ratio);

    if ratio >= config.critical_ratio {
        PressureLevel::Critical
    } else if ratio >= config.hard_pressure_ratio {
        PressureLevel::Hard
    } else if ratio >= config.soft_pressure_ratio {
        PressureLevel::Soft
    } else {
        PressureLevel::Normal
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupAction {
    None,
    Light,
    Moderate,
    Aggressive,
}

impl CleanupAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Light => "light",
            Self::Moderate => "moderate",
            Self::Aggressive => "aggressive",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupStep {
    ClearFileCache,
    EvictColdCache,
    EvictStaleCache,
    TriggerGc,
    CompactHistory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupRecommendation {
    pub action: CleanupAction,
    pub steps: Vec<CleanupStep>,
}

/// Build the source monitor's ordered cleanup plan for a pressure tier.
pub fn recommend_cleanup(
    pressure: PressureLevel,
    enable_explicit_gc: bool,
) -> CleanupRecommendation {
    let (action, steps) = match pressure {
        PressureLevel::Normal => (CleanupAction::None, vec![]),
        PressureLevel::Soft => (CleanupAction::Light, vec![CleanupStep::EvictStaleCache]),
        PressureLevel::Hard => (
            CleanupAction::Moderate,
            vec![
                CleanupStep::EvictColdCache,
                CleanupStep::CompactHistory,
                CleanupStep::ClearFileCache,
            ],
        ),
        PressureLevel::Critical => {
            let mut steps = vec![
                CleanupStep::EvictColdCache,
                CleanupStep::CompactHistory,
                CleanupStep::ClearFileCache,
            ];
            if enable_explicit_gc {
                steps.push(CleanupStep::TriggerGc);
            }
            (CleanupAction::Aggressive, steps)
        }
    };
    CleanupRecommendation { action, steps }
}

/// Numeric priority used to compare active, queued, and requested cleanups.
pub const fn cleanup_action_rank(action: CleanupAction) -> u8 {
    match action {
        CleanupAction::None => 0,
        CleanupAction::Light => 1,
        CleanupAction::Moderate => 2,
        CleanupAction::Aggressive => 3,
    }
}

pub const fn is_cleanup_escalation(action: CleanupAction, last_action: CleanupAction) -> bool {
    cleanup_action_rank(action) > cleanup_action_rank(last_action)
}

/// Compute cooldown with exponential backoff after three ineffective
/// aggressive cleanups. The exponent caps at six, matching the TS monitor.
pub fn cleanup_cooldown_ms(
    action: CleanupAction,
    config: &MemoryPressureConfig,
    consecutive_ineffective_aggressive_cleanups: u64,
) -> f64 {
    let base = config.cleanup_cooldown_ms;
    if action != CleanupAction::Aggressive
        || base == 0.0
        || consecutive_ineffective_aggressive_cleanups < 3
    {
        return base;
    }
    let exponent = (consecutive_ineffective_aggressive_cleanups - 2).min(6);
    base * 2_f64.powi(exponent as i32)
}

/// Decide whether a cleanup may run at the supplied elapsed time. An
/// escalation bypasses cooldown; otherwise equality with the cooldown permits
/// the cleanup, matching the source's strict `<` block condition.
pub fn may_run_cleanup(
    action: CleanupAction,
    last_action: CleanupAction,
    elapsed_since_last_cleanup_ms: f64,
    config: &MemoryPressureConfig,
    consecutive_ineffective_aggressive_cleanups: u64,
) -> bool {
    is_cleanup_escalation(action, last_action)
        || elapsed_since_last_cleanup_ms
            >= cleanup_cooldown_ms(action, config, consecutive_ineffective_aggressive_cleanups)
}

/// Diagnostic cadence shared by cleanup failure and ineffectiveness events:
/// emit at 3, again at 10, then every 20 events.
pub const fn should_emit_repeated_diagnostic(count: u64) -> bool {
    count == 3 || count == 10 || (count > 10 && count % 20 == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_source() {
        assert_eq!(
            MemoryPressureConfig::default(),
            MemoryPressureConfig {
                soft_pressure_ratio: 0.5,
                hard_pressure_ratio: 0.65,
                critical_ratio: 0.8,
                cleanup_cooldown_ms: 5_000.0,
                enable_explicit_gc: true,
            }
        );
        assert_eq!(DEFAULT_PRESSURE_CONFIG, MemoryPressureConfig::default());
        assert_eq!(
            validate_memory_pressure_config(&DEFAULT_PRESSURE_CONFIG),
            Ok(())
        );
    }

    #[test]
    fn validates_ratio_bounds_order_and_cooldown() {
        let mut config = DEFAULT_PRESSURE_CONFIG;
        config.soft_pressure_ratio = 0.3;
        config.hard_pressure_ratio = 0.7;
        config.critical_ratio = 0.98;
        config.cleanup_cooldown_ms = 0.0;
        assert_eq!(validate_memory_pressure_config(&config), Ok(()));

        config.soft_pressure_ratio = f64::NAN;
        assert_eq!(
            validate_memory_pressure_config(&config),
            Err("softPressureRatio must be a finite ratio in [0.3, 0.98]")
        );
        config = DEFAULT_PRESSURE_CONFIG;
        config.hard_pressure_ratio = f64::INFINITY;
        assert_eq!(
            validate_memory_pressure_config(&config),
            Err("hardPressureRatio must be a finite ratio in [0.3, 0.98]")
        );
        config = DEFAULT_PRESSURE_CONFIG;
        config.critical_ratio = 0.99;
        assert_eq!(
            validate_memory_pressure_config(&config),
            Err("criticalRatio must be a finite ratio in [0.3, 0.98]")
        );
        config = DEFAULT_PRESSURE_CONFIG;
        config.soft_pressure_ratio = config.hard_pressure_ratio;
        assert_eq!(
            validate_memory_pressure_config(&config),
            Err("softPressureRatio must be < hardPressureRatio")
        );
        config = DEFAULT_PRESSURE_CONFIG;
        config.hard_pressure_ratio = config.critical_ratio;
        assert_eq!(
            validate_memory_pressure_config(&config),
            Err("hardPressureRatio must be < criticalRatio")
        );
        config = DEFAULT_PRESSURE_CONFIG;
        config.cleanup_cooldown_ms = -1.0;
        assert_eq!(
            validate_memory_pressure_config(&config),
            Err("cleanupCooldownMs must be a non-negative number")
        );
        config.cleanup_cooldown_ms = f64::INFINITY;
        assert_eq!(
            validate_memory_pressure_config(&config),
            Err("cleanupCooldownMs must be a non-negative number")
        );
    }

    #[test]
    fn pressure_uses_the_stronger_rss_or_heap_ratio_and_inclusive_thresholds() {
        let config = DEFAULT_PRESSURE_CONFIG;
        let gib = 1024_u64.pow(3);
        assert_eq!(
            pressure_level(&config, gib, 16 * gib, 0, 0),
            PressureLevel::Normal
        );
        assert_eq!(
            pressure_level(&config, 8 * gib, 16 * gib, 0, 0),
            PressureLevel::Soft
        );
        assert_eq!(
            pressure_level(&config, 10_400_000_000, 16_000_000_000, 0, 0),
            PressureLevel::Hard
        );
        assert_eq!(
            pressure_level(&config, 12_800_000_000, 16_000_000_000, 0, 0),
            PressureLevel::Critical
        );
        // A low RSS ratio does not mask heap pressure.
        assert_eq!(
            pressure_level(&config, 1, 64 * gib, 3 * gib / 5, gib),
            PressureLevel::Soft
        );
        // Zero limits disable only that metric, as in the source.
        assert_eq!(pressure_level(&config, 1, 0, 1, 0), PressureLevel::Normal);
    }

    #[test]
    fn recommendations_and_step_order_match_pressure_tiers() {
        assert_eq!(
            recommend_cleanup(PressureLevel::Normal, true),
            CleanupRecommendation {
                action: CleanupAction::None,
                steps: vec![],
            }
        );
        assert_eq!(
            recommend_cleanup(PressureLevel::Soft, true),
            CleanupRecommendation {
                action: CleanupAction::Light,
                steps: vec![CleanupStep::EvictStaleCache],
            }
        );
        let middle_steps = vec![
            CleanupStep::EvictColdCache,
            CleanupStep::CompactHistory,
            CleanupStep::ClearFileCache,
        ];
        assert_eq!(
            recommend_cleanup(PressureLevel::Hard, true),
            CleanupRecommendation {
                action: CleanupAction::Moderate,
                steps: middle_steps.clone(),
            }
        );
        assert_eq!(
            recommend_cleanup(PressureLevel::Critical, false),
            CleanupRecommendation {
                action: CleanupAction::Aggressive,
                steps: middle_steps.clone(),
            }
        );
        let mut critical_steps = middle_steps;
        critical_steps.push(CleanupStep::TriggerGc);
        assert_eq!(
            recommend_cleanup(PressureLevel::Critical, true),
            CleanupRecommendation {
                action: CleanupAction::Aggressive,
                steps: critical_steps,
            }
        );
    }

    #[test]
    fn action_ranks_and_escalation_match_source() {
        assert_eq!(cleanup_action_rank(CleanupAction::None), 0);
        assert_eq!(cleanup_action_rank(CleanupAction::Light), 1);
        assert_eq!(cleanup_action_rank(CleanupAction::Moderate), 2);
        assert_eq!(cleanup_action_rank(CleanupAction::Aggressive), 3);
        assert!(!is_cleanup_escalation(
            CleanupAction::Light,
            CleanupAction::Light
        ));
        assert!(is_cleanup_escalation(
            CleanupAction::Aggressive,
            CleanupAction::Light
        ));
        assert!(!is_cleanup_escalation(
            CleanupAction::Light,
            CleanupAction::Aggressive
        ));
    }

    #[test]
    fn aggressive_cleanup_cooldown_backs_off_and_escalations_bypass_it() {
        let mut config = DEFAULT_PRESSURE_CONFIG;
        config.cleanup_cooldown_ms = 1_000.0;
        for (ineffective, expected) in [
            (0, 1_000.0),
            (2, 1_000.0),
            (3, 2_000.0),
            (4, 4_000.0),
            (8, 64_000.0),
            (100, 64_000.0),
        ] {
            assert_eq!(
                cleanup_cooldown_ms(CleanupAction::Aggressive, &config, ineffective),
                expected
            );
        }
        assert_eq!(
            cleanup_cooldown_ms(CleanupAction::Moderate, &config, 100),
            1_000.0
        );
        assert!(!may_run_cleanup(
            CleanupAction::Aggressive,
            CleanupAction::Aggressive,
            1_999.0,
            &config,
            3,
        ));
        assert!(may_run_cleanup(
            CleanupAction::Aggressive,
            CleanupAction::Aggressive,
            2_000.0,
            &config,
            3,
        ));
        assert!(may_run_cleanup(
            CleanupAction::Aggressive,
            CleanupAction::Light,
            0.0,
            &config,
            100,
        ));
        config.cleanup_cooldown_ms = 0.0;
        assert_eq!(
            cleanup_cooldown_ms(CleanupAction::Aggressive, &config, 100),
            0.0
        );
    }

    #[test]
    fn repeated_diagnostics_emit_at_source_cadence() {
        for count in [3, 10, 20, 40, 60, 100] {
            assert!(should_emit_repeated_diagnostic(count), "{count}");
        }
        for count in [0, 1, 2, 4, 9, 11, 19, 21, 39, 41] {
            assert!(!should_emit_repeated_diagnostic(count), "{count}");
        }
    }
}
