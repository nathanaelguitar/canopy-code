//! Daemon-wide accounting for adaptive live-journal growth, ported from
//! `journalGrowthPolicy.ts`.

use super::replay_window_limits::{
    JOURNAL_GROWTH_HARD_CAP_BYTES, JournalGrowthSessionLimit, MAX_SAFE_INTEGER,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalGrowthPolicyOptions {
    pub baseline_events: u64,
    pub baseline_bytes: u64,
    pub pool_bytes: u64,
    pub hard_cap_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalGrowthRequest<'a> {
    pub current_max_events: u64,
    pub current_max_bytes: u64,
    /// Every session sharing the pool, including the requester's current cap.
    pub all_session_limits: &'a [JournalGrowthSessionLimit],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalGrowthGrant {
    pub max_events: u64,
    pub max_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalGrowthPolicy {
    options: JournalGrowthPolicyOptions,
    hard_cap_events: u64,
}

impl JournalGrowthPolicy {
    pub fn new(options: JournalGrowthPolicyOptions) -> Self {
        // Clamp before converting to an integer. A tiny baseline byte cap
        // combined with a large event cap can make the proportional result
        // exceed JS's safe-integer range even when each input is valid.
        let proportional = scaled_event_limit(
            options.hard_cap_bytes,
            options.baseline_bytes,
            options.baseline_events,
        );
        let hard_cap_events = options
            .baseline_events
            .max(proportional)
            .min(MAX_SAFE_INTEGER);
        Self {
            options,
            hard_cap_events,
        }
    }

    pub fn hard_cap_events(&self) -> u64 {
        self.hard_cap_events
    }

    /// Grant one cap increase while accounting for every session's growth
    /// beyond its own starting baseline.
    pub fn grant(&self, request: JournalGrowthRequest<'_>) -> Option<JournalGrowthGrant> {
        let options = self.options;
        if request.current_max_bytes >= options.hard_cap_bytes {
            return None;
        }

        let extra_granted = request
            .all_session_limits
            .iter()
            .fold(0_u64, |sum, session| {
                sum.saturating_add(session.limit_bytes.saturating_sub(session.baseline_bytes))
            });
        let available = options.pool_bytes.saturating_sub(extra_granted);
        if available == 0 {
            return None;
        }

        // Saturating arithmetic is safe here: every grant is clamped by the
        // configured hard cap, while saturation agrees with JS's practical
        // behavior for sums far beyond its safe integer range.
        let max_bytes = request
            .current_max_bytes
            .saturating_mul(2)
            .min(request.current_max_bytes.saturating_add(available))
            .min(options.hard_cap_bytes);
        if max_bytes <= request.current_max_bytes {
            return None;
        }

        let max_events = request
            .current_max_events
            .max(scaled_event_limit(
                max_bytes,
                options.baseline_bytes,
                options.baseline_events,
            ))
            .min(self.hard_cap_events);

        Some(JournalGrowthGrant {
            max_events,
            max_bytes,
        })
    }
}

fn scaled_event_limit(bytes: u64, baseline_bytes: u64, baseline_events: u64) -> u64 {
    let proportional = (bytes as f64 / baseline_bytes as f64) * baseline_events as f64;
    if proportional.is_nan() || proportional <= 0.0 {
        0
    } else if proportional >= MAX_SAFE_INTEGER as f64 {
        MAX_SAFE_INTEGER
    } else {
        proportional.ceil() as u64
    }
}

impl Default for JournalGrowthPolicyOptions {
    fn default() -> Self {
        Self {
            baseline_events: super::replay_window_limits::DEFAULT_MAX_JOURNAL_EVENTS,
            baseline_bytes: super::replay_window_limits::DEFAULT_MAX_JOURNAL_BYTES,
            pool_bytes: 0,
            hard_cap_bytes: JOURNAL_GROWTH_HARD_CAP_BYTES,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acp_bridge::replay_window_limits::{
        DEFAULT_MAX_JOURNAL_BYTES, DEFAULT_MAX_JOURNAL_EVENTS,
    };

    const MIB: u64 = 1024 * 1024;
    const HARD_CAP: u64 = 256 * MIB;

    fn session(limit_bytes: u64, baseline_bytes: u64) -> JournalGrowthSessionLimit {
        JournalGrowthSessionLimit {
            limit_bytes,
            baseline_bytes,
        }
    }

    fn policy(pool_bytes: u64) -> JournalGrowthPolicy {
        JournalGrowthPolicy::new(JournalGrowthPolicyOptions {
            baseline_events: DEFAULT_MAX_JOURNAL_EVENTS,
            baseline_bytes: DEFAULT_MAX_JOURNAL_BYTES,
            pool_bytes,
            hard_cap_bytes: HARD_CAP,
        })
    }

    fn grant(
        policy: &JournalGrowthPolicy,
        current_max_events: u64,
        current_max_bytes: u64,
        limits: &[JournalGrowthSessionLimit],
    ) -> Option<JournalGrowthGrant> {
        policy.grant(JournalGrowthRequest {
            current_max_events,
            current_max_bytes,
            all_session_limits: limits,
        })
    }

    #[test]
    fn doubles_caps_and_scales_events_proportionally() {
        let limits = [session(
            DEFAULT_MAX_JOURNAL_BYTES,
            DEFAULT_MAX_JOURNAL_BYTES,
        )];
        assert_eq!(
            grant(
                &policy(48 * MIB),
                DEFAULT_MAX_JOURNAL_EVENTS,
                DEFAULT_MAX_JOURNAL_BYTES,
                &limits
            ),
            Some(JournalGrowthGrant {
                max_bytes: 16 * MIB,
                max_events: 20_000,
            })
        );
    }

    #[test]
    fn baselines_are_not_charged_and_each_session_uses_its_own_baseline() {
        let many_baselines =
            vec![session(DEFAULT_MAX_JOURNAL_BYTES, DEFAULT_MAX_JOURNAL_BYTES); 33];
        assert_eq!(
            grant(
                &policy(32 * MIB),
                DEFAULT_MAX_JOURNAL_EVENTS,
                DEFAULT_MAX_JOURNAL_BYTES,
                &many_baselines
            )
            .unwrap()
            .max_bytes,
            16 * MIB
        );

        let mixed_baselines = [
            session(DEFAULT_MAX_JOURNAL_BYTES, DEFAULT_MAX_JOURNAL_BYTES),
            session(16 * MIB, 16 * MIB),
        ];
        assert_eq!(
            grant(
                &policy(12 * MIB),
                DEFAULT_MAX_JOURNAL_EVENTS,
                DEFAULT_MAX_JOURNAL_BYTES,
                &mixed_baselines
            )
            .unwrap()
            .max_bytes,
            16 * MIB
        );

        let grown_mixed_baseline = [
            session(DEFAULT_MAX_JOURNAL_BYTES, DEFAULT_MAX_JOURNAL_BYTES),
            session(24 * MIB, 16 * MIB),
        ];
        assert_eq!(
            grant(
                &policy(20 * MIB),
                DEFAULT_MAX_JOURNAL_EVENTS,
                DEFAULT_MAX_JOURNAL_BYTES,
                &grown_mixed_baseline
            )
            .unwrap()
            .max_bytes,
            16 * MIB
        );
    }

    #[test]
    fn honors_partial_pool_remaining_headroom_and_charges_the_requester() {
        let limits = [
            session(DEFAULT_MAX_JOURNAL_BYTES, DEFAULT_MAX_JOURNAL_BYTES),
            session(52 * MIB, DEFAULT_MAX_JOURNAL_BYTES),
        ];
        assert_eq!(
            grant(
                &policy(48 * MIB),
                DEFAULT_MAX_JOURNAL_EVENTS,
                DEFAULT_MAX_JOURNAL_BYTES,
                &limits
            ),
            Some(JournalGrowthGrant {
                max_bytes: 12 * MIB,
                max_events: 15_000,
            })
        );

        let requester = [session(16 * MIB, DEFAULT_MAX_JOURNAL_BYTES)];
        assert_eq!(
            grant(&policy(20 * MIB), 20_000, 16 * MIB, &requester),
            Some(JournalGrowthGrant {
                max_bytes: 28 * MIB,
                max_events: 35_000,
            })
        );

        let exhausted = [
            session(DEFAULT_MAX_JOURNAL_BYTES, DEFAULT_MAX_JOURNAL_BYTES),
            session(56 * MIB, DEFAULT_MAX_JOURNAL_BYTES),
        ];
        assert_eq!(
            grant(
                &policy(48 * MIB),
                DEFAULT_MAX_JOURNAL_EVENTS,
                DEFAULT_MAX_JOURNAL_BYTES,
                &exhausted
            ),
            None
        );
    }

    #[test]
    fn respects_per_session_hard_cap_and_scales_proportional_event_cap() {
        let at_cap = [session(HARD_CAP, DEFAULT_MAX_JOURNAL_BYTES)];
        assert_eq!(grant(&policy(512 * MIB), 320_000, HARD_CAP, &at_cap), None);

        let at_half = [session(128 * MIB, DEFAULT_MAX_JOURNAL_BYTES)];
        assert_eq!(
            grant(&policy(512 * MIB), 160_000, 128 * MIB, &at_half),
            Some(JournalGrowthGrant {
                max_bytes: HARD_CAP,
                max_events: 320_000,
            })
        );

        let overshooting_double = [session(192 * MIB, DEFAULT_MAX_JOURNAL_BYTES)];
        assert_eq!(
            grant(&policy(512 * MIB), 240_000, 192 * MIB, &overshooting_double),
            Some(JournalGrowthGrant {
                max_bytes: HARD_CAP,
                max_events: 320_000,
            })
        );
    }

    #[test]
    fn clamps_proportional_event_caps_to_javascript_safe_integer() {
        let policy = JournalGrowthPolicy::new(JournalGrowthPolicyOptions {
            baseline_events: MAX_SAFE_INTEGER,
            baseline_bytes: 1,
            pool_bytes: 64 * MIB,
            hard_cap_bytes: HARD_CAP,
        });
        let limits = [session(DEFAULT_MAX_JOURNAL_BYTES, 1)];
        assert_eq!(policy.hard_cap_events(), MAX_SAFE_INTEGER);
        assert_eq!(
            grant(
                &policy,
                MAX_SAFE_INTEGER,
                DEFAULT_MAX_JOURNAL_BYTES,
                &limits
            ),
            Some(JournalGrowthGrant {
                max_bytes: 16 * MIB,
                max_events: MAX_SAFE_INTEGER,
            })
        );
    }

    #[test]
    fn saturates_extreme_pool_accounting_and_ignores_sessions_below_baseline() {
        let extreme_policy = policy(u64::MAX);
        let limits = [session(0, 20 * MIB), session(u64::MAX, 0)];
        assert_eq!(
            grant(
                &extreme_policy,
                DEFAULT_MAX_JOURNAL_EVENTS,
                DEFAULT_MAX_JOURNAL_BYTES,
                &limits
            ),
            None
        );

        let below_baseline = [session(4 * MIB, 8 * MIB)];
        assert_eq!(
            grant(
                &policy(8 * MIB),
                DEFAULT_MAX_JOURNAL_EVENTS,
                DEFAULT_MAX_JOURNAL_BYTES,
                &below_baseline
            )
            .unwrap()
            .max_bytes,
            16 * MIB
        );
    }
}
