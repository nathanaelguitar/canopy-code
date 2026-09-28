//! Human-readable memory age and staleness guidance.
//!
//! Port of `packages/core/src/memory/memoryAge.ts`. `recall` owns the shared
//! implementation because recall formatting uses the same injected-clock
//! variants; this module exposes the source-level age API.

pub use super::recall::{
    memory_age, memory_age_at, memory_age_days, memory_age_days_at, memory_freshness_note,
    memory_freshness_note_at, memory_freshness_text, memory_freshness_text_at,
};

#[cfg(test)]
mod tests {
    use super::*;

    const DAY_MS: f64 = 86_400_000.0;

    #[test]
    fn age_rounds_down_and_clamps_future_times_to_today() {
        let now = 1_800_000_000_000.0;
        assert_eq!(memory_age_days_at(now, now), 0.0);
        assert_eq!(memory_age_at(now, now), "today");
        assert_eq!(memory_age_at(now - DAY_MS, now), "yesterday");
        assert_eq!(memory_age_days_at(now - 1.9 * DAY_MS, now), 1.0);
        assert_eq!(memory_age_days_at(now + DAY_MS, now), 0.0);
    }

    #[test]
    fn staleness_note_is_silent_until_a_memory_is_more_than_one_day_old() {
        let now = 1_800_000_000_000.0;
        assert_eq!(memory_freshness_text_at(now, now), "");
        assert_eq!(memory_freshness_note_at(now - DAY_MS, now), "");
        let text = memory_freshness_text_at(now - 2.0 * DAY_MS, now);
        assert!(text.starts_with("This memory is 2 days old."));
        assert_eq!(
            memory_freshness_note_at(now - 2.0 * DAY_MS, now),
            format!("<system-reminder>{text}</system-reminder>\n")
        );
    }
}
