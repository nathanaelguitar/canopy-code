//! Pure scheduling calculations shared by cron scheduler consumers.
//!
//! Source: the scheduling helpers in `packages/core/src/services/cronScheduler.ts`.
//! Clock time and timezone are supplied by callers: `now` is a `DateTime` in
//! the timezone whose local cron fields should be evaluated. A persistent
//! [`NextDurableFireCache`] gives callers the source's bounded memoization
//! behavior (512 entries, clear wholesale at capacity). The source cache is
//! process-global; this Rust cache is explicit so tests and hosts can control
//! its lifetime. Like the source cache key, it does not include `now` or the
//! timezone, so callers should clear it when changing either context.
//!
//! The underlying `cron_parser::next_fire_time` scans elapsed minute instants.
//! Around daylight-saving transitions this can differ from JavaScript's local
//! `Date.setMinutes` stepping; see `utils::cron_parser` for the details.

use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Timelike};

use crate::services::cron_tasks_file::CronTask;
use crate::utils::cron_parser::next_fire_time;

/// Default wakeup interval used for non-finite delay input.
pub const WAKEUP_DEFAULT_SECONDS: u32 = 1_200;
/// Minimum accepted `/loop` wakeup delay.
pub const WAKEUP_MIN_SECONDS: u32 = 60;
/// Maximum accepted `/loop` wakeup delay.
pub const WAKEUP_MAX_SECONDS: u32 = 3_600;
/// Recurring jitter ceiling, in milliseconds.
pub const MAX_RECURRING_JITTER_MS: i64 = 15 * 60 * 1_000;
/// One-shot early jitter ceiling, in milliseconds.
pub const MAX_ONESHOT_JITTER_MS: i64 = 90 * 1_000;
/// Maximum durable-fire cache entries before a wholesale clear.
pub const NEXT_DURABLE_FIRE_CACHE_MAX: usize = 512;

const JS_DATE_MAX_MS: f64 = 8_640_000_000_000_000.0;

/// Normalize the recurring-task expiry age.
///
/// Zero (including negative zero) disables expiry by returning infinity;
/// positive values pass through, while negative and NaN values use `fallback`.
pub fn normalize_recurring_max_age(value: f64, fallback: f64) -> f64 {
    if value == 0.0 {
        f64::INFINITY
    } else if value > 0.0 {
        value
    } else {
        fallback
    }
}

/// Clamp a `/loop` delay to 60–3600 seconds, defaulting non-finite input to
/// 1200 seconds. Finite values are rounded using the source's `Math.round`
/// behavior before clamping.
pub fn clamp_wakeup_seconds(delay_seconds: f64) -> u32 {
    if !delay_seconds.is_finite() {
        return WAKEUP_DEFAULT_SECONDS;
    }

    // Negative inputs always land below the minimum after rounding, so Rust's
    // away-from-zero half rounding cannot change the final clamped result.
    delay_seconds
        .round()
        .clamp(WAKEUP_MIN_SECONDS as f64, WAKEUP_MAX_SECONDS as f64) as u32
}

/// Return the deterministic jitter for a cron job, in milliseconds.
///
/// Recurring jobs are delayed by up to 10% of the interval between two
/// consecutive cron times, capped at 15 minutes. One-shot jobs that land on
/// minute 0 or 30 receive a negative offset of up to 90 seconds. `now` supplies
/// the wall clock and timezone used to inspect the cron schedule.
pub fn compute_jitter_ms<Tz: TimeZone>(
    id: &str,
    cron_expr: &str,
    recurring: bool,
    now: &DateTime<Tz>,
) -> i64 {
    let hash = hash_id(id);
    if recurring {
        let Ok(first) = next_fire_time(cron_expr, now) else {
            return 0;
        };
        let Ok(second) = next_fire_time(cron_expr, &first) else {
            return 0;
        };
        let period_ms = second.timestamp_millis() - first.timestamp_millis();
        let ten_percent = (period_ms as f64 * 0.1).floor();
        let jitter_modulus = ten_percent.min(MAX_RECURRING_JITTER_MS as f64).max(1.0) as u64;
        (hash % jitter_modulus) as i64
    } else {
        let Ok(next) = next_fire_time(cron_expr, now) else {
            return 0;
        };
        if next.minute() % 30 == 0 {
            -((hash % MAX_ONESHOT_JITTER_MS as u64) as i64)
        } else {
            0
        }
    }
}

/// Hash an ID with JavaScript's signed 32-bit polynomial hash and absolute
/// value semantics. `encode_utf16` matches `charCodeAt`, including surrogate
/// pairs for non-BMP characters.
pub fn hash_id(id: &str) -> u64 {
    let hash = id.encode_utf16().fold(0_i32, |hash, code_unit| {
        hash.wrapping_mul(31).wrapping_add(code_unit as i32)
    });
    hash.unsigned_abs() as u64
}

/// Find the next cron time after an epoch-millisecond anchor, then add jitter.
/// Invalid dates, malformed expressions, and schedules with no match in the
/// parser's four-year horizon return `None`.
pub fn compute_next_fire_ms<Tz: TimeZone>(
    cron_expr: &str,
    after_ms: f64,
    jitter_ms: i64,
    timezone: &Tz,
) -> Option<i64> {
    let after = date_from_js_millis(after_ms, timezone)?;
    let next = next_fire_time(cron_expr, &after).ok()?;
    next.timestamp_millis().checked_add(jitter_ms)
}

/// Bounded memo for effective durable-task fire times.
#[derive(Debug, Default)]
pub struct NextDurableFireCache {
    entries: HashMap<NextDurableFireCacheKey, Option<i64>>,
}

impl NextDurableFireCache {
    /// Create an empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Clear all cached projections.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Number of cached projections, including cached `None` results.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no projections are cached.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct NextDurableFireCacheKey {
    id: String,
    cron: String,
    recurring: bool,
    anchor_bits: u64,
}

/// Compute the effective next fire time for a durable task, accounting for
/// jitter and memoizing by `(id, cron, recurring, anchor)`.
///
/// Recurring tasks anchor at `lastFiredAt ?? createdAt`; one-shots always use
/// `createdAt`. Disabled tasks are not filtered here, matching the source
/// helper; callers must treat disabled tasks as having no next fire.
pub fn effective_next_durable_fire_ms<Tz: TimeZone + Clone>(
    task: &CronTask,
    now: &DateTime<Tz>,
    cache: &mut NextDurableFireCache,
) -> Option<i64> {
    let created_at_ms = task.get("createdAt")?.as_f64()?;
    let recurring = task.recurring();
    let anchor_ms = if recurring {
        task.get("lastFiredAt")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(created_at_ms)
    } else {
        created_at_ms
    };
    let anchor_bits = if anchor_ms == 0.0 {
        0.0_f64.to_bits()
    } else {
        anchor_ms.to_bits()
    };
    let key = NextDurableFireCacheKey {
        id: task.id().to_owned(),
        cron: task.cron().to_owned(),
        recurring,
        anchor_bits,
    };

    if let Some(cached) = cache.entries.get(&key) {
        return *cached;
    }

    let jitter_ms = compute_jitter_ms(task.id(), task.cron(), recurring, now);
    let timezone = now.timezone();
    let result = compute_next_fire_ms(task.cron(), anchor_ms, jitter_ms, &timezone);

    // Match the source's wholesale clear before insertion, rather than LRU.
    if cache.entries.len() >= NEXT_DURABLE_FIRE_CACHE_MAX {
        cache.entries.clear();
    }
    cache.entries.insert(key, result);
    result
}

fn date_from_js_millis<Tz: TimeZone>(value: f64, timezone: &Tz) -> Option<DateTime<Tz>> {
    // JavaScript Date applies TimeClip: finite values outside +/- 8.64e15 ms
    // are invalid, and fractional milliseconds truncate toward zero.
    if !value.is_finite() || value.abs() > JS_DATE_MAX_MS {
        return None;
    }
    timezone.timestamp_millis_opt(value.trunc() as i64).single()
}
