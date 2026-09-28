//! Bounded replay and adaptive journal limit policies ported from the ACP
//! bridge's `replayWindowLimits.ts`.

use std::fmt;

pub const DEFAULT_COMPACTED_REPLAY_MAX_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_COMPACTED_REPLAY_MAX_BYTES: u64 = 256 * 1024 * 1024;

pub const DEFAULT_MAX_JOURNAL_EVENTS: u64 = 10_000;
pub const DEFAULT_MAX_JOURNAL_BYTES: u64 = 8 * 1024 * 1024;
pub const JOURNAL_GROWTH_HARD_CAP_BYTES: u64 = 256 * 1024 * 1024;

pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const MAX_SAFE_INTEGER_F64: f64 = MAX_SAFE_INTEGER as f64;

/// A session's growth-accounting entry: its current byte cap and the cap it
/// started with. Growth is charged relative to each session's own baseline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalGrowthSessionLimit {
    pub limit_bytes: u64,
    pub baseline_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LimitKind {
    CompactedReplayMaxBytes,
    MaxJournalEvents,
    MaxJournalBytes,
    JournalGrowthPoolBytes,
}

impl LimitKind {
    const fn field_name(self) -> &'static str {
        match self {
            Self::CompactedReplayMaxBytes => "compactedReplayMaxBytes",
            Self::MaxJournalEvents => "maxJournalEvents",
            Self::MaxJournalBytes => "maxJournalBytes",
            Self::JournalGrowthPoolBytes => "journalGrowthPoolBytes",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidReplayWindowLimitError {
    kind: LimitKind,
    value: String,
}

impl fmt::Display for InvalidReplayWindowLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            LimitKind::CompactedReplayMaxBytes => write!(
                formatter,
                "Invalid {}: {}. Must be a positive safe integer in [1, {}].",
                self.kind.field_name(),
                self.value,
                MAX_COMPACTED_REPLAY_MAX_BYTES
            ),
            _ => write!(
                formatter,
                "Invalid {}: {}. Must be a positive safe integer.",
                self.kind.field_name(),
                self.value
            ),
        }
    }
}

impl std::error::Error for InvalidReplayWindowLimitError {}

fn invalid(kind: LimitKind, value: f64) -> InvalidReplayWindowLimitError {
    InvalidReplayWindowLimitError {
        kind,
        value: js_number_string(value),
    }
}

fn is_positive_safe_integer(value: f64) -> bool {
    value.is_finite() && value.fract() == 0.0 && (1.0..=MAX_SAFE_INTEGER_F64).contains(&value)
}

fn normalize_positive_safe_integer(
    kind: LimitKind,
    value: Option<f64>,
    default: Option<u64>,
) -> Result<Option<u64>, InvalidReplayWindowLimitError> {
    let Some(value) = value else {
        return Ok(default);
    };
    if !is_positive_safe_integer(value) {
        return Err(invalid(kind, value));
    }
    Ok(Some(value as u64))
}

/// Defaults to 4 MiB and rejects values above the 256 MiB hard limit.
pub fn normalize_compacted_replay_max_bytes(
    value: Option<f64>,
) -> Result<u64, InvalidReplayWindowLimitError> {
    let Some(value) = value else {
        return Ok(DEFAULT_COMPACTED_REPLAY_MAX_BYTES);
    };
    if !is_positive_safe_integer(value) || value > MAX_COMPACTED_REPLAY_MAX_BYTES as f64 {
        return Err(invalid(LimitKind::CompactedReplayMaxBytes, value));
    }
    Ok(value as u64)
}

/// Defaults to 10,000 retained journal events.
pub fn normalize_max_journal_events(
    value: Option<f64>,
) -> Result<u64, InvalidReplayWindowLimitError> {
    Ok(normalize_positive_safe_integer(
        LimitKind::MaxJournalEvents,
        value,
        Some(DEFAULT_MAX_JOURNAL_EVENTS),
    )?
    .expect("a journal event default is configured"))
}

/// Defaults to 8 MiB of retained journal data.
pub fn normalize_max_journal_bytes(
    value: Option<f64>,
) -> Result<u64, InvalidReplayWindowLimitError> {
    Ok(normalize_positive_safe_integer(
        LimitKind::MaxJournalBytes,
        value,
        Some(DEFAULT_MAX_JOURNAL_BYTES),
    )?
    .expect("a journal byte default is configured"))
}

/// `None` disables adaptive journal growth; a configured pool must be a
/// positive safe integer.
pub fn normalize_journal_growth_pool_bytes(
    value: Option<f64>,
) -> Result<Option<u64>, InvalidReplayWindowLimitError> {
    normalize_positive_safe_integer(LimitKind::JournalGrowthPoolBytes, value, None)
}

/// Formats a JavaScript number for source-compatible validation diagnostics.
/// Rust's shortest-roundtrip scientific formatter supplies the digits; this
/// adjusts decimal/scientific thresholds and exponent spelling to JS rules.
fn js_number_string(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value == f64::INFINITY {
        return "Infinity".to_owned();
    }
    if value == f64::NEG_INFINITY {
        return "-Infinity".to_owned();
    }
    if value == 0.0 {
        return "0".to_owned();
    }

    let negative = value.is_sign_negative();
    let absolute = value.abs();
    let scientific = format!("{absolute:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("scientific float formatting includes an exponent");
    let exponent: i32 = exponent
        .parse()
        .expect("scientific float formatting includes an integer exponent");
    let decimal_offset = mantissa.find('.').unwrap_or(mantissa.len()) as i32 + exponent;
    let digits: String = mantissa
        .chars()
        .filter(|character| *character != '.')
        .collect();

    let body = if (-6..21).contains(&exponent) {
        if decimal_offset <= 0 {
            format!("0.{}{}", "0".repeat((-decimal_offset) as usize), digits)
        } else if decimal_offset as usize >= digits.len() {
            format!(
                "{}{}",
                digits,
                "0".repeat(decimal_offset as usize - digits.len())
            )
        } else {
            let split = decimal_offset as usize;
            format!("{}.{}", &digits[..split], &digits[split..])
        }
    } else {
        let sign = if exponent >= 0 { "+" } else { "" };
        format!("{mantissa}e{sign}{exponent}")
    };

    if negative { format!("-{body}") } else { body }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_accepts_safe_integer_boundaries() {
        assert_eq!(
            normalize_compacted_replay_max_bytes(None).unwrap(),
            DEFAULT_COMPACTED_REPLAY_MAX_BYTES
        );
        assert_eq!(
            normalize_max_journal_events(None).unwrap(),
            DEFAULT_MAX_JOURNAL_EVENTS
        );
        assert_eq!(
            normalize_max_journal_bytes(None).unwrap(),
            DEFAULT_MAX_JOURNAL_BYTES
        );
        assert_eq!(normalize_journal_growth_pool_bytes(None).unwrap(), None);

        assert_eq!(normalize_compacted_replay_max_bytes(Some(1.0)).unwrap(), 1);
        assert_eq!(
            normalize_compacted_replay_max_bytes(Some(MAX_COMPACTED_REPLAY_MAX_BYTES as f64))
                .unwrap(),
            MAX_COMPACTED_REPLAY_MAX_BYTES
        );
        assert_eq!(
            normalize_max_journal_events(Some(MAX_SAFE_INTEGER as f64)).unwrap(),
            MAX_SAFE_INTEGER
        );
        assert_eq!(
            normalize_max_journal_bytes(Some(MAX_SAFE_INTEGER as f64)).unwrap(),
            MAX_SAFE_INTEGER
        );
        assert_eq!(
            normalize_journal_growth_pool_bytes(Some(MAX_SAFE_INTEGER as f64)).unwrap(),
            Some(MAX_SAFE_INTEGER)
        );
    }

    #[test]
    fn reports_source_compatible_validation_errors() {
        assert_eq!(
            normalize_compacted_replay_max_bytes(Some(0.0))
                .unwrap_err()
                .to_string(),
            "Invalid compactedReplayMaxBytes: 0. Must be a positive safe integer in [1, 268435456]."
        );
        assert_eq!(
            normalize_compacted_replay_max_bytes(Some(256.5))
                .unwrap_err()
                .to_string(),
            "Invalid compactedReplayMaxBytes: 256.5. Must be a positive safe integer in [1, 268435456]."
        );
        assert_eq!(
            normalize_max_journal_events(Some(f64::NAN))
                .unwrap_err()
                .to_string(),
            "Invalid maxJournalEvents: NaN. Must be a positive safe integer."
        );
        assert_eq!(
            normalize_max_journal_bytes(Some(f64::INFINITY))
                .unwrap_err()
                .to_string(),
            "Invalid maxJournalBytes: Infinity. Must be a positive safe integer."
        );
        assert_eq!(
            normalize_journal_growth_pool_bytes(Some((MAX_SAFE_INTEGER + 1) as f64))
                .unwrap_err()
                .to_string(),
            "Invalid journalGrowthPoolBytes: 9007199254740992. Must be a positive safe integer."
        );
        assert!(normalize_max_journal_events(Some(-1.0)).is_err());
        assert!(normalize_max_journal_bytes(Some(1.5)).is_err());
        assert!(normalize_journal_growth_pool_bytes(Some(f64::NEG_INFINITY)).is_err());
        assert!(
            normalize_compacted_replay_max_bytes(Some((MAX_COMPACTED_REPLAY_MAX_BYTES + 1) as f64))
                .is_err()
        );
    }

    #[test]
    fn formats_javascript_number_thresholds_for_errors() {
        assert_eq!(js_number_string(-0.0), "0");
        assert_eq!(js_number_string(1e-6), "0.000001");
        assert_eq!(js_number_string(1e-7), "1e-7");
        assert_eq!(js_number_string(1e20), "100000000000000000000");
        assert_eq!(js_number_string(1e21), "1e+21");
    }
}
