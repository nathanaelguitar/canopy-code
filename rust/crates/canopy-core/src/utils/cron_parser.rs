//! Minimal parser and evaluator for Canopy's five-field cron expressions.
//!
//! The accepted syntax is `minute hour day-of-month month day-of-week` with
//! wildcards, comma lists, ranges, and positive steps. Day-of-week accepts
//! both `0` and `7` for Sunday. Extended cron syntax (names, `L`, `W`, `?`,
//! and similar extensions) is not supported.
//!
//! Day-of-month and day-of-week follow Vixie cron's rule: if neither field
//! begins with `*`, either field may match; if either begins with `*`, both
//! fields must match. The wildcard flag checks the leading character of the
//! complete field, so `*/2` and `*,3` count as wildcard fields, while `1,*`
//! does not.
//!
//! The date APIs accept any Chrono time zone. They inspect the supplied
//! [`DateTime`] in that value's time zone, so callers can pass [`chrono::Local`] to
//! use the machine's local zone or a fixed/explicit zone for deterministic
//! behavior. [`next_fire_time`] advances by elapsed one-minute instants. This
//! agrees with the TypeScript implementation away from daylight-saving
//! transitions, but it can visit both copies of a repeated wall-clock minute
//! during a fall-back transition; the TypeScript `Date.setMinutes` loop uses
//! local-calendar setters and can resolve that transition differently. A
//! nonexistent spring-forward wall time is naturally skipped. This module
//! does not implement Vixie cron's daemon-specific DST catch-up rules.

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike};
use indexmap::IndexSet;
use std::fmt;

const MAX_SEARCH_MINUTES: usize = 4 * 366 * 24 * 60;

/// Parsed cron fields. Values preserve their first-seen order, like JavaScript
/// `Set`; day-of-week uses `0` for Sunday (including expressions that used `7`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CronFields {
    pub minute: IndexSet<u8>,
    pub hour: IndexSet<u8>,
    pub day_of_month: IndexSet<u8>,
    pub month: IndexSet<u8>,
    pub day_of_week: IndexSet<u8>,
    /// Whether the day-of-month field begins with `*`.
    pub dom_is_wild: bool,
    /// Whether the day-of-week field begins with `*`.
    pub dow_is_wild: bool,
}

impl CronFields {
    /// Evaluates the already-parsed fields against a date in its own time zone.
    pub fn matches<Tz: TimeZone>(&self, date: &DateTime<Tz>) -> bool {
        if !self.minute.contains(&(date.minute() as u8))
            || !self.hour.contains(&(date.hour() as u8))
            || !self.month.contains(&(date.month() as u8))
        {
            return false;
        }

        let dom_matches = self.day_of_month.contains(&(date.day() as u8));
        let dow_matches = self
            .day_of_week
            .contains(&(date.weekday().num_days_from_sunday() as u8));
        if !self.dom_is_wild && !self.dow_is_wild {
            dom_matches || dow_matches
        } else {
            dom_matches && dow_matches
        }
    }
}

/// A cron syntax error. The message follows the TypeScript parser's wording.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CronParseError {
    message: String,
}

impl CronParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for CronParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CronParseError {}

/// Failures that can occur while finding a next fire time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NextFireError {
    /// The supplied expression is malformed.
    Parse(CronParseError),
    /// No date matched during the four-year search window.
    NoMatchingTime { cron_expr: String },
    /// The input date is too close to Chrono's representable boundary to
    /// advance to the first candidate minute.
    DateOutOfRange,
}

impl fmt::Display for NextFireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(error) => error.fmt(f),
            Self::NoMatchingTime { cron_expr } => write!(
                f,
                "No matching fire time found within 4 years for: \"{cron_expr}\""
            ),
            Self::DateOutOfRange => {
                f.write_str("Date/time overflow while searching for next fire time")
            }
        }
    }
}

impl std::error::Error for NextFireError {}

impl From<CronParseError> for NextFireError {
    fn from(error: CronParseError) -> Self {
        Self::Parse(error)
    }
}

/// Parses one five-field cron expression.
pub fn parse_cron(cron_expr: &str) -> Result<CronFields, CronParseError> {
    let trimmed = trim_ecmascript_whitespace(cron_expr);
    let mut parts: Vec<&str> = split_ecmascript_whitespace(trimmed).collect();
    // JavaScript's `''.split(/\s+/)` yields one empty field, whereas Rust's
    // `split_whitespace` yields none. Preserve the source parser's field count.
    if parts.is_empty() {
        parts.push("");
    }
    if parts.len() != 5 {
        return Err(CronParseError::new(format!(
            "Cron expression must have exactly 5 fields, got {}: \"{}\"",
            parts.len(),
            cron_expr
        )));
    }

    // Preserve source error precedence: day-of-week is parsed first.
    let mut day_of_week = parse_field(parts[4], 0, 7)?;
    if day_of_week.shift_remove(&7) {
        day_of_week.insert(0);
    }

    Ok(CronFields {
        minute: parse_field(parts[0], 0, 59)?,
        hour: parse_field(parts[1], 0, 23)?,
        day_of_month: parse_field(parts[2], 1, 31)?,
        month: parse_field(parts[3], 1, 12)?,
        day_of_week,
        dom_is_wild: trim_ecmascript_whitespace(parts[2]).starts_with('*'),
        dow_is_wild: trim_ecmascript_whitespace(parts[4]).starts_with('*'),
    })
}

/// Returns whether the cron expression matches `date` in the date's time zone.
pub fn matches<Tz: TimeZone>(cron_expr: &str, date: &DateTime<Tz>) -> Result<bool, CronParseError> {
    Ok(parse_cron(cron_expr)?.matches(date))
}

/// Finds the first matching minute strictly after `after`.
///
/// The input's seconds and fractional seconds are cleared, then the search
/// starts one minute later, matching the TypeScript `Date` implementation.
/// Search stops after four 366-day years.
pub fn next_fire_time<Tz: TimeZone>(
    cron_expr: &str,
    after: &DateTime<Tz>,
) -> Result<DateTime<Tz>, NextFireError> {
    let fields = parse_cron(cron_expr)?;
    let mut candidate = after
        .clone()
        .with_second(0)
        .and_then(|date| date.with_nanosecond(0))
        .and_then(|date| date.checked_add_signed(Duration::minutes(1)))
        .ok_or(NextFireError::DateOutOfRange)?;

    for _ in 0..MAX_SEARCH_MINUTES {
        if fields.matches(&candidate) {
            return Ok(candidate);
        }
        candidate = candidate
            .checked_add_signed(Duration::minutes(1))
            .ok_or(NextFireError::DateOutOfRange)?;
    }

    Err(NextFireError::NoMatchingTime {
        cron_expr: cron_expr.to_owned(),
    })
}

fn parse_field(field: &str, min: u8, max: u8) -> Result<IndexSet<u8>, CronParseError> {
    let mut values = IndexSet::new();

    for part in field.split(',') {
        let trimmed = trim_ecmascript_whitespace(part);
        if trimmed.is_empty() {
            return Err(CronParseError::new(format!(
                "Empty field segment in \"{field}\""
            )));
        }

        let step_parts: Vec<&str> = trimmed.split('/').collect();
        if step_parts.len() > 2 {
            return Err(CronParseError::new(format!(
                "Invalid step expression: \"{trimmed}\""
            )));
        }

        let base = step_parts[0];
        let (range_start, range_end) = if base == "*" {
            (min as u64, max as u64)
        } else if base.contains('-') {
            let range_parts: Vec<&str> = base.split('-').collect();
            if range_parts.len() != 2
                || !is_integer_token(range_parts[0])
                || !is_integer_token(range_parts[1])
            {
                return Err(CronParseError::new(format!("Invalid range: \"{base}\"")));
            }
            let start = parse_integer_saturating(range_parts[0]);
            let end = parse_integer_saturating(range_parts[1]);
            if start < min as u64 || end > max as u64 || start > end {
                return Err(CronParseError::new(format!(
                    "Range {base} out of bounds [{min}-{max}]"
                )));
            }
            (start, end)
        } else {
            if !is_integer_token(base) {
                return Err(CronParseError::new(format!("Invalid value: \"{base}\"")));
            }
            let value = parse_integer_saturating(base);
            if value < min as u64 || value > max as u64 {
                return Err(CronParseError::new(format!(
                    "Value \"{base}\" out of bounds [{min}-{max}]"
                )));
            }
            // Vixie cron treats N/step as N-max/step, not as only N.
            let end = if step_parts.len() == 2 {
                max as u64
            } else {
                value
            };
            (value, end)
        };

        if step_parts.len() == 2 && !is_integer_token(step_parts[1]) {
            return Err(CronParseError::new(format!(
                "Invalid step: \"{}\"",
                step_parts[1]
            )));
        }
        let step = if step_parts.len() == 2 {
            parse_integer_saturating(step_parts[1])
        } else {
            1
        };
        if step == 0 {
            let invalid = step_parts.get(1).copied().unwrap_or("");
            return Err(CronParseError::new(format!("Invalid step: \"{invalid}\"")));
        }

        let mut value = range_start;
        loop {
            values.insert(value as u8);
            if value > range_end || step > range_end - value {
                break;
            }
            value += step;
        }
    }

    Ok(values)
}

fn is_integer_token(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

/// ECMAScript's `String.prototype.trim` and regular-expression `\s` use the
/// same WhiteSpace and LineTerminator code points. Rust's Unicode whitespace
/// predicates differ (notably around FEFF and NEL), so keep the source set
/// explicit here.
fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'..='\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}

fn trim_ecmascript_whitespace(value: &str) -> &str {
    value.trim_matches(is_ecmascript_whitespace)
}

fn split_ecmascript_whitespace(value: &str) -> impl Iterator<Item = &str> {
    value
        .split(is_ecmascript_whitespace)
        .filter(|part| !part.is_empty())
}

/// Parses arbitrarily long ASCII digit strings without overflowing. Values
/// larger than any cron field are later reported as out of bounds; a huge
/// positive step simply selects its starting value once.
fn parse_integer_saturating(value: &str) -> u64 {
    value.bytes().fold(0_u64, |number, byte| {
        number
            .saturating_mul(10)
            .saturating_add((byte - b'0') as u64)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, TimeZone};

    fn date(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> DateTime<FixedOffset> {
        FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .unwrap()
    }

    fn values(set: &IndexSet<u8>) -> Vec<u8> {
        set.iter().copied().collect()
    }

    #[test]
    fn parses_wildcards_values_ranges_lists_and_steps() {
        let all = parse_cron("* * * * *").unwrap();
        assert_eq!(all.minute.len(), 60);
        assert_eq!(all.hour.len(), 24);
        assert_eq!(all.day_of_month.len(), 31);
        assert_eq!(all.month.len(), 12);
        assert_eq!(all.day_of_week.len(), 7);

        assert_eq!(values(&parse_cron("5 14 1 6 3").unwrap().minute), [5]);
        assert_eq!(
            values(&parse_cron("1-5 * * * *").unwrap().minute),
            [1, 2, 3, 4, 5]
        );
        assert_eq!(
            values(&parse_cron("0,15,30,45 * * * *").unwrap().minute),
            [0, 15, 30, 45]
        );
        assert_eq!(
            values(&parse_cron("30,15,30 * * * *").unwrap().minute),
            [30, 15]
        );
        assert_eq!(
            values(&parse_cron("*/15 * * * *").unwrap().minute),
            [0, 15, 30, 45]
        );
        assert_eq!(
            values(&parse_cron("1-10/3 * * * *").unwrap().minute),
            [1, 4, 7, 10]
        );
        assert_eq!(
            values(&parse_cron("5/15 * * * *").unwrap().minute),
            [5, 20, 35, 50]
        );
        assert_eq!(
            values(&parse_cron("* 0/6 * * *").unwrap().hour),
            [0, 6, 12, 18]
        );
    }

    #[test]
    fn wildcard_flags_follow_the_leading_character_of_each_day_field() {
        assert!(parse_cron("0 0 */2 * 1").unwrap().dom_is_wild);
        assert!(parse_cron("0 0 15 * */3").unwrap().dow_is_wild);
        assert!(parse_cron("0 0 *,10 * 1").unwrap().dom_is_wild);
        assert!(parse_cron("0 0 15 * *,3").unwrap().dow_is_wild);
        assert!(!parse_cron("0 0 1,* * 1").unwrap().dom_is_wild);
        assert!(!parse_cron("0 0 15 * 1,*").unwrap().dow_is_wild);
    }

    #[test]
    fn normalizes_seven_to_sunday() {
        let fields = parse_cron("* * * * 7").unwrap();
        assert!(fields.day_of_week.contains(&0));
        assert!(!fields.day_of_week.contains(&7));
        assert_eq!(values(&parse_cron("* * * * 0,7").unwrap().day_of_week), [0]);
    }

    #[test]
    fn rejects_wrong_field_count_empty_segments_and_out_of_range_values() {
        for expression in ["* * *", "* * * * * *", ""] {
            assert!(
                parse_cron(expression)
                    .unwrap_err()
                    .to_string()
                    .contains("must have exactly 5 fields")
            );
        }
        assert!(
            parse_cron("0,,5 * * * *")
                .unwrap_err()
                .to_string()
                .contains("Empty field segment")
        );
        for expression in [
            "60 * * * *",
            "* 25 * * *",
            "* * 0 * *",
            "* * * 13 *",
            "* * * * 8",
            "999999999999999999999999 * * * *",
        ] {
            assert!(
                parse_cron(expression)
                    .unwrap_err()
                    .to_string()
                    .contains("out of bounds")
            );
        }
    }

    #[test]
    fn uses_ecmascript_whitespace_boundaries_for_trim_and_field_splitting() {
        // ECMAScript trim and /\s+/ include BOM (FEFF), including at the
        // whole-expression and individual-field boundaries.
        let fields = parse_cron("\u{FEFF}*\u{FEFF}*\u{FEFF}*\u{FEFF}*\u{FEFF}*\u{FEFF}").unwrap();
        assert_eq!(fields.minute.len(), 60);
        assert_eq!(fields.day_of_week.len(), 7);
        assert_eq!(
            parse_cron("* * * * \u{FEFF}*\u{FEFF}")
                .unwrap()
                .day_of_week
                .len(),
            7
        );

        // NEL (U+0085) is not ECMAScript whitespace. It must remain inside
        // the token instead of being trimmed or splitting fields.
        assert!(
            parse_cron("\u{0085}* * * * *")
                .unwrap_err()
                .to_string()
                .contains("Invalid value")
        );
        assert!(
            parse_cron("* * * * \u{0085}*")
                .unwrap_err()
                .to_string()
                .contains("Invalid value")
        );
    }

    #[test]
    fn rejects_invalid_steps_and_malformed_tokens() {
        for expression in [
            "*/0 * * * *",
            "*/15garbage * * * *",
            "1-10/3x * * * *",
            "5/2x * * * *",
        ] {
            assert!(
                parse_cron(expression)
                    .unwrap_err()
                    .to_string()
                    .contains("Invalid step")
            );
        }
        assert!(
            parse_cron("5x * * * *")
                .unwrap_err()
                .to_string()
                .contains("Invalid value")
        );
        for expression in ["1-5x * * * *", "1-2-3 * * * *"] {
            assert!(
                parse_cron(expression)
                    .unwrap_err()
                    .to_string()
                    .contains("Invalid range")
            );
        }
        assert!(
            parse_cron("*/2/3 * * * *")
                .unwrap_err()
                .to_string()
                .contains("Invalid step expression")
        );
    }

    #[test]
    fn matches_time_and_calendar_fields_in_the_supplied_zone() {
        let wednesday = date(2025, 1, 15, 10, 30, 0);
        assert!(matches("* * * * *", &wednesday).unwrap());
        assert!(matches("30 * * * *", &wednesday).unwrap());
        assert!(!matches("31 * * * *", &wednesday).unwrap());
        assert!(matches("0 10 * * 3", &date(2025, 1, 15, 10, 0, 0)).unwrap());
        assert!(!matches("0 10 * * 1", &date(2025, 1, 15, 10, 0, 0)).unwrap());
        assert!(matches("5/15 * * * *", &date(2025, 1, 15, 10, 20, 0)).unwrap());
        assert!(!matches("*/5 * * * *", &date(2025, 1, 15, 10, 3, 0)).unwrap());
    }

    #[test]
    fn uses_vixie_or_when_both_day_fields_are_constrained() {
        let wednesday_jan_15 = date(2025, 1, 15, 10, 0, 0);
        assert!(matches("0 10 1 * 3", &wednesday_jan_15).unwrap());
        assert!(matches("0 10 15 * 1", &wednesday_jan_15).unwrap());
        assert!(!matches("0 10 1 * 1", &wednesday_jan_15).unwrap());
    }

    #[test]
    fn uses_vixie_and_when_either_day_field_begins_with_a_wildcard() {
        let wednesday_jan_15 = date(2025, 1, 15, 10, 0, 0);
        assert!(!matches("0 10 1 * *", &wednesday_jan_15).unwrap());
        assert!(!matches("0 10 * * 1", &wednesday_jan_15).unwrap());
        assert!(!matches("0 0 */2 * 1", &date(2025, 1, 1, 0, 0, 0)).unwrap());
        assert!(!matches("0 0 15 * */3", &date(2025, 1, 1, 0, 0, 0)).unwrap());
    }

    #[test]
    fn next_fire_time_starts_at_the_next_whole_minute_and_is_strictly_after() {
        let now = date(2025, 1, 15, 10, 30, 15);
        let next = next_fire_time("* * * * *", &now).unwrap();
        assert_eq!(next, date(2025, 1, 15, 10, 31, 0));

        let exactly_on_boundary = date(2025, 1, 15, 10, 0, 0);
        assert_eq!(
            next_fire_time("*/5 * * * *", &exactly_on_boundary).unwrap(),
            date(2025, 1, 15, 10, 5, 0)
        );
    }

    #[test]
    fn next_fire_time_rolls_across_hour_day_and_wildcard_day_rules() {
        assert_eq!(
            next_fire_time("45 * * * *", &date(2025, 1, 15, 10, 30, 0)).unwrap(),
            date(2025, 1, 15, 10, 45, 0)
        );
        assert_eq!(
            next_fire_time("15 * * * *", &date(2025, 1, 15, 10, 50, 0)).unwrap(),
            date(2025, 1, 15, 11, 15, 0)
        );
        assert_eq!(
            next_fire_time("0 9 * * *", &date(2025, 1, 15, 15, 0, 0)).unwrap(),
            date(2025, 1, 16, 9, 0, 0)
        );
        assert_eq!(
            next_fire_time("0 0 */2 * 1", &date(2024, 12, 31, 23, 59, 0)).unwrap(),
            date(2025, 1, 13, 0, 0, 0)
        );
        assert_eq!(
            next_fire_time("0 0 15 * */3", &date(2024, 12, 31, 23, 59, 0)).unwrap(),
            date(2025, 1, 15, 0, 0, 0)
        );
    }

    #[test]
    fn next_fire_time_returns_parse_and_search_errors() {
        assert!(matches!(
            next_fire_time("not a cron expression", &date(2025, 1, 1, 0, 0, 0)),
            Err(NextFireError::Parse(_))
        ));
        // February never has a thirty-first. The four-year bound avoids an
        // infinite search for a syntactically valid but impossible schedule.
        assert!(matches!(
            next_fire_time("0 0 31 2 *", &date(2025, 1, 1, 0, 0, 0)),
            Err(NextFireError::NoMatchingTime { .. })
        ));
    }

    #[test]
    fn date_time_helpers_keep_the_fixed_offset() {
        let offset = FixedOffset::east_opt(5 * 60 * 60).unwrap();
        let local = offset
            .with_ymd_and_hms(2025, 1, 15, 10, 30, 0)
            .single()
            .unwrap();
        let next = next_fire_time("* * * * *", &local).unwrap();
        assert_eq!(next.offset(), local.offset());
        assert_eq!((next.hour(), next.minute()), (10, 31));
    }
}
