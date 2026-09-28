//! Human-readable labels for a small set of common recurring cron patterns.
//!
//! This mirrors `packages/core/src/utils/cronDisplay.ts`. Anything malformed
//! or outside the recognized patterns is returned verbatim. Step labels are
//! emitted only when the step describes the true interval across a field
//! boundary (and, for days, across month lengths).

/// Returns a friendly label for common step schedules, otherwise `cron_expr`.
pub fn human_readable_cron(cron_expr: &str) -> String {
    let mut parts: Vec<&str> =
        split_ecmascript_whitespace(trim_ecmascript_whitespace(cron_expr)).collect();
    if parts.is_empty() {
        parts.push("");
    }
    if parts.len() != 5 {
        return cron_expr.to_owned();
    }

    let [minute, hour, day_of_month, month, day_of_week] =
        <[&str; 5]>::try_from(parts.as_slice()).expect("the expression has five fields");

    if minute.starts_with("*/")
        && hour == "*"
        && day_of_month == "*"
        && month == "*"
        && day_of_week == "*"
    {
        if let Some(step) = even_step_of(&minute[2..], 60) {
            return if step == 1 {
                "Every minute".to_owned()
            } else {
                format!("Every {step} minutes")
            };
        }
    }

    if is_ascii_digits(minute)
        && hour.starts_with("*/")
        && day_of_month == "*"
        && month == "*"
        && day_of_week == "*"
    {
        if let Some(step) = even_step_of(&hour[2..], 24) {
            return if step == 1 {
                "Every hour".to_owned()
            } else {
                format!("Every {step} hours")
            };
        }
    }

    if is_ascii_digits(minute)
        && is_ascii_digits(hour)
        && day_of_month.starts_with("*/")
        && month == "*"
        && day_of_week == "*"
        && parse_positive_integer(&day_of_month[2..]) == Some(1)
    {
        return "Every day".to_owned();
    }

    cron_expr.to_owned()
}

fn is_ascii_digits(token: &str) -> bool {
    !token.is_empty() && token.bytes().all(|byte| byte.is_ascii_digit())
}

fn parse_positive_integer(token: &str) -> Option<u64> {
    if !is_ascii_digits(token) {
        return None;
    }
    let value = token.bytes().fold(0_u64, |value, byte| {
        value
            .saturating_mul(10)
            .saturating_add((byte - b'0') as u64)
    });
    (value > 0).then_some(value)
}

fn even_step_of(token: &str, unit: u64) -> Option<u64> {
    let step = parse_positive_integer(token)?;
    (step < unit && unit % step == 0).then_some(step)
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_common_step_expressions() {
        assert_eq!(human_readable_cron("*/15 * * * *"), "Every 15 minutes");
        assert_eq!(human_readable_cron("0 */2 * * *"), "Every 2 hours");
        assert_eq!(human_readable_cron("0 0 */1 * *"), "Every day");
    }

    #[test]
    fn falls_back_for_malformed_steps() {
        for expression in [
            "*/15x * * * *",
            "*/0 * * * *",
            "0 */2x * * *",
            "0 0 */3x * *",
        ] {
            assert_eq!(human_readable_cron(expression), expression);
        }
    }

    #[test]
    fn falls_back_for_large_full_field_and_uneven_steps() {
        for expression in [
            "*/90 * * * *",
            "0 */30 * * *",
            "0 0 */40 * *",
            "*/60 * * * *",
            "0 */24 * * *",
            "*/25 * * * *",
            "0 */7 * * *",
            "0 0 */2 * *",
            "0 0 */3 * *",
            "0 0 */15 * *",
            "0 0 */16 * *",
            "0 0 */31 * *",
        ] {
            assert_eq!(human_readable_cron(expression), expression);
        }
    }

    #[test]
    fn keeps_labels_for_true_intervals() {
        for (expression, label) in [
            ("*/1 * * * *", "Every minute"),
            ("*/20 * * * *", "Every 20 minutes"),
            ("*/30 * * * *", "Every 30 minutes"),
            ("0 */1 * * *", "Every hour"),
            ("0 */6 * * *", "Every 6 hours"),
            ("0 */12 * * *", "Every 12 hours"),
            ("0 0 */1 * *", "Every day"),
        ] {
            assert_eq!(human_readable_cron(expression), label);
        }
    }

    #[test]
    fn falls_back_for_bad_field_counts_and_preserves_expression() {
        for expression in ["", "   ", "* * *", "* * * * * *", "  */5 * * *  "] {
            assert_eq!(human_readable_cron(expression), expression);
        }
    }

    #[test]
    fn recognizes_ecmascript_field_separators() {
        assert_eq!(
            human_readable_cron("\u{feff}*/15\u{feff}* * * *"),
            "Every 15 minutes"
        );
        let non_ecmascript_separator = "*/15\u{0085}* * * *";
        assert_eq!(
            human_readable_cron(non_ecmascript_separator),
            non_ecmascript_separator
        );
    }
}
