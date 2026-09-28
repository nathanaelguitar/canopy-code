//! Stop-hook continuation caps.
//!
//! Port of `packages/core/src/hooks/stopHookCap.ts`.

/// Default number of consecutive Stop-hook blocks allowed before ending a turn.
pub const DEFAULT_STOP_HOOK_BLOCK_CAP: u32 = 8;
/// Upper bound for configured Stop-hook block caps.
pub const MAX_STOP_HOOK_BLOCK_CAP: u32 = 100;
/// Environment variable that overrides the configured cap.
pub const STOP_HOOK_BLOCK_CAP_ENV: &str = "CANOPY_CODE_STOP_HOOK_BLOCK_CAP";

/// Normalize a numeric configuration value to the supported whole-number cap.
///
/// `None` represents an absent value. Non-finite values and values below one
/// use the default; fractional values are floored before applying the maximum.
pub fn normalize_stop_hook_blocking_cap(value: Option<f64>) -> u32 {
    let Some(value) = value.filter(|value| value.is_finite()) else {
        return DEFAULT_STOP_HOOK_BLOCK_CAP;
    };
    let normalized = value.floor();
    if normalized < 1.0 {
        DEFAULT_STOP_HOOK_BLOCK_CAP
    } else if normalized >= f64::from(MAX_STOP_HOOK_BLOCK_CAP) {
        MAX_STOP_HOOK_BLOCK_CAP
    } else {
        normalized as u32
    }
}

/// Resolve the cap, preferring a non-blank environment override to config.
///
/// Environment parsing follows JavaScript `Number(value)` and
/// `Number.isInteger`: decimal and unsigned binary/octal/hex forms are
/// accepted, while malformed and fractional values select the default.
pub fn resolve_stop_hook_blocking_cap(config_value: Option<f64>) -> u32 {
    let env_value = std::env::var(STOP_HOOK_BLOCK_CAP_ENV).ok();
    resolve_stop_hook_blocking_cap_with_env(config_value, env_value.as_deref())
}

fn resolve_stop_hook_blocking_cap_with_env(
    config_value: Option<f64>,
    env_value: Option<&str>,
) -> u32 {
    if let Some(value) = env_value.filter(|value| !js_trim(value).is_empty()) {
        return parse_stop_hook_blocking_cap_env(value);
    }
    normalize_stop_hook_blocking_cap(config_value)
}

fn parse_stop_hook_blocking_cap_env(value: &str) -> u32 {
    match parse_js_number(value) {
        Some(parsed) if parsed.is_finite() && parsed.fract() == 0.0 => {
            normalize_stop_hook_blocking_cap(Some(parsed))
        }
        _ => DEFAULT_STOP_HOOK_BLOCK_CAP,
    }
}

fn parse_js_number(value: &str) -> Option<f64> {
    let value = js_trim(value);
    if value.is_empty() {
        return Some(0.0);
    }
    if matches!(value, "Infinity" | "+Infinity") {
        return Some(f64::INFINITY);
    }
    if value == "-Infinity" {
        return Some(f64::NEG_INFINITY);
    }

    let bytes = value.as_bytes();
    if bytes.len() >= 2 && bytes[0] == b'0' {
        let radix = match bytes[1] {
            b'x' | b'X' => Some(16_u32),
            b'b' | b'B' => Some(2),
            b'o' | b'O' => Some(8),
            _ => None,
        };
        if let Some(radix) = radix {
            return parse_radix_number(&bytes[2..], radix);
        }
    }

    if !is_js_decimal_number(value) {
        return None;
    }
    value.parse().ok()
}

fn parse_radix_number(digits: &[u8], radix: u32) -> Option<f64> {
    if digits.is_empty() {
        return None;
    }
    let mut result = 0.0_f64;
    for byte in digits {
        let digit = match byte {
            b'0'..=b'9' => u32::from(*byte - b'0'),
            b'a'..=b'f' => u32::from(*byte - b'a') + 10,
            b'A'..=b'F' => u32::from(*byte - b'A') + 10,
            _ => return None,
        };
        if digit >= radix {
            return None;
        }
        result = result * f64::from(radix) + f64::from(digit);
    }
    Some(result)
}

fn is_js_decimal_number(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut cursor = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let integer_digits = consume_digits(bytes, &mut cursor);
    let fraction_digits = if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        consume_digits(bytes, &mut cursor)
    } else {
        0
    };
    if integer_digits + fraction_digits == 0 {
        return false;
    }
    if matches!(bytes.get(cursor), Some(b'e' | b'E')) {
        cursor += 1;
        if matches!(bytes.get(cursor), Some(b'+' | b'-')) {
            cursor += 1;
        }
        if consume_digits(bytes, &mut cursor) == 0 {
            return false;
        }
    }
    cursor == bytes.len()
}

fn consume_digits(bytes: &[u8], cursor: &mut usize) -> usize {
    let start = *cursor;
    while bytes.get(*cursor).is_some_and(u8::is_ascii_digit) {
        *cursor += 1;
    }
    *cursor - start
}

fn js_trim(value: &str) -> &str {
    value.trim_matches(|character: char| {
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
    })
}

/// Names accepted by [`format_stop_hook_blocking_cap_warning`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopHookLabel {
    Stop,
    SubagentStop,
}

/// Format the user-visible warning emitted after the cap is reached.
pub fn format_stop_hook_blocking_cap_warning(hook: StopHookLabel, cap: u32) -> String {
    let hook_name = match hook {
        StopHookLabel::Stop => "Stop hook",
        StopHookLabel::SubagentStop => "SubagentStop hook",
    };
    let times_word = if cap == 1 { "time" } else { "times" };
    format!(
        "{hook_name} blocked continuation {cap} consecutive {times_word}; overriding and ending the turn."
    )
}

/// Append a warning after visible text, separated by one blank line.
pub fn append_stop_hook_blocking_cap_warning(text: &str, warning: Option<&str>) -> String {
    let Some(warning) = warning.filter(|warning| !warning.is_empty()) else {
        return text.to_owned();
    };
    if text.is_empty() {
        warning.to_owned()
    } else {
        format!("{text}\n\n{warning}")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_STOP_HOOK_BLOCK_CAP, MAX_STOP_HOOK_BLOCK_CAP, StopHookLabel,
        append_stop_hook_blocking_cap_warning, format_stop_hook_blocking_cap_warning,
        normalize_stop_hook_blocking_cap, parse_stop_hook_blocking_cap_env,
        resolve_stop_hook_blocking_cap_with_env,
    };

    #[test]
    fn normalizes_absent_invalid_and_below_minimum_values_to_default() {
        for value in [
            None,
            Some(0.0),
            Some(-1.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
        ] {
            assert_eq!(
                normalize_stop_hook_blocking_cap(value),
                DEFAULT_STOP_HOOK_BLOCK_CAP
            );
        }
    }

    #[test]
    fn floors_fractional_config_values_and_caps_large_values() {
        assert_eq!(normalize_stop_hook_blocking_cap(Some(3.7)), 3);
        assert_eq!(
            normalize_stop_hook_blocking_cap(Some(100.9)),
            MAX_STOP_HOOK_BLOCK_CAP
        );
        assert_eq!(
            normalize_stop_hook_blocking_cap(Some(99_999.0)),
            MAX_STOP_HOOK_BLOCK_CAP
        );
    }

    #[test]
    fn environment_override_precedes_config_and_blank_values_are_ignored() {
        assert_eq!(
            resolve_stop_hook_blocking_cap_with_env(Some(12.0), Some("3")),
            3
        );
        assert_eq!(
            resolve_stop_hook_blocking_cap_with_env(Some(12.0), Some("  \t")),
            12
        );
        assert_eq!(
            resolve_stop_hook_blocking_cap_with_env(None, None),
            DEFAULT_STOP_HOOK_BLOCK_CAP
        );
    }

    #[test]
    fn environment_accepts_integer_number_syntax_and_rejects_fractional_or_invalid_values() {
        for (input, expected) in [
            ("3", 3),
            ("1.0", 1),
            ("1e2", 100),
            ("0x10", 16),
            ("0b11", 3),
            ("0o10", 8),
        ] {
            assert_eq!(parse_stop_hook_blocking_cap_env(input), expected, "{input}");
        }
        for input in ["1.5", "nope", "+0x10", "0x", "1e309"] {
            assert_eq!(
                parse_stop_hook_blocking_cap_env(input),
                DEFAULT_STOP_HOOK_BLOCK_CAP,
                "{input}"
            );
        }
        assert_eq!(
            parse_stop_hook_blocking_cap_env("99999"),
            MAX_STOP_HOOK_BLOCK_CAP
        );
    }

    #[test]
    fn warning_wording_and_singular_plural_match() {
        assert_eq!(
            format_stop_hook_blocking_cap_warning(StopHookLabel::Stop, 8),
            "Stop hook blocked continuation 8 consecutive times; overriding and ending the turn."
        );
        assert_eq!(
            format_stop_hook_blocking_cap_warning(StopHookLabel::SubagentStop, 1),
            "SubagentStop hook blocked continuation 1 consecutive time; overriding and ending the turn."
        );
    }

    #[test]
    fn appends_warning_only_when_present_and_nonempty() {
        assert_eq!(append_stop_hook_blocking_cap_warning("done", None), "done");
        assert_eq!(
            append_stop_hook_blocking_cap_warning("done", Some("")),
            "done"
        );
        assert_eq!(
            append_stop_hook_blocking_cap_warning("", Some("warning")),
            "warning"
        );
        assert_eq!(
            append_stop_hook_blocking_cap_warning("done", Some("warning")),
            "done\n\nwarning"
        );
    }
}
