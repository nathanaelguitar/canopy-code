//! Detection of the optional bare/simple Canopy mode.
//!
//! The environment lookup is supplied by the caller in [`is_bare_mode_with`],
//! so policy can be tested without mutating process-global environment state.

pub const CANOPY_CODE_SIMPLE_ENV_VAR: &str = "QWEN_CODE_SIMPLE";

/// Match the explicit truthy strings accepted by `packages/core/src/utils/bareMode.ts`.
///
/// Trimming uses ECMAScript's `String.trim()` whitespace set. In particular,
/// U+FEFF is trimmed and U+0085 is not.
pub fn is_truthy(value: Option<&str>) -> bool {
    let Some(value) = value else {
        return false;
    };
    let value = value.trim_matches(is_ecmascript_trim_whitespace);
    matches!(value.to_lowercase().as_str(), "1" | "true" | "yes" | "on")
}

/// Apply the CLI flag and read the simple-mode environment variable lazily.
///
/// Passing a lookup closure keeps environment access injectable. As in the
/// TypeScript short-circuit expression, `Some(true)` returns before the
/// environment is queried; `Some(false)` still allows the environment to
/// enable bare mode.
pub fn is_bare_mode_with<F>(cli_flag: Option<bool>, mut read_environment: F) -> bool
where
    F: FnMut(&str) -> Option<String>,
{
    cli_flag == Some(true) || is_truthy(read_environment(CANOPY_CODE_SIMPLE_ENV_VAR).as_deref())
}

/// Read the simple-mode environment variable from the current process.
pub fn is_bare_mode(cli_flag: Option<bool>) -> bool {
    is_bare_mode_with(cli_flag, |key| std::env::var(key).ok())
}

fn is_ecmascript_trim_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}' // CHARACTER TABULATION
            | '\u{000a}' // LINE FEED
            | '\u{000b}' // LINE TABULATION
            | '\u{000c}' // FORM FEED
            | '\u{000d}' // CARRIAGE RETURN
            | '\u{0020}' // SPACE
            | '\u{00a0}' // NO-BREAK SPACE
            | '\u{1680}' // OGHAM SPACE MARK
            | '\u{2000}'
            ..='\u{200a}' // EN QUAD .. HAIR SPACE
            | '\u{2028}' // LINE SEPARATOR
            | '\u{2029}' // PARAGRAPH SEPARATOR
            | '\u{202f}' // NARROW NO-BREAK SPACE
            | '\u{205f}' // MEDIUM MATHEMATICAL SPACE
            | '\u{3000}' // IDEOGRAPHIC SPACE
            | '\u{feff}' // ZERO WIDTH NO-BREAK SPACE / BOM
    )
}

#[cfg(test)]
mod tests {
    use super::{CANOPY_CODE_SIMPLE_ENV_VAR, is_bare_mode_with, is_truthy};

    #[test]
    fn recognizes_only_the_four_truthy_tokens_case_insensitively() {
        for token in ["1", "true", "yes", "on", "TrUe", " YES ", "\ton\n"] {
            assert!(is_truthy(Some(token)), "{token:?}");
        }
        for token in ["", "0", "false", "no", "off", "enabled", "true!", "on 1"] {
            assert!(!is_truthy(Some(token)), "{token:?}");
        }
        assert!(!is_truthy(None));
    }

    #[test]
    fn uses_ecmascript_whitespace_at_the_edges() {
        for token in [
            "\u{00a0}true\u{00a0}",
            "\u{1680}yes\u{1680}",
            "\u{2003}on\u{2003}",
            "\u{2028}1\u{2029}",
            "\u{feff}TRUE\u{feff}",
        ] {
            assert!(is_truthy(Some(token)), "{token:?}");
        }
        // Rust's general Unicode whitespace predicate accepts NEL; ECMAScript
        // String.trim() does not, so the token must stay false here.
        for token in ["\u{0085}true", "true\u{0085}", "\u{180e}yes", "on\u{200b}"] {
            assert!(!is_truthy(Some(token)), "{token:?}");
        }
        assert!(!is_truthy(Some("\u{3000}\u{feff}")));
    }

    #[test]
    fn cli_true_enables_mode_and_short_circuits_environment_lookup() {
        let mut queried = false;
        assert!(is_bare_mode_with(Some(true), |_| {
            queried = true;
            Some("false".to_owned())
        }));
        assert!(!queried);
    }

    #[test]
    fn false_or_missing_cli_flag_defers_to_the_injected_environment() {
        for cli_flag in [None, Some(false)] {
            let mut queried_key = None;
            assert!(is_bare_mode_with(cli_flag, |key| {
                queried_key = Some(key.to_owned());
                Some(" YES ".to_owned())
            }));
            assert_eq!(queried_key.as_deref(), Some(CANOPY_CODE_SIMPLE_ENV_VAR));

            assert!(!is_bare_mode_with(cli_flag, |_| Some("off".to_owned())));
            assert!(!is_bare_mode_with(cli_flag, |_| None));
        }
    }
}
