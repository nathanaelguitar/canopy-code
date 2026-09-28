//! OSC 8 hyperlink primitives and version-gated terminal detection.
//!
//! Detection reads the process environment on every call. Rust has no
//! Node-style arbitrary `WriteStream`, so callers can check stdout with
//! [`supports_hyperlinks`] or pass a stream's known TTY state to
//! [`supports_hyperlinks_for_stream`].

use std::io::IsTerminal;

/// Every environment variable inspected by [`supports_hyperlinks`].
pub const HYPERLINK_ENV_KEYS: &[&str] = &[
    "NO_COLOR",
    "FORCE_COLOR",
    "CI",
    "TMUX",
    "STY",
    "TERM_PROGRAM",
    "TERM_PROGRAM_VERSION",
    "WT_SESSION",
    "KITTY_WINDOW_ID",
    "VTE_VERSION",
    "DOMTERM",
    "GHOSTTY_RESOURCES_DIR",
    "KONSOLE_VERSION",
    "TERMINAL_EMULATOR",
    "ALACRITTY_LOG",
    "ALACRITTY_WINDOW_ID",
    "ALACRITTY_SOCKET",
    "TERM",
    "TEAMCITY_VERSION",
    "FORCE_HYPERLINK",
    "CANOPY_DISABLE_HYPERLINKS",
];

/// Wrap an OSC sequence for tmux or GNU screen, reading the environment on
/// each call. tmux has precedence if both `TMUX` and `STY` are set.
pub fn wrap_for_multiplexer(sequence: &str) -> String {
    wrap_for_multiplexer_with(sequence, |key| std::env::var(key).ok())
}

fn wrap_for_multiplexer_with<F>(sequence: &str, mut env: F) -> String
where
    F: FnMut(&str) -> Option<String>,
{
    if env("TMUX").is_some_and(|value| !value.is_empty()) {
        // tmux requires every ESC byte inside the payload to be doubled.
        return format!("\x1bPtmux;{}\x1b\\", sequence.replace('\x1b', "\x1b\x1b"));
    }
    if env("STY").is_some_and(|value| !value.is_empty()) {
        return format!("\x1bP{sequence}\x1b\\");
    }
    sequence.to_owned()
}

/// Remove controls that can terminate or fracture an OSC payload, plus bidi
/// formatting controls that can spoof the visible hyperlink label.
pub fn sanitize_for_osc(value: &str) -> String {
    value
        .chars()
        .filter(|character| {
            let code = *character as u32;
            !((0x00..=0x1f).contains(&code)
                || code == 0x7f
                || (0x80..=0x9f).contains(&code)
                || matches!(code, 0x200e | 0x200f | 0x2028 | 0x2029)
                || (0x202a..=0x202e).contains(&code)
                || (0x2066..=0x2069).contains(&code))
        })
        .collect()
}

/// Wrap a URL in an OSC 8 envelope. `None` uses the sanitized URL as the
/// label; `Some("")` intentionally produces an empty visible label.
pub fn osc8_hyperlink(url: &str, label: Option<&str>) -> String {
    let safe_url = sanitize_for_osc(url);
    let safe_label = sanitize_for_osc(label.unwrap_or(url));
    wrap_for_multiplexer(&format!("\x1b]8;;{safe_url}\x07{safe_label}\x1b]8;;\x07"))
}

/// Detect OSC 8 support for stdout. Environment values are read afresh on
/// every invocation and are not cached.
pub fn supports_hyperlinks() -> bool {
    supports_hyperlinks_for_stream(Some(std::io::stdout().is_terminal()))
}

/// Detect OSC 8 support for a stream with the supplied TTY state. `None` and
/// `Some(false)` both refuse hyperlinks, matching the source's missing/non-TTY
/// stream guard. Use [`supports_hyperlinks`] to detect stdout automatically.
pub fn supports_hyperlinks_for_stream(stream_is_tty: Option<bool>) -> bool {
    supports_hyperlinks_with(stream_is_tty, |key| std::env::var(key).ok(), cfg!(windows))
}

fn supports_hyperlinks_with<F>(stream_is_tty: Option<bool>, mut env: F, is_windows: bool) -> bool
where
    F: FnMut(&str) -> Option<String>,
{
    // Hard opt-outs win unconditionally.
    if env("CANOPY_DISABLE_HYPERLINKS").as_deref() == Some("1") {
        return false;
    }
    if env("NO_COLOR").is_some_and(|value| !value.is_empty()) {
        return false;
    }
    if matches!(env("FORCE_COLOR").as_deref(), Some("0" | "false")) {
        return false;
    }

    // Escapes must never be emitted to a file or pipe, even when forced.
    if stream_is_tty != Some(true) {
        return false;
    }

    // Explicit force overrides the terminal heuristics, but not opt-outs or
    // the TTY guard. Any non-zero integer, or an empty value, enables it.
    if let Some(force) = env("FORCE_HYPERLINK") {
        return should_force_hyperlinks(&force);
    }

    if is_truthy(&env("CI")) || is_truthy(&env("TEAMCITY_VERSION")) {
        return false;
    }

    // Multiplexers hide the host's terminal identity.
    if is_truthy(&env("TMUX")) || is_truthy(&env("STY")) {
        return false;
    }

    if is_truthy(&env("WT_SESSION")) {
        return true;
    }
    if is_truthy(&env("KITTY_WINDOW_ID")) || env("TERM").as_deref() == Some("xterm-kitty") {
        return true;
    }
    if is_truthy(&env("DOMTERM")) {
        return true;
    }
    if is_truthy(&env("GHOSTTY_RESOURCES_DIR")) || env("TERM").as_deref() == Some("xterm-ghostty") {
        return true;
    }

    if let Some(konsole_version) = env("KONSOLE_VERSION") {
        let parsed = parse_js_integer(&konsole_version);
        if parsed.is_some_and(|version| version.is_finite() && version >= 210400.0) {
            return true;
        }
    }

    if env("TERM").as_deref() == Some("alacritty")
        || env("ALACRITTY_LOG").is_some()
        || env("ALACRITTY_WINDOW_ID").is_some()
        || env("ALACRITTY_SOCKET").is_some()
    {
        return true;
    }
    if env("TERMINAL_EMULATOR").as_deref() == Some("JetBrains-JediTerm") {
        return true;
    }

    if let Some(program) = env("TERM_PROGRAM").filter(|value| !value.is_empty()) {
        let version = parse_version(env("TERM_PROGRAM_VERSION").as_deref());
        match program.as_str() {
            "iTerm.app" => {
                if version.major == 3.0 {
                    return version.minor >= 1.0;
                }
                return version.major > 3.0;
            }
            "WezTerm" => return version.major >= 20200620.0,
            "vscode" => {
                return version.major > 1.0 || (version.major == 1.0 && version.minor >= 72.0);
            }
            "ghostty" => return true,
            "mintty" => {
                if env("TERM_PROGRAM_VERSION")
                    .as_deref()
                    .is_none_or(str::is_empty)
                {
                    return false;
                }
                return version.major > 3.0 || (version.major == 3.0 && version.minor >= 3.0);
            }
            // Warp does not support OSC 8. Hyper and unknown programs require
            // explicit force because escape passthrough is not dependable.
            _ => {}
        }
    }

    if let Some(vte_version) = env("VTE_VERSION").filter(|value| !value.is_empty()) {
        let version = parse_version(Some(&vte_version));
        // VTE 0.50.0 advertises support but crashes when OSC 8 is emitted.
        if version.major == 0.0 && version.minor == 50.0 && version.patch == 0.0 {
            return false;
        }
        if version.major > 0.0 || version.minor >= 50.0 {
            return true;
        }
        return false;
    }

    // Legacy Windows console hosts do not support OSC 8 outside Windows
    // Terminal. The fallback is false on other unknown terminal programs too.
    if is_windows {
        return false;
    }
    false
}

fn is_truthy(value: &Option<String>) -> bool {
    value.as_ref().is_some_and(|value| !value.is_empty())
}

fn should_force_hyperlinks(value: &str) -> bool {
    if value.is_empty() {
        return true;
    }
    let trimmed = trim_ecmascript_whitespace(value);
    let digits = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    trimmed.parse::<f64>().is_ok_and(|number| number != 0.0)
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct ParsedVersion {
    major: f64,
    minor: f64,
    patch: f64,
}

fn parse_version(version: Option<&str>) -> ParsedVersion {
    let Some(version) = version.filter(|version| !version.is_empty()) else {
        return ParsedVersion::default();
    };

    if (3..=4).contains(&version.len()) && version.bytes().all(|byte| byte.is_ascii_digit()) {
        let split_at = version.len() - 2;
        let minor = parse_js_integer(&version[..split_at]).unwrap_or(0.0);
        let patch = parse_js_integer(&version[split_at..]).unwrap_or(0.0);
        return ParsedVersion {
            major: 0.0,
            minor,
            patch,
        };
    }

    let mut parts = version.split('.').map(|part| {
        parse_js_integer(part)
            .filter(|number| *number != 0.0 && !number.is_nan())
            .unwrap_or(0.0)
    });
    ParsedVersion {
        major: parts.next().unwrap_or(0.0),
        minor: parts.next().unwrap_or(0.0),
        patch: parts.next().unwrap_or(0.0),
    }
}

/// Parse an integer prefix like `Number.parseInt(value, 10)`, using IEEE-754
/// numbers just as JavaScript does. Non-numeric suffixes are ignored.
fn parse_js_integer(value: &str) -> Option<f64> {
    let trimmed = trim_ecmascript_whitespace_start(value);
    let (negative, digits) = match trimmed.as_bytes().first() {
        Some(b'+') => (false, &trimmed[1..]),
        Some(b'-') => (true, &trimmed[1..]),
        _ => (false, trimmed),
    };
    let digit_count = digits.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        return None;
    }
    let mut numeric = String::with_capacity(digit_count + usize::from(negative));
    if negative {
        numeric.push('-');
    }
    numeric.push_str(&digits[..digit_count]);
    numeric.parse::<f64>().ok()
}

fn trim_ecmascript_whitespace(value: &str) -> &str {
    value.trim_matches(is_ecmascript_whitespace)
}

fn trim_ecmascript_whitespace_start(value: &str) -> &str {
    value.trim_start_matches(is_ecmascript_whitespace)
}

fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000A}'
            | '\u{000B}'
            | '\u{000C}'
            | '\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200A}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202F}'
                | '\u{205F}'
                | '\u{3000}'
                | '\u{FEFF}'
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn env(values: &[(&str, &str)]) -> HashMap<String, String> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn detect(values: &[(&str, &str)]) -> bool {
        let values = env(values);
        supports_hyperlinks_with(Some(true), |key| values.get(key).cloned(), false)
    }

    #[test]
    fn sanitizes_envelope_breakers_bidi_controls_and_line_separators() {
        assert_eq!(sanitize_for_osc("a\x07b\x1bc"), "abc");
        assert_eq!(sanitize_for_osc("safe\u{202e}kcatta.com"), "safekcatta.com");
        assert_eq!(sanitize_for_osc("a\u{0080}\u{009f}\u{2028}\u{2029}b"), "ab");
        assert_eq!(
            sanitize_for_osc("https://example.com/a?b=c"),
            "https://example.com/a?b=c"
        );
    }

    #[test]
    fn builds_osc8_envelopes_and_uses_url_as_default_label() {
        let url = "https://example.com/x";
        assert_eq!(
            osc8_hyperlink(url, None),
            format!("\x1b]8;;{url}\x07{url}\x1b]8;;\x07")
        );
        assert_eq!(
            osc8_hyperlink("https://example.com", Some("click")),
            "\x1b]8;;https://example.com\x07click\x1b]8;;\x07"
        );
        assert_eq!(
            osc8_hyperlink("https://x\x07", Some("label\x1b")),
            "\x1b]8;;https://x\x07label\x1b]8;;\x07"
        );
        assert_eq!(
            osc8_hyperlink("https://example.com", Some("")),
            "\x1b]8;;https://example.com\x07\x1b]8;;\x07"
        );
    }

    #[test]
    fn wraps_passthrough_for_tmux_and_screen() {
        let sequence = "\x1b]8;;https://x\x07";
        assert_eq!(wrap_for_multiplexer_with(sequence, |_| None), sequence);
        let tmux = env(&[("TMUX", "/tmp/tmux/default,1,0")]);
        assert_eq!(
            wrap_for_multiplexer_with(sequence, |key| tmux.get(key).cloned()),
            "\x1bPtmux;\x1b\x1b]8;;https://x\x07\x1b\\"
        );
        let screen = env(&[("STY", "1234.pts-0.host")]);
        assert_eq!(
            wrap_for_multiplexer_with(sequence, |key| screen.get(key).cloned()),
            format!("\x1bP{sequence}\x1b\\")
        );
    }

    #[test]
    fn tmux_wrapper_wins_when_both_multiplexer_markers_are_set() {
        let values = env(&[("TMUX", "session"), ("STY", "screen")]);
        assert_eq!(
            wrap_for_multiplexer_with("\x1bX", |key| values.get(key).cloned()),
            "\x1bPtmux;\x1b\x1bX\x1b\\"
        );
        assert_eq!(wrap_for_multiplexer_with("x", |_| Some(String::new())), "x");
    }

    #[test]
    fn refuses_absent_and_non_tty_streams_even_when_forced() {
        let values = env(&[("FORCE_HYPERLINK", "1")]);
        let get = |key: &str| values.get(key).cloned();
        assert!(!supports_hyperlinks_with(None, get, false));
        assert!(!supports_hyperlinks_with(Some(false), get, false));
    }

    #[test]
    fn applies_hard_opt_outs_before_force_and_terminal_heuristics() {
        assert!(!detect(&[
            ("CANOPY_DISABLE_HYPERLINKS", "1"),
            ("FORCE_HYPERLINK", "1")
        ]));
        assert!(!detect(&[("NO_COLOR", "1"), ("FORCE_HYPERLINK", "1")]));
        assert!(!detect(&[("FORCE_COLOR", "0"), ("FORCE_HYPERLINK", "1")]));
        assert!(!detect(&[
            ("FORCE_COLOR", "false"),
            ("FORCE_HYPERLINK", "1")
        ]));
        assert!(detect(&[("FORCE_HYPERLINK", "1"), ("WT_SESSION", "yes")]));
        assert!(detect(&[("NO_COLOR", ""), ("FORCE_HYPERLINK", "1")]));
        assert!(detect(&[
            ("FORCE_COLOR", "False"),
            ("FORCE_HYPERLINK", "1")
        ]));
    }

    #[test]
    fn interprets_force_hyperlink_as_empty_or_nonzero_ascii_integer() {
        for enabled in [
            "",
            "1",
            "+1",
            "-1",
            "2",
            "  +7\u{feff}",
            "999999999999999999999999999",
        ] {
            assert!(detect(&[("FORCE_HYPERLINK", enabled)]), "{enabled:?}");
        }
        for disabled in ["0", "-0", "+000", "0000", " 0 ", "no", "1.0", "١"] {
            assert!(!detect(&[("FORCE_HYPERLINK", disabled)]), "{disabled:?}");
        }
        assert!(!should_force_hyperlinks("\u{feff}"));
        assert!(!should_force_hyperlinks("\u{feff}0\u{feff}"));
    }

    #[test]
    fn refuses_ci_teamcity_and_multiplexers_unless_forced() {
        assert!(!detect(&[("CI", "true"), ("WT_SESSION", "yes")]));
        assert!(!detect(&[
            ("TEAMCITY_VERSION", "2025.1"),
            ("WT_SESSION", "yes")
        ]));
        assert!(!detect(&[("TMUX", "session"), ("WT_SESSION", "yes")]));
        assert!(!detect(&[("STY", "screen"), ("WT_SESSION", "yes")]));
        assert!(detect(&[("CI", ""), ("FORCE_HYPERLINK", "1")]));
        assert!(detect(&[("TMUX", "session"), ("FORCE_HYPERLINK", "1")]));
    }

    #[test]
    fn detects_terminals_from_unversioned_capability_environment() {
        for (key, value) in [
            ("WT_SESSION", "1"),
            ("KITTY_WINDOW_ID", "1"),
            ("DOMTERM", "1"),
            ("GHOSTTY_RESOURCES_DIR", "/ghostty"),
            ("ALACRITTY_LOG", ""),
            ("ALACRITTY_WINDOW_ID", ""),
            ("ALACRITTY_SOCKET", ""),
            ("TERMINAL_EMULATOR", "JetBrains-JediTerm"),
        ] {
            assert!(detect(&[(key, value)]), "{key}={value:?}");
        }
        assert!(detect(&[("TERM", "xterm-kitty")]));
        assert!(detect(&[("TERM", "xterm-ghostty")]));
        assert!(detect(&[("TERM", "alacritty")]));
        assert!(!detect(&[("WT_SESSION", "")]));
    }

    #[test]
    fn version_gates_konsole_and_known_terminal_programs() {
        assert!(detect(&[("KONSOLE_VERSION", "210400")]));
        assert!(detect(&[("KONSOLE_VERSION", "230805")]));
        assert!(!detect(&[("KONSOLE_VERSION", "210399")]));
        assert!(!detect(&[("KONSOLE_VERSION", "not-a-version")]));

        for version in ["3.1", "4.0"] {
            assert!(detect(&[
                ("TERM_PROGRAM", "iTerm.app"),
                ("TERM_PROGRAM_VERSION", version)
            ]));
        }
        for version in ["0.9", "3.0"] {
            assert!(!detect(&[
                ("TERM_PROGRAM", "iTerm.app"),
                ("TERM_PROGRAM_VERSION", version)
            ]));
        }
        assert!(!detect(&[("TERM_PROGRAM", "iTerm.app")]));

        assert!(detect(&[
            ("TERM_PROGRAM", "WezTerm"),
            ("TERM_PROGRAM_VERSION", "20200620")
        ]));
        assert!(!detect(&[
            ("TERM_PROGRAM", "WezTerm"),
            ("TERM_PROGRAM_VERSION", "20200619")
        ]));
        assert!(detect(&[
            ("TERM_PROGRAM", "vscode"),
            ("TERM_PROGRAM_VERSION", "1.72.0")
        ]));
        assert!(!detect(&[
            ("TERM_PROGRAM", "vscode"),
            ("TERM_PROGRAM_VERSION", "1.71.9")
        ]));
        assert!(detect(&[
            ("TERM_PROGRAM", "vscode"),
            ("TERM_PROGRAM_VERSION", "2.0")
        ]));
        assert!(detect(&[("TERM_PROGRAM", "ghostty")]));
    }

    #[test]
    fn mintty_requires_a_hardened_version_and_warp_remains_unsupported() {
        assert!(!detect(&[("TERM_PROGRAM", "mintty")]));
        assert!(!detect(&[
            ("TERM_PROGRAM", "mintty"),
            ("TERM_PROGRAM_VERSION", "3.1.0")
        ]));
        assert!(!detect(&[
            ("TERM_PROGRAM", "mintty"),
            ("TERM_PROGRAM_VERSION", "3.2.9")
        ]));
        assert!(detect(&[
            ("TERM_PROGRAM", "mintty"),
            ("TERM_PROGRAM_VERSION", "3.3.0")
        ]));
        assert!(detect(&[
            ("TERM_PROGRAM", "mintty"),
            ("TERM_PROGRAM_VERSION", "4.0.0")
        ]));
        assert!(!detect(&[
            ("TERM_PROGRAM", "WarpTerminal"),
            ("TERM_PROGRAM_VERSION", "1.0")
        ]));
        assert!(!detect(&[
            ("TERM_PROGRAM", "Hyper"),
            ("TERM_PROGRAM_VERSION", "4.0")
        ]));
    }

    #[test]
    fn vte_heuristic_accepts_supported_versions_except_the_crashing_release() {
        for version in ["0.50.1", "0.78.0", "1.0.0", "5001", "7800"] {
            assert!(detect(&[("VTE_VERSION", version)]), "{version}");
        }
        for version in ["0.49.0", "0.50.0", "5000", "not-a-version"] {
            assert!(!detect(&[("VTE_VERSION", version)]), "{version}");
        }
    }

    #[test]
    fn parse_version_matches_dot_and_packed_vte_forms() {
        assert_eq!(
            parse_version(Some("5000")),
            ParsedVersion {
                major: 0.0,
                minor: 50.0,
                patch: 0.0
            }
        );
        assert_eq!(
            parse_version(Some("7800")),
            ParsedVersion {
                major: 0.0,
                minor: 78.0,
                patch: 0.0
            }
        );
        assert_eq!(
            parse_version(Some("0.50.1")),
            ParsedVersion {
                major: 0.0,
                minor: 50.0,
                patch: 1.0
            }
        );
        assert_eq!(
            parse_version(Some("3.3.9-extra")),
            ParsedVersion {
                major: 3.0,
                minor: 3.0,
                patch: 9.0
            }
        );
        assert_eq!(parse_version(Some("")), ParsedVersion::default());
    }

    #[test]
    fn reads_environment_again_for_each_detection_and_wrapper_call() {
        let mut forced = true;
        assert!(supports_hyperlinks_with(
            Some(true),
            |key| (forced && key == "FORCE_HYPERLINK").then(|| "1".to_owned()),
            false
        ));
        forced = false;
        assert!(!supports_hyperlinks_with(
            Some(true),
            |key| (forced && key == "FORCE_HYPERLINK").then(|| "1".to_owned()),
            false
        ));

        let mut tmux = true;
        assert!(
            wrap_for_multiplexer_with("x", |key| {
                (key == "TMUX" && tmux).then(|| "session".to_owned())
            })
            .starts_with("\x1bPtmux;")
        );
        tmux = false;
        assert_eq!(
            wrap_for_multiplexer_with("x", |key| {
                (key == "TMUX" && tmux).then(|| "session".to_owned())
            }),
            "x"
        );
    }

    #[test]
    fn platform_fallback_is_false_for_unknown_programs() {
        let values = env(&[("TERM", "unknown")]);
        assert!(!supports_hyperlinks_with(
            Some(true),
            |key| values.get(key).cloned(),
            true
        ));
        assert!(!supports_hyperlinks_with(
            Some(true),
            |key| values.get(key).cloned(),
            false
        ));
    }
}
