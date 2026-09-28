//! Terminal and compact-display sanitizers for untrusted text.
//!
//! Mirrors `packages/core/src/utils/terminalSafe.ts`, including its separate
//! policy for TTY escape sequences and notification labels.

use regex::Regex;
use std::sync::LazyLock;

/// OSC sequences terminated by BEL or the two-byte ST sequence.
pub static TERMINAL_OSC_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)").expect("constant OSC regex is valid")
});

/// Common CSI cursor, color, and erase sequences.
pub static TERMINAL_CSI_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\x1b\[[0-9;?]*[a-zA-Z]").expect("constant CSI regex is valid"));

/// SS2, SS3, and DCS leader sequences.
pub static TERMINAL_SHIFT_DCS_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\x1b[NOP]").expect("constant shift/DCS regex is valid"));

/// Maximum code-point count for a background-notification label.
pub const NOTIFICATION_LABEL_MAX_LENGTH: usize = 80;

/// Strip recognized terminal sequences and replace all remaining C0/C1
/// controls and DEL with spaces. Every raw ESC and single-byte terminal C1
/// character is removed from the result.
pub fn strip_terminal_control_sequences(text: &str) -> String {
    let text = TERMINAL_OSC_REGEX.replace_all(text, " ");
    let text = TERMINAL_CSI_REGEX.replace_all(&text, " ");
    let text = TERMINAL_SHIFT_DCS_REGEX.replace_all(&text, " ");
    text.chars()
        .map(|character| {
            let code = character as u32;
            if code <= 0x1f || (0x7f..=0x9f).contains(&code) {
                ' '
            } else {
                character
            }
        })
        .collect()
}

/// Returns whether a code point is a bidi embedding, override, or isolate.
pub fn is_bidi_control_char(code: u32) -> bool {
    (0x202a..=0x202e).contains(&code) || (0x2066..=0x2069).contains(&code)
}

/// Strip C0 controls except TAB, C1 controls, and bidi override/isolate
/// characters. DEL is preserved, matching the source utility's ranges.
pub fn strip_display_control_chars(text: &str) -> String {
    text.chars()
        .filter(|character| {
            let code = *character as u32;
            code == 0x09
                || (code >= 0x20 && !(0x80..=0x9f).contains(&code) && !is_bidi_control_char(code))
        })
        .collect()
}

/// Normalize a label to one line and truncate it to at most 80 Unicode scalar
/// values, appending `...` when it exceeds the cap.
pub fn truncate_notification_label(label: &str) -> String {
    let stripped = strip_display_control_chars(label);
    let mut normalized = String::with_capacity(stripped.len());
    let mut pending_space = false;
    for character in stripped.chars() {
        if is_ecmascript_whitespace(character) {
            if !normalized.is_empty() {
                pending_space = true;
            }
        } else {
            if pending_space {
                normalized.push(' ');
                pending_space = false;
            }
            normalized.push(character);
        }
    }

    let mut characters = normalized.chars();
    let prefix: String = characters
        .by_ref()
        .take(NOTIFICATION_LABEL_MAX_LENGTH)
        .collect();
    if characters.next().is_none() {
        return normalized;
    }

    let prefix: String = prefix
        .chars()
        .take(NOTIFICATION_LABEL_MAX_LENGTH - 3)
        .collect();
    format!("{prefix}...")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_control_filter_preserves_ascii_and_tab() {
        assert_eq!(
            strip_display_control_chars("hello\tworld 123"),
            "hello\tworld 123"
        );
    }

    #[test]
    fn display_control_filter_removes_c0_and_c1_but_keeps_del() {
        assert_eq!(
            strip_display_control_chars("a\0b\u{0007}c\u{001b}d\ne\rf\u{0008}g"),
            "abcdefg"
        );
        assert_eq!(
            strip_display_control_chars("before\u{0085}mid\u{009b}after"),
            "beforemidafter"
        );
        assert_eq!(strip_display_control_chars("a\u{007f}b"), "a\u{007f}b");
    }

    #[test]
    fn display_control_filter_removes_bidi_ranges_and_keeps_neighbors() {
        assert_eq!(
            strip_display_control_chars("safe\u{202a}danger\u{202c}more\u{202e}trojan\u{202c}"),
            "safedangermoretrojan"
        );
        assert_eq!(
            strip_display_control_chars("a\u{2066}b\u{2067}c\u{2068}d\u{2069}e"),
            "abcde"
        );
        assert_eq!(
            strip_display_control_chars("a\u{2029}b\u{202f}c\u{2065}d\u{206a}e"),
            "a\u{2029}b\u{202f}c\u{2065}d\u{206a}e"
        );
    }

    #[test]
    fn bidi_predicate_matches_only_embedding_override_and_isolate_ranges() {
        for code in 0x202a..=0x202e {
            assert!(is_bidi_control_char(code));
        }
        for code in 0x2066..=0x2069 {
            assert!(is_bidi_control_char(code));
        }
        for code in [0x2029, 0x202f, 0x2065, 0x206a] {
            assert!(!is_bidi_control_char(code));
        }
    }

    #[test]
    fn display_control_filter_handles_trojan_source_and_is_idempotent() {
        let trojan = "/*\u{202e} } if (isAdmin) begin admin only \u{202c}*/";
        assert_eq!(
            strip_display_control_chars(trojan),
            "/* } if (isAdmin) begin admin only */"
        );
        let input = "a\0b\u{202e}c\u{0085}d\u{2068}e";
        let once = strip_display_control_chars(input);
        assert_eq!(strip_display_control_chars(&once), once);
        assert_eq!(strip_display_control_chars(""), "");
    }

    #[test]
    fn terminal_escape_filter_replaces_osc_csi_shift_and_remaining_controls() {
        let input = "a\x1b[31mb\x1b]0;title\x07c\x1bNd\x1bOe\x1bPf\u{80}g";
        let output = strip_terminal_control_sequences(input);
        assert!(!output.contains('\x1b'));
        assert_eq!(output, "a b c d e f g");
    }

    #[test]
    fn terminal_escape_filter_handles_st_and_unterminated_sequences() {
        assert_eq!(
            strip_terminal_control_sequences("x\x1b]8;;https://example.test\x1b\\link"),
            "x link"
        );
        assert_eq!(
            strip_terminal_control_sequences("a\x1b]unfinished"),
            "a ]unfinished"
        );
    }

    #[test]
    fn truncates_notification_label_after_stripping_and_collapsing_whitespace() {
        assert_eq!(truncate_notification_label("a\u{202e}b\tc\n d "), "ab c d");
    }

    #[test]
    fn truncates_ascii_at_code_point_limit() {
        let over = truncate_notification_label(&"x".repeat(90));
        assert_eq!(over, format!("{}...", "x".repeat(77)));
        assert_eq!(over.chars().count(), NOTIFICATION_LABEL_MAX_LENGTH);
        assert_eq!(truncate_notification_label(&"x".repeat(80)), "x".repeat(80));
    }

    #[test]
    fn counts_astral_characters_as_code_points_and_never_splits_them() {
        assert_eq!(
            truncate_notification_label(&"😀".repeat(60)),
            "😀".repeat(60)
        );
        assert_eq!(
            truncate_notification_label(&"😀".repeat(80)),
            "😀".repeat(80)
        );
        let truncated = truncate_notification_label(&"😀".repeat(90));
        assert_eq!(truncated, format!("{}...", "😀".repeat(77)));
        assert_eq!(truncated.chars().count(), NOTIFICATION_LABEL_MAX_LENGTH);
    }

    #[test]
    fn unicode_whitespace_collapses_using_ecmascript_rules() {
        assert_eq!(
            truncate_notification_label("\u{feff}one\u{feff}two\u{feff}"),
            "one two"
        );
    }
}
