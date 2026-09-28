//! Project JSON strings to a byte budget while retaining a head and tail.
//!
//! Rust `str` values are valid UTF-8 and cannot contain isolated UTF-16
//! surrogates. The UTF-16 accounting helper models JavaScript's `JSON.stringify`
//! behavior for isolated surrogates, but the public projection functions can
//! only receive valid Rust strings.

/// The two quote bytes surrounding a JSON string.
pub const JSON_STRING_DELIMITER_BYTES: usize = 2;

/// Return the UTF-8 byte length of a JSON-encoded string, or `limit_bytes + 1`
/// once the encoded value exceeds the limit. This mirrors the source bridge's
/// capped estimator and avoids constructing the escaped JSON string.
pub fn estimate_json_string_bytes(value: &str, limit_bytes: usize) -> usize {
    let unescaped_bytes = value.len().saturating_add(JSON_STRING_DELIMITER_BYTES);
    if unescaped_bytes > limit_bytes {
        return limit_bytes.saturating_add(1);
    }
    if value
        .bytes()
        .all(|byte| byte >= 0x20 && byte != b'"' && byte != b'\\')
    {
        return unescaped_bytes;
    }

    let payload_budget = limit_bytes.saturating_sub(JSON_STRING_DELIMITER_BYTES);
    let payload_bytes = json_payload_bytes_utf16(value.encode_utf16(), payload_budget);
    if payload_bytes > payload_budget {
        limit_bytes.saturating_add(1)
    } else {
        payload_bytes.saturating_add(JSON_STRING_DELIMITER_BYTES)
    }
}

/// Return the UTF-8 byte length of a string's JSON-escaped payload.
pub fn json_string_payload_byte_length(value: &str) -> usize {
    json_payload_bytes_utf16(value.encode_utf16(), usize::MAX)
}

/// Return the UTF-8 byte length of a JSON-encoded string, including quotes.
pub fn json_string_json_byte_length(value: &str) -> usize {
    JSON_STRING_DELIMITER_BYTES + json_string_payload_byte_length(value)
}

/// Truncate a payload to a JSON byte budget, keeping about 20% at the head and
/// 80% at the tail around `marker`.
pub fn truncate_json_string_payload(
    value: &str,
    original_payload_bytes: usize,
    payload_budget: usize,
    marker: &str,
) -> String {
    if original_payload_bytes <= payload_budget {
        return value.to_owned();
    }

    let marker_payload_bytes = json_string_payload_byte_length(marker);
    if payload_budget < marker_payload_bytes {
        return value[..select_prefix(value, payload_budget)].to_owned();
    }

    let source_budget = payload_budget - marker_payload_bytes;
    let head_budget = source_budget / 5;
    let tail_budget = source_budget - head_budget;
    let head_end = select_prefix(value, head_budget);
    let tail_start = select_suffix(value, tail_budget);

    let mut projected = String::with_capacity(head_end + marker.len() + value.len() - tail_start);
    projected.push_str(&value[..head_end]);
    projected.push_str(marker);
    projected.push_str(&value[tail_start..]);
    projected
}

/// Project a string so its JSON encoding fits within `json_byte_budget`.
pub fn project_json_string_to_byte_budget(
    value: &str,
    json_byte_budget: usize,
    marker: &str,
) -> String {
    let payload_budget = json_byte_budget.saturating_sub(JSON_STRING_DELIMITER_BYTES);
    let payload_bytes = json_payload_bytes_utf16(value.encode_utf16(), payload_budget);
    if payload_bytes <= payload_budget {
        return value.to_owned();
    }

    let projected = truncate_json_string_payload(value, payload_bytes, payload_budget, marker);
    if json_string_json_byte_length(&projected) <= json_byte_budget {
        return projected;
    }

    let fallback_end = select_prefix(marker, payload_budget);
    let fallback = marker[..fallback_end].to_owned();
    if json_string_json_byte_length(&fallback) <= json_byte_budget {
        fallback
    } else {
        String::new()
    }
}

fn json_payload_bytes_utf16<I>(units: I, stop_after_bytes: usize) -> usize
where
    I: IntoIterator<Item = u16>,
{
    let mut units = units.into_iter().peekable();
    let mut bytes = 0;
    while let Some(code) = units.next() {
        let part_bytes = match code {
            0x22 | 0x5c => 2,
            0x08 | 0x09 | 0x0a | 0x0c | 0x0d => 2,
            0x00..=0x1f => 6,
            0x20..=0x7f => 1,
            0x80..=0x7ff => 2,
            0xd800..=0xdbff => {
                if units
                    .peek()
                    .is_some_and(|next| (0xdc00..=0xdfff).contains(next))
                {
                    units.next();
                    4
                } else {
                    6
                }
            }
            0xdc00..=0xdfff => 6,
            _ => 3,
        };
        bytes += part_bytes;
        if bytes > stop_after_bytes {
            return bytes;
        }
    }
    bytes
}

fn json_payload_byte_length_for_char(character: char) -> usize {
    match character as u32 {
        0x22 | 0x5c => 2,
        0x08 | 0x09 | 0x0a | 0x0c | 0x0d => 2,
        0x00..=0x1f => 6,
        0x20..=0x7f => 1,
        0x80..=0x7ff => 2,
        0x800..=0xffff => 3,
        _ => 4,
    }
}

fn select_prefix(value: &str, budget: usize) -> usize {
    let mut end = 0;
    let mut bytes = 0;
    for (index, character) in value.char_indices() {
        let part_bytes = json_payload_byte_length_for_char(character);
        if bytes + part_bytes > budget {
            break;
        }
        bytes += part_bytes;
        end = index + character.len_utf8();
    }
    end
}

fn select_suffix(value: &str, budget: usize) -> usize {
    let mut start = value.len();
    let mut bytes = 0;
    for (index, character) in value.char_indices().rev() {
        let part_bytes = json_payload_byte_length_for_char(character);
        if bytes + part_bytes > budget {
            break;
        }
        bytes += part_bytes;
        start = index;
    }
    start
}

#[cfg(test)]
mod tests {
    use super::{
        json_payload_bytes_utf16, json_string_json_byte_length, json_string_payload_byte_length,
        project_json_string_to_byte_budget,
    };

    const BUDGET: usize = 65_536;
    const MARKER: &str = "\n[... truncated for test transport ...]\n";

    #[test]
    fn accounts_for_utf8_json_string_bytes_and_escapes() {
        let samples = [
            "",
            "plain ASCII",
            "\"\\\n\u{08}\u{0c}\r\t",
            "\0\u{01}\u{1f}",
            "汉字",
            "😀",
            "\u{2028}\u{2029}",
            "\u{7f}\u{80}\u{7ff}\u{800}",
        ];

        for sample in samples {
            let expected = serde_json::to_string(sample).unwrap().len();
            assert_eq!(json_string_json_byte_length(sample), expected, "{sample:?}");
            assert_eq!(
                super::estimate_json_string_bytes(sample, usize::MAX),
                expected,
                "{sample:?}"
            );
        }
    }

    #[test]
    fn capped_estimate_returns_only_the_over_limit_sentinel() {
        assert_eq!(super::estimate_json_string_bytes("x", 2), 3);
        assert_eq!(super::estimate_json_string_bytes("x", 3), 3);
        assert_eq!(super::estimate_json_string_bytes("\u{01}", 5), 6);
        assert_eq!(super::estimate_json_string_bytes("\u{01}", 8), 8);
    }

    #[test]
    fn accounts_for_javascript_lone_surrogate_escaping_in_utf16_units() {
        assert_eq!(json_payload_bytes_utf16([0xd800], usize::MAX), 6);
        assert_eq!(json_payload_bytes_utf16([0xdc00], usize::MAX), 6);
        assert_eq!(json_payload_bytes_utf16([0xd83d, 0xde00], usize::MAX), 4);
        assert_eq!(json_string_payload_byte_length("😀"), 4);
    }

    #[test]
    fn enforces_the_json_string_byte_budget() {
        let exact = "x".repeat(BUDGET - 2);
        assert_eq!(
            project_json_string_to_byte_budget(&exact, BUDGET, MARKER),
            exact
        );

        let oversized = "x".repeat(BUDGET);
        let projected = project_json_string_to_byte_budget(&oversized, BUDGET, MARKER);
        assert!(json_string_json_byte_length(&projected) <= BUDGET);
        assert!(projected.contains(MARKER));
        assert_ne!(projected, oversized);
    }

    #[test]
    fn projects_multibyte_and_escaped_text_within_budget() {
        for value in ["汉".repeat(100_000), "\"\\\n\0".repeat(30_000)] {
            let projected = project_json_string_to_byte_budget(&value, BUDGET, MARKER);
            assert!(json_string_json_byte_length(&projected) <= BUDGET);
            assert!(projected.contains(MARKER));
        }
    }

    #[test]
    fn keeps_an_approximately_twenty_eighty_head_and_tail_preview() {
        let value = format!("HEAD-{}-{}-TAIL", "h".repeat(200_000), "t".repeat(200_000));
        let projected = project_json_string_to_byte_budget(&value, BUDGET, MARKER);
        let (head, tail) = projected.split_once(MARKER).unwrap();

        assert!(projected.starts_with("HEAD-"));
        assert!(projected.ends_with("-TAIL"));
        assert!(tail.len() as f64 > head.len() as f64 * 3.9);
        assert!((tail.len() as f64) < head.len() as f64 * 4.1);
    }

    #[test]
    fn never_splits_a_supplementary_unicode_character() {
        let suffix = "-tail!!";
        let value = format!("head-{}{}", "😀".repeat(100_000), suffix);
        let projected = project_json_string_to_byte_budget(&value, BUDGET, MARKER);

        assert!(projected.ends_with(suffix));
        assert!(projected.contains('😀'));
        assert!(json_string_json_byte_length(&projected) <= BUDGET);
    }

    #[test]
    fn uses_a_source_prefix_when_the_marker_does_not_fit() {
        let projected = project_json_string_to_byte_budget("abcdef", 5, MARKER);
        assert_eq!(projected, "abc");
        assert_eq!(json_string_json_byte_length(&projected), 5);

        // A quoted JSON string cannot fit below two bytes; the last-resort
        // fallback is therefore the empty Rust string.
        assert_eq!(project_json_string_to_byte_budget("abcdef", 1, MARKER), "");
    }

    #[test]
    fn projection_is_idempotent_and_keeps_small_strings_unchanged() {
        let small = "small";
        let large = format!("head-{}-tail", "x".repeat(100_000));
        let once = project_json_string_to_byte_budget(&large, BUDGET, MARKER);

        assert_eq!(
            project_json_string_to_byte_budget(small, BUDGET, MARKER),
            small
        );
        assert_eq!(
            project_json_string_to_byte_budget(&once, BUDGET, MARKER),
            once
        );
    }
}
