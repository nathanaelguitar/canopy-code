//! Deterministic parsing for channel memory commands.
//!
//! This mirrors `packages/channels/base/src/channel-memory-intent.ts`. It is
//! intentionally narrow: ambiguous prose is left for the channel's normal
//! intent path instead of being interpreted as a memory command.

use regex::Regex;
use std::sync::OnceLock;

/// A recognized channel memory command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChannelMemoryIntent {
    Remember { texts: Vec<String> },
    List { page: u64 },
    Inspect { id: String },
    Remove { id: String },
    Update { id: String, text: String },
    UpdateConfirm,
    RemoveConfirm,
    ClearRequest,
    ClearConfirm,
}

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Strip the same unsafe invisible characters as `sanitize.ts`'s
/// `PROMPT_UNSAFE_INVISIBLES`. The shared Rust sanitizer exposes operations
/// with different replacement semantics, so this module keeps the exact
/// remove behavior local.
fn unsafe_invisibles() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"[\p{Cf}\p{Variation_Selector}\u{0080}-\u{009F}\u{2028}\u{2029}]")
            .expect("unsafe invisible pattern is valid")
    })
}

fn strip_unsafe_invisibles(text: &str) -> String {
    unsafe_invisibles().replace_all(text, "").into_owned()
}

/// ECMAScript `String.prototype.trim` whitespace. Rust's `str::trim` differs
/// for a small set of Unicode/control characters, so spell out JS's set for
/// parity after unsafe invisibles have been removed.
fn trim_js(text: &str) -> &str {
    fn is_js_whitespace(ch: char) -> bool {
        matches!(
            ch as u32,
            0x0009..=0x000D
                | 0x0020
                | 0x00A0
                | 0x1680
                | 0x2000..=0x200A
                | 0x2028..=0x2029
                | 0x202F
                | 0x205F
                | 0x3000
                | 0xFEFF
        )
    }

    let start = text
        .char_indices()
        .find_map(|(index, ch)| (!is_js_whitespace(ch)).then_some(index))
        .unwrap_or(text.len());
    let end = text
        .char_indices()
        .rev()
        .find_map(|(index, ch)| (!is_js_whitespace(ch)).then_some(index + ch.len_utf8()))
        .unwrap_or(start);
    &text[start..end]
}

// JavaScript's `\s`/`\S` use ECMAScript whitespace, while Rust regex's
// Unicode whitespace table includes a few additional characters. Invisible
// format, C1, LS, and PS characters have already been stripped before these
// expressions run.
const JS_SPACE: &str = r"[\x09-\x0D\x20\u{00A0}\u{1680}\u{2000}-\u{200A}\u{202F}\u{205F}\u{3000}]";
const JS_NON_SPACE: &str =
    r"[^\x09-\x0D\x20\u{00A0}\u{1680}\u{2000}-\u{200A}\u{202F}\u{205F}\u{3000}]";

fn patterns(patterns: Vec<String>) -> Vec<Regex> {
    patterns
        .iter()
        .map(|pattern| Regex::new(pattern).expect("memory intent pattern is valid"))
        .collect()
}

fn remember_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        patterns(vec![
            format!(r"(?s)^记住[:：]{JS_SPACE}*(.+)$"),
            format!(r"(?s)^记一下[:：,，]?{JS_SPACE}*(.+)$"),
            format!(r"(?s)^帮我记一下[:：,，]?{JS_SPACE}*(.+)$"),
            format!(r"(?s)^帮我记住[:：,，]?{JS_SPACE}*(.+)$"),
            format!(r"(?s)^以后记住[:：,，]?{JS_SPACE}*(.+)$"),
            format!(r"(?isu)^remember:{JS_SPACE}*(.+)$"),
        ])
    })
}

fn list_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        patterns(vec![
            r"^你现在记住了什么[?？]?$".to_owned(),
            r"^查看记忆$".to_owned(),
            r"^当前记忆$".to_owned(),
            r"^这个聊天你记住了什么[?？]?$".to_owned(),
            r"(?iu)^what do you remember[?？]?$".to_owned(),
        ])
    })
}

fn list_page_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        patterns(vec![
            format!(r"^查看第{JS_SPACE}*([0-9]+){JS_SPACE}*页记忆$"),
            format!(r"(?iu)^show memory page{JS_SPACE}+([0-9]+)$"),
        ])
    })
}

fn inspect_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        patterns(vec![
            format!(r"^查看记忆{JS_SPACE}+({JS_NON_SPACE}+)$"),
            format!(r"(?iu)^show memory{JS_SPACE}+({JS_NON_SPACE}+)$"),
        ])
    })
}

fn remove_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        patterns(vec![
            format!(r"^忘掉{JS_SPACE}+({JS_NON_SPACE}+)$"),
            format!(r"^删除{JS_SPACE}+({JS_NON_SPACE}+)$"),
            format!(r"^删掉{JS_SPACE}+({JS_NON_SPACE}+)$"),
            format!(r"(?iu)^forget{JS_SPACE}+({JS_NON_SPACE}+)$"),
            format!(r"(?iu)^delete{JS_SPACE}+({JS_NON_SPACE}+)$"),
            format!(r"(?iu)^remove{JS_SPACE}+({JS_NON_SPACE}+)$"),
        ])
    })
}

fn update_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        patterns(vec![
            format!(r"(?s)^把{JS_SPACE}+({JS_NON_SPACE}+){JS_SPACE}+改成{JS_SPACE}*(.+)$"),
            format!(r"(?s)^更新{JS_SPACE}+({JS_NON_SPACE}+){JS_SPACE}+为{JS_SPACE}*(.+)$"),
            format!(r"(?isu)^update{JS_SPACE}+({JS_NON_SPACE}+){JS_SPACE}+to{JS_SPACE}*(.+)$"),
            format!(r"(?isu)^change{JS_SPACE}+({JS_NON_SPACE}+){JS_SPACE}+to{JS_SPACE}*(.+)$"),
        ])
    })
}

fn clear_request_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        patterns(vec![
            r"^清空记忆$".to_owned(),
            r"^清除记忆$".to_owned(),
            r"^忘掉这个聊天的所有记忆$".to_owned(),
            r"^把[^\r\n]+的?记忆清空$".to_owned(),
            r"(?iu)^clear memory$".to_owned(),
        ])
    })
}

fn clear_confirm_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        patterns(vec![
            r"^确认清空记忆$".to_owned(),
            r"^确认清除记忆$".to_owned(),
            r"(?iu)^confirm clear memory$".to_owned(),
        ])
    })
}

fn update_confirm_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        patterns(vec![
            r"^确认更新记忆$".to_owned(),
            r"(?iu)^confirm memory update$".to_owned(),
        ])
    })
}

fn remove_confirm_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        patterns(vec![
            r"^确认删除记忆$".to_owned(),
            r"(?iu)^confirm memory removal$".to_owned(),
        ])
    })
}

fn is_memory_id(id: &str) -> bool {
    id.len() == 14
        && id.starts_with("m-")
        && id[2..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn captures<'t>(patterns: &[Regex], text: &'t str) -> Option<regex::Captures<'t>> {
    patterns.iter().find_map(|pattern| pattern.captures(text))
}

/// Parse one deterministic memory intent, preserving the TypeScript parser's
/// matching priority and case behavior.
pub fn parse_channel_memory_intent(text: &str) -> Option<ChannelMemoryIntent> {
    let cleaned = strip_unsafe_invisibles(text);
    let trimmed = trim_js(&cleaned);
    if trimmed.is_empty() || trimmed.starts_with('/') {
        return None;
    }

    if update_confirm_patterns()
        .iter()
        .any(|pattern| pattern.is_match(trimmed))
    {
        return Some(ChannelMemoryIntent::UpdateConfirm);
    }
    if remove_confirm_patterns()
        .iter()
        .any(|pattern| pattern.is_match(trimmed))
    {
        return Some(ChannelMemoryIntent::RemoveConfirm);
    }
    if clear_confirm_patterns()
        .iter()
        .any(|pattern| pattern.is_match(trimmed))
    {
        return Some(ChannelMemoryIntent::ClearConfirm);
    }
    if let Some(found) = captures(remove_patterns(), trimmed) {
        if let Some(id) = found.get(1).map(|capture| capture.as_str()) {
            if is_memory_id(id) {
                return Some(ChannelMemoryIntent::Remove { id: id.to_owned() });
            }
        }
    }
    if let Some(found) = captures(update_patterns(), trimmed) {
        let id = found.get(1).map(|capture| capture.as_str());
        let updated = found.get(2).map(|capture| trim_js(capture.as_str()));
        if let (Some(id), Some(updated)) = (id, updated) {
            if !updated.is_empty() && is_memory_id(id) {
                return Some(ChannelMemoryIntent::Update {
                    id: id.to_owned(),
                    text: updated.to_owned(),
                });
            }
        }
    }
    if clear_request_patterns()
        .iter()
        .any(|pattern| pattern.is_match(trimmed))
    {
        return Some(ChannelMemoryIntent::ClearRequest);
    }
    if let Some(found) = captures(inspect_patterns(), trimmed) {
        if let Some(id) = found.get(1).map(|capture| capture.as_str()) {
            if is_memory_id(id) {
                return Some(ChannelMemoryIntent::Inspect { id: id.to_owned() });
            }
        }
    }
    if let Some(found) = captures(list_page_patterns(), trimmed) {
        if let Some(page) = found
            .get(1)
            .and_then(|capture| capture.as_str().parse::<u64>().ok())
            .filter(|page| *page > 0 && *page <= MAX_SAFE_INTEGER)
        {
            return Some(ChannelMemoryIntent::List { page });
        }
        return None;
    }
    if list_patterns()
        .iter()
        .any(|pattern| pattern.is_match(trimmed))
    {
        return Some(ChannelMemoryIntent::List { page: 1 });
    }
    if let Some(found) = captures(remember_patterns(), trimmed) {
        if let Some(remembered) = found.get(1).map(|capture| {
            let without_invisibles = strip_unsafe_invisibles(capture.as_str());
            trim_js(&without_invisibles).to_owned()
        }) {
            if !remembered.is_empty() {
                return Some(ChannelMemoryIntent::Remember {
                    texts: vec![remembered],
                });
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{ChannelMemoryIntent as Intent, parse_channel_memory_intent as parse};

    const ID: &str = "m-a31f0d82c7e4";

    #[test]
    fn parses_chinese_and_english_remember_forms_with_unicode_trim() {
        assert_eq!(
            parse("\u{3000}记住： 默认使用 staging 环境 \u{00a0}"),
            Some(Intent::Remember {
                texts: vec!["默认使用 staging 环境".to_owned()]
            })
        );
        assert_eq!(
            parse("帮我记一下，发布前跑 npm run build"),
            Some(Intent::Remember {
                texts: vec!["发布前跑 npm run build".to_owned()]
            })
        );
        assert_eq!(
            parse("以后记住要先看 CI"),
            Some(Intent::Remember {
                texts: vec!["要先看 CI".to_owned()]
            })
        );
        assert_eq!(
            parse("REMEMBER:  use staging  "),
            Some(Intent::Remember {
                texts: vec!["use staging".to_owned()]
            })
        );
        assert_eq!(parse("记住：   "), None);
        assert_eq!(parse("remember:"), None);
    }

    #[test]
    fn strips_unsafe_zero_width_and_bidi_invisibles_before_matching() {
        assert_eq!(parse("查看记忆\u{200b}"), Some(Intent::List { page: 1 }));
        assert_eq!(
            parse("记住: keep\u{200b}this\u{202e}safe\u{fe0f}"),
            Some(Intent::Remember {
                texts: vec!["keepthissafe".to_owned()]
            })
        );
        assert_eq!(
            parse("\u{0085}\u{200b}查看记忆"),
            Some(Intent::List { page: 1 })
        );
    }

    #[test]
    fn parses_list_pages_only_as_positive_safe_ascii_integers() {
        assert_eq!(parse("查看第 2 页记忆"), Some(Intent::List { page: 2 }));
        assert_eq!(parse("SHOW MEMORY PAGE 3"), Some(Intent::List { page: 3 }));
        assert_eq!(parse("查看第 0 页记忆"), None);
        assert_eq!(parse("查看第 -1 页记忆"), None);
        assert_eq!(parse("show memory page 1.5"), None);
        assert_eq!(parse("show memory page 9007199254740992"), None);
        assert_eq!(parse("show memory page １２"), None);
    }

    #[test]
    fn parses_valid_ids_and_rejects_invalid_ids() {
        assert_eq!(
            parse(&format!("查看记忆 {ID}")),
            Some(Intent::Inspect { id: ID.to_owned() })
        );
        assert_eq!(
            parse(&format!("SHOW MEMORY {ID}")),
            Some(Intent::Inspect { id: ID.to_owned() })
        );
        assert_eq!(
            parse(&format!("forget {ID}")),
            Some(Intent::Remove { id: ID.to_owned() })
        );
        assert_eq!(
            parse(&format!("删除 {ID}")),
            Some(Intent::Remove { id: ID.to_owned() })
        );
        assert_eq!(parse("忘掉 m-not-valid"), None);
        assert_eq!(parse("忘掉 M-a31f0d82c7e4"), None);
    }

    #[test]
    fn parses_multiline_updates_before_broad_clear_requests() {
        assert_eq!(
            parse(&format!("把 {ID} 改成 默认的记忆清空")),
            Some(Intent::Update {
                id: ID.to_owned(),
                text: "默认的记忆清空".to_owned()
            })
        );
        assert_eq!(
            parse(&format!("update {ID} to\n  use production\n")),
            Some(Intent::Update {
                id: ID.to_owned(),
                text: "use production".to_owned()
            })
        );
        assert_eq!(
            parse("更新 m-a31f0d82c7e4 为　默认使用 production　"),
            Some(Intent::Update {
                id: ID.to_owned(),
                text: "默认使用 production".to_owned()
            })
        );
        assert_eq!(parse(&format!("把 {ID} 改成   ")), None);
    }

    #[test]
    fn preserves_confirmation_clear_priority_and_slash_exclusion() {
        assert_eq!(parse("确认更新记忆"), Some(Intent::UpdateConfirm));
        assert_eq!(parse("CONFIRM MEMORY REMOVAL"), Some(Intent::RemoveConfirm));
        assert_eq!(parse("confirm clear memory"), Some(Intent::ClearConfirm));
        assert_eq!(parse("清空记忆"), Some(Intent::ClearRequest));
        assert_eq!(parse("把聊天的记忆清空"), Some(Intent::ClearRequest));
        assert_eq!(parse("/forget m-a31f0d82c7e4"), None);
        assert_eq!(parse(" /remember-channel use staging"), None);
        assert_eq!(parse("保存配置到本地"), None);
        assert_eq!(parse("remember this might be tricky later"), None);
    }
}
