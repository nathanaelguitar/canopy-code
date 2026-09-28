//! GitLab bot mention matching, matching `packages/channels/gitlab/src/mention.ts`.

/// Escapes the regular expression metacharacters escaped by the TypeScript helper.
pub fn escape_regex(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        if matches!(
            ch,
            '.' | '*' | '+' | '?' | '^' | '$' | '{' | '}' | '(' | ')' | '|' | '[' | ']' | '\\'
        ) {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

/// Returns whether `text` contains a GitLab-style mention of `username`.
///
/// A mention must begin at the start of the text or after ECMAScript whitespace or
/// one of `([{<;:"'`. Its next character cannot be an ASCII letter, digit, `_`, `.`,
/// `/`, or `-`. GitLab treats a period as part of a username continuation.
pub fn test_bot_mention(text: &str, username: &str) -> bool {
    text.char_indices().any(|(at, ch)| {
        ch == '@' && has_valid_prefix(text, at) && mention_end(text, at, username).is_some()
    })
}

/// Removes every GitLab-style mention of `username`, preserving all other text.
pub fn strip_bot_mention(text: &str, username: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut copied_through = 0;
    let mut cursor = 0;

    while cursor < text.len() {
        let ch = text[cursor..]
            .chars()
            .next()
            .expect("cursor is maintained on a character boundary");
        if ch == '@'
            && has_valid_prefix(text, cursor)
            && let Some(end) = mention_end(text, cursor, username)
        {
            result.push_str(&text[copied_through..cursor]);
            copied_through = end;
            cursor = end;
        } else {
            cursor += ch.len_utf8();
        }
    }

    result.push_str(&text[copied_through..]);
    result
}

fn has_valid_prefix(text: &str, at: usize) -> bool {
    match text[..at].chars().next_back() {
        None => true,
        Some(previous) => {
            is_ecmascript_whitespace(previous)
                || matches!(previous, '(' | '[' | '{' | '<' | ';' | ':' | '"' | '\'')
        }
    }
}

fn mention_end(text: &str, at: usize, username: &str) -> Option<usize> {
    let after_at = at + 1;
    let remaining = &text[after_at..];
    let mut matched_end = after_at;
    let mut actual_chars = remaining.char_indices();

    for expected in username.chars() {
        let (offset, actual) = actual_chars.next()?;
        if !js_case_insensitive_char_eq(expected, actual) {
            return None;
        }
        matched_end = after_at + offset + actual.len_utf8();
    }

    if let Some(next) = text[matched_end..].chars().next()
        && is_disallowed_mention_continuation(next)
    {
        return None;
    }

    Some(matched_end)
}

fn is_disallowed_mention_continuation(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '/' | '-')
}

fn is_ecmascript_whitespace(ch: char) -> bool {
    matches!(
        ch,
        '\u{0009}'..='\u{000D}'
            | ' '
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}

// JavaScript's `/i` without the `u` flag compares one UTF-16 code unit at a time.
// Thus supplementary characters only match themselves; BMP case pairs use simple
// uppercase mappings, with mappings from non-ASCII into ASCII deliberately blocked.
fn js_case_insensitive_char_eq(left: char, right: char) -> bool {
    if left > '\u{FFFF}' || right > '\u{FFFF}' {
        return left == right;
    }
    js_canonicalize_bmp(left) == js_canonicalize_bmp(right)
}

fn js_canonicalize_bmp(ch: char) -> char {
    let mut uppercase = ch.to_uppercase();
    let Some(first) = uppercase.next() else {
        return ch;
    };
    if uppercase.next().is_some() || first > '\u{FFFF}' || (ch > '\u{007F}' && first <= '\u{007F}')
    {
        ch
    } else {
        first
    }
}

#[cfg(test)]
mod tests {
    use super::{escape_regex, strip_bot_mention, test_bot_mention};

    #[test]
    fn escape_regex_matches_typescript_helper() {
        assert_eq!(escape_regex("a.b*c"), r"a\.b\*c");
        assert_eq!(escape_regex("user[name]"), r"user\[name\]");
    }

    #[test]
    fn detects_mentions_with_source_boundaries_and_case_rules() {
        let bot = "test-bot";
        for text in [
            "@test-bot hello",
            "hey @test-bot please fix",
            "@TEST-BOT hello",
            "@Test-Bot hello",
            "(@test-bot) hello",
            "hello @test-bot",
        ] {
            assert!(test_bot_mention(text, bot), "expected mention in {text:?}");
        }
        for text in [
            "@test-bot-extra hello",
            "user@test-bot.com",
            "test-bot hello",
            "x@test-bot",
            "@myXbot hello",
        ] {
            assert!(
                !test_bot_mention(text, bot),
                "unexpected mention in {text:?}"
            );
        }
    }

    #[test]
    fn escapes_username_metacharacters_and_rejects_dot_suffixes() {
        assert!(test_bot_mention("@my.bot hello", "my.bot"));
        assert!(!test_bot_mention("@myXbot hello", "my.bot"));
        assert!(!test_bot_mention("@bot.next", "bot"));
        assert!(!test_bot_mention("@bot/name", "bot"));
    }

    #[test]
    fn strips_every_complete_match_and_preserves_non_matches() {
        assert_eq!(
            strip_bot_mention("@test-bot please fix", "test-bot"),
            " please fix"
        );
        assert_eq!(
            strip_bot_mention("@test-bot hello @test-bot", "test-bot"),
            " hello "
        );
        assert_eq!(
            strip_bot_mention("no mention here", "test-bot"),
            "no mention here"
        );
        assert_eq!(
            strip_bot_mention("@test-bot-extra hello", "test-bot"),
            "@test-bot-extra hello"
        );
    }

    #[test]
    fn uses_ecmascript_whitespace_and_replaces_without_consuming_it() {
        assert!(test_bot_mention("\u{00A0}@test-bot", "test-bot"));
        assert_eq!(
            strip_bot_mention("  @test-bot\t@TEST-BOT", "test-bot"),
            "  \t"
        );
        assert!(!test_bot_mention("x@test-bot", "test-bot"));
    }

    #[test]
    fn empty_username_matches_an_at_sign_with_a_valid_following_boundary() {
        assert!(test_bot_mention(" @ hello", ""));
        assert!(!test_bot_mention("x@ hello", ""));
        assert!(!test_bot_mention("@.hello", ""));
        assert_eq!(strip_bot_mention("@ @x", ""), " @x");
    }
}
