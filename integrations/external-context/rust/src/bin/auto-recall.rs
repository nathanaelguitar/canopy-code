use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use canopy_external_context::{
    ExternalContextConfig, ProviderClient, load_config, render_external_context,
    search_with_timeout,
};
use regex::Regex;
use serde_json::{Value, json};

const MAX_HOOK_INPUT_BYTES: usize = 1024 * 1024;
const MAX_AUTO_QUERY_CHARACTERS: usize = 512;
const MAX_SANITIZER_INPUT_CHARACTERS: usize = 4096;
const HOOK_WALL_CLOCK_TIMEOUT: Duration = Duration::from_millis(6500);

fn main() {
    let deadline = std::time::Instant::now() + HOOK_WALL_CLOCK_TIMEOUT;
    let output = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime
            .block_on(async {
                tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), run()).await
            })
            .ok()
            .flatten()
            .unwrap_or_else(empty_output),
        Err(_) => empty_output(),
    };
    print!(
        "{}",
        serde_json::to_string(&output).unwrap_or_else(|_| "{}".into())
    );
}

async fn run() -> Option<Value> {
    let input = read_hook_input().await?;
    let object = input.as_object()?;
    if object.get("hook_event_name").and_then(Value::as_str) != Some("UserPromptSubmit") {
        return Some(empty_output());
    }
    let submitted_prompt = object.get("submitted_prompt").and_then(Value::as_str)?;
    if submitted_prompt.chars().all(is_js_whitespace) {
        return Some(empty_output());
    }
    let cwd = object.get("cwd").and_then(Value::as_str)?;

    // Match the source hook's important gate: malformed and unrelated events
    // return before loading configuration or resolving provider credentials.
    let Some(config) = load_config_off_thread().await else {
        return Some(empty_output());
    };
    if config.version != 2 {
        return Some(empty_output());
    }
    let Some(auto_recall) = config.auto_recall.as_ref() else {
        return Some(empty_output());
    };

    let Some(cwd) = resolve_directory_off_thread(cwd.to_owned()).await else {
        return Some(empty_output());
    };
    if !cwd.starts_with(&auto_recall.repository_root) {
        return Some(empty_output());
    }

    let credential = match &config.provider {
        canopy_external_context::ProviderConfig::Mem0PlatformV3 { api_key, .. } => api_key,
        canopy_external_context::ProviderConfig::GenericHttpSearchV1 { token, .. } => token,
    };
    let Some(query) = create_auto_recall_query(submitted_prompt, credential) else {
        return Some(empty_output());
    };

    let provider = match ProviderClient::new(config.provider.clone()) {
        Ok(provider) => provider,
        Err(_) => return Some(empty_output()),
    };
    let items = match search_with_timeout(
        &provider,
        &query,
        5,
        Duration::from_millis(auto_recall.timeout_ms),
    )
    .await
    {
        Ok(items) if !items.is_empty() => items,
        _ => return Some(empty_output()),
    };

    let (additional_context, _) = render_external_context(&items);
    Some(json!({
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": additional_context,
        }
    }))
}

async fn read_hook_input() -> Option<Value> {
    // Tokio's stdin reader uses a blocking worker that cannot be cancelled.
    // A detached standard thread lets the 6.5 second outer timeout still end
    // the hook if the caller leaves stdin open without completing the JSON.
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("external-context-hook-stdin".into())
        .spawn(move || {
            let mut stdin = std::io::stdin().take((MAX_HOOK_INPUT_BYTES + 1) as u64);
            let mut bytes = Vec::new();
            let value = stdin
                .read_to_end(&mut bytes)
                .ok()
                .filter(|_| bytes.len() <= MAX_HOOK_INPUT_BYTES)
                .and_then(|_| serde_json::from_slice(&bytes).ok());
            let _ = sender.send(value);
        })
        .ok()?;
    receiver.await.ok().flatten()
}

async fn load_config_off_thread() -> Option<ExternalContextConfig> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("external-context-hook-config".into())
        .spawn(move || {
            let _ = sender.send(load_config().ok());
        })
        .ok()?;
    receiver.await.ok().flatten()
}

async fn resolve_directory_off_thread(value: String) -> Option<PathBuf> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("external-context-hook-cwd".into())
        .spawn(move || {
            let _ = sender.send(resolve_directory(&value));
        })
        .ok()?;
    receiver.await.ok().flatten()
}

fn resolve_directory(value: &str) -> Option<PathBuf> {
    let path = Path::new(value);
    if !path.is_absolute() {
        return None;
    }
    let resolved = std::fs::canonicalize(path).ok()?;
    std::fs::metadata(&resolved)
        .ok()?
        .is_dir()
        .then_some(resolved)
}

fn create_auto_recall_query(submitted_prompt: &str, credential: &str) -> Option<String> {
    let mut query = submitted_prompt
        .chars()
        .take(MAX_SANITIZER_INPUT_CHARACTERS)
        .collect::<String>();
    query = strip_fenced_code(&query);

    if !credential.is_empty() {
        query = query.replace(credential, " ");
    }

    query = secret_assignment_regex()
        .replace_all(&query, " ")
        .into_owned();
    query = bearer_regex().replace_all(&query, "$1 ").into_owned();
    query = redact_jwts(&query);
    query = redact_long_tokens(&query);

    let normalized = collapse_js_whitespace(&query);
    let result = normalized
        .chars()
        .take(MAX_AUTO_QUERY_CHARACTERS)
        .collect::<String>();
    (!result.is_empty()).then_some(result)
}

fn strip_fenced_code(source: &str) -> String {
    let mut output = String::with_capacity(source.len());
    let mut cursor = 0;
    while cursor < source.len() {
        let remaining = &source[cursor..];
        let next = earliest_fence(remaining);
        let Some((offset, fence)) = next else {
            output.push_str(remaining);
            break;
        };
        let start = cursor + offset;
        output.push_str(&source[cursor..start]);
        output.push(' ');
        let content_start = start + fence.len();
        if let Some(close_offset) = source[content_start..].find(fence) {
            cursor = content_start + close_offset + fence.len();
        } else {
            // The source hook also removes an unmatched opening fence and all
            // remaining input.
            break;
        }
    }
    output
}

fn earliest_fence(source: &str) -> Option<(usize, &'static str)> {
    match (source.find("```"), source.find("~~~")) {
        (None, None) => None,
        (Some(index), None) => Some((index, "```")),
        (None, Some(index)) => Some((index, "~~~")),
        (Some(ticks), Some(tildes)) if ticks <= tildes => Some((ticks, "```")),
        (Some(_), Some(tildes)) => Some((tildes, "~~~")),
    }
}

fn redact_jwts(source: &str) -> String {
    let mut output = String::with_capacity(source.len());
    let mut cursor = 0;
    let bytes = source.as_bytes();
    let mut start = 0;
    while start < bytes.len() {
        if !source.is_char_boundary(start) || !is_token_byte(bytes[start]) {
            start += 1;
            continue;
        }
        let previous = source[..start].chars().next_back();
        if previous.is_some_and(is_ascii_word) == is_ascii_word_byte(bytes[start]) {
            start += 1;
            continue;
        }

        let Some((second_end, third_end)) = parse_jwt_candidate(bytes, start) else {
            start += 1;
            continue;
        };
        let third_start = second_end + 1;
        let Some(end) = rightmost_word_boundary(source, third_start, third_end, 8) else {
            start += 1;
            continue;
        };
        output.push_str(&source[cursor..start]);
        output.push(' ');
        cursor = end;
        start = end;
    }
    output.push_str(&source[cursor..]);
    output
}

fn redact_long_tokens(source: &str) -> String {
    let mut output = String::with_capacity(source.len());
    let mut cursor = 0;
    let bytes = source.as_bytes();
    let mut start = 0;
    while start < bytes.len() {
        if !source.is_char_boundary(start) || !is_token_byte(bytes[start]) {
            start += 1;
            continue;
        }
        let previous = source[..start].chars().next_back();
        if previous.is_some_and(is_ascii_word) == is_ascii_word_byte(bytes[start]) {
            start += 1;
            continue;
        }

        let mut run_end = start;
        while run_end < bytes.len() && is_token_byte(bytes[run_end]) {
            run_end += 1;
        }
        let Some(end) = rightmost_word_boundary(source, start, run_end, 32) else {
            start += 1;
            continue;
        };
        output.push_str(&source[cursor..start]);
        output.push(' ');
        cursor = end;
        start = end;
    }
    output.push_str(&source[cursor..]);
    output
}

fn parse_jwt_candidate(bytes: &[u8], start: usize) -> Option<(usize, usize)> {
    let first_end = token_run_end(bytes, start);
    if first_end.saturating_sub(start) < 8 || bytes.get(first_end) != Some(&b'.') {
        return None;
    }
    let second_start = first_end + 1;
    let second_end = token_run_end(bytes, second_start);
    if second_end.saturating_sub(second_start) < 8 || bytes.get(second_end) != Some(&b'.') {
        return None;
    }
    let third_start = second_end + 1;
    let third_end = token_run_end(bytes, third_start);
    if third_end.saturating_sub(third_start) < 8 {
        return None;
    }
    Some((second_end, third_end))
}

fn token_run_end(bytes: &[u8], start: usize) -> usize {
    let mut end = start;
    while end < bytes.len() && is_token_byte(bytes[end]) {
        end += 1;
    }
    end
}

fn rightmost_word_boundary(
    source: &str,
    start: usize,
    run_end: usize,
    minimum: usize,
) -> Option<usize> {
    if run_end.saturating_sub(start) < minimum {
        return None;
    }
    for end in (start + minimum..=run_end).rev() {
        let last = source.as_bytes()[end - 1];
        let after = source[end..].chars().next();
        if is_ascii_word_byte(last) != after.is_some_and(is_ascii_word) {
            return Some(end);
        }
    }
    None
}

fn collapse_js_whitespace(source: &str) -> String {
    let mut output = String::with_capacity(source.len());
    let mut pending_space = false;
    for character in source.chars() {
        if is_js_whitespace(character) {
            pending_space = !output.is_empty();
        } else {
            if pending_space {
                output.push(' ');
                pending_space = false;
            }
            output.push(character);
        }
    }
    output
}

fn is_js_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

fn is_ascii_word(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

fn is_ascii_word_byte(character: u8) -> bool {
    character.is_ascii_alphanumeric() || character == b'_'
}

fn is_token_byte(character: u8) -> bool {
    is_ascii_word_byte(character) || character == b'-'
}

fn secret_assignment_regex() -> &'static Regex {
    static REGEX: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
        Regex::new(
            r#"(?i-u)["']?[A-Za-z0-9_.-]*(?:api[_-]?key|token|password|secret)[A-Za-z0-9_.-]*["']?[\x{0009}\x{000b}\x{000c} \x{00a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}]*[:=][\x{0009}\x{000b}\x{000c} \x{00a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}]*(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\x{0009}\x{000a}\x{000b}\x{000c}\x{000d} \x{00a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff},;]+)?"#,
        )
        .expect("valid secret assignment pattern")
    });
    &REGEX
}

fn bearer_regex() -> &'static Regex {
    static REGEX: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
        Regex::new(r"(?i-u)(^|[^A-Za-z0-9_])Bearer[\x{0009}\x{000a}\x{000b}\x{000c}\x{000d} \x{00a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}]+[A-Za-z0-9._~+/=-]+")
            .expect("valid bearer token pattern")
    });
    &REGEX
}

fn empty_output() -> Value {
    json!({})
}
