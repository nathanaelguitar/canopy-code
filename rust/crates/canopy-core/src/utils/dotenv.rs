use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

/// Parses the common dotenv syntax used by Canopy configuration files.
///
/// Duplicate keys in one file use the last value, matching `dotenv.parse`.
/// Double-quoted `\\n` and `\\r` sequences become line breaks; quoted
/// values may also span physical lines.
pub fn parse_dotenv(input: &str) -> HashMap<String, String> {
    let normalized = input.replace("\r\n", "\n").replace('\r', "\n");
    let lines: Vec<&str> = normalized.split('\n').collect();
    let mut vars = HashMap::new();
    let mut line_index = 0;

    while line_index < lines.len() {
        let Some((key, value_start)) = parse_assignment(lines[line_index]) else {
            line_index += 1;
            continue;
        };

        let trimmed = value_start.trim_start();
        let Some(quote) = trimmed
            .chars()
            .next()
            .filter(|c| matches!(c, '\'' | '"' | '`'))
        else {
            let value = trimmed.split('#').next().unwrap_or_default().trim();
            vars.insert(key, value.to_owned());
            line_index += 1;
            continue;
        };

        let start_line_index = line_index;
        let mut quoted = trimmed.to_owned();
        let close = loop {
            if let Some(close) = find_closing_quote(&quoted, quote) {
                break Some(close);
            }
            line_index += 1;
            if line_index == lines.len() {
                break None;
            }
            quoted.push('\n');
            quoted.push_str(lines[line_index]);
        };

        if let Some(close) = close {
            let suffix = quoted[close + quote.len_utf8()..].trim_start();
            if suffix.is_empty() || suffix.starts_with('#') {
                let mut value = quoted[quote.len_utf8()..close].to_owned();
                if quote == '"' {
                    value = value.replace("\\n", "\n").replace("\\r", "\r");
                }
                vars.insert(key, value);
            }
            line_index += 1;
        } else {
            // dotenv's parser falls back to its unquoted alternative when a
            // line has an unmatched quote. Preserve that line literally and
            // allow following physical lines to be parsed independently.
            let value = trimmed.split('#').next().unwrap_or_default().trim();
            vars.insert(key, value.to_owned());
            line_index = start_line_index + 1;
        }
    }

    vars
}

/// Reads the global Canopy `.env` before the user's `~/.env` and returns
/// fallback values without changing the supplied process environment.
///
/// `process_env` is a snapshot of the current environment. A truthy
/// `QWEN_HOME` omits `~/.env`, matching the source's string check. A value in
/// the global Canopy file wins over the same key in `~/.env`; keys already
/// present in `process_env` are omitted. Unreadable files are skipped, like
/// dotenv's quiet read mode.
pub fn get_home_env_fallback_vars(
    global_canopy_dir: &Path,
    home_dir: &Path,
    process_env: &HashSet<String>,
    qwen_home_is_set: bool,
) -> HashMap<String, String> {
    let mut candidates = vec![global_canopy_dir.join(".env")];
    if !qwen_home_is_set && !home_dir.as_os_str().is_empty() {
        candidates.push(home_dir.join(".env"));
    }

    let mut result = HashMap::new();
    for candidate in candidates {
        let Ok(contents) = fs::read(candidate) else {
            continue;
        };
        let contents = String::from_utf8_lossy(&contents);
        for (key, value) in parse_dotenv(&contents) {
            if !process_env.contains(&key) {
                result.entry(key).or_insert(value);
            }
        }
    }
    result
}

fn parse_assignment(line: &str) -> Option<(String, &str)> {
    let mut assignment = line.trim_start();
    if assignment.is_empty() || assignment.starts_with('#') {
        return None;
    }
    if let Some(exported) = assignment.strip_prefix("export") {
        if exported.chars().next().is_some_and(char::is_whitespace) {
            assignment = exported.trim_start();
        }
    }

    let equals = assignment.find('=');
    let colon = assignment
        .char_indices()
        .find(|(index, character)| {
            *character == ':'
                && assignment[index + 1..]
                    .chars()
                    .next()
                    .is_some_and(char::is_whitespace)
        })
        .map(|(index, _)| index);
    let separator = match (equals, colon) {
        (Some(equals), Some(colon)) => equals.min(colon),
        (Some(equals), None) => equals,
        (None, Some(colon)) => colon,
        (None, None) => return None,
    };
    let key = assignment[..separator].trim();
    if key.is_empty()
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        return None;
    }
    Some((key.to_owned(), &assignment[separator + 1..]))
}

fn find_closing_quote(value: &str, quote: char) -> Option<usize> {
    let mut escaped = false;
    for (index, character) in value.char_indices().skip(1) {
        if character == quote && !escaped {
            return Some(index);
        }
        if character == '\\' {
            escaped = !escaped;
        } else {
            escaped = false;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{get_home_env_fallback_vars, parse_dotenv};
    use std::collections::HashSet;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT_TEMP_DIR: AtomicUsize = AtomicUsize::new(0);

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let id = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("canopy-dotenv-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn parses_assignments_exports_comments_and_quotes() {
        let vars = parse_dotenv(
            "# ignored\nPLAIN=value # inline comment\nexport EMPTY=\nSINGLE=' x # y '\nDOUBLE=\"say \\\"hi\\\"\"\nTEMPLATE=`raw # text`\nBAD KEY=no\n",
        );

        assert_eq!(vars.get("PLAIN").map(String::as_str), Some("value"));
        assert_eq!(vars.get("EMPTY").map(String::as_str), Some(""));
        assert_eq!(vars.get("SINGLE").map(String::as_str), Some(" x # y "));
        assert_eq!(
            vars.get("DOUBLE").map(String::as_str),
            Some("say \\\"hi\\\"")
        );
        assert_eq!(vars.get("TEMPLATE").map(String::as_str), Some("raw # text"));
        assert!(!vars.contains_key("BAD"));
    }

    #[test]
    fn expands_escaped_newlines_and_preserves_multiline_quoted_values() {
        let vars = parse_dotenv("ESCAPED=\"first\\nsecond\\rthird\"\nMULTILINE='one\ntwo'\n");

        assert_eq!(
            vars.get("ESCAPED").map(String::as_str),
            Some("first\nsecond\rthird")
        );
        assert_eq!(vars.get("MULTILINE").map(String::as_str), Some("one\ntwo"));
    }

    #[test]
    fn duplicate_definitions_in_one_file_use_the_last_value() {
        let vars = parse_dotenv("DUPLICATE=first\nDUPLICATE=last\n");
        assert_eq!(vars.get("DUPLICATE").map(String::as_str), Some("last"));
    }

    #[test]
    fn parses_colon_assignments_and_keeps_unmatched_quotes_literal() {
        let vars = parse_dotenv("COLON: value\nUNMATCHED=\"literal\n");

        assert_eq!(vars.get("COLON").map(String::as_str), Some("value"));
        assert_eq!(vars.get("UNMATCHED").map(String::as_str), Some("\"literal"));
    }

    #[test]
    fn global_canopy_values_win_and_existing_process_keys_are_omitted() {
        let temp = TempDir::new();
        let global = temp.0.join("canopy");
        let home = temp.0.join("home");
        fs::create_dir_all(&global).unwrap();
        fs::create_dir_all(&home).unwrap();
        fs::write(
            global.join(".env"),
            "SHARED=global\nGLOBAL_ONLY=global\nEXISTING=file\n",
        )
        .unwrap();
        fs::write(
            home.join(".env"),
            "SHARED=home\nHOME_ONLY=home\nEXISTING=home-file\n",
        )
        .unwrap();
        let process_env = HashSet::from(["EXISTING".to_owned()]);

        let vars = get_home_env_fallback_vars(&global, &home, &process_env, false);

        assert_eq!(vars.get("SHARED").map(String::as_str), Some("global"));
        assert_eq!(vars.get("GLOBAL_ONLY").map(String::as_str), Some("global"));
        assert_eq!(vars.get("HOME_ONLY").map(String::as_str), Some("home"));
        assert!(!vars.contains_key("EXISTING"));
    }

    #[test]
    fn skips_home_dotenv_when_qwen_home_is_set() {
        let temp = TempDir::new();
        let global = temp.0.join("custom-canopy");
        let home = temp.0.join("home");
        fs::create_dir_all(&global).unwrap();
        fs::create_dir_all(&home).unwrap();
        fs::write(global.join(".env"), "GLOBAL=loaded\n").unwrap();
        fs::write(home.join(".env"), "HOME=skipped\n").unwrap();
        let process_env = HashSet::from(["QWEN_HOME".to_owned()]);

        let vars = get_home_env_fallback_vars(&global, &home, &process_env, true);

        assert_eq!(vars.get("GLOBAL").map(String::as_str), Some("loaded"));
        assert!(!vars.contains_key("HOME"));
    }

    #[test]
    fn empty_qwen_home_still_allows_the_home_dotenv_fallback() {
        let temp = TempDir::new();
        let global = temp.0.join("custom-canopy");
        let home = temp.0.join("home");
        fs::create_dir_all(&global).unwrap();
        fs::create_dir_all(&home).unwrap();
        fs::write(home.join(".env"), "HOME=loaded\n").unwrap();
        let process_env = HashSet::from(["QWEN_HOME".to_owned()]);

        let vars = get_home_env_fallback_vars(&global, &home, &process_env, false);

        assert_eq!(vars.get("HOME").map(String::as_str), Some("loaded"));
    }
}
