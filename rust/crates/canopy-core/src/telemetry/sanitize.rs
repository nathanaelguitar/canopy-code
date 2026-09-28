/// Sanitize a hook command name before it is included in telemetry.
///
/// Only the first whitespace-delimited token is retained, and path prefixes
/// are removed using both Unix and Windows separators. This keeps arguments
/// (which may contain credentials) and usernames in absolute paths out of the
/// event.
pub fn sanitize_hook_name(hook_name: &str) -> String {
    let trimmed = hook_name.trim_matches(is_javascript_whitespace);
    let Some(command) = trimmed.split(is_javascript_whitespace).next() else {
        return "unknown-command".to_owned();
    };

    let basename = command.rsplit(['/', '\\']).next().unwrap_or_default();
    if basename.is_empty() {
        "unknown-command".to_owned()
    } else {
        basename.to_owned()
    }
}

/// ECMAScript's `String.trim()` and regular-expression `\s` character set.
///
/// Rust's `char::is_whitespace` uses the Unicode `White_Space` property, which
/// differs at a few code points (for example, U+0085 is whitespace in Rust but
/// not in ECMAScript). Spell out the ECMAScript set so command tokenization
/// agrees with the TypeScript implementation.
fn is_javascript_whitespace(ch: char) -> bool {
    matches!(
        ch,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
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

#[cfg(test)]
mod tests {
    use super::sanitize_hook_name;

    #[test]
    fn empty_or_whitespace_only_names_use_fallback() {
        assert_eq!(sanitize_hook_name(""), "unknown-command");
        assert_eq!(sanitize_hook_name("   "), "unknown-command");
        assert_eq!(sanitize_hook_name("\t\n\r"), "unknown-command");
    }

    #[test]
    fn extracts_unix_path_basename() {
        assert_eq!(sanitize_hook_name("/usr/bin/git"), "git");
        assert_eq!(
            sanitize_hook_name("/path/to/.gemini/hooks/check-secrets.sh"),
            "check-secrets.sh"
        );
        assert_eq!(
            sanitize_hook_name("/home/user/script.py --arg=value"),
            "script.py"
        );
    }

    #[test]
    fn extracts_windows_path_basename() {
        assert_eq!(
            sanitize_hook_name(r"C:\Windows\System32\cmd.exe"),
            "cmd.exe"
        );
        assert_eq!(
            sanitize_hook_name(r"C:\Users\User\Documents\test.bat /c"),
            "test.bat"
        );
    }

    #[test]
    fn returns_simple_command_without_arguments() {
        assert_eq!(sanitize_hook_name("git status"), "git");
        assert_eq!(sanitize_hook_name("node index.js"), "node");
        assert_eq!(
            sanitize_hook_name("python script.py --api-key=abc123"),
            "python"
        );
        assert_eq!(sanitize_hook_name("simple-command"), "simple-command");
        assert_eq!(sanitize_hook_name("one-word"), "one-word");
    }

    #[test]
    fn handles_relative_paths_and_complex_command_lines() {
        assert_eq!(sanitize_hook_name("./my-script.sh"), "my-script.sh");
        assert_eq!(sanitize_hook_name("../tools/tool.exe"), "tool.exe");
        assert_eq!(
            sanitize_hook_name("/path/to/.gemini/hooks/check-secrets.sh --api-key=abc123"),
            "check-secrets.sh"
        );
        assert_eq!(
            sanitize_hook_name("python /home/user/script.py --token=xyz --verbose"),
            "python"
        );
    }

    #[test]
    fn malformed_separator_only_paths_use_fallback() {
        assert_eq!(sanitize_hook_name("/"), "unknown-command");
        assert_eq!(sanitize_hook_name("\\"), "unknown-command");
    }

    #[test]
    fn matches_ecmascript_whitespace_for_trimming_and_splitting() {
        assert_eq!(sanitize_hook_name("\u{FEFF}git\u{00A0}status"), "git");
        // U+0085 is Unicode White_Space, but ECMAScript does not treat it as
        // trim or `\s`; preserve it inside the first command token.
        assert_eq!(sanitize_hook_name("git\u{0085}status"), "git\u{0085}status");
    }
}
