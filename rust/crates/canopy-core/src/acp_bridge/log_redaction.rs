//! Credential redaction applied to child stderr before daemon logging.

use std::sync::OnceLock;

use regex::Regex;

const REDACTED: &str = "<redacted>";
static PATTERNS: OnceLock<Vec<(Regex, &'static str)>> = OnceLock::new();

pub fn redact_log_credentials(line: &str) -> String {
    let patterns = PATTERNS.get_or_init(|| {
        [
            (r"(?i)(Bearer\s+)[A-Za-z0-9._~+/=-]+", "$1<redacted>"),
            (r"(?i)(QQBot\s+)[A-Za-z0-9._~+/=-]+", "$1<redacted>"),
            (r"(?i)(Authorization:\s*)\S+(?:\s+\S+)?", "$1<redacted>"),
            (r"(?i)(x-acs-dingtalk-access-token:\s*)\S+", "$1<redacted>"),
            (r"sk-[a-zA-Z0-9-]{20,}", "sk-<redacted>"),
            (r"(?:ghp_|gho_|ghs_|ghu_|github_pat_|glpat-|xox[b]-|xox[p]-)[a-zA-Z0-9_-]{20,}", "<redacted>"),
            (r"(?:AKIA|ASIA)[A-Z0-9]{16}", "<redacted>"),
            (r"(?i)((?:api[_-]?key|token|secret|password|pwd)[_-]?[=:]\s*)\S{10,}", "$1<redacted>"),
            (r"([A-Z][A-Z0-9]{0,50}(?:_[A-Z0-9]{1,50}){0,10}_(?:KEY|TOKEN|SECRET|PASSWORD)\s*[=:]\s*)\S{10,}", "$1<redacted>"),
            (r#"(?i)(\"(?:api_key|api-key|apikey|token|secret|password|pwd|access_token|client_secret|app_secret|authorization)\"\s*:\s*\")[^\"]{10,}(\")"#, "$1<redacted>$2"),
            (r"(?i)\b([a-z][a-z0-9+.-]{0,31}://)(?:[^/\s]+@)+", "$1<redacted>@"),
        ].into_iter().map(|(pattern, replacement)| (Regex::new(pattern).expect("redaction regex is valid"), replacement)).collect()
    });
    let mut result = line.to_owned();
    for (pattern, replacement) in patterns {
        result = pattern.replace_all(&result, *replacement).into_owned();
    }
    result
}

pub fn redacted_marker() -> &'static str {
    REDACTED
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn redacts_tokens_headers_env_values_and_url_credentials() {
        let source = "Bearer abc.def Authorization: Basic verylongsecretvalue QWEN_API_TOKEN=abcdefghijkl https://user:password@example.com";
        let redacted = redact_log_credentials(source);
        assert!(!redacted.contains("abc.def"));
        assert!(!redacted.contains("verylongsecretvalue"));
        assert!(!redacted.contains("abcdefghijkl"));
        assert!(!redacted.contains("user:password"));
    }
}
