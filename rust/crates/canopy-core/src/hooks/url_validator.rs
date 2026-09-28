//! URL allowlist and SSRF checks for HTTP hooks.
//!
//! Port of `packages/core/src/hooks/urlValidator.ts`.
//!
//! Compatibility gaps:
//! - This module checks IP literals only. It does not resolve DNS names or
//!   protect the connection step against DNS rebinding; the TypeScript runtime
//!   has a separate `ssrfGuardedLookup` helper for that.
//! - URL parsing is provided by `reqwest::Url` (the Rust `url` crate), whose
//!   normalization and error cases are close to but not identical to the
//!   WHATWG `URL` implementation in Node.js.
//! - Wildcard patterns are translated using the same escaping rule, but Rust's
//!   `regex` syntax is not identical to JavaScript `RegExp`. Invalid patterns
//!   are returned as constructor errors instead of throwing from construction.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use regex::{Regex, RegexBuilder};
use reqwest::Url;

const BLOCKED_HOSTS: &[&str] = &[
    "localhost.localdomain",
    "ip6-localhost",
    "ip6-loopback",
    "metadata.google.internal",
    "169.254.169.254",
    "metadata.azure.internal",
];

/// Result returned by [`UrlValidator::validate`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationResult {
    pub allowed: bool,
    pub reason: Option<String>,
}

/// Validate hook URLs against an optional wildcard allowlist and SSRF rules.
#[derive(Clone, Debug)]
pub struct UrlValidator {
    allowed_patterns: Vec<String>,
    compiled_patterns: Vec<Regex>,
    allow_private_network_hosts: bool,
}

impl Default for UrlValidator {
    fn default() -> Self {
        Self::new(Vec::<String>::new(), false).expect("an empty URL allowlist always compiles")
    }
}

impl UrlValidator {
    /// Create a validator, compiling each pattern as an anchored, case-
    /// insensitive regular expression. `*` matches any number of characters.
    ///
    /// A pattern containing `\\.` is treated as pre-escaped, matching the
    /// source's compatibility rule: in that case only `*` is transformed.
    pub fn new<I, S>(
        allowed_patterns: I,
        allow_private_network_hosts: bool,
    ) -> Result<Self, regex::Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let allowed_patterns: Vec<String> = allowed_patterns
            .into_iter()
            .map(|pattern| pattern.as_ref().to_owned())
            .collect();
        let compiled_patterns = allowed_patterns
            .iter()
            .map(|pattern| compile_pattern(pattern))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            allowed_patterns,
            compiled_patterns,
            allow_private_network_hosts,
        })
    }

    /// Check whether the raw URL string matches the allowlist.
    /// An empty allowlist allows all strings, as in the source implementation.
    pub fn is_allowed(&self, url: &str) -> bool {
        self.allowed_patterns.is_empty()
            || self
                .compiled_patterns
                .iter()
                .any(|pattern| pattern.is_match(url))
    }

    /// Return whether the URL is blocked by the hostname or IP-literal rules.
    /// Invalid absolute URLs are blocked.
    pub fn is_blocked(&self, url: &str) -> bool {
        let Ok(parsed) = Url::parse(url) else {
            return true;
        };
        let Some(hostname) = parsed.host_str() else {
            // Node's URL parser returns an empty hostname for valid hostless
            // schemes such as `data:` and `mailto:`; they pass this layer.
            return false;
        };
        let hostname = hostname
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(hostname)
            .to_ascii_lowercase();

        if BLOCKED_HOSTS.contains(&hostname.as_str()) {
            return true;
        }

        let Ok(address) = hostname.parse::<IpAddr>() else {
            return false;
        };

        if is_metadata_address(address) {
            return true;
        }

        !self.allow_private_network_hosts && is_blocked_address(address)
    }

    /// Validate a URL, applying SSRF checks before its allowlist.
    pub fn validate(&self, url: &str) -> ValidationResult {
        if self.is_blocked(url) {
            return ValidationResult {
                allowed: false,
                reason: Some("URL is blocked for security reasons (SSRF protection)".to_owned()),
            };
        }

        if !self.is_allowed(url) {
            return ValidationResult {
                allowed: false,
                reason: Some(format!(
                    "URL does not match any allowed pattern. Allowed patterns: {}",
                    self.allowed_patterns.join(", ")
                )),
            };
        }

        ValidationResult {
            allowed: true,
            reason: None,
        }
    }
}

/// Create a validator from optional configuration, defaulting to no allowlist
/// and the normal private-network blocking policy.
pub fn create_url_validator(
    allowed_urls: Option<Vec<String>>,
    allow_private_network_hosts: Option<bool>,
) -> Result<UrlValidator, regex::Error> {
    UrlValidator::new(
        allowed_urls.unwrap_or_default(),
        allow_private_network_hosts.unwrap_or(false),
    )
}

fn compile_pattern(pattern: &str) -> Result<Regex, regex::Error> {
    let pre_escaped = pattern.contains("\\.");
    let mut translated = String::with_capacity(pattern.len() + 8);

    for character in pattern.chars() {
        if character == '*' {
            translated.push_str(".*");
        } else if !pre_escaped && is_regex_metacharacter(character) {
            translated.push('\\');
            translated.push(character);
        } else {
            translated.push(character);
        }
    }

    RegexBuilder::new(&format!("^{translated}$"))
        .case_insensitive(true)
        .build()
}

fn is_regex_metacharacter(character: char) -> bool {
    matches!(
        character,
        '.' | '+' | '?' | '^' | '$' | '{' | '}' | '(' | ')' | '|' | '[' | ']' | '\\'
    )
}

fn is_metadata_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            address == Ipv4Addr::new(169, 254, 169, 254)
                || address == Ipv4Addr::new(100, 100, 100, 200)
        }
        IpAddr::V6(address) => mapped_ipv4(address).is_some_and(|mapped| {
            mapped == Ipv4Addr::new(169, 254, 169, 254)
                || mapped == Ipv4Addr::new(100, 100, 100, 200)
        }),
    }
}

fn is_blocked_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_blocked_v4(address),
        IpAddr::V6(address) => is_blocked_v6(address),
    }
}

fn is_blocked_v4(address: Ipv4Addr) -> bool {
    let [first, second, _, _] = address.octets();
    if first == 127 {
        return false;
    }

    first == 0
        || first == 10
        || (first == 100 && (64..=127).contains(&second))
        || (first == 169 && second == 254)
        || (first == 172 && (16..=31).contains(&second))
        || (first == 192 && second == 168)
}

fn is_blocked_v6(address: Ipv6Addr) -> bool {
    if address == Ipv6Addr::LOCALHOST {
        return false;
    }
    if address.is_unspecified() {
        return true;
    }
    if let Some(mapped) = mapped_ipv4(address) {
        return is_blocked_v4(mapped);
    }

    let first = address.segments()[0];
    (first & 0xfe00) == 0xfc00 || (first & 0xffc0) == 0xfe80
}

fn mapped_ipv4(address: Ipv6Addr) -> Option<Ipv4Addr> {
    let segments = address.segments();
    if segments[..5] != [0, 0, 0, 0, 0] || segments[5] != 0xffff {
        return None;
    }

    let high = segments[6].to_be_bytes();
    let low = segments[7].to_be_bytes();
    Some(Ipv4Addr::new(high[0], high[1], low[0], low[1]))
}

#[cfg(test)]
mod tests {
    use super::{UrlValidator, create_url_validator};

    fn make_validator(patterns: &[&str]) -> UrlValidator {
        UrlValidator::new(patterns, false).expect("test patterns compile")
    }

    #[test]
    fn allows_loopback_and_localhost_but_blocks_private_networks_by_default() {
        let validator = make_validator(&[]);
        for url in [
            "http://127.0.0.1:8080/api",
            "http://localhost:9876/hook",
            "http://[::1]/hook",
        ] {
            assert!(!validator.is_blocked(url), "{url}");
        }
        for url in [
            "http://192.168.1.1/api",
            "http://10.0.0.1/api",
            "http://172.31.255.255/api",
            "http://100.100.100.10/api",
            "http://[fc00::1]/api",
            "http://[fe80::1]/api",
        ] {
            assert!(validator.is_blocked(url), "{url}");
        }
    }

    #[test]
    fn metadata_addresses_and_hostname_blocklist_remain_blocked_when_private_hosts_are_allowed() {
        let validator = UrlValidator::new(Vec::<String>::new(), true).unwrap();
        for url in [
            "http://169.254.169.254/latest/meta-data",
            "http://100.100.100.200/latest/meta-data",
            "http://[::ffff:169.254.169.254]/latest/meta-data",
            "http://[::ffff:a9fe:a9fe]/latest/meta-data",
            "http://[::ffff:6464:64c8]/latest/meta-data",
            "http://metadata.google.internal/computeMetadata",
            "http://metadata.azure.internal/",
            "http://localhost.localdomain/",
            "http://ip6-localhost/",
            "http://ip6-loopback/",
        ] {
            assert!(validator.is_blocked(url), "{url}");
        }
        assert!(!validator.is_blocked("http://192.168.1.1/api"));
        assert!(!validator.is_blocked("http://[::ffff:ac10:1]/api"));
    }

    #[test]
    fn malformed_urls_are_blocked_and_public_hosts_are_allowed() {
        let validator = make_validator(&[]);
        assert!(validator.is_blocked("not-a-url"));
        assert!(validator.is_blocked(""));
        assert!(!validator.is_blocked("https://api.example.com/hook"));
    }

    #[test]
    fn matches_exact_wildcard_multiple_and_case_insensitive_patterns() {
        let exact = make_validator(&["https://api\\.example\\.com/hook"]);
        assert!(exact.is_allowed("https://api.example.com/hook"));
        assert!(!exact.is_allowed("https://api.example.com/other"));

        let wildcard = make_validator(&[
            "https://API\\.example\\.com/*",
            "https://hooks\\.example\\.com/*",
        ]);
        assert!(wildcard.is_allowed("https://api.example.com/v1/hook"));
        assert!(wildcard.is_allowed("https://hooks.example.com/test"));
        assert!(!wildcard.is_allowed("https://other.example.com/hook"));
    }

    #[test]
    fn escapes_unescaped_regex_metacharacters_and_keeps_preescaped_regex_behavior() {
        let unescaped = make_validator(&["https://api.example.com/a+b?x=1"]);
        assert!(unescaped.is_allowed("https://api.example.com/a+b?x=1"));
        assert!(!unescaped.is_allowed("https://api.example.com/aaab?x=1"));

        // Once `\\.` appears, the source treats the whole pattern as an
        // authored regex, so the `+` remains a quantifier.
        let pre_escaped = make_validator(&["https://api\\.example\\.com/a+"]);
        assert!(pre_escaped.is_allowed("https://api.example.com/aa"));
        assert!(!pre_escaped.is_allowed("https://api.example.com/a+"));
    }

    #[test]
    fn validate_checks_ssrf_before_allowlist_and_reports_source_reasons() {
        let validator = make_validator(&["*"]);
        let blocked = validator.validate("http://192.168.1.1:8080/api");
        assert!(!blocked.allowed);
        assert_eq!(
            blocked.reason.as_deref(),
            Some("URL is blocked for security reasons (SSRF protection)")
        );

        let not_allowed = make_validator(&["https://api\\.example\\.com/*"])
            .validate("https://other.example.com/hook");
        assert!(!not_allowed.allowed);
        assert!(
            not_allowed
                .reason
                .as_deref()
                .unwrap()
                .contains("does not match")
        );

        assert_eq!(validator.validate("http://localhost/hook").reason, None);
    }

    #[test]
    fn factory_defaults_missing_configuration_and_rejects_invalid_regex() {
        let validator = create_url_validator(None, None).unwrap();
        assert!(validator.is_allowed("https://any.example.com/hook"));
        assert!(UrlValidator::new(["\\.["], false).is_err());
    }
}
