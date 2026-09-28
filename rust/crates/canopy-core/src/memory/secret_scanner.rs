//! High-confidence credential detection for repository-shared team memory.
//!
//! This is a Rust port of `packages/core/src/memory/secret-scanner.ts`. The
//! scanner reports rule IDs and readable labels only; it never returns the
//! matched text.

use std::collections::VecDeque;
use std::sync::OnceLock;

use regex::{Regex, RegexBuilder};
use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SecretMatch {
    #[serde(rename = "ruleId")]
    pub rule_id: String,
    pub label: String,
}

struct SecretRule {
    id: &'static str,
    pattern: &'static str,
}

// Keep source order. `scan_for_secrets` returns at most one result for each
// rule, in this order, even when a rule's expression can find several values.
const SECRET_RULES: &[SecretRule] = &[
    SecretRule {
        id: "aws-access-token",
        pattern: r"(?:^|[^A-Za-z0-9_])(?:A3T[A-Z0-9]|AKIA|ASIA|ABIA|ACCA)[A-Z0-9]{16}(?:$|[^A-Za-z0-9_])",
    },
    SecretRule {
        id: "alibaba-cloud-access-key",
        pattern: r"(?:^|[^A-Za-z0-9_])LTAI[a-zA-Z0-9]{12,20}(?:$|[^A-Za-z0-9_])",
    },
    SecretRule {
        id: "gcp-api-key",
        pattern: r"AIza[A-Za-z0-9_-]{35}",
    },
    SecretRule {
        id: "gcp-oauth-client-secret",
        pattern: r"GOCSPX-[A-Za-z0-9_-]{24}",
    },
    SecretRule {
        id: "digitalocean-pat",
        pattern: r"dop_v1_[a-f0-9]{64}",
    },
    SecretRule {
        id: "anthropic-api-key",
        pattern: r"sk-ant-[a-zA-Z0-9_-]{20,}",
    },
    SecretRule {
        id: "openai-api-key",
        pattern: r"sk-(?:proj|svcacct|admin)-[a-zA-Z0-9_-]{20,}|sk-[a-zA-Z0-9_-]{20,512}T3BlbkFJ[a-zA-Z0-9_-]{20,512}|sk-[a-zA-Z0-9]{48}",
    },
    SecretRule {
        id: "huggingface-access-token",
        pattern: r"hf_[a-zA-Z0-9]{34,}",
    },
    SecretRule {
        id: "github-pat",
        pattern: r"ghp_[0-9a-zA-Z]{36}",
    },
    SecretRule {
        id: "github-fine-grained-pat",
        pattern: r"github_pat_[A-Za-z0-9_]{82}",
    },
    SecretRule {
        id: "github-app-token",
        pattern: r"(?:ghu|ghs)_[0-9a-zA-Z]{36}",
    },
    SecretRule {
        id: "github-oauth",
        pattern: r"gho_[0-9a-zA-Z]{36}",
    },
    SecretRule {
        id: "gitlab-pat",
        pattern: r"glpat-[A-Za-z0-9_-]{20}",
    },
    SecretRule {
        id: "slack-bot-token",
        pattern: r"xox[b]-[0-9]{10,13}-[0-9]{10,13}[a-zA-Z0-9-]*",
    },
    SecretRule {
        id: "slack-app-token",
        pattern: r"[xX][aA][pP][pP]-[0-9]-[A-Za-z0-9]+-[0-9]+-[A-Za-z0-9]+",
    },
    SecretRule {
        id: "sendgrid-api-token",
        pattern: r"SG\.[A-Za-z0-9_-]{22}\.[A-Za-z0-9_-]{43}",
    },
    SecretRule {
        id: "npm-access-token",
        pattern: r"npm_[a-zA-Z0-9]{36}",
    },
    SecretRule {
        id: "stripe-access-token",
        pattern: r"(?:sk|rk)_(?:test|live|prod)_[a-zA-Z0-9]{10,99}",
    },
];

static COMPILED_RULES: OnceLock<Vec<(&'static str, Regex)>> = OnceLock::new();
static PRIVATE_KEY_BEGIN: OnceLock<Regex> = OnceLock::new();
static PRIVATE_KEY_END: OnceLock<Regex> = OnceLock::new();

fn compiled_rules() -> &'static [(&'static str, Regex)] {
    COMPILED_RULES.get_or_init(|| {
        SECRET_RULES
            .iter()
            .map(|rule| {
                (
                    rule.id,
                    Regex::new(rule.pattern).expect("secret detection regex is valid"),
                )
            })
            .collect()
    })
}

/// Scan text for credential patterns. Results are deduplicated by rule ID and
/// contain no matched value or location.
pub fn scan_for_secrets(content: &str) -> Vec<SecretMatch> {
    let mut matches = compiled_rules()
        .iter()
        .filter(|(_, pattern)| pattern.is_match(content))
        .map(|(id, _)| SecretMatch {
            rule_id: (*id).to_owned(),
            label: rule_id_to_label(id),
        })
        .collect::<Vec<_>>();
    if contains_private_key(content) {
        matches.push(SecretMatch {
            rule_id: "private-key".to_owned(),
            label: rule_id_to_label("private-key"),
        });
    }
    matches
}

fn rule_id_to_label(rule_id: &str) -> String {
    rule_id
        .split('-')
        .map(|part| match part {
            "aws" => "AWS".to_owned(),
            "gcp" => "GCP".to_owned(),
            "api" => "API".to_owned(),
            "pat" => "PAT".to_owned(),
            "oauth" => "OAuth".to_owned(),
            "npm" => "NPM".to_owned(),
            "github" => "GitHub".to_owned(),
            "gitlab" => "GitLab".to_owned(),
            "openai" => "OpenAI".to_owned(),
            "digitalocean" => "DigitalOcean".to_owned(),
            "huggingface" => "HuggingFace".to_owned(),
            "alibaba" => "Alibaba".to_owned(),
            "sendgrid" => "SendGrid".to_owned(),
            _ => {
                let mut chars = part.chars();
                match chars.next() {
                    Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                    None => String::new(),
                }
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn private_key_begin_regex() -> &'static Regex {
    PRIVATE_KEY_BEGIN.get_or_init(|| {
        // Enumerate the earliest viable terminator here; the source's greedy
        // prefix backtracks when the full body/footer expression fails.
        RegexBuilder::new(r"-----BEGIN[ A-Z0-9_-]{0,100}?PRIVATE KEY(?: BLOCK)?-----")
            // Source JS has `/i` without `/u`, so case folding is ASCII-only.
            .case_insensitive(true)
            .unicode(false)
            .build()
            .expect("private-key begin marker regex is valid")
    })
}

fn private_key_end_regex() -> &'static Regex {
    PRIVATE_KEY_END.get_or_init(|| {
        RegexBuilder::new(r"-----END[ A-Z0-9_-]{0,100}?PRIVATE KEY(?: BLOCK)?-----")
            .case_insensitive(true)
            .unicode(false)
            .build()
            .expect("private-key end marker regex is valid")
    })
}

/// The private-key source expression counts UTF-16 code units in its bounded
/// header and body repetitions. Match markers with regexes, then compare their
/// distances in UTF-16 units so astral characters behave like JavaScript's
/// non-Unicode regex mode.
fn contains_private_key(content: &str) -> bool {
    let headers = private_key_begin_regex()
        .find_iter(content)
        .map(|marker| marker.end())
        .collect::<Vec<_>>();
    let footers = private_key_end_regex()
        .find_iter(content)
        .map(|marker| marker.start())
        .collect::<Vec<_>>();
    if headers.is_empty() || footers.is_empty() {
        return false;
    }

    // Both offset lists are ordered. Convert all relevant byte offsets in one
    // pass over the string rather than repeatedly rescanning prefixes.
    let mut events = Vec::with_capacity(headers.len() + footers.len());
    events.extend(
        headers
            .iter()
            .enumerate()
            .map(|(index, offset)| (*offset, true, index)),
    );
    events.extend(
        footers
            .iter()
            .enumerate()
            .map(|(index, offset)| (*offset, false, index)),
    );
    events.sort_unstable_by_key(|(offset, _, _)| *offset);

    let mut header_units = vec![0usize; headers.len()];
    let mut footer_units = vec![0usize; footers.len()];
    let mut byte_cursor = 0usize;
    let mut unit_cursor = 0usize;
    for (offset, is_header, index) in events {
        unit_cursor += content[byte_cursor..offset].encode_utf16().count();
        if is_header {
            header_units[index] = unit_cursor;
        } else {
            footer_units[index] = unit_cursor;
        }
        byte_cursor = offset;
    }

    let mut pending = VecDeque::new();
    let mut next_header = 0usize;
    for footer_units in footer_units {
        while header_units
            .get(next_header)
            .is_some_and(|header| header.saturating_add(64) <= footer_units)
        {
            pending.push_back(header_units[next_header]);
            next_header += 1;
        }
        while pending
            .front()
            .is_some_and(|header| footer_units.saturating_sub(*header) > 16_384)
        {
            pending.pop_front();
        }
        if !pending.is_empty() {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{SecretMatch, private_key_begin_regex, private_key_end_regex, scan_for_secrets};

    fn has_rule(content: &str, id: &str) -> bool {
        scan_for_secrets(content)
            .iter()
            .any(|found| found.rule_id == id)
    }

    #[test]
    fn returns_no_matches_for_clean_content() {
        assert!(
            scan_for_secrets("Integration tests must hit a real database, not mocks.").is_empty()
        );
    }

    #[test]
    fn detects_one_canonical_sample_per_rule_in_source_order() {
        let samples = [
            ("aws-access-token", "AKIAIOSFODNN7EXAMPLE".to_owned()),
            (
                "alibaba-cloud-access-key",
                format!("LTAI{}", "a".repeat(16)),
            ),
            ("gcp-api-key", format!("AIza{}", "a".repeat(35))),
            (
                "gcp-oauth-client-secret",
                format!("GOCSPX-{}", "a".repeat(24)),
            ),
            ("digitalocean-pat", format!("dop_v1_{}", "a".repeat(64))),
            (
                "anthropic-api-key",
                format!("sk-ant-api03-{}", "a".repeat(30)),
            ),
            ("openai-api-key", format!("sk-proj-{}", "a".repeat(48))),
            (
                "huggingface-access-token",
                format!("hf_a1{}", "b".repeat(32)),
            ),
            ("github-pat", format!("ghp_{}", "a".repeat(36))),
            (
                "github-fine-grained-pat",
                format!("github_pat_{}", "a".repeat(82)),
            ),
            ("github-app-token", format!("ghu_{}", "a".repeat(36))),
            ("github-oauth", format!("gho_{}", "a".repeat(36))),
            ("gitlab-pat", format!("glpat-{}", "a".repeat(20))),
            (
                "slack-bot-token",
                format!("xoxb-{}-{}abcd", "1".repeat(12), "1".repeat(12)),
            ),
            (
                "slack-app-token",
                "xapp-1-ABC123-1234567890-abcdef".to_owned(),
            ),
            (
                "sendgrid-api-token",
                format!("SG.{}.{}", "a".repeat(22), "b".repeat(43)),
            ),
            ("npm-access-token", format!("npm_{}", "a".repeat(36))),
            ("stripe-access-token", format!("sk_live_{}", "a".repeat(24))),
        ];
        let content = samples
            .iter()
            .map(|(_, sample)| sample.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        let actual = scan_for_secrets(&content)
            .into_iter()
            .map(|found| found.rule_id)
            .collect::<Vec<_>>();
        let expected = samples
            .iter()
            .map(|(id, _)| (*id).to_owned())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn detects_all_current_openai_key_formats() {
        for sample in [
            format!("sk-proj-{}", "a".repeat(48)),
            format!("sk-svcacct-{}", "a".repeat(48)),
            format!("sk-{}T3BlbkFJ{}", "a".repeat(20), "b".repeat(20)),
            format!("sk-{}_x_T3BlbkFJ{}_y", "a".repeat(18), "b".repeat(18)),
        ] {
            assert!(has_rule(&sample, "openai-api-key"), "{sample}");
        }
    }

    #[test]
    fn accepts_punctuation_after_tokens_and_ascii_word_boundaries_for_cloud_keys() {
        let anthropic = format!("sk-ant-api03-{}", "a".repeat(30));
        for delimiter in ['.', ',', ')', '}'] {
            assert!(has_rule(
                &format!("{anthropic}{delimiter}"),
                "anthropic-api-key"
            ));
        }
        assert!(has_rule("(AKIA0918ABCDEFGH9012)", "aws-access-token"));
        assert!(!has_rule("xAKIA0918ABCDEFGH9012", "aws-access-token"));
        assert!(!has_rule("AKIA0918ABCDEFGH9012x", "aws-access-token"));
    }

    #[test]
    fn detects_pem_private_keys_and_avoids_matching_incomplete_markers() {
        let pem = format!(
            "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----",
            "A".repeat(120)
        );
        assert!(has_rule(&pem, "private-key"));
        let payload = ["-----BEGIN PRIVATE KEY-----"; 4000].join("\n");
        assert!(!has_rule(&payload, "private-key"));
    }

    #[test]
    fn private_key_bounds_count_utf16_code_units_like_javascript() {
        let exact_min = format!(
            "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
            "😀".repeat(32)
        );
        let under_min = format!(
            "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
            "😀".repeat(31)
        );
        assert!(has_rule(&exact_min, "private-key"));
        assert!(!has_rule(&under_min, "private-key"));
    }

    #[test]
    fn private_key_header_and_body_keep_the_source_bounds_and_ascii_case_fold() {
        let make_key = |padding: usize, body_len: usize| {
            format!(
                "-----BEGIN{}PRIVATE KEY-----{}-----END PRIVATE KEY-----",
                "X".repeat(padding),
                "A".repeat(body_len)
            )
        };
        assert!(private_key_begin_regex().is_match("-----begin private key-----"));
        assert!(private_key_end_regex().is_match("-----end private key-----"));
        assert!(has_rule(&make_key(100, 16_384), "private-key"));
        assert!(!has_rule(&make_key(100, 16_385), "private-key"));
        assert!(has_rule(
            &"-----begin private key-----{}-----end private key-----"
                .replace("{}", &"A".repeat(64)),
            "private-key"
        ));
        assert!(!has_rule(&make_key(101, 64), "private-key"));
    }

    #[test]
    fn result_matches_javascript_shape_and_does_not_expose_secret_text() {
        let secret = format!("ghp_{}", "b".repeat(36));
        let matches = scan_for_secrets(&secret);
        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches[0],
            SecretMatch {
                rule_id: "github-pat".to_owned(),
                label: "GitHub PAT".to_owned(),
            }
        );
        let json = serde_json::to_string(&matches).unwrap();
        assert!(json.contains("\"ruleId\":\"github-pat\""));
        assert!(!json.contains(&secret));
    }

    #[test]
    fn formats_rule_labels_with_source_special_cases() {
        assert_eq!(
            scan_for_secrets("AKIAIOSFODNN7EXAMPLE")[0].label,
            "AWS Access Token"
        );
        assert_eq!(
            scan_for_secrets(&format!("LTAI{}", "a".repeat(16)))[0].label,
            "Alibaba Cloud Access Key"
        );
        assert_eq!(
            scan_for_secrets(&format!("github_pat_{}", "a".repeat(82)))[0].label,
            "GitHub Fine Grained PAT"
        );
    }

    #[test]
    fn does_not_match_one_character_short_of_minimum() {
        assert!(!has_rule(&format!("ghp_{} ", "a".repeat(35)), "github-pat"));
    }

    #[test]
    fn openai_marker_expression_stays_linear_on_long_nonmatches() {
        let payload = format!("sk-{}", "a-".repeat(50_000));
        assert!(!has_rule(&payload, "openai-api-key"));
    }
}
