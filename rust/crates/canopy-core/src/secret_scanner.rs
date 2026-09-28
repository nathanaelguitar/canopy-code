use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use regex::Regex;

struct SecretRule {
    id: &'static str,
    label: &'static str,
    pattern: &'static str,
}

const RULES: &[SecretRule] = &[
    SecretRule {
        id: "aws-access-token",
        label: "AWS Access Token",
        pattern: r"(?:^|[^A-Za-z0-9_])(?:A3T[A-Z0-9]|AKIA|ASIA|ABIA|ACCA)[A-Z0-9]{16}(?:$|[^A-Za-z0-9_])",
    },
    SecretRule {
        id: "alibaba-cloud-access-key",
        label: "Alibaba Cloud Access Key",
        pattern: r"(?:^|[^A-Za-z0-9_])LTAI[a-zA-Z0-9]{12,20}(?:$|[^A-Za-z0-9_])",
    },
    SecretRule {
        id: "gcp-api-key",
        label: "GCP API Key",
        pattern: r"AIza[A-Za-z0-9_-]{35}",
    },
    SecretRule {
        id: "gcp-oauth-client-secret",
        label: "GCP OAuth Client Secret",
        pattern: r"GOCSPX-[A-Za-z0-9_-]{24}",
    },
    SecretRule {
        id: "digitalocean-pat",
        label: "DigitalOcean PAT",
        pattern: r"dop_v1_[a-f0-9]{64}",
    },
    SecretRule {
        id: "anthropic-api-key",
        label: "Anthropic API Key",
        pattern: r"sk-ant-[a-zA-Z0-9_-]{20,}",
    },
    SecretRule {
        id: "openai-api-key",
        label: "OpenAI API Key",
        pattern: r"sk-(?:proj|svcacct|admin)-[a-zA-Z0-9_-]{20,}|sk-[a-zA-Z0-9_-]{20,512}T3BlbkFJ[a-zA-Z0-9_-]{20,512}|sk-[a-zA-Z0-9]{48}",
    },
    SecretRule {
        id: "huggingface-access-token",
        label: "HuggingFace Access Token",
        pattern: r"hf_[a-zA-Z0-9]{34,}",
    },
    SecretRule {
        id: "github-pat",
        label: "GitHub PAT",
        pattern: r"ghp_[0-9a-zA-Z]{36}",
    },
    SecretRule {
        id: "github-fine-grained-pat",
        label: "GitHub Fine Grained PAT",
        pattern: r"github_pat_[A-Za-z0-9_]{82}",
    },
    SecretRule {
        id: "github-app-token",
        label: "GitHub App Token",
        pattern: r"(?:ghu|ghs)_[0-9a-zA-Z]{36}",
    },
    SecretRule {
        id: "github-oauth",
        label: "GitHub OAuth",
        pattern: r"gho_[0-9a-zA-Z]{36}",
    },
    SecretRule {
        id: "gitlab-pat",
        label: "GitLab PAT",
        pattern: r"glpat-[A-Za-z0-9_-]{20}",
    },
    SecretRule {
        id: "slack-bot-token",
        label: "Slack Bot Token",
        pattern: r"xox[b]-[0-9]{10,13}-[0-9]{10,13}[a-zA-Z0-9-]*",
    },
    SecretRule {
        id: "slack-app-token",
        label: "Slack App Token",
        pattern: r"(?i)xapp-\d-[A-Z0-9]+-\d+-[a-z0-9]+",
    },
    SecretRule {
        id: "sendgrid-api-token",
        label: "SendGrid API Token",
        pattern: r"SG\.[A-Za-z0-9_-]{22}\.[A-Za-z0-9_-]{43}",
    },
    SecretRule {
        id: "npm-access-token",
        label: "NPM Access Token",
        pattern: r"npm_[a-zA-Z0-9]{36}",
    },
    SecretRule {
        id: "stripe-access-token",
        label: "Stripe Access Token",
        pattern: r"(?:sk|rk)_(?:test|live|prod)_[a-zA-Z0-9]{10,99}",
    },
];

static COMPILED_RULES: OnceLock<Vec<(&'static str, &'static str, Regex)>> = OnceLock::new();
static PRIVATE_KEY_BEGIN: OnceLock<Regex> = OnceLock::new();
static PRIVATE_KEY_END: OnceLock<Regex> = OnceLock::new();

fn compiled_rules() -> &'static [(&'static str, &'static str, Regex)] {
    COMPILED_RULES.get_or_init(|| {
        RULES
            .iter()
            .map(|rule| {
                (
                    rule.id,
                    rule.label,
                    Regex::new(rule.pattern).expect("secret detection regex is valid"),
                )
            })
            .collect()
    })
}

/// Return a safe error if content includes a high-confidence credential and
/// the destination is in repository-shared team memory. Secret values never
/// appear in the result.
pub fn check_team_memory_secrets(
    file_path: &Path,
    content: &str,
    project_root: &Path,
) -> Option<String> {
    let team_root =
        canonicalize_nearest_existing(&find_git_root(project_root).join(".canopy/team-memory"));
    let destination = canonicalize_nearest_existing(file_path);
    if !destination.starts_with(&team_root) {
        return None;
    }
    let mut labels = compiled_rules()
        .iter()
        .filter(|(_id, _label, pattern)| pattern.is_match(content))
        .map(|(_id, label, _)| *label)
        .collect::<Vec<_>>();
    if contains_private_key(content) {
        labels.push("Private Key");
    }
    if labels.is_empty() {
        return None;
    }
    Some(format!(
        "Content contains potential secrets ({}) and cannot be written to team memory. Team memory is shared with all repository collaborators. Remove the sensitive content and try again.",
        labels.join(", ")
    ))
}

fn contains_private_key(content: &str) -> bool {
    let begin = PRIVATE_KEY_BEGIN.get_or_init(|| {
        Regex::new(r"(?i)-----BEGIN[ A-Z0-9_-]{0,100}PRIVATE KEY(?: BLOCK)?-----")
            .expect("private-key begin marker regex is valid")
    });
    let end = PRIVATE_KEY_END.get_or_init(|| {
        Regex::new(r"(?i)-----END[ A-Z0-9_-]{0,100}PRIVATE KEY(?: BLOCK)?-----")
            .expect("private-key end marker regex is valid")
    });
    let headers = begin
        .find_iter(content)
        .map(|header| header.end())
        .collect::<Vec<_>>();
    let footers = end
        .find_iter(content)
        .map(|footer| footer.start())
        .collect::<Vec<_>>();
    let mut pending = VecDeque::new();
    let mut next_header = 0;
    for footer_start in footers {
        while headers
            .get(next_header)
            .is_some_and(|header_end| header_end.saturating_add(64) <= footer_start)
        {
            pending.push_back(headers[next_header]);
            next_header += 1;
        }
        while pending
            .front()
            .is_some_and(|header_end| footer_start.saturating_sub(*header_end) > 16_384)
        {
            pending.pop_front();
        }
        if !pending.is_empty() {
            return true;
        }
    }
    false
}

fn find_git_root(project_root: &Path) -> PathBuf {
    let mut current = project_root.to_path_buf();
    loop {
        if current.join(".git").exists() {
            return current;
        }
        let Some(parent) = current.parent() else {
            return project_root.to_path_buf();
        };
        if parent == current {
            return project_root.to_path_buf();
        }
        current = parent.to_path_buf();
    }
}

fn canonicalize_nearest_existing(path: &Path) -> PathBuf {
    let mut current = path.to_path_buf();
    let mut missing = Vec::new();
    loop {
        match std::fs::canonicalize(&current) {
            Ok(mut canonical) => {
                for part in missing.into_iter().rev() {
                    canonical.push(part);
                }
                return canonical;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(name) = current.file_name() else {
                    return path.to_path_buf();
                };
                missing.push(name.to_os_string());
                let Some(parent) = current.parent() else {
                    return path.to_path_buf();
                };
                current = parent.to_path_buf();
            }
            Err(_) => return path.to_path_buf(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    struct TempWorkspace(PathBuf);

    impl TempWorkspace {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("canopy-secret-check-{}", Uuid::new_v4()));
            std::fs::create_dir_all(path.join(".git")).unwrap();
            Self(path)
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn blocks_curated_secret_families_only_in_team_memory_without_exposing_values() {
        let workspace = TempWorkspace::new();
        let target = workspace.0.join(".canopy/team-memory/notes.md");
        let secret = format!("ghp_{}", "a".repeat(36));
        let error = check_team_memory_secrets(&target, &secret, &workspace.0).unwrap();
        assert!(error.contains("GitHub PAT"));
        assert!(!error.contains(&secret));
        assert!(
            check_team_memory_secrets(&workspace.0.join("notes.md"), &secret, &workspace.0)
                .is_none()
        );
        assert!(check_team_memory_secrets(&target, "A safe design note.", &workspace.0).is_none());
    }

    #[test]
    fn recognizes_the_curated_provider_and_account_token_formats() {
        let workspace = TempWorkspace::new();
        let target = workspace.0.join(".canopy/team-memory/notes.md");
        let cases = [
            ("AWS Access Token", "AKIAIOSFODNN7EXAMPLE".to_owned()),
            (
                "Alibaba Cloud Access Key",
                format!("LTAI{}", "a".repeat(16)),
            ),
            ("GCP API Key", format!("AIza{}", "a".repeat(35))),
            (
                "GCP OAuth Client Secret",
                format!("GOCSPX-{}", "a".repeat(24)),
            ),
            ("DigitalOcean PAT", format!("dop_v1_{}", "a".repeat(64))),
            ("Anthropic API Key", format!("sk-ant-{}", "a".repeat(20))),
            ("OpenAI API Key", format!("sk-proj-{}", "a".repeat(20))),
            ("HuggingFace Access Token", format!("hf_{}", "a".repeat(34))),
            ("GitHub PAT", format!("ghp_{}", "a".repeat(36))),
            (
                "GitHub Fine Grained PAT",
                format!("github_pat_{}", "a".repeat(82)),
            ),
            ("GitHub App Token", format!("ghu_{}", "a".repeat(36))),
            ("GitHub OAuth", format!("gho_{}", "a".repeat(36))),
            ("GitLab PAT", format!("glpat-{}", "a".repeat(20))),
            (
                "Slack Bot Token",
                format!("xoxb-{}-{}abcd", "1".repeat(12), "2".repeat(12)),
            ),
            (
                "Slack App Token",
                "xapp-1-ABC123-1234567890-abcdef".to_owned(),
            ),
            (
                "SendGrid API Token",
                format!("SG.{}.{}", "a".repeat(22), "b".repeat(43)),
            ),
            ("NPM Access Token", format!("npm_{}", "a".repeat(36))),
            ("Stripe Access Token", format!("sk_live_{}", "a".repeat(24))),
        ];
        for (label, secret) in cases {
            let error = check_team_memory_secrets(&target, &secret, &workspace.0)
                .unwrap_or_else(|| panic!("expected {label} to be detected"));
            assert!(error.contains(label), "expected {label} label in {error}");
            assert!(!error.contains(&secret));
        }
    }

    #[test]
    fn detects_private_keys_with_a_bounded_linear_regex() {
        let workspace = TempWorkspace::new();
        let target = workspace.0.join(".canopy/team-memory/secret.txt");
        let private_key = format!(
            "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----",
            "A".repeat(120)
        );
        assert!(
            check_team_memory_secrets(&target, &private_key, &workspace.0)
                .unwrap()
                .contains("Private Key")
        );
        let unmatched = format!("{}\n", "-----BEGIN PRIVATE KEY-----\n".repeat(4000));
        assert!(check_team_memory_secrets(&target, &unmatched, &workspace.0).is_none());
    }
}
