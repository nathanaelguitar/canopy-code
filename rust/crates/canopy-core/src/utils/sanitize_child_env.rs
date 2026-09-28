//! Narrowly remove Canopy's internal secrets before spawning user-facing
//! child processes.

/// Internal credential keys that must not cross a user-command or stdio MCP
/// process boundary. Third-party credentials intentionally remain untouched.
pub(crate) const INTERNAL_SECRET_ENV_VARS: [&str; 3] = [
    "QWEN_SERVER_TOKEN",
    "QWEN_DAEMON_TOKEN",
    "QWEN_CODE_PRIVATE_ACP_CAPABILITY",
];

/// Return a copied environment with only the Canopy-internal secret keys
/// removed. The input map is never mutated.
pub(crate) fn sanitize_child_env<M>(env: &M) -> M
where
    M: Clone + IntoIterator<Item = (String, String)> + FromIterator<(String, String)>,
{
    M::clone(env)
        .into_iter()
        .filter(|(key, _)| !INTERNAL_SECRET_ENV_VARS.contains(&key.as_str()))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::{INTERNAL_SECRET_ENV_VARS, sanitize_child_env};

    #[test]
    fn removes_exact_internal_keys_without_mutating_or_dropping_user_credentials() {
        let source = BTreeMap::from([
            ("QWEN_SERVER_TOKEN".to_owned(), "serve-secret".to_owned()),
            ("QWEN_DAEMON_TOKEN".to_owned(), "daemon-secret".to_owned()),
            (
                "QWEN_CODE_PRIVATE_ACP_CAPABILITY".to_owned(),
                "private-capability".to_owned(),
            ),
            (
                "CANOPY_PRIVATE_ACP_CAPABILITY".to_owned(),
                "legacy-capability".to_owned(),
            ),
            ("GH_TOKEN".to_owned(), "gh-token".to_owned()),
            ("GITHUB_TOKEN".to_owned(), "github-token".to_owned()),
            ("AWS_ACCESS_KEY_ID".to_owned(), "aws-key".to_owned()),
            ("NPM_TOKEN".to_owned(), "npm-token".to_owned()),
            ("PATH".to_owned(), "/usr/bin".to_owned()),
        ]);

        let sanitized = sanitize_child_env(&source);

        assert_eq!(INTERNAL_SECRET_ENV_VARS.len(), 3);
        for key in INTERNAL_SECRET_ENV_VARS {
            assert!(!sanitized.contains_key(key));
            assert!(source.contains_key(key), "input key {key} was mutated");
        }
        assert_eq!(
            sanitized.len(),
            source.len() - INTERNAL_SECRET_ENV_VARS.len()
        );
        assert_eq!(
            sanitized["CANOPY_PRIVATE_ACP_CAPABILITY"],
            "legacy-capability"
        );
        assert_eq!(sanitized["GH_TOKEN"], "gh-token");
        assert_eq!(sanitized["GITHUB_TOKEN"], "github-token");
        assert_eq!(sanitized["AWS_ACCESS_KEY_ID"], "aws-key");
        assert_eq!(sanitized["NPM_TOKEN"], "npm-token");
        assert_eq!(sanitized["PATH"], "/usr/bin");
    }

    #[test]
    fn returns_a_fresh_hash_map_and_is_a_no_op_without_internal_secrets() {
        let source = HashMap::from([
            ("PATH".to_owned(), "/usr/bin".to_owned()),
            ("GH_TOKEN".to_owned(), "gh-token".to_owned()),
        ]);
        let sanitized = sanitize_child_env(&source);

        assert_eq!(sanitized, source);
        assert_eq!(source.len(), 2);
    }
}
