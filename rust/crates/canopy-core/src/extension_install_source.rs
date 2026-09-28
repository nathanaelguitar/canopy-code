//! Pure classification of extension install source strings.
//!
//! This mirrors the non-I/O portion of `parseInstallSource` in
//! `packages/core/src/extension/marketplace.ts`. The caller supplies whether
//! the repository portion exists locally; fetching GitHub marketplace data,
//! filesystem inspection, and attaching marketplace metadata remain caller
//! responsibilities.
//!
//! URL handling uses `reqwest::Url`, so malformed URLs and uncommon URL
//! canonicalization cases can differ from Node's WHATWG `URL` implementation.

use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::extensions::redact_url_credentials;

/// Install-source kinds persisted by `ExtensionInstallMetadata`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum InstallSourceType {
    Git,
    Local,
    Link,
    GithubRelease,
    Npm,
    ArchiveUrl,
}

/// Parsed source metadata that does not require filesystem or network access.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParsedInstallSource {
    pub source: String,
    #[serde(rename = "type")]
    pub install_type: InstallSourceType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugin_name: Option<String>,
}

/// A GitHub repository path resolved from a source string.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitHubRepository {
    pub owner: String,
    pub repo: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InstallSourceError {
    NotFound(String),
    InvalidGitHubRepository(String),
    GitHubSshReleaseUnsupported,
}

impl fmt::Display for InstallSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(source) => write!(
                formatter,
                "Install source not found: {}",
                redact_url_credentials(source)
            ),
            Self::InvalidGitHubRepository(source) => write!(
                formatter,
                "Invalid GitHub repository source: {}. Expected \"owner/repo\" or a github repo uri.",
                redact_url_credentials(source)
            ),
            Self::GitHubSshReleaseUnsupported => formatter.write_str(
                "GitHub release-based extensions are not supported for SSH. You must use an HTTPS URI with a personal access token to download releases from private repositories. You can set your personal access token in the GITHUB_TOKEN environment variable and install the extension via SSH.",
            ),
        }
    }
}

impl std::error::Error for InstallSourceError {}

/// Parse `<source>[:plugin-name]`, preserving URL and Windows-drive colons.
pub fn parse_source_and_plugin_name(source: &str) -> (&str, Option<&str>) {
    let lower_source = source.to_ascii_lowercase();
    let schemes = ["http://", "https://", "git@", "sso://"];

    for scheme in schemes {
        if lower_source.starts_with(scheme) {
            let after_scheme = &source[scheme.len()..];
            if let Some(last_colon) = after_scheme.rfind(':') {
                let potential_plugin = &after_scheme[last_colon + 1..];
                if !potential_plugin.is_empty()
                    && !potential_plugin.contains('/')
                    && !starts_with_ascii_digit(potential_plugin)
                {
                    let split_at = scheme.len() + last_colon;
                    return (&source[..split_at], Some(&source[split_at + 1..]));
                }
            }
            return (source, None);
        }
    }

    if let Some(last_colon) = source.rfind(':') {
        if last_colon > 1 {
            return (&source[..last_colon], Some(&source[last_colon + 1..]));
        }
    }
    (source, None)
}

fn starts_with_ascii_digit(value: &str) -> bool {
    value.as_bytes().first().is_some_and(u8::is_ascii_digit)
}

/// Whether a source is exactly the TypeScript `owner/repo` shorthand form.
pub fn is_owner_repo_format(source: &str) -> bool {
    let Some((owner, repo)) = source.split_once('/') else {
        return false;
    };
    !owner.is_empty()
        && !repo.is_empty()
        && !repo.contains('/')
        && owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
        && repo
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}

/// Whether the source has one of the Git URL prefixes accepted by marketplace installs.
pub fn is_git_url(source: &str) -> bool {
    let lower = source.to_ascii_lowercase();
    lower.starts_with("http://")
        || lower.starts_with("https://")
        || lower.starts_with("git@")
        || lower.starts_with("sso://")
}

/// Whether an absolute HTTPS URL ends in a supported archive extension.
/// Query strings and fragments do not affect the URL pathname check.
pub fn is_supported_archive_url(source: &str) -> bool {
    let Ok(url) = Url::parse(source) else {
        return false;
    };
    url.scheme() == "https"
        && [".tar.gz", ".zip"]
            .iter()
            .any(|extension| url.path().to_ascii_lowercase().ends_with(extension))
}

/// Whether a scoped npm source matches marketplace's accepted package syntax.
pub fn is_scoped_npm_package(source: &str) -> bool {
    let Some(rest) = source.strip_prefix('@') else {
        return false;
    };
    let Some((scope, package_and_version)) = rest.split_once('/') else {
        return false;
    };
    let (package, has_version, version) = match package_and_version.split_once('@') {
        Some((package, version)) => (package, true, version),
        None => (package_and_version, false, ""),
    };
    let valid_name = |name: &str| {
        !name.is_empty()
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
    };
    if !valid_name(scope) || !valid_name(package) {
        return false;
    }
    !has_version || !version.is_empty()
}

/// Classify an install source. `local_path_exists` is supplied by the caller
/// because the TypeScript parser checks the filesystem before every other
/// source format.
pub fn parse_install_source(
    source: &str,
    local_path_exists: bool,
) -> Result<ParsedInstallSource, InstallSourceError> {
    let (repo, plugin_name) = parse_source_and_plugin_name(source);
    let mut resolved_source = repo.to_owned();
    let install_type = if local_path_exists {
        InstallSourceType::Local
    } else if is_supported_archive_url(repo) {
        InstallSourceType::ArchiveUrl
    } else if is_git_url(repo) {
        InstallSourceType::Git
    } else if is_scoped_npm_package(repo) {
        InstallSourceType::Npm
    } else if is_owner_repo_format(repo) {
        resolved_source = format!("https://github.com/{repo}");
        InstallSourceType::Git
    } else {
        return Err(InstallSourceError::NotFound(repo.to_owned()));
    };

    Ok(ParsedInstallSource {
        source: resolved_source,
        install_type,
        plugin_name: plugin_name.map(str::to_owned),
    })
}

/// Parse a shorthand or GitHub URL into owner/repository components.
///
/// This helper follows `parseGitHubRepoForReleases`; it does not establish
/// that the repository exists or that it has releases.
pub fn parse_github_repo_for_releases(
    source: &str,
) -> Result<GitHubRepository, InstallSourceError> {
    if source.starts_with("git@github.com:") {
        return Err(InstallSourceError::GitHubSshReleaseUnsupported);
    }

    let base = Url::parse("https://github.com/").expect("static GitHub base URL is valid");
    let parsed = Url::parse(source).or_else(|_| base.join(source));
    let Ok(parsed) = parsed else {
        return Err(InstallSourceError::InvalidGitHubRepository(
            source.to_owned(),
        ));
    };
    let parts = parsed.path().strip_prefix('/').unwrap_or(parsed.path());
    let mut parts = parts.split('/');
    let owner = parts.next().unwrap_or_default();
    let repo = parts.next().unwrap_or_default();
    let has_extra_part = parts.next().is_some();
    if parsed.host_str() != Some("github.com")
        || parsed.port().is_some()
        || owner.is_empty()
        || repo.is_empty()
        || has_extra_part
    {
        return Err(InstallSourceError::InvalidGitHubRepository(
            source.to_owned(),
        ));
    }
    if owner.starts_with("git@github.com") {
        return Err(InstallSourceError::GitHubSshReleaseUnsupported);
    }

    Ok(GitHubRepository {
        owner: owner.to_owned(),
        repo: repo.strip_suffix(".git").unwrap_or(repo).to_owned(),
    })
}
