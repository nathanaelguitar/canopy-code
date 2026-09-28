//! Extension enablement override rules and activation resolution.
//!
//! This ports the pure matching logic from `extension/override.ts` and
//! `ExtensionStore.getActivation()` without owning snapshot persistence.

use indexmap::IndexMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ExtensionActivation {
    Enabled,
    Disabled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceActivation {
    Enabled,
    Disabled,
    Inherit,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionPolicy {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_generation: Option<u64>,
    pub default_activation: ExtensionActivation,
    pub workspace_overrides: IndexMap<String, WorkspaceActivation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_path_rules: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionStoreSnapshot {
    pub version: u32,
    pub generation: u64,
    pub legacy_projection_hash: String,
    pub extensions: IndexMap<String, ExtensionPolicy>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionIdentity {
    pub id: String,
    pub name: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationSource {
    CliOverride,
    WorkspaceOverride,
    LegacyPathRule,
    Default,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExtensionActivationResult {
    #[serde(rename = "default")]
    pub default_activation: ExtensionActivation,
    pub workspace: WorkspaceActivation,
    pub effective: ExtensionActivation,
    pub source: ActivationSource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Override {
    pub base_rule: String,
    pub is_disable: bool,
    pub include_subdirs: bool,
}

impl Override {
    pub fn from_input(input_rule: &str, include_subdirs: bool) -> Self {
        let is_disable = input_rule.starts_with('!');
        let base_rule = input_rule.strip_prefix('!').unwrap_or(input_rule);
        Self {
            base_rule: ensure_leading_and_trailing_slash(base_rule),
            is_disable,
            include_subdirs,
        }
    }

    pub fn from_file_rule(file_rule: &str) -> Self {
        let is_disable = file_rule.starts_with('!');
        let base_rule = file_rule.strip_prefix('!').unwrap_or(file_rule);
        let include_subdirs = base_rule.ends_with('*');
        let base_rule = if include_subdirs {
            &base_rule[..base_rule.len() - 1]
        } else {
            base_rule
        };
        Self {
            base_rule: base_rule.to_owned(),
            is_disable,
            include_subdirs,
        }
    }

    pub fn conflicts_with(&self, other: &Self) -> bool {
        self.base_rule == other.base_rule
            && (self.include_subdirs != other.include_subdirs
                || self.is_disable != other.is_disable)
    }

    pub fn is_equal_to(&self, other: &Self) -> bool {
        self == other
    }

    pub fn as_regex(&self) -> Regex {
        glob_to_regex(&format!(
            "{}{}",
            self.base_rule,
            if self.include_subdirs { "*" } else { "" }
        ))
    }

    pub fn is_child_of(&self, parent: &Self) -> bool {
        parent.include_subdirs && parent.as_regex().is_match(&self.base_rule)
    }

    pub fn output(&self) -> String {
        format!(
            "{}{}{}",
            if self.is_disable { "!" } else { "" },
            self.base_rule,
            if self.include_subdirs { "*" } else { "" }
        )
    }

    pub fn matches_path(&self, path: &str) -> bool {
        self.as_regex().is_match(path)
    }
}

/// Resolve a workspace path using realpath when it exists. A missing path
/// falls back to an absolute, lexically normalized spelling, as Node's
/// `path.resolve()` does before `realpathSync.native()`.
pub fn canonicalize_workspace_path(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let resolved = resolve_absolute_normalized(path.as_ref())?;
    match fs::canonicalize(&resolved) {
        Ok(canonical) => Ok(canonical),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(resolved),
        Err(error) => Err(error),
    }
}

/// Normalize path separators and ensure a leading and trailing slash, matching
/// the legacy path candidate normalization in the TypeScript store.
pub fn normalize_rule_path(path: &str) -> String {
    let mut normalized = path.replace('\\', "/");
    if !normalized.starts_with('/') {
        normalized.insert(0, '/');
    }
    if !normalized.ends_with('/') {
        normalized.push('/');
    }
    normalized
}

/// Resolve activation for an identity from an already-loaded snapshot.
///
/// Legacy rules are applied in stored order, so the last matching rule wins.
pub fn get_activation(
    snapshot: &ExtensionStoreSnapshot,
    identity: &ExtensionIdentity,
    workspace_path: impl AsRef<Path>,
) -> io::Result<ExtensionActivationResult> {
    let Some(policy) = snapshot.extensions.get(&identity.id) else {
        return Ok(default_enabled());
    };
    if policy.name != identity.name {
        return Ok(default_enabled());
    }

    let canonical_workspace = canonicalize_workspace_path(workspace_path.as_ref())?;
    let canonical_workspace = canonical_workspace.to_string_lossy().into_owned();
    match policy.workspace_overrides.get(&canonical_workspace) {
        Some(WorkspaceActivation::Enabled) => {
            return Ok(ExtensionActivationResult {
                default_activation: policy.default_activation,
                workspace: WorkspaceActivation::Enabled,
                effective: ExtensionActivation::Enabled,
                source: ActivationSource::WorkspaceOverride,
            });
        }
        Some(WorkspaceActivation::Disabled) => {
            return Ok(ExtensionActivationResult {
                default_activation: policy.default_activation,
                workspace: WorkspaceActivation::Disabled,
                effective: ExtensionActivation::Disabled,
                source: ActivationSource::WorkspaceOverride,
            });
        }
        Some(WorkspaceActivation::Inherit) => {
            return Ok(ExtensionActivationResult {
                default_activation: policy.default_activation,
                workspace: WorkspaceActivation::Inherit,
                effective: policy.default_activation,
                source: ActivationSource::Default,
            });
        }
        None => {}
    }

    let legacy_candidates = [
        normalize_rule_path(&workspace_path.as_ref().to_string_lossy()),
        normalize_rule_path(&canonical_workspace),
    ];
    let mut effective = policy.default_activation;
    let mut matched = false;
    for rule in policy.legacy_path_rules.iter().flatten() {
        let override_rule = Override::from_file_rule(rule);
        if !legacy_candidates
            .iter()
            .any(|candidate| override_rule.matches_path(candidate))
        {
            continue;
        }
        effective = if override_rule.is_disable {
            ExtensionActivation::Disabled
        } else {
            ExtensionActivation::Enabled
        };
        matched = true;
    }

    Ok(ExtensionActivationResult {
        default_activation: policy.default_activation,
        workspace: WorkspaceActivation::Inherit,
        effective,
        source: if matched {
            ActivationSource::LegacyPathRule
        } else {
            ActivationSource::Default
        },
    })
}

fn default_enabled() -> ExtensionActivationResult {
    ExtensionActivationResult {
        default_activation: ExtensionActivation::Enabled,
        workspace: WorkspaceActivation::Inherit,
        effective: ExtensionActivation::Enabled,
        source: ActivationSource::Default,
    }
}

fn ensure_leading_and_trailing_slash(path: &str) -> String {
    let mut normalized = path.replace('\\', "/");
    if !normalized.starts_with('/') {
        normalized.insert(0, '/');
    }
    if !normalized.ends_with('/') {
        normalized.push('/');
    }
    normalized
}

fn glob_to_regex(glob: &str) -> Regex {
    let characters = glob.chars().collect::<Vec<_>>();
    let mut pattern = String::from("^");
    for (index, character) in characters.iter().copied().enumerate() {
        if character == '*' {
            if index > 0 && characters[index - 1] == '/' {
                pattern.pop();
                pattern.push_str("(/[^\\n\\r\\u{2028}\\u{2029}]*)?");
            } else {
                pattern.push_str("([^\\n\\r\\u{2028}\\u{2029}]*)?");
            }
        } else {
            if matches!(
                character,
                '.' | '+' | '?' | '^' | '$' | '{' | '}' | '(' | ')' | '|' | '[' | ']' | '\\'
            ) {
                pattern.push('\\');
            }
            pattern.push(character);
        }
    }
    // JavaScript's `$` also matches immediately before a final line terminator.
    pattern.push_str("(?:\\r\\n|\\n|\\r|\\u{2028}|\\u{2029})?\\z");
    Regex::new(&pattern).expect("escaped override glob must compile as a regex")
}

fn resolve_absolute_normalized(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    Ok(normalize_absolute(&absolute))
}

fn normalize_absolute(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if normalized.file_name().is_some() {
                    normalized.pop();
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}
