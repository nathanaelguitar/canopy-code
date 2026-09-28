//! Pure projection of captured Git commit-diff output for attribution.
//!
//! Port of the metadata parsing used by `getCommittedFileInfo` and
//! `parseNumstat` in `packages/core/src/tools/shell.ts`. This module does not
//! invoke Git; callers own command execution and pass the captured output.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::OnceLock;

use indexmap::IndexMap;
use regex::Regex;
use serde_json::Value;

use crate::services::commit_attribution::StagedFileInfo;

const APPROX_CHARS_PER_LINE: f64 = 40.0;
const BINARY_DIFF_SIZE_FALLBACK: f64 = 1024.0;
const DEFAULT_COAUTHOR_NAME: &str = "Canopy-Coder";
const DEFAULT_COAUTHOR_EMAIL: &str = "qwen-coder@alibabacloud.com";

/// Settings consumed by the shell commit hook. The source Config currently
/// uses fixed Canopy co-author identity values and defaults commit attribution
/// to enabled when the setting is absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitAttributionGitConfig {
    pub commit_enabled: bool,
    pub coauthor_name: String,
    pub coauthor_email: String,
    pub generator_name: Option<String>,
}

impl CommitAttributionGitConfig {
    /// Resolve the source-compatible `general.gitCoAuthor` toggle from merged
    /// settings while retaining the actual model name for note generation.
    pub fn from_merged_settings(merged_settings: &Value, generator_name: Option<String>) -> Self {
        let git_coauthor = merged_settings.pointer("/general/gitCoAuthor");
        let commit_enabled = match git_coauthor {
            None | Some(Value::Null) => true,
            Some(Value::Bool(enabled)) => *enabled,
            Some(Value::Object(settings)) => {
                settings.get("commit").map(setting_boolean).unwrap_or(true)
            }
            Some(value) => setting_boolean(value),
        };
        Self {
            commit_enabled,
            coauthor_name: DEFAULT_COAUTHOR_NAME.to_owned(),
            coauthor_email: DEFAULT_COAUTHOR_EMAIL.to_owned(),
            generator_name: generator_name.filter(|name| !name.trim().is_empty()),
        }
    }
}

impl Default for CommitAttributionGitConfig {
    fn default() -> Self {
        Self {
            commit_enabled: true,
            coauthor_name: DEFAULT_COAUTHOR_NAME.to_owned(),
            coauthor_email: DEFAULT_COAUTHOR_EMAIL.to_owned(),
            generator_name: None,
        }
    }
}

fn setting_boolean(value: &Value) -> bool {
    match value {
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_i64() == Some(1),
        Value::String(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "true" | "yes" | "on" | "1"
        ),
        _ => false,
    }
}

/// A successful analysis either found a real empty commit or a nonempty file
/// projection. Keeping the empty case separate avoids treating failed Git
/// commands as an empty commit.
#[derive(Clone, Debug)]
pub enum CommittedFileInfoProjection {
    EmptyCommit,
    Files(StagedFileInfo),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitFileInfoAnalysisFailure {
    NameOutputUnavailable,
    StatusOutputUnavailable,
    NumstatOutputUnavailable,
    NumstatHadNoEntriesForChangedFiles,
}

impl fmt::Display for CommitFileInfoAnalysisFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::NameOutputUnavailable => "git name-only output was unavailable",
            Self::StatusOutputUnavailable => "git name-status output was unavailable",
            Self::NumstatOutputUnavailable => "git numstat output was unavailable",
            Self::NumstatHadNoEntriesForChangedFiles => {
                "git numstat had no parseable entries for changed files"
            }
        };
        formatter.write_str(message)
    }
}

impl Error for CommitFileInfoAnalysisFailure {}

/// Parse successful `--name-only`, `--name-status`, and `--numstat` command
/// outputs into the commit-attribution projection. A missing command result or
/// empty parsed numstat for a nonempty file list is an error; only successful
/// outputs with no changed paths produce `EmptyCommit`.
pub fn project_captured_commit_file_info(
    name_output: Option<&str>,
    status_output: Option<&str>,
    numstat_output: Option<&str>,
    repo_root: Option<&str>,
) -> Result<CommittedFileInfoProjection, CommitFileInfoAnalysisFailure> {
    let name_output = name_output.ok_or(CommitFileInfoAnalysisFailure::NameOutputUnavailable)?;
    let status_output =
        status_output.ok_or(CommitFileInfoAnalysisFailure::StatusOutputUnavailable)?;
    let numstat_output =
        numstat_output.ok_or(CommitFileInfoAnalysisFailure::NumstatOutputUnavailable)?;

    let files: Vec<String> = name_output
        .split('\n')
        .map(str::trim)
        .filter(|file| !file.is_empty())
        .map(str::to_owned)
        .collect();
    if files.is_empty() {
        return Ok(CommittedFileInfoProjection::EmptyCommit);
    }

    let mut deleted_files = HashSet::new();
    let mut renamed_files = IndexMap::new();
    for line in status_output.split('\n') {
        if let Some(deleted_path) = line.strip_prefix("D\t") {
            deleted_files.insert(deleted_path.trim().to_owned());
            continue;
        }

        let mut parts = line.split('\t');
        let status = parts.next().unwrap_or_default();
        if status.starts_with('R')
            && let (Some(old_path), Some(new_path)) = (parts.next(), parts.next())
        {
            renamed_files.insert(old_path.trim().to_owned(), new_path.trim().to_owned());
        }
    }

    let diff_sizes = parse_numstat(numstat_output);
    if diff_sizes.is_empty() {
        return Err(CommitFileInfoAnalysisFailure::NumstatHadNoEntriesForChangedFiles);
    }

    Ok(CommittedFileInfoProjection::Files(StagedFileInfo {
        files,
        diff_sizes,
        deleted_files,
        renamed_files,
        repo_root: repo_root
            .map(str::trim)
            .filter(|root| !root.is_empty())
            .map(PathBuf::from),
    }))
}

/// Parse Git numstat rows into approximate changed-character sizes.
///
/// Text files use `(added lines + removed lines) * 40`; binary rows use the
/// source runtime's fixed 1024 fallback. Rename path notation is normalized to
/// the destination path so it matches `--name-only` and note-payload lookups.
pub fn parse_numstat(numstat_output: &str) -> IndexMap<String, f64> {
    static LINE_RE: OnceLock<Regex> = OnceLock::new();
    let line_re = LINE_RE.get_or_init(|| {
        Regex::new(r"^([0-9-]+)\t([0-9-]+)\t(.+)$").expect("numstat row regex is valid")
    });

    let mut sizes = IndexMap::new();
    for line in numstat_output.split('\n').filter(|line| !line.is_empty()) {
        let Some(captures) = line_re.captures(line) else {
            continue;
        };
        let additions = captures.get(1).map_or("", |capture| capture.as_str());
        let deletions = captures.get(2).map_or("", |capture| capture.as_str());
        let Some(path) = captures.get(3).map(|capture| capture.as_str()) else {
            continue;
        };
        let path = normalize_numstat_path(path);

        let size = if additions == "-" && deletions == "-" {
            BINARY_DIFF_SIZE_FALLBACK
        } else {
            let (Ok(additions), Ok(deletions)) =
                (additions.parse::<f64>(), deletions.parse::<f64>())
            else {
                continue;
            };
            (additions + deletions) * APPROX_CHARS_PER_LINE
        };
        sizes.insert(path, size);
    }
    sizes
}

fn normalize_numstat_path(path: &str) -> String {
    static BRACE_RE: OnceLock<Regex> = OnceLock::new();
    static BARE_RENAME_RE: OnceLock<Regex> = OnceLock::new();
    let brace_re = BRACE_RE.get_or_init(|| {
        Regex::new(r"\{[^}]*?=>\s*([^}]*)\}").expect("brace rename regex is valid")
    });
    let bare_rename_re = BARE_RENAME_RE
        .get_or_init(|| Regex::new(r"^(.*?)\s=>\s(.*)$").expect("bare rename regex is valid"));

    let path = brace_re.replace_all(path.trim(), "$1");
    bare_rename_re
        .captures(path.as_ref())
        .and_then(|captures| captures.get(2))
        .map(|new_path| new_path.as_str().trim().to_owned())
        .unwrap_or_else(|| path.trim().to_owned())
}
