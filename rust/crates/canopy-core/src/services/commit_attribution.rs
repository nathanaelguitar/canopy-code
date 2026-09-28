//! Per-file AI contribution tracking and commit-note payload generation.
//!
//! Port of `packages/core/src/services/commitAttribution.ts`. Git command
//! execution and staged-diff collection remain caller responsibilities; this
//! module accepts their staged-file projection and produces the same note data.

use std::collections::HashSet;
use std::env;
use std::path::{Component, MAIN_SEPARATOR, Path, PathBuf};
use std::sync::OnceLock;

use indexmap::IndexMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const ATTRIBUTION_SNAPSHOT_VERSION: u32 = 1;
pub const MAX_EXCLUDED_GENERATED_SAMPLE: usize = 50;
const SANITIZED_GENERATOR_NAME: &str = "Canopy-Coder";

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileAttribution {
    pub ai_contribution: f64,
    pub ai_created: bool,
    pub content_hash: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileAttributionDetail {
    pub ai_chars: f64,
    pub human_chars: f64,
    pub percent: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub surface: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttributionSummary {
    pub ai_percent: u64,
    pub ai_chars: f64,
    pub human_chars: f64,
    pub total_files_touched: usize,
    pub surfaces: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SurfaceBreakdown {
    #[serde(rename = "aiChars")]
    pub ai_chars: f64,
    pub percent: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitAttributionNote {
    pub version: u32,
    pub generator: String,
    pub files: IndexMap<String, FileAttributionDetail>,
    pub summary: AttributionSummary,
    pub surface_breakdown: IndexMap<String, SurfaceBreakdown>,
    pub excluded_generated: Vec<String>,
    pub excluded_generated_count: usize,
    pub prompt_count: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttributionSnapshot {
    #[serde(rename = "type")]
    pub snapshot_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    pub surface: String,
    pub file_states: IndexMap<String, FileAttribution>,
    pub prompt_count: f64,
    pub prompt_count_at_last_commit: f64,
}

/// Staged paths and approximate change sizes supplied by the commit caller.
/// `diff_sizes` contains the `(added + removed) * 40` proxy for text and 1024
/// for binary files, as collected by the source runtime's git numstat reader.
#[derive(Clone, Debug, Default)]
pub struct StagedFileInfo {
    pub files: Vec<String>,
    pub diff_sizes: IndexMap<String, f64>,
    pub deleted_files: HashSet<String>,
    /// Old repo-relative path to new repo-relative path from git rename data.
    pub renamed_files: IndexMap<String, String>,
    /// Optional for compatibility with staged-file adapters that only return
    /// the file list and sizes.
    pub repo_root: Option<PathBuf>,
}

/// Tracks AI edits until their files are included in a commit.
#[derive(Clone, Debug)]
pub struct CommitAttributionService {
    file_attributions: IndexMap<PathBuf, FileAttribution>,
    surface: String,
    prompt_count: f64,
    prompt_count_at_last_commit: f64,
}

impl Default for CommitAttributionService {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitAttributionService {
    pub fn new() -> Self {
        Self::with_surface(get_client_surface())
    }

    pub fn with_surface(surface: impl Into<String>) -> Self {
        Self {
            file_attributions: IndexMap::new(),
            surface: surface.into(),
            prompt_count: 0.0,
            prompt_count_at_last_commit: 0.0,
        }
    }

    /// Record one tool-mediated AI edit, resetting accumulated attribution
    /// when the file's previous post-write hash no longer matches `old_content`.
    pub fn record_edit(
        &mut self,
        file_path: impl AsRef<Path>,
        old_content: Option<&str>,
        new_content: &str,
    ) {
        let key = realpath_or_self(file_path.as_ref());
        let existing = self.file_attributions.get(&key).cloned();
        let is_new_file = old_content.is_none();
        let mut ai_contribution = existing.as_ref().map_or(0.0, |attr| attr.ai_contribution);
        let mut ai_created = existing.as_ref().is_some_and(|attr| attr.ai_created);

        if existing.is_some() && is_new_file {
            ai_contribution = 0.0;
            ai_created = false;
        }

        if let (Some(existing), Some(old_content)) = (existing.as_ref(), old_content)
            && !existing.content_hash.is_empty()
            && existing.content_hash != compute_content_hash(old_content)
        {
            ai_contribution = 0.0;
            ai_created = false;
        }

        ai_contribution += compute_char_contribution(old_content.unwrap_or(""), new_content);
        if is_new_file {
            ai_created = true;
        }

        self.file_attributions.insert(
            key,
            FileAttribution {
                ai_contribution,
                ai_created,
                content_hash: compute_content_hash(new_content),
            },
        );
    }

    /// Drop entries whose caller-provided committed content diverged from the
    /// last AI write. Returning `None` means the path is outside this check.
    pub fn validate_against<F>(&mut self, mut get_content: F)
    where
        F: FnMut(&Path) -> Option<String>,
    {
        let stale: Vec<PathBuf> = self
            .file_attributions
            .iter()
            .filter_map(|(path, attr)| {
                if attr.content_hash.is_empty() {
                    return None;
                }
                let current = get_content(path)?;
                (compute_content_hash(&current) != attr.content_hash).then(|| path.clone())
            })
            .collect();
        for path in stale {
            self.file_attributions.shift_remove(&path);
        }
    }

    pub fn increment_prompt_count(&mut self) {
        self.prompt_count += 1.0;
    }

    pub fn prompt_count(&self) -> f64 {
        self.prompt_count
    }

    pub fn prompts_since_last_commit(&self) -> f64 {
        self.prompt_count - self.prompt_count_at_last_commit
    }

    pub fn attributions(&self) -> IndexMap<PathBuf, FileAttribution> {
        self.file_attributions.clone()
    }

    pub fn file_attribution(&self, file_path: impl AsRef<Path>) -> Option<FileAttribution> {
        self.file_attributions
            .get(&realpath_or_self(file_path.as_ref()))
            .cloned()
    }

    pub fn has_attributions(&self) -> bool {
        !self.file_attributions.is_empty()
    }

    pub fn surface(&self) -> &str {
        &self.surface
    }

    /// Clear all file state; successful commits also advance the prompt window.
    pub fn clear_attributions(&mut self, commit_succeeded: bool) {
        if commit_succeeded {
            self.prompt_count_at_last_commit = self.prompt_count;
        }
        self.file_attributions.clear();
    }

    /// Clear only canonical absolute paths included in the just-made commit.
    pub fn clear_attributed_files(&mut self, committed_absolute_paths: &HashSet<PathBuf>) {
        self.prompt_count_at_last_commit = self.prompt_count;
        for path in committed_absolute_paths {
            self.file_attributions.shift_remove(path);
        }
    }

    /// Advance the prompt window after a commit when its file set could not be
    /// determined, retaining pending file attribution for later commits.
    pub fn note_commit_without_clearing(&mut self) {
        self.prompt_count_at_last_commit = self.prompt_count;
    }

    /// Match git's repo-relative paths against canonical paths captured when
    /// edits were recorded. Walking stored keys also handles deletions and
    /// symlinked parent directories.
    pub fn match_committed_files<I, S>(
        &self,
        relative_files: I,
        canonical_repo_root: impl AsRef<Path>,
    ) -> HashSet<PathBuf>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let wanted: HashSet<String> = relative_files
            .into_iter()
            .map(|path| path.as_ref().to_owned())
            .collect();
        let root = realpath_or_self(canonical_repo_root.as_ref());
        self.file_attributions
            .keys()
            .filter(|path| wanted.contains(&relative_path(&root, path)))
            .cloned()
            .collect()
    }

    /// Move tracked paths across git renames before matching the committed
    /// destination file names.
    pub fn apply_committed_renames(
        &mut self,
        renamed_files: &IndexMap<String, String>,
        canonical_repo_root: impl AsRef<Path>,
    ) {
        if renamed_files.is_empty() {
            return;
        }
        let root = realpath_or_self(canonical_repo_root.as_ref());
        let original_entries: Vec<(PathBuf, FileAttribution)> = self
            .file_attributions
            .iter()
            .map(|(path, attr)| (path.clone(), attr.clone()))
            .collect();
        for (old_absolute, attr) in original_entries {
            let old_relative = relative_path(&root, &old_absolute);
            let Some(new_relative) = renamed_files.get(&old_relative) else {
                continue;
            };
            let new_absolute = realpath_or_self(&root.join(new_relative));
            if new_absolute == old_absolute {
                continue;
            }

            if let Some(existing) = self.file_attributions.get(&new_absolute).cloned() {
                self.file_attributions.insert(
                    new_absolute.clone(),
                    FileAttribution {
                        ai_contribution: existing.ai_contribution + attr.ai_contribution,
                        ai_created: existing.ai_created || attr.ai_created,
                        content_hash: if existing.content_hash.is_empty() {
                            attr.content_hash
                        } else {
                            existing.content_hash
                        },
                    },
                );
            } else {
                self.file_attributions
                    .insert(new_absolute.clone(), attr.clone());
            }
            self.file_attributions.shift_remove(&old_absolute);
        }
    }

    pub fn to_snapshot(&self) -> AttributionSnapshot {
        let file_states = self
            .file_attributions
            .iter()
            .map(|(path, attr)| (path.to_string_lossy().into_owned(), attr.clone()))
            .collect();
        AttributionSnapshot {
            snapshot_type: "attribution-snapshot".to_owned(),
            version: Some(ATTRIBUTION_SNAPSHOT_VERSION),
            surface: self.surface.clone(),
            file_states,
            prompt_count: self.prompt_count,
            prompt_count_at_last_commit: self.prompt_count_at_last_commit,
        }
    }

    /// Restore a potentially damaged persisted JSON value. Envelope errors
    /// reset the service; invalid per-field values use source-compatible
    /// defaults, and canonical path collisions merge their contributions.
    pub fn restore_from_snapshot(&mut self, snapshot: &Value) {
        let Some(object) = snapshot.as_object() else {
            self.reset();
            return;
        };
        if object.get("type").and_then(Value::as_str) != Some("attribution-snapshot") {
            self.reset();
            return;
        }
        let version = object
            .get("version")
            .filter(|version| !version.is_null())
            .map_or(Some(1.0), Value::as_f64);
        if version != Some(1.0) {
            self.reset();
            return;
        }

        self.surface = object
            .get("surface")
            .and_then(Value::as_str)
            .filter(|surface| !surface.is_empty())
            .map_or_else(get_client_surface, str::to_owned);
        self.prompt_count = sanitized_count(object.get("promptCount"));
        self.prompt_count_at_last_commit =
            sanitized_count(object.get("promptCountAtLastCommit")).min(self.prompt_count);
        self.file_attributions.clear();

        let Some(file_states) = object.get("fileStates").and_then(Value::as_object) else {
            return;
        };
        for (path, value) in file_states {
            let key = realpath_or_self(Path::new(path));
            let incoming = sanitize_attribution(value);
            if let Some(existing) = self.file_attributions.get(&key).cloned() {
                self.file_attributions.insert(
                    key,
                    FileAttribution {
                        ai_contribution: existing.ai_contribution + incoming.ai_contribution,
                        ai_created: existing.ai_created || incoming.ai_created,
                        content_hash: if incoming.content_hash.is_empty() {
                            existing.content_hash
                        } else {
                            incoming.content_hash
                        },
                    },
                );
            } else {
                self.file_attributions.insert(key, incoming);
            }
        }
    }

    pub fn generate_note_payload(
        &self,
        staged_info: &StagedFileInfo,
        base_dir: impl AsRef<Path>,
        generator_name: Option<&str>,
    ) -> CommitAttributionNote {
        let generator = sanitize_model_name(generator_name.unwrap_or(SANITIZED_GENERATOR_NAME));
        let canonical_base = realpath_or_self(base_dir.as_ref());
        let ai_lookup: IndexMap<String, &FileAttribution> = self
            .file_attributions
            .iter()
            .map(|(path, attr)| (relative_path(&canonical_base, path), attr))
            .collect();

        let mut files = IndexMap::new();
        let mut excluded_generated = Vec::new();
        let mut excluded_generated_count = 0;
        let mut surface_counts: IndexMap<String, f64> = IndexMap::new();
        let mut total_ai_chars = 0.0;
        let mut total_human_chars = 0.0;

        for relative_file in &staged_info.files {
            if is_generated_file(relative_file) {
                excluded_generated_count += 1;
                if excluded_generated.len() < MAX_EXCLUDED_GENERATED_SAMPLE {
                    excluded_generated.push(relative_file.clone());
                }
                continue;
            }

            let tracked = ai_lookup.get(relative_file);
            let diff_size = staged_info
                .diff_sizes
                .get(relative_file)
                .copied()
                .unwrap_or(0.0);
            let ai_chars = tracked.map_or(0.0, |attr| attr.ai_contribution.min(diff_size));
            let human_chars = (diff_size - ai_chars).max(0.0);
            let total = ai_chars + human_chars;
            let percent = rounded_percent(ai_chars, total);
            files.insert(
                relative_file.clone(),
                FileAttributionDetail {
                    ai_chars,
                    human_chars,
                    percent,
                    surface: Some(self.surface.clone()),
                },
            );
            total_ai_chars += ai_chars;
            total_human_chars += human_chars;
            *surface_counts.entry(self.surface.clone()).or_default() += ai_chars;
        }

        let total_chars = total_ai_chars + total_human_chars;
        let ai_percent = rounded_percent(total_ai_chars, total_chars);
        let surface_breakdown = surface_counts
            .into_iter()
            .map(|(surface, ai_chars)| {
                (
                    surface,
                    SurfaceBreakdown {
                        ai_chars,
                        percent: rounded_percent(ai_chars, total_chars),
                    },
                )
            })
            .collect();

        CommitAttributionNote {
            version: 1,
            generator,
            summary: AttributionSummary {
                ai_percent,
                ai_chars: total_ai_chars,
                human_chars: total_human_chars,
                total_files_touched: files.len(),
                surfaces: vec![self.surface.clone()],
            },
            files,
            surface_breakdown,
            excluded_generated,
            excluded_generated_count,
            prompt_count: self.prompts_since_last_commit(),
        }
    }

    fn reset(&mut self) {
        self.file_attributions.clear();
        self.surface = get_client_surface();
        self.prompt_count = 0.0;
        self.prompt_count_at_last_commit = 0.0;
    }
}

pub fn get_client_surface() -> String {
    env::var("CANOPY_CODE_ENTRYPOINT").unwrap_or_else(|_| "cli".to_owned())
}

/// Prefix/suffix contribution proxy, measured in UTF-16 code units to match
/// JavaScript `String.length` and indexed string access (including surrogate
/// pairs). It returns the larger changed span from the old and new contents.
pub fn compute_char_contribution(old_content: &str, new_content: &str) -> f64 {
    let old: Vec<u16> = old_content.encode_utf16().collect();
    let new: Vec<u16> = new_content.encode_utf16().collect();
    if old.is_empty() || new.is_empty() {
        return if old.is_empty() {
            new.len() as f64
        } else {
            old.len() as f64
        };
    }

    let min_len = old.len().min(new.len());
    let mut prefix_end = 0;
    while prefix_end < min_len && old[prefix_end] == new[prefix_end] {
        prefix_end += 1;
    }
    let mut suffix_len = 0;
    while suffix_len < min_len - prefix_end
        && old[old.len() - 1 - suffix_len] == new[new.len() - 1 - suffix_len]
    {
        suffix_len += 1;
    }
    let old_changed_len = old.len() - prefix_end - suffix_len;
    let new_changed_len = new.len() - prefix_end - suffix_len;
    old_changed_len.max(new_changed_len) as f64
}

pub fn compute_content_hash(content: &str) -> String {
    let normalized = canonicalize_for_hash(content);
    format!("{:x}", Sha256::digest(normalized.as_bytes()))
}

fn canonicalize_for_hash(content: &str) -> String {
    let without_bom = content.strip_prefix('\u{feff}').unwrap_or(content);
    without_bom.replace("\r\n", "\n")
}

fn sanitize_attribution(value: &Value) -> FileAttribution {
    let Some(object) = value.as_object() else {
        return FileAttribution::default();
    };
    FileAttribution {
        ai_contribution: sanitized_count(object.get("aiContribution")),
        ai_created: object
            .get("aiCreated")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        content_hash: object
            .get("contentHash")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    }
}

fn sanitized_count(value: Option<&Value>) -> f64 {
    value
        .and_then(Value::as_f64)
        .filter(|count| count.is_finite() && *count >= 0.0)
        .unwrap_or(0.0)
}

fn sanitize_model_name(name: &str) -> String {
    static INTERNAL_MODEL_PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    let patterns = INTERNAL_MODEL_PATTERNS.get_or_init(|| {
        [
            r"(?i)qwen[-_]?[0-9]+(\.[0-9]+)?[-_]?b?",
            r"(?i)qwen[-_]?coder[-_]?[0-9]*",
            r"(?i)qwen[-_]?max",
            r"(?i)qwen[-_]?plus",
            r"(?i)qwen[-_]?turbo",
        ]
        .into_iter()
        .map(|pattern| Regex::new(pattern).expect("static model-name regex is valid"))
        .collect()
    });
    if patterns.iter().any(|pattern| pattern.is_match(name)) {
        SANITIZED_GENERATOR_NAME.to_owned()
    } else {
        name.to_owned()
    }
}

fn rounded_percent(numerator: f64, denominator: f64) -> u64 {
    if denominator > 0.0 {
        ((numerator / denominator) * 100.0).round() as u64
    } else {
        0
    }
}

fn realpath_or_self(path: &Path) -> PathBuf {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return canonical;
    }
    if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name())
        && let Ok(canonical_parent) = std::fs::canonicalize(parent)
    {
        return canonical_parent.join(file_name);
    }
    path.to_path_buf()
}

fn relative_path(base: &Path, target: &Path) -> String {
    let base_components: Vec<Component<'_>> = base
        .components()
        .filter(|component| !matches!(component, Component::CurDir))
        .collect();
    let target_components: Vec<Component<'_>> = target
        .components()
        .filter(|component| !matches!(component, Component::CurDir))
        .collect();
    let common_len = base_components
        .iter()
        .zip(&target_components)
        .take_while(|(left, right)| left == right)
        .count();
    let mut relative = PathBuf::new();
    for component in &base_components[common_len..] {
        if matches!(component, Component::Normal(_)) {
            relative.push("..");
        } else {
            return normalize_separators(target);
        }
    }
    for component in &target_components[common_len..] {
        relative.push(component.as_os_str());
    }
    normalize_separators(&relative)
}

fn normalize_separators(path: &Path) -> String {
    let path = path.to_string_lossy();
    if MAIN_SEPARATOR == '/' {
        path.into_owned()
    } else {
        path.replace(MAIN_SEPARATOR, "/")
    }
}

fn is_generated_file(file_path: &str) -> bool {
    const EXCLUDED_FILENAMES: &[&str] = &[
        "package-lock.json",
        "yarn.lock",
        "pnpm-lock.yaml",
        "bun.lockb",
        "bun.lock",
        "composer.lock",
        "gemfile.lock",
        "cargo.lock",
        "poetry.lock",
        "pipfile.lock",
        "shrinkwrap.json",
        "npm-shrinkwrap.json",
        ".terraform.lock.hcl",
    ];
    const EXCLUDED_EXTENSIONS: &[&str] = &[
        ".min.js",
        ".min.css",
        ".min.html",
        ".bundle.js",
        ".bundle.css",
        ".generated.ts",
        ".generated.js",
    ];
    const EXCLUDED_DIRECTORIES: &[&str] = &[
        "dist",
        "build",
        "out",
        "output",
        "node_modules",
        "vendor",
        "vendored",
        "third_party",
        "third-party",
        "external",
        ".next",
        ".nuxt",
        ".svelte-kit",
        "coverage",
        "__pycache__",
        ".tox",
        "venv",
        ".venv",
    ];

    static EXCLUDED_NAME_PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    let patterns = EXCLUDED_NAME_PATTERNS.get_or_init(|| {
        [
            r"(?i)^.*\.min\.[a-z]+$",
            r"(?i)^.*-min\.[a-z]+$",
            r"(?i)^.*\.bundle\.[a-z]+$",
            r"(?i)^.*\.generated\.[a-z]+$",
            r"(?i)^.*\.gen\.[a-z]+$",
            r"(?i)^.*\.auto\.[a-z]+$",
            r"(?i)^.*_generated\.[a-z]+$",
            r"(?i)^.*_gen\.[a-z]+$",
            r"(?i)^.*\.pb\.(go|js|ts|py|rb)$",
            r"(?i)^.*_pb2?\.py$",
            r"(?i)^.*\.pb\.h$",
            r"(?i)^.*\.grpc\.[a-z]+$",
            r"(?i)^.*\.swagger\.[a-z]+$",
            r"(?i)^.*\.openapi\.[a-z]+$",
        ]
        .into_iter()
        .map(|pattern| Regex::new(pattern).expect("static generated-file regex is valid"))
        .collect()
    });

    let normalized = format!(
        "/{}",
        file_path
            .replace(MAIN_SEPARATOR, "/")
            .trim_start_matches('/')
    );
    let lower_path = normalized.to_lowercase();
    let file_name = Path::new(file_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_lowercase();
    if EXCLUDED_FILENAMES.contains(&file_name.as_str()) {
        return true;
    }

    if EXCLUDED_EXTENSIONS
        .iter()
        .any(|extension| file_name.ends_with(extension))
    {
        return true;
    }
    let segments: Vec<&str> = lower_path
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    if segments
        .iter()
        .take(segments.len().saturating_sub(1))
        .any(|segment| EXCLUDED_DIRECTORIES.contains(segment))
    {
        return true;
    }
    if lower_path.contains("/target/release/") || lower_path.contains("/target/debug/") {
        return true;
    }
    patterns.iter().any(|pattern| pattern.is_match(&file_name))
}
