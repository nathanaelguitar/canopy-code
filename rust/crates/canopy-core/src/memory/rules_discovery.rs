//! Baseline and path-conditional `.canopy/rules` discovery.
//!
//! Port of `packages/core/src/utils/rulesDiscovery.ts`. Directory paths and
//! the global Canopy directory can be supplied by the host; conditional
//! matching reuses the core's symlink-aware project-relative path helper.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

use globset::{GlobBuilder, GlobMatcher};
use regex::Regex;
use serde::Serialize;
use serde_json::Value;

const CANOPY_DIR: &str = ".canopy";
const RULES_DIR: &str = "rules";

/// A parsed baseline or path-conditional rule file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleFile {
    pub file_path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paths: Option<Vec<String>>,
    pub content: String,
}

/// Baseline prompt content and conditional rules for lazy turn-level loading.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadRulesResponse {
    pub content: String,
    pub rule_count: usize,
    pub conditional_rules: Vec<RuleFile>,
}

/// Parse one rule file. Files with an empty body after comment stripping are
/// ignored. Malformed frontmatter retains the body but contributes no fields,
/// matching the TypeScript parser's best-effort YAML handling.
pub fn parse_rule_file(raw_content: &str, file_path: impl Into<PathBuf>) -> Option<RuleFile> {
    let file_path = file_path.into();
    let normalized = normalize_content(raw_content);
    let (body, mut description, mut paths) =
        if let Some((frontmatter, body)) = split_frontmatter(&normalized) {
            let parsed = crate::utils::yaml::parse(frontmatter);
            let description = parsed
                .get("description")
                .filter(|value| !value.is_null())
                .map(js_string);
            let paths = match parsed.get("paths") {
                Some(Value::Array(values)) => {
                    let values = values
                        .iter()
                        .map(js_string)
                        .filter(|value| !value.is_empty())
                        .collect::<Vec<_>>();
                    (!values.is_empty()).then_some(values)
                }
                Some(Value::String(value)) if !value.is_empty() => Some(vec![value.clone()]),
                _ => None,
            };
            (body, description, paths)
        } else {
            (normalized.as_str(), None, None)
        };

    let content = trim_javascript_whitespace(&strip_html_comments(body)).to_owned();
    if content.is_empty() {
        return None;
    }

    Some(RuleFile {
        file_path,
        description: description.take(),
        paths: paths.take(),
        content,
    })
}

/// Discover global rules and, for trusted workspaces, project rules. Baseline
/// files are formatted immediately; rules with `paths` remain available for
/// [`ConditionalRulesRegistry`]. Invalid exclude globs are returned as errors
/// when a scanned directory contains Markdown files, matching lazy source
/// matcher compilation.
pub async fn load_rules(
    project_root: impl AsRef<Path>,
    folder_trust: bool,
    excludes: &[String],
) -> Result<LoadRulesResponse, String> {
    load_rules_with_global_dir(
        project_root,
        crate::storage::Storage::get_global_canopy_dir(),
        folder_trust,
        excludes,
    )
    .await
}

/// Variant of [`load_rules`] with an explicit global Canopy directory for
/// hosts that manage runtime storage themselves.
pub async fn load_rules_with_global_dir(
    project_root: impl AsRef<Path>,
    global_canopy_directory: impl AsRef<Path>,
    folder_trust: bool,
    excludes: &[String],
) -> Result<LoadRulesResponse, String> {
    let project_root = absolute_normalized(project_root.as_ref());
    let global_rules_dir = absolute_normalized(global_canopy_directory.as_ref()).join(RULES_DIR);
    let mut all_rules = load_rules_from_dir(&global_rules_dir, excludes).await?;

    if folder_trust {
        let project_rules_dir = project_root.join(CANOPY_DIR).join(RULES_DIR);
        if project_rules_dir != global_rules_dir {
            all_rules.extend(load_rules_from_dir(&project_rules_dir, excludes).await?);
        }
    }

    let mut baseline_rules = Vec::new();
    let mut conditional_rules = Vec::new();
    for rule in all_rules {
        if rule.paths.is_some() {
            conditional_rules.push(rule);
        } else {
            baseline_rules.push(rule);
        }
    }
    let content = format_rules(&baseline_rules, &project_root);
    Ok(LoadRulesResponse {
        content,
        rule_count: baseline_rules.len(),
        conditional_rules,
    })
}

/// Format rule files with stable source markers and forward-slash display
/// paths, matching the system-prompt representation.
pub fn format_rules(rules: &[RuleFile], project_root: impl AsRef<Path>) -> String {
    let project_root = project_root.as_ref();
    rules
        .iter()
        .map(|rule| {
            let raw_display_path = if rule.file_path.is_absolute() {
                relative_path(project_root, &rule.file_path)
                    .to_string_lossy()
                    .into_owned()
            } else {
                rule.file_path.to_string_lossy().into_owned()
            };
            let display_path = raw_display_path.replace('\\', "/");
            format!(
                "--- Rule from: {display_path} ---\n{}\n--- End of Rule from: {display_path} ---",
                rule.content
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[derive(Clone, Debug)]
struct CompiledRule {
    rule: RuleFile,
    matchers: Vec<GlobMatcher>,
}

/// Session-scoped registry that consumes each matching rule once.
pub struct ConditionalRulesRegistry {
    compiled_rules: Vec<CompiledRule>,
    injected: Mutex<HashSet<String>>,
    project_root: PathBuf,
}

impl ConditionalRulesRegistry {
    /// Compile all conditional patterns. Invalid patterns fail construction,
    /// as `picomatch` does while building the source registry.
    pub fn new(rules: &[RuleFile], project_root: impl Into<PathBuf>) -> Result<Self, String> {
        let mut compiled_rules = Vec::with_capacity(rules.len());
        for rule in rules {
            let mut matchers = Vec::new();
            for pattern in rule.paths.as_deref().unwrap_or_default() {
                matchers.push(compile_glob(pattern)?);
            }
            compiled_rules.push(CompiledRule {
                rule: rule.clone(),
                matchers,
            });
        }
        Ok(Self {
            compiled_rules,
            injected: Mutex::new(HashSet::new()),
            project_root: absolute_normalized(&project_root.into()),
        })
    }

    /// Return formatted rules newly matching this path, considering both the
    /// lexical path and its realpath-resolved form when available.
    pub async fn match_and_consume(&self, file_path: impl AsRef<Path>) -> Option<String> {
        if self.compiled_rules.is_empty() {
            return None;
        }
        let relative_paths = crate::skills::resolve_symlink_aware_relative_paths(
            file_path.as_ref(),
            &self.project_root,
        )
        .await;
        if relative_paths.is_empty() {
            return None;
        }

        let mut injected = lock(&self.injected);
        let mut newly_matched = Vec::new();
        for compiled in &self.compiled_rules {
            let key = compiled.rule.file_path.to_string_lossy().into_owned();
            if injected.contains(&key)
                || !relative_paths.iter().any(|relative_path| {
                    compiled
                        .matchers
                        .iter()
                        .any(|matcher| matcher.is_match(relative_path))
                })
            {
                continue;
            }
            injected.insert(key);
            newly_matched.push(compiled.rule.clone());
        }
        if newly_matched.is_empty() {
            return None;
        }
        Some(format_rules(&newly_matched, &self.project_root))
    }

    pub fn total_count(&self) -> usize {
        self.compiled_rules.len()
    }

    pub fn injected_count(&self) -> usize {
        lock(&self.injected).len()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

async fn load_rules_from_dir(dir: &Path, excludes: &[String]) -> Result<Vec<RuleFile>, String> {
    let mut files = Vec::new();
    collect_markdown_files(dir, &mut files).await;
    files.sort_by(|left, right| {
        left.to_string_lossy()
            .encode_utf16()
            .cmp(right.to_string_lossy().encode_utf16())
    });
    if files.is_empty() {
        return Ok(Vec::new());
    }
    let exclude_matchers = excludes
        .iter()
        .map(|pattern| compile_glob(pattern))
        .collect::<Result<Vec<_>, _>>()?;

    let mut rules = Vec::new();
    for file_path in files {
        let file_name = file_path.to_string_lossy();
        if exclude_matchers
            .iter()
            .any(|matcher| matcher.is_match(file_name.as_ref()))
        {
            continue;
        }
        let Ok(bytes) = tokio::fs::read(&file_path).await else {
            continue;
        };
        let content = String::from_utf8_lossy(&bytes);
        if let Some(rule) = parse_rule_file(&content, file_path) {
            rules.push(rule);
        }
    }
    Ok(rules)
}

fn collect_markdown_files<'a>(
    directory: &'a Path,
    files: &'a mut Vec<PathBuf>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let Ok(mut entries) = tokio::fs::read_dir(directory).await else {
            return;
        };
        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) | Err(_) => break,
            };
            let path = entry.path();
            let Ok(file_type) = entry.file_type().await else {
                continue;
            };
            if file_type.is_dir() {
                collect_markdown_files(&path, files).await;
            } else if file_type.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".md"))
            {
                files.push(path);
            }
        }
    })
}

fn compile_glob(pattern: &str) -> Result<GlobMatcher, String> {
    GlobBuilder::new(pattern)
        .literal_separator(true)
        .backslash_escape(true)
        .build()
        .map(|glob| glob.compile_matcher())
        .map_err(|error| format!("invalid glob pattern {pattern:?}: {error}"))
}

fn split_frontmatter(content: &str) -> Option<(&str, &str)> {
    let after_open = content.strip_prefix("---\n")?;
    let mut search_from = 0;
    while let Some(relative) = after_open[search_from..].find("\n---") {
        let close_start = search_from + relative;
        let marker_start = close_start + 1;
        let marker_end = marker_start + 3;
        if marker_end == after_open.len() || after_open[marker_end..].starts_with('\n') {
            let body_start = if marker_end < after_open.len() {
                marker_end + 1
            } else {
                marker_end
            };
            return Some((&after_open[..close_start], &after_open[body_start..]));
        }
        search_from = close_start + 1;
        if search_from >= after_open.len() {
            return None;
        }
    }
    None
}

fn normalize_content(content: &str) -> String {
    content
        .strip_prefix('\u{feff}')
        .unwrap_or(content)
        .replace("\r\n", "\n")
        .replace('\r', "\n")
}

fn strip_html_comments(content: &str) -> String {
    static COMMENT_REGEX: OnceLock<Regex> = OnceLock::new();
    let regex = COMMENT_REGEX.get_or_init(|| Regex::new(r"(?s)<!--.*?-->").expect("valid regex"));
    let mut result = content.to_owned();
    loop {
        let next = regex.replace_all(&result, "").into_owned();
        if next == result {
            break;
        }
        result = next;
    }
    result.replace("<!--", "")
}

fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                integer.to_string()
            } else if let Some(integer) = value.as_u64() {
                integer.to_string()
            } else if let Some(number) = value.as_f64() {
                if number.fract() == 0.0 && number.abs() < 1.0e21 {
                    format!("{number:.0}")
                } else {
                    number.to_string()
                }
            } else {
                value.to_string()
            }
        }
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(|value| match value {
                Value::Null => String::new(),
                other => js_string(other),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

fn trim_javascript_whitespace(value: &str) -> &str {
    value.trim_matches(|character: char| {
        matches!(
            character,
            '\u{0009}'..='\u{000D}'
                | '\u{0020}'
                | '\u{00A0}'
                | '\u{1680}'
                | '\u{2000}'..='\u{200A}'
                | '\u{2028}'..='\u{2029}'
                | '\u{202F}'
                | '\u{205F}'
                | '\u{3000}'
                | '\u{FEFF}'
        )
    })
}

fn absolute_normalized(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            std::path::Component::RootDir => normalized.push(component.as_os_str()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !normalized.pop() && !normalized.is_absolute() {
                    normalized.push("..");
                }
            }
            std::path::Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn relative_path(from: &Path, to: &Path) -> PathBuf {
    let from = from.components().collect::<Vec<_>>();
    let to = to.components().collect::<Vec<_>>();
    let shared = from
        .iter()
        .zip(&to)
        .take_while(|(left, right)| left == right)
        .count();
    let mut relative = PathBuf::new();
    for component in &from[shared..] {
        if matches!(
            component,
            std::path::Component::Normal(_) | std::path::Component::ParentDir
        ) {
            relative.push("..");
        }
    }
    for component in &to[shared..] {
        relative.push(component.as_os_str());
    }
    relative
}
