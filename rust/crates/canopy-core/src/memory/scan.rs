//! Managed auto-memory topic scanning and frontmatter parsing.
//!
//! Port of `packages/core/src/memory/scan.ts`. The scanner deliberately skips
//! individual unreadable topic files so one damaged or concurrently removed
//! document cannot erase all other memories from a rebuilt index.

use std::io;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use super::paths::AutoMemoryPaths;
use super::store::AutoMemoryType;

pub const MAX_SCANNED_MEMORY_FILES: usize = 200;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScannedAutoMemoryDocument {
    #[serde(rename = "type")]
    pub memory_type: AutoMemoryType,
    pub file_path: PathBuf,
    pub relative_path: String,
    pub filename: String,
    pub title: String,
    pub description: String,
    pub body: String,
    pub mtime_ms: f64,
}

/// Parse one managed topic document. CRLF is normalized before delimiter and
/// frontmatter matching, as team files may come from Windows checkouts.
pub fn parse_auto_memory_topic_document(
    file_path: impl AsRef<Path>,
    content: &str,
    mtime_ms: f64,
    relative_path: Option<&str>,
) -> Option<ScannedAutoMemoryDocument> {
    let file_path = file_path.as_ref();
    let normalized = content.replace("\r\n", "\n");
    let after_open = normalized.strip_prefix("---\n")?;
    let close_start = after_open
        .find("\n---\n")
        .or_else(|| after_open.strip_suffix("\n---").map(|body| body.len()))?;
    let frontmatter = &after_open[..close_start];
    let body_start = close_start + "\n---".len();
    let body = after_open[body_start..]
        .strip_prefix('\n')
        .unwrap_or(&after_open[body_start..]);

    let raw_type = parse_frontmatter_value(frontmatter, "type")?;
    let memory_type = AutoMemoryType::ALL
        .into_iter()
        .find(|candidate| candidate.as_str() == raw_type)?;
    let filename = file_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let relative_path = relative_path
        .map(str::to_owned)
        .unwrap_or_else(|| filename.clone());

    Some(ScannedAutoMemoryDocument {
        memory_type,
        file_path: file_path.to_path_buf(),
        relative_path,
        filename,
        title: parse_frontmatter_value(frontmatter, "name")
            .or_else(|| parse_frontmatter_value(frontmatter, "title"))
            .unwrap_or_else(|| memory_type.as_str().to_owned()),
        description: parse_frontmatter_value(frontmatter, "description").unwrap_or_default(),
        body: trim_js_whitespace(body).to_owned(),
        mtime_ms,
    })
}

/// Mirrors the source regex `^key:[^\\S\\n]*(.+)$` in multiline mode. Only
/// horizontal whitespace may separate the colon from a non-empty value; an
/// empty value must not consume the next frontmatter line.
fn parse_frontmatter_value(frontmatter: &str, key: &str) -> Option<String> {
    for line in frontmatter.split('\n') {
        let Some(value) = line
            .strip_prefix(key)
            .and_then(|rest| rest.strip_prefix(':'))
        else {
            continue;
        };
        // The source regex requires at least one character after the colon.
        // Whitespace-only values still match and trim to an empty string.
        if value.is_empty() {
            continue;
        }
        let value =
            value.trim_start_matches(|character| character != '\n' && is_js_whitespace(character));
        return Some(trim_js_whitespace(value).to_owned());
    }
    None
}

fn is_js_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000a}'
            | '\u{000b}'
            | '\u{000c}'
            | '\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

fn trim_js_whitespace(value: &str) -> &str {
    value.trim_matches(is_js_whitespace)
}

/// Scan private project memory using newest-first capping.
pub async fn scan_auto_memory_topic_documents(
    paths: &AutoMemoryPaths,
) -> io::Result<Vec<ScannedAutoMemoryDocument>> {
    scan_auto_memory_documents_from_root(&paths.auto_memory_root(), false).await
}

/// Scan cross-project user memory using newest-first capping.
pub async fn scan_user_auto_memory_topic_documents(
    paths: &AutoMemoryPaths,
) -> io::Result<Vec<ScannedAutoMemoryDocument>> {
    scan_auto_memory_documents_from_root(&paths.user_auto_memory_root(), false).await
}

/// Scan shared team memory with a deterministic path-ordered cap.
pub async fn scan_team_auto_memory_topic_documents(
    paths: &AutoMemoryPaths,
) -> io::Result<Vec<ScannedAutoMemoryDocument>> {
    scan_auto_memory_documents_from_root(&paths.team_auto_memory_root(), true).await
}

async fn scan_auto_memory_documents_from_root(
    root: &Path,
    deterministic: bool,
) -> io::Result<Vec<ScannedAutoMemoryDocument>> {
    let relative_paths = match list_markdown_files(root) {
        Ok(paths) => paths,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };

    let reads = relative_paths.into_iter().map(|relative_path| {
        let file_path = root.join(Path::new(&relative_path));
        async move {
            let (content, metadata) =
                tokio::try_join!(tokio::fs::read(&file_path), tokio::fs::metadata(&file_path))
                    .ok()?;
            let mtime_ms = metadata.modified().ok().map(system_time_ms).unwrap_or(0.0);
            let content = String::from_utf8_lossy(&content);
            parse_auto_memory_topic_document(&file_path, &content, mtime_ms, Some(&relative_path))
        }
    });
    let mut docs: Vec<_> = join_all(reads).await.into_iter().flatten().collect();

    if deterministic {
        docs.sort_by(|left, right| compare_js_strings(&left.relative_path, &right.relative_path));
    } else {
        docs.sort_by(|left, right| {
            right
                .mtime_ms
                .total_cmp(&left.mtime_ms)
                .then_with(|| locale_like_filename_cmp(&left.filename, &right.filename))
        });
    }
    docs.truncate(MAX_SCANNED_MEMORY_FILES);
    Ok(docs)
}

fn list_markdown_files(root: &Path) -> io::Result<Vec<String>> {
    let mut paths = Vec::new();
    for entry in WalkDir::new(root).follow_links(false).min_depth(1) {
        let entry = entry.map_err(|error| {
            error
                .into_io_error()
                .unwrap_or_else(|| io::Error::other("failed to walk memory directory"))
        })?;
        let path = entry.path();
        let relative = path.strip_prefix(root).unwrap_or(path);
        let display = relative.to_string_lossy().replace('\\', "/");
        if display.ends_with(".md")
            && Path::new(&display)
                .file_name()
                .is_some_and(|name| name != "MEMORY.md")
        {
            paths.push(display);
        }
    }
    paths.sort_by(|left, right| compare_js_strings(left, right));
    Ok(paths)
}

fn compare_js_strings(left: &str, right: &str) -> std::cmp::Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn system_time_ms(time: std::time::SystemTime) -> f64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs_f64() * 1000.0,
        Err(error) => -error.duration().as_secs_f64() * 1000.0,
    }
}

/// Node's default localeCompare is ICU-backed. This stable fallback preserves
/// common English filename ordering without adding an ICU dependency to core.
fn locale_like_filename_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    let primary = left.to_lowercase().cmp(&right.to_lowercase());
    if primary != std::cmp::Ordering::Equal {
        return primary;
    }
    for (left_char, right_char) in left.chars().zip(right.chars()) {
        if left_char == right_char {
            continue;
        }
        let left_lower = left_char.to_lowercase().collect::<String>();
        let right_lower = right_char.to_lowercase().collect::<String>();
        if left_lower != right_lower {
            return left_lower.cmp(&right_lower);
        }
        // ICU's default English collation places lowercase before uppercase
        // when the primary case-insensitive keys are equal.
        let left_rank = if left_char.is_uppercase() { 1 } else { 0 };
        let right_rank = if right_char.is_uppercase() { 1 } else { 0 };
        let case = left_rank.cmp(&right_rank);
        if case != std::cmp::Ordering::Equal {
            return case;
        }
    }
    left.len().cmp(&right.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{MemoryProjectScope, paths::AutoMemoryPaths};

    #[test]
    fn parses_managed_frontmatter_and_crlf() {
        let parsed = parse_auto_memory_topic_document(
            "/tmp/project.md",
            "---\r\ntype: project\r\nname: CRLF Memory\r\ndescription: Windows line endings\r\n---\r\n\r\nBody line one.\r\n",
            12.5,
            None,
        )
        .unwrap();
        assert_eq!(parsed.memory_type, AutoMemoryType::Project);
        assert_eq!(parsed.title, "CRLF Memory");
        assert_eq!(parsed.description, "Windows line endings");
        assert_eq!(parsed.body, "Body line one.");
        assert_eq!(parsed.relative_path, "project.md");
    }

    #[test]
    fn empty_frontmatter_field_does_not_consume_following_line() {
        let parsed = parse_auto_memory_topic_document(
            "empty.md",
            "---\ntype: project\ndescription:\nname: Later\n---\nbody",
            0.0,
            None,
        )
        .unwrap();
        assert_eq!(parsed.title, "Later");
        assert_eq!(parsed.description, "");
    }

    #[test]
    fn trims_ecmascript_whitespace_in_frontmatter_and_body() {
        let parsed = parse_auto_memory_topic_document(
            "bom.md",
            "---\ntype: \u{feff}project\u{feff}\nname: \u{feff}Title\u{feff}\n---\n\u{feff} body \u{feff}",
            0.0,
            None,
        )
        .unwrap();
        assert_eq!(parsed.memory_type, AutoMemoryType::Project);
        assert_eq!(parsed.title, "Title");
        assert_eq!(parsed.body, "body");
    }

    #[test]
    fn rejects_invalid_delimiter_or_type() {
        assert!(parse_auto_memory_topic_document("a.md", "type: user", 0.0, None).is_none());
        assert!(
            parse_auto_memory_topic_document("a.md", "---\ntype: unknown\n---\nbody", 0.0, None)
                .is_none()
        );
    }

    #[test]
    fn deterministic_path_sort_matches_javascript_utf16_order() {
        assert_eq!(
            compare_js_strings("😀.md", "\u{e000}.md"),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn private_filename_tie_breaker_matches_common_english_locale_cases() {
        assert_eq!(
            locale_like_filename_cmp("a.md", "A.md"),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            locale_like_filename_cmp("file-1.md", "file_1.md"),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            locale_like_filename_cmp("a.md", "á.md"),
            std::cmp::Ordering::Less
        );
    }

    #[tokio::test]
    async fn missing_root_is_empty_and_unreadable_file_is_skipped() {
        let temp =
            std::env::temp_dir().join(format!("canopy-memory-scan-{}", uuid::Uuid::new_v4()));
        let paths = AutoMemoryPaths::new(
            temp.join("project"),
            temp.join("state"),
            false,
            MemoryProjectScope::Workspace,
        );
        assert!(
            scan_auto_memory_topic_documents(&paths)
                .await
                .unwrap()
                .is_empty()
        );

        let root = paths.auto_memory_root();
        tokio::fs::create_dir_all(root.join("nested"))
            .await
            .unwrap();
        tokio::fs::write(
            root.join("nested/good.md"),
            "---\ntype: feedback\nname: Good\ndescription: kept\n---\nbody",
        )
        .await
        .unwrap();
        tokio::fs::create_dir_all(root.join("nested/broken.md"))
            .await
            .unwrap();
        let docs = scan_auto_memory_topic_documents(&paths).await.unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].relative_path, "nested/good.md");
        let _ = tokio::fs::remove_dir_all(temp).await;
    }
}
