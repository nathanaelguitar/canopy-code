use std::io;
use std::time::UNIX_EPOCH;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use super::paths::AutoMemoryPaths;

pub const AUTO_MEMORY_SCHEMA_VERSION: u32 = 1;
pub const AUTO_MEMORY_TYPES: [&str; 4] = ["user", "feedback", "project", "reference"];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AutoMemoryType {
    User,
    Feedback,
    Project,
    Reference,
}

impl AutoMemoryType {
    pub const ALL: [Self; 4] = [Self::User, Self::Feedback, Self::Project, Self::Reference];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Feedback => "feedback",
            Self::Project => "project",
            Self::Reference => "reference",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoMemorySourceRef {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub recorded_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_ids: Option<Vec<String>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AutoMemoryStatus {
    Updated,
    Noop,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoMemoryMetadata {
    pub version: u32,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_extraction_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_extraction_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_extraction_touched_topics: Option<Vec<AutoMemoryType>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_extraction_status: Option<AutoMemoryStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_dream_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_dream_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_dream_touched_topics: Option<Vec<AutoMemoryType>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_dream_status: Option<AutoMemoryStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recent_session_ids_since_dream: Option<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoMemoryExtractCursor {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processed_offset: Option<u64>,
    pub updated_at: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AutoMemoryFileStats {
    pub size: u64,
    /// Milliseconds since the Unix epoch, matching Node's `Stats.mtimeMs`.
    pub mtime_ms: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AutoMemoryIndexRead {
    pub content: String,
    pub stats: AutoMemoryFileStats,
}

pub fn create_default_auto_memory_metadata(now: DateTime<Utc>) -> AutoMemoryMetadata {
    let iso = iso_timestamp(now);
    AutoMemoryMetadata {
        version: AUTO_MEMORY_SCHEMA_VERSION,
        created_at: iso.clone(),
        updated_at: iso,
        last_extraction_at: None,
        last_extraction_session_id: None,
        last_extraction_touched_topics: None,
        last_extraction_status: None,
        last_dream_at: None,
        last_dream_session_id: None,
        last_dream_touched_topics: None,
        last_dream_status: None,
        recent_session_ids_since_dream: None,
    }
}

pub fn create_default_auto_memory_extract_cursor(now: DateTime<Utc>) -> AutoMemoryExtractCursor {
    AutoMemoryExtractCursor {
        session_id: None,
        processed_offset: None,
        updated_at: iso_timestamp(now),
    }
}

pub fn create_default_auto_memory_index() -> String {
    String::new()
}

/// Create the private per-project auto-memory root and its three source-format
/// starter files. Existing files win, including when another process creates
/// the same file concurrently.
pub async fn ensure_auto_memory_scaffold(paths: &AutoMemoryPaths) -> io::Result<()> {
    ensure_auto_memory_scaffold_at(paths, Utc::now()).await
}

pub async fn ensure_auto_memory_scaffold_at(
    paths: &AutoMemoryPaths,
    now: DateTime<Utc>,
) -> io::Result<()> {
    let root = paths.auto_memory_root();
    tokio::fs::create_dir_all(&root).await?;
    write_file_if_missing(
        &paths.auto_memory_index_path(),
        create_default_auto_memory_index().as_bytes(),
    )
    .await?;
    let metadata = create_default_auto_memory_metadata(now);
    write_json_file_if_missing(&paths.auto_memory_metadata_path(), &metadata).await?;
    let cursor = create_default_auto_memory_extract_cursor(now);
    write_json_file_if_missing(&paths.auto_memory_extract_cursor_path(), &cursor).await
}

/// Ensure the cross-project root and its empty index. User memory intentionally
/// has no project metadata or extraction cursor.
pub async fn ensure_user_auto_memory_scaffold(paths: &AutoMemoryPaths) -> io::Result<()> {
    tokio::fs::create_dir_all(paths.user_auto_memory_root()).await?;
    write_file_if_missing(
        &paths.user_auto_memory_index_path(),
        create_default_auto_memory_index().as_bytes(),
    )
    .await
}

pub async fn read_auto_memory_index(paths: &AutoMemoryPaths) -> io::Result<Option<String>> {
    read_utf8_or_none(&paths.auto_memory_index_path()).await
}

pub async fn read_auto_memory_index_with_stats(
    paths: &AutoMemoryPaths,
) -> io::Result<Option<AutoMemoryIndexRead>> {
    read_memory_index_with_stats(&paths.auto_memory_index_path()).await
}

pub async fn read_user_auto_memory_index(paths: &AutoMemoryPaths) -> io::Result<Option<String>> {
    read_utf8_or_none(&paths.user_auto_memory_index_path()).await
}

pub async fn read_user_auto_memory_index_with_stats(
    paths: &AutoMemoryPaths,
) -> io::Result<Option<AutoMemoryIndexRead>> {
    read_memory_index_with_stats(&paths.user_auto_memory_index_path()).await
}

async fn write_json_file_if_missing<T: Serialize>(
    path: &std::path::Path,
    value: &T,
) -> io::Result<()> {
    let mut content = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    content.push(b'\n');
    write_file_if_missing(path, &content).await
}

async fn write_file_if_missing(path: &std::path::Path, content: &[u8]) -> io::Result<()> {
    let mut file = match tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => return Ok(()),
        Err(error) => return Err(error),
    };
    file.write_all(content).await
}

async fn read_utf8_or_none(path: &std::path::Path) -> io::Result<Option<String>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).into_owned())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

async fn read_memory_index_with_stats(
    path: &std::path::Path,
) -> io::Result<Option<AutoMemoryIndexRead>> {
    let metadata = match tokio::fs::metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
    let mtime_ms = match modified.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs_f64() * 1000.0,
        Err(error) => -error.duration().as_secs_f64() * 1000.0,
    };
    Ok(Some(AutoMemoryIndexRead {
        content: String::from_utf8_lossy(&bytes).into_owned(),
        stats: AutoMemoryFileStats {
            size: metadata.len(),
            mtime_ms,
        },
    }))
}

fn iso_timestamp(now: DateTime<Utc>) -> String {
    now.to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::paths::{
        AUTO_MEMORY_EXTRACT_CURSOR_FILENAME, AUTO_MEMORY_INDEX_FILENAME,
        AUTO_MEMORY_METADATA_FILENAME,
    };
    use crate::memory::{AutoMemoryPaths, MemoryProjectScope};
    use chrono::TimeZone;
    use std::path::PathBuf;

    fn temp_paths(label: &str) -> (PathBuf, AutoMemoryPaths) {
        let root = std::env::temp_dir().join(format!(
            "canopy-memory-store-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let paths = AutoMemoryPaths::new(
            root.join("project"),
            root.join("state"),
            false,
            MemoryProjectScope::Workspace,
        );
        (root, paths)
    }

    fn fixed_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 4, 1, 8, 0, 0).unwrap() + chrono::Duration::milliseconds(123)
    }

    #[tokio::test]
    async fn writes_source_compatible_default_scaffold_and_preserves_existing_files() {
        let (temp, paths) = temp_paths("scaffold");
        let now = fixed_time();
        ensure_auto_memory_scaffold_at(&paths, now).await.unwrap();
        assert_eq!(
            tokio::fs::read_to_string(paths.auto_memory_index_path())
                .await
                .unwrap(),
            ""
        );
        assert_eq!(
            tokio::fs::read_to_string(paths.auto_memory_metadata_path())
                .await
                .unwrap(),
            "{\n  \"version\": 1,\n  \"createdAt\": \"2026-04-01T08:00:00.123Z\",\n  \"updatedAt\": \"2026-04-01T08:00:00.123Z\"\n}\n"
        );
        assert_eq!(
            tokio::fs::read_to_string(paths.auto_memory_extract_cursor_path())
                .await
                .unwrap(),
            "{\n  \"updatedAt\": \"2026-04-01T08:00:00.123Z\"\n}\n"
        );

        tokio::fs::write(paths.auto_memory_index_path(), "# Existing\n")
            .await
            .unwrap();
        ensure_auto_memory_scaffold_at(&paths, now + chrono::Duration::days(1))
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read_to_string(paths.auto_memory_index_path())
                .await
                .unwrap(),
            "# Existing\n"
        );

        ensure_user_auto_memory_scaffold(&paths).await.unwrap();
        assert_eq!(
            tokio::fs::read_to_string(paths.user_auto_memory_index_path())
                .await
                .unwrap(),
            ""
        );
        assert!(
            !paths
                .user_auto_memory_root()
                .join(AUTO_MEMORY_METADATA_FILENAME)
                .exists()
        );
        assert!(
            !paths
                .user_auto_memory_root()
                .join(AUTO_MEMORY_EXTRACT_CURSOR_FILENAME)
                .exists()
        );
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[tokio::test]
    async fn reads_missing_existing_invalid_utf8_and_stats_with_source_error_boundaries() {
        let (temp, paths) = temp_paths("read");
        assert_eq!(read_auto_memory_index(&paths).await.unwrap(), None);
        assert_eq!(
            read_auto_memory_index_with_stats(&paths).await.unwrap(),
            None
        );

        ensure_auto_memory_scaffold(&paths).await.unwrap();
        tokio::fs::write(paths.auto_memory_index_path(), [b'a', 0xff])
            .await
            .unwrap();
        assert_eq!(
            read_auto_memory_index(&paths).await.unwrap().as_deref(),
            Some("a\u{fffd}")
        );
        let read = read_auto_memory_index_with_stats(&paths)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.content, "a\u{fffd}");
        assert_eq!(read.stats.size, 2);
        assert!(read.stats.mtime_ms > 0.0);

        let bad_path = paths.auto_memory_root().join(AUTO_MEMORY_INDEX_FILENAME);
        tokio::fs::remove_file(&bad_path).await.unwrap();
        tokio::fs::create_dir(&bad_path).await.unwrap();
        assert_eq!(
            read_auto_memory_index(&paths).await.unwrap_err().kind(),
            io::ErrorKind::IsADirectory
        );
        assert_eq!(
            read_auto_memory_index_with_stats(&paths)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::IsADirectory
        );
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[tokio::test]
    async fn concurrent_scaffold_creation_keeps_a_single_complete_set_of_files() {
        let (temp, paths) = temp_paths("race");
        let (left, right) = tokio::join!(
            ensure_auto_memory_scaffold_at(&paths, fixed_time()),
            ensure_auto_memory_scaffold_at(&paths, fixed_time()),
        );
        left.unwrap();
        right.unwrap();
        assert_eq!(
            tokio::fs::read_to_string(paths.auto_memory_index_path())
                .await
                .unwrap(),
            ""
        );
        let metadata: AutoMemoryMetadata = serde_json::from_slice(
            &tokio::fs::read(paths.auto_memory_metadata_path())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(metadata.version, AUTO_MEMORY_SCHEMA_VERSION);
        assert_eq!(metadata.created_at, metadata.updated_at);
        std::fs::remove_dir_all(temp).unwrap();
    }
}
