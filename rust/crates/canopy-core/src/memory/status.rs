//! Managed auto-memory status aggregation.
//!
//! Port of `packages/core/src/memory/status.ts`. Task storage is injected so
//! the status API can be connected to the native runtime without coupling the
//! filesystem layer to the background-task manager.

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::paths::AutoMemoryPaths;
use super::scan::scan_auto_memory_topic_documents;
use super::store::AutoMemoryType;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ManagedMemoryTaskType {
    Extract,
    Dream,
    #[serde(rename = "skill-review")]
    SkillReview,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryTaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Skipped,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryTaskRecord {
    pub id: String,
    pub task_type: ManagedMemoryTaskType,
    pub project_root: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub status: MemoryTaskStatus,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

/// Read-only task listing required by the status command.
pub trait MemoryTaskSource: Send + Sync {
    fn list_tasks_by_type(
        &self,
        task_type: ManagedMemoryTaskType,
        project_root: &Path,
    ) -> Vec<MemoryTaskRecord>;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedAutoMemoryTopicStatus {
    pub topic: AutoMemoryType,
    pub entry_count: usize,
    pub file_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedAutoMemoryStatus {
    pub root: PathBuf,
    pub index_path: PathBuf,
    pub index_content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    pub extraction_running: bool,
    pub topics: Vec<ManagedAutoMemoryTopicStatus>,
    pub extraction_tasks: Vec<MemoryTaskRecord>,
    pub dream_tasks: Vec<MemoryTaskRecord>,
}

async fn read_json_file(path: &Path) -> Option<Value> {
    let content = tokio::fs::read(path).await.ok()?;
    serde_json::from_slice(&content).ok()
}

/// Read index, cursor, metadata, topic documents, and task state concurrently.
/// Index read errors map to an empty string; malformed or missing JSON maps to
/// `None`, matching the TypeScript status helper's best-effort reads.
pub async fn get_managed_auto_memory_status(
    paths: &AutoMemoryPaths,
    tasks: &dyn MemoryTaskSource,
) -> io::Result<ManagedAutoMemoryStatus> {
    let root = paths.auto_memory_root();
    let index_path = paths.auto_memory_index_path();
    let cursor_path = paths.auto_memory_extract_cursor_path();
    let metadata_path = paths.auto_memory_metadata_path();

    let (index_content, cursor, metadata, docs) = tokio::join!(
        async {
            tokio::fs::read(&index_path)
                .await
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                .unwrap_or_default()
        },
        read_json_file(&cursor_path),
        read_json_file(&metadata_path),
        scan_auto_memory_topic_documents(paths),
    );
    let docs = docs?;

    let mut grouped: Vec<(AutoMemoryType, Vec<PathBuf>)> = AutoMemoryType::ALL
        .into_iter()
        .map(|topic| (topic, Vec::new()))
        .collect();
    for doc in docs {
        if let Some((_, file_paths)) = grouped
            .iter_mut()
            .find(|(topic, _)| *topic == doc.memory_type)
        {
            file_paths.push(doc.file_path);
        }
    }
    let topics = grouped
        .into_iter()
        .map(|(topic, file_paths)| ManagedAutoMemoryTopicStatus {
            topic,
            entry_count: file_paths.len(),
            file_paths,
        })
        .collect();

    let extraction_running = tasks
        .list_tasks_by_type(ManagedMemoryTaskType::Extract, paths.project_root())
        .iter()
        .any(|task| task.status == MemoryTaskStatus::Running);
    let extraction_tasks =
        tasks.list_tasks_by_type(ManagedMemoryTaskType::Extract, paths.project_root());
    let dream_tasks = tasks.list_tasks_by_type(ManagedMemoryTaskType::Dream, paths.project_root());

    Ok(ManagedAutoMemoryStatus {
        root,
        index_path,
        index_content,
        cursor,
        metadata,
        extraction_running,
        topics,
        extraction_tasks: extraction_tasks.into_iter().take(8).collect(),
        dream_tasks: dream_tasks.into_iter().take(5).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::paths::MemoryProjectScope;
    use std::sync::Mutex;
    use uuid::Uuid;

    #[derive(Default)]
    struct Tasks {
        extraction: Vec<MemoryTaskRecord>,
        dream: Vec<MemoryTaskRecord>,
        requested: Mutex<Vec<ManagedMemoryTaskType>>,
    }

    impl MemoryTaskSource for Tasks {
        fn list_tasks_by_type(
            &self,
            task_type: ManagedMemoryTaskType,
            _project_root: &Path,
        ) -> Vec<MemoryTaskRecord> {
            self.requested.lock().unwrap().push(task_type.clone());
            match task_type {
                ManagedMemoryTaskType::Extract => self.extraction.clone(),
                ManagedMemoryTaskType::Dream => self.dream.clone(),
                ManagedMemoryTaskType::SkillReview => Vec::new(),
            }
        }
    }

    fn task(task_type: ManagedMemoryTaskType, status: MemoryTaskStatus) -> MemoryTaskRecord {
        MemoryTaskRecord {
            id: Uuid::new_v4().to_string(),
            task_type,
            project_root: "/workspace".to_owned(),
            session_id: None,
            status,
            created_at: "2026-01-01T00:00:00.000Z".to_owned(),
            updated_at: "2026-01-01T00:00:00.000Z".to_owned(),
            progress_text: None,
            error: None,
            metadata: None,
        }
    }

    #[tokio::test]
    async fn aggregates_topic_files_and_task_state_with_best_effort_reads() {
        let temp = std::env::temp_dir().join(format!("canopy-status-{}", Uuid::new_v4()));
        let paths = AutoMemoryPaths::new("/workspace", &temp, false, MemoryProjectScope::Workspace);
        let root = paths.auto_memory_root();
        tokio::fs::create_dir_all(root.join("user")).await.unwrap();
        tokio::fs::write(paths.auto_memory_index_path(), "# Memory index\n")
            .await
            .unwrap();
        tokio::fs::write(
            paths.auto_memory_metadata_path(),
            r#"{"version":1,"createdAt":"now"}"#,
        )
        .await
        .unwrap();
        tokio::fs::write(paths.auto_memory_extract_cursor_path(), "{ malformed json")
            .await
            .unwrap();
        tokio::fs::write(
            root.join("user/preferences.md"),
            "---\ntype: user\nname: Preferences\ndescription: Stable preferences\n---\nbody\n",
        )
        .await
        .unwrap();

        let tasks = Tasks {
            extraction: vec![
                task(ManagedMemoryTaskType::Extract, MemoryTaskStatus::Completed),
                task(ManagedMemoryTaskType::Extract, MemoryTaskStatus::Running),
            ],
            dream: vec![task(
                ManagedMemoryTaskType::Dream,
                MemoryTaskStatus::Pending,
            )],
            ..Tasks::default()
        };
        let status = get_managed_auto_memory_status(&paths, &tasks)
            .await
            .unwrap();

        assert_eq!(status.index_content, "# Memory index\n");
        assert!(status.metadata.is_some());
        assert!(status.cursor.is_none());
        assert!(status.extraction_running);
        assert_eq!(status.topics[0].topic, AutoMemoryType::User);
        assert_eq!(status.topics[0].entry_count, 1);
        assert_eq!(
            status.topics[0].file_paths,
            vec![root.join("user/preferences.md")]
        );
        assert_eq!(status.extraction_tasks.len(), 2);
        assert_eq!(status.dream_tasks.len(), 1);
        assert_eq!(
            *tasks.requested.lock().unwrap(),
            vec![
                ManagedMemoryTaskType::Extract,
                ManagedMemoryTaskType::Extract,
                ManagedMemoryTaskType::Dream
            ]
        );

        tokio::fs::remove_dir_all(temp).await.unwrap();
    }

    #[tokio::test]
    async fn missing_files_return_empty_index_and_no_json() {
        let temp = std::env::temp_dir().join(format!("canopy-status-{}", Uuid::new_v4()));
        let paths = AutoMemoryPaths::new("/workspace", &temp, false, MemoryProjectScope::Workspace);
        let status = get_managed_auto_memory_status(&paths, &Tasks::default())
            .await
            .unwrap();
        assert_eq!(status.index_content, "");
        assert!(status.cursor.is_none());
        assert!(status.metadata.is_none());
        assert!(status.topics.iter().all(|topic| topic.entry_count == 0));
        let _ = tokio::fs::remove_dir_all(temp).await;
    }
}
