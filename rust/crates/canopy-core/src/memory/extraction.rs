//! Managed auto-memory extraction cursor and index orchestration.
//!
//! Port of `packages/core/src/memory/extract.ts`. Fork execution and live
//! instruction refresh are supplied by the runtime adapter in
//! `extraction_agent_planner.rs`.

use std::io;
use std::path::Path;

use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::future::join;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::extraction_agent_planner::{
    AutoMemoryExtractionRuntime, ExtractionAgentPlannerError, run_auto_memory_extraction_by_agent,
};
use super::indexer::{rebuild_managed_auto_memory_index, rebuild_user_auto_memory_index};
use super::paths::AutoMemoryPaths;
use super::store::{
    AutoMemoryExtractCursor, AutoMemoryType, ensure_auto_memory_scaffold_at,
    ensure_user_auto_memory_scaffold,
};
use crate::utils::atomic_file_write::{AtomicWriteOptions, atomic_write_file};

const EXTRACTION_LOG_CONTEXT: &str = "managed auto-memory extraction";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutoMemoryExtractSkippedReason {
    AlreadyRunning,
    Queued,
    MemoryTool,
    MemoryPressure,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoMemoryExtractResult {
    pub touched_topics: Vec<AutoMemoryType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped_reason: Option<AutoMemoryExtractSkippedReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_message: Option<String>,
    pub cursor: AutoMemoryExtractCursor,
}

#[derive(Debug, Error)]
pub enum AutoMemoryExtractError {
    #[error("Failed to scaffold project managed memory: {0}")]
    ProjectScaffold(#[source] io::Error),
    #[error(
        "Managed auto-memory extraction requires runtime configuration for forked-agent execution."
    )]
    RuntimeRequired,
    #[error("Failed to read extraction cursor: {0}")]
    CursorRead(#[source] io::Error),
    #[error("Failed to write extraction cursor: {0}")]
    CursorWrite(#[source] io::Error),
    #[error(transparent)]
    Agent(#[from] ExtractionAgentPlannerError),
    #[error("Failed to rebuild project memory index: {0}")]
    ProjectIndexRebuild(#[source] io::Error),
}

/// Run the managed-memory extraction lifecycle. Project-index failures bubble
/// up so the cursor remains unchanged and the source slice can be retried;
/// user-memory indexing and instruction refresh are best effort.
pub async fn run_auto_memory_extract(
    paths: &AutoMemoryPaths,
    session_id: &str,
    history: &[Value],
    now: DateTime<Utc>,
    runtime: Option<&dyn AutoMemoryExtractionRuntime>,
) -> Result<AutoMemoryExtractResult, AutoMemoryExtractError> {
    ensure_auto_memory_scaffold_at(paths, now)
        .await
        .map_err(AutoMemoryExtractError::ProjectScaffold)?;

    if let Err(error) = ensure_user_auto_memory_scaffold(paths).await {
        if let Some(runtime) = runtime {
            runtime.warn(&format!(
                "User-level auto-memory scaffold failed (non-critical, will skip user-level writes this run): {error}"
            ));
        }
    }

    // The source requires Config before checking the cursor, even when the
    // history currently has no unread user messages.
    let runtime = runtime.ok_or(AutoMemoryExtractError::RuntimeRequired)?;
    let current_cursor = read_extract_cursor(&paths.auto_memory_extract_cursor_path())
        .await
        .map_err(AutoMemoryExtractError::CursorRead)?;
    let raw_offset = if current_cursor.session_id.as_deref() == Some(session_id) {
        current_cursor.processed_offset.unwrap_or(0)
    } else {
        0
    };
    let history_len = history.len() as u64;
    // Compression can shrink history between calls. Restart from zero instead
    // of retaining an offset beyond the new end.
    let start_offset = if raw_offset > history_len {
        0
    } else {
        raw_offset as usize
    };

    if !has_new_nonempty_user_message(&history[start_offset..]) {
        let cursor = AutoMemoryExtractCursor {
            session_id: Some(session_id.to_owned()),
            processed_offset: Some(history_len),
            updated_at: iso_timestamp(now),
        };
        write_extract_cursor(&paths.auto_memory_extract_cursor_path(), &cursor)
            .await
            .map_err(AutoMemoryExtractError::CursorWrite)?;
        return Ok(AutoMemoryExtractResult {
            touched_topics: Vec::new(),
            skipped_reason: None,
            system_message: None,
            cursor,
        });
    }

    let agent_result = run_auto_memory_extraction_by_agent(paths, history, runtime).await?;
    if !agent_result.touched_topics.is_empty() {
        bump_metadata(
            &paths.auto_memory_metadata_path(),
            now,
            session_id,
            &agent_result.touched_topics,
        )
        .await;

        // Both scopes may rebuild concurrently. A project rebuild failure must
        // surface, while user-level EACCES/read-only failures are isolated.
        let rebuild_project =
            agent_result.touched_project_scope || !agent_result.touched_user_scope;
        let project_future = async {
            if rebuild_project {
                rebuild_managed_auto_memory_index(paths).await.map(|_| ())
            } else {
                Ok(())
            }
        };
        let user_future = async {
            if agent_result.touched_user_scope
                && let Err(error) = rebuild_user_auto_memory_index(paths).await
            {
                runtime.warn(&format!(
                    "Auto-memory user-level index rebuild failed (non-critical, project-level rebuild unaffected): {error}"
                ));
            }
        };
        let (project_result, ()) = join(project_future, user_future).await;
        project_result.map_err(AutoMemoryExtractError::ProjectIndexRebuild)?;

        if let Err(error) = runtime
            .refresh_memory_instruction(EXTRACTION_LOG_CONTEXT)
            .await
        {
            runtime.warn(&format!(
                "{EXTRACTION_LOG_CONTEXT}: refreshMemoryInstruction failed: {error}"
            ));
        }
    }

    let made_genuine_progress =
        !agent_result.touched_topics.is_empty() || agent_result.has_tool_activity;
    let cursor = AutoMemoryExtractCursor {
        session_id: Some(session_id.to_owned()),
        processed_offset: Some(if made_genuine_progress {
            history_len
        } else {
            start_offset as u64
        }),
        updated_at: iso_timestamp(now),
    };
    write_extract_cursor(&paths.auto_memory_extract_cursor_path(), &cursor)
        .await
        .map_err(AutoMemoryExtractError::CursorWrite)?;

    Ok(AutoMemoryExtractResult {
        touched_topics: agent_result.touched_topics,
        skipped_reason: None,
        system_message: agent_result.system_message,
        cursor,
    })
}

async fn read_extract_cursor(path: &Path) -> io::Result<AutoMemoryExtractCursor> {
    let content = match tokio::fs::read(path).await {
        Ok(content) => content,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(AutoMemoryExtractCursor {
                session_id: None,
                processed_offset: None,
                updated_at: iso_timestamp(DateTime::<Utc>::from(std::time::UNIX_EPOCH)),
            });
        }
        Err(error) => return Err(error),
    };
    serde_json::from_slice(&content)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

async fn write_extract_cursor(path: &Path, cursor: &AutoMemoryExtractCursor) -> io::Result<()> {
    let mut bytes = serde_json::to_vec_pretty(cursor).map_err(io::Error::other)?;
    bytes.push(b'\n');
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        atomic_write_file(path, &bytes, &AtomicWriteOptions::default())
    })
    .await
    .map_err(io::Error::other)?
}

async fn bump_metadata(
    path: &Path,
    now: DateTime<Utc>,
    session_id: &str,
    touched_topics: &[AutoMemoryType],
) {
    let Ok(content) = tokio::fs::read(path).await else {
        return;
    };
    let Ok(mut metadata) = serde_json::from_slice::<Value>(&content) else {
        return;
    };
    let Some(metadata) = metadata.as_object_mut() else {
        return;
    };
    let timestamp = iso_timestamp(now);
    metadata.insert("updatedAt".to_owned(), Value::String(timestamp.clone()));
    metadata.insert("lastExtractionAt".to_owned(), Value::String(timestamp));
    metadata.insert(
        "lastExtractionSessionId".to_owned(),
        Value::String(session_id.to_owned()),
    );
    let Ok(topics) = serde_json::to_value(touched_topics) else {
        return;
    };
    metadata.insert("lastExtractionTouchedTopics".to_owned(), topics);
    let extraction_status = if touched_topics.is_empty() {
        "noop"
    } else {
        "updated"
    };
    metadata.insert(
        "lastExtractionStatus".to_owned(),
        Value::String(extraction_status.to_owned()),
    );
    let Ok(mut bytes) = serde_json::to_vec_pretty(&metadata) else {
        return;
    };
    bytes.push(b'\n');
    let path = path.to_path_buf();
    let _ = tokio::task::spawn_blocking(move || {
        atomic_write_file(path, &bytes, &AtomicWriteOptions::default())
    })
    .await;
}

fn has_new_nonempty_user_message(history_slice: &[Value]) -> bool {
    history_slice.iter().any(|message| {
        message.get("role").and_then(Value::as_str) == Some("user")
            && message
                .get("parts")
                .and_then(Value::as_array)
                .is_some_and(|parts| parts.iter().any(part_text_is_nonempty))
    })
}

fn part_text_is_nonempty(part: &Value) -> bool {
    // partToString() without verbose mode reads text and ignores all other
    // multimodal/function fields. Scan by reference so huge messages do not
    // allocate a normalized copy just to test whether they contain text.
    let Some(text) = part
        .as_str()
        .or_else(|| part.get("text").and_then(Value::as_str))
    else {
        return false;
    };
    text.chars()
        .any(|character| !is_ecmascript_whitespace(character))
}

fn is_ecmascript_whitespace(character: char) -> bool {
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

fn iso_timestamp(now: DateTime<Utc>) -> String {
    now.to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryProjectScope;
    use crate::memory::extraction_agent_planner::{
        ExtractionAgentFuture, ExtractionAgentRequest, ExtractionAgentRunResult,
        ExtractionAgentStatus, ExtractionRefreshFuture,
    };
    use chrono::TimeZone;
    use serde_json::json;
    use std::path::PathBuf;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeRuntime {
        agent_result: Mutex<Option<ExtractionAgentRunResult>>,
        requests: Mutex<Vec<ExtractionAgentRequest>>,
        warnings: Mutex<Vec<String>>,
        refreshes: Mutex<Vec<String>>,
    }

    impl AutoMemoryExtractionRuntime for FakeRuntime {
        fn execute_extraction_agent<'a>(
            &'a self,
            request: ExtractionAgentRequest,
        ) -> ExtractionAgentFuture<'a> {
            self.requests.lock().unwrap().push(request);
            let result =
                self.agent_result
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap_or(ExtractionAgentRunResult {
                        status: ExtractionAgentStatus::Completed,
                        terminate_reason: None,
                        files_touched: Vec::new(),
                        files_written: Vec::new(),
                    });
            Box::pin(async move { Ok(result) })
        }

        fn refresh_memory_instruction<'a>(
            &'a self,
            log_context: &'static str,
        ) -> ExtractionRefreshFuture<'a> {
            self.refreshes.lock().unwrap().push(log_context.to_owned());
            Box::pin(async { Ok(()) })
        }

        fn warn(&self, message: &str) {
            self.warnings.lock().unwrap().push(message.to_owned());
        }
    }

    fn test_paths() -> (PathBuf, AutoMemoryPaths) {
        let root = std::env::temp_dir().join(format!("canopy-extract-{}", uuid::Uuid::new_v4()));
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

    fn user_history(text: &str) -> Vec<Value> {
        vec![json!({"role":"user", "parts":[{"text":text}]})]
    }

    #[tokio::test]
    async fn skips_empty_history_and_advances_cursor_without_starting_agent() {
        let (temp, paths) = test_paths();
        let runtime = FakeRuntime::default();
        let result =
            run_auto_memory_extract(&paths, "session-1", &[], fixed_time(), Some(&runtime))
                .await
                .unwrap();
        assert!(result.touched_topics.is_empty());
        assert_eq!(result.cursor.processed_offset, Some(0));
        assert!(runtime.requests.lock().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(temp);
    }

    #[tokio::test]
    async fn only_text_user_messages_count_and_ecmascript_whitespace_is_trimmed() {
        let (temp, paths) = test_paths();
        let runtime = FakeRuntime::default();
        let history = vec![
            json!({"role":"user", "parts":[{"inlineData":{"mimeType":"image/png"}}]}),
            json!({"role":"model", "parts":[{"text":"answer"}]}),
            json!({"role":"user", "parts":[{"text":"\u{feff}\u{00a0} \n"}]}),
        ];
        let result =
            run_auto_memory_extract(&paths, "session-1", &history, fixed_time(), Some(&runtime))
                .await
                .unwrap();
        assert!(runtime.requests.lock().unwrap().is_empty());
        assert_eq!(result.cursor.processed_offset, Some(3));
        let _ = std::fs::remove_dir_all(temp);
    }

    #[tokio::test]
    async fn advances_cursor_after_tool_activity_even_when_no_topic_changed() {
        let (temp, paths) = test_paths();
        let runtime = FakeRuntime::default();
        *runtime.agent_result.lock().unwrap() = Some(ExtractionAgentRunResult {
            status: ExtractionAgentStatus::Completed,
            terminate_reason: None,
            files_touched: vec!["/outside/readme.md".to_owned()],
            files_written: Vec::new(),
        });
        let result = run_auto_memory_extract(
            &paths,
            "session-1",
            &user_history("hello"),
            fixed_time(),
            Some(&runtime),
        )
        .await
        .unwrap();
        assert_eq!(result.cursor.processed_offset, Some(1));
        assert_eq!(result.touched_topics, Vec::<AutoMemoryType>::new());
        assert!(runtime.refreshes.lock().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(temp);
    }

    #[tokio::test]
    async fn zero_tool_activity_keeps_unread_offset_for_retry() {
        let (temp, paths) = test_paths();
        let runtime = FakeRuntime::default();
        let result = run_auto_memory_extract(
            &paths,
            "session-1",
            &user_history("remember this"),
            fixed_time(),
            Some(&runtime),
        )
        .await
        .unwrap();
        assert_eq!(result.cursor.processed_offset, Some(0));
        let _ = std::fs::remove_dir_all(temp);
    }

    #[tokio::test]
    async fn cursor_reuses_offset_for_same_session_and_resets_for_new_session() {
        let (temp, paths) = test_paths();
        let runtime = FakeRuntime::default();
        let history = user_history("hello");
        let result = run_auto_memory_extract(&paths, "one", &history, fixed_time(), Some(&runtime))
            .await
            .unwrap();
        assert_eq!(result.cursor.processed_offset, Some(0));
        // No real progress means the same unread slice is retried.
        assert_eq!(
            run_auto_memory_extract(&paths, "one", &history, fixed_time(), Some(&runtime))
                .await
                .unwrap()
                .cursor
                .processed_offset,
            Some(0)
        );
        assert_eq!(
            run_auto_memory_extract(&paths, "two", &history, fixed_time(), Some(&runtime))
                .await
                .unwrap()
                .cursor
                .processed_offset,
            Some(0)
        );
        assert_eq!(runtime.requests.lock().unwrap().len(), 3);
        let _ = std::fs::remove_dir_all(temp);
    }

    #[tokio::test]
    async fn refreshes_and_rebuilds_indexes_after_project_write() {
        let (temp, paths) = test_paths();
        ensure_auto_memory_scaffold_at(&paths, fixed_time())
            .await
            .unwrap();
        let runtime = FakeRuntime::default();
        let metadata_path = paths.auto_memory_metadata_path();
        let mut metadata: Value =
            serde_json::from_slice(&tokio::fs::read(&metadata_path).await.unwrap()).unwrap();
        metadata["futureField"] = json!({"preserve": true});
        tokio::fs::write(
            &metadata_path,
            serde_json::to_vec_pretty(&metadata).unwrap(),
        )
        .await
        .unwrap();
        let project_file = paths.auto_memory_root().join("project/decision.md");
        std::fs::create_dir_all(project_file.parent().unwrap()).unwrap();
        std::fs::write(
            &project_file,
            "---\nname: Decision\ndescription: Durable decision\ntype: project\n---\nDecision body.\n",
        )
        .unwrap();
        *runtime.agent_result.lock().unwrap() = Some(ExtractionAgentRunResult {
            status: ExtractionAgentStatus::Completed,
            terminate_reason: None,
            files_touched: vec![project_file.display().to_string()],
            files_written: vec![project_file.display().to_string()],
        });
        let result = run_auto_memory_extract(
            &paths,
            "session-1",
            &user_history("A lasting product decision."),
            fixed_time(),
            Some(&runtime),
        )
        .await
        .unwrap();
        assert_eq!(result.touched_topics, vec![AutoMemoryType::Project]);
        assert!(result.system_message.unwrap().contains("project.md"));
        assert_eq!(
            runtime.refreshes.lock().unwrap().as_slice(),
            &[EXTRACTION_LOG_CONTEXT]
        );
        assert!(
            tokio::fs::read_to_string(paths.auto_memory_index_path())
                .await
                .unwrap()
                .contains("decision.md")
        );
        let metadata: Value = serde_json::from_slice(
            &tokio::fs::read(paths.auto_memory_metadata_path())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(metadata["lastExtractionSessionId"], "session-1");
        assert_eq!(metadata["futureField"]["preserve"], true);
        assert_eq!(metadata["lastExtractionStatus"], "updated");
        let _ = std::fs::remove_dir_all(temp);
    }

    #[tokio::test]
    async fn project_index_failure_keeps_cursor_unadvanced() {
        let (temp, paths) = test_paths();
        ensure_auto_memory_scaffold_at(&paths, fixed_time())
            .await
            .unwrap();
        let runtime = FakeRuntime::default();
        *runtime.agent_result.lock().unwrap() = Some(ExtractionAgentRunResult {
            status: ExtractionAgentStatus::Completed,
            terminate_reason: None,
            files_touched: vec!["/anything".into()],
            files_written: vec![
                paths
                    .auto_memory_root()
                    .join("user/prefs.md")
                    .display()
                    .to_string(),
            ],
        });
        // A topic counts as project scope by default (filesWritten has a
        // project-memory root path). Force rebuild failure with a directory
        // at the index's file path.
        std::fs::remove_file(paths.auto_memory_index_path()).unwrap();
        std::fs::create_dir(paths.auto_memory_index_path()).unwrap();
        let error = run_auto_memory_extract(
            &paths,
            "session-1",
            &user_history("keep this"),
            fixed_time(),
            Some(&runtime),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            AutoMemoryExtractError::ProjectIndexRebuild(_)
        ));
        // The cursor remains the scaffold's default because rebuild failed.
        let cursor: AutoMemoryExtractCursor = serde_json::from_slice(
            &tokio::fs::read(paths.auto_memory_extract_cursor_path())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(cursor.session_id, None);
        let _ = std::fs::remove_dir_all(temp);
    }

    #[tokio::test]
    async fn throws_for_missing_runtime_after_scaffolding() {
        let (temp, paths) = test_paths();
        let error = run_auto_memory_extract(&paths, "session", &[], fixed_time(), None)
            .await
            .unwrap_err();
        assert!(matches!(error, AutoMemoryExtractError::RuntimeRequired));
        assert!(paths.auto_memory_extract_cursor_path().exists());
        let _ = std::fs::remove_dir_all(temp);
    }
}
