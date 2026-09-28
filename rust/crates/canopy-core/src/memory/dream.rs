//! Managed auto-memory dream orchestration and metadata updates.
//!
//! Port of `packages/core/src/memory/dream.ts`. The dream agent runs through
//! an injected native runtime; this module owns scaffold/index/metadata
//! sequencing and abort-aware post-processing.

use std::io;
use std::time::Instant;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json;
use tokio::sync::watch;

use super::dream_agent_planner::{
    DreamAgentRuntime, DreamPlannerError, DreamPlannerOptions,
    plan_managed_auto_memory_dream_by_agent,
};
use super::indexer::rebuild_managed_auto_memory_index;
use super::paths::AutoMemoryPaths;
use super::store::{
    AutoMemoryMetadata, AutoMemoryStatus, AutoMemoryType, ensure_auto_memory_scaffold,
};
use crate::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MemoryDreamTrigger {
    #[default]
    Auto,
    Manual,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RunDreamOptions {
    pub trigger: MemoryDreamTrigger,
    pub record_metadata: bool,
    pub suppress_chat_recording: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutoMemoryDreamResult {
    pub touched_topics: Vec<AutoMemoryType>,
    pub deduped_entries: usize,
    pub system_message: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryDreamTelemetryEvent {
    pub trigger: MemoryDreamTrigger,
    pub status: AutoMemoryStatus,
    pub deduped_entries: usize,
    pub touched_topics: Vec<AutoMemoryType>,
    pub duration_ms: u64,
}

/// `record_memory_dream` is intentionally optional for runtimes that do not
/// yet have the telemetry subsystem wired into the native memory layer.
pub trait DreamOrchestratorRuntime: DreamAgentRuntime {
    fn record_memory_dream(&self, _event: MemoryDreamTelemetryEvent) {}
}

#[derive(Debug, thiserror::Error)]
pub enum AutoMemoryDreamError {
    #[error("Managed auto-memory dream requires an agent runtime.")]
    MissingRuntime,
    #[error("Failed to scaffold auto-memory: {0}")]
    Scaffold(#[source] io::Error),
    #[error(transparent)]
    Planner(#[from] DreamPlannerError),
    #[error("Failed to rebuild managed auto-memory index: {0}")]
    IndexRebuild(#[source] io::Error),
}

/// Execute a managed-memory dream. A cancellation observed after the agent
/// returns preserves its partial touched-topic result and skips both index
/// work and scheduler metadata, matching the manager's cancel-safe ordering.
pub async fn run_managed_auto_memory_dream<R: DreamOrchestratorRuntime>(
    paths: &AutoMemoryPaths,
    now: DateTime<Utc>,
    runtime: Option<&R>,
    abort_signal: Option<watch::Receiver<bool>>,
    options: RunDreamOptions,
    planner_options: DreamPlannerOptions,
) -> Result<AutoMemoryDreamResult, AutoMemoryDreamError> {
    ensure_auto_memory_scaffold(paths)
        .await
        .map_err(AutoMemoryDreamError::Scaffold)?;
    let started = Instant::now();
    let Some(runtime) = runtime else {
        return Err(AutoMemoryDreamError::MissingRuntime);
    };

    let agent_result = plan_managed_auto_memory_dream_by_agent(
        paths,
        runtime,
        abort_signal.clone(),
        DreamPlannerOptions {
            suppress_chat_recording: options.suppress_chat_recording,
            ..planner_options
        },
    )
    .await?;
    let touched_topics = infer_touched_topics(&agent_result.files_touched);
    let result = AutoMemoryDreamResult {
        touched_topics: touched_topics.clone(),
        deduped_entries: 0,
        system_message: Some(format!(
            "Managed auto-memory dream (agent): {}",
            dream_summary(
                agent_result.final_text.as_deref(),
                agent_result.files_touched.len()
            )
        )),
    };

    if is_aborted(&abort_signal) {
        return Ok(result);
    }
    if !touched_topics.is_empty() {
        rebuild_managed_auto_memory_index(paths)
            .await
            .map_err(AutoMemoryDreamError::IndexRebuild)?;
    }
    if options.record_metadata {
        update_dream_metadata_result(paths, now, &touched_topics, None).await;
    }

    runtime.record_memory_dream(MemoryDreamTelemetryEvent {
        trigger: options.trigger,
        status: if touched_topics.is_empty() {
            AutoMemoryStatus::Noop
        } else {
            AutoMemoryStatus::Updated
        },
        deduped_entries: result.deduped_entries,
        touched_topics,
        duration_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
    });
    Ok(result)
}

pub fn infer_touched_topics(files_touched: &[std::path::PathBuf]) -> Vec<AutoMemoryType> {
    let normalized = files_touched
        .iter()
        .map(|path| path.to_string_lossy().replace('\\', "/"))
        .collect::<Vec<_>>();
    AutoMemoryType::ALL
        .into_iter()
        .filter(|topic| {
            let marker = format!("/{}/", topic.as_str());
            normalized.iter().any(|path| path.contains(&marker))
        })
        .collect()
}

/// Update dream gating metadata best-effort. Callers that own task status
/// transitions can choose to leave this off until their success commit.
pub async fn update_dream_metadata_result(
    paths: &AutoMemoryPaths,
    now: DateTime<Utc>,
    touched_topics: &[AutoMemoryType],
    session_id: Option<&str>,
) {
    let path = paths.auto_memory_metadata_path();
    let Ok(content) = tokio::fs::read(&path).await else {
        return;
    };
    let Ok(content) = String::from_utf8(content) else {
        return;
    };
    let Ok(mut metadata) = serde_json::from_str::<AutoMemoryMetadata>(&content) else {
        return;
    };
    let timestamp = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    metadata.updated_at = timestamp.clone();
    metadata.last_dream_at = Some(timestamp);
    metadata.last_dream_touched_topics = Some(touched_topics.to_vec());
    metadata.last_dream_status = Some(if touched_topics.is_empty() {
        AutoMemoryStatus::Noop
    } else {
        AutoMemoryStatus::Updated
    });
    if let Some(session_id) = session_id {
        metadata.last_dream_session_id = Some(session_id.to_owned());
        metadata.recent_session_ids_since_dream = Some(Vec::new());
    }
    let Ok(mut bytes) = serde_json::to_vec_pretty(&metadata) else {
        return;
    };
    bytes.push(b'\n');
    let _ = atomic_write_file(
        path,
        &bytes,
        &AtomicWriteOptions {
            symlink_policy: SymlinkPolicy::Follow,
            ..AtomicWriteOptions::default()
        },
    );
}

/// Record that a user manually invoked `/dream`, clearing the scheduler's
/// same-session dedupe list along with the completion timestamp.
pub async fn write_dream_manual_run_to_metadata(
    paths: &AutoMemoryPaths,
    session_id: &str,
    now: DateTime<Utc>,
) {
    update_dream_metadata_result(paths, now, &[], Some(session_id)).await;
}

fn is_aborted(abort_signal: &Option<watch::Receiver<bool>>) -> bool {
    abort_signal.as_ref().is_some_and(|signal| *signal.borrow())
}

fn dream_summary(final_text: Option<&str>, files_touched: usize) -> String {
    let Some(text) = final_text.map(str::trim).filter(|text| !text.is_empty()) else {
        return format!("updated {files_touched} file(s)");
    };
    text.chars().take(300).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn touched_topics_are_unique_and_follow_source_order() {
        let files = [
            "/memory/reference/api.md",
            "/memory/user/prefs.md",
            "/memory/reference/notes.md",
            "/memory/pinned/private.md",
            "/memory/no-topic.md",
        ]
        .into_iter()
        .map(std::path::PathBuf::from)
        .collect::<Vec<_>>();
        assert_eq!(
            infer_touched_topics(&files),
            vec![AutoMemoryType::User, AutoMemoryType::Reference]
        );
    }

    #[test]
    fn summary_trims_and_falls_back_to_file_count() {
        assert_eq!(
            dream_summary(Some("  merged memories  "), 1),
            "merged memories"
        );
        assert_eq!(dream_summary(Some("  "), 3), "updated 3 file(s)");
        assert_eq!(dream_summary(None, 0), "updated 0 file(s)");
        assert_eq!(dream_summary(Some(&"x".repeat(400)), 0).len(), 300);
    }
}
