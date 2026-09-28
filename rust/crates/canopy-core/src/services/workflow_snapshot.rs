//! Durable summaries of completed workflow runs.
//!
//! Ports `packages/core/src/agents/workflow-snapshot.ts`. A workflow registry
//! entry is projected before the first filesystem await so later activity
//! cannot change the persisted settlement snapshot. The directory is injected
//! because the native workflow registry and Config adapter are still separate
//! integration work.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Maximum number of completed workflow snapshots retained on disk.
pub const MAX_RETAINED_SNAPSHOTS: usize = 30;

/// Workflow status as represented by the in-memory registry.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkflowStatus {
    Running,
    Pausing,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl WorkflowStatus {
    fn terminal(self) -> Option<WorkflowTerminalStatus> {
        match self {
            Self::Completed => Some(WorkflowTerminalStatus::Completed),
            Self::Failed => Some(WorkflowTerminalStatus::Failed),
            Self::Cancelled => Some(WorkflowTerminalStatus::Cancelled),
            Self::Running | Self::Pausing | Self::Paused => None,
        }
    }
}

/// Statuses that may be persisted in a completed workflow snapshot.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkflowTerminalStatus {
    Completed,
    Failed,
    Cancelled,
}

/// Metadata parsed from the workflow script's `export const meta` value.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowMeta {
    pub name: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub when_to_use: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phases: Option<Vec<WorkflowMetaPhase>>,
}

/// Optional detail for one phase in [`WorkflowMeta`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowMetaPhase {
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// Minimal registry-entry projection consumed by the snapshot writer.
///
/// `per_phase_tokens` is a vector rather than a map so insertion order and
/// the source JavaScript `Map`'s `[phaseOrNull, tokens]` pairs are preserved.
/// `result` uses `Option<Value>` so `None` means JavaScript `undefined` (omit
/// the field) while `Some(WorkflowSnapshotResult::Json(Value::Null))` means
/// explicit JSON null.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowSnapshotSource {
    pub run_id: String,
    pub meta: Option<WorkflowMeta>,
    pub status: WorkflowStatus,
    pub script: Option<String>,
    pub script_path: Option<String>,
    pub phases: Vec<String>,
    pub agents_dispatched: u64,
    pub agents_completed: u64,
    pub tokens_spent: u64,
    pub token_budget_total: Option<u64>,
    pub per_phase_tokens: Vec<(Option<String>, u64)>,
    pub recent_logs: Vec<String>,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub result: Option<WorkflowSnapshotResult>,
    pub error: Option<String>,
}

/// JSON-serializable projection of a terminal workflow run.
///
/// Optional fields backed by JavaScript `undefined` are omitted. `meta` and
/// `tokenBudgetTotal` deliberately remain present as explicit nulls when
/// absent, matching the source object literal and `JSON.stringify` behavior.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowSnapshot {
    pub run_id: String,
    pub meta: Option<WorkflowMeta>,
    pub status: WorkflowTerminalStatus,
    pub script: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script_path: Option<String>,
    pub phases: Vec<String>,
    pub agents_dispatched: u64,
    pub agents_completed: u64,
    pub tokens_spent: u64,
    pub token_budget_total: Option<u64>,
    pub per_phase_tokens: Vec<(Option<String>, u64)>,
    pub recent_logs: Vec<String>,
    pub start_time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// A workflow result before the source's JSON-safety projection.
///
/// Rust callers normally use [`WorkflowSnapshotResult::Json`]. The explicit
/// non-JSON case preserves the source behavior for dynamic JavaScript values
/// such as BigInt, functions, or circular objects, which become a placeholder
/// containing their JavaScript `typeof` label.
#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowSnapshotResult {
    Json(Value),
    NonJsonSerializable { js_type: String },
}

impl WorkflowSnapshotResult {
    fn safe_json_value(&self) -> Value {
        match self {
            Self::Json(value) => value.clone(),
            Self::NonJsonSerializable { js_type } => {
                Value::String(format!("(non-JSON-serializable {js_type})"))
            }
        }
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum WorkflowSnapshotError {
    #[error("Cannot snapshot active workflow {0}.")]
    ActiveWorkflow(String),
}

/// Project a registry entry into the serializable terminal snapshot.
pub fn to_snapshot(
    task: &WorkflowSnapshotSource,
) -> Result<WorkflowSnapshot, WorkflowSnapshotError> {
    let status = task
        .status
        .terminal()
        .ok_or_else(|| WorkflowSnapshotError::ActiveWorkflow(task.run_id.clone()))?;

    Ok(WorkflowSnapshot {
        run_id: task.run_id.clone(),
        meta: task.meta.clone(),
        status,
        script: task.script.clone().unwrap_or_default(),
        script_path: task.script_path.clone(),
        phases: task.phases.clone(),
        agents_dispatched: task.agents_dispatched,
        agents_completed: task.agents_completed,
        tokens_spent: task.tokens_spent,
        token_budget_total: task.token_budget_total,
        per_phase_tokens: task.per_phase_tokens.clone(),
        recent_logs: task.recent_logs.clone(),
        start_time: task.start_time,
        end_time: task.end_time,
        result: task
            .result
            .as_ref()
            .map(WorkflowSnapshotResult::safe_json_value),
        error: task.error.clone(),
    })
}

/// Result of a best-effort snapshot write. I/O failures are reported as text
/// instead of being propagated, since persistence is only a convenience.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkflowSnapshotWriteReport {
    pub written: bool,
    pub warnings: Vec<String>,
}

/// Write `<workflow_runs_dir>/<runId>.json`, then prune snapshots beyond the
/// retention cap. `None` models a Config without storage. Projection happens
/// synchronously before the first filesystem await, and persistence errors
/// are collected in the report instead of failing workflow settlement.
pub async fn write_workflow_snapshot(
    workflow_runs_dir: Option<&Path>,
    task: &WorkflowSnapshotSource,
) -> WorkflowSnapshotWriteReport {
    let Some(directory) = workflow_runs_dir else {
        return WorkflowSnapshotWriteReport::default();
    };

    // Keep this above the first await: the live registry entry can continue to
    // mutate while the directory is being created or the file is being saved.
    let snapshot = match to_snapshot(task) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return WorkflowSnapshotWriteReport {
                written: false,
                warnings: vec![error.to_string()],
            };
        }
    };

    let mut report = WorkflowSnapshotWriteReport {
        written: false,
        warnings: Vec::new(),
    };
    if let Err(error) = tokio::fs::create_dir_all(directory).await {
        report.warnings.push(format!(
            "writeWorkflowSnapshot failed for {}: {error}",
            task.run_id
        ));
        return report;
    }

    let snapshot_path = directory.join(format!("{}.json", task.run_id));
    let serialized = match serde_json::to_vec_pretty(&snapshot) {
        Ok(serialized) => serialized,
        Err(error) => {
            report.warnings.push(format!(
                "writeWorkflowSnapshot failed for {}: {error}",
                task.run_id
            ));
            return report;
        }
    };
    if let Err(error) = tokio::fs::write(&snapshot_path, serialized).await {
        report.warnings.push(format!(
            "writeWorkflowSnapshot failed for {}: {error}",
            task.run_id
        ));
        return report;
    }

    report.written = true;
    report.warnings.extend(prune_snapshots(directory).await);
    report
}

/// Load syntactically valid JSON snapshots newest-first by `startTime`. The
/// TypeScript reader casts parsed JSON without validating its shape, so retain
/// incomplete and non-object JSON values as well. A missing/unreadable
/// directory and malformed files produce an empty/partial list, respectively.
pub async fn list_workflow_snapshots(workflow_runs_dir: Option<&Path>) -> Vec<Value> {
    let Some(directory) = workflow_runs_dir else {
        return Vec::new();
    };
    let mut files = match tokio::fs::read_dir(directory).await {
        Ok(files) => files,
        Err(_) => return Vec::new(),
    };

    let mut snapshots = Vec::new();
    loop {
        let entry = match files.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) | Err(_) => break,
        };
        if !entry.file_name().to_string_lossy().ends_with(".json") {
            continue;
        }
        let Ok(contents) = tokio::fs::read_to_string(entry.path()).await else {
            continue;
        };
        if let Ok(snapshot) = serde_json::from_str::<Value>(&contents) {
            snapshots.push(snapshot);
        }
    }

    // Rust's sort is stable, matching the source's stable Array.sort when its
    // subtraction comparator returns NaN (which JavaScript treats as zero).
    snapshots.sort_by(|left, right| {
        js_sort_number(right.get("startTime").unwrap_or(&Value::Null))
            .partial_cmp(&js_sort_number(
                left.get("startTime").unwrap_or(&Value::Null),
            ))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    snapshots
}

fn js_sort_number(value: &Value) -> f64 {
    match value {
        Value::Null => 0.0,
        Value::Bool(value) => f64::from(u8::from(*value)),
        Value::Number(value) => value.as_f64().unwrap_or(f64::NAN),
        Value::String(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                0.0
            } else {
                trimmed.parse::<f64>().unwrap_or(f64::NAN)
            }
        }
        Value::Array(values) if values.is_empty() => 0.0,
        Value::Array(values) if values.len() == 1 => {
            let as_string = match &values[0] {
                Value::Null => String::new(),
                Value::Bool(value) => value.to_string(),
                Value::Number(value) => value.to_string(),
                Value::String(value) => value.clone(),
                Value::Array(_) | Value::Object(_) => return f64::NAN,
            };
            js_sort_number(&Value::String(as_string))
        }
        Value::Array(_) | Value::Object(_) => f64::NAN,
    }
}

async fn prune_snapshots(directory: &Path) -> Vec<String> {
    let mut entries = match tokio::fs::read_dir(directory).await {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };
    let mut snapshots = Vec::<(OsString, PathBuf, i128)>::new();
    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) | Err(_) => break,
        };
        let name = entry.file_name();
        if !name.to_string_lossy().ends_with(".json") {
            continue;
        }
        let mtime = match tokio::fs::metadata(entry.path()).await {
            Ok(metadata) => metadata.modified().map(system_time_millis).unwrap_or(0),
            Err(_) => 0,
        };
        snapshots.push((name, entry.path(), mtime));
    }

    if snapshots.len() <= MAX_RETAINED_SNAPSHOTS {
        return Vec::new();
    }
    // `sort_by_key` is stable: equal-mtime snapshots keep the filesystem's
    // enumeration order, as the source's stable JavaScript sort does.
    snapshots.sort_by_key(|(_, _, mtime)| *mtime);
    let overflow = snapshots.len() - MAX_RETAINED_SNAPSHOTS;
    let mut warnings = Vec::new();
    for (file_name, path, _) in snapshots.into_iter().take(overflow) {
        let display_name = file_name.to_string_lossy();
        if let Err(error) = tokio::fs::remove_file(&path).await {
            warnings.push(format!("prune unlink failed for {display_name}: {error}"));
        }

        let Some(run_id) = display_name.strip_suffix(".json") else {
            continue;
        };
        if is_generated_run_id(run_id) {
            let run_dir = directory.join(run_id);
            if let Err(error) = remove_any_force(&run_dir).await {
                warnings.push(format!("prune journal dir failed for {run_id}: {error}"));
            }
        }
    }
    warnings
}

fn system_time_millis(time: SystemTime) -> i128 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i128,
        Err(error) => -(error.duration().as_millis() as i128),
    }
}

/// This guard mirrors `/^wf_[0-9a-f]+$/` in the TypeScript pruner. A filename
/// can remove a sibling journal tree only when its stem is a generated run ID.
fn is_generated_run_id(run_id: &str) -> bool {
    run_id.strip_prefix("wf_").is_some_and(|hex| {
        !hex.is_empty()
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

async fn remove_any_force(path: &Path) -> std::io::Result<()> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.file_type().is_dir() {
        tokio::fs::remove_dir_all(path).await
    } else {
        tokio::fs::remove_file(path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs::{self, FileTimes, OpenOptions};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let nonce = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "canopy-workflow-snapshot-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn source() -> WorkflowSnapshotSource {
        WorkflowSnapshotSource {
            run_id: "wf_a".to_owned(),
            meta: Some(WorkflowMeta {
                name: "demo".to_owned(),
                description: "d".to_owned(),
                when_to_use: None,
                phases: None,
            }),
            status: WorkflowStatus::Completed,
            script: Some("return 1;".to_owned()),
            script_path: None,
            phases: vec!["Plan".to_owned(), "Build".to_owned()],
            agents_dispatched: 3,
            agents_completed: 3,
            tokens_spent: 450,
            token_budget_total: Some(1000),
            per_phase_tokens: vec![(Some("Plan".to_owned()), 200), (None, 50)],
            recent_logs: vec!["log1".to_owned()],
            start_time: 1_700_000_000_000,
            end_time: Some(1_700_000_005_000),
            result: Some(WorkflowSnapshotResult::Json(json!({"answer": 42}))),
            error: None,
        }
    }

    fn set_mtime(path: &Path, seconds: u64) {
        let file = OpenOptions::new().write(true).open(path).unwrap();
        file.set_times(FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(seconds)))
            .unwrap();
    }

    #[test]
    fn rejects_active_workflow_statuses_and_accepts_only_terminal_states() {
        for status in [
            WorkflowStatus::Running,
            WorkflowStatus::Pausing,
            WorkflowStatus::Paused,
        ] {
            let mut task = source();
            task.status = status;
            assert_eq!(
                to_snapshot(&task).unwrap_err().to_string(),
                "Cannot snapshot active workflow wf_a."
            );
        }
        for (status, expected) in [
            (WorkflowStatus::Completed, WorkflowTerminalStatus::Completed),
            (WorkflowStatus::Failed, WorkflowTerminalStatus::Failed),
            (WorkflowStatus::Cancelled, WorkflowTerminalStatus::Cancelled),
        ] {
            let mut task = source();
            task.status = status;
            assert_eq!(to_snapshot(&task).unwrap().status, expected);
        }
    }

    #[test]
    fn serializes_exact_snapshot_fields_nulls_omissions_and_phase_pairs() {
        let mut task = source();
        task.meta = None;
        task.script = None;
        task.script_path = None;
        task.token_budget_total = None;
        task.end_time = None;
        task.result = None;
        task.error = None;
        let actual = serde_json::to_value(to_snapshot(&task).unwrap()).unwrap();
        assert_eq!(
            actual,
            json!({
                "runId": "wf_a",
                "meta": null,
                "status": "completed",
                "script": "",
                "phases": ["Plan", "Build"],
                "agentsDispatched": 3,
                "agentsCompleted": 3,
                "tokensSpent": 450,
                "tokenBudgetTotal": null,
                "perPhaseTokens": [["Plan", 200], [null, 50]],
                "recentLogs": ["log1"],
                "startTime": 1_700_000_000_000_i64
            })
        );
    }

    #[test]
    fn explicit_null_result_is_kept_and_optional_meta_fields_are_omitted() {
        let mut task = source();
        task.result = Some(WorkflowSnapshotResult::Json(Value::Null));
        task.meta.as_mut().unwrap().when_to_use = None;
        task.meta.as_mut().unwrap().phases = Some(vec![WorkflowMetaPhase {
            title: "Plan".to_owned(),
            detail: None,
            model: Some("fast".to_owned()),
        }]);
        let actual = serde_json::to_value(to_snapshot(&task).unwrap()).unwrap();
        assert_eq!(actual["result"], Value::Null);
        assert!(actual["meta"].get("whenToUse").is_none());
        assert!(actual["meta"]["phases"][0].get("detail").is_none());
        assert_eq!(actual["meta"]["phases"][0]["model"], "fast");
        assert!(actual.get("scriptPath").is_none());
        assert!(actual.get("error").is_none());
    }

    #[test]
    fn replaces_non_json_result_with_the_source_placeholder() {
        let mut task = source();
        task.result = Some(WorkflowSnapshotResult::NonJsonSerializable {
            js_type: "bigint".to_owned(),
        });
        let actual = serde_json::to_value(to_snapshot(&task).unwrap()).unwrap();
        assert_eq!(actual["result"], "(non-JSON-serializable bigint)");
    }

    #[test]
    fn to_snapshot_clones_mutable_arrays() {
        let task = source();
        let snapshot = to_snapshot(&task).unwrap();
        assert_eq!(snapshot.phases, vec!["Plan".to_owned(), "Build".to_owned()]);
        assert_eq!(
            snapshot.per_phase_tokens,
            vec![(Some("Plan".to_owned()), 200), (None, 50)]
        );
    }

    #[tokio::test]
    async fn write_and_list_round_trip_newest_first_and_skip_corrupt_files() {
        let temp = TempDir::new();
        let directory = temp.path().join("workflows");
        let mut old = source();
        old.run_id = "wf_old".to_owned();
        old.start_time = 1000;
        assert!(
            write_workflow_snapshot(Some(&directory), &old)
                .await
                .written
        );
        let mut newest = source();
        newest.run_id = "wf_new".to_owned();
        newest.start_time = 9000;
        assert!(
            write_workflow_snapshot(Some(&directory), &newest)
                .await
                .written
        );
        tokio::fs::write(directory.join("broken.json"), b"{ broken")
            .await
            .unwrap();

        let snapshots = list_workflow_snapshots(Some(&directory)).await;
        assert_eq!(
            snapshots
                .iter()
                .map(|item| item["runId"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["wf_new", "wf_old"]
        );
        assert_eq!(
            snapshots[0]["perPhaseTokens"],
            json!([["Plan", 200], [null, 50]])
        );
        assert!(list_workflow_snapshots(None).await.is_empty());
    }

    #[tokio::test]
    async fn listing_retains_valid_json_without_the_snapshot_schema() {
        let temp = TempDir::new();
        tokio::fs::write(
            temp.path().join("partial.json"),
            br#"{"startTime":500,"extra":true}"#,
        )
        .await
        .unwrap();
        tokio::fs::write(
            temp.path().join("complete.json"),
            br#"{"runId":"wf_new","startTime":900}"#,
        )
        .await
        .unwrap();

        let snapshots = list_workflow_snapshots(Some(temp.path())).await;

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0]["runId"], "wf_new");
        assert_eq!(snapshots[1]["extra"], true);
    }

    #[tokio::test]
    async fn write_is_best_effort_for_active_entries_and_io_failures() {
        let temp = TempDir::new();
        let mut active = source();
        active.status = WorkflowStatus::Running;
        let active_report = write_workflow_snapshot(Some(temp.path()), &active).await;
        assert!(!active_report.written);
        assert!(active_report.warnings[0].contains("Cannot snapshot active workflow wf_a."));

        let file_path = temp.path().join("not-a-directory");
        tokio::fs::write(&file_path, b"file").await.unwrap();
        let failed_report = write_workflow_snapshot(Some(&file_path), &source()).await;
        assert!(!failed_report.written);
        assert_eq!(failed_report.warnings.len(), 1);
    }

    #[tokio::test]
    async fn prunes_oldest_by_mtime_and_removes_only_generated_run_journals() {
        let temp = TempDir::new();
        let directory = temp.path();
        for index in 0..(MAX_RETAINED_SNAPSHOTS + 4) {
            let run_id = format!("wf_{index:x}");
            let snapshot_path = directory.join(format!("{run_id}.json"));
            tokio::fs::write(
                &snapshot_path,
                serde_json::to_vec(&to_snapshot(&source()).unwrap()).unwrap(),
            )
            .await
            .unwrap();
            set_mtime(&snapshot_path, 100 + index as u64);
            tokio::fs::create_dir_all(directory.join(&run_id))
                .await
                .unwrap();
            tokio::fs::write(directory.join(&run_id).join("journal.jsonl"), b"{}\n")
                .await
                .unwrap();
        }

        let mut newest = source();
        newest.run_id = "wf_ff".to_owned();
        // Terminal workflows have a sibling resume-journal directory even
        // though the snapshot writer only writes the JSON summary.
        tokio::fs::create_dir_all(directory.join(&newest.run_id))
            .await
            .unwrap();
        let report = write_workflow_snapshot(Some(directory), &newest).await;
        assert!(report.written);
        let entries = tokio::fs::read_dir(directory).await.unwrap();
        let mut snapshot_count = 0;
        let mut journal_count = 0;
        let mut files = entries;
        while let Some(entry) = files.next_entry().await.unwrap() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".json") {
                snapshot_count += 1;
            } else if name.starts_with("wf_") {
                journal_count += 1;
            }
        }
        assert_eq!(snapshot_count, MAX_RETAINED_SNAPSHOTS);
        assert_eq!(journal_count, MAX_RETAINED_SNAPSHOTS);
        assert!(!directory.join("wf_0").exists());
        assert!(!directory.join("wf_4").exists());
        assert!(directory.join("wf_5").exists());
    }

    #[tokio::test]
    async fn pruning_never_recursively_deletes_from_crafted_snapshot_names() {
        let temp = TempDir::new();
        let directory = temp.path().join("workflows");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let canary = temp.path().join("CANARY.txt");
        tokio::fs::write(&canary, b"keep").await.unwrap();
        let sibling = directory.join("notarun");
        tokio::fs::create_dir_all(&sibling).await.unwrap();
        let sibling_canary = sibling.join("keep.txt");
        tokio::fs::write(&sibling_canary, b"keep").await.unwrap();

        for index in 0..MAX_RETAINED_SNAPSHOTS {
            let path = directory.join(format!("wf_{index:x}.json"));
            tokio::fs::write(&path, b"{}").await.unwrap();
            set_mtime(&path, 100 + index as u64);
        }
        for name in ["...json", "notarun.json"] {
            let path = directory.join(name);
            tokio::fs::write(&path, b"{}").await.unwrap();
            set_mtime(&path, 1);
        }
        let mut newest = source();
        newest.run_id = "wf_ff".to_owned();
        assert!(
            write_workflow_snapshot(Some(&directory), &newest)
                .await
                .written
        );

        assert!(canary.exists());
        assert!(sibling_canary.exists());
    }

    #[test]
    fn generated_run_directory_guard_matches_lowercase_nonempty_hex_ids() {
        assert!(is_generated_run_id("wf_0"));
        assert!(is_generated_run_id("wf_1234abcd"));
        for invalid in ["wf_", "wf_ABC", "wf_g", "..", "notarun", "wf_../x"] {
            assert!(
                !is_generated_run_id(invalid),
                "unexpectedly accepted {invalid}"
            );
        }
    }
}
