//! Durable per-session usage summaries and dashboard aggregation.
//!
//! Ports `packages/core/src/services/usageHistoryService.ts`. Paths and the
//! current time are passed in by callers so this module is independent of the
//! TypeScript Config singleton and straightforward to test.

use chrono::{DateTime, Days, Local, NaiveDateTime, TimeZone, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::Path;

const MS_PER_DAY: i64 = 24 * 60 * 60 * 1000;
pub const LIVE_REBUILD_WINDOW_DAYS: i64 = 35;

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelUsage {
    pub requests: f64,
    pub input_tokens: f64,
    pub output_tokens: f64,
    pub cached_tokens: f64,
    pub thoughts_tokens: f64,
    pub total_tokens: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_latency_ms: Option<f64>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolUsage {
    pub count: f64,
    pub success: f64,
    pub fail: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_duration_ms: Option<f64>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct SkillUsage {
    pub count: f64,
    pub success: f64,
    pub fail: f64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolTotals {
    pub total_calls: f64,
    pub total_success: f64,
    pub total_fail: f64,
    pub by_name: IndexMap<String, ToolUsage>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillTotals {
    pub total_calls: f64,
    pub total_success: f64,
    pub total_fail: f64,
    pub by_name: IndexMap<String, SkillUsage>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileTotals {
    pub lines_added: f64,
    pub lines_removed: f64,
}

/// Persisted JSONL schema. Optional fields are omitted, matching JS JSON
/// serialization of `undefined` (notably older records without skills).
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageSummaryRecord {
    pub version: u8,
    pub session_id: String,
    pub timestamp: i64,
    pub start_time: i64,
    pub project: String,
    pub duration_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_latency_ms: Option<f64>,
    pub models: IndexMap<String, ModelUsage>,
    pub tools: ToolTotals,
    pub files: FileTotals,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skills: Option<SkillTotals>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ModelMetrics {
    pub api: ApiMetrics,
    pub tokens: TokenMetrics,
    #[serde(default)]
    pub by_source: IndexMap<String, SourceMetrics>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ApiMetrics {
    pub total_requests: f64,
    pub total_errors: f64,
    pub total_latency_ms: f64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TokenMetrics {
    #[serde(rename = "prompt")]
    pub prompt_tokens: f64,
    pub candidates: f64,
    #[serde(rename = "total")]
    pub total_tokens: f64,
    #[serde(rename = "cached")]
    pub cached_tokens: f64,
    #[serde(rename = "thoughts")]
    pub thoughts_tokens: f64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SourceMetrics {
    pub api: ApiMetrics,
    pub tokens: TokenMetrics,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolMetric {
    pub count: f64,
    pub success: f64,
    pub fail: f64,
    pub duration_ms: f64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SkillMetric {
    pub count: f64,
    pub success: f64,
    pub fail: f64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SessionMetrics {
    pub models: IndexMap<String, ModelMetrics>,
    pub tools: ToolMetricCollection,
    pub files: FileMetricTotals,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skills: Option<SkillMetricCollection>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolMetricCollection {
    pub total_calls: f64,
    pub total_success: f64,
    pub total_fail: f64,
    #[serde(default)]
    pub total_duration_ms: f64,
    pub by_name: IndexMap<String, ToolMetric>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SkillMetricCollection {
    pub total_calls: f64,
    pub total_success: f64,
    pub total_fail: f64,
    pub by_name: IndexMap<String, SkillMetric>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FileMetricTotals {
    pub total_lines_added: f64,
    pub total_lines_removed: f64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeRange {
    #[default]
    Today,
    Week,
    Month,
    All,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregatedModelUsage {
    pub requests: f64,
    pub input_tokens: f64,
    pub output_tokens: f64,
    pub cached_tokens: f64,
    pub thoughts_tokens: f64,
    pub total_tokens: f64,
    pub total_latency_ms: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregatedTool {
    pub name: String,
    pub count: f64,
    pub success: f64,
    pub fail: f64,
    pub total_duration_ms: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregatedSkill {
    pub name: String,
    pub count: f64,
    pub success: f64,
    pub fail: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectUsage {
    pub path: String,
    pub session_count: usize,
    pub total_duration_ms: i64,
    pub total_tokens: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregatedTools {
    pub total_calls: f64,
    pub total_success: f64,
    pub total_fail: f64,
    pub top_tools: Vec<AggregatedTool>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregatedFiles {
    pub lines_added: f64,
    pub lines_removed: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregatedSkills {
    pub total_calls: f64,
    pub top_skills: Vec<AggregatedSkill>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageDashboardTotals {
    pub total_tokens: f64,
    pub input_tokens: f64,
    pub output_tokens: f64,
    pub cached_tokens: f64,
    pub thoughts_tokens: f64,
    pub requests: f64,
    pub sessions: usize,
    pub tool_calls: f64,
    pub lines_added: f64,
    pub lines_removed: f64,
    pub cache_read_rate: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageModelShare {
    pub model: String,
    pub total_tokens: f64,
    pub cache_read_rate: f64,
    pub share: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageSkillCall {
    pub name: String,
    pub count: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageDailyPoint {
    pub date: String,
    pub tokens: f64,
    pub sessions: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageHeatmapDay {
    pub tokens: f64,
    pub cache_read_rate: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageDashboard {
    pub generated_at: String,
    pub range: TimeRange,
    pub summary: UsageDashboardTotals,
    pub models: Vec<UsageModelShare>,
    pub skills: Vec<UsageSkillCall>,
    pub daily: Vec<UsageDailyPoint>,
    pub heatmap: IndexMap<String, UsageHeatmapDay>,
    pub heatmap_days: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UsageDashboardOptions {
    pub range: Option<TimeRange>,
    /// Mirrors `Math.floor` in the TypeScript dashboard. Non-positive and
    /// absent values select the 183-day default; a positive fraction may floor
    /// to zero, matching the source.
    pub heatmap_days: Option<f64>,
}

const DEFAULT_HEATMAP_DAYS: usize = 183;
const MAX_DAILY_DAYS: i64 = 92;

fn serialize_js_date<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&date.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregatedReport {
    pub time_range: TimeRange,
    #[serde(serialize_with = "serialize_js_date")]
    pub period_start: DateTime<Utc>,
    #[serde(serialize_with = "serialize_js_date")]
    pub period_end: DateTime<Utc>,
    pub session_count: usize,
    pub total_duration_ms: i64,
    pub total_latency_ms: f64,
    pub total_requests: f64,
    pub models: IndexMap<String, AggregatedModelUsage>,
    pub tools: AggregatedTools,
    pub files: AggregatedFiles,
    pub skills: AggregatedSkills,
    pub projects: Vec<ProjectUsage>,
}

#[derive(Clone, Debug, Default)]
pub struct LoadOptions {
    pub persist_rebuild: Option<bool>,
}

#[derive(Clone, Debug, Default)]
pub struct LiveLoadOptions {
    /// Transcript mtime lower bound. `None` selects the 35-day incremental
    /// window when a persisted base exists, or a complete replay otherwise.
    pub since_ms: Option<i64>,
}

/// Convert live session metrics to the durable usage record schema.
pub fn metrics_to_usage_record(
    session_id: impl Into<String>,
    project: impl Into<String>,
    start_time: i64,
    end_time: i64,
    metrics: &SessionMetrics,
) -> UsageSummaryRecord {
    let mut total_latency_ms = 0.0;
    let models = metrics
        .models
        .iter()
        .map(|(name, metric)| {
            total_latency_ms += metric.api.total_latency_ms;
            (
                name.clone(),
                ModelUsage {
                    requests: metric.api.total_requests,
                    input_tokens: metric.tokens.prompt_tokens,
                    output_tokens: metric.tokens.candidates,
                    cached_tokens: metric.tokens.cached_tokens,
                    thoughts_tokens: metric.tokens.thoughts_tokens,
                    total_tokens: js_number_or(
                        metric.tokens.total_tokens,
                        metric.tokens.prompt_tokens
                            + metric.tokens.candidates
                            + metric.tokens.thoughts_tokens,
                    ),
                    total_latency_ms: Some(metric.api.total_latency_ms),
                },
            )
        })
        .collect();
    let tools = ToolTotals {
        total_calls: metrics.tools.total_calls,
        total_success: metrics.tools.total_success,
        total_fail: metrics.tools.total_fail,
        by_name: metrics
            .tools
            .by_name
            .iter()
            .map(|(name, stats)| {
                (
                    name.clone(),
                    ToolUsage {
                        count: stats.count,
                        success: stats.success,
                        fail: stats.fail,
                        total_duration_ms: Some(stats.duration_ms),
                    },
                )
            })
            .collect(),
    };
    let skills = metrics.skills.as_ref().map(|skills| SkillTotals {
        total_calls: skills.total_calls,
        total_success: skills.total_success,
        total_fail: skills.total_fail,
        by_name: skills
            .by_name
            .iter()
            .map(|(name, stats)| {
                (
                    name.clone(),
                    SkillUsage {
                        count: stats.count,
                        success: stats.success,
                        fail: stats.fail,
                    },
                )
            })
            .collect(),
    });

    UsageSummaryRecord {
        version: 1,
        session_id: session_id.into(),
        timestamp: end_time,
        start_time,
        project: project.into(),
        duration_ms: end_time - start_time,
        total_latency_ms: Some(total_latency_ms),
        models,
        tools,
        files: FileTotals {
            lines_added: metrics.files.total_lines_added,
            lines_removed: metrics.files.total_lines_removed,
        },
        skills,
    }
}

/// Append a finalized session summary to `{root}/usage_record.jsonl`.
pub fn persist_session_usage(
    root: impl AsRef<Path>,
    session_id: impl Into<String>,
    project: impl Into<String>,
    start_time: i64,
    end_time: i64,
    metrics: &SessionMetrics,
) -> io::Result<()> {
    let record = metrics_to_usage_record(session_id, project, start_time, end_time, metrics);
    crate::jsonl::write_line_sync(root.as_ref().join("usage_record.jsonl"), &record)
}

/// Load stored summaries, migrating transcript history if no valid v1 record
/// is present. Read-side deduplication is last-wins by session id.
pub fn load_usage_history(
    root: impl AsRef<Path>,
    skip_session_in_rebuild: Option<&str>,
    options: LoadOptions,
) -> io::Result<Vec<UsageSummaryRecord>> {
    let root = root.as_ref();
    let stored = read_valid_history(&root.join("usage_record.jsonl"));
    if !stored.is_empty() {
        return Ok(dedup_by_session_id(stored));
    }
    let rebuilt = rebuild_from_session_jsonl(
        root,
        RebuildOptions {
            skip_session_in_rebuild: skip_session_in_rebuild.map(str::to_owned),
            persist: options.persist_rebuild.unwrap_or(true),
            ..RebuildOptions::default()
        },
    )?;
    Ok(dedup_by_session_id(rebuilt))
}

/// Merge recent never-persisted transcript usage with durable history. The
/// persisted row is authoritative if both sources contain a session id.
pub fn load_usage_history_with_live(
    root: impl AsRef<Path>,
    now_ms: i64,
    options: LiveLoadOptions,
) -> Vec<UsageSummaryRecord> {
    let root = root.as_ref();
    let persisted = read_valid_history(&root.join("usage_record.jsonl"));
    let persisted_ids = persisted
        .iter()
        .map(|record| record.session_id.clone())
        .collect::<HashSet<_>>();
    let since_ms = options.since_ms.or_else(|| {
        (!persisted_ids.is_empty()).then_some(now_ms - LIVE_REBUILD_WINDOW_DAYS * MS_PER_DAY)
    });
    let rebuilt = rebuild_from_session_jsonl(
        root,
        RebuildOptions {
            persist: false,
            since_ms,
            skip_session_ids: persisted_ids,
            ..RebuildOptions::default()
        },
    )
    .unwrap_or_default();
    let mut both = rebuilt;
    both.extend(persisted);
    dedup_by_session_id(both)
}

/// Salvage telemetry before deleting a transcript. Failures are deliberately
/// non-fatal to deletion; an unreadable usage file permits append, while an
/// already-present session is left untouched.
pub fn persist_usage_before_transcript_deletion(
    root: impl AsRef<Path>,
    transcript_path: impl AsRef<Path>,
) -> bool {
    let root = root.as_ref();
    let result = (|| -> io::Result<bool> {
        let transcript = crate::jsonl::read(transcript_path)?;
        let Some(summarized) = summarize_transcript(&transcript) else {
            return Ok(false);
        };
        let Some(record) = summarized.record else {
            return Ok(false);
        };
        let usage_path = root.join("usage_record.jsonl");
        if fs::metadata(&usage_path).is_ok()
            && let Ok(existing) = crate::jsonl::read(&usage_path)
            && existing.iter().any(|value| {
                value.get("sessionId").and_then(Value::as_str)
                    == Some(summarized.session_id.as_str())
            })
        {
            return Ok(false);
        }
        crate::jsonl::write_line_sync(usage_path, &record)?;
        Ok(true)
    })();
    result.unwrap_or(false)
}

#[derive(Default)]
struct RebuildOptions {
    skip_session_in_rebuild: Option<String>,
    persist: bool,
    since_ms: Option<i64>,
    skip_session_ids: HashSet<String>,
}

fn rebuild_from_session_jsonl(
    root: &Path,
    options: RebuildOptions,
) -> io::Result<Vec<UsageSummaryRecord>> {
    let projects_dir = root.join("projects");
    let Ok(project_dirs) = fs::read_dir(projects_dir) else {
        return Ok(Vec::new());
    };
    let mut results = Vec::new();
    let mut seen = HashSet::new();
    for project_entry in project_dirs.flatten() {
        let chats_dir = project_entry.path().join("chats");
        let Ok(files) = fs::read_dir(chats_dir) else {
            continue;
        };
        for file in files.flatten() {
            let file_path = file.path();
            let file_name = file.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            if !file_name.ends_with(".jsonl") {
                continue;
            }
            if let Some(since) = options.since_ms {
                let Ok(metadata) = fs::metadata(&file_path) else {
                    continue;
                };
                let Ok(modified) = metadata.modified() else {
                    continue;
                };
                let mtime_ms = system_time_millis(modified);
                if mtime_ms < since as f64 {
                    continue;
                }
            }
            if !options.skip_session_ids.is_empty() {
                let session_id = file_name.strip_suffix(".jsonl").unwrap_or(file_name);
                if options.skip_session_ids.contains(session_id) {
                    continue;
                }
            }
            let Ok(records) = crate::jsonl::read(&file_path) else {
                continue;
            };
            let Some(summary) = summarize_transcript(&records) else {
                continue;
            };
            if !seen.insert(summary.session_id.clone()) {
                continue;
            }
            if let Some(record) = summary.record {
                results.push(record);
            }
        }
    }
    if options.persist {
        let usage_path = root.join("usage_record.jsonl");
        for record in &results {
            if options.skip_session_in_rebuild.as_deref() == Some(record.session_id.as_str()) {
                continue;
            }
            crate::jsonl::write_line_sync(&usage_path, record)?;
        }
    }
    Ok(results)
}

fn system_time_millis(time: std::time::SystemTime) -> f64 {
    match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs_f64() * 1000.0,
        Err(error) => -(error.duration().as_secs_f64() * 1000.0),
    }
}

struct TranscriptSummary {
    session_id: String,
    record: Option<UsageSummaryRecord>,
}

fn summarize_transcript(records: &[Value]) -> Option<TranscriptSummary> {
    let first = records.first()?;
    let session_id = first.get("sessionId")?.as_str()?.to_owned();
    if session_id.is_empty() {
        return None;
    }
    let project = first
        .get("cwd")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    // UiTelemetryService initializes the skill bucket even when no skills are
    // invoked, so transcript-derived v1 records include an empty skills field.
    let mut metrics = SessionMetrics {
        skills: Some(SkillMetricCollection::default()),
        ..SessionMetrics::default()
    };
    let mut has_events = false;
    for record in records {
        if record.get("type").and_then(Value::as_str) != Some("system")
            || record.get("subtype").and_then(Value::as_str) != Some("ui_telemetry")
        {
            continue;
        }
        let Some(event) = record
            .get("systemPayload")
            .and_then(|payload| payload.get("uiEvent"))
        else {
            continue;
        };
        if !js_truthy(event) {
            continue;
        }
        has_events = true;
        if event.is_object() {
            let _ = accumulate_ui_event(&mut metrics, event);
        }
    }
    if !has_events {
        return Some(TranscriptSummary {
            session_id,
            record: None,
        });
    }
    let Some(start_time) = records.first().and_then(parse_record_timestamp) else {
        return Some(TranscriptSummary {
            session_id,
            record: None,
        });
    };
    let Some(end_time) = records.last().and_then(parse_record_timestamp) else {
        return Some(TranscriptSummary {
            session_id,
            record: None,
        });
    };
    Some(TranscriptSummary {
        session_id: session_id.clone(),
        record: Some(metrics_to_usage_record(
            session_id, project, start_time, end_time, &metrics,
        )),
    })
}

fn parse_record_timestamp(record: &Value) -> Option<i64> {
    let timestamp = record.get("timestamp")?;
    if let Some(timestamp) = timestamp.as_f64() {
        if timestamp.is_finite() && timestamp.abs() <= 8.64e15 {
            // ECMAScript TimeClip truncates fractional milliseconds toward 0.
            return Some(timestamp.trunc() as i64);
        }
    }
    parse_timestamp(timestamp.as_str()?)
}

fn parse_timestamp(timestamp: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(timestamp)
        .ok()
        .map(|value| value.timestamp_millis())
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(timestamp, "%Y-%m-%d")
                .ok()
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map(|value| value.and_utc().timestamp_millis())
        })
        .or_else(|| {
            let local = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H:%M:%S%.f").ok()?;
            match Local.from_local_datetime(&local) {
                chrono::LocalResult::Single(value) => Some(value.timestamp_millis()),
                chrono::LocalResult::Ambiguous(first, second) => {
                    Some(first.min(second).timestamp_millis())
                }
                chrono::LocalResult::None => (1..=240).find_map(|minutes| {
                    let candidate = local + chrono::Duration::minutes(minutes);
                    Local
                        .from_local_datetime(&candidate)
                        .earliest()
                        .map(|value| value.timestamp_millis())
                }),
            }
        })
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn number(event: &Value, key: &str) -> f64 {
    event.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

fn accumulate_ui_event(metrics: &mut SessionMetrics, event: &Value) -> bool {
    let Some(name) = event.get("event.name").and_then(Value::as_str) else {
        return false;
    };
    match name {
        "canopy-code.api_response" | "canopy-code.api_error" => {
            let model_name = event
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let model = metrics.models.entry(model_name).or_default();
            model.api.total_requests += 1.0;
            model.api.total_latency_ms += number(event, "duration_ms");
            if name == "canopy-code.api_response" {
                model.tokens.prompt_tokens += number(event, "input_token_count");
                model.tokens.candidates += number(event, "output_token_count");
                model.tokens.total_tokens += number(event, "total_token_count");
                model.tokens.cached_tokens += number(event, "cached_content_token_count");
                model.tokens.thoughts_tokens += number(event, "thoughts_token_count");
            } else {
                model.api.total_errors += 1.0;
            }
            true
        }
        "canopy-code.tool_call" => {
            let tools = &mut metrics.tools;
            let name = event
                .get("function_name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let success = event
                .get("success")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let duration = number(event, "duration_ms");
            tools.total_calls += 1.0;
            tools.total_duration_ms += duration;
            tools.total_success += if success { 1.0 } else { 0.0 };
            tools.total_fail += if success { 0.0 } else { 1.0 };
            let stats = tools.by_name.entry(name).or_default();
            stats.count += 1.0;
            stats.success += if success { 1.0 } else { 0.0 };
            stats.fail += if success { 0.0 } else { 1.0 };
            stats.duration_ms += duration;
            if let Some(metadata) = event.get("metadata") {
                metrics.files.total_lines_added += metadata
                    .get("model_added_lines")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
                metrics.files.total_lines_removed += metadata
                    .get("model_removed_lines")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
            }
            true
        }
        _ => false,
    }
}

fn read_valid_history(path: &Path) -> Vec<UsageSummaryRecord> {
    crate::jsonl::read(path)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| serde_json::from_value(value).ok())
        .filter(|record: &UsageSummaryRecord| record.version == 1)
        .collect()
}

fn dedup_by_session_id(records: Vec<UsageSummaryRecord>) -> Vec<UsageSummaryRecord> {
    let mut indices = HashMap::<String, usize>::new();
    let mut deduped = Vec::<UsageSummaryRecord>::new();
    for record in records {
        if let Some(index) = indices.get(&record.session_id).copied() {
            deduped[index] = record;
        } else {
            indices.insert(record.session_id.clone(), deduped.len());
            deduped.push(record);
        }
    }
    deduped
}

/// Aggregate records in the selected range. `now_ms` injection gives callers
/// stable calendar boundaries in tests and deterministic dashboard snapshots.
pub fn aggregate_usage(
    records: &[UsageSummaryRecord],
    range: TimeRange,
    now_ms: i64,
) -> AggregatedReport {
    let (start_ms, end_ms) = get_time_range_bounds(range, now_ms);
    let filtered = records
        .iter()
        .filter(|record| record.timestamp >= start_ms && record.timestamp <= end_ms)
        .collect::<Vec<_>>();

    let mut models = IndexMap::<String, AggregatedModelUsage>::new();
    let mut tool_counts = IndexMap::<String, AggregatedTool>::new();
    let mut skill_counts = IndexMap::<String, AggregatedSkill>::new();
    let mut projects = IndexMap::<String, ProjectUsage>::new();
    let (mut total_duration_ms, mut total_latency_ms, mut total_requests) = (0, 0.0, 0.0);
    let (mut total_tool_calls, mut total_tool_success, mut total_tool_fail) = (0.0, 0.0, 0.0);
    let (mut lines_added, mut lines_removed, mut total_skill_calls) = (0.0, 0.0, 0.0);

    for record in &filtered {
        total_duration_ms += record.duration_ms;
        total_latency_ms += record.total_latency_ms.unwrap_or(0.0);
        total_tool_calls += record.tools.total_calls;
        total_tool_success += record.tools.total_success;
        total_tool_fail += record.tools.total_fail;
        lines_added += record.files.lines_added;
        lines_removed += record.files.lines_removed;
        let mut session_tokens = 0.0;
        for (name, model) in &record.models {
            total_requests += model.requests;
            session_tokens += model.total_tokens;
            let aggregated = models
                .entry(name.clone())
                .or_insert_with(|| AggregatedModelUsage {
                    requests: 0.0,
                    input_tokens: 0.0,
                    output_tokens: 0.0,
                    cached_tokens: 0.0,
                    thoughts_tokens: 0.0,
                    total_tokens: 0.0,
                    total_latency_ms: 0.0,
                });
            aggregated.requests += model.requests;
            aggregated.input_tokens += model.input_tokens;
            aggregated.output_tokens += model.output_tokens;
            aggregated.cached_tokens += model.cached_tokens;
            aggregated.thoughts_tokens += model.thoughts_tokens;
            aggregated.total_tokens += model.total_tokens;
            aggregated.total_latency_ms += model.total_latency_ms.unwrap_or(0.0);
        }
        for (name, tool) in &record.tools.by_name {
            let aggregated = tool_counts
                .entry(name.clone())
                .or_insert_with(|| AggregatedTool {
                    name: name.clone(),
                    ..AggregatedTool::default()
                });
            aggregated.count += tool.count;
            aggregated.success += tool.success;
            aggregated.fail += tool.fail;
            aggregated.total_duration_ms += tool.total_duration_ms.unwrap_or(0.0);
        }
        if let Some(skills) = &record.skills {
            total_skill_calls += skills.total_calls;
            for (name, skill) in &skills.by_name {
                let aggregated =
                    skill_counts
                        .entry(name.clone())
                        .or_insert_with(|| AggregatedSkill {
                            name: name.clone(),
                            ..AggregatedSkill::default()
                        });
                aggregated.count += skill.count;
                aggregated.success += skill.success;
                aggregated.fail += skill.fail;
            }
        }
        let project = projects
            .entry(record.project.clone())
            .or_insert_with(|| ProjectUsage {
                path: record.project.clone(),
                ..ProjectUsage::default()
            });
        project.session_count += 1;
        project.total_duration_ms += record.duration_ms;
        project.total_tokens += session_tokens;
    }

    let mut top_tools = tool_counts.into_values().collect::<Vec<_>>();
    top_tools.sort_by(|a, b| {
        b.count
            .partial_cmp(&a.count)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    top_tools.truncate(10);
    let mut top_skills = skill_counts.into_values().collect::<Vec<_>>();
    top_skills.sort_by(|a, b| {
        b.count
            .partial_cmp(&a.count)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    top_skills.truncate(25);
    let mut projects = projects.into_values().collect::<Vec<_>>();
    projects.sort_by(|a, b| {
        b.total_tokens
            .partial_cmp(&a.total_tokens)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    AggregatedReport {
        time_range: range,
        period_start: Utc
            .timestamp_millis_opt(start_ms)
            .single()
            .unwrap_or_else(Utc::now),
        period_end: Utc
            .timestamp_millis_opt(end_ms)
            .single()
            .unwrap_or_else(Utc::now),
        session_count: filtered.len(),
        total_duration_ms,
        total_latency_ms,
        total_requests,
        models,
        tools: AggregatedTools {
            total_calls: total_tool_calls,
            total_success: total_tool_success,
            total_fail: total_tool_fail,
            top_tools,
        },
        files: AggregatedFiles {
            lines_added,
            lines_removed,
        },
        skills: AggregatedSkills {
            total_calls: total_skill_calls,
            top_skills,
        },
        projects,
    }
}

/// Fallback token total shared by the dashboard's summary, model shares,
/// daily series, and heatmap. The lower-level usage report deliberately keeps
/// the persisted `totalTokens` value unchanged.
pub fn dashboard_model_tokens(
    total_tokens: f64,
    input_tokens: f64,
    output_tokens: f64,
    thoughts_tokens: f64,
) -> f64 {
    js_number_or(total_tokens, input_tokens + output_tokens + thoughts_tokens)
}

fn js_number_or(value: f64, fallback: f64) -> f64 {
    if value == 0.0 || value.is_nan() {
        fallback
    } else {
        value
    }
}

/// Build the usage dashboard aggregation from an already loaded history.
/// `now_ms` is injected so range windows, generatedAt, and the charts share one
/// coherent instant in callers and tests.
pub fn build_usage_dashboard(
    records: &[UsageSummaryRecord],
    options: UsageDashboardOptions,
    now_ms: i64,
) -> UsageDashboard {
    let range = options.range.unwrap_or(TimeRange::Today);
    let heatmap_days = match options.heatmap_days {
        Some(days) if days > 0.0 => days.floor() as usize,
        _ => DEFAULT_HEATMAP_DAYS,
    };
    let report = aggregate_usage(records, range, now_ms);

    let mut summary = UsageDashboardTotals {
        requests: report.total_requests,
        sessions: report.session_count,
        tool_calls: report.tools.total_calls,
        lines_added: report.files.lines_added,
        lines_removed: report.files.lines_removed,
        ..UsageDashboardTotals::default()
    };
    for model in report.models.values() {
        summary.input_tokens += model.input_tokens;
        summary.output_tokens += model.output_tokens;
        summary.cached_tokens += model.cached_tokens;
        summary.thoughts_tokens += model.thoughts_tokens;
        summary.total_tokens += dashboard_model_tokens(
            model.total_tokens,
            model.input_tokens,
            model.output_tokens,
            model.thoughts_tokens,
        );
    }
    summary.cache_read_rate = if summary.input_tokens > 0.0 {
        summary.cached_tokens / summary.input_tokens
    } else {
        0.0
    };

    let mut models = report
        .models
        .iter()
        .map(|(name, model)| {
            let total_tokens = dashboard_model_tokens(
                model.total_tokens,
                model.input_tokens,
                model.output_tokens,
                model.thoughts_tokens,
            );
            UsageModelShare {
                model: name.clone(),
                total_tokens,
                cache_read_rate: if model.input_tokens > 0.0 {
                    model.cached_tokens / model.input_tokens
                } else {
                    0.0
                },
                share: if summary.total_tokens > 0.0 {
                    total_tokens / summary.total_tokens
                } else {
                    0.0
                },
            }
        })
        .collect::<Vec<_>>();
    models.sort_by(|a, b| {
        b.total_tokens
            .partial_cmp(&a.total_tokens)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let skills = report
        .skills
        .top_skills
        .iter()
        .map(|skill| UsageSkillCall {
            name: skill.name.clone(),
            count: skill.count,
        })
        .collect();

    let (range_start_ms, _) = get_time_range_bounds(range, now_ms);
    let daily_start_ms = range_start_ms.max(now_ms.saturating_sub(MAX_DAILY_DAYS * MS_PER_DAY));
    let daily = build_daily(records, daily_start_ms, now_ms);
    let days_i64 = i64::try_from(heatmap_days).unwrap_or(i64::MAX);
    let span_ms = days_i64.saturating_mul(MS_PER_DAY);
    let heatmap_start_ms = if span_ms == i64::MAX {
        i64::MIN
    } else {
        now_ms.saturating_sub(span_ms)
    };
    let heatmap = build_heatmap(records, heatmap_start_ms, now_ms);

    let generated_at = Utc
        .timestamp_millis_opt(now_ms)
        .single()
        .unwrap_or_else(Utc::now)
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

    UsageDashboard {
        generated_at,
        range,
        summary,
        models,
        skills,
        daily,
        heatmap,
        heatmap_days,
    }
}

/// Load the global durable/live history and build one read-only dashboard
/// snapshot, matching the TypeScript `loadUsageDashboard` wrapper.
pub fn load_usage_dashboard(
    root: impl AsRef<Path>,
    options: UsageDashboardOptions,
    now_ms: i64,
) -> UsageDashboard {
    let records = load_usage_history_with_live(root, now_ms, LiveLoadOptions::default());
    build_usage_dashboard(&records, options, now_ms)
}

fn local_date_key(timestamp_ms: i64) -> String {
    Local
        .timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or_else(Local::now)
        .format("%Y-%m-%d")
        .to_string()
}

fn local_start_of_day(timestamp_ms: i64) -> DateTime<Local> {
    let local = Local
        .timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or_else(Local::now);
    let midnight = local.date_naive().and_hms_opt(0, 0, 0).unwrap();
    match Local.from_local_datetime(&midnight) {
        chrono::LocalResult::Single(value) => value,
        chrono::LocalResult::Ambiguous(first, second) => first.min(second),
        chrono::LocalResult::None => {
            // JavaScript's local Date constructor normalizes a nonexistent
            // local midnight forward to the first valid wall-clock time.
            (1..=240)
                .find_map(|minutes| {
                    let candidate = midnight + chrono::Duration::minutes(minutes);
                    Local.from_local_datetime(&candidate).earliest()
                })
                .unwrap_or(local)
        }
    }
}

fn build_daily(records: &[UsageSummaryRecord], start_ms: i64, end_ms: i64) -> Vec<UsageDailyPoint> {
    let mut tokens_by_day = HashMap::<String, f64>::new();
    let mut sessions_by_day = HashMap::<String, usize>::new();
    for record in records {
        if record.timestamp < start_ms || record.timestamp > end_ms {
            continue;
        }
        let key = local_date_key(record.timestamp);
        let mut tokens = 0.0;
        for model in record.models.values() {
            tokens += dashboard_model_tokens(
                model.total_tokens,
                model.input_tokens,
                model.output_tokens,
                model.thoughts_tokens,
            );
        }
        *tokens_by_day.entry(key.clone()).or_default() += tokens;
        *sessions_by_day.entry(key).or_default() += 1;
    }

    let mut date = local_start_of_day(start_ms).date_naive();
    let last_date = local_start_of_day(end_ms).date_naive();
    let mut daily = Vec::new();
    while date <= last_date {
        let key = date.format("%Y-%m-%d").to_string();
        daily.push(UsageDailyPoint {
            date: key.clone(),
            tokens: tokens_by_day.get(&key).copied().unwrap_or(0.0),
            sessions: sessions_by_day.get(&key).copied().unwrap_or(0),
        });
        let Some(next) = date.checked_add_days(Days::new(1)) else {
            break;
        };
        date = next;
    }
    daily
}

fn build_heatmap(
    records: &[UsageSummaryRecord],
    start_ms: i64,
    end_ms: i64,
) -> IndexMap<String, UsageHeatmapDay> {
    let mut totals = IndexMap::<String, (f64, f64, f64)>::new();
    for record in records {
        if record.timestamp < start_ms || record.timestamp > end_ms {
            continue;
        }
        let key = local_date_key(record.timestamp);
        let totals = totals.entry(key).or_default();
        for model in record.models.values() {
            totals.0 += dashboard_model_tokens(
                model.total_tokens,
                model.input_tokens,
                model.output_tokens,
                model.thoughts_tokens,
            );
            totals.1 += model.input_tokens;
            totals.2 += model.cached_tokens;
        }
    }
    totals
        .into_iter()
        .map(|(date, (tokens, input, cached))| {
            (
                date,
                UsageHeatmapDay {
                    tokens,
                    cache_read_rate: if input > 0.0 { cached / input } else { 0.0 },
                },
            )
        })
        .collect()
}

/// Calendar bounds equivalent to the TypeScript service. Today uses the
/// machine's local midnight; week/month are rolling seven/thirty-day windows.
pub fn get_time_range_bounds(range: TimeRange, now_ms: i64) -> (i64, i64) {
    let start = match range {
        TimeRange::Today => {
            let now = Local
                .timestamp_millis_opt(now_ms)
                .single()
                .unwrap_or_else(Local::now);
            now.date_naive()
                .and_hms_opt(0, 0, 0)
                .and_then(|midnight| Local.from_local_datetime(&midnight).single())
                .map(|midnight| midnight.timestamp_millis())
                .unwrap_or(now_ms)
        }
        TimeRange::Week => now_ms - 7 * MS_PER_DAY,
        TimeRange::Month => now_ms - 30 * MS_PER_DAY,
        TimeRange::All => 0,
    };
    (start, now_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::from_str;
    use serde_json::json;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn sample_record(session_id: &str, timestamp: i64, tokens: f64) -> UsageSummaryRecord {
        UsageSummaryRecord {
            version: 1,
            session_id: session_id.into(),
            timestamp,
            start_time: timestamp - 60_000,
            project: "/project".into(),
            duration_ms: 60_000,
            total_latency_ms: Some(1200.0),
            models: IndexMap::from([(
                "model-a".into(),
                ModelUsage {
                    requests: 1.0,
                    input_tokens: tokens * 0.6,
                    output_tokens: tokens * 0.3,
                    cached_tokens: 0.0,
                    thoughts_tokens: tokens * 0.1,
                    total_tokens: tokens,
                    total_latency_ms: None,
                },
            )]),
            tools: ToolTotals {
                total_calls: 2.0,
                total_success: 1.0,
                total_fail: 1.0,
                by_name: IndexMap::from([(
                    "edit".into(),
                    ToolUsage {
                        count: 2.0,
                        success: 1.0,
                        fail: 1.0,
                        total_duration_ms: Some(800.0),
                    },
                )]),
            },
            files: FileTotals {
                lines_added: 5.0,
                lines_removed: 2.0,
            },
            skills: None,
        }
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("canopy-usage-{test_name}-{nonce}"));
        fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn metrics_conversion_sums_latency_and_falls_back_to_token_parts() {
        let metrics = SessionMetrics {
            models: IndexMap::from([
                (
                    "m1".into(),
                    ModelMetrics {
                        api: ApiMetrics {
                            total_requests: 2.0,
                            total_latency_ms: 120.0,
                            ..ApiMetrics::default()
                        },
                        tokens: TokenMetrics {
                            prompt_tokens: 10.0,
                            candidates: 5.0,
                            thoughts_tokens: 2.0,
                            ..TokenMetrics::default()
                        },
                        ..ModelMetrics::default()
                    },
                ),
                (
                    "m2".into(),
                    ModelMetrics {
                        api: ApiMetrics {
                            total_requests: 1.0,
                            total_latency_ms: 80.0,
                            ..ApiMetrics::default()
                        },
                        tokens: TokenMetrics {
                            total_tokens: 18.0,
                            ..TokenMetrics::default()
                        },
                        ..ModelMetrics::default()
                    },
                ),
            ]),
            ..SessionMetrics::default()
        };
        let record = metrics_to_usage_record("s", "/p", 100, 500, &metrics);
        assert_eq!(record.version, 1);
        assert_eq!(record.duration_ms, 400);
        assert_eq!(record.total_latency_ms, Some(200.0));
        assert_eq!(record.models["m1"].total_tokens, 17.0);
        assert_eq!(record.models["m2"].total_tokens, 18.0);
        assert!(record.skills.is_none());
        let serialized = serde_json::to_value(&record).unwrap();
        assert_eq!(serialized["tools"]["totalCalls"], 0.0);
        assert!(serialized.get("skills").is_none());
        assert_eq!(serialized["models"]["m1"]["totalLatencyMs"], 120.0);
    }

    #[test]
    fn metrics_conversion_copies_skill_breakdown() {
        let metrics = SessionMetrics {
            skills: Some(SkillMetricCollection {
                total_calls: 3.0,
                total_success: 2.0,
                total_fail: 1.0,
                by_name: IndexMap::from([(
                    "review".into(),
                    SkillMetric {
                        count: 3.0,
                        success: 2.0,
                        fail: 1.0,
                    },
                )]),
            }),
            ..SessionMetrics::default()
        };
        let record = metrics_to_usage_record("s", "/p", 0, 1, &metrics);
        assert_eq!(record.skills.unwrap().by_name["review"].success, 2.0);
    }

    #[test]
    fn metrics_input_accepts_the_existing_session_metrics_json_shape() {
        let metrics: SessionMetrics = serde_json::from_value(json!({
            "models": {
                "model-a": {
                    "api": {"totalRequests": 2, "totalErrors": 1, "totalLatencyMs": 900},
                    "tokens": {"prompt": 60, "candidates": 30, "total": 100, "cached": 0, "thoughts": 10},
                    "bySource": {}
                }
            },
            "tools": {
                "totalCalls": 1, "totalSuccess": 1, "totalFail": 0,
                "totalDurationMs": 15, "totalDecisions": {},
                "byName": {"edit": {"count": 1, "success": 1, "fail": 0, "durationMs": 15, "decisions": {}}}
            },
            "files": {"totalLinesAdded": 4, "totalLinesRemoved": 1}
        }))
        .unwrap();
        let record = metrics_to_usage_record("s", "/p", 1, 2, &metrics);
        assert_eq!(record.models["model-a"].requests, 2.0);
        assert_eq!(record.models["model-a"].total_tokens, 100.0);
        assert_eq!(record.tools.by_name["edit"].total_duration_ms, Some(15.0));
        assert_eq!(record.files.lines_added, 4.0);
    }

    #[test]
    fn transcript_replay_uses_namespaced_ui_telemetry_event_names() {
        let mut metrics = SessionMetrics::default();
        assert!(!accumulate_ui_event(
            &mut metrics,
            &json!({"event.name":"api_response","model":"m","total_token_count":7})
        ));
        assert!(metrics.models.is_empty());
        assert!(accumulate_ui_event(
            &mut metrics,
            &json!({"event.name":"canopy-code.api_response","model":"m","duration_ms":3,"input_token_count":2,"output_token_count":4,"thoughts_token_count":1,"total_token_count":7})
        ));
        assert_eq!(metrics.models["m"].api.total_requests, 1.0);
        assert_eq!(metrics.models["m"].tokens.total_tokens, 7.0);
        assert!(accumulate_ui_event(
            &mut metrics,
            &json!({"event.name":"canopy-code.tool_call","function_name":"edit","success":true,"duration_ms":5})
        ));
        assert_eq!(metrics.tools.by_name["edit"].duration_ms, 5.0);
    }

    #[test]
    fn legacy_records_without_latency_duration_or_skills_deserialize() {
        let record: UsageSummaryRecord = from_str(
            r#"{"version":1,"sessionId":"s","timestamp":10,"startTime":0,"project":"/p","durationMs":10,"models":{"m":{"requests":1,"inputTokens":2,"outputTokens":3,"cachedTokens":0,"thoughtsTokens":0,"totalTokens":5}},"tools":{"totalCalls":0,"totalSuccess":0,"totalFail":0,"byName":{"edit":{"count":1,"success":1,"fail":0}}},"files":{"linesAdded":0,"linesRemoved":0}}"#,
        )
        .unwrap();
        assert_eq!(record.models["m"].total_latency_ms, None);
        assert_eq!(record.tools.by_name["edit"].total_duration_ms, None);
        assert!(record.skills.is_none());
    }

    #[test]
    fn aggregation_sorts_and_caps_top_rows_and_sums_legacy_optional_fields_as_zero() {
        let mut records = vec![
            sample_record("a", 1_000_000, 100.0),
            sample_record("b", 1_000_001, 200.0),
        ];
        records[0].tools.by_name.insert(
            "bash".into(),
            ToolUsage {
                count: 10.0,
                success: 10.0,
                fail: 0.0,
                total_duration_ms: None,
            },
        );
        for n in 0..40 {
            records[0]
                .skills
                .get_or_insert_with(SkillTotals::default)
                .by_name
                .insert(
                    format!("skill-{n}"),
                    SkillUsage {
                        count: n as f64 + 1.0,
                        success: n as f64 + 1.0,
                        fail: 0.0,
                    },
                );
        }
        let report = aggregate_usage(&records, TimeRange::All, 2_000_000);
        assert_eq!(report.total_latency_ms, 2400.0);
        assert_eq!(report.total_requests, 2.0);
        assert_eq!(report.tools.top_tools[0].name, "bash");
        assert_eq!(
            report
                .tools
                .top_tools
                .iter()
                .find(|tool| tool.name == "edit")
                .unwrap()
                .total_duration_ms,
            1600.0
        );
        assert_eq!(report.skills.top_skills.len(), 25);
        assert_eq!(report.skills.top_skills[0].name, "skill-39");
        assert_eq!(report.projects[0].total_tokens, 300.0);
        let serialized = serde_json::to_value(&report).unwrap();
        assert!(
            serialized["periodStart"]
                .as_str()
                .unwrap()
                .ends_with(".000Z")
        );
    }

    #[test]
    fn durable_load_deduplicates_last_value_and_preserves_first_id_position() {
        let root = temp_root("dedup");
        let path = root.join("usage_record.jsonl");
        crate::jsonl::write(
            &path,
            &[
                sample_record("a", 1, 10.0),
                sample_record("b", 2, 20.0),
                sample_record("a", 3, 30.0),
            ],
        )
        .unwrap();
        let records = load_usage_history(&root, None, LoadOptions::default()).unwrap();
        assert_eq!(
            records
                .iter()
                .map(|record| record.session_id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(records[0].models["model-a"].total_tokens, 30.0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn transcript_replay_summarizes_telemetry_and_with_live_is_read_only() {
        let root = temp_root("live");
        let chats = root.join("projects/project/chats");
        fs::create_dir_all(&chats).unwrap();
        let transcript = chats.join("session-live.jsonl");
        let records = vec![
            json!({"sessionId":"session-live","cwd":"/work","timestamp":"2026-06-11T00:00:00Z","type":"user"}),
            json!({"sessionId":"session-live","timestamp":"2026-06-11T00:01:00Z","type":"system","subtype":"ui_telemetry","systemPayload":{"uiEvent":{"event.name":"canopy-code.api_response","model":"model-a","duration_ms":900,"input_token_count":60,"output_token_count":30,"cached_content_token_count":0,"thoughts_token_count":10,"total_token_count":100}}}),
            json!({"sessionId":"session-live","timestamp":"2026-06-11T00:02:00Z","type":"assistant"}),
        ];
        crate::jsonl::write(&transcript, &records).unwrap();
        let merged =
            load_usage_history_with_live(&root, 1_800_000_000_000, LiveLoadOptions::default());
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].project, "/work");
        assert_eq!(merged[0].models["model-a"].total_tokens, 100.0);
        assert_eq!(merged[0].skills.as_ref().unwrap().total_calls, 0.0);
        assert!(!root.join("usage_record.jsonl").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn migration_skips_in_progress_session_write_but_returns_it() {
        let root = temp_root("migration");
        let chats = root.join("projects/project/chats");
        fs::create_dir_all(&chats).unwrap();
        let records = vec![
            json!({"sessionId":"live","cwd":"/work","timestamp":"2026-06-11T00:00:00Z","type":"user"}),
            json!({"sessionId":"live","timestamp":"2026-06-11T00:01:00Z","type":"system","subtype":"ui_telemetry","systemPayload":{"uiEvent":{"event.name":"canopy-code.api_error","model":"model-a","duration_ms":900}}}),
        ];
        crate::jsonl::write(chats.join("live.jsonl"), &records).unwrap();
        let loaded = load_usage_history(&root, Some("live"), LoadOptions::default()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(!root.join("usage_record.jsonl").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn migration_surfaces_usage_file_write_errors() {
        let root = temp_root("migration-error");
        let chats = root.join("projects/project/chats");
        fs::create_dir_all(&chats).unwrap();
        fs::create_dir(root.join("usage_record.jsonl")).unwrap();
        let records = vec![
            json!({"sessionId":"s","cwd":"/p","timestamp":"2026-06-11T00:00:00Z","type":"user"}),
            json!({"sessionId":"s","timestamp":"2026-06-11T00:01:00Z","type":"system","subtype":"ui_telemetry","systemPayload":{"uiEvent":{"event.name":"canopy-code.api_error","model":"m","duration_ms":5}}}),
        ];
        crate::jsonl::write(chats.join("s.jsonl"), &records).unwrap();
        assert!(load_usage_history(&root, None, LoadOptions::default()).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_merge_prefers_persisted_values_and_never_writes_replayed_rows() {
        let root = temp_root("live-authoritative");
        let chats = root.join("projects/project/chats");
        fs::create_dir_all(&chats).unwrap();
        crate::jsonl::write(
            root.join("usage_record.jsonl"),
            &[sample_record("same", 1_700_000_000_000, 100.0)],
        )
        .unwrap();
        let transcript = vec![
            json!({"sessionId":"same","cwd":"/work","timestamp":"2026-06-11T00:00:00Z","type":"user"}),
            json!({"sessionId":"same","timestamp":"2026-06-11T00:01:00Z","type":"system","subtype":"ui_telemetry","systemPayload":{"uiEvent":{"event.name":"canopy-code.api_response","model":"model-a","duration_ms":900,"input_token_count":60,"output_token_count":30,"cached_content_token_count":0,"thoughts_token_count":10,"total_token_count":100}}}),
            json!({"sessionId":"same","timestamp":"2026-06-11T00:02:00Z","type":"assistant"}),
        ];
        crate::jsonl::write(chats.join("same.jsonl"), &transcript).unwrap();
        let merged = load_usage_history_with_live(
            &root,
            1_800_000_000_000,
            LiveLoadOptions { since_ms: Some(0) },
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].models["model-a"].total_tokens, 100.0);
        assert_eq!(
            crate::jsonl::read(root.join("usage_record.jsonl"))
                .unwrap()
                .len(),
            1
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn transcript_deletion_salvage_is_deduplicated_and_nonfatal() {
        let root = temp_root("salvage");
        let transcript = root.join("transcript.jsonl");
        crate::jsonl::write(&transcript, &[
            json!({"sessionId":"s","cwd":"/p","timestamp":"2026-06-11T00:00:00Z","type":"user"}),
            json!({"sessionId":"s","timestamp":"2026-06-11T00:01:00Z","type":"system","subtype":"ui_telemetry","systemPayload":{"uiEvent":{"event.name":"canopy-code.api_response","model":"m","duration_ms":2,"input_token_count":1,"output_token_count":1,"total_token_count":2}}}),
        ]).unwrap();
        assert!(persist_usage_before_transcript_deletion(&root, &transcript));
        assert!(!persist_usage_before_transcript_deletion(
            &root,
            &transcript
        ));
        assert!(!persist_usage_before_transcript_deletion(
            &root,
            root.join("missing.jsonl")
        ));
        assert_eq!(
            crate::jsonl::read(root.join("usage_record.jsonl"))
                .unwrap()
                .len(),
            1
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn bounds_use_rolling_ranges_and_local_calendar_day() {
        let now = 1_750_000_000_000;
        assert_eq!(
            get_time_range_bounds(TimeRange::Week, now),
            (now - 7 * MS_PER_DAY, now)
        );
        assert_eq!(
            get_time_range_bounds(TimeRange::Month, now),
            (now - 30 * MS_PER_DAY, now)
        );
        assert_eq!(get_time_range_bounds(TimeRange::All, now), (0, now));
        let (today, end) = get_time_range_bounds(TimeRange::Today, now);
        assert!(today <= now && end == now);
        assert!(now - today < MS_PER_DAY + 60 * 60 * 1000);
    }

    #[test]
    fn dashboard_matches_source_token_fallback_cache_rates_and_series_bounds() {
        let now = 1_750_000_000_000;
        let mut record = sample_record("dashboard", now - MS_PER_DAY, 100.0);
        let model = record.models.get_mut("model-a").unwrap();
        model.total_tokens = 0.0;
        model.cached_tokens = 12.0;
        let report = build_usage_dashboard(
            &[record.clone()],
            UsageDashboardOptions {
                range: Some(TimeRange::Month),
                heatmap_days: Some(1.9),
            },
            now,
        );

        assert_eq!(report.range, TimeRange::Month);
        assert_eq!(report.heatmap_days, 1);
        assert_eq!(report.summary.total_tokens, 100.0);
        assert_eq!(report.summary.input_tokens, 60.0);
        assert_eq!(report.summary.cache_read_rate, 0.2);
        assert_eq!(report.models[0].total_tokens, 100.0);
        assert_eq!(report.models[0].share, 1.0);
        assert!(report.daily.len() >= 30);
        let day = local_date_key(record.timestamp);
        assert!(
            report
                .daily
                .iter()
                .any(|point| { point.date == day && point.tokens == 100.0 && point.sessions == 1 })
        );
        assert_eq!(report.heatmap[&day].tokens, 100.0);
        assert_eq!(report.heatmap[&day].cache_read_rate, 0.2);

        let empty = build_usage_dashboard(&[], UsageDashboardOptions::default(), now);
        assert_eq!(empty.range, TimeRange::Today);
        assert_eq!(empty.heatmap_days, DEFAULT_HEATMAP_DAYS);
        assert!(empty.daily.len() <= 2);
        assert_eq!(empty.summary.total_tokens, 0.0);
    }

    #[test]
    fn dashboard_and_record_conversion_use_javascript_zero_or_nan_fallback() {
        assert_eq!(dashboard_model_tokens(f64::NAN, 1.0, 2.0, 3.0), 6.0);
        assert_eq!(dashboard_model_tokens(0.0, 1.0, 2.0, 3.0), 6.0);
        assert_eq!(dashboard_model_tokens(1.0, 2.0, 3.0, 4.0), 1.0);
    }
}
