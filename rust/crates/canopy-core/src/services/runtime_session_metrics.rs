//! Bounded, in-memory counters for the currently running Rust agent sessions.
//!
//! The collector keeps only aggregate model/tool metrics. It never retains a
//! prompt, tool call arguments, tool output, response body, or session event.

use std::sync::Mutex;

use indexmap::IndexMap;
use serde_json::Value;

use super::usage_history::{ApiMetrics, ModelMetrics, SessionMetrics, SourceMetrics};

const MAX_TRACKED_SESSIONS: usize = 32;
const MAX_MODELS_PER_SESSION: usize = 64;
const MAX_SOURCES_PER_MODEL: usize = 16;
const MAX_TOOLS_PER_SESSION: usize = 256;
const MAX_LABEL_CHARS: usize = 128;

const OTHER_MODELS: &str = "(other models)";
const OTHER_SOURCES: &str = "other";
const OTHER_TOOLS: &str = "(other tools)";

#[derive(Default)]
struct CollectorState {
    /// Insertion order is also the least-recently-used eviction order.
    sessions: IndexMap<String, SessionMetrics>,
}

/// Captures numeric in-process metrics without keeping request or tool data.
///
/// Metrics are partitioned by session ID, bounded to the most recently active
/// sessions, and further bounded by model/source/tool name counts. `snapshot`
/// returns `None` until at least one eligible model or tool event was recorded.
#[derive(Default)]
pub struct RuntimeSessionMetricsCollector {
    state: Mutex<CollectorState>,
}

impl RuntimeSessionMetricsCollector {
    /// Record one completed or failed provider request. A `None` usage value
    /// records request/error/duration counters when the provider did not report
    /// usage metadata. Only numeric aggregates are retained.
    pub fn record_model_request(
        &self,
        session_id: &str,
        model: &str,
        source: &str,
        succeeded: bool,
        duration_ms: u64,
        usage: Option<&Value>,
    ) {
        let model = bounded_label(model, "unknown");
        let source = bounded_label(source, "unknown");
        self.update_session(session_id, |metrics| {
            let model_key = bounded_entry_key(
                &metrics.models,
                &model,
                MAX_MODELS_PER_SESSION,
                OTHER_MODELS,
            );
            let model_metrics = metrics.models.entry(model_key).or_default();
            update_model_metrics(model_metrics, succeeded, duration_ms as f64, usage);

            let source_key = bounded_entry_key(
                &model_metrics.by_source,
                &source,
                MAX_SOURCES_PER_MODEL,
                OTHER_SOURCES,
            );
            let source_metrics = model_metrics.by_source.entry(source_key).or_default();
            update_source_metrics(source_metrics, succeeded, duration_ms as f64, usage);
        });
    }

    /// Record a completed tool execution. Only its bounded name, outcome, and
    /// elapsed time are retained.
    pub fn record_tool_call(
        &self,
        session_id: &str,
        tool_name: &str,
        succeeded: bool,
        duration_ms: u64,
    ) {
        let tool_name = bounded_label(tool_name, "unknown");
        self.update_session(session_id, |metrics| {
            let tools = &mut metrics.tools;
            tools.total_calls += 1.0;
            tools.total_duration_ms += duration_ms as f64;
            if succeeded {
                tools.total_success += 1.0;
            } else {
                tools.total_fail += 1.0;
            }
            let key = bounded_entry_key(
                &tools.by_name,
                &tool_name,
                MAX_TOOLS_PER_SESSION,
                OTHER_TOOLS,
            );
            let stats = tools.by_name.entry(key).or_default();
            stats.count += 1.0;
            stats.duration_ms += duration_ms as f64;
            if succeeded {
                stats.success += 1.0;
            } else {
                stats.fail += 1.0;
            }
        });
    }

    /// Return a point-in-time copy for one session, if eligible metrics exist.
    pub fn snapshot(&self, session_id: &str) -> Option<SessionMetrics> {
        let key = bounded_label(session_id, "");
        if key.is_empty() {
            return None;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let metrics = state.sessions.shift_remove(&key)?;
        let result = has_recorded_metrics(&metrics).then(|| metrics.clone());
        state.sessions.insert(key, metrics);
        result
    }

    fn update_session(&self, session_id: &str, update: impl FnOnce(&mut SessionMetrics)) {
        let key = bounded_label(session_id, "");
        if key.is_empty() {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let mut metrics = state.sessions.shift_remove(&key).unwrap_or_default();
        // A `None` value means there is no native skill-launch event source;
        // do not present a fabricated zero-valued skills report.
        update(&mut metrics);
        state.sessions.insert(key, metrics);
        if state.sessions.len() > MAX_TRACKED_SESSIONS {
            state.sessions.shift_remove_index(0);
        }
    }
}

fn has_recorded_metrics(metrics: &SessionMetrics) -> bool {
    !metrics.models.is_empty() || !metrics.tools.by_name.is_empty()
}

fn bounded_label(value: &str, fallback: &str) -> String {
    let value = value.trim();
    let value = if value.is_empty() { fallback } else { value };
    value.chars().take(MAX_LABEL_CHARS).collect()
}

fn bounded_entry_key<T>(
    entries: &IndexMap<String, T>,
    name: &str,
    max_entries: usize,
    overflow_name: &str,
) -> String {
    if entries.contains_key(name) || entries.len() < max_entries.saturating_sub(1) {
        name.to_owned()
    } else {
        overflow_name.to_owned()
    }
}

fn update_model_metrics(
    metrics: &mut ModelMetrics,
    succeeded: bool,
    duration_ms: f64,
    usage: Option<&Value>,
) {
    update_api_metrics(&mut metrics.api, succeeded, duration_ms);
    if let Some(usage) = usage {
        update_token_metrics(&mut metrics.tokens, usage);
    }
}

fn update_source_metrics(
    metrics: &mut SourceMetrics,
    succeeded: bool,
    duration_ms: f64,
    usage: Option<&Value>,
) {
    update_api_metrics(&mut metrics.api, succeeded, duration_ms);
    if let Some(usage) = usage {
        update_token_metrics(&mut metrics.tokens, usage);
    }
}

fn update_api_metrics(metrics: &mut ApiMetrics, succeeded: bool, duration_ms: f64) {
    metrics.total_requests += 1.0;
    metrics.total_latency_ms += duration_ms;
    if !succeeded {
        metrics.total_errors += 1.0;
    }
}

fn update_token_metrics(metrics: &mut super::usage_history::TokenMetrics, usage: &Value) {
    let input = numeric_usage(usage, "promptTokenCount");
    let output = numeric_usage(usage, "candidatesTokenCount");
    let cached = numeric_usage(usage, "cachedContentTokenCount");
    let thoughts = numeric_usage(usage, "thoughtsTokenCount");
    let total = numeric_usage(usage, "totalTokenCount");

    metrics.prompt_tokens += if input > 0.0 { input } else { cached };
    metrics.candidates += output;
    metrics.cached_tokens += cached;
    metrics.thoughts_tokens += thoughts;
    metrics.total_tokens += if total > 0.0 {
        total
    } else {
        input + output + thoughts
    };
}

fn numeric_usage(usage: &Value, key: &str) -> f64 {
    usage
        .get(key)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(0.0)
}
