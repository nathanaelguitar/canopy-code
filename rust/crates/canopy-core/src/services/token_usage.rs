//! Per-request token usage records and day/month exports.
//!
//! Ports the local JSONL contract and query/export helpers from
//! `packages/core/src/services/tokenUsageService.ts`. Hosts provide
//! session/provider context when calling `record_api_response` after a
//! completed request; recording failure need not fail the model response.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Datelike, Local, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const TOKEN_USAGE_SCHEMA_VERSION: u8 = 1;
pub const TOKEN_USAGE_DIRECTORY: &str = "usage";
pub const TOKEN_USAGE_FILE_PREFIX: &str = "token-usage-";
const MAIN_SOURCE: &str = "main";
const UNKNOWN_AUTH_TYPE: &str = "unknown";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenUsagePeriod {
    Day,
    Month,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenUsageExportFormat {
    Json,
    Csv,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsageRecord {
    pub schema_version: u8,
    pub id: String,
    pub timestamp: String,
    /// Local calendar bucket in the process timezone that wrote the record.
    pub local_date: String,
    /// Local calendar month in the process timezone that wrote the record.
    pub local_month: String,
    pub session_id: String,
    pub model: String,
    pub auth_type: String,
    pub source: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
    pub thoughts_tokens: u64,
    pub total_tokens: u64,
    /// End-to-end API response duration; this is not generation timing.
    pub api_duration_ms: u64,
}

/// Subset of `ApiResponseEvent` used to persist per-request usage.
#[derive(Clone, Debug, Default)]
pub struct ApiResponseUsageInput<'a> {
    pub timestamp: Option<&'a str>,
    pub model: Option<&'a str>,
    pub auth_type: Option<&'a str>,
    pub source: Option<&'a str>,
    pub input_tokens: Option<f64>,
    pub output_tokens: Option<f64>,
    pub cached_tokens: Option<f64>,
    pub thoughts_tokens: Option<f64>,
    pub total_tokens: Option<f64>,
    pub api_duration_ms: Option<f64>,
}

impl<'a> ApiResponseUsageInput<'a> {
    /// Adapt the provider-neutral Gemini-shaped usage metadata emitted by the
    /// native stream converters to the telemetry field names in the source
    /// `ApiResponseEvent`.
    pub fn from_usage_metadata(
        usage: &'a Value,
        timestamp: Option<&'a str>,
        model: Option<&'a str>,
        auth_type: Option<&'a str>,
        source: Option<&'a str>,
        api_duration_ms: Option<f64>,
    ) -> Self {
        Self {
            timestamp,
            model,
            auth_type,
            source,
            input_tokens: usage.get("promptTokenCount").and_then(Value::as_f64),
            output_tokens: usage.get("candidatesTokenCount").and_then(Value::as_f64),
            cached_tokens: usage.get("cachedContentTokenCount").and_then(Value::as_f64),
            thoughts_tokens: usage.get("thoughtsTokenCount").and_then(Value::as_f64),
            total_tokens: usage.get("totalTokenCount").and_then(Value::as_f64),
            api_duration_ms,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsageTotals {
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
    pub thoughts_tokens: u64,
    pub total_tokens: u64,
    pub api_duration_ms: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsageGroupSummary {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(flatten)]
    pub totals: TokenUsageTotals,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsageSummary {
    pub period: TokenUsagePeriod,
    pub value: String,
    pub generated_at: String,
    pub totals: TokenUsageTotals,
    pub by_model: Vec<TokenUsageGroupSummary>,
    pub by_auth_type: Vec<TokenUsageGroupSummary>,
    pub by_model_and_auth_type: Vec<TokenUsageGroupSummary>,
    pub by_source: Vec<TokenUsageGroupSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenUsageQuery {
    pub period: TokenUsagePeriod,
    pub value: Option<String>,
}

impl TokenUsageRecord {
    pub fn from_api_response(
        session_id: impl Into<String>,
        event: ApiResponseUsageInput<'_>,
        now: DateTime<Utc>,
    ) -> Self {
        let timestamp = event
            .timestamp
            .filter(|timestamp| !timestamp.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
        let parsed = DateTime::parse_from_rfc3339(&timestamp)
            .map(|timestamp| timestamp.with_timezone(&Utc))
            .unwrap_or(now);
        let local = parsed.with_timezone(&Local);
        let input_tokens = non_negative_integer(event.input_tokens);
        let cached_tokens = non_negative_integer(event.cached_tokens);
        let output_tokens = non_negative_integer(event.output_tokens);
        let thoughts_tokens = non_negative_integer(event.thoughts_tokens);
        let total_tokens = positive_integer(event.total_tokens).unwrap_or_else(|| {
            input_tokens
                .saturating_add(output_tokens)
                .saturating_add(thoughts_tokens)
        });

        Self {
            schema_version: TOKEN_USAGE_SCHEMA_VERSION,
            id: Uuid::new_v4().to_string(),
            timestamp,
            local_date: format!(
                "{:04}-{:02}-{:02}",
                local.year(),
                local.month(),
                local.day()
            ),
            local_month: format!("{:04}-{:02}", local.year(), local.month()),
            session_id: session_id.into(),
            model: non_empty(event.model).unwrap_or_else(|| "unknown".to_owned()),
            auth_type: non_empty(event.auth_type).unwrap_or_else(|| UNKNOWN_AUTH_TYPE.to_owned()),
            source: non_empty(event.source).unwrap_or_else(|| MAIN_SOURCE.to_owned()),
            input_tokens: if input_tokens > 0 {
                input_tokens
            } else {
                cached_tokens
            },
            output_tokens,
            cached_tokens,
            thoughts_tokens,
            total_tokens,
            api_duration_ms: non_negative_integer(event.api_duration_ms),
        }
    }
}

pub fn token_usage_file_path(runtime_base_dir: &Path, month: &str) -> Result<PathBuf, String> {
    if !is_valid_month(month) {
        return Err(format!(
            "Invalid month value \"{month}\". Expected YYYY-MM."
        ));
    }
    Ok(runtime_base_dir
        .join(TOKEN_USAGE_DIRECTORY)
        .join(format!("{TOKEN_USAGE_FILE_PREFIX}{month}.jsonl")))
}

pub fn record_api_response(
    runtime_base_dir: &Path,
    session_id: impl Into<String>,
    event: ApiResponseUsageInput<'_>,
    now: DateTime<Utc>,
) -> io::Result<TokenUsageRecord> {
    let record = TokenUsageRecord::from_api_response(session_id, event, now);
    let path = token_usage_file_path(runtime_base_dir, &record.local_month)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    crate::jsonl::write_line(path, &record)?;
    Ok(record)
}

pub fn query_token_usage(
    runtime_base_dir: &Path,
    query: TokenUsageQuery,
    now: DateTime<Utc>,
) -> Result<TokenUsageSummary, String> {
    let value = normalize_period_value(query.period, query.value.as_deref(), now)?;
    let month = match query.period {
        TokenUsagePeriod::Day => value[..7].to_owned(),
        TokenUsagePeriod::Month => value.clone(),
    };
    let path = token_usage_file_path(runtime_base_dir, &month)?;
    let values = crate::jsonl::read(path)
        .map_err(|error| format!("could not read token usage history: {error}"))?;
    let records = values
        .into_iter()
        .filter_map(parse_token_usage_record)
        .filter(|record| match query.period {
            TokenUsagePeriod::Day => record.local_date == value,
            TokenUsagePeriod::Month => record.local_month == value,
        })
        .collect::<Vec<_>>();
    Ok(summarize_records(query.period, value, records, now))
}

pub fn format_token_usage_summary_as_csv(summary: &TokenUsageSummary) -> String {
    const HEADER: &str = "period,value,group_type,group_key,model,auth_type,source,requests,input_tokens,output_tokens,cached_tokens,thoughts_tokens,total_tokens,api_duration_ms";
    let mut rows = Vec::<Vec<String>>::new();
    rows.push(vec![
        period_name(summary.period).to_owned(),
        summary.value.clone(),
        "total".to_owned(),
        "total".to_owned(),
        String::new(),
        String::new(),
        String::new(),
        summary.totals.requests.to_string(),
        summary.totals.input_tokens.to_string(),
        summary.totals.output_tokens.to_string(),
        summary.totals.cached_tokens.to_string(),
        summary.totals.thoughts_tokens.to_string(),
        summary.totals.total_tokens.to_string(),
        summary.totals.api_duration_ms.to_string(),
    ]);
    append_group_rows(
        &mut rows,
        summary.period,
        &summary.value,
        "model",
        &summary.by_model,
    );
    append_group_rows(
        &mut rows,
        summary.period,
        &summary.value,
        "auth_type",
        &summary.by_auth_type,
    );
    append_group_rows(
        &mut rows,
        summary.period,
        &summary.value,
        "model_auth_type",
        &summary.by_model_and_auth_type,
    );
    append_group_rows(
        &mut rows,
        summary.period,
        &summary.value,
        "source",
        &summary.by_source,
    );
    let mut csv = HEADER.to_owned();
    for row in rows {
        csv.push('\n');
        csv.push_str(
            &row.into_iter()
                .map(|field| csv_escape(&field))
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    csv
}

pub fn format_token_usage_summary_as_json(summary: &TokenUsageSummary) -> Result<String, String> {
    serde_json::to_string_pretty(summary)
        .map(|json| format!("{json}\n"))
        .map_err(|error| format!("could not encode token usage summary: {error}"))
}

pub fn export_token_usage_summary(
    runtime_base_dir: &Path,
    query: TokenUsageQuery,
    format: TokenUsageExportFormat,
    now: DateTime<Utc>,
) -> Result<String, String> {
    let summary = query_token_usage(runtime_base_dir, query, now)?;
    match format {
        TokenUsageExportFormat::Json => format_token_usage_summary_as_json(&summary),
        TokenUsageExportFormat::Csv => {
            Ok(format!("{}\n", format_token_usage_summary_as_csv(&summary)))
        }
    }
}

fn normalize_period_value(
    period: TokenUsagePeriod,
    value: Option<&str>,
    now: DateTime<Utc>,
) -> Result<String, String> {
    let now = now.with_timezone(&Local);
    let current = match period {
        TokenUsagePeriod::Day => format!("{:04}-{:02}-{:02}", now.year(), now.month(), now.day()),
        TokenUsagePeriod::Month => format!("{:04}-{:02}", now.year(), now.month()),
    };
    let normalized = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(&current);
    let valid = match period {
        TokenUsagePeriod::Day => is_valid_day(normalized),
        TokenUsagePeriod::Month => is_valid_month(normalized),
    };
    if !valid {
        return Err(format!(
            "Invalid {} value \"{normalized}\". Expected {}.",
            period_name(period),
            match period {
                TokenUsagePeriod::Day => "YYYY-MM-DD",
                TokenUsagePeriod::Month => "YYYY-MM",
            }
        ));
    }
    Ok(normalized.to_owned())
}

fn is_valid_day(value: &str) -> bool {
    let Ok(date) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") else {
        return false;
    };
    date.format("%Y-%m-%d").to_string() == value
}

fn is_valid_month(value: &str) -> bool {
    if value.len() != 7 || value.as_bytes().get(4) != Some(&b'-') {
        return false;
    }
    let year = &value[..4];
    let month = &value[5..];
    year.bytes().all(|byte| byte.is_ascii_digit())
        && month.bytes().all(|byte| byte.is_ascii_digit())
        && month
            .parse::<u32>()
            .is_ok_and(|month| (1..=12).contains(&month))
}

fn parse_token_usage_record(value: Value) -> Option<TokenUsageRecord> {
    let record = serde_json::from_value::<TokenUsageRecord>(value).ok()?;
    (record.schema_version > 0 && record.schema_version <= TOKEN_USAGE_SCHEMA_VERSION)
        .then_some(record)
}

fn summarize_records(
    period: TokenUsagePeriod,
    value: String,
    records: Vec<TokenUsageRecord>,
    now: DateTime<Utc>,
) -> TokenUsageSummary {
    let mut totals = TokenUsageTotals::default();
    let mut by_model = HashMap::<String, TokenUsageGroupSummary>::new();
    let mut by_auth_type = HashMap::<String, TokenUsageGroupSummary>::new();
    let mut by_model_and_auth_type = HashMap::<String, TokenUsageGroupSummary>::new();
    let mut by_source = HashMap::<String, TokenUsageGroupSummary>::new();
    for record in records {
        add_record(&mut totals, &record);
        add_group_record(
            &mut by_model,
            &record.model,
            Some(record.model.clone()),
            None,
            None,
            &record,
        );
        add_group_record(
            &mut by_auth_type,
            &record.auth_type,
            None,
            Some(record.auth_type.clone()),
            None,
            &record,
        );
        add_group_record(
            &mut by_model_and_auth_type,
            &format!("{}|{}", record.model, record.auth_type),
            Some(record.model.clone()),
            Some(record.auth_type.clone()),
            None,
            &record,
        );
        add_group_record(
            &mut by_source,
            &record.source,
            None,
            None,
            Some(record.source.clone()),
            &record,
        );
    }
    TokenUsageSummary {
        period,
        value,
        generated_at: now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        totals,
        by_model: sorted_groups(by_model),
        by_auth_type: sorted_groups(by_auth_type),
        by_model_and_auth_type: sorted_groups(by_model_and_auth_type),
        by_source: sorted_groups(by_source),
    }
}

fn add_record(totals: &mut TokenUsageTotals, record: &TokenUsageRecord) {
    totals.requests = totals.requests.saturating_add(1);
    totals.input_tokens = totals.input_tokens.saturating_add(record.input_tokens);
    totals.output_tokens = totals.output_tokens.saturating_add(record.output_tokens);
    totals.cached_tokens = totals.cached_tokens.saturating_add(record.cached_tokens);
    totals.thoughts_tokens = totals
        .thoughts_tokens
        .saturating_add(record.thoughts_tokens);
    totals.total_tokens = totals.total_tokens.saturating_add(record.total_tokens);
    totals.api_duration_ms = totals
        .api_duration_ms
        .saturating_add(record.api_duration_ms);
}

fn add_group_record(
    groups: &mut HashMap<String, TokenUsageGroupSummary>,
    key: &str,
    model: Option<String>,
    auth_type: Option<String>,
    source: Option<String>,
    record: &TokenUsageRecord,
) {
    let group = groups
        .entry(key.to_owned())
        .or_insert_with(|| TokenUsageGroupSummary {
            key: key.to_owned(),
            model,
            auth_type,
            source,
            totals: TokenUsageTotals::default(),
        });
    add_record(&mut group.totals, record);
}

fn sorted_groups(groups: HashMap<String, TokenUsageGroupSummary>) -> Vec<TokenUsageGroupSummary> {
    let mut groups = groups.into_values().collect::<Vec<_>>();
    groups.sort_by(|left, right| {
        right
            .totals
            .total_tokens
            .cmp(&left.totals.total_tokens)
            .then_with(|| left.key.cmp(&right.key))
    });
    groups
}

fn append_group_rows(
    rows: &mut Vec<Vec<String>>,
    period: TokenUsagePeriod,
    value: &str,
    group_type: &str,
    groups: &[TokenUsageGroupSummary],
) {
    rows.extend(groups.iter().map(|group| {
        vec![
            period_name(period).to_owned(),
            value.to_owned(),
            group_type.to_owned(),
            group.key.clone(),
            group.model.clone().unwrap_or_default(),
            group.auth_type.clone().unwrap_or_default(),
            group.source.clone().unwrap_or_default(),
            group.totals.requests.to_string(),
            group.totals.input_tokens.to_string(),
            group.totals.output_tokens.to_string(),
            group.totals.cached_tokens.to_string(),
            group.totals.thoughts_tokens.to_string(),
            group.totals.total_tokens.to_string(),
            group.totals.api_duration_ms.to_string(),
        ]
    }));
}

fn csv_escape(value: &str) -> String {
    let dangerous = value
        .trim_start()
        .chars()
        .next()
        .is_some_and(|character| matches!(character, '=' | '+' | '-' | '@'))
        || value
            .chars()
            .next()
            .is_some_and(|character| matches!(character, '\t' | '\r' | '\n'));
    let value = if dangerous {
        format!("'{value}")
    } else {
        value.to_owned()
    };
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value
    }
}

fn period_name(period: TokenUsagePeriod) -> &'static str {
    match period {
        TokenUsagePeriod::Day => "day",
        TokenUsagePeriod::Month => "month",
    }
}

fn non_empty(value: Option<&str>) -> Option<String> {
    value.filter(|value| !value.is_empty()).map(str::to_owned)
}

fn non_negative_integer(value: Option<f64>) -> u64 {
    value
        .filter(|value| value.is_finite() && *value > 0.0)
        .map(|value| value.trunc().min(u64::MAX as f64) as u64)
        .unwrap_or(0)
}

fn positive_integer(value: Option<f64>) -> Option<u64> {
    let value = non_negative_integer(value);
    (value > 0).then_some(value)
}
