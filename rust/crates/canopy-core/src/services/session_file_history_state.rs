//! Session-scoped file-history snapshot restoration.
//!
//! Port of `packages/core/src/services/session-file-history-state.ts`. The
//! accumulator keeps the first-insertion ordering of prompt IDs, replaces
//! snapshots in retained slots, and permanently ignores IDs after eviction.
//! A complete record batch is decoded before accumulator state changes, so a
//! malformed snapshot cannot partially apply.

use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, NaiveDate, NaiveDateTime, SecondsFormat, TimeZone, Timelike, Utc};
use serde::Deserialize;
use serde_json::Value;

pub const MAX_SNAPSHOTS: usize = 100;

#[derive(Clone, Debug, PartialEq)]
pub struct FileHistoryBackup {
    pub backup_file_name: Option<String>,
    pub version: Option<f64>,
    pub backup_time: DateTime<Utc>,
    pub failed: Option<bool>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FileHistorySnapshot {
    pub prompt_id: String,
    pub tracked_file_backups: HashMap<String, FileHistoryBackup>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SerializedFileHistorySnapshot {
    prompt_id: String,
    #[serde(default)]
    timestamp: Value,
    tracked_file_backups: HashMap<String, SerializedFileHistoryBackup>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SerializedFileHistoryBackup {
    #[serde(default)]
    backup_file_name: Option<String>,
    #[serde(default)]
    version: Option<f64>,
    #[serde(default)]
    backup_time: Value,
    #[serde(default)]
    failed: Option<bool>,
}

impl From<SerializedFileHistorySnapshot> for FileHistorySnapshot {
    fn from(snapshot: SerializedFileHistorySnapshot) -> Self {
        Self {
            prompt_id: snapshot.prompt_id,
            timestamp: safe_parse_date(&snapshot.timestamp),
            tracked_file_backups: snapshot
                .tracked_file_backups
                .into_iter()
                .map(|(path, backup)| {
                    (
                        path,
                        FileHistoryBackup {
                            backup_file_name: backup.backup_file_name,
                            version: backup.version,
                            backup_time: safe_parse_date(&backup.backup_time),
                            failed: backup.failed,
                        },
                    )
                })
                .collect(),
        }
    }
}

/// Converts a restored snapshot back to the JSON shape written by the
/// TypeScript `serializeSnapshot` helper. Dates use fixed millisecond UTC ISO
/// strings, and a false `failed` flag is omitted as in the source.
pub fn serialize_snapshot(snapshot: &FileHistorySnapshot) -> Value {
    let tracked_file_backups: serde_json::Map<String, Value> = snapshot
        .tracked_file_backups
        .iter()
        .map(|(path, backup)| {
            let mut value = serde_json::Map::new();
            value.insert(
                "backupFileName".to_owned(),
                backup
                    .backup_file_name
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            );
            if let Some(version) = backup.version {
                value.insert(
                    "version".to_owned(),
                    serde_json::Number::from_f64(version)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                );
            }
            value.insert(
                "backupTime".to_owned(),
                Value::String(format_date(backup.backup_time)),
            );
            if backup.failed == Some(true) {
                value.insert("failed".to_owned(), Value::Bool(true));
            }
            (path.clone(), Value::Object(value))
        })
        .collect();

    serde_json::json!({
        "promptId": snapshot.prompt_id,
        "timestamp": format_date(snapshot.timestamp),
        "trackedFileBackups": tracked_file_backups,
    })
}

fn format_date(date: DateTime<Utc>) -> String {
    date.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn safe_parse_date(value: &Value) -> DateTime<Utc> {
    let parsed = match value {
        Value::String(value) => parse_js_date_string(value),
        // JavaScript's Date constructor interprets numbers as milliseconds
        // since the Unix epoch. Serialized Canopy records normally contain
        // strings, but accepting numbers matches the runtime behavior too.
        Value::Number(value) => value.as_f64().and_then(|millis| {
            if millis.is_finite() && millis.abs() <= 8.64e15 {
                Utc.timestamp_millis_opt(millis.trunc() as i64).single()
            } else {
                None
            }
        }),
        Value::Null => Some(epoch()),
        Value::Bool(value) => Utc.timestamp_millis_opt(i64::from(*value)).single(),
        _ => None,
    };
    // Match safeParseDate: unsupported/invalid dates become epoch rather than
    // making the whole session restore fail.
    parsed.unwrap_or_else(epoch)
}

fn parse_js_date_string(value: &str) -> Option<DateTime<Utc>> {
    if let Ok(date) = DateTime::parse_from_rfc3339(value) {
        return Some(date.with_timezone(&Utc).with_nanosecond_zeroed_to_millis());
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Some(date.and_hms_opt(0, 0, 0)?.and_utc());
    }
    if let Ok(date) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(date.and_utc().with_nanosecond_zeroed_to_millis());
    }
    None
}

trait MillisecondPrecision {
    fn with_nanosecond_zeroed_to_millis(self) -> Self;
}

impl MillisecondPrecision for DateTime<Utc> {
    fn with_nanosecond_zeroed_to_millis(self) -> Self {
        let nanos = self.timestamp_subsec_nanos();
        self.with_nanosecond((nanos / 1_000_000) * 1_000_000)
            .expect("millisecond nanoseconds are valid")
    }
}

fn epoch() -> DateTime<Utc> {
    Utc.timestamp_opt(0, 0)
        .single()
        .expect("Unix epoch is representable")
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

/// Rebuilds bounded file-history state from session transcript records.
#[derive(Debug, Default)]
pub struct SessionFileHistoryAccumulator {
    seen_prompt_ids: HashSet<String>,
    retained_prompt_ids: VecDeque<String>,
    snapshots_by_prompt_id: HashMap<String, FileHistorySnapshot>,
}

impl SessionFileHistoryAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a transcript JSON value. Irrelevant records and payloads whose
    /// `snapshots` field is not an array are ignored. For matching records,
    /// serde validation errors are returned before any state is modified.
    pub fn add(&mut self, record: &Value) -> Result<(), serde_json::Error> {
        if record.get("type").and_then(Value::as_str) != Some("system")
            || record.get("subtype").and_then(Value::as_str) != Some("file_history_snapshot")
        {
            return Ok(());
        }
        let Some(payload) = record.get("systemPayload").filter(|value| js_truthy(value)) else {
            return Ok(());
        };
        let Some(snapshots) = payload.get("snapshots").and_then(Value::as_array) else {
            return Ok(());
        };

        // Decode the full batch up front. `deserializeSnapshots` maps the
        // array before the TypeScript accumulator starts applying items.
        let serialized: Vec<SerializedFileHistorySnapshot> =
            serde_json::from_value(Value::Array(snapshots.clone()))?;
        let batch: Vec<FileHistorySnapshot> = serialized.into_iter().map(Into::into).collect();

        for snapshot in batch {
            let prompt_id = snapshot.prompt_id.clone();
            if self.seen_prompt_ids.contains(&prompt_id) {
                if self.snapshots_by_prompt_id.contains_key(&prompt_id) {
                    self.snapshots_by_prompt_id.insert(prompt_id, snapshot);
                }
                continue;
            }

            self.seen_prompt_ids.insert(prompt_id.clone());
            self.retained_prompt_ids.push_back(prompt_id.clone());
            self.snapshots_by_prompt_id.insert(prompt_id, snapshot);
            if self.retained_prompt_ids.len() > MAX_SNAPSHOTS {
                if let Some(evicted_prompt_id) = self.retained_prompt_ids.pop_front() {
                    self.snapshots_by_prompt_id.remove(&evicted_prompt_id);
                }
            }
        }
        Ok(())
    }

    /// Returns snapshots in first-insertion order, or `None` when none were
    /// restored, matching the source's `undefined` result.
    pub fn finish(&self) -> Option<Vec<FileHistorySnapshot>> {
        if self.retained_prompt_ids.is_empty() {
            return None;
        }
        Some(
            self.retained_prompt_ids
                .iter()
                .filter_map(|prompt_id| self.snapshots_by_prompt_id.get(prompt_id).cloned())
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use serde_json::{Value, json};

    use super::{MAX_SNAPSHOTS, SessionFileHistoryAccumulator, serialize_snapshot};

    fn snapshot(prompt_id: String, timestamp: &str) -> Value {
        json!({
            "promptId": prompt_id,
            "timestamp": timestamp,
            "trackedFileBackups": {},
        })
    }

    fn snapshot_record(snapshots: Vec<Value>) -> Value {
        json!({
            "type": "system",
            "subtype": "file_history_snapshot",
            "systemPayload": { "snapshots": snapshots },
        })
    }

    fn date(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("valid test date")
            .with_timezone(&Utc)
    }

    #[test]
    fn keeps_final_hundred_insertion_slots_and_replaces_retained_values() {
        let mut accumulator = SessionFileHistoryAccumulator::new();
        accumulator
            .add(&snapshot_record(
                (0..=MAX_SNAPSHOTS)
                    .map(|index| {
                        snapshot(
                            format!("prompt-{index}"),
                            &format!("2026-01-01T00:00:{:02}.000Z", index % 60),
                        )
                    })
                    .collect(),
            ))
            .expect("well-formed snapshots");
        accumulator
            .add(&snapshot_record(vec![
                snapshot("prompt-0".to_owned(), "2026-02-01T00:00:00.000Z"),
                snapshot("prompt-50".to_owned(), "2026-03-01T00:00:00.000Z"),
            ]))
            .expect("well-formed replacements");

        let restored = accumulator.finish().expect("non-empty state");
        assert_eq!(restored.len(), MAX_SNAPSHOTS);
        assert_eq!(restored[0].prompt_id, "prompt-1");
        assert_eq!(restored[MAX_SNAPSHOTS - 1].prompt_id, "prompt-100");
        assert_eq!(restored[49].timestamp, date("2026-03-01T00:00:00.000Z"));

        // An evicted ID remains in seenPromptIds and cannot be reinserted.
        accumulator
            .add(&snapshot_record(vec![snapshot(
                "prompt-0".to_owned(),
                "2026-04-01T00:00:00.000Z",
            )]))
            .expect("well-formed duplicate");
        assert_eq!(accumulator.finish().expect("still non-empty").len(), 100);
        assert!(
            accumulator
                .finish()
                .expect("still non-empty")
                .iter()
                .all(|item| item.prompt_id != "prompt-0")
        );
    }

    #[test]
    fn malformed_batch_does_not_partially_apply() {
        let mut accumulator = SessionFileHistoryAccumulator::new();
        accumulator
            .add(&snapshot_record(vec![snapshot(
                "before".to_owned(),
                "2026-01-01T00:00:00.000Z",
            )]))
            .expect("initial record");

        let malformed = snapshot_record(vec![
            snapshot("partial".to_owned(), "2026-01-02T00:00:00.000Z"),
            json!({
                "promptId": "malformed",
                "timestamp": "2026-01-03T00:00:00.000Z",
                "trackedFileBackups": null,
            }),
        ]);
        assert!(accumulator.add(&malformed).is_err());
        assert_eq!(
            accumulator
                .finish()
                .expect("initial state remains")
                .iter()
                .map(|item| item.prompt_id.as_str())
                .collect::<Vec<_>>(),
            vec!["before"]
        );
    }

    #[test]
    fn skips_irrelevant_records_and_non_array_snapshot_payloads() {
        let mut accumulator = SessionFileHistoryAccumulator::new();
        accumulator
            .add(&json!({"type": "user", "subtype": "file_history_snapshot"}))
            .expect("irrelevant record");
        accumulator
            .add(&json!({
                "type": "system",
                "subtype": "file_history_snapshot",
                "systemPayload": {"snapshots": "not-an-array"}
            }))
            .expect("non-array snapshots are ignored");
        assert_eq!(accumulator.finish(), None);
    }

    #[test]
    fn invalid_dates_fall_back_to_epoch_and_serialization_uses_iso_millis() {
        let mut accumulator = SessionFileHistoryAccumulator::new();
        accumulator
            .add(&snapshot_record(vec![json!({
                "promptId": "p",
                "timestamp": "not a date",
                "trackedFileBackups": {
                    "file.txt": {
                        "backupFileName": null,
                        "version": 2,
                        "backupTime": "2026-01-02T03:04:05.123456Z",
                        "failed": false
                    }
                }
            })]))
            .expect("well-formed record with invalid date strings");

        let restored = accumulator.finish().expect("snapshot restored");
        assert_eq!(restored[0].timestamp, super::epoch());
        let serialized = serialize_snapshot(&restored[0]);
        assert_eq!(serialized["timestamp"], "1970-01-01T00:00:00.000Z");
        assert_eq!(
            serialized["trackedFileBackups"]["file.txt"]["backupTime"],
            "2026-01-02T03:04:05.123Z"
        );
        assert!(
            serialized["trackedFileBackups"]["file.txt"]
                .get("failed")
                .is_none()
        );
    }

    #[test]
    fn missing_snapshots_finish_as_none() {
        assert_eq!(SessionFileHistoryAccumulator::new().finish(), None);
    }
}
