use serde_json::{Value, json};

use super::protocol::{
    GOAL_STATE_VERSION, GoalActivity, GoalRecord, GoalSnapshotV2, GoalStateCause,
    GoalStateRecordPayloadV2, GoalStatus, TranscriptCursor,
};
use super::reducer::parse_goal_state_record_payload_v2;

const LEGACY_ACTIVE_KINDS: &[&str] = &["set", "checking"];
const LEGACY_STOPPED_KINDS: &[&str] = &["achieved", "cleared", "failed", "aborted", "paused"];

#[derive(Clone, Debug, PartialEq)]
pub enum GoalRecovery {
    V2 {
        payload: Box<GoalStateRecordPayloadV2>,
    },
    Legacy {
        objective: String,
    },
    Unsupported {
        reason: String,
    },
    None,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GoalRecoveryRecord {
    pub uuid: String,
    pub record_type: String,
    pub subtype: Option<String>,
    pub system_payload: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GoalRecoverySelection {
    pub recovery: GoalRecovery,
    pub source_uuid: Option<String>,
}

pub fn recover_goal_from_records(records: &[GoalRecoveryRecord]) -> GoalRecovery {
    select_goal_recovery_from_records(records).recovery
}

pub fn select_goal_recovery_from_records(records: &[GoalRecoveryRecord]) -> GoalRecoverySelection {
    let mut unsupported: Option<(GoalRecovery, String)> = None;
    for record in records.iter().rev() {
        if record.subtype.as_deref() != Some("goal_state") {
            continue;
        }
        let payload = (record.record_type == "system")
            .then_some(record.system_payload.as_ref())
            .flatten()
            .and_then(parse_goal_state_record_payload_v2);
        if let Some(payload) = payload {
            return GoalRecoverySelection {
                recovery: GoalRecovery::V2 {
                    payload: Box::new(payload),
                },
                source_uuid: Some(record.uuid.clone()),
            };
        }
        if unsupported.is_none() {
            unsupported = Some((
                GoalRecovery::Unsupported {
                    reason: format!(
                        "Goal lifecycle record {} is malformed or uses an unsupported version",
                        record.uuid
                    ),
                },
                record.uuid.clone(),
            ));
        }
    }

    if let Some((recovery, source_uuid)) = unsupported {
        return GoalRecoverySelection {
            recovery,
            source_uuid: Some(source_uuid),
        };
    }
    recover_legacy_goal(records)
}

fn recover_legacy_goal(records: &[GoalRecoveryRecord]) -> GoalRecoverySelection {
    for record in records.iter().rev() {
        if record.record_type != "system" || record.subtype.as_deref() != Some("slash_command") {
            continue;
        }
        let Some(payload) = record.system_payload.as_ref().and_then(Value::as_object) else {
            continue;
        };
        if payload.get("phase").and_then(Value::as_str) != Some("result") {
            continue;
        }
        let Some(items) = payload.get("outputHistoryItems").and_then(Value::as_array) else {
            continue;
        };
        for item in items.iter().rev() {
            let Some(item) = item.as_object() else {
                continue;
            };
            if item.get("type").and_then(Value::as_str) != Some("goal_status") {
                continue;
            }
            let kind = item.get("kind").and_then(Value::as_str);
            let condition = item.get("condition").and_then(Value::as_str);
            let (Some(kind), Some(condition)) = (kind, condition) else {
                return unsupported_legacy(record);
            };
            if LEGACY_STOPPED_KINDS.contains(&kind) {
                return GoalRecoverySelection {
                    recovery: GoalRecovery::None,
                    source_uuid: Some(record.uuid.clone()),
                };
            }
            if !LEGACY_ACTIVE_KINDS.contains(&kind) || condition.trim().is_empty() {
                return unsupported_legacy(record);
            }
            return GoalRecoverySelection {
                recovery: GoalRecovery::Legacy {
                    objective: condition.trim().to_owned(),
                },
                source_uuid: Some(record.uuid.clone()),
            };
        }
    }
    GoalRecoverySelection {
        recovery: GoalRecovery::None,
        source_uuid: None,
    }
}

fn unsupported_legacy(record: &GoalRecoveryRecord) -> GoalRecoverySelection {
    GoalRecoverySelection {
        recovery: GoalRecovery::Unsupported {
            reason: format!(
                "Legacy Goal record {} cannot be recovered safely",
                record.uuid
            ),
        },
        source_uuid: Some(record.uuid.clone()),
    }
}

pub fn normalize_goal_recovery_record(record: &GoalRecoveryRecord) -> Option<GoalRecoveryRecord> {
    if record.subtype.as_deref() == Some("goal_state") {
        let system_payload = Some(if record.record_type == "system" {
            record
                .system_payload
                .as_ref()
                .and_then(parse_goal_state_record_payload_v2)
                .and_then(|payload| serde_json::to_value(payload).ok())
                .unwrap_or(Value::Null)
        } else {
            Value::Null
        });
        return Some(GoalRecoveryRecord {
            uuid: record.uuid.clone(),
            record_type: record.record_type.clone(),
            subtype: record.subtype.clone(),
            system_payload,
        });
    }
    if record.record_type != "system" || record.subtype.as_deref() != Some("slash_command") {
        return None;
    }
    let payload = record.system_payload.as_ref()?.as_object()?;
    if payload.get("phase").and_then(Value::as_str) != Some("result") {
        return None;
    }
    let items = payload.get("outputHistoryItems")?.as_array()?;
    let goal_status_items = items
        .iter()
        .filter(|value| {
            value
                .as_object()
                .and_then(|item| item.get("type"))
                .and_then(Value::as_str)
                == Some("goal_status")
        })
        .cloned()
        .collect::<Vec<_>>();
    if goal_status_items.is_empty() {
        return None;
    }
    Some(GoalRecoveryRecord {
        uuid: record.uuid.clone(),
        record_type: record.record_type.clone(),
        subtype: record.subtype.clone(),
        system_payload: Some(json!({
            "phase":"result",
            "outputHistoryItems":goal_status_items,
        })),
    })
}

pub fn is_goal_recovery_candidate(record: &GoalRecoveryRecord) -> bool {
    if record.subtype.as_deref() == Some("goal_state") {
        return true;
    }
    if record.record_type != "system" || record.subtype.as_deref() != Some("slash_command") {
        return false;
    }
    let Some(payload) = record.system_payload.as_ref().and_then(Value::as_object) else {
        return false;
    };
    payload.get("phase").and_then(Value::as_str) == Some("result")
        && payload
            .get("outputHistoryItems")
            .and_then(Value::as_array)
            .is_some_and(|items| items.iter().any(is_goal_status_item))
}

fn is_goal_status_item(value: &Value) -> bool {
    value
        .as_object()
        .and_then(|item| item.get("type"))
        .and_then(Value::as_str)
        == Some("goal_status")
}

#[derive(Clone, Debug, PartialEq)]
pub struct MigratedGoalStateInput {
    pub objective: String,
    pub goal_id: String,
    pub record_uuid: String,
    pub now: f64,
}

pub fn create_migrated_goal_state(
    input: &MigratedGoalStateInput,
) -> Result<GoalStateRecordPayloadV2, String> {
    let objective = input.objective.trim();
    if objective.is_empty() {
        return Err("Migrated Goal objective must not be empty".to_owned());
    }
    Ok(GoalStateRecordPayloadV2 {
        v: GOAL_STATE_VERSION,
        cause: GoalStateCause::Migrated,
        snapshot: GoalSnapshotV2 {
            v: GOAL_STATE_VERSION,
            activity: GoalActivity::Idle,
            goal: Some(GoalRecord {
                goal_id: input.goal_id.clone(),
                revision: 1,
                objective: objective.to_owned(),
                status: GoalStatus::Paused,
                evidence_cursor: TranscriptCursor {
                    record_id: Some(input.record_uuid.clone()),
                },
                turn_count: 0,
                active_time_ms: 0.0,
                created_at: input.now,
                updated_at: input.now,
                evidence_checkpoint: None,
                last_reason: None,
                limit_kind: None,
            }),
        },
        checkpoint_pending: None,
        blocked_audit: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v2_payload(cause: &str, objective: &str, status: &str) -> Value {
        json!({
            "v":2,"cause":cause,
            "snapshot":{"v":2,"activity":"idle","goal":{
                "goalId":"g-1","revision":1,"objective":objective,"status":status,
                "evidenceCursor":{"recordId":"r-1"},"turnCount":0,"activeTimeMs":0,
                "createdAt":1,"updatedAt":1
            }}
        })
    }

    fn record(
        uuid: &str,
        subtype: Option<&str>,
        system_payload: Option<Value>,
    ) -> GoalRecoveryRecord {
        GoalRecoveryRecord {
            uuid: uuid.to_owned(),
            record_type: "system".to_owned(),
            subtype: subtype.map(str::to_owned),
            system_payload,
        }
    }

    #[test]
    fn selects_latest_valid_v2_record_and_skips_newer_malformed_records() {
        let valid = v2_payload("pause", "ship", "paused");
        let records = vec![
            record("state-1", Some("goal_state"), Some(valid.clone())),
            record(
                "state-2",
                Some("goal_state"),
                Some(json!({"v":3,"snapshot":{}})),
            ),
        ];
        let selected = select_goal_recovery_from_records(&records);
        assert_eq!(selected.source_uuid.as_deref(), Some("state-1"));
        assert!(matches!(selected.recovery, GoalRecovery::V2 { .. }));
    }

    #[test]
    fn malformed_v2_record_is_unsupported_instead_of_falling_back_to_legacy() {
        let legacy = record(
            "legacy",
            Some("slash_command"),
            Some(json!({
                "phase":"result",
                "outputHistoryItems":[{"type":"goal_status","kind":"set","condition":"old"}]
            })),
        );
        let malformed = record("bad-state", Some("goal_state"), Some(json!({"v":3})));
        let selected = select_goal_recovery_from_records(&[legacy, malformed]);
        assert_eq!(selected.source_uuid.as_deref(), Some("bad-state"));
        assert_eq!(
            selected.recovery,
            GoalRecovery::Unsupported {
                reason:
                    "Goal lifecycle record bad-state is malformed or uses an unsupported version"
                        .to_owned()
            }
        );
    }

    #[test]
    fn non_system_lifecycle_record_is_unsupported_and_does_not_revive_legacy_state() {
        let legacy = record(
            "legacy",
            Some("slash_command"),
            Some(json!({
                "phase":"result",
                "outputHistoryItems":[{"type":"goal_status","kind":"set","condition":"old"}]
            })),
        );
        let mut non_system = record(
            "user-state",
            Some("goal_state"),
            Some(v2_payload("pause", "x", "paused")),
        );
        non_system.record_type = "user".to_owned();
        let selected = select_goal_recovery_from_records(&[legacy, non_system]);
        assert_eq!(selected.source_uuid.as_deref(), Some("user-state"));
        assert_eq!(
            selected.recovery,
            GoalRecovery::Unsupported {
                reason:
                    "Goal lifecycle record user-state is malformed or uses an unsupported version"
                        .to_owned()
            }
        );
    }

    #[test]
    fn legacy_projection_recovers_only_latest_active_or_stopped_status() {
        let active = record(
            "legacy-active",
            Some("slash_command"),
            Some(json!({
                "phase":"result",
                "outputHistoryItems":[{"type":"goal_status","kind":"checking","condition":"  ship it  "}]
            })),
        );
        assert_eq!(
            recover_goal_from_records(&[active]),
            GoalRecovery::Legacy {
                objective: "ship it".to_owned()
            }
        );

        let stopped = record(
            "legacy-stopped",
            Some("slash_command"),
            Some(json!({
                "phase":"result",
                "outputHistoryItems":[{"type":"goal_status","kind":"paused","condition":"ship it"}]
            })),
        );
        assert_eq!(recover_goal_from_records(&[stopped]), GoalRecovery::None);
    }

    #[test]
    fn malformed_latest_legacy_goal_status_is_not_skipped() {
        let malformed = record(
            "legacy-bad",
            Some("slash_command"),
            Some(json!({
                "phase":"result",
                "outputHistoryItems":[{"type":"goal_status","kind":"checking","condition":" "}]
            })),
        );
        assert_eq!(
            recover_goal_from_records(&[malformed]),
            GoalRecovery::Unsupported {
                reason: "Legacy Goal record legacy-bad cannot be recovered safely".to_owned()
            }
        );
    }

    #[test]
    fn normalize_and_candidate_keep_only_goal_recovery_projection() {
        let legacy_record = record(
            "legacy",
            Some("slash_command"),
            Some(json!({
                "phase":"result","rawCommand":"/goal",
                "outputHistoryItems":[
                    {"type":"text","text":"skip"},
                    {"type":"goal_status","kind":"set","condition":"ship"}
                ]
            })),
        );
        assert!(is_goal_recovery_candidate(&legacy_record));
        let normalized = normalize_goal_recovery_record(&legacy_record).unwrap();
        assert_eq!(
            normalized.system_payload.unwrap(),
            json!({
                "phase":"result",
                "outputHistoryItems":[{"type":"goal_status","kind":"set","condition":"ship"}]
            })
        );

        let lifecycle = record(
            "state",
            Some("goal_state"),
            Some(v2_payload("pause", "ship", "paused")),
        );
        let normalized = normalize_goal_recovery_record(&lifecycle).unwrap();
        assert!(
            normalized
                .system_payload
                .as_ref()
                .and_then(parse_goal_state_record_payload_v2)
                .is_some()
        );
    }

    #[test]
    fn migration_creates_a_paused_v2_goal_and_rejects_empty_objectives() {
        let migrated = create_migrated_goal_state(&MigratedGoalStateInput {
            objective: " ship ".to_owned(),
            goal_id: "g-migrated".to_owned(),
            record_uuid: "migration-record".to_owned(),
            now: 10.0,
        })
        .unwrap();
        let goal = migrated.snapshot.goal.unwrap();
        assert_eq!(migrated.cause, GoalStateCause::Migrated);
        assert_eq!(goal.status, GoalStatus::Paused);
        assert_eq!(goal.objective, "ship");
        assert_eq!(
            goal.evidence_cursor.record_id.as_deref(),
            Some("migration-record")
        );
        assert!(
            create_migrated_goal_state(&MigratedGoalStateInput {
                objective: " ".to_owned(),
                goal_id: "g".to_owned(),
                record_uuid: "record".to_owned(),
                now: 1.0,
            })
            .is_err()
        );
    }
}
