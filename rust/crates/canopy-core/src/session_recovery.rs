//! Decide how a persisted session can safely resume after process exit.
//!
//! This mirrors `packages/core/src/core/session-recovery.ts`: detect the tail
//! interruption against the original history, repair a separate API history,
//! and require confirmation for tool interruptions or damaged ancestry.

use serde_json::Value;

use crate::session_api_history::{
    BuildApiHistoryOptions, SessionApiHistoryError, build_api_history_from_conversation,
};
use crate::tool_repair::{ORPHAN_TOOL_USE_REPAIR_REASON, repair_orphaned_tool_use_turns};
use crate::turn_interruption::{
    TurnInterruption, build_synthetic_tool_response_parts, detect_turn_interruption,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SessionRecoveryOptions {
    pub allow_auto_continue: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryGap {
    pub child_uuid: String,
    pub missing_parent_uuid: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionRecoveryKind {
    Clean,
    InterruptedPrompt,
    InterruptedTurn,
    DegradedHistory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryRepair {
    SynthesizedToolResult {
        call_id: String,
        name: String,
    },
    DroppedDuplicateToolResult {
        call_id: String,
        name: String,
    },
    UncertainToolEffect {
        call_id: String,
        name: String,
    },
    HistoryGap {
        child_uuid: String,
        missing_parent_uuid: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionRecoveryContinuationMode {
    RetryUserParts,
    ToolResultParts,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionRecoveryContinuation {
    pub mode: SessionRecoveryContinuationMode,
    pub parts: Vec<Value>,
    pub display_text: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionRecoveryPlan {
    pub plan_id: String,
    pub session_id: String,
    pub kind: SessionRecoveryKind,
    pub original_api_history: Vec<Value>,
    pub api_history: Vec<Value>,
    pub repairs: Vec<RecoveryRepair>,
    pub can_continue: bool,
    pub can_auto_continue: bool,
    pub requires_user_confirmation: bool,
    pub visible_notice: Option<String>,
    pub continuation: Option<SessionRecoveryContinuation>,
}

fn visible_notice(
    kind: SessionRecoveryKind,
    repairs: &[RecoveryRepair],
    history_gaps: &[HistoryGap],
) -> Option<String> {
    let uncertain_effects = repairs
        .iter()
        .filter(|repair| matches!(repair, RecoveryRepair::UncertainToolEffect { .. }))
        .count();
    if uncertain_effects > 0 {
        return Some(format!(
            "Previous session stopped with {uncertain_effects} tool side effect(s) whose result was not saved. Inspect the workspace before retrying those tools."
        ));
    }

    match kind {
        SessionRecoveryKind::Clean => None,
        SessionRecoveryKind::DegradedHistory => Some(format!(
            "Resumed session history is incomplete: detected {} missing parent link(s). Automatic continuation is disabled for this recovery.",
            history_gaps.len()
        )),
        SessionRecoveryKind::InterruptedPrompt => Some(
            "Previous session appears to have stopped after user input before the model completed a response."
                .to_owned(),
        ),
        SessionRecoveryKind::InterruptedTurn => {
            let synthesized = repairs
                .iter()
                .filter(|repair| matches!(repair, RecoveryRepair::SynthesizedToolResult { .. }))
                .count();
            if synthesized > 0 {
                Some(format!(
                    "Previous session appears to have stopped during tool execution. Synthesized {synthesized} failed tool result(s) so the history can continue safely."
                ))
            } else {
                Some("Previous session appears to have stopped during tool execution.".to_owned())
            }
        }
    }
}

/// Rebuild API history from persisted conversation records, then create a
/// recovery plan. Records are consumed to avoid copying large tool outputs.
pub fn build_session_recovery_plan(
    session_id: impl Into<String>,
    conversation_records: impl IntoIterator<Item = Value>,
    history_gaps: &[HistoryGap],
    options: SessionRecoveryOptions,
) -> Result<SessionRecoveryPlan, SessionApiHistoryError> {
    let api_history = build_api_history_from_conversation(
        conversation_records,
        BuildApiHistoryOptions::default(),
    )?;
    Ok(build_session_recovery_plan_from_api_history(
        session_id,
        api_history,
        history_gaps,
        options,
    ))
}

/// Build a recovery plan from provider-facing history. The input is consumed;
/// the returned plan keeps the original and repaired histories independently.
pub fn build_session_recovery_plan_from_api_history(
    session_id: impl Into<String>,
    original_api_history: Vec<Value>,
    history_gaps: &[HistoryGap],
    options: SessionRecoveryOptions,
) -> SessionRecoveryPlan {
    let session_id = session_id.into();
    let plan_id = format!("{}:{}", session_id, original_api_history.len());
    let mut api_history = original_api_history.clone();
    let repair_result = repair_orphaned_tool_use_turns(&mut api_history);

    let mut repairs: Vec<RecoveryRepair> = repair_result
        .injected
        .into_iter()
        .map(|repair| RecoveryRepair::SynthesizedToolResult {
            call_id: repair.call_id,
            name: repair.name,
        })
        .collect();
    repairs.extend(repair_result.dropped_duplicates.into_iter().map(|repair| {
        RecoveryRepair::DroppedDuplicateToolResult {
            call_id: repair.call_id,
            name: repair.name,
        }
    }));
    repairs.extend(history_gaps.iter().map(|gap| RecoveryRepair::HistoryGap {
        child_uuid: gap.child_uuid.clone(),
        missing_parent_uuid: gap.missing_parent_uuid.clone(),
    }));

    if !history_gaps.is_empty() {
        return SessionRecoveryPlan {
            plan_id,
            session_id,
            kind: SessionRecoveryKind::DegradedHistory,
            original_api_history,
            api_history,
            repairs,
            can_continue: false,
            can_auto_continue: false,
            requires_user_confirmation: true,
            visible_notice: visible_notice(SessionRecoveryKind::DegradedHistory, &[], history_gaps),
            continuation: None,
        };
    }

    match detect_turn_interruption(&original_api_history) {
        TurnInterruption::None => SessionRecoveryPlan {
            plan_id,
            session_id,
            kind: SessionRecoveryKind::Clean,
            original_api_history,
            api_history,
            repairs,
            can_continue: false,
            can_auto_continue: false,
            requires_user_confirmation: false,
            visible_notice: None,
            continuation: None,
        },
        TurnInterruption::InterruptedPrompt { parts } => {
            let kind = SessionRecoveryKind::InterruptedPrompt;
            let notice = visible_notice(kind, &repairs, history_gaps);
            SessionRecoveryPlan {
                plan_id,
                session_id,
                kind,
                original_api_history,
                api_history,
                repairs,
                can_continue: true,
                can_auto_continue: options.allow_auto_continue,
                requires_user_confirmation: !options.allow_auto_continue,
                visible_notice: notice,
                continuation: Some(SessionRecoveryContinuation {
                    mode: SessionRecoveryContinuationMode::RetryUserParts,
                    parts,
                    display_text: "Continue interrupted user prompt".to_owned(),
                }),
            }
        }
        TurnInterruption::InterruptedTurn { dangling_calls } => {
            let kind = SessionRecoveryKind::InterruptedTurn;
            let notice = visible_notice(kind, &repairs, history_gaps);
            let continuation_parts =
                build_synthetic_tool_response_parts(&dangling_calls, ORPHAN_TOOL_USE_REPAIR_REASON);
            SessionRecoveryPlan {
                plan_id,
                session_id,
                kind,
                original_api_history,
                api_history,
                repairs,
                can_continue: true,
                can_auto_continue: false,
                requires_user_confirmation: true,
                visible_notice: notice,
                continuation: Some(SessionRecoveryContinuation {
                    mode: SessionRecoveryContinuationMode::ToolResultParts,
                    parts: continuation_parts,
                    display_text: "Continue interrupted tool turn".to_owned(),
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn completed_model_tail_is_clean() {
        let plan = build_session_recovery_plan_from_api_history(
            "session-1",
            vec![
                json!({"role":"user","parts":[{"text":"hello"}]}),
                json!({"role":"model","parts":[{"text":"done"}]}),
            ],
            &[],
            SessionRecoveryOptions::default(),
        );
        assert_eq!(plan.kind, SessionRecoveryKind::Clean);
        assert!(!plan.can_continue);
        assert!(plan.repairs.is_empty());
        assert!(plan.visible_notice.is_none());
    }

    #[test]
    fn interrupted_prompt_needs_confirmation_unless_enabled() {
        let history = vec![json!({"role":"user","parts":[{"text":"do it"}]})];
        let plan = build_session_recovery_plan_from_api_history(
            "s",
            history.clone(),
            &[],
            SessionRecoveryOptions::default(),
        );
        assert_eq!(plan.kind, SessionRecoveryKind::InterruptedPrompt);
        assert!(plan.can_continue);
        assert!(!plan.can_auto_continue);
        assert!(plan.requires_user_confirmation);
        assert_eq!(
            plan.continuation.unwrap().parts,
            vec![json!({"text":"do it"})]
        );

        let auto = build_session_recovery_plan_from_api_history(
            "s",
            history,
            &[],
            SessionRecoveryOptions {
                allow_auto_continue: true,
            },
        );
        assert!(auto.can_auto_continue);
        assert!(!auto.requires_user_confirmation);
    }

    #[test]
    fn interrupted_tool_tail_is_repaired_but_still_requires_confirmation() {
        let plan = build_session_recovery_plan_from_api_history(
            "session-1",
            vec![
                json!({"role":"model","parts":[{"functionCall":{"id":"c1","name":"read_file","args":{}}}]}),
            ],
            &[],
            SessionRecoveryOptions {
                allow_auto_continue: true,
            },
        );
        assert_eq!(plan.kind, SessionRecoveryKind::InterruptedTurn);
        assert!(!plan.can_auto_continue);
        assert!(plan.requires_user_confirmation);
        assert_eq!(plan.original_api_history.len(), 1);
        assert_eq!(
            plan.api_history[1]["parts"][0]["functionResponse"]["id"],
            "c1"
        );
        assert!(matches!(
            plan.repairs[0],
            RecoveryRepair::SynthesizedToolResult { .. }
        ));
        assert!(matches!(
            plan.continuation.unwrap().mode,
            SessionRecoveryContinuationMode::ToolResultParts
        ));
    }

    #[test]
    fn history_gaps_override_continuation_and_report_degraded_history() {
        let plan = build_session_recovery_plan_from_api_history(
            "session-1",
            vec![json!({"role":"user","parts":[{"text":"unfinished"}]})],
            &[HistoryGap {
                child_uuid: "child".into(),
                missing_parent_uuid: "missing".into(),
            }],
            SessionRecoveryOptions {
                allow_auto_continue: true,
            },
        );
        assert_eq!(plan.kind, SessionRecoveryKind::DegradedHistory);
        assert!(!plan.can_continue);
        assert!(!plan.can_auto_continue);
        assert!(plan.requires_user_confirmation);
        assert!(plan.continuation.is_none());
        assert!(
            plan.visible_notice
                .unwrap()
                .contains("1 missing parent link(s)")
        );
        assert!(matches!(
            plan.repairs.last(),
            Some(RecoveryRepair::HistoryGap { .. })
        ));
    }

    #[test]
    fn conversation_builder_uses_api_history_reconstruction() {
        let plan = build_session_recovery_plan(
            "s",
            vec![json!({"type":"user","message":{"role":"user","parts":[{"text":"prompt"}]}})],
            &[],
            SessionRecoveryOptions::default(),
        )
        .unwrap();
        assert_eq!(plan.kind, SessionRecoveryKind::InterruptedPrompt);
        assert_eq!(plan.plan_id, "s:1");
    }
}
