use crate::goals::evidence::ValidatedGoalEvidenceRecord;
use crate::goals::protocol::{
    GOAL_CHECKPOINT_CLAIM_LIMIT, GOAL_CHECKPOINT_CLAIM_MAX_BYTES,
    GOAL_CHECKPOINT_CLAIM_MAX_CHARACTERS, GOAL_CHECKPOINT_SOURCE_REFERENCE_LIMIT,
    GoalEvidenceCheckpoint, GoalEvidenceCheckpointClaim, GoalEvidenceProofKind, GoalRecord,
};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct GoalCheckpointVerifierInput {
    pub goal: GoalCheckpointVerifierGoal,
    pub previous_claims: Vec<GoalEvidenceCheckpointClaim>,
    pub evidence: Vec<ValidatedGoalEvidenceRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoalCheckpointVerifierGoal {
    pub goal_id: String,
    pub revision: u64,
    pub objective: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoalCheckpointVerifierClaim {
    pub proof_kind: GoalEvidenceProofKind,
    pub claim: String,
    pub source_refs: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GoalCheckpointVerificationResult {
    pub claims: Vec<GoalCheckpointVerifierClaim>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{message}")]
pub struct InvalidGoalCheckpointError {
    pub message: String,
}

impl InvalidGoalCheckpointError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Assign Core-owned claim IDs and verify that claims preserve the proof kind
/// of every cited prior claim or transcript evidence record.
pub fn materialize_goal_evidence_checkpoint(
    checkpoint_id: impl Into<String>,
    created_at: f64,
    previous_claims: &[GoalEvidenceCheckpointClaim],
    evidence: &[ValidatedGoalEvidenceRecord],
    result: &GoalCheckpointVerificationResult,
) -> Result<GoalEvidenceCheckpoint, InvalidGoalCheckpointError> {
    if result.claims.is_empty() || result.claims.len() > GOAL_CHECKPOINT_CLAIM_LIMIT {
        return Err(InvalidGoalCheckpointError::new(format!(
            "Goal checkpoint must contain between 1 and {GOAL_CHECKPOINT_CLAIM_LIMIT} claims"
        )));
    }

    let mut sources = std::collections::HashMap::new();
    for claim in previous_claims {
        sources.insert(claim.id.as_str(), claim.proof_kind);
    }
    for record in evidence {
        sources.insert(record.uuid.as_str(), record.proof_kind);
    }

    let mut checkpoint_bytes = 0;
    let mut claims = Vec::with_capacity(result.claims.len());
    let checkpoint_id = checkpoint_id.into();
    for (index, verifier_claim) in result.claims.iter().enumerate() {
        let claim = trim_js(&verifier_claim.claim);
        if claim.is_empty() || claim.chars().count() > GOAL_CHECKPOINT_CLAIM_MAX_CHARACTERS {
            return Err(InvalidGoalCheckpointError::new(format!(
                "Goal checkpoint claim {} has an invalid length",
                index + 1
            )));
        }
        let source_refs = &verifier_claim.source_refs;
        if source_refs.is_empty()
            || source_refs.len() > GOAL_CHECKPOINT_SOURCE_REFERENCE_LIMIT
            || source_refs.iter().any(String::is_empty)
            || source_refs
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != source_refs.len()
        {
            return Err(InvalidGoalCheckpointError::new(format!(
                "Goal checkpoint claim {} has invalid source references",
                index + 1
            )));
        }
        for reference in source_refs {
            let Some(source_proof_kind) = sources.get(reference.as_str()) else {
                return Err(InvalidGoalCheckpointError::new(format!(
                    "Goal checkpoint claim {} cites unknown source {reference}",
                    index + 1
                )));
            };
            if *source_proof_kind != verifier_claim.proof_kind {
                return Err(InvalidGoalCheckpointError::new(format!(
                    "Goal checkpoint claim {} changes the proof kind of source {reference}",
                    index + 1
                )));
            }
        }
        checkpoint_bytes += claim.len();
        claims.push(GoalEvidenceCheckpointClaim {
            id: format!("{checkpoint_id}:{}", index + 1),
            proof_kind: verifier_claim.proof_kind,
            claim: claim.to_owned(),
            source_refs: source_refs.clone(),
        });
    }
    if checkpoint_bytes > GOAL_CHECKPOINT_CLAIM_MAX_BYTES {
        return Err(InvalidGoalCheckpointError::new(format!(
            "Goal checkpoint exceeds the {GOAL_CHECKPOINT_CLAIM_MAX_BYTES}-byte claim limit"
        )));
    }
    Ok(GoalEvidenceCheckpoint {
        checkpoint_id,
        created_at,
        claims,
    })
}

pub fn goal_checkpoint_verifier_input(
    goal: &GoalRecord,
    window: &crate::goals::evidence::GoalEvidenceCheckpointWindow,
) -> GoalCheckpointVerifierInput {
    GoalCheckpointVerifierInput {
        goal: GoalCheckpointVerifierGoal {
            goal_id: goal.goal_id.clone(),
            revision: goal.revision,
            objective: goal.objective.clone(),
        },
        previous_claims: window.previous_claims.clone(),
        evidence: window.evidence.clone(),
    }
}

fn trim_js(value: &str) -> &str {
    value.trim_matches(|character: char| character.is_whitespace() || character == '\u{feff}')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goals::evidence::GoalEvidenceProvenance;

    fn evidence() -> Vec<ValidatedGoalEvidenceRecord> {
        vec![ValidatedGoalEvidenceRecord {
            uuid: "assistant-1".to_owned(),
            provenance: GoalEvidenceProvenance::AssistantOutput,
            turn_id: "turn-1".to_owned(),
            preview: "Delivered result".to_owned(),
            proof_kind: GoalEvidenceProofKind::DeliveredOutput,
            content: "Delivered result".to_owned(),
        }]
    }

    #[test]
    fn assigns_checkpoint_ids_after_validating_sources() {
        let checkpoint = materialize_goal_evidence_checkpoint(
            "checkpoint-1",
            42.0,
            &[],
            &evidence(),
            &GoalCheckpointVerificationResult {
                claims: vec![GoalCheckpointVerifierClaim {
                    proof_kind: GoalEvidenceProofKind::DeliveredOutput,
                    claim: "  The result was delivered.\n".to_owned(),
                    source_refs: vec!["assistant-1".to_owned()],
                }],
            },
        )
        .unwrap();
        assert_eq!(checkpoint.claims[0].id, "checkpoint-1:1");
        assert_eq!(checkpoint.claims[0].claim, "The result was delivered.");
    }

    #[test]
    fn rejects_unknown_sources_and_proof_kind_changes() {
        let unknown = GoalCheckpointVerificationResult {
            claims: vec![GoalCheckpointVerifierClaim {
                proof_kind: GoalEvidenceProofKind::DeliveredOutput,
                claim: "Unknown result".to_owned(),
                source_refs: vec!["missing".to_owned()],
            }],
        };
        assert!(
            materialize_goal_evidence_checkpoint("cp", 1.0, &[], &evidence(), &unknown)
                .unwrap_err()
                .message
                .contains("cites unknown source missing")
        );
        let changed = GoalCheckpointVerificationResult {
            claims: vec![GoalCheckpointVerifierClaim {
                proof_kind: GoalEvidenceProofKind::ExternalFact,
                claim: "The implementation was verified".to_owned(),
                source_refs: vec!["assistant-1".to_owned()],
            }],
        };
        assert!(
            materialize_goal_evidence_checkpoint("cp", 1.0, &[], &evidence(), &changed)
                .unwrap_err()
                .message
                .contains("changes the proof kind")
        );
    }

    #[test]
    fn applies_cumulative_utf8_text_byte_limit_only_to_claims() {
        let claims = (0..16)
            .map(|index| GoalCheckpointVerifierClaim {
                proof_kind: GoalEvidenceProofKind::DeliveredOutput,
                claim: format!("Claim {index}: {}", "x".repeat(1_900)),
                source_refs: vec!["assistant-1".to_owned()],
            })
            .collect();
        let error = materialize_goal_evidence_checkpoint(
            "checkpoint-1",
            1.0,
            &[],
            &evidence(),
            &GoalCheckpointVerificationResult { claims },
        )
        .unwrap_err();
        assert!(error.message.contains("exceeds the 16000-byte"));
    }
}
