use crate::goals::protocol::{
    GoalEvidenceCheckpointClaim, GoalEvidenceProofKind, GoalRecord, GoalTerminalProposal,
    GoalTerminalProposalStatus, GoalTurnPermit, is_repeated_blocker_proposal,
};
use crate::transcript::{
    TranscriptRecord, TranscriptRecordType, is_user_prompt_submit_context_part_text,
    project_user_transcript_for_display,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

pub const GOAL_EVIDENCE_REFERENCE_LIMIT: usize = 100;
pub const GOAL_EVIDENCE_CATALOG_PREVIEW_LIMIT: usize = 240;
pub const GOAL_EVIDENCE_CATALOG_ENTRY_LIMIT: usize = 100;
pub const GOAL_EVIDENCE_CATALOG_BYTE_LIMIT: usize = 24_000;
pub const GOAL_EVIDENCE_CATALOG_LINEAGE_LIMIT: usize = 16;
pub const GOAL_EVIDENCE_CHECKPOINT_ENTRY_THRESHOLD: usize = 80;
pub const GOAL_EVIDENCE_CHECKPOINT_BYTE_THRESHOLD: usize = 19_200;
pub const GOAL_EVIDENCE_CHECKPOINT_CONTENT_BYTE_LIMIT: usize = 2_000;
pub const GOAL_EVIDENCE_VERIFIER_BYTE_LIMIT: usize = 256_000;
const CHECKPOINT_CONTENT_TRUNCATION_MARKER: &str = "\n…[truncated]";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalEvidenceProvenance {
    RealUser,
    AssistantOutput,
    ToolResult,
    GoalCheckpoint,
}

impl GoalEvidenceProvenance {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RealUser => "real_user",
            Self::AssistantOutput => "assistant_output",
            Self::ToolResult => "tool_result",
            Self::GoalCheckpoint => "goal_checkpoint",
        }
    }

    fn proof_kind(self) -> GoalEvidenceProofKind {
        match self {
            Self::RealUser => GoalEvidenceProofKind::UserInput,
            Self::AssistantOutput => GoalEvidenceProofKind::DeliveredOutput,
            Self::ToolResult => GoalEvidenceProofKind::ExternalFact,
            Self::GoalCheckpoint => unreachable!("checkpoint provenance is assigned directly"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalEvidenceTurnContext {
    pub goal_id: String,
    pub revision: f64,
    pub turn_id: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalEvidenceCatalogEntry {
    pub uuid: String,
    pub provenance: GoalEvidenceProvenance,
    pub turn_id: String,
    pub preview: String,
    pub proof_kind: GoalEvidenceProofKind,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalEvidenceCatalog {
    pub entries: Vec<GoalEvidenceCatalogEntry>,
    pub lineage_turn_ids: Vec<String>,
    pub truncated: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidatedGoalEvidenceRecord {
    pub uuid: String,
    pub provenance: GoalEvidenceProvenance,
    pub turn_id: String,
    pub preview: String,
    pub proof_kind: GoalEvidenceProofKind,
    pub content: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidatedGoalEvidence {
    pub cited_records: Vec<ValidatedGoalEvidenceRecord>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalEvidenceCheckpointWindow {
    pub previous_claims: Vec<GoalEvidenceCheckpointClaim>,
    pub evidence: Vec<ValidatedGoalEvidenceRecord>,
    pub truncated: bool,
    pub should_checkpoint: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvidenceSourceUnavailableCode {
    CursorUnset,
    CursorNotFound,
    DuplicateRecordUuid,
    PermitGoalMismatch,
    MalformedTurnContext,
    TurnReentry,
    CurrentTurnNotTail,
}

impl EvidenceSourceUnavailableCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CursorUnset => "cursor_unset",
            Self::CursorNotFound => "cursor_not_found",
            Self::DuplicateRecordUuid => "duplicate_record_uuid",
            Self::PermitGoalMismatch => "permit_goal_mismatch",
            Self::MalformedTurnContext => "malformed_turn_context",
            Self::TurnReentry => "turn_reentry",
            Self::CurrentTurnNotTail => "current_turn_not_tail",
        }
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{message}")]
pub struct EvidenceSourceUnavailableError {
    pub code: EvidenceSourceUnavailableCode,
    pub message: String,
}

impl EvidenceSourceUnavailableError {
    fn new(code: EvidenceSourceUnavailableCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidGoalEvidenceReferenceCode {
    NoEvidenceReferences,
    TooManyEvidenceReferences,
    DuplicateEvidenceReference,
    EvidencePayloadTooLarge,
    MissingReference,
    PreCursorReference,
    IneligibleReference,
    ReferenceNotCatalogued,
    MissingGoalContext,
    WrongGoalId,
    WrongRevision,
    WrongTurnLineage,
    CatalogTruncated,
    ImmediateBlockerExternalEvidenceRequired,
    ImmediateBlockerNewerEvidenceRequired,
    RepeatedBlockerTurnCoverage,
}

impl InvalidGoalEvidenceReferenceCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoEvidenceReferences => "no_evidence_references",
            Self::TooManyEvidenceReferences => "too_many_evidence_references",
            Self::DuplicateEvidenceReference => "duplicate_evidence_reference",
            Self::EvidencePayloadTooLarge => "evidence_payload_too_large",
            Self::MissingReference => "missing_reference",
            Self::PreCursorReference => "pre_cursor_reference",
            Self::IneligibleReference => "ineligible_reference",
            Self::ReferenceNotCatalogued => "reference_not_catalogued",
            Self::MissingGoalContext => "missing_goal_context",
            Self::WrongGoalId => "wrong_goal_id",
            Self::WrongRevision => "wrong_revision",
            Self::WrongTurnLineage => "wrong_turn_lineage",
            Self::CatalogTruncated => "catalog_truncated",
            Self::ImmediateBlockerExternalEvidenceRequired => {
                "immediate_blocker_external_evidence_required"
            }
            Self::ImmediateBlockerNewerEvidenceRequired => {
                "immediate_blocker_newer_evidence_required"
            }
            Self::RepeatedBlockerTurnCoverage => "repeated_blocker_turn_coverage",
        }
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{message}")]
pub struct InvalidGoalEvidenceReferenceError {
    pub code: InvalidGoalEvidenceReferenceCode,
    pub message: String,
    pub reference: Option<String>,
}

impl InvalidGoalEvidenceReferenceError {
    fn new(code: InvalidGoalEvidenceReferenceCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            reference: None,
        }
    }

    fn for_reference(
        code: InvalidGoalEvidenceReferenceCode,
        message: impl Into<String>,
        reference: &str,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            reference: Some(reference.to_owned()),
        }
    }
}

pub struct GoalEvidenceContext<'a> {
    pub records: &'a [TranscriptRecord],
    pub goal: &'a GoalRecord,
    pub permit: &'a GoalTurnPermit,
}

pub struct GoalEvidenceValidationInput<'a> {
    pub context: GoalEvidenceContext<'a>,
    pub proposal: &'a GoalTerminalProposal,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GoalEvidenceRecordIndexHint {
    pub uuid: String,
    pub parsed_goal_context: Option<GoalEvidenceTurnContext>,
    pub claimed_goal_id: Option<String>,
    pub claimed_revision: Option<f64>,
    pub provenance: Option<GoalEvidenceProvenance>,
    pub has_catalog_eligible_content: bool,
    pub has_raw_eligible_content: bool,
    pub catalog_entry_bytes: Option<usize>,
}

pub struct GoalEvidenceRecordIndexAccumulator {
    uuid: String,
    parsed_goal_context: Option<GoalEvidenceTurnContext>,
    claimed_goal_id: Option<String>,
    claimed_revision: Option<f64>,
    provenance: Option<GoalEvidenceProvenance>,
    has_object_system_payload: bool,
    display_text: Option<String>,
    has_hook_context: bool,
    prefix_preview: String,
    last_part_preview_values: Vec<String>,
    last_part_is_hook_context: bool,
    part_count: usize,
    has_raw_eligible_content: bool,
}

impl GoalEvidenceRecordIndexAccumulator {
    pub fn new(record: &TranscriptRecord) -> Self {
        let parsed_goal_context = parse_goal_context(record_goal_context(record));
        let claimed = record_goal_context(record).and_then(Value::as_object);
        let system_payload =
            record_extra(record, "systemPayload").filter(|value| value.is_object());
        let display_text = system_payload
            .and_then(|payload| payload.get("displayText"))
            .and_then(Value::as_str)
            .map(|text| truncate_utf16(text, GOAL_EVIDENCE_CATALOG_PREVIEW_LIMIT));
        let has_hook_context = system_payload
            .and_then(|payload| payload.get("hookContext"))
            .and_then(Value::as_str)
            .is_some();
        let provenance = parsed_goal_context
            .as_ref()
            .and_then(|_| coherent_evidence_provenance(record));
        let mut accumulator = Self {
            uuid: record.uuid.clone(),
            parsed_goal_context,
            claimed_goal_id: claimed
                .and_then(|context| context.get("goalId"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            claimed_revision: claimed
                .and_then(|context| context.get("revision"))
                .and_then(Value::as_f64),
            provenance,
            has_object_system_payload: system_payload.is_some(),
            display_text,
            has_hook_context,
            prefix_preview: String::new(),
            last_part_preview_values: Vec::new(),
            last_part_is_hook_context: false,
            part_count: 0,
            has_raw_eligible_content: false,
        };
        accumulator.add_fragment(record);
        accumulator
    }

    pub fn add_fragment(&mut self, record: &TranscriptRecord) {
        if self.provenance.is_none() {
            return;
        }
        let Some(parts) = record
            .message
            .as_ref()
            .and_then(|message| message.parts.as_ref())
        else {
            return;
        };
        for part in parts {
            self.finish_previous_part();
            let mut preview_values = Vec::new();
            if part.get("thought").and_then(Value::as_bool) != Some(true)
                && let Some(text) = part.get("text").and_then(Value::as_str)
            {
                preview_values.push(truncate_utf16(text, GOAL_EVIDENCE_CATALOG_PREVIEW_LIMIT));
                if !trim_js(text).is_empty() {
                    self.has_raw_eligible_content = true;
                }
            }
            if self.provenance == Some(GoalEvidenceProvenance::ToolResult)
                && let Some(response) = part.get("functionResponse")
            {
                preview_values.push(render_tool_response_preview(response));
                if response.get("response").is_some() {
                    self.has_raw_eligible_content = true;
                }
            }
            self.last_part_is_hook_context = part
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(is_user_prompt_submit_context_part_text);
            self.last_part_preview_values = preview_values;
            self.part_count += 1;
        }
    }

    pub fn finish(&self) -> GoalEvidenceRecordIndexHint {
        let has_final_hook_context_part = self.part_count > 1 && self.last_part_is_hook_context;
        let preview = if self.provenance == Some(GoalEvidenceProvenance::RealUser)
            && (self.has_hook_context || has_final_hook_context_part)
            && self.display_text.is_some()
        {
            trim_js(self.display_text.as_deref().unwrap_or_default()).to_owned()
        } else if self.provenance == Some(GoalEvidenceProvenance::RealUser)
            && !self.has_object_system_payload
            && has_final_hook_context_part
        {
            trim_js(&self.prefix_preview).to_owned()
        } else {
            trim_js(&append_preview_values(
                &self.prefix_preview,
                &self.last_part_preview_values,
            ))
            .to_owned()
        };
        let entry_bytes = self
            .parsed_goal_context
            .as_ref()
            .zip(self.provenance)
            .filter(|(_, _)| !preview.is_empty())
            .map(|(context, provenance)| {
                serialized_catalog_entry_bytes(&GoalEvidenceCatalogEntry {
                    uuid: self.uuid.clone(),
                    provenance,
                    turn_id: context.turn_id.clone(),
                    preview,
                    proof_kind: provenance.proof_kind(),
                })
            });
        GoalEvidenceRecordIndexHint {
            uuid: self.uuid.clone(),
            parsed_goal_context: self.parsed_goal_context.clone(),
            claimed_goal_id: self.claimed_goal_id.clone(),
            claimed_revision: self.claimed_revision,
            provenance: self.provenance,
            has_catalog_eligible_content: entry_bytes.is_some(),
            has_raw_eligible_content: self.has_raw_eligible_content,
            catalog_entry_bytes: entry_bytes,
        }
    }

    fn finish_previous_part(&mut self) {
        if self.part_count == 0 {
            return;
        }
        self.prefix_preview =
            append_preview_values(&self.prefix_preview, &self.last_part_preview_values);
    }
}

pub struct GoalEvidenceCheckpointAccumulator<'a> {
    candidate_uuids: Vec<String>,
    candidate_uuid_set: HashSet<String>,
    captured: HashMap<String, ValidatedGoalEvidenceRecord>,
    goal: &'a GoalRecord,
    truncated: bool,
    should_checkpoint: bool,
}

impl<'a> GoalEvidenceCheckpointAccumulator<'a> {
    pub fn new(
        hints: &[GoalEvidenceRecordIndexHint],
        goal: &'a GoalRecord,
        permit: &GoalTurnPermit,
    ) -> Result<Self, EvidenceSourceUnavailableError> {
        validate_permit(goal, permit)?;
        let index_by_uuid = build_unique_index(hints.iter().map(|hint| hint.uuid.as_str()))?;
        let cursor_index = find_cursor_index(goal, &index_by_uuid)?;
        let lineage_turn_ids = collect_hint_lineage(hints, cursor_index, goal)?;
        validate_lineage_tail(&lineage_turn_ids, permit)?;

        let checkpoint_entries = checkpoint_catalog_entries(goal);
        let checkpoint_bytes = checkpoint_entries
            .iter()
            .map(serialized_catalog_entry_bytes)
            .sum::<usize>();
        let mut truncated = checkpoint_entries.len() >= GOAL_EVIDENCE_CATALOG_ENTRY_LIMIT
            || checkpoint_bytes > GOAL_EVIDENCE_CATALOG_BYTE_LIMIT;
        let raw_entry_limit =
            GOAL_EVIDENCE_CATALOG_ENTRY_LIMIT.saturating_sub(checkpoint_entries.len());
        let mut catalog_bytes = checkpoint_bytes;
        let mut candidate_uuids = Vec::new();
        let mut candidate_uuid_set = HashSet::new();

        for hint in hints[cursor_index + 1..].iter().rev() {
            if truncated {
                break;
            }
            let Some(context) = hint.parsed_goal_context.as_ref() else {
                if hint.claimed_goal_id.as_deref() == Some(&goal.goal_id)
                    && hint.claimed_revision == Some(goal.revision as f64)
                {
                    return Err(EvidenceSourceUnavailableError::new(
                        EvidenceSourceUnavailableCode::MalformedTurnContext,
                        format!(
                            "Goal-owned transcript record {} has malformed turn context.",
                            hint.uuid
                        ),
                    ));
                }
                continue;
            };
            if context.goal_id != goal.goal_id || context.revision != goal.revision as f64 {
                continue;
            }
            if candidate_uuids.len() >= raw_entry_limit {
                if hint.has_raw_eligible_content {
                    truncated = true;
                    break;
                }
                continue;
            }
            if !hint.has_catalog_eligible_content {
                continue;
            }
            let Some(entry_bytes) = hint.catalog_entry_bytes else {
                truncated = true;
                break;
            };
            if catalog_bytes + entry_bytes > GOAL_EVIDENCE_CATALOG_BYTE_LIMIT {
                truncated = true;
                break;
            }
            candidate_uuids.push(hint.uuid.clone());
            candidate_uuid_set.insert(hint.uuid.clone());
            catalog_bytes += entry_bytes;
        }

        let should_checkpoint = !truncated
            && !candidate_uuids.is_empty()
            && (checkpoint_entries.len() + candidate_uuids.len()
                >= GOAL_EVIDENCE_CHECKPOINT_ENTRY_THRESHOLD
                || catalog_bytes >= GOAL_EVIDENCE_CHECKPOINT_BYTE_THRESHOLD);
        Ok(Self {
            candidate_uuids,
            candidate_uuid_set,
            captured: HashMap::new(),
            goal,
            truncated,
            should_checkpoint,
        })
    }

    pub fn get_candidate_uuids(&self) -> &[String] {
        if self.should_checkpoint {
            &self.candidate_uuids
        } else {
            &[]
        }
    }

    pub fn capture(&mut self, record: &TranscriptRecord) {
        if !self.should_checkpoint || !self.candidate_uuid_set.contains(&record.uuid) {
            return;
        }
        let Some(provenance) = coherent_evidence_provenance(record) else {
            return;
        };
        let Some(context) = parse_goal_context(record_goal_context(record)) else {
            return;
        };
        if context.goal_id != self.goal.goal_id || context.revision != self.goal.revision as f64 {
            return;
        }
        let preview = evidence_preview(record, provenance);
        let content = evidence_content(record, provenance);
        if preview.is_empty() || content.is_empty() {
            return;
        }
        self.captured.insert(
            record.uuid.clone(),
            ValidatedGoalEvidenceRecord {
                uuid: record.uuid.clone(),
                provenance,
                turn_id: context.turn_id,
                preview,
                proof_kind: provenance.proof_kind(),
                content: cap_checkpoint_content(&content),
            },
        );
    }

    pub fn finish(self) -> Result<GoalEvidenceCheckpointWindow, InvalidGoalEvidenceReferenceError> {
        let mut evidence = if self.should_checkpoint {
            let mut selected = Vec::with_capacity(self.candidate_uuids.len());
            for uuid in &self.candidate_uuids {
                let Some(entry) = self.captured.get(uuid) else {
                    return Err(InvalidGoalEvidenceReferenceError::for_reference(
                        InvalidGoalEvidenceReferenceCode::IneligibleReference,
                        format!("Transcript record {uuid} has no eligible evidence content."),
                        uuid,
                    ));
                };
                selected.push(entry.clone());
            }
            selected
        } else {
            Vec::new()
        };
        evidence.reverse();
        Ok(GoalEvidenceCheckpointWindow {
            previous_claims: self
                .goal
                .evidence_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.claims.clone())
                .unwrap_or_default(),
            evidence,
            truncated: self.truncated,
            should_checkpoint: self.should_checkpoint,
        })
    }
}

pub fn get_goal_evidence_record_index_hint(
    record: &TranscriptRecord,
) -> GoalEvidenceRecordIndexHint {
    GoalEvidenceRecordIndexAccumulator::new(record).finish()
}

pub fn build_goal_evidence_catalog(
    input: &GoalEvidenceContext<'_>,
) -> Result<GoalEvidenceCatalog, EvidenceSourceUnavailableError> {
    let analysis = analyze_evidence(input)?;
    Ok(GoalEvidenceCatalog {
        entries: analysis.catalog,
        lineage_turn_ids: analysis
            .lineage_turn_ids
            .into_iter()
            .rev()
            .take(GOAL_EVIDENCE_CATALOG_LINEAGE_LIMIT)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect(),
        truncated: analysis.catalog_truncated,
    })
}

pub fn build_goal_evidence_checkpoint_window(
    input: &GoalEvidenceContext<'_>,
) -> Result<GoalEvidenceCheckpointWindow, EvidenceSourceUnavailableErrorOrReference> {
    let hints = input
        .records
        .iter()
        .map(get_goal_evidence_record_index_hint)
        .collect::<Vec<_>>();
    let mut accumulator = GoalEvidenceCheckpointAccumulator::new(&hints, input.goal, input.permit)?;
    let candidates = accumulator
        .get_candidate_uuids()
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    for record in input
        .records
        .iter()
        .filter(|record| candidates.contains(&record.uuid))
    {
        accumulator.capture(record);
    }
    Ok(accumulator.finish()?)
}

#[derive(Debug, Error)]
pub enum EvidenceSourceUnavailableErrorOrReference {
    #[error(transparent)]
    Source(#[from] EvidenceSourceUnavailableError),
    #[error(transparent)]
    Reference(#[from] InvalidGoalEvidenceReferenceError),
}

pub fn validate_goal_evidence_references(
    input: &GoalEvidenceValidationInput<'_>,
) -> Result<ValidatedGoalEvidence, EvidenceSourceUnavailableErrorOrReference> {
    let references = &input.proposal.evidence_refs;
    if references.is_empty() {
        return Err(InvalidGoalEvidenceReferenceError::new(
            InvalidGoalEvidenceReferenceCode::NoEvidenceReferences,
            "A terminal Goal proposal must cite at least one evidence record.",
        )
        .into());
    }
    if references.len() > GOAL_EVIDENCE_REFERENCE_LIMIT {
        return Err(InvalidGoalEvidenceReferenceError::new(
            InvalidGoalEvidenceReferenceCode::TooManyEvidenceReferences,
            format!(
                "A terminal Goal proposal may cite at most {GOAL_EVIDENCE_REFERENCE_LIMIT} evidence records."
            ),
        )
        .into());
    }
    if references.iter().collect::<HashSet<_>>().len() != references.len() {
        return Err(InvalidGoalEvidenceReferenceError::new(
            InvalidGoalEvidenceReferenceCode::DuplicateEvidenceReference,
            "A terminal Goal proposal must not cite the same evidence record more than once.",
        )
        .into());
    }

    let analysis = analyze_evidence(&input.context)?;
    if analysis.catalog_truncated
        && !(is_repeated_blocker_proposal(input.proposal)
            && repeated_blocker_coverage_catalogued(&analysis))
    {
        return Err(InvalidGoalEvidenceReferenceError::new(
            InvalidGoalEvidenceReferenceCode::CatalogTruncated,
            crate::goals::protocol::GOAL_EVIDENCE_CATALOG_EXHAUSTED_REASON,
        )
        .into());
    }
    let cited_records = references
        .iter()
        .map(|reference| validate_reference(reference, input, &analysis))
        .collect::<Result<Vec<_>, _>>()?;
    let evidence_bytes = cited_records
        .iter()
        .map(|record| record.content.len())
        .sum::<usize>();
    if evidence_bytes > GOAL_EVIDENCE_VERIFIER_BYTE_LIMIT {
        return Err(InvalidGoalEvidenceReferenceError::new(
            InvalidGoalEvidenceReferenceCode::EvidencePayloadTooLarge,
            format!(
                "Cited Goal evidence exceeds the {GOAL_EVIDENCE_VERIFIER_BYTE_LIMIT}-byte verifier limit."
            ),
        )
        .into());
    }
    validate_blocker_coverage(input.proposal, &cited_records, &analysis)?;
    Ok(ValidatedGoalEvidence { cited_records })
}

struct EvidenceAnalysis {
    cursor_index: usize,
    catalog: Vec<GoalEvidenceCatalogEntry>,
    eligible_by_uuid: HashMap<String, GoalEvidenceCatalogEntry>,
    index_by_uuid: HashMap<String, usize>,
    lineage_turn_ids: Vec<String>,
    catalog_truncated: bool,
}

fn analyze_evidence(
    input: &GoalEvidenceContext<'_>,
) -> Result<EvidenceAnalysis, EvidenceSourceUnavailableError> {
    validate_permit(input.goal, input.permit)?;
    let index_by_uuid =
        build_unique_index(input.records.iter().map(|record| record.uuid.as_str()))?;
    let cursor_index = find_cursor_index(input.goal, &index_by_uuid)?;
    let lineage_turn_ids = collect_direct_lineage(input.records, cursor_index, input.goal)?;
    validate_lineage_tail(&lineage_turn_ids, input.permit)?;

    let checkpoint_entries = checkpoint_catalog_entries(input.goal);
    let mut selected = Vec::new();
    let mut catalog_bytes = checkpoint_entries
        .iter()
        .map(serialized_catalog_entry_bytes)
        .sum::<usize>();
    let mut catalog_truncated = checkpoint_entries.len() >= GOAL_EVIDENCE_CATALOG_ENTRY_LIMIT
        || catalog_bytes > GOAL_EVIDENCE_CATALOG_BYTE_LIMIT;
    let raw_entry_limit =
        GOAL_EVIDENCE_CATALOG_ENTRY_LIMIT.saturating_sub(checkpoint_entries.len());
    for record in input.records[cursor_index + 1..].iter().rev() {
        if catalog_truncated {
            break;
        }
        if selected.len() >= raw_entry_limit {
            if has_catalog_eligible_evidence(record, input) {
                catalog_truncated = true;
                break;
            }
            continue;
        }
        let Some(entry) = catalog_evidence(record, input) else {
            continue;
        };
        let entry_bytes = serialized_catalog_entry_bytes(&entry);
        if catalog_bytes + entry_bytes > GOAL_EVIDENCE_CATALOG_BYTE_LIMIT {
            catalog_truncated = true;
            break;
        }
        catalog_bytes += entry_bytes;
        selected.push(entry);
    }
    selected.reverse();
    let mut catalog = checkpoint_entries;
    catalog.extend(selected);
    let eligible_by_uuid = catalog
        .iter()
        .map(|entry| (entry.uuid.clone(), entry.clone()))
        .collect();
    Ok(EvidenceAnalysis {
        cursor_index,
        catalog,
        eligible_by_uuid,
        index_by_uuid,
        lineage_turn_ids,
        catalog_truncated,
    })
}

fn validate_permit(
    goal: &GoalRecord,
    permit: &GoalTurnPermit,
) -> Result<(), EvidenceSourceUnavailableError> {
    if permit.goal_id != goal.goal_id
        || permit.revision != goal.revision
        || !is_non_empty_string(&permit.turn_id)
    {
        return Err(EvidenceSourceUnavailableError::new(
            EvidenceSourceUnavailableCode::PermitGoalMismatch,
            "The current Goal permit does not match the Goal evidence revision.",
        ));
    }
    Ok(())
}

fn build_unique_index<'a>(
    uuids: impl Iterator<Item = &'a str>,
) -> Result<HashMap<String, usize>, EvidenceSourceUnavailableError> {
    let mut index_by_uuid = HashMap::new();
    for (index, uuid) in uuids.enumerate() {
        if index_by_uuid.insert(uuid.to_owned(), index).is_some() {
            return Err(EvidenceSourceUnavailableError::new(
                EvidenceSourceUnavailableCode::DuplicateRecordUuid,
                format!("The active transcript chain contains duplicate record UUID {uuid}."),
            ));
        }
    }
    Ok(index_by_uuid)
}

fn find_cursor_index(
    goal: &GoalRecord,
    index_by_uuid: &HashMap<String, usize>,
) -> Result<usize, EvidenceSourceUnavailableError> {
    let Some(cursor_id) = goal.evidence_cursor.record_id.as_deref() else {
        return Err(EvidenceSourceUnavailableError::new(
            EvidenceSourceUnavailableCode::CursorUnset,
            "The Goal evidence cursor is not available.",
        ));
    };
    index_by_uuid.get(cursor_id).copied().ok_or_else(|| {
        EvidenceSourceUnavailableError::new(
            EvidenceSourceUnavailableCode::CursorNotFound,
            format!("The Goal evidence cursor {cursor_id} is not in the active transcript chain."),
        )
    })
}

fn collect_hint_lineage(
    hints: &[GoalEvidenceRecordIndexHint],
    cursor_index: usize,
    goal: &GoalRecord,
) -> Result<Vec<String>, EvidenceSourceUnavailableError> {
    let mut lineage = Vec::new();
    let mut seen = HashSet::new();
    let mut current: Option<String> = None;
    for hint in &hints[cursor_index + 1..] {
        let Some(context) = hint.parsed_goal_context.as_ref() else {
            if hint.claimed_goal_id.as_deref() == Some(&goal.goal_id)
                && hint.claimed_revision == Some(goal.revision as f64)
            {
                return Err(EvidenceSourceUnavailableError::new(
                    EvidenceSourceUnavailableCode::MalformedTurnContext,
                    format!(
                        "Goal-owned transcript record {} has malformed turn context.",
                        hint.uuid
                    ),
                ));
            }
            continue;
        };
        if context.goal_id != goal.goal_id || context.revision != goal.revision as f64 {
            continue;
        }
        if current.as_deref() == Some(context.turn_id.as_str()) {
            continue;
        }
        if !seen.insert(context.turn_id.clone()) {
            return Err(EvidenceSourceUnavailableError::new(
                EvidenceSourceUnavailableCode::TurnReentry,
                format!(
                    "Goal turn {} re-enters the active transcript lineage.",
                    context.turn_id
                ),
            ));
        }
        current = Some(context.turn_id.clone());
        lineage.push(context.turn_id.clone());
    }
    Ok(lineage)
}

fn validate_lineage_tail(
    lineage_turn_ids: &[String],
    permit: &GoalTurnPermit,
) -> Result<(), EvidenceSourceUnavailableError> {
    if lineage_turn_ids.last().map(String::as_str) != Some(permit.turn_id.as_str()) {
        return Err(EvidenceSourceUnavailableError::new(
            EvidenceSourceUnavailableCode::CurrentTurnNotTail,
            "The current Goal permit is not the tail of the active transcript lineage.",
        ));
    }
    Ok(())
}

fn collect_direct_lineage(
    records: &[TranscriptRecord],
    cursor_index: usize,
    goal: &GoalRecord,
) -> Result<Vec<String>, EvidenceSourceUnavailableError> {
    let mut lineage = Vec::new();
    let mut seen = HashSet::new();
    let mut current: Option<String> = None;
    for record in &records[cursor_index + 1..] {
        let raw_context = record_goal_context(record);
        let Some(context) = parse_goal_context(raw_context) else {
            let claims_revision = raw_context.is_some_and(|value| {
                value.get("goalId").and_then(Value::as_str) == Some(&goal.goal_id)
                    && value.get("revision").and_then(Value::as_f64) == Some(goal.revision as f64)
            });
            if claims_revision {
                return Err(EvidenceSourceUnavailableError::new(
                    EvidenceSourceUnavailableCode::MalformedTurnContext,
                    format!(
                        "Goal-owned transcript record {} has malformed turn context.",
                        record.uuid
                    ),
                ));
            }
            continue;
        };
        if context.goal_id != goal.goal_id || context.revision != goal.revision as f64 {
            continue;
        }
        if current.as_deref() == Some(context.turn_id.as_str()) {
            continue;
        }
        if !seen.insert(context.turn_id.clone()) {
            return Err(EvidenceSourceUnavailableError::new(
                EvidenceSourceUnavailableCode::TurnReentry,
                format!(
                    "Goal turn {} re-enters the active transcript lineage.",
                    context.turn_id
                ),
            ));
        }
        current = Some(context.turn_id.clone());
        lineage.push(context.turn_id);
    }
    Ok(lineage)
}

fn validate_reference(
    reference: &str,
    input: &GoalEvidenceValidationInput<'_>,
    analysis: &EvidenceAnalysis,
) -> Result<ValidatedGoalEvidenceRecord, InvalidGoalEvidenceReferenceError> {
    if let Some(checkpoint_claim) = input
        .context
        .goal
        .evidence_checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.claims.iter().find(|claim| claim.id == reference))
    {
        let Some(entry) = analysis.eligible_by_uuid.get(reference) else {
            return Err(InvalidGoalEvidenceReferenceError::for_reference(
                InvalidGoalEvidenceReferenceCode::ReferenceNotCatalogued,
                format!(
                    "Evidence reference {reference} is outside the bounded Goal evidence catalog."
                ),
                reference,
            ));
        };
        return Ok(ValidatedGoalEvidenceRecord {
            uuid: entry.uuid.clone(),
            provenance: entry.provenance,
            turn_id: entry.turn_id.clone(),
            preview: entry.preview.clone(),
            proof_kind: entry.proof_kind,
            content: checkpoint_claim.claim.clone(),
        });
    }

    let Some(record_index) = analysis.index_by_uuid.get(reference).copied() else {
        return Err(InvalidGoalEvidenceReferenceError::for_reference(
            InvalidGoalEvidenceReferenceCode::MissingReference,
            format!("Evidence reference {reference} is not in the active transcript chain."),
            reference,
        ));
    };
    if record_index <= analysis.cursor_index {
        return Err(InvalidGoalEvidenceReferenceError::for_reference(
            InvalidGoalEvidenceReferenceCode::PreCursorReference,
            format!("Evidence reference {reference} is not after the Goal evidence cursor."),
            reference,
        ));
    }
    let record = &input.context.records[record_index];
    let Some(provenance) = coherent_evidence_provenance(record) else {
        return Err(InvalidGoalEvidenceReferenceError::for_reference(
            InvalidGoalEvidenceReferenceCode::IneligibleReference,
            format!("Transcript record {reference} is not an eligible evidence source."),
            reference,
        ));
    };
    let Some(context) = parse_goal_context(record_goal_context(record)) else {
        return Err(InvalidGoalEvidenceReferenceError::for_reference(
            InvalidGoalEvidenceReferenceCode::MissingGoalContext,
            format!("Evidence reference {reference} has no valid Goal turn context."),
            reference,
        ));
    };
    if context.goal_id != input.context.goal.goal_id {
        return Err(InvalidGoalEvidenceReferenceError::for_reference(
            InvalidGoalEvidenceReferenceCode::WrongGoalId,
            format!("Evidence reference {reference} belongs to a different Goal."),
            reference,
        ));
    }
    if context.revision != input.context.goal.revision as f64 {
        return Err(InvalidGoalEvidenceReferenceError::for_reference(
            InvalidGoalEvidenceReferenceCode::WrongRevision,
            format!("Evidence reference {reference} belongs to a different Goal revision."),
            reference,
        ));
    }
    if !analysis.lineage_turn_ids.contains(&context.turn_id) {
        return Err(InvalidGoalEvidenceReferenceError::for_reference(
            InvalidGoalEvidenceReferenceCode::WrongTurnLineage,
            format!("Evidence reference {reference} is not in the active Goal turn lineage."),
            reference,
        ));
    }
    let Some(entry) = analysis.eligible_by_uuid.get(reference) else {
        return Err(InvalidGoalEvidenceReferenceError::for_reference(
            InvalidGoalEvidenceReferenceCode::ReferenceNotCatalogued,
            format!("Evidence reference {reference} is outside the bounded Goal evidence catalog."),
            reference,
        ));
    };
    let content = evidence_content(record, provenance);
    if content.is_empty() {
        return Err(InvalidGoalEvidenceReferenceError::for_reference(
            InvalidGoalEvidenceReferenceCode::IneligibleReference,
            format!("Transcript record {reference} has no eligible evidence content."),
            reference,
        ));
    }
    Ok(ValidatedGoalEvidenceRecord {
        uuid: entry.uuid.clone(),
        provenance: entry.provenance,
        turn_id: entry.turn_id.clone(),
        preview: entry.preview.clone(),
        proof_kind: entry.proof_kind,
        content,
    })
}

fn repeated_blocker_coverage_catalogued(analysis: &EvidenceAnalysis) -> bool {
    let required = analysis
        .lineage_turn_ids
        .iter()
        .rev()
        .take(3)
        .cloned()
        .collect::<Vec<_>>();
    let current = required.first();
    required.iter().all(|turn_id| {
        analysis.catalog.iter().any(|entry| {
            &entry.turn_id == turn_id
                && (Some(turn_id) == current
                    || entry.provenance != GoalEvidenceProvenance::AssistantOutput)
        })
    })
}

fn validate_blocker_coverage(
    proposal: &GoalTerminalProposal,
    cited_records: &[ValidatedGoalEvidenceRecord],
    analysis: &EvidenceAnalysis,
) -> Result<(), InvalidGoalEvidenceReferenceError> {
    if proposal.status != GoalTerminalProposalStatus::Blocked {
        return Ok(());
    }
    if matches!(
        proposal.blocker_kind,
        Some(
            crate::goals::protocol::GoalBlockerKind::Authority
                | crate::goals::protocol::GoalBlockerKind::External
        )
    ) {
        if !cited_records.iter().any(|record| {
            matches!(
                record.proof_kind,
                GoalEvidenceProofKind::UserInput | GoalEvidenceProofKind::ExternalFact
            )
        }) {
            return Err(InvalidGoalEvidenceReferenceError::new(
                InvalidGoalEvidenceReferenceCode::ImmediateBlockerExternalEvidenceRequired,
                "An immediate blocker requires cited user input or external tool evidence.",
            ));
        }
        let cited = cited_records
            .iter()
            .map(|record| record.uuid.as_str())
            .collect::<HashSet<_>>();
        let oldest_blocker_index = cited_records
            .iter()
            .filter(|record| {
                matches!(
                    record.proof_kind,
                    GoalEvidenceProofKind::UserInput | GoalEvidenceProofKind::ExternalFact
                )
            })
            .filter_map(|record| {
                analysis
                    .catalog
                    .iter()
                    .position(|entry| entry.uuid == record.uuid)
            })
            .min()
            .unwrap_or(analysis.catalog.len());
        if analysis.catalog[oldest_blocker_index.saturating_add(1)..]
            .iter()
            .any(|entry| !cited.contains(entry.uuid.as_str()))
        {
            return Err(InvalidGoalEvidenceReferenceError::new(
                InvalidGoalEvidenceReferenceCode::ImmediateBlockerNewerEvidenceRequired,
                "An immediate blocker must cite every newer bounded evidence record so contradictory evidence cannot be omitted.",
            ));
        }
        return Ok(());
    }

    let required = analysis
        .lineage_turn_ids
        .iter()
        .rev()
        .take(3)
        .cloned()
        .collect::<Vec<_>>();
    let current_turn_id = required.first().map(String::as_str);
    let cited_turns = cited_records
        .iter()
        .filter(|record| {
            record.provenance != GoalEvidenceProvenance::AssistantOutput
                || Some(record.turn_id.as_str()) == current_turn_id
        })
        .map(|record| record.turn_id.as_str())
        .collect::<HashSet<_>>();
    if required.len() != 3
        || required
            .iter()
            .any(|turn_id| !cited_turns.contains(turn_id.as_str()))
    {
        return Err(InvalidGoalEvidenceReferenceError::new(
            InvalidGoalEvidenceReferenceCode::RepeatedBlockerTurnCoverage,
            "A repeated blocker requires evidence from the current and two immediately preceding Goal turns.",
        ));
    }
    Ok(())
}

fn checkpoint_catalog_entries(goal: &GoalRecord) -> Vec<GoalEvidenceCatalogEntry> {
    goal.evidence_checkpoint
        .as_ref()
        .map(|checkpoint| {
            checkpoint
                .claims
                .iter()
                .map(|claim| GoalEvidenceCatalogEntry {
                    uuid: claim.id.clone(),
                    provenance: GoalEvidenceProvenance::GoalCheckpoint,
                    turn_id: format!("checkpoint:{}", checkpoint.checkpoint_id),
                    preview: truncate_utf16(&claim.claim, GOAL_EVIDENCE_CATALOG_PREVIEW_LIMIT),
                    proof_kind: claim.proof_kind,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn has_catalog_eligible_evidence(
    record: &TranscriptRecord,
    input: &GoalEvidenceContext<'_>,
) -> bool {
    let Some(provenance) = coherent_evidence_provenance(record) else {
        return false;
    };
    let Some(context) = parse_goal_context(record_goal_context(record)) else {
        return false;
    };
    if context.goal_id != input.goal.goal_id || context.revision != input.goal.revision as f64 {
        return false;
    }
    record_parts(record).iter().any(|part| {
        (part.get("thought").and_then(Value::as_bool) != Some(true)
            && part
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| !trim_js(text).is_empty()))
            || (provenance == GoalEvidenceProvenance::ToolResult
                && part
                    .get("functionResponse")
                    .is_some_and(|response| response.get("response").is_some()))
    })
}

fn catalog_evidence(
    record: &TranscriptRecord,
    input: &GoalEvidenceContext<'_>,
) -> Option<GoalEvidenceCatalogEntry> {
    let provenance = coherent_evidence_provenance(record)?;
    let context = parse_goal_context(record_goal_context(record))?;
    if context.goal_id != input.goal.goal_id || context.revision != input.goal.revision as f64 {
        return None;
    }
    let preview = evidence_preview(record, provenance);
    if preview.is_empty() {
        return None;
    }
    Some(GoalEvidenceCatalogEntry {
        uuid: record.uuid.clone(),
        provenance,
        turn_id: context.turn_id,
        preview,
        proof_kind: provenance.proof_kind(),
    })
}

fn coherent_evidence_provenance(record: &TranscriptRecord) -> Option<GoalEvidenceProvenance> {
    if record.record_type == TranscriptRecordType::System {
        return None;
    }
    let provenance = match record_extra(record, "provenance") {
        None | Some(Value::Null) => legacy_safe_provenance(record),
        Some(Value::String(value)) => parse_provenance(value),
        Some(_) => None,
    }?;
    match provenance {
        GoalEvidenceProvenance::RealUser
            if record.record_type == TranscriptRecordType::User
                && (record.subtype.is_none()
                    || record.subtype.as_deref() == Some("mid_turn_user_message")) =>
        {
            Some(provenance)
        }
        GoalEvidenceProvenance::AssistantOutput
            if record.record_type == TranscriptRecordType::Assistant
                && record.subtype.is_none() =>
        {
            Some(provenance)
        }
        GoalEvidenceProvenance::ToolResult
            if record.record_type == TranscriptRecordType::ToolResult
                && record.subtype.is_none() =>
        {
            Some(provenance)
        }
        _ => None,
    }
}

fn parse_provenance(value: &str) -> Option<GoalEvidenceProvenance> {
    match value {
        "real_user" => Some(GoalEvidenceProvenance::RealUser),
        "assistant_output" => Some(GoalEvidenceProvenance::AssistantOutput),
        "tool_result" => Some(GoalEvidenceProvenance::ToolResult),
        "goal_checkpoint" => Some(GoalEvidenceProvenance::GoalCheckpoint),
        _ => None,
    }
}

fn legacy_safe_provenance(record: &TranscriptRecord) -> Option<GoalEvidenceProvenance> {
    match record.record_type {
        TranscriptRecordType::User
            if record.subtype.is_none()
                || record.subtype.as_deref() == Some("mid_turn_user_message") =>
        {
            Some(GoalEvidenceProvenance::RealUser)
        }
        TranscriptRecordType::Assistant if record.subtype.is_none() => {
            Some(GoalEvidenceProvenance::AssistantOutput)
        }
        TranscriptRecordType::ToolResult if record.subtype.is_none() => {
            Some(GoalEvidenceProvenance::ToolResult)
        }
        _ => None,
    }
}

fn evidence_content(record: &TranscriptRecord, provenance: GoalEvidenceProvenance) -> String {
    let projection = user_projection(record, provenance);
    if let Some(display_text) = projection
        .as_ref()
        .and_then(|projection| projection.display_text.as_deref())
    {
        return trim_js(display_text).to_owned();
    }
    let parts = projection.as_ref().map_or_else(
        || record_parts(record).to_vec(),
        |projection| projection.parts.clone(),
    );
    let mut values = Vec::new();
    for part in &parts {
        if part.get("thought").and_then(Value::as_bool) != Some(true)
            && let Some(text) = part.get("text").and_then(Value::as_str)
        {
            values.push(text.to_owned());
        }
        if provenance == GoalEvidenceProvenance::ToolResult
            && let Some(response) = part.get("functionResponse")
        {
            let rendered = render_tool_response(response);
            if !rendered.is_empty() {
                values.push(rendered);
            }
        }
    }
    trim_js(&values.join("\n")).to_owned()
}

fn evidence_preview(record: &TranscriptRecord, provenance: GoalEvidenceProvenance) -> String {
    let projection = user_projection(record, provenance);
    if let Some(display_text) = projection
        .as_ref()
        .and_then(|projection| projection.display_text.as_deref())
    {
        return trim_js(&truncate_utf16(
            display_text,
            GOAL_EVIDENCE_CATALOG_PREVIEW_LIMIT,
        ))
        .to_owned();
    }
    let parts = projection.as_ref().map_or_else(
        || record_parts(record).to_vec(),
        |projection| projection.parts.clone(),
    );
    let mut preview = String::new();
    for part in &parts {
        if part.get("thought").and_then(Value::as_bool) != Some(true)
            && let Some(text) = part.get("text").and_then(Value::as_str)
        {
            preview = append_preview(&preview, text);
        }
        if provenance == GoalEvidenceProvenance::ToolResult
            && let Some(response) = part.get("functionResponse")
        {
            preview = append_preview(&preview, &render_tool_response_preview(response));
        }
        if utf16_len(&preview) >= GOAL_EVIDENCE_CATALOG_PREVIEW_LIMIT {
            break;
        }
    }
    trim_js(&preview).to_owned()
}

fn user_projection(
    record: &TranscriptRecord,
    provenance: GoalEvidenceProvenance,
) -> Option<crate::transcript::UserTranscriptDisplayProjection> {
    if provenance != GoalEvidenceProvenance::RealUser {
        return None;
    }
    let message = record
        .message
        .as_ref()
        .and_then(|message| serde_json::to_value(message).ok());
    Some(project_user_transcript_for_display(
        message.as_ref(),
        record_extra(record, "systemPayload"),
    ))
}

fn render_tool_response(response_part: &Value) -> String {
    let Some(response) = response_part.get("response") else {
        return String::new();
    };
    let mut rendered = Map::new();
    if let Some(name) = response_part.get("name") {
        rendered.insert("name".to_owned(), name.clone());
    }
    rendered.insert("response".to_owned(), response.clone());
    serde_json::to_string(&Value::Object(rendered)).unwrap_or_default()
}

fn render_tool_response_preview(response_part: &Value) -> String {
    let Some(response) = response_part.get("response") else {
        return String::new();
    };
    let mut rendered = Map::new();
    if let Some(name) = response_part.get("name") {
        rendered.insert("name".to_owned(), name.clone());
    }
    rendered.insert("response".to_owned(), summarize_json_value(response, 0));
    truncate_utf16(
        &serde_json::to_string(&Value::Object(rendered)).unwrap_or_default(),
        GOAL_EVIDENCE_CATALOG_PREVIEW_LIMIT,
    )
}

fn summarize_json_value(value: &Value, depth: usize) -> Value {
    match value {
        Value::String(text) => {
            Value::String(truncate_utf16(text, GOAL_EVIDENCE_CATALOG_PREVIEW_LIMIT))
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => value.clone(),
        Value::Array(values) if depth >= 2 => Value::String("[Nested value]".to_owned()),
        Value::Object(_) if depth >= 2 => Value::String("[Nested value]".to_owned()),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .take(6)
                .map(|entry| summarize_json_value(entry, depth + 1))
                .collect(),
        ),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .take(6)
                .map(|(key, entry)| (key.clone(), summarize_json_value(entry, depth + 1)))
                .collect(),
        ),
    }
}

fn cap_checkpoint_content(content: &str) -> String {
    if content.len() <= GOAL_EVIDENCE_CHECKPOINT_CONTENT_BYTE_LIMIT {
        return content.to_owned();
    }
    let budget =
        GOAL_EVIDENCE_CHECKPOINT_CONTENT_BYTE_LIMIT - CHECKPOINT_CONTENT_TRUNCATION_MARKER.len();
    let mut used = 0;
    let mut end = 0;
    for (index, character) in content.char_indices() {
        if used + character.len_utf8() > budget {
            break;
        }
        used += character.len_utf8();
        end = index + character.len_utf8();
    }
    format!("{}{CHECKPOINT_CONTENT_TRUNCATION_MARKER}", &content[..end])
}

fn parse_goal_context(value: Option<&Value>) -> Option<GoalEvidenceTurnContext> {
    let object = value?.as_object()?;
    if !has_only_keys(object, &["goalId", "revision", "turnId"]) {
        return None;
    }
    let goal_id = object.get("goalId")?.as_str()?;
    let turn_id = object.get("turnId")?.as_str()?;
    let revision = object.get("revision")?.as_f64()?;
    if !is_non_empty_string(goal_id)
        || !is_non_empty_string(turn_id)
        || !revision.is_finite()
        || revision.fract() != 0.0
        || revision < 1.0
    {
        return None;
    }
    Some(GoalEvidenceTurnContext {
        goal_id: goal_id.to_owned(),
        revision,
        turn_id: turn_id.to_owned(),
    })
}

fn record_goal_context(record: &TranscriptRecord) -> Option<&Value> {
    record_extra(record, "goalContext")
}
fn record_extra<'a>(record: &'a TranscriptRecord, key: &str) -> Option<&'a Value> {
    record.extra.get(key)
}
fn record_parts(record: &TranscriptRecord) -> &[Value] {
    record
        .message
        .as_ref()
        .and_then(|message| message.parts.as_deref())
        .unwrap_or_default()
}
fn has_only_keys(map: &Map<String, Value>, keys: &[&str]) -> bool {
    map.keys().all(|key| keys.contains(&key.as_str()))
}
fn is_non_empty_string(value: &str) -> bool {
    !trim_js(value).is_empty()
}
fn serialized_catalog_entry_bytes(entry: &GoalEvidenceCatalogEntry) -> usize {
    serde_json::to_vec(entry).map_or(usize::MAX, |json| json.len())
}
fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}
fn truncate_utf16(value: &str, max_units: usize) -> String {
    let mut units = 0;
    let mut end = 0;
    for (index, character) in value.char_indices() {
        let next = units + character.len_utf16();
        if next > max_units {
            break;
        }
        units = next;
        end = index + character.len_utf8();
    }
    value[..end].to_owned()
}
fn trim_js(value: &str) -> &str {
    value.trim_matches(|character: char| character.is_whitespace() || character == '\u{feff}')
}
fn append_preview(current: &str, value: &str) -> String {
    append_preview_values(
        current,
        &[truncate_utf16(value, GOAL_EVIDENCE_CATALOG_PREVIEW_LIMIT)],
    )
}
fn append_preview_values(current: &str, values: &[String]) -> String {
    let mut preview = current.to_owned();
    for value in values {
        if value.is_empty() || utf16_len(&preview) >= GOAL_EVIDENCE_CATALOG_PREVIEW_LIMIT {
            continue;
        }
        let separator = if preview.is_empty() { "" } else { "\n" };
        let remaining = GOAL_EVIDENCE_CATALOG_PREVIEW_LIMIT.saturating_sub(utf16_len(&preview));
        let segment = format!("{separator}{value}");
        preview.push_str(&truncate_utf16(&segment, remaining));
    }
    preview
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goals::protocol::{
        GoalBlockerKind, GoalStatus, GoalTerminalProposalStatus, TranscriptCursor,
    };
    use crate::transcript::TranscriptMessage;
    use serde_json::json;

    fn goal(cursor: Option<&str>) -> GoalRecord {
        GoalRecord {
            goal_id: "goal-1".to_owned(),
            revision: 2,
            objective: "Ship the change".to_owned(),
            status: GoalStatus::Active,
            evidence_cursor: TranscriptCursor {
                record_id: cursor.map(str::to_owned),
            },
            turn_count: 2,
            active_time_ms: 1.0,
            created_at: 1.0,
            updated_at: 2.0,
            evidence_checkpoint: None,
            last_reason: None,
            limit_kind: None,
        }
    }
    fn permit(turn: &str) -> GoalTurnPermit {
        GoalTurnPermit {
            goal_id: "goal-1".to_owned(),
            revision: 2,
            turn_id: turn.to_owned(),
        }
    }
    fn record(
        uuid: &str,
        kind: TranscriptRecordType,
        turn: Option<&str>,
        provenance: Option<&str>,
        text: Option<&str>,
    ) -> TranscriptRecord {
        let mut extra = std::collections::BTreeMap::new();
        if let Some(provenance) = provenance {
            extra.insert("provenance".to_owned(), json!(provenance));
        }
        if let Some(turn) = turn {
            extra.insert(
                "goalContext".to_owned(),
                json!({"goalId":"goal-1","revision":2,"turnId":turn}),
            );
        }
        let message = text.map(|text| TranscriptMessage {
            role: None,
            parts: Some(vec![json!({"text":text})]),
        });
        TranscriptRecord {
            uuid: uuid.to_owned(),
            parent_uuid: None,
            session_id: "s".to_owned(),
            record_type: kind,
            subtype: None,
            timestamp: None,
            message,
            extra,
        }
    }
    fn context<'a>(
        records: &'a [TranscriptRecord],
        goal: &'a GoalRecord,
        permit: &'a GoalTurnPermit,
    ) -> GoalEvidenceContext<'a> {
        GoalEvidenceContext {
            records,
            goal,
            permit,
        }
    }
    fn proposal(refs: &[&str]) -> GoalTerminalProposal {
        GoalTerminalProposal {
            status: GoalTerminalProposalStatus::Complete,
            reason: "done".to_owned(),
            evidence_refs: refs.iter().map(|value| (*value).to_owned()).collect(),
            blocker_kind: None,
        }
    }

    #[test]
    fn catalog_is_bounded_and_keeps_newest_entries() {
        let mut records = vec![record(
            "cursor",
            TranscriptRecordType::System,
            None,
            Some("goal_control"),
            None,
        )];
        records.extend((0..101).map(|index| {
            record(
                &format!("e{index}"),
                TranscriptRecordType::Assistant,
                Some("turn-3"),
                Some("assistant_output"),
                Some(&format!("output {index}")),
            )
        }));
        let goal = goal(Some("cursor"));
        let turn_permit = permit("turn-3");
        let catalog = build_goal_evidence_catalog(&context(&records, &goal, &turn_permit)).unwrap();
        assert!(catalog.truncated);
        assert_eq!(catalog.entries.len(), 100);
        assert_eq!(catalog.entries.last().unwrap().uuid, "e100");
        let error = validate_goal_evidence_references(&GoalEvidenceValidationInput {
            context: context(&records, &goal, &turn_permit),
            proposal: &proposal(&["e100"]),
        })
        .unwrap_err();
        assert_eq!(
            match error {
                EvidenceSourceUnavailableErrorOrReference::Reference(error) => error.code,
                _ => panic!("expected reference error"),
            },
            InvalidGoalEvidenceReferenceCode::CatalogTruncated
        );
    }

    #[test]
    fn lineage_and_context_fail_closed() {
        let goal = goal(Some("cursor"));
        let turn_permit = permit("turn-3");
        let malformed = vec![
            record("cursor", TranscriptRecordType::System, None, None, None),
            {
                let mut record = record(
                    "bad",
                    TranscriptRecordType::Assistant,
                    None,
                    Some("assistant_output"),
                    Some("text"),
                );
                record.extra.insert(
                    "goalContext".to_owned(),
                    json!({"goalId":"goal-1","revision":2}),
                );
                record
            },
        ];
        let error =
            build_goal_evidence_catalog(&context(&malformed, &goal, &turn_permit)).unwrap_err();
        assert_eq!(
            error.code,
            EvidenceSourceUnavailableCode::MalformedTurnContext
        );
        let reentry = vec![
            record("cursor", TranscriptRecordType::System, None, None, None),
            record(
                "a1",
                TranscriptRecordType::Assistant,
                Some("a"),
                None,
                Some("a"),
            ),
            record(
                "b",
                TranscriptRecordType::Assistant,
                Some("b"),
                None,
                Some("b"),
            ),
            record(
                "a2",
                TranscriptRecordType::Assistant,
                Some("a"),
                None,
                Some("a"),
            ),
        ];
        assert_eq!(
            build_goal_evidence_catalog(&context(&reentry, &goal, &permit("a")))
                .unwrap_err()
                .code,
            EvidenceSourceUnavailableCode::TurnReentry
        );
    }

    #[test]
    fn validates_provenance_and_user_display_projection() {
        let mut user = record(
            "user",
            TranscriptRecordType::User,
            Some("turn-3"),
            Some("real_user"),
            Some("model prompt"),
        );
        user.extra.insert(
            "systemPayload".to_owned(),
            json!({"displayText":"raw prompt","hookContext":"private context"}),
        );
        let records = vec![
            record("cursor", TranscriptRecordType::System, None, None, None),
            user,
        ];
        let goal = goal(Some("cursor"));
        let permit = permit("turn-3");
        let evidence = validate_goal_evidence_references(&GoalEvidenceValidationInput {
            context: context(&records, &goal, &permit),
            proposal: &proposal(&["user"]),
        })
        .unwrap();
        assert_eq!(evidence.cited_records[0].content, "raw prompt");
        assert_eq!(
            evidence.cited_records[0].proof_kind,
            GoalEvidenceProofKind::UserInput
        );
    }

    #[test]
    fn checkpoint_window_caps_content_and_requires_threshold() {
        let mut records = vec![record(
            "cursor",
            TranscriptRecordType::System,
            None,
            None,
            None,
        )];
        records.extend((0..80).map(|index| {
            let content = if index == 0 {
                format!("中{}", "x".repeat(4_000))
            } else {
                "small".to_owned()
            };
            record(
                &format!("e{index}"),
                TranscriptRecordType::Assistant,
                Some("turn-3"),
                Some("assistant_output"),
                Some(&content),
            )
        }));
        let goal = goal(Some("cursor"));
        let permit = permit("turn-3");
        let window =
            build_goal_evidence_checkpoint_window(&context(&records, &goal, &permit)).unwrap();
        assert!(window.should_checkpoint);
        assert_eq!(window.evidence.len(), 80);
        assert!(
            window.evidence[0]
                .content
                .ends_with(CHECKPOINT_CONTENT_TRUNCATION_MARKER)
        );
        assert!(window.evidence[0].content.len() <= GOAL_EVIDENCE_CHECKPOINT_CONTENT_BYTE_LIMIT);
        assert!(window.evidence[0].content.starts_with("中"));
    }

    #[test]
    fn immediate_blocker_requires_external_evidence_and_newer_coverage() {
        let records = vec![
            record("cursor", TranscriptRecordType::System, None, None, None),
            record(
                "user",
                TranscriptRecordType::User,
                Some("turn-2"),
                None,
                Some("No access"),
            ),
            record(
                "answer",
                TranscriptRecordType::Assistant,
                Some("turn-3"),
                None,
                Some("Need access"),
            ),
        ];
        let goal = goal(Some("cursor"));
        let permit = permit("turn-3");
        let blocked = GoalTerminalProposal {
            status: GoalTerminalProposalStatus::Blocked,
            reason: "blocked".to_owned(),
            evidence_refs: vec!["user".to_owned(), "answer".to_owned()],
            blocker_kind: Some(GoalBlockerKind::External),
        };
        assert_eq!(
            validate_goal_evidence_references(&GoalEvidenceValidationInput {
                context: context(&records, &goal, &permit),
                proposal: &blocked
            })
            .unwrap()
            .cited_records
            .len(),
            2
        );
        let omitted = GoalTerminalProposal {
            evidence_refs: vec!["user".to_owned()],
            ..blocked
        };
        let error = validate_goal_evidence_references(&GoalEvidenceValidationInput {
            context: context(&records, &goal, &permit),
            proposal: &omitted,
        })
        .unwrap_err();
        assert!(matches!(
            error,
            EvidenceSourceUnavailableErrorOrReference::Reference(
                InvalidGoalEvidenceReferenceError {
                    code: InvalidGoalEvidenceReferenceCode::ImmediateBlockerNewerEvidenceRequired,
                    ..
                }
            )
        ));
    }
}
