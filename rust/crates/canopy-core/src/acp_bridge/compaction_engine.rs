//! Bounded per-session live journal and turn-boundary replay compaction.
//!
//! The event bus reconnect ring is a separate transport buffer. This engine
//! owns the longer lived replay projection and in-flight turn journal, and its
//! adaptive journal limits draw on a daemon-shared growth registry when one is
//! supplied by the runtime.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use thiserror::Error;
use uuid::Uuid;

use super::event_bus::{BridgeEvent, EVENT_SCHEMA_VERSION};
use super::journal_growth_policy::{
    JournalGrowthGrant, JournalGrowthPolicy, JournalGrowthPolicyOptions,
};
use super::replay_window_limits::{
    DEFAULT_COMPACTED_REPLAY_MAX_BYTES, DEFAULT_MAX_JOURNAL_BYTES, DEFAULT_MAX_JOURNAL_EVENTS,
    JOURNAL_GROWTH_HARD_CAP_BYTES, JournalGrowthSessionLimit, MAX_COMPACTED_REPLAY_MAX_BYTES,
    MAX_SAFE_INTEGER,
};

const GROWTH_REASK_INTERVAL_MS: u128 = 10_000;
const MAX_GROWTH_GRANTS_PER_BREACH: usize = 64;
const TRANSIENT_TYPES: [&str; 5] = [
    "history_truncated",
    "slow_client_warning",
    "client_evicted",
    "replay_complete",
    "stream_error",
];

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LiveReplayMode {
    Full,
    Summary,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionReplaySnapshot {
    pub compacted_turns: Vec<BridgeEvent>,
    pub live_journal: Vec<BridgeEvent>,
    pub last_event_id: u64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalLimits {
    pub max_events: u64,
    pub max_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionMemoryStats {
    pub compacted_replay_bytes: u64,
    pub compacted_replay_events: u64,
    pub full_journal_bytes: u64,
    pub full_journal_events: u64,
    pub summary_journal_bytes: u64,
    pub summary_journal_events: u64,
}

/// A shared, stateless-policy accounting table. Clone one `Arc` into every
/// workspace runtime that must draw from the same daemon-wide byte pool.
#[derive(Debug, Default)]
pub struct JournalGrowthRegistry {
    sessions: Mutex<HashMap<Uuid, JournalGrowthSessionLimit>>,
}

impl JournalGrowthRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn register(&self, baseline_bytes: u64) -> Uuid {
        let id = Uuid::new_v4();
        lock(&self.sessions).insert(
            id,
            JournalGrowthSessionLimit {
                limit_bytes: baseline_bytes,
                baseline_bytes,
            },
        );
        id
    }

    fn grant(
        &self,
        id: Uuid,
        policy: &JournalGrowthPolicy,
        current: JournalLimits,
    ) -> Option<JournalGrowthGrant> {
        let mut sessions = lock(&self.sessions);
        let requester = sessions.get_mut(&id)?;
        requester.limit_bytes = current.max_bytes;
        let all_limits: Vec<_> = sessions.values().copied().collect();
        let grant = policy.grant(super::journal_growth_policy::JournalGrowthRequest {
            current_max_events: current.max_events,
            current_max_bytes: current.max_bytes,
            all_session_limits: &all_limits,
        })?;
        if let Some(requester) = sessions.get_mut(&id) {
            requester.limit_bytes = grant.max_bytes;
        }
        Some(grant)
    }

    fn release(&self, id: Uuid) {
        lock(&self.sessions).remove(&id);
    }

    fn set_limit(&self, id: Uuid, limit_bytes: u64) {
        if let Some(session) = lock(&self.sessions).get_mut(&id) {
            session.limit_bytes = limit_bytes;
        }
    }

    pub fn session_limits(&self) -> Vec<JournalGrowthSessionLimit> {
        lock(&self.sessions).values().copied().collect()
    }
}

/// Per-bus compaction and growth configuration. `growth_registry` is optional;
/// when supplied, it should be shared by runtimes consuming one daemon pool.
#[derive(Clone, Debug)]
pub struct SessionCompactionOptions {
    pub max_replay_bytes: u64,
    pub max_journal_events: u64,
    pub max_journal_bytes: u64,
    pub journal_growth_pool_bytes: Option<u64>,
    pub growth_registry: Option<Arc<JournalGrowthRegistry>>,
}

impl Default for SessionCompactionOptions {
    fn default() -> Self {
        Self {
            max_replay_bytes: DEFAULT_COMPACTED_REPLAY_MAX_BYTES,
            max_journal_events: DEFAULT_MAX_JOURNAL_EVENTS,
            max_journal_bytes: DEFAULT_MAX_JOURNAL_BYTES,
            journal_growth_pool_bytes: None,
            growth_registry: None,
        }
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CompactionConfigError {
    #[error("Invalid {name}: {value}. Must be a positive safe integer.")]
    InvalidLimit { name: &'static str, value: u64 },
    #[error(
        "Invalid compactedReplayMaxBytes: {value}. Must be a positive safe integer in [1, {maximum}]."
    )]
    InvalidReplayLimit { value: u64, maximum: u64 },
}

#[derive(Clone, Debug)]
struct JournalEntry {
    event: BridgeEvent,
    bytes: u64,
}

#[derive(Clone, Debug, Default)]
struct LiveJournalState {
    entries: VecDeque<JournalEntry>,
    bytes: u64,
    truncated_events: u64,
    latest_record_id: Option<String>,
}

impl LiveJournalState {
    fn clear_entries(&mut self) {
        self.entries.clear();
        self.bytes = 0;
        self.truncated_events = 0;
    }

    fn clear(&mut self) {
        self.clear_entries();
        self.latest_record_id = None;
    }
}

#[derive(Clone)]
struct ReplaySegment {
    events: Vec<BridgeEvent>,
    bytes: u64,
}

enum TurnSlot {
    Text {
        update_type: String,
        parent_tool_call_id: Option<String>,
        source_record_ids: Option<Vec<String>>,
        chunks: Vec<String>,
        chunk_bytes: Vec<u64>,
        chunk_metas: Vec<Option<Value>>,
        last_event_id: Option<u64>,
        last_meta: Option<Value>,
        last_envelope_meta: Option<Map<String, Value>>,
        last_prompt_id: Option<String>,
        last_originator_client_id: Option<String>,
        last_session_id: Option<String>,
        source_bytes: u64,
        source_events: u64,
    },
    Tool {
        tool_call_id: String,
        event: BridgeEvent,
    },
    LatestWins {
        key: String,
        event: BridgeEvent,
    },
    Misc(BridgeEvent),
}

#[derive(Clone)]
struct TextSlotIndexEntry {
    source_record_ids: Option<Vec<String>>,
    index: usize,
}

impl TurnSlot {
    fn bytes(&self) -> u64 {
        match self {
            Self::Text { source_bytes, .. } => *source_bytes,
            Self::Tool { event, .. } | Self::LatestWins { event, .. } | Self::Misc(event) => {
                serialized_bytes(event)
            }
        }
    }

    fn event_count(&self) -> u64 {
        match self {
            Self::Text { source_events, .. } => *source_events,
            _ => 1,
        }
    }

    fn into_event(self) -> BridgeEvent {
        match self {
            Self::Text {
                update_type,
                chunks,
                last_event_id,
                last_meta,
                last_envelope_meta,
                last_prompt_id,
                last_originator_client_id,
                last_session_id,
                ..
            } => make_merged_text_event(
                update_type,
                chunks.concat(),
                last_event_id,
                last_meta,
                last_envelope_meta,
                last_prompt_id,
                last_originator_client_id,
                last_session_id,
            ),
            Self::Tool { mut event, .. } => {
                normalize_tool_call_type(&mut event);
                event
            }
            Self::LatestWins { event, .. } | Self::Misc(event) => event,
        }
    }
}

/// Turn-boundary replay compaction with bounded live journals and adaptive
/// journal growth. The event bus invokes this only after an event passes its
/// max-event admission check; ring retention remains independent.
pub struct TurnBoundaryCompactionEngine {
    options: SessionCompactionOptions,
    max_journal_events: u64,
    max_journal_bytes: u64,
    growth_registry: Option<Arc<JournalGrowthRegistry>>,
    growth_registration: Option<Uuid>,
    growth_policy: Option<JournalGrowthPolicy>,
    growth_denied_at: Option<Instant>,
    last_event_id: u64,
    replay: VecDeque<ReplaySegment>,
    replay_bytes: u64,
    full_journal: LiveJournalState,
    summary_journal: LiveJournalState,
    turn_slots: Vec<TurnSlot>,
    text_slot_index: HashMap<(String, String), Vec<TextSlotIndexEntry>>,
    turn_slot_bytes: u64,
    turn_truncated_events: u64,
    truncated_replay_events: u64,
    closed: bool,
}

impl TurnBoundaryCompactionEngine {
    pub fn new(options: SessionCompactionOptions) -> Result<Self, CompactionConfigError> {
        validate_limit("maxJournalEvents", options.max_journal_events)?;
        validate_limit("maxJournalBytes", options.max_journal_bytes)?;
        if options.max_replay_bytes == 0
            || options.max_replay_bytes > MAX_COMPACTED_REPLAY_MAX_BYTES
        {
            return Err(CompactionConfigError::InvalidReplayLimit {
                value: options.max_replay_bytes,
                maximum: MAX_COMPACTED_REPLAY_MAX_BYTES,
            });
        }
        if let Some(pool_bytes) = options.journal_growth_pool_bytes {
            validate_limit("journalGrowthPoolBytes", pool_bytes)?;
        }

        let growth_registry = options.journal_growth_pool_bytes.map(|_| {
            options
                .growth_registry
                .clone()
                .unwrap_or_else(|| Arc::new(JournalGrowthRegistry::new()))
        });
        let growth_registration = growth_registry
            .as_ref()
            .map(|registry| registry.register(options.max_journal_bytes));
        let growth_policy = options.journal_growth_pool_bytes.map(|pool_bytes| {
            JournalGrowthPolicy::new(JournalGrowthPolicyOptions {
                baseline_events: options.max_journal_events,
                baseline_bytes: options.max_journal_bytes,
                pool_bytes,
                hard_cap_bytes: JOURNAL_GROWTH_HARD_CAP_BYTES,
            })
        });

        Ok(Self {
            max_journal_events: options.max_journal_events,
            max_journal_bytes: options.max_journal_bytes,
            options,
            growth_registry,
            growth_registration,
            growth_policy,
            growth_denied_at: None,
            last_event_id: 0,
            replay: VecDeque::new(),
            replay_bytes: 0,
            full_journal: LiveJournalState::default(),
            summary_journal: LiveJournalState::default(),
            turn_slots: Vec::new(),
            text_slot_index: HashMap::new(),
            turn_slot_bytes: 0,
            turn_truncated_events: 0,
            truncated_replay_events: 0,
            closed: false,
        })
    }

    pub fn journal_limits(&self) -> JournalLimits {
        JournalLimits {
            max_events: self.max_journal_events,
            max_bytes: self.max_journal_bytes,
        }
    }

    pub fn memory_stats(&self) -> CompactionMemoryStats {
        CompactionMemoryStats {
            compacted_replay_bytes: self.replay_bytes,
            compacted_replay_events: self.replay.iter().fold(0_u64, |sum, segment| {
                sum.saturating_add(segment.events.len() as u64)
            }),
            full_journal_bytes: self.full_journal.bytes,
            full_journal_events: self.full_journal.entries.len() as u64,
            summary_journal_bytes: self.summary_journal.bytes,
            summary_journal_events: self.summary_journal.entries.len() as u64,
        }
    }

    pub fn ingest(&mut self, event: BridgeEvent, byte_length: Option<u64>) {
        if self.closed {
            return;
        }
        if let Some(id) = event.id {
            self.last_event_id = id;
        }
        if is_transient(&event.event_type) {
            return;
        }
        let bytes = byte_length.unwrap_or_else(|| serialized_bytes(&event));
        let turn_boundary = is_turn_boundary(&event.event_type);
        self.append_live_journal(event.clone(), bytes, turn_boundary);

        if turn_boundary {
            self.finish_turn(event);
            return;
        }
        self.append_turn_event(event, bytes);
    }

    pub fn snapshot(&self, mode: LiveReplayMode) -> SessionReplaySnapshot {
        let mut compacted_turns: Vec<_> = self
            .replay
            .iter()
            .flat_map(|segment| segment.events.iter().cloned())
            .collect();
        if self.truncated_replay_events > 0 {
            compacted_turns.insert(
                0,
                BridgeEvent::new(
                    "history_truncated",
                    json!({
                        "reason":"replay_window_exceeded",
                        "scope":"compacted_replay",
                        "truncatedEvents":self.truncated_replay_events,
                        "retainedEvents":self.replay.iter().fold(0_usize, |sum, segment| {
                            sum.saturating_add(segment.events.len())
                        }),
                        "maxBytes":self.options.max_replay_bytes,
                        "fullTranscriptAvailable":true
                    }),
                ),
            );
        }
        let journal = match mode {
            LiveReplayMode::Full => &self.full_journal,
            LiveReplayMode::Summary => &self.summary_journal,
        };
        let mut live_journal = Vec::new();
        let mut text_segment_bytes = 0_u64;
        let mut text_segment_chunks = 0_usize;
        for entry in &journal.entries {
            let update_type = session_update_type(&entry.event);
            let is_text = matches!(
                update_type.as_deref(),
                Some("agent_message_chunk" | "agent_thought_chunk")
            );
            let can_merge = is_text
                && live_journal_text_chunk(&entry.event)
                && text_segment_chunks < 256
                && text_segment_bytes.saturating_add(entry.bytes) <= self.max_journal_bytes
                && live_journal.last().is_some_and(|previous| {
                    live_journal_text_chunk(previous)
                        && session_update_type(previous) == update_type
                        && compatible_live_text_events(previous, &entry.event)
                });
            if can_merge {
                if let Some(previous) = live_journal.last_mut() {
                    let mut text = session_update_text(previous).unwrap_or_default().to_owned();
                    text.push_str(session_update_text(&entry.event).unwrap_or_default());
                    *previous = merge_live_text_events(previous, &entry.event, text);
                }
                text_segment_bytes = text_segment_bytes.saturating_add(entry.bytes);
                text_segment_chunks = text_segment_chunks.saturating_add(1);
            } else {
                live_journal.push(entry.event.clone());
                if is_text {
                    text_segment_bytes = entry.bytes;
                    text_segment_chunks = 1;
                } else {
                    text_segment_bytes = 0;
                    text_segment_chunks = 0;
                }
            }
        }
        if journal.truncated_events > 0 {
            let mut marker = json!({
                "reason":"replay_window_exceeded",
                "scope":"live_journal",
                "truncatedEvents":journal.truncated_events,
                "retainedEvents":journal.entries.len(),
                "maxBytes":self.max_journal_bytes,
                "maxEvents":self.max_journal_events,
                "fullTranscriptAvailable":true
            });
            if let Some(record_id) = &journal.latest_record_id {
                marker["recordId"] = json!(record_id);
            }
            live_journal.insert(0, BridgeEvent::new("history_truncated", marker));
        }
        SessionReplaySnapshot {
            compacted_turns,
            live_journal,
            last_event_id: self.last_event_id,
        }
    }

    /// Seed retained history as compacted replay frames. Seed frames do not
    /// enter the live journal and do not alter the reconnect ring.
    pub fn seed_replay_events(&mut self, events: Vec<BridgeEvent>) {
        if self.closed {
            return;
        }
        self.replay.clear();
        self.replay_bytes = 0;
        self.truncated_replay_events = 0;
        self.full_journal.clear();
        self.summary_journal.clear();
        self.turn_slots.clear();
        self.text_slot_index.clear();
        self.turn_slot_bytes = 0;
        self.turn_truncated_events = 0;
        self.growth_denied_at = None;
        self.last_event_id = 0;
        let mut seeded_record_id: Option<String> = None;
        let mut seeded_segment = Vec::new();
        let flush_seed_segment = |engine: &mut Self, segment: &mut Vec<BridgeEvent>| {
            if !segment.is_empty() {
                engine.append_replay_segment(std::mem::take(segment));
            }
        };
        for event in events {
            if let Some(id) = event.id {
                self.last_event_id = id;
            }
            if let Some(record_id) = replay_record_id(&event) {
                self.full_journal.latest_record_id = Some(record_id.clone());
                if summary_event(&event) {
                    self.summary_journal.latest_record_id = Some(record_id);
                }
            }
            if is_transient(&event.event_type) {
                continue;
            }
            let next_record_id = replay_record_id(&event);
            if next_record_id.is_none() {
                flush_seed_segment(self, &mut seeded_segment);
                seeded_record_id = None;
                self.append_replay_segment(vec![event]);
                continue;
            }
            if seeded_record_id
                .as_deref()
                .is_some_and(|current| Some(current) != next_record_id.as_deref())
            {
                flush_seed_segment(self, &mut seeded_segment);
            }
            seeded_record_id = next_record_id;
            seeded_segment.push(event);
        }
        flush_seed_segment(self, &mut seeded_segment);
    }

    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.replay.clear();
        self.replay_bytes = 0;
        self.full_journal.clear();
        self.summary_journal.clear();
        self.turn_slots.clear();
        self.text_slot_index.clear();
        self.turn_slot_bytes = 0;
        if let (Some(registry), Some(registration)) =
            (&self.growth_registry, self.growth_registration.take())
        {
            registry.release(registration);
        }
    }

    fn append_live_journal(&mut self, event: BridgeEvent, bytes: u64, turn_boundary: bool) {
        let summary = summary_event(&event);
        if let Some(record_id) = replay_record_id(&event) {
            self.full_journal.latest_record_id = Some(record_id.clone());
            if summary {
                self.summary_journal.latest_record_id = Some(record_id);
            }
        }
        self.append_live_journal_entry(false, event.clone(), bytes, turn_boundary);
        if summary {
            self.append_live_journal_entry(true, event, bytes, turn_boundary);
        }
    }

    fn append_live_journal_entry(
        &mut self,
        summary: bool,
        event: BridgeEvent,
        bytes: u64,
        turn_boundary: bool,
    ) {
        {
            let journal = if summary {
                &mut self.summary_journal
            } else {
                &mut self.full_journal
            };
            journal.entries.push_back(JournalEntry { event, bytes });
            journal.bytes = journal.bytes.saturating_add(bytes);
        }
        if !turn_boundary && self.journal_over_limit(summary) {
            self.maybe_grow_journal(summary);
        }
        while self.journal_over_limit(summary) {
            let journal = if summary {
                &mut self.summary_journal
            } else {
                &mut self.full_journal
            };
            if journal.entries.len() <= 1 {
                break;
            }
            if let Some(entry) = journal.entries.pop_front() {
                journal.bytes = journal.bytes.saturating_sub(entry.bytes);
                journal.truncated_events = journal.truncated_events.saturating_add(1);
            }
        }
    }

    fn journal_over_limit(&self, summary: bool) -> bool {
        let journal = if summary {
            &self.summary_journal
        } else {
            &self.full_journal
        };
        journal.entries.len() as u64 > self.max_journal_events
            || (journal.bytes > self.max_journal_bytes && journal.entries.len() > 1)
    }

    fn maybe_grow_journal(&mut self, summary: bool) {
        let (Some(registry), Some(registration), Some(policy)) = (
            &self.growth_registry,
            self.growth_registration,
            &self.growth_policy,
        ) else {
            return;
        };
        let now = Instant::now();
        if self
            .growth_denied_at
            .is_some_and(|denied| now.duration_since(denied).as_millis() < GROWTH_REASK_INTERVAL_MS)
        {
            return;
        }
        let original = self.journal_limits();
        let original_retained = if summary {
            retained_tail_count(
                &self.summary_journal.entries,
                original.max_events,
                original.max_bytes,
            )
        } else {
            retained_tail_count(
                &self.full_journal.entries,
                original.max_events,
                original.max_bytes,
            )
        };
        for _ in 0..MAX_GROWTH_GRANTS_PER_BREACH {
            let Some(grant) = registry.grant(registration, policy, self.journal_limits()) else {
                break;
            };
            if grant.max_bytes <= self.max_journal_bytes
                || grant.max_events < self.max_journal_events
            {
                break;
            }
            self.max_journal_bytes = grant.max_bytes;
            self.max_journal_events = grant.max_events;
            let retained = if summary {
                retained_tail_count(
                    &self.summary_journal.entries,
                    self.max_journal_events,
                    self.max_journal_bytes,
                )
            } else {
                retained_tail_count(
                    &self.full_journal.entries,
                    self.max_journal_events,
                    self.max_journal_bytes,
                )
            };
            if retained > original_retained {
                self.growth_denied_at = None;
                return;
            }
        }
        self.max_journal_bytes = original.max_bytes;
        self.max_journal_events = original.max_events;
        registry.set_limit(registration, original.max_bytes);
        self.growth_denied_at = Some(now);
    }

    fn append_turn_event(&mut self, event: BridgeEvent, bytes: u64) {
        let update_type = session_update_type(&event);
        let update_meta = update_update_meta(&event).cloned();
        let parent_tool_call_id = extract_parent_tool_call_id(update_meta.as_ref());
        let source_record_ids = extract_source_record_ids(update_meta.as_ref());
        if let Some(update_type) = update_type.as_deref()
            && matches!(update_type, "agent_message_chunk" | "agent_thought_chunk")
            && !has_discrete_message_meta(update_meta.as_ref())
        {
            let existing_index = parent_tool_call_id.as_ref().and_then(|parent_id| {
                let key = (update_type.to_owned(), parent_id.clone());
                self.text_slot_index
                    .get(&key)
                    .and_then(|entries| {
                        entries.iter().find(|entry| {
                            entry.source_record_ids.as_ref() == source_record_ids.as_ref()
                        })
                    })
                    .map(|entry| entry.index)
                    .filter(|index| {
                        matches!(
                            self.turn_slots.get(*index),
                            Some(TurnSlot::Text {
                                update_type: indexed_type,
                                parent_tool_call_id: Some(indexed_parent),
                                source_record_ids: indexed_source_ids,
                                ..
                            }) if indexed_type == update_type
                                && indexed_parent == parent_id
                                && indexed_source_ids == &source_record_ids
                        )
                    })
            });

            if let Some(index) = existing_index {
                append_text_slot_chunk(
                    &mut self.turn_slots[index],
                    &event,
                    update_meta.as_ref(),
                    bytes,
                );
            } else if let Some(parent_id) = parent_tool_call_id.as_ref() {
                let index = self.turn_slots.len();
                self.turn_slots.push(TurnSlot::Text {
                    chunks: vec![session_update_text(&event).unwrap_or_default().to_owned()],
                    chunk_bytes: vec![bytes],
                    update_type: update_type.to_owned(),
                    parent_tool_call_id: parent_tool_call_id.clone(),
                    source_record_ids: source_record_ids.clone(),
                    chunk_metas: vec![update_meta.clone()],
                    last_event_id: event.id,
                    last_meta: update_meta,
                    last_envelope_meta: event.meta.clone(),
                    last_prompt_id: event.prompt_id.clone(),
                    last_originator_client_id: event.originator_client_id.clone(),
                    last_session_id: capture_session_id(&event),
                    source_bytes: bytes,
                    source_events: 1,
                });
                self.text_slot_index
                    .entry((update_type.to_owned(), parent_id.clone()))
                    .or_default()
                    .push(TextSlotIndexEntry {
                        source_record_ids,
                        index,
                    });
            } else if self.turn_slots.last().is_some_and(|slot| {
                matches!(
                    slot,
                    TurnSlot::Text {
                        update_type: previous_type,
                        parent_tool_call_id: None,
                        source_record_ids: previous_source_record_ids,
                        ..
                    } if previous_type == update_type
                        && previous_source_record_ids == &source_record_ids
                )
            }) {
                let last_index = self.turn_slots.len() - 1;
                append_text_slot_chunk(
                    &mut self.turn_slots[last_index],
                    &event,
                    update_meta.as_ref(),
                    bytes,
                );
            } else {
                self.turn_slots.push(TurnSlot::Text {
                    chunks: vec![session_update_text(&event).unwrap_or_default().to_owned()],
                    chunk_bytes: vec![bytes],
                    update_type: update_type.to_owned(),
                    parent_tool_call_id: None,
                    source_record_ids,
                    chunk_metas: vec![update_meta.clone()],
                    last_event_id: event.id,
                    last_meta: update_meta,
                    last_envelope_meta: event.meta.clone(),
                    last_prompt_id: event.prompt_id.clone(),
                    last_originator_client_id: event.originator_client_id.clone(),
                    last_session_id: capture_session_id(&event),
                    source_bytes: bytes,
                    source_events: 1,
                });
            }
            self.recompute_turn_bytes();
            self.enforce_turn_limits();
            return;
        } else if matches!(
            update_type.as_deref(),
            Some("tool_call" | "tool_call_update")
        ) {
            if let Some(tool_call_id) = session_update_tool_call_id(&event) {
                if let Some(existing) = self.turn_slots.iter_mut().find(|slot| {
                    matches!(slot, TurnSlot::Tool { tool_call_id: id, .. } if id == &tool_call_id)
                }) {
                    if let TurnSlot::Tool { event: previous, .. } = existing {
                        *previous = merge_tool_call_event(previous, &event);
                    }
                } else {
                    let parent_id = extract_parent_tool_call_id(update_update_meta(&event));
                    self.turn_slots.push(TurnSlot::Tool { tool_call_id, event });
                    if let Some(parent_id) = parent_id {
                        self.text_slot_index
                            .remove(&("agent_message_chunk".to_owned(), parent_id.clone()));
                        self.text_slot_index
                            .remove(&("agent_thought_chunk".to_owned(), parent_id));
                    }
                }
            } else {
                self.turn_slots.push(TurnSlot::Misc(event));
            }
        } else if let Some(key) = update_type
            .as_deref()
            .filter(|value| matches!(*value, "available_commands_update" | "current_mode_update"))
        {
            if let Some(existing) = self.turn_slots.iter_mut().find(|slot| {
                matches!(slot, TurnSlot::LatestWins { key: existing, .. } if existing == key)
            }) {
                if let TurnSlot::LatestWins { event: previous, .. } = existing {
                    *previous = event;
                }
            } else {
                self.turn_slots.push(TurnSlot::LatestWins {
                    key: key.to_owned(),
                    event,
                });
            }
        } else {
            self.turn_slots.push(TurnSlot::Misc(event));
        }
        self.recompute_turn_bytes();
        self.enforce_turn_limits();
    }

    fn recompute_turn_bytes(&mut self) {
        self.turn_slot_bytes = self
            .turn_slots
            .iter()
            .fold(0_u64, |sum, slot| sum.saturating_add(slot.bytes()));
    }

    fn enforce_turn_limits(&mut self) {
        let mut evicted_slots = false;
        while self.turn_accumulator_events() > self.options.max_journal_events
            || (self.turn_slot_bytes > self.options.max_journal_bytes && self.turn_slots.len() > 1)
        {
            if self.turn_slots.len() == 1 {
                let Some(TurnSlot::Text {
                    chunks,
                    chunk_bytes,
                    chunk_metas,
                    source_bytes,
                    source_events,
                    ..
                }) = self.turn_slots.first_mut()
                else {
                    break;
                };
                if chunks.len() <= 1 {
                    break;
                }
                chunks.remove(0);
                let removed_bytes = chunk_bytes.remove(0);
                chunk_metas.remove(0);
                *source_bytes = source_bytes.saturating_sub(removed_bytes);
                *source_events = source_events.saturating_sub(1);
                self.turn_slot_bytes = self.turn_slot_bytes.saturating_sub(removed_bytes);
                self.turn_truncated_events = self.turn_truncated_events.saturating_add(1);
                continue;
            }
            let dropped = self.turn_slots.remove(0);
            self.turn_slot_bytes = self.turn_slot_bytes.saturating_sub(dropped.bytes());
            self.turn_truncated_events = self
                .turn_truncated_events
                .saturating_add(dropped.event_count());
            evicted_slots = true;
        }
        if self.turn_slots.len() == 1
            && let Some(TurnSlot::Text {
                chunk_metas,
                last_meta,
                ..
            }) = self.turn_slots.first_mut()
        {
            *last_meta = chunk_metas.iter().fold(None, |merged, incoming| {
                merge_transcript_update_meta(merged.as_ref(), incoming.as_ref())
            });
        }
        if evicted_slots {
            self.rebuild_text_slot_index();
        }
    }

    fn rebuild_text_slot_index(&mut self) {
        self.text_slot_index.clear();
        for (index, slot) in self.turn_slots.iter().enumerate() {
            match slot {
                TurnSlot::Tool { event, .. } => {
                    if let Some(parent_id) = extract_parent_tool_call_id(update_update_meta(event))
                    {
                        self.text_slot_index
                            .remove(&("agent_message_chunk".to_owned(), parent_id.clone()));
                        self.text_slot_index
                            .remove(&("agent_thought_chunk".to_owned(), parent_id));
                    }
                }
                TurnSlot::Text {
                    update_type,
                    parent_tool_call_id: Some(parent_id),
                    source_record_ids,
                    ..
                } => {
                    self.text_slot_index
                        .entry((update_type.clone(), parent_id.clone()))
                        .or_default()
                        .push(TextSlotIndexEntry {
                            source_record_ids: source_record_ids.clone(),
                            index,
                        });
                }
                _ => {}
            }
        }
    }

    fn turn_accumulator_events(&self) -> u64 {
        self.turn_slots
            .iter()
            .fold(0_u64, |sum, slot| sum.saturating_add(slot.event_count()))
    }

    fn finish_turn(&mut self, boundary: BridgeEvent) {
        let mut compacted = Vec::new();
        if self.turn_truncated_events > 0 {
            compacted.push(BridgeEvent::new(
                "history_truncated",
                json!({
                    "reason":"replay_window_exceeded",
                    "scope":"turn_compaction",
                    "truncatedEvents":self.turn_truncated_events,
                    "retainedEvents":self.turn_slots.len(),
                    "maxBytes":self.options.max_journal_bytes,
                    "maxEvents":self.options.max_journal_events,
                    "fullTranscriptAvailable":true
                }),
            ));
        }
        compacted.extend(self.turn_slots.drain(..).map(TurnSlot::into_event));
        self.text_slot_index.clear();
        compacted.push(boundary.clone());
        self.append_replay_segment(compacted);
        self.turn_slot_bytes = 0;
        self.turn_truncated_events = 0;
        self.growth_denied_at = None;
        self.full_journal.clear_entries();
        self.summary_journal.clear_entries();
    }

    fn append_replay_segment(&mut self, events: Vec<BridgeEvent>) {
        let bytes = events.iter().fold(0_u64, |sum, event| {
            sum.saturating_add(serialized_bytes(event))
        });
        self.replay.push_back(ReplaySegment { events, bytes });
        self.replay_bytes = self.replay_bytes.saturating_add(bytes);
        while self.replay_bytes > self.options.max_replay_bytes && self.replay.len() > 1 {
            if let Some(segment) = self.replay.pop_front() {
                self.replay_bytes = self.replay_bytes.saturating_sub(segment.bytes);
                self.truncated_replay_events = self
                    .truncated_replay_events
                    .saturating_add(segment.events.len() as u64);
            }
        }
    }
}

impl Drop for TurnBoundaryCompactionEngine {
    fn drop(&mut self) {
        if let (Some(registry), Some(registration)) =
            (&self.growth_registry, self.growth_registration.take())
        {
            registry.release(registration);
        }
    }
}

fn validate_limit(name: &'static str, value: u64) -> Result<(), CompactionConfigError> {
    if value == 0 || value > MAX_SAFE_INTEGER {
        return Err(CompactionConfigError::InvalidLimit { name, value });
    }
    Ok(())
}

fn retained_tail_count(entries: &VecDeque<JournalEntry>, max_events: u64, max_bytes: u64) -> u64 {
    let mut count = 0_u64;
    let mut bytes = 0_u64;
    for entry in entries.iter().rev() {
        if count.saturating_add(1) > max_events
            || (count > 0 && bytes.saturating_add(entry.bytes) > max_bytes)
        {
            break;
        }
        count = count.saturating_add(1);
        bytes = bytes.saturating_add(entry.bytes);
    }
    count
}

fn serialized_bytes(event: &BridgeEvent) -> u64 {
    serde_json::to_vec(event).map_or(0, |value| value.len() as u64)
}

fn is_transient(event_type: &str) -> bool {
    TRANSIENT_TYPES.contains(&event_type)
}

fn is_turn_boundary(event_type: &str) -> bool {
    matches!(event_type, "turn_complete" | "turn_error")
}

fn session_update_type(event: &BridgeEvent) -> Option<String> {
    event
        .data
        .get("update")?
        .get("sessionUpdate")?
        .as_str()
        .map(str::to_owned)
        .filter(|_| event.event_type == "session_update")
}

fn session_update_text(event: &BridgeEvent) -> Option<&str> {
    event
        .data
        .get("update")?
        .get("content")?
        .get("text")?
        .as_str()
}

fn session_update_tool_call_id(event: &BridgeEvent) -> Option<String> {
    event
        .data
        .get("update")?
        .get("toolCallId")?
        .as_str()
        .map(str::to_owned)
        .filter(|value| !value.is_empty())
}

fn replay_record_id(event: &BridgeEvent) -> Option<String> {
    (event.event_type == "session_update")
        .then(|| {
            event
                .data
                .get("update")?
                .get("_meta")?
                .get("qwen.session.recordId")?
                .as_str()
                .map(str::to_owned)
        })
        .flatten()
}

fn update_update_meta(event: &BridgeEvent) -> Option<&Value> {
    event.data.get("update")?.get("_meta")
}

fn extract_parent_tool_call_id(meta: Option<&Value>) -> Option<String> {
    meta?
        .get("parentToolCallId")?
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn extract_source_record_ids(meta: Option<&Value>) -> Option<Vec<String>> {
    let transcript = json_object_spread(meta?.get("qwenTranscript")?)?;
    extract_source_ids_from_transcript(&transcript)
}

fn extract_source_ids_from_transcript(transcript: &Map<String, Value>) -> Option<Vec<String>> {
    let ids = transcript.get("sourceRecordIds")?.as_array()?;
    let mut unique = Vec::new();
    for id in ids.iter().filter_map(Value::as_str) {
        if !unique.iter().any(|existing| existing == id) {
            unique.push(id.to_owned());
        }
    }
    (!unique.is_empty()).then_some(unique)
}

fn has_discrete_message_meta(meta: Option<&Value>) -> bool {
    meta.and_then(|meta| meta.get("qwenDiscreteMessage")) == Some(&Value::Bool(true))
}

fn json_object_spread(value: &Value) -> Option<Map<String, Value>> {
    match value {
        Value::Object(map) => Some(map.clone()),
        Value::Array(values) => Some(
            values
                .iter()
                .enumerate()
                .map(|(index, value)| (index.to_string(), value.clone()))
                .collect(),
        ),
        _ => None,
    }
}

fn has_only_modeled_chunk_keys(event: &BridgeEvent) -> bool {
    let Some(data) = event.data.as_object() else {
        return false;
    };
    if !data
        .keys()
        .all(|key| matches!(key.as_str(), "sessionId" | "update"))
    {
        return false;
    }
    let Some(update) = data.get("update").and_then(Value::as_object) else {
        return false;
    };
    if !update
        .keys()
        .all(|key| matches!(key.as_str(), "sessionUpdate" | "content" | "_meta"))
    {
        return false;
    }
    update.get("content").is_none_or(|content| {
        content.as_object().is_some_and(|content| {
            content
                .keys()
                .all(|key| matches!(key.as_str(), "type" | "text"))
        })
    })
}

fn has_unmodeled_text_meta(meta: Option<&Value>) -> bool {
    let Some(meta) = meta else {
        return false;
    };
    let Some(meta) = json_object_spread(meta) else {
        return true;
    };
    for (key, value) in meta {
        match key.as_str() {
            "timestamp" | "serverTimestamp" => {}
            "parentToolCallId" | "subagentType" if value.is_string() => {}
            "qwenTranscript" => {
                let Some(transcript) = json_object_spread(&value) else {
                    return true;
                };
                for (field, field_value) in transcript {
                    match field.as_str() {
                        "sourceRecordIds"
                            if field_value
                                .as_array()
                                .is_some_and(|ids| ids.iter().all(Value::is_string)) => {}
                        "planToolCallId" if field_value.is_string() => {}
                        _ => return true,
                    }
                }
            }
            _ => return true,
        }
    }
    false
}

fn append_text_slot_chunk(
    slot: &mut TurnSlot,
    event: &BridgeEvent,
    update_meta: Option<&Value>,
    bytes: u64,
) {
    let TurnSlot::Text {
        chunks,
        chunk_bytes,
        chunk_metas,
        last_event_id,
        last_meta,
        last_envelope_meta,
        last_prompt_id,
        last_originator_client_id,
        last_session_id,
        source_bytes,
        source_events,
        ..
    } = slot
    else {
        return;
    };

    chunks.push(session_update_text(event).unwrap_or_default().to_owned());
    chunk_bytes.push(bytes);
    chunk_metas.push(update_meta.cloned());
    *last_meta = merge_transcript_update_meta(last_meta.as_ref(), update_meta);
    *last_event_id = event.id.or(*last_event_id);
    *last_envelope_meta = event.meta.clone().or_else(|| last_envelope_meta.clone());
    *last_prompt_id = event.prompt_id.clone().or_else(|| last_prompt_id.clone());
    *last_originator_client_id = event
        .originator_client_id
        .clone()
        .or_else(|| last_originator_client_id.clone());
    *last_session_id = capture_session_id(event).or_else(|| last_session_id.clone());
    *source_bytes = source_bytes.saturating_add(bytes);
    *source_events = source_events.saturating_add(1);
}

fn live_journal_text_chunk(event: &BridgeEvent) -> bool {
    if event.event_type != "session_update"
        || !matches!(
            session_update_type(event).as_deref(),
            Some("agent_message_chunk" | "agent_thought_chunk")
        )
        || !has_only_modeled_chunk_keys(event)
        || has_discrete_message_meta(update_update_meta(event))
        || has_unmodeled_text_meta(update_update_meta(event))
    {
        return false;
    }
    event
        .data
        .get("update")
        .and_then(|update| update.get("content"))
        .is_some_and(|content| {
            content.get("type").and_then(Value::as_str) == Some("text")
                && content.get("text").and_then(Value::as_str).is_some()
        })
}

fn compatible_live_text_events(left: &BridgeEvent, right: &BridgeEvent) -> bool {
    left.prompt_id == right.prompt_id
        && left.originator_client_id == right.originator_client_id
        && capture_session_id(left) == capture_session_id(right)
        && extract_parent_tool_call_id(update_update_meta(left))
            == extract_parent_tool_call_id(update_update_meta(right))
        && extract_source_record_ids(update_update_meta(left))
            == extract_source_record_ids(update_update_meta(right))
        && has_only_timestamp_envelope_meta(left.meta.as_ref())
        && has_only_timestamp_envelope_meta(right.meta.as_ref())
}

fn has_only_timestamp_envelope_meta(meta: Option<&Map<String, Value>>) -> bool {
    meta.is_none_or(|meta| {
        meta.keys()
            .all(|key| key == "timestamp" || key == "serverTimestamp")
    })
}

fn capture_session_id(event: &BridgeEvent) -> Option<String> {
    event
        .data
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn merge_live_text_events(
    existing: &BridgeEvent,
    incoming: &BridgeEvent,
    text: String,
) -> BridgeEvent {
    let mut merged = incoming.clone();
    merged.id = incoming.id.or(existing.id);
    merged.prompt_id = incoming
        .prompt_id
        .clone()
        .or_else(|| existing.prompt_id.clone());
    merged.originator_client_id = incoming
        .originator_client_id
        .clone()
        .or_else(|| existing.originator_client_id.clone());
    merged.meta = incoming.meta.clone().or_else(|| existing.meta.clone());

    let mut data = existing.data.as_object().cloned().unwrap_or_default();
    if let Some(incoming_data) = incoming.data.as_object() {
        data.extend(incoming_data.clone());
    }
    let mut update = existing
        .data
        .get("update")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if let Some(incoming_update) = incoming.data.get("update").and_then(Value::as_object) {
        update.extend(incoming_update.clone());
    }
    update.insert("content".to_owned(), json!({"type":"text", "text":text}));
    data.insert("update".to_owned(), Value::Object(update));
    merged.data = Value::Object(data);
    merged
}

fn make_merged_text_event(
    update_type: String,
    text: String,
    event_id: Option<u64>,
    meta: Option<Value>,
    envelope_meta: Option<Map<String, Value>>,
    prompt_id: Option<String>,
    originator_client_id: Option<String>,
    session_id: Option<String>,
) -> BridgeEvent {
    let mut update = Map::new();
    update.insert("sessionUpdate".to_owned(), Value::String(update_type));
    update.insert("content".to_owned(), json!({"type":"text", "text":text}));
    if let Some(meta) = meta.filter(|meta| !meta.is_null()) {
        update.insert("_meta".to_owned(), meta);
    }
    let mut data = Map::new();
    if let Some(session_id) = session_id {
        data.insert("sessionId".to_owned(), Value::String(session_id));
    }
    data.insert("update".to_owned(), Value::Object(update));
    BridgeEvent {
        id: event_id.filter(|id| *id != 0),
        v: EVENT_SCHEMA_VERSION,
        event_type: "session_update".to_owned(),
        data: Value::Object(data),
        prompt_id,
        meta: envelope_meta,
        originator_client_id,
    }
}

fn normalize_tool_call_type(event: &mut BridgeEvent) {
    if let Value::Object(data) = &mut event.data
        && let Some(Value::Object(update)) = data.get_mut("update")
        && update.get("sessionUpdate").and_then(Value::as_str) == Some("tool_call_update")
    {
        update.insert(
            "sessionUpdate".to_owned(),
            Value::String("tool_call".to_owned()),
        );
    }
}

fn merge_transcript_update_meta(
    existing: Option<&Value>,
    incoming: Option<&Value>,
) -> Option<Value> {
    let existing_record = existing.and_then(json_object_spread);
    let incoming_record = incoming.and_then(json_object_spread);
    if existing_record.is_none() && incoming_record.is_none() {
        return None;
    }
    let existing_transcript = existing_record
        .as_ref()
        .and_then(|record| record.get("qwenTranscript"))
        .and_then(json_object_spread);
    let incoming_transcript = incoming_record
        .as_ref()
        .and_then(|record| record.get("qwenTranscript"))
        .and_then(json_object_spread);
    let mut source_record_ids = Vec::new();
    for id in existing_transcript
        .as_ref()
        .and_then(extract_source_ids_from_transcript)
        .into_iter()
        .flatten()
        .chain(
            incoming_transcript
                .as_ref()
                .and_then(extract_source_ids_from_transcript)
                .into_iter()
                .flatten(),
        )
    {
        if !source_record_ids.iter().any(|existing| existing == &id) {
            source_record_ids.push(id);
        }
    }

    let mut merged = existing_record.unwrap_or_default();
    if let Some(incoming_record) = incoming_record {
        merged.extend(incoming_record);
    }
    if existing_transcript.is_some()
        || incoming_transcript.is_some()
        || !source_record_ids.is_empty()
    {
        let mut transcript = existing_transcript.unwrap_or_default();
        if let Some(incoming_transcript) = incoming_transcript {
            transcript.extend(incoming_transcript);
        }
        if !source_record_ids.is_empty() {
            transcript.insert(
                "sourceRecordIds".to_owned(),
                Value::Array(source_record_ids.into_iter().map(Value::String).collect()),
            );
        }
        merged.insert("qwenTranscript".to_owned(), Value::Object(transcript));
    }
    Some(Value::Object(merged))
}

fn merge_tool_call_event(existing: &BridgeEvent, incoming: &BridgeEvent) -> BridgeEvent {
    let existing_data = existing.data.as_object();
    let incoming_data = incoming.data.as_object();
    let existing_update = existing_data
        .and_then(|data| data.get("update"))
        .and_then(Value::as_object);
    let incoming_update = incoming_data
        .and_then(|data| data.get("update"))
        .and_then(Value::as_object);

    let mut update = existing_update.cloned().unwrap_or_default();
    if let Some(incoming_update) = incoming_update {
        for (key, value) in incoming_update {
            if !value.is_null() {
                update.insert(key.clone(), value.clone());
            }
        }
    }
    if let Some(meta) = merge_transcript_update_meta(
        existing_update.and_then(|update| update.get("_meta")),
        incoming_update.and_then(|update| update.get("_meta")),
    ) {
        update.insert("_meta".to_owned(), meta);
    }
    update.insert(
        "sessionUpdate".to_owned(),
        Value::String("tool_call".to_owned()),
    );

    let mut data = existing_data.cloned().unwrap_or_default();
    if let Some(incoming_data) = incoming_data {
        data.extend(incoming_data.clone());
    }
    data.insert("update".to_owned(), Value::Object(update));

    let mut meta = existing.meta.clone().unwrap_or_default();
    if let Some(incoming_meta) = &incoming.meta {
        meta.extend(incoming_meta.clone());
    }
    BridgeEvent {
        id: incoming.id.or(existing.id),
        v: EVENT_SCHEMA_VERSION,
        event_type: "session_update".to_owned(),
        data: Value::Object(data),
        prompt_id: incoming
            .prompt_id
            .clone()
            .or_else(|| existing.prompt_id.clone()),
        meta: (existing.meta.is_some() || incoming.meta.is_some()).then_some(meta),
        originator_client_id: incoming
            .originator_client_id
            .clone()
            .or_else(|| existing.originator_client_id.clone()),
    }
}

fn summary_event(event: &BridgeEvent) -> bool {
    if event.event_type != "session_update" {
        return true;
    }
    let Some(update) = event.data.get("update") else {
        return true;
    };
    let Some(meta) = update.get("_meta") else {
        return true;
    };
    let parent = extract_parent_tool_call_id(Some(meta));
    if parent.is_none() || parent.as_deref() == update.get("toolCallId").and_then(Value::as_str) {
        return true;
    }
    if update.get("sessionUpdate").and_then(Value::as_str) != Some("agent_message_chunk") {
        return false;
    }
    meta.get("usage").is_some_and(|usage| {
        usage
            .get("inputTokens")
            .and_then(Value::as_number)
            .is_some()
            || usage
                .get("outputTokens")
                .and_then(Value::as_number)
                .is_some()
    })
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
