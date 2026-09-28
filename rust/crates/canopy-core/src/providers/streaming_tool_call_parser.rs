//! Reassembles OpenAI-compatible streamed function arguments.
//!
//! This follows Canopy's `StreamingToolCallParser` routing rules, including
//! providers that send arguments before IDs, reuse indices, or omit IDs on
//! continuation chunks. Argument storage is bounded across one stream so a
//! malformed or hostile response cannot grow this parser without limit.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value, json};

use crate::providers::openai_compatible::MAX_RESPONSE_BODY_BYTES;

pub const MAX_TOOL_CALLS_PER_STREAM: usize = 1024;
pub const MAX_TOOL_CALL_ID_BYTES: usize = 4096;
pub const MAX_TOOL_CALL_NAME_BYTES: usize = 4096;
pub const MAX_TOOL_ARGUMENT_BYTES: usize = MAX_RESPONSE_BODY_BYTES;

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ToolCallMeta {
    pub id: Option<String>,
    pub name: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ToolCallParseResult {
    /// Storage index after any collision remapping.
    pub actual_index: Option<u64>,
    pub complete: bool,
    pub value: Option<Value>,
    pub error: Option<String>,
    pub repaired: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompletedToolCall {
    pub id: Option<String>,
    pub name: String,
    pub args: Map<String, Value>,
    pub index: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ToolCallParseState {
    pub depth: i64,
    pub in_string: bool,
    pub escape: bool,
}

#[derive(Clone, Debug, Default)]
struct ToolCallState {
    buffer: String,
    parse: ToolCallParseState,
    meta: ToolCallMeta,
}

#[derive(Clone, Debug, Default)]
pub struct StreamingToolCallParser {
    states: HashMap<u64, ToolCallState>,
    /// `Map` iteration order is observable in the TypeScript implementation.
    index_order: Vec<u64>,
    nameless_indices: HashSet<u64>,
    id_to_index: HashMap<String, u64>,
    pending_index_remaps: HashMap<u64, u64>,
    next_available_index: u64,
    buffered_argument_bytes: usize,
    conflicting_tool_call_identity: bool,
    invalid_tool_call_index: bool,
    resource_limit_exceeded: bool,
}

impl StreamingToolCallParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one delta. `index` is a floating point value because provider JSON
    /// can contain non-integer or unsafe JavaScript-number indices.
    pub fn add_chunk(
        &mut self,
        index: f64,
        chunk: &str,
        id: Option<&str>,
        name: Option<&str>,
    ) -> ToolCallParseResult {
        let Some(index) = safe_index(index) else {
            self.conflicting_tool_call_identity = true;
            self.invalid_tool_call_index = true;
            return ToolCallParseResult {
                error: Some(format!("Invalid tool call index: {index}")),
                ..ToolCallParseResult::default()
            };
        };
        if self.resource_limit_exceeded {
            return self.limit_error(None, "tool call stream has exceeded its resource limit");
        }
        let valid_name = name.map(str::trim).filter(|name| !name.is_empty());

        if id.is_some_and(|id| id.len() > MAX_TOOL_CALL_ID_BYTES)
            || valid_name.is_some_and(|name| name.len() > MAX_TOOL_CALL_NAME_BYTES)
        {
            self.resource_limit_exceeded = true;
            return self.limit_error(Some(index), "tool call metadata exceeds its size limit");
        }
        if id.is_some_and(|id| {
            !self.id_to_index.contains_key(id)
                && self.id_to_index.len() >= MAX_TOOL_CALLS_PER_STREAM
        }) {
            self.resource_limit_exceeded = true;
            return self.limit_error(Some(index), "too many distinct tool call IDs");
        }

        if id.is_none() && valid_name.is_none() && chunk.trim().is_empty() {
            let state = self.states.get(&index);
            let depth = state.map_or(0, |state| state.parse.depth);
            let in_string = state.is_some_and(|state| state.parse.in_string);
            if state.is_none() || (depth == 0 && !in_string) {
                return ToolCallParseResult::default();
            }
        }

        let mut actual_index = index;
        let is_known_id = id.is_some_and(|id| self.id_to_index.contains_key(id));
        let existing_name = self
            .states
            .get(&index)
            .and_then(|state| state.meta.name.as_deref());
        let is_name_only_delta = valid_name.is_some_and(|valid_name| {
            chunk.is_empty() && (existing_name.is_none() || existing_name == Some(valid_name))
        });

        if let Some(id) = id {
            if let Some(mapped_index) = self.id_to_index.get(id) {
                actual_index = *mapped_index;
            } else {
                let pending_index = self.pending_index_remaps.get(&index).copied();
                let pending_is_unclaimed = pending_index.is_some_and(|pending| {
                    self.states
                        .get(&pending)
                        .is_none_or(|state| state.meta.id.is_none())
                });
                if pending_is_unclaimed {
                    actual_index = pending_index.unwrap_or(index);
                    self.id_to_index.insert(id.to_owned(), actual_index);
                } else {
                    let collision = self.states.get(&index).and_then(|state| {
                        state
                            .meta
                            .id
                            .as_deref()
                            .filter(|existing_id| *existing_id != id)
                            .map(|_| state.clone())
                    });
                    if let Some(existing) = collision {
                        let mut existing_complete = existing.parse.depth == 0;
                        if existing_complete && !existing.buffer.trim().is_empty() {
                            existing_complete =
                                serde_json::from_str::<Value>(&existing.buffer).is_ok();
                        }
                        if existing_complete {
                            actual_index = self.first_unoccupied_index();
                            if existing.meta.name.is_none() {
                                self.conflicting_tool_call_identity = true;
                            }
                        } else {
                            self.conflicting_tool_call_identity = true;
                        }
                    }
                    self.id_to_index.insert(id.to_owned(), actual_index);
                }
            }
        } else if !is_name_only_delta {
            if let Some(remapped_index) = self.pending_index_remaps.get(&index).copied() {
                actual_index = remapped_index;
                if let Some(state) = self.states.get(&actual_index) {
                    let complete = state.parse.depth == 0
                        && !state.buffer.trim().is_empty()
                        && serde_json::from_str::<Value>(&state.buffer).is_ok();
                    if complete {
                        actual_index = self.find_most_recent_incomplete_index();
                    }
                }
            } else if let Some(state) = self.states.get(&index) {
                if state.parse.depth > 0 || state.buffer.trim().is_empty() {
                    actual_index = index;
                } else if serde_json::from_str::<Value>(&state.buffer).is_ok() {
                    actual_index = self.find_most_recent_incomplete_index();
                } else {
                    actual_index = index;
                }
            }
        }

        if actual_index != index
            && !self.pending_index_remaps.contains_key(&index)
            && self.pending_index_remaps.len() >= MAX_TOOL_CALLS_PER_STREAM
        {
            self.resource_limit_exceeded = true;
            return self.limit_error(
                Some(actual_index),
                "too many pending tool call index remaps",
            );
        }

        if !self.states.contains_key(&actual_index) {
            if self.states.len() >= MAX_TOOL_CALLS_PER_STREAM {
                self.resource_limit_exceeded = true;
                return self.limit_error(Some(actual_index), "too many streamed tool calls");
            }
            self.states.insert(actual_index, ToolCallState::default());
            self.index_order.push(actual_index);
        }

        if chunk.is_empty() && (id.is_some() || valid_name.is_some()) {
            let Some(state) = self.states.get_mut(&actual_index) else {
                return ToolCallParseResult {
                    actual_index: Some(actual_index),
                    error: Some("tool call state is unavailable".to_owned()),
                    ..ToolCallParseResult::default()
                };
            };
            if let Some(id) = id {
                state.meta.id = Some(id.to_owned());
            }
            if state.meta.name.is_none() {
                state.meta.name = valid_name.map(str::to_owned);
            }
            if state.meta.name.is_none() && state.meta.id.is_some() {
                self.nameless_indices.insert(actual_index);
            } else {
                self.nameless_indices.remove(&actual_index);
            }
            if actual_index != index {
                self.pending_index_remaps.insert(index, actual_index);
            }
            return ToolCallParseResult {
                actual_index: Some(actual_index),
                ..ToolCallParseResult::default()
            };
        }

        if is_known_id
            && self.states.get(&actual_index).is_some_and(|state| {
                state.parse.depth == 0
                    && !state.buffer.trim().is_empty()
                    && serde_json::from_str::<Value>(&state.buffer).is_ok()
            })
        {
            return ToolCallParseResult {
                actual_index: Some(actual_index),
                ..ToolCallParseResult::default()
            };
        }

        let identity_changed = id.is_some_and(|id| {
            self.states
                .get(&actual_index)
                .and_then(|state| state.meta.id.as_deref())
                .is_some_and(|existing_id| existing_id != id)
        });
        if let Some(state) = self.states.get_mut(&actual_index) {
            if let Some(id) = id {
                state.meta.id = Some(id.to_owned());
            }
            if let Some(valid_name) = valid_name {
                if !identity_changed
                    && state
                        .meta
                        .name
                        .as_deref()
                        .is_some_and(|existing_name| existing_name != valid_name)
                {
                    self.conflicting_tool_call_identity = true;
                } else {
                    state.meta.name = Some(valid_name.to_owned());
                }
            }
        }
        if actual_index != index {
            self.pending_index_remaps.insert(index, actual_index);
        }

        let current_len = self
            .states
            .get(&actual_index)
            .map_or(0, |state| state.buffer.len());
        let new_len = match current_len.checked_add(chunk.len()) {
            Some(len) => len,
            None => {
                self.resource_limit_exceeded = true;
                return self.limit_error(Some(actual_index), "tool call argument size overflow");
            }
        };
        if new_len > MAX_TOOL_ARGUMENT_BYTES
            || self
                .buffered_argument_bytes
                .checked_add(chunk.len())
                .is_none_or(|total| total > MAX_TOOL_ARGUMENT_BYTES)
        {
            self.resource_limit_exceeded = true;
            return self.limit_error(
                Some(actual_index),
                "tool call arguments exceed the per-stream size limit",
            );
        }

        let (current_depth, current_in_string, current_escape, current_buffer) = {
            let Some(state) = self.states.get(&actual_index) else {
                return ToolCallParseResult {
                    actual_index: Some(actual_index),
                    error: Some("tool call state is unavailable".to_owned()),
                    ..ToolCallParseResult::default()
                };
            };
            (
                state.parse.depth,
                state.parse.in_string,
                state.parse.escape,
                state.buffer.clone(),
            )
        };
        let new_buffer = format!("{current_buffer}{chunk}");
        self.buffered_argument_bytes += chunk.len();
        let Some(state) = self.states.get_mut(&actual_index) else {
            return ToolCallParseResult {
                actual_index: Some(actual_index),
                error: Some("tool call state is unavailable".to_owned()),
                ..ToolCallParseResult::default()
            };
        };
        state.buffer = new_buffer.clone();
        if state.meta.name.is_none() && (state.meta.id.is_some() || !new_buffer.trim().is_empty()) {
            self.nameless_indices.insert(actual_index);
        } else {
            self.nameless_indices.remove(&actual_index);
        }

        let mut depth = current_depth;
        let mut in_string = current_in_string;
        let mut escape = current_escape;
        for character in chunk.chars() {
            if !in_string {
                match character {
                    '{' | '[' => depth += 1,
                    '}' | ']' => depth -= 1,
                    _ => {}
                }
            }
            if character == '"' && !escape {
                in_string = !in_string;
            }
            escape = character == '\\' && !escape;
        }
        let Some(state) = self.states.get_mut(&actual_index) else {
            return ToolCallParseResult {
                actual_index: Some(actual_index),
                error: Some("tool call state is unavailable".to_owned()),
                ..ToolCallParseResult::default()
            };
        };
        state.parse = ToolCallParseState {
            depth,
            in_string,
            escape,
        };

        if depth == 0 && !new_buffer.trim().is_empty() {
            match serde_json::from_str::<Value>(&new_buffer) {
                Ok(value) => ToolCallParseResult {
                    actual_index: Some(actual_index),
                    complete: true,
                    value: Some(value),
                    ..ToolCallParseResult::default()
                },
                Err(error) if in_string => {
                    if let Ok(value) = serde_json::from_str::<Value>(&format!("{new_buffer}\"")) {
                        ToolCallParseResult {
                            actual_index: Some(actual_index),
                            complete: true,
                            value: Some(value),
                            repaired: true,
                            ..ToolCallParseResult::default()
                        }
                    } else {
                        ToolCallParseResult {
                            actual_index: Some(actual_index),
                            error: Some(error.to_string()),
                            ..ToolCallParseResult::default()
                        }
                    }
                }
                Err(error) => ToolCallParseResult {
                    actual_index: Some(actual_index),
                    error: Some(error.to_string()),
                    ..ToolCallParseResult::default()
                },
            }
        } else {
            ToolCallParseResult {
                actual_index: Some(actual_index),
                ..ToolCallParseResult::default()
            }
        }
    }

    pub fn get_tool_call_meta(&self, index: u64) -> ToolCallMeta {
        self.states
            .get(&index)
            .map(|state| state.meta.clone())
            .unwrap_or_default()
    }

    pub fn has_nameless_tool_call(&self) -> bool {
        !self.nameless_indices.is_empty()
    }

    pub fn has_conflicting_tool_call_identity(&self) -> bool {
        self.conflicting_tool_call_identity
    }

    pub fn has_invalid_tool_call_index(&self) -> bool {
        self.invalid_tool_call_index
    }

    pub fn has_resource_limit_error(&self) -> bool {
        self.resource_limit_exceeded
    }

    pub fn has_invalid_tool_call_arguments(&self) -> bool {
        if self.resource_limit_exceeded {
            return true;
        }
        self.index_order.iter().any(|index| {
            let Some(state) = self.states.get(index) else {
                return false;
            };
            if state.meta.name.is_none() || state.buffer.is_empty() {
                return false;
            }
            !matches!(
                serde_json::from_str::<Value>(&state.buffer),
                Ok(Value::Object(_))
            )
        })
    }

    pub fn get_completed_tool_calls(&self) -> Vec<CompletedToolCall> {
        if self.resource_limit_exceeded {
            return Vec::new();
        }
        let mut completed = Vec::new();
        let mut emitted_ids = HashSet::new();
        for index in &self.index_order {
            let Some(state) = self.states.get(index) else {
                continue;
            };
            let Some(name) = state.meta.name.as_ref() else {
                continue;
            };
            if state
                .meta
                .id
                .as_ref()
                .is_some_and(|id| !emitted_ids.insert(id.clone()))
            {
                continue;
            }

            let value = if state.buffer.trim().is_empty() {
                json!({})
            } else if let Ok(value) = serde_json::from_str::<Value>(&state.buffer) {
                value
            } else {
                if state.parse.in_string {
                    if let Ok(value) = serde_json::from_str::<Value>(&format!("{}\"", state.buffer))
                    {
                        value
                    } else {
                        safe_json_parse(&state.buffer)
                    }
                } else {
                    safe_json_parse(&state.buffer)
                }
            };
            let args = match value {
                Value::Object(object) => object,
                _ => Map::new(),
            };
            completed.push(CompletedToolCall {
                id: state.meta.id.clone(),
                name: name.clone(),
                args,
                index: *index,
            });
        }
        completed
    }

    pub fn find_next_available_index(&mut self) -> u64 {
        while let Some(state) = self.states.get(&self.next_available_index) {
            if state.meta.name.is_none() || state.parse.depth > 0 || state.meta.id.is_none() {
                return self.next_available_index;
            }
            if !state.buffer.trim().is_empty()
                && serde_json::from_str::<Value>(&state.buffer).is_err()
            {
                return self.next_available_index;
            }
            self.next_available_index = self.next_available_index.saturating_add(1);
        }
        let index = self.next_available_index;
        self.next_available_index = self.next_available_index.saturating_add(1);
        index
    }

    pub fn find_most_recent_incomplete_index(&mut self) -> u64 {
        let mut newest = None;
        for index in &self.index_order {
            let Some(state) = self.states.get(index) else {
                continue;
            };
            let incomplete = if state.meta.id.is_some()
                && (state.parse.depth > 0
                    || (state.buffer.trim().is_empty() && state.meta.name.is_none()))
            {
                true
            } else if !state.buffer.trim().is_empty() {
                serde_json::from_str::<Value>(&state.buffer).is_err()
            } else {
                false
            };
            if incomplete && newest.is_none_or(|previous| *index > previous) {
                newest = Some(*index);
            }
        }
        newest.unwrap_or_else(|| self.find_next_available_index())
    }

    pub fn reset_index(&mut self, index: u64) {
        if !self.states.contains_key(&index) && self.states.len() >= MAX_TOOL_CALLS_PER_STREAM {
            self.resource_limit_exceeded = true;
            return;
        }
        self.ensure_state(index);
        if let Some(state) = self.states.get_mut(&index) {
            self.buffered_argument_bytes = self
                .buffered_argument_bytes
                .saturating_sub(state.buffer.len());
            *state = ToolCallState::default();
        }
        self.nameless_indices.remove(&index);
        self.pending_index_remaps
            .retain(|provider_index, actual_index| {
                *provider_index != index && *actual_index != index
            });
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }

    pub fn get_buffer(&self, index: u64) -> &str {
        self.states
            .get(&index)
            .map_or("", |state| state.buffer.as_str())
    }

    pub fn get_state(&self, index: u64) -> ToolCallParseState {
        self.states
            .get(&index)
            .map(|state| state.parse.clone())
            .unwrap_or_default()
    }

    pub fn has_incomplete_tool_calls(&self) -> bool {
        self.index_order.iter().any(|index| {
            self.states.get(index).is_some_and(|state| {
                state.meta.name.is_some() && (state.parse.depth > 0 || state.parse.in_string)
            })
        })
    }

    fn ensure_state(&mut self, index: u64) {
        if let Entry::Vacant(entry) = self.states.entry(index) {
            entry.insert(ToolCallState::default());
            self.index_order.push(index);
        }
    }

    fn first_unoccupied_index(&self) -> u64 {
        let mut index = 0;
        while self.states.contains_key(&index) {
            index = index.saturating_add(1);
        }
        index
    }

    fn limit_error(&self, index: Option<u64>, message: &str) -> ToolCallParseResult {
        ToolCallParseResult {
            actual_index: index,
            error: Some(message.to_owned()),
            ..ToolCallParseResult::default()
        }
    }
}

fn safe_index(index: f64) -> Option<u64> {
    if !index.is_finite() || index < 0.0 || index.fract() != 0.0 || index > MAX_SAFE_INTEGER {
        None
    } else {
        Some(index as u64)
    }
}

fn safe_json_parse(input: &str) -> Value {
    if let Ok(value) = serde_json::from_str(input) {
        return value;
    }
    jsonrepair_rs::jsonrepair(input)
        .ok()
        .filter(|repaired| repaired.len() <= MAX_TOOL_ARGUMENT_BYTES)
        .and_then(|repaired| serde_json::from_str(&repaired).ok())
        .unwrap_or_else(|| json!({}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_fragments_and_tracks_nested_json_strings() {
        let mut parser = StreamingToolCallParser::new();
        assert!(
            !parser
                .add_chunk(0.0, r#"{"outer":{"text":"a {"#, Some("c1"), Some("run"))
                .complete
        );
        assert_eq!(parser.get_state(0).depth, 2);
        let parsed = parser.add_chunk(0.0, r#"b } c"}}"#, None, None);
        assert!(parsed.complete);
        assert_eq!(parsed.value, Some(json!({"outer":{"text":"a {b } c"}})));
    }

    #[test]
    fn ignores_phantoms_and_tracks_real_nameless_calls() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, "", None, None);
        assert!(!parser.has_nameless_tool_call());
        parser.add_chunk(0.0, "", Some("c1"), None);
        assert!(parser.has_nameless_tool_call());
        parser.reset_index(0);
        parser.add_chunk(0.0, r#"{"x":1}"#, None, None);
        assert!(parser.has_nameless_tool_call());
    }

    #[test]
    fn preserves_empty_no_argument_calls_when_an_index_is_reused() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, "", Some("c1"), Some("first"));
        parser.add_chunk(0.0, r#"{"x":1}"#, Some("c2"), Some("second"));
        let calls = parser.get_completed_tool_calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].index, 0);
        assert_eq!(calls[0].args, Map::new());
        assert_eq!(calls[1].index, 1);
        assert_eq!(calls[1].args, json!({"x":1}).as_object().unwrap().clone());
    }

    #[test]
    fn routes_idless_continuations_to_the_latest_incomplete_call() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, r#"{"a":"#, Some("c1"), Some("one"));
        parser.add_chunk(1.0, r#"{"b":"#, Some("c2"), Some("two"));
        let one = parser.add_chunk(0.0, "1}", None, None);
        let two = parser.add_chunk(1.0, "2}", None, None);
        assert_eq!(one.value, Some(json!({"a":1})));
        assert_eq!(two.value, Some(json!({"b":2})));
    }

    #[test]
    fn routes_late_ids_to_openers_that_were_remapped_before_the_id() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, r#"{"a":1}"#, Some("first"), Some("one"));
        parser.add_chunk(0.0, r#"{"b":"#, None, Some("two"));
        let opened = parser.add_chunk(0.0, "2}", Some("second"), None);
        assert_eq!(opened.actual_index, Some(1));
        assert_eq!(parser.get_tool_call_meta(1).id.as_deref(), Some("second"));
        assert_eq!(
            parser.get_completed_tool_calls()[1].args,
            json!({"b":2}).as_object().unwrap().clone()
        );
    }

    #[test]
    fn ignores_completed_id_replays_and_deduplicates_emission() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, r#"{"x":1}"#, Some("c1"), Some("run"));
        let replay = parser.add_chunk(0.0, r#"{"x":2}"#, Some("c1"), Some("run"));
        assert!(!replay.complete);
        assert_eq!(parser.get_buffer(0), r#"{"x":1}"#);
        assert_eq!(parser.get_completed_tool_calls().len(), 1);
    }

    #[test]
    fn appends_repeated_id_argument_deltas_after_an_empty_opener() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, "", Some("c1"), Some("run"));
        parser.add_chunk(0.0, r#"{"text":"hello"#, Some("c1"), None);
        parser.add_chunk(0.0, " ", Some("c1"), None);
        let result = parser.add_chunk(0.0, "world\"}", Some("c1"), None);
        assert!(result.complete);
        assert_eq!(
            parser.get_completed_tool_calls()[0].args,
            json!({"text":"hello world"}).as_object().unwrap().clone()
        );
    }

    #[test]
    fn ignores_metadata_replays_and_preserves_the_first_name() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, r#"{"path":"a.ts"}"#, Some("c1"), Some("read_file"));
        parser.add_chunk(0.0, "", Some("c1"), Some("shell"));
        assert_eq!(
            parser.get_tool_call_meta(0).name.as_deref(),
            Some("read_file")
        );
        assert_eq!(
            parser.get_completed_tool_calls()[0].args,
            json!({"path":"a.ts"}).as_object().unwrap().clone()
        );
    }

    #[test]
    fn flags_conflicting_names_but_keeps_the_first_for_a_stable_id() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, r#"{"path":"#, Some("c1"), Some("read_file"));
        parser.add_chunk(0.0, "\"a.ts\"}", Some("c1"), Some("shell"));
        assert!(parser.has_conflicting_tool_call_identity());
        assert_eq!(parser.get_completed_tool_calls()[0].name, "read_file");
    }

    #[test]
    fn relocates_colliding_calls_and_keeps_idless_continuations_with_the_new_call() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, r#"{"a":1}"#, Some("c1"), Some("one"));
        let opener = parser.add_chunk(0.0, r#"{"b":"#, Some("c2"), Some("two"));
        assert_eq!(opener.actual_index, Some(1));
        let continuation = parser.add_chunk(0.0, "2}", None, None);
        assert_eq!(continuation.actual_index, Some(1));
        let calls = parser.get_completed_tool_calls();
        assert_eq!(calls[0].args, json!({"a":1}).as_object().unwrap().clone());
        assert_eq!(calls[1].args, json!({"b":2}).as_object().unwrap().clone());
        assert!(!parser.has_conflicting_tool_call_identity());
    }

    #[test]
    fn late_id_claims_a_remapped_argument_slot_without_losing_continuations() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, r#"{"first":true}"#, Some("c1"), Some("one"));
        let opener = parser.add_chunk(0.0, r#"{"second":"#, None, Some("two"));
        assert_eq!(opener.actual_index, Some(1));
        let identified = parser.add_chunk(0.0, "", Some("c2"), None);
        assert_eq!(identified.actual_index, Some(1));
        let continuation = parser.add_chunk(0.0, "true}", None, None);
        assert_eq!(continuation.actual_index, Some(1));
        assert_eq!(
            parser.get_completed_tool_calls()[1].id.as_deref(),
            Some("c2")
        );
        assert_eq!(
            parser.get_completed_tool_calls()[1].args,
            json!({"second":true}).as_object().unwrap().clone()
        );
    }

    #[test]
    fn a_new_id_does_not_hijack_a_remap_already_claimed_by_another_id() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, r#"{"a":1}"#, Some("c1"), Some("one"));
        parser.add_chunk(0.0, "", Some("c2"), Some("two"));
        parser.add_chunk(0.0, r#"{"b":2}"#, None, None);
        let third = parser.add_chunk(0.0, r#"{"c":3}"#, Some("c3"), Some("three"));
        assert_ne!(third.actual_index, Some(1));
        let calls = parser.get_completed_tool_calls();
        assert_eq!(
            calls
                .iter()
                .find(|call| call.id.as_deref() == Some("c2"))
                .unwrap()
                .args,
            json!({"b":2}).as_object().unwrap().clone()
        );
        assert_eq!(
            calls
                .iter()
                .find(|call| call.id.as_deref() == Some("c3"))
                .unwrap()
                .args,
            json!({"c":3}).as_object().unwrap().clone()
        );
    }

    #[test]
    fn overwrites_pending_remap_for_each_new_colliding_opener() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, r#"{"a":1}"#, Some("c1"), Some("one"));
        parser.add_chunk(0.0, "", Some("c2"), Some("two"));
        parser.add_chunk(0.0, r#"{"b":2}"#, None, None);
        let third = parser.add_chunk(0.0, "", Some("c3"), Some("three"));
        assert_eq!(third.actual_index, Some(2));
        let continuation = parser.add_chunk(0.0, r#"{"c":3}"#, None, None);
        assert_eq!(continuation.actual_index, Some(2));
        let calls = parser.get_completed_tool_calls();
        assert_eq!(
            calls
                .iter()
                .find(|call| call.id.as_deref() == Some("c2"))
                .unwrap()
                .args,
            json!({"b":2}).as_object().unwrap().clone()
        );
        assert_eq!(
            calls
                .iter()
                .find(|call| call.id.as_deref() == Some("c3"))
                .unwrap()
                .args,
            json!({"c":3}).as_object().unwrap().clone()
        );
    }

    #[test]
    fn repairs_truncated_strings_and_malformed_json_with_object_validation() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, r#"{"text":"open"#, Some("c1"), Some("run"));
        assert_eq!(
            parser.get_completed_tool_calls()[0].args,
            json!({"text":"open"}).as_object().unwrap().clone()
        );
        parser.reset();
        parser.add_chunk(
            0.0,
            r#"{"valid":"data","invalid":}"#,
            Some("c2"),
            Some("run"),
        );
        assert_eq!(
            parser.get_completed_tool_calls()[0].args,
            json!({"valid":"data","invalid":null})
                .as_object()
                .unwrap()
                .clone()
        );
        parser.reset();
        parser.add_chunk(0.0, "[1,2]", Some("c3"), Some("run"));
        assert!(parser.has_invalid_tool_call_arguments());
        assert_eq!(parser.get_completed_tool_calls()[0].args, Map::new());
    }

    #[test]
    fn trims_names_and_keeps_the_first_name_for_an_identity() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, "", Some("c1"), Some("  read_file  "));
        parser.add_chunk(0.0, " ", Some("c1"), Some("other"));
        assert_eq!(
            parser.get_tool_call_meta(0).name.as_deref(),
            Some("read_file")
        );
        assert!(parser.has_conflicting_tool_call_identity());
    }

    #[test]
    fn rejects_non_safe_provider_indices() {
        let mut parser = StreamingToolCallParser::new();
        for index in [-1.0, 0.5, f64::NAN, f64::INFINITY, 9_007_199_254_740_992.0] {
            assert!(parser.add_chunk(index, "{}", None, None).error.is_some());
        }
        assert!(parser.has_invalid_tool_call_index());
        assert!(parser.has_conflicting_tool_call_identity());
    }

    #[test]
    fn flags_incomplete_named_arguments_and_resets_per_stream() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(0.0, r#"{"path":"#, Some("c1"), Some("write_file"));
        assert!(parser.has_incomplete_tool_calls());
        parser.reset();
        assert!(!parser.has_incomplete_tool_calls());
        assert!(!parser.has_invalid_tool_call_index());
        assert!(!parser.has_conflicting_tool_call_identity());
    }

    #[test]
    fn rejects_argument_growth_past_the_stream_budget_without_retaining_it() {
        let mut parser = StreamingToolCallParser::new();
        let too_large = "x".repeat(MAX_TOOL_ARGUMENT_BYTES + 1);
        let result = parser.add_chunk(0.0, &too_large, Some("c1"), Some("run"));
        assert!(result.error.is_some());
        assert!(parser.has_resource_limit_error());
        assert!(parser.get_buffer(0).is_empty());
    }

    #[test]
    fn preserves_insertion_order_for_completed_calls() {
        let mut parser = StreamingToolCallParser::new();
        parser.add_chunk(7.0, "{}", Some("seven"), Some("seven"));
        parser.add_chunk(2.0, "{}", Some("two"), Some("two"));
        let calls = parser.get_completed_tool_calls();
        assert_eq!(
            calls.iter().map(|call| call.index).collect::<Vec<_>>(),
            [7, 2]
        );
    }
}
