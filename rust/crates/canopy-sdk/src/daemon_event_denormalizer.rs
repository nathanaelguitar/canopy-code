//! Convert ACP JSON-RPC notifications into daemon events.
//!
//! ACP notifications do not carry the monotonic IDs used by REST/SSE. This
//! module assigns process-wide synthetic IDs and keeps the complete normalized
//! JSON envelope in [`DaemonEvent::raw`]. These IDs are local ordering tokens;
//! they must not be used for REST `Last-Event-ID` replay.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Map, Value};

use crate::daemon_sse::DaemonEvent;

static NEXT_SYNTHETIC_ID: AtomicU64 = AtomicU64::new(1);

/// Normalize one ACP notification using its JSON-RPC method and optional
/// `params` value.
///
/// Returns `None` when the method does not carry a daemon event, when a
/// `session/update` or `_qwen/notify` payload has no non-empty event type, or
/// if the process-wide synthetic counter is exhausted. Supported methods are:
///
/// - `session/update`, in both the current `{ update: { sessionUpdate, ... } }`
///   shape and the legacy `{ type, data }` shape;
/// - `_qwen/notify`, preferring a non-empty `type`, then `kind`;
/// - direct workspace event methods containing `_` or `/`.
///
/// For an ACP JSON object, pass its `method` string and `params` member here.
/// Missing `params` is treated as an empty object, matching the TypeScript
/// denormalizer. A direct workspace method retains the provided `params` JSON
/// value as event data. Synthetic IDs are globally increasing within this
/// process and are unrelated to REST/SSE replay IDs.
pub fn denormalize_acp_notification(method: &str, params: Option<&Value>) -> Option<DaemonEvent> {
    let empty_params = Value::Object(Map::new());
    let params = params.unwrap_or(&empty_params);
    let params_object = params.as_object();

    if method == "session/update" {
        let raw_update = params_object
            .and_then(|object| object.get("update"))
            .and_then(Value::as_object);

        // If `update` is an object, this is the current format. It does not
        // fall back to the legacy top-level `type` when sessionUpdate is bad.
        let event_type = if let Some(update) = raw_update {
            update.get("sessionUpdate").and_then(Value::as_str)
        } else {
            params_object
                .and_then(|object| object.get("type"))
                .and_then(Value::as_str)
        }?;
        if event_type.is_empty() {
            return None;
        }

        let data = if let Some(update) = raw_update {
            let mut data = update.clone();
            if let Some(session_id) = params_object
                .and_then(|object| object.get("sessionId"))
                .and_then(Value::as_str)
            {
                data.insert("sessionId".to_owned(), Value::String(session_id.to_owned()));
            }
            Value::Object(data)
        } else {
            params_object
                .and_then(|object| object.get("data"))
                .filter(|value| !value.is_null())
                .cloned()
                .unwrap_or_else(|| params.clone())
        };

        let metadata = raw_update
            .and_then(|update| update.get("_meta"))
            .and_then(Value::as_object)
            .or_else(|| {
                params_object
                    .and_then(|object| object.get("_meta"))
                    .and_then(Value::as_object)
            })
            .cloned();
        let originator_client_id = string_field(params_object, "originatorClientId");

        return build_event(event_type, data, metadata, originator_client_id);
    }

    if method == "_qwen/notify" {
        let event_type = params_object
            .and_then(|object| object.get("type"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .or_else(|| {
                params_object
                    .and_then(|object| object.get("kind"))
                    .and_then(Value::as_str)
            })?;
        if event_type.is_empty() {
            return None;
        }

        let data = params_object
            .and_then(|object| object.get("data"))
            .filter(|value| !value.is_null())
            .cloned()
            .unwrap_or_else(|| params.clone());
        let metadata = params_object
            .and_then(|object| object.get("_meta"))
            .and_then(Value::as_object)
            .cloned();
        let originator_client_id = string_field(params_object, "originatorClientId");

        return build_event(event_type, data, metadata, originator_client_id);
    }

    if method.contains('_') || method.contains('/') {
        let event_type = method.rsplit('/').next().unwrap_or(method);
        let metadata = params_object
            .and_then(|object| object.get("_meta"))
            .and_then(Value::as_object)
            .cloned();
        let originator_client_id = string_field(params_object, "originatorClientId");
        return build_event(event_type, params.clone(), metadata, originator_client_id);
    }

    None
}

/// Filter daemon events for one session.
///
/// Events whose object-valued `data.sessionId` is a non-empty string are
/// retained only when it equals `session_id`. Events without such a session ID
/// are workspace-scoped and pass through. The returned iterator preserves the
/// input order and owns each yielded [`DaemonEvent`].
pub fn filter_events_by_session<I>(events: I, session_id: &str) -> impl Iterator<Item = DaemonEvent>
where
    I: IntoIterator<Item = DaemonEvent>,
{
    let session_id = session_id.to_owned();
    events.into_iter().filter(move |event| {
        let Some(event_session_id) = event
            .data
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|object| object.get("sessionId"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        else {
            return true;
        };
        event_session_id == session_id
    })
}

fn build_event(
    event_type: &str,
    data: Value,
    metadata: Option<Map<String, Value>>,
    originator_client_id: Option<String>,
) -> Option<DaemonEvent> {
    let id = next_synthetic_id()?;
    let mut raw = Map::new();
    raw.insert("id".to_owned(), Value::from(id));
    raw.insert("v".to_owned(), Value::from(1));
    raw.insert("type".to_owned(), Value::String(event_type.to_owned()));
    raw.insert("data".to_owned(), data.clone());
    if let Some(metadata) = &metadata {
        raw.insert("_meta".to_owned(), Value::Object(metadata.clone()));
    }
    if let Some(originator_client_id) = &originator_client_id {
        raw.insert(
            "originatorClientId".to_owned(),
            Value::String(originator_client_id.clone()),
        );
    }

    Some(DaemonEvent {
        id: Some(id),
        version: 1,
        event_type: event_type.to_owned(),
        data: Some(data),
        prompt_id: None,
        metadata,
        originator_client_id,
        raw: Value::Object(raw),
    })
}

fn next_synthetic_id() -> Option<u64> {
    // fetch_update prevents wraparound from violating the monotonic-ID
    // contract. Exhaustion is practically unreachable, but drops the event
    // rather than emitting a reused ID.
    NEXT_SYNTHETIC_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .ok()
}

fn string_field(object: Option<&Map<String, Value>>, name: &str) -> Option<String> {
    object
        .and_then(|object| object.get(name))
        .and_then(Value::as_str)
        .map(str::to_owned)
}
