//! Bounded parser for the daemon's REST/SSE event stream.
//!
//! This mirrors the TypeScript SDK's `parseSseStream`: it accepts LF and CRLF
//! frame boundaries, ignores comments and non-data fields, joins repeated
//! `data:` lines, skips malformed or unknown event envelopes, and emits the
//! daemon's version-1 event objects without discarding their raw JSON.

use std::fmt::Display;
use std::time::Duration;

use futures_util::{Stream, StreamExt, stream};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::time::timeout;

/// Default idle-read timeout used by the TypeScript REST/SSE transport.
pub const DEFAULT_SSE_IDLE_TIMEOUT: Duration = Duration::from_secs(45);

// The TS implementation caps 16 Mi UTF-16 code units. 48 MiB bounds the
// corresponding UTF-8 input (three-byte BMP characters are the largest ratio)
// while also preventing an unterminated upstream response from growing without
// limit. The limit is checked before copying each network chunk.
const MAX_UNREAD_SSE_BYTES: usize = 48 * 1024 * 1024;
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;
const MAX_SAFE_INTEGER_U64: u64 = 9_007_199_254_740_991;

/// A daemon event frame. `raw` retains all fields so clients can consume newer
/// event metadata without waiting for this Rust model to be updated.
#[derive(Clone, Debug)]
pub struct DaemonEvent {
    /// Monotonic session event id, absent on synthetic terminal frames.
    pub id: Option<u64>,
    /// The only event schema currently emitted by the daemon.
    pub version: u8,
    /// Wire discriminator from the JSON `type` property.
    pub event_type: String,
    /// Opaque JSON event body. The TypeScript event contract permits any JSON.
    pub data: Option<Value>,
    /// Prompt identifier when the event belongs to an admitted turn.
    pub prompt_id: Option<String>,
    /// Optional daemon metadata, including timestamps.
    pub metadata: Option<Map<String, Value>>,
    /// Client identity that originated the event, when present.
    pub originator_client_id: Option<String>,
    /// Complete event JSON, including future fields.
    pub raw: Value,
}

/// One JSON SSE payload together with its optional event-bus cursor. ACP HTTP
/// streams carry raw JSON-RPC objects, so the SSE `id:` line must be preserved
/// separately from fields inside the JSON payload.
#[derive(Clone, Debug)]
pub struct SseJsonFrame {
    /// Validated decimal SSE event ID. Invalid or absent `id:` fields are None.
    pub id: Option<u64>,
    /// Parsed JSON from the frame's joined `data:` lines.
    pub data: Value,
}

/// Failure while reading or framing an SSE stream.
#[derive(Debug, Error)]
pub enum SseError {
    #[error("SSE stream was idle for {0:?}")]
    IdleTimeout(Duration),
    #[error("SSE unread frame exceeded the {MAX_UNREAD_SSE_BYTES}-byte limit")]
    FrameTooLarge,
    #[error("SSE source failed: {0}")]
    Source(String),
}

struct ParseState<S> {
    source: Option<S>,
    buffer: Vec<u8>,
    cursor: usize,
    idle_timeout: Option<Duration>,
    finished: bool,
}

/// Parse a byte stream containing server-sent events into daemon events.
///
/// `source` should yield chunks of bytes, as `reqwest::Response::bytes_stream`
/// does. Malformed frames are skipped as in the TypeScript SDK. A source error,
/// an idle timeout, or oversized unread input is yielded once and then
/// terminates the stream. Events are parsed and yielded incrementally, so a
/// large network chunk containing many small events cannot become a second,
/// unbounded queue of decoded objects. Dropping the returned stream drops
/// `source`, which closes the underlying response body.
pub fn parse_sse_stream<S, B, E>(
    source: S,
    idle_timeout: Option<Duration>,
) -> impl Stream<Item = Result<DaemonEvent, SseError>>
where
    S: Stream<Item = Result<B, E>> + Unpin + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
    E: Display + Send + 'static,
{
    parse_sse_json_stream(source, idle_timeout).filter_map(|result| async move {
        match result {
            Err(error) => Some(Err(error)),
            Ok(value) => parse_daemon_event(value).map(Ok),
        }
    })
}

/// Parse each SSE `data:` payload as raw JSON without applying the daemon
/// event-envelope validation. The ACP HTTP transport uses the same framing
/// rules for JSON-RPC requests, responses, and notifications.
pub fn parse_sse_json_stream<S, B, E>(
    source: S,
    idle_timeout: Option<Duration>,
) -> impl Stream<Item = Result<Value, SseError>>
where
    S: Stream<Item = Result<B, E>> + Unpin + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
    E: Display + Send + 'static,
{
    parse_sse_frames(source, idle_timeout).map(|result| result.map(|frame| frame.data))
}

/// Parse SSE JSON payloads while preserving each frame's validated `id:`
/// cursor. A later malformed `id:` line clears an earlier ID in that frame,
/// matching the ACP HTTP TypeScript transport.
pub fn parse_sse_frames<S, B, E>(
    source: S,
    idle_timeout: Option<Duration>,
) -> impl Stream<Item = Result<SseJsonFrame, SseError>>
where
    S: Stream<Item = Result<B, E>> + Unpin + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
    E: Display + Send + 'static,
{
    stream::unfold(
        ParseState {
            source: Some(source),
            buffer: Vec::new(),
            cursor: 0,
            idle_timeout,
            finished: false,
        },
        |mut state| async move {
            loop {
                if let Some((start, separator_len)) = find_separator(&state.buffer, state.cursor) {
                    let event = parse_frame_json(&state.buffer[state.cursor..start]);
                    state.cursor = start + separator_len;
                    if let Some(frame) = event {
                        return Some((Ok(frame), state));
                    }
                    continue;
                }

                if state.finished {
                    if state.cursor < state.buffer.len() {
                        let event = parse_frame_json(&state.buffer[state.cursor..]);
                        state.cursor = state.buffer.len();
                        if let Some(frame) = event {
                            return Some((Ok(frame), state));
                        }
                    }
                    return None;
                }

                let Some(source) = state.source.as_mut() else {
                    state.finished = true;
                    continue;
                };
                let next = match state.idle_timeout {
                    Some(duration) => match timeout(duration, source.next()).await {
                        Ok(next) => next,
                        Err(_) => {
                            state.finished = true;
                            state.source.take();
                            state.buffer.clear();
                            state.cursor = 0;
                            return Some((Err(SseError::IdleTimeout(duration)), state));
                        }
                    },
                    None => source.next().await,
                };

                match next {
                    Some(Ok(chunk)) => {
                        let bytes = chunk.as_ref();
                        let unread_len = state.buffer.len().saturating_sub(state.cursor);
                        if unread_len.saturating_add(bytes.len()) > MAX_UNREAD_SSE_BYTES {
                            state.finished = true;
                            state.source.take();
                            state.buffer.clear();
                            state.cursor = 0;
                            return Some((Err(SseError::FrameTooLarge), state));
                        }
                        if state.cursor > 0 {
                            state.buffer.drain(..state.cursor);
                            state.cursor = 0;
                        }
                        state.buffer.extend_from_slice(bytes);
                    }
                    Some(Err(error)) => {
                        state.finished = true;
                        state.source.take();
                        state.buffer.clear();
                        state.cursor = 0;
                        return Some((Err(SseError::Source(error.to_string())), state));
                    }
                    None => {
                        state.source.take();
                        state.finished = true;
                    }
                }
            }
        },
    )
}

fn find_separator(buffer: &[u8], start: usize) -> Option<(usize, usize)> {
    let mut cursor = start;
    while cursor < buffer.len() {
        if buffer[cursor..].starts_with(b"\r\n\r\n") {
            return Some((cursor, 4));
        }
        if buffer[cursor..].starts_with(b"\n\n") {
            return Some((cursor, 2));
        }
        cursor += 1;
    }
    None
}

fn parse_frame_json(raw: &[u8]) -> Option<SseJsonFrame> {
    if raw.is_empty() {
        return None;
    }
    let mut data_lines = Vec::new();
    let mut id = None;
    for line in raw.split(|byte| *byte == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if let Some(value) = line.strip_prefix(b"id:") {
            let value = String::from_utf8_lossy(value);
            id = parse_sse_event_id(value.trim());
            continue;
        }
        let Some(value) = line.strip_prefix(b"data:") else {
            continue;
        };
        let value = value.strip_prefix(b" ").unwrap_or(value);
        data_lines.push(String::from_utf8_lossy(value).into_owned());
    }
    if data_lines.is_empty() {
        return None;
    }
    let data_text = data_lines.join("\n");
    Some(SseJsonFrame {
        id,
        data: serde_json::from_str(&data_text).ok()?,
    })
}

fn parse_sse_event_id(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let id = value.parse::<u64>().ok()?;
    (id <= MAX_SAFE_INTEGER_U64).then_some(id)
}

fn parse_daemon_event(raw: Value) -> Option<DaemonEvent> {
    let object = raw.as_object()?;

    let version = number_as_f64(object.get("v")?)?;
    if version != 1.0 {
        return None;
    }
    let event_type = object.get("type")?.as_str()?.to_owned();

    let id = match object.get("id") {
        None => None,
        Some(value) => {
            let number = number_as_f64(value)?;
            if !number.is_finite()
                || number.fract() != 0.0
                || !(1.0..=MAX_SAFE_INTEGER).contains(&number)
            {
                return None;
            }
            Some(number as u64)
        }
    };

    let prompt_id = object
        .get("promptId")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let metadata = object.get("_meta").and_then(Value::as_object).cloned();
    let originator_client_id = object
        .get("originatorClientId")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let data = object.get("data").cloned();

    Some(DaemonEvent {
        id,
        version: 1,
        event_type,
        data,
        prompt_id,
        metadata,
        originator_client_id,
        raw,
    })
}

fn number_as_f64(value: &Value) -> Option<f64> {
    value.as_f64()
}
