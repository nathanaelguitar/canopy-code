//! Bounded NDJSON and JSON-RPC framing for ACP transports.

use std::collections::HashMap;

use serde_json::Value;
use thiserror::Error;

const MAX_JSON_RPC_METHOD_BYTES: usize = 1_024;
const MAX_JSON_RPC_ID_BYTES: usize = 256;
const MAX_JSON_RPC_ERROR_MESSAGE_BYTES: usize = 1_024;
const MAX_JSON_DEPTH: usize = 64;
const MAX_JSON_NODES: usize = 10_000;
const MAX_JSON_ARRAY_LENGTH: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NdJsonStreamLimits {
    pub max_frame_bytes: usize,
    pub max_queued_messages: usize,
    pub max_queued_bytes: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DecodedNdJsonFrame {
    pub value: Value,
    pub frame_bytes: usize,
    pub queue_charge_bytes: usize,
}

impl NdJsonStreamLimits {
    pub fn validate(self) -> Result<Self, NdJsonError> {
        if self.max_frame_bytes == 0 || self.max_queued_messages == 0 || self.max_queued_bytes == 0
        {
            return Err(NdJsonError::InvalidLimits);
        }
        Ok(self)
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum NdJsonError {
    #[error(
        "NDJSON {direction} frame exceeds {limit_bytes} bytes (observed {observed_bytes} bytes)"
    )]
    FrameTooLarge {
        direction: &'static str,
        limit_bytes: usize,
        observed_bytes: usize,
    },
    #[error(
        "NDJSON decoded queue is full (required {required_bytes} bytes, available {available_bytes} bytes)"
    )]
    QueueLimit {
        max_queued_messages: usize,
        max_queued_bytes: usize,
        required_bytes: usize,
        available_bytes: usize,
    },
    #[error("NDJSON input ended with an incomplete {observed_bytes}-byte frame")]
    IncompleteFrame { observed_bytes: usize },
    #[error("NDJSON input ended while the bounded transport was active")]
    UnexpectedEof,
    #[error("NDJSON input contains an invalid {observed_bytes}-byte message ({kind})")]
    InvalidMessage {
        kind: &'static str,
        observed_bytes: usize,
    },
    #[error("invalid NDJSON limits: all limits must be positive")]
    InvalidLimits,
}

/// Incremental bounded decoder. Input framing is performed on raw bytes so
/// split UTF-8 characters and chunks behave the same as the TS implementation.
pub struct NdJsonDecoder {
    limits: NdJsonStreamLimits,
    pending: Vec<u8>,
    queued_messages: usize,
    queued_bytes: usize,
    outstanding: HashMap<String, usize>,
    inbound_requests: HashMap<String, usize>,
    inbound_bytes: usize,
}

impl NdJsonDecoder {
    pub fn new(limits: NdJsonStreamLimits) -> Result<Self, NdJsonError> {
        Ok(Self {
            limits: limits.validate()?,
            pending: Vec::new(),
            queued_messages: 0,
            queued_bytes: 0,
            outstanding: HashMap::new(),
            inbound_requests: HashMap::new(),
            inbound_bytes: 0,
        })
    }

    /// Notify the decoder when the consumer removes frames from its queue.
    pub fn release_queued(&mut self, messages: usize, bytes: usize) {
        self.queued_messages = self.queued_messages.saturating_sub(messages);
        self.queued_bytes = self.queued_bytes.saturating_sub(bytes);
    }

    /// Register an outbound JSON-RPC request before writing it to the child.
    pub fn admit_outbound_request(
        &mut self,
        id: &Value,
        frame_bytes: usize,
    ) -> Result<(), NdJsonError> {
        if !is_json_rpc_id(id) {
            return Err(invalid("ndjson_invalid_message", frame_bytes));
        }
        let key = id_key(id);
        let retained_bytes = self.outstanding.values().sum::<usize>();
        let available = self.limits.max_queued_bytes.saturating_sub(retained_bytes);
        if self.outstanding.contains_key(&key)
            || self.outstanding.len() >= self.limits.max_queued_messages
            || frame_bytes > available
        {
            return Err(queue_error(self.limits, frame_bytes, available));
        }
        self.outstanding.insert(key, frame_bytes);
        Ok(())
    }

    pub fn discard_outbound_request(&mut self, id: &Value) {
        self.outstanding.remove(&id_key(id));
    }

    /// Feed bytes and return all complete, validated messages in this chunk.
    pub fn feed(&mut self, input: &[u8]) -> Result<Vec<Value>, NdJsonError> {
        Ok(self
            .feed_detailed(input)?
            .into_iter()
            .map(|frame| frame.value)
            .collect())
    }

    pub fn feed_detailed(&mut self, input: &[u8]) -> Result<Vec<DecodedNdJsonFrame>, NdJsonError> {
        let mut output = Vec::new();
        let mut start = 0;
        while let Some(relative) = input[start..].iter().position(|byte| *byte == b'\n') {
            let end = start + relative;
            let frame_len = self.pending.len() + (end - start) + 1;
            if frame_len > self.limits.max_frame_bytes {
                return Err(NdJsonError::FrameTooLarge {
                    direction: "received",
                    limit_bytes: self.limits.max_frame_bytes,
                    observed_bytes: frame_len,
                });
            }
            if self.pending.is_empty() && input[start..end].iter().all(u8::is_ascii_whitespace) {
                start = end + 1;
                continue;
            }
            let mut frame = std::mem::take(&mut self.pending);
            frame.extend_from_slice(&input[start..end]);
            let frame_bytes = frame.len() + 1;
            let min_charge = self
                .limits
                .max_queued_bytes
                .div_ceil(self.limits.max_queued_messages);
            let charge = frame_bytes.max(min_charge);
            let available = self
                .limits
                .max_queued_bytes
                .saturating_sub(self.queued_bytes);
            if self.queued_messages >= self.limits.max_queued_messages || charge > available {
                return Err(queue_error(self.limits, charge, available));
            }
            let parsed = parse_bounded_frame(&frame)?;
            self.validate_inbound(&parsed, frame_bytes)?;
            self.queued_messages += 1;
            self.queued_bytes += charge;
            output.push(DecodedNdJsonFrame {
                value: parsed,
                frame_bytes,
                queue_charge_bytes: charge,
            });
            start = end + 1;
        }
        if start < input.len() {
            self.pending.extend_from_slice(&input[start..]);
            if self.pending.len() > self.limits.max_frame_bytes {
                return Err(NdJsonError::FrameTooLarge {
                    direction: "received",
                    limit_bytes: self.limits.max_frame_bytes,
                    observed_bytes: self.pending.len(),
                });
            }
        }
        Ok(output)
    }

    pub fn finish(&mut self, fatal_clean_eof: bool) -> Result<(), NdJsonError> {
        if !self.pending.is_empty() {
            let bytes = self.pending.len();
            self.pending.clear();
            return Err(NdJsonError::IncompleteFrame {
                observed_bytes: bytes,
            });
        }
        if fatal_clean_eof {
            return Err(NdJsonError::UnexpectedEof);
        }
        self.outstanding.clear();
        self.inbound_requests.clear();
        self.inbound_bytes = 0;
        Ok(())
    }

    fn validate_inbound(&mut self, value: &Value, frame_bytes: usize) -> Result<(), NdJsonError> {
        if is_response(value) {
            let key = id_key(&value["id"]);
            if self.outstanding.remove(&key).is_none() {
                return Err(invalid("ndjson_invalid_message", frame_bytes));
            }
        }
        if is_request(value) {
            let key = id_key(&value["id"]);
            let available = self
                .limits
                .max_queued_bytes
                .saturating_sub(self.inbound_bytes);
            if self.inbound_requests.contains_key(&key)
                || self.inbound_requests.len() >= self.limits.max_queued_messages
                || frame_bytes > available
            {
                return Err(queue_error(self.limits, frame_bytes, available));
            }
            self.inbound_requests.insert(key, frame_bytes);
            self.inbound_bytes += frame_bytes;
        }
        Ok(())
    }

    /// Release the inbound request ledger after sending its response.
    pub fn release_inbound_response(&mut self, id: &Value) {
        if let Some(bytes) = self.inbound_requests.remove(&id_key(id)) {
            self.inbound_bytes = self.inbound_bytes.saturating_sub(bytes);
        }
    }
}

pub fn encode_frame(message: &Value, max_frame_bytes: usize) -> Result<Vec<u8>, NdJsonError> {
    let mut bytes =
        serde_json::to_vec(message).map_err(|_| invalid("ndjson_invalid_message", 0))?;
    let observed = bytes.len() + 1;
    if observed > max_frame_bytes {
        return Err(NdJsonError::FrameTooLarge {
            direction: "sent",
            limit_bytes: max_frame_bytes,
            observed_bytes: observed,
        });
    }
    bytes.push(b'\n');
    Ok(bytes)
}

fn parse_bounded_frame(frame: &[u8]) -> Result<Value, NdJsonError> {
    let bytes = frame.strip_suffix(b"\r").unwrap_or(frame);
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Err(invalid("ndjson_invalid_message", frame.len()));
    }
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| invalid("ndjson_parse_error", frame.len()))?;
    if !is_json_rpc_message(&value) || !has_bounded_structure(&value) {
        return Err(invalid("ndjson_invalid_message", frame.len()));
    }
    Ok(value)
}

fn is_json_rpc_message(value: &Value) -> bool {
    let Some(obj) = value.as_object() else {
        return false;
    };
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return false;
    }
    if let Some(method) = obj.get("method") {
        return method
            .as_str()
            .is_some_and(|m| m.len() <= MAX_JSON_RPC_METHOD_BYTES)
            && (!obj.contains_key("id") || is_json_rpc_id(&obj["id"]));
    }
    if !obj.contains_key("id") || !is_json_rpc_id(&obj["id"]) {
        return false;
    }
    let result = obj.contains_key("result");
    let error = obj.contains_key("error");
    if result == error {
        return false;
    }
    if !error {
        return true;
    }
    obj.get("error")
        .and_then(Value::as_object)
        .is_some_and(|e| {
            e.get("code")
                .and_then(Value::as_f64)
                .is_some_and(f64::is_finite)
                && e.get("message")
                    .and_then(Value::as_str)
                    .is_some_and(|m| m.len() <= MAX_JSON_RPC_ERROR_MESSAGE_BYTES)
        })
}

fn is_json_rpc_id(id: &Value) -> bool {
    id.is_null()
        || id
            .as_str()
            .is_some_and(|s| s.len() <= MAX_JSON_RPC_ID_BYTES)
        || id.as_f64().is_some_and(f64::is_finite)
}
fn id_key(id: &Value) -> String {
    serde_json::to_string(id).unwrap_or_default()
}
fn is_request(v: &Value) -> bool {
    v.get("method").is_some() && v.get("id").is_some()
}
fn is_response(v: &Value) -> bool {
    v.get("method").is_none() && v.get("id").is_some()
}
fn invalid(kind: &'static str, bytes: usize) -> NdJsonError {
    NdJsonError::InvalidMessage {
        kind,
        observed_bytes: bytes,
    }
}
fn queue_error(limits: NdJsonStreamLimits, required: usize, available: usize) -> NdJsonError {
    NdJsonError::QueueLimit {
        max_queued_messages: limits.max_queued_messages,
        max_queued_bytes: limits.max_queued_bytes,
        required_bytes: required,
        available_bytes: available,
    }
}
fn has_bounded_structure(value: &Value) -> bool {
    let mut stack = vec![(value, 1usize)];
    let mut nodes = 0usize;
    while let Some((value, depth)) = stack.pop() {
        nodes += 1;
        if nodes > MAX_JSON_NODES || depth > MAX_JSON_DEPTH {
            return false;
        }
        match value {
            Value::Array(values) => {
                if values.len() > MAX_JSON_ARRAY_LENGTH {
                    return false;
                }
                stack.extend(values.iter().map(|v| (v, depth + 1)));
            }
            Value::Object(values) => stack.extend(values.values().map(|v| (v, depth + 1))),
            _ => {}
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    fn decoder() -> NdJsonDecoder {
        NdJsonDecoder::new(NdJsonStreamLimits {
            max_frame_bytes: 128,
            max_queued_messages: 4,
            max_queued_bytes: 512,
        })
        .unwrap()
    }

    #[test]
    fn split_frames_and_whitespace_lines_decode_by_raw_bytes() {
        let mut d = decoder();
        assert!(
            d.feed(b" \r\n{\"jsonrpc\":\"2.0\",\"method\":\"x\",\"params\":{}")
                .unwrap()
                .is_empty()
        );
        let frames = d.feed(b"}\n").unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["method"], "x");
    }

    #[test]
    fn bounded_transport_rejects_unmatched_response_and_partial_eof() {
        let mut d = decoder();
        assert!(matches!(
            d.feed(b"{\"jsonrpc\":\"2.0\",\"id\":4,\"result\":null}\n"),
            Err(NdJsonError::InvalidMessage { .. })
        ));
        let partial = b"{\"jsonrpc\":\"2.0\",\"method\":\"x\"}";
        assert!(d.feed(partial).unwrap().is_empty());
        assert_eq!(
            d.finish(false),
            Err(NdJsonError::IncompleteFrame {
                observed_bytes: partial.len()
            })
        );
    }

    #[test]
    fn outbound_request_ledger_requires_matching_response() {
        let mut d = decoder();
        d.admit_outbound_request(&Value::from(1), 20).unwrap();
        let response = d
            .feed(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":null}\n")
            .unwrap();
        assert_eq!(response.len(), 1);
    }
}
