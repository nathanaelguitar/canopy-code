//! QQ Bot gateway protocol state from `packages/channels/qqbot/src/QQChannel.ts`.
//!
//! This owns the IDENTIFY/RESUME handshake, sequence tracking, heartbeat
//! decisions, READY/RESUMED distinction, invalid-session reset, and close-code
//! reconnect policy. The channel host owns socket lifetime, timers, logging,
//! persistence callbacks, and dispatching event payloads to the session router.

use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::channels::qqbot_types::{Intent, OpCode};

const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(45_000);
const MIN_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(5_000);
const HEARTBEAT_ACK_TIMEOUT_MULTIPLIER: u32 = 2;

const CHANNEL_EVENTS: &[&str] = &[
    "C2C_MESSAGE_CREATE",
    "GROUP_AT_MESSAGE_CREATE",
    "GROUP_MESSAGE_CREATE",
    "GROUP_ADD_ROBOT",
    "GROUP_DEL_ROBOT",
    "GROUP_MSG_REJECT",
    "GROUP_MSG_RECEIVE",
];

/// Actions the channel host must perform after receiving a gateway message.
#[derive(Clone, Debug, PartialEq)]
pub enum QQGatewayEffect {
    /// Send this JSON frame over the open WebSocket.
    Send(Value),
    /// Close the socket; the host decides when to reconnect.
    Close { code: u16, reason: &'static str },
    /// READY or RESUMED completed. Cold READY requires persisted channel and
    /// session state restoration before accepting new prompts.
    Ready {
        resumed: bool,
        restore_persisted_state: bool,
        session_id: String,
    },
    /// A recognized QQ event for the platform adapter to process.
    Dispatch { event: String, data: Value },
    /// INVALID_SESSION requires flushing debounce-buffered state before the
    /// next READY performs a cold restore.
    FlushPersistedState,
}

/// Result of the gateway's WebSocket close handler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QQGatewayCloseOutcome {
    pub should_reconnect: bool,
    pub can_resume: bool,
}

/// Stateful QQ Bot gateway handshake and heartbeat policy.
#[derive(Clone, Debug)]
pub struct QQGatewayProtocol {
    session_id: String,
    sequence: i64,
    heartbeat_interval: Duration,
    last_heartbeat_ack: Option<Instant>,
    try_resume: bool,
    cold_start: bool,
    ready: bool,
    server_requested_reconnect: bool,
}

impl Default for QQGatewayProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl QQGatewayProtocol {
    pub fn new() -> Self {
        Self {
            session_id: String::new(),
            sequence: 0,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            last_heartbeat_ack: None,
            try_resume: false,
            cold_start: true,
            ready: false,
            server_requested_reconnect: false,
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn sequence(&self) -> i64 {
        self.sequence
    }

    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    pub fn can_resume(&self) -> bool {
        self.try_resume && !self.session_id.is_empty()
    }

    /// Apply one decoded gateway envelope. `access_token` is used to build the
    /// next IDENTIFY or RESUME frame; callers should not log returned frames.
    pub fn handle_message(
        &mut self,
        message: &Value,
        access_token: &str,
        now: Instant,
    ) -> Vec<QQGatewayEffect> {
        match message.get("op").and_then(Value::as_i64) {
            Some(OpCode::HELLO) => {
                let heartbeat_millis = message
                    .pointer("/d/heartbeat_interval")
                    .and_then(Value::as_f64)
                    .filter(|millis| millis.is_finite() && *millis != 0.0)
                    .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL.as_millis() as f64)
                    .max(MIN_HEARTBEAT_INTERVAL.as_millis() as f64)
                    .min(u64::MAX as f64) as u64;
                self.heartbeat_interval = Duration::from_millis(heartbeat_millis);
                vec![QQGatewayEffect::Send(
                    self.identify_or_resume_frame(access_token),
                )]
            }
            Some(OpCode::DISPATCH) => self.handle_dispatch(message, now),
            Some(OpCode::HEARTBEAT_ACK) => {
                self.last_heartbeat_ack = Some(now);
                Vec::new()
            }
            Some(OpCode::RECONNECT) => {
                self.server_requested_reconnect = true;
                vec![QQGatewayEffect::Close {
                    code: 4000,
                    reason: "server requested reconnect",
                }]
            }
            Some(OpCode::INVALID_SESSION) => {
                self.try_resume = false;
                self.cold_start = true;
                self.ready = false;
                self.last_heartbeat_ack = None;
                vec![
                    QQGatewayEffect::FlushPersistedState,
                    QQGatewayEffect::Send(self.identify_frame(access_token)),
                ]
            }
            _ => Vec::new(),
        }
    }

    /// Build a heartbeat frame or request reconnect when acknowledgements
    /// have been absent for more than two negotiated intervals.
    pub fn heartbeat_tick(&self, now: Instant) -> Option<QQGatewayEffect> {
        if !self.ready {
            return None;
        }
        if self.last_heartbeat_ack.is_some_and(|last_ack| {
            now.saturating_duration_since(last_ack)
                > self
                    .heartbeat_interval
                    .saturating_mul(HEARTBEAT_ACK_TIMEOUT_MULTIPLIER)
        }) {
            return Some(QQGatewayEffect::Close {
                code: 4001,
                reason: "heartbeat acknowledgement timeout",
            });
        }
        Some(QQGatewayEffect::Send(json!({
            "op": OpCode::HEARTBEAT,
            "d": self.sequence,
        })))
    }

    /// Apply the source adapter's close-code rules. A requested reconnect
    /// bypasses the configured attempt cap; ordinary non-normal closes obey it.
    pub fn on_close(
        &mut self,
        code: u16,
        reconnect_attempts: u64,
        max_reconnect_attempts: f64,
    ) -> QQGatewayCloseOutcome {
        self.ready = false;
        self.last_heartbeat_ack = None;
        let server_requested = std::mem::take(&mut self.server_requested_reconnect);
        let unlimited = max_reconnect_attempts <= 0.0;
        let below_limit = unlimited || (reconnect_attempts as f64) < max_reconnect_attempts;
        let should_reconnect = server_requested || (code != 1000 && below_limit);

        // QQ close codes 1000 and 4000 preserve the resumable server session;
        // other codes mean the gateway session has been lost.
        if code != 1000 && code != 4000 {
            self.try_resume = false;
            self.cold_start = true;
        }

        QQGatewayCloseOutcome {
            should_reconnect,
            can_resume: self.can_resume(),
        }
    }

    fn handle_dispatch(&mut self, message: &Value, now: Instant) -> Vec<QQGatewayEffect> {
        if let Some(sequence) = message.get("s").and_then(Value::as_i64) {
            self.sequence = sequence;
        }
        match message.get("t").and_then(Value::as_str) {
            Some("READY") => {
                self.session_id = message
                    .pointer("/d/session_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                self.try_resume = true;
                let restore_persisted_state = self.cold_start;
                self.cold_start = false;
                self.ready = true;
                self.last_heartbeat_ack = Some(now);
                vec![QQGatewayEffect::Ready {
                    resumed: false,
                    restore_persisted_state,
                    session_id: self.session_id.clone(),
                }]
            }
            Some("RESUMED") => {
                self.ready = true;
                self.cold_start = false;
                self.last_heartbeat_ack = Some(now);
                vec![QQGatewayEffect::Ready {
                    resumed: true,
                    restore_persisted_state: false,
                    session_id: self.session_id.clone(),
                }]
            }
            Some(event) if CHANNEL_EVENTS.contains(&event) => {
                vec![QQGatewayEffect::Dispatch {
                    event: event.to_owned(),
                    data: message.get("d").cloned().unwrap_or(Value::Null),
                }]
            }
            _ => Vec::new(),
        }
    }

    fn identify_or_resume_frame(&self, access_token: &str) -> Value {
        if self.can_resume() {
            json!({
                "op": OpCode::RESUME,
                "d": {
                    "token": format!("QQBot {access_token}"),
                    "session_id": self.session_id,
                    "seq": self.sequence,
                },
            })
        } else {
            self.identify_frame(access_token)
        }
    }

    fn identify_frame(&self, access_token: &str) -> Value {
        let intents = Intent::C2C_MESSAGE | Intent::GROUP_AT_MESSAGE | Intent::GROUP_MESSAGE;
        json!({
            "op": OpCode::IDENTIFY,
            "d": {
                "token": format!("QQBot {access_token}"),
                "intents": intents,
                "shard": [0, 1],
                "properties": {},
            },
        })
    }
}
