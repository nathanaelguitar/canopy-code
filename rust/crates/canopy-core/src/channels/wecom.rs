//! WeCom smart-bot message projection and payload helpers.
//!
//! This ports the deterministic message, attachment-reference, and outbound
//! markdown behavior from `packages/channels/wecom/src/WeComAdapter.ts`.
//! WebSocket lifecycle, attachment download/decryption, and CLI session
//! dispatch are separate host work.

use super::channel_prompt::{ChannelAttachmentType, ChannelPromptAttachment};
use super::session_router::SessionScope;
use aes::Aes256;
use aes::cipher::{BlockDecrypt, KeyInit, generic_array::GenericArray};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::{Host, Url};
use uuid::Uuid;

/// WeCom refuses markdown payloads larger than this UTF-8 byte count.
pub const WECOM_MARKDOWN_CHUNK_BYTES: usize = 3_800;
/// Source adapter cap for each outbound media file.
pub const WECOM_MAX_MEDIA_BYTES: usize = 20 * 1024 * 1024;
/// Bound the aggregate media retained or staged for one inbound callback.
pub const WECOM_MAX_MESSAGE_MEDIA_BYTES: usize = 32 * 1024 * 1024;
/// Avoid accumulating an unbounded number of attachment descriptors per callback.
pub const WECOM_MAX_MESSAGE_MEDIA_REFERENCES: usize = 16;
pub const WECOM_DEDUP_TTL: Duration = Duration::from_secs(5 * 60);
pub const WECOM_DEDUP_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const WECOM_MEDIA_SOCKET_TIMEOUT: Duration = Duration::from_secs(10);
const WECOM_MEDIA_ABSOLUTE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct WeComConfig {
    pub bot_id: String,
    pub secret: String,
    pub ws_url: Option<String>,
}

impl fmt::Debug for WeComConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WeComConfig")
            .field("bot_id", &self.bot_id)
            .field("secret", &"[redacted]")
            .field("ws_url", &self.ws_url)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WeComConfigError {
    MissingBotIdOrSecret,
    InvalidWebSocketUrl,
}

impl fmt::Display for WeComConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingBotIdOrSecret => formatter.write_str("WeCom requires botId and secret"),
            Self::InvalidWebSocketUrl => formatter.write_str("WeCom wsUrl must use wss://"),
        }
    }
}

impl std::error::Error for WeComConfigError {}

/// Parse the fields consumed by the TypeScript WeCom adapter.
pub fn parse_wecom_config(value: &Value) -> Result<WeComConfig, WeComConfigError> {
    let object = value.as_object();
    let bot_id = object
        .and_then(|map| map.get("botId"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    let secret = object
        .and_then(|map| map.get("secret"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    if bot_id.is_empty() || secret.is_empty() {
        return Err(WeComConfigError::MissingBotIdOrSecret);
    }
    let ws_url = object
        .and_then(|map| map.get("wsUrl"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    if ws_url.as_deref().is_some_and(|value| {
        Url::parse(value)
            .map(|url| url.scheme() != "wss")
            .unwrap_or(true)
    }) {
        return Err(WeComConfigError::InvalidWebSocketUrl);
    }
    Ok(WeComConfig {
        bot_id,
        secret,
        ws_url,
    })
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum WeComMediaType {
    Image,
    File,
    Voice,
    Video,
}

impl WeComMediaType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::File => "file",
            Self::Voice => "voice",
            Self::Video => "video",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "image" => Some(Self::Image),
            "file" => Some(Self::File),
            "voice" => Some(Self::Voice),
            "video" => Some(Self::Video),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeComInboundMediaRef {
    pub media_type: WeComMediaType,
    pub url: String,
    pub aes_key: Option<String>,
    pub file_name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeComInboundProjection {
    pub message_id: String,
    /// Present only when WeCom supplied an ID; synthetic IDs are not suitable
    /// for the source adapter's duplicate suppression map.
    pub raw_message_id: Option<String>,
    pub sender_id: String,
    pub sender_name: String,
    pub chat_id: String,
    pub is_group: bool,
    /// WeCom only delivers group callbacks after a bot mention.
    pub is_mentioned: bool,
    pub is_reply_to_bot: bool,
    pub text: String,
    pub referenced_text: Option<String>,
    pub media: Vec<WeComInboundMediaRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WeComProjectionError {
    UnrecognizedPayload,
    MissingSenderId,
    MissingChatId,
}

impl fmt::Display for WeComProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnrecognizedPayload => formatter.write_str("unrecognized WeCom payload"),
            Self::MissingSenderId => formatter.write_str("WeCom message is missing senderId"),
            Self::MissingChatId => formatter.write_str("WeCom message is missing chatId"),
        }
    }
}

impl std::error::Error for WeComProjectionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WeComMessageAdmission {
    Accepted,
    Untracked,
    DuplicateInFlight,
    DuplicateSeen,
}

#[derive(Default)]
struct WeComDedupState {
    in_flight: HashSet<String>,
    seen: HashMap<String, Instant>,
}

/// Thread-safe five-minute message deduplication with the same admission and
/// commit point as the TypeScript adapter. A message is marked seen only after
/// preflight and attachment download succeed.
#[derive(Clone, Default)]
pub struct WeComMessageDeduper {
    state: Arc<Mutex<WeComDedupState>>,
}

impl WeComMessageDeduper {
    pub fn begin(&self, message_id: Option<&str>) -> WeComMessageAdmission {
        let Some(message_id) = message_id.filter(|value| !value.is_empty()) else {
            return WeComMessageAdmission::Untracked;
        };
        let mut state = lock(&self.state);
        if state.in_flight.contains(message_id) {
            return WeComMessageAdmission::DuplicateInFlight;
        }
        if state.seen.contains_key(message_id) {
            return WeComMessageAdmission::DuplicateSeen;
        }
        state.in_flight.insert(message_id.to_owned());
        WeComMessageAdmission::Accepted
    }

    pub fn mark_ready_to_process(&self, message_id: &str) {
        if !message_id.is_empty() {
            lock(&self.state)
                .seen
                .insert(message_id.to_owned(), Instant::now());
        }
    }

    /// End an in-flight callback. If preflight, download, or another step
    /// failed before processInbound started, remove the provisional seen entry
    /// so the platform can retry the callback.
    pub fn finish(&self, message_id: Option<&str>, process_started: bool) {
        let Some(message_id) = message_id.filter(|value| !value.is_empty()) else {
            return;
        };
        let mut state = lock(&self.state);
        state.in_flight.remove(message_id);
        if !process_started {
            state.seen.remove(message_id);
        }
    }

    /// The TypeScript adapter runs this from a 60-second interval.
    pub fn cleanup_expired(&self) -> usize {
        let now = Instant::now();
        let mut state = lock(&self.state);
        let before = state.seen.len();
        state
            .seen
            .retain(|_, timestamp| now.duration_since(*timestamp) <= WECOM_DEDUP_TTL);
        before - state.seen.len()
    }

    pub fn disconnect(&self) {
        let mut state = lock(&self.state);
        state.in_flight.clear();
        state.seen.clear();
    }
}

/// Build the route key used to associate an inbound message's temporary files
/// with the session that receives its prompt.
pub fn attachment_route_key(
    channel_name: &str,
    scope: SessionScope,
    sender_id: &str,
    chat_id: &str,
    thread_id: Option<&str>,
) -> String {
    match scope {
        SessionScope::Thread => format!("{channel_name}:{}", thread_id.unwrap_or(chat_id)),
        SessionScope::ChatThread => match thread_id {
            Some(thread_id) => format!("{channel_name}:{chat_id}:{thread_id}"),
            None => format!("{channel_name}:{chat_id}"),
        },
        SessionScope::Single => format!("{channel_name}:__single__"),
        SessionScope::User => format!("{channel_name}:{sender_id}:{chat_id}"),
    }
}

#[derive(Default)]
struct WeComAttachmentLeaseState {
    dirs_by_message: HashMap<String, Vec<std::path::PathBuf>>,
    message_by_dir: HashMap<std::path::PathBuf, String>,
    dirs_by_session: HashMap<String, Vec<std::path::PathBuf>>,
    untracked_dirs_by_route: HashMap<String, Vec<std::path::PathBuf>>,
    buffered_messages: HashSet<String>,
    coalesced_messages: HashMap<String, Vec<String>>,
}

/// Owns temporary non-image attachments until ChannelBase reports that the
/// associated prompt or buffered prompt has ended, been dropped, or drained.
#[derive(Clone, Default)]
pub struct WeComAttachmentLeases {
    state: Arc<Mutex<WeComAttachmentLeaseState>>,
}

impl WeComAttachmentLeases {
    pub fn remember_dir(
        &self,
        dir: impl Into<std::path::PathBuf>,
        message_id: Option<&str>,
        route_key: Option<&str>,
    ) -> bool {
        let dir = dir.into();
        let Ok(root) = ensure_channel_files_root() else {
            return false;
        };
        let Ok(dir) = std::fs::canonicalize(dir) else {
            return false;
        };
        if dir == root || !dir.starts_with(&root) {
            return false;
        }
        let mut state = lock(&self.state);
        if let Some(message_id) = message_id {
            let dirs = state
                .dirs_by_message
                .entry(message_id.to_owned())
                .or_default();
            if !dirs.contains(&dir) {
                dirs.push(dir.clone());
            }
            state.message_by_dir.insert(dir, message_id.to_owned());
        } else if let Some(route_key) = route_key {
            let dirs = state
                .untracked_dirs_by_route
                .entry(route_key.to_owned())
                .or_default();
            if !dirs.contains(&dir) {
                dirs.push(dir);
            }
        }
        message_id.is_some() || route_key.is_some()
    }

    pub fn on_prompt_buffered(
        &self,
        session_id: &str,
        message_id: Option<&str>,
        route_key: Option<&str>,
    ) {
        if let Some(message_id) = message_id {
            let mut state = lock(&self.state);
            state.buffered_messages.insert(message_id.to_owned());
            remember_message_dirs_for_session(&mut state, message_id, session_id);
        } else {
            self.remember_untracked_dirs_for_session(session_id, route_key);
        }
    }

    pub fn on_prompt_start(
        &self,
        session_id: &str,
        message_id: Option<&str>,
        route_key: Option<&str>,
    ) {
        if let Some(message_id) = message_id {
            let mut state = lock(&self.state);
            remember_message_dirs_for_session(&mut state, message_id, session_id);
        } else {
            self.remember_untracked_dirs_for_session(session_id, route_key);
        }
    }

    pub fn on_prompt_buffer_drained(&self, message_ids: &[String]) {
        let Some(last_message_id) = message_ids.last() else {
            return;
        };
        lock(&self.state)
            .coalesced_messages
            .insert(last_message_id.clone(), message_ids.to_vec());
    }

    pub fn on_prompt_buffer_dropped(&self, session_id: &str, message_ids: &[String]) {
        for message_id in message_ids {
            self.cleanup_message(message_id);
        }
        self.cleanup_untracked_dirs_for_session(session_id);
    }

    pub fn on_prompt_end(&self, session_id: &str, message_id: Option<&str>) {
        let Some(message_id) = message_id else {
            self.cleanup_session(session_id);
            return;
        };
        let coalesced = lock(&self.state).coalesced_messages.remove(message_id);
        if let Some(message_ids) = coalesced {
            for coalesced_id in message_ids {
                self.cleanup_message(&coalesced_id);
            }
            self.cleanup_untracked_dirs_for_session(session_id);
        } else {
            self.cleanup_message(message_id);
        }
    }

    pub fn cleanup_all(&self) {
        let dirs = {
            let mut state = lock(&self.state);
            let mut dirs = HashSet::new();
            dirs.extend(state.dirs_by_session.values().flatten().cloned());
            dirs.extend(state.dirs_by_message.values().flatten().cloned());
            dirs.extend(state.untracked_dirs_by_route.values().flatten().cloned());
            state.dirs_by_session.clear();
            state.dirs_by_message.clear();
            state.message_by_dir.clear();
            state.untracked_dirs_by_route.clear();
            state.buffered_messages.clear();
            state.coalesced_messages.clear();
            dirs
        };
        cleanup_attachment_dirs(dirs);
    }

    fn remember_untracked_dirs_for_session(&self, session_id: &str, route_key: Option<&str>) {
        let Some(route_key) = route_key else {
            return;
        };
        let mut state = lock(&self.state);
        let Some(dirs) = state.untracked_dirs_by_route.remove(route_key) else {
            return;
        };
        let session_dirs = state
            .dirs_by_session
            .entry(session_id.to_owned())
            .or_default();
        for dir in dirs {
            if !session_dirs.contains(&dir) {
                session_dirs.push(dir);
            }
        }
    }

    fn cleanup_message(&self, message_id: &str) {
        let dirs = {
            let mut state = lock(&self.state);
            state.buffered_messages.remove(message_id);
            let Some(dirs) = state.dirs_by_message.remove(message_id) else {
                return;
            };
            for dir in &dirs {
                state.message_by_dir.remove(dir);
            }
            remove_attachment_dirs_from_sessions(&mut state, &dirs);
            dirs
        };
        cleanup_attachment_dirs(dirs);
    }

    fn cleanup_session(&self, session_id: &str) {
        let dirs = {
            let mut state = lock(&self.state);
            let Some(dirs) = state.dirs_by_session.remove(session_id) else {
                return;
            };
            remove_attachment_dirs_from_messages(&mut state, &dirs);
            for dir in &dirs {
                state.message_by_dir.remove(dir);
            }
            dirs
        };
        cleanup_attachment_dirs(dirs);
    }

    fn cleanup_untracked_dirs_for_session(&self, session_id: &str) {
        let dirs = {
            let mut state = lock(&self.state);
            let Some(session_dirs) = state.dirs_by_session.get(session_id).cloned() else {
                return;
            };
            let (untracked, tracked): (Vec<_>, Vec<_>) = session_dirs
                .into_iter()
                .partition(|dir| !state.message_by_dir.contains_key(dir));
            if tracked.is_empty() {
                state.dirs_by_session.remove(session_id);
            } else {
                state.dirs_by_session.insert(session_id.to_owned(), tracked);
            }
            untracked
        };
        cleanup_attachment_dirs(dirs);
    }
}

fn remember_message_dirs_for_session(
    state: &mut WeComAttachmentLeaseState,
    message_id: &str,
    session_id: &str,
) {
    let Some(dirs) = state.dirs_by_message.get(message_id).cloned() else {
        return;
    };
    let session_dirs = state
        .dirs_by_session
        .entry(session_id.to_owned())
        .or_default();
    for dir in dirs {
        if !session_dirs.contains(&dir) {
            session_dirs.push(dir);
        }
    }
}

fn remove_attachment_dirs_from_sessions(
    state: &mut WeComAttachmentLeaseState,
    dirs: &[std::path::PathBuf],
) {
    let removed = dirs.iter().collect::<HashSet<_>>();
    state.dirs_by_session.retain(|_, session_dirs| {
        session_dirs.retain(|dir| !removed.contains(dir));
        !session_dirs.is_empty()
    });
}

fn remove_attachment_dirs_from_messages(
    state: &mut WeComAttachmentLeaseState,
    dirs: &[std::path::PathBuf],
) {
    let removed = dirs.iter().collect::<HashSet<_>>();
    state.dirs_by_message.retain(|message_id, message_dirs| {
        message_dirs.retain(|dir| !removed.contains(dir));
        if message_dirs.is_empty() {
            state.buffered_messages.remove(message_id);
            false
        } else {
            true
        }
    });
    for dir in dirs {
        state.message_by_dir.remove(dir);
    }
}

fn cleanup_attachment_dirs(dirs: impl IntoIterator<Item = std::path::PathBuf>) {
    let allowed = std::env::temp_dir().join("channel-files");
    let Ok(allowed) = std::fs::canonicalize(allowed) else {
        return;
    };
    for dir in dirs {
        let Ok(metadata) = std::fs::symlink_metadata(&dir) else {
            continue;
        };
        if metadata.file_type().is_symlink() {
            let Some(parent) = dir.parent() else {
                continue;
            };
            let Ok(real_parent) = std::fs::canonicalize(parent) else {
                continue;
            };
            if real_parent.starts_with(&allowed) {
                let _ = std::fs::remove_file(dir);
            }
            continue;
        }
        let Ok(real) = std::fs::canonicalize(&dir) else {
            continue;
        };
        if real == allowed || !real.starts_with(&allowed) {
            continue;
        }
        if metadata.is_dir() {
            let _ = std::fs::remove_dir_all(dir);
        } else {
            let _ = std::fs::remove_file(dir);
        }
    }
}

fn ensure_channel_files_root() -> Result<std::path::PathBuf, WeComMediaError> {
    let allowed = std::env::temp_dir().join("channel-files");
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&allowed)
            .map_err(|error| {
                WeComMediaError::new(format!("cannot prepare outbound media directory: {error}"))
            })?;
    }
    #[cfg(not(unix))]
    std::fs::create_dir_all(&allowed).map_err(|error| {
        WeComMediaError::new(format!("cannot prepare outbound media directory: {error}"))
    })?;
    let metadata = std::fs::symlink_metadata(&allowed).map_err(|error| {
        WeComMediaError::new(format!("cannot inspect outbound media directory: {error}"))
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(WeComMediaError::new(
            "cannot prepare outbound media directory: not a safe directory",
        ));
    }
    std::fs::canonicalize(allowed).map_err(|error| {
        WeComMediaError::new(format!("cannot resolve outbound media directory: {error}"))
    })
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Project an SDK callback into the source adapter's authenticated inbound
/// message shape. The caller remains responsible for preflight, deduplication,
/// downloads, and invoking the session router.
pub fn project_inbound_message(
    payload: &Value,
    bot_id: &str,
) -> Result<WeComInboundProjection, WeComProjectionError> {
    let body = payload
        .as_object()
        .and_then(|raw| raw.get("body"))
        .and_then(Value::as_object)
        .or_else(|| payload.as_object())
        .ok_or(WeComProjectionError::UnrecognizedPayload)?;
    let sender = get_record(Some(body), "from");
    let sender_id = get_string(sender, "userid").to_owned();
    if sender_id.is_empty() {
        return Err(WeComProjectionError::MissingSenderId);
    }
    let is_group = get_string(Some(body), "chattype") == "group";
    let raw_chat_id = get_string(Some(body), "chatid");
    let chat_id = if is_group || !raw_chat_id.is_empty() {
        raw_chat_id.to_owned()
    } else {
        sender_id.clone()
    };
    if chat_id.is_empty() {
        return Err(WeComProjectionError::MissingChatId);
    }
    let raw_message_id = nonempty(get_string(Some(body), "msgid"));
    let message_id = raw_message_id
        .clone()
        .unwrap_or_else(|| format!("synthetic-{}", Uuid::new_v4()));
    let quote = get_record(Some(body), "quote");
    let is_reply_to_bot = get_record(quote, "from")
        .and_then(|from| from.get("userid"))
        .and_then(Value::as_str)
        .is_some_and(|sender| sender == bot_id);
    let referenced_text = quote.map(extract_text).filter(|text| !text.is_empty());

    Ok(WeComInboundProjection {
        message_id,
        raw_message_id,
        sender_name: nonempty(get_string(sender, "name")).unwrap_or_else(|| sender_id.clone()),
        sender_id,
        chat_id,
        is_group,
        is_mentioned: true,
        is_reply_to_bot,
        text: extract_text(body),
        referenced_text,
        media: collect_inbound_media_refs(body),
    })
}

pub fn extract_text(body: &Map<String, Value>) -> String {
    let message_type = get_string(Some(body), "msgtype");
    if message_type == "mixed" {
        let items = get_record(Some(body), "mixed")
            .and_then(|mixed| mixed.get("msg_item"))
            .and_then(Value::as_array);
        return items
            .into_iter()
            .flatten()
            .filter_map(Value::as_object)
            .filter_map(|item| {
                let kind = get_string(Some(item), "msgtype");
                if kind == "text" || kind == "voice" {
                    let field = if kind == "text" { "text" } else { "voice" };
                    let text = get_string(get_record(Some(item), field), "content");
                    (!text.is_empty()).then_some(text)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            .trim()
            .to_owned();
    }
    let text = get_string(get_record(Some(body), "text"), "content");
    if !text.is_empty() {
        return text.to_owned();
    }
    let voice_text = get_string(get_record(Some(body), "voice"), "content");
    if !voice_text.is_empty() {
        return voice_text.to_owned();
    }
    match message_type {
        "image" => "(image)".to_owned(),
        "voice" => "(voice)".to_owned(),
        "video" => "(video)".to_owned(),
        "file" => {
            let name = sanitize_file_name(get_string(get_record(Some(body), "file"), "filename"));
            format!("(file: {})", if name.is_empty() { "file" } else { &name })
        }
        _ => String::new(),
    }
}

/// Collect media in the same order as the TypeScript adapter, descending into
/// quoted messages to a maximum depth of three and deduplicating URLs.
pub fn collect_inbound_media_refs(body: &Map<String, Value>) -> Vec<WeComInboundMediaRef> {
    let mut refs = Vec::new();
    let mut seen_urls = std::collections::HashSet::new();
    collect_media_recursive(body, 0, &mut seen_urls, &mut refs);
    refs
}

fn collect_media_recursive(
    body: &Map<String, Value>,
    depth: usize,
    seen_urls: &mut std::collections::HashSet<String>,
    output: &mut Vec<WeComInboundMediaRef>,
) {
    if depth > 3 {
        return;
    }
    let mixed_items = get_record(Some(body), "mixed")
        .and_then(|mixed| mixed.get("msg_item"))
        .and_then(Value::as_array);
    for item in mixed_items
        .into_iter()
        .flatten()
        .filter_map(Value::as_object)
    {
        let Some(kind) = WeComMediaType::from_str(get_string(Some(item), "msgtype")) else {
            continue;
        };
        add_media_ref(
            kind,
            get_record(Some(item), kind.as_str()).unwrap_or(&Map::new()),
            seen_urls,
            output,
        );
    }
    for kind in [
        WeComMediaType::Image,
        WeComMediaType::File,
        WeComMediaType::Video,
        WeComMediaType::Voice,
    ] {
        let empty = Map::new();
        add_media_ref(
            kind,
            get_record(Some(body), kind.as_str()).unwrap_or(&empty),
            seen_urls,
            output,
        );
    }
    if let Some(quote) = get_record(Some(body), "quote") {
        collect_media_recursive(quote, depth + 1, seen_urls, output);
    }
}

fn add_media_ref(
    media_type: WeComMediaType,
    source: &Map<String, Value>,
    seen_urls: &mut std::collections::HashSet<String>,
    output: &mut Vec<WeComInboundMediaRef>,
) {
    let url = get_string(Some(source), "url");
    if url.is_empty() || !seen_urls.insert(url.to_owned()) {
        return;
    }
    let file_name = nonempty(get_string(Some(source), "filename"))
        .or_else(|| nonempty(get_string(Some(source), "file_name")));
    output.push(WeComInboundMediaRef {
        media_type,
        url: url.to_owned(),
        aes_key: nonempty(get_string(Some(source), "aeskey")),
        file_name,
    });
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeComOutboundMediaMarker {
    pub media_type: WeComMediaType,
    pub path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeComOutboundContent {
    pub cleaned_text: String,
    pub media: Vec<WeComOutboundMediaMarker>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeComDownloadedMedia {
    pub data: Vec<u8>,
    pub file_name: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WeComAttachmentDownloadResult {
    pub attachments: Vec<ChannelPromptAttachment>,
    pub failures: Vec<(WeComMediaType, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeComOutboundFile {
    pub data: Vec<u8>,
    pub file_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeComMediaError(String);

impl fmt::Display for WeComMediaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for WeComMediaError {}

impl WeComMediaError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// Read an outbound marker path only when it resolves beneath the adapter's
/// private `tmp/channel-files` staging directory. The opened file must remain
/// a regular file and the read is capped even if its size changes after stat.
pub async fn read_outbound_media(
    raw_path: &str,
    cwd: &Path,
) -> Result<WeComOutboundFile, WeComMediaError> {
    let allowed_real = ensure_channel_files_root()?;
    let input = Path::new(raw_path);
    let resolved = if input.is_absolute() {
        input.to_path_buf()
    } else {
        let cwd = if cwd.is_absolute() {
            cwd.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|error| WeComMediaError::new(format!("cannot resolve cwd: {error}")))?
                .join(cwd)
        };
        cwd.join(input)
    };
    let real = std::fs::canonicalize(&resolved).map_err(|error| {
        WeComMediaError::new(format!("cannot resolve outbound media file: {error}"))
    })?;
    if !real.starts_with(&allowed_real) {
        return Err(WeComMediaError::new(
            "Media path outside allowed outbound directory",
        ));
    }

    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(&real).await.map_err(|error| {
        WeComMediaError::new(format!("cannot open outbound media file: {error}"))
    })?;
    let metadata = file.metadata().await.map_err(|error| {
        WeComMediaError::new(format!("cannot inspect outbound media file: {error}"))
    })?;
    if !metadata.is_file() {
        let name = resolved
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        return Err(WeComMediaError::new(format!("Not a regular file: {name}")));
    }
    if metadata.len() > WECOM_MAX_MEDIA_BYTES as u64 {
        return Err(WeComMediaError::new(format!(
            "Media file too large: {} bytes",
            metadata.len()
        )));
    }
    let mut limited = file.take(WECOM_MAX_MEDIA_BYTES as u64 + 1);
    let mut data = Vec::with_capacity(metadata.len().min(WECOM_MAX_MEDIA_BYTES as u64) as usize);
    limited.read_to_end(&mut data).await.map_err(|error| {
        WeComMediaError::new(format!("cannot read outbound media file: {error}"))
    })?;
    if data.len() > WECOM_MAX_MEDIA_BYTES {
        return Err(WeComMediaError::new("Media file too large"));
    }
    let file_name = real
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_owned();
    Ok(WeComOutboundFile { data, file_name })
}

/// Download an inbound attachment without following redirects, contacting
/// private addresses, or retaining more than the adapter's 20 MiB cap.
/// DNS results are pinned into the request's resolver configuration so the
/// address checked here is the address reqwest connects to.
pub async fn download_inbound_media(
    reference: &WeComInboundMediaRef,
) -> Result<WeComDownloadedMedia, WeComMediaError> {
    let url = Url::parse(&reference.url)
        .map_err(|_| WeComMediaError::new("unsafe media URL (invalid URL)"))?;
    if url.scheme() != "https" {
        return Err(WeComMediaError::new(
            "unsafe media URL (non-HTTPS protocol)",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(WeComMediaError::new(
            "unsafe media URL (URL contains embedded credentials)",
        ));
    }
    let host = url
        .host()
        .ok_or_else(|| WeComMediaError::new("unsafe media URL (missing hostname)"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| WeComMediaError::new("unsafe media URL (missing port)"))?;
    let (domain, pinned_addrs) = match host {
        Host::Domain(domain) => {
            let resolver_domain = domain.to_owned();
            let normalized = domain.trim_end_matches('.').to_ascii_lowercase();
            if normalized.is_empty()
                || normalized == "localhost"
                || normalized.ends_with(".localhost")
                || normalized.ends_with(".local")
            {
                return Err(WeComMediaError::new("unsafe media URL (local hostname)"));
            }
            if !normalized.contains('.') {
                return Err(WeComMediaError::new("unsafe media URL (bare hostname)"));
            }
            let resolved = tokio::time::timeout(
                WECOM_MEDIA_SOCKET_TIMEOUT,
                tokio::net::lookup_host((normalized.as_str(), port)),
            )
            .await
            .map_err(|_| {
                WeComMediaError::new(format!(
                    "unsafe media URL (DNS lookup timed out for {normalized})"
                ))
            })?
            .map_err(|error| {
                WeComMediaError::new(format!(
                    "unsafe media URL (DNS lookup failed for {normalized}: {error})"
                ))
            })?;
            let mut addresses = Vec::new();
            for address in resolved {
                if !is_public_ip_address(address.ip()) {
                    return Err(WeComMediaError::new(format!(
                        "unsafe media URL ({normalized} resolved to private address {})",
                        address.ip()
                    )));
                }
                if !addresses.contains(&address) {
                    addresses.push(address);
                }
            }
            if addresses.is_empty() {
                return Err(WeComMediaError::new(format!(
                    "unsafe media URL (no DNS records for {normalized})"
                )));
            }
            (Some(resolver_domain), addresses)
        }
        Host::Ipv4(address) => {
            let address = IpAddr::V4(address);
            if !is_public_ip_address(address) {
                return Err(WeComMediaError::new(format!(
                    "unsafe media URL (private address {address})"
                )));
            }
            (None, vec![SocketAddr::new(address, port)])
        }
        Host::Ipv6(address) => {
            let address = IpAddr::V6(address);
            if !is_public_ip_address(address) {
                return Err(WeComMediaError::new(format!(
                    "unsafe media URL (private address {address})"
                )));
            }
            (None, vec![SocketAddr::new(address, port)])
        }
    };

    let mut builder = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(WECOM_MEDIA_SOCKET_TIMEOUT)
        .timeout(WECOM_MEDIA_ABSOLUTE_TIMEOUT);
    if let Some(domain) = domain.as_deref() {
        builder = builder.resolve_to_addrs(domain, &pinned_addrs);
    }
    let client = builder
        .build()
        .map_err(|_| WeComMediaError::new("could not configure guarded media client"))?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|_| WeComMediaError::new("media download request failed"))?;
    let status = response.status();
    if status.is_redirection() {
        return Err(WeComMediaError::new("redirected media URL"));
    }
    if !status.is_success() {
        return Err(WeComMediaError::new(format!(
            "media download failed: HTTP {}",
            status.as_u16()
        )));
    }
    if response
        .content_length()
        .is_some_and(|size| size > WECOM_MAX_MEDIA_BYTES as u64)
    {
        return Err(WeComMediaError::new("oversized attachment"));
    }
    let file_name = response
        .headers()
        .get(reqwest::header::CONTENT_DISPOSITION)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_disposition_filename);
    let mut stream = response.bytes_stream();
    let mut data = Vec::new();
    while let Some(chunk) = tokio::time::timeout(WECOM_MEDIA_SOCKET_TIMEOUT, stream.next())
        .await
        .map_err(|_| WeComMediaError::new("media download timed out"))?
    {
        let chunk = chunk.map_err(|_| WeComMediaError::new("media response read failed"))?;
        if data.len().saturating_add(chunk.len()) > WECOM_MAX_MEDIA_BYTES {
            return Err(WeComMediaError::new("oversized attachment"));
        }
        data.extend_from_slice(&chunk);
    }
    if data.is_empty() {
        return Err(WeComMediaError::new("empty media response"));
    }
    let data = match reference.aes_key.as_deref() {
        Some(key) if !key.is_empty() => decrypt_file(&data, key)?,
        _ => data,
    };
    Ok(WeComDownloadedMedia {
        data,
        file_name: reference.file_name.clone().or(file_name),
    })
}

/// Download and project every referenced attachment in source order. A host
/// supplies the connection-generation predicate so disconnect can stop new
/// downloads and remove a just-written file before it reaches a session.
pub async fn download_message_attachments<F>(
    projection: &WeComInboundProjection,
    route_key: &str,
    leases: &WeComAttachmentLeases,
    should_continue: F,
) -> WeComAttachmentDownloadResult
where
    F: Fn() -> bool,
{
    let mut result = WeComAttachmentDownloadResult::default();
    let mut total_media_bytes = 0usize;
    for (index, reference) in projection.media.iter().enumerate() {
        if index >= WECOM_MAX_MESSAGE_MEDIA_REFERENCES {
            result.failures.push((
                reference.media_type,
                format!(
                    "message attachment count exceeded the {WECOM_MAX_MESSAGE_MEDIA_REFERENCES}-item limit"
                ),
            ));
            break;
        }
        if !should_continue() {
            break;
        }
        let downloaded = match download_inbound_media(reference).await {
            Ok(value) => value,
            Err(error) => {
                result
                    .failures
                    .push((reference.media_type, error.to_string()));
                continue;
            }
        };
        let next_total = total_media_bytes.saturating_add(downloaded.data.len());
        if next_total > WECOM_MAX_MESSAGE_MEDIA_BYTES {
            result.failures.push((
                reference.media_type,
                format!(
                    "message attachments exceeded the {}-byte aggregate limit",
                    WECOM_MAX_MESSAGE_MEDIA_BYTES
                ),
            ));
            break;
        }
        if !should_continue() {
            break;
        }
        let file_name = sanitize_file_name(
            reference
                .file_name
                .as_deref()
                .or(downloaded.file_name.as_deref())
                .unwrap_or_default(),
        );
        if reference.media_type == WeComMediaType::Image {
            total_media_bytes = next_total;
            result.attachments.push(ChannelPromptAttachment {
                kind: Some(ChannelAttachmentType::Image),
                data: Some(BASE64_STANDARD.encode(&downloaded.data)),
                file_path: None,
                file_name: Some(file_name),
                mime_type: Some(detect_image_mime(&downloaded.data).to_owned()),
            });
            continue;
        }

        let root = match ensure_channel_files_root() {
            Ok(root) => root,
            Err(error) => {
                result
                    .failures
                    .push((reference.media_type, error.to_string()));
                continue;
            }
        };
        let dir = root.join(Uuid::new_v4().to_string());
        let create_dir_result = create_private_attachment_dir(&dir);
        if let Err(error) = create_dir_result {
            result
                .failures
                .push((reference.media_type, error.to_string()));
            continue;
        }
        let safe_name = if file_name.is_empty() {
            format!("wecom_{}", reference.media_type.as_str())
        } else {
            file_name
        };
        let file_path = dir.join(&safe_name);
        if !should_continue() {
            cleanup_attachment_dirs([dir]);
            break;
        }
        if let Err(error) = write_private_attachment(&file_path, &downloaded.data).await {
            cleanup_attachment_dirs([dir]);
            result
                .failures
                .push((reference.media_type, error.to_string()));
            continue;
        }
        if !should_continue() {
            cleanup_attachment_dirs([dir]);
            break;
        }
        if !leases.remember_dir(dir.clone(), Some(&projection.message_id), Some(route_key)) {
            cleanup_attachment_dirs([dir]);
            result.failures.push((
                reference.media_type,
                "could not retain attachment for prompt lifecycle".to_owned(),
            ));
            continue;
        }
        total_media_bytes = next_total;
        let kind = match reference.media_type {
            WeComMediaType::File => ChannelAttachmentType::File,
            WeComMediaType::Voice => ChannelAttachmentType::Audio,
            WeComMediaType::Video => ChannelAttachmentType::Video,
            WeComMediaType::Image => unreachable!("image attachments use inline base64"),
        };
        result.attachments.push(ChannelPromptAttachment {
            kind: Some(kind),
            data: None,
            file_path: Some(file_path.to_string_lossy().into_owned()),
            file_name: Some(safe_name),
            mime_type: Some(media_type_mime(reference.media_type).to_owned()),
        });
    }
    result
}

/// Apply the source adapter's text placeholder when a media-only message has
/// at least one successfully downloaded attachment.
pub fn inbound_text_with_attachment_fallback(
    original_text: &str,
    attachments: &[ChannelPromptAttachment],
) -> String {
    if !original_text.is_empty() || attachments.is_empty() {
        return original_text.to_owned();
    }
    if attachments
        .iter()
        .any(|attachment| attachment.kind == Some(ChannelAttachmentType::Image))
    {
        "(image)".to_owned()
    } else {
        let name = attachments
            .first()
            .and_then(|attachment| attachment.file_name.as_deref())
            .filter(|name| !name.is_empty())
            .unwrap_or("file");
        format!("(file: {name})")
    }
}

fn create_private_attachment_dir(path: &Path) -> Result<(), WeComMediaError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(path)
            .map_err(|error| {
                WeComMediaError::new(format!("cannot create attachment directory: {error}"))
            })
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir(path).map_err(|error| {
            WeComMediaError::new(format!("cannot create attachment directory: {error}"))
        })
    }
}

async fn write_private_attachment(path: &Path, data: &[u8]) -> Result<(), WeComMediaError> {
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options
        .open(path)
        .await
        .map_err(|error| WeComMediaError::new(format!("cannot create attachment file: {error}")))?;
    file.write_all(data)
        .await
        .map_err(|error| WeComMediaError::new(format!("cannot write attachment file: {error}")))
}

/// Decrypt the SDK's AES-256-CBC file format with its 32-byte PKCS#7 padding.
pub fn decrypt_file(encrypted: &[u8], aes_key: &str) -> Result<Vec<u8>, WeComMediaError> {
    if encrypted.is_empty() {
        return Err(WeComMediaError::new(
            "decryptFile: encryptedBuffer is empty or not provided",
        ));
    }
    if aes_key.is_empty() {
        return Err(WeComMediaError::new(
            "decryptFile: aesKey must be a non-empty string",
        ));
    }
    let key = BASE64_STANDARD
        .decode(aes_key)
        .map_err(|_| WeComMediaError::new("decryptFile: invalid base64 aesKey"))?;
    if key.len() != 32 {
        return Err(WeComMediaError::new(
            "decryptFile: aesKey must decode to 32 bytes",
        ));
    }
    if encrypted.len() % 16 != 0 {
        return Err(WeComMediaError::new(
            "decryptFile: ciphertext length is not AES-block aligned",
        ));
    }
    let cipher = Aes256::new_from_slice(&key)
        .map_err(|_| WeComMediaError::new("decryptFile: invalid AES-256 key"))?;
    let iv = &key[..16];
    let mut output = encrypted.to_vec();
    let mut previous = [0_u8; 16];
    previous.copy_from_slice(iv);
    for (block_index, block) in output.chunks_exact_mut(16).enumerate() {
        let cipher_offset = block_index * 16;
        let mut cipher_block = [0_u8; 16];
        cipher_block.copy_from_slice(&encrypted[cipher_offset..cipher_offset + 16]);
        let mut decoded = GenericArray::clone_from_slice(block);
        cipher.decrypt_block(&mut decoded);
        for index in 0..16 {
            block[index] = decoded[index] ^ previous[index];
        }
        previous = cipher_block;
    }
    let padding = usize::from(*output.last().expect("non-empty ciphertext"));
    if !(1..=32).contains(&padding)
        || padding > output.len()
        || output[output.len() - padding..]
            .iter()
            .any(|byte| usize::from(*byte) != padding)
    {
        return Err(WeComMediaError::new("decryptFile: invalid PKCS#7 padding"));
    }
    output.truncate(output.len() - padding);
    Ok(output)
}

fn parse_content_disposition_filename(value: &str) -> Option<String> {
    let mut plain = None;
    for part in value.split(';').map(str::trim) {
        let Some((name, raw)) = part.split_once('=') else {
            continue;
        };
        let name = name.trim();
        let raw = raw.trim().trim_matches('"');
        if name.eq_ignore_ascii_case("filename*") {
            if let Some(encoded) = raw.strip_prefix("UTF-8''").or_else(|| {
                raw.get(..7)
                    .filter(|prefix| prefix.eq_ignore_ascii_case("UTF-8''"))
                    .and_then(|_| raw.get(7..))
            }) {
                if let Some(decoded) = percent_decode_utf8(encoded) {
                    return Some(decoded);
                }
                return Some(encoded.to_owned());
            }
        } else if name.eq_ignore_ascii_case("filename") {
            plain = Some(raw.to_owned());
        }
    }
    plain
}

fn percent_decode_utf8(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = hex_digit(*bytes.get(index + 1)?)?;
            let low = hex_digit(*bytes.get(index + 2)?)?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn is_public_ip_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    !(a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0 && (c == 0 || c == 2))
        || (a == 192 && b == 88 && c == 99)
        || (a == 192 && b == 168)
        || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
        || (a == 203 && b == 0 && c == 113)
        || a >= 224)
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    let groups = address.segments();
    if let Some(embedded) = embedded_ipv4(groups) {
        return is_public_ipv4(embedded);
    }
    let first = groups[0];
    let all_zero = groups.iter().all(|group| *group == 0);
    let loopback = groups[..7].iter().all(|group| *group == 0) && groups[7] == 1;
    let low = u32::from(groups[6]) << 16 | u32::from(groups[7]);
    let low_zero_private = groups[..5].iter().all(|group| *group == 0)
        && low != 0
        && !is_public_ipv4(Ipv4Addr::from(low));
    !(all_zero
        || loopback
        || low_zero_private
        || (0xfc00..=0xfdff).contains(&first)
        || (0xff00..=0xffff).contains(&first)
        || (first == 0x0100 && groups[1..4].iter().all(|group| *group == 0))
        || (first == 0x2001 && groups[1] == 0x0002)
        || (first == 0x2001 && groups[1] == 0x0db8)
        || (first == 0x2001 && (groups[1] & 0xfff0) == 0x0010)
        || (first == 0x2001 && (groups[1] & 0xfff0) == 0x0030)
        || (0xfe80..=0xfeff).contains(&first))
}

fn embedded_ipv4(groups: [u16; 8]) -> Option<Ipv4Addr> {
    if groups[..4].iter().all(|group| *group == 0)
        && groups[4] == 0xffff
        && groups[5] == 0
        && (groups[6] != 0 || groups[7] != 0)
    {
        return Some(ipv4_from_groups(groups[6], groups[7]));
    }
    if groups[..5].iter().all(|group| *group == 0)
        && groups[5] == 0xffff
        && (groups[6] != 0 || groups[7] != 0)
    {
        return Some(ipv4_from_groups(groups[6], groups[7]));
    }
    if groups[..6].iter().all(|group| *group == 0) && (groups[6] != 0 || groups[7] != 0) {
        return Some(ipv4_from_groups(groups[6], groups[7]));
    }
    if groups[0] == 0x2002 {
        return Some(ipv4_from_groups(groups[1], groups[2]));
    }
    if groups[0] == 0x2001 && groups[1] == 0 {
        return Some(ipv4_from_groups(!groups[6], !groups[7]));
    }
    if groups[0] == 0x0064 && groups[1] == 0xff9b {
        return Some(ipv4_from_groups(groups[6], groups[7]));
    }
    None
}

fn ipv4_from_groups(high: u16, low: u16) -> Ipv4Addr {
    Ipv4Addr::new((high >> 8) as u8, high as u8, (low >> 8) as u8, low as u8)
}

/// Extract supported `[IMAGE: path]` markers outside fenced, inline, and
/// indented code. The TypeScript adapter currently recognizes IMAGE only.
pub fn parse_outbound_media_markers(text: &str) -> WeComOutboundContent {
    let code_ranges = code_ranges(text);
    let mut media = Vec::new();
    let mut removals = Vec::new();
    let bytes = text.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor] != b'[' {
            cursor += char_len_at(text, cursor);
            continue;
        }
        let Some(close_rel) = text[cursor..].find(']') else {
            break;
        };
        let close = cursor + close_rel;
        let inner = &text[cursor + 1..close];
        let colon = inner.find(':');
        if let Some(colon) = colon {
            let kind = &inner[..colon];
            let path = inner[colon + 1..].trim();
            if kind.eq_ignore_ascii_case("image")
                && !path.is_empty()
                && !code_ranges
                    .iter()
                    .any(|(start, end)| cursor >= *start && cursor < *end)
            {
                media.push(WeComOutboundMediaMarker {
                    media_type: WeComMediaType::Image,
                    path: path.to_owned(),
                });
                removals.push((cursor, close + 1));
            }
        }
        cursor = close + 1;
    }
    let mut cleaned = text.to_owned();
    for (start, end) in removals.into_iter().rev() {
        cleaned.replace_range(start..end, "");
    }
    let mut compact = String::with_capacity(cleaned.len());
    let mut newline_run = 0usize;
    for character in cleaned.chars() {
        if character == '\n' {
            newline_run += 1;
            if newline_run <= 2 {
                compact.push(character);
            }
        } else {
            newline_run = 0;
            compact.push(character);
        }
    }
    WeComOutboundContent {
        cleaned_text: compact.trim().to_owned(),
        media,
    }
}

/// Split markdown into bounded UTF-8 chunks and close/reopen code fences at
/// chunk boundaries, matching the adapter's 3,800-byte policy.
pub fn split_markdown_chunks(text: &str) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut code_fence: Option<&str> = None;
    for line in text.split('\n') {
        let candidate_fence = toggle_code_fence_state(line, code_fence);
        let candidate = if current.is_empty() {
            line.to_owned()
        } else {
            format!("{current}\n{line}")
        };
        if markdown_chunk_fits(&candidate, candidate_fence) {
            current = candidate;
            code_fence = candidate_fence;
            continue;
        }
        flush_markdown_chunk(&mut chunks, &mut current, code_fence);
        let retried_fence = toggle_code_fence_state(line, code_fence);
        let retried = if current.is_empty() {
            line.to_owned()
        } else {
            format!("{current}\n{line}")
        };
        if markdown_chunk_fits(&retried, retried_fence) {
            current = retried;
            code_fence = retried_fence;
            continue;
        }
        let mut needs_line_break = !current.is_empty();
        let mut char_cursor = 0;
        while char_cursor < line.len() {
            let token = if line[char_cursor..].starts_with("```") {
                "```".to_owned()
            } else if line[char_cursor..].starts_with("~~~") {
                "~~~".to_owned()
            } else {
                let character = line[char_cursor..].chars().next().expect("valid boundary");
                character.to_string()
            };
            let next_fence = match code_fence {
                Some(open) if token == open => None,
                None if token == "```" => Some("```"),
                None if token == "~~~" => Some("~~~"),
                current => current,
            };
            let addition = if needs_line_break && !current.is_empty() {
                format!("\n{token}")
            } else {
                token.clone()
            };
            let candidate = format!("{current}{addition}");
            if !markdown_chunk_fits(&candidate, next_fence) {
                flush_markdown_chunk(&mut chunks, &mut current, code_fence);
                if current.is_empty() {
                    current.push_str(&token);
                } else {
                    current.push('\n');
                    current.push_str(&token);
                }
            } else {
                current = candidate;
            }
            code_fence = next_fence;
            needs_line_break = false;
            char_cursor += token.len();
        }
    }
    flush_markdown_chunk(&mut chunks, &mut current, code_fence);
    chunks
}

fn markdown_chunk_fits(value: &str, next_fence: Option<&str>) -> bool {
    let byte_count = value.len() + next_fence.map_or(0, |fence| fence.len() + 1);
    byte_count <= WECOM_MARKDOWN_CHUNK_BYTES
}

fn flush_markdown_chunk(chunks: &mut Vec<String>, current: &mut String, code_fence: Option<&str>) {
    if current.is_empty() {
        return;
    }
    if let Some(fence) = code_fence {
        chunks.push(format!("{current}\n{fence}"));
        *current = fence.to_owned();
    } else {
        chunks.push(std::mem::take(current));
    }
}

fn toggle_code_fence_state<'a>(line: &str, current: Option<&'a str>) -> Option<&'a str> {
    let mut state = current;
    let mut cursor = 0;
    while cursor < line.len() {
        let token = if line[cursor..].starts_with("```") {
            Some("```")
        } else if line[cursor..].starts_with("~~~") {
            Some("~~~")
        } else {
            None
        };
        if let Some(token) = token {
            state = match state {
                Some(open) if open == token => None,
                None => Some(token),
                other => other,
            };
            cursor += token.len();
        } else {
            cursor += char_len_at(line, cursor);
        }
    }
    state
}

/// Build the markdown message payload used by `WSClient.sendMessage`.
pub fn markdown_message_payload(chunk: &str) -> Value {
    json!({ "msgtype": "markdown", "markdown": { "content": chunk } })
}

pub fn sanitize_file_name(name: &str) -> String {
    let base = Path::new(name)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    let mut output = String::with_capacity(base.len());
    for character in base.chars() {
        if character == '\0' {
            continue;
        }
        if character.is_alphanumeric() || matches!(character, '.' | '_' | '-') {
            output.push(character);
        } else {
            output.push('_');
        }
    }
    let leading_dots = output
        .chars()
        .take_while(|character| *character == '.')
        .count();
    if leading_dots > 0 {
        output.replace_range(..leading_dots, "_");
    }
    output
}

pub fn detect_image_mime(data: &[u8]) -> &'static str {
    if data.starts_with(b"\x89PNG") {
        "image/png"
    } else if data.starts_with(b"GIF") {
        "image/gif"
    } else if data.len() >= 12 && &data[0..4] == b"RIFF" && &data[8..12] == b"WEBP" {
        "image/webp"
    } else if data.starts_with(b"\xff\xd8\xff") {
        "image/jpeg"
    } else {
        "application/octet-stream"
    }
}

pub fn media_type_mime(media_type: WeComMediaType) -> &'static str {
    match media_type {
        WeComMediaType::Video => "video/mp4",
        WeComMediaType::Voice => "audio/amr",
        WeComMediaType::Image | WeComMediaType::File => "application/octet-stream",
    }
}

pub fn extract_media_id(value: &Value) -> Option<&str> {
    let object = value.as_object()?;
    object
        .get("media_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            object
                .get("mediaId")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
        })
        .or_else(|| {
            object
                .get("body")
                .and_then(Value::as_object)
                .and_then(|body| body.get("media_id"))
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
        })
}

fn code_ranges(text: &str) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut fence_start: Option<(usize, &str)> = None;
    let mut cursor = 0;
    while cursor < text.len() {
        let token = if text[cursor..].starts_with("```") {
            Some("```")
        } else if text[cursor..].starts_with("~~~") {
            Some("~~~")
        } else {
            None
        };
        if let Some(token) = token {
            if let Some((start, open)) = fence_start {
                if token == open {
                    ranges.push((start, cursor + 3));
                    fence_start = None;
                }
            } else {
                fence_start = Some((cursor, token));
            }
            cursor += 3;
        } else {
            cursor += char_len_at(text, cursor);
        }
    }
    if let Some((start, _)) = fence_start {
        ranges.push((start, text.len()));
    }

    let mut line_start = 0;
    for line in text.split_inclusive('\n') {
        let content = line.strip_suffix('\n').unwrap_or(line);
        if (content.starts_with('\t') || content.starts_with("    "))
            && !ranges
                .iter()
                .any(|(start, end)| line_start >= *start && line_start < *end)
        {
            ranges.push((line_start, line_start + content.len()));
        }
        line_start += line.len();
    }
    if line_start < text.len() {
        let line = &text[line_start..];
        if (line.starts_with('\t') || line.starts_with("    "))
            && !ranges
                .iter()
                .any(|(start, end)| line_start >= *start && line_start < *end)
        {
            ranges.push((line_start, text.len()));
        }
    }

    let mut cursor = 0;
    while cursor < text.len() {
        if text.as_bytes()[cursor] != b'`' {
            cursor += char_len_at(text, cursor);
            continue;
        }
        let start = cursor;
        while cursor < text.len() && text.as_bytes()[cursor] == b'`' {
            cursor += 1;
        }
        let delimiter_len = cursor - start;
        let rest = &text[cursor..];
        let close_limit = rest.find('\n').unwrap_or(rest.len());
        let close = rest[..close_limit]
            .as_bytes()
            .windows(delimiter_len)
            .position(|window| window.iter().all(|byte| *byte == b'`'));
        if let Some(offset) = close {
            let end = cursor + offset + delimiter_len;
            if !ranges
                .iter()
                .any(|(from, to)| start >= *from && start < *to)
            {
                ranges.push((start, end));
            }
            cursor = end;
        }
    }
    ranges
}

fn get_record<'a>(
    value: Option<&'a Map<String, Value>>,
    key: &str,
) -> Option<&'a Map<String, Value>> {
    value?.get(key)?.as_object()
}

fn get_string<'a>(value: Option<&'a Map<String, Value>>, key: &str) -> &'a str {
    value
        .and_then(|object| object.get(key))
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn nonempty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}

fn char_len_at(value: &str, index: usize) -> usize {
    value[index..]
        .chars()
        .next()
        .map(char::len_utf8)
        .unwrap_or(1)
}
