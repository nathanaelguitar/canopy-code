//! Telegram channel adapter behavior.
//!
//! Port of `packages/channels/telegram/src/TelegramAdapter.ts`. Telegram's
//! HTTP API and getUpdates long-poll are behind [`TelegramApi`], while inbound
//! dispatch is behind [`TelegramInboundHandler`] so the daemon can connect the
//! adapter to its session router and shared ChannelBase command pipeline.

use super::channel_loop_store::SessionTarget;
use super::channel_prompt::{ChannelAttachmentType, ChannelPromptAttachment, ChannelPromptInput};
use super::gate_types::Envelope as GateEnvelope;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::task::JoinHandle;
use uuid::Uuid;

const TELEGRAM_API_BASE: &str = "https://api.telegram.org";
const TELEGRAM_FILE_BASE: &str = "https://api.telegram.org/file";
/// Maximum buffered Telegram photo payload before it is encoded for ACP.
pub const TELEGRAM_MAX_PHOTO_BYTES: usize = 8 * 1024 * 1024;
/// Maximum buffered Telegram document or voice payload before it is saved.
pub const TELEGRAM_MAX_ATTACHMENT_BYTES: usize = 32 * 1024 * 1024;
const TELEGRAM_WELCOME: &str = "Qwen Code Telegram bot\n\nSend any message to chat with Qwen Code.\nUse /new to start a fresh conversation.\nUse /cancel to stop a running request.\nUse /help to see available commands.";
const TELEGRAM_ERROR_REPLY: &str = "Sorry, something went wrong processing your message.";
const TYPING_INTERVAL: Duration = Duration::from_secs(4);
const LONG_POLL_SECONDS: u64 = 30;
const MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct TelegramUser {
    pub id: i64,
    #[serde(default)]
    pub first_name: String,
    #[serde(default)]
    pub last_name: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct TelegramChat {
    pub id: i64,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub title: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct TelegramEntity {
    #[serde(rename = "type")]
    pub kind: String,
    pub offset: usize,
    pub length: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct TelegramPhotoSize {
    pub file_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct TelegramDocument {
    pub file_id: String,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub file_size: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct TelegramVoice {
    pub file_id: String,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub file_size: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct TelegramReply {
    #[serde(default)]
    pub from: Option<TelegramUser>,
    #[serde(default)]
    pub text: Option<String>,
}

/// The fields of Telegram Message consumed by the Canopy channel adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct TelegramMessage {
    #[serde(default)]
    pub from: Option<TelegramUser>,
    pub chat: TelegramChat,
    #[serde(default)]
    pub message_thread_id: Option<i64>,
    #[serde(default)]
    pub reply_to_message: Option<TelegramReply>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub entities: Vec<TelegramEntity>,
    #[serde(default)]
    pub photo: Vec<TelegramPhotoSize>,
    #[serde(default)]
    pub document: Option<TelegramDocument>,
    #[serde(default)]
    pub voice: Option<TelegramVoice>,
    #[serde(default)]
    pub caption: Option<String>,
    #[serde(default)]
    pub caption_entities: Vec<TelegramEntity>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct TelegramUpdate {
    pub update_id: i64,
    #[serde(default)]
    pub message: Option<TelegramMessage>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct TelegramFile {
    #[serde(default)]
    pub file_path: Option<String>,
    #[serde(default)]
    pub file_size: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct TelegramMe {
    pub id: i64,
    #[serde(default)]
    pub username: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TelegramBotCommand {
    pub command: &'static str,
    pub description: &'static str,
}

pub const TELEGRAM_BOT_COMMANDS: [TelegramBotCommand; 5] = [
    TelegramBotCommand {
        command: "start",
        description: "Show quick-start help",
    },
    TelegramBotCommand {
        command: "help",
        description: "Show available commands",
    },
    TelegramBotCommand {
        command: "new",
        description: "Start a fresh conversation",
    },
    TelegramBotCommand {
        command: "cancel",
        description: "Cancel the running request",
    },
    TelegramBotCommand {
        command: "status",
        description: "Show session info",
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelegramAttachmentKind {
    File,
    Audio,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramAttachment {
    pub kind: TelegramAttachmentKind,
    pub file_path: String,
    pub mime_type: String,
    pub file_name: String,
}

/// Inbound data preserved by the adapter before ChannelBase dispatch.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelegramInboundEnvelope {
    pub channel_name: String,
    pub sender_id: String,
    pub sender_name: String,
    pub chat_id: String,
    pub chat_name: Option<String>,
    pub thread_id: Option<String>,
    pub text: String,
    pub is_group: bool,
    pub is_mentioned: bool,
    pub is_reply_to_bot: bool,
    pub referenced_text: Option<String>,
    pub image_base64: Option<String>,
    pub image_mime_type: Option<String>,
    pub attachments: Vec<TelegramAttachment>,
    /// The native adapter ran its host's authorization hook before media I/O.
    #[doc(hidden)]
    pub preflighted: bool,
}

impl TelegramInboundEnvelope {
    /// Projection into the existing shared ChannelBase prompt contract.
    pub fn to_prompt_input(&self) -> ChannelPromptInput {
        self.to_prompt_input_with_image(true)
    }

    /// Prompt projection without cloning the potentially large inline image.
    pub fn to_prompt_input_without_image(&self) -> ChannelPromptInput {
        self.to_prompt_input_with_image(false)
    }

    fn to_prompt_input_with_image(&self, include_image: bool) -> ChannelPromptInput {
        ChannelPromptInput {
            sender_id: self.sender_id.clone(),
            sender_name: self.sender_name.clone(),
            chat_id: self.chat_id.clone(),
            text: self.text.clone(),
            thread_id: self.thread_id.clone(),
            is_group: self.is_group,
            referenced_text: self.referenced_text.clone(),
            image_base64: include_image.then(|| self.image_base64.clone()).flatten(),
            image_mime_type: include_image
                .then(|| self.image_mime_type.clone())
                .flatten(),
            attachments: self
                .attachments
                .iter()
                .map(TelegramAttachment::to_prompt_attachment)
                .collect(),
            ..ChannelPromptInput::default()
        }
    }

    /// Projection into the shared authorization-gate contract.
    pub fn to_gate_envelope(&self) -> GateEnvelope {
        GateEnvelope {
            sender_id: self.sender_id.clone(),
            sender_name: self.sender_name.clone(),
            chat_id: self.chat_id.clone(),
            chat_name: self.chat_name.clone(),
            is_group: self.is_group,
            is_mentioned: self.is_mentioned,
            is_reply_to_bot: self.is_reply_to_bot,
        }
    }
}

impl TelegramAttachment {
    fn to_prompt_attachment(&self) -> ChannelPromptAttachment {
        ChannelPromptAttachment {
            kind: Some(match self.kind {
                TelegramAttachmentKind::File => ChannelAttachmentType::File,
                TelegramAttachmentKind::Audio => ChannelAttachmentType::Audio,
            }),
            file_path: Some(self.file_path.clone()),
            file_name: Some(self.file_name.clone()),
            mime_type: Some(self.mime_type.clone()),
            ..ChannelPromptAttachment::default()
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramSessionRoute {
    /// Present when a response is being sent during inbound dispatch. A route
    /// with no thread explicitly targets the general chat and overrides a
    /// possibly stale session target.
    pub thread_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelegramLifecycleKind {
    Started,
    TextChunk,
    ToolCall,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramTaskLifecycleEvent {
    pub channel_name: String,
    pub chat_id: String,
    pub session_id: String,
    pub kind: TelegramLifecycleKind,
}

pub type TelegramFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;
pub type TelegramInboundFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// Session runtime seam. Slash commands other than /start (including /help,
/// /new, /cancel, and /status) are forwarded here for the shared command path.
pub trait TelegramInboundHandler: Send + Sync + 'static {
    /// Gives the native host a chance to authorize a media update before the
    /// adapter downloads its photo, document, or voice payload. Returning
    /// `false` drops the update without fetching media.
    fn preflight_inbound<'a>(
        &'a self,
        _envelope: &'a TelegramInboundEnvelope,
    ) -> TelegramFuture<'a, bool> {
        Box::pin(async { Ok(true) })
    }

    fn handle_inbound<'a>(&'a self, envelope: TelegramInboundEnvelope)
    -> TelegramInboundFuture<'a>;

    /// Called when a collect-mode prompt is buffered behind an active turn.
    /// Telegram's current adapter does not set message IDs, so this is normally
    /// `None`; the optional value keeps the shared ChannelBase hook contract.
    fn on_prompt_buffered(
        &self,
        _chat_id: &str,
        _session_id: &str,
        _message_id: Option<&str>,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Called before a collect buffer is coalesced and re-entered. As in
    /// ChannelBase, implementations are notified only when at least one message
    /// ID is available.
    fn on_prompt_buffer_drained(
        &self,
        _chat_id: &str,
        _session_id: &str,
        _message_ids: &[String],
    ) -> Result<(), String> {
        Ok(())
    }

    fn on_session_died(&self, _session_id: &str) {}
}

/// HTTP and polling seam used by the Telegram adapter and deterministic tests.
pub trait TelegramApi: Send + Sync + 'static {
    fn get_me(&self) -> TelegramFuture<'_, TelegramMe>;
    fn set_commands(&self, commands: Vec<TelegramBotCommand>) -> TelegramFuture<'_, ()>;
    fn delete_webhook(&self, drop_pending_updates: bool) -> TelegramFuture<'_, ()>;
    fn get_updates<'a>(
        &'a self,
        offset: Option<i64>,
        timeout: u64,
    ) -> TelegramFuture<'a, Vec<TelegramUpdate>>;
    fn get_file<'a>(&'a self, file_id: &'a str) -> TelegramFuture<'a, TelegramFile>;
    fn download_file<'a>(&'a self, file_path: &'a str) -> TelegramFuture<'a, Vec<u8>>;
    fn download_file_bounded<'a>(
        &'a self,
        _file_path: &'a str,
        _max_bytes: usize,
    ) -> TelegramFuture<'a, Vec<u8>> {
        // Do not fall back to `download_file`: an implementation that buffers
        // the full response before checking its length defeats this method's
        // memory bound. Adapters must opt in to a bounded download.
        Box::pin(async {
            Err("Telegram API implementation does not support bounded file downloads".to_owned())
        })
    }
    fn send_message<'a>(
        &'a self,
        chat_id: &'a str,
        text: &'a str,
        options: TelegramSendOptions,
    ) -> TelegramFuture<'a, ()>;
    fn send_chat_action<'a>(&'a self, chat_id: &'a str, action: &'a str) -> TelegramFuture<'a, ()>;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TelegramSendOptions {
    pub parse_mode: Option<TelegramParseMode>,
    pub message_thread_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelegramParseMode {
    Html,
}

/// Reqwest implementation of Bot API calls and the file-download endpoint.
pub struct ReqwestTelegramApi {
    client: reqwest::Client,
    token: String,
    api_base: String,
    file_base: String,
}

impl ReqwestTelegramApi {
    pub fn new(token: impl Into<String>) -> Result<Self, String> {
        Self::with_config(token, None, TELEGRAM_API_BASE, TELEGRAM_FILE_BASE)
    }

    pub fn with_proxy(token: impl Into<String>, proxy: &str) -> Result<Self, String> {
        Self::with_config(token, Some(proxy), TELEGRAM_API_BASE, TELEGRAM_FILE_BASE)
    }

    /// Explicit endpoints support local Bot API proxies and transport tests.
    pub fn with_config(
        token: impl Into<String>,
        proxy: Option<&str>,
        api_base: impl Into<String>,
        file_base: impl Into<String>,
    ) -> Result<Self, String> {
        let mut builder = reqwest::Client::builder();
        if let Some(proxy) = proxy {
            let proxy = reqwest::Proxy::all(proxy).map_err(|error| error.to_string())?;
            builder = builder.proxy(proxy);
        }
        let client = builder.build().map_err(|error| error.to_string())?;
        Ok(Self {
            client,
            token: token.into(),
            api_base: api_base.into().trim_end_matches('/').to_owned(),
            file_base: file_base.into().trim_end_matches('/').to_owned(),
        })
    }

    fn api_url(&self, method: &str) -> String {
        format!("{}/bot{}/{}", self.api_base, self.token, method)
    }

    fn file_url(&self, file_path: &str) -> String {
        format!(
            "{}/bot{}/{}",
            self.file_base,
            self.token,
            file_path.trim_start_matches('/')
        )
    }

    async fn post_result<T: DeserializeOwned>(
        &self,
        method: &str,
        body: Value,
    ) -> Result<T, String> {
        let response = self
            .client
            .post(self.api_url(method))
            .json(&body)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status();
        let value: Value = response.json().await.map_err(|error| error.to_string())?;
        if !status.is_success() || value.get("ok").and_then(Value::as_bool) != Some(true) {
            let description = value
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("Telegram Bot API request failed");
            return Err(format!("HTTP {status}: {description}"));
        }
        serde_json::from_value(value.get("result").cloned().unwrap_or(Value::Null))
            .map_err(|error| error.to_string())
    }
}

impl TelegramApi for ReqwestTelegramApi {
    fn get_me(&self) -> TelegramFuture<'_, TelegramMe> {
        Box::pin(async move { self.post_result("getMe", json!({})).await })
    }

    fn set_commands(&self, commands: Vec<TelegramBotCommand>) -> TelegramFuture<'_, ()> {
        Box::pin(async move {
            self.post_result::<bool>("setMyCommands", json!({"commands": commands}))
                .await?;
            Ok(())
        })
    }

    fn delete_webhook(&self, drop_pending_updates: bool) -> TelegramFuture<'_, ()> {
        Box::pin(async move {
            self.post_result::<bool>(
                "deleteWebhook",
                json!({"drop_pending_updates": drop_pending_updates}),
            )
            .await?;
            Ok(())
        })
    }

    fn get_updates<'a>(
        &'a self,
        offset: Option<i64>,
        timeout: u64,
    ) -> TelegramFuture<'a, Vec<TelegramUpdate>> {
        Box::pin(async move {
            let mut request = json!({"timeout": timeout, "allowed_updates": ["message"]});
            if let Some(offset) = offset {
                request["offset"] = json!(offset);
            }
            let response = self
                .client
                .post(self.api_url("getUpdates"))
                .json(&request)
                .timeout(Duration::from_secs(timeout.saturating_add(5)))
                .send()
                .await
                .map_err(|error| error.to_string())?;
            let status = response.status();
            let value: Value = response.json().await.map_err(|error| error.to_string())?;
            if !status.is_success() || value.get("ok").and_then(Value::as_bool) != Some(true) {
                let description = value
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("Telegram long poll failed");
                return Err(format!("HTTP {status}: {description}"));
            }
            serde_json::from_value(value.get("result").cloned().unwrap_or(Value::Null))
                .map_err(|error| error.to_string())
        })
    }

    fn get_file<'a>(&'a self, file_id: &'a str) -> TelegramFuture<'a, TelegramFile> {
        Box::pin(async move {
            self.post_result("getFile", json!({"file_id": file_id}))
                .await
        })
    }

    fn download_file<'a>(&'a self, file_path: &'a str) -> TelegramFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let response = self
                .client
                .get(self.file_url(file_path))
                .send()
                .await
                .map_err(|error| error.to_string())?;
            if !response.status().is_success() {
                return Err(format!("HTTP {}", response.status()));
            }
            response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|error| error.to_string())
        })
    }

    fn download_file_bounded<'a>(
        &'a self,
        file_path: &'a str,
        max_bytes: usize,
    ) -> TelegramFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let mut response = self
                .client
                .get(self.file_url(file_path))
                .timeout(Duration::from_secs(40))
                .send()
                .await
                .map_err(|error| error.to_string())?;
            if !response.status().is_success() {
                return Err(format!("HTTP {}", response.status()));
            }
            if response
                .content_length()
                .is_some_and(|length| length > max_bytes as u64)
            {
                return Err(format!("Telegram file exceeds the {max_bytes}-byte limit"));
            }
            let capacity = response
                .content_length()
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or_default()
                .min(max_bytes);
            let mut bytes = Vec::with_capacity(capacity);
            while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
                if bytes.len().saturating_add(chunk.len()) > max_bytes {
                    return Err(format!("Telegram file exceeds the {max_bytes}-byte limit"));
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(bytes)
        })
    }

    fn send_message<'a>(
        &'a self,
        chat_id: &'a str,
        text: &'a str,
        options: TelegramSendOptions,
    ) -> TelegramFuture<'a, ()> {
        Box::pin(async move {
            let mut body = json!({"chat_id": chat_id, "text": text});
            if options.parse_mode == Some(TelegramParseMode::Html) {
                body["parse_mode"] = json!("HTML");
            }
            if let Some(thread_id) = options.message_thread_id {
                body["message_thread_id"] = json!(thread_id);
            }
            self.post_result::<Value>("sendMessage", body).await?;
            Ok(())
        })
    }

    fn send_chat_action<'a>(&'a self, chat_id: &'a str, action: &'a str) -> TelegramFuture<'a, ()> {
        Box::pin(async move {
            self.post_result::<bool>(
                "sendChatAction",
                json!({"chat_id": chat_id, "action": action}),
            )
            .await?;
            Ok(())
        })
    }
}

#[derive(Default)]
struct TelegramConnectionState {
    bot_id: i64,
    bot_username: String,
    polling_task: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct TypingState {
    active_sessions: HashMap<String, HashSet<String>>,
    intervals: HashMap<String, JoinHandle<()>>,
}

struct TelegramTyping {
    api: Arc<dyn TelegramApi>,
    state: Mutex<TypingState>,
}

impl TelegramTyping {
    fn new(api: Arc<dyn TelegramApi>) -> Self {
        Self {
            api,
            state: Mutex::new(TypingState::default()),
        }
    }

    fn start(self: &Arc<Self>, chat_id: &str, session_id: &str) {
        let mut state = lock_unpoisoned(&self.state);
        state
            .active_sessions
            .entry(chat_id.to_owned())
            .or_default()
            .insert(session_id.to_owned());
        if state.intervals.contains_key(chat_id) {
            return;
        }

        let api = self.api.clone();
        let chat_id = chat_id.to_owned();
        let interval_chat = chat_id.clone();
        let interval = tokio::spawn(async move {
            loop {
                tokio::time::sleep(TYPING_INTERVAL).await;
                let _ = api.send_chat_action(&interval_chat, "typing").await;
            }
        });
        state.intervals.insert(chat_id.clone(), interval);
        drop(state);
        let api = self.api.clone();
        tokio::spawn(async move {
            let _ = api.send_chat_action(&chat_id, "typing").await;
        });
    }

    fn stop(&self, chat_id: &str, session_id: &str) {
        let mut state = lock_unpoisoned(&self.state);
        if let Some(sessions) = state.active_sessions.get_mut(chat_id) {
            sessions.remove(session_id);
            if !sessions.is_empty() {
                return;
            }
        }
        state.active_sessions.remove(chat_id);
        if let Some(interval) = state.intervals.remove(chat_id) {
            interval.abort();
        }
    }

    fn stop_session(&self, session_id: &str) {
        let chats = lock_unpoisoned(&self.state)
            .active_sessions
            .iter()
            .filter(|(_, sessions)| sessions.contains(session_id))
            .map(|(chat_id, _)| chat_id.clone())
            .collect::<Vec<_>>();
        for chat_id in chats {
            self.stop(&chat_id, session_id);
        }
    }

    fn disconnect(&self) {
        let mut state = lock_unpoisoned(&self.state);
        state.active_sessions.clear();
        for interval in state.intervals.drain().map(|(_, task)| task) {
            interval.abort();
        }
    }
}

/// The Rust Telegram adapter. Create it in an `Arc` so its cancellable poll
/// task can hold a weak reference rather than keeping the channel alive.
pub struct TelegramChannel {
    name: String,
    api: Arc<dyn TelegramApi>,
    handler: Arc<dyn TelegramInboundHandler>,
    state: Mutex<TelegramConnectionState>,
    typing: Arc<TelegramTyping>,
}

impl TelegramChannel {
    pub fn new(
        name: impl Into<String>,
        token: impl Into<String>,
        handler: Arc<dyn TelegramInboundHandler>,
    ) -> Result<Self, String> {
        let api = Arc::new(ReqwestTelegramApi::new(token)?);
        Ok(Self::with_api(name, api, handler))
    }

    pub fn with_proxy(
        name: impl Into<String>,
        token: impl Into<String>,
        proxy: &str,
        handler: Arc<dyn TelegramInboundHandler>,
    ) -> Result<Self, String> {
        let api = Arc::new(ReqwestTelegramApi::with_proxy(token, proxy)?);
        Ok(Self::with_api(name, api, handler))
    }

    pub fn with_api(
        name: impl Into<String>,
        api: Arc<dyn TelegramApi>,
        handler: Arc<dyn TelegramInboundHandler>,
    ) -> Self {
        let typing = Arc::new(TelegramTyping::new(api.clone()));
        Self {
            name: name.into(),
            api,
            handler,
            state: Mutex::new(TelegramConnectionState::default()),
            typing,
        }
    }

    pub fn supports_proactive_send(&self) -> bool {
        true
    }

    pub fn supports_proactive_target(&self, target: &SessionTarget) -> bool {
        target.thread_id.as_deref().is_none_or(|thread_id| {
            !thread_id.is_empty() && thread_id.bytes().all(|byte| byte.is_ascii_digit())
        })
    }

    pub fn bot_identity(&self) -> (i64, String) {
        let state = lock_unpoisoned(&self.state);
        (state.bot_id, state.bot_username.clone())
    }

    /// Resolve the bot, register its menu, drop pending updates and start one
    /// long-poll task. Menu registration and polling launch remain best effort,
    /// matching grammY startup behavior.
    pub async fn connect(self: &Arc<Self>) -> Result<(), String> {
        if let Some(previous) = lock_unpoisoned(&self.state).polling_task.take() {
            previous.abort();
        }
        let me = self.api.get_me().await?;
        {
            let mut state = lock_unpoisoned(&self.state);
            state.bot_id = me.id;
            state.bot_username = me.username.unwrap_or_default();
        }
        if let Err(error) = self.api.set_commands(TELEGRAM_BOT_COMMANDS.to_vec()).await {
            eprintln!(
                "[Telegram:{}] Failed to register bot commands: {error}",
                self.name
            );
        }
        let weak = Arc::downgrade(self);
        let api = self.api.clone();
        let task = tokio::spawn(async move {
            poll_loop(weak, api).await;
        });
        lock_unpoisoned(&self.state).polling_task = Some(task);
        Ok(())
    }

    pub fn disconnect(&self) {
        if let Some(task) = lock_unpoisoned(&self.state).polling_task.take() {
            task.abort();
        }
        self.typing.disconnect();
    }

    pub fn on_prompt_start(&self, chat_id: &str, session_id: Option<&str>) {
        self.typing.start(chat_id, session_id.unwrap_or(chat_id));
    }

    pub fn on_prompt_end(&self, chat_id: &str, session_id: Option<&str>) {
        self.typing.stop(chat_id, session_id.unwrap_or(chat_id));
    }

    pub fn on_prompt_buffered(
        &self,
        chat_id: &str,
        session_id: &str,
        message_id: Option<&str>,
    ) -> Result<(), String> {
        self.handler
            .on_prompt_buffered(chat_id, session_id, message_id)
    }

    pub fn on_prompt_buffer_drained(
        &self,
        chat_id: &str,
        session_id: &str,
        message_ids: &[String],
    ) -> Result<(), String> {
        if message_ids.is_empty() {
            return Ok(());
        }
        self.handler
            .on_prompt_buffer_drained(chat_id, session_id, message_ids)
    }

    pub fn on_task_lifecycle(&self, event: &TelegramTaskLifecycleEvent) {
        if event.channel_name != self.name {
            return;
        }
        match event.kind {
            TelegramLifecycleKind::Started => self.typing.start(&event.chat_id, &event.session_id),
            TelegramLifecycleKind::Completed
            | TelegramLifecycleKind::Cancelled
            | TelegramLifecycleKind::Failed => self.typing.stop(&event.chat_id, &event.session_id),
            TelegramLifecycleKind::TextChunk | TelegramLifecycleKind::ToolCall => {}
        }
    }

    pub fn on_session_died(&self, session_id: &str) {
        self.typing.stop_session(session_id);
        self.handler.on_session_died(session_id);
    }

    pub fn build_envelope(
        &self,
        message: &TelegramMessage,
        text: &str,
        entities: &[TelegramEntity],
    ) -> Option<TelegramInboundEnvelope> {
        let sender = message.from.as_ref()?;
        let state = lock_unpoisoned(&self.state);
        let bot_username = state.bot_username.clone();
        let bot_id = state.bot_id;
        drop(state);
        let is_group = message.chat.kind == "group" || message.chat.kind == "supergroup";
        let is_mentioned = entities.iter().any(|entity| {
            if bot_username.is_empty() {
                return false;
            }
            let value = slice_utf16(text, entity.offset, entity.length);
            if entity.kind == "mention" {
                value.eq_ignore_ascii_case(&format!("@{bot_username}"))
            } else if entity.kind == "bot_command" {
                value
                    .find('@')
                    .is_some_and(|at| value[at + 1..].eq_ignore_ascii_case(&bot_username))
            } else {
                false
            }
        });
        let is_reply_to_bot = message
            .reply_to_message
            .as_ref()
            .and_then(|reply| reply.from.as_ref())
            .is_some_and(|from| from.id == bot_id);
        let clean_text = if is_mentioned && !bot_username.is_empty() {
            trim_ecmascript(&remove_all_bot_mentions(text, &bot_username)).to_owned()
        } else {
            text.to_owned()
        };
        Some(TelegramInboundEnvelope {
            channel_name: self.name.clone(),
            sender_id: sender.id.to_string(),
            sender_name: if let Some(last_name) =
                sender.last_name.as_deref().filter(|last| !last.is_empty())
            {
                format!("{} {last_name}", sender.first_name)
            } else {
                sender.first_name.clone()
            },
            chat_id: message.chat.id.to_string(),
            chat_name: (is_group).then(|| message.chat.title.clone()).flatten(),
            thread_id: message.message_thread_id.map(|id| id.to_string()),
            text: clean_text,
            is_group,
            is_mentioned,
            is_reply_to_bot,
            referenced_text: message
                .reply_to_message
                .as_ref()
                .and_then(|reply| reply.text.clone())
                .filter(|text| !text.is_empty()),
            ..TelegramInboundEnvelope::default()
        })
    }

    /// Process supported message payloads; media download failure still
    /// dispatches the source-compatible explanatory text to the session.
    pub async fn handle_update(self: &Arc<Self>, update: TelegramUpdate) -> Result<(), String> {
        let Some(message) = update.message else {
            return Ok(());
        };
        let Some(sender) = message.from.as_ref() else {
            return Ok(());
        };
        let mut envelope = if let Some(text) = message.text.as_deref() {
            self.build_envelope(&message, text, &message.entities)
        } else if !message.photo.is_empty() {
            self.build_envelope(
                &message,
                message
                    .caption
                    .as_deref()
                    .filter(|caption| !caption.is_empty())
                    .unwrap_or("(image)"),
                &message.caption_entities,
            )
        } else if let Some(document) = message.document.as_ref() {
            let file_name = document
                .file_name
                .clone()
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| format!("file_{}", unix_millis()));
            self.build_envelope(
                &message,
                message
                    .caption
                    .as_deref()
                    .filter(|caption| !caption.is_empty())
                    .unwrap_or(&format!("(file: {file_name})")),
                &message.caption_entities,
            )
        } else if message.voice.is_some() {
            self.build_envelope(
                &message,
                message
                    .caption
                    .as_deref()
                    .filter(|caption| !caption.is_empty())
                    .unwrap_or("(voice message)"),
                &message.caption_entities,
            )
        } else {
            None
        };
        let Some(mut envelope) = envelope.take() else {
            let _ = sender;
            return Ok(());
        };

        if !message.photo.is_empty() || message.document.is_some() || message.voice.is_some() {
            if !self.handler.preflight_inbound(&envelope).await? {
                return Ok(());
            }
            envelope.preflighted = true;
        }

        if let Some(photo) = message.photo.last() {
            match self
                .fetch_telegram_file_bounded(&photo.file_id, TELEGRAM_MAX_PHOTO_BYTES, None)
                .await
            {
                Ok(bytes) => {
                    envelope.image_base64 = Some(base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        bytes,
                    ))
                }
                Err(error) => {
                    eprintln!("[Telegram:{}] Failed to download photo: {error}", self.name)
                }
            }
            if envelope.image_base64.is_some() {
                envelope.image_mime_type = Some("image/jpeg".to_owned());
            }
        } else if let Some(document) = message.document.as_ref() {
            let file_name = document
                .file_name
                .clone()
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| format!("file_{}", unix_millis()));
            match self
                .fetch_telegram_file_bounded(
                    &document.file_id,
                    TELEGRAM_MAX_ATTACHMENT_BYTES,
                    document.file_size,
                )
                .await
                .and_then(|bytes| {
                    save_attachment(&file_name, &bytes).map_err(|error| error.to_string())
                }) {
                Ok(file_path) => {
                    envelope.text = message.caption.clone().unwrap_or_default();
                    envelope.attachments.push(TelegramAttachment {
                        kind: TelegramAttachmentKind::File,
                        file_path,
                        mime_type: document
                            .mime_type
                            .clone()
                            .unwrap_or_else(|| "application/octet-stream".to_owned()),
                        file_name,
                    });
                }
                Err(error) => {
                    eprintln!(
                        "[Telegram:{}] Failed to download document: {error}",
                        self.name
                    );
                    envelope.text = format!(
                        "{}\n\n(User sent a file \"{file_name}\" but download failed)",
                        message.caption.as_deref().unwrap_or_default()
                    );
                }
            }
        } else if let Some(voice) = message.voice.as_ref() {
            let file_name = format!("voice_{}.ogg", unix_millis());
            match self
                .fetch_telegram_file_bounded(
                    &voice.file_id,
                    TELEGRAM_MAX_ATTACHMENT_BYTES,
                    voice.file_size,
                )
                .await
                .and_then(|bytes| {
                    save_attachment(&file_name, &bytes).map_err(|error| error.to_string())
                }) {
                Ok(file_path) => {
                    envelope.text = message.caption.clone().unwrap_or_default();
                    envelope.attachments.push(TelegramAttachment {
                        kind: TelegramAttachmentKind::Audio,
                        file_path,
                        mime_type: voice
                            .mime_type
                            .clone()
                            .unwrap_or_else(|| "audio/ogg".to_owned()),
                        file_name,
                    });
                }
                Err(error) => {
                    eprintln!(
                        "[Telegram:{}] Failed to download voice message: {error}",
                        self.name
                    );
                    envelope.text = format!(
                        "{}\n\n(User sent a voice message but download failed)",
                        message.caption.as_deref().unwrap_or_default()
                    );
                }
            }
        }

        let channel = Arc::downgrade(self);
        tokio::spawn(async move {
            if let Some(channel) = channel.upgrade() {
                if let Err(error) = channel.handle_inbound(envelope.clone()).await {
                    eprintln!(
                        "[Telegram:{}] Error handling message: {error}",
                        channel.name
                    );
                    let _ = channel
                        .send_raw_message(
                            &envelope.chat_id,
                            TELEGRAM_ERROR_REPLY,
                            envelope.thread_id.as_deref(),
                        )
                        .await;
                }
            }
        });
        Ok(())
    }

    async fn fetch_telegram_file_bounded(
        &self,
        file_id: &str,
        max_bytes: usize,
        message_file_size: Option<u64>,
    ) -> Result<Vec<u8>, String> {
        if message_file_size.is_some_and(|size| size > max_bytes as u64) {
            return Err(format!("Telegram file exceeds the {max_bytes}-byte limit"));
        }
        let file = self.api.get_file(file_id).await?;
        if file.file_size.is_some_and(|size| size > max_bytes as u64) {
            return Err(format!("Telegram file exceeds the {max_bytes}-byte limit"));
        }
        let path = file
            .file_path
            .ok_or_else(|| "Telegram getFile response omitted file_path".to_owned())?;
        self.api.download_file_bounded(&path, max_bytes).await
    }

    pub async fn handle_inbound(&self, envelope: TelegramInboundEnvelope) -> Result<(), String> {
        let bot_username = lock_unpoisoned(&self.state).bot_username.clone();
        if is_start_command(&envelope.text, &bot_username) {
            return self
                .send_message(
                    &envelope.chat_id,
                    TELEGRAM_WELCOME,
                    Some(&TelegramSessionRoute {
                        thread_id: envelope.thread_id.clone(),
                    }),
                )
                .await;
        }
        self.handler.handle_inbound(envelope).await
    }

    pub async fn send_message(
        &self,
        chat_id: &str,
        text: &str,
        route: Option<&TelegramSessionRoute>,
    ) -> Result<(), String> {
        self.send_telegram_message(
            chat_id,
            text,
            route.and_then(|route| route.thread_id.as_deref()),
        )
        .await
    }

    /// Route a model response to the current inbound topic when supplied;
    /// otherwise use a matching session-router target's forum topic.
    pub async fn send_response(
        &self,
        chat_id: &str,
        text: &str,
        inbound_route: Option<&TelegramSessionRoute>,
        routed_target: Option<&SessionTarget>,
    ) -> Result<(), String> {
        let thread_id = if let Some(route) = inbound_route {
            route.thread_id.as_deref()
        } else {
            routed_target
                .filter(|target| target.channel_name == self.name && target.chat_id == chat_id)
                .and_then(|target| target.thread_id.as_deref())
        };
        self.send_telegram_message(chat_id, text, thread_id).await
    }

    pub async fn push_proactive(&self, target: &SessionTarget, text: &str) -> Result<(), String> {
        if !self.supports_proactive_target(target) {
            return Err("Telegram forum topic id must contain only decimal digits".to_owned());
        }
        self.send_telegram_message(&target.chat_id, text, target.thread_id.as_deref())
            .await
    }

    async fn send_telegram_message(
        &self,
        chat_id: &str,
        text: &str,
        thread_id: Option<&str>,
    ) -> Result<(), String> {
        let chunks = split_html_for_telegram(&telegram_format(text));
        let thread_number = thread_id
            .map(|value| {
                value
                    .parse::<i64>()
                    .map_err(|_| format!("invalid Telegram thread id: {value}"))
            })
            .transpose()?;
        for chunk in chunks {
            let html_options = TelegramSendOptions {
                parse_mode: Some(TelegramParseMode::Html),
                message_thread_id: thread_number,
            };
            if self
                .api
                .send_message(chat_id, &chunk, html_options)
                .await
                .is_err()
            {
                let fallback = strip_html_tags(&chunk);
                let text_options = TelegramSendOptions {
                    parse_mode: None,
                    message_thread_id: thread_number,
                };
                self.api
                    .send_message(chat_id, &fallback, text_options)
                    .await?;
            }
        }
        Ok(())
    }

    async fn send_raw_message(
        &self,
        chat_id: &str,
        text: &str,
        thread_id: Option<&str>,
    ) -> Result<(), String> {
        let thread_id = thread_id
            .map(|value| {
                value
                    .parse::<i64>()
                    .map_err(|_| format!("invalid Telegram thread id: {value}"))
            })
            .transpose()?;
        self.api
            .send_message(
                chat_id,
                text,
                TelegramSendOptions {
                    parse_mode: None,
                    message_thread_id: thread_id,
                },
            )
            .await
    }
}

impl Drop for TelegramChannel {
    fn drop(&mut self) {
        if let Some(task) = lock_unpoisoned(&self.state).polling_task.take() {
            task.abort();
        }
        self.typing.disconnect();
    }
}

async fn poll_loop(channel: Weak<TelegramChannel>, api: Arc<dyn TelegramApi>) {
    if let Err(error) = api.delete_webhook(true).await {
        if let Some(channel) = channel.upgrade() {
            eprintln!("[Telegram:{}] Bot launch error: {error}", channel.name);
        }
    }
    let mut offset = None;
    let mut backoff = Duration::from_secs(1);
    loop {
        if channel.strong_count() == 0 {
            return;
        }
        match api.get_updates(offset, LONG_POLL_SECONDS).await {
            Ok(updates) => {
                backoff = Duration::from_secs(1);
                for update in updates {
                    offset = Some(update.update_id.saturating_add(1));
                    if let Some(adapter) = channel.upgrade() {
                        let _ = adapter.handle_update(update).await;
                    } else {
                        return;
                    }
                }
            }
            Err(error) => {
                if let Some(channel) = channel.upgrade() {
                    eprintln!("[Telegram:{}] Long-poll error: {error}", channel.name);
                }
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
            }
        }
    }
}

fn lock_unpoisoned<T>(lock: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn slice_utf16(text: &str, offset: usize, length: usize) -> String {
    let units = text
        .encode_utf16()
        .skip(offset)
        .take(length)
        .collect::<Vec<_>>();
    char::decode_utf16(units)
        .map(|character| character.unwrap_or(char::REPLACEMENT_CHARACTER))
        .collect()
}

fn remove_all_bot_mentions(text: &str, username: &str) -> String {
    let needle = format!("@{username}");
    let mut output = String::with_capacity(text.len());
    let mut rest = text;
    while !rest.is_empty() {
        if rest.len() >= needle.len()
            && rest
                .get(..needle.len())
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(&needle))
        {
            rest = &rest[needle.len()..];
        } else {
            let Some(character) = rest.chars().next() else {
                break;
            };
            output.push(character);
            rest = &rest[character.len_utf8()..];
        }
    }
    output
}

fn is_start_command(text: &str, bot_username: &str) -> bool {
    let command = text.split_whitespace().next().unwrap_or_default();
    let mut parts = command.split('@');
    if parts.next() != Some("/start") {
        return false;
    }
    match parts.next() {
        None => true,
        Some(username) => {
            !username.is_empty()
                && !bot_username.is_empty()
                && username.eq_ignore_ascii_case(bot_username)
                && parts.next().is_none()
        }
    }
}

fn save_attachment(file_name: &str, bytes: &[u8]) -> std::io::Result<String> {
    let root = std::env::temp_dir()
        .join("channel-files")
        .join(Uuid::new_v4().to_string());
    std::fs::create_dir_all(&root)?;
    let safe_name = Path::new(file_name)
        .file_name()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| std::ffi::OsStr::new("file"));
    let path: PathBuf = root.join(safe_name);
    std::fs::write(&path, bytes)?;
    Ok(path.to_string_lossy().into_owned())
}

fn unix_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(character, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}

fn trim_ecmascript(text: &str) -> &str {
    text.trim_matches(is_ecmascript_whitespace)
}

/// Markdown-to-Telegram-HTML conversion compatible with
/// telegram-markdown-formatter 0.1.2 for the formatter's documented syntax.
pub fn telegram_format(input: &str) -> String {
    let mut text = combine_blockquotes(input);
    let (updated, code_blocks) = extract_code_blocks(&text);
    text = updated;
    let (updated, inline_code) = extract_inline_code(&text);
    text = escape_html(&updated);
    text = map_markdown_lines(&text);
    text = replace_delimited(&text, "***", "<b><i>", "</i></b>", false);
    text = replace_delimited(&text, "___", "<u><i>", "</i></u>", false);
    text = replace_delimited(&text, "**", "<b>", "</b>", false);
    text = replace_delimited(&text, "__", "<u>", "</u>", false);
    text = replace_delimited(&text, "~~", "<s>", "</s>", false);
    text = replace_delimited(&text, "||", "<span class=\"tg-spoiler\">", "</span>", false);
    text = replace_delimited(&text, "*", "<i>", "</i>", true);
    text = replace_delimited(&text, "_", "<i>", "</i>", false);
    text = remove_storage_placeholders(&text);
    text = convert_markdown_links(&text);
    for (placeholder, snippet) in inline_code {
        text = text.replace(
            &placeholder,
            &format!("<code>{}</code>", escape_html(&snippet)),
        );
    }
    for (placeholder, html) in code_blocks {
        text = text.replace(&placeholder, &html);
    }
    text = text
        .replace("&lt;blockquote&gt;", "<blockquote>")
        .replace("&lt;blockquote expandable&gt;", "<blockquote expandable>")
        .replace("&lt;/blockquote&gt;", "</blockquote>")
        .replace(
            "&lt;span class=\"tg-spoiler\"&gt;",
            "<span class=\"tg-spoiler\">",
        );
    let text = collapse_blank_lines(&text);
    trim_ecmascript(&text).to_owned()
}

fn combine_blockquotes(input: &str) -> String {
    let mut output = Vec::new();
    let mut buffered = Vec::<String>::new();
    let mut in_quote = false;
    let mut expandable = false;
    for line in input.split('\n') {
        if let Some(quoted) = line.strip_prefix("**>") {
            in_quote = true;
            expandable = true;
            buffered.push(trim_ecmascript(quoted).to_owned());
        } else if let Some(quoted) = line.strip_prefix('>') {
            if !in_quote {
                in_quote = true;
                expandable = false;
            }
            buffered.push(trim_ecmascript(quoted).to_owned());
        } else {
            if in_quote {
                let tag = if expandable {
                    "<blockquote expandable>"
                } else {
                    "<blockquote>"
                };
                output.push(format!("{tag}{}</blockquote>", buffered.join("\n")));
                buffered.clear();
                in_quote = false;
                expandable = false;
            }
            output.push(line.to_owned());
        }
    }
    if in_quote {
        let tag = if expandable {
            "<blockquote expandable>"
        } else {
            "<blockquote>"
        };
        output.push(format!("{tag}{}</blockquote>", buffered.join("\n")));
    }
    output.join("\n")
}

fn extract_code_blocks(input: &str) -> (String, Vec<(String, String)>) {
    let marker = char::from(0x60);
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut blocks = Vec::new();
    let mut cursor = 0;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != marker as u8 {
            index += 1;
            continue;
        }
        let start = index;
        while index < bytes.len() && bytes[index] == marker as u8 {
            index += 1;
        }
        let fence_len = index - start;
        if fence_len < 3 {
            continue;
        }
        let lang_start = index;
        while index < bytes.len() && bytes[index].is_ascii_alphanumeric()
            || index < bytes.len() && bytes[index] == b'_'
        {
            index += 1;
        }
        let language = &input[lang_start..index];
        if index < bytes.len() && bytes[index] == b'\r' && bytes.get(index + 1) == Some(&b'\n') {
            index += 2;
        } else if index < bytes.len() && bytes[index] == b'\n' {
            index += 1;
        }
        let content_start = index;
        let mut close_start = None;
        while index < bytes.len() {
            if bytes[index] != marker as u8 {
                index += 1;
                continue;
            }
            let candidate_start = index;
            while index < bytes.len() && bytes[index] == marker as u8 {
                index += 1;
            }
            if index - candidate_start == fence_len {
                close_start = Some(candidate_start);
                break;
            }
        }
        output.push_str(&input[cursor..start]);
        let content_end = close_start
            .map(|close| {
                if close > content_start && bytes[close - 1] == b'\n' {
                    if close > content_start + 1 && bytes[close - 2] == b'\r' {
                        close - 2
                    } else {
                        close - 1
                    }
                } else {
                    close
                }
            })
            .unwrap_or(input.len());
        let newline_suffix = close_start
            .map(|close| &input[content_end..close])
            .unwrap_or("");
        let content = if close_start.is_none() {
            format!("{}\n", &input[content_start..content_end])
        } else {
            format!("{}{}", &input[content_start..content_end], newline_suffix)
        };
        let class = if language.is_empty() {
            String::new()
        } else {
            format!(" class=\"language-{language}\"")
        };
        let placeholder = format!("CODEBLOCKPLACEHOLDER{}", blocks.len());
        blocks.push((
            placeholder.clone(),
            format!("<pre><code{class}>{}</code></pre>", escape_html(&content)),
        ));
        output.push_str(&placeholder);
        if let Some(close) = close_start {
            index = close + fence_len;
            cursor = index;
        } else {
            cursor = input.len();
            index = input.len();
        }
    }
    output.push_str(&input[cursor..]);
    (output, blocks)
}

fn extract_inline_code(input: &str) -> (String, Vec<(String, String)>) {
    let marker = char::from(0x60);
    let mut output = String::with_capacity(input.len());
    let mut snippets = Vec::new();
    let mut cursor = 0;
    let mut search = 0;
    while let Some(relative) = input[search..].find(marker) {
        let open = search + relative;
        let Some(close_relative) = input[open + marker.len_utf8()..].find(marker) else {
            output.push_str(&input[cursor..open]);
            let placeholder = format!("INLINECODEPLACEHOLDER{}", snippets.len());
            snippets.push((
                placeholder.clone(),
                input[open + marker.len_utf8()..].to_owned(),
            ));
            output.push_str(&placeholder);
            cursor = input.len();
            break;
        };
        let close = open + marker.len_utf8() + close_relative;
        if close == open + marker.len_utf8() {
            search = close + 1;
            continue;
        }
        output.push_str(&input[cursor..open]);
        let placeholder = format!("INLINECODEPLACEHOLDER{}", snippets.len());
        snippets.push((placeholder.clone(), input[open + 1..close].to_owned()));
        output.push_str(&placeholder);
        cursor = close + 1;
        search = cursor;
    }
    if cursor == 0 {
        output.push_str(input);
    } else {
        output.push_str(&input[cursor..]);
    }
    (output, snippets)
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn map_markdown_lines(input: &str) -> String {
    input
        .split_inclusive('\n')
        .map(|part| {
            let (line, ending) = part
                .strip_suffix('\n')
                .map(|line| (line, "\n"))
                .unwrap_or((part, ""));
            let heading = line
                .chars()
                .take_while(|character| *character == '#')
                .count();
            if (1..=6).contains(&heading)
                && line[heading..]
                    .chars()
                    .next()
                    .is_some_and(char::is_whitespace)
            {
                return format!(
                    "<b>{}</b>{ending}",
                    line[heading..].trim_start_matches(char::is_whitespace)
                );
            }
            let leading = line.len() - line.trim_start_matches([' ', '\t']).len();
            let content = &line[leading..];
            if content.starts_with("- ") || content.starts_with("* ") {
                format!("{}• {}{ending}", &line[..leading], &content[2..])
            } else {
                part.to_owned()
            }
        })
        .collect()
}

fn replace_delimited(
    input: &str,
    delimiter: &str,
    open_tag: &str,
    close_tag: &str,
    italic_star_rules: bool,
) -> String {
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    let mut search = 0;
    while let Some(relative) = input[search..].find(delimiter) {
        let open = search + relative;
        if open < cursor {
            search = cursor;
            continue;
        }
        let prior_word = input[..open].chars().next_back().is_some_and(|character| {
            if italic_star_rules {
                character.is_ascii_alphanumeric()
            } else {
                is_ascii_word(character)
            }
        });
        let open_end = open + delimiter.len();
        let first_inside = input[open_end..].chars().next();
        if prior_word
            || first_inside.is_none()
            || (italic_star_rules && first_inside.is_some_and(char::is_whitespace))
        {
            search = open_end;
            continue;
        }
        let mut close_search = open_end;
        let mut closing = None;
        while let Some(relative_close) = input[close_search..].find(delimiter) {
            let close = close_search + relative_close;
            let after = close + delimiter.len();
            let last_inside = input[open_end..close].chars().next_back();
            let after_word = input[after..].chars().next().is_some_and(|character| {
                if italic_star_rules {
                    character.is_ascii_alphanumeric()
                } else {
                    is_ascii_word(character)
                }
            });
            let valid_star =
                !italic_star_rules || (!last_inside.is_none_or(char::is_whitespace) && !after_word);
            if valid_star {
                closing = Some(close);
                break;
            }
            close_search = close + delimiter.len();
        }
        let Some(close) = closing else {
            search = open_end;
            continue;
        };
        output.push_str(&input[cursor..open]);
        output.push_str(open_tag);
        output.push_str(&input[open_end..close]);
        output.push_str(close_tag);
        cursor = close + delimiter.len();
        search = cursor;
    }
    output.push_str(&input[cursor..]);
    output
}

fn is_ascii_word(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

fn remove_storage_placeholders(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    while let Some(start_relative) = input[cursor..].find('【') {
        let start = cursor + start_relative;
        let Some(end_relative) = input[start + '【'.len_utf8()..].find('】') else {
            break;
        };
        let end = start + '【'.len_utf8() + end_relative + '】'.len_utf8();
        output.push_str(&input[cursor..start]);
        cursor = end;
    }
    output.push_str(&input[cursor..]);
    output
}

fn convert_markdown_links(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    let bytes = input.as_bytes();
    let mut index = 0;
    while index + 1 < bytes.len() {
        if bytes[index] == b']' && bytes[index + 1] == b'(' {
            if let Some(open) = input[..index].rfind('[') {
                if let Some(close_relative) = input[index + 2..].find(')') {
                    let close = index + 2 + close_relative;
                    output.push_str(&input[cursor..open]);
                    output.push_str("<a href=\"");
                    output.push_str(&input[index + 2..close]);
                    output.push_str("\">");
                    output.push_str(&input[open + 1..index]);
                    output.push_str("</a>");
                    cursor = close + 1;
                    index = cursor;
                    continue;
                }
            }
        }
        index += 1;
    }
    output.push_str(&input[cursor..]);
    output
}

fn collapse_blank_lines(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut newlines = 0;
    for character in input.chars() {
        if character == '\n' {
            newlines += 1;
            if newlines <= 2 {
                output.push(character);
            }
        } else {
            newlines = 0;
            output.push(character);
        }
    }
    output
}

/// Split formatted HTML into valid Telegram-sized chunks, carrying open tags
/// across chunk boundaries. Length accounting follows JavaScript UTF-16 units.
pub fn split_html_for_telegram(html: &str) -> Vec<String> {
    const LIMIT: usize = 4096;
    if html.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::<String>::new();
    let mut body = String::new();
    let mut active = Vec::<HtmlTag>::new();
    let mut chunk_start = Vec::<HtmlTag>::new();
    let mut index = 0;
    while index < html.len() {
        let token_end = if html.as_bytes()[index] == b'<' {
            html[index..]
                .find('>')
                .map(|relative| index + relative + 1)
                .unwrap_or_else(|| next_char_boundary(html, index))
        } else {
            next_char_boundary(html, index)
        };
        let token = &html[index..token_end];
        let next_active = apply_html_tag(&active, token);
        let prospective_len = html_tag_open_len(&chunk_start)
            + utf16_len(&body)
            + utf16_len(token)
            + html_tag_close_len(&next_active);
        if prospective_len > LIMIT && !body.is_empty() {
            let mut chunk = html_tags_open(&chunk_start);
            chunk.push_str(&body);
            chunk.push_str(&html_tags_close(&active));
            chunks.push(chunk);
            chunk_start = active.clone();
            body.clear();
        }
        body.push_str(token);
        active = next_active;
        index = token_end;
    }
    if !trim_ecmascript(&body).is_empty() {
        let mut chunk = html_tags_open(&chunk_start);
        chunk.push_str(&body);
        chunk.push_str(&html_tags_close(&active));
        chunks.push(chunk);
    }

    let mut merged = Vec::new();
    let mut buffered = String::new();
    for chunk in chunks {
        if utf16_len(&buffered) + utf16_len(&chunk) <= LIMIT {
            buffered.push_str(&chunk);
        } else {
            if !buffered.is_empty() {
                merged.push(std::mem::take(&mut buffered));
            }
            buffered = chunk;
        }
    }
    if !buffered.is_empty() {
        merged.push(buffered);
    }
    merged
}

#[derive(Clone)]
struct HtmlTag {
    name: String,
    attributes: String,
}

fn apply_html_tag(active: &[HtmlTag], token: &str) -> Vec<HtmlTag> {
    let Some(inner) = token
        .strip_prefix('<')
        .and_then(|tag| tag.strip_suffix('>'))
    else {
        return active.to_vec();
    };
    let closing = inner.starts_with('/');
    let inner = inner.strip_prefix('/').unwrap_or(inner);
    let name_end = inner
        .find(|character: char| !character.is_ascii_alphanumeric())
        .unwrap_or(inner.len());
    let name = &inner[..name_end];
    if !matches!(
        name,
        "b" | "i" | "u" | "s" | "code" | "pre" | "a" | "span" | "blockquote"
    ) {
        return active.to_vec();
    }
    let mut updated = active.to_vec();
    if closing {
        if let Some(position) = updated.iter().rposition(|tag| tag.name == name) {
            updated.remove(position);
        }
    } else {
        let attributes = normalize_html_attributes(&inner[name_end..]);
        updated.push(HtmlTag {
            name: name.to_owned(),
            attributes,
        });
    }
    updated
}

fn normalize_html_attributes(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut index = 0;
    let mut attributes = Vec::new();
    while index < bytes.len() {
        while index < bytes.len() && !(bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
        {
            index += 1;
        }
        let start = index;
        while index < bytes.len() && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
        {
            index += 1;
        }
        if start == index {
            break;
        }
        let key = &raw[start..index];
        if bytes.get(index) != Some(&b'=') {
            continue;
        }
        index += 1;
        if index >= bytes.len() || (bytes[index] != b'\'' && bytes[index] != b'"') {
            continue;
        }
        let quote = bytes[index];
        index += 1;
        let value_start = index;
        while index < bytes.len() && bytes[index] != quote {
            index += 1;
        }
        attributes.push(format!("{key}=\"{}\"", &raw[value_start..index]));
        if index < bytes.len() {
            index += 1;
        }
    }
    if attributes.is_empty() {
        String::new()
    } else {
        format!(" {}", attributes.join(" "))
    }
}

fn html_tags_open(tags: &[HtmlTag]) -> String {
    tags.iter()
        .map(|tag| format!("<{}{}>", tag.name, tag.attributes))
        .collect()
}
fn html_tags_close(tags: &[HtmlTag]) -> String {
    tags.iter()
        .rev()
        .map(|tag| format!("</{}>", tag.name))
        .collect()
}
fn html_tag_open_len(tags: &[HtmlTag]) -> usize {
    utf16_len(&html_tags_open(tags))
}
fn html_tag_close_len(tags: &[HtmlTag]) -> usize {
    utf16_len(&html_tags_close(tags))
}
fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}
fn next_char_boundary(text: &str, index: usize) -> usize {
    index
        + text[index..]
            .chars()
            .next()
            .map(char::len_utf8)
            .unwrap_or(0)
}

/// The HTML-to-plain fallback used only for a chunk rejected in HTML mode.
pub fn strip_html_tags(html: &str) -> String {
    let mut output = String::with_capacity(html.len());
    let mut inside_tag = false;
    for character in html.chars() {
        match character {
            '<' => inside_tag = true,
            '>' if inside_tag => inside_tag = false,
            _ if !inside_tag => output.push(character),
            _ => {}
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::mpsc;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Call {
        GetMe,
        Menu(Vec<TelegramBotCommand>),
        DeleteWebhook(bool),
        Poll(Option<i64>, u64),
        GetFile(String),
        Download(String),
        Send(String, String, TelegramSendOptions),
        Action(String, String),
    }

    #[derive(Default)]
    struct MockState {
        calls: Vec<Call>,
        files: HashMap<String, Vec<u8>>,
        failed_downloads: HashSet<String>,
        fail_html_containing: HashSet<String>,
        fail_menu: bool,
        fail_actions: bool,
    }

    #[derive(Default)]
    struct MockApi {
        state: Mutex<MockState>,
    }

    impl MockApi {
        fn calls(&self) -> Vec<Call> {
            lock_unpoisoned(&self.state).calls.clone()
        }

        fn add_file(&self, file_id: &str, bytes: &[u8]) {
            lock_unpoisoned(&self.state)
                .files
                .insert(file_id.into(), bytes.to_vec());
        }

        fn fail_download(&self, file_id: &str) {
            lock_unpoisoned(&self.state)
                .failed_downloads
                .insert(file_id.into());
        }
    }

    impl TelegramApi for MockApi {
        fn get_me(&self) -> TelegramFuture<'_, TelegramMe> {
            lock_unpoisoned(&self.state).calls.push(Call::GetMe);
            Box::pin(async {
                Ok(TelegramMe {
                    id: 123,
                    username: Some("qwen_bot".into()),
                })
            })
        }

        fn set_commands(&self, commands: Vec<TelegramBotCommand>) -> TelegramFuture<'_, ()> {
            let mut state = lock_unpoisoned(&self.state);
            state.calls.push(Call::Menu(commands));
            let fail = state.fail_menu;
            Box::pin(async move {
                if fail {
                    Err("menu failure".into())
                } else {
                    Ok(())
                }
            })
        }

        fn delete_webhook(&self, drop_pending_updates: bool) -> TelegramFuture<'_, ()> {
            lock_unpoisoned(&self.state)
                .calls
                .push(Call::DeleteWebhook(drop_pending_updates));
            Box::pin(async { Ok(()) })
        }

        fn get_updates<'a>(
            &'a self,
            offset: Option<i64>,
            timeout: u64,
        ) -> TelegramFuture<'a, Vec<TelegramUpdate>> {
            lock_unpoisoned(&self.state)
                .calls
                .push(Call::Poll(offset, timeout));
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(Vec::new())
            })
        }

        fn get_file<'a>(&'a self, file_id: &'a str) -> TelegramFuture<'a, TelegramFile> {
            lock_unpoisoned(&self.state)
                .calls
                .push(Call::GetFile(file_id.into()));
            Box::pin(async move {
                Ok(TelegramFile {
                    file_path: Some(file_id.into()),
                    file_size: None,
                })
            })
        }

        fn download_file<'a>(&'a self, file_path: &'a str) -> TelegramFuture<'a, Vec<u8>> {
            let mut state = lock_unpoisoned(&self.state);
            state.calls.push(Call::Download(file_path.into()));
            let fail = state.failed_downloads.contains(file_path);
            let bytes = state
                .files
                .get(file_path)
                .cloned()
                .unwrap_or_else(|| b"file-bytes".to_vec());
            Box::pin(async move {
                if fail {
                    Err("download failed".into())
                } else {
                    Ok(bytes)
                }
            })
        }

        fn download_file_bounded<'a>(
            &'a self,
            file_path: &'a str,
            max_bytes: usize,
        ) -> TelegramFuture<'a, Vec<u8>> {
            let mut state = lock_unpoisoned(&self.state);
            state.calls.push(Call::Download(file_path.into()));
            let fail = state.failed_downloads.contains(file_path);
            let Some(stored_bytes) = state.files.get(file_path) else {
                return Box::pin(async move {
                    if fail {
                        Err("download failed".into())
                    } else if b"file-bytes".len() > max_bytes {
                        Err(format!("Telegram file exceeds the {max_bytes}-byte limit"))
                    } else {
                        Ok(b"file-bytes".to_vec())
                    }
                });
            };
            if stored_bytes.len() > max_bytes {
                return Box::pin(async move {
                    Err(format!("Telegram file exceeds the {max_bytes}-byte limit"))
                });
            }
            let bytes = stored_bytes.clone();
            Box::pin(async move {
                if fail {
                    Err("download failed".into())
                } else {
                    Ok(bytes)
                }
            })
        }

        fn send_message<'a>(
            &'a self,
            chat_id: &'a str,
            text: &'a str,
            options: TelegramSendOptions,
        ) -> TelegramFuture<'a, ()> {
            let mut state = lock_unpoisoned(&self.state);
            state
                .calls
                .push(Call::Send(chat_id.into(), text.into(), options));
            let reject = options.parse_mode == Some(TelegramParseMode::Html)
                && state
                    .fail_html_containing
                    .iter()
                    .any(|needle| text.contains(needle));
            Box::pin(async move {
                if reject {
                    Err("rejected HTML".into())
                } else {
                    Ok(())
                }
            })
        }

        fn send_chat_action<'a>(
            &'a self,
            chat_id: &'a str,
            action: &'a str,
        ) -> TelegramFuture<'a, ()> {
            let mut state = lock_unpoisoned(&self.state);
            state
                .calls
                .push(Call::Action(chat_id.into(), action.into()));
            let fail = state.fail_actions;
            Box::pin(async move {
                if fail {
                    Err("action failed".into())
                } else {
                    Ok(())
                }
            })
        }
    }

    struct MockHandler {
        sender: mpsc::UnboundedSender<TelegramInboundEnvelope>,
        fail: AtomicBool,
    }

    impl TelegramInboundHandler for MockHandler {
        fn handle_inbound<'a>(
            &'a self,
            envelope: TelegramInboundEnvelope,
        ) -> TelegramInboundFuture<'a> {
            let sender = self.sender.clone();
            let fail = self.fail.load(Ordering::Relaxed);
            Box::pin(async move {
                sender.send(envelope).map_err(|error| error.to_string())?;
                if fail {
                    Err("handler failed".into())
                } else {
                    Ok(())
                }
            })
        }
    }

    fn make_channel(
        api: Arc<MockApi>,
    ) -> (
        Arc<TelegramChannel>,
        mpsc::UnboundedReceiver<TelegramInboundEnvelope>,
    ) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let handler = Arc::new(MockHandler {
            sender,
            fail: AtomicBool::new(false),
        });
        (
            Arc::new(TelegramChannel::with_api("telegram", api, handler)),
            receiver,
        )
    }

    fn message() -> TelegramMessage {
        TelegramMessage {
            from: Some(TelegramUser {
                id: 7,
                first_name: "Ada".into(),
                last_name: Some("Lovelace".into()),
                username: None,
            }),
            chat: TelegramChat {
                id: -42,
                kind: "supergroup".into(),
                title: Some("Project".into()),
            },
            message_thread_id: Some(8),
            reply_to_message: None,
            text: None,
            entities: Vec::new(),
            photo: Vec::new(),
            document: None,
            voice: None,
            caption: None,
            caption_entities: Vec::new(),
        }
    }

    fn update(message: TelegramMessage) -> TelegramUpdate {
        TelegramUpdate {
            update_id: 9,
            message: Some(message),
        }
    }

    async fn recv(
        receiver: &mut mpsc::UnboundedReceiver<TelegramInboundEnvelope>,
    ) -> TelegramInboundEnvelope {
        tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn connect_registers_menu_before_drop_pending_long_poll() {
        let api = Arc::new(MockApi::default());
        let (channel, _) = make_channel(api.clone());
        channel.connect().await.unwrap();
        tokio::task::yield_now().await;
        channel.disconnect();
        let calls = api.calls();
        assert_eq!(calls[0], Call::GetMe);
        assert_eq!(calls[1], Call::Menu(TELEGRAM_BOT_COMMANDS.to_vec()));
        assert!(calls.contains(&Call::DeleteWebhook(true)));
        assert!(calls.contains(&Call::Poll(None, LONG_POLL_SECONDS)));
        assert_eq!(channel.bot_identity(), (123, "qwen_bot".into()));
    }

    #[test]
    fn envelope_preserves_topic_reply_and_utf16_command_mention() {
        let (channel, _) = make_channel(Arc::new(MockApi::default()));
        {
            let mut state = lock_unpoisoned(&channel.state);
            state.bot_id = 123;
            state.bot_username = "qwen_bot".into();
        }
        let mut incoming = message();
        incoming.reply_to_message = Some(TelegramReply {
            from: Some(TelegramUser {
                id: 123,
                first_name: "Bot".into(),
                last_name: None,
                username: None,
            }),
            text: Some("quoted text".into()),
        });
        let text = "😀 /cancel@QWEN_BOT";
        let envelope = channel
            .build_envelope(
                &incoming,
                text,
                &[TelegramEntity {
                    kind: "bot_command".into(),
                    offset: 3,
                    length: 16,
                }],
            )
            .unwrap();
        assert_eq!(envelope.sender_name, "Ada Lovelace");
        assert_eq!(envelope.chat_name.as_deref(), Some("Project"));
        assert_eq!(envelope.thread_id.as_deref(), Some("8"));
        assert!(envelope.is_group && envelope.is_mentioned && envelope.is_reply_to_bot);
        assert_eq!(envelope.referenced_text.as_deref(), Some("quoted text"));
        assert_eq!(envelope.text, "😀 /cancel");
        assert!(envelope.to_gate_envelope().is_mentioned);
        assert_eq!(
            envelope.to_prompt_input().referenced_text.as_deref(),
            Some("quoted text")
        );
    }

    #[test]
    fn only_commands_addressed_to_this_bot_count_as_mentions() {
        let (channel, _) = make_channel(Arc::new(MockApi::default()));
        lock_unpoisoned(&channel.state).bot_username = "qwen_bot".into();
        let base = message();
        let direct = channel
            .build_envelope(
                &base,
                "/cancel",
                &[TelegramEntity {
                    kind: "bot_command".into(),
                    offset: 0,
                    length: 7,
                }],
            )
            .unwrap();
        let addressed = channel
            .build_envelope(
                &base,
                "/cancel@qwen_bot",
                &[TelegramEntity {
                    kind: "bot_command".into(),
                    offset: 0,
                    length: 16,
                }],
            )
            .unwrap();
        let other = channel
            .build_envelope(
                &base,
                "/cancel@other_bot",
                &[TelegramEntity {
                    kind: "bot_command".into(),
                    offset: 0,
                    length: 17,
                }],
            )
            .unwrap();
        assert!(!direct.is_mentioned && addressed.is_mentioned && !other.is_mentioned);
    }

    #[tokio::test]
    async fn photos_download_the_largest_size_and_preserve_caption() {
        let api = Arc::new(MockApi::default());
        api.add_file("largest", b"largest image");
        let (channel, mut receiver) = make_channel(api.clone());
        let mut incoming = message();
        incoming.caption = Some("look".into());
        incoming.photo = vec![
            TelegramPhotoSize {
                file_id: "small".into(),
            },
            TelegramPhotoSize {
                file_id: "largest".into(),
            },
        ];
        channel.handle_update(update(incoming)).await.unwrap();
        let envelope = recv(&mut receiver).await;
        assert_eq!(envelope.text, "look");
        assert_eq!(
            envelope.image_base64.as_deref(),
            Some("bGFyZ2VzdCBpbWFnZQ==")
        );
        assert_eq!(envelope.image_mime_type.as_deref(), Some("image/jpeg"));
        assert!(api.calls().contains(&Call::GetFile("largest".into())));
        assert!(!api.calls().contains(&Call::GetFile("small".into())));
    }

    #[tokio::test]
    async fn documents_and_voice_become_prompt_file_attachments() {
        let api = Arc::new(MockApi::default());
        api.add_file("document", b"document body");
        api.add_file("voice", b"voice body");
        let (channel, mut receiver) = make_channel(api);
        let mut document = message();
        document.document = Some(TelegramDocument {
            file_id: "document".into(),
            file_name: Some("report.txt".into()),
            mime_type: Some("text/plain".into()),
            file_size: None,
        });
        document.caption = Some("notes".into());
        channel.handle_update(update(document)).await.unwrap();
        let document_envelope = recv(&mut receiver).await;
        assert_eq!(document_envelope.text, "notes");
        let document_path = document_envelope.attachments[0].file_path.clone();
        assert_eq!(
            document_envelope.attachments[0].kind,
            TelegramAttachmentKind::File
        );
        assert_eq!(document_envelope.attachments[0].mime_type, "text/plain");
        assert_eq!(std::fs::read(&document_path).unwrap(), b"document body");
        assert_eq!(
            document_envelope.to_prompt_input().attachments[0].kind,
            Some(ChannelAttachmentType::File)
        );

        let mut voice = message();
        voice.voice = Some(TelegramVoice {
            file_id: "voice".into(),
            mime_type: None,
            file_size: None,
        });
        channel.handle_update(update(voice)).await.unwrap();
        let voice_envelope = recv(&mut receiver).await;
        let voice_path = voice_envelope.attachments[0].file_path.clone();
        assert_eq!(voice_envelope.text, "");
        assert_eq!(
            voice_envelope.attachments[0].kind,
            TelegramAttachmentKind::Audio
        );
        assert_eq!(voice_envelope.attachments[0].mime_type, "audio/ogg");
        assert_eq!(std::fs::read(&voice_path).unwrap(), b"voice body");
        assert_eq!(
            voice_envelope.to_prompt_input().attachments[0].kind,
            Some(ChannelAttachmentType::Audio)
        );
        for path in [document_path, voice_path] {
            if let Some(parent) = Path::new(&path).parent() {
                let _ = std::fs::remove_dir_all(parent);
            }
        }
    }

    #[tokio::test]
    async fn failed_media_download_dispatches_explanatory_text() {
        let api = Arc::new(MockApi::default());
        api.fail_download("lost");
        let (channel, mut receiver) = make_channel(api);
        let mut incoming = message();
        incoming.document = Some(TelegramDocument {
            file_id: "lost".into(),
            file_name: Some("notes.pdf".into()),
            mime_type: None,
            file_size: None,
        });
        incoming.caption = Some("see attached".into());
        channel.handle_update(update(incoming)).await.unwrap();
        let envelope = recv(&mut receiver).await;
        assert!(envelope.attachments.is_empty());
        assert_eq!(
            envelope.text,
            "see attached\n\n(User sent a file \"notes.pdf\" but download failed)"
        );
    }

    #[tokio::test]
    async fn start_command_is_local_in_its_topic_and_cancel_is_forwarded() {
        let api = Arc::new(MockApi::default());
        let (channel, mut receiver) = make_channel(api.clone());
        let mut envelope = TelegramInboundEnvelope {
            chat_id: "-42".into(),
            text: "/start".into(),
            thread_id: Some("8".into()),
            ..TelegramInboundEnvelope::default()
        };
        channel.handle_inbound(envelope.clone()).await.unwrap();
        let calls = api.calls();
        assert!(
            matches!(&calls[0], Call::Send(chat, text, options) if chat == "-42" && text.contains("Qwen Code Telegram bot") && options.parse_mode == Some(TelegramParseMode::Html) && options.message_thread_id == Some(8))
        );
        envelope.text = "/cancel".into();
        channel.handle_inbound(envelope).await.unwrap();
        assert_eq!(recv(&mut receiver).await.text, "/cancel");
    }

    #[tokio::test]
    async fn send_falls_back_for_only_the_rejected_html_chunk() {
        let api = Arc::new(MockApi::default());
        lock_unpoisoned(&api.state)
            .fail_html_containing
            .insert("bad".into());
        let (channel, _) = make_channel(api.clone());
        channel
            .send_message("chat", &format!("**bad**\n\n{}", "x".repeat(5000)), None)
            .await
            .unwrap();
        let sends = api
            .calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Send(_, text, options) => Some((text, options)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(sends.len() >= 3);
        assert_eq!(sends[0].1.parse_mode, Some(TelegramParseMode::Html));
        assert_eq!(sends[1].1.parse_mode, None);
        assert!(sends[1].0.starts_with("bad\n\n"));
        assert_eq!(sends[2].1.parse_mode, Some(TelegramParseMode::Html));
    }

    #[tokio::test]
    async fn current_route_overrides_old_session_topic_and_proactive_send_keeps_topic() {
        let api = Arc::new(MockApi::default());
        let (channel, _) = make_channel(api.clone());
        let target = SessionTarget {
            channel_name: "telegram".into(),
            sender_id: "7".into(),
            chat_id: "chat".into(),
            thread_id: Some("42".into()),
            is_group: Some(true),
            extra: Default::default(),
        };
        channel
            .send_response(
                "chat",
                "general",
                Some(&TelegramSessionRoute { thread_id: None }),
                Some(&target),
            )
            .await
            .unwrap();
        channel.push_proactive(&target, "reminder").await.unwrap();
        let sends = api
            .calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Send(_, _, options) => Some(options),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(sends[0].message_thread_id, None);
        assert_eq!(sends[1].message_thread_id, Some(42));
        let invalid = SessionTarget {
            thread_id: Some("42oops".into()),
            ..target
        };
        assert!(!channel.supports_proactive_target(&invalid));
        assert!(channel.push_proactive(&invalid, "no send").await.is_err());
        assert_eq!(sends.len(), 2);
    }

    #[tokio::test]
    async fn typing_deduplicates_active_chat_and_disconnect_clears_state() {
        let api = Arc::new(MockApi::default());
        let (channel, _) = make_channel(api.clone());
        channel.on_prompt_start("chat", Some("one"));
        channel.on_prompt_start("chat", Some("two"));
        tokio::task::yield_now().await;
        assert_eq!(
            lock_unpoisoned(&channel.typing.state).active_sessions["chat"].len(),
            2
        );
        assert_eq!(
            api.calls()
                .iter()
                .filter(|call| matches!(call, Call::Action(_, _)))
                .count(),
            1
        );
        channel.on_task_lifecycle(&TelegramTaskLifecycleEvent {
            channel_name: "telegram".into(),
            chat_id: "chat".into(),
            session_id: "one".into(),
            kind: TelegramLifecycleKind::Completed,
        });
        assert_eq!(
            lock_unpoisoned(&channel.typing.state).active_sessions["chat"].len(),
            1
        );
        channel.disconnect();
        assert!(
            lock_unpoisoned(&channel.typing.state)
                .active_sessions
                .is_empty()
        );
        assert!(lock_unpoisoned(&channel.typing.state).intervals.is_empty());
    }

    #[test]
    fn markdown_formatter_covers_inline_code_blocks_quotes_lists_and_links() {
        assert_eq!(
            telegram_format(
                "# Header\n\n**bold** and _italic_ and ~~gone~~\n- item\n\n[x](https://example.com) & <tag>"
            ),
            "<b>Header</b>\n\n<b>bold</b> and <i>italic</i> and <s>gone</s>\n• item\n\n<a href=\"https://example.com\">x</a> &amp; &lt;tag&gt;"
        );
        assert_eq!(telegram_format("`<code>`"), "<code>&lt;code&gt;</code>");
        assert_eq!(
            telegram_format("```rust\nlet x = 1;\n```"),
            "<pre><code class=\"language-rust\">let x = 1;\n</code></pre>"
        );
        assert_eq!(
            telegram_format("> quote\n> second"),
            "<blockquote>quote\nsecond</blockquote>"
        );
    }

    #[test]
    fn html_chunks_follow_utf16_limit_and_reopen_tags() {
        let html = format!("<b>{}</b>", "😀&".repeat(2200));
        let chunks = split_html_for_telegram(&html);
        assert!(chunks.len() >= 2);
        assert!(chunks.iter().all(|chunk| utf16_len(chunk) <= 4096));
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.starts_with("<b>") && chunk.ends_with("</b>"))
        );
        let text = chunks
            .iter()
            .map(|chunk| strip_html_tags(chunk))
            .collect::<String>();
        assert_eq!(text, "😀&".repeat(2200));
    }

    #[test]
    fn plain_fallback_removes_tags_and_preserves_entities() {
        assert_eq!(strip_html_tags("<b>one &amp; two</b>"), "one &amp; two");
    }
}
