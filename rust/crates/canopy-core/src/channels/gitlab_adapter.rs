//! GitLab todo polling and note delivery behavior.
//!
//! Ports `packages/channels/gitlab/src/GitlabAdapter.ts` behind a narrow API
//! trait. The module builds source-compatible envelopes, processes pending
//! todos in cursor order, and exposes callbacks for note acknowledgement
//! emojis. Daemon and CLI channel registration remain host integration work.

use super::channel_polling::{PollingChannelBase, PollingTask};
use super::gitlab_mention::strip_bot_mention;
use super::sanitize::sanitize_log_text;
use futures_util::future::BoxFuture;
use futures_util::future::join_all;
use reqwest::{Client, Method, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, watch};

const DEFAULT_GITLAB_URL: &str = "https://gitlab.com";
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const TODOS_PER_PAGE: u32 = 100;
const MAX_TODO_PAGES: u32 = 500;
const MAX_PENDING_TODOS: usize = TODOS_PER_PAGE as usize * MAX_TODO_PAGES as usize;
const CLEANUP_CONCURRENCY: usize = 16;
const ERROR_NOTE: &str = "⚠️ Failed to process this request. Please re-mention the bot to retry.";

pub type GitlabFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// GitLab channel fields consumed by the TypeScript adapter.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GitlabChannelConfig {
    #[serde(default = "default_channel_type", rename = "type")]
    pub channel_type: String,
    pub token: String,
    #[serde(default, rename = "baseUrl", skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(
        default,
        rename = "action_prompt_template",
        skip_serializing_if = "Option::is_none"
    )]
    pub action_prompt_template: Option<HashMap<String, String>>,
    #[serde(
        default,
        rename = "groupPolicy",
        skip_serializing_if = "Option::is_none"
    )]
    pub group_policy: Option<String>,
    #[serde(
        default,
        rename = "allowedUsers",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub allowed_users: Vec<String>,
    #[serde(
        default,
        rename = "pollInterval",
        skip_serializing_if = "Option::is_none"
    )]
    pub poll_interval: Option<f64>,
    #[serde(
        default,
        rename = "senderPolicy",
        skip_serializing_if = "Option::is_none"
    )]
    pub sender_policy: Option<String>,
    #[serde(
        default,
        rename = "sessionScope",
        skip_serializing_if = "Option::is_none"
    )]
    pub session_scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, rename = "dmPolicy", skip_serializing_if = "Option::is_none")]
    pub dm_policy: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub groups: HashMap<String, Value>,
}

impl GitlabChannelConfig {
    pub fn api_host(&self) -> String {
        self.base_url
            .as_deref()
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_GITLAB_URL)
            .trim_end_matches('/')
            .to_owned()
    }

    fn has_templates(&self) -> bool {
        self.action_prompt_template
            .as_ref()
            .is_some_and(|templates| !templates.is_empty())
    }
}

fn default_channel_type() -> String {
    "gitlab".to_owned()
}

/// Persisted `PollingChannelBase` cursor. Its JSON keys match TypeScript.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GitlabCursor {
    pub last_processed_id: f64,
    pub initialized: bool,
}

impl Default for GitlabCursor {
    fn default() -> Self {
        Self {
            last_processed_id: 0.0,
            initialized: false,
        }
    }
}

/// Validate a restored cursor with the same required fields as the Zod schema.
/// Unknown fields are ignored as Zod's default object mode strips them.
pub fn validate_gitlab_cursor(value: Value) -> Option<GitlabCursor> {
    serde_json::from_value(value).ok()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GitlabTargetKind {
    Issue,
    MergeRequest,
}

impl GitlabTargetKind {
    fn from_todo_type(value: &str) -> Option<Self> {
        match value {
            "Issue" => Some(Self::Issue),
            "MergeRequest" => Some(Self::MergeRequest),
            _ => None,
        }
    }

    fn thread_prefix(self) -> &'static str {
        match self {
            Self::Issue => "issue",
            Self::MergeRequest => "mr",
        }
    }

    fn api_collection(self) -> &'static str {
        match self {
            Self::Issue => "issues",
            Self::MergeRequest => "merge_requests",
        }
    }
}

/// The GitLab todo fields used by the adapter. Extra REST fields are ignored.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct GitlabTodo {
    pub id: u64,
    #[serde(default)]
    pub action_name: String,
    #[serde(default)]
    pub target_type: String,
    #[serde(default)]
    pub target_url: String,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub project: Option<GitlabTodoProject>,
    #[serde(default)]
    pub author: Option<GitlabTodoAuthor>,
    #[serde(default)]
    pub target: Option<GitlabTodoTarget>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct GitlabTodoProject {
    pub path_with_namespace: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct GitlabTodoAuthor {
    pub username: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct GitlabTodoTarget {
    #[serde(default)]
    pub iid: Option<u64>,
    #[serde(default)]
    pub title: Option<String>,
}

/// Platform-independent subset of the TypeScript `Envelope` used by GitLab.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GitlabEnvelope {
    pub channel_name: String,
    pub sender_id: String,
    pub sender_name: String,
    pub chat_id: String,
    pub text: String,
    pub thread_id: String,
    pub message_id: String,
    pub is_group: bool,
    pub is_mentioned: bool,
    pub is_reply_to_bot: bool,
    pub metadata: String,
}

/// Host seam that passes generated envelopes into the shared channel runtime.
pub trait GitlabInboundHandler: Send + Sync + 'static {
    fn handle_inbound<'a>(
        &'a self,
        envelope: GitlabEnvelope,
    ) -> GitlabFuture<'a, Result<(), String>>;
}

/// GitLab REST operations used by polling, note delivery, and acknowledgements.
/// Implementations must honor cancellation and return transport/API failures.
pub trait GitlabApi: Send + Sync + 'static {
    fn show_current_username<'a>(
        &'a self,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<String, String>>;

    fn list_pending_todos<'a>(
        &'a self,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<Vec<GitlabTodo>, String>>;

    fn mark_todo_done<'a>(
        &'a self,
        todo_id: u64,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<(), String>>;

    fn fetch_description<'a>(
        &'a self,
        project: &'a str,
        target_kind: GitlabTargetKind,
        iid: u64,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<String, String>>;

    fn create_note<'a>(
        &'a self,
        project: &'a str,
        target_kind: GitlabTargetKind,
        iid: u64,
        body: &'a str,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<(), String>>;

    fn award_note_eyes<'a>(
        &'a self,
        project: &'a str,
        target_kind: GitlabTargetKind,
        iid: u64,
        note_id: u64,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<u64, String>>;

    fn remove_note_award<'a>(
        &'a self,
        project: &'a str,
        target_kind: GitlabTargetKind,
        iid: u64,
        note_id: u64,
        award_id: u64,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<(), String>>;
}

/// Cloneable cancellation handle used by API requests and the poll loop.
#[derive(Clone, Debug)]
pub struct GitlabCancellationToken {
    sender: watch::Sender<bool>,
    receiver: watch::Receiver<bool>,
}

impl GitlabCancellationToken {
    pub fn new() -> Self {
        let (sender, receiver) = watch::channel(false);
        Self { sender, receiver }
    }

    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.receiver.borrow()
    }

    pub async fn cancelled(&self) {
        let mut receiver = self.receiver.clone();
        loop {
            if *receiver.borrow() {
                return;
            }
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Default for GitlabCancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Bounded GitLab REST client suitable for production adapters.
#[derive(Clone)]
pub struct ReqwestGitlabApi {
    client: Client,
    api_host: String,
    token: String,
}

impl ReqwestGitlabApi {
    pub fn new(api_host: impl Into<String>, token: impl Into<String>) -> Result<Self, String> {
        let client = Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .map_err(|_| "could not initialize GitLab HTTP client".to_owned())?;
        Ok(Self {
            client,
            api_host: api_host.into().trim_end_matches('/').to_owned(),
            token: token.into(),
        })
    }

    pub fn from_config(config: &GitlabChannelConfig) -> Result<Self, String> {
        Self::new(config.api_host(), config.token.clone())
    }

    async fn request_raw(
        &self,
        method: Method,
        path: String,
        query: Vec<(String, String)>,
        body: Option<Value>,
        cancellation: &GitlabCancellationToken,
    ) -> Result<GitlabHttpResponse, String> {
        if cancellation.is_cancelled() {
            return Err("GitLab request cancelled".to_owned());
        }
        let url = format!("{}/api/v4/{path}", self.api_host);
        let mut builder = self
            .client
            .request(method, url)
            .header("PRIVATE-TOKEN", &self.token);
        if !query.is_empty() {
            builder = builder.query(&query);
        }
        if let Some(body) = body {
            builder = builder.json(&body);
        }
        let response = tokio::select! {
            _ = cancellation.cancelled() => return Err("GitLab request cancelled".to_owned()),
            response = builder.send() => response.map_err(|_| "GitLab request failed".to_owned())?,
        };
        read_gitlab_response(response, cancellation).await
    }

    async fn request_json(
        &self,
        method: Method,
        path: String,
        query: Vec<(String, String)>,
        body: Option<Value>,
        cancellation: &GitlabCancellationToken,
    ) -> Result<(Value, Option<String>), String> {
        let response = self
            .request_raw(method, path, query, body, cancellation)
            .await?;
        let value = if response.body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&response.body)
                .map_err(|_| "GitLab returned invalid JSON".to_owned())?
        };
        Ok((value, response.next_page))
    }
}

struct GitlabHttpResponse {
    body: Vec<u8>,
    next_page: Option<String>,
}

async fn read_gitlab_response(
    mut response: Response,
    cancellation: &GitlabCancellationToken,
) -> Result<GitlabHttpResponse, String> {
    let status = response.status();
    let next_page = response
        .headers()
        .get("x-next-page")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut bytes = Vec::new();
    loop {
        let chunk = tokio::select! {
            _ = cancellation.cancelled() => return Err("GitLab request cancelled".to_owned()),
            chunk = response.chunk() => chunk.map_err(|_| "GitLab response read failed".to_owned())?,
        };
        let Some(chunk) = chunk else {
            break;
        };
        if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(format!(
                "GitLab response exceeded the {MAX_RESPONSE_BYTES}-byte limit"
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        return Err(format!("GitLab API returned HTTP {}", status.as_u16()));
    }
    Ok(GitlabHttpResponse {
        body: bytes,
        next_page,
    })
}

impl GitlabApi for ReqwestGitlabApi {
    fn show_current_username<'a>(
        &'a self,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<String, String>> {
        Box::pin(async move {
            let (value, _) = self
                .request_json(
                    Method::GET,
                    "user".to_owned(),
                    Vec::new(),
                    None,
                    cancellation,
                )
                .await?;
            value
                .get("username")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| "GitLab current-user response omitted username".to_owned())
        })
    }

    fn list_pending_todos<'a>(
        &'a self,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<Vec<GitlabTodo>, String>> {
        Box::pin(async move {
            let mut all = Vec::new();
            let mut page = 1u32;
            let mut pages_fetched = 0u32;
            loop {
                pages_fetched += 1;
                if pages_fetched > MAX_TODO_PAGES {
                    return Err("GitLab todo pagination exceeded its page limit".to_owned());
                }
                let (value, next_page) = self
                    .request_json(
                        Method::GET,
                        "todos".to_owned(),
                        vec![
                            ("state".to_owned(), "pending".to_owned()),
                            ("per_page".to_owned(), TODOS_PER_PAGE.to_string()),
                            ("page".to_owned(), page.to_string()),
                        ],
                        None,
                        cancellation,
                    )
                    .await?;
                let todos: Vec<GitlabTodo> = serde_json::from_value(value)
                    .map_err(|_| "GitLab returned an invalid todo list".to_owned())?;
                if all.len().saturating_add(todos.len()) > MAX_PENDING_TODOS {
                    return Err(format!(
                        "GitLab pending todo list exceeded the {MAX_PENDING_TODOS}-item limit"
                    ));
                }
                all.extend(todos);
                let Some(next_page) = next_page.filter(|value| !value.is_empty()) else {
                    break;
                };
                let next_page = next_page
                    .parse::<u32>()
                    .map_err(|_| "GitLab returned an invalid pagination cursor".to_owned())?;
                if next_page <= page || next_page > MAX_TODO_PAGES {
                    return Err("GitLab todo pagination exceeded its page limit".to_owned());
                }
                page = next_page;
            }
            Ok(all)
        })
    }

    fn mark_todo_done<'a>(
        &'a self,
        todo_id: u64,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.request_raw(
                Method::POST,
                format!("todos/{todo_id}/mark_as_done"),
                Vec::new(),
                None,
                cancellation,
            )
            .await?;
            Ok(())
        })
    }

    fn fetch_description<'a>(
        &'a self,
        project: &'a str,
        target_kind: GitlabTargetKind,
        iid: u64,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<String, String>> {
        Box::pin(async move {
            let path = format!(
                "projects/{}/{}/{iid}",
                encode_project_path(project),
                target_kind.api_collection()
            );
            let (value, _) = self
                .request_json(Method::GET, path, Vec::new(), None, cancellation)
                .await?;
            Ok(value
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned())
        })
    }

    fn create_note<'a>(
        &'a self,
        project: &'a str,
        target_kind: GitlabTargetKind,
        iid: u64,
        body: &'a str,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let path = format!(
                "projects/{}/{}/{iid}/notes",
                encode_project_path(project),
                target_kind.api_collection()
            );
            self.request_raw(
                Method::POST,
                path,
                Vec::new(),
                Some(json!({ "body": body })),
                cancellation,
            )
            .await?;
            Ok(())
        })
    }

    fn award_note_eyes<'a>(
        &'a self,
        project: &'a str,
        target_kind: GitlabTargetKind,
        iid: u64,
        note_id: u64,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<u64, String>> {
        Box::pin(async move {
            let path = format!(
                "projects/{}/{}/{iid}/notes/{note_id}/award_emoji",
                encode_project_path(project),
                target_kind.api_collection()
            );
            let (value, _) = self
                .request_json(
                    Method::POST,
                    path,
                    Vec::new(),
                    Some(json!({ "name": "eyes" })),
                    cancellation,
                )
                .await?;
            value
                .get("id")
                .and_then(Value::as_u64)
                .ok_or_else(|| "GitLab award response omitted id".to_owned())
        })
    }

    fn remove_note_award<'a>(
        &'a self,
        project: &'a str,
        target_kind: GitlabTargetKind,
        iid: u64,
        note_id: u64,
        award_id: u64,
        cancellation: &'a GitlabCancellationToken,
    ) -> GitlabFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let path = format!(
                "projects/{}/{}/{iid}/notes/{note_id}/award_emoji/{award_id}",
                encode_project_path(project),
                target_kind.api_collection()
            );
            self.request_raw(Method::DELETE, path, Vec::new(), None, cancellation)
                .await?;
            Ok(())
        })
    }
}

fn encode_project_path(project: &str) -> String {
    let mut encoded = String::with_capacity(project.len());
    for byte in project.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(
                char::from_digit((byte >> 4) as u32, 16)
                    .unwrap()
                    .to_ascii_uppercase(),
            );
            encoded.push(
                char::from_digit((byte & 0x0f) as u32, 16)
                    .unwrap()
                    .to_ascii_uppercase(),
            );
        }
    }
    encoded
}

/// GitLab todo adapter with an injectable API and host inbound callback.
pub struct GitlabAdapter {
    name: String,
    poller: Arc<GitlabPoller>,
    polling: PollingChannelBase<GitlabCursor>,
}

impl GitlabAdapter {
    pub fn new(
        name: impl Into<String>,
        mut config: GitlabChannelConfig,
        api: Arc<dyn GitlabApi>,
        handler: Arc<dyn GitlabInboundHandler>,
        channels_dir: impl Into<std::path::PathBuf>,
    ) -> Self {
        let name = name.into();
        config.allowed_users = config
            .allowed_users
            .into_iter()
            .map(|user| user.to_lowercase())
            .collect();
        let poller = Arc::new(GitlabPoller::new(
            name.clone(),
            config.clone(),
            api,
            handler,
        ));
        let polling = PollingChannelBase::new(
            name.clone(),
            GitlabCursor::default(),
            poller.clone(),
            channels_dir,
            config.poll_interval,
        );
        Self {
            name,
            poller,
            polling,
        }
    }

    pub fn from_global(
        name: impl Into<String>,
        config: GitlabChannelConfig,
        api: Arc<dyn GitlabApi>,
        handler: Arc<dyn GitlabInboundHandler>,
    ) -> io::Result<Self> {
        Ok(Self::new(
            name,
            config,
            api,
            handler,
            super::paths::global_channels_root()?,
        ))
    }

    pub fn with_reqwest(
        name: impl Into<String>,
        config: GitlabChannelConfig,
        handler: Arc<dyn GitlabInboundHandler>,
        channels_dir: impl Into<std::path::PathBuf>,
    ) -> Result<Self, String> {
        let api = Arc::new(ReqwestGitlabApi::from_config(&config)?);
        Ok(Self::new(name, config, api, handler, channels_dir))
    }

    /// Resolve the bot identity, emit TypeScript-compatible config warnings,
    /// then start the reusable persisted polling loop.
    pub async fn connect(&self) -> Result<(), String> {
        self.poller.reset_cancellation();
        self.poller.connect().await?;
        self.polling
            .start_poll_loop()
            .map_err(|error| format!("could not start GitLab polling: {error}"))
    }

    pub fn disconnect(&self) {
        self.poller.cancel_requests();
        self.polling.stop_poll_loop();
    }

    pub async fn disconnect_and_wait(&self) {
        self.disconnect();
        self.polling.stop_poll_loop_and_wait().await;
    }

    pub async fn cursor_snapshot(&self) -> GitlabCursor {
        self.polling.cursor_snapshot().await
    }

    /// Run one poll against caller-owned cursor state, useful to native hosts
    /// that schedule polling outside `PollingChannelBase`.
    pub async fn poll_once(&self, cursor: &mut GitlabCursor) -> Result<(), String> {
        self.poller.poll_once_inner(cursor).await
    }

    pub fn allowed_users(&self) -> Vec<String> {
        self.poller.config.allowed_users.clone()
    }

    /// GitLab replies require an issue or merge-request thread route.
    pub async fn send_message(&self, _chat_id: &str, _text: &str) -> Result<(), String> {
        Err(format!(
            "[Channel:{}] sendMessage requires a threadId; use send_thread_message",
            self.name
        ))
    }

    pub async fn send_thread_message(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        text: &str,
    ) -> Result<(), String> {
        let Some(thread_id) = thread_id else {
            return Err(format!(
                "[Channel:{}] sendThreadMessage requires a threadId (e.g. \"issue:42\" or \"mr:7\")",
                self.name
            ));
        };
        let Some((kind, iid)) = parse_thread_id(thread_id) else {
            return Err(format!(
                "[Channel:{}] invalid threadId format: {thread_id}",
                self.name
            ));
        };
        let cancellation = self.poller.cancellation();
        self.poller
            .api
            .create_note(chat_id, kind, iid, text, &cancellation)
            .await
    }

    /// Called by the host when the associated prompt starts processing.
    pub fn on_prompt_start(&self, chat_id: &str, message_id: Option<&str>) -> Result<(), String> {
        self.poller.on_prompt_start(chat_id, message_id)
    }

    /// Called by the host when the associated prompt finishes.
    pub fn on_prompt_end(&self, chat_id: &str, message_id: Option<&str>) -> Result<(), String> {
        self.poller.on_prompt_end(chat_id, message_id)
    }
}

struct GitlabPoller {
    name: String,
    config: GitlabChannelConfig,
    api_host: String,
    api: Arc<dyn GitlabApi>,
    handler: Arc<dyn GitlabInboundHandler>,
    bot_username: RwLock<String>,
    description_cache: AsyncMutex<HashMap<String, String>>,
    reactions: Mutex<HashMap<String, GitlabReactionEntry>>,
    cancellation: Mutex<GitlabCancellationToken>,
    poll_lock: AsyncMutex<()>,
}

#[derive(Clone)]
struct GitlabTarget {
    iid: u64,
    title: String,
    kind: GitlabTargetKind,
}

#[derive(Clone)]
struct GitlabReactionEntry {
    target: GitlabTarget,
    note_id: u64,
    lifecycle: Arc<Mutex<GitlabAwardLifecycle>>,
}

#[derive(Default)]
struct GitlabAwardLifecycle {
    state: GitlabAwardState,
    ended: bool,
}

#[derive(Clone, Copy, Default)]
enum GitlabAwardState {
    #[default]
    Idle,
    Pending,
    Awarded(u64),
    Removing,
    Failed,
}

impl GitlabPoller {
    fn new(
        name: String,
        config: GitlabChannelConfig,
        api: Arc<dyn GitlabApi>,
        handler: Arc<dyn GitlabInboundHandler>,
    ) -> Self {
        let api_host = config.api_host();
        Self {
            name,
            config,
            api_host,
            api,
            handler,
            bot_username: RwLock::new(String::new()),
            description_cache: AsyncMutex::new(HashMap::new()),
            reactions: Mutex::new(HashMap::new()),
            cancellation: Mutex::new(GitlabCancellationToken::new()),
            poll_lock: AsyncMutex::new(()),
        }
    }

    fn cancellation(&self) -> GitlabCancellationToken {
        self.cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn reset_cancellation(&self) {
        *self
            .cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = GitlabCancellationToken::new();
    }

    fn cancel_requests(&self) {
        self.cancellation().cancel();
    }

    async fn connect(&self) -> Result<(), String> {
        if !self.config.has_templates() {
            eprintln!(
                "[Channel:{}] warning: action_prompt_template is not configured; no todos will be processed",
                sanitize_log_text(&self.name, 128),
            );
        }
        if !matches!(
            self.config.group_policy.as_deref(),
            Some("open" | "allowlist" | "pairing")
        ) {
            let policy = self.config.group_policy.as_deref().unwrap_or("disabled");
            eprintln!(
                "[Channel:{}] warning: groupPolicy is \"{}\"; must be \"open\", \"allowlist\" (with the project listed), or \"pairing\" (after one-time group approval) for todos to be dispatched",
                sanitize_log_text(&self.name, 128),
                sanitize_log_text(policy, 64),
            );
        }
        let cancellation = self.cancellation();
        let username = self.api.show_current_username(&cancellation).await?;
        *self
            .bot_username
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = username;
        Ok(())
    }

    async fn poll_once_inner(&self, cursor: &mut GitlabCursor) -> Result<(), String> {
        let cancellation = self.cancellation();
        let _poll = tokio::select! {
            _ = cancellation.cancelled() => return Err("GitLab polling cancelled".to_owned()),
            guard = self.poll_lock.lock() => guard,
        };
        let Some(templates) = self
            .config
            .action_prompt_template
            .as_ref()
            .filter(|templates| !templates.is_empty())
        else {
            return Ok(());
        };
        if cancellation.is_cancelled() {
            return Err("GitLab polling cancelled".to_owned());
        }
        self.description_cache.lock().await.clear();
        let last_id = cursor.last_processed_id;
        let all_todos = self.api.list_pending_todos(&cancellation).await?;
        if cancellation.is_cancelled() {
            return Err("GitLab polling cancelled".to_owned());
        }

        // First poll drains existing todo state, then sets the initial cursor.
        if !cursor.initialized {
            if !all_todos.is_empty() {
                let max_id = all_todos.iter().map(|todo| todo.id).max().unwrap_or(0);
                self.queue_todos_best_effort(&all_todos, &cancellation)?;
                cursor.last_processed_id = max_id as f64;
            }
            cursor.initialized = true;
            return Ok(());
        }

        let mut todos: Vec<_> = all_todos
            .iter()
            .filter(|todo| todo.id as f64 > last_id)
            .cloned()
            .collect();
        todos.sort_by_key(|todo| todo.id);

        let stale: Vec<_> = all_todos
            .iter()
            .filter(|todo| todo.id as f64 <= last_id)
            .cloned()
            .collect();
        self.queue_todos_best_effort(&stale, &cancellation)?;

        for todo in todos {
            if cancellation.is_cancelled() {
                return Err("GitLab polling cancelled".to_owned());
            }
            let Some(project) = todo.project.as_ref() else {
                self.skip_todo(&todo, cursor, &cancellation).await?;
                continue;
            };
            let Some(kind) = GitlabTargetKind::from_todo_type(&todo.target_type) else {
                self.skip_todo(&todo, cursor, &cancellation).await?;
                continue;
            };
            let Some(raw_target) = todo.target.as_ref() else {
                self.skip_todo(&todo, cursor, &cancellation).await?;
                continue;
            };
            let Some(iid) = raw_target.iid.filter(|iid| *iid != 0) else {
                self.skip_todo(&todo, cursor, &cancellation).await?;
                continue;
            };
            let target = GitlabTarget {
                iid,
                title: raw_target.title.clone().unwrap_or_default(),
                kind,
            };
            let Some(template) = resolve_template(templates, &todo.action_name) else {
                self.skip_todo(&todo, cursor, &cancellation).await?;
                continue;
            };
            let chat_id = project.path_with_namespace.as_str();

            if let Err(error) = self
                .process_todo(&todo, &target, template, chat_id, &cancellation)
                .await
            {
                if cancellation.is_cancelled() {
                    return Err("GitLab polling cancelled".to_owned());
                }
                eprintln!(
                    "[Channel:{}] error processing todo {}: {}",
                    sanitize_log_text(&self.name, 128),
                    todo.id,
                    sanitize_log_text(&error, 512),
                );
                let _ = self
                    .api
                    .create_note(chat_id, kind, iid, ERROR_NOTE, &cancellation)
                    .await;
            }

            // The source advances before the best-effort `done` call.
            cursor.last_processed_id = todo.id as f64;
            self.mark_todo_best_effort(todo.id, &cancellation).await?;
        }
        Ok(())
    }

    fn queue_todos_best_effort(
        &self,
        todos: &[GitlabTodo],
        cancellation: &GitlabCancellationToken,
    ) -> Result<(), String> {
        if cancellation.is_cancelled() {
            return Err("GitLab polling cancelled".to_owned());
        }
        if todos.is_empty() {
            return Ok(());
        }
        let ids: Vec<_> = todos.iter().map(|todo| todo.id).collect();
        let api = self.api.clone();
        let cancellation = cancellation.clone();
        tokio::spawn(async move {
            for batch in ids.chunks(CLEANUP_CONCURRENCY) {
                let _ = join_all(
                    batch
                        .iter()
                        .copied()
                        .map(|todo_id| api.mark_todo_done(todo_id, &cancellation)),
                )
                .await;
                if cancellation.is_cancelled() {
                    break;
                }
            }
        });
        Ok(())
    }

    async fn mark_todo_best_effort(
        &self,
        todo_id: u64,
        cancellation: &GitlabCancellationToken,
    ) -> Result<(), String> {
        let _ = self.api.mark_todo_done(todo_id, cancellation).await;
        if cancellation.is_cancelled() {
            Err("GitLab polling cancelled".to_owned())
        } else {
            Ok(())
        }
    }

    async fn skip_todo(
        &self,
        todo: &GitlabTodo,
        cursor: &mut GitlabCursor,
        cancellation: &GitlabCancellationToken,
    ) -> Result<(), String> {
        self.mark_todo_best_effort(todo.id, cancellation).await?;
        cursor.last_processed_id = todo.id as f64;
        Ok(())
    }

    async fn process_todo(
        &self,
        todo: &GitlabTodo,
        target: &GitlabTarget,
        template: &str,
        chat_id: &str,
        cancellation: &GitlabCancellationToken,
    ) -> Result<(), String> {
        let author = todo
            .author
            .as_ref()
            .ok_or_else(|| "GitLab todo omitted author".to_owned())?;
        let bot_username = self
            .bot_username
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if author.username == bot_username {
            return Ok(());
        }

        let thread_id = format!("{}:{}", target.kind.thread_prefix(), target.iid);
        let note_id = parse_note_id(&todo.target_url);
        let needs_description = note_id.is_none() || template.contains("%description%");
        let mut description = String::new();
        if needs_description {
            if note_id.is_some() {
                match self
                    .fetch_description(chat_id, target.kind, target.iid, cancellation)
                    .await
                {
                    Ok(value) => description = value,
                    Err(error) => eprintln!(
                        "[Channel:{}] fetchDescription failed (metadata only): {}",
                        sanitize_log_text(&self.name, 128),
                        sanitize_log_text(&error, 512),
                    ),
                }
            } else {
                description = self
                    .fetch_description(chat_id, target.kind, target.iid, cancellation)
                    .await?;
            }
        }

        let body = todo.body.as_deref().unwrap_or_default();
        let text = if note_id.is_some() {
            body
        } else if !description.is_empty() {
            description.as_str()
        } else {
            body
        };
        if text.is_empty() {
            return Ok(());
        }
        let author_username = &author.username;
        let metadata = build_metadata(
            template,
            &self.api_host,
            todo,
            target,
            chat_id,
            author_username,
            &todo.id.to_string(),
            &description,
        );
        let envelope = GitlabEnvelope {
            channel_name: self.name.clone(),
            sender_id: author_username.to_lowercase(),
            sender_name: author_username.clone(),
            chat_id: chat_id.to_owned(),
            text: strip_bot_mention(text, &bot_username),
            thread_id,
            message_id: todo.id.to_string(),
            is_group: true,
            is_mentioned: true,
            is_reply_to_bot: false,
            metadata,
        };

        if let Some(note_id) = note_id {
            let entry = GitlabReactionEntry {
                target: target.clone(),
                note_id,
                lifecycle: Arc::new(Mutex::new(GitlabAwardLifecycle::default())),
            };
            self.reactions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(todo.id.to_string(), entry);
        }
        let result = self.handler.handle_inbound(envelope).await;
        self.reactions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&todo.id.to_string());
        result
    }

    async fn fetch_description(
        &self,
        project: &str,
        target_kind: GitlabTargetKind,
        iid: u64,
        cancellation: &GitlabCancellationToken,
    ) -> Result<String, String> {
        let key = format!("{project}/{}/{iid}", target_kind.thread_prefix());
        if let Some(cached) = self.description_cache.lock().await.get(&key).cloned() {
            return Ok(cached);
        }
        let description = self
            .api
            .fetch_description(project, target_kind, iid, cancellation)
            .await?;
        self.description_cache
            .lock()
            .await
            .insert(key, description.clone());
        Ok(description)
    }

    fn on_prompt_start(&self, chat_id: &str, message_id: Option<&str>) -> Result<(), String> {
        let Some(message_id) = message_id.filter(|value| !value.is_empty()) else {
            return Ok(());
        };
        let entry = self
            .reactions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(message_id)
            .cloned();
        let Some(entry) = entry else {
            return Ok(());
        };
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| "GitLab acknowledgement requires a Tokio runtime".to_owned())?;
        {
            let mut lifecycle = entry
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !matches!(
                lifecycle.state,
                GitlabAwardState::Idle | GitlabAwardState::Failed
            ) {
                return Ok(());
            }
            lifecycle.state = GitlabAwardState::Pending;
        }
        let api = self.api.clone();
        let target = entry.target;
        let note_id = entry.note_id;
        let lifecycle = entry.lifecycle;
        let project = chat_id.to_owned();
        let channel_name = self.name.clone();
        let cancellation = self.cancellation();
        runtime.spawn(async move {
            let award = api
                .award_note_eyes(&project, target.kind, target.iid, note_id, &cancellation)
                .await;
            match award {
                Ok(award_id) => {
                    let should_remove = {
                        let mut state = lifecycle
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if state.ended {
                            state.state = GitlabAwardState::Removing;
                            true
                        } else {
                            state.state = GitlabAwardState::Awarded(award_id);
                            false
                        }
                    };
                    if should_remove {
                        remove_award_best_effort(
                            api,
                            target,
                            project,
                            note_id,
                            award_id,
                            cancellation,
                            channel_name,
                        )
                        .await;
                    }
                }
                Err(error) => {
                    lifecycle
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .state = GitlabAwardState::Failed;
                    eprintln!(
                        "[Channel:{}] failed to acknowledge note {}: {}",
                        sanitize_log_text(&channel_name, 128),
                        note_id,
                        sanitize_log_text(&error, 512),
                    );
                }
            }
        });
        Ok(())
    }

    fn on_prompt_end(&self, chat_id: &str, message_id: Option<&str>) -> Result<(), String> {
        let Some(message_id) = message_id.filter(|value| !value.is_empty()) else {
            return Ok(());
        };
        let entry = self
            .reactions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(message_id);
        let Some(entry) = entry else {
            return Ok(());
        };
        let award_id = {
            let mut lifecycle = entry
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            lifecycle.ended = true;
            match lifecycle.state {
                GitlabAwardState::Awarded(award_id) => {
                    lifecycle.state = GitlabAwardState::Removing;
                    Some(award_id)
                }
                _ => None,
            }
        };
        let Some(award_id) = award_id else {
            return Ok(());
        };
        let api = self.api.clone();
        let target = entry.target;
        let note_id = entry.note_id;
        let project = chat_id.to_owned();
        let channel_name = self.name.clone();
        let cancellation = self.cancellation();
        tokio::runtime::Handle::try_current()
            .map_err(|_| "GitLab acknowledgement cleanup requires a Tokio runtime".to_owned())?
            .spawn(async move {
                remove_award_best_effort(
                    api,
                    target,
                    project,
                    note_id,
                    award_id,
                    cancellation,
                    channel_name,
                )
                .await;
            });
        Ok(())
    }
}

impl PollingTask<GitlabCursor> for GitlabPoller {
    fn poll_once<'a>(&'a self, cursor: &'a mut GitlabCursor) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(self.poll_once_inner(cursor))
    }
}

async fn remove_award_best_effort(
    api: Arc<dyn GitlabApi>,
    target: GitlabTarget,
    project: String,
    note_id: u64,
    award_id: u64,
    cancellation: GitlabCancellationToken,
    channel_name: String,
) {
    if let Err(error) = api
        .remove_note_award(
            &project,
            target.kind,
            target.iid,
            note_id,
            award_id,
            &cancellation,
        )
        .await
    {
        eprintln!(
            "[Channel:{}] failed to remove acknowledgement from note {}: {}",
            sanitize_log_text(&channel_name, 128),
            note_id,
            sanitize_log_text(&error, 512),
        );
    }
}

fn resolve_template<'a>(
    templates: &'a HashMap<String, String>,
    action_name: &str,
) -> Option<&'a str> {
    templates
        .get(action_name)
        .filter(|template| !template.is_empty())
        .or_else(|| {
            (action_name == "directly_addressed")
                .then(|| templates.get("mentioned"))
                .flatten()
                .filter(|template| !template.is_empty())
        })
        .map(String::as_str)
}

fn parse_note_id(target_url: &str) -> Option<u64> {
    let suffix = target_url.rsplit_once("#note_")?.1;
    if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    suffix.parse().ok()
}

fn parse_thread_id(value: &str) -> Option<(GitlabTargetKind, u64)> {
    let (prefix, number) = value.split_once(':')?;
    let kind = match prefix {
        "issue" => GitlabTargetKind::Issue,
        "mr" => GitlabTargetKind::MergeRequest,
        _ => return None,
    };
    if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some((kind, number.parse().ok()?))
}

fn build_metadata(
    template: &str,
    api_host: &str,
    todo: &GitlabTodo,
    target: &GitlabTarget,
    project: &str,
    author: &str,
    todo_id: &str,
    description: &str,
) -> String {
    let vars = [
        ("project", project.to_owned()),
        ("project_url", format!("{api_host}/{project}")),
        ("author", author.to_owned()),
        ("target_type", todo.target_type.clone()),
        ("iid", target.iid.to_string()),
        ("title", target.title.clone()),
        ("description", description.to_owned()),
        ("todo_id", todo_id.to_owned()),
    ];
    let mut rendered = String::with_capacity(template.len());
    let mut cursor = 0;
    while cursor < template.len() {
        let Some(offset) = template[cursor..].find('%') else {
            rendered.push_str(&template[cursor..]);
            break;
        };
        let start = cursor + offset;
        rendered.push_str(&template[cursor..start]);
        if template[start..].starts_with("%%") {
            rendered.push('%');
            cursor = start + 2;
            continue;
        }
        let key_start = start + 1;
        let bytes = template.as_bytes();
        let mut end = key_start;
        while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
            end += 1;
        }
        if end > key_start && end < bytes.len() && bytes[end] == b'%' {
            let key = &template[key_start..end];
            if let Some((_, value)) = vars.iter().find(|(name, _)| *name == key) {
                rendered.push_str(value);
            } else {
                rendered.push_str(&template[start..=end]);
            }
            cursor = end + 1;
        } else {
            rendered.push('%');
            cursor = key_start;
        }
    }
    rendered
}
