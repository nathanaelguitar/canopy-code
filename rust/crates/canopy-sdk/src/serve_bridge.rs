//! Rust slice of the TypeScript SDK's `daemon-mcp/serve-bridge`.
//!
//! This module exposes the stdio MCP JSON-RPC transport, two infrastructure
//! tools, eight session tools, ten workspace-read tools, and nine
//! workspace-write tools, plus `prompt` and `prompt_cancel`. Session SSE
//! collection is bounded and tracked in `PORT_STATUS.md`.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

use crate::daemon_auto_reconnect::AutoReconnectSubscribeOptions;
use crate::daemon_client::{
    DaemonClient, DaemonClientError, DaemonClientOptions, DaemonRequestOptions,
};
use crate::daemon_rest::RestSseCancellation;

const DEFAULT_DAEMON_URL: &str = "http://127.0.0.1:4170";
const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_STDIO_FRAME_BYTES: usize = 8 * 1024 * 1024;
const MAX_IN_FLIGHT_REQUESTS: usize = 64;
const MAX_IN_FLIGHT_CANCELLATIONS: usize = 8;
const MAX_PERSISTENT_SESSION_STREAMS: usize = 64;
const MAX_PROMPT_TEXT_BYTES: usize = 1024 * 1024;
const SESSION_STREAM_IDLE_TTL: Duration = Duration::from_secs(30 * 60);
const SESSION_STREAM_CLEANUP_INTERVAL: Duration = Duration::from_secs(5 * 60);
const PROMPT_COMPLETION_TIMEOUT: Duration = Duration::from_secs(30);
const PROTOCOL_VERSION: &str = "2024-11-05";

/// Configuration for the Rust serve-bridge slice.
#[derive(Clone)]
pub struct ServeBridgeOptions {
    /// Base URL of the `qwen serve` daemon.
    pub daemon_url: String,
    /// Optional Bearer token. If absent, the daemon client's normal
    /// `QWEN_SERVER_TOKEN` fallback applies.
    pub token: Option<String>,
    /// Fallback workspace for session create/load/resume, matching the
    /// TypeScript bridge's `QWEN_WORKSPACE_CWD` behavior.
    pub workspace_cwd: Option<String>,
    /// Permit workspace-wide settings changes and global-scope memory/agent
    /// writes. Defaults to false, matching the TypeScript bridge.
    pub allow_global_scope: bool,
    /// `None` disables HTTP request deadlines, like the SDK client option.
    pub fetch_timeout: Option<Duration>,
}

impl Default for ServeBridgeOptions {
    fn default() -> Self {
        Self {
            daemon_url: DEFAULT_DAEMON_URL.to_owned(),
            token: None,
            workspace_cwd: None,
            allow_global_scope: false,
            fetch_timeout: Some(DEFAULT_FETCH_TIMEOUT),
        }
    }
}

impl ServeBridgeOptions {
    /// Read the environment variables used by the TypeScript stdio entry
    /// point.
    pub fn from_env() -> Self {
        Self {
            daemon_url: std::env::var("QWEN_DAEMON_URL")
                .unwrap_or_else(|_| DEFAULT_DAEMON_URL.to_owned()),
            token: std::env::var("QWEN_DAEMON_TOKEN").ok(),
            workspace_cwd: std::env::var("QWEN_WORKSPACE_CWD").ok(),
            allow_global_scope: std::env::var("QWEN_BRIDGE_ALLOW_GLOBAL_SCOPE")
                .is_ok_and(|value| value == "true"),
            ..Self::default()
        }
    }
}

/// Errors that stop the stdio bridge itself.
#[derive(Debug, Error)]
pub enum ServeBridgeError {
    #[error("daemon client setup failed: {0}")]
    Daemon(#[from] DaemonClientError),
    #[error("stdio transport failed: {0}")]
    Io(#[from] io::Error),
}

/// MCP stdio bridge for daemon, session, agent, and workspace tools.
#[derive(Clone)]
pub struct ServeBridge {
    client: DaemonClient,
    workspace_cwd: Option<String>,
    allow_global_scope: bool,
    default_session_id: Arc<Mutex<Option<String>>>,
    event_streams: Arc<Mutex<HashMap<String, Arc<SessionEventStream>>>>,
}

struct SessionEventStream {
    cancellation: RestSseCancellation,
    active_collector: Mutex<Option<Arc<PromptCollector>>>,
    last_activity: Mutex<Instant>,
}

struct PromptCollector {
    state: Mutex<PromptCollectorState>,
    done_sender: watch::Sender<bool>,
    request_cancellation: RestSseCancellation,
}

struct PromptCollectorState {
    text: String,
    resolved: bool,
    interrupted: bool,
    truncated: bool,
}

struct PromptCollectorSnapshot {
    text: String,
    resolved: bool,
    interrupted: bool,
    truncated: bool,
}

impl SessionEventStream {
    fn new() -> Self {
        Self {
            cancellation: RestSseCancellation::new(),
            active_collector: Mutex::new(None),
            last_activity: Mutex::new(Instant::now()),
        }
    }

    fn touch(&self) {
        *self
            .last_activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Instant::now();
    }

    fn last_activity(&self) -> Instant {
        *self
            .last_activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn collector(&self) -> Option<Arc<PromptCollector>> {
        self.active_collector
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn interrupt_collector(&self) {
        if let Some(collector) = self.collector() {
            collector.resolve(true);
        }
    }
}

impl PromptCollector {
    fn new() -> Arc<Self> {
        let (done_sender, _) = watch::channel(false);
        Arc::new(Self {
            state: Mutex::new(PromptCollectorState {
                text: String::new(),
                resolved: false,
                interrupted: false,
                truncated: false,
            }),
            done_sender,
            request_cancellation: RestSseCancellation::new(),
        })
    }

    fn push_text(&self, text: &str) {
        if text.is_empty() {
            return;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.resolved || state.truncated {
            return;
        }
        let available = MAX_PROMPT_TEXT_BYTES.saturating_sub(state.text.len());
        if text.len() <= available {
            state.text.push_str(text);
            return;
        }
        let mut end = available.min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        state.text.push_str(&text[..end]);
        state.truncated = true;
    }

    fn resolve(&self, interrupted: bool) {
        let should_notify = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if interrupted {
                state.interrupted = true;
            }
            if state.resolved {
                false
            } else {
                state.resolved = true;
                true
            }
        };
        if should_notify {
            self.done_sender.send_replace(true);
        }
    }

    async fn wait(&self, timeout_duration: Duration) -> bool {
        let mut receiver = self.done_sender.subscribe();
        if *receiver.borrow() {
            return true;
        }
        tokio::time::timeout(timeout_duration, async move {
            loop {
                if receiver.changed().await.is_err() || *receiver.borrow() {
                    return;
                }
            }
        })
        .await
        .is_ok()
    }

    fn snapshot(&self) -> PromptCollectorSnapshot {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        PromptCollectorSnapshot {
            text: state.text.clone(),
            resolved: state.resolved,
            interrupted: state.interrupted,
            truncated: state.truncated,
        }
    }
}

impl ServeBridge {
    /// Construct a bridge with bounded daemon requests (30 seconds by
    /// default), the same default URL as the TypeScript CLI, and optional
    /// Bearer authentication.
    pub fn new(options: ServeBridgeOptions) -> Result<Self, ServeBridgeError> {
        let client = DaemonClient::with_options(
            options.daemon_url,
            DaemonClientOptions {
                token: options.token,
                fetch_timeout: options.fetch_timeout,
                ..DaemonClientOptions::default()
            },
        )?;
        Ok(Self {
            client,
            workspace_cwd: options.workspace_cwd,
            allow_global_scope: options.allow_global_scope,
            default_session_id: Arc::new(Mutex::new(None)),
            event_streams: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Construct a bridge from the `QWEN_DAEMON_URL`, `QWEN_DAEMON_TOKEN`,
    /// and `QWEN_WORKSPACE_CWD` environment variables.
    pub fn from_env() -> Result<Self, ServeBridgeError> {
        Self::new(ServeBridgeOptions::from_env())
    }

    /// Set or clear the session selected when `session_id` is omitted from
    /// session-scoped bridge tools.
    pub fn set_default_session_id(&self, session_id: Option<String>) {
        *self
            .default_session_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = session_id;
    }

    fn activate_session(&self, session_id: Option<String>) {
        let previous = self
            .default_session_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if previous != session_id
            && let Some(previous) = previous
        {
            self.stop_event_stream(&previous);
        }
        self.set_default_session_id(session_id.clone());
        if let Some(session_id) = session_id {
            self.start_event_stream(session_id);
        }
    }

    fn start_event_stream(&self, session_id: String) {
        let (stream, evicted) = {
            let mut streams = self
                .event_streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if streams
                .get(&session_id)
                .is_some_and(|existing| !existing.cancellation.is_cancelled())
            {
                return;
            }
            streams.remove(&session_id);

            let evicted_id = if streams.len() >= MAX_PERSISTENT_SESSION_STREAMS {
                streams
                    .iter()
                    .min_by_key(|(_, candidate)| candidate.last_activity())
                    .map(|(id, _)| id.clone())
            } else {
                None
            };
            let evicted = evicted_id.and_then(|id| streams.remove(&id).map(|stream| (id, stream)));
            if streams.len() >= MAX_PERSISTENT_SESSION_STREAMS {
                eprintln!(
                    "[serve-bridge] cannot start SSE stream for {session_id}: all session stream slots are active"
                );
                return;
            }

            let stream = Arc::new(SessionEventStream::new());
            streams.insert(session_id.clone(), Arc::clone(&stream));
            (stream, evicted)
        };

        if let Some((evicted_id, evicted)) = evicted {
            evicted.cancellation.cancel();
            evicted.interrupt_collector();
            let mut default_session_id = self
                .default_session_id
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if default_session_id.as_deref() == Some(evicted_id.as_str()) {
                *default_session_id = None;
            }
        }
        let bridge = self.clone();
        tokio::spawn(async move {
            bridge.consume_session_events(session_id, stream).await;
        });
    }

    fn stop_event_stream(&self, session_id: &str) {
        let stream = self
            .event_streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        if let Some(stream) = stream {
            stream.cancellation.cancel();
            stream.interrupt_collector();
        }
    }

    fn stop_all_event_streams(&self, cancel_prompts: bool) {
        let streams = std::mem::take(
            &mut *self
                .event_streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for stream in streams.into_values() {
            stream.cancellation.cancel();
            if let Some(collector) = stream.collector() {
                if cancel_prompts {
                    collector.request_cancellation.cancel();
                }
                collector.resolve(true);
            }
        }
    }

    async fn consume_session_events(&self, session_id: String, stream: Arc<SessionEventStream>) {
        let mut events = match self
            .client
            .subscribe_events(
                &session_id,
                AutoReconnectSubscribeOptions {
                    cancellation: Some(stream.cancellation.clone()),
                    ..AutoReconnectSubscribeOptions::default()
                },
            )
            .await
        {
            Ok(events) => events,
            Err(error) => {
                if !stream.cancellation.is_cancelled() {
                    eprintln!(
                        "[serve-bridge] SSE stream ended unexpectedly for {session_id}: {error}"
                    );
                }
                self.finish_event_stream(&session_id, &stream);
                return;
            }
        };
        let mut cleanup = tokio::time::interval(SESSION_STREAM_CLEANUP_INTERVAL);
        cleanup.tick().await;
        loop {
            tokio::select! {
                _ = cleanup.tick() => {
                    if Instant::now().duration_since(stream.last_activity()) >= SESSION_STREAM_IDLE_TTL {
                        eprintln!("[serve-bridge] Cleaning up idle session SSE: {session_id}");
                        break;
                    }
                }
                event = events.next() => {
                    match event {
                        Some(Ok(event)) => self.consume_session_event(&session_id, &stream, &event),
                        Some(Err(error)) => {
                            if !stream.cancellation.is_cancelled() {
                                eprintln!("[serve-bridge] SSE stream ended unexpectedly for {session_id}: {error}");
                            }
                            break;
                        }
                        None => break,
                    }
                }
            }
        }
        self.finish_event_stream(&session_id, &stream);
    }

    fn consume_session_event(
        &self,
        session_id: &str,
        stream: &SessionEventStream,
        event: &crate::daemon_sse::DaemonEvent,
    ) {
        let Some(update) = event
            .data
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|data| data.get("update"))
            .and_then(Value::as_object)
        else {
            return;
        };
        let Some(update_name) = update.get("sessionUpdate").and_then(Value::as_str) else {
            return;
        };
        if update_name == "agent_message_chunk" {
            stream.touch();
            if let Some(collector) = stream.collector() {
                if let Some(text) = update
                    .get("content")
                    .and_then(Value::as_object)
                    .and_then(|content| content.get("text"))
                    .and_then(Value::as_str)
                {
                    collector.push_text(text);
                }
                // The daemon places `_meta` on the final message chunk.
                if update.contains_key("_meta") {
                    collector.resolve(false);
                }
            }
        } else if update_name.to_ascii_lowercase().contains("error")
            || update_name.to_ascii_lowercase().contains("fail")
        {
            eprintln!("[serve-bridge] daemon error event for {session_id}: {update_name}");
            stream.interrupt_collector();
        }
    }

    fn finish_event_stream(&self, session_id: &str, stream: &Arc<SessionEventStream>) {
        stream.interrupt_collector();
        let removed = {
            let mut streams = self
                .event_streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if streams
                .get(session_id)
                .is_some_and(|current| Arc::ptr_eq(current, stream))
            {
                streams.remove(session_id);
                true
            } else {
                false
            }
        };
        if removed {
            let mut default_session_id = self
                .default_session_id
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if default_session_id.as_deref() == Some(session_id) {
                *default_session_id = None;
            }
        }
    }

    /// Serve newline-delimited MCP JSON-RPC messages on stdin/stdout.
    /// Requests run concurrently so `prompt_cancel` can interrupt a waiting
    /// `prompt`; in-flight work is capped. Diagnostics go to stderr. Each
    /// input and output frame is bounded to 8 MiB; daemon JSON responses are
    /// additionally bounded by `DaemonClient` to 1 MiB.
    pub async fn run_stdio(&self) -> Result<(), ServeBridgeError> {
        let stdin = tokio::io::stdin();
        let stdout = tokio::io::stdout();
        let mut reader = BufReader::new(stdin);
        let writer = Arc::new(tokio::sync::Mutex::new(BufWriter::new(stdout)));
        let request_gate = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUESTS));
        let cancellation_gate = Arc::new(Semaphore::new(MAX_IN_FLIGHT_CANCELLATIONS));
        let mut requests: JoinSet<Result<(), io::Error>> = JoinSet::new();

        loop {
            while let Some(result) = requests.try_join_next() {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        self.stop_all_event_streams(true);
                        return Err(error.into());
                    }
                    Err(error) => eprintln!("[serve-bridge] request task failed: {error}"),
                }
            }
            let frame = match read_bounded_frame(&mut reader, MAX_STDIO_FRAME_BYTES).await {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(error) => {
                    self.stop_all_event_streams(true);
                    return Err(error.into());
                }
            };
            match frame {
                StdioFrame::TooLarge => {
                    let response = json_rpc_error(
                        Value::Null,
                        -32700,
                        "Request exceeds the 8 MiB frame limit",
                    );
                    let mut writer = writer.lock().await;
                    if let Err(error) = write_json_line(&mut *writer, response).await {
                        self.stop_all_event_streams(true);
                        return Err(error.into());
                    }
                }
                StdioFrame::Line(line) => {
                    if line.iter().all(u8::is_ascii_whitespace) {
                        continue;
                    }
                    let control_request = is_prompt_cancel_frame(&line);
                    let gate = if control_request {
                        &cancellation_gate
                    } else {
                        &request_gate
                    };
                    let permit = match Arc::clone(gate).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            if let Some(response) = overloaded_request_error(&line) {
                                let mut writer = writer.lock().await;
                                if let Err(error) = write_json_line(&mut *writer, response).await {
                                    self.stop_all_event_streams(true);
                                    return Err(error.into());
                                }
                            }
                            continue;
                        }
                    };
                    let bridge = self.clone();
                    let writer = Arc::clone(&writer);
                    requests.spawn(async move {
                        let _permit = permit;
                        if let Some(response) = bridge.process_message(&line).await {
                            let mut writer = writer.lock().await;
                            write_json_line(&mut *writer, response).await?;
                        }
                        Ok::<(), io::Error>(())
                    });
                }
            }
        }

        // Closing stdin is the stdio client's shutdown signal. Stop the
        // long-lived event readers and cancel in-flight prompts before
        // joining their request tasks.
        self.stop_all_event_streams(true);
        while let Some(result) = requests.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    self.stop_all_event_streams(true);
                    return Err(error.into());
                }
                Err(error) => eprintln!("[serve-bridge] request task failed: {error}"),
            }
        }
        Ok(())
    }

    async fn process_message(&self, frame: &[u8]) -> Option<Value> {
        let request: Value = match serde_json::from_slice(frame) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("[serve-bridge] invalid JSON-RPC frame: {error}");
                return Some(json_rpc_error(Value::Null, -32700, "Parse error"));
            }
        };
        let Some(object) = request.as_object() else {
            return Some(json_rpc_error(
                Value::Null,
                -32600,
                "Invalid Request: expected a JSON-RPC object",
            ));
        };
        let id = object.get("id").cloned();
        let method = object.get("method").and_then(Value::as_str);
        if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") || method.is_none() {
            return Some(json_rpc_error(
                id.unwrap_or(Value::Null),
                -32600,
                "Invalid Request: expected jsonrpc '2.0' and a string method",
            ));
        }
        let method = method.expect("validated above");
        let Some(id) = id else {
            // Notifications, including `notifications/initialized`, have no
            // response. The bridge has no server-originated notifications.
            return None;
        };
        let params = object.get("params").cloned().unwrap_or_else(|| json!({}));

        let result = match method {
            "initialize" => {
                let requested = params
                    .get("protocolVersion")
                    .and_then(Value::as_str)
                    .unwrap_or(PROTOCOL_VERSION);
                if requested != PROTOCOL_VERSION {
                    return Some(json_rpc_error(
                        id,
                        -32602,
                        format!("Unsupported protocol version: {requested}"),
                    ));
                }
                json!({
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "qwen-serve-bridge", "version": "1.0.0"}
                })
            }
            "ping" => json!({}),
            "tools/list" => json!({"tools": tool_catalog()}),
            "tools/call" => {
                let Some(name) = params.get("name").and_then(Value::as_str) else {
                    return Some(json_rpc_error(
                        id,
                        -32602,
                        "Invalid params: tools/call requires a tool name",
                    ));
                };
                let args = match params.get("arguments") {
                    Some(value) => value.clone(),
                    None => json!({}),
                };
                self.call_tool(name, args).await
            }
            _ => {
                return Some(json_rpc_error(
                    id,
                    -32601,
                    format!("Method not found: {method}"),
                ));
            }
        };
        Some(json!({"jsonrpc":"2.0", "id":id, "result":result}))
    }

    async fn call_tool(&self, name: &str, arguments: Value) -> Value {
        let args = match validate_tool_arguments(name, arguments) {
            Ok(args) => args,
            Err(message) => return tool_error(message),
        };
        if is_session_tool(name) {
            return self.call_session_tool(name, &args).await;
        }
        if is_agent_tool(name) {
            return self.call_agent_tool(name, &args).await;
        }
        if is_workspace_write_tool(name) {
            return self.call_workspace_write_tool(name, &args).await;
        }
        let result = match name {
            "health" => self.client.health().await,
            "capabilities" => self.client.capabilities().await,
            "file_read" => {
                let mut options = Map::new();
                for (source, target) in [
                    ("max_bytes", "maxBytes"),
                    ("line", "line"),
                    ("limit", "limit"),
                    ("cursor", "cursor"),
                ] {
                    if let Some(value) = args.get(source) {
                        options.insert(target.to_owned(), value.clone());
                    }
                }
                self.client
                    .read_workspace_file(
                        args["path"].as_str().expect("validated path"),
                        Value::Object(options),
                        None,
                    )
                    .await
            }
            "file_read_bytes" => {
                let query = {
                    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
                    serializer.append_pair("path", args["path"].as_str().expect("validated path"));
                    for (source, target) in [("offset", "offset"), ("max_bytes", "maxBytes")] {
                        if let Some(value) = args.get(source) {
                            serializer.append_pair(target, &javascript_number_string(value));
                        }
                    }
                    serializer.finish()
                };
                self.client
                    .request_json(
                        "GET",
                        "/file/bytes",
                        &query,
                        None,
                        "GET /file/bytes",
                        DaemonRequestOptions::default(),
                    )
                    .await
            }
            "file_stat" => {
                self.client
                    .file_stat(args["path"].as_str().expect("validated path"))
                    .await
            }
            "dir_list" => {
                self.client
                    .dir_list(args["path"].as_str().expect("validated path"))
                    .await
            }
            "glob" => {
                self.client
                    .glob(args["pattern"].as_str().expect("validated pattern"))
                    .await
            }
            "workspace_mcp_status" => {
                self.workspace_get("/workspace/mcp", "GET /workspace/mcp")
                    .await
            }
            "workspace_skills" => {
                self.workspace_get("/workspace/skills", "GET /workspace/skills")
                    .await
            }
            "workspace_providers" => {
                self.workspace_get("/workspace/providers", "GET /workspace/providers")
                    .await
            }
            "workspace_env" => {
                self.workspace_get("/workspace/env", "GET /workspace/env")
                    .await
            }
            "workspace_preflight" => {
                self.workspace_get("/workspace/preflight", "GET /workspace/preflight")
                    .await
            }
            _ => return tool_error(format!("Unknown tool: {name}")),
        };

        match result {
            Ok(value) => json_result(value),
            Err(error) => {
                eprintln!("[serve-bridge] Tool error: {error}");
                tool_error(error.to_string())
            }
        }
    }

    async fn call_session_tool(&self, name: &str, args: &Map<String, Value>) -> Value {
        match name {
            "session_create" => {
                let mut request = Map::new();
                if let Some(workspace_cwd) = args
                    .get("workspace_cwd")
                    .cloned()
                    .or_else(|| self.workspace_cwd.as_ref().map(|cwd| json!(cwd)))
                {
                    request.insert("workspaceCwd".into(), workspace_cwd);
                }
                for (source, target) in [
                    ("model_service_id", "modelServiceId"),
                    ("session_id", "sessionId"),
                    ("session_scope", "sessionScope"),
                ] {
                    insert_mapped_field(&mut request, args, source, target);
                }
                match self
                    .client
                    .create_or_attach_session(Value::Object(request), None)
                    .await
                {
                    Ok(value) => {
                        self.activate_session(
                            value
                                .get("sessionId")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                        );
                        json_result(value)
                    }
                    Err(error) => self.daemon_error(error),
                }
            }
            "session_load" | "session_resume" => {
                let mut request = Map::new();
                if let Some(workspace_cwd) = args
                    .get("workspace_cwd")
                    .cloned()
                    .or_else(|| self.workspace_cwd.as_ref().map(|cwd| json!(cwd)))
                {
                    request.insert("workspaceCwd".into(), workspace_cwd);
                }
                let session_id = args["session_id"].as_str().expect("validated session_id");
                let result = if name == "session_load" {
                    self.client
                        .load_session(session_id, Value::Object(request), None)
                        .await
                } else {
                    self.client
                        .resume_session(session_id, Value::Object(request), None)
                        .await
                };
                match result {
                    Ok(value) => {
                        self.activate_session(
                            value
                                .get("sessionId")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                        );
                        json_result(value)
                    }
                    Err(error) => self.daemon_error(error),
                }
            }
            "session_close" => {
                let explicit = args.get("session_id").and_then(Value::as_str);
                let session_id = match self.resolve_session_id(explicit) {
                    Ok(session_id) => session_id,
                    Err(message) => return tool_error(message),
                };
                let result = self.client.close_session(&session_id, None).await;
                self.stop_event_stream(&session_id);
                {
                    let mut default_session_id = self
                        .default_session_id
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if default_session_id.as_deref() == Some(session_id.as_str()) {
                        *default_session_id = None;
                    }
                }
                match result {
                    Ok(()) => json_result(json!({"ok":true,"sessionId":session_id})),
                    Err(error) => self.daemon_error(error),
                }
            }
            "session_update_metadata" => {
                let explicit = args.get("session_id").and_then(Value::as_str);
                let session_id = match self.resolve_session_id(explicit) {
                    Ok(session_id) => session_id,
                    Err(message) => return tool_error(message),
                };
                let mut metadata = Map::new();
                insert_mapped_field(&mut metadata, args, "display_name", "displayName");
                self.finish_daemon_result(
                    self.client
                        .update_session_metadata(&session_id, Value::Object(metadata), None)
                        .await,
                )
            }
            "session_list" => {
                let workspace_cwd = args["workspace_cwd"]
                    .as_str()
                    .expect("validated workspace_cwd");
                match self
                    .client
                    .list_workspace_sessions(workspace_cwd, json!({}))
                    .await
                {
                    Ok(sessions) => json_result(json!({"sessions":sessions})),
                    Err(error) => self.daemon_error(error),
                }
            }
            "session_set_model" => {
                let explicit = args.get("session_id").and_then(Value::as_str);
                let session_id = match self.resolve_session_id(explicit) {
                    Ok(session_id) => session_id,
                    Err(message) => return tool_error(message),
                };
                self.finish_daemon_result(
                    self.client
                        .set_session_model(
                            &session_id,
                            args["model_id"].as_str().expect("validated model_id"),
                            None,
                        )
                        .await,
                )
            }
            "session_context" => {
                let explicit = args.get("session_id").and_then(Value::as_str);
                let session_id = match self.resolve_session_id(explicit) {
                    Ok(session_id) => session_id,
                    Err(message) => return tool_error(message),
                };
                self.finish_daemon_result(self.client.session_context(&session_id, None).await)
            }
            _ => tool_error(format!("Unknown tool: {name}")),
        }
    }

    async fn call_agent_tool(&self, name: &str, args: &Map<String, Value>) -> Value {
        let explicit = args.get("session_id").and_then(Value::as_str);
        let session_id = match self.resolve_session_id(explicit) {
            Ok(session_id) => session_id,
            Err(message) => return tool_error(message),
        };
        let stream = self
            .event_streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&session_id)
            .cloned();

        if name == "prompt_cancel" {
            // Best-effort cancel must not prevent collector resolution.
            let _ = self.client.cancel(&session_id, None).await;
            if let Some(stream) = stream
                && let Some(collector) = stream.collector()
            {
                collector.resolve(true);
            }
            return json_result(json!({"ok":true,"sessionId":session_id}));
        }

        let Some(stream) = stream else {
            return tool_error(
                "No SSE stream for session. Was the session created via session_create?",
            );
        };
        let collector = PromptCollector::new();
        {
            let mut active = stream
                .active_collector
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if active.is_some() {
                return tool_error(
                    "Another prompt is already in progress for this session. Wait for it to complete or call prompt_cancel first.",
                );
            }
            stream.touch();
            *active = Some(Arc::clone(&collector));
        }

        let prompt_result = self
            .client
            .prompt(
                &session_id,
                json!({
                    "prompt":[{"type":"text", "text":args["prompt"]}]
                }),
                Some(collector.request_cancellation.clone()),
                None,
            )
            .await;
        let result = match prompt_result {
            Err(error) => self.daemon_error(error),
            Ok(prompt_result) => {
                let completed = collector.wait(PROMPT_COMPLETION_TIMEOUT).await;
                let snapshot = collector.snapshot();
                if !completed && !snapshot.resolved {
                    // Mirror the source's timeout recovery: best-effort cancel,
                    // then return any text already collected as an error.
                    let _ = self.client.cancel(&session_id, None).await;
                    let snapshot = collector.snapshot();
                    prompt_timeout_result(&session_id, &snapshot.text, snapshot.truncated)
                } else if snapshot.interrupted {
                    prompt_interrupted_result(&session_id, &snapshot.text, snapshot.truncated)
                } else {
                    let mut output = Map::new();
                    output.insert("session_id".into(), json!(session_id));
                    if let Some(stop_reason) = prompt_result.get("stopReason") {
                        output.insert("stop_reason".into(), stop_reason.clone());
                    }
                    output.insert(
                        "response".into(),
                        json!(if snapshot.text.is_empty() {
                            "(task completed, no text output)"
                        } else {
                            snapshot.text.as_str()
                        }),
                    );
                    if snapshot.truncated {
                        output.insert(
                            "warning".into(),
                            json!("Agent response exceeded the 1 MiB bridge collection limit; output was truncated."),
                        );
                        json_tool_result(Value::Object(output), true)
                    } else {
                        json_result(Value::Object(output))
                    }
                }
            }
        };

        {
            let mut active = stream
                .active_collector
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if active
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &collector))
            {
                *active = None;
            }
        }
        result
    }

    async fn call_workspace_write_tool(&self, name: &str, args: &Map<String, Value>) -> Value {
        match name {
            "file_write" => {
                let mode = args["mode"].as_str().expect("validated mode");
                let expected_hash = args.get("expected_hash").and_then(Value::as_str);
                if mode == "replace" && expected_hash.is_none_or(str::is_empty) {
                    return tool_error("expected_hash is required for replace mode.");
                }
                let mut request = Map::new();
                request.insert("path".into(), args["path"].clone());
                request.insert("content".into(), args["content"].clone());
                request.insert("mode".into(), json!(mode));
                if let Some(expected_hash) = expected_hash.filter(|value| !value.is_empty()) {
                    request.insert("expectedHash".into(), json!(expected_hash));
                }
                self.finish_daemon_result(
                    self.client
                        .write_workspace_file(Value::Object(request), None)
                        .await,
                )
            }
            "file_edit" => {
                let request = json!({
                    "path": args["path"],
                    "oldText": args["old_text"],
                    "newText": args["new_text"],
                    "expectedHash": args["expected_hash"]
                });
                self.finish_daemon_result(self.client.edit_workspace_file(request, None).await)
            }
            "session_set_approval_mode" => {
                let mode = args["mode"].as_str().expect("validated mode");
                let persist = args.get("persist").and_then(Value::as_bool) == Some(true);
                if !self.allow_global_scope {
                    if matches!(mode, "yolo" | "auto" | "auto-edit") {
                        return tool_error(
                            "Approval modes 'yolo', 'auto', 'auto-edit' are restricted for security. Set QWEN_BRIDGE_ALLOW_GLOBAL_SCOPE=true to enable.",
                        );
                    }
                    if persist {
                        return tool_error(
                            "Persisting approval mode changes is restricted for security. Set QWEN_BRIDGE_ALLOW_GLOBAL_SCOPE=true to enable.",
                        );
                    }
                }
                let explicit = args.get("session_id").and_then(Value::as_str);
                let session_id = match self.resolve_session_id(explicit) {
                    Ok(session_id) => session_id,
                    Err(message) => return tool_error(message),
                };
                self.finish_daemon_result(
                    self.client
                        .set_session_approval_mode(&session_id, mode, persist, None)
                        .await,
                )
            }
            "workspace_tool_toggle" => {
                if !self.allow_global_scope {
                    return tool_error(
                        "Tool toggling is restricted for security. Set QWEN_BRIDGE_ALLOW_GLOBAL_SCOPE=true to enable.",
                    );
                }
                self.finish_daemon_result(
                    self.client
                        .set_workspace_tool_enabled(
                            args["tool_name"].as_str().expect("validated tool_name"),
                            args["enabled"].as_bool().expect("validated enabled"),
                            None,
                        )
                        .await,
                )
            }
            "workspace_init" => self.finish_daemon_result(
                self.client
                    .init_workspace(
                        args.get("force").and_then(Value::as_bool) == Some(true),
                        None,
                    )
                    .await,
            ),
            "workspace_mcp_restart" => {
                if !self.allow_global_scope {
                    return tool_error(
                        "MCP server restart is restricted for security. Set QWEN_BRIDGE_ALLOW_GLOBAL_SCOPE=true to enable.",
                    );
                }
                self.finish_daemon_result(
                    self.client
                        .restart_mcp_server(
                            args["server_name"].as_str().expect("validated server_name"),
                            None,
                            None,
                            None,
                        )
                        .await,
                )
            }
            "workspace_memory_read" => {
                match self
                    .workspace_get("/workspace/memory", "GET /workspace/memory")
                    .await
                {
                    Ok(value) => json_result(value),
                    Err(error) => self.daemon_error(error),
                }
            }
            "workspace_memory_write" => {
                let scope = args["scope"].as_str().expect("validated scope");
                if scope == "global" && !self.allow_global_scope {
                    return tool_error(
                        "Global scope is disabled for security. Set QWEN_BRIDGE_ALLOW_GLOBAL_SCOPE=true to enable.",
                    );
                }
                let mut request = Map::new();
                request.insert("scope".into(), json!(scope));
                request.insert("content".into(), args["content"].clone());
                if let Some(mode) = args.get("mode") {
                    request.insert("mode".into(), mode.clone());
                }
                self.finish_daemon_result(
                    self.client
                        .write_workspace_memory(Value::Object(request), None)
                        .await,
                )
            }
            "workspace_agents_manage" => self.manage_workspace_agents(args).await,
            _ => tool_error(format!("Unknown tool: {name}")),
        }
    }

    async fn manage_workspace_agents(&self, args: &Map<String, Value>) -> Value {
        let action = args["action"].as_str().expect("validated action");
        let scope = args.get("scope").and_then(Value::as_str);
        match action {
            "list" => match self
                .client
                .request_json(
                    "GET",
                    "/workspace/agents",
                    "",
                    None,
                    "GET /workspace/agents",
                    DaemonRequestOptions::default(),
                )
                .await
            {
                Ok(value) => json_result(value),
                Err(error) => self.daemon_error(error),
            },
            "get" => {
                if !args
                    .get("agent_type")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.is_empty())
                {
                    return tool_error("agent_type is required for get action.");
                }
                self.finish_daemon_result(
                    self.client
                        .get_workspace_agent(
                            args["agent_type"].as_str().expect("checked above"),
                            scope,
                        )
                        .await,
                )
            }
            "create" => {
                if let Some(error) = self.validate_global_scope(scope) {
                    return error;
                }
                let fields = ["name", "description", "system_prompt", "scope"];
                if fields.iter().any(|field| {
                    !args
                        .get(*field)
                        .and_then(Value::as_str)
                        .is_some_and(|value| !value.is_empty())
                }) {
                    return tool_error(
                        "name, description, system_prompt, and scope are required for create action.",
                    );
                }
                let mut request = Map::new();
                insert_mapped_field(&mut request, args, "name", "name");
                insert_mapped_field(&mut request, args, "description", "description");
                insert_mapped_field(&mut request, args, "system_prompt", "systemPrompt");
                insert_mapped_field(&mut request, args, "scope", "scope");
                copy_optional_agent_fields(&mut request, args);
                self.finish_daemon_result(
                    self.client
                        .create_workspace_agent(Value::Object(request), None)
                        .await,
                )
            }
            "update" => {
                if let Some(error) = self.validate_global_scope(scope) {
                    return error;
                }
                if !args
                    .get("agent_type")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.is_empty())
                {
                    return tool_error("agent_type is required for update action.");
                }
                const UPDATABLE_FIELDS: &[&str] = &[
                    "description",
                    "system_prompt",
                    "tools",
                    "disallowed_tools",
                    "model",
                    "approval_mode",
                    "permission_mode",
                    "max_turns",
                    "color",
                    "mcp_servers",
                    "hooks",
                    "background",
                ];
                if !UPDATABLE_FIELDS
                    .iter()
                    .any(|field| args.contains_key(*field))
                {
                    return tool_error(
                        "At least one field to update must be provided (description, system_prompt, tools, disallowed_tools, model, approval_mode, permission_mode, max_turns, color, mcp_servers, hooks, or background).",
                    );
                }
                let mut request = Map::new();
                copy_optional_agent_fields(&mut request, args);
                self.finish_daemon_result(
                    self.client
                        .update_workspace_agent(
                            args["agent_type"].as_str().expect("checked above"),
                            Value::Object(request),
                            scope,
                            None,
                        )
                        .await,
                )
            }
            "delete" => {
                if let Some(error) = self.validate_global_scope(scope) {
                    return error;
                }
                if !args
                    .get("agent_type")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.is_empty())
                {
                    return tool_error("agent_type is required for delete action.");
                }
                let agent_type = args["agent_type"].as_str().expect("checked above");
                match self
                    .client
                    .delete_workspace_agent(agent_type, scope, None)
                    .await
                {
                    Ok(()) => json_result(json!({"ok":true,"deleted":agent_type})),
                    Err(error) => self.daemon_error(error),
                }
            }
            _ => tool_error(format!("Unknown action: {action}")),
        }
    }

    fn resolve_session_id(&self, explicit: Option<&str>) -> Result<String, String> {
        let session_id = explicit.map(str::to_owned).or_else(|| {
            self.default_session_id
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        });
        match session_id {
            Some(session_id) if !session_id.is_empty() => {
                let stream = self
                    .event_streams
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&session_id)
                    .cloned();
                if let Some(stream) = stream {
                    stream.touch();
                }
                Ok(session_id)
            }
            _ => Err(
                "No session active. Call session_create first, or pass an explicit session_id."
                    .into(),
            ),
        }
    }

    fn validate_global_scope(&self, scope: Option<&str>) -> Option<Value> {
        if scope == Some("global") && !self.allow_global_scope {
            Some(tool_error(
                "Global scope is disabled for security. Set QWEN_BRIDGE_ALLOW_GLOBAL_SCOPE=true to enable.",
            ))
        } else {
            None
        }
    }

    fn finish_daemon_result(&self, result: Result<Value, DaemonClientError>) -> Value {
        match result {
            Ok(value) => json_result(value),
            Err(error) => self.daemon_error(error),
        }
    }

    fn daemon_error(&self, error: DaemonClientError) -> Value {
        eprintln!("[serve-bridge] Tool error: {error}");
        tool_error(error.to_string())
    }

    async fn workspace_get(&self, path: &str, label: &str) -> Result<Value, DaemonClientError> {
        self.client
            .request_json(
                "GET",
                path,
                "",
                None,
                label,
                DaemonRequestOptions::default(),
            )
            .await
    }
}

/// Convenience entry point for applications that use the standard bridge
/// environment variables and want to serve directly on the process stdio.
pub async fn run_stdio_from_env() -> Result<(), ServeBridgeError> {
    ServeBridge::from_env()?.run_stdio().await
}

/// Return the tools advertised by this Rust vertical slice.
pub fn tool_catalog() -> Vec<Value> {
    vec![
        tool_definition(
            "health",
            "Check if the qwen serve daemon is alive.",
            empty_schema(),
        ),
        tool_definition(
            "capabilities",
            "Get qwen serve daemon capabilities including protocol versions, mode, features, model services, and workspace CWD.",
            empty_schema(),
        ),
        tool_definition(
            "session_create",
            "Create a new qwen-code session or attach to an existing one. The created session becomes the default for subsequent tool calls.",
            object_schema(
                json!({
                    "workspace_cwd": string_schema("Workspace path. Defaults to daemon primary workspace."),
                    "model_service_id": string_schema("Model service to use."),
                    "session_id": string_schema("UUID v1-v5 to assign to the new session."),
                    "session_scope": enum_schema(&["single", "thread"], "Session scope.")
                }),
                &[],
            ),
        ),
        tool_definition(
            "session_load",
            "Restore a persisted session with SSE history replay. Sets the loaded session as the default.",
            object_schema(
                json!({
                    "session_id": string_schema("Session ID to restore."),
                    "workspace_cwd": string_schema("Workspace path.")
                }),
                &["session_id"],
            ),
        ),
        tool_definition(
            "session_resume",
            "Restore a session without history replay. Sets the resumed session as the default.",
            object_schema(
                json!({
                    "session_id": string_schema("Session ID to resume."),
                    "workspace_cwd": string_schema("Workspace path.")
                }),
                &["session_id"],
            ),
        ),
        tool_definition(
            "session_close",
            "Force-close a live session.",
            object_schema(
                json!({
                    "session_id": string_schema("Session ID. Uses default session if omitted.")
                }),
                &[],
            ),
        ),
        tool_definition(
            "session_update_metadata",
            "Update session metadata such as display name.",
            object_schema(
                json!({
                    "session_id": string_schema("Session ID. Uses default session if omitted."),
                    "display_name": string_schema("New display name for the session.")
                }),
                &[],
            ),
        ),
        tool_definition(
            "session_list",
            "List live sessions for a workspace.",
            object_schema(
                json!({
                    "workspace_cwd": string_schema("Workspace path to list sessions for.")
                }),
                &["workspace_cwd"],
            ),
        ),
        tool_definition(
            "session_set_model",
            "Switch the active model for a session.",
            object_schema(
                json!({
                    "model_id": string_schema("Model ID to switch to."),
                    "session_id": string_schema("Session ID. Uses default session if omitted.")
                }),
                &["model_id"],
            ),
        ),
        tool_definition(
            "session_context",
            "Get the current session model/mode/config state.",
            object_schema(
                json!({
                    "session_id": string_schema("Session ID. Uses default session if omitted.")
                }),
                &[],
            ),
        ),
        tool_definition(
            "prompt",
            "Send a prompt to the qwen-code agent and wait for the full response. This tool blocks until the agent completes processing, which may take minutes for complex tasks. After the HTTP response returns, a 30s collection timeout guards against missing completion signals — if the SSE completion event is not received within 30s, partial text is returned with an error. Do not set a short client-side timeout.",
            object_schema(
                json!({
                    "prompt": string_schema("The prompt text to send to the agent."),
                    "session_id": string_schema("Session ID. Uses default session if omitted.")
                }),
                &["prompt"],
            ),
        ),
        tool_definition(
            "prompt_cancel",
            "Cancel the currently active prompt in a session.",
            object_schema(
                json!({
                    "session_id": string_schema("Session ID. Uses default session if omitted.")
                }),
                &[],
            ),
        ),
        tool_definition(
            "file_read",
            "Read a text file from the workspace. Returns content and SHA-256 hash.",
            object_schema(
                json!({
                    "path": string_schema("File path (relative to workspace root)."),
                    "max_bytes": number_schema("Maximum bytes to read."),
                    "line": number_schema("Starting line number."),
                    "limit": number_schema("Number of lines to read."),
                    "cursor": string_schema("Resume token from a previous read's nextCursor. Reaches any point in the file in constant time, unlike a large `line` offset.")
                }),
                &["path"],
            ),
        ),
        tool_definition(
            "file_read_bytes",
            "Read raw bytes from a file as base64. For binary or bounded reads.",
            object_schema(
                json!({
                    "path": string_schema("File path (relative to workspace root)."),
                    "offset": number_schema("Byte offset to start reading."),
                    "max_bytes": number_schema("Maximum bytes to read.")
                }),
                &["path"],
            ),
        ),
        tool_definition(
            "file_stat",
            "Get file or directory metadata (size, timestamps, type).",
            object_schema(
                json!({"path": string_schema("File path to stat.")}),
                &["path"],
            ),
        ),
        tool_definition(
            "dir_list",
            "List files and directories in a workspace directory (max 2000 entries).",
            object_schema(
                json!({"path": string_schema("Directory path to list.")}),
                &["path"],
            ),
        ),
        tool_definition(
            "glob",
            "Find files matching a glob pattern in the workspace (max 5000 results).",
            object_schema(
                json!({"pattern": string_schema("Glob pattern (e.g. \"**/*.ts\", \"src/**/*.js\").")}),
                &["pattern"],
            ),
        ),
        tool_definition(
            "workspace_mcp_status",
            "Get MCP server status including discovery state, server list, budgets.",
            empty_schema(),
        ),
        tool_definition(
            "workspace_skills",
            "List available skills in the workspace.",
            empty_schema(),
        ),
        tool_definition(
            "workspace_providers",
            "Get model provider status including current provider and available models.",
            empty_schema(),
        ),
        tool_definition(
            "workspace_env",
            "Get daemon runtime environment snapshot (platform, sandbox, proxy, env var presence). Never leaks secret values.",
            empty_schema(),
        ),
        tool_definition(
            "workspace_preflight",
            "Run readiness checks. Daemon-level cells always populated; ACP-level cells show not_started when idle.",
            empty_schema(),
        ),
        tool_definition(
            "file_write",
            "Create or replace a text file in the workspace. Supports hash-verified atomic writes.",
            object_schema(
                json!({
                    "path": string_schema("File path (relative to workspace root)."),
                    "content": string_schema("File content to write."),
                    "mode": enum_schema(&["create", "replace"], "\"create\" for new files, \"replace\" for existing."),
                    "expected_hash": string_schema("Expected SHA-256 hash for replace mode (required for replace).")
                }),
                &["path", "content", "mode"],
            ),
        ),
        tool_definition(
            "file_edit",
            "Make a single text replacement in a file. Requires exact-once match of old_text.",
            object_schema(
                json!({
                    "path": string_schema("File path."),
                    "old_text": string_schema("Text to find (must match exactly once)."),
                    "new_text": string_schema("Replacement text."),
                    "expected_hash": string_schema("Expected SHA-256 hash of the current file.")
                }),
                &["path", "old_text", "new_text", "expected_hash"],
            ),
        ),
        tool_definition(
            "session_set_approval_mode",
            "Change the approval mode of a session (plan, default, auto-edit, auto, yolo).",
            object_schema(
                json!({
                    "mode": enum_schema(&["plan", "default", "auto-edit", "auto", "yolo"], "Approval mode."),
                    "persist": boolean_schema("Also write to workspace settings file."),
                    "session_id": string_schema("Session ID. Uses default session if omitted.")
                }),
                &["mode"],
            ),
        ),
        tool_definition(
            "workspace_tool_toggle",
            "Enable or disable a tool in the workspace settings.",
            object_schema(
                json!({
                    "tool_name": string_schema("Name of the tool to toggle."),
                    "enabled": boolean_schema("Whether to enable (true) or disable (false) the tool.")
                }),
                &["tool_name", "enabled"],
            ),
        ),
        tool_definition(
            "workspace_init",
            "Scaffold an empty QWEN.md at the workspace root. No LLM invocation.",
            object_schema(
                json!({"force": boolean_schema("Overwrite existing QWEN.md if present.")}),
                &[],
            ),
        ),
        tool_definition(
            "workspace_mcp_restart",
            "Restart a configured MCP server. Pre-checks budget before restarting.",
            object_schema(
                json!({"server_name": string_schema("Name of the MCP server to restart.")}),
                &["server_name"],
            ),
        ),
        tool_definition(
            "workspace_memory_read",
            "Read workspace memory (QWEN.md hierarchy).",
            empty_schema(),
        ),
        tool_definition(
            "workspace_memory_write",
            "Write to workspace memory (QWEN.md). Supports append or replace mode.",
            object_schema(
                json!({
                    "scope": enum_schema(&["workspace", "global"], "Memory scope."),
                    "content": string_schema("Content to write."),
                    "mode": enum_schema(&["append", "replace"], "Write mode (default: append).")
                }),
                &["scope", "content"],
            ),
        ),
        tool_definition(
            "workspace_agents_manage",
            "Manage workspace agent definitions. Use action to list, get, create, update, or delete agents.",
            object_schema(
                json!({
                    "action": enum_schema(&["list", "get", "create", "update", "delete"], "CRUD action to perform."),
                    "agent_type": string_schema("Agent type name (required for get/update/delete)."),
                    "name": string_schema("Agent name (create only, required for create)."),
                    "description": string_schema("Agent description (required for create)."),
                    "system_prompt": string_schema("System prompt (required for create)."),
                    "scope": enum_schema(&["workspace", "global"], "Agent scope (required for create)."),
                    "tools": string_array_schema("Allowed tool names."),
                    "disallowed_tools": string_array_schema("Disallowed tool names."),
                    "model": string_schema("Model ID for the agent."),
                    "approval_mode": string_schema("Approval mode."),
                    "permission_mode": string_schema("Permission mode."),
                    "max_turns": json!({"type":"integer", "exclusiveMinimum":0}),
                    "color": plain_string_schema(),
                    "mcp_servers": record_schema(),
                    "hooks": record_schema(),
                    "background": plain_boolean_schema()
                }),
                &["action"],
            ),
        ),
    ]
}

fn tool_definition(name: &str, description: &str, input_schema: Value) -> Value {
    json!({"name":name, "description":description, "inputSchema":input_schema})
}

fn empty_schema() -> Value {
    object_schema(json!({}), &[])
}

fn object_schema(properties: Value, required: &[&str]) -> Value {
    let mut schema = json!({
        "type": "object",
        "properties": properties,
        "additionalProperties": false
    });
    if !required.is_empty() {
        schema["required"] = json!(required);
    }
    schema
}

fn string_schema(description: &str) -> Value {
    json!({"type":"string", "description":description})
}

fn plain_string_schema() -> Value {
    json!({"type":"string"})
}

fn number_schema(description: &str) -> Value {
    json!({"type":"number", "description":description})
}

fn enum_schema(values: &[&str], description: &str) -> Value {
    json!({"type":"string", "enum":values, "description":description})
}

fn boolean_schema(description: &str) -> Value {
    json!({"type":"boolean", "description":description})
}

fn plain_boolean_schema() -> Value {
    json!({"type":"boolean"})
}

fn string_array_schema(description: &str) -> Value {
    json!({
        "type":"array",
        "items":{"type":"string"},
        "description":description
    })
}

fn record_schema() -> Value {
    json!({"type":"object", "additionalProperties":{}})
}

fn is_workspace_write_tool(name: &str) -> bool {
    matches!(
        name,
        "file_write"
            | "file_edit"
            | "session_set_approval_mode"
            | "workspace_tool_toggle"
            | "workspace_init"
            | "workspace_mcp_restart"
            | "workspace_memory_read"
            | "workspace_memory_write"
            | "workspace_agents_manage"
    )
}

fn is_session_tool(name: &str) -> bool {
    matches!(
        name,
        "session_create"
            | "session_load"
            | "session_resume"
            | "session_close"
            | "session_update_metadata"
            | "session_list"
            | "session_set_model"
            | "session_context"
    )
}

fn is_agent_tool(name: &str) -> bool {
    matches!(name, "prompt" | "prompt_cancel")
}

fn is_prompt_cancel_frame(frame: &[u8]) -> bool {
    let Ok(request) = serde_json::from_slice::<Value>(frame) else {
        return false;
    };
    request.get("method").and_then(Value::as_str) == Some("tools/call")
        && request
            .get("params")
            .and_then(|params| params.get("name"))
            .and_then(Value::as_str)
            == Some("prompt_cancel")
}

fn overloaded_request_error(frame: &[u8]) -> Option<Value> {
    let request = serde_json::from_slice::<Value>(frame).ok()?;
    let id = request.get("id")?.clone();
    Some(json_rpc_error(
        id,
        -32000,
        "Serve bridge is at its in-flight request limit",
    ))
}

fn insert_mapped_field(
    destination: &mut Map<String, Value>,
    source: &Map<String, Value>,
    source_name: &str,
    destination_name: &str,
) {
    if let Some(value) = source.get(source_name) {
        destination.insert(destination_name.to_owned(), value.clone());
    }
}

fn copy_optional_agent_fields(destination: &mut Map<String, Value>, source: &Map<String, Value>) {
    for (source_name, destination_name) in [
        ("tools", "tools"),
        ("disallowed_tools", "disallowedTools"),
        ("model", "model"),
        ("approval_mode", "approvalMode"),
        ("permission_mode", "permissionMode"),
        ("max_turns", "maxTurns"),
        ("color", "color"),
        ("mcp_servers", "mcpServers"),
        ("hooks", "hooks"),
        ("background", "background"),
    ] {
        insert_mapped_field(destination, source, source_name, destination_name);
    }
}

fn validate_tool_arguments(name: &str, arguments: Value) -> Result<Map<String, Value>, String> {
    let Some(mut args) = arguments.as_object().cloned() else {
        return Err("Invalid arguments: expected an object".into());
    };
    let (
        required_strings,
        optional_strings,
        optional_numbers,
        required_booleans,
        optional_booleans,
        array_fields,
        record_fields,
    ): (
        &[&str],
        &[&str],
        &[&str],
        &[&str],
        &[&str],
        &[&str],
        &[&str],
    ) = match name {
        "health"
        | "capabilities"
        | "workspace_mcp_status"
        | "workspace_skills"
        | "workspace_providers"
        | "workspace_env"
        | "workspace_preflight"
        | "workspace_memory_read" => (&[], &[], &[], &[], &[], &[], &[]),
        "prompt" => (&["prompt"], &["session_id"], &[], &[], &[], &[], &[]),
        "prompt_cancel" => (&[], &["session_id"], &[], &[], &[], &[], &[]),
        "session_create" => (
            &[],
            &[
                "workspace_cwd",
                "model_service_id",
                "session_id",
                "session_scope",
            ],
            &[],
            &[],
            &[],
            &[],
            &[],
        ),
        "session_load" | "session_resume" => {
            (&["session_id"], &["workspace_cwd"], &[], &[], &[], &[], &[])
        }
        "session_close" | "session_context" => (&[], &["session_id"], &[], &[], &[], &[], &[]),
        "session_update_metadata" => (
            &[],
            &["session_id", "display_name"],
            &[],
            &[],
            &[],
            &[],
            &[],
        ),
        "session_list" => (&["workspace_cwd"], &[], &[], &[], &[], &[], &[]),
        "session_set_model" => (&["model_id"], &["session_id"], &[], &[], &[], &[], &[]),
        "file_read" => (
            &["path"],
            &["cursor"],
            &["max_bytes", "line", "limit"],
            &[],
            &[],
            &[],
            &[],
        ),
        "file_read_bytes" => (&["path"], &[], &["offset", "max_bytes"], &[], &[], &[], &[]),
        "file_stat" | "dir_list" => (&["path"], &[], &[], &[], &[], &[], &[]),
        "glob" => (&["pattern"], &[], &[], &[], &[], &[], &[]),
        "file_write" => (
            &["path", "content", "mode"],
            &["expected_hash"],
            &[],
            &[],
            &[],
            &[],
            &[],
        ),
        "file_edit" => (
            &["path", "old_text", "new_text", "expected_hash"],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
        ),
        "session_set_approval_mode" => {
            (&["mode"], &["session_id"], &[], &[], &["persist"], &[], &[])
        }
        "workspace_tool_toggle" => (&["tool_name"], &[], &[], &["enabled"], &[], &[], &[]),
        "workspace_init" => (&[], &[], &[], &[], &["force"], &[], &[]),
        "workspace_mcp_restart" => (&["server_name"], &[], &[], &[], &[], &[], &[]),
        "workspace_memory_write" => (&["scope", "content"], &["mode"], &[], &[], &[], &[], &[]),
        "workspace_agents_manage" => (
            &["action"],
            &[
                "agent_type",
                "name",
                "description",
                "system_prompt",
                "scope",
                "model",
                "approval_mode",
                "permission_mode",
                "color",
            ],
            &["max_turns"],
            &[],
            &["background"],
            &["tools", "disallowed_tools"],
            &["mcp_servers", "hooks"],
        ),
        _ => return Err(format!("Unknown tool: {name}")),
    };

    for field in required_strings {
        if !args.get(*field).is_some_and(Value::is_string) {
            return Err(format!("Invalid arguments: '{field}' must be a string"));
        }
    }
    for field in optional_strings {
        if args.contains_key(*field) && !args.get(*field).is_some_and(Value::is_string) {
            return Err(format!("Invalid arguments: '{field}' must be a string"));
        }
    }
    for field in optional_numbers {
        if args.contains_key(*field) && !args.get(*field).is_some_and(Value::is_number) {
            return Err(format!("Invalid arguments: '{field}' must be a number"));
        }
    }
    for field in required_booleans {
        if !args.get(*field).is_some_and(Value::is_boolean) {
            return Err(format!("Invalid arguments: '{field}' must be a boolean"));
        }
    }
    for field in optional_booleans {
        if args.contains_key(*field) && !args.get(*field).is_some_and(Value::is_boolean) {
            return Err(format!("Invalid arguments: '{field}' must be a boolean"));
        }
    }
    for field in array_fields {
        if args.contains_key(*field)
            && !args
                .get(*field)
                .and_then(Value::as_array)
                .is_some_and(|values| values.iter().all(Value::is_string))
        {
            return Err(format!(
                "Invalid arguments: '{field}' must be an array of strings"
            ));
        }
    }
    for field in record_fields {
        if args.contains_key(*field) && !args.get(*field).is_some_and(Value::is_object) {
            return Err(format!("Invalid arguments: '{field}' must be an object"));
        }
    }

    let enum_fields: &[(&str, &[&str])] = match name {
        "file_write" => &[("mode", &["create", "replace"])],
        "session_create" => &[("session_scope", &["single", "thread"])],
        "session_set_approval_mode" => {
            &[("mode", &["plan", "default", "auto-edit", "auto", "yolo"])]
        }
        "workspace_memory_write" => &[
            ("scope", &["workspace", "global"]),
            ("mode", &["append", "replace"]),
        ],
        "workspace_agents_manage" => &[
            ("action", &["list", "get", "create", "update", "delete"]),
            ("scope", &["workspace", "global"]),
        ],
        _ => &[],
    };
    for (field, values) in enum_fields {
        if args.contains_key(*field)
            && !args
                .get(*field)
                .and_then(Value::as_str)
                .is_some_and(|value| values.iter().any(|allowed| *allowed == value))
        {
            return Err(format!(
                "Invalid arguments: '{field}' must be one of {}",
                values.join(", ")
            ));
        }
    }
    if name == "workspace_agents_manage"
        && let Some(value) = args.get("max_turns")
        && !value
            .as_f64()
            .is_some_and(|number| number.is_finite() && number.fract() == 0.0 && number > 0.0)
    {
        return Err("Invalid arguments: 'max_turns' must be a positive integer".into());
    }

    // Zod object schemas use the default strip mode. Discard unknown keys
    // before passing arguments to the daemon methods.
    let known = required_strings
        .iter()
        .chain(optional_strings)
        .chain(optional_numbers)
        .chain(required_booleans)
        .chain(optional_booleans)
        .chain(array_fields)
        .chain(record_fields)
        .copied()
        .collect::<Vec<_>>();
    args.retain(|key, _| known.contains(&key.as_str()));
    Ok(args)
}

fn json_result(value: Value) -> Value {
    json_tool_result(value, false)
}

fn json_tool_result(value: Value, is_error: bool) -> Value {
    let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| "null".into());
    let mut result = json!({"content":[{"type":"text", "text":text}]});
    if is_error {
        result["isError"] = json!(true);
    }
    result
}

fn prompt_timeout_result(session_id: &str, partial_text: &str, truncated: bool) -> Value {
    let warning = if truncated {
        "Agent response may be incomplete. _meta event not received within 30s. Collected text was truncated at 1 MiB."
    } else {
        "Agent response may be incomplete. _meta event not received within 30s."
    };
    json_tool_result(
        json!({
            "session_id":session_id,
            "stop_reason":"timeout",
            "response":if partial_text.is_empty() { "(no text received)" } else { partial_text },
            "warning":warning
        }),
        true,
    )
}

fn prompt_interrupted_result(session_id: &str, partial_text: &str, truncated: bool) -> Value {
    let warning = if truncated {
        "SSE stream was closed before the response completed. Collected text was truncated at 1 MiB."
    } else {
        "SSE stream was closed before the response completed."
    };
    json_tool_result(
        json!({
            "session_id":session_id,
            "stop_reason":"interrupted",
            "response":if partial_text.is_empty() { "(no text received)" } else { partial_text },
            "warning":warning
        }),
        true,
    )
}

fn tool_error(message: impl Into<String>) -> Value {
    json!({"content":[{"type":"text", "text":message.into()}], "isError":true})
}

fn json_rpc_error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc":"2.0",
        "id":id,
        "error":{"code":code,"message":message.into()}
    })
}

fn javascript_number_string(value: &Value) -> String {
    if let Some(number) = value.as_i64() {
        return number.to_string();
    }
    if let Some(number) = value.as_u64() {
        return number.to_string();
    }
    let number = value.as_f64().unwrap_or_default();
    if number == 0.0 {
        return "0".into();
    }
    if number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_991.0 {
        return format!("{number:.0}");
    }
    number.to_string()
}

enum StdioFrame {
    Line(Vec<u8>),
    TooLarge,
}

async fn read_bounded_frame<R>(reader: &mut R, limit: usize) -> io::Result<Option<StdioFrame>>
where
    R: AsyncBufRead + Unpin,
{
    let mut bytes = Vec::with_capacity(4096);
    let mut oversized = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if oversized {
                return Ok(Some(StdioFrame::TooLarge));
            }
            if bytes.is_empty() {
                return Ok(None);
            }
            return Ok(Some(StdioFrame::Line(bytes)));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        if !oversized {
            if bytes.len().saturating_add(consumed) > limit {
                oversized = true;
                bytes.clear();
            } else {
                bytes.extend_from_slice(&available[..consumed]);
            }
        }
        reader.consume(consumed);
        if newline.is_some() {
            if oversized {
                return Ok(Some(StdioFrame::TooLarge));
            }
            if bytes.last() == Some(&b'\n') {
                bytes.pop();
            }
            return Ok(Some(StdioFrame::Line(bytes)));
        }
    }
}

async fn write_json_line<W>(writer: &mut W, mut message: Value) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut bytes = serde_json::to_vec(&message)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if bytes.len() > MAX_STDIO_FRAME_BYTES {
        message = json_rpc_error(
            message.get("id").cloned().unwrap_or(Value::Null),
            -32603,
            "Response exceeds the 8 MiB frame limit",
        );
        bytes = serde_json::to_vec(&message)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    }
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await
}
