//! Shared child-process ACP channel and JSON-RPC session factory.
//!
//! A bridge runtime creates one factory per workspace. The factory coalesces
//! channel startup, performs `initialize` once, multiplexes `session/new`
//! calls over the resulting connection, and returns per-session agents that
//! issue `session/prompt`, `session/cancel`, and the Canopy session-close
//! extension method.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use serde_json::{Map, Value, json};
use tokio::sync::{Mutex, watch};

use super::bridge_client::{AcpRpcConnection, AcpRpcHandle};
use super::client_dispatch::{AcpBridgeClient, AcpClientDispatchOptions, run_client_dispatch};
use super::ndjson::NdJsonStreamLimits;
use super::process_registry::{ProcessRegistry, TrackedChildProcess};
use super::session_runtime::{
    BridgeRestoreAction, BridgeRestoreRequest, BridgeSessionAgent, BridgeSessionFactory,
    BridgeSessionFactoryError, BridgeSessionScope, BridgeSpawnRequest, CreatedBridgeSession,
    SessionRuntimeFuture,
};
pub use super::session_source::SESSION_SOURCE_META_KEY;
use super::spawn_channel::{
    DAEMON_ACP_MAX_FRAME_BYTES, DAEMON_ACP_MAX_QUEUED_BYTES, DAEMON_ACP_MAX_QUEUED_MESSAGES,
    SpawnChannelOptions, spawn_acp_channel,
};
pub use super::timeouts::{DEFAULT_SESSION_RESTORE_TIMEOUT_MS, MAX_SESSION_RESTORE_TIMEOUT_MS};

pub const REQUESTED_SESSION_ID_META_KEY: &str = "qwen-code/sessionId";
pub const WORKTREE_MCP_DEFER_META_KEY: &str = "qwen.session.deferMcpDiscovery";
pub const SESSION_CLOSE_EXT_METHOD: &str = "qwen/control/session/close";
pub const DEFAULT_CHANNEL_INITIALIZE_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_SESSION_NEW_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_SESSION_CLOSE_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_CHANNEL_IDLE_TIMEOUT_MS: u64 = 30 * 60 * 1_000;
pub const LOAD_REPLAY_MODE_META_KEY: &str = "qwen.session.loadReplayMode";
pub const LOAD_REPLAY_PAGE_SIZE_META_KEY: &str = "qwen.session.loadReplayPageSize";
pub const LOAD_REPLAY_HIDE_INHERITED_META_KEY: &str = "qwen.session.loadReplayHideInherited";
pub const LOAD_REPLAY_BULK_MODE: &str = "bulk";

pub type TranscriptPathResolver = dyn Fn(&str) -> Option<PathBuf> + Send + Sync + 'static;

#[derive(Clone)]
pub struct RpcSessionFactoryOptions {
    pub spawn: SpawnChannelOptions,
    pub initialize_params: Value,
    pub ndjson_limits: NdJsonStreamLimits,
    pub dispatch: AcpClientDispatchOptions,
    pub initialize_timeout_ms: u64,
    pub session_new_timeout_ms: u64,
    pub session_restore_timeout_ms: u64,
    pub session_close_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub transcript_path: Option<Arc<TranscriptPathResolver>>,
}

impl RpcSessionFactoryOptions {
    pub fn new(spawn: SpawnChannelOptions, initialize_params: Value) -> Self {
        Self {
            spawn,
            initialize_params,
            ndjson_limits: NdJsonStreamLimits {
                max_frame_bytes: DAEMON_ACP_MAX_FRAME_BYTES,
                max_queued_messages: DAEMON_ACP_MAX_QUEUED_MESSAGES,
                max_queued_bytes: DAEMON_ACP_MAX_QUEUED_BYTES,
            },
            dispatch: AcpClientDispatchOptions::default(),
            initialize_timeout_ms: DEFAULT_CHANNEL_INITIALIZE_TIMEOUT_MS,
            session_new_timeout_ms: DEFAULT_SESSION_NEW_TIMEOUT_MS,
            session_restore_timeout_ms: DEFAULT_SESSION_RESTORE_TIMEOUT_MS,
            session_close_timeout_ms: DEFAULT_SESSION_CLOSE_TIMEOUT_MS,
            idle_timeout_ms: DEFAULT_CHANNEL_IDLE_TIMEOUT_MS,
            transcript_path: None,
        }
    }
}

struct SharedAcpChannel {
    rpc: AcpRpcHandle,
    process: TrackedChildProcess,
    exited: StdMutex<watch::Receiver<Option<super::channel::AcpChannelExitInfo>>>,
    failed: StdMutex<watch::Receiver<Option<super::channel::ChannelFailure>>>,
    sessions: AtomicUsize,
    in_flight_session_work: AtomicUsize,
    restore_blocked: AtomicBool,
    idle_generation: AtomicU64,
    closing: AtomicBool,
}

impl SharedAcpChannel {
    fn is_live(&self) -> bool {
        !self.closing.load(Ordering::Acquire)
            && self
                .exited
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .borrow()
                .is_none()
            && self
                .failed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .borrow()
                .is_none()
    }
}

/// Concrete one-child/many-session ACP factory. The `initialize_params` value
/// is supplied by the host so protocol-version and capability metadata stay
/// aligned with that host's ACP SDK/configuration.
pub struct RpcBridgeSessionFactory {
    options: RpcSessionFactoryOptions,
    process_registry: ProcessRegistry,
    client: Arc<dyn AcpBridgeClient>,
    channel: Mutex<Option<Arc<SharedAcpChannel>>>,
}

impl RpcBridgeSessionFactory {
    pub fn new(
        options: RpcSessionFactoryOptions,
        process_registry: ProcessRegistry,
        client: Arc<dyn AcpBridgeClient>,
    ) -> Self {
        Self {
            options,
            process_registry,
            client,
            channel: Mutex::new(None),
        }
    }

    async fn ensure_channel(&self, workspace_cwd: &Path) -> Result<Arc<SharedAcpChannel>, String> {
        let mut channel_slot = self.channel.lock().await;
        if let Some(channel) = channel_slot.as_ref().cloned() {
            if channel.is_live() && !channel.restore_blocked.load(Ordering::Acquire) {
                channel.idle_generation.fetch_add(1, Ordering::AcqRel);
                return Ok(channel);
            }
            if channel.is_live()
                && channel.restore_blocked.load(Ordering::Acquire)
                && (channel.sessions.load(Ordering::Acquire) > 0
                    || channel.in_flight_session_work.load(Ordering::Acquire) > 0)
            {
                return Err("ACP channel is fenced after a timed-out session restore".into());
            }
            if channel.is_live()
                && channel.restore_blocked.load(Ordering::Acquire)
                && !channel.closing.swap(true, Ordering::AcqRel)
            {
                channel.process.terminate().await.map_err(|error| {
                    format!("failed to reap timed-out restore channel: {error}")
                })?;
            }
        }
        *channel_slot = None;
        let mut spawn_options = self.options.spawn.clone();
        spawn_options.workspace_cwd = workspace_cwd.to_path_buf();
        let child = spawn_acp_channel(spawn_options, &self.process_registry)
            .await
            .map_err(|error| format!("ACP channel spawn failed: {error}"))?;
        let process = child.process.clone();
        let mut connection = AcpRpcConnection::new(child, self.options.ndjson_limits)
            .map_err(|error| format!("ACP connection setup failed: {error}"))?;
        let rpc = connection.handle();
        let exited = connection.exited();
        let failed = connection.transport_failed();
        let client = self.client.clone();
        let dispatch_options = self.options.dispatch.clone();
        tokio::spawn(async move {
            let _ = run_client_dispatch(&mut connection, client, dispatch_options).await;
        });
        match rpc
            .request(
                "initialize",
                self.options.initialize_params.clone(),
                self.options.initialize_timeout_ms,
            )
            .await
        {
            Ok(_response) => {
                let channel = Arc::new(SharedAcpChannel {
                    rpc,
                    process,
                    exited: StdMutex::new(exited),
                    failed: StdMutex::new(failed),
                    sessions: AtomicUsize::new(0),
                    in_flight_session_work: AtomicUsize::new(0),
                    restore_blocked: AtomicBool::new(false),
                    idle_generation: AtomicU64::new(0),
                    closing: AtomicBool::new(false),
                });
                *channel_slot = Some(channel.clone());
                Ok(channel)
            }
            Err(error) => {
                let _ = rpc.kill().await;
                Err(format!("ACP initialize failed: {error}"))
            }
        }
    }

    pub async fn shutdown_channel(&self) -> Result<(), String> {
        let channel = self.channel.lock().await.take();
        if let Some(channel) = channel {
            channel.closing.store(true, Ordering::Release);
            channel.process.terminate().await?;
        }
        Ok(())
    }

    pub fn kill_channel_sync(&self) {
        if let Ok(channel) = self.channel.try_lock()
            && let Some(channel) = channel.as_ref()
        {
            channel.closing.store(true, Ordering::Release);
            channel.process.kill_sync();
        }
        self.process_registry.kill_all_sync();
    }
}

impl BridgeSessionFactory for RpcBridgeSessionFactory {
    fn spawn<'a>(
        &'a self,
        request: BridgeSpawnRequest,
        workspace_cwd: PathBuf,
        _effective_scope: BridgeSessionScope,
    ) -> SessionRuntimeFuture<'a, Result<CreatedBridgeSession, String>> {
        Box::pin(async move {
            let channel = self.ensure_channel(&workspace_cwd).await?;
            channel
                .in_flight_session_work
                .fetch_add(1, Ordering::AcqRel);
            channel.idle_generation.fetch_add(1, Ordering::AcqRel);
            let work_guard = SessionWorkGuard {
                channel: channel.clone(),
                idle_timeout_ms: self.options.idle_timeout_ms,
            };
            let session_result = async {
                let params = session_new_params(&request, &workspace_cwd);
                let response = channel
                    .rpc
                    .request("session/new", params, self.options.session_new_timeout_ms)
                    .await
                    .map_err(|error| format!("ACP session/new failed: {error}"))?;
                let session_id = response
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| "ACP session/new response did not contain sessionId".to_owned())?
                    .to_owned();
                channel.sessions.fetch_add(1, Ordering::AcqRel);
                channel.idle_generation.fetch_add(1, Ordering::AcqRel);
                let transcript_path = self
                    .options
                    .transcript_path
                    .as_ref()
                    .and_then(|resolve| resolve(&session_id));
                Ok(CreatedBridgeSession {
                    session_id: session_id.clone(),
                    effective_cwd: workspace_cwd,
                    created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
                    transcript_path,
                    restore_state: None,
                    agent: Arc::new(RpcBridgeSessionAgent {
                        session_id,
                        channel: channel.clone(),
                        close_timeout_ms: self.options.session_close_timeout_ms,
                        idle_timeout_ms: self.options.idle_timeout_ms,
                        closed: AtomicBool::new(false),
                    }),
                })
            }
            .await;
            drop(work_guard);
            session_result
        })
    }

    fn restore<'a>(
        &'a self,
        request: BridgeRestoreRequest,
        action: BridgeRestoreAction,
        workspace_cwd: PathBuf,
    ) -> SessionRuntimeFuture<'a, Result<CreatedBridgeSession, BridgeSessionFactoryError>> {
        Box::pin(async move {
            let channel = self
                .ensure_channel(&workspace_cwd)
                .await
                .map_err(BridgeSessionFactoryError::Operation)?;
            channel
                .in_flight_session_work
                .fetch_add(1, Ordering::AcqRel);
            channel.idle_generation.fetch_add(1, Ordering::AcqRel);
            let work_guard = SessionWorkGuard {
                channel: channel.clone(),
                idle_timeout_ms: self.options.idle_timeout_ms,
            };
            let (method, params) = session_restore_request(&request, action, &workspace_cwd);
            let restore_timeout_ms = self
                .options
                .session_restore_timeout_ms
                .clamp(1, MAX_SESSION_RESTORE_TIMEOUT_MS);
            let restored = channel
                .rpc
                .request(method, params, restore_timeout_ms)
                .await;
            let result = match restored {
                Ok(restore_state) => {
                    channel.sessions.fetch_add(1, Ordering::AcqRel);
                    channel.idle_generation.fetch_add(1, Ordering::AcqRel);
                    let transcript_path = self
                        .options
                        .transcript_path
                        .as_ref()
                        .and_then(|resolve| resolve(&request.session_id));
                    Ok(CreatedBridgeSession {
                        session_id: request.session_id.clone(),
                        effective_cwd: workspace_cwd,
                        created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
                        transcript_path,
                        restore_state: Some(restore_state),
                        agent: Arc::new(RpcBridgeSessionAgent {
                            session_id: request.session_id,
                            channel: channel.clone(),
                            close_timeout_ms: self.options.session_close_timeout_ms,
                            idle_timeout_ms: self.options.idle_timeout_ms,
                            closed: AtomicBool::new(false),
                        }),
                    })
                }
                Err(super::bridge_client::AcpRpcError::Timeout(timeout_ms)) => {
                    let no_other_channel_work = channel.sessions.load(Ordering::Acquire) == 0
                        && channel.in_flight_session_work.load(Ordering::Acquire) == 1;
                    if no_other_channel_work {
                        if !channel.closing.swap(true, Ordering::AcqRel) {
                            let _ = channel.process.terminate().await;
                        }
                    } else {
                        channel.restore_blocked.store(true, Ordering::Release);
                    }
                    Err(BridgeSessionFactoryError::Timeout(timeout_ms))
                }
                Err(error) => Err(BridgeSessionFactoryError::Operation(format!(
                    "ACP {} failed: {error}",
                    restore_action_label(action)
                ))),
            };
            drop(work_guard);
            result
        })
    }

    fn shutdown<'a>(&'a self) -> SessionRuntimeFuture<'a, Result<(), String>> {
        Box::pin(async move { self.shutdown_channel().await })
    }

    fn kill_all_sync(&self) {
        self.kill_channel_sync();
    }
}

fn session_new_params(request: &BridgeSpawnRequest, workspace_cwd: &Path) -> Value {
    let mut metadata = Map::new();
    if let Some(source_type) = request.source_type.as_deref() {
        let mut source = Map::new();
        source.insert("sourceType".into(), json!(source_type));
        if let Some(source_id) = request.source_id.as_deref() {
            source.insert("sourceId".into(), json!(source_id));
        }
        metadata.insert(SESSION_SOURCE_META_KEY.into(), Value::Object(source));
    }
    if let Some(session_id) = request.session_id.as_deref() {
        metadata.insert(REQUESTED_SESSION_ID_META_KEY.into(), json!(session_id));
    }
    if request.worktree.is_some() {
        metadata.insert(WORKTREE_MCP_DEFER_META_KEY.into(), json!(true));
    }
    json!({
        "cwd": workspace_cwd.to_string_lossy(),
        "mcpServers": request.session_mcp_servers.clone(),
        "_meta": Value::Object(metadata),
    })
}

fn session_restore_request(
    request: &BridgeRestoreRequest,
    action: BridgeRestoreAction,
    workspace_cwd: &Path,
) -> (&'static str, Value) {
    let mut metadata = Map::new();
    if let Some(source_type) = request.source_type.as_deref() {
        let mut source = Map::new();
        source.insert("sourceType".into(), json!(source_type));
        if let Some(source_id) = request.source_id.as_deref() {
            source.insert("sourceId".into(), json!(source_id));
        }
        metadata.insert(SESSION_SOURCE_META_KEY.into(), Value::Object(source));
    }
    if action == BridgeRestoreAction::Load && request.history_replay.as_deref() == Some("response")
    {
        metadata.insert(
            LOAD_REPLAY_MODE_META_KEY.into(),
            json!(LOAD_REPLAY_BULK_MODE),
        );
        if let Some(page_size) = request.history_page_size {
            metadata.insert(LOAD_REPLAY_PAGE_SIZE_META_KEY.into(), json!(page_size));
        }
    }
    if action == BridgeRestoreAction::Load && request.hide_inherited_history {
        metadata.insert(LOAD_REPLAY_HIDE_INHERITED_META_KEY.into(), json!(true));
    }
    let method = match action {
        BridgeRestoreAction::Load => "session/load",
        BridgeRestoreAction::Resume => "unstable_resumeSession",
    };
    (
        method,
        json!({
            "sessionId":request.session_id,
            "cwd":workspace_cwd.to_string_lossy(),
            "mcpServers":request.session_mcp_servers.clone(),
            "_meta":Value::Object(metadata),
        }),
    )
}

fn restore_action_label(action: BridgeRestoreAction) -> &'static str {
    match action {
        BridgeRestoreAction::Load => "session/load",
        BridgeRestoreAction::Resume => "session/resume",
    }
}

struct SessionWorkGuard {
    channel: Arc<SharedAcpChannel>,
    idle_timeout_ms: u64,
}

impl Drop for SessionWorkGuard {
    fn drop(&mut self) {
        self.channel
            .in_flight_session_work
            .fetch_sub(1, Ordering::AcqRel);
        schedule_idle_shutdown(self.channel.clone(), self.idle_timeout_ms);
    }
}

fn schedule_idle_shutdown(channel: Arc<SharedAcpChannel>, idle_timeout_ms: u64) {
    if channel.sessions.load(Ordering::Acquire) != 0
        || channel.in_flight_session_work.load(Ordering::Acquire) != 0
    {
        return;
    }
    let generation = channel.idle_generation.fetch_add(1, Ordering::AcqRel) + 1;
    if idle_timeout_ms == 0 {
        if !channel.closing.swap(true, Ordering::AcqRel) {
            tokio::spawn(async move {
                let _ = channel.process.terminate().await;
            });
        }
        return;
    }
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(idle_timeout_ms)).await;
        if channel.idle_generation.load(Ordering::Acquire) == generation
            && channel.sessions.load(Ordering::Acquire) == 0
            && channel.in_flight_session_work.load(Ordering::Acquire) == 0
            && !channel.closing.swap(true, Ordering::AcqRel)
        {
            let _ = channel.process.terminate().await;
        }
    });
}

struct RpcBridgeSessionAgent {
    session_id: String,
    channel: Arc<SharedAcpChannel>,
    close_timeout_ms: u64,
    idle_timeout_ms: u64,
    closed: AtomicBool,
}

impl BridgeSessionAgent for RpcBridgeSessionAgent {
    fn prompt<'a>(
        &'a self,
        prompt: Value,
        mut cancelled: watch::Receiver<bool>,
    ) -> SessionRuntimeFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            let request = self.channel.rpc.request(
                "session/prompt",
                json!({"sessionId":self.session_id,"prompt":prompt}),
                0,
            );
            tokio::select! {
                result = request => result.map_err(|error| format!("ACP session/prompt failed: {error}")),
                changed = cancelled.changed() => {
                    if changed.is_ok() && *cancelled.borrow() {
                        let _ = self.channel.rpc.notify("session/cancel", json!({"sessionId":self.session_id})).await;
                        Err("ACP prompt cancelled".into())
                    } else {
                        Err("ACP prompt cancellation signal closed".into())
                    }
                }
            }
        })
    }

    fn cancel<'a>(&'a self) -> SessionRuntimeFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.channel
                .rpc
                .notify("session/cancel", json!({"sessionId":self.session_id}))
                .await
                .map_err(|error| format!("ACP session/cancel failed: {error}"))
        })
    }

    fn shutdown<'a>(&'a self) -> SessionRuntimeFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if self.closed.load(Ordering::Acquire) {
                return Ok(());
            }
            let response = self
                .channel
                .rpc
                .request(
                    SESSION_CLOSE_EXT_METHOD,
                    json!({
                        "sessionId":self.session_id,
                        "drainTimeoutMs":self.close_timeout_ms.saturating_sub(500),
                    }),
                    self.close_timeout_ms,
                )
                .await
                .map_err(|error| format!("ACP session close failed: {error}"))?;
            if response.get("closed").and_then(Value::as_bool) != Some(true) {
                return Err("ACP child did not confirm session close".into());
            }
            self.closed.store(true, Ordering::Release);
            let _ =
                self.channel
                    .sessions
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                        Some(count.saturating_sub(1))
                    });
            schedule_idle_shutdown(self.channel.clone(), self.idle_timeout_ms);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_new_params_preserve_source_requested_id_and_worktree_meta() {
        let request = BridgeSpawnRequest {
            session_id: Some("session-1".into()),
            source_type: Some("browser_tab".into()),
            source_id: Some("tab-1".into()),
            worktree: Some(json!({"path":"/tmp/worktree"})),
            ..BridgeSpawnRequest::default()
        };
        let params = session_new_params(&request, Path::new("/workspace"));
        assert_eq!(params["cwd"], "/workspace");
        assert_eq!(params["mcpServers"], json!([]));
        assert_eq!(
            params["_meta"],
            json!({
                "qwen.session.source":{"sourceType":"browser_tab","sourceId":"tab-1"},
                "qwen-code/sessionId":"session-1",
                "qwen.session.deferMcpDiscovery":true
            })
        );
    }

    #[test]
    fn session_load_uses_bounded_response_replay_metadata() {
        let request = BridgeRestoreRequest {
            session_id: "session-1".into(),
            source_type: Some("browser_tab".into()),
            source_id: Some("tab-1".into()),
            history_replay: Some("response".into()),
            history_page_size: Some(50),
            hide_inherited_history: true,
            ..BridgeRestoreRequest::default()
        };
        let (method, params) =
            session_restore_request(&request, BridgeRestoreAction::Load, Path::new("/workspace"));
        assert_eq!(method, "session/load");
        assert_eq!(params["sessionId"], "session-1");
        assert_eq!(params["cwd"], "/workspace");
        assert_eq!(params["mcpServers"], json!([]));
        assert_eq!(
            params["_meta"],
            json!({
                "qwen.session.source":{"sourceType":"browser_tab","sourceId":"tab-1"},
                "qwen.session.loadReplayMode":"bulk",
                "qwen.session.loadReplayPageSize":50,
                "qwen.session.loadReplayHideInherited":true
            })
        );
    }

    #[test]
    fn session_resume_does_not_forward_load_only_replay_options() {
        let request = BridgeRestoreRequest {
            session_id: "session-1".into(),
            history_replay: Some("response".into()),
            history_page_size: Some(50),
            hide_inherited_history: true,
            ..BridgeRestoreRequest::default()
        };
        let (method, params) = session_restore_request(
            &request,
            BridgeRestoreAction::Resume,
            Path::new("/workspace"),
        );
        assert_eq!(method, "unstable_resumeSession");
        assert_eq!(params["_meta"], json!({}));
    }
}
