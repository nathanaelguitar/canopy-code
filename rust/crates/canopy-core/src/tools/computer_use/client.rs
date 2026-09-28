//! Lazy MCP client for the pinned `cua-driver` executable.
//!
//! The shared MCP runtime owns protocol framing and stdio process transport;
//! this layer supplies the computer-use-specific spawn configuration, runtime
//! screenshot setting, idle shutdown, and retry behavior used by the Node
//! client.

use super::constants::{CUA_DRIVER_VERSION, binary_path};
use crate::tools::mcp::client_runtime::{
    McpClientError, McpClientRuntime, McpRequestOptions, McpTransportBuildOptions,
    McpTransportFactory,
};
use crate::tools::mcp::native_transports::NativeMcpTransportFactory;
use crate::utils::cancellation::CancellationToken;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

pub const DEFAULT_COMPUTER_USE_IDLE_TIMEOUT_MS: u64 = 5 * 60 * 1000;
pub const MAX_COMPUTER_USE_IDLE_TIMEOUT_MS: u64 = 2_147_483_647;
const COMPUTER_USE_RECONNECT_ATTEMPTS: usize = 3;
const COMPUTER_USE_RECONNECT_BACKOFF: Duration = Duration::from_secs(1);

pub type ComputerUseProgress = Arc<dyn Fn(&str) + Send + Sync + 'static>;

/// Configuration for the computer-use MCP process.
#[derive(Clone)]
pub struct ComputerUseClientOptions {
    /// Absolute path to the installed `cua-driver` executable.
    pub binary: PathBuf,
    /// Progress callback for startup and best-effort runtime configuration.
    pub on_progress: Option<ComputerUseProgress>,
    /// Longest screenshot edge in pixels. `None` leaves the driver's default.
    pub max_image_dimension: Option<u32>,
    /// Idle lifetime; zero disables automatic shutdown and invalid values use
    /// the five-minute default.
    pub idle_timeout_ms: Option<f64>,
    /// Optional adapter override, primarily useful for embedding and tests.
    pub transport_factory: Option<Arc<dyn McpTransportFactory>>,
}

impl ComputerUseClientOptions {
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
            on_progress: None,
            max_image_dimension: None,
            idle_timeout_ms: None,
            transport_factory: None,
        }
    }
}

/// One lazily spawned CUA MCP client. Concurrent `start` calls serialize on
/// the same gate and observe the same connected runtime, coalescing one spawn.
#[derive(Clone)]
pub struct ComputerUseClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    binary: PathBuf,
    on_progress: ComputerUseProgress,
    max_image_dimension: Mutex<Option<u32>>,
    idle_timeout_ms: AtomicU64,
    factory: Arc<dyn McpTransportFactory>,
    client: RwLock<Option<Arc<McpClientRuntime>>>,
    lifecycle: AsyncMutex<()>,
    active_calls: AtomicUsize,
    idle_generation: AtomicU64,
    idle_task: Mutex<Option<JoinHandle<()>>>,
}

impl ComputerUseClient {
    pub fn new(options: ComputerUseClientOptions) -> Self {
        let factory = options
            .transport_factory
            .unwrap_or_else(|| Arc::new(NativeMcpTransportFactory::new()));
        let on_progress = options
            .on_progress
            .unwrap_or_else(|| Arc::new(|_: &str| {}) as ComputerUseProgress);
        Self {
            inner: Arc::new(ClientInner {
                binary: options.binary,
                on_progress,
                max_image_dimension: Mutex::new(options.max_image_dimension),
                idle_timeout_ms: AtomicU64::new(normalize_idle_timeout_ms(options.idle_timeout_ms)),
                factory,
                client: RwLock::new(None),
                lifecycle: AsyncMutex::new(()),
                active_calls: AtomicUsize::new(0),
                idle_generation: AtomicU64::new(0),
                idle_task: Mutex::new(None),
            }),
        }
    }

    /// Process-wide default instance, using the version-pinned install path.
    pub fn shared() -> &'static Self {
        static SHARED: OnceLock<ComputerUseClient> = OnceLock::new();
        SHARED.get_or_init(|| Self::new(ComputerUseClientOptions::new(default_binary_path())))
    }

    pub fn is_started(&self) -> bool {
        self.inner
            .client
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .is_some()
    }

    /// Update the screenshot cap used by the next connection. `0` explicitly
    /// disables resizing, matching the driver's `set_config` contract.
    pub fn set_max_image_dimension(&self, value: Option<u32>) {
        *self
            .inner
            .max_image_dimension
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = value;
    }

    pub fn set_idle_timeout_ms(&self, value: Option<f64>) {
        self.inner
            .idle_timeout_ms
            .store(normalize_idle_timeout_ms(value), Ordering::Release);
        self.inner.schedule_idle_stop();
    }

    /// Connect on demand. Concurrent calls share one in-flight transport
    /// creation and MCP initialize handshake.
    pub async fn start(
        &self,
        on_progress: Option<ComputerUseProgress>,
    ) -> Result<(), McpClientError> {
        self.inner.start(on_progress).await
    }

    pub async fn list_tools(&self) -> Result<Value, McpClientError> {
        let client = self.current_client()?;
        client
            .request_raw("tools/list", json!({}), McpRequestOptions::default())
            .await
    }

    /// Call a driver tool and reconnect up to three times when the stdio
    /// client or the driver's daemon socket reports a recoverable closure.
    pub async fn call_tool(
        &self,
        name: &str,
        arguments: Map<String, Value>,
    ) -> Result<Value, McpClientError> {
        self.call_tool_with_cancellation(name, arguments, None)
            .await
    }

    /// Call a driver tool while observing the host's prompt cancellation
    /// token. Cancelling stops waiting for the MCP response and prevents
    /// reconnect retries, but it cannot retract a desktop action whose MCP
    /// request may already have reached the driver; callers must report that
    /// outcome as uncertain.
    pub async fn call_tool_with_cancellation(
        &self,
        name: &str,
        arguments: Map<String, Value>,
        cancellation: Option<CancellationToken>,
    ) -> Result<Value, McpClientError> {
        self.call_tool_with_cancellation_and_dispatch(name, arguments, cancellation, None, None)
            .await
    }

    /// Variant that marks the point where an MCP request starts polling its
    /// transport. Cancellation before this hook means no request was
    /// dispatched; cancellation afterward may leave a desktop action applied.
    pub async fn call_tool_with_cancellation_and_dispatch(
        &self,
        name: &str,
        arguments: Map<String, Value>,
        cancellation: Option<CancellationToken>,
        on_request_started: Option<Arc<dyn Fn() + Send + Sync>>,
        on_request_completed: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Result<Value, McpClientError> {
        let (client, _active_call) = self.inner.begin_call(cancellation.as_ref()).await?;
        let options = || McpRequestOptions {
            cancellation: cancellation.clone(),
            on_request_started: on_request_started.clone(),
            on_request_completed: on_request_completed.clone(),
            ..McpRequestOptions::default()
        };
        match client.call_tool(name, &arguments, options()).await {
            Ok(result) => Ok(result),
            Err(error) if is_transport_closed_error(&error) => {
                let mut last_error = error;
                for _ in 0..COMPUTER_USE_RECONNECT_ATTEMPTS {
                    if cancellation
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled)
                    {
                        return Err(McpClientError::Cancelled);
                    }
                    if let Some(token) = cancellation.as_ref() {
                        tokio::select! {
                            biased;
                            _ = token.cancelled() => return Err(McpClientError::Cancelled),
                            _ = self.stop() => {}
                        }
                        tokio::select! {
                            biased;
                            _ = token.cancelled() => return Err(McpClientError::Cancelled),
                            result = self.start(None) => result?,
                        }
                    } else {
                        self.stop().await;
                        self.start(None).await?;
                    }
                    let client = self.current_client()?;
                    match client.call_tool(name, &arguments, options()).await {
                        Ok(result) => return Ok(result),
                        Err(error) if is_transport_closed_error(&error) => {
                            last_error = error;
                            if let Some(token) = cancellation.as_ref() {
                                tokio::select! {
                                    biased;
                                    _ = token.cancelled() => return Err(McpClientError::Cancelled),
                                    _ = tokio::time::sleep(COMPUTER_USE_RECONNECT_BACKOFF) => {}
                                }
                            } else {
                                tokio::time::sleep(COMPUTER_USE_RECONNECT_BACKOFF).await;
                            }
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(last_error)
            }
            Err(error) => Err(error),
        }
    }

    /// Best-effort teardown. Safe to call repeatedly.
    pub async fn stop(&self) {
        self.inner.clear_idle_stop_timer();
        let _lifecycle = self.inner.lifecycle.lock().await;
        self.inner.disconnect_locked().await;
    }

    fn current_client(&self) -> Result<Arc<McpClientRuntime>, McpClientError> {
        self.inner
            .client
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
            .ok_or(McpClientError::NotConnected)
    }
}

impl ClientInner {
    async fn start(
        self: &Arc<Self>,
        on_progress: Option<ComputerUseProgress>,
    ) -> Result<(), McpClientError> {
        self.clear_idle_stop_timer();
        let _lifecycle = self.lifecycle.lock().await;
        if self.is_started() {
            self.schedule_idle_stop();
            return Ok(());
        }

        let progress = on_progress.as_ref().unwrap_or(&self.on_progress);
        progress("Starting Computer Use driver...");

        let config = json!({
            "command": self.binary,
            "args": ["mcp"]
        });
        let client = Arc::new(McpClientRuntime::new(
            "computer-use",
            config,
            Arc::clone(&self.factory),
        ));
        let transport_options = McpTransportBuildOptions {
            // The MCP runtime removes Canopy's internal daemon credentials
            // before the native stdio adapter launches the child.
            parent_env: std::env::vars_os()
                .map(|(name, value)| {
                    (
                        name.to_string_lossy().into_owned(),
                        value.to_string_lossy().into_owned(),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
            ..McpTransportBuildOptions::default()
        };
        client.connect(&transport_options, None).await?;
        *self
            .client
            .write()
            .unwrap_or_else(|error| error.into_inner()) = Some(Arc::clone(&client));

        let max_image_dimension = *self
            .max_image_dimension
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(max_image_dimension) = max_image_dimension {
            let mut arguments = Map::new();
            arguments.insert(
                "max_image_dimension".to_owned(),
                Value::from(max_image_dimension),
            );
            if let Err(error) = client
                .call_tool("set_config", &arguments, McpRequestOptions::default())
                .await
            {
                progress(&format!(
                    "Computer Use: could not apply max_image_dimension={max_image_dimension} ({}); using driver default.",
                    error
                ));
            }
        }
        self.schedule_idle_stop();
        Ok(())
    }

    async fn begin_call(
        self: &Arc<Self>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<(Arc<McpClientRuntime>, ActiveCall), McpClientError> {
        self.clear_idle_stop_timer();
        let _lifecycle = if let Some(cancellation) = cancellation {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(McpClientError::Cancelled),
                guard = self.lifecycle.lock() => guard,
            }
        } else {
            self.lifecycle.lock().await
        };
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Err(McpClientError::Cancelled);
        }
        let client = self
            .client
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
            .ok_or(McpClientError::NotConnected)?;
        self.active_calls.fetch_add(1, Ordering::AcqRel);
        Ok((
            client,
            ActiveCall {
                inner: Arc::downgrade(self),
                released: false,
            },
        ))
    }

    fn is_started(&self) -> bool {
        self.client
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .is_some()
    }

    async fn disconnect_locked(&self) {
        let client = self
            .client
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(client) = client {
            let _ = client.disconnect().await;
        }
    }

    fn schedule_idle_stop(self: &Arc<Self>) {
        let generation = self.clear_idle_stop_timer();
        let timeout_ms = self.idle_timeout_ms.load(Ordering::Acquire);
        if timeout_ms == 0 || !self.is_started() || self.active_calls.load(Ordering::Acquire) > 0 {
            return;
        }
        let weak = Arc::downgrade(self);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let task = runtime.spawn(async move {
            tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
            let Some(inner) = weak.upgrade() else {
                return;
            };
            let _lifecycle = inner.lifecycle.lock().await;
            if inner.idle_generation.load(Ordering::Acquire) != generation
                || inner.active_calls.load(Ordering::Acquire) > 0
            {
                return;
            }
            inner.take_idle_task(generation);
            inner.disconnect_locked().await;
        });
        let mut idle_task = self
            .idle_task
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if self.idle_generation.load(Ordering::Acquire) == generation {
            *idle_task = Some(task);
        } else {
            task.abort();
        }
    }

    fn clear_idle_stop_timer(&self) -> u64 {
        let generation = self.idle_generation.fetch_add(1, Ordering::AcqRel) + 1;
        if let Some(task) = self
            .idle_task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            task.abort();
        }
        generation
    }

    fn take_idle_task(&self, generation: u64) {
        if self.idle_generation.load(Ordering::Acquire) == generation {
            self.idle_task
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
        }
    }
}

struct ActiveCall {
    inner: Weak<ClientInner>,
    released: bool,
}

impl Drop for ActiveCall {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        if let Some(inner) = self.inner.upgrade() {
            if inner.active_calls.fetch_sub(1, Ordering::AcqRel) == 1 {
                inner.schedule_idle_stop();
            }
        }
        self.released = true;
    }
}

fn normalize_idle_timeout_ms(value: Option<f64>) -> u64 {
    match value {
        None => DEFAULT_COMPUTER_USE_IDLE_TIMEOUT_MS,
        Some(value)
            if !value.is_finite()
                || value < 0.0
                || value > MAX_COMPUTER_USE_IDLE_TIMEOUT_MS as f64 =>
        {
            DEFAULT_COMPUTER_USE_IDLE_TIMEOUT_MS
        }
        Some(value) => value as u64,
    }
}

/// Match the recoverable closed-transport messages observed from the stdio
/// MCP client and the driver's daemon socket forwarding layer.
pub fn is_transport_closed_error(error: &McpClientError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    [
        "connection closed",
        "not connected",
        "connection refused",
        "daemon transport error",
        "os error 61",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

fn default_binary_path() -> PathBuf {
    let platform = match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        other => other,
    };
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_default();
    binary_path(home, platform, arch, CUA_DRIVER_VERSION).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::mcp::client_runtime::{
        MCP_PROTOCOL_VERSION, McpTransport, McpTransportError, McpTransportFactory,
        McpTransportSpec,
    };
    use crate::utils::cancellation::CancellationToken;
    use futures_util::future::BoxFuture;
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use tokio::sync::Notify;

    struct FakeState {
        create_count: AtomicUsize,
        close_count: AtomicUsize,
        specs: Mutex<Vec<McpTransportSpec>>,
        calls: Mutex<Vec<Value>>,
        tool_results: Mutex<VecDeque<Result<Value, McpTransportError>>>,
        create_gate: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
        call_gate: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
        fail_set_config: AtomicBool,
    }

    impl FakeState {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                create_count: AtomicUsize::new(0),
                close_count: AtomicUsize::new(0),
                specs: Mutex::new(Vec::new()),
                calls: Mutex::new(Vec::new()),
                tool_results: Mutex::new(VecDeque::new()),
                create_gate: Mutex::new(None),
                call_gate: Mutex::new(None),
                fail_set_config: AtomicBool::new(false),
            })
        }
    }

    struct FakeFactory(Arc<FakeState>);

    impl McpTransportFactory for FakeFactory {
        fn create<'a>(
            &'a self,
            spec: McpTransportSpec,
            _cancellation: Option<CancellationToken>,
        ) -> BoxFuture<'a, Result<Arc<dyn McpTransport>, McpTransportError>> {
            Box::pin(async move {
                self.0.create_count.fetch_add(1, Ordering::SeqCst);
                self.0
                    .specs
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push(spec);
                let gate = self
                    .0
                    .create_gate
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone();
                if let Some((entered, release)) = gate {
                    entered.notify_one();
                    release.notified().await;
                }
                Ok(Arc::new(FakeTransport(Arc::clone(&self.0))) as Arc<dyn McpTransport>)
            })
        }
    }

    struct FakeTransport(Arc<FakeState>);

    impl McpTransport for FakeTransport {
        fn request<'a>(
            &'a self,
            request: Value,
            _cancellation: Option<CancellationToken>,
        ) -> BoxFuture<'a, Result<Value, McpTransportError>> {
            Box::pin(async move {
                let method = request["method"].as_str().unwrap_or_default();
                let id = request["id"].as_u64().unwrap_or_default();
                let result = match method {
                    "initialize" => json!({
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {},
                        "serverInfo": {"name":"fake-cua","version":"test"}
                    }),
                    "tools/call" => {
                        self.0
                            .calls
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .push(request.clone());
                        let name = request["params"]["name"].as_str().unwrap_or_default();
                        if name == "set_config" && self.0.fail_set_config.load(Ordering::SeqCst) {
                            return Err(McpTransportError::Transport(
                                "set_config unavailable".to_owned(),
                            ));
                        }
                        if name != "set_config" {
                            let gate = self
                                .0
                                .call_gate
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .clone();
                            if let Some((entered, release)) = gate {
                                entered.notify_one();
                                release.notified().await;
                            }
                            if let Some(result) = self
                                .0
                                .tool_results
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .pop_front()
                            {
                                return result.map(
                                    |result| json!({"jsonrpc":"2.0","id":id,"result":result}),
                                );
                            }
                        }
                        json!({"content":[{"type":"text","text":"ok"}],"isError":false})
                    }
                    "tools/list" => json!({"tools":[{
                        "name":"list_windows",
                        "inputSchema":{"type":"object","properties":{}}
                    }]}),
                    _ => json!({}),
                };
                Ok(json!({"jsonrpc":"2.0","id":id,"result":result}))
            })
        }

        fn notify<'a>(
            &'a self,
            _notification: Value,
            _cancellation: Option<CancellationToken>,
        ) -> BoxFuture<'a, Result<(), McpTransportError>> {
            Box::pin(async { Ok(()) })
        }

        fn close<'a>(&'a self) -> BoxFuture<'a, Result<(), McpTransportError>> {
            Box::pin(async move {
                self.0.close_count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    fn make_client(state: Arc<FakeState>, idle_timeout_ms: Option<f64>) -> ComputerUseClient {
        let mut options = ComputerUseClientOptions::new("/fake/cua-driver");
        options.idle_timeout_ms = idle_timeout_ms;
        options.transport_factory = Some(Arc::new(FakeFactory(state)));
        ComputerUseClient::new(options)
    }

    fn empty_args() -> Map<String, Value> {
        Map::new()
    }

    #[tokio::test]
    async fn remains_lazy_until_start_and_rejects_calls_before_start() {
        let state = FakeState::new();
        let client = make_client(Arc::clone(&state), Some(0.0));
        assert!(!client.is_started());
        assert_eq!(state.create_count.load(Ordering::SeqCst), 0);
        assert_eq!(
            client.call_tool("list_windows", empty_args()).await,
            Err(McpClientError::NotConnected)
        );
        assert_eq!(state.create_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn coalesces_concurrent_start_calls_and_uses_mcp_binary_arguments() {
        let state = FakeState::new();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        *state
            .create_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner()) =
            Some((Arc::clone(&entered), Arc::clone(&release)));
        let client = make_client(Arc::clone(&state), Some(0.0));
        let first_client = client.clone();
        let first = tokio::spawn(async move { first_client.start(None).await });
        entered.notified().await;
        let second_client = client.clone();
        let second = tokio::spawn(async move { second_client.start(None).await });
        release.notify_one();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(state.create_count.load(Ordering::SeqCst), 1);
        {
            let specs = state
                .specs
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            assert_eq!(specs[0].command.as_deref(), Some("/fake/cua-driver"));
            assert_eq!(specs[0].args, ["mcp"]);
        }
        client.stop().await;
    }

    #[tokio::test]
    async fn applies_runtime_screenshot_config_best_effort_after_connect() {
        let state = FakeState::new();
        state.fail_set_config.store(true, Ordering::SeqCst);
        let progress = Arc::new(Mutex::new(Vec::new()));
        let progress_sink = Arc::clone(&progress);
        let mut options = ComputerUseClientOptions::new("/fake/cua-driver");
        options.max_image_dimension = Some(0);
        options.idle_timeout_ms = Some(0.0);
        options.on_progress = Some(Arc::new(move |message| {
            progress_sink.lock().unwrap().push(message.to_owned());
        }));
        options.transport_factory = Some(Arc::new(FakeFactory(Arc::clone(&state))));
        let client = ComputerUseClient::new(options);
        client.start(None).await.unwrap();
        assert!(
            client.is_started(),
            "a failed set_config must not abort startup"
        );
        {
            let calls = state
                .calls
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            assert_eq!(calls[0]["params"]["name"], "set_config");
            assert_eq!(calls[0]["params"]["arguments"]["max_image_dimension"], 0);
        }
        assert!(progress.lock().unwrap().iter().any(|message| {
            message.contains("max_image_dimension=0") && message.contains("set_config unavailable")
        }));
        client.stop().await;
    }

    #[tokio::test]
    async fn inherits_parent_environment_and_lists_tools() {
        let state = FakeState::new();
        let client = make_client(Arc::clone(&state), Some(0.0));
        client.start(None).await.unwrap();
        {
            let specs = state
                .specs
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Ok(path) = std::env::var("PATH") {
                assert_eq!(specs[0].env.get("PATH"), Some(&path));
            }
        }
        let tools = client.list_tools().await.unwrap();
        assert_eq!(tools["tools"][0]["name"], "list_windows");
        client.stop().await;
    }

    #[tokio::test]
    async fn recovers_from_closed_transport_and_reapplies_runtime_config() {
        let state = FakeState::new();
        state
            .tool_results
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push_back(Err(McpTransportError::Transport(
                "Connection closed".to_owned(),
            )));
        let mut options = ComputerUseClientOptions::new("/fake/cua-driver");
        options.idle_timeout_ms = Some(0.0);
        options.max_image_dimension = Some(1280);
        options.transport_factory = Some(Arc::new(FakeFactory(Arc::clone(&state))));
        let client = ComputerUseClient::new(options);
        client.start(None).await.unwrap();
        let result = client
            .call_tool("get_window_state", empty_args())
            .await
            .unwrap();
        assert_eq!(result["content"][0]["text"], "ok");
        assert_eq!(state.create_count.load(Ordering::SeqCst), 2);
        let config_calls = state
            .calls
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter(|call| call["params"]["name"] == "set_config")
            .count();
        assert_eq!(config_calls, 2);
        client.stop().await;
    }

    #[tokio::test]
    async fn backs_off_between_retries_after_a_second_closed_transport() {
        let state = FakeState::new();
        {
            let mut results = state
                .tool_results
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            results.push_back(Err(McpTransportError::Transport(
                "Connection closed".to_owned(),
            )));
            results.push_back(Err(McpTransportError::Transport(
                "Connection refused (os error 61)".to_owned(),
            )));
        }
        let client = make_client(Arc::clone(&state), Some(0.0));
        client.start(None).await.unwrap();
        let started_at = std::time::Instant::now();
        let result = client
            .call_tool("get_window_state", empty_args())
            .await
            .unwrap();
        assert_eq!(result["content"][0]["text"], "ok");
        assert!(started_at.elapsed() >= COMPUTER_USE_RECONNECT_BACKOFF);
        assert_eq!(state.create_count.load(Ordering::SeqCst), 3);
        client.stop().await;
    }

    #[tokio::test]
    async fn idle_shutdown_waits_for_active_calls_then_closes() {
        let state = FakeState::new();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        *state
            .call_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner()) =
            Some((Arc::clone(&entered), Arc::clone(&release)));
        let client = make_client(Arc::clone(&state), Some(20.0));
        client.start(None).await.unwrap();
        let call_client = client.clone();
        let call = tokio::spawn(async move {
            call_client
                .call_tool("get_window_state", empty_args())
                .await
        });
        entered.notified().await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(state.close_count.load(Ordering::SeqCst), 0);
        release.notify_one();
        call.await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(state.close_count.load(Ordering::SeqCst), 1);
        assert!(!client.is_started());
    }

    #[tokio::test]
    async fn explicit_stop_is_idempotent_and_cancels_idle_shutdown() {
        let state = FakeState::new();
        let client = make_client(Arc::clone(&state), Some(20.0));
        client.start(None).await.unwrap();
        client.stop().await;
        client.stop().await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(state.close_count.load(Ordering::SeqCst), 1);
        assert!(!client.is_started());
    }

    #[test]
    fn idle_timeout_normalization_matches_source_defaults_and_limits() {
        assert_eq!(normalize_idle_timeout_ms(None), 300_000);
        assert_eq!(normalize_idle_timeout_ms(Some(f64::NAN)), 300_000);
        assert_eq!(normalize_idle_timeout_ms(Some(f64::INFINITY)), 300_000);
        assert_eq!(normalize_idle_timeout_ms(Some(f64::NEG_INFINITY)), 300_000);
        assert_eq!(normalize_idle_timeout_ms(Some(-1.0)), 300_000);
        assert_eq!(normalize_idle_timeout_ms(Some(2_147_483_648.0)), 300_000);
        assert_eq!(normalize_idle_timeout_ms(Some(0.0)), 0);
        assert_eq!(normalize_idle_timeout_ms(Some(123.9)), 123);
        assert_eq!(
            normalize_idle_timeout_ms(Some(MAX_COMPUTER_USE_IDLE_TIMEOUT_MS as f64)),
            MAX_COMPUTER_USE_IDLE_TIMEOUT_MS
        );
    }

    #[test]
    fn closed_transport_classifier_matches_driver_and_stdio_errors_only() {
        for message in [
            "Connection closed",
            "MCP error -32000: Connection closed",
            "Not connected",
            "daemon transport error forwarding tool: Connection refused (os error 61)",
        ] {
            assert!(is_transport_closed_error(&McpClientError::Transport(
                message.to_owned()
            )));
        }
        assert!(!is_transport_closed_error(&McpClientError::Transport(
            "element_index out of range".to_owned()
        )));
    }
}
