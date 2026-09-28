//! Loopback-only HTTP daemon slice for the native `canopy serve` command.
//!
//! The current Rust daemon exposes bootstrap health, capabilities, daemon
//! status, bounded workspace-memory file metadata, read-only workspace Git
//! status/log/diff summaries, catalog/runtime-backed session status, a bounded
//! transcript event replay route, and a native bounded JSONL record-page
//! route. It does not start agent sessions or claim broader daemon API parity.

use std::collections::HashSet;
use std::convert::Infallible;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use canopy_core::acp_bridge::transcript_replay_page::{
    TRANSCRIPT_REPLAY_MAX_CURSOR_BYTES, TRANSCRIPT_REPLAY_MAX_OUTPUT_BYTES,
    TranscriptReplayPageError, replay_transcript_record_page,
};
use canopy_core::config::migrations::{run_migrations, settings_need_migration};
use canopy_core::config::{SettingScope, SettingsPaths, merge_settings};
use canopy_core::env_var_resolver::resolve_env_vars_in_object;
use canopy_core::git_branch_operations::{
    GitBranchOperationError, GitCommitOptions, GitPullOptions, GitPushOptions, git_checkout,
    git_commit, git_create_branch, git_pull, git_push, is_valid_checkout_ref, is_valid_ref_name,
};
use canopy_core::jsonc::strip_json_comments;
use canopy_core::memory::{AGENT_CONTEXT_FILENAME, DEFAULT_CONTEXT_FILENAME};
use canopy_core::services::native_memory_probe::NativeMemoryProbe;
use canopy_core::services::session_registry::is_pid_alive;
use canopy_core::services::session_transcript_reader::{
    SESSION_TRANSCRIPT_MAX_LIMIT, SESSION_TRANSCRIPT_MAX_PAGE_BYTES,
    SessionTranscriptReadPageOptions, SessionTranscriptReader, SessionTranscriptReaderError,
};
use canopy_core::session_catalog::SessionCatalog;
use canopy_core::session_paths::{SessionArchiveState, is_valid_session_id};
use canopy_core::storage::Storage;
use canopy_core::trusted_folders::{
    TRUSTED_FOLDERS_FILENAME, TrustLevel, TrustRule, resolve_trust_decision,
};
use canopy_core::utils::terminal_safe::strip_terminal_control_sequences;
use chrono::{SecondsFormat, TimeZone, Utc};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{
    AUTHORIZATION, CACHE_CONTROL, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST, ORIGIN,
    RETRY_AFTER, VARY,
};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::timeout;
use url::form_urlencoded;

const DEFAULT_PORT: u16 = 4170;
const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_HEADER_COUNT: usize = 64;
const MAX_AUTHORIZATION_HEADER_BYTES: usize = MAX_HEADER_BYTES;
const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;
const MAX_CONNECTIONS: usize = 128;
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(5);
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MEMORY_PROBE_TIMEOUT: Duration = Duration::from_millis(1_500);
const MEMORY_PROBE_CONCURRENCY: usize = 2;
const WORKSPACE_MEMORY_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);
const WORKSPACE_MEMORY_LOOKUP_CONCURRENCY: usize = 2;
const MAX_WORKSPACE_MEMORY_SETTINGS_BYTES: u64 = 4 * 1024 * 1024;
const MAX_WORKSPACE_MEMORY_TRUST_BYTES: u64 = 1024 * 1024;
const WORKSPACE_GIT_LOOKUP_TIMEOUT: Duration = Duration::from_secs(6);
const WORKSPACE_GIT_LOOKUP_CONCURRENCY: usize = 2;
const WORKSPACE_GIT_PROCESS_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_WORKSPACE_GIT_STATUS_BYTES: usize = 8 * 1024 * 1024;
const MAX_WORKSPACE_GIT_METADATA_BYTES: u64 = 4 * 1024;
const MAX_WORKSPACE_GIT_STASH_LOG_BYTES: u64 = 4 * 1024 * 1024;
const WORKSPACE_GIT_BRANCHES_LOOKUP_TIMEOUT: Duration = Duration::from_secs(35);
const WORKSPACE_GIT_BRANCHES_LOOKUP_CONCURRENCY: usize = 2;
const MAX_WORKSPACE_GIT_BRANCHES_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const WORKSPACE_GIT_MUTATION_TIMEOUT: Duration = Duration::from_secs(225);
const WORKSPACE_GIT_MUTATION_CONCURRENCY: usize = 1;
const MAX_WORKSPACE_GIT_MUTATION_RESPONSE_BYTES: usize = 10 * 1024 * 1024;
const WORKSPACE_GIT_LOG_LOOKUP_TIMEOUT: Duration = Duration::from_secs(12);
const WORKSPACE_GIT_LOG_LOOKUP_CONCURRENCY: usize = 2;
const WORKSPACE_GIT_LOG_PROCESS_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_WORKSPACE_GIT_LOG_BYTES: usize = 1024 * 1024;
const MAX_WORKSPACE_GIT_NUMSTAT_BYTES: usize = 4 * 1024 * 1024;
const MAX_WORKSPACE_GIT_LOG_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const WORKSPACE_GIT_DIFF_LOOKUP_TIMEOUT: Duration = Duration::from_secs(20);
const WORKSPACE_GIT_DIFF_LOOKUP_CONCURRENCY: usize = 2;
const MAX_WORKSPACE_GIT_DIFF_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
const MAX_WORKSPACE_GIT_DIFF_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_WORKSPACE_GIT_DIFF_DETAILS: usize = 500;
const MAX_WORKSPACE_GIT_DIFF_FILES: usize = 50;
const MAX_WORKSPACE_GIT_UNTRACKED_FILE_BYTES: u64 = 1_000_000;
const WORKSPACE_GIT_UNTRACKED_READ_CHUNK_BYTES: usize = 64 * 1024;
const WORKSPACE_GIT_BINARY_SNIFF_BYTES: usize = 8 * 1024;
const DEFAULT_WORKSPACE_GIT_LOG_LIMIT: usize = 50;
const MAX_WORKSPACE_GIT_LOG_LIMIT: usize = 200;
const MAX_WORKSPACE_GIT_LOG_RANGE_BYTES: usize = 256;
const MAX_WORKSPACE_GIT_LOG_FILES: usize = 50;
const SESSION_LOOKUP_CONCURRENCY: usize = 2;
const SESSION_TRANSCRIPT_LOOKUP_CONCURRENCY: usize = 1;
const MAX_SESSION_STATUS_SIDECAR_BYTES: u64 = 64 * 1024;
const MAX_SESSION_STATUS_RESPONSE_BYTES: usize = 16 * 1024;
const MAX_SESSION_STATUS_CWD_BYTES: usize = 4 * 1024;
const MAX_SESSION_STATUS_METADATA_BYTES: usize = 1024;
const MAX_SESSION_TRANSCRIPT_CURSOR_BYTES: usize = 64 * 1024;
const MAX_SESSION_TRANSCRIPT_EVENT_ENVELOPE_BYTES: usize = 64 * 1024;
const MAX_SESSION_TRANSCRIPT_RESPONSE_BYTES: usize = SESSION_TRANSCRIPT_MAX_PAGE_BYTES;
const MAX_SESSION_TRANSCRIPT_EVENTS_RESPONSE_BYTES: usize = TRANSCRIPT_REPLAY_MAX_OUTPUT_BYTES
    + TRANSCRIPT_REPLAY_MAX_CURSOR_BYTES
    + MAX_SESSION_TRANSCRIPT_EVENT_ENVELOPE_BYTES;
const MAX_WORKSPACE_MEMORY_CONTEXT_FILENAMES: usize = 8;
const MAX_WORKSPACE_MEMORY_FILENAME_BYTES: usize = 1024;
const MAX_WORKSPACE_MEMORY_PATH_BYTES: usize = 4 * 1024;
const MAX_WORKSPACE_MEMORY_ERROR_BYTES: usize = 1024;
const MAX_WORKSPACE_MEMORY_RESPONSE_BYTES: usize = 64 * 1024;

const HEALTH_OK: &str = r#"{"status":"ok"}"#;
const HEALTH_BOOTSTRAP: &str = r#"{"status":"degraded","reason":"bootstrap"}"#;
const CORS_DENIED: &str = r#"{"error":"Request denied by CORS policy"}"#;
const HOST_DENIED: &str = r#"{"error":"Invalid Host header"}"#;
const UNAUTHORIZED: &str = r#"{"error":"Unauthorized"}"#;
const INVALID_STATUS_DETAIL: &str =
    r#"{"error":"detail must be one of: summary, full","code":"invalid_detail"}"#;
const USAGE: &str =
    "Usage: canopy serve [--port <0-65535>] [--token <token>] [--require-auth] [--help]";
const SERVE_FEATURES: &[&str] = &[
    "health",
    "daemon_status",
    "capabilities",
    "session_status",
    "session_transcript",
    "session_transcript_pagination",
    "session_transcript_records",
    "workspace_memory_read",
    "workspace_git_read",
    "workspace_git_branches_read",
    "workspace_git_mutation",
    "workspace_git_log_read",
    "workspace_git_diff_read",
];

struct ServeContext {
    process_started_at: String,
    listener_ready_at: String,
    listener_ready_ms: u64,
    started: Instant,
    workspace_cwd: String,
    workspace_memory_settings_paths: SettingsPaths,
    workspace_memory_context_filenames: Vec<String>,
    workspace_git_lookup_slots: Arc<Semaphore>,
    workspace_git_branches_lookup_slots: Arc<Semaphore>,
    workspace_git_mutation_slots: Arc<Semaphore>,
    workspace_git_log_lookup_slots: Arc<Semaphore>,
    workspace_git_diff_lookup_slots: Arc<Semaphore>,
    storage: Storage,
    session_catalog: SessionCatalog,
    auth_token_hash: Option<[u8; 32]>,
    require_auth: bool,
    memory_probe_slots: Arc<Semaphore>,
    workspace_memory_lookup_slots: Arc<Semaphore>,
    session_lookup_slots: Arc<Semaphore>,
    session_transcript_lookup_slots: Arc<Semaphore>,
}

impl ServeContext {
    fn new(auth_token: Option<&str>, require_auth: bool) -> Result<Self, String> {
        let cwd = std::env::current_dir()
            .map_err(|error| format!("Could not resolve serve workspace: {error}"))?;
        let cwd = std::fs::canonicalize(&cwd).unwrap_or(cwd);
        let storage = Storage::new(cwd.clone());
        let workspace_memory_settings_paths = SettingsPaths::from_environment(cwd.clone());
        let workspace_memory_context_filenames =
            workspace_memory_context_filenames(&workspace_memory_settings_paths);
        let session_catalog =
            SessionCatalog::new(storage.runtime_base_dir().to_path_buf(), cwd.clone());
        Ok(Self {
            process_started_at: timestamp(),
            listener_ready_at: String::new(),
            listener_ready_ms: 0,
            started: Instant::now(),
            workspace_cwd: cwd.to_string_lossy().into_owned(),
            workspace_memory_settings_paths,
            workspace_memory_context_filenames,
            workspace_git_lookup_slots: Arc::new(Semaphore::new(WORKSPACE_GIT_LOOKUP_CONCURRENCY)),
            workspace_git_branches_lookup_slots: Arc::new(Semaphore::new(
                WORKSPACE_GIT_BRANCHES_LOOKUP_CONCURRENCY,
            )),
            workspace_git_mutation_slots: Arc::new(Semaphore::new(
                WORKSPACE_GIT_MUTATION_CONCURRENCY,
            )),
            workspace_git_log_lookup_slots: Arc::new(Semaphore::new(
                WORKSPACE_GIT_LOG_LOOKUP_CONCURRENCY,
            )),
            workspace_git_diff_lookup_slots: Arc::new(Semaphore::new(
                WORKSPACE_GIT_DIFF_LOOKUP_CONCURRENCY,
            )),
            storage,
            session_catalog,
            auth_token_hash: auth_token.map(hash_auth_token),
            require_auth,
            memory_probe_slots: Arc::new(Semaphore::new(MEMORY_PROBE_CONCURRENCY)),
            workspace_memory_lookup_slots: Arc::new(Semaphore::new(
                WORKSPACE_MEMORY_LOOKUP_CONCURRENCY,
            )),
            session_lookup_slots: Arc::new(Semaphore::new(SESSION_LOOKUP_CONCURRENCY)),
            session_transcript_lookup_slots: Arc::new(Semaphore::new(
                SESSION_TRANSCRIPT_LOOKUP_CONCURRENCY,
            )),
        })
    }

    fn mark_listener_ready(&mut self) {
        self.listener_ready_at = timestamp();
        self.listener_ready_ms = elapsed_ms(self.started);
    }
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn hash_auth_token(token: &str) -> [u8; 32] {
    let digest = Sha256::digest(token.as_bytes());
    let mut hashed = [0_u8; 32];
    hashed.copy_from_slice(&digest);
    hashed
}

fn bearer_token_matches(
    header: Option<&hyper::header::HeaderValue>,
    expected_hash: &[u8; 32],
) -> bool {
    let Some(header) = header else {
        return false;
    };
    let bytes = header.as_bytes();
    if bytes.len() > MAX_AUTHORIZATION_HEADER_BYTES {
        return false;
    }
    let Some(scheme_end) = bytes.iter().position(|byte| *byte == b' ') else {
        return false;
    };
    if scheme_end == 0 || !bytes[..scheme_end].eq_ignore_ascii_case(b"bearer") {
        return false;
    }
    let mut credentials_start = scheme_end + 1;
    while bytes
        .get(credentials_start)
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
    {
        credentials_start += 1;
    }
    if credentials_start == bytes.len() {
        return false;
    }

    let candidate_hash = Sha256::digest(&bytes[credentials_start..]);
    let difference = expected_hash
        .iter()
        .zip(candidate_hash.iter())
        .fold(0_u8, |difference, (expected, candidate)| {
            difference | (*expected ^ *candidate)
        });
    difference == 0
}

/// Bind the native daemon to the same loopback default as the TypeScript CLI.
pub(super) async fn bind_loopback(port: u16) -> Result<TcpListener, String> {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    TcpListener::bind(address)
        .await
        .map_err(|error| format!("Could not bind serve listener at {address}: {error}"))
}

/// Serve the transport until `shutdown` resolves, then drain active requests.
/// A supplied listener is rejected unless it is bound to a loopback address.
pub(super) async fn serve_until_shutdown<F>(
    listener: TcpListener,
    shutdown: F,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
{
    let token = std::env::var("QWEN_SERVER_TOKEN")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let mut context = ServeContext::new(token.as_deref(), false)?;
    context.mark_listener_ready();
    serve_with_context(listener, shutdown, Arc::new(context)).await
}

async fn serve_with_context<F>(
    listener: TcpListener,
    shutdown: F,
    context: Arc<ServeContext>,
) -> Result<(), String>
where
    F: Future<Output = ()> + Send,
{
    let local_address = listener
        .local_addr()
        .map_err(|error| format!("Could not inspect serve listener: {error}"))?;
    if !local_address.ip().is_loopback() {
        return Err(format!(
            "Native serve only accepts loopback listeners, got {local_address}"
        ));
    }

    let mut http = http1::Builder::new();
    http.timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .max_buf_size(MAX_HEADER_BYTES)
        .max_headers(MAX_HEADER_COUNT);

    let graceful = GracefulShutdown::new();
    let mut connections = JoinSet::new();
    let mut shutdown = Box::pin(shutdown);
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
    let mut listener_error = None;

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, peer_address) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        listener_error = Some(format!("Serve listener failed: {error}"));
                        break;
                    }
                };
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                let port = local_address.port();
                let context = Arc::clone(&context);
                let service = service_fn(move |request| {
                    handle_request(request, port, Arc::clone(&context))
                });
                let connection = http.serve_connection(TokioIo::new(stream), service);
                let connection = graceful.watch(connection);
                connections.spawn(async move {
                    let _permit = permit;
                    if let Err(error) = connection.await {
                        eprintln!("serve connection from {peer_address} ended: {error}");
                    }
                });
            }
        }
    }

    drop(listener);
    if timeout(SHUTDOWN_TIMEOUT, graceful.shutdown())
        .await
        .is_err()
    {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        return Err("Serve transport shutdown exceeded five seconds".to_owned());
    }
    while connections.join_next().await.is_some() {}
    listener_error.map_or(Ok(()), Err)
}

async fn handle_request(
    request: Request<Incoming>,
    port: u16,
    context: Arc<ServeContext>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if request.headers().get(ORIGIN).is_some_and(|value| {
        value
            .to_str()
            .map_or(true, |origin| !is_same_origin(origin, port))
    }) {
        return Ok(json_response(
            StatusCode::FORBIDDEN,
            CORS_DENIED,
            false,
            true,
            Some((VARY, "Origin")),
        ));
    }

    let host = request
        .headers()
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !is_allowed_host(host, port) {
        return Ok(json_response(
            StatusCode::FORBIDDEN,
            HOST_DENIED,
            false,
            true,
            None,
        ));
    }

    let is_head = request.method() == Method::HEAD;
    let mut authorization_values = request.headers().get_all(AUTHORIZATION).iter();
    let authorization = authorization_values.next();
    let duplicate_authorization = authorization_values.next().is_some();
    if context.auth_token_hash.as_ref().is_some_and(|expected| {
        duplicate_authorization || !bearer_token_matches(authorization, expected)
    }) {
        return Ok(json_response(
            StatusCode::UNAUTHORIZED,
            UNAUTHORIZED,
            is_head,
            false,
            None,
        ));
    }

    let path = request.uri().path();
    let query = request.uri().query().map(str::to_owned);
    let is_health_path = matches_route_path(path, "/health");
    let is_capabilities_path = matches_route_path(path, "/capabilities");
    let is_daemon_status_path = matches_route_path(path, "/daemon/status");
    let is_workspace_memory_path = matches_route_path(path, "/workspace/memory");
    let is_workspace_git_path = matches_route_path(path, "/workspace/git");
    let is_workspace_git_branches_path = matches_route_path(path, "/workspace/git/branches");
    let is_workspace_git_log_path = matches_route_path(path, "/workspace/git/log");
    let is_workspace_git_log_commit_path = matches_route_path(path, "/workspace/git/log/commit");
    let is_workspace_git_diff_path = matches_route_path(path, "/workspace/git/diff");
    let is_workspace_git_diff_file_path = matches_route_path(path, "/workspace/git/diff/file");
    let git_mutation_route = workspace_git_mutation_route(request.method(), path);
    let session_status_id = session_status_route_id(path).map(str::to_owned);
    let transcript_events_id = session_transcript_route_id(path).map(str::to_owned);
    let transcript_records_id = session_transcript_records_route_id(path).map(str::to_owned);
    let is_read_method = request.method() == Method::GET || is_head;
    let is_deep_health = is_deep_health_query(query.as_deref());
    let mutation_content_type = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    if request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > MAX_REQUEST_BODY_BYTES as u64)
    {
        return Ok(text_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Request body too large",
            false,
            true,
        ));
    }

    let captured_body = match timeout(
        BODY_READ_TIMEOUT,
        consume_bounded_body(request.into_body(), git_mutation_route.is_some()),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(BoundedRequestBodyError::TooLarge)) => {
            return Ok(text_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Request body too large",
                false,
                true,
            ));
        }
        Ok(Err(BoundedRequestBodyError::Invalid)) => {
            return Ok(text_response(
                StatusCode::BAD_REQUEST,
                "Invalid request body",
                false,
                true,
            ));
        }
        Err(_) => {
            return Ok(text_response(
                StatusCode::REQUEST_TIMEOUT,
                "Request body timed out",
                false,
                true,
            ));
        }
    };

    let is_read_route = is_health_path
        || is_capabilities_path
        || is_daemon_status_path
        || is_workspace_memory_path
        || is_workspace_git_path
        || is_workspace_git_branches_path
        || is_workspace_git_log_path
        || is_workspace_git_log_commit_path
        || is_workspace_git_diff_path
        || is_workspace_git_diff_file_path
        || session_status_id.is_some()
        || transcript_events_id.is_some()
        || transcript_records_id.is_some();
    if !(is_read_method && is_read_route) && git_mutation_route.is_none() {
        return Ok(text_response(
            StatusCode::NOT_FOUND,
            "Not Found",
            is_head,
            false,
        ));
    }

    if let Some(route) = git_mutation_route {
        let mutation_body = match parse_workspace_git_mutation_body(
            mutation_content_type.as_deref(),
            captured_body.as_deref().unwrap_or_default(),
        ) {
            Ok(body) => body,
            Err(()) => {
                return Ok(json_value_response(
                    StatusCode::BAD_REQUEST,
                    json!({
                        "error": "Invalid JSON request body",
                        "code": "invalid_json"
                    }),
                    false,
                    false,
                ));
            }
        };
        if context.auth_token_hash.is_none() {
            return Ok(json_value_response(
                StatusCode::UNAUTHORIZED,
                json!({
                    "error": "This route requires the daemon to be configured with a bearer token. Set QWEN_SERVER_TOKEN or pass --token to enable bearer auth.",
                    "code": "token_required"
                }),
                false,
                false,
            ));
        }
        return Ok(workspace_git_mutation_response(&context, route, mutation_body).await);
    }

    if is_health_path && is_deep_health {
        let mut response = json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            HEALTH_BOOTSTRAP,
            is_head,
            false,
            None,
        );
        response
            .headers_mut()
            .insert(RETRY_AFTER, "1".parse().unwrap());
        return Ok(response);
    }

    if is_health_path {
        return Ok(json_response(
            StatusCode::OK,
            HEALTH_OK,
            is_head,
            false,
            None,
        ));
    }

    if is_capabilities_path {
        return Ok(json_value_response(
            StatusCode::OK,
            capabilities_body(&context),
            is_head,
            false,
        ));
    }

    if is_workspace_memory_path {
        return Ok(workspace_memory_response(&context, is_head).await);
    }

    if is_workspace_git_path {
        return Ok(workspace_git_response(&context, query.as_deref(), is_head).await);
    }

    if is_workspace_git_branches_path {
        return Ok(workspace_git_branches_response(&context, is_head).await);
    }

    if is_workspace_git_log_path {
        return Ok(workspace_git_log_list_response(&context, query.as_deref(), is_head).await);
    }

    if is_workspace_git_log_commit_path {
        return Ok(workspace_git_log_commit_response(&context, query.as_deref(), is_head).await);
    }

    if is_workspace_git_diff_path {
        return Ok(workspace_git_diff_response(&context, is_head).await);
    }

    if is_workspace_git_diff_file_path {
        return Ok(workspace_git_diff_file_response(&context, query.as_deref(), is_head).await);
    }

    if let Some(session_id) = session_status_id {
        return Ok(session_status_response(&context, &session_id, is_head).await);
    }

    if let Some(session_id) = transcript_events_id {
        return Ok(session_transcript_events_response(
            &context,
            &session_id,
            query.as_deref(),
            is_head,
        )
        .await);
    }

    if let Some(session_id) = transcript_records_id {
        return Ok(session_transcript_records_response(
            &context,
            &session_id,
            query.as_deref(),
            is_head,
        )
        .await);
    }

    let Some(detail) = parse_daemon_status_detail(query.as_deref()) else {
        return Ok(json_response(
            StatusCode::BAD_REQUEST,
            INVALID_STATUS_DETAIL,
            is_head,
            false,
            None,
        ));
    };
    let body = daemon_status_body(&context, detail).await;
    Ok(json_value_response(StatusCode::OK, body, is_head, false))
}

fn session_status_route_id(path: &str) -> Option<&str> {
    let path = path.strip_suffix('/').unwrap_or(path);
    let mut segments = path.split('/');
    let (Some(""), Some(resource), Some(session_id), Some(status), None) = (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) else {
        return None;
    };
    (resource.eq_ignore_ascii_case("session") && status.eq_ignore_ascii_case("status"))
        .then_some(session_id)
}

fn session_transcript_route_id(path: &str) -> Option<&str> {
    let path = path.strip_suffix('/').unwrap_or(path);
    let mut segments = path.split('/');
    let (Some(""), Some(resource), Some(session_id), Some(transcript), None) = (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) else {
        return None;
    };
    (resource.eq_ignore_ascii_case("session") && transcript.eq_ignore_ascii_case("transcript"))
        .then_some(session_id)
}

fn session_transcript_records_route_id(path: &str) -> Option<&str> {
    let path = path.strip_suffix('/').unwrap_or(path);
    let mut segments = path.split('/');
    let (Some(""), Some(resource), Some(session_id), Some(transcript), Some(records), None) = (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) else {
        return None;
    };
    (resource.eq_ignore_ascii_case("session")
        && transcript.eq_ignore_ascii_case("transcript")
        && records.eq_ignore_ascii_case("records"))
    .then_some(session_id)
}

async fn workspace_memory_response(
    context: &ServeContext,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let Ok(permit) = Arc::clone(&context.workspace_memory_lookup_slots).try_acquire_owned() else {
        return json_value_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace memory discovery is busy",
                "code": "memory_discovery_busy"
            }),
            head_only,
            false,
        );
    };
    let settings_paths = context.workspace_memory_settings_paths.clone();
    let context_filenames = context.workspace_memory_context_filenames.clone();
    let lookup = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        collect_workspace_memory_status(&settings_paths, &context_filenames)
    });

    match timeout(WORKSPACE_MEMORY_LOOKUP_TIMEOUT, lookup).await {
        Ok(Ok(Ok(body))) => bounded_workspace_memory_response(body, head_only),
        Ok(Ok(Err(WorkspaceMemoryCollectionError::Untrusted))) => json_value_response(
            StatusCode::FORBIDDEN,
            json!({
                "error": "Workspace is not trusted.",
                "code": "untrusted_workspace"
            }),
            head_only,
            false,
        ),
        Ok(Ok(Err(WorkspaceMemoryCollectionError::Discovery(error)))) => {
            eprintln!("Could not discover workspace memory: {error}");
            workspace_memory_error_response(head_only)
        }
        Ok(Err(error)) => {
            eprintln!("Workspace memory lookup task failed: {error}");
            workspace_memory_error_response(head_only)
        }
        Err(_) => {
            eprintln!("Workspace memory discovery exceeded its time limit");
            workspace_memory_error_response(head_only)
        }
    }
}

enum WorkspaceMemoryCollectionError {
    Untrusted,
    Discovery(String),
}

fn collect_workspace_memory_status(
    settings_paths: &SettingsPaths,
    filenames: &[String],
) -> Result<Value, WorkspaceMemoryCollectionError> {
    if !is_workspace_memory_trusted(settings_paths) {
        return Err(WorkspaceMemoryCollectionError::Untrusted);
    }

    if filenames.len() > MAX_WORKSPACE_MEMORY_CONTEXT_FILENAMES
        || filenames
            .iter()
            .any(|filename| filename.len() > MAX_WORKSPACE_MEMORY_FILENAME_BYTES)
    {
        return Err(WorkspaceMemoryCollectionError::Discovery(
            "Configured workspace memory filenames exceed native limits".to_owned(),
        ));
    }

    let workspace_cwd = settings_paths.workspace_dir.as_path();
    let global_dir = settings_paths
        .user
        .parent()
        .map(|directory| directory.to_path_buf())
        .unwrap_or_else(Storage::get_global_canopy_dir);
    let mut files = Vec::new();
    let mut errors = Vec::new();
    let mut total_bytes = 0_u64;

    for (scope, root) in [
        ("workspace", workspace_cwd),
        ("global", global_dir.as_path()),
    ] {
        for filename in filenames {
            let candidate = root.join(filename);
            let candidate_path = candidate.to_string_lossy().into_owned();
            if candidate_path.len() > MAX_WORKSPACE_MEMORY_PATH_BYTES {
                errors.push(json!({
                    "kind": "memory_file",
                    "status": "error",
                    "error": "Memory file path exceeds the native metadata limit",
                    "errorKind": "stat_failed",
                    "hint": truncate_memory_string(
                        &candidate_path,
                        MAX_WORKSPACE_MEMORY_PATH_BYTES
                    )
                }));
                continue;
            }

            match std::fs::metadata(&candidate) {
                Ok(metadata) if metadata.is_file() => {
                    let bytes = metadata.len();
                    total_bytes = total_bytes.saturating_add(bytes);
                    files.push(json!({
                        "kind": "memory_file",
                        "path": candidate_path,
                        "scope": scope,
                        "bytes": bytes
                    }));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => errors.push(json!({
                    "kind": "memory_file",
                    "status": "error",
                    "error": truncate_memory_string(
                        &error.to_string(),
                        MAX_WORKSPACE_MEMORY_ERROR_BYTES
                    ),
                    "errorKind": "stat_failed",
                    "hint": candidate_path
                })),
            }
        }
    }

    if files.is_empty() && errors.is_empty() {
        return Ok(json!({
            "v": 1,
            "workspaceCwd": workspace_cwd.to_string_lossy(),
            "initialized": false,
            "files": [],
            "totalBytes": 0,
            "fileCount": 0,
            "ruleCount": 0
        }));
    }

    let file_count = files.len();
    let mut body = json!({
        "v": 1,
        "workspaceCwd": workspace_cwd.to_string_lossy(),
        "initialized": true,
        "files": files,
        "totalBytes": total_bytes,
        "fileCount": file_count,
        "ruleCount": 0
    });
    if !errors.is_empty() {
        body["errors"] = json!(errors);
    }
    Ok(body)
}

fn workspace_memory_context_filenames(paths: &SettingsPaths) -> Vec<String> {
    let defaults = || {
        vec![
            DEFAULT_CONTEXT_FILENAME.to_owned(),
            AGENT_CONTEXT_FILENAME.to_owned(),
        ]
    };
    let trusted = is_workspace_memory_trusted(paths);
    let Ok((system, system_defaults, user, workspace)) = read_memory_settings(paths) else {
        return defaults();
    };
    let merged = merge_settings(&system, &system_defaults, &user, &workspace, trusted);
    let resolved = resolve_env_vars_in_object(&Value::Object(merged), None);
    let Some(configured) = resolved
        .get("context")
        .and_then(Value::as_object)
        .and_then(|context| context.get("fileName"))
    else {
        return defaults();
    };

    let filenames = match configured {
        Value::String(filename) if !filename.trim().is_empty() => {
            vec![filename.trim().to_owned()]
        }
        Value::Array(filenames) if !filenames.is_empty() => {
            let Some(filenames) = filenames
                .iter()
                .map(Value::as_str)
                .collect::<Option<Vec<_>>>()
            else {
                return defaults();
            };
            filenames
                .into_iter()
                .map(|filename| filename.trim().to_owned())
                .collect()
        }
        _ => return defaults(),
    };

    if filenames.len() > MAX_WORKSPACE_MEMORY_CONTEXT_FILENAMES
        || filenames
            .iter()
            .any(|filename| filename.len() > MAX_WORKSPACE_MEMORY_FILENAME_BYTES)
    {
        defaults()
    } else {
        filenames
    }
}

fn is_workspace_memory_trusted(paths: &SettingsPaths) -> bool {
    let system = match read_memory_settings_scope(&paths.system, SettingScope::System) {
        Ok(settings) => settings,
        Err(_) => return false,
    };
    let user = match read_memory_settings_scope(&paths.user, SettingScope::User) {
        Ok(settings) => settings,
        Err(_) => return false,
    };
    let system_defaults =
        match read_memory_settings_scope(&paths.system_defaults, SettingScope::SystemDefaults) {
            Ok(settings) => settings,
            Err(_) => return false,
        };

    let system_enabled = match folder_trust_enabled(&system) {
        Ok(enabled) => enabled,
        Err(()) => return false,
    };
    let user_enabled = match folder_trust_enabled(&user) {
        Ok(enabled) => enabled,
        Err(()) => return false,
    };
    let defaults_enabled = match folder_trust_enabled(&system_defaults) {
        Ok(enabled) => enabled,
        Err(()) => return false,
    };
    let trust_enabled = system_enabled
        .or(user_enabled)
        .or(defaults_enabled)
        .unwrap_or(false);
    if !trust_enabled {
        return true;
    }

    let trust_path = paths.trusted_folders_path.clone().unwrap_or_else(|| {
        paths
            .user
            .parent()
            .map(|directory| directory.join(TRUSTED_FOLDERS_FILENAME))
            .unwrap_or_else(|| Storage::get_global_canopy_dir().join(TRUSTED_FOLDERS_FILENAME))
    });
    let trusted_config =
        match read_policy_json_object(&trust_path, MAX_WORKSPACE_MEMORY_TRUST_BYTES, true) {
            Ok(Some(config)) => config,
            Ok(None) => serde_json::Map::new(),
            Err(_) => return false,
        };
    let mut rules = Vec::with_capacity(trusted_config.len());
    for (rule_path, raw_level) in trusted_config {
        let Some(level) = raw_level.as_str().and_then(parse_trust_level) else {
            return false;
        };
        rules.push(TrustRule {
            path: PathBuf::from(rule_path),
            trust_level: level,
        });
    }

    resolve_trust_decision(&rules, &paths.workspace_dir, &paths.process_cwd) == Some(true)
}

fn folder_trust_enabled(settings: &serde_json::Map<String, Value>) -> Result<Option<bool>, ()> {
    let Some(security) = settings.get("security") else {
        return Ok(None);
    };
    let Some(security) = security.as_object() else {
        return Err(());
    };
    let Some(folder_trust) = security.get("folderTrust") else {
        return Ok(None);
    };
    let Some(folder_trust) = folder_trust.as_object() else {
        return Err(());
    };
    let Some(enabled) = folder_trust.get("enabled") else {
        return Ok(None);
    };
    enabled.as_bool().map(Some).ok_or(())
}

fn parse_trust_level(value: &str) -> Option<TrustLevel> {
    match value {
        "TRUST_FOLDER" => Some(TrustLevel::TrustFolder),
        "TRUST_PARENT" => Some(TrustLevel::TrustParent),
        "DO_NOT_TRUST" => Some(TrustLevel::DoNotTrust),
        _ => None,
    }
}

fn read_memory_settings(
    paths: &SettingsPaths,
) -> Result<
    (
        serde_json::Map<String, Value>,
        serde_json::Map<String, Value>,
        serde_json::Map<String, Value>,
        serde_json::Map<String, Value>,
    ),
    String,
> {
    Ok((
        read_memory_settings_scope(&paths.system, SettingScope::System)?,
        read_memory_settings_scope(&paths.system_defaults, SettingScope::SystemDefaults)?,
        read_memory_settings_scope(&paths.user, SettingScope::User)?,
        read_memory_settings_scope(&paths.workspace, SettingScope::Workspace)?,
    ))
}

fn read_memory_settings_scope(
    path: &Path,
    scope: SettingScope,
) -> Result<serde_json::Map<String, Value>, String> {
    let Some(value) = read_policy_json_object(path, MAX_WORKSPACE_MEMORY_SETTINGS_BYTES, false)?
    else {
        return Ok(serde_json::Map::new());
    };
    let value = Value::Object(value);
    let migrated = if settings_need_migration(&value) {
        run_migrations(&value, scope.as_source_name()).settings
    } else {
        value
    };
    Ok(resolve_env_vars_in_object(&migrated, None)
        .as_object()
        .cloned()
        .unwrap_or_default())
}

fn read_policy_json_object(
    path: &Path,
    max_bytes: u64,
    reject_symlink: bool,
) -> Result<Option<serde_json::Map<String, Value>>, String> {
    let link_metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    if (reject_symlink && link_metadata.file_type().is_symlink())
        || (!link_metadata.file_type().is_symlink() && !link_metadata.is_file())
    {
        return Err("Policy input must be a regular file".to_owned());
    }

    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err("Policy input must be a regular file".to_owned());
    }
    if metadata.len() > max_bytes {
        return Err(format!("Policy input exceeds {max_bytes} bytes"));
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > max_bytes {
        return Err(format!("Policy input exceeds {max_bytes} bytes"));
    }
    let contents = String::from_utf8(bytes).map_err(|error| error.to_string())?;
    let parsed = serde_json::from_str::<Value>(&strip_json_comments(&contents))
        .map_err(|error| error.to_string())?;
    parsed
        .as_object()
        .cloned()
        .map(Some)
        .ok_or_else(|| "Policy input must contain a JSON object".to_owned())
}

fn truncate_memory_string(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = 0;
    for (index, character) in value.char_indices() {
        if index + character.len_utf8() > max_bytes {
            break;
        }
        end = index + character.len_utf8();
    }
    format!("{}…", &value[..end])
}

fn bounded_workspace_memory_response(body: Value, head_only: bool) -> Response<Full<Bytes>> {
    let bytes = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
    if bytes.len() > MAX_WORKSPACE_MEMORY_RESPONSE_BYTES {
        return json_value_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            json!({
                "error": "Workspace memory response exceeds the native response limit",
                "code": "memory_response_too_large",
                "maxBytes": MAX_WORKSPACE_MEMORY_RESPONSE_BYTES
            }),
            head_only,
            false,
        );
    }
    json_bytes_response(StatusCode::OK, bytes, head_only, false)
}

fn workspace_memory_error_response(head_only: bool) -> Response<Full<Bytes>> {
    json_value_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        json!({
            "error": "Failed to discover workspace memory",
            "code": "memory_discovery_failed"
        }),
        head_only,
        false,
    )
}

async fn workspace_git_response(
    context: &ServeContext,
    query: Option<&str>,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let Ok(permit) = Arc::clone(&context.workspace_git_lookup_slots).try_acquire_owned() else {
        return json_value_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace Git status lookup is busy",
                "code": "git_status_busy"
            }),
            head_only,
            false,
        );
    };

    let settings_paths = context.workspace_memory_settings_paths.clone();
    let workspace_cwd = PathBuf::from(&context.workspace_cwd);
    let wait_for_fresh = workspace_git_wait_requested(query);
    let lookup = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if !is_workspace_memory_trusted(&settings_paths) {
            return Err(());
        }
        Ok(workspace_git_status_body(&workspace_cwd, wait_for_fresh))
    });

    match timeout(WORKSPACE_GIT_LOOKUP_TIMEOUT, lookup).await {
        Ok(Ok(Ok(body))) => json_value_response(StatusCode::OK, body, head_only, false),
        Ok(Ok(Err(()))) => json_value_response(
            StatusCode::FORBIDDEN,
            json!({
                "error": "Workspace is not trusted.",
                "code": "untrusted_workspace"
            }),
            head_only,
            false,
        ),
        Ok(Err(error)) => {
            eprintln!("Workspace Git status task failed: {error}");
            json_value_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "Failed to read workspace Git status", "code": "git_status_failed" }),
                head_only,
                false,
            )
        }
        Err(_) => json_value_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace Git status lookup timed out",
                "code": "git_status_timeout"
            }),
            head_only,
            false,
        ),
    }
}

async fn workspace_git_branches_response(
    context: &ServeContext,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let Ok(permit) = Arc::clone(&context.workspace_git_branches_lookup_slots).try_acquire_owned()
    else {
        return workspace_git_branches_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace Git branch lookup is busy",
                "code": "git_branches_busy"
            }),
            head_only,
        );
    };

    let settings_paths = context.workspace_memory_settings_paths.clone();
    let workspace_cwd = PathBuf::from(&context.workspace_cwd);
    let lookup = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if !is_workspace_memory_trusted(&settings_paths) {
            return Err(WorkspaceGitBranchesLookupError::Untrusted);
        }
        crate::git_branches::fetch_git_branches(&workspace_cwd)
            .map_err(WorkspaceGitBranchesLookupError::Git)
    });

    match timeout(WORKSPACE_GIT_BRANCHES_LOOKUP_TIMEOUT, lookup).await {
        Ok(Ok(Ok(branches))) => {
            let mut body = match serde_json::to_value(branches) {
                Ok(Value::Object(body)) => Value::Object(body),
                Ok(_) => {
                    return workspace_git_branches_json_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        json!({
                            "error": "Failed to serialize workspace Git branches",
                            "code": "git_branches_failed"
                        }),
                        head_only,
                    );
                }
                Err(error) => {
                    eprintln!("Workspace Git branches serialization failed: {error}");
                    return workspace_git_branches_json_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        json!({
                            "error": "Failed to serialize workspace Git branches",
                            "code": "git_branches_failed"
                        }),
                        head_only,
                    );
                }
            };
            if let Some(body) = body.as_object_mut() {
                body.insert("v".to_owned(), Value::from(1));
                body.insert(
                    "workspaceCwd".to_owned(),
                    Value::String(context.workspace_cwd.clone()),
                );
                body.insert("available".to_owned(), Value::Bool(true));
            }
            workspace_git_branches_json_response(StatusCode::OK, body, head_only)
        }
        Ok(Ok(Err(WorkspaceGitBranchesLookupError::Untrusted))) => {
            workspace_git_branches_json_response(
                StatusCode::FORBIDDEN,
                json!({
                    "error": "Workspace is not trusted.",
                    "code": "untrusted_workspace"
                }),
                head_only,
            )
        }
        Ok(Ok(Err(WorkspaceGitBranchesLookupError::Git(
            crate::git_branches::GitBranchesError::NotRepository,
        )))) => workspace_git_branches_json_response(
            StatusCode::NOT_FOUND,
            json!({
                "error": "not_a_git_repository",
                "message": "Workspace is not a Git repository."
            }),
            head_only,
        ),
        Ok(Ok(Err(WorkspaceGitBranchesLookupError::Git(
            crate::git_branches::GitBranchesError::TimedOut { .. },
        )))) => workspace_git_branches_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace Git branch lookup timed out",
                "code": "git_branches_timeout"
            }),
            head_only,
        ),
        Ok(Ok(Err(WorkspaceGitBranchesLookupError::Git(
            crate::git_branches::GitBranchesError::OutputLimitExceeded { .. },
        )))) => workspace_git_branches_json_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            json!({
                "error": "Workspace Git branch output is too large",
                "code": "git_branches_output_too_large"
            }),
            head_only,
        ),
        Ok(Ok(Err(WorkspaceGitBranchesLookupError::Git(error)))) => {
            eprintln!("Workspace Git branches lookup failed: {error:?}");
            workspace_git_branches_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": "Failed to read workspace Git branches",
                    "code": "git_branches_failed"
                }),
                head_only,
            )
        }
        Ok(Err(error)) => {
            eprintln!("Workspace Git branches task failed: {error}");
            workspace_git_branches_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": "Failed to read workspace Git branches",
                    "code": "git_branches_failed"
                }),
                head_only,
            )
        }
        Err(_) => workspace_git_branches_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace Git branch lookup timed out",
                "code": "git_branches_timeout"
            }),
            head_only,
        ),
    }
}

enum WorkspaceGitBranchesLookupError {
    Untrusted,
    Git(crate::git_branches::GitBranchesError),
}

#[derive(Clone, Copy)]
enum WorkspaceGitMutationRoute {
    Checkout,
    Branch,
    Push,
    Pull,
    Commit,
}

impl WorkspaceGitMutationRoute {
    fn path(self) -> &'static str {
        match self {
            Self::Checkout => "/workspace/git/checkout",
            Self::Branch => "/workspace/git/branch",
            Self::Push => "/workspace/git/push",
            Self::Pull => "/workspace/git/pull",
            Self::Commit => "/workspace/git/commit",
        }
    }

    fn route(self) -> String {
        format!("POST {}", self.path())
    }
}

fn workspace_git_mutation_route(method: &Method, path: &str) -> Option<WorkspaceGitMutationRoute> {
    if method != Method::POST {
        return None;
    }
    if matches_route_path(path, "/workspace/git/checkout") {
        Some(WorkspaceGitMutationRoute::Checkout)
    } else if matches_route_path(path, "/workspace/git/branch") {
        Some(WorkspaceGitMutationRoute::Branch)
    } else if matches_route_path(path, "/workspace/git/push") {
        Some(WorkspaceGitMutationRoute::Push)
    } else if matches_route_path(path, "/workspace/git/pull") {
        Some(WorkspaceGitMutationRoute::Pull)
    } else if matches_route_path(path, "/workspace/git/commit") {
        Some(WorkspaceGitMutationRoute::Commit)
    } else {
        None
    }
}

fn parse_workspace_git_mutation_body(content_type: Option<&str>, body: &[u8]) -> Result<Value, ()> {
    let is_json = content_type
        .and_then(|content_type| content_type.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"));
    if !is_json || body.is_empty() {
        return Ok(Value::Object(serde_json::Map::new()));
    }
    let mut value = serde_json::from_slice::<Value>(body).map_err(|_| ())?;
    match &mut value {
        Value::Object(object) => {
            for key in ["__proto__", "constructor", "prototype"] {
                object.remove(key);
            }
        }
        Value::Array(_) => return Ok(Value::Object(serde_json::Map::new())),
        _ => return Err(()),
    }
    Ok(value)
}

enum WorkspaceGitMutationInput {
    Checkout(String),
    Branch(String, Option<String>),
    Push(GitPushOptions),
    Pull(GitPullOptions),
    Commit(String, GitCommitOptions),
}

fn validate_workspace_git_mutation(
    route: WorkspaceGitMutationRoute,
    body: &Value,
) -> Result<WorkspaceGitMutationInput, Value> {
    let empty_body = serde_json::Map::new();
    let body = body.as_object().unwrap_or(&empty_body);
    let string_field = |name: &str| body.get(name).and_then(Value::as_str);

    match route {
        WorkspaceGitMutationRoute::Checkout => {
            let Some(reference) = string_field("ref") else {
                return Err(json!({ "error": "missing_ref", "message": "ref is required" }));
            };
            if reference.trim().is_empty() {
                return Err(json!({ "error": "missing_ref", "message": "ref is required" }));
            }
            if reference.trim().starts_with('-') || !is_valid_checkout_ref(reference) {
                return Err(json!({
                    "error": "invalid_ref",
                    "message": "Invalid checkout ref"
                }));
            }
            Ok(WorkspaceGitMutationInput::Checkout(
                reference.trim().to_owned(),
            ))
        }
        WorkspaceGitMutationRoute::Branch => {
            let Some(name) = string_field("name") else {
                return Err(json!({
                    "error": "invalid_branch_name",
                    "message": "Invalid branch name"
                }));
            };
            if !is_valid_ref_name(name) || name.starts_with('-') {
                return Err(json!({
                    "error": "invalid_branch_name",
                    "message": "Invalid branch name"
                }));
            }
            let start_point = match body.get("startPoint") {
                None => None,
                Some(Value::String(start_point)) => {
                    let start_point = start_point.trim();
                    if start_point.is_empty() {
                        None
                    } else if !start_point.starts_with('-') && is_valid_checkout_ref(start_point) {
                        Some(start_point.to_owned())
                    } else {
                        return Err(json!({
                            "error": "invalid_start_point",
                            "message": "Invalid start point"
                        }));
                    }
                }
                Some(_) => {
                    return Err(json!({
                        "error": "invalid_start_point",
                        "message": "startPoint must be a string"
                    }));
                }
            };
            Ok(WorkspaceGitMutationInput::Branch(
                name.to_owned(),
                start_point,
            ))
        }
        WorkspaceGitMutationRoute::Push => {
            let set_upstream = match body.get("setUpstream") {
                None => false,
                Some(Value::Bool(value)) => *value,
                Some(_) => {
                    return Err(json!({
                        "error": "invalid_set_upstream",
                        "message": "setUpstream must be a boolean"
                    }));
                }
            };
            let force = match body.get("force") {
                None => false,
                Some(Value::Bool(value)) => *value,
                Some(_) => {
                    return Err(json!({
                        "error": "invalid_force",
                        "message": "force must be a boolean"
                    }));
                }
            };
            Ok(WorkspaceGitMutationInput::Push(GitPushOptions {
                set_upstream,
                force,
            }))
        }
        WorkspaceGitMutationRoute::Pull => {
            let rebase = match body.get("rebase") {
                None => false,
                Some(Value::Bool(value)) => *value,
                Some(_) => {
                    return Err(json!({
                        "error": "invalid_rebase",
                        "message": "rebase must be a boolean"
                    }));
                }
            };
            let fetch_only = match body.get("fetchOnly") {
                None => false,
                Some(Value::Bool(value)) => *value,
                Some(_) => {
                    return Err(json!({
                        "error": "invalid_fetch_only",
                        "message": "fetchOnly must be a boolean"
                    }));
                }
            };
            Ok(WorkspaceGitMutationInput::Pull(GitPullOptions {
                rebase,
                fetch_only,
            }))
        }
        WorkspaceGitMutationRoute::Commit => {
            let Some(message) = string_field("message") else {
                return Err(json!({
                    "error": "missing_message",
                    "message": "message is required"
                }));
            };
            if message.trim().is_empty() {
                return Err(json!({
                    "error": "missing_message",
                    "message": "message is required"
                }));
            }
            let all = match body.get("all") {
                None => false,
                Some(Value::Bool(value)) => *value,
                Some(_) => {
                    return Err(json!({
                        "error": "invalid_all",
                        "message": "all must be a boolean"
                    }));
                }
            };
            Ok(WorkspaceGitMutationInput::Commit(
                message.trim().to_owned(),
                GitCommitOptions { all },
            ))
        }
    }
}

fn run_workspace_git_mutation(
    workspace_cwd: &Path,
    input: WorkspaceGitMutationInput,
) -> Result<Value, GitBranchOperationError> {
    match input {
        WorkspaceGitMutationInput::Checkout(reference) => {
            let result = git_checkout(workspace_cwd, &reference)?;
            Ok(json!({ "branch": result.branch, "detached": result.detached }))
        }
        WorkspaceGitMutationInput::Branch(name, start_point) => {
            let result = git_create_branch(workspace_cwd, &name, start_point.as_deref())?;
            Ok(json!({ "branch": result.branch, "detached": result.detached }))
        }
        WorkspaceGitMutationInput::Push(options) => {
            let result = git_push(workspace_cwd, options)?;
            Ok(json!({ "success": result.success, "output": result.output }))
        }
        WorkspaceGitMutationInput::Pull(options) => {
            let result = git_pull(workspace_cwd, options)?;
            Ok(json!({ "success": result.success, "output": result.output }))
        }
        WorkspaceGitMutationInput::Commit(message, options) => {
            let result = git_commit(workspace_cwd, &message, options)?;
            Ok(json!({ "sha": result.sha, "subject": result.subject }))
        }
    }
}

enum WorkspaceGitMutationFailure {
    Untrusted,
    Invalid(Value),
    Git(GitBranchOperationError),
}

async fn workspace_git_mutation_response(
    context: &ServeContext,
    route: WorkspaceGitMutationRoute,
    body: Value,
) -> Response<Full<Bytes>> {
    let Ok(permit) = Arc::clone(&context.workspace_git_mutation_slots).try_acquire_owned() else {
        return workspace_git_mutation_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace Git mutation is busy",
                "code": "git_mutation_busy"
            }),
        );
    };
    let settings_paths = context.workspace_memory_settings_paths.clone();
    let workspace_cwd = PathBuf::from(&context.workspace_cwd);
    let lookup = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if !is_workspace_memory_trusted(&settings_paths) {
            return Err(WorkspaceGitMutationFailure::Untrusted);
        }
        let input = validate_workspace_git_mutation(route, &body)
            .map_err(WorkspaceGitMutationFailure::Invalid)?;
        // Recheck trust after validation and directly before starting Git.
        if !is_workspace_memory_trusted(&settings_paths) {
            return Err(WorkspaceGitMutationFailure::Untrusted);
        }
        run_workspace_git_mutation(&workspace_cwd, input).map_err(WorkspaceGitMutationFailure::Git)
    });

    match timeout(WORKSPACE_GIT_MUTATION_TIMEOUT, lookup).await {
        Ok(Ok(Ok(body))) => workspace_git_mutation_json_response(StatusCode::OK, body),
        Ok(Ok(Err(WorkspaceGitMutationFailure::Untrusted))) => {
            workspace_git_mutation_json_response(
                StatusCode::FORBIDDEN,
                json!({
                    "error": "Workspace is not trusted.",
                    "code": "untrusted_workspace"
                }),
            )
        }
        Ok(Ok(Err(WorkspaceGitMutationFailure::Invalid(body)))) => {
            workspace_git_mutation_json_response(StatusCode::BAD_REQUEST, body)
        }
        Ok(Ok(Err(WorkspaceGitMutationFailure::Git(error)))) => {
            workspace_git_mutation_error_response(&context.workspace_cwd, route, error)
        }
        Ok(Err(error)) => {
            eprintln!("Workspace Git mutation task failed: {error}");
            workspace_git_mutation_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": "Failed to update workspace Git state",
                    "code": "git_mutation_failed"
                }),
            )
        }
        Err(_) => workspace_git_mutation_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace Git mutation timed out",
                "code": "git_mutation_timeout"
            }),
        ),
    }
}

fn workspace_git_mutation_error_response(
    workspace_cwd: &str,
    route: WorkspaceGitMutationRoute,
    error: GitBranchOperationError,
) -> Response<Full<Bytes>> {
    let detail = match error {
        GitBranchOperationError::InvalidInput(message) => message,
        GitBranchOperationError::CommandFailed { stderr, .. } if !stderr.trim().is_empty() => {
            stderr
        }
        GitBranchOperationError::CommandFailed { .. } => "Git command failed".to_owned(),
        GitBranchOperationError::TimedOut { command } => format!("{command} timed out"),
        GitBranchOperationError::OutputLimitExceeded { command } => {
            format!("{command} exceeded the output limit")
        }
    };
    let detail = strip_terminal_control_sequences(&detail);
    let workspace_path = Path::new(workspace_cwd);
    let mut message = detail.replace(&workspace_path.to_string_lossy().to_string(), "<workspace>");
    if let Some(git_root) = find_git_root(workspace_path)
        && git_root != workspace_path
    {
        message = message.replace(&git_root.to_string_lossy().to_string(), "<workspace>");
    }
    message = truncate_utf8_bytes(&message, 512).to_owned();
    let classification = message.to_ascii_lowercase();

    let (status, code) = if classification.contains("not a git repository")
        || classification.contains("invalid reference")
    {
        (StatusCode::NOT_FOUND, Some("not_a_git_repository"))
    } else if classification.contains("dirty")
        || classification.contains("uncommitted")
        || classification.contains("would be overwritten")
    {
        (StatusCode::CONFLICT, Some("dirty_working_tree"))
    } else if classification.contains("already exists") {
        (StatusCode::CONFLICT, Some("branch_already_exists"))
    } else if classification.contains("nothing to commit") {
        (StatusCode::BAD_REQUEST, Some("nothing_to_commit"))
    } else if classification.contains("detached head") {
        (StatusCode::CONFLICT, Some("detached_head"))
    } else if classification.contains("no upstream")
        || classification.contains("no tracking information")
    {
        (StatusCode::BAD_REQUEST, Some("no_upstream"))
    } else {
        eprintln!("canopy serve: {} failed: {message}", route.route());
        return workspace_git_mutation_json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": message }),
        );
    };
    workspace_git_mutation_json_response(status, json!({ "error": code, "message": message }))
}

fn truncate_utf8_bytes(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn workspace_git_mutation_json_response(status: StatusCode, body: Value) -> Response<Full<Bytes>> {
    let mut serialized = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
    let status = if serialized.len() > MAX_WORKSPACE_GIT_MUTATION_RESPONSE_BYTES {
        serialized = serde_json::to_vec(&json!({
            "error": "Workspace Git mutation response is too large",
            "code": "git_mutation_response_too_large"
        }))
        .unwrap_or_else(|_| b"{}".to_vec());
        StatusCode::PAYLOAD_TOO_LARGE
    } else {
        status
    };
    let mut response = json_bytes_response(status, serialized, false, false);
    response.headers_mut().insert(
        CACHE_CONTROL,
        hyper::header::HeaderValue::from_static("no-store"),
    );
    response
}

fn workspace_git_branches_json_response(
    status: StatusCode,
    body: Value,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let mut serialized = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
    let status = if serialized.len() > MAX_WORKSPACE_GIT_BRANCHES_RESPONSE_BYTES {
        serialized = serde_json::to_vec(&json!({
            "error": "Workspace Git branches response is too large",
            "code": "git_branches_response_too_large"
        }))
        .unwrap_or_else(|_| b"{}".to_vec());
        StatusCode::PAYLOAD_TOO_LARGE
    } else {
        status
    };
    let mut response = json_bytes_response(status, serialized, head_only, false);
    response.headers_mut().insert(
        CACHE_CONTROL,
        hyper::header::HeaderValue::from_static("no-store"),
    );
    response.headers_mut().insert(
        hyper::header::HeaderName::from_static("x-content-type-options"),
        hyper::header::HeaderValue::from_static("nosniff"),
    );
    response
}

async fn workspace_git_diff_response(
    context: &ServeContext,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let Ok(permit) = Arc::clone(&context.workspace_git_diff_lookup_slots).try_acquire_owned()
    else {
        return workspace_git_diff_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace Git diff lookup is busy",
                "code": "git_diff_busy"
            }),
            head_only,
        );
    };

    let settings_paths = context.workspace_memory_settings_paths.clone();
    let workspace_cwd = PathBuf::from(&context.workspace_cwd);
    let lookup = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if !is_workspace_memory_trusted(&settings_paths) {
            return Err(());
        }
        Ok(workspace_git_diff_body(&workspace_cwd))
    });

    match timeout(WORKSPACE_GIT_DIFF_LOOKUP_TIMEOUT, lookup).await {
        Ok(Ok(Ok(body))) => workspace_git_diff_json_response(StatusCode::OK, body, head_only),
        Ok(Ok(Err(()))) => workspace_git_diff_json_response(
            StatusCode::FORBIDDEN,
            json!({
                "error": "Workspace is not trusted.",
                "code": "untrusted_workspace"
            }),
            head_only,
        ),
        Ok(Err(error)) => {
            eprintln!("Workspace Git diff task failed: {error}");
            workspace_git_diff_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": "Failed to read workspace Git diff",
                    "code": "git_diff_failed"
                }),
                head_only,
            )
        }
        Err(_) => workspace_git_diff_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace Git diff lookup timed out",
                "code": "git_diff_timeout"
            }),
            head_only,
        ),
    }
}

async fn workspace_git_diff_file_response(
    context: &ServeContext,
    query: Option<&str>,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let Ok(permit) = Arc::clone(&context.workspace_git_diff_lookup_slots).try_acquire_owned()
    else {
        return workspace_git_diff_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace Git diff lookup is busy",
                "code": "git_diff_busy"
            }),
            head_only,
        );
    };

    let settings_paths = context.workspace_memory_settings_paths.clone();
    let workspace_cwd = context.workspace_cwd.clone();
    let query = query.map(str::to_owned);
    let lookup = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if !is_workspace_memory_trusted(&settings_paths) {
            return Err(WorkspaceGitDiffFileLookupError::Untrusted);
        }
        let Some((path, old_path)) = parse_workspace_git_diff_file_query(query.as_deref()) else {
            return Err(WorkspaceGitDiffFileLookupError::InvalidQuery);
        };
        let result = crate::git_diff_hunks::fetch_git_diff_hunks_for_file(
            Path::new(&workspace_cwd),
            &path,
            old_path.as_deref(),
        );
        Ok((path, result))
    });

    match timeout(WORKSPACE_GIT_DIFF_LOOKUP_TIMEOUT, lookup).await {
        Ok(Ok(Ok((path, result)))) => {
            let hunks = result
                .as_ref()
                .map(|result| {
                    result
                        .hunks
                        .iter()
                        .map(|hunk| {
                            json!({
                                "oldStart": hunk.old_start,
                                "oldLines": hunk.old_lines,
                                "newStart": hunk.new_start,
                                "newLines": hunk.new_lines,
                                "lines": hunk.lines
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let mut body = json!({
                "v": 1,
                "workspaceCwd": context.workspace_cwd,
                "path": path,
                "available": !hunks.is_empty(),
                "hunks": hunks
            });
            if result.as_ref().is_some_and(|result| result.truncated) {
                body["truncated"] = Value::Bool(true);
            }
            workspace_git_diff_json_response(StatusCode::OK, body, head_only)
        }
        Ok(Ok(Err(WorkspaceGitDiffFileLookupError::Untrusted))) => {
            workspace_git_diff_json_response(
                StatusCode::FORBIDDEN,
                json!({
                    "error": "Workspace is not trusted.",
                    "code": "untrusted_workspace"
                }),
                head_only,
            )
        }
        Ok(Ok(Err(WorkspaceGitDiffFileLookupError::InvalidQuery))) => {
            workspace_git_diff_json_response(
                StatusCode::BAD_REQUEST,
                json!({
                    "errorKind": "parse_error",
                    "error": "path query parameter is required",
                    "status": 400
                }),
                head_only,
            )
        }
        Ok(Err(error)) => {
            eprintln!("Workspace Git diff file task failed: {error}");
            workspace_git_diff_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": "Failed to read workspace Git diff",
                    "code": "git_diff_failed"
                }),
                head_only,
            )
        }
        Err(_) => workspace_git_diff_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace Git diff lookup timed out",
                "code": "git_diff_timeout"
            }),
            head_only,
        ),
    }
}

#[derive(Clone, Copy, Debug)]
enum WorkspaceGitDiffFileLookupError {
    Untrusted,
    InvalidQuery,
}

fn parse_workspace_git_diff_file_query(query: Option<&str>) -> Option<(String, Option<String>)> {
    let mut path = None;
    let mut repeated_path = false;
    let mut old_path = None;
    let mut repeated_old_path = false;
    for (key, value) in form_urlencoded::parse(query.unwrap_or_default().as_bytes()) {
        if key == "path" {
            if path.replace(value.into_owned()).is_some() {
                repeated_path = true;
            }
        } else if key == "oldPath" && old_path.replace(value.into_owned()).is_some() {
            repeated_old_path = true;
        }
    }
    let path = path?;
    if repeated_path || path.is_empty() {
        return None;
    }
    let old_path = if repeated_old_path {
        None
    } else {
        old_path.filter(|path| !path.is_empty())
    };
    Some((path, old_path))
}

#[derive(Clone, Debug, Default)]
struct WorkspaceGitDiffStats {
    files_count: u64,
    lines_added: u64,
    lines_removed: u64,
}

#[derive(Clone, Debug)]
struct WorkspaceGitDiffFile {
    path: String,
    old_path: Option<String>,
    added: u64,
    removed: u64,
    is_binary: bool,
    is_untracked: bool,
    is_deleted: bool,
    truncated: bool,
}

fn workspace_git_diff_body(workspace_cwd: &Path) -> Value {
    let unavailable = || {
        json!({
            "v": 1,
            "workspaceCwd": workspace_cwd.to_string_lossy(),
            "available": false,
            "filesCount": 0,
            "linesAdded": 0,
            "linesRemoved": 0,
            "files": [],
            "hiddenCount": 0
        })
    };
    let Some(git_root) = find_git_root(workspace_cwd) else {
        return unavailable();
    };
    if resolve_git_directory(&git_root)
        .as_deref()
        .and_then(detect_git_operation)
        .is_some_and(|operation| matches!(operation, "merge" | "rebase" | "cherry-pick" | "revert"))
    {
        return unavailable();
    }

    let run = |args: &[&str], cap| {
        let args = args
            .iter()
            .map(|argument| (*argument).to_owned())
            .collect::<Vec<_>>();
        run_bounded_git_log_command(&git_root, &args, cap)
    };
    let shortstat = run(
        &[
            "--no-optional-locks",
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "HEAD",
            "--shortstat",
        ],
        16 * 1024,
    );
    let untracked_output = run(
        &[
            "--no-optional-locks",
            "ls-files",
            "-z",
            "--others",
            "--exclude-standard",
        ],
        MAX_WORKSPACE_GIT_DIFF_OUTPUT_BYTES,
    );
    let untracked_count = untracked_output
        .as_deref()
        .map(|output| output.iter().filter(|byte| **byte == 0).count() as u64)
        .unwrap_or(0);
    let mut stats = shortstat
        .as_deref()
        .and_then(parse_workspace_git_shortstat)
        .unwrap_or_default();

    if stats.files_count.saturating_add(untracked_count) > MAX_WORKSPACE_GIT_DIFF_DETAILS as u64 {
        stats.files_count = stats.files_count.saturating_add(untracked_count);
        return workspace_git_diff_body_from_parts(workspace_cwd, stats, Vec::new());
    }

    let Some(numstat_output) = run(
        &[
            "--no-optional-locks",
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "HEAD",
            "--numstat",
            "-z",
        ],
        MAX_WORKSPACE_GIT_DIFF_OUTPUT_BYTES,
    ) else {
        return unavailable();
    };
    let mut tracked_stats = WorkspaceGitDiffStats::default();
    let mut files = parse_workspace_git_numstat(&numstat_output, &mut tracked_stats);
    stats = tracked_stats;
    let deleted_paths = run(
        &[
            "--no-optional-locks",
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "HEAD",
            "--name-status",
            "-z",
        ],
        MAX_WORKSPACE_GIT_DIFF_OUTPUT_BYTES,
    )
    .as_deref()
    .map(parse_workspace_git_deleted_paths);
    if let Some(deleted_paths) = deleted_paths {
        for file in &mut files {
            file.is_deleted = deleted_paths.contains(&file.path);
        }
    }

    let untracked_paths = untracked_output
        .as_deref()
        .map(parse_workspace_git_untracked_paths)
        .unwrap_or_default();
    stats.files_count = stats.files_count.saturating_add(untracked_count);
    let mut untracked_stats = Vec::with_capacity(untracked_paths.len());
    for path in &untracked_paths {
        untracked_stats.push((
            path.clone(),
            count_workspace_git_untracked_file(&git_root, path),
        ));
    }
    for (_, line_stats) in &untracked_stats {
        stats.lines_added = stats.lines_added.saturating_add(line_stats.added);
    }

    let remaining = MAX_WORKSPACE_GIT_DIFF_FILES.saturating_sub(files.len());
    files.extend(
        untracked_stats
            .into_iter()
            .take(remaining)
            .map(|(path, line_stats)| WorkspaceGitDiffFile {
                path,
                old_path: None,
                added: line_stats.added,
                removed: 0,
                is_binary: line_stats.is_binary,
                is_untracked: true,
                is_deleted: false,
                truncated: line_stats.truncated,
            }),
    );
    workspace_git_diff_body_from_parts(workspace_cwd, stats, files)
}

fn workspace_git_diff_body_from_parts(
    workspace_cwd: &Path,
    stats: WorkspaceGitDiffStats,
    files: Vec<WorkspaceGitDiffFile>,
) -> Value {
    let visible_count = files.len() as u64;
    let files = files
        .into_iter()
        .map(|file| {
            let mut value = json!({
                "path": file.path,
                "added": file.added,
                "removed": file.removed,
                "isBinary": file.is_binary,
                "isUntracked": file.is_untracked,
                "isDeleted": file.is_deleted,
                "truncated": file.truncated
            });
            if let Some(old_path) = file.old_path {
                value["oldPath"] = Value::String(old_path);
            }
            value
        })
        .collect::<Vec<_>>();
    json!({
        "v": 1,
        "workspaceCwd": workspace_cwd.to_string_lossy(),
        "available": true,
        "filesCount": stats.files_count,
        "linesAdded": stats.lines_added,
        "linesRemoved": stats.lines_removed,
        "files": files,
        "hiddenCount": stats.files_count.saturating_sub(visible_count)
    })
}

fn parse_workspace_git_shortstat(output: &[u8]) -> Option<WorkspaceGitDiffStats> {
    let output = String::from_utf8_lossy(output);
    let line = output.lines().find(|line| !line.trim().is_empty())?;
    let tokens = line.split_whitespace().collect::<Vec<_>>();
    let files_count = tokens.first()?.parse::<u64>().ok()?;
    if !matches!(tokens.get(1).copied(), Some("file" | "files"))
        || tokens.get(2).copied() != Some("changed,")
    {
        return None;
    }
    let mut stats = WorkspaceGitDiffStats {
        files_count,
        ..WorkspaceGitDiffStats::default()
    };
    for pair in tokens.windows(2) {
        let Ok(count) = pair[0].parse::<u64>() else {
            continue;
        };
        match pair[1].trim_end_matches(',') {
            "insertion(+)" | "insertions(+)" => stats.lines_added = count,
            "deletion(-)" | "deletions(-)" => stats.lines_removed = count,
            _ => {}
        }
    }
    Some(stats)
}

fn parse_workspace_git_numstat(
    output: &[u8],
    stats: &mut WorkspaceGitDiffStats,
) -> Vec<WorkspaceGitDiffFile> {
    let mut files = Vec::with_capacity(MAX_WORKSPACE_GIT_DIFF_FILES);
    let mut tokens = output.split(|byte| *byte == 0);
    let mut pending_rename: Option<(u64, u64, bool)> = None;
    let mut rename_old_path: Option<String> = None;
    while let Some(token) = tokens.next() {
        if token.is_empty() {
            continue;
        }
        if let Some((added, removed, is_binary)) = pending_rename {
            let path = String::from_utf8_lossy(token).into_owned();
            if let Some(old_path) = rename_old_path.take() {
                commit_workspace_git_diff_file(
                    &mut files,
                    stats,
                    WorkspaceGitDiffFile {
                        path,
                        old_path: Some(old_path),
                        added,
                        removed,
                        is_binary,
                        is_untracked: false,
                        is_deleted: false,
                        truncated: false,
                    },
                );
                pending_rename = None;
            } else {
                rename_old_path = Some(path);
            }
            continue;
        }
        let Some(first_tab) = token.iter().position(|byte| *byte == b'\t') else {
            continue;
        };
        let Some(relative_second_tab) = token[first_tab + 1..]
            .iter()
            .position(|byte| *byte == b'\t')
        else {
            continue;
        };
        let second_tab = first_tab + 1 + relative_second_tab;
        let added_text = &token[..first_tab];
        let removed_text = &token[first_tab + 1..second_tab];
        let raw_path = &token[second_tab + 1..];
        let is_binary = added_text == b"-" || removed_text == b"-";
        let added = if is_binary {
            0
        } else {
            std::str::from_utf8(added_text)
                .ok()
                .and_then(|text| text.parse::<u64>().ok())
                .unwrap_or(0)
        };
        let removed = if is_binary {
            0
        } else {
            std::str::from_utf8(removed_text)
                .ok()
                .and_then(|text| text.parse::<u64>().ok())
                .unwrap_or(0)
        };
        if raw_path.is_empty() {
            pending_rename = Some((added, removed, is_binary));
            rename_old_path = None;
            continue;
        }
        commit_workspace_git_diff_file(
            &mut files,
            stats,
            WorkspaceGitDiffFile {
                path: String::from_utf8_lossy(raw_path).into_owned(),
                old_path: None,
                added,
                removed,
                is_binary,
                is_untracked: false,
                is_deleted: false,
                truncated: false,
            },
        );
    }
    files
}

fn commit_workspace_git_diff_file(
    files: &mut Vec<WorkspaceGitDiffFile>,
    stats: &mut WorkspaceGitDiffStats,
    file: WorkspaceGitDiffFile,
) {
    stats.files_count = stats.files_count.saturating_add(1);
    stats.lines_added = stats.lines_added.saturating_add(file.added);
    stats.lines_removed = stats.lines_removed.saturating_add(file.removed);
    if files.len() < MAX_WORKSPACE_GIT_DIFF_FILES {
        files.push(file);
    }
}

fn parse_workspace_git_deleted_paths(output: &[u8]) -> HashSet<String> {
    let mut deleted = HashSet::new();
    let mut tokens = output
        .split(|byte| *byte == 0)
        .filter(|token| !token.is_empty());
    while let Some(status) = tokens.next() {
        if status
            .first()
            .is_some_and(|status| matches!(status, b'R' | b'C'))
        {
            let _old_path = tokens.next();
            let _new_path = tokens.next();
        } else if status.first() == Some(&b'D') {
            if let Some(path) = tokens.next()
                && deleted.len() < MAX_WORKSPACE_GIT_DIFF_DETAILS
            {
                deleted.insert(String::from_utf8_lossy(path).into_owned());
            }
        } else {
            let _path = tokens.next();
        }
    }
    deleted
}

fn parse_workspace_git_untracked_paths(output: &[u8]) -> Vec<String> {
    output
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .take(MAX_WORKSPACE_GIT_DIFF_DETAILS)
        .map(|path| String::from_utf8_lossy(path).into_owned())
        .collect()
}

#[derive(Clone, Copy, Debug, Default)]
struct WorkspaceGitUntrackedLines {
    added: u64,
    is_binary: bool,
    truncated: bool,
}

fn count_workspace_git_untracked_file(
    git_root: &Path,
    relative_path: &str,
) -> WorkspaceGitUntrackedLines {
    let path = Path::new(relative_path);
    if path.is_absolute()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return WorkspaceGitUntrackedLines {
            is_binary: true,
            ..WorkspaceGitUntrackedLines::default()
        };
    }
    let full_path = git_root.join(path);
    let Ok(link_metadata) = fs::symlink_metadata(&full_path) else {
        return WorkspaceGitUntrackedLines {
            is_binary: true,
            ..WorkspaceGitUntrackedLines::default()
        };
    };
    if !link_metadata.is_file() {
        return WorkspaceGitUntrackedLines {
            is_binary: true,
            ..WorkspaceGitUntrackedLines::default()
        };
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
    }
    let Ok(mut file) = options.open(&full_path) else {
        return WorkspaceGitUntrackedLines {
            is_binary: true,
            ..WorkspaceGitUntrackedLines::default()
        };
    };
    let Ok(opened_metadata) = file.metadata() else {
        return WorkspaceGitUntrackedLines {
            is_binary: true,
            ..WorkspaceGitUntrackedLines::default()
        };
    };
    if !opened_metadata.is_file() {
        return WorkspaceGitUntrackedLines {
            is_binary: true,
            ..WorkspaceGitUntrackedLines::default()
        };
    }

    let cap = usize::try_from(MAX_WORKSPACE_GIT_UNTRACKED_FILE_BYTES).unwrap_or(usize::MAX);
    let mut buffer = [0_u8; WORKSPACE_GIT_UNTRACKED_READ_CHUNK_BYTES];
    let mut bytes_read = 0usize;
    let mut lines = 0u64;
    let mut sniffed = 0usize;
    let mut last_byte = None;
    while bytes_read < cap {
        let amount = buffer.len().min(cap - bytes_read);
        let read = match file.read(&mut buffer[..amount]) {
            Ok(read) => read,
            Err(_) => {
                return WorkspaceGitUntrackedLines {
                    is_binary: true,
                    ..WorkspaceGitUntrackedLines::default()
                };
            }
        };
        if read == 0 {
            break;
        }
        let sniff_amount = read.min(WORKSPACE_GIT_BINARY_SNIFF_BYTES.saturating_sub(sniffed));
        if buffer[..sniff_amount].contains(&0) {
            return WorkspaceGitUntrackedLines {
                is_binary: true,
                ..WorkspaceGitUntrackedLines::default()
            };
        }
        sniffed = sniffed.saturating_add(sniff_amount);
        lines = lines
            .saturating_add(buffer[..read].iter().filter(|byte| **byte == b'\n').count() as u64);
        last_byte = buffer.get(read - 1).copied();
        bytes_read = bytes_read.saturating_add(read);
    }
    let truncated = file
        .metadata()
        .map(|metadata| metadata.len() > bytes_read as u64)
        .unwrap_or(false);
    if !truncated && last_byte.is_some_and(|byte| byte != b'\n') {
        lines = lines.saturating_add(1);
    }
    WorkspaceGitUntrackedLines {
        added: lines,
        is_binary: false,
        truncated,
    }
}

fn workspace_git_diff_json_response(
    status: StatusCode,
    body: Value,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let mut serialized = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
    let status = if serialized.len() > MAX_WORKSPACE_GIT_DIFF_RESPONSE_BYTES {
        serialized = serde_json::to_vec(&json!({
            "error": "Workspace Git diff response is too large",
            "code": "git_diff_response_too_large"
        }))
        .unwrap_or_else(|_| b"{}".to_vec());
        StatusCode::PAYLOAD_TOO_LARGE
    } else {
        status
    };
    let mut response = json_bytes_response(status, serialized, head_only, false);
    response.headers_mut().insert(
        CACHE_CONTROL,
        hyper::header::HeaderValue::from_static("no-store"),
    );
    response.headers_mut().insert(
        hyper::header::HeaderName::from_static("x-content-type-options"),
        hyper::header::HeaderValue::from_static("nosniff"),
    );
    response
}

#[derive(Clone, Debug)]
struct WorkspaceGitLogQuery {
    limit: usize,
    skip: u64,
    range: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkspaceGitLogReadError {
    Untrusted,
}

async fn workspace_git_log_list_response(
    context: &ServeContext,
    query: Option<&str>,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let query = match parse_workspace_git_log_query(query) {
        Ok(query) => query,
        Err(()) => {
            return workspace_git_log_json_response(
                StatusCode::BAD_REQUEST,
                json!({
                    "errorKind": "parse_error",
                    "error": "range query parameter is invalid",
                    "status": 400
                }),
                head_only,
            );
        }
    };

    workspace_git_log_lookup(context, head_only, move |workspace_cwd| {
        workspace_git_log_list_body(workspace_cwd, &query)
    })
    .await
}

async fn workspace_git_log_commit_response(
    context: &ServeContext,
    query: Option<&str>,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let sha = match query_parameter(query, "sha") {
        Ok(Some(sha)) if valid_workspace_git_sha(&sha) => sha,
        _ => {
            return workspace_git_log_json_response(
                StatusCode::BAD_REQUEST,
                json!({
                    "errorKind": "parse_error",
                    "error": "sha query parameter is required",
                    "status": 400
                }),
                head_only,
            );
        }
    };

    workspace_git_log_lookup(context, head_only, move |workspace_cwd| {
        workspace_git_commit_body(workspace_cwd, &sha)
    })
    .await
}

async fn workspace_git_log_lookup<F>(
    context: &ServeContext,
    head_only: bool,
    read: F,
) -> Response<Full<Bytes>>
where
    F: FnOnce(&Path) -> Value + Send + 'static,
{
    let Ok(permit) = Arc::clone(&context.workspace_git_log_lookup_slots).try_acquire_owned() else {
        return workspace_git_log_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace Git log lookup is busy",
                "code": "git_log_busy"
            }),
            head_only,
        );
    };

    let settings_paths = context.workspace_memory_settings_paths.clone();
    let workspace_cwd = PathBuf::from(&context.workspace_cwd);
    let lookup = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if !is_workspace_memory_trusted(&settings_paths) {
            return Err(WorkspaceGitLogReadError::Untrusted);
        }
        Ok(read(&workspace_cwd))
    });

    match timeout(WORKSPACE_GIT_LOG_LOOKUP_TIMEOUT, lookup).await {
        Ok(Ok(Ok(body))) => workspace_git_log_json_response(StatusCode::OK, body, head_only),
        Ok(Ok(Err(WorkspaceGitLogReadError::Untrusted))) => workspace_git_log_json_response(
            StatusCode::FORBIDDEN,
            json!({
                "error": "Workspace is not trusted.",
                "code": "untrusted_workspace"
            }),
            head_only,
        ),
        Ok(Err(error)) => {
            eprintln!("Workspace Git log task failed: {error}");
            workspace_git_log_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": "Failed to read workspace Git log",
                    "code": "git_log_failed"
                }),
                head_only,
            )
        }
        Err(_) => workspace_git_log_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Workspace Git log lookup timed out",
                "code": "git_log_timeout"
            }),
            head_only,
        ),
    }
}

fn parse_workspace_git_log_query(query: Option<&str>) -> Result<WorkspaceGitLogQuery, ()> {
    let raw_limit = query_parameter(query, "limit").ok().flatten();
    let limit = parse_integer_prefix(raw_limit.as_deref())
        .unwrap_or(DEFAULT_WORKSPACE_GIT_LOG_LIMIT as i128)
        .clamp(1, MAX_WORKSPACE_GIT_LOG_LIMIT as i128) as usize;

    let raw_skip = query_parameter(query, "skip").ok().flatten();
    let skip = parse_integer_prefix(raw_skip.as_deref())
        .unwrap_or(0)
        .max(0);
    let skip = u64::try_from(skip).unwrap_or(u64::MAX);

    let range = query_parameter(query, "range")
        .map_err(|()| ())?
        .and_then(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_owned())
        });
    if range
        .as_deref()
        .is_some_and(|value| !valid_workspace_git_range(value))
    {
        return Err(());
    }

    Ok(WorkspaceGitLogQuery { limit, skip, range })
}

fn parse_integer_prefix(value: Option<&str>) -> Option<i128> {
    let value = value?.trim_start();
    let (negative, digits) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    let digit_count = digits.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        return None;
    }
    let mut parsed = 0_i128;
    for byte in digits.as_bytes().iter().take(digit_count) {
        parsed = parsed
            .saturating_mul(10)
            .saturating_add(i128::from(*byte - b'0'));
    }
    Some(if negative {
        parsed.saturating_neg()
    } else {
        parsed
    })
}

fn valid_workspace_git_range(range: &str) -> bool {
    !range.is_empty()
        && range.len() <= MAX_WORKSPACE_GIT_LOG_RANGE_BYTES
        && !range.starts_with('-')
        && !range.starts_with("..")
        && range.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'/' | b'~' | b'^' | b'-')
        })
}

fn valid_workspace_git_sha(sha: &str) -> bool {
    (7..=40).contains(&sha.len()) && sha.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn workspace_git_log_list_body(workspace_cwd: &Path, query: &WorkspaceGitLogQuery) -> Value {
    let Some(git_root) = find_git_root(workspace_cwd) else {
        return json!({
            "v": 1,
            "workspaceCwd": workspace_cwd.to_string_lossy(),
            "available": false,
            "entries": [],
            "hasMore": false
        });
    };

    let format = "--format=%H%x00%h%x00%an%x00%ae%x00%at%x00%s%x00%D%x00%P";
    let mut args = vec![
        "-c".to_owned(),
        "core.quotepath=false".to_owned(),
        "--no-optional-locks".to_owned(),
        "log".to_owned(),
        "-z".to_owned(),
        format.to_owned(),
        "-n".to_owned(),
        (query.limit + 1).to_string(),
    ];
    if query.skip > 0 {
        args.push("--skip".to_owned());
        args.push(query.skip.to_string());
    }
    if let Some(range) = &query.range {
        args.push(range.clone());
        args.push("--".to_owned());
    }

    let Some(output) = run_bounded_git_log_command(&git_root, &args, MAX_WORKSPACE_GIT_LOG_BYTES)
    else {
        let head_resolves = run_bounded_git_log_command(
            &git_root,
            &[
                "--no-optional-locks".to_owned(),
                "rev-parse".to_owned(),
                "--verify".to_owned(),
                "HEAD".to_owned(),
            ],
            1024,
        )
        .is_some();
        return if head_resolves {
            json!({
                "v": 1,
                "workspaceCwd": workspace_cwd.to_string_lossy(),
                "available": false,
                "entries": [],
                "hasMore": false
            })
        } else {
            json!({
                "v": 1,
                "workspaceCwd": workspace_cwd.to_string_lossy(),
                "available": true,
                "entries": [],
                "hasMore": false
            })
        };
    };

    let mut fields = nul_fields(&output);
    if fields.last().is_some_and(String::is_empty) {
        fields.pop();
    }
    let record_count = fields.len() / 8;
    let has_more = record_count > query.limit;
    let returned_count = record_count.min(query.limit);
    let mut entries = Vec::with_capacity(returned_count);
    for index in 0..returned_count {
        let offset = index * 8;
        let parts = &fields[offset..offset + 8];
        let parents = split_git_parents(&parts[7]);
        let mut entry = json!({
            "sha": parts[0],
            "shortSha": parts[1],
            "authorName": parts[2],
            "authorEmail": parts[3],
            "authorDate": parse_git_timestamp(&parts[4]),
            "subject": parts[5],
            "parents": parents
        });
        if !parts[6].is_empty() {
            entry["refs"] = Value::String(parts[6].clone());
        }
        entries.push(entry);
    }

    json!({
        "v": 1,
        "workspaceCwd": workspace_cwd.to_string_lossy(),
        "available": true,
        "entries": entries,
        "hasMore": has_more
    })
}

fn workspace_git_commit_body(workspace_cwd: &Path, requested_sha: &str) -> Value {
    let unavailable = || {
        json!({
            "v": 1,
            "workspaceCwd": workspace_cwd.to_string_lossy(),
            "available": false
        })
    };
    let Some(git_root) = find_git_root(workspace_cwd) else {
        return unavailable();
    };

    let metadata_args = vec![
        "-c".to_owned(),
        "core.quotepath=false".to_owned(),
        "--no-optional-locks".to_owned(),
        "log".to_owned(),
        "-1".to_owned(),
        "-z".to_owned(),
        "--format=%H%x00%h%x00%an%x00%ae%x00%at%x00%s%x00%D%x00%P%x00%b".to_owned(),
        requested_sha.to_owned(),
    ];
    let Some(metadata) =
        run_bounded_git_log_command(&git_root, &metadata_args, MAX_WORKSPACE_GIT_LOG_BYTES)
    else {
        return unavailable();
    };
    let mut parts = nul_fields(&metadata);
    if parts.last().is_some_and(String::is_empty) {
        parts.pop();
    }
    if parts.len() != 9 {
        return unavailable();
    }

    let parents = split_git_parents(&parts[7]);
    let mut diff_args = vec![
        "-c".to_owned(),
        "core.quotepath=false".to_owned(),
        "--no-optional-locks".to_owned(),
        "diff-tree".to_owned(),
        "--no-commit-id".to_owned(),
        "--numstat".to_owned(),
        "-M".to_owned(),
        "-r".to_owned(),
        "-z".to_owned(),
    ];
    if parents.len() > 1 {
        diff_args.push(format!("{requested_sha}^1"));
        diff_args.push(requested_sha.to_owned());
    } else {
        diff_args.push("--root".to_owned());
        diff_args.push(requested_sha.to_owned());
    }

    let (files, files_count, lines_added, lines_removed) =
        run_bounded_git_log_command(&git_root, &diff_args, MAX_WORKSPACE_GIT_NUMSTAT_BYTES)
            .map(|output| parse_git_numstat(&output))
            .unwrap_or_default();

    let body = parts[8].strip_suffix('\n').unwrap_or(&parts[8]);
    let hidden_count = files_count.saturating_sub(files.len() as u64);
    let mut result = json!({
        "v": 1,
        "workspaceCwd": workspace_cwd.to_string_lossy(),
        "available": true,
        "sha": parts[0],
        "shortSha": parts[1],
        "authorName": parts[2],
        "authorEmail": parts[3],
        "authorDate": parse_git_timestamp(&parts[4]),
        "subject": parts[5],
        "body": body,
        "parents": parents,
        "files": files,
        "filesCount": files_count,
        "linesAdded": lines_added,
        "linesRemoved": lines_removed,
        "hiddenCount": hidden_count
    });
    if !parts[6].is_empty() {
        result["refs"] = Value::String(parts[6].clone());
    }
    result
}

fn nul_fields(output: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(output)
        .split('\0')
        .map(str::to_owned)
        .collect()
}

fn split_git_parents(parents: &str) -> Vec<String> {
    parents
        .split(' ')
        .filter(|parent| !parent.is_empty())
        .map(str::to_owned)
        .collect()
}

fn parse_git_timestamp(value: &str) -> i64 {
    parse_integer_prefix(Some(value))
        .and_then(|value| i64::try_from(value).ok())
        .unwrap_or(0)
}

fn parse_git_numstat(output: &[u8]) -> (Vec<Value>, u64, u64, u64) {
    #[derive(Clone)]
    struct PendingRename {
        added: u64,
        removed: u64,
        is_binary: bool,
    }

    let text = String::from_utf8_lossy(output);
    let mut tokens = text.split('\0').collect::<Vec<_>>();
    if tokens.last() == Some(&"") {
        tokens.pop();
    }
    let mut pending: Option<PendingRename> = None;
    let mut rename_old_seen = false;
    let mut files = Vec::with_capacity(MAX_WORKSPACE_GIT_LOG_FILES);
    let mut files_count = 0_u64;
    let mut lines_added = 0_u64;
    let mut lines_removed = 0_u64;

    for token in tokens {
        if pending.is_some() {
            if !rename_old_seen {
                rename_old_seen = true;
            } else {
                let stats = pending.take().unwrap();
                rename_old_seen = false;
                commit_numstat_entry(
                    token,
                    stats.added,
                    stats.removed,
                    stats.is_binary,
                    &mut files,
                    &mut files_count,
                    &mut lines_added,
                    &mut lines_removed,
                );
            }
            continue;
        }

        let Some(first_tab) = token.find('\t') else {
            continue;
        };
        let Some(second_tab_relative) = token[first_tab + 1..].find('\t') else {
            continue;
        };
        let second_tab = first_tab + 1 + second_tab_relative;
        let added_text = &token[..first_tab];
        let removed_text = &token[first_tab + 1..second_tab];
        let path = &token[second_tab + 1..];
        let is_binary = added_text == "-" || removed_text == "-";
        let added = if is_binary {
            0
        } else {
            parse_integer_prefix(Some(added_text))
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or(0)
        };
        let removed = if is_binary {
            0
        } else {
            parse_integer_prefix(Some(removed_text))
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or(0)
        };
        if path.is_empty() {
            pending = Some(PendingRename {
                added,
                removed,
                is_binary,
            });
        } else {
            commit_numstat_entry(
                path,
                added,
                removed,
                is_binary,
                &mut files,
                &mut files_count,
                &mut lines_added,
                &mut lines_removed,
            );
        }
    }

    (files, files_count, lines_added, lines_removed)
}

fn commit_numstat_entry(
    path: &str,
    added: u64,
    removed: u64,
    is_binary: bool,
    files: &mut Vec<Value>,
    files_count: &mut u64,
    lines_added: &mut u64,
    lines_removed: &mut u64,
) {
    *files_count = (*files_count).saturating_add(1);
    *lines_added = (*lines_added).saturating_add(added);
    *lines_removed = (*lines_removed).saturating_add(removed);
    if files.len() < MAX_WORKSPACE_GIT_LOG_FILES {
        files.push(json!({
            "path": path,
            "added": added,
            "removed": removed,
            "isBinary": is_binary
        }));
    }
}

fn run_bounded_git_log_command(
    git_root: &Path,
    args: &[String],
    output_limit: usize,
) -> Option<Vec<u8>> {
    let mut child = Command::new("git")
        .args(args)
        .current_dir(git_root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return None;
    };
    let abort_reader = Arc::new(AtomicBool::new(false));
    let reader_abort = Arc::clone(&abort_reader);
    let reader = match thread::Builder::new()
        .name("canopy-serve-git-reader".to_owned())
        .spawn(move || {
            let mut stdout = stdout;
            let mut output = Vec::with_capacity(output_limit.min(64 * 1024));
            let mut buffer = [0_u8; 16 * 1024];
            loop {
                let read = match stdout.read(&mut buffer) {
                    Ok(read) => read,
                    Err(_) => {
                        reader_abort.store(true, Ordering::Release);
                        return None;
                    }
                };
                if read == 0 {
                    return Some(output);
                }
                if output.len().saturating_add(read) > output_limit {
                    reader_abort.store(true, Ordering::Release);
                    return None;
                }
                output.extend_from_slice(&buffer[..read]);
            }
        }) {
        Ok(reader) => reader,
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
    };

    let deadline = Instant::now() + WORKSPACE_GIT_LOG_PROCESS_TIMEOUT;
    let exit_status = loop {
        if abort_reader.load(Ordering::Acquire) {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
        }
    };
    let output = reader.join().ok().flatten()?;
    exit_status
        .filter(|status| status.success())
        .map(|_| output)
}

fn workspace_git_log_json_response(
    status: StatusCode,
    body: Value,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let mut serialized = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
    let status = if serialized.len() > MAX_WORKSPACE_GIT_LOG_RESPONSE_BYTES {
        serialized = serde_json::to_vec(&json!({
            "error": "Workspace Git log response is too large",
            "code": "git_log_response_too_large"
        }))
        .unwrap_or_else(|_| b"{}".to_vec());
        StatusCode::PAYLOAD_TOO_LARGE
    } else {
        status
    };
    let mut response = json_bytes_response(status, serialized, head_only, false);
    response.headers_mut().insert(
        CACHE_CONTROL,
        hyper::header::HeaderValue::from_static("no-store"),
    );
    response.headers_mut().insert(
        hyper::header::HeaderName::from_static("x-content-type-options"),
        hyper::header::HeaderValue::from_static("nosniff"),
    );
    response
}

fn workspace_git_wait_requested(query: Option<&str>) -> bool {
    let Some(query) = query else {
        return false;
    };
    let mut wait = None;
    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        if key == "wait" {
            if wait.is_some() {
                return false;
            }
            wait = Some(value.into_owned());
        }
    }
    wait.as_deref() == Some("1")
}

fn workspace_git_status_body(workspace_cwd: &Path, wait_for_fresh: bool) -> Value {
    let Some(git_root) = find_git_root(workspace_cwd) else {
        return workspace_git_branch_only(workspace_cwd);
    };
    let process_timeout = if wait_for_fresh {
        WORKSPACE_GIT_PROCESS_TIMEOUT
    } else {
        Duration::from_secs(2)
    };
    let Some(output) = run_bounded_git_status(&git_root, process_timeout) else {
        return workspace_git_branch_only(workspace_cwd);
    };
    let Some(summary) = parse_git_status_summary(&output) else {
        return workspace_git_branch_only(workspace_cwd);
    };

    let git_directory = resolve_git_directory(&git_root);
    let stash_count = git_directory
        .as_deref()
        .map(count_git_stash_entries)
        .unwrap_or(0);
    let operation = git_directory.as_deref().and_then(detect_git_operation);
    let mut body = json!({
        "v": 2,
        "workspaceCwd": workspace_cwd.to_string_lossy(),
        "branch": summary.branch,
        "detached": summary.detached,
        "staged": summary.staged,
        "unstaged": summary.unstaged,
        "untracked": summary.untracked,
        "conflicted": summary.conflicted,
        "hasUpstream": summary.has_upstream,
        "ahead": summary.ahead,
        "behind": summary.behind,
        "stashCount": stash_count,
        "computedAt": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0)
    });
    if let Some(operation) = operation {
        body["operation"] = Value::String(operation.to_owned());
    }
    body
}

fn workspace_git_branch_only(workspace_cwd: &Path) -> Value {
    json!({
        "v": 2,
        "workspaceCwd": workspace_cwd.to_string_lossy(),
        "branch": null
    })
}

#[derive(Default)]
struct GitStatusSummary {
    branch: Option<String>,
    detached: bool,
    has_upstream: bool,
    ahead: u64,
    behind: u64,
    staged: u64,
    unstaged: u64,
    untracked: u64,
    conflicted: u64,
}

fn parse_git_status_summary(output: &[u8]) -> Option<GitStatusSummary> {
    let mut tokens = output.split(|byte| *byte == 0);
    let branch_line = String::from_utf8_lossy(tokens.next()?);
    let mut summary = parse_git_status_branch_line(&branch_line)?;
    let mut tokens = tokens.peekable();
    while let Some(token) = tokens.next() {
        if token.is_empty() || token.len() < 3 || token[2] != b' ' {
            continue;
        }
        let x = token[0];
        let y = token[1];
        if x == b'?' && y == b'?' {
            summary.untracked += 1;
            continue;
        }
        if x == b'!' && y == b'!' {
            continue;
        }
        if x == b'U' || y == b'U' || (x == b'D' && y == b'D') || (x == b'A' && y == b'A') {
            summary.conflicted += 1;
        } else {
            if x != b' ' && x != b'?' {
                summary.staged += 1;
            }
            if y != b' ' && y != b'?' {
                summary.unstaged += 1;
            }
        }
        if matches!(x, b'R' | b'C') || matches!(y, b'R' | b'C') {
            let _ = tokens.next();
        }
    }
    Some(summary)
}

fn parse_git_status_branch_line(line: &str) -> Option<GitStatusSummary> {
    let mut summary = GitStatusSummary::default();
    let Some(mut description) = line.strip_prefix("## ") else {
        return None;
    };
    if description.starts_with("HEAD (no branch)") {
        summary.detached = true;
        return Some(summary);
    }
    for prefix in ["No commits yet on ", "Initial commit on "] {
        if let Some(branch) = description.strip_prefix(prefix) {
            summary.branch = nonempty_git_string(branch);
            return Some(summary);
        }
    }

    if let Some(bracket_start) = description.rfind(" [")
        && description.ends_with(']')
    {
        let counts = &description[bracket_start + 2..description.len() - 1];
        for part in counts.split(',') {
            let mut words = part.split_whitespace();
            let (Some(kind), Some(value)) = (words.next(), words.next()) else {
                continue;
            };
            let Ok(count) = value.parse::<u64>() else {
                continue;
            };
            match kind {
                "ahead" => summary.ahead = count,
                "behind" => summary.behind = count,
                _ => {}
            }
        }
        description = description[..bracket_start].trim_end();
    }
    summary.has_upstream = description.contains("...");
    let branch = description
        .rfind("...")
        .map(|separator| &description[..separator])
        .unwrap_or(description);
    summary.branch = nonempty_git_string(branch);
    Some(summary)
}

fn nonempty_git_string(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}

fn find_git_root(workspace_cwd: &Path) -> Option<PathBuf> {
    let mut current = workspace_cwd.to_path_buf();
    for _ in 0..1024 {
        if fs::metadata(current.join(".git")).is_ok() {
            return Some(current);
        }
        let parent = current.parent()?;
        if parent == current {
            return None;
        }
        current = parent.to_path_buf();
    }
    None
}

fn run_bounded_git_status(git_root: &Path, process_timeout: Duration) -> Option<Vec<u8>> {
    let mut child = Command::new("git")
        .arg("-c")
        .arg("core.quotepath=false")
        .arg("--no-optional-locks")
        .arg("status")
        .arg("--porcelain=v1")
        .arg("--branch")
        .arg("-z")
        .current_dir(git_root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let stdout = child.stdout.take()?;
    let abort_reader = Arc::new(AtomicBool::new(false));
    let reader_abort = Arc::clone(&abort_reader);
    let reader = match thread::Builder::new()
        .name("canopy-serve-git-status".to_owned())
        .spawn(move || {
            let mut stdout = stdout;
            let mut output = Vec::with_capacity(MAX_WORKSPACE_GIT_STATUS_BYTES.min(64 * 1024));
            let mut buffer = [0_u8; 16 * 1024];
            loop {
                let read = match stdout.read(&mut buffer) {
                    Ok(read) => read,
                    Err(_) => {
                        reader_abort.store(true, Ordering::Release);
                        return None;
                    }
                };
                if read == 0 {
                    return Some(output);
                }
                if output.len().saturating_add(read) > MAX_WORKSPACE_GIT_STATUS_BYTES {
                    reader_abort.store(true, Ordering::Release);
                    return None;
                }
                output.extend_from_slice(&buffer[..read]);
            }
        }) {
        Ok(reader) => reader,
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
    };

    let deadline = Instant::now() + process_timeout;
    let exit_status = loop {
        if abort_reader.load(Ordering::Acquire) {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
        }
    };
    let output = reader.join().ok().flatten()?;
    exit_status
        .filter(|status| status.success())
        .map(|_| output)
}

fn resolve_git_directory(git_root: &Path) -> Option<PathBuf> {
    let dot_git = git_root.join(".git");
    let metadata = fs::metadata(&dot_git).ok()?;
    if metadata.is_dir() {
        return Some(dot_git);
    }
    if !metadata.is_file() {
        return None;
    }
    let contents = read_git_metadata_file(&dot_git, MAX_WORKSPACE_GIT_METADATA_BYTES)?;
    let contents = String::from_utf8_lossy(&contents);
    let raw_path = contents.lines().find_map(|line| {
        line.trim()
            .strip_prefix("gitdir:")
            .map(str::trim)
            .filter(|value| !value.is_empty())
    })?;
    let path = Path::new(raw_path);
    Some(if path.is_absolute() {
        path.to_path_buf()
    } else {
        normalize_workspace_git_path(path, git_root)
    })
}

fn normalize_workspace_git_path(path: &Path, base: &Path) -> PathBuf {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push(component.as_os_str());
                }
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn resolve_common_git_directory(git_directory: &Path) -> PathBuf {
    let Some(contents) = read_git_metadata_file(
        &git_directory.join("commondir"),
        MAX_WORKSPACE_GIT_METADATA_BYTES,
    ) else {
        return git_directory.to_path_buf();
    };
    let contents = String::from_utf8_lossy(&contents);
    let raw_path = contents.lines().next().unwrap_or_default().trim();
    if raw_path.is_empty() {
        return git_directory.to_path_buf();
    }
    let path = Path::new(raw_path);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        normalize_workspace_git_path(path, git_directory)
    }
}

fn read_git_metadata_file(path: &Path, max_bytes: u64) -> Option<Vec<u8>> {
    let link_metadata = fs::symlink_metadata(path).ok()?;
    if link_metadata.file_type().is_symlink()
        || !link_metadata.is_file()
        || link_metadata.len() > max_bytes
    {
        return None;
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
    }
    let mut file = options.open(path).ok()?;
    if !file.metadata().ok()?.is_file() {
        return None;
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .ok()?;
    (bytes.len() as u64 <= max_bytes).then_some(bytes)
}

fn count_git_stash_entries(git_directory: &Path) -> u64 {
    let common_directory = resolve_common_git_directory(git_directory);
    let stash_log = common_directory.join("logs/refs/stash");
    let Some(contents) = read_git_metadata_file(&stash_log, MAX_WORKSPACE_GIT_STASH_LOG_BYTES)
    else {
        return 0;
    };
    contents
        .split(|byte| *byte == b'\n')
        .filter(|line| line.iter().any(|byte| !byte.is_ascii_whitespace()))
        .count() as u64
}

fn detect_git_operation(git_directory: &Path) -> Option<&'static str> {
    [
        ("rebase-merge", "rebase"),
        ("rebase-apply", "rebase"),
        ("MERGE_HEAD", "merge"),
        ("CHERRY_PICK_HEAD", "cherry-pick"),
        ("REVERT_HEAD", "revert"),
        ("BISECT_LOG", "bisect"),
    ]
    .into_iter()
    .find_map(|(name, operation)| {
        fs::metadata(git_directory.join(name))
            .is_ok()
            .then_some(operation)
    })
}

async fn session_status_response(
    context: &ServeContext,
    requested_id: &str,
    head_only: bool,
) -> Response<Full<Bytes>> {
    if !is_valid_session_id(requested_id) {
        return session_status_json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "error": "Invalid session ID",
                "code": "invalid_session_id",
                "sessionId": truncate_path_id_for_error(requested_id)
            }),
            head_only,
        );
    }

    let session_id = normalize_session_status_id(requested_id);
    let Ok(permit) = Arc::clone(&context.session_lookup_slots).try_acquire_owned() else {
        return session_status_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Session status lookup is busy",
                "code": "session_status_busy"
            }),
            head_only,
        );
    };
    let catalog = context.session_catalog.clone();
    let storage = context.storage.clone();
    let lookup_id = session_id.clone();
    let lookup =
        tokio::task::spawn_blocking(move || -> std::io::Result<Option<(StatusCode, Value)>> {
            let _permit = permit;
            let Some(runtime_started_at) = read_live_session_status(&storage, &lookup_id) else {
                return Ok(None);
            };
            let Some(item) = catalog.get_session(&lookup_id, SessionArchiveState::Active)? else {
                return Ok(None);
            };
            let Some(body) = session_summary_body(&item, runtime_started_at) else {
                return Ok(Some((
                    StatusCode::UNPROCESSABLE_ENTITY,
                    json!({
                        "error": "Session metadata exceeds the response limits",
                        "code": "session_status_metadata_too_large"
                    }),
                )));
            };
            Ok(Some((StatusCode::OK, body)))
        })
        .await;

    match lookup {
        Ok(Ok(Some((status, body)))) => session_status_json_response(status, body, head_only),
        Ok(Ok(None)) => session_status_json_response(
            StatusCode::NOT_FOUND,
            json!({
                "error": format!("No session with id \"{session_id}\""),
                "code": "session_not_found",
                "sessionId": session_id
            }),
            head_only,
        ),
        Ok(Err(error)) => {
            eprintln!("Could not read session status catalog: {error}");
            session_status_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": "Could not read session status",
                    "code": "session_status_error"
                }),
                head_only,
            )
        }
        Err(error) => {
            eprintln!("Session status lookup task failed: {error}");
            session_status_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": "Could not read session status",
                    "code": "session_status_error"
                }),
                head_only,
            )
        }
    }
}

#[derive(Default)]
struct SessionTranscriptPageQuery {
    limit: Option<usize>,
    cursor: Option<String>,
    before_record_id: Option<String>,
}

#[derive(Clone, Copy)]
enum SessionTranscriptQueryError {
    InvalidLimitPositive,
    InvalidLimitRange,
    InvalidCursor,
    InvalidBeforeRecordId,
    CursorAndBeforeRecordId,
}

impl SessionTranscriptQueryError {
    fn response(self) -> Value {
        match self {
            Self::InvalidLimitPositive => json!({
                "error": "`limit` must be a positive integer",
                "code": "invalid_transcript_limit"
            }),
            Self::InvalidLimitRange => json!({
                "error": format!("`limit` must be between 1 and {SESSION_TRANSCRIPT_MAX_LIMIT}"),
                "code": "invalid_transcript_limit",
                "maxLimit": SESSION_TRANSCRIPT_MAX_LIMIT
            }),
            Self::InvalidCursor => json!({
                "error": "`cursor` must be a non-empty string",
                "code": "invalid_transcript_cursor"
            }),
            Self::InvalidBeforeRecordId => json!({
                "error": "`beforeRecordId` must be a non-empty record id",
                "code": "invalid_transcript_cursor"
            }),
            Self::CursorAndBeforeRecordId => json!({
                "error": "`cursor` and `beforeRecordId` are mutually exclusive",
                "code": "invalid_transcript_cursor"
            }),
        }
    }
}

fn query_parameter(query: Option<&str>, name: &str) -> Result<Option<String>, ()> {
    let mut found = None;
    if let Some(query) = query {
        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            if key == name {
                if found.is_some() {
                    return Err(());
                }
                found = Some(value.into_owned());
            }
        }
    }
    Ok(found)
}

fn parse_session_transcript_page_query(
    query: Option<&str>,
) -> Result<SessionTranscriptPageQuery, SessionTranscriptQueryError> {
    let limit = query_parameter(query, "limit")
        .map_err(|()| SessionTranscriptQueryError::InvalidLimitPositive)?;
    let limit = match limit {
        None => None,
        Some(value) if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) => {
            return Err(SessionTranscriptQueryError::InvalidLimitPositive);
        }
        Some(value) => {
            let parsed = value.parse::<u64>().unwrap_or(u64::MAX);
            if parsed == 0 || parsed > SESSION_TRANSCRIPT_MAX_LIMIT as u64 {
                return Err(SessionTranscriptQueryError::InvalidLimitRange);
            }
            Some(parsed as usize)
        }
    };

    let cursor = query_parameter(query, "cursor")
        .map_err(|()| SessionTranscriptQueryError::InvalidCursor)?;
    let cursor = match cursor {
        Some(value)
            if value.trim().is_empty() || value.len() > MAX_SESSION_TRANSCRIPT_CURSOR_BYTES =>
        {
            return Err(SessionTranscriptQueryError::InvalidCursor);
        }
        value => value,
    };

    let before_record_id = query_parameter(query, "beforeRecordId")
        .map_err(|()| SessionTranscriptQueryError::InvalidBeforeRecordId)?;
    let before_record_id = match before_record_id {
        Some(value) if value.trim().is_empty() || value.encode_utf16().count() > 200 => {
            return Err(SessionTranscriptQueryError::InvalidBeforeRecordId);
        }
        value => value,
    };
    if cursor.is_some() && before_record_id.is_some() {
        return Err(SessionTranscriptQueryError::CursorAndBeforeRecordId);
    }

    Ok(SessionTranscriptPageQuery {
        limit,
        cursor,
        before_record_id,
    })
}

async fn session_transcript_events_response(
    context: &ServeContext,
    requested_id: &str,
    query: Option<&str>,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let options = match parse_session_transcript_page_query(query) {
        Ok(options) => options,
        Err(error) => {
            return session_transcript_json_response(
                StatusCode::BAD_REQUEST,
                error.response(),
                head_only,
            );
        }
    };
    if !is_valid_session_id(requested_id) {
        return session_transcript_json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "error": "Invalid session ID",
                "code": "invalid_session_id",
                "sessionId": truncate_path_id_for_error(requested_id)
            }),
            head_only,
        );
    }

    let session_id = normalize_session_status_id(requested_id);
    let Ok(permit) = Arc::clone(&context.session_transcript_lookup_slots).try_acquire_owned()
    else {
        return session_transcript_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Session transcript lookup is busy",
                "code": "session_transcript_busy"
            }),
            head_only,
        );
    };
    let catalog = context.session_catalog.clone();
    let storage = context.storage.clone();
    let project_root = context.workspace_cwd.clone();
    let lookup_id = session_id.clone();
    let lookup = tokio::task::spawn_blocking(
        move || -> Result<Option<Value>, SessionTranscriptReaderError> {
            let _permit = permit;
            let Some(runtime_started_at) = read_live_session_status(&storage, &lookup_id) else {
                return Ok(None);
            };
            let Some(item) = catalog.get_session(&lookup_id, SessionArchiveState::Active)? else {
                return Ok(None);
            };

            let reader = SessionTranscriptReader::new(
                storage.runtime_base_dir().to_path_buf(),
                project_root,
            );
            let page = reader.read_page(
                &lookup_id,
                SessionTranscriptReadPageOptions {
                    cursor: options.cursor.as_deref(),
                    before_record_id: options.before_record_id.as_deref(),
                    limit: options.limit,
                    max_bytes: Some(SESSION_TRANSCRIPT_MAX_PAGE_BYTES),
                    ..SessionTranscriptReadPageOptions::default()
                },
            )?;
            if page.file_path != item.file_path
                || page.session_id != lookup_id
                || page.records.iter().any(|record| {
                    record.get("sessionId").and_then(Value::as_str) != Some(lookup_id.as_str())
                })
            {
                return Err(SessionTranscriptReaderError::SnapshotUnavailable(
                    lookup_id.clone(),
                ));
            }

            // The runtime-status sidecar proves process ownership but does not
            // report active-prompt state. Preserve unresolved calls instead of
            // projecting them as failed while a foreign live prompt may be active.
            let replay =
                replay_transcript_record_page(&reader, &page, false).map_err(
                    |error| match error {
                        TranscriptReplayPageError::Reader(error) => error,
                        TranscriptReplayPageError::UnsupportedReplayVersion => {
                            SessionTranscriptReaderError::InvalidCursor
                        }
                    },
                )?;

            let Some(runtime_still_owned_at) = read_live_session_status(&storage, &lookup_id)
            else {
                return Ok(None);
            };
            let Some(item_still_owned) =
                catalog.get_session(&lookup_id, SessionArchiveState::Active)?
            else {
                return Ok(None);
            };
            if runtime_still_owned_at != runtime_started_at
                || item_still_owned.file_path != item.file_path
                || item_still_owned.cwd != item.cwd
            {
                return Ok(None);
            }

            let mut body = json!({
                "v": replay.v,
                "sessionId": replay.session_id,
                "events": replay.events,
                "hasMore": replay.has_more,
                "startTime": replay.start_time,
                "lastUpdated": replay.last_updated
            });
            if let Some(cursor) = replay.next_cursor {
                body["nextCursor"] = Value::String(cursor);
            }
            if let Some(partial) = replay.partial {
                body["partial"] = json!(partial);
            }
            if let Some(replay_error) = replay.replay_error {
                body["replayError"] = Value::String(replay_error);
            }
            redact_skill_details(&mut body["events"]);
            let response_bytes = serde_json::to_vec(&body).unwrap_or_default().len();
            if response_bytes > MAX_SESSION_TRANSCRIPT_EVENTS_RESPONSE_BYTES {
                return Err(SessionTranscriptReaderError::PageTooLarge {
                    page_bytes: response_bytes,
                    max_bytes: MAX_SESSION_TRANSCRIPT_EVENTS_RESPONSE_BYTES,
                });
            }
            Ok(Some(body))
        },
    )
    .await;

    match lookup {
        Ok(Ok(Some(body))) => session_transcript_json_response(StatusCode::OK, body, head_only),
        Ok(Ok(None)) => session_transcript_json_response(
            StatusCode::NOT_FOUND,
            json!({
                "error": format!("No session with id \"{session_id}\""),
                "code": "session_not_found",
                "sessionId": session_id
            }),
            head_only,
        ),
        Ok(Err(error)) => {
            let (status, body) = session_transcript_reader_error_response(&session_id, error);
            session_transcript_json_response(status, body, head_only)
        }
        Err(error) => {
            eprintln!("Session transcript replay task failed: {error}");
            session_transcript_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": "Could not read session transcript",
                    "code": "session_transcript_error",
                    "sessionId": session_id
                }),
                head_only,
            )
        }
    }
}

async fn session_transcript_records_response(
    context: &ServeContext,
    requested_id: &str,
    query: Option<&str>,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let options = match parse_session_transcript_page_query(query) {
        Ok(options) => options,
        Err(error) => {
            return session_transcript_json_response(
                StatusCode::BAD_REQUEST,
                error.response(),
                head_only,
            );
        }
    };
    if !is_valid_session_id(requested_id) {
        return session_transcript_json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "error": "Invalid session ID",
                "code": "invalid_session_id",
                "sessionId": truncate_path_id_for_error(requested_id)
            }),
            head_only,
        );
    }

    let session_id = normalize_session_status_id(requested_id);
    let Ok(permit) = Arc::clone(&context.session_transcript_lookup_slots).try_acquire_owned()
    else {
        return session_transcript_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "Session transcript lookup is busy",
                "code": "session_transcript_busy"
            }),
            head_only,
        );
    };
    let catalog = context.session_catalog.clone();
    let storage = context.storage.clone();
    let project_root = context.workspace_cwd.clone();
    let lookup_id = session_id.clone();
    let lookup = tokio::task::spawn_blocking(move || -> Result<Option<Value>, SessionTranscriptReaderError> {
        let _permit = permit;
        let Some(runtime_started_at) = read_live_session_status(&storage, &lookup_id) else {
            return Ok(None);
        };
        let Some(item) = catalog.get_session(&lookup_id, SessionArchiveState::Active)? else {
            return Ok(None);
        };

        let reader = SessionTranscriptReader::new(
            storage.runtime_base_dir().to_path_buf(),
            project_root,
        );
        let page = reader.read_page(
            &lookup_id,
            SessionTranscriptReadPageOptions {
                cursor: options.cursor.as_deref(),
                before_record_id: options.before_record_id.as_deref(),
                limit: options.limit,
                max_bytes: Some(SESSION_TRANSCRIPT_MAX_PAGE_BYTES),
                ..SessionTranscriptReadPageOptions::default()
            },
        )?;
        if page.file_path != item.file_path
            || page.session_id != lookup_id
            || page
                .records
                .iter()
                .any(|record| {
                    record.get("sessionId").and_then(Value::as_str)
                        != Some(lookup_id.as_str())
                })
        {
            return Err(SessionTranscriptReaderError::SnapshotUnavailable(
                lookup_id.clone(),
            ));
        }

        let Some(runtime_still_owned_at) = read_live_session_status(&storage, &lookup_id) else {
            return Ok(None);
        };
        let Some(item_still_owned) = catalog.get_session(&lookup_id, SessionArchiveState::Active)?
        else {
            return Ok(None);
        };
        if runtime_still_owned_at != runtime_started_at
            || item_still_owned.file_path != item.file_path
            || item_still_owned.cwd != item.cwd
        {
            return Ok(None);
        }

        let mut records = page.records;
        for record in &mut records {
            redact_skill_details(record);
        }
        let mut body = json!({
            "v": 1,
            "sessionId": lookup_id,
            "records": records,
            "hasMore": page.has_more,
            "startTime": page.start_time,
            "lastUpdated": page.last_updated,
            "direction": match page.direction {
                canopy_core::services::session_transcript_reader::SessionTranscriptDirection::Forward => "forward",
                canopy_core::services::session_transcript_reader::SessionTranscriptDirection::Backward => "backward"
            }
        });
        if let Some(cursor) = page.next_cursor {
            body["nextCursor"] = Value::String(cursor);
        }
        if let Some(branch_points) = page.branch_points_by_assistant_uuid {
            body["branchPointsByAssistantUuid"] = json!(branch_points);
        }
        let response_bytes = serde_json::to_vec(&body).unwrap_or_default().len();
        if response_bytes > MAX_SESSION_TRANSCRIPT_RESPONSE_BYTES {
            return Err(SessionTranscriptReaderError::PageTooLarge {
                page_bytes: response_bytes,
                max_bytes: MAX_SESSION_TRANSCRIPT_RESPONSE_BYTES,
            });
        }
        Ok(Some(body))
    })
    .await;

    match lookup {
        Ok(Ok(Some(body))) => session_transcript_json_response(StatusCode::OK, body, head_only),
        Ok(Ok(None)) => session_transcript_json_response(
            StatusCode::NOT_FOUND,
            json!({
                "error": format!("No session with id \"{session_id}\""),
                "code": "session_not_found",
                "sessionId": session_id
            }),
            head_only,
        ),
        Ok(Err(error)) => {
            let (status, body) = session_transcript_reader_error_response(&session_id, error);
            session_transcript_json_response(status, body, head_only)
        }
        Err(error) => {
            eprintln!("Session transcript lookup task failed: {error}");
            session_transcript_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": "Could not read session transcript",
                    "code": "session_transcript_error",
                    "sessionId": session_id
                }),
                head_only,
            )
        }
    }
}

fn session_transcript_reader_error_response(
    session_id: &str,
    error: SessionTranscriptReaderError,
) -> (StatusCode, Value) {
    match error {
        SessionTranscriptReaderError::InvalidSessionId => (
            StatusCode::BAD_REQUEST,
            json!({
                "error": "Invalid session ID",
                "code": "invalid_session_id",
                "sessionId": session_id
            }),
        ),
        SessionTranscriptReaderError::SnapshotUnavailable(unavailable_id) => (
            StatusCode::CONFLICT,
            json!({
                "error": format!("Transcript snapshot is unavailable for session {unavailable_id}"),
                "code": "transcript_snapshot_unavailable",
                "sessionId": unavailable_id
            }),
        ),
        SessionTranscriptReaderError::TranscriptTooLarge {
            session_id,
            snapshot_size,
            max_bytes,
        } => (
            StatusCode::PAYLOAD_TOO_LARGE,
            json!({
                "error": format!("Transcript snapshot for session {session_id} is too large to index ({snapshot_size} bytes, max {max_bytes} bytes)"),
                "code": "transcript_too_large",
                "sessionId": session_id,
                "snapshotSize": snapshot_size,
                "maxBytes": max_bytes
            }),
        ),
        SessionTranscriptReaderError::PageTooLarge {
            page_bytes,
            max_bytes,
        } => (
            StatusCode::PAYLOAD_TOO_LARGE,
            json!({
                "error": format!("Transcript page for session {session_id} exceeds the page budget ({page_bytes} bytes, max {max_bytes} bytes)"),
                "code": "transcript_page_too_large",
                "sessionId": session_id,
                "pageBytes": page_bytes,
                "maxBytes": max_bytes
            }),
        ),
        SessionTranscriptReaderError::InvalidCursor => (
            StatusCode::BAD_REQUEST,
            json!({
                "error": "Invalid transcript cursor",
                "code": "invalid_transcript_cursor",
                "sessionId": session_id
            }),
        ),
        SessionTranscriptReaderError::InvalidLimit => (
            StatusCode::BAD_REQUEST,
            json!({
                "error": format!("`limit` must be between 1 and {SESSION_TRANSCRIPT_MAX_LIMIT}"),
                "code": "invalid_transcript_limit",
                "maxLimit": SESSION_TRANSCRIPT_MAX_LIMIT
            }),
        ),
        SessionTranscriptReaderError::InvalidPageByteLimit => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "error": "Could not read session transcript",
                "code": "session_transcript_error",
                "sessionId": session_id
            }),
        ),
        SessionTranscriptReaderError::InvalidCompressedHistory => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "error": "Could not read session transcript",
                "code": "session_transcript_error",
                "sessionId": session_id
            }),
        ),
        SessionTranscriptReaderError::Io(error) => {
            eprintln!("Could not read session transcript {session_id}: {error}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": "Could not read session transcript",
                    "code": "session_transcript_error",
                    "sessionId": session_id
                }),
            )
        }
    }
}

fn redact_skill_details(value: &mut Value) {
    match value {
        Value::Object(object) => {
            let is_commands_update = object.get("sessionUpdate").and_then(Value::as_str)
                == Some("available_commands_update");
            if is_commands_update {
                let remove_meta = object
                    .get_mut("_meta")
                    .and_then(Value::as_object_mut)
                    .is_some_and(|meta| {
                        meta.remove("availableSkillDetails");
                        meta.is_empty()
                    });
                if remove_meta {
                    object.remove("_meta");
                }
            }
            for child in object.values_mut() {
                redact_skill_details(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_skill_details(item);
            }
        }
        _ => {}
    }
}

fn session_transcript_json_response(
    status: StatusCode,
    value: Value,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let mut response = json_value_response(status, value, head_only, false);
    response.headers_mut().insert(
        CACHE_CONTROL,
        hyper::header::HeaderValue::from_static("no-store"),
    );
    response
}

fn normalize_session_status_id(session_id: &str) -> String {
    let bytes = session_id.as_bytes();
    let canonical_uuid = bytes.len() == 36
        && [8, 13, 18, 23].iter().all(|index| bytes[*index] == b'-')
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
        && matches!(bytes[14].to_ascii_lowercase(), b'1'..=b'5')
        && matches!(bytes[19].to_ascii_lowercase(), b'8' | b'9' | b'a' | b'b');
    if canonical_uuid {
        session_id.to_ascii_lowercase()
    } else {
        session_id.to_owned()
    }
}

fn truncate_path_id_for_error(session_id: &str) -> String {
    let mut truncated = String::new();
    for character in session_id.chars().take(128) {
        truncated.push(character);
    }
    if truncated.len() < session_id.len() {
        truncated.push_str("…");
    }
    truncated
}

fn read_live_session_status(storage: &Storage, session_id: &str) -> Option<f64> {
    let path = storage.get_runtime_status_path(session_id);
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK);
    }
    let file = options.open(path).ok()?;
    if !file.metadata().ok()?.is_file()
        || file.metadata().ok()?.len() > MAX_SESSION_STATUS_SIDECAR_BYTES
    {
        return None;
    }
    let mut bytes = Vec::with_capacity(MAX_SESSION_STATUS_SIDECAR_BYTES as usize);
    file.take(MAX_SESSION_STATUS_SIDECAR_BYTES + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > MAX_SESSION_STATUS_SIDECAR_BYTES {
        return None;
    }
    let status: Value = serde_json::from_slice(&bytes).ok()?;
    let object = status.as_object()?;
    let schema_version = object.get("schema_version")?.as_f64()?;
    let pid = object.get("pid")?.as_f64()?;
    let stored_id = object.get("session_id")?.as_str()?;
    let work_dir = object.get("work_dir")?.as_str()?;
    let stored_hostname = object.get("hostname")?.as_str()?;
    let started_at = object.get("started_at")?.as_f64()?;
    let canopy_version = object.get("canopy_version")?;
    let expected_hostname = local_hostname()?;
    if schema_version != 1.0
        || schema_version.fract() != 0.0
        || stored_id != session_id
        || work_dir.len() > MAX_SESSION_STATUS_CWD_BYTES
        || work_dir.contains('\0')
        || stored_hostname != expected_hostname
        || !started_at.is_finite()
        || started_at < 0.0
        || !matches!(canopy_version, Value::Null | Value::String(_))
        || !pid.is_finite()
        || pid.fract() != 0.0
        || pid <= 0.0
        || pid > f64::from(i32::MAX)
        || !is_pid_alive(pid as i64)
    {
        return None;
    }
    Some(started_at)
}

fn local_hostname() -> Option<String> {
    #[cfg(unix)]
    {
        return nix::unistd::gethostname()
            .ok()
            .map(|hostname| hostname.to_string_lossy().into_owned());
    }
    #[cfg(windows)]
    {
        return std::env::var("COMPUTERNAME").ok();
    }
    #[allow(unreachable_code)]
    None
}

fn session_summary_body(
    item: &canopy_core::session_catalog::SessionListItem,
    runtime_started_at: f64,
) -> Option<Value> {
    if !bounded_session_status_field(&item.cwd, MAX_SESSION_STATUS_CWD_BYTES)
        || item.custom_title.as_deref().is_some_and(|value| {
            !bounded_session_status_field(value, MAX_SESSION_STATUS_METADATA_BYTES)
        })
        || item.parent_session_id.as_deref().is_some_and(|value| {
            !bounded_session_status_field(value, MAX_SESSION_STATUS_METADATA_BYTES)
        })
        || item.source_type.as_deref().is_some_and(|value| {
            !bounded_session_status_field(value, MAX_SESSION_STATUS_METADATA_BYTES)
        })
        || item.source_id.as_deref().is_some_and(|value| {
            !bounded_session_status_field(value, MAX_SESSION_STATUS_METADATA_BYTES)
        })
    {
        return None;
    }
    let milliseconds = (runtime_started_at * 1_000.0).round() as i64;
    let created_at = Utc
        .timestamp_millis_opt(milliseconds)
        .single()
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
        .unwrap_or_else(timestamp);
    let mut body = json!({
        "sessionId": item.session_id,
        "workspaceCwd": item.cwd,
        "createdAt": created_at,
        "clientCount": 0,
        "hasActivePrompt": false,
        "isWaitingForPermission": false,
        "isWaitingForUserQuestion": false,
        "pendingInteractionCount": 0,
        "hasTurnError": false,
        "pendingInteractions": []
    });
    if let Some(value) = item.custom_title.as_deref() {
        body["displayName"] = json!(value);
    }
    if let Some(value) = item.parent_session_id.as_deref() {
        body["parentSessionId"] = json!(value);
    }
    if let Some(value) = item.source_type.as_deref() {
        body["sourceType"] = json!(value);
    }
    if let Some(value) = item.source_id.as_deref() {
        body["sourceId"] = json!(value);
    }
    Some(body)
}

fn bounded_session_status_field(value: &str, max_bytes: usize) -> bool {
    value.len() <= max_bytes && !value.contains('\0')
}

fn session_status_json_response(
    status: StatusCode,
    value: Value,
    head_only: bool,
) -> Response<Full<Bytes>> {
    let body = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
    if body.len() > MAX_SESSION_STATUS_RESPONSE_BYTES {
        return json_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            r#"{"error":"Session metadata exceeds the response limits","code":"session_status_metadata_too_large"}"#,
            head_only,
            false,
            None,
        );
    }
    json_bytes_response(status, body, head_only, false)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundedRequestBodyError {
    TooLarge,
    Invalid,
}

async fn consume_bounded_body(
    mut body: Incoming,
    capture: bool,
) -> Result<Option<Bytes>, BoundedRequestBodyError> {
    let mut bytes_read = 0_usize;
    let mut captured = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| BoundedRequestBodyError::Invalid)?;
        if let Some(data) = frame.data_ref() {
            bytes_read = bytes_read.saturating_add(data.len());
            if bytes_read > MAX_REQUEST_BODY_BYTES {
                return Err(BoundedRequestBodyError::TooLarge);
            }
            if capture {
                captured.extend_from_slice(data);
            }
        }
    }
    Ok(capture.then(|| Bytes::from(captured)))
}

fn matches_route_path(path: &str, expected: &str) -> bool {
    let path = path.strip_suffix('/').unwrap_or(path);
    path.eq_ignore_ascii_case(expected)
}

fn parse_daemon_status_detail(query: Option<&str>) -> Option<&'static str> {
    let mut detail = None;
    if let Some(query) = query {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if key == "detail" {
                if detail.is_some() {
                    return None;
                }
                detail = Some(value.into_owned());
            }
        }
    }
    match detail.as_deref() {
        None => Some("summary"),
        Some("summary") => Some("summary"),
        Some("full") => Some("full"),
        Some(_) => None,
    }
}

fn capabilities_body(context: &ServeContext) -> Value {
    json!({
        "v": 1,
        "protocolVersions": { "current": "v1", "supported": ["v1"] },
        "canopyCodeVersion": env!("CARGO_PKG_VERSION"),
        "mode": "native",
        "features": SERVE_FEATURES,
        "modelServices": [],
        "workspaceCwd": context.workspace_cwd.as_str(),
        "transports": ["rest"],
        "policy": { "permission": "first-responder" },
        "limits": {
            "maxPendingPromptsPerSession": 5,
            "sessionRestoreTimeoutMs": 60_000
        }
    })
}

async fn daemon_status_body(context: &ServeContext, detail: &str) -> Value {
    let process_memory = process_memory_body(context).await;
    let mut body = json!({
        "v": 1,
        "detail": detail,
        "generatedAt": timestamp(),
        "status": "warning",
        "issues": [{
            "code": "daemon_runtime_starting",
            "severity": "warning",
            "message": "The Rust daemon has no agent-session runtime in this serve slice."
        }],
        "daemon": {
            "pid": std::process::id(),
            "uptimeMs": elapsed_ms(context.started),
            "mode": "native",
            "workspaceCwd": context.workspace_cwd.as_str(),
            "startup": {
                "processStartedAt": context.process_started_at.as_str(),
                "listenerReadyAt": context.listener_ready_at.as_str(),
                "processToListenMs": context.listener_ready_ms,
                "runCanopyServeToListenMs": context.listener_ready_ms,
                "preheat": { "status": "not_scheduled" }
            },
            "canopyCodeVersion": env!("CARGO_PKG_VERSION")
        },
        "security": {
            "tokenConfigured": context.auth_token_hash.is_some(),
            "requireAuth": context.require_auth,
            "loopbackBind": true,
            "allowOriginConfigured": false,
            "allowOriginMode": "none",
            "sessionShellCommandEnabled": false
        },
        "limits": {
            "maxSessions": 32,
            "maxTotalSessions": null,
            "maxPendingPromptsPerSession": 5,
            "listenerMaxConnections": MAX_CONNECTIONS,
            "eventRingSize": 8000,
            "compactedReplayMaxBytes": 4 * 1024 * 1024,
            "maxJournalEvents": 10_000,
            "maxJournalBytes": 8 * 1024 * 1024,
            "promptDeadlineMs": null,
            "writerIdleTimeoutMs": null,
            "channelIdleTimeoutMs": 0,
            "sessionIdleTimeoutMs": 1_800_000,
            "acpConnectionCap": null,
            "acpPreAttachMaxFramesPerStream": null,
            "acpPreAttachMaxFramesPerConnection": null,
            "acpPreAttachMaxFramesGlobal": null,
            "acpPreAttachMaxPayloadBytesPerConnection": null,
            "acpPreAttachMaxPayloadBytesGlobal": null,
            "memory": null
        },
        "capabilities": {
            "protocolVersions": { "current": "v1", "supported": ["v1"] },
            "features": SERVE_FEATURES
        },
        "runtime": {
            "loading": true,
            "sessions": { "active": 0 },
            "permissions": { "pending": 0, "policy": "first-responder" },
            "channel": { "live": false },
            "channelWorker": { "enabled": false, "state": "disabled", "channels": [] },
            "transport": {
                "restSseActive": 0,
                "acp": {
                    "enabled": false,
                    "connections": 0,
                    "connectionStreams": 0,
                    "sessionStreams": 0,
                    "sseStreams": 0,
                    "wsStreams": 0,
                    "pendingClientRequests": 0,
                    "preAttach": {
                        "bufferedConnectionFrames": 0,
                        "bufferedSessionFrames": 0,
                        "pendingDeliveryFrames": 0,
                        "usedFrames": 0,
                        "usedBytes": 0,
                        "highWaterFrames": 0,
                        "highWaterBytes": 0,
                        "guardFailures": 0
                    }
                }
            },
            "rateLimit": {
                "enabled": false,
                "rejectedSinceStart": { "prompt": 0, "mutation": 0, "read": 0 }
            },
            "activity": {
                "activePrompts": 0,
                "pendingPrompts": 0,
                "queuedPrompts": 0,
                "lastActivityAt": null,
                "idleSinceMs": null
            },
            "process": process_memory
        }
    });
    if detail == "full" {
        body["full"] = json!({
            "sessions": [],
            "acpMounts": [],
            "acpConnections": [],
            "workspace": {},
            "auth": {
                "supportedDeviceFlowProviders": [],
                "pendingDeviceFlowCount": 0
            }
        });
    }
    body
}

async fn process_memory_body(context: &ServeContext) -> Value {
    let Ok(permit) = Arc::clone(&context.memory_probe_slots).try_acquire_owned() else {
        return zero_process_memory();
    };
    let sample = timeout(
        MEMORY_PROBE_TIMEOUT,
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            NativeMemoryProbe::system().memory_usage()
        }),
    )
    .await;
    match sample {
        Ok(Ok(Ok(memory))) => json!({
            "rss": memory.rss,
            "heapTotal": memory.heap_total,
            "heapUsed": memory.heap_used,
            "external": memory.external,
            "arrayBuffers": 0
        }),
        _ => zero_process_memory(),
    }
}

fn zero_process_memory() -> Value {
    json!({
        "rss": 0,
        "heapTotal": 0,
        "heapUsed": 0,
        "external": 0,
        "arrayBuffers": 0
    })
}

fn is_deep_health_query(query: Option<&str>) -> bool {
    let Some(query) = query else {
        return false;
    };
    let mut deep_value = None;
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if key == "deep" {
            if deep_value.is_some() {
                return false;
            }
            deep_value = Some(value.into_owned());
        }
    }
    matches!(deep_value.as_deref(), Some("" | "1" | "true"))
}

fn is_allowed_host(host: &str, port: u16) -> bool {
    let host = host.to_ascii_lowercase();
    let port = port.to_string();
    [
        format!("localhost:{port}"),
        format!("127.0.0.1:{port}"),
        format!("[::1]:{port}"),
        format!("host.docker.internal:{port}"),
    ]
    .iter()
    .any(|allowed| allowed == &host)
        || matches!(port.as_str(), "80" | "443")
            && ["localhost", "127.0.0.1", "[::1]", "host.docker.internal"].contains(&host.as_str())
}

fn is_same_origin(origin: &str, port: u16) -> bool {
    let origin = origin.to_ascii_lowercase();
    let port_text = port.to_string();
    let hosts = [
        format!("localhost:{port_text}"),
        format!("127.0.0.1:{port_text}"),
        format!("[::1]:{port_text}"),
        format!("host.docker.internal:{port_text}"),
    ];
    let mut allowed = Vec::with_capacity(hosts.len() * 2 + 8);
    for scheme in ["http://", "https://"] {
        allowed.extend(hosts.iter().map(|host| format!("{scheme}{host}")));
        if matches!(port, 80 | 443) {
            allowed.extend([
                format!("{scheme}localhost"),
                format!("{scheme}127.0.0.1"),
                format!("{scheme}[::1]"),
                format!("{scheme}host.docker.internal"),
            ]);
        }
    }
    allowed.iter().any(|candidate| candidate == &origin)
}

fn json_response(
    status: StatusCode,
    body: &'static str,
    head_only: bool,
    close_connection: bool,
    extra_header: Option<(hyper::header::HeaderName, &'static str)>,
) -> Response<Full<Bytes>> {
    let body_bytes = if head_only {
        Bytes::new()
    } else {
        Bytes::from_static(body.as_bytes())
    };
    let mut response = Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json; charset=utf-8")
        .header(CONTENT_LENGTH, body.len().to_string());
    if close_connection {
        response = response.header(CONNECTION, "close");
    }
    if let Some((name, value)) = extra_header {
        response = response.header(name, value);
    }
    response.body(Full::new(body_bytes)).unwrap()
}

fn json_value_response(
    status: StatusCode,
    body: Value,
    head_only: bool,
    close_connection: bool,
) -> Response<Full<Bytes>> {
    let body = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
    json_bytes_response(status, body, head_only, close_connection)
}

fn json_bytes_response(
    status: StatusCode,
    body: Vec<u8>,
    head_only: bool,
    close_connection: bool,
) -> Response<Full<Bytes>> {
    let content_length = body.len().to_string();
    let body = if head_only {
        Bytes::new()
    } else {
        Bytes::from(body)
    };
    let mut response = Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json; charset=utf-8")
        .header(CONTENT_LENGTH, content_length);
    if close_connection {
        response = response.header(CONNECTION, "close");
    }
    response.body(Full::new(body)).unwrap()
}

fn text_response(
    status: StatusCode,
    body: &'static str,
    head_only: bool,
    close_connection: bool,
) -> Response<Full<Bytes>> {
    let body_bytes = if head_only {
        Bytes::new()
    } else {
        Bytes::from_static(body.as_bytes())
    };
    let mut response = Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(CONTENT_LENGTH, body.len().to_string());
    if close_connection {
        response = response.header(CONNECTION, "close");
    }
    response.body(Full::new(body_bytes)).unwrap()
}

/// Run the loopback-only native daemon. The host remains fixed to loopback;
/// session, TLS and other daemon options are rejected by the parser.
pub(super) fn run(args: &[String]) -> Result<(), String> {
    let options = match parse_options(args)? {
        Some(options) => options,
        None => {
            println!("{USAGE}");
            println!(
                "Loopback-only daemon routes: GET/HEAD /health, /capabilities, /daemon/status, /workspace/memory, /workspace/git, /workspace/git/branches, /workspace/git/log, /workspace/git/log/commit, /workspace/git/diff, /workspace/git/diff/file, /session/:id/status, /session/:id/transcript, and /session/:id/transcript/records; POST /workspace/git/checkout, /workspace/git/branch, /workspace/git/push, /workspace/git/pull, and /workspace/git/commit."
            );
            println!(
                "Workspace memory returns bounded file metadata for context files at the workspace root and global memory directory; it does not return file contents or load workspace trust settings. Session reads are limited to active live native sessions; transcript returns bounded replayed ACP events and transcript/records returns bounded JSONL records. Active-prompt state is unavailable, so dangling calls are left unresolved. This serve process starts no agent-session runtime."
            );
            println!(
                "A configured --token or QWEN_SERVER_TOKEN requires Authorization: Bearer <token> on every route, including /health."
            );
            println!(
                "Without a token, loopback access stays open unless --require-auth is set; --require-auth requires a non-empty token."
            );
            return Ok(());
        }
    };
    let ServeOptions {
        port,
        token: cli_token,
        require_auth,
    } = options;
    let warn_token_argument = cli_token.as_ref().is_some_and(|value| !value.is_empty());
    let raw_token = cli_token.or_else(|| std::env::var("QWEN_SERVER_TOKEN").ok());
    let token = raw_token
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    if require_auth && token.is_none() {
        return Err(
            "Refusing to start with --require-auth set but no bearer token configured. Set QWEN_SERVER_TOKEN or pass --token, or omit --require-auth to keep the loopback developer default."
                .to_owned(),
        );
    }
    if warn_token_argument {
        eprintln!(
            "canopy serve: --token is visible in the process command line; prefer QWEN_SERVER_TOKEN for a non-trivial deployment."
        );
    }
    let mut context = ServeContext::new(token.as_deref(), require_auth)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("Could not start serve runtime: {error}"))?;
    runtime.block_on(async move {
        let listener = bind_loopback(port).await?;
        let address = listener
            .local_addr()
            .map_err(|error| format!("Could not inspect serve listener: {error}"))?;
        context.mark_listener_ready();
        println!("Canopy serve listening at http://{address}");
        serve_with_context(listener, shutdown_signal(), Arc::new(context)).await
    })
}

struct ServeOptions {
    port: u16,
    token: Option<String>,
    require_auth: bool,
}

fn parse_options(args: &[String]) -> Result<Option<ServeOptions>, String> {
    let mut port = DEFAULT_PORT;
    let mut token = None;
    let mut require_auth = false;
    let mut index = 0;
    while index < args.len() {
        let argument = &args[index];
        if matches!(argument.as_str(), "--help" | "-h") {
            return Ok(None);
        }
        if argument == "--transport-smoke" {
            // Kept as a no-op for scripts that used the earlier transport preview.
            index += 1;
            continue;
        }
        if argument == "--no-require-auth" {
            require_auth = false;
            index += 1;
            continue;
        }
        if argument == "--require-auth" {
            require_auth = true;
            index += 1;
            continue;
        }
        if let Some(value) = argument.strip_prefix("--require-auth=") {
            require_auth = match value {
                "true" => true,
                "false" => false,
                _ => return Err(format!("Invalid --require-auth value: {value}")),
            };
            index += 1;
            continue;
        }
        if let Some(value) = argument.strip_prefix("--token=") {
            token = Some(value.to_owned());
            index += 1;
            continue;
        }
        if argument == "--token" {
            index += 1;
            let value = args
                .get(index)
                .ok_or_else(|| "--token requires a value".to_owned())?;
            if value.starts_with("--") {
                return Err("--token requires a value".to_owned());
            }
            token = Some(value.clone());
            index += 1;
            continue;
        }
        let value = if let Some(value) = argument.strip_prefix("--port=") {
            value
        } else if argument == "--port" {
            index += 1;
            args.get(index)
                .map(String::as_str)
                .ok_or_else(|| "--port requires a value".to_owned())?
        } else {
            return Err(format!(
                "Unsupported native serve option: {argument}. Active options are --port, --token, --require-auth, and --help; --transport-smoke is ignored as a legacy alias. The listener is loopback-only and other daemon runtime options are not implemented.\n{USAGE}"
            ));
        };
        port = value
            .parse::<u16>()
            .map_err(|_| format!("Invalid serve port: {value}"))?;
        index += 1;
    }
    Ok(Some(ServeOptions {
        port,
        token,
        require_auth,
    }))
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(error) => {
                eprintln!("Could not register SIGTERM handler: {error}");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    eprintln!("Could not receive Ctrl+C: {error}");
                }
            }
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        eprintln!("Could not receive Ctrl+C: {error}");
    }
}
