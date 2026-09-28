//! Workspace-qualified projection of the TypeScript `WorkspaceDaemonClient`.
//!
//! Request and response payloads remain `serde_json::Value`, matching the
//! parent Rust daemon client and allowing daemon fields to evolve without a
//! release of this crate. Routes are always rooted at
//! `/workspaces/{encoded-selector}`; operations that TypeScript deliberately
//! delegates to the daemon-global client (the live controls) retain their
//! global routes.

use std::time::Duration;

use serde_json::{Value, json};

use crate::daemon_client::{
    DaemonClient, DaemonClientError, DaemonRequestMode, DaemonRequestOptions, RequestTimeout,
    encode_uri_component,
};
use crate::daemon_rest::RestSseCancellation;
use crate::daemon_upload_progress::UploadProgress;

const CHANNEL_NOTIFY_TIMEOUT: Duration = Duration::from_secs(35);
const CHANNEL_CONTROL_TIMEOUT: Duration = Duration::from_secs(2_130);
const MCP_RESTART_TIMEOUT: Duration = Duration::from_secs(330);
const VOICE_TRANSCRIPTION_TIMEOUT: Duration = Duration::from_secs(65);
const DEFAULT_SESSION_PAGE_SIZE: u32 = 20;

/// Per-call settings accepted by workspace routes.
#[derive(Clone)]
pub struct WorkspaceRequestOptions {
    pub client_id: Option<String>,
    /// `Disabled` maps to the TypeScript client's `timeoutMs: 0` behavior.
    pub timeout: RequestTimeout,
    pub cancellation: Option<RestSseCancellation>,
}

impl Default for WorkspaceRequestOptions {
    fn default() -> Self {
        Self {
            client_id: None,
            timeout: RequestTimeout::ClientDefault,
            cancellation: None,
        }
    }
}

/// A workspace-scoped view over a shared [`DaemonClient`].
#[derive(Clone)]
pub struct WorkspaceDaemonClientProjection {
    client: DaemonClient,
    selector: String,
}

impl WorkspaceDaemonClientProjection {
    /// Create a projection from a workspace ID. The value is encoded as one
    /// URL path component, like TypeScript's `workspaceById`.
    pub fn by_id(client: DaemonClient, workspace_id: &str) -> Self {
        Self::new(client, workspace_id)
    }

    /// Create a projection from a registered workspace cwd.
    pub fn by_cwd(client: DaemonClient, workspace_cwd: &str) -> Self {
        Self::new(client, workspace_cwd)
    }

    /// Create a projection from a raw workspace selector.
    pub fn new(client: DaemonClient, selector: &str) -> Self {
        Self {
            client,
            selector: encode_uri_component(selector),
        }
    }

    /// Return the already URL-encoded selector used by workspace routes.
    pub fn selector(&self) -> &str {
        &self.selector
    }

    /// Dispatch an arbitrary workspace route, preserving the daemon's open
    /// JSON response shape and the shared transport's error semantics.
    pub async fn request_json(
        &self,
        method: &str,
        suffix: &str,
        query: &str,
        body: Option<Value>,
        label: &str,
        options: WorkspaceRequestOptions,
    ) -> Result<Value, DaemonClientError> {
        let (suffix, embedded_query) = split_suffix_query(suffix);
        let query = if query.is_empty() {
            embedded_query
        } else if embedded_query.is_empty() {
            query.to_owned()
        } else {
            format!("{}&{}", query.trim_start_matches('?'), embedded_query)
        };
        let path = self.path(suffix);
        self.client
            .request_json(
                method,
                &path,
                &query,
                body,
                label,
                self.daemon_options(options),
            )
            .await
    }

    /// Execute an endpoint that normally returns no content. The 404 code is
    /// treated as success only when explicitly supplied by the route wrapper.
    pub async fn request_no_content(
        &self,
        method: &str,
        suffix: &str,
        query: &str,
        body: Option<Value>,
        label: &str,
        ok_not_found_code: Option<&str>,
        options: WorkspaceRequestOptions,
    ) -> Result<(), DaemonClientError> {
        let (suffix, embedded_query) = split_suffix_query(suffix);
        let query = if query.is_empty() {
            embedded_query
        } else if embedded_query.is_empty() {
            query.to_owned()
        } else {
            format!("{}&{}", query.trim_start_matches('?'), embedded_query)
        };
        let path = self.path(suffix);
        let mut daemon_options = self.daemon_options(options);
        daemon_options.mode = DaemonRequestMode::Rest;
        self.client
            .request_no_content(
                method,
                &path,
                &query,
                body,
                label,
                ok_not_found_code,
                daemon_options,
            )
            .await
    }

    fn path(&self, suffix: &str) -> String {
        format!("/workspaces/{}{}", self.selector, suffix)
    }

    fn daemon_options(&self, options: WorkspaceRequestOptions) -> DaemonRequestOptions {
        DaemonRequestOptions {
            client_id: options.client_id,
            cancellation: options.cancellation,
            timeout: options.timeout,
            ..DaemonRequestOptions::default()
        }
    }

    async fn get(
        &self,
        suffix: &str,
        label: &str,
        options: WorkspaceRequestOptions,
    ) -> Result<Value, DaemonClientError> {
        self.request_json("GET", suffix, "", None, label, options)
            .await
    }

    async fn post(
        &self,
        suffix: &str,
        label: &str,
        body: Value,
        options: WorkspaceRequestOptions,
    ) -> Result<Value, DaemonClientError> {
        self.request_json("POST", suffix, "", Some(body), label, options)
            .await
    }

    async fn channel_request(
        &self,
        suffix: &str,
        label: &str,
        request: Option<(&str, Value)>,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        let mut options = options.unwrap_or_default();
        options.timeout = (options.timeout != RequestTimeout::ClientDefault)
            .then_some(options.timeout)
            .unwrap_or_else(|| {
                if request.is_some() {
                    RequestTimeout::After(CHANNEL_CONTROL_TIMEOUT)
                } else {
                    RequestTimeout::ClientDefault
                }
            });
        let (suffix, query) = split_suffix_query(suffix);
        let path = self.path(suffix);
        let mut daemon_options = self.daemon_options(options);
        daemon_options.mode = DaemonRequestMode::Rest;
        match request {
            Some((method, body)) => {
                self.client
                    .request_json(method, &path, &query, Some(body), label, daemon_options)
                    .await
            }
            None => {
                self.client
                    .request_json("GET", &path, &query, None, label, daemon_options)
                    .await
            }
        }
    }

    /// GET `/mcp`.
    pub async fn workspace_mcp(&self) -> Result<Value, DaemonClientError> {
        self.get(
            "/mcp",
            "GET /workspaces/:workspace/mcp",
            WorkspaceRequestOptions::default(),
        )
        .await
    }

    /// Deliver text through this workspace's channel worker.
    pub async fn notify(
        &self,
        request: Value,
        options: WorkspaceRequestOptions,
    ) -> Result<Value, DaemonClientError> {
        let mut options = options;
        if options.timeout == RequestTimeout::ClientDefault {
            options.timeout = RequestTimeout::After(CHANNEL_NOTIFY_TIMEOUT);
        }
        options.cancellation = None;
        let mut daemon_options = self.daemon_options(options);
        daemon_options.mode = DaemonRequestMode::Rest;
        let path = self.path("/notify");
        self.client
            .request_json(
                "POST",
                &path,
                "",
                Some(request),
                "POST /workspaces/:workspace/notify",
                daemon_options,
            )
            .await
    }

    pub async fn workspace_channel_types(
        &self,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        self.channel_request(
            "/channel-types",
            "GET /workspaces/:workspace/channel-types",
            None,
            options,
        )
        .await
    }

    pub async fn workspace_channels(
        &self,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        self.channel_request(
            "/channels",
            "GET /workspaces/:workspace/channels",
            None,
            options,
        )
        .await
    }

    pub async fn upsert_workspace_channel(
        &self,
        name: &str,
        request: Value,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        self.channel_request(
            &format!("/channels/{}", encode_uri_component(name)),
            "PUT /workspaces/:workspace/channels/:name",
            Some(("PUT", request)),
            options,
        )
        .await
    }

    pub async fn delete_workspace_channel(
        &self,
        name: &str,
        request: Value,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        self.channel_request(
            &format!("/channels/{}", encode_uri_component(name)),
            "DELETE /workspaces/:workspace/channels/:name",
            Some(("DELETE", request)),
            options,
        )
        .await
    }

    pub async fn set_workspace_channel_startup(
        &self,
        name: &str,
        request: Value,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        self.channel_request(
            &format!("/channels/{}/startup", encode_uri_component(name)),
            "PUT /workspaces/:workspace/channels/:name/startup",
            Some(("PUT", request)),
            options,
        )
        .await
    }

    async fn channel_action(
        &self,
        name: &str,
        action: &str,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        self.channel_request(
            &format!("/channels/{}/{action}", encode_uri_component(name)),
            match action {
                "start" => "POST /workspaces/:workspace/channels/:name/start",
                "stop" => "POST /workspaces/:workspace/channels/:name/stop",
                _ => "POST /workspaces/:workspace/channels/:name/restart",
            },
            Some(("POST", json!({}))),
            options,
        )
        .await
    }

    pub async fn start_workspace_channel(
        &self,
        name: &str,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        self.channel_action(name, "start", options).await
    }
    pub async fn stop_workspace_channel(
        &self,
        name: &str,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        self.channel_action(name, "stop", options).await
    }
    pub async fn restart_workspace_channel(
        &self,
        name: &str,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        self.channel_action(name, "restart", options).await
    }

    pub async fn workspace_channel_pairing_requests(
        &self,
        name: &str,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        self.channel_request(
            &format!("/channels/{}/pairing-requests", encode_uri_component(name)),
            "GET /workspaces/:workspace/channels/:name/pairing-requests",
            None,
            options,
        )
        .await
    }
    pub async fn approve_workspace_channel_pairing(
        &self,
        name: &str,
        request: Value,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        self.channel_request(
            &format!(
                "/channels/{}/pairing-requests/approve",
                encode_uri_component(name)
            ),
            "POST /workspaces/:workspace/channels/:name/pairing-requests/approve",
            Some(("POST", request)),
            options,
        )
        .await
    }
    pub async fn workspace_channel_pairing_approvals(
        &self,
        name: &str,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        self.channel_request(
            &format!("/channels/{}/pairing-approvals", encode_uri_component(name)),
            "GET /workspaces/:workspace/channels/:name/pairing-approvals",
            None,
            options,
        )
        .await
    }
    pub async fn revoke_workspace_channel_pairing_approval(
        &self,
        name: &str,
        request: Value,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        self.channel_request(
            &format!("/channels/{}/pairing-approvals", encode_uri_component(name)),
            "DELETE /workspaces/:workspace/channels/:name/pairing-approvals",
            Some(("DELETE", request)),
            options,
        )
        .await
    }

    pub async fn initialize_workspace_mcp(&self) -> Result<Value, DaemonClientError> {
        self.post(
            "/mcp/initialize",
            "POST /workspaces/:workspace/mcp/initialize",
            json!({}),
            WorkspaceRequestOptions::default(),
        )
        .await
    }
    pub async fn reload_workspace_mcp(&self, options: Value) -> Result<Value, DaemonClientError> {
        self.post(
            "/mcp/reload",
            "POST /workspaces/:workspace/mcp/reload",
            options,
            WorkspaceRequestOptions::default(),
        )
        .await
    }

    pub async fn workspace_voice(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let mut options = WorkspaceRequestOptions {
            client_id,
            ..Default::default()
        };
        options.timeout = RequestTimeout::ClientDefault;
        let mut daemon_options = self.daemon_options(options);
        daemon_options.mode = DaemonRequestMode::Rest;
        self.client
            .request_json(
                "GET",
                &self.path("/voice"),
                "",
                None,
                "GET /workspaces/:workspace/voice",
                daemon_options,
            )
            .await
    }

    pub async fn set_workspace_voice(
        &self,
        update: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let mut daemon_options = self.daemon_options(WorkspaceRequestOptions {
            client_id,
            ..Default::default()
        });
        daemon_options.mode = DaemonRequestMode::Rest;
        self.client
            .request_json(
                "POST",
                &self.path("/voice"),
                "",
                Some(update),
                "POST /workspaces/:workspace/voice",
                daemon_options,
            )
            .await
    }

    pub async fn transcribe_workspace_voice(
        &self,
        audio: Vec<u8>,
        mime_type: &str,
        voice_model: Option<&str>,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
        cancellation: Option<RestSseCancellation>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[("voiceModel", voice_model.map(str::to_owned))]);
        let response = self
            .client
            .request_bytes(
                "POST",
                &self.path("/voice/transcribe"),
                &query,
                Some(audio),
                "POST /workspaces/:workspace/voice/transcribe",
                DaemonRequestOptions {
                    client_id,
                    cancellation,
                    mode: DaemonRequestMode::Rest,
                    content_type: Some(mime_type.to_owned()),
                    timeout: RequestTimeout::After(
                        timeout_duration.unwrap_or(VOICE_TRANSCRIPTION_TIMEOUT),
                    ),
                    ..DaemonRequestOptions::default()
                },
            )
            .await?;
        serde_json::from_slice(&response.body).map_err(|error| {
            DaemonClientError::InvalidResponse(format!(
                "POST /workspaces/:workspace/voice/transcribe: invalid JSON response: {error}"
            ))
        })
    }

    pub async fn live_status(&self, client_id: Option<String>) -> Result<Value, DaemonClientError> {
        self.client.live_status(client_id).await
    }
    pub async fn live_setup_status(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.client.live_setup_status(client_id).await
    }
    pub async fn update_live_setup(
        &self,
        update: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.client.update_live_setup(update, client_id).await
    }
    pub async fn retry_live_host_install(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.client.retry_live_host_install(client_id).await
    }
    pub async fn launch_live_host(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.client.launch_live_host(client_id).await
    }
    pub async fn start_live(
        &self,
        mode: Option<&str>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.client.start_live(mode, client_id).await
    }
    pub async fn stop_live(&self, client_id: Option<String>) -> Result<Value, DaemonClientError> {
        self.client.stop_live(client_id).await
    }
    pub async fn set_live_mute(
        &self,
        update: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.client.set_live_mute(update, client_id).await
    }
    pub async fn set_live_shortcut(
        &self,
        shortcut: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.client.set_live_shortcut(shortcut, client_id).await
    }

    pub async fn workspace_git(
        &self,
        cwd: Option<&str>,
        wait: bool,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[
            ("cwd", cwd.map(str::to_owned)),
            ("wait", wait.then(|| "1".into())),
        ]);
        self.workspace_get("/git", query, "GET /workspaces/:workspace/git", true, None)
            .await
    }
    pub async fn workspace_git_diff(&self, cwd: Option<&str>) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[("cwd", cwd.map(str::to_owned))]);
        self.workspace_get(
            "/git/diff",
            query,
            "GET /workspaces/:workspace/git/diff",
            true,
            None,
        )
        .await
    }
    pub async fn workspace_git_diff_file(
        &self,
        path: &str,
        old_path: Option<&str>,
        cwd: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[
            ("path", Some(path.to_owned())),
            ("oldPath", old_path.map(str::to_owned)),
            ("cwd", cwd.map(str::to_owned)),
        ]);
        self.workspace_get(
            "/git/diff/file",
            query,
            "GET /workspaces/:workspace/git/diff/file",
            true,
            None,
        )
        .await
    }
    pub async fn workspace_git_log(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
        cwd: Option<&str>,
        range: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[
            ("limit", limit.map(|v| v.to_string())),
            ("skip", skip.map(|v| v.to_string())),
            ("cwd", cwd.map(str::to_owned)),
            ("range", range.map(str::to_owned)),
        ]);
        self.workspace_get(
            "/git/log",
            query,
            "GET /workspaces/:workspace/git/log",
            true,
            None,
        )
        .await
    }
    pub async fn workspace_git_commit_detail(
        &self,
        sha: &str,
        cwd: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[
            ("sha", Some(sha.to_owned())),
            ("cwd", cwd.map(str::to_owned)),
        ]);
        self.workspace_get(
            "/git/log/commit",
            query,
            "GET /workspaces/:workspace/git/log/commit",
            true,
            None,
        )
        .await
    }
    pub async fn workspace_git_branches(
        &self,
        cwd: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[("cwd", cwd.map(str::to_owned))]);
        self.workspace_get(
            "/git/branches",
            query,
            "GET /workspaces/:workspace/git/branches",
            true,
            None,
        )
        .await
    }
    pub async fn workspace_git_checkout(
        &self,
        reference: &str,
        cwd: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[("cwd", cwd.map(str::to_owned))]);
        self.workspace_post(
            "/git/checkout",
            query,
            "POST /workspaces/:workspace/git/checkout",
            json!({"ref":reference}),
            true,
            None,
        )
        .await
    }
    pub async fn workspace_git_create_branch(
        &self,
        name: &str,
        start_point: Option<&str>,
        cwd: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[("cwd", cwd.map(str::to_owned))]);
        let mut body = json!({"name":name});
        if let Some(start_point) = start_point {
            body["startPoint"] = json!(start_point);
        }
        self.workspace_post(
            "/git/branch",
            query,
            "POST /workspaces/:workspace/git/branch",
            body,
            true,
            None,
        )
        .await
    }
    pub async fn workspace_git_push(
        &self,
        options: Option<Value>,
        cwd: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[("cwd", cwd.map(str::to_owned))]);
        self.workspace_post(
            "/git/push",
            query,
            "POST /workspaces/:workspace/git/push",
            options.unwrap_or_else(|| json!({})),
            true,
            None,
        )
        .await
    }
    pub async fn workspace_git_pull(
        &self,
        options: Option<Value>,
        cwd: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[("cwd", cwd.map(str::to_owned))]);
        self.workspace_post(
            "/git/pull",
            query,
            "POST /workspaces/:workspace/git/pull",
            options.unwrap_or_else(|| json!({})),
            true,
            None,
        )
        .await
    }
    pub async fn workspace_git_commit(
        &self,
        message: &str,
        options: Option<Value>,
        cwd: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[("cwd", cwd.map(str::to_owned))]);
        let mut body = options
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({}));
        body["message"] = json!(message);
        self.workspace_post(
            "/git/commit",
            query,
            "POST /workspaces/:workspace/git/commit",
            body,
            true,
            None,
        )
        .await
    }
    pub async fn workspace_github_pull_requests(&self) -> Result<Value, DaemonClientError> {
        self.workspace_get(
            "/github/prs",
            String::new(),
            "GET /workspaces/:workspace/github/prs",
            true,
            None,
        )
        .await
    }
    pub async fn workspace_github_create_pull_request(
        &self,
        request: Value,
        cwd: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[("cwd", cwd.map(str::to_owned))]);
        self.workspace_post(
            "/github/prs/create",
            query,
            "POST /workspaces/:workspace/github/prs/create",
            request,
            true,
            None,
        )
        .await
    }
    pub async fn workspace_github_default_branch(&self) -> Result<Value, DaemonClientError> {
        self.workspace_get(
            "/github/default-branch",
            String::new(),
            "GET /workspaces/:workspace/github/default-branch",
            true,
            None,
        )
        .await
    }

    async fn workspace_get(
        &self,
        path: &str,
        query: String,
        label: &str,
        rest: bool,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        let options = options.unwrap_or_default();
        let mut daemon_options = self.daemon_options(options);
        if rest {
            daemon_options.mode = DaemonRequestMode::Rest;
        }
        let route = self.path(path);
        self.client
            .request_json("GET", &route, &query, None, label, daemon_options)
            .await
    }
    async fn workspace_post(
        &self,
        path: &str,
        query: String,
        label: &str,
        body: Value,
        rest: bool,
        options: Option<WorkspaceRequestOptions>,
    ) -> Result<Value, DaemonClientError> {
        let options = options.unwrap_or_default();
        let mut daemon_options = self.daemon_options(options);
        if rest {
            daemon_options.mode = DaemonRequestMode::Rest;
        }
        let route = self.path(path);
        self.client
            .request_json("POST", &route, &query, Some(body), label, daemon_options)
            .await
    }

    pub async fn workspace_skills(&self) -> Result<Value, DaemonClientError> {
        self.get(
            "/skills",
            "GET /workspaces/:workspace/skills",
            WorkspaceRequestOptions::default(),
        )
        .await
    }
    pub async fn workspace_providers(&self) -> Result<Value, DaemonClientError> {
        self.get(
            "/providers",
            "GET /workspaces/:workspace/providers",
            WorkspaceRequestOptions::default(),
        )
        .await
    }
    pub async fn workspace_hooks(&self) -> Result<Value, DaemonClientError> {
        self.get(
            "/hooks",
            "GET /workspaces/:workspace/hooks",
            WorkspaceRequestOptions::default(),
        )
        .await
    }
    pub async fn workspace_env(&self) -> Result<Value, DaemonClientError> {
        self.get(
            "/env",
            "GET /workspaces/:workspace/env",
            WorkspaceRequestOptions::default(),
        )
        .await
    }
    pub async fn workspace_preflight(&self) -> Result<Value, DaemonClientError> {
        self.get(
            "/preflight",
            "GET /workspaces/:workspace/preflight",
            WorkspaceRequestOptions::default(),
        )
        .await
    }
    pub async fn workspace_tools(&self) -> Result<Value, DaemonClientError> {
        self.get(
            "/tools",
            "GET /workspaces/:workspace/tools",
            WorkspaceRequestOptions::default(),
        )
        .await
    }
    pub async fn workspace_memory(&self) -> Result<Value, DaemonClientError> {
        self.get(
            "/memory",
            "GET /workspaces/:workspace/memory",
            WorkspaceRequestOptions::default(),
        )
        .await
    }

    pub async fn remove(
        &self,
        force: Option<bool>,
        timeout: Option<Duration>,
    ) -> Result<Value, DaemonClientError> {
        let body = force.map(|force| json!({"force":force}));
        let path = self.path("");
        self.client
            .request_json(
                "DELETE",
                &path,
                "",
                body,
                "DELETE /workspaces/:workspace",
                DaemonRequestOptions {
                    timeout: timeout
                        .map(RequestTimeout::After)
                        .unwrap_or(RequestTimeout::ClientDefault),
                    mode: DaemonRequestMode::Rest,
                    ..DaemonRequestOptions::default()
                },
            )
            .await
    }

    pub async fn write_workspace_memory(
        &self,
        mut request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        request["scope"] = json!("workspace");
        self.post(
            "/memory",
            "POST /workspaces/:workspace/memory",
            request,
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn list_workspace_agents(&self) -> Result<Value, DaemonClientError> {
        self.get(
            "/agents",
            "GET /workspaces/:workspace/agents",
            WorkspaceRequestOptions::default(),
        )
        .await
    }
    pub async fn create_workspace_agent(
        &self,
        mut request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        if request.get("scope").is_none_or(Value::is_null) {
            request["scope"] = json!("workspace");
        }
        self.post(
            "/agents",
            "POST /workspaces/:workspace/agents",
            request,
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn get_workspace_agent(&self, agent_type: &str) -> Result<Value, DaemonClientError> {
        let suffix = format!("/agents/{}", encode_uri_component(agent_type));
        self.get(
            &suffix,
            "GET /workspaces/:workspace/agents/:agentType",
            WorkspaceRequestOptions::default(),
        )
        .await
    }
    pub async fn update_workspace_agent(
        &self,
        agent_type: &str,
        request: Value,
        scope: Option<&str>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let suffix = format!("/agents/{}", encode_uri_component(agent_type));
        let query = encode_query(&[("scope", scope.map(str::to_owned))]);
        self.workspace_post(
            &suffix,
            query,
            "POST /workspaces/:workspace/agents/:agentType",
            request,
            false,
            Some(WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            }),
        )
        .await
    }
    pub async fn delete_workspace_agent(
        &self,
        agent_type: &str,
        scope: Option<&str>,
        client_id: Option<String>,
    ) -> Result<(), DaemonClientError> {
        let suffix = format!("/agents/{}", encode_uri_component(agent_type));
        let query = encode_query(&[("scope", scope.map(str::to_owned))]);
        self.request_no_content(
            "DELETE",
            &suffix,
            &query,
            None,
            "DELETE /workspaces/:workspace/agents/:agentType",
            Some("agent_not_found"),
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn list_workspace_sessions_page(
        &self,
        options: Option<Value>,
    ) -> Result<Value, DaemonClientError> {
        let options = options.unwrap_or_else(|| json!({}));
        if options.get("sourceType").is_some() || options.get("sourceId").is_some() {
            self.client
                .require_capability("session_source_metadata")
                .await?;
        }
        let requested = options
            .get("pageSize")
            .and_then(Value::as_f64)
            .filter(|number| number.is_finite())
            .unwrap_or(DEFAULT_SESSION_PAGE_SIZE as f64);
        let size = requested.round().clamp(1.0, 1000.0) as u32;
        let query = encode_query(&[
            ("size", Some(size.to_string())),
            (
                "cursor",
                options
                    .get("cursor")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            (
                "archiveState",
                options
                    .get("archiveState")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            (
                "view",
                options
                    .get("view")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            (
                "group",
                options
                    .get("group")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            (
                "parentSessionId",
                options
                    .get("parentSessionId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            (
                "sourceType",
                options
                    .get("sourceType")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            (
                "sourceId",
                options
                    .get("sourceId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
        ]);
        self.workspace_get(
            "/sessions",
            query,
            "GET /workspaces/:workspace/sessions",
            false,
            None,
        )
        .await
    }
    pub async fn list_workspace_sessions(
        &self,
        options: Option<Value>,
    ) -> Result<Value, DaemonClientError> {
        let page = self.list_workspace_sessions_page(options).await?;
        page.get("sessions").cloned().ok_or_else(|| {
            DaemonClientError::InvalidResponse(
                "GET /workspaces/:workspace/sessions: missing sessions array".into(),
            )
        })
    }
    pub async fn get_workspace_session_info(&self) -> Result<Value, DaemonClientError> {
        self.get(
            "/session-info",
            "GET /workspaces/:workspace/session-info",
            WorkspaceRequestOptions::default(),
        )
        .await
    }
    pub async fn get_session_live_state(
        &self,
        options: WorkspaceRequestOptions,
    ) -> Result<Value, DaemonClientError> {
        let mut options = options;
        if options.timeout == RequestTimeout::ClientDefault {
            options.timeout = RequestTimeout::ClientDefault;
        }
        let mut daemon_options = self.daemon_options(options);
        daemon_options.mode = DaemonRequestMode::Rest;
        self.client
            .request_json(
                "GET",
                &self.path("/sessions/live-state"),
                "",
                None,
                "GET /workspaces/:workspace/sessions/live-state",
                daemon_options,
            )
            .await
    }
    pub async fn get_session_transcript_page(
        &self,
        session_id: &str,
        cursor: Option<&str>,
        before_record_id: Option<&str>,
        limit: Option<u32>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[
            ("cursor", cursor.map(str::to_owned)),
            ("beforeRecordId", before_record_id.map(str::to_owned)),
            ("limit", limit.map(|value| value.to_string())),
        ]);
        self.workspace_get(
            &format!("/session/{}/transcript", encode_uri_component(session_id)),
            query,
            "GET /workspaces/:workspace/session/:id/transcript",
            false,
            Some(WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            }),
        )
        .await
    }

    pub async fn export_session(
        &self,
        session_id: &str,
        format: Option<&str>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.export_workspace_session(session_id, false, format, client_id)
            .await
    }
    pub async fn export_archived_session(
        &self,
        session_id: &str,
        format: Option<&str>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.export_workspace_session(session_id, true, format, client_id)
            .await
    }
    async fn export_workspace_session(
        &self,
        session_id: &str,
        archived: bool,
        format: Option<&str>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let output_format = format.unwrap_or("html");
        let query = encode_query(&[("format", format.map(str::to_owned))]);
        let suffix = if archived {
            format!(
                "/session/{}/archive/export",
                encode_uri_component(session_id)
            )
        } else {
            format!("/session/{}/export", encode_uri_component(session_id))
        };
        let label = if archived {
            "GET /workspaces/:workspace/session/:id/archive/export"
        } else {
            "GET /workspaces/:workspace/session/:id/export"
        };
        let response = self
            .client
            .request_bytes(
                "GET",
                &self.path(&suffix),
                &query,
                None,
                label,
                DaemonRequestOptions {
                    client_id,
                    mode: DaemonRequestMode::Rest,
                    ..DaemonRequestOptions::default()
                },
            )
            .await?;
        let content = String::from_utf8_lossy(&response.body).into_owned();
        let filename = response
            .content_disposition
            .as_deref()
            .and_then(disposition_filename)
            .unwrap_or_else(|| format!("export.{output_format}"));
        Ok(
            json!({"content":content,"filename":filename,"mimeType":response.content_type.unwrap_or_default(),"format":output_format}),
        )
    }

    pub async fn update_session_metadata(
        &self,
        session_id: &str,
        metadata: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let mut options = self.daemon_options(WorkspaceRequestOptions {
            client_id,
            ..Default::default()
        });
        options.mode = DaemonRequestMode::Rest;
        self.client
            .request_json(
                "PATCH",
                &self.path(&format!(
                    "/session/{}/metadata",
                    encode_uri_component(session_id)
                )),
                "",
                Some(metadata),
                "PATCH /workspaces/:workspace/session/:id/metadata",
                options,
            )
            .await
    }
    pub async fn update_session_organization(
        &self,
        session_id: &str,
        update: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "PATCH",
            &format!("/session/{}/organization", encode_uri_component(session_id)),
            "",
            Some(update),
            "PATCH /workspaces/:workspace/session/:id/organization",
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn list_session_groups(&self) -> Result<Value, DaemonClientError> {
        self.get(
            "/session-groups",
            "GET /workspaces/:workspace/session-groups",
            WorkspaceRequestOptions::default(),
        )
        .await
    }
    pub async fn create_session_group(&self, input: Value) -> Result<Value, DaemonClientError> {
        let response = self
            .post(
                "/session-groups",
                "POST /workspaces/:workspace/session-groups",
                input,
                WorkspaceRequestOptions::default(),
            )
            .await?;
        response.get("group").cloned().ok_or_else(|| {
            DaemonClientError::InvalidResponse(
                "POST /workspaces/:workspace/session-groups: missing group".into(),
            )
        })
    }
    pub async fn update_session_group(
        &self,
        group_id: &str,
        update: Value,
    ) -> Result<Value, DaemonClientError> {
        let response = self
            .request_json(
                "PATCH",
                &format!("/session-groups/{}", encode_uri_component(group_id)),
                "",
                Some(update),
                "PATCH /workspaces/:workspace/session-groups/:groupId",
                WorkspaceRequestOptions::default(),
            )
            .await?;
        response.get("group").cloned().ok_or_else(|| {
            DaemonClientError::InvalidResponse(
                "PATCH /workspaces/:workspace/session-groups/:groupId: missing group".into(),
            )
        })
    }
    pub async fn delete_session_group(&self, group_id: &str) -> Result<Value, DaemonClientError> {
        self.request_json(
            "DELETE",
            &format!("/session-groups/{}", encode_uri_component(group_id)),
            "",
            None,
            "DELETE /workspaces/:workspace/session-groups/:groupId",
            WorkspaceRequestOptions::default(),
        )
        .await
    }
    pub async fn delete_sessions_data(
        &self,
        session_ids: Vec<String>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.post(
            "/sessions/delete",
            "POST /workspaces/:workspace/sessions/delete",
            json!({"sessionIds":session_ids}),
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn archive_sessions_data(
        &self,
        session_ids: Vec<String>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.post(
            "/sessions/archive",
            "POST /workspaces/:workspace/sessions/archive",
            json!({"sessionIds":session_ids}),
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn unarchive_sessions_data(
        &self,
        session_ids: Vec<String>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.post(
            "/sessions/unarchive",
            "POST /workspaces/:workspace/sessions/unarchive",
            json!({"sessionIds":session_ids}),
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn read_workspace_file(
        &self,
        file_path: &str,
        max_bytes: Option<u64>,
        line: Option<u32>,
        limit: Option<u32>,
        cursor: Option<&str>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[
            ("path", Some(file_path.to_owned())),
            ("maxBytes", max_bytes.map(|v| v.to_string())),
            ("line", line.map(|v| v.to_string())),
            ("limit", limit.map(|v| v.to_string())),
            ("cursor", cursor.map(str::to_owned)),
        ]);
        self.workspace_get(
            "/file",
            query,
            "GET /workspaces/:workspace/file",
            false,
            Some(WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            }),
        )
        .await
    }
    pub async fn read_workspace_file_bytes(
        &self,
        file_path: &str,
        offset: Option<u64>,
        max_bytes: Option<u64>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[
            ("path", Some(file_path.to_owned())),
            ("offset", offset.map(|v| v.to_string())),
            ("maxBytes", max_bytes.map(|v| v.to_string())),
        ]);
        self.workspace_get(
            "/file/bytes",
            query,
            "GET /workspaces/:workspace/file/bytes",
            false,
            Some(WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            }),
        )
        .await
    }
    pub async fn file_stat(&self, file_path: &str) -> Result<Value, DaemonClientError> {
        self.workspace_get(
            "/stat",
            encode_query(&[("path", Some(file_path.to_owned()))]),
            "GET /workspaces/:workspace/stat",
            false,
            None,
        )
        .await
    }
    pub async fn dir_list(&self, directory_path: &str) -> Result<Value, DaemonClientError> {
        self.workspace_get(
            "/list",
            encode_query(&[("path", Some(directory_path.to_owned()))]),
            "GET /workspaces/:workspace/list",
            false,
            None,
        )
        .await
    }
    pub async fn glob(
        &self,
        pattern: &str,
        max_results: Option<u32>,
        cancellation: Option<RestSseCancellation>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[
            ("pattern", Some(pattern.to_owned())),
            ("maxResults", max_results.map(|v| v.to_string())),
        ]);
        self.workspace_get(
            "/glob",
            query,
            "GET /workspaces/:workspace/glob",
            false,
            Some(WorkspaceRequestOptions {
                cancellation,
                ..Default::default()
            }),
        )
        .await
    }
    pub async fn write_workspace_file(
        &self,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.post(
            "/file/write",
            "POST /workspaces/:workspace/file/write",
            request,
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn edit_workspace_file(
        &self,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.post(
            "/file/edit",
            "POST /workspaces/:workspace/file/edit",
            request,
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn upload_workspace_file(
        &self,
        file_path: &str,
        data: Vec<u8>,
        timeout: Option<Duration>,
        client_id: Option<String>,
        cancellation: Option<RestSseCancellation>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[("path", Some(file_path.to_owned()))]);
        let response = self
            .client
            .request_bytes(
                "POST",
                &self.path("/file/upload"),
                &query,
                Some(data),
                "POST /workspaces/:workspace/file/upload",
                DaemonRequestOptions {
                    client_id,
                    cancellation,
                    mode: DaemonRequestMode::Rest,
                    content_type: Some("application/octet-stream".into()),
                    timeout: timeout
                        .map(RequestTimeout::After)
                        .unwrap_or(RequestTimeout::ClientDefault),
                    ..DaemonRequestOptions::default()
                },
            )
            .await?;
        let value: Value = serde_json::from_slice(&response.body).map_err(|error| {
            DaemonClientError::InvalidResponse(format!(
                "POST /workspaces/:workspace/file/upload: invalid upload response JSON: {error}"
            ))
        })?;
        if value.get("path").is_none() {
            return Err(DaemonClientError::InvalidResponse(
                "POST /workspaces/:workspace/file/upload: invalid upload response body".into(),
            ));
        }
        Ok(value)
    }

    /// Upload a workspace file and report bytes yielded to the HTTP transport.
    /// The progress callback runs on the async task that drives the request.
    pub async fn upload_workspace_file_with_progress<F>(
        &self,
        file_path: &str,
        data: Vec<u8>,
        timeout: Option<Duration>,
        client_id: Option<String>,
        cancellation: Option<RestSseCancellation>,
        on_progress: F,
    ) -> Result<Value, DaemonClientError>
    where
        F: FnMut(UploadProgress) + Send + 'static,
    {
        self.client
            .upload_file_to_path_with_progress(
                &self.path("/file/upload"),
                file_path,
                data,
                "POST /workspaces/:workspace/file/upload",
                timeout,
                client_id,
                cancellation,
                on_progress,
            )
            .await
    }

    pub async fn workspace_settings(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.get(
            "/settings",
            "GET /workspaces/:workspace/settings",
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn set_workspace_setting(
        &self,
        key: &str,
        value: Value,
        mcp_server_mutation: Option<Value>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let mut body = json!({"scope":"workspace","key":key,"value":value});
        if let Some(mutation) = mcp_server_mutation {
            body["mcpServerMutation"] = mutation;
        }
        self.post(
            "/settings",
            "POST /workspaces/:workspace/settings",
            body,
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn workspace_trust(
        &self,
        status_version: Option<u8>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let query = if status_version == Some(2) {
            "statusVersion=2"
        } else {
            ""
        };
        self.workspace_get(
            "/trust",
            query.to_owned(),
            "GET /workspaces/:workspace/trust",
            false,
            Some(WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            }),
        )
        .await
    }
    pub async fn request_workspace_trust_change(
        &self,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.post(
            "/trust/request",
            "POST /workspaces/:workspace/trust/request",
            request,
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn workspace_permissions(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.get(
            "/permissions",
            "GET /workspaces/:workspace/permissions",
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn set_workspace_permission_rules(
        &self,
        rule_type: &str,
        rules: &[String],
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let mut normalized = Vec::with_capacity(rules.len());
        for rule in rules {
            let rule = rule.trim();
            if rule.is_empty() {
                return Err(DaemonClientError::InvalidRoute(
                    "rule must be a non-empty string".into(),
                ));
            }
            normalized.push(rule.to_owned());
        }
        self.post(
            "/permissions",
            "POST /workspaces/:workspace/permissions",
            json!({"scope":"workspace","ruleType":rule_type,"rules":normalized}),
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn set_workspace_tool_enabled(
        &self,
        tool_name: &str,
        enabled: bool,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.post(
            &format!("/tools/{}/enable", encode_uri_component(tool_name)),
            "POST /workspaces/:workspace/tools/:name/enable",
            json!({"enabled":enabled}),
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn set_workspace_skill_enabled(
        &self,
        skill_name: &str,
        enabled: bool,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.post(
            &format!("/skills/{}/enable", encode_uri_component(skill_name)),
            "POST /workspaces/:workspace/skills/:name/enable",
            json!({"enabled":enabled}),
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn set_workspace_skills_enabled(
        &self,
        skill_names: &[String],
        enabled: bool,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.post(
            "/skills/enable",
            "POST /workspaces/:workspace/skills/enable",
            json!({"skillNames":skill_names,"enabled":enabled}),
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn restart_mcp_server(
        &self,
        server_name: &str,
        entry_index: Option<&str>,
        timeout: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query(&[("entryIndex", entry_index.map(str::to_owned))]);
        self.workspace_post(
            &format!("/mcp/{}/restart", encode_uri_component(server_name)),
            query,
            "POST /workspaces/:workspace/mcp/:server/restart",
            json!({}),
            false,
            Some(WorkspaceRequestOptions {
                client_id,
                timeout: timeout
                    .map(RequestTimeout::After)
                    .unwrap_or(RequestTimeout::After(MCP_RESTART_TIMEOUT)),
                ..Default::default()
            }),
        )
        .await
    }
    pub async fn reload(
        &self,
        client_id: Option<String>,
        timeout: Option<Duration>,
    ) -> Result<Value, DaemonClientError> {
        self.workspace_post(
            "/reload",
            String::new(),
            "POST /workspaces/:workspace/reload",
            json!({}),
            false,
            Some(WorkspaceRequestOptions {
                client_id,
                timeout: timeout
                    .map(RequestTimeout::After)
                    .unwrap_or(RequestTimeout::ClientDefault),
                ..Default::default()
            }),
        )
        .await
    }
    pub async fn init_workspace(
        &self,
        force: bool,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.post(
            "/init",
            "POST /workspaces/:workspace/init",
            if force {
                json!({"force":true})
            } else {
                json!({})
            },
            WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            },
        )
        .await
    }
    pub async fn workspace_extensions(&self) -> Result<Value, DaemonClientError> {
        self.workspace_get(
            "/extensions",
            String::new(),
            "GET /workspaces/:workspace/extensions",
            true,
            None,
        )
        .await
    }
    pub async fn set_extension_activation(
        &self,
        extension_id: &str,
        state: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let mut daemon_options = self.daemon_options(WorkspaceRequestOptions {
            client_id,
            ..Default::default()
        });
        daemon_options.mode = DaemonRequestMode::Rest;
        self.client
            .request_json(
                "PUT",
                &self.path(&format!(
                    "/extensions/{}/activation",
                    encode_uri_component(extension_id)
                )),
                "",
                Some(json!({"state":state})),
                "PUT /workspaces/:workspace/extensions/:extensionId/activation",
                daemon_options,
            )
            .await
    }
    pub async fn clear_extension_activation(
        &self,
        extension_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let mut daemon_options = self.daemon_options(WorkspaceRequestOptions {
            client_id,
            ..Default::default()
        });
        daemon_options.mode = DaemonRequestMode::Rest;
        self.client
            .request_json(
                "DELETE",
                &self.path(&format!(
                    "/extensions/{}/activation",
                    encode_uri_component(extension_id)
                )),
                "",
                None,
                "DELETE /workspaces/:workspace/extensions/:extensionId/activation",
                daemon_options,
            )
            .await
    }
    pub async fn refresh_extension_runtime(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.workspace_post(
            "/extensions/refresh",
            String::new(),
            "POST /workspaces/:workspace/extensions/refresh",
            json!({}),
            true,
            Some(WorkspaceRequestOptions {
                client_id,
                ..Default::default()
            }),
        )
        .await
    }
}

fn split_suffix_query(suffix: &str) -> (&str, String) {
    match suffix.split_once('?') {
        Some((path, query)) => (path, query.to_owned()),
        None => (suffix, String::new()),
    }
}

fn encode_query(pairs: &[(&str, Option<String>)]) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in pairs {
        if let Some(value) = value {
            serializer.append_pair(key, value);
        }
    }
    serializer.finish()
}

fn disposition_filename(value: &str) -> Option<String> {
    value.split(';').find_map(|parameter| {
        let (name, value) = parameter.trim().split_once('=')?;
        name.eq_ignore_ascii_case("filename")
            .then(|| value.trim().trim_matches('"').to_owned())
    })
}
