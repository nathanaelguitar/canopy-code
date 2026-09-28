//! Opt-in, best-effort PostHog events for the mobile MCP server.
//!
//! Calls only enqueue a small event in a bounded channel. One detached worker
//! performs all network I/O so telemetry cannot delay a synchronous MCP
//! response or spawn an unbounded number of threads.

use std::env;
use std::sync::OnceLock;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::Duration;

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

const POSTHOG_URL: &str = "https://us.i.posthog.com/i/v0/e/";
const POSTHOG_API_KEY: &str = "phc_KHRTZmkDsU7A8EbydEK8s4lJpPoTDyyBhSlwer694cS";
const QUEUE_CAPACITY: usize = 32;
const MAX_AGENT_NAME_BYTES: usize = 256;

struct TelemetryEvent {
    name: &'static str,
    properties: Map<String, Value>,
    agent_name: Option<String>,
}

static EVENT_SENDER: OnceLock<Option<SyncSender<TelemetryEvent>>> = OnceLock::new();

/// Record server creation, matching the TypeScript `launch` event.
pub fn launch() {
    track("launch", Map::new(), None);
}

/// Record a successful tool call without including its arguments or result.
pub fn tool_invoked(name: &str, duration_ms: u128, agent_name: Option<&str>) {
    let mut properties = Map::new();
    properties.insert(
        "ToolName".to_owned(),
        Value::String(bounded_tool_name(name)),
    );
    properties.insert(
        "Duration".to_owned(),
        Value::Number((duration_ms.min(u128::from(u64::MAX)) as u64).into()),
    );
    track("tool_invoked", properties, agent_name);
}

/// Record a failed tool call without sending its error text or arguments.
pub fn tool_failed(name: &str, agent_name: Option<&str>) {
    let mut properties = Map::new();
    properties.insert(
        "ToolName".to_owned(),
        Value::String(bounded_tool_name(name)),
    );
    track("tool_failed", properties, agent_name);
}

/// Record the selected robot platform and, when known, its device type.
pub fn robot_selected(platform: &str, device_type: Option<&str>, agent_name: Option<&str>) {
    let mut properties = Map::new();
    properties.insert(
        "DevicePlatform".to_owned(),
        Value::String(platform.to_owned()),
    );
    if let Some(device_type) = device_type {
        properties.insert(
            "DeviceType".to_owned(),
            Value::String(device_type.to_owned()),
        );
    }
    track("get_robot", properties, agent_name);
}

/// Record the image metadata attached to a successful screenshot response.
pub fn screenshot_taken(
    base64_size: usize,
    mime_type: &str,
    width: u32,
    height: u32,
    agent_name: Option<&str>,
) {
    let mut properties = Map::new();
    properties.insert(
        "ToolName".to_owned(),
        Value::String("mobile_take_screenshot".to_owned()),
    );
    properties.insert(
        "ScreenshotFilesize".to_owned(),
        Value::Number((base64_size as u64).into()),
    );
    properties.insert(
        "ScreenshotMimeType".to_owned(),
        Value::String(mime_type.to_owned()),
    );
    properties.insert("ScreenshotWidth".to_owned(), Value::Number(width.into()));
    properties.insert("ScreenshotHeight".to_owned(), Value::Number(height.into()));
    track("tool_invoked", properties, agent_name);
}

fn track(name: &'static str, properties: Map<String, Value>, agent_name: Option<&str>) {
    if !env::var_os("MOBILEMCP_ENABLE_TELEMETRY").is_some_and(|value| !value.is_empty()) {
        return;
    }
    let event = TelemetryEvent {
        name,
        properties,
        agent_name: agent_name.and_then(bounded_agent_name),
    };
    if let Some(sender) = event_sender() {
        // Never wait for network or queue capacity on the protocol thread. If
        // delivery is behind, dropping an event is preferable to delaying a
        // tool response or accumulating memory without bound.
        let _ = sender.try_send(event);
    }
}

fn event_sender() -> Option<&'static SyncSender<TelemetryEvent>> {
    EVENT_SENDER
        .get_or_init(|| {
            let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
            thread::Builder::new()
                .name("mobile-mcp-telemetry".to_owned())
                .spawn(move || deliver_events(receiver))
                .ok()?;
            Some(sender)
        })
        .as_ref()
}

fn deliver_events(receiver: Receiver<TelemetryEvent>) {
    let client = match reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(4))
        .build()
    {
        Ok(client) => client,
        Err(_) => return,
    };
    let distinct_id = anonymous_identity();

    while let Ok(event) = receiver.recv() {
        let mut properties = Map::new();
        properties.insert("Platform".to_owned(), Value::String(platform_name()));
        properties.insert("Product".to_owned(), Value::String("mobile-mcp".to_owned()));
        properties.insert(
            "Version".to_owned(),
            Value::String(crate::SERVER_VERSION.to_owned()),
        );
        properties.insert("CI".to_owned(), Value::String(ci_value()));
        if let Some(agent_name) = event.agent_name.filter(|name| !name.is_empty()) {
            properties.insert("AgentName".to_owned(), Value::String(agent_name));
        }
        properties.extend(event.properties);

        let payload = json!({
            "api_key": POSTHOG_API_KEY,
            "event": event.name,
            "properties": properties,
            "distinct_id": distinct_id,
        });
        // The response is intentionally ignored. Errors and rejected requests
        // stay off stderr to preserve mobile-mcp's normal protocol logging.
        let _ = client.post(POSTHOG_URL).json(&payload).send();
    }
}

fn bounded_agent_name(name: &str) -> Option<String> {
    if name.is_empty() {
        return None;
    }
    let mut end = name.len().min(MAX_AGENT_NAME_BYTES);
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    Some(name[..end].to_owned())
}

fn bounded_tool_name(name: &str) -> String {
    // Registered MCP tool names are short constants. Bound unknown client
    // input too, so even a malformed request cannot occupy a large queue slot.
    let mut end = name.len().min(128);
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    name[..end].to_owned()
}

fn platform_name() -> String {
    match env::consts::OS {
        "macos" => "darwin".to_owned(),
        "windows" => "win32".to_owned(),
        platform => platform.to_owned(),
    }
}

fn ci_value() -> String {
    env::var("CI")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "0".to_owned())
}

fn anonymous_identity() -> String {
    let hostname = hostname();
    let executable = env::current_exe()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_default();
    let digest = Sha256::digest(format!("{hostname}{executable}").as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

#[cfg(unix)]
fn hostname() -> String {
    let mut buffer = [0_u8; 256];
    let status = unsafe { libc::gethostname(buffer.as_mut_ptr().cast(), buffer.len()) };
    if status == 0 {
        let end = buffer
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(buffer.len());
        return String::from_utf8_lossy(&buffer[..end]).into_owned();
    }
    env::var("HOSTNAME").unwrap_or_default()
}

#[cfg(windows)]
fn hostname() -> String {
    env::var("COMPUTERNAME")
        .or_else(|_| env::var("HOSTNAME"))
        .unwrap_or_default()
}

#[cfg(not(any(unix, windows)))]
fn hostname() -> String {
    env::var("HOSTNAME").unwrap_or_default()
}
