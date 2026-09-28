//! Lifecycle registry for long-running monitor tasks.
//!
//! This ports the in-memory state and notification contract from
//! `packages/core/src/services/monitorRegistry.ts`. Process creation and output
//! draining remain the responsibility of the Monitor tool integration.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use indexmap::IndexMap;

use crate::utils::cancellation::CancellationToken;
use crate::utils::terminal_safe::{strip_display_control_chars, truncate_notification_label};
use crate::utils::xml::escape_xml;

pub const MAX_CONCURRENT_MONITORS: usize = 16;
pub const MAX_RETAINED_TERMINAL_MONITORS: usize = 128;
pub const EVENT_LINE_TRUNCATE_UTF16_UNITS: usize = 2_000;

/// Lifecycle state for one monitor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MonitorStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl MonitorStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    const fn is_terminal(self) -> bool {
        !matches!(self, Self::Running)
    }
}

/// Mutable monitor task record. Registry lookups and callbacks share the same
/// `Arc<Mutex<_>>`, matching the object identity of the TypeScript registry.
#[derive(Clone)]
pub struct MonitorTask {
    pub id: String,
    pub kind: &'static str,
    pub description: String,
    pub monitor_id: String,
    pub command: String,
    pub status: MonitorStatus,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub output_file: String,
    pub output_offset: u64,
    pub notified: bool,
    pub todo_work_chain_id: Option<String>,
    pub abort_controller: CancellationToken,
    pub pid: Option<u32>,
    pub tool_use_id: Option<String>,
    pub owner_agent_id: Option<String>,
    pub event_count: u64,
    pub last_event_time: i64,
    pub max_events: u64,
    pub idle_timeout_ms: u64,
    pub dropped_lines: u64,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    idle_timer: Arc<IdleTimerControl>,
}

/// Caller-owned fields for [`MonitorRegistry::register`].
pub struct MonitorTaskRegistration {
    pub monitor_id: String,
    pub command: String,
    pub description: String,
    pub status: MonitorStatus,
    pub start_time: i64,
    pub output_file: String,
    pub abort_controller: CancellationToken,
    pub event_count: u64,
    pub last_event_time: i64,
    pub max_events: u64,
    pub idle_timeout_ms: u64,
    pub dropped_lines: u64,
    pub end_time: Option<i64>,
    pub pid: Option<u32>,
    pub tool_use_id: Option<String>,
    pub owner_agent_id: Option<String>,
    pub todo_work_chain_id: Option<String>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

/// Compatibility alias retained by the source registry.
pub type MonitorEntry = MonitorTask;
pub type SharedMonitorTask = Arc<Mutex<MonitorTask>>;

/// Error returned when the active-monitor limit is reached.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MonitorRegistryError {
    MaximumConcurrentMonitorsReached { limit: usize },
}

impl std::fmt::Display for MonitorRegistryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaximumConcurrentMonitorsReached { limit } => write!(
                formatter,
                "Cannot start monitor: maximum concurrent monitors ({limit}) reached. Stop an existing monitor first."
            ),
        }
    }
}

impl std::error::Error for MonitorRegistryError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MonitorNotificationMeta {
    pub monitor_id: String,
    pub status: MonitorStatus,
    pub event_count: u64,
    pub tool_use_id: Option<String>,
    pub owner_agent_id: Option<String>,
    pub todo_work_chain_id: Option<String>,
}

pub type MonitorNotificationCallback =
    Arc<dyn Fn(String, String, MonitorNotificationMeta) + Send + Sync + 'static>;
pub type MonitorOwnerLifecycleCallback = Arc<dyn Fn() + Send + Sync + 'static>;
pub type MonitorRegisterCallback = Arc<dyn Fn(SharedMonitorTask) + Send + Sync + 'static>;
pub type MonitorStatusChangeCallback =
    Arc<dyn Fn(Option<SharedMonitorTask>) + Send + Sync + 'static>;

/// Whether cancellation should send a terminal notification. Omitted and
/// `Some(true)` both select the user-visible cancellation path.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MonitorCancelOptions {
    pub notify: Option<bool>,
}

impl MonitorCancelOptions {
    fn notify(self) -> bool {
        self.notify != Some(false)
    }
}

struct RegistryState {
    monitors: IndexMap<String, SharedMonitorTask>,
    agent_notification_callbacks: std::collections::HashMap<String, MonitorNotificationCallback>,
    agent_lifecycle_callbacks: std::collections::HashMap<String, MonitorOwnerLifecycleCallback>,
    notification_callback: Option<MonitorNotificationCallback>,
    register_callback: Option<MonitorRegisterCallback>,
    status_change_callback: Option<MonitorStatusChangeCallback>,
}

struct MonitorRegistryInner {
    state: Mutex<RegistryState>,
}

/// Thread-safe registry. Callback invocation always happens outside registry
/// locks so callbacks can safely inspect or mutate the registry.
#[derive(Clone)]
pub struct MonitorRegistry {
    inner: Arc<MonitorRegistryInner>,
}

impl Default for MonitorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MonitorRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MonitorRegistryInner {
                state: Mutex::new(RegistryState {
                    monitors: IndexMap::new(),
                    agent_notification_callbacks: std::collections::HashMap::new(),
                    agent_lifecycle_callbacks: std::collections::HashMap::new(),
                    notification_callback: None,
                    register_callback: None,
                    status_change_callback: None,
                }),
            }),
        }
    }

    pub fn register(
        &self,
        registration: MonitorTaskRegistration,
    ) -> Result<SharedMonitorTask, MonitorRegistryError> {
        let timer = Arc::new(IdleTimerControl::default());
        timer.schedule(registration.idle_timeout_ms);
        let monitor_id = registration.monitor_id.clone();
        let task = Arc::new(Mutex::new(MonitorTask {
            id: monitor_id.clone(),
            kind: "monitor",
            description: registration.description,
            monitor_id: monitor_id.clone(),
            command: registration.command,
            status: registration.status,
            start_time: registration.start_time,
            end_time: registration.end_time,
            output_file: registration.output_file,
            output_offset: 0,
            notified: false,
            todo_work_chain_id: registration.todo_work_chain_id,
            abort_controller: registration.abort_controller,
            pid: registration.pid,
            tool_use_id: registration.tool_use_id,
            owner_agent_id: registration.owner_agent_id,
            event_count: registration.event_count,
            last_event_time: registration.last_event_time,
            max_events: registration.max_events,
            idle_timeout_ms: registration.idle_timeout_ms,
            dropped_lines: registration.dropped_lines,
            exit_code: registration.exit_code,
            error: registration.error,
            idle_timer: Arc::clone(&timer),
        }));

        let (register_callback, status_callback) = {
            let mut state = lock(&self.inner.state);
            let running_count = state
                .monitors
                .values()
                .filter(|entry| lock(entry).status == MonitorStatus::Running)
                .count();
            if running_count >= MAX_CONCURRENT_MONITORS {
                timer.stop();
                return Err(MonitorRegistryError::MaximumConcurrentMonitorsReached {
                    limit: MAX_CONCURRENT_MONITORS,
                });
            }

            if let Some(previous) = state.monitors.insert(monitor_id.clone(), Arc::clone(&task)) {
                lock(&previous).idle_timer.stop();
            }
            let owner_agent_id = lock(&task).owner_agent_id.clone();
            (
                owner_agent_id
                    .is_none()
                    .then(|| state.register_callback.clone())
                    .flatten(),
                state.status_change_callback.clone(),
            )
        };

        if let Some(callback) = register_callback {
            let task_for_callback = Arc::clone(&task);
            let _ = catch_unwind(AssertUnwindSafe(|| callback(task_for_callback)));
        }
        fire_status_change(status_callback, Some(Arc::clone(&task)));

        // Start after synchronous registration callbacks, matching the source
        // timer's event-loop behavior for zero-length idle timeouts.
        start_idle_timer_worker(Arc::downgrade(&self.inner), monitor_id, timer);
        Ok(task)
    }

    pub fn get(&self, monitor_id: &str) -> Option<SharedMonitorTask> {
        lock(&self.inner.state).monitors.get(monitor_id).cloned()
    }

    /// Return entries in insertion order, like JavaScript `Map.values()`.
    pub fn get_all(&self) -> Vec<SharedMonitorTask> {
        lock(&self.inner.state).monitors.values().cloned().collect()
    }

    pub fn get_running(&self) -> Vec<SharedMonitorTask> {
        lock(&self.inner.state)
            .monitors
            .values()
            .filter(|entry| lock(entry).status == MonitorStatus::Running)
            .cloned()
            .collect()
    }

    pub fn has_running_for_owner(&self, owner_agent_id: &str) -> bool {
        lock(&self.inner.state).monitors.values().any(|entry| {
            let task = lock(entry);
            task.owner_agent_id.as_deref() == Some(owner_agent_id)
                && task.status == MonitorStatus::Running
        })
    }

    pub fn set_notification_callback(&self, callback: Option<MonitorNotificationCallback>) {
        lock(&self.inner.state).notification_callback = callback;
    }

    pub fn set_agent_notification_callback(
        &self,
        agent_id: impl Into<String>,
        callback: Option<MonitorNotificationCallback>,
    ) {
        let mut state = lock(&self.inner.state);
        let agent_id = agent_id.into();
        if let Some(callback) = callback {
            state
                .agent_notification_callbacks
                .insert(agent_id, callback);
        } else {
            state.agent_notification_callbacks.remove(&agent_id);
        }
    }

    pub fn set_agent_lifecycle_callback(
        &self,
        agent_id: impl Into<String>,
        callback: Option<MonitorOwnerLifecycleCallback>,
    ) {
        let mut state = lock(&self.inner.state);
        let agent_id = agent_id.into();
        if let Some(callback) = callback {
            state.agent_lifecycle_callbacks.insert(agent_id, callback);
        } else {
            state.agent_lifecycle_callbacks.remove(&agent_id);
        }
    }

    pub fn set_register_callback(&self, callback: Option<MonitorRegisterCallback>) {
        lock(&self.inner.state).register_callback = callback;
    }

    pub fn set_status_change_callback(&self, callback: Option<MonitorStatusChangeCallback>) {
        lock(&self.inner.state).status_change_callback = callback;
    }

    /// Record one monitor output line and auto-complete after `max_events`.
    /// Per-event counter changes do not fire the status-change callback.
    pub fn emit_event(&self, monitor_id: &str, line: &str) {
        let snapshot = {
            let state = lock(&self.inner.state);
            let Some(task) = state.monitors.get(monitor_id) else {
                return;
            };
            let mut entry = lock(task);
            if entry.status != MonitorStatus::Running {
                return;
            }
            entry.event_count = entry.event_count.saturating_add(1);
            entry.last_event_time = current_time_millis();
            entry.idle_timer.schedule(entry.idle_timeout_ms);
            entry.clone()
        };

        let event_line = truncate_event_line(line);
        let (display_text, model_text, meta) = stream_notification(&snapshot, &event_line);
        self.dispatch_notification(&snapshot, display_text, model_text, meta);

        if snapshot.event_count >= snapshot.max_events {
            let settled = settle_entry(
                &self.inner,
                monitor_id,
                MonitorStatus::Completed,
                None,
                |entry| entry.error = Some("Max events reached".to_owned()),
            );
            if let Some((task, terminal)) = settled {
                terminal.abort_controller.cancel();
                self.emit_terminal_notification(&task, Some("Max events reached"));
            }
        }
    }

    pub fn complete(&self, monitor_id: &str, exit_code: Option<i32>) {
        let settled = settle_entry(
            &self.inner,
            monitor_id,
            MonitorStatus::Completed,
            None,
            |entry| {
                if let Some(exit_code) = exit_code {
                    entry.exit_code = Some(exit_code);
                }
            },
        );
        if let Some((task, _)) = settled {
            let detail = exit_code.map(|code| format!("Exited with code {code}"));
            self.emit_terminal_notification(&task, detail.as_deref());
        }
    }

    pub fn fail(&self, monitor_id: &str, error: impl Into<String>) {
        let error = error.into();
        let settled = settle_entry(
            &self.inner,
            monitor_id,
            MonitorStatus::Failed,
            None,
            |entry| entry.error = Some(error.clone()),
        );
        if let Some((task, _)) = settled {
            self.emit_terminal_notification(&task, Some(&error));
        }
    }

    /// Cancel a running monitor. Silent cancellation settles first, suppresses
    /// the terminal notification, and wakes its owner lifecycle callback.
    pub fn cancel(&self, monitor_id: &str) {
        self.cancel_with_options(monitor_id, MonitorCancelOptions::default());
    }

    pub fn cancel_with_options(&self, monitor_id: &str, options: MonitorCancelOptions) {
        let abort_controller = {
            let state = lock(&self.inner.state);
            state.monitors.get(monitor_id).and_then(|task| {
                let entry = lock(task);
                (entry.status == MonitorStatus::Running).then(|| entry.abort_controller.clone())
            })
        };
        let Some(abort_controller) = abort_controller else {
            return;
        };

        if !options.notify() {
            let Some((task, terminal)) = settle_entry(
                &self.inner,
                monitor_id,
                MonitorStatus::Cancelled,
                None,
                |_| {},
            ) else {
                return;
            };
            lock(&task).notified = true;
            terminal.abort_controller.cancel();
            self.dispatch_owner_lifecycle_wake(&terminal);
            return;
        }

        // Cancellation wakes any process/task listener before we force the
        // registry status, allowing a concurrent natural completion to win.
        abort_controller.cancel();
        if let Some((task, _)) = settle_entry(
            &self.inner,
            monitor_id,
            MonitorStatus::Cancelled,
            None,
            |_| {},
        ) {
            self.emit_terminal_notification(&task, None);
        }
    }

    pub fn abort_all(&self) {
        self.abort_all_with_options(MonitorCancelOptions::default());
    }

    pub fn abort_all_with_options(&self, options: MonitorCancelOptions) {
        let ids = lock(&self.inner.state)
            .monitors
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for monitor_id in ids {
            self.cancel_with_options(&monitor_id, options);
        }
    }

    pub fn cancel_running_for_owner(&self, owner_agent_id: &str) {
        self.cancel_running_for_owner_with_options(owner_agent_id, MonitorCancelOptions::default());
    }

    pub fn cancel_running_for_owner_with_options(
        &self,
        owner_agent_id: &str,
        options: MonitorCancelOptions,
    ) {
        let ids = lock(&self.inner.state)
            .monitors
            .values()
            .filter_map(|entry| {
                let entry = lock(entry);
                (entry.owner_agent_id.as_deref() == Some(owner_agent_id)
                    && entry.status == MonitorStatus::Running)
                    .then(|| entry.monitor_id.clone())
            })
            .collect::<Vec<_>>();
        for monitor_id in ids {
            self.cancel_with_options(&monitor_id, options);
        }
    }

    /// Clear owner callbacks and all entries, cancelling active tokens and
    /// stopping every idle timer. A nonempty reset fires one `None` status
    /// change after the map is cleared.
    pub fn reset(&self) {
        let entries = {
            let mut state = lock(&self.inner.state);
            state.agent_notification_callbacks.clear();
            state.agent_lifecycle_callbacks.clear();
            if state.monitors.is_empty() {
                return;
            }
            let entries = state.monitors.values().cloned().collect::<Vec<_>>();
            for entry in &entries {
                lock(entry).idle_timer.stop();
            }
            entries
        };

        for entry in &entries {
            let abort_controller = {
                let task = lock(entry);
                (task.status == MonitorStatus::Running).then(|| task.abort_controller.clone())
            };
            if let Some(abort_controller) = abort_controller {
                abort_controller.cancel();
            }
        }

        let status_callback = {
            let mut state = lock(&self.inner.state);
            state.monitors.clear();
            state.status_change_callback.clone()
        };
        fire_status_change(status_callback, None);
    }

    fn emit_terminal_notification(&self, task: &SharedMonitorTask, detail: Option<&str>) {
        let snapshot = {
            let mut task = lock(task);
            if task.notified {
                return;
            }
            task.notified = true;
            task.clone()
        };
        let (display_text, model_text, meta) = terminal_notification(&snapshot, detail);
        self.dispatch_notification(&snapshot, display_text, model_text, meta);
    }

    fn dispatch_notification(
        &self,
        task: &MonitorTask,
        display_text: String,
        model_text: String,
        meta: MonitorNotificationMeta,
    ) {
        let callback = {
            let state = lock(&self.inner.state);
            if let Some(owner_agent_id) = task.owner_agent_id.as_deref() {
                state
                    .agent_notification_callbacks
                    .get(owner_agent_id)
                    .cloned()
            } else {
                state.notification_callback.clone()
            }
        };
        let Some(callback) = callback else {
            if let Some(owner_agent_id) = task.owner_agent_id.as_deref() {
                eprintln!(
                    "Dropping monitor notification for {}: owner agent {} has no notification callback",
                    task.monitor_id, owner_agent_id
                );
            }
            return;
        };
        let _ = catch_unwind(AssertUnwindSafe(|| {
            callback(display_text, model_text, meta)
        }));
    }

    fn dispatch_owner_lifecycle_wake(&self, task: &MonitorTask) {
        let Some(owner_agent_id) = task.owner_agent_id.as_deref() else {
            return;
        };
        let callback = lock(&self.inner.state)
            .agent_lifecycle_callbacks
            .get(owner_agent_id)
            .cloned();
        if let Some(callback) = callback {
            let _ = catch_unwind(AssertUnwindSafe(|| callback()));
        }
    }
}

/// Resolve the reserved monitor output file path.
pub fn get_monitor_output_path(
    project_dir: impl AsRef<Path>,
    session_id: &str,
    monitor_id: &str,
) -> PathBuf {
    let path = project_dir
        .as_ref()
        .join("monitors")
        .join(sanitize_filename_component(session_id))
        .join(format!(
            "monitor-{}.log",
            sanitize_filename_component(monitor_id)
        ));
    normalize_path(&path)
}

pub fn sanitize_filename_component(value: &str) -> String {
    value
        .encode_utf16()
        .map(|unit| {
            if unit <= 0x7f
                && ((unit as u8).is_ascii_alphanumeric()
                    || unit == b'_' as u16
                    || unit == b'-' as u16)
            {
                char::from_u32(u32::from(unit)).unwrap_or('_')
            } else {
                '_'
            }
        })
        .collect()
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() && !normalized.has_root() {
                    normalized.push(component.as_os_str());
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn settle_entry(
    inner: &Arc<MonitorRegistryInner>,
    monitor_id: &str,
    status: MonitorStatus,
    idle_guard: Option<(&Arc<IdleTimerControl>, u64)>,
    update: impl FnOnce(&mut MonitorTask),
) -> Option<(SharedMonitorTask, MonitorTask)> {
    let (task, snapshot, status_callback) = {
        let mut state = lock(&inner.state);
        let task = state.monitors.get(monitor_id)?.clone();
        let snapshot = {
            let mut entry = lock(&task);
            if entry.status != MonitorStatus::Running {
                return None;
            }
            if let Some((timer, generation)) = idle_guard {
                if !Arc::ptr_eq(&entry.idle_timer, timer)
                    || entry.idle_timer.generation() != generation
                {
                    return None;
                }
            }
            update(&mut entry);
            entry.status = status;
            entry.end_time = Some(current_time_millis());
            entry.idle_timer.stop();
            entry.clone()
        };
        prune_terminal_entries(&mut state);
        (task, snapshot, state.status_change_callback.clone())
    };
    fire_status_change(status_callback, Some(Arc::clone(&task)));
    Some((task, snapshot))
}

fn prune_terminal_entries(state: &mut RegistryState) {
    let mut terminal = state
        .monitors
        .iter()
        .enumerate()
        .filter_map(|(order, (monitor_id, task))| {
            let task = lock(task);
            task.status.is_terminal().then(|| {
                (
                    task.end_time.unwrap_or(task.start_time),
                    task.start_time,
                    order,
                    monitor_id.clone(),
                )
            })
        })
        .collect::<Vec<_>>();
    terminal.sort_by_key(|(end_time, start_time, order, _)| (*end_time, *start_time, *order));
    let excess = terminal
        .len()
        .saturating_sub(MAX_RETAINED_TERMINAL_MONITORS);
    for (_, _, _, monitor_id) in terminal.into_iter().take(excess) {
        state.monitors.shift_remove(&monitor_id);
    }
}

fn fire_status_change(
    callback: Option<MonitorStatusChangeCallback>,
    task: Option<SharedMonitorTask>,
) {
    if let Some(callback) = callback {
        let _ = catch_unwind(AssertUnwindSafe(|| callback(task)));
    }
}

fn stream_notification(
    task: &MonitorTask,
    event_line: &str,
) -> (String, String, MonitorNotificationMeta) {
    let description = truncate_notification_label(&task.description);
    let safe_event_line = strip_display_control_chars(event_line);
    let display_text = format!(
        "Monitor \"{description}\" event #{}: {safe_event_line}",
        task.event_count
    );
    let mut xml_parts = vec![
        "<task-notification>".to_owned(),
        format!("<task-id>{}</task-id>", escape_xml(&task.monitor_id)),
    ];
    if let Some(tool_use_id) = task.tool_use_id.as_deref() {
        xml_parts.push(format!(
            "<tool-use-id>{}</tool-use-id>",
            escape_xml(tool_use_id)
        ));
    }
    xml_parts.extend([
        "<kind>monitor</kind>".to_owned(),
        "<status>running</status>".to_owned(),
        format!("<event-count>{}</event-count>", task.event_count),
        format!(
            "<summary>Monitor \"{}\" emitted event #{}.</summary>",
            escape_xml(&description),
            task.event_count
        ),
        format!("<result>{}</result>", escape_xml(&safe_event_line)),
        "</task-notification>".to_owned(),
    ]);
    let meta = notification_meta(task);
    (display_text, xml_parts.join("\n"), meta)
}

fn terminal_notification(
    task: &MonitorTask,
    detail: Option<&str>,
) -> (String, String, MonitorNotificationMeta) {
    let status_text = match task.status {
        MonitorStatus::Completed => "completed",
        MonitorStatus::Failed => "failed",
        MonitorStatus::Cancelled => "was cancelled",
        MonitorStatus::Running => return (String::new(), String::new(), notification_meta(task)),
    };
    let description = truncate_notification_label(&task.description);
    let dropped_suffix = if task.dropped_lines > 0 {
        format!(", {} lines dropped due to throttling", task.dropped_lines)
    } else {
        String::new()
    };
    let display_text = format!(
        "Monitor \"{description}\" {status_text}. ({} events{dropped_suffix})",
        task.event_count
    );
    let dropped_summary = if task.dropped_lines > 0 {
        format!(" {} lines dropped due to throttling.", task.dropped_lines)
    } else {
        String::new()
    };
    let mut xml_parts = vec![
        "<task-notification>".to_owned(),
        format!("<task-id>{}</task-id>", escape_xml(&task.monitor_id)),
    ];
    if let Some(tool_use_id) = task.tool_use_id.as_deref() {
        xml_parts.push(format!(
            "<tool-use-id>{}</tool-use-id>",
            escape_xml(tool_use_id)
        ));
    }
    xml_parts.extend([
        "<kind>monitor</kind>".to_owned(),
        format!("<status>{}</status>", escape_xml(task.status.as_str())),
        format!("<event-count>{}</event-count>", task.event_count),
        format!(
            "<summary>Monitor \"{}\" {status_text}. Total events: {}.{dropped_summary}</summary>",
            escape_xml(&description),
            task.event_count
        ),
        format!(
            "<command>{}</command>",
            escape_xml(&strip_display_control_chars(&task.command))
        ),
    ]);
    if let Some(detail) = detail.filter(|detail| !detail.is_empty()) {
        xml_parts.push(format!(
            "<result>{}</result>",
            escape_xml(&strip_display_control_chars(detail))
        ));
    }
    xml_parts.push("</task-notification>".to_owned());
    (display_text, xml_parts.join("\n"), notification_meta(task))
}

fn notification_meta(task: &MonitorTask) -> MonitorNotificationMeta {
    MonitorNotificationMeta {
        monitor_id: task.monitor_id.clone(),
        status: task.status,
        event_count: task.event_count,
        tool_use_id: task.tool_use_id.clone(),
        owner_agent_id: task.owner_agent_id.clone(),
        todo_work_chain_id: task.todo_work_chain_id.clone(),
    }
}

fn truncate_event_line(line: &str) -> String {
    let mut utf16 = line.encode_utf16();
    let prefix = utf16
        .by_ref()
        .take(EVENT_LINE_TRUNCATE_UTF16_UNITS)
        .collect::<Vec<_>>();
    if utf16.next().is_none() {
        return line.to_owned();
    }
    format!("{}...[truncated]", String::from_utf16_lossy(&prefix))
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn start_idle_timer_worker(
    registry: Weak<MonitorRegistryInner>,
    monitor_id: String,
    timer: Arc<IdleTimerControl>,
) {
    let _ = std::thread::Builder::new()
        .name(format!(
            "monitor-idle-{}",
            sanitize_filename_component(&monitor_id)
        ))
        .spawn(move || {
            while let Some(generation) = timer.wait_for_expiry() {
                let Some(inner) = registry.upgrade() else {
                    return;
                };
                let abort_controller = {
                    let state = lock(&inner.state);
                    let Some(task) = state.monitors.get(&monitor_id) else {
                        return;
                    };
                    let task = lock(task);
                    if task.status != MonitorStatus::Running {
                        timer.stop();
                        return;
                    }
                    if !Arc::ptr_eq(&task.idle_timer, &timer) {
                        timer.stop();
                        return;
                    }
                    if timer.generation() != generation {
                        continue;
                    }
                    task.abort_controller.clone()
                };

                // Give asynchronous cancellation listeners a chance to flush
                // their last buffered line before settling, like AbortSignal.
                abort_controller.cancel();
                let Some((task, _)) = settle_entry(
                    &inner,
                    &monitor_id,
                    MonitorStatus::Completed,
                    Some((&timer, generation)),
                    |entry| entry.error = Some("Idle timeout".to_owned()),
                ) else {
                    continue;
                };
                let registry = MonitorRegistry { inner };
                registry.emit_terminal_notification(&task, Some("Idle timeout"));
                return;
            }
        });
}

#[derive(Default)]
struct IdleTimerControl {
    state: Mutex<IdleTimerState>,
    changed: Condvar,
}

#[derive(Default)]
struct IdleTimerState {
    generation: u64,
    deadline: Option<Instant>,
    stopped: bool,
}

impl IdleTimerControl {
    fn schedule(&self, timeout_ms: u64) -> u64 {
        let mut state = lock(&self.state);
        state.generation = state.generation.wrapping_add(1);
        state.deadline = Some(
            Instant::now()
                .checked_add(Duration::from_millis(timeout_ms))
                .unwrap_or_else(Instant::now),
        );
        self.changed.notify_all();
        state.generation
    }

    fn stop(&self) {
        let mut state = lock(&self.state);
        state.generation = state.generation.wrapping_add(1);
        state.deadline = None;
        state.stopped = true;
        self.changed.notify_all();
    }

    fn generation(&self) -> u64 {
        lock(&self.state).generation
    }

    fn wait_for_expiry(&self) -> Option<u64> {
        let mut state = lock(&self.state);
        loop {
            if state.stopped {
                return None;
            }
            let Some(deadline) = state.deadline else {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                continue;
            };
            let generation = state.generation;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                state.deadline = None;
                return Some(generation);
            }
            let (next_state, timeout) = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next_state;
            if timeout.timed_out() && state.generation == generation && !state.stopped {
                state.deadline = None;
                return Some(generation);
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
