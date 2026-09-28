//! Registry for managed background shell processes.
//!
//! Source: `packages/core/src/services/backgroundShellRegistry.ts`. Entries
//! transition once from `running` to a terminal state, retain at most 32
//! terminal rows, persist an owner-only status sidecar, and emit bounded,
//! terminal-safe completion notifications.

use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, SecondsFormat, Utc};
use indexmap::IndexMap;
use serde_json::{Value, json};

use crate::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};
use crate::utils::cancellation::CancellationToken;
use crate::utils::terminal_safe::{
    is_bidi_control_char, strip_display_control_chars, truncate_notification_label,
};
use crate::utils::xml::escape_xml;

pub const MAX_RETAINED_TERMINAL_SHELLS: usize = 32;
pub const MAX_NOTIFICATION_OUTPUT_TAIL_BYTES: usize = 8192;
const MAX_NOTIFICATION_MODEL_COMMAND_LENGTH: usize = 500;

/// Lifecycle state for one managed background shell.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackgroundShellStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl BackgroundShellStatus {
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

/// Shared task envelope and shell-specific metadata.
///
/// `shell_id`/`output_path` are retained as compatibility aliases for
/// `id`/`output_file`. The entry is shared with callbacks and registry
/// lookups, matching the source implementation's mutable object identity.
#[derive(Clone)]
pub struct ShellTask {
    pub id: String,
    pub kind: &'static str,
    pub description: String,
    pub shell_id: String,
    pub command: String,
    pub cwd: String,
    pub pid: Option<u32>,
    pub status: BackgroundShellStatus,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub output_file: String,
    pub output_path: String,
    pub output_offset: u64,
    pub notified: bool,
    pub todo_work_chain_id: Option<String>,
    pub abort_controller: CancellationToken,
}

/// Caller-provided registration fields. Registry-owned envelope values
/// (`id`, `kind`, `description`, output aliases, offset and notification bit)
/// are derived by [`BackgroundShellRegistry::register`].
pub struct ShellTaskRegistration {
    pub shell_id: String,
    pub command: String,
    pub cwd: String,
    pub pid: Option<u32>,
    pub status: BackgroundShellStatus,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub output_path: String,
    pub todo_work_chain_id: Option<String>,
    pub abort_controller: CancellationToken,
}

pub type SharedShellTask = Arc<Mutex<ShellTask>>;
pub type BackgroundShellRegisterCallback = Arc<dyn Fn(SharedShellTask) + Send + Sync>;
pub type BackgroundShellStatusChangeCallback = Arc<dyn Fn(Option<SharedShellTask>) + Send + Sync>;
pub type BackgroundShellNotificationCallback =
    Arc<dyn Fn(String, String, ShellNotificationMeta) + Send + Sync>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShellNotificationMeta {
    pub shell_id: String,
    pub status: BackgroundShellStatus,
    pub exit_code: Option<i32>,
    pub todo_work_chain_id: Option<String>,
}

/// Convert an output path to the adjacent status sidecar path.
pub fn status_file_path_for(output_file: &str) -> String {
    output_file
        .strip_suffix(".output")
        .map(|prefix| format!("{prefix}.status"))
        .unwrap_or_else(|| format!("{output_file}.status"))
}

/// Mutable registry. Methods take `&mut self` to serialize map operations;
/// entry handles remain shareable and can be inspected by UI callbacks.
pub struct BackgroundShellRegistry {
    entries: IndexMap<String, SharedShellTask>,
    register_callback: Option<BackgroundShellRegisterCallback>,
    notification_callback: Option<BackgroundShellNotificationCallback>,
    status_change_callback: Option<BackgroundShellStatusChangeCallback>,
}

impl Default for BackgroundShellRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl BackgroundShellRegistry {
    pub fn new() -> Self {
        Self {
            entries: IndexMap::new(),
            register_callback: None,
            notification_callback: None,
            status_change_callback: None,
        }
    }

    pub fn set_register_callback(&mut self, callback: Option<BackgroundShellRegisterCallback>) {
        self.register_callback = callback;
    }

    pub fn set_notification_callback(
        &mut self,
        callback: Option<BackgroundShellNotificationCallback>,
    ) {
        self.notification_callback = callback;
    }

    pub fn set_status_change_callback(
        &mut self,
        callback: Option<BackgroundShellStatusChangeCallback>,
    ) {
        self.status_change_callback = callback;
    }

    /// Clear the status subscriber only if it is the currently installed
    /// callback, preserving a newer subscriber installed by another owner.
    pub fn clear_status_change_callback(&mut self, callback: &BackgroundShellStatusChangeCallback) {
        if self
            .status_change_callback
            .as_ref()
            .is_some_and(|installed| Arc::ptr_eq(installed, callback))
        {
            self.status_change_callback = None;
        }
    }

    pub fn register(&mut self, registration: ShellTaskRegistration) -> SharedShellTask {
        let shell_id = registration.shell_id;
        let output_path = registration.output_path;
        let entry = Arc::new(Mutex::new(ShellTask {
            id: shell_id.clone(),
            kind: "shell",
            description: registration.command.clone(),
            shell_id: shell_id.clone(),
            command: registration.command,
            cwd: registration.cwd,
            pid: registration.pid,
            status: registration.status,
            start_time: registration.start_time,
            end_time: registration.end_time,
            exit_code: registration.exit_code,
            error: registration.error,
            output_file: output_path.clone(),
            output_path,
            output_offset: 0,
            notified: false,
            todo_work_chain_id: registration.todo_work_chain_id,
            abort_controller: registration.abort_controller,
        }));

        self.entries.insert(shell_id, Arc::clone(&entry));
        write_status_file(&entry);
        self.fire_register(Arc::clone(&entry));
        self.fire_status_change(Some(Arc::clone(&entry)));
        entry
    }

    pub fn get(&self, shell_id: &str) -> Option<SharedShellTask> {
        self.entries.get(shell_id).cloned()
    }

    /// Return handles in insertion order, as JavaScript `Map.values()` does.
    pub fn get_all(&self) -> Vec<SharedShellTask> {
        self.entries.values().cloned().collect()
    }

    pub fn has_running_entries(&self) -> bool {
        self.entries
            .values()
            .any(|entry| lock(entry).status == BackgroundShellStatus::Running)
    }

    pub fn complete(&mut self, shell_id: &str, exit_code: i32, end_time: i64) {
        let Some(entry) = self.entries.get(shell_id).cloned() else {
            return;
        };
        {
            let mut task = lock(&entry);
            if task.status != BackgroundShellStatus::Running {
                return;
            }
            task.status = BackgroundShellStatus::Completed;
            task.exit_code = Some(exit_code);
            task.end_time = Some(end_time);
        }
        write_status_file(&entry);
        self.emit_notification(&entry);
        self.prune_terminal_entries();
        self.fire_status_change(Some(entry));
    }

    pub fn fail(&mut self, shell_id: &str, error: impl Into<String>, end_time: i64) {
        let Some(entry) = self.entries.get(shell_id).cloned() else {
            return;
        };
        {
            let mut task = lock(&entry);
            if task.status != BackgroundShellStatus::Running {
                return;
            }
            task.status = BackgroundShellStatus::Failed;
            task.error = Some(error.into());
            task.end_time = Some(end_time);
        }
        write_status_file(&entry);
        self.emit_notification(&entry);
        self.prune_terminal_entries();
        self.fire_status_change(Some(entry));
    }

    pub fn cancel(&mut self, shell_id: &str, end_time: i64) {
        let Some(entry) = self.entries.get(shell_id).cloned() else {
            return;
        };
        if !settle_as_cancelled(&entry, end_time) {
            return;
        }
        self.emit_notification(&entry);
        self.prune_terminal_entries();
        self.fire_status_change(Some(entry));
    }

    /// Request process cancellation while leaving the task `running` until
    /// the spawn/settle path reports its actual end time and outcome.
    pub fn request_cancel(&self, shell_id: &str) {
        let Some(entry) = self.entries.get(shell_id) else {
            return;
        };
        let task = lock(entry);
        if task.status == BackgroundShellStatus::Running {
            task.abort_controller.cancel();
        }
    }

    /// Cancel every active shell at one timestamp and fire one batched
    /// status-change callback. Shutdown does not emit user notifications.
    pub fn abort_all(&mut self) {
        let end_time = current_time_millis();
        let mut last_cancelled = None;
        for entry in self.entries.values() {
            if settle_as_cancelled(entry, end_time) {
                last_cancelled = Some(Arc::clone(entry));
            }
        }
        if last_cancelled.is_some() {
            self.prune_terminal_entries();
            self.fire_status_change(last_cancelled);
        }
    }

    /// Forget all entries without signalling their processes. The caller
    /// must establish that there are no live managed shells first.
    pub fn reset(&mut self) {
        let first_entry = self.entries.values().next().cloned();
        if first_entry.is_none() {
            return;
        }
        self.entries.clear();
        self.fire_status_change(first_entry);
    }

    fn prune_terminal_entries(&mut self) {
        let mut terminal = self
            .entries
            .iter()
            .enumerate()
            .filter_map(|(order, (id, entry))| {
                let task = lock(entry);
                task.status.is_terminal().then(|| {
                    (
                        task.end_time.unwrap_or(task.start_time),
                        task.start_time,
                        order,
                        id.clone(),
                    )
                })
            })
            .collect::<Vec<_>>();
        terminal.sort_by_key(|(end_time, start_time, order, _)| (*end_time, *start_time, *order));
        let excess = terminal.len().saturating_sub(MAX_RETAINED_TERMINAL_SHELLS);
        for (_, _, _, id) in terminal.into_iter().take(excess) {
            self.entries.shift_remove(&id);
        }
    }

    fn fire_register(&self, entry: SharedShellTask) {
        if let Some(callback) = self.register_callback.clone() {
            let _ = catch_unwind(AssertUnwindSafe(|| callback(entry)));
        }
    }

    fn fire_status_change(&self, entry: Option<SharedShellTask>) {
        if let Some(callback) = self.status_change_callback.clone() {
            let _ = catch_unwind(AssertUnwindSafe(|| callback(entry)));
        }
    }

    fn emit_notification(&self, entry: &SharedShellTask) {
        let (task, callback) = {
            let mut task = lock(entry);
            if task.notified {
                return;
            }
            task.notified = true;
            (task.clone(), self.notification_callback.clone())
        };

        let Some(callback) = callback else {
            return;
        };

        let status_text = match task.status {
            BackgroundShellStatus::Completed => "completed",
            BackgroundShellStatus::Failed => "failed",
            BackgroundShellStatus::Cancelled => "was cancelled",
            BackgroundShellStatus::Running => return,
        };
        let command_label = truncate_notification_label(&task.command);
        let (command_for_model, command_truncated) = truncate_command_for_model(&task.command);
        let display_text = format!("Background shell \"{command_label}\" {status_text}.");

        let mut xml_parts = vec![
            "<task-notification>".to_owned(),
            format!("<task-id>{}</task-id>", escape_xml(&task.shell_id)),
            "<kind>shell</kind>".to_owned(),
            format!("<status>{}</status>", task.status.as_str()),
            format!(
                "<summary>Shell command \"{}\" {status_text}.</summary>",
                escape_xml(&command_label)
            ),
            if command_truncated {
                format!(
                    "<command truncated=\"true\">{}</command>",
                    escape_xml(&command_for_model)
                )
            } else {
                format!("<command>{}</command>", escape_xml(&command_for_model))
            },
            format!(
                "<cwd>{}</cwd>",
                escape_xml(&strip_display_control_chars(&task.cwd))
            ),
        ];
        if let Some(pid) = task.pid {
            xml_parts.push(format!("<pid>{pid}</pid>"));
        }
        if let Some(exit_code) = task.exit_code {
            xml_parts.push(format!("<exit-code>{exit_code}</exit-code>"));
        }
        if let Some(error) = task.error.as_deref().filter(|error| !error.is_empty()) {
            xml_parts.push(format!(
                "<result>{}</result>",
                escape_xml(&strip_display_control_chars(error))
            ));
        }
        if let Some(output_tail) = read_output_tail(&task.output_file) {
            match output_tail {
                OutputTail::Unreadable => {
                    xml_parts.push("<output-tail error=\"unreadable\" />".to_owned());
                }
                OutputTail::Text { text, truncated } => xml_parts.push(format!(
                    "<output-tail truncated=\"{truncated}\">{}</output-tail>",
                    escape_xml(&text)
                )),
            }
        }
        xml_parts.push(format!(
            "<output-file>{}</output-file>",
            escape_xml(&strip_display_control_chars(&task.output_file))
        ));
        xml_parts.push("</task-notification>".to_owned());

        let meta = ShellNotificationMeta {
            shell_id: task.shell_id,
            status: task.status,
            exit_code: task.exit_code,
            todo_work_chain_id: task.todo_work_chain_id,
        };
        let model_text = xml_parts.join("\n");
        let _ = catch_unwind(AssertUnwindSafe(|| {
            callback(display_text, model_text, meta)
        }));
    }
}

fn settle_as_cancelled(entry: &SharedShellTask, end_time: i64) -> bool {
    let mut task = lock(entry);
    if task.status != BackgroundShellStatus::Running {
        return false;
    }
    task.status = BackgroundShellStatus::Cancelled;
    task.end_time = Some(end_time);
    write_status_file_locked(&task);
    task.abort_controller.cancel();
    true
}

fn write_status_file(entry: &SharedShellTask) {
    write_status_file_locked(&lock(entry));
}

fn write_status_file_locked(task: &ShellTask) {
    let payload = status_payload(task);
    let Ok(contents) = serde_json::to_vec_pretty(&payload) else {
        return;
    };
    let options = AtomicWriteOptions {
        mode: Some(0o600),
        force_mode: true,
        flush: false,
        symlink_policy: SymlinkPolicy::NoFollow,
        ..AtomicWriteOptions::default()
    };
    let path = status_file_path_for(&task.output_file);
    // The TS implementation logs this best-effort failure; the registry
    // state and process lifecycle must remain healthy when persistence fails.
    let _ = atomic_write_file(path, &contents, &options);
}

fn status_payload(task: &ShellTask) -> Value {
    let mut payload = json!({
        "id": &task.id,
        "status": task.status.as_str(),
        "command": &task.command,
        "cwd": &task.cwd,
        "startTime": iso_timestamp(task.start_time),
        "updatedAt": iso_timestamp(current_time_millis()),
    });
    let object = payload
        .as_object_mut()
        .expect("JSON object constructed above");
    if let Some(pid) = task.pid {
        object.insert("pid".to_owned(), json!(pid));
    }
    if let Some(end_time) = task.end_time {
        object.insert("endTime".to_owned(), json!(iso_timestamp(end_time)));
    }
    if let Some(exit_code) = task.exit_code {
        object.insert("exitCode".to_owned(), json!(exit_code));
    }
    if let Some(error) = task.error.as_ref() {
        object.insert("error".to_owned(), json!(error));
    }
    payload
}

fn iso_timestamp(timestamp_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
        .unwrap_or_else(Utc::now)
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

enum OutputTail {
    Unreadable,
    Text { text: String, truncated: bool },
}

fn read_output_tail(output_file: &str) -> Option<OutputTail> {
    let result = read_output_tail_inner(Path::new(output_file));
    match result {
        Ok(Some((text, truncated))) if !text.is_empty() => {
            Some(OutputTail::Text { text, truncated })
        }
        Ok(_) => None,
        Err(_) => Some(OutputTail::Unreadable),
    }
}

fn read_output_tail_inner(path: &Path) -> io::Result<Option<(String, bool)>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Ok(None);
    }

    let length = metadata
        .len()
        .min(MAX_NOTIFICATION_OUTPUT_TAIL_BYTES as u64) as usize;
    let start = metadata.len() - length as u64;
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = vec![0; length];
    let bytes_read = file.read(&mut bytes)?;
    bytes.truncate(bytes_read);

    // Match the source byte-tail behavior: when starting partway through a
    // UTF-8 sequence, discard continuation bytes at the left boundary.
    let mut offset = 0;
    if start > 0 {
        while offset < bytes.len() && bytes[offset] & 0xc0 == 0x80 {
            offset += 1;
        }
    }
    let decoded = String::from_utf8_lossy(&bytes[offset..]);
    let text = strip_output_control_chars(&decoded).trim_end().to_owned();
    Ok(Some((text, start > 0)))
}

fn strip_output_control_chars(text: &str) -> String {
    text.chars()
        .filter(|character| {
            let code = *character as u32;
            matches!(code, 0x09 | 0x0a | 0x0d)
                || (code >= 0x20 && !(0x80..=0x9f).contains(&code) && !is_bidi_control_char(code))
        })
        .collect()
}

fn truncate_command_for_model(command: &str) -> (String, bool) {
    let sanitized = strip_display_control_chars(command);
    let utf16_length = sanitized.encode_utf16().count();
    if utf16_length <= MAX_NOTIFICATION_MODEL_COMMAND_LENGTH {
        return (sanitized, false);
    }
    let prefix = sanitized
        .encode_utf16()
        .take(MAX_NOTIFICATION_MODEL_COMMAND_LENGTH - 3)
        .collect::<Vec<_>>();
    let mut result = String::from_utf16_lossy(&prefix);
    result.push_str("...");
    (result, true)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "canopy-background-shell-{}-{}",
                std::process::id(),
                uuid::Uuid::new_v4()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn output(&self, name: &str) -> String {
            self.0.join(name).to_string_lossy().into_owned()
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn registration(dir: &TestDir, id: &str) -> ShellTaskRegistration {
        ShellTaskRegistration {
            shell_id: id.to_owned(),
            command: "printf hello".to_owned(),
            cwd: "/workspace".to_owned(),
            pid: Some(123),
            status: BackgroundShellStatus::Running,
            start_time: 1_700_000_000_000,
            end_time: None,
            exit_code: None,
            error: None,
            output_path: dir.output(&format!("shell-{id}.output")),
            todo_work_chain_id: Some("chain-1".to_owned()),
            abort_controller: CancellationToken::new(),
        }
    }

    #[test]
    fn registration_aliases_fields_and_fires_callbacks_in_order() {
        let dir = TestDir::new();
        let mut registry = BackgroundShellRegistry::new();
        let order = Arc::new(Mutex::new(Vec::new()));
        let register_order = Arc::clone(&order);
        registry.set_register_callback(Some(Arc::new(move |entry| {
            lock(&register_order).push(format!("register:{}", lock(&entry).shell_id));
        })));
        let status_order = Arc::clone(&order);
        registry.set_status_change_callback(Some(Arc::new(move |entry| {
            if let Some(entry) = entry {
                lock(&status_order).push(format!("status:{}", lock(&entry).shell_id));
            }
        })));

        let entry = registry.register(registration(&dir, "one"));
        let task = lock(&entry);
        assert_eq!(task.id, "one");
        assert_eq!(task.kind, "shell");
        assert_eq!(task.description, task.command);
        assert_eq!(task.output_file, task.output_path);
        assert_eq!(task.output_offset, 0);
        assert!(!task.notified);
        assert_eq!(
            lock(&order).clone(),
            vec!["register:one".to_owned(), "status:one".to_owned()]
        );
    }

    #[test]
    fn transitions_are_one_shot_and_request_cancel_waits_for_settlement() {
        let dir = TestDir::new();
        let mut registry = BackgroundShellRegistry::new();
        let entry = registry.register(registration(&dir, "one"));
        registry.request_cancel("one");
        {
            let task = lock(&entry);
            assert_eq!(task.status, BackgroundShellStatus::Running);
            assert!(task.abort_controller.is_cancelled());
            assert_eq!(task.end_time, None);
        }
        registry.complete("one", 0, 2_000);
        registry.fail("one", "late failure", 3_000);
        assert_eq!(lock(&entry).status, BackgroundShellStatus::Completed);
        assert_eq!(lock(&entry).exit_code, Some(0));
    }

    #[test]
    fn abort_all_cancels_all_without_notifications_and_fires_once() {
        let dir = TestDir::new();
        let mut registry = BackgroundShellRegistry::new();
        let mut entries = Vec::new();
        for id in ["a", "b"] {
            entries.push(registry.register(registration(&dir, id)));
        }
        let changes = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&changes);
        registry.set_status_change_callback(Some(Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        })));
        let notifications = Arc::new(AtomicUsize::new(0));
        let observed_notifications = Arc::clone(&notifications);
        registry.set_notification_callback(Some(Arc::new(move |_, _, _| {
            observed_notifications.fetch_add(1, Ordering::SeqCst);
        })));

        registry.abort_all();

        assert_eq!(changes.load(Ordering::SeqCst), 1);
        assert_eq!(notifications.load(Ordering::SeqCst), 0);
        for entry in entries {
            let task = lock(&entry);
            assert_eq!(task.status, BackgroundShellStatus::Cancelled);
            assert!(!task.notified);
            assert!(task.abort_controller.is_cancelled());
        }
    }

    #[test]
    fn keeps_only_the_newest_32_terminal_tasks_and_never_prunes_running() {
        let dir = TestDir::new();
        let mut registry = BackgroundShellRegistry::new();
        let running = registry.register(registration(&dir, "running"));
        for index in 0..MAX_RETAINED_TERMINAL_SHELLS + 2 {
            let id = format!("done-{index}");
            registry.register(registration(&dir, &id));
            registry.complete(&id, 0, 10_000 + index as i64);
        }
        assert_eq!(registry.get_all().len(), MAX_RETAINED_TERMINAL_SHELLS + 1);
        assert!(registry.get("done-0").is_none());
        assert!(registry.get("done-1").is_none());
        assert!(registry.get("done-33").is_some());
        assert_eq!(lock(&running).status, BackgroundShellStatus::Running);
    }

    #[test]
    fn sidecar_is_owner_only_and_tracks_status_transitions() {
        let dir = TestDir::new();
        let mut registry = BackgroundShellRegistry::new();
        let output = dir.output("shell-one.output");
        let entry = registry.register(registration(&dir, "one"));
        let status_path = status_file_path_for(&output);
        let initial: Value = serde_json::from_slice(&fs::read(&status_path).unwrap()).unwrap();
        assert_eq!(initial["status"], "running");
        assert_eq!(initial["startTime"], "2023-11-14T22:13:20.000Z");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&status_path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        registry.fail("one", "spawn failed", 1_700_000_001_000);
        let final_status: Value = serde_json::from_slice(&fs::read(&status_path).unwrap()).unwrap();
        assert_eq!(final_status["status"], "failed");
        assert_eq!(final_status["error"], "spawn failed");
        assert_eq!(final_status["endTime"], "2023-11-14T22:13:21.000Z");
        assert_eq!(lock(&entry).status, BackgroundShellStatus::Failed);
    }

    #[test]
    fn notifications_escape_xml_strip_bidi_and_bound_the_output_tail() {
        let dir = TestDir::new();
        let output_path = dir.output("shell-one.output");
        fs::write(
            &output_path,
            format!(
                "{}\nlast line\n",
                "x".repeat(MAX_NOTIFICATION_OUTPUT_TAIL_BYTES)
            ),
        )
        .unwrap();
        let mut registry = BackgroundShellRegistry::new();
        let captured = Arc::new(Mutex::new(None::<(String, String, ShellNotificationMeta)>));
        let captured_callback = Arc::clone(&captured);
        registry.set_notification_callback(Some(Arc::new(move |display, model, meta| {
            *lock(&captured_callback) = Some((display, model, meta));
        })));
        let mut registration = registration(&dir, "one&two");
        registration.command = "echo \"<x>\"\u{202e}".to_owned();
        registration.cwd = "/repo\u{202e}".to_owned();
        registration.output_path = output_path;
        registry.register(registration);
        registry.fail("one&two", "bad <result>\u{202e}", 2_000);

        let (display, model, meta) = lock(&captured).take().unwrap();
        assert!(display.contains("echo \"<x>\""));
        assert!(model.contains("<task-id>one&amp;two</task-id>"));
        assert!(model.contains("&lt;result&gt;"));
        assert!(model.contains("truncated=\"true\""));
        assert!(model.contains("last line</output-tail>"));
        assert!(!model.contains('\u{202e}'));
        assert_eq!(meta.status, BackgroundShellStatus::Failed);
        assert_eq!(meta.todo_work_chain_id.as_deref(), Some("chain-1"));
    }

    #[cfg(unix)]
    #[test]
    fn status_sidecar_replaces_symlink_without_writing_through_it() {
        use std::os::unix::fs::symlink;

        let dir = TestDir::new();
        let secret = dir.output("secret");
        let output = dir.output("shell.output");
        let status_path = PathBuf::from(status_file_path_for(&output));
        fs::write(&secret, "untouched").unwrap();
        symlink(&secret, &status_path).unwrap();
        let mut registration = registration(&dir, "symlink");
        registration.output_path = output;
        let mut registry = BackgroundShellRegistry::new();
        registry.register(registration);

        assert_eq!(fs::read_to_string(&secret).unwrap(), "untouched");
        assert!(
            !fs::symlink_metadata(status_path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn throwing_callbacks_do_not_break_registry_state() {
        let dir = TestDir::new();
        let mut registry = BackgroundShellRegistry::new();
        registry.set_register_callback(Some(Arc::new(|_| panic!("register callback"))));
        registry
            .set_notification_callback(Some(Arc::new(|_, _, _| panic!("notification callback"))));
        let entry = registry.register(registration(&dir, "one"));
        registry.complete("one", 0, 2_000);
        assert_eq!(lock(&entry).status, BackgroundShellStatus::Completed);
        assert!(lock(&entry).notified);
    }

    #[test]
    fn reset_forgets_entries_without_cancelling_them() {
        let dir = TestDir::new();
        let mut registry = BackgroundShellRegistry::new();
        let entry = registry.register(registration(&dir, "one"));
        registry.reset();
        assert!(registry.get_all().is_empty());
        assert_eq!(lock(&entry).status, BackgroundShellStatus::Running);
        assert!(!lock(&entry).abort_controller.is_cancelled());
    }

    #[test]
    fn status_path_uses_output_suffix_or_appends_sidecar_extension() {
        assert_eq!(
            status_file_path_for("/tmp/shell-a.output"),
            "/tmp/shell-a.status"
        );
        assert_eq!(
            status_file_path_for("/tmp/custom.log"),
            "/tmp/custom.log.status"
        );
    }
}
