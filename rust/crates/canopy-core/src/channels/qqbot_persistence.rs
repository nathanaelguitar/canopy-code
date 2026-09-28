//! Persisted routing state for QQ Bot channels.
//!
//! Port of the routing-state helpers in `packages/channels/qqbot/src/QQChannel.ts`.
//! The module owns only the state format and persistence policy; QQChannel must
//! still connect its maps, lifecycle, and channel-specific path to this helper.

use indexmap::IndexMap;
use serde::Serialize;
use serde_json::Value;
use std::error::Error;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

/// Delay used by `saveQQState()` to coalesce bursts of map updates.
pub const SAVE_DEBOUNCE: Duration = Duration::from_millis(500);
/// New state files are created with owner-only permissions, as in Node's
/// `writeFileSync(..., { mode: 0o600 })`.
pub const STATE_FILE_MODE: u32 = 0o600;
const REPLY_MSG_ID_TTL_MS: f64 = 300_000.0;
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// QQ chat type stored in the routing state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QqChatType {
    C2c,
    Group,
}

/// New-format reply correlation entry. Old string entries are upgraded on load.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplyMessageId {
    pub msg_id: String,
    pub timestamp: f64,
}

/// Routing maps persisted by QQChannel.
///
/// `IndexMap` preserves JavaScript `Map` insertion order in the serialized
/// entry arrays, including when an existing key is updated.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QqbotRoutingState {
    pub chat_type_map: IndexMap<String, QqChatType>,
    pub reply_msg_id: IndexMap<String, ReplyMessageId>,
    pub msg_seq_map: IndexMap<String, u64>,
    pub group_active_msg_enabled: IndexMap<String, bool>,
    pub bot_open_id_by_group: IndexMap<String, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedQqbotRoutingState<'a> {
    chat_type_map: Vec<(&'a String, &'a QqChatType)>,
    reply_msg_id: Vec<(&'a String, &'a ReplyMessageId)>,
    msg_seq_map: Vec<(&'a String, &'a u64)>,
    group_active_msg_enabled: Vec<(&'a String, &'a bool)>,
    bot_open_id_by_group: Vec<(&'a String, &'a String)>,
}

/// Serialize in the same five-field order and `[key, value]` array shape as
/// `JSON.stringify(serializeQQState())`.
pub fn serialize_qq_state(state: &QqbotRoutingState) -> Result<String, serde_json::Error> {
    let wire = PersistedQqbotRoutingState {
        chat_type_map: state.chat_type_map.iter().collect(),
        reply_msg_id: state.reply_msg_id.iter().collect(),
        msg_seq_map: state.msg_seq_map.iter().collect(),
        group_active_msg_enabled: state.group_active_msg_enabled.iter().collect(),
        bot_open_id_by_group: state.bot_open_id_by_group.iter().collect(),
    };
    serde_json::to_string(&wire)
}

/// Number of raw and accepted entries for one restored map. `rejected` follows
/// the source's `totalRaw - Map.size` calculation, so duplicate keys count as
/// rejected in this diagnostic even though the last value wins.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreCount {
    pub field: &'static str,
    pub accepted: usize,
    pub rejected: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RestoreReport {
    pub fields: Vec<RestoreCount>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RestoreError {
    InvalidJson(String),
    NonObjectRoot,
    NonIterableEntry(&'static str),
}

impl fmt::Display for RestoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJson(message) => write!(formatter, "{message}"),
            Self::NonObjectRoot => formatter.write_str("state root is not an object"),
            Self::NonIterableEntry(field) => {
                write!(formatter, "{field} contains a non-iterable entry")
            }
        }
    }
}

impl Error for RestoreError {}

/// Restore routing maps from JSON into `state`.
///
/// `now_ms` is injected to make legacy reply-ID normalization and timestamp
/// bounds deterministic. Missing or JavaScript-falsy properties leave their
/// corresponding existing map untouched, matching the source's `if (raw.x)`
/// guards. A truthy non-array property replaces that map with an empty map.
pub fn restore_qq_state_json(
    input: &str,
    now_ms: f64,
    state: &mut QqbotRoutingState,
) -> Result<RestoreReport, RestoreError> {
    if input
        .trim()
        .parse::<f64>()
        .is_ok_and(|number| !number.is_finite())
    {
        return Err(RestoreError::NonObjectRoot);
    }
    let normalized = replace_non_finite_json_numbers(input);
    let raw: Value = serde_json::from_str(&normalized)
        .map_err(|error| RestoreError::InvalidJson(error.to_string()))?;
    let Some(object) = raw.as_object() else {
        return Err(RestoreError::NonObjectRoot);
    };

    let mut report = RestoreReport::default();
    if let Some(value) = object.get("chatTypeMap").filter(|value| js_truthy(value)) {
        let (next, count) = restore_map(value, "chatTypeMap", |key, value| {
            let chat_type = match value.as_str()? {
                "c2c" => QqChatType::C2c,
                "group" => QqChatType::Group,
                _ => return None,
            };
            valid_key(key).then_some(chat_type)
        })?;
        state.chat_type_map = next;
        report.fields.push(count);
    }

    if let Some(value) = object.get("replyMsgId").filter(|value| js_truthy(value)) {
        let (next, count) = restore_map(value, "replyMsgId", |key, value| {
            if !valid_key(key) {
                return None;
            }
            match value {
                Value::String(msg_id) if utf16_len(msg_id) <= 128 => Some(ReplyMessageId {
                    msg_id: msg_id.clone(),
                    timestamp: now_ms,
                }),
                Value::Object(entry) => {
                    let msg_id = entry.get("msgId")?.as_str()?;
                    let timestamp = entry.get("timestamp")?.as_f64()?;
                    (utf16_len(msg_id) <= 128
                        && timestamp.is_finite()
                        && timestamp <= now_ms + REPLY_MSG_ID_TTL_MS)
                        .then(|| ReplyMessageId {
                            msg_id: msg_id.to_owned(),
                            timestamp,
                        })
                }
                _ => None,
            }
        })?;
        state.reply_msg_id = next;
        report.fields.push(count);
    }

    if let Some(value) = object.get("msgSeqMap").filter(|value| js_truthy(value)) {
        let (next, count) = restore_map(value, "msgSeqMap", |key, value| {
            let number = value.as_f64()?;
            if !valid_key(key)
                || !number.is_finite()
                || number < 0.0
                || number.fract() != 0.0
                || number > MAX_SAFE_INTEGER
            {
                return None;
            }
            Some(number as u64)
        })?;
        state.msg_seq_map = next;
        report.fields.push(count);
    }

    if let Some(value) = object
        .get("groupActiveMsgEnabled")
        .filter(|value| js_truthy(value))
    {
        let (next, count) = restore_map(value, "groupActiveMsgEnabled", |key, value| {
            valid_key(key).then_some(value.as_bool()?)
        })?;
        state.group_active_msg_enabled = next;
        report.fields.push(count);
    }

    if let Some(value) = object
        .get("botOpenIdByGroup")
        .filter(|value| js_truthy(value))
    {
        let (next, count) = restore_map(value, "botOpenIdByGroup", |key, value| {
            let open_id = value.as_str()?;
            (valid_key(key) && valid_open_id(open_id)).then(|| open_id.to_owned())
        })?;
        state.bot_open_id_by_group = next;
        report.fields.push(count);
    }

    Ok(report)
}

fn restore_map<T>(
    raw: &Value,
    field: &'static str,
    mut validate: impl FnMut(&str, &Value) -> Option<T>,
) -> Result<(IndexMap<String, T>, RestoreCount), RestoreError> {
    let array = raw.as_array();
    let total_raw = array.map_or(0, Vec::len);
    let mut map = IndexMap::new();
    if let Some(entries) = array {
        for entry in entries {
            let (key, value) = tuple_fields(entry).ok_or(RestoreError::NonIterableEntry(field))?;
            let Some(key) = key else {
                continue;
            };
            let Some(key) = key.as_str() else { continue };
            let Some(value) = value else { continue };
            if let Some(restored) = validate(key, &value) {
                map.insert(key.to_owned(), restored);
            }
        }
    }
    let count = RestoreCount {
        field,
        accepted: map.len(),
        rejected: total_raw.saturating_sub(map.len()),
    };
    Ok((map, count))
}

/// JavaScript destructuring of `[key, value]` also accepts iterable strings.
/// Other JSON values are non-iterable and throw inside the source's outer
/// restore try/catch.
fn tuple_fields(entry: &Value) -> Option<(Option<Value>, Option<Value>)> {
    match entry {
        Value::Array(values) => Some((values.first().cloned(), values.get(1).cloned())),
        Value::String(value) => {
            let mut chars = value.chars();
            let key = chars.next().map(|ch| Value::String(ch.to_string()));
            let value = chars.next().map(|ch| Value::String(ch.to_string()));
            Some((key, value))
        }
        _ => None,
    }
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn valid_key(key: &str) -> bool {
    utf16_len(key) <= 256
}

fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

fn valid_open_id(open_id: &str) -> bool {
    open_id.len() == 32 && open_id.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// `JSON.parse` accepts exponent literals that overflow to JavaScript
/// `Infinity`; Rust's strict JSON parser rejects them. Replace only such number
/// tokens with a truthy object before parsing so truthy non-array map fields
/// still clear their map, while element values fail the ordinary type filters.
fn replace_non_finite_json_numbers(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            if !byte.is_ascii() {
                let character = input[index..].chars().next().expect("valid UTF-8");
                output.push(character);
                index += character.len_utf8();
                continue;
            }
            output.push(byte as char);
            index += 1;
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        if byte == b'"' {
            in_string = true;
            output.push('"');
            index += 1;
            continue;
        }
        if byte == b'-' || byte.is_ascii_digit() {
            let start = index;
            while index < bytes.len()
                && (bytes[index].is_ascii_digit()
                    || matches!(bytes[index], b'-' | b'+' | b'.' | b'e' | b'E'))
            {
                index += 1;
            }
            let token = &input[start..index];
            if token.parse::<f64>().is_ok_and(|number| !number.is_finite()) {
                output.push_str("{\"__qqbot_nonfinite_number__\":true}");
            } else {
                output.push_str(token);
            }
            continue;
        }
        if !byte.is_ascii() {
            let character = input[index..].chars().next().expect("valid UTF-8");
            output.push(character);
            index += character.len_utf8();
        } else {
            output.push(byte as char);
            index += 1;
        }
    }
    output
}

/// Narrow file-system seam used by production persistence and deterministic
/// tests. Implementations must write the temp file with mode `0o600` when it
/// does not yet exist.
pub trait QqStateFileOps: Send + Sync {
    fn exists(&self, path: &Path) -> bool;
    fn read_to_string(&self, path: &Path) -> io::Result<String>;
    fn write_temp(&self, path: &Path, contents: &str) -> io::Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove_temp(&self, path: &Path) -> io::Result<()>;
}

#[derive(Default)]
pub struct FsQqStateFileOps;

impl QqStateFileOps for FsQqStateFileOps {
    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn read_to_string(&self, path: &Path) -> io::Result<String> {
        fs::read_to_string(path)
    }

    fn write_temp(&self, path: &Path, contents: &str) -> io::Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(STATE_FILE_MODE);
        }
        let mut file = options.open(path)?;
        file.write_all(contents.as_bytes())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn remove_temp(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }
}

/// QQ state persistence coordinator with the source's debounce and flush
/// behavior. The timer is a detached Tokio task, which naturally disappears
/// when its runtime shuts down; callers should invoke `save` on a Tokio runtime.
pub struct QqbotStatePersistence {
    path: PathBuf,
    channel_name: String,
    state: Arc<RwLock<QqbotRoutingState>>,
    disposed: Arc<AtomicBool>,
    timer: Mutex<Option<JoinHandle<()>>>,
    write_lock: Arc<Mutex<()>>,
    files: Arc<dyn QqStateFileOps>,
    debounce: Duration,
}

impl QqbotStatePersistence {
    pub fn new(path: impl Into<PathBuf>, channel_name: impl Into<String>) -> Self {
        Self::with_file_ops(
            path,
            channel_name,
            Arc::new(FsQqStateFileOps),
            SAVE_DEBOUNCE,
        )
    }

    pub fn with_file_ops(
        path: impl Into<PathBuf>,
        channel_name: impl Into<String>,
        files: Arc<dyn QqStateFileOps>,
        debounce: Duration,
    ) -> Self {
        Self {
            path: path.into(),
            channel_name: channel_name.into(),
            state: Arc::new(RwLock::new(QqbotRoutingState::default())),
            disposed: Arc::new(AtomicBool::new(false)),
            timer: Mutex::new(None),
            write_lock: Arc::new(Mutex::new(())),
            files,
            debounce,
        }
    }

    pub fn state(&self) -> Arc<RwLock<QqbotRoutingState>> {
        Arc::clone(&self.state)
    }

    pub fn set_disposed(&self, disposed: bool) {
        self.disposed.store(disposed, Ordering::SeqCst);
    }

    /// Schedule a debounced save. Returns `Ok(())` without scheduling if the
    /// channel is disposed, and replaces any still-pending timer otherwise.
    pub fn save(&self) -> Result<(), QqStateScheduleError> {
        if self.disposed.load(Ordering::SeqCst) {
            return Ok(());
        }
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| QqStateScheduleError::NoTokioRuntime)?;
        let mut timer = self
            .timer
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(previous) = timer.take() {
            previous.abort();
        }

        let state = Arc::clone(&self.state);
        let disposed = Arc::clone(&self.disposed);
        let files = Arc::clone(&self.files);
        let path = self.path.clone();
        let name = self.channel_name.clone();
        let delay = self.debounce;
        let write_lock = self.write_lock_ref();
        *timer = Some(handle.spawn(async move {
            tokio::time::sleep(delay).await;
            if disposed.load(Ordering::SeqCst) {
                return;
            }
            let _write_guard = write_lock
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if disposed.load(Ordering::SeqCst) {
                return;
            }
            let snapshot = state
                .read()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone();
            write_state(&*files, &path, &name, "saveQQState", &snapshot);
        }));
        Ok(())
    }

    /// Immediately persist the latest snapshot and cancel a pending debounced
    /// save. Intentionally does not skip the write after disposal.
    pub fn flush(&self) {
        let mut timer = self
            .timer
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(pending) = timer.take() {
            pending.abort();
        }
        let _write_guard = self
            .write_lock
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let snapshot = self
            .state
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        write_state(
            &*self.files,
            &self.path,
            &self.channel_name,
            "flushQQState",
            &snapshot,
        );
    }

    /// Load and validate the file at the current wall-clock time.
    pub fn restore(&self) -> bool {
        self.restore_at(current_time_ms())
    }

    /// Load and validate the file at an injected time for deterministic tests.
    pub fn restore_at(&self, now_ms: f64) -> bool {
        if !self.files.exists(&self.path) {
            return false;
        }
        let input = match self.files.read_to_string(&self.path) {
            Ok(input) => input,
            Err(error) => {
                eprintln!(
                    "[QQ:{}] Failed to restore QQ state: {}",
                    self.channel_name,
                    crate::channels::sanitize::sanitize_log_text(&error.to_string(), 200)
                );
                return false;
            }
        };
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        match restore_qq_state_json(&input, now_ms, &mut state) {
            Ok(report) => {
                for count in report.fields {
                    if count.rejected > 0 {
                        eprintln!(
                            "[QQ:{}] restoreQQState: accepted {} {} entries (rejected {})",
                            self.channel_name, count.accepted, count.field, count.rejected
                        );
                    }
                }
                true
            }
            Err(RestoreError::NonObjectRoot) => {
                eprintln!(
                    "[QQ:{}] Invalid QQ state file (not an object), ignoring",
                    self.channel_name
                );
                false
            }
            Err(error) => {
                eprintln!(
                    "[QQ:{}] Failed to restore QQ state: {}",
                    self.channel_name,
                    crate::channels::sanitize::sanitize_log_text(&error.to_string(), 200)
                );
                false
            }
        }
    }

    fn write_lock_ref(&self) -> Arc<Mutex<()>> {
        Arc::clone(&self.write_lock)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QqStateScheduleError {
    NoTokioRuntime,
}

impl fmt::Display for QqStateScheduleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoTokioRuntime => formatter.write_str("save requires an active Tokio runtime"),
        }
    }
}

impl Error for QqStateScheduleError {}

fn write_state(
    files: &dyn QqStateFileOps,
    path: &Path,
    channel_name: &str,
    method: &str,
    state: &QqbotRoutingState,
) {
    let tmp_path = append_tmp_suffix(path);
    let result = serialize_qq_state(state)
        .map_err(|error| io::Error::other(error.to_string()))
        .and_then(|contents| files.write_temp(&tmp_path, &contents))
        .and_then(|()| files.rename(&tmp_path, path));
    if let Err(error) = result {
        let _ = files.remove_temp(&tmp_path);
        eprintln!(
            "[QQ:{channel_name}] {method} write failed: {}",
            crate::channels::sanitize::sanitize_log_text(&error.to_string(), 200)
        );
    }
}

fn append_tmp_suffix(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".tmp");
    PathBuf::from(value)
}

fn current_time_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;

    #[derive(Default)]
    struct MemoryFiles {
        contents: Mutex<HashMap<PathBuf, String>>,
        operations: Mutex<Vec<String>>,
        fail_write: AtomicBool,
    }

    impl MemoryFiles {
        fn contents(&self, path: impl AsRef<Path>) -> Option<String> {
            self.contents
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .get(path.as_ref())
                .cloned()
        }

        fn operations(&self) -> Vec<String> {
            self.operations
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
        }
    }

    impl QqStateFileOps for MemoryFiles {
        fn exists(&self, path: &Path) -> bool {
            self.contents
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .contains_key(path)
        }

        fn read_to_string(&self, path: &Path) -> io::Result<String> {
            self.contents(path)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing"))
        }

        fn write_temp(&self, path: &Path, contents: &str) -> io::Result<()> {
            self.operations
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(format!("write:{}", path.display()));
            if self.fail_write.load(Ordering::SeqCst) {
                return Err(io::Error::other("write failed\nwith controls"));
            }
            self.contents
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert(path.to_owned(), contents.to_owned());
            Ok(())
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.operations
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(format!("rename:{}:{}", from.display(), to.display()));
            let mut contents = self
                .contents
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let value = contents
                .remove(from)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing temp"))?;
            contents.insert(to.to_owned(), value);
            Ok(())
        }

        fn remove_temp(&self, path: &Path) -> io::Result<()> {
            self.operations
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(format!("remove:{}", path.display()));
            self.contents
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .remove(path);
            Ok(())
        }
    }

    fn persistence(files: Arc<MemoryFiles>, debounce: Duration) -> QqbotStatePersistence {
        QqbotStatePersistence::with_file_ops("/tmp/qq-state.json", "test-bot", files, debounce)
    }

    #[test]
    fn serializes_all_five_maps_as_ordered_entry_arrays() {
        let mut state = QqbotRoutingState::default();
        state.chat_type_map.insert("u1".into(), QqChatType::C2c);
        state.chat_type_map.insert("g1".into(), QqChatType::Group);
        state.reply_msg_id.insert(
            "u1".into(),
            ReplyMessageId {
                msg_id: "msg-1".into(),
                timestamp: 1000.0,
            },
        );
        state.msg_seq_map.insert("msg-1".into(), 3);
        state.group_active_msg_enabled.insert("g1".into(), true);
        state
            .bot_open_id_by_group
            .insert("g1".into(), "abcdef0123456789abcdef0123456789".into());

        assert_eq!(
            serialize_qq_state(&state).unwrap(),
            r#"{"chatTypeMap":[["u1","c2c"],["g1","group"]],"replyMsgId":[["u1",{"msgId":"msg-1","timestamp":1000.0}]],"msgSeqMap":[["msg-1",3]],"groupActiveMsgEnabled":[["g1",true]],"botOpenIdByGroup":[["g1","abcdef0123456789abcdef0123456789"]]}"#
        );
    }

    #[test]
    fn restores_maps_filters_values_and_upgrades_legacy_reply_ids() {
        let input = r#"{
          "chatTypeMap":[["u","c2c"],["g","group"],["bad","direct"]],
          "replyMsgId":[["old","legacy"],["current",{"msgId":"new","timestamp":301000}],["expired",{"msgId":"kept","timestamp":-1}],["future",{"msgId":"no","timestamp":301001}]],
          "msgSeqMap":[["zero",0],["safe",9007199254740991],["fraction",1.5],["negative",-1],["unsafe",9007199254740992],["overflow",1e999]],
          "groupActiveMsgEnabled":[["yes",true],["no",false],["bad",1]],
          "botOpenIdByGroup":[["good","AbCdEf0123456789abcdef0123456789"],["bad","not-an-openid"]]
        }"#;
        let mut state = QqbotRoutingState::default();
        let report = restore_qq_state_json(input, 1000.0, &mut state).unwrap();

        assert_eq!(state.chat_type_map.get("u"), Some(&QqChatType::C2c));
        assert_eq!(state.chat_type_map.get("g"), Some(&QqChatType::Group));
        assert!(!state.chat_type_map.contains_key("bad"));
        assert_eq!(
            state.reply_msg_id.get("old"),
            Some(&ReplyMessageId {
                msg_id: "legacy".into(),
                timestamp: 1000.0,
            })
        );
        assert_eq!(state.reply_msg_id["current"].timestamp, 301000.0);
        assert_eq!(state.reply_msg_id["expired"].timestamp, -1.0);
        assert!(!state.reply_msg_id.contains_key("future"));
        assert_eq!(state.msg_seq_map["zero"], 0);
        assert_eq!(state.msg_seq_map["safe"], 9_007_199_254_740_991);
        assert_eq!(state.msg_seq_map.len(), 2);
        assert_eq!(state.group_active_msg_enabled.len(), 2);
        assert_eq!(state.bot_open_id_by_group.len(), 1);
        assert_eq!(report.fields.len(), 5);
        assert_eq!(report.fields[2].field, "msgSeqMap");
        assert_eq!(report.fields[2].rejected, 4);
    }

    #[test]
    fn validates_utf16_key_and_reply_id_lengths_and_openid_shape() {
        let accepted_key = "😀".repeat(128); // 256 UTF-16 code units
        let rejected_key = "😀".repeat(129);
        let accepted_reply = "r".repeat(128);
        let rejected_reply = "r".repeat(129);
        let input = serde_json::json!({
            "chatTypeMap": [[accepted_key, "group"], [rejected_key, "group"]],
            "replyMsgId": [["ok", accepted_reply], ["long", rejected_reply]],
            "botOpenIdByGroup": [
                ["upper", "ABCDEF0123456789ABCDEF0123456789"],
                ["short", "abcdef"],
                ["nonhex", "gabcdef0123456789abcdef012345678"],
            ],
        })
        .to_string();
        let mut state = QqbotRoutingState::default();
        restore_qq_state_json(&input, 5.0, &mut state).unwrap();

        assert_eq!(state.chat_type_map.len(), 1);
        assert_eq!(state.reply_msg_id["ok"].msg_id.len(), 128);
        assert_eq!(state.reply_msg_id.len(), 1);
        assert_eq!(state.bot_open_id_by_group.len(), 1);
    }

    #[test]
    fn handles_falsy_and_truthy_non_array_fields_like_source_guards() {
        let mut state = QqbotRoutingState::default();
        state.chat_type_map.insert("kept".into(), QqChatType::C2c);
        state.msg_seq_map.insert("removed".into(), 2);
        let report =
            restore_qq_state_json(r#"{"chatTypeMap":null,"msgSeqMap":1e999}"#, 0.0, &mut state)
                .unwrap();
        assert!(state.chat_type_map.contains_key("kept"));
        assert!(state.msg_seq_map.is_empty());
        assert_eq!(report.fields.len(), 1);
        assert_eq!(report.fields[0].field, "msgSeqMap");
    }

    #[test]
    fn rejects_invalid_roots_json_and_non_iterable_entries() {
        let mut state = QqbotRoutingState::default();
        assert_eq!(
            restore_qq_state_json("42", 0.0, &mut state),
            Err(RestoreError::NonObjectRoot)
        );
        assert_eq!(
            restore_qq_state_json("1e999", 0.0, &mut state),
            Err(RestoreError::NonObjectRoot)
        );
        assert!(matches!(
            restore_qq_state_json("{bad", 0.0, &mut state),
            Err(RestoreError::InvalidJson(_))
        ));
        assert_eq!(
            restore_qq_state_json(r#"{"msgSeqMap":[null]}"#, 0.0, &mut state),
            Err(RestoreError::NonIterableEntry("msgSeqMap"))
        );
    }

    #[tokio::test]
    async fn save_debounces_repeated_updates_and_writes_temp_then_rename() {
        let files = Arc::new(MemoryFiles::default());
        let store = persistence(Arc::clone(&files), Duration::from_millis(35));
        store
            .state()
            .write()
            .unwrap()
            .msg_seq_map
            .insert("message".into(), 1);
        store.save().unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        store
            .state()
            .write()
            .unwrap()
            .msg_seq_map
            .insert("message".into(), 7);
        store.save().unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;

        let operations = files.operations();
        assert_eq!(operations.len(), 2);
        assert_eq!(operations[0], "write:/tmp/qq-state.json.tmp");
        assert_eq!(
            operations[1],
            "rename:/tmp/qq-state.json.tmp:/tmp/qq-state.json"
        );
        let written: Value =
            serde_json::from_str(&files.contents("/tmp/qq-state.json").expect("saved state"))
                .unwrap();
        assert_eq!(written["msgSeqMap"], serde_json::json!([["message", 7]]));
    }

    #[tokio::test]
    async fn flush_cancels_pending_save_and_still_writes_after_disposal() {
        let files = Arc::new(MemoryFiles::default());
        let store = persistence(Arc::clone(&files), Duration::from_millis(80));
        store.save().unwrap();
        store.set_disposed(true);
        store.flush();
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(files.operations().len(), 2);
        assert!(files.contents("/tmp/qq-state.json").is_some());
        assert!(store.save().is_ok());
        assert_eq!(files.operations().len(), 2);
    }

    #[tokio::test]
    async fn disposed_timer_does_not_write_and_failure_cleans_temp() {
        let files = Arc::new(MemoryFiles::default());
        let store = persistence(Arc::clone(&files), Duration::from_millis(10));
        store.save().unwrap();
        store.set_disposed(true);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(files.operations().is_empty());

        store.set_disposed(false);
        files.fail_write.store(true, Ordering::SeqCst);
        store.flush();
        assert_eq!(
            files.operations(),
            [
                "write:/tmp/qq-state.json.tmp",
                "remove:/tmp/qq-state.json.tmp"
            ]
        );
    }

    #[test]
    fn restore_file_returns_false_for_missing_or_corrupt_state() {
        let files = Arc::new(MemoryFiles::default());
        let store = persistence(Arc::clone(&files), Duration::ZERO);
        assert!(!store.restore_at(0.0));
        files
            .contents
            .lock()
            .unwrap()
            .insert(PathBuf::from("/tmp/qq-state.json"), "not json".into());
        assert!(!store.restore_at(0.0));
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_temp_file_uses_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let sequence = AtomicUsize::new(0).fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "qqbot-state-mode-{}-{sequence}.tmp",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        FsQqStateFileOps.write_temp(&path, "{}").unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        let _ = fs::remove_file(&path);
        assert_eq!(mode, STATE_FILE_MODE);
    }
}
