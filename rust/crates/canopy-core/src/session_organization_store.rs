//! Persistent session groups, pin state, and quick color tags.
//!
//! This is the standalone Rust counterpart of
//! `packages/core/src/services/session-organization-service.ts`. It stores
//! `session-organization.v1.json` in the project's Canopy project directory,
//! validates mutations, drops malformed entries while reading, and atomically
//! replaces the file with private permissions.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use chrono::{SecondsFormat, Utc};
use indexmap::IndexMap;
use serde::de;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use crate::storage::Storage;
use crate::utils::atomic_file_write::{AtomicWriteOptions, atomic_write_file};

pub const GROUP_COLOR_OPTIONS: [SessionGroupPresetColor; 6] = [
    SessionGroupPresetColor::Red,
    SessionGroupPresetColor::Orange,
    SessionGroupPresetColor::Yellow,
    SessionGroupPresetColor::Green,
    SessionGroupPresetColor::Blue,
    SessionGroupPresetColor::Purple,
];

pub const SESSION_ORGANIZATION_STORE_FILE: &str = "session-organization.v1.json";
pub const MAX_GROUP_NAME_LENGTH: usize = 64;
pub const MAX_GROUPS: usize = 200;

// These caps keep parsing and mutations bounded. A cap violation makes the
// store unreadable for writes so an operation cannot silently erase entries.
const MAX_STORE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_SESSION_ENTRIES: usize = 50_000;
const MAX_ID_BYTES: usize = 1_024;
const MAX_REMOVAL_IDS: usize = 50_000;
const SCHEMA_VERSION: u64 = 1;
const FALLBACK_GROUP_COLOR: SessionGroupColor =
    SessionGroupColor::Preset(SessionGroupPresetColor::Blue);
const EPOCH_ISO: &str = "1970-01-01T00:00:00.000Z";

static MUTATION_LOCK: Mutex<()> = Mutex::new(());

/// The preset colors accepted for groups and session tags.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionGroupPresetColor {
    Red,
    Orange,
    Yellow,
    Green,
    Blue,
    Purple,
}

impl SessionGroupPresetColor {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Red => "red",
            Self::Orange => "orange",
            Self::Yellow => "yellow",
            Self::Green => "green",
            Self::Blue => "blue",
            Self::Purple => "purple",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "red" => Some(Self::Red),
            "orange" => Some(Self::Orange),
            "yellow" => Some(Self::Yellow),
            "green" => Some(Self::Green),
            "blue" => Some(Self::Blue),
            "purple" => Some(Self::Purple),
            _ => None,
        }
    }
}

/// A preset or normalized six-digit hexadecimal group color.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionGroupColor {
    Preset(SessionGroupPresetColor),
    Hex(String),
}

impl SessionGroupColor {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Preset(color) => color.as_str(),
            Self::Hex(color) => color,
        }
    }
}

impl Serialize for SessionGroupColor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SessionGroupColor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        normalize_group_color(&value).map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionGroup {
    pub id: String,
    pub name: String,
    pub color: SessionGroupColor,
    pub order: f64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionOrganizationView {
    pub group_id: Option<String>,
    pub color: Option<SessionGroupPresetColor>,
    pub pinned_at: Option<String>,
    pub updated_at: String,
    pub is_pinned: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionOrganizationSnapshot {
    pub groups: Vec<SessionGroup>,
    pub sessions: IndexMap<String, SessionOrganizationView>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionGroupCatalog {
    pub groups: Vec<SessionGroup>,
    pub color_options: Vec<SessionGroupPresetColor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionGroupInput {
    pub name: String,
    pub color: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpdateSessionGroupInput {
    pub name: Option<String>,
    pub color: Option<String>,
    pub order: Option<i64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpdateSessionOrganizationInput {
    pub is_pinned: Option<bool>,
    /// `None` leaves the group unchanged; `Some(None)` clears it.
    pub group_id: Option<Option<String>>,
    /// `None` leaves the tag unchanged; `Some(None)` clears it.
    pub color: Option<Option<String>>,
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct SessionOrganizationError {
    pub message: String,
    pub code: &'static str,
    pub field: Option<&'static str>,
}

impl SessionOrganizationError {
    fn new(message: impl Into<String>, code: &'static str, field: Option<&'static str>) -> Self {
        Self {
            message: message.into(),
            code,
            field,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct StoreData {
    groups: Vec<SessionGroup>,
    sessions: IndexMap<String, StoredOrganization>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoredOrganization {
    group_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    color: Option<SessionGroupPresetColor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pinned_at: Option<String>,
    updated_at: String,
}

/// Synchronous store for the session-organization v1 document.
///
/// `new` takes the project root (the `cwd` passed to TypeScript's
/// `SessionOrganizationService`) and resolves the same storage path contract.
/// `at_store_path` is available to callers that already resolved the project
/// directory themselves.
pub struct SessionOrganizationStore {
    file_path: PathBuf,
    read_failed: Mutex<bool>,
    warning_keys: Mutex<HashSet<String>>,
    on_warning: Option<Arc<dyn Fn(&str) + Send + Sync>>,
}

impl SessionOrganizationStore {
    pub fn new(cwd: impl Into<PathBuf>) -> Self {
        let storage = Storage::new(cwd);
        Self::at_store_path(
            storage
                .get_project_dir()
                .join(SESSION_ORGANIZATION_STORE_FILE),
        )
    }

    pub fn at_store_path(file_path: impl Into<PathBuf>) -> Self {
        Self::with_path_and_warning_handler(file_path, None)
    }

    pub fn with_warning_handler<F>(cwd: impl Into<PathBuf>, on_warning: F) -> Self
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        let storage = Storage::new(cwd);
        Self::with_path_and_warning_handler(
            storage
                .get_project_dir()
                .join(SESSION_ORGANIZATION_STORE_FILE),
            Some(Arc::new(on_warning)),
        )
    }

    pub fn at_store_path_with_warning_handler<F>(
        file_path: impl Into<PathBuf>,
        on_warning: F,
    ) -> Self
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        Self::with_path_and_warning_handler(file_path, Some(Arc::new(on_warning)))
    }

    fn with_path_and_warning_handler(
        file_path: impl Into<PathBuf>,
        on_warning: Option<Arc<dyn Fn(&str) + Send + Sync>>,
    ) -> Self {
        Self {
            file_path: file_path.into(),
            read_failed: Mutex::new(false),
            warning_keys: Mutex::new(HashSet::new()),
            on_warning,
        }
    }

    pub fn store_path(&self) -> &Path {
        &self.file_path
    }

    pub fn list_groups(&self) -> SessionGroupCatalog {
        let store = self.read_store();
        SessionGroupCatalog {
            groups: sort_groups(store.groups),
            color_options: GROUP_COLOR_OPTIONS.to_vec(),
        }
    }

    pub fn read_snapshot(&self) -> SessionOrganizationSnapshot {
        let store = self.read_store();
        let valid_group_ids: HashSet<&str> =
            store.groups.iter().map(|group| group.id.as_str()).collect();
        let mut sessions = IndexMap::with_capacity(store.sessions.len());
        for (session_id, organization) in store.sessions {
            let mut view = view_organization(Some(&organization));
            if let Some(group_id) = view.group_id.as_deref() {
                if !valid_group_ids.contains(group_id) {
                    self.warn_once(
                        format!("orphaned-group:{session_id}\0{group_id}"),
                        format!("Dropped orphaned session group reference: session {session_id} references missing group {group_id}"),
                    );
                    view.group_id = None;
                }
            }
            sessions.insert(session_id, view);
        }
        SessionOrganizationSnapshot {
            groups: sort_groups(store.groups),
            sessions,
        }
    }

    pub fn create_group(
        &self,
        input: CreateSessionGroupInput,
    ) -> Result<SessionGroup, SessionOrganizationError> {
        let name = normalize_group_name(&input.name)?;
        let color = normalize_group_color(&input.color)?;
        let _guard = lock(&MUTATION_LOCK);
        let mut store = self.read_store();
        assert_group_name_available(&store.groups, &name, None)?;
        if store.groups.len() >= MAX_GROUPS {
            return Err(SessionOrganizationError::new(
                format!("Maximum number of groups ({MAX_GROUPS}) reached"),
                "group_limit_reached",
                None,
            ));
        }
        let now = now_iso();
        let max_order = store
            .groups
            .iter()
            .map(|group| group.order)
            .fold(-1.0_f64, f64::max);
        let order = (max_order + 1.0).min(9_007_199_254_740_991.0);
        let group = SessionGroup {
            id: Uuid::new_v4().to_string(),
            name,
            color,
            order,
            created_at: now.clone(),
            updated_at: now,
        };
        store.groups.push(group.clone());
        self.write_store(&store)?;
        Ok(group)
    }

    pub fn update_group(
        &self,
        group_id: &str,
        input: UpdateSessionGroupInput,
    ) -> Result<SessionGroup, SessionOrganizationError> {
        validate_id(group_id, "groupId")?;
        let _guard = lock(&MUTATION_LOCK);
        let mut store = self.read_store();
        let Some(group_index) = store.groups.iter().position(|group| group.id == group_id) else {
            return Err(SessionOrganizationError::new(
                format!("Group not found: {group_id}"),
                "group_not_found",
                Some("groupId"),
            ));
        };
        if let Some(name_input) = input.name {
            let name = normalize_group_name(&name_input)?;
            assert_group_name_available(&store.groups, &name, Some(group_id))?;
            store.groups[group_index].name = name;
        }
        if let Some(color_input) = input.color {
            store.groups[group_index].color = normalize_group_color(&color_input)?;
        }
        if let Some(order) = input.order {
            store.groups[group_index].order = normalize_order(order)?;
        }
        store.groups[group_index].updated_at = now_iso();
        let group = store.groups[group_index].clone();
        self.write_store(&store)?;
        Ok(group)
    }

    pub fn delete_group(&self, group_id: &str) -> Result<bool, SessionOrganizationError> {
        validate_id(group_id, "groupId")?;
        let _guard = lock(&MUTATION_LOCK);
        let mut store = self.read_store();
        let old_len = store.groups.len();
        store.groups.retain(|group| group.id != group_id);
        if old_len == store.groups.len() {
            return Ok(false);
        }
        let now = now_iso();
        for session in store.sessions.values_mut() {
            if session.group_id.as_deref() == Some(group_id) {
                session.group_id = None;
                session.updated_at.clone_from(&now);
            }
        }
        self.write_store(&store)?;
        Ok(true)
    }

    pub fn update_session_organization(
        &self,
        session_id: &str,
        input: UpdateSessionOrganizationInput,
    ) -> Result<SessionOrganizationView, SessionOrganizationError> {
        validate_id(session_id, "sessionId")?;
        let has_update =
            input.group_id.is_some() || input.is_pinned.is_some() || input.color.is_some();
        let _guard = lock(&MUTATION_LOCK);
        let mut store = self.read_store();
        let mut current = view_organization(store.sessions.get(session_id));
        if !has_update {
            return Ok(current);
        }
        let now = now_iso();
        if let Some(color) = input.color {
            current.color = match color {
                Some(color) => Some(SessionGroupPresetColor::parse(&color).ok_or_else(|| {
                    SessionOrganizationError::new(
                        "`color` must be one of the supported color options",
                        "invalid_group_color",
                        Some("color"),
                    )
                })?),
                None => None,
            };
        }
        if let Some(group_id) = input.group_id {
            if let Some(group_id) = group_id.as_deref() {
                validate_id(group_id, "groupId")?;
                if !store.groups.iter().any(|group| group.id == group_id) {
                    return Err(SessionOrganizationError::new(
                        format!("Group not found: {group_id}"),
                        "group_not_found",
                        Some("groupId"),
                    ));
                }
            }
            current.group_id = group_id;
        }
        if let Some(is_pinned) = input.is_pinned {
            if is_pinned {
                if current.pinned_at.is_none() {
                    current.pinned_at = Some(now.clone());
                }
            } else {
                current.pinned_at = None;
            }
        }
        current.updated_at.clone_from(&now);
        if !store.sessions.contains_key(session_id) && store.sessions.len() >= MAX_SESSION_ENTRIES {
            return Err(SessionOrganizationError::new(
                format!("Maximum number of session entries ({MAX_SESSION_ENTRIES}) reached"),
                "session_limit_reached",
                Some("sessionId"),
            ));
        }
        store.sessions.insert(
            session_id.to_string(),
            StoredOrganization {
                group_id: current.group_id.clone(),
                color: current.color,
                pinned_at: current.pinned_at.clone(),
                updated_at: current.updated_at.clone(),
            },
        );
        self.write_store(&store)?;
        Ok(current)
    }

    pub fn remove_session(&self, session_id: &str) -> Result<(), SessionOrganizationError> {
        validate_id(session_id, "sessionId")?;
        let _guard = lock(&MUTATION_LOCK);
        let mut store = self.read_store();
        if store.sessions.shift_remove(session_id).is_some() {
            self.write_store(&store)?;
        }
        Ok(())
    }

    pub fn remove_sessions(&self, session_ids: &[String]) -> Result<(), SessionOrganizationError> {
        if session_ids.len() > MAX_REMOVAL_IDS {
            return Err(SessionOrganizationError::new(
                format!("At most {MAX_REMOVAL_IDS} session ids may be removed at once"),
                "session_id_limit_reached",
                Some("sessionIds"),
            ));
        }
        for session_id in session_ids {
            validate_id(session_id, "sessionId")?;
        }
        if session_ids.is_empty() {
            return Ok(());
        }
        let _guard = lock(&MUTATION_LOCK);
        let mut store = self.read_store();
        let mut changed = false;
        let unique_ids: HashSet<&str> = session_ids.iter().map(String::as_str).collect();
        for session_id in unique_ids {
            changed |= store.sessions.shift_remove(session_id).is_some();
        }
        if changed {
            self.write_store(&store)?;
        }
        Ok(())
    }

    fn read_store(&self) -> StoreData {
        let mut file = match File::open(&self.file_path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.set_read_failed(false);
                return StoreData::default();
            }
            Err(error) => {
                return self.unreadable(&format!("{}", error));
            }
        };
        let mut bytes = Vec::new();
        if let Err(error) = file
            .by_ref()
            .take(MAX_STORE_BYTES + 1)
            .read_to_end(&mut bytes)
        {
            return self.unreadable(&format!("{}", error));
        }
        if bytes.len() as u64 > MAX_STORE_BYTES {
            return self.unreadable(&format!(
                "store exceeds the {MAX_STORE_BYTES}-byte input limit"
            ));
        }
        let contents = String::from_utf8_lossy(&bytes);
        let raw: Value = match serde_json::from_str(&contents) {
            Ok(raw) => raw,
            Err(error) => return self.unreadable(&error.to_string()),
        };
        let Some(raw) = raw.as_object() else {
            return self.unreadable("store root is not an object");
        };
        if raw.get("schemaVersion").and_then(Value::as_f64) != Some(SCHEMA_VERSION as f64) {
            let found = raw
                .get("schemaVersion")
                .map(ToString::to_string)
                .unwrap_or_else(|| "undefined".to_string());
            return self.unreadable(&format!(
                "store schema is unsupported (found schemaVersion {found}, expected {SCHEMA_VERSION})"
            ));
        }

        let raw_groups = raw.get("groups").and_then(Value::as_array);
        if raw_groups.is_some_and(|groups| groups.len() > MAX_GROUPS) {
            return self.unreadable(&format!(
                "store contains more than {MAX_GROUPS} session groups"
            ));
        }
        let mut groups = Vec::new();
        if let Some(raw_groups) = raw_groups {
            for raw_group in raw_groups {
                if let Some(group) = self.normalize_group(raw_group) {
                    groups.push(group);
                }
            }
        }

        let raw_sessions = raw.get("sessions").and_then(Value::as_object);
        if raw_sessions.is_some_and(|sessions| sessions.len() > MAX_SESSION_ENTRIES) {
            return self.unreadable(&format!(
                "store contains more than {MAX_SESSION_ENTRIES} session entries"
            ));
        }
        let mut sessions = IndexMap::new();
        if let Some(raw_sessions) = raw_sessions {
            for (session_id, raw_organization) in raw_sessions {
                let Some(organization) = raw_organization.as_object() else {
                    self.warn_malformed_session(session_id);
                    continue;
                };
                let group_id = organization
                    .get("groupId")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let color = organization
                    .get("color")
                    .and_then(Value::as_str)
                    .and_then(SessionGroupPresetColor::parse);
                let pinned_at = organization
                    .get("pinnedAt")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let updated_at = organization
                    .get("updatedAt")
                    .and_then(Value::as_str)
                    .unwrap_or(EPOCH_ISO)
                    .to_string();
                sessions.insert(
                    session_id.clone(),
                    StoredOrganization {
                        group_id,
                        color,
                        pinned_at,
                        updated_at,
                    },
                );
            }
        }
        self.set_read_failed(false);
        StoreData {
            groups: dedupe_groups(groups, |message| self.warn(message)),
            sessions,
        }
    }

    fn normalize_group(&self, raw_value: &Value) -> Option<SessionGroup> {
        let Some(value) = raw_value.as_object() else {
            self.warn_malformed_group(raw_value);
            return None;
        };
        let Some(id) = value.get("id").and_then(Value::as_str) else {
            self.warn_malformed_group_id("<unknown>");
            return None;
        };
        let Some(name) = value.get("name").and_then(Value::as_str) else {
            self.warn_malformed_group_id(id);
            return None;
        };
        let Some(raw_color) = value.get("color").and_then(Value::as_str) else {
            self.warn_malformed_group_id(id);
            return None;
        };
        let Some(order) = value.get("order").and_then(Value::as_f64) else {
            self.warn_malformed_group_id(id);
            return None;
        };
        if !order.is_finite() {
            self.warn_malformed_group_id(id);
            return None;
        }
        let Some(created_at) = value.get("createdAt").and_then(Value::as_str) else {
            self.warn_malformed_group_id(id);
            return None;
        };
        let Some(updated_at) = value.get("updatedAt").and_then(Value::as_str) else {
            self.warn_malformed_group_id(id);
            return None;
        };
        let color = match normalize_group_color(raw_color) {
            Ok(color) => color,
            Err(_) => {
                self.warn_once(
                    format!("unknown-group-color:{id}\0{raw_color}"),
                    format!(
                        "Session group \"{name}\" (id: {id}) uses unsupported color \"{raw_color}\"; using \"blue\""
                    ),
                );
                FALLBACK_GROUP_COLOR.clone()
            }
        };
        Some(SessionGroup {
            id: id.to_string(),
            name: name.to_string(),
            color,
            order,
            created_at: created_at.to_string(),
            updated_at: updated_at.to_string(),
        })
    }

    fn write_store(&self, store: &StoreData) -> Result<(), SessionOrganizationError> {
        if *lock(&self.read_failed) {
            return Err(SessionOrganizationError::new(
                format!(
                    "Cannot update session organization store because it could not be read: {}. Delete the file to reset session organization (group/pin data will be lost), or restore it from backup.",
                    self.file_path.display()
                ),
                "session_organization_store_unreadable",
                None,
            ));
        }
        let mut root = Map::new();
        root.insert("schemaVersion".to_string(), Value::from(SCHEMA_VERSION));
        root.insert(
            "groups".to_string(),
            serde_json::to_value(sort_groups(store.groups.clone())).map_err(|error| {
                store_io_error(format!("could not serialize session groups: {error}"))
            })?,
        );
        root.insert(
            "sessions".to_string(),
            serde_json::to_value(&store.sessions).map_err(|error| {
                store_io_error(format!(
                    "could not serialize session organizations: {error}"
                ))
            })?,
        );
        let contents = serde_json::to_vec_pretty(&Value::Object(root)).map_err(|error| {
            store_io_error(format!(
                "could not serialize session organization store: {error}"
            ))
        })?;
        if contents.len() as u64 > MAX_STORE_BYTES {
            return Err(SessionOrganizationError::new(
                format!(
                    "session organization store exceeds the {MAX_STORE_BYTES}-byte output limit"
                ),
                "session_organization_store_limit_reached",
                None,
            ));
        }
        let parent = self
            .file_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::create_dir_all(parent).map_err(|error| store_io_error(error.to_string()))?;
        let options = AtomicWriteOptions {
            mode: Some(0o600),
            force_mode: true,
            ..AtomicWriteOptions::default()
        };
        atomic_write_file(&self.file_path, &contents, &options)
            .map_err(|error| store_io_error(error.to_string()))?;
        self.set_read_failed(false);
        Ok(())
    }

    fn unreadable(&self, reason: &str) -> StoreData {
        self.set_read_failed(true);
        self.warn_once(
            format!("unreadable-store:{reason}"),
            format!(
                "Failed to read session organization store at {}: {reason}",
                self.file_path.display()
            ),
        );
        StoreData::default()
    }

    fn set_read_failed(&self, failed: bool) {
        *lock(&self.read_failed) = failed;
    }

    fn warn_malformed_session(&self, session_id: &str) {
        self.warn_once(
            format!("malformed-session:{session_id}"),
            format!("Dropped malformed session organization entry: {session_id}"),
        );
    }

    fn warn_malformed_group(&self, value: &Value) {
        let id = value
            .as_object()
            .and_then(|object| object.get("id"))
            .and_then(Value::as_str)
            .unwrap_or("<unknown>");
        self.warn_malformed_group_id(id);
    }

    fn warn_malformed_group_id(&self, id: &str) {
        self.warn_once(
            format!("malformed-group:{id}"),
            format!("Dropped malformed session group entry: {id}"),
        );
    }

    fn warn_once(&self, key: String, message: String) {
        let should_warn = lock(&self.warning_keys).insert(key);
        if should_warn {
            self.warn(&message);
        }
    }

    fn warn(&self, message: &str) {
        if let Some(on_warning) = &self.on_warning {
            on_warning(message);
        }
    }
}

fn view_organization(organization: Option<&StoredOrganization>) -> SessionOrganizationView {
    let group_id = organization.and_then(|organization| organization.group_id.clone());
    let color = organization.and_then(|organization| organization.color);
    let pinned_at = organization.and_then(|organization| organization.pinned_at.clone());
    let updated_at = organization
        .map(|organization| organization.updated_at.clone())
        .unwrap_or_else(|| EPOCH_ISO.to_string());
    SessionOrganizationView {
        group_id,
        color,
        is_pinned: pinned_at.is_some(),
        pinned_at,
        updated_at,
    }
}

fn normalize_group_name(name: &str) -> Result<String, SessionOrganizationError> {
    let trimmed = trim_js_whitespace(name);
    if trimmed.is_empty()
        || trimmed.encode_utf16().count() > MAX_GROUP_NAME_LENGTH
        || has_control_character(trimmed)
    {
        return Err(SessionOrganizationError::new(
            "`name` must be 1-64 characters and contain no control characters",
            "invalid_group_name",
            Some("name"),
        ));
    }
    Ok(trimmed.to_string())
}

fn normalize_group_color(color: &str) -> Result<SessionGroupColor, SessionOrganizationError> {
    let normalized = trim_js_whitespace(color);
    if let Some(preset) = SessionGroupPresetColor::parse(normalized) {
        return Ok(SessionGroupColor::Preset(preset));
    }
    let bytes = normalized.as_bytes();
    if bytes.len() == 7 && bytes[0] == b'#' && bytes[1..].iter().all(u8::is_ascii_hexdigit) {
        return Ok(SessionGroupColor::Hex(normalized.to_ascii_lowercase()));
    }
    Err(SessionOrganizationError::new(
        "`color` must be a supported preset or a #RRGGBB hex value",
        "invalid_group_color",
        Some("color"),
    ))
}

fn normalize_order(order: i64) -> Result<f64, SessionOrganizationError> {
    if order.unsigned_abs() > 9_007_199_254_740_991_u64 {
        return Err(SessionOrganizationError::new(
            "`order` must be a safe integer",
            "invalid_group_order",
            Some("order"),
        ));
    }
    Ok(order as f64)
}

fn assert_group_name_available(
    groups: &[SessionGroup],
    name: &str,
    except_group_id: Option<&str>,
) -> Result<(), SessionOrganizationError> {
    let key = group_name_key(name);
    if groups.iter().any(|group| {
        Some(group.id.as_str()) != except_group_id && group_name_key(&group.name) == key
    }) {
        return Err(SessionOrganizationError::new(
            format!("Group name already exists: {name}"),
            "group_name_conflict",
            Some("name"),
        ));
    }
    Ok(())
}

fn group_name_key(name: &str) -> String {
    trim_js_whitespace(name).to_lowercase()
}

fn sort_groups(mut groups: Vec<SessionGroup>) -> Vec<SessionGroup> {
    groups.sort_by(|a, b| {
        a.order
            .partial_cmp(&b.order)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.id.cmp(&b.id))
    });
    groups
}

fn dedupe_groups<F>(groups: Vec<SessionGroup>, mut warn: F) -> Vec<SessionGroup>
where
    F: FnMut(&str),
{
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(groups.len());
    for group in groups {
        let key = group_name_key(&group.name);
        if !seen.insert(key) {
            warn(&format!(
                "Dropped duplicate session group by name: \"{}\" (id: {})",
                group.name, group.id
            ));
            continue;
        }
        deduped.push(group);
    }
    deduped
}

fn trim_js_whitespace(value: &str) -> &str {
    value.trim_matches(|character: char| character.is_whitespace() || character == '\u{feff}')
}

fn has_control_character(value: &str) -> bool {
    value.chars().any(|character| {
        let code_point = character as u32;
        (0x00..=0x1f).contains(&code_point)
            || (0x7f..=0x9f).contains(&code_point)
            || (0x200b..=0x200f).contains(&code_point)
            || (0x202a..=0x202e).contains(&code_point)
            || (0x2066..=0x2069).contains(&code_point)
            || code_point == 0xfeff
    })
}

fn validate_id(value: &str, field: &'static str) -> Result<(), SessionOrganizationError> {
    if value.len() > MAX_ID_BYTES {
        return Err(invalid_id_error(field));
    }
    Ok(())
}

fn invalid_id_error(field: &'static str) -> SessionOrganizationError {
    SessionOrganizationError::new(
        format!("`{field}` exceeds the {MAX_ID_BYTES}-byte input limit"),
        "invalid_session_id",
        Some(field),
    )
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn store_io_error(message: String) -> SessionOrganizationError {
    SessionOrganizationError::new(message, "session_organization_store_io", None)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
