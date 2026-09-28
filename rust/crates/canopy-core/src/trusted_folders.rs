//! Workspace trust rules and their path-precedence contract.
//!
//! This ports the read and decision path from `trustedFolders.ts`,
//! `path-comparison.ts`, and `trust-precedence.ts`. The settings UI's locked,
//! comment-preserving write path is a separate porting slice.

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, FileTimes, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::jsonc::{parse_jsonc_object, strip_json_comments, update_jsonc_object_content};
use crate::storage::{Storage, normalize_absolute};

pub const TRUSTED_FOLDERS_FILENAME: &str = "trustedFolders.json";
const MAX_TRUSTED_FOLDERS_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TrustLevel {
    TrustFolder,
    TrustParent,
    DoNotTrust,
}

impl TrustLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TrustFolder => "TRUST_FOLDER",
            Self::TrustParent => "TRUST_PARENT",
            Self::DoNotTrust => "DO_NOT_TRUST",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "TRUST_FOLDER" => Some(Self::TrustFolder),
            "TRUST_PARENT" => Some(Self::TrustParent),
            "DO_NOT_TRUST" => Some(Self::DoNotTrust),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustRule {
    pub path: PathBuf,
    pub trust_level: TrustLevel,
}

pub type TrustedFoldersConfig = IndexMap<String, Value>;

#[derive(Clone, Debug, PartialEq)]
pub struct TrustedFoldersFile {
    pub path: PathBuf,
    pub config: TrustedFoldersConfig,
}

#[derive(Debug, Error)]
pub enum TrustedFoldersError {
    #[error("Error in {path}: {message}\nPlease fix the configuration file and try again.")]
    Invalid { path: PathBuf, message: String },
    #[error("Error in {path}: {source}\nPlease fix the configuration file and try again.")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

type TrustChangeListener = Arc<dyn Fn() + Send + Sync + 'static>;

#[derive(Default)]
struct TrustChangeListeners {
    next_id: AtomicU64,
    listeners: Mutex<HashMap<u64, TrustChangeListener>>,
}

/// Cached in-memory trust configuration with source-compatible mutation and
/// change notifications. File writes are committed before the cache changes.
pub struct LoadedTrustedFolders {
    user: Mutex<TrustedFoldersFile>,
    errors: Vec<TrustedFoldersError>,
    listeners: Arc<TrustChangeListeners>,
}

pub struct TrustedFoldersSubscription {
    listeners: Weak<TrustChangeListeners>,
    id: u64,
}

impl Drop for TrustedFoldersSubscription {
    fn drop(&mut self) {
        if let Some(listeners) = self.listeners.upgrade() {
            listeners
                .listeners
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&self.id);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceTrustState {
    Trusted,
    Untrusted,
    Unknown,
}

impl WorkspaceTrustState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trusted => "trusted",
            Self::Untrusted => "untrusted",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceTrustSource {
    Disabled,
    Ide,
    File,
    None,
}

impl WorkspaceTrustSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Ide => "ide",
            Self::File => "file",
            Self::None => "none",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceTrustStatus {
    pub v: u8,
    pub workspace_cwd: String,
    pub folder_trust_enabled: bool,
    pub effective: EffectiveTrust,
    pub explicit_trust_level: Option<TrustLevel>,
    pub requires_daemon_restart_for_changes: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct EffectiveTrust {
    pub state: WorkspaceTrustState,
    pub source: WorkspaceTrustSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrustResult {
    pub is_trusted: Option<bool>,
    pub source: Option<WorkspaceTrustSource>,
}

#[derive(Clone, Debug)]
struct PrecedenceRule {
    trusted: bool,
    variants: BTreeSet<PathBuf>,
    payload: TrustLevel,
}

/// Select the configured trusted-folders file. The environment override is a
/// complete path; otherwise it follows Canopy's global config directory.
pub fn get_trusted_folders_path() -> PathBuf {
    if let Some(path) =
        std::env::var_os("CANOPY_CODE_TRUSTED_FOLDERS_PATH").filter(|path| !path.is_empty())
    {
        return PathBuf::from(path);
    }
    Storage::get_global_canopy_dir().join(TRUSTED_FOLDERS_FILENAME)
}

/// Read rules from a trusted-folders file. Missing files mean that there are
/// no explicit rules, matching the source loader. Unknown trust-level strings
/// are ignored by rule construction, as in the TypeScript `switch` default.
pub fn load_trusted_folders(path: &Path) -> Result<Vec<TrustRule>, TrustedFoldersError> {
    Ok(config_rules(&read_config_for_load(path)?))
}

impl LoadedTrustedFolders {
    /// Load the cached trust file. Missing files create an empty config;
    /// read/parse errors are retained and exposed without crashing the CLI.
    pub fn load(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let (config, errors) = match read_config_for_load(&path) {
            Ok(config) => (config, Vec::new()),
            Err(error) => (TrustedFoldersConfig::new(), vec![error]),
        };
        Self {
            user: Mutex::new(TrustedFoldersFile { path, config }),
            errors,
            listeners: Arc::new(TrustChangeListeners::default()),
        }
    }

    pub fn user(&self) -> TrustedFoldersFile {
        self.user
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn errors(&self) -> &[TrustedFoldersError] {
        &self.errors
    }

    pub fn rules(&self) -> Vec<TrustRule> {
        config_rules(
            &self
                .user
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .config,
        )
    }

    pub fn is_path_trusted(&self, location: &Path, cwd: &Path) -> Option<bool> {
        let rules = self.rules();
        resolve_trust_decision(&rules, location, cwd)
    }

    /// Update one rule after re-reading disk under the cross-process lock.
    /// The in-memory config changes only after the atomic replacement succeeds.
    pub fn set_value(
        &self,
        path: &str,
        trust_level: TrustLevel,
    ) -> Result<(), TrustedFoldersError> {
        let mut user = self
            .user
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let committed = write_trusted_folders(&user.path, |mut disk_config| {
            disk_config.insert(
                path.to_owned(),
                Value::String(trust_level.as_str().to_owned()),
            );
            disk_config
        })?;
        user.config = committed;
        drop(user);
        self.notify_changed();
        Ok(())
    }

    pub fn on_changed(
        &self,
        listener: impl Fn() + Send + Sync + 'static,
    ) -> TrustedFoldersSubscription {
        let id = self.listeners.next_id.fetch_add(1, Ordering::Relaxed);
        self.listeners
            .listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, Arc::new(listener));
        TrustedFoldersSubscription {
            listeners: Arc::downgrade(&self.listeners),
            id,
        }
    }

    fn notify_changed(&self) {
        let listeners = self
            .listeners
            .listeners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for listener in listeners {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| listener()));
        }
    }
}

/// Synchronize a trusted-folders file to the provided config. Disk-only keys
/// are removed, matching the source `saveTrustedFolders` API.
pub fn save_trusted_folders(
    trusted_folders: &TrustedFoldersFile,
) -> Result<(), TrustedFoldersError> {
    write_trusted_folders(&trusted_folders.path, |_| trusted_folders.config.clone()).map(|_| ())
}

fn config_rules(config: &TrustedFoldersConfig) -> Vec<TrustRule> {
    config
        .iter()
        .filter_map(|(path, value)| {
            let level = value.as_str().and_then(TrustLevel::parse)?;
            Some(TrustRule {
                path: PathBuf::from(path),
                trust_level: level,
            })
        })
        .collect()
}

fn read_config_for_load(path: &Path) -> Result<TrustedFoldersConfig, TrustedFoldersError> {
    let contents = match read_trusted_folders_contents(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(TrustedFoldersConfig::new());
        }
        Err(source) => {
            return Err(TrustedFoldersError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let parsed =
        serde_json::from_str::<Value>(&strip_json_comments(&contents)).map_err(|error| {
            TrustedFoldersError::Invalid {
                path: path.to_path_buf(),
                message: error.to_string(),
            }
        })?;
    let Some(object) = parsed.as_object() else {
        return Err(TrustedFoldersError::Invalid {
            path: path.to_path_buf(),
            message: "Trusted folders file is not a valid JSON object.".to_owned(),
        });
    };
    Ok(object
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect())
}

fn write_trusted_folders(
    path: &Path,
    update: impl FnOnce(TrustedFoldersConfig) -> TrustedFoldersConfig,
) -> Result<TrustedFoldersConfig, TrustedFoldersError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|source| TrustedFoldersError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let _lock = TrustedFoldersLock::acquire(path).map_err(|source| TrustedFoldersError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    let original = match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
                return Err(TrustedFoldersError::Invalid {
                    path: path.to_path_buf(),
                    message: "Trusted folders path must be a regular file.".to_owned(),
                });
            }
            read_trusted_folders_contents(path).map_err(|source| TrustedFoldersError::Io {
                path: path.to_path_buf(),
                source,
            })?
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => "{}".to_owned(),
        Err(source) => {
            return Err(TrustedFoldersError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };

    let object = parse_jsonc_object(&original).map_err(|error| {
        let message = match error {
            crate::jsonc::JsoncError::RootNotObject => {
                "Trusted folders file is not a valid JSON object.".to_owned()
            }
            crate::jsonc::JsoncError::Parse(message) => message,
        };
        TrustedFoldersError::Invalid {
            path: path.to_path_buf(),
            message,
        }
    })?;
    let disk_config = object.into_iter().collect::<TrustedFoldersConfig>();
    validate_trusted_folders_config(path, &disk_config)?;
    let next_config = update(disk_config);
    validate_trusted_folders_config(path, &next_config)?;
    let target = next_config
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Map<_, _>>();
    let content = update_jsonc_object_content(&original, &target).map_err(|error| {
        TrustedFoldersError::Invalid {
            path: path.to_path_buf(),
            message: error.to_string(),
        }
    })?;
    if content.len() as u64 > MAX_TRUSTED_FOLDERS_BYTES {
        return Err(TrustedFoldersError::Invalid {
            path: path.to_path_buf(),
            message: format!(
                "Trusted folders file exceeds the {MAX_TRUSTED_FOLDERS_BYTES}-byte limit."
            ),
        });
    }
    let reparsed = parse_jsonc_object(&content).map_err(|error| TrustedFoldersError::Invalid {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    if !jsonc_objects_equal(&reparsed, &target) {
        return Err(TrustedFoldersError::Invalid {
            path: path.to_path_buf(),
            message: "Edited JSONC does not match the intended trusted folders.".to_owned(),
        });
    }
    atomic_write_trusted_folders(path, content.as_bytes()).map_err(|source| {
        TrustedFoldersError::Io {
            path: path.to_path_buf(),
            source,
        }
    })?;
    Ok(next_config)
}

fn read_trusted_folders_contents(path: &Path) -> io::Result<String> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    if metadata.len() > MAX_TRUSTED_FOLDERS_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("trusted folders file exceeds the {MAX_TRUSTED_FOLDERS_BYTES}-byte limit"),
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_TRUSTED_FOLDERS_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_TRUSTED_FOLDERS_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("trusted folders file exceeds the {MAX_TRUSTED_FOLDERS_BYTES}-byte limit"),
        ));
    }
    String::from_utf8(bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn validate_trusted_folders_config(
    path: &Path,
    config: &TrustedFoldersConfig,
) -> Result<(), TrustedFoldersError> {
    for (rule_path, trust_level) in config {
        if trust_level.as_str().and_then(TrustLevel::parse).is_none() {
            return Err(TrustedFoldersError::Invalid {
                path: path.to_path_buf(),
                message: format!(
                    "Invalid trusted folder rule for {}.",
                    serde_json::to_string(rule_path).unwrap_or_else(|_| "\"\"".to_owned())
                ),
            });
        }
    }
    Ok(())
}

fn jsonc_objects_equal(left: &Map<String, Value>, right: &Map<String, Value>) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .all(|(key, value)| right.get(key).is_some_and(|other| other == value))
}

struct TrustedFoldersLock {
    path: PathBuf,
    identity: LockIdentity,
    stop: Option<std::sync::mpsc::Sender<()>>,
    heartbeat: Option<JoinHandle<()>>,
}

#[derive(Clone, Copy)]
struct LockIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl LockIdentity {
    fn read(metadata: &fs::Metadata) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            Self {}
        }
    }

    fn matches(self, metadata: &fs::Metadata) -> bool {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            self.device == metadata.dev() && self.inode == metadata.ino()
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            true
        }
    }
}

impl TrustedFoldersLock {
    const STALE_AFTER: Duration = Duration::from_secs(10);
    const HEARTBEAT_EVERY: Duration = Duration::from_secs(3);

    fn acquire(file_path: &Path) -> io::Result<Self> {
        let lock_path = append_path_suffix(file_path, ".lock");
        create_lock_directory(&lock_path)?;
        let metadata = fs::symlink_metadata(&lock_path)?;
        if !metadata.file_type().is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "trusted folders lock path is not a directory",
            ));
        }
        let identity = LockIdentity::read(&metadata);
        touch_lock_directory(&lock_path)?;

        let heartbeat_path = lock_path.clone();
        let (stop, receiver) = std::sync::mpsc::channel();
        let heartbeat = thread::spawn(move || {
            loop {
                match receiver.recv_timeout(Self::HEARTBEAT_EVERY) {
                    Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(error) = touch_lock_directory(&heartbeat_path) {
                            eprintln!("trusted folders lock heartbeat failed: {error}");
                            break;
                        }
                    }
                }
            }
        });
        Ok(Self {
            path: lock_path,
            identity,
            stop: Some(stop),
            heartbeat: Some(heartbeat),
        })
    }
}

impl Drop for TrustedFoldersLock {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(heartbeat) = self.heartbeat.take() {
            let _ = heartbeat.join();
        }
        if let Ok(metadata) = fs::symlink_metadata(&self.path) {
            if metadata.file_type().is_dir() && self.identity.matches(&metadata) {
                let _ = fs::remove_dir(&self.path);
            }
        }
    }
}

fn create_lock_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        match builder.create(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(path)?;
                if !metadata.file_type().is_dir() {
                    return Err(error);
                }
                let modified = metadata.modified()?;
                if !modified
                    .elapsed()
                    .is_ok_and(|age| age > TrustedFoldersLock::STALE_AFTER)
                {
                    return Err(error);
                }
                let current = fs::symlink_metadata(path)?;
                if LockIdentity::read(&metadata).matches(&current)
                    && current.modified()? == modified
                    && modified
                        .elapsed()
                        .is_ok_and(|age| age > TrustedFoldersLock::STALE_AFTER)
                {
                    fs::remove_dir(path)?;
                    builder.create(path)?;
                    return Ok(());
                }
                Err(error)
            }
            Err(error) => Err(error),
        }
    }
    #[cfg(not(unix))]
    {
        match fs::create_dir(path) {
            Ok(()) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

fn touch_lock_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.set_times(FileTimes::new().set_modified(SystemTime::now()))
}

fn append_path_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn atomic_write_trusted_folders(path: &Path, contents: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "trusted folders path has no filename",
        )
    })?;
    let mut temporary = None;
    for _ in 0..8 {
        let mut temp_name = std::ffi::OsString::from(".");
        temp_name.push(file_name);
        temp_name.push(format!(".tmp-{}", uuid::Uuid::new_v4().simple()));
        let temporary_path = parent.join(temp_name);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        match options.open(&temporary_path) {
            Ok(file) => {
                temporary = Some((temporary_path, file));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    let (temporary_path, mut file) = temporary.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a trusted-folders temporary file",
        )
    })?;
    let result = (|| {
        file.write_all(contents)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        file.sync_all()?;
        drop(file);
        match fs::symlink_metadata(path) {
            Ok(metadata)
                if metadata.file_type().is_symlink() || !metadata.file_type().is_file() =>
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "trusted folders path must be a regular file",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        fs::rename(&temporary_path, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

/// Path variants mirror lexical `path.resolve` and `realpathSync` comparisons.
pub fn get_path_comparison_variants(raw_path: &Path, cwd: &Path) -> BTreeSet<PathBuf> {
    let mut variants = BTreeSet::new();
    let lexical = normalize_absolute(raw_path, cwd);
    variants.insert(lexical.clone());
    if let Ok(real) = std::fs::canonicalize(&lexical) {
        variants.insert(normalize_absolute(&real, cwd));
    }
    variants
}

pub fn are_paths_equivalent(left: &Path, right: &Path, cwd: &Path) -> bool {
    let left = get_path_comparison_variants(left, cwd);
    let right = get_path_comparison_variants(right, cwd);
    left.iter().any(|path| right.contains(path))
}

fn path_depth(path: &Path) -> usize {
    path.components()
        .filter(|component| matches!(component, Component::Normal(_)))
        .count()
}

fn parent_directory(path: &Path) -> PathBuf {
    if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            parent.to_path_buf()
        }
    } else if path.is_absolute() {
        path.to_path_buf()
    } else {
        PathBuf::from(".")
    }
}

fn build_precedence_rules(rules: &[TrustRule], cwd: &Path) -> Vec<PrecedenceRule> {
    rules
        .iter()
        .map(|rule| {
            let rule_path = if rule.trust_level == TrustLevel::TrustParent {
                parent_directory(&rule.path)
            } else {
                rule.path.clone()
            };
            PrecedenceRule {
                trusted: rule.trust_level != TrustLevel::DoNotTrust,
                variants: get_path_comparison_variants(&rule_path, cwd),
                payload: rule.trust_level,
            }
        })
        .collect()
}

fn matching_depth(rule: &PrecedenceRule, location_variants: &BTreeSet<PathBuf>) -> Option<usize> {
    let mut deepest = None;
    for location in location_variants {
        for root in &rule.variants {
            if location.starts_with(root) {
                let depth = path_depth(root);
                deepest = Some(deepest.map_or(depth, |current: usize| current.max(depth)));
            }
        }
    }
    deepest
}

fn resolve_trust_rule<'a>(
    rules: &'a [PrecedenceRule],
    location_variants: &BTreeSet<PathBuf>,
) -> Option<&'a PrecedenceRule> {
    let mut winner: Option<&PrecedenceRule> = None;
    let mut winner_depth = 0;
    for rule in rules {
        let Some(depth) = matching_depth(rule, location_variants) else {
            continue;
        };
        if winner.is_none()
            || depth > winner_depth
            || (depth == winner_depth && !rule.trusted && winner.is_some_and(|item| item.trusted))
        {
            winner = Some(rule);
            winner_depth = depth;
        }
    }
    winner
}

pub fn resolve_trust_decision(rules: &[TrustRule], location: &Path, cwd: &Path) -> Option<bool> {
    let variants = get_path_comparison_variants(location, cwd);
    let built = build_precedence_rules(rules, cwd);
    resolve_trust_rule(&built, &variants).map(|rule| rule.trusted)
}

pub fn get_explicit_trust_level(
    rules: &[TrustRule],
    workspace_cwd: &Path,
    process_cwd: &Path,
) -> Option<TrustLevel> {
    let variants = get_path_comparison_variants(workspace_cwd, process_cwd);
    let built = build_precedence_rules(rules, process_cwd);
    resolve_trust_rule(&built, &variants).map(|rule| rule.payload)
}

pub fn get_workspace_trust_status(
    folder_trust_enabled: bool,
    workspace_cwd: &Path,
    rules: &[TrustRule],
    ide_trust: Option<bool>,
    process_cwd: &Path,
) -> WorkspaceTrustStatus {
    if !folder_trust_enabled {
        return WorkspaceTrustStatus {
            v: 1,
            workspace_cwd: workspace_cwd.to_string_lossy().into_owned(),
            folder_trust_enabled: false,
            effective: EffectiveTrust {
                state: WorkspaceTrustState::Trusted,
                source: WorkspaceTrustSource::Disabled,
            },
            explicit_trust_level: None,
            requires_daemon_restart_for_changes: true,
        };
    }

    if let Some(trusted) =
        ide_trust.filter(|_| are_paths_equivalent(workspace_cwd, process_cwd, process_cwd))
    {
        return WorkspaceTrustStatus {
            v: 1,
            workspace_cwd: workspace_cwd.to_string_lossy().into_owned(),
            folder_trust_enabled: true,
            effective: EffectiveTrust {
                state: if trusted {
                    WorkspaceTrustState::Trusted
                } else {
                    WorkspaceTrustState::Untrusted
                },
                source: WorkspaceTrustSource::Ide,
            },
            explicit_trust_level: None,
            requires_daemon_restart_for_changes: true,
        };
    }

    let decision = resolve_trust_decision(rules, workspace_cwd, process_cwd);
    WorkspaceTrustStatus {
        v: 1,
        workspace_cwd: workspace_cwd.to_string_lossy().into_owned(),
        folder_trust_enabled: true,
        effective: EffectiveTrust {
            state: match decision {
                Some(true) => WorkspaceTrustState::Trusted,
                Some(false) => WorkspaceTrustState::Untrusted,
                None => WorkspaceTrustState::Unknown,
            },
            source: if decision.is_some() {
                WorkspaceTrustSource::File
            } else {
                WorkspaceTrustSource::None
            },
        },
        explicit_trust_level: get_explicit_trust_level(rules, workspace_cwd, process_cwd),
        requires_daemon_restart_for_changes: true,
    }
}

pub fn trust_result(status: &WorkspaceTrustStatus) -> TrustResult {
    if status.effective.source == WorkspaceTrustSource::Disabled {
        return TrustResult {
            is_trusted: Some(true),
            source: None,
        };
    }
    TrustResult {
        is_trusted: match status.effective.state {
            WorkspaceTrustState::Trusted => Some(true),
            WorkspaceTrustState::Untrusted => Some(false),
            WorkspaceTrustState::Unknown => None,
        },
        source: match status.effective.source {
            WorkspaceTrustSource::Ide | WorkspaceTrustSource::File => Some(status.effective.source),
            WorkspaceTrustSource::Disabled | WorkspaceTrustSource::None => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn temporary_trust_file() -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "canopy-trusted-folders-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&root).unwrap();
        (root.clone(), root.join(TRUSTED_FOLDERS_FILENAME))
    }

    fn rule(path: &str, trust_level: TrustLevel) -> TrustRule {
        TrustRule {
            path: PathBuf::from(path),
            trust_level,
        }
    }

    #[test]
    fn deepest_rule_wins_independent_of_insertion_order() {
        let root = rule("/projects", TrustLevel::TrustFolder);
        let child = rule("/projects/evil", TrustLevel::DoNotTrust);
        for rules in [
            vec![root.clone(), child.clone()],
            vec![child.clone(), root.clone()],
        ] {
            assert_eq!(
                resolve_trust_decision(&rules, Path::new("/projects/evil/src"), Path::new("/")),
                Some(false)
            );
        }
        assert_eq!(
            resolve_trust_decision(
                &[
                    rule("/projects", TrustLevel::DoNotTrust),
                    rule("/projects/good", TrustLevel::TrustFolder)
                ],
                Path::new("/projects/good/src"),
                Path::new("/")
            ),
            Some(true)
        );
    }

    #[test]
    fn untrusted_rule_wins_a_same_depth_tie() {
        assert_eq!(
            resolve_trust_decision(
                &[
                    rule("/projects/evil", TrustLevel::TrustFolder),
                    rule("/projects/evil", TrustLevel::DoNotTrust)
                ],
                Path::new("/projects/evil/src"),
                Path::new("/")
            ),
            Some(false)
        );
    }

    #[test]
    fn trust_parent_applies_to_the_containing_directory() {
        let rules = [
            rule("/projects", TrustLevel::DoNotTrust),
            rule("/projects/good/marker", TrustLevel::TrustParent),
        ];
        assert_eq!(
            resolve_trust_decision(&rules, Path::new("/projects/good/src"), Path::new("/")),
            Some(true)
        );
        assert_eq!(
            resolve_trust_decision(&rules, Path::new("/projects/other/src"), Path::new("/")),
            Some(false)
        );
    }

    #[test]
    fn unknown_is_distinct_from_untrusted_and_disabled_is_trusted() {
        let rules = [rule("/projects/blocked", TrustLevel::DoNotTrust)];
        let cwd = Path::new("/");
        let unknown = get_workspace_trust_status(true, Path::new("/other"), &rules, None, cwd);
        assert_eq!(unknown.effective.state, WorkspaceTrustState::Unknown);
        assert_eq!(trust_result(&unknown).is_trusted, None);
        let disabled = get_workspace_trust_status(false, Path::new("/other"), &rules, None, cwd);
        assert_eq!(disabled.effective.source, WorkspaceTrustSource::Disabled);
        assert_eq!(trust_result(&disabled).is_trusted, Some(true));
    }

    #[test]
    fn ide_trust_overrides_file_rules_only_for_the_process_workspace() {
        let rules = [rule("/project", TrustLevel::DoNotTrust)];
        let status = get_workspace_trust_status(
            true,
            Path::new("/project"),
            &rules,
            Some(true),
            Path::new("/project"),
        );
        assert_eq!(status.effective.source, WorkspaceTrustSource::Ide);
        assert_eq!(status.effective.state, WorkspaceTrustState::Trusted);

        let other = get_workspace_trust_status(
            true,
            Path::new("/project"),
            &rules,
            Some(true),
            Path::new("/other"),
        );
        assert_eq!(other.effective.source, WorkspaceTrustSource::File);
        assert_eq!(other.effective.state, WorkspaceTrustState::Untrusted);
    }

    #[cfg(unix)]
    #[test]
    fn canonical_path_variants_make_symlink_aliases_match() {
        use std::os::unix::fs::symlink;

        let scratch = std::env::temp_dir().join(format!("canopy-trust-{}", uuid::Uuid::new_v4()));
        let real = scratch.join("real");
        let alias = scratch.join("alias");
        std::fs::create_dir_all(&real).unwrap();
        symlink(&real, &alias).unwrap();
        let rules = [TrustRule {
            path: alias.clone(),
            trust_level: TrustLevel::DoNotTrust,
        }];
        assert_eq!(resolve_trust_decision(&rules, &real, &scratch), Some(false));
        std::fs::remove_dir_all(scratch).unwrap();
    }

    #[test]
    fn loader_accepts_jsonc_and_rejects_non_object_roots() {
        let file =
            std::env::temp_dir().join(format!("canopy-trusted-{}.json", uuid::Uuid::new_v4()));
        std::fs::write(&file, "{\n // project\n \"/project\": \"TRUST_FOLDER\"\n}").unwrap();
        let rules = load_trusted_folders(&file).unwrap();
        assert_eq!(rules, [rule("/project", TrustLevel::TrustFolder)]);
        std::fs::write(&file, "[]").unwrap();
        assert!(matches!(
            load_trusted_folders(&file),
            Err(TrustedFoldersError::Invalid { .. })
        ));
        std::fs::remove_file(file).unwrap();
    }

    #[test]
    fn set_value_preserves_comments_and_commits_cache_after_write() {
        let (root, file) = temporary_trust_file();
        std::fs::write(
            &file,
            "{\n  // existing project\n  \"/existing\": \"TRUST_FOLDER\"\n}\n",
        )
        .unwrap();
        let loaded = LoadedTrustedFolders::load(&file);
        assert!(loaded.errors().is_empty());
        let called = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let called_by_listener = Arc::clone(&called);
        let _subscription = loaded.on_changed(move || {
            called_by_listener.fetch_add(1, Ordering::Relaxed);
        });

        loaded.set_value("/new", TrustLevel::DoNotTrust).unwrap();

        let contents = std::fs::read_to_string(&file).unwrap();
        assert!(contents.contains("// existing project"));
        assert!(contents.contains("\"/existing\": \"TRUST_FOLDER\""));
        assert!(contents.contains("\"/new\": \"DO_NOT_TRUST\""));
        assert_eq!(loaded.user().config["/new"], json!("DO_NOT_TRUST"));
        assert_eq!(called.load(Ordering::Relaxed), 1);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn set_value_reloads_disk_rules_under_the_shared_lock() {
        let (root, file) = temporary_trust_file();
        let loaded = LoadedTrustedFolders::load(&file);
        std::fs::write(&file, "{\n  \"/external\": \"DO_NOT_TRUST\"\n}\n").unwrap();

        loaded.set_value("/new", TrustLevel::TrustParent).unwrap();

        let config = loaded.user().config;
        assert_eq!(config["/external"], json!("DO_NOT_TRUST"));
        assert_eq!(config["/new"], json!("TRUST_PARENT"));
        assert!(!file.with_file_name("trustedFolders.json.lock").exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn save_sync_removes_disk_only_rules_and_their_comments() {
        let (root, file) = temporary_trust_file();
        std::fs::write(
            &file,
            "{\n  // stale entry\n  \"/stale\": \"TRUST_FOLDER\",\n  // keep this entry\n  \"/keep\": \"TRUST_PARENT\"\n}",
        )
        .unwrap();
        save_trusted_folders(&TrustedFoldersFile {
            path: file.clone(),
            config: IndexMap::from([("/keep".to_owned(), json!("TRUST_PARENT"))]),
        })
        .unwrap();
        let contents = std::fs::read_to_string(&file).unwrap();
        assert!(!contents.contains("/stale"));
        assert!(!contents.contains("stale entry"));
        assert!(contents.contains("keep this entry"));
        assert_eq!(
            load_trusted_folders(&file).unwrap(),
            [rule("/keep", TrustLevel::TrustParent)]
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn malformed_or_invalid_disk_data_is_never_overwritten_or_cached() {
        let (root, file) = temporary_trust_file();
        let malformed = "{ // comment\n \"/broken\": \"TRUST_FOLDER\",, }";
        std::fs::write(&file, malformed).unwrap();
        let loaded = LoadedTrustedFolders::load(&file);
        assert_eq!(loaded.errors().len(), 1);
        assert!(loaded.set_value("/new", TrustLevel::TrustFolder).is_err());
        assert_eq!(loaded.user().config.len(), 0);
        assert_eq!(std::fs::read_to_string(&file).unwrap(), malformed);

        let invalid = r#"{"/invalid": "SOMETIMES"}"#;
        std::fs::write(&file, invalid).unwrap();
        let loaded = LoadedTrustedFolders::load(&file);
        assert!(loaded.set_value("/new", TrustLevel::TrustFolder).is_err());
        assert_eq!(std::fs::read_to_string(&file).unwrap(), invalid);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn oversized_config_is_rejected_before_reading_it_into_memory() {
        let (root, file) = temporary_trust_file();
        let disk = File::create(&file).unwrap();
        disk.set_len(MAX_TRUSTED_FOLDERS_BYTES + 1).unwrap();
        let loaded = LoadedTrustedFolders::load(&file);
        assert_eq!(loaded.errors().len(), 1);
        assert!(loaded.set_value("/new", TrustLevel::TrustFolder).is_err());
        assert_eq!(
            std::fs::metadata(&file).unwrap().len(),
            MAX_TRUSTED_FOLDERS_BYTES + 1
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn writer_refuses_symlinks_and_sets_owner_only_permissions() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let (root, file) = temporary_trust_file();
        let target = root.join("redirected.json");
        std::fs::write(&target, "{}\n").unwrap();
        symlink(&target, &file).unwrap();
        let error = save_trusted_folders(&TrustedFoldersFile {
            path: file.clone(),
            config: IndexMap::from([("/repo".to_owned(), json!("TRUST_FOLDER"))]),
        })
        .unwrap_err();
        assert!(error.to_string().contains("regular file"));
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "{}\n");
        std::fs::remove_file(&file).unwrap();

        save_trusted_folders(&TrustedFoldersFile {
            path: file.clone(),
            config: IndexMap::from([("/repo".to_owned(), json!("TRUST_FOLDER"))]),
        })
        .unwrap();
        assert_eq!(
            std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn lock_directory_blocks_a_second_writer_and_is_removed_on_release() {
        let (_root, file) = temporary_trust_file();
        let lock_path = append_path_suffix(&file, ".lock");
        let first = TrustedFoldersLock::acquire(&file).unwrap();
        assert!(lock_path.is_dir());
        assert!(TrustedFoldersLock::acquire(&file).is_err());
        drop(first);
        assert!(!lock_path.exists());
        std::fs::remove_dir_all(file.parent().unwrap()).unwrap();
    }
}
