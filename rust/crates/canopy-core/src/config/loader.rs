//! Four-scope settings loading and merge behavior.
//!
//! Source: `packages/cli/src/config/settings.ts`. Settings remain generic JSON
//! objects at runtime: Canopy warns about unknown keys but does not reject
//! schema-invalid values while loading the application.

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};
use thiserror::Error;

use crate::config::environment::{RuntimeEnvironmentSnapshot, load_environment};
use crate::config::migrations::{SETTINGS_VERSION, run_migrations, settings_need_migration};
use crate::config::schema::merge_strategy_for_path;
use crate::env_var_resolver::resolve_env_vars_in_object;
use crate::jsonc::{strip_json_comments, update_jsonc_content, update_jsonc_object_content};
use crate::settings_merge::custom_deep_merge;
use crate::storage::{Storage, normalize_absolute};
use crate::trusted_folders::{
    LoadedTrustedFolders, TRUSTED_FOLDERS_FILENAME, resolve_trust_decision,
};
use crate::utils::atomic_file_write::{AtomicWriteOptions, atomic_write_file};
use crate::utils::dotenv::{get_home_env_fallback_vars, parse_dotenv};

pub const SETTINGS_VERSION_KEY: &str = "$version";
pub const CORRUPTED_SUFFIX: &str = ".corrupted";
pub const ENV_CORRUPTED_PATH: &str = "CANOPY_CODE_SETTINGS_CORRUPTED_PATH";
pub const ENV_WAS_RECOVERED: &str = "CANOPY_CODE_SETTINGS_WAS_RECOVERED";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SettingScope {
    User,
    Workspace,
    System,
    SystemDefaults,
}

impl SettingScope {
    pub fn as_source_name(self) -> &'static str {
        match self {
            Self::User => "User",
            Self::Workspace => "Workspace",
            Self::System => "System",
            Self::SystemDefaults => "SystemDefaults",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SettingsFile {
    pub settings: Map<String, Value>,
    pub original_settings: Map<String, Value>,
    pub path: PathBuf,
    pub raw_json: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettingsError {
    pub message: String,
    pub path: PathBuf,
}

#[derive(Debug, Error)]
pub enum SettingsLoadError {
    #[error("{0}")]
    FatalConfig(String),
    #[error("{0}")]
    InvalidSettingsEnvironment(String),
}

#[derive(Clone, Debug)]
pub struct SettingsPaths {
    pub system: PathBuf,
    pub system_defaults: PathBuf,
    pub user: PathBuf,
    pub workspace: PathBuf,
    /// Optional `CANOPY_CODE_TRUSTED_FOLDERS_PATH`, resolved against the
    /// process working directory just like Node's filesystem APIs.
    pub trusted_folders_path: Option<PathBuf>,
    pub home_dir: PathBuf,
    pub workspace_dir: PathBuf,
    pub process_cwd: PathBuf,
}

impl SettingsPaths {
    pub fn from_environment(workspace_dir: impl Into<PathBuf>) -> Self {
        let mut process_env = env::vars().collect::<HashMap<_, _>>();
        let home_dir = home_dir_from_environment().unwrap_or_default();
        let process_cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        pre_resolve_home_env_overrides(&mut process_env, &home_dir, &process_cwd);
        Self::from_environment_with_env(workspace_dir, &process_env)
    }

    /// Resolves settings paths against an environment snapshot that has
    /// already had user-level home `.env` overrides pre-resolved.
    pub fn from_environment_with_env(
        workspace_dir: impl Into<PathBuf>,
        process_env: &HashMap<String, String>,
    ) -> Self {
        let workspace_dir = workspace_dir.into();
        let process_cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let system = process_env
            .get("CANOPY_CODE_SYSTEM_SETTINGS_PATH")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(default_system_settings_path);
        let system_defaults = process_env
            .get("CANOPY_CODE_SYSTEM_DEFAULTS_PATH")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                system
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join("system-defaults.json")
            });
        let trusted_folders_path = process_env
            .get("CANOPY_CODE_TRUSTED_FOLDERS_PATH")
            .filter(|value| !value.is_empty())
            .map(|value| normalize_absolute(Path::new(value), &process_cwd));
        let home_dir = home_dir_from_environment().unwrap_or_default();
        let global_dir = global_canopy_dir_from_environment(process_env, &home_dir, &process_cwd);
        let user = global_dir.join("settings.json");
        let workspace = Storage::new(workspace_dir.clone()).get_workspace_settings_path();
        Self {
            system,
            system_defaults,
            user,
            workspace,
            trusted_folders_path,
            home_dir,
            workspace_dir,
            process_cwd,
        }
    }

    pub fn for_test(
        system: impl Into<PathBuf>,
        system_defaults: impl Into<PathBuf>,
        user: impl Into<PathBuf>,
        workspace: impl Into<PathBuf>,
        home_dir: impl Into<PathBuf>,
        workspace_dir: impl Into<PathBuf>,
        process_cwd: impl Into<PathBuf>,
    ) -> Self {
        Self {
            system: system.into(),
            system_defaults: system_defaults.into(),
            user: user.into(),
            workspace: workspace.into(),
            trusted_folders_path: None,
            home_dir: home_dir.into(),
            workspace_dir: workspace_dir.into(),
            process_cwd: process_cwd.into(),
        }
    }
}

fn default_system_settings_path() -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        PathBuf::from("/Library/Application Support/CanopyCode/settings.json")
    }
    #[cfg(target_os = "windows")]
    {
        PathBuf::from(r"C:\ProgramData\canopy-code\settings.json")
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        PathBuf::from("/etc/canopy-code/settings.json")
    }
}

fn home_dir_from_environment() -> Option<PathBuf> {
    env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

const HOME_ENV_BOOTSTRAP_KEYS: &[&str] = &[
    "QWEN_HOME",
    "CANOPY_RUNTIME_DIR",
    "CANOPY_CODE_MCP_APPROVALS_PATH",
    "CANOPY_CODE_TRUSTED_FOLDERS_PATH",
];

/// Reads the bootstrap subset of user `.env` files before settings paths are
/// selected. This mirrors `preResolveHomeEnvOverrides`; it updates only the
/// caller's environment snapshot and never mutates global process state.
fn pre_resolve_home_env_overrides(
    process_env: &mut HashMap<String, String>,
    home_dir: &Path,
    process_cwd: &Path,
) {
    if HOME_ENV_BOOTSTRAP_KEYS
        .iter()
        .all(|key| process_env.get(*key).is_some_and(|value| !value.is_empty()))
    {
        return;
    }

    let initial_qwen_home = process_env.get("QWEN_HOME").cloned();
    let initial_global_dir = global_canopy_dir_from_environment(process_env, home_dir, process_cwd);
    let mut candidates = vec![initial_global_dir.join(".env")];
    if initial_qwen_home.as_deref().is_none_or(str::is_empty) {
        candidates.push(home_dir.join(".env"));
    }
    for candidate in candidates {
        read_home_env_overrides(&candidate, process_env);
    }

    let discovered_qwen_home = process_env
        .get("QWEN_HOME")
        .filter(|value| !value.is_empty());
    if discovered_qwen_home.is_some_and(|value| Some(value) != initial_qwen_home.as_ref()) {
        let discovered_global_dir =
            global_canopy_dir_from_environment(process_env, home_dir, process_cwd);
        if discovered_global_dir != initial_global_dir {
            read_home_env_overrides(&discovered_global_dir.join(".env"), process_env);
        }
    }
}

fn read_home_env_overrides(path: &Path, process_env: &mut HashMap<String, String>) {
    let Ok(bytes) = fs::read(path) else {
        return;
    };
    let parsed = parse_dotenv(&String::from_utf8_lossy(&bytes));
    for key in HOME_ENV_BOOTSTRAP_KEYS {
        if let Some(value) = parsed.get(*key).filter(|value| !value.is_empty()) {
            if !process_env.contains_key(*key) {
                process_env.insert((*key).to_owned(), value.clone());
            }
        }
    }
}

fn global_canopy_dir_from_environment(
    process_env: &HashMap<String, String>,
    home_dir: &Path,
    process_cwd: &Path,
) -> PathBuf {
    let configured = process_env
        .get("QWEN_HOME")
        .filter(|value| !value.is_empty());
    let base = configured
        .map(|value| expand_home(value, home_dir))
        .unwrap_or_else(|| {
            let base_home = if home_dir.as_os_str().is_empty() {
                env::temp_dir()
            } else {
                home_dir.to_owned()
            };
            base_home.join(crate::storage::CANOPY_DIR)
        });
    normalize_absolute(&base, process_cwd)
}

fn expand_home(value: &str, home_dir: &Path) -> PathBuf {
    if value == "~" {
        return home_dir.to_owned();
    }
    if let Some(tail) = value
        .strip_prefix("~/")
        .or_else(|| value.strip_prefix("~\\"))
    {
        return tail
            .split(['/', '\\'])
            .filter(|part| !part.is_empty())
            .fold(home_dir.to_owned(), |mut path, part| {
                path.push(part);
                path
            });
    }
    PathBuf::from(value)
}

#[derive(Clone, Debug)]
pub struct LoadSettingsOptions {
    /// Snapshot of process environment variables. Test callers can inject a
    /// deterministic environment; consumed recovery markers are removed here.
    pub process_env: HashMap<String, String>,
    /// Explicit `.env` fallback values. `None` reads the global Canopy `.env`
    /// and, unless QWEN_HOME redirects the home, `~/.env`.
    pub home_env_fallback: Option<HashMap<String, String>>,
    pub consume_corruption_env_vars: bool,
    pub skip_workspace_settings: bool,
    /// Skip `.env` and `settings.env` activation, matching `skipLoadEnvironment`.
    pub skip_load_environment: bool,
    /// When present, bypasses the trusted-folders file decision.
    pub workspace_trusted: Option<bool>,
}

impl Default for LoadSettingsOptions {
    fn default() -> Self {
        Self {
            process_env: env::vars().collect(),
            home_env_fallback: None,
            consume_corruption_env_vars: true,
            skip_workspace_settings: false,
            skip_load_environment: false,
            workspace_trusted: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LoadedSettings {
    pub system: SettingsFile,
    pub system_defaults: SettingsFile,
    pub user: SettingsFile,
    pub workspace: SettingsFile,
    pub is_trusted: bool,
    pub migrated_in_memory_scopes: HashSet<SettingScope>,
    pub migration_warnings: Vec<String>,
    pub corrupted_path: Option<PathBuf>,
    pub was_recovered: bool,
    pub workspace_settings_active: bool,
    pub settings_errors: Vec<SettingsError>,
    pub merged: Map<String, Value>,
    pub runtime_environment: RuntimeEnvironmentSnapshot,
}

impl LoadedSettings {
    pub fn for_scope(&self, scope: SettingScope) -> &SettingsFile {
        match scope {
            SettingScope::User => &self.user,
            SettingScope::Workspace => &self.workspace,
            SettingScope::System => &self.system,
            SettingScope::SystemDefaults => &self.system_defaults,
        }
    }

    pub fn recompute_merged(&mut self) {
        self.merged = merge_settings(
            &self.system.settings,
            &self.system_defaults.settings,
            &self.user.settings,
            &self.workspace.settings,
            self.is_trusted,
        );
    }
}

#[derive(Clone, Debug, Default)]
struct ScopeLoadResult {
    settings: Map<String, Value>,
    raw_json: Option<String>,
    migration_warnings: Vec<String>,
    corrupted_path: Option<PathBuf>,
    was_recovered: bool,
}

/// Loads settings using environment-derived paths.
pub fn load_settings(
    workspace_dir: impl Into<PathBuf>,
    options: &mut LoadSettingsOptions,
) -> Result<LoadedSettings, SettingsLoadError> {
    let home_dir = home_dir_from_environment().unwrap_or_default();
    let process_cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    pre_resolve_home_env_overrides(&mut options.process_env, &home_dir, &process_cwd);
    let paths = SettingsPaths::from_environment_with_env(workspace_dir, &options.process_env);
    load_settings_from_paths(&paths, options)
}

/// Loads system, system-default, user, and workspace files; resolves env
/// placeholders; evaluates workspace trust; and computes the merged settings.
pub fn load_settings_from_paths(
    paths: &SettingsPaths,
    options: &mut LoadSettingsOptions,
) -> Result<LoadedSettings, SettingsLoadError> {
    let mut errors = Vec::new();
    let mut system_result =
        load_and_migrate(&paths.system, SettingScope::System, options, &mut errors);
    let mut defaults_result = load_and_migrate(
        &paths.system_defaults,
        SettingScope::SystemDefaults,
        options,
        &mut errors,
    );
    let mut user_result = load_and_migrate(&paths.user, SettingScope::User, options, &mut errors);

    let resolved_workspace = resolve_real_or_absolute(&paths.workspace_dir, &paths.process_cwd);
    let resolved_home = paths
        .home_dir
        .canonicalize()
        .unwrap_or_else(|_| normalize_absolute(&paths.home_dir, &paths.process_cwd));
    let workspace_settings_active =
        !options.skip_workspace_settings && resolved_workspace != resolved_home;
    let mut workspace_result = if workspace_settings_active {
        load_and_migrate(
            &paths.workspace,
            SettingScope::Workspace,
            options,
            &mut errors,
        )
    } else {
        ScopeLoadResult::default()
    };

    let system_original = system_result.settings.clone();
    let defaults_original = defaults_result.settings.clone();
    let user_original = user_result.settings.clone();
    let workspace_original = workspace_result.settings.clone();

    let fallback_env = match options.home_env_fallback.clone() {
        Some(values) => values,
        None => read_home_env_fallback(paths, &options.process_env),
    };
    let mut resolution_env = options.process_env.clone();
    for (key, value) in fallback_env {
        resolution_env.entry(key).or_insert(value);
    }

    let mut system_settings = resolve_map(&system_result.settings, &resolution_env);
    let mut defaults_settings = resolve_map(&defaults_result.settings, &resolution_env);
    let mut user_settings = resolve_map(&user_result.settings, &resolution_env);
    let mut workspace_settings = resolve_map(&workspace_result.settings, &resolution_env);
    normalize_legacy_theme(&mut user_settings);
    normalize_legacy_theme(&mut workspace_settings);

    let initial_trust_settings = merge_settings(
        &system_settings,
        &Map::new(),
        &user_settings,
        &Map::new(),
        true,
    );
    let is_trusted = match options.workspace_trusted {
        Some(is_trusted) => is_trusted,
        None => resolve_workspace_trust(
            &initial_trust_settings,
            &resolved_workspace,
            &paths.process_cwd,
            &paths.user,
            paths.trusted_folders_path.as_deref(),
        )?
        .unwrap_or(true),
    };

    let merged = merge_settings(
        &system_settings,
        &defaults_settings,
        &user_settings,
        &workspace_settings,
        is_trusted,
    );

    let runtime_environment = if options.skip_load_environment {
        RuntimeEnvironmentSnapshot {
            effective_env: options.process_env.clone(),
            ..RuntimeEnvironmentSnapshot::default()
        }
    } else {
        load_environment(&merged, paths, options, Some(is_trusted))?
    };

    if !errors.is_empty() {
        let error_messages = errors
            .iter()
            .map(|error| format!("Error in {}: {}", error.path.display(), error.message))
            .collect::<Vec<_>>();
        return Err(SettingsLoadError::FatalConfig(format!(
            "{}\nPlease fix the configuration file(s) and try again.",
            error_messages.join("\n")
        )));
    }

    // Build the effective env map before returning, as source loadSettings()
    // applies `.env`/`settings.env` before its final aggregated parse-error
    // check. The Rust caller can pass this snapshot to spawned sessions.
    let mut migration_warnings = detect_canopy_home_redirect_without_migration(paths, options)
        .into_iter()
        .collect::<Vec<_>>();
    migration_warnings.append(&mut system_result.migration_warnings);
    migration_warnings.append(&mut defaults_result.migration_warnings);
    migration_warnings.append(&mut user_result.migration_warnings);
    // Preserve the source's migration warning scope order.
    migration_warnings.append(&mut workspace_result.migration_warnings);

    Ok(LoadedSettings {
        system: SettingsFile {
            settings: std::mem::take(&mut system_settings),
            original_settings: system_original,
            path: paths.system.clone(),
            raw_json: system_result.raw_json,
        },
        system_defaults: SettingsFile {
            settings: std::mem::take(&mut defaults_settings),
            original_settings: defaults_original,
            path: paths.system_defaults.clone(),
            raw_json: defaults_result.raw_json,
        },
        user: SettingsFile {
            settings: std::mem::take(&mut user_settings),
            original_settings: user_original,
            path: paths.user.clone(),
            raw_json: user_result.raw_json,
        },
        workspace: SettingsFile {
            settings: std::mem::take(&mut workspace_settings),
            original_settings: workspace_original,
            path: paths.workspace.clone(),
            raw_json: workspace_result.raw_json,
        },
        is_trusted,
        migrated_in_memory_scopes: HashSet::new(),
        migration_warnings,
        corrupted_path: user_result.corrupted_path,
        was_recovered: user_result.was_recovered,
        workspace_settings_active,
        settings_errors: errors,
        merged,
        runtime_environment,
    })
}

/// Four-tier precedence with trusted workspace filtering and source MCP
/// provenance stamping. Higher-priority scopes are later merge inputs.
pub fn merge_settings(
    system: &Map<String, Value>,
    system_defaults: &Map<String, Value>,
    user: &Map<String, Value>,
    workspace: &Map<String, Value>,
    is_trusted: bool,
) -> Map<String, Value> {
    let safe_workspace = if is_trusted {
        tag_mcp_server_scope(strip_workspace_security_bypasses(workspace), "workspace")
    } else {
        Map::new()
    };
    let system = tag_mcp_server_scope(system.clone(), "system");
    custom_deep_merge(
        merge_strategy_for_path,
        &[
            system_defaults.clone(),
            user.clone(),
            safe_workspace,
            system,
        ],
    )
}

fn load_and_migrate(
    path: &Path,
    scope: SettingScope,
    options: &mut LoadSettingsOptions,
    errors: &mut Vec<SettingsError>,
) -> ScopeLoadResult {
    let content = match fs::read(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return ScopeLoadResult::default();
        }
        Err(error) => {
            errors.push(SettingsError {
                message: error.to_string(),
                path: path.to_owned(),
            });
            return ScopeLoadResult::default();
        }
    };
    let content = String::from_utf8_lossy(&content).into_owned();
    let mut parsed = match serde_json::from_str::<Value>(&strip_json_comments(&content)) {
        Ok(value) => value,
        Err(_) => {
            let corrupted_path = PathBuf::from(format!("{}{CORRUPTED_SUFFIX}", path.display()));
            let corrupted_saved = fs::copy(path, &corrupted_path).is_ok();
            if corrupted_saved {
                let _ = write_atomic(path, b"{}");
            }
            return ScopeLoadResult {
                settings: Map::new(),
                raw_json: None,
                migration_warnings: Vec::new(),
                corrupted_path: corrupted_saved.then_some(corrupted_path),
                was_recovered: false,
            };
        }
    };
    let corrupted_path = PathBuf::from(format!("{}{CORRUPTED_SUFFIX}", path.display()));
    let mut corrupted_saved = false;
    let mut was_recovered = false;
    if options.consume_corruption_env_vars
        && scope == SettingScope::User
        && options
            .process_env
            .get(ENV_CORRUPTED_PATH)
            .is_some_and(|value| value == &corrupted_path.to_string_lossy())
    {
        corrupted_saved = true;
        was_recovered = options
            .process_env
            .get(ENV_WAS_RECOVERED)
            .is_some_and(|value| value == "1");
        options.process_env.remove(ENV_CORRUPTED_PATH);
        options.process_env.remove(ENV_WAS_RECOVERED);
    }

    let Some(object) = parsed.as_object() else {
        errors.push(SettingsError {
            message: "Settings file is not a valid JSON object.".to_owned(),
            path: path.to_owned(),
        });
        return ScopeLoadResult::default();
    };
    let mut settings = object.clone();

    let has_version = settings.contains_key(SETTINGS_VERSION_KEY);
    let version = settings.get(SETTINGS_VERSION_KEY).and_then(Value::as_f64);
    let invalid_version = has_version && version.is_none();
    let legacy_numeric_version = version.is_some_and(|version| version < SETTINGS_VERSION as f64);
    let migration_warnings;
    if settings_need_migration(&parsed) {
        let result = run_migrations(&parsed, scope.as_source_name());
        if !result.executed_migrations.is_empty() {
            parsed = result.settings;
            settings = parsed.as_object().cloned().unwrap_or_default();
            migration_warnings = result.warnings;
            persist_settings_object(path, &settings);
        } else if (legacy_numeric_version || invalid_version) && !corrupted_saved {
            settings.insert(
                SETTINGS_VERSION_KEY.to_owned(),
                Value::Number(SETTINGS_VERSION.into()),
            );
            migration_warnings = Vec::new();
            persist_settings_object(path, &settings);
        } else {
            migration_warnings = Vec::new();
        }
    } else if (!has_version || invalid_version || legacy_numeric_version) && !corrupted_saved {
        settings.insert(
            SETTINGS_VERSION_KEY.to_owned(),
            Value::Number(SETTINGS_VERSION.into()),
        );
        migration_warnings = Vec::new();
        persist_settings_object(path, &settings);
    } else {
        migration_warnings = Vec::new();
    }

    ScopeLoadResult {
        settings,
        raw_json: Some(content),
        migration_warnings,
        corrupted_path: corrupted_saved.then_some(corrupted_path),
        was_recovered,
    }
}

fn detect_canopy_home_redirect_without_migration(
    paths: &SettingsPaths,
    options: &LoadSettingsOptions,
) -> Option<String> {
    if !options
        .process_env
        .get("QWEN_HOME")
        .is_some_and(|value| !value.is_empty())
    {
        return None;
    }
    let active_dir = global_canopy_dir_from_environment(
        &options.process_env,
        &paths.home_dir,
        &paths.process_cwd,
    );
    let actual_user_dir = paths.user.parent().unwrap_or_else(|| Path::new("."));
    if normalize_absolute(&active_dir, &paths.process_cwd)
        != normalize_absolute(actual_user_dir, &paths.process_cwd)
    {
        return None;
    }
    let legacy_dir = normalize_absolute(
        &paths.home_dir.join(crate::storage::CANOPY_DIR),
        &paths.process_cwd,
    );
    if normalize_absolute(&active_dir, &paths.process_cwd) == legacy_dir || paths.user.exists() {
        return None;
    }
    let legacy_user_settings = legacy_dir.join("settings.json");
    if !legacy_user_settings.exists() {
        return None;
    }
    Some(format!(
        "QWEN_HOME points to \"{}\" but no settings.json was found there. Existing config remains at \"{}\" — OAuth tokens, settings, memory, extensions, and skills are not auto-migrated. Copy them manually if you want them to apply at the new location.",
        active_dir.display(),
        legacy_dir.display()
    ))
}

fn persist_settings_object(path: &Path, settings: &Map<String, Value>) {
    let Ok(content) = fs::read_to_string(path) else {
        return;
    };
    let Ok(updated) = update_jsonc_object_content(&content, settings) else {
        return;
    };
    let _ = write_atomic(path, updated.as_bytes());
}

/// Persist one dotted-path setting while preserving existing JSONC comments
/// and formatting. The update is written through the shared atomic file writer;
/// malformed files are left untouched and reported to the caller.
pub fn update_setting_value(path: &Path, key: &str, value: Value) -> std::io::Result<()> {
    let keys = key.split('.').collect::<Vec<_>>();
    if keys.is_empty()
        || keys.iter().any(|part| {
            part.is_empty() || matches!(*part, "__proto__" | "constructor" | "prototype")
        })
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid settings key path",
        ));
    }

    let original = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => "{}\n".to_owned(),
        Err(error) => return Err(error),
    };
    let mut nested = value;
    for key in keys.iter().skip(1).rev() {
        let mut object = Map::new();
        object.insert((*key).to_owned(), nested);
        nested = Value::Object(object);
    }
    let mut updates = Map::new();
    updates.insert(keys[0].to_owned(), nested);

    let updated = update_jsonc_content(&original, &updates, false, &[]).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("could not update settings JSONC: {error}"),
        )
    })?;
    write_atomic(path, updated.as_bytes())
}

fn write_atomic(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    atomic_write_file(path, contents, &AtomicWriteOptions::default())
}

fn resolve_real_or_absolute(path: &Path, cwd: &Path) -> PathBuf {
    let absolute = normalize_absolute(path, cwd);
    absolute.canonicalize().unwrap_or(absolute)
}

fn read_home_env_fallback(
    paths: &SettingsPaths,
    process_env: &HashMap<String, String>,
) -> HashMap<String, String> {
    let global_canopy_dir = paths.user.parent().unwrap_or_else(|| Path::new("."));
    let process_keys = process_env.keys().cloned().collect::<HashSet<_>>();
    get_home_env_fallback_vars(
        global_canopy_dir,
        &paths.home_dir,
        &process_keys,
        process_env
            .get("QWEN_HOME")
            .is_some_and(|value| !value.is_empty()),
    )
}

fn resolve_map(
    source: &Map<String, Value>,
    environment: &HashMap<String, String>,
) -> Map<String, Value> {
    resolve_env_vars_in_object(&Value::Object(source.clone()), Some(environment))
        .as_object()
        .cloned()
        .unwrap_or_default()
}

fn normalize_legacy_theme(settings: &mut Map<String, Value>) {
    let Some(theme) = settings
        .get_mut("ui")
        .and_then(Value::as_object_mut)
        .and_then(|ui| ui.get_mut("theme"))
        .and_then(|value| value.as_str())
        .map(str::to_owned)
    else {
        return;
    };
    let normalized = match theme.as_str() {
        "VS" => "Default Light",
        "VS2015" => "Default",
        _ => return,
    };
    settings
        .get_mut("ui")
        .and_then(Value::as_object_mut)
        .expect("theme came from object")
        .insert("theme".to_owned(), Value::String(normalized.to_owned()));
}

pub(super) fn resolve_workspace_trust(
    settings: &Map<String, Value>,
    workspace_dir: &Path,
    process_cwd: &Path,
    user_settings_path: &Path,
    trusted_folders_path: Option<&Path>,
) -> Result<Option<bool>, SettingsLoadError> {
    let folder_trust_enabled = settings
        .get("security")
        .and_then(Value::as_object)
        .and_then(|security| security.get("folderTrust"))
        .and_then(Value::as_object)
        .and_then(|folder_trust| folder_trust.get("enabled"))
        .is_some_and(js_truthy);
    if !folder_trust_enabled {
        return Ok(Some(true));
    }
    let trusted_path = trusted_folders_path
        .map(Path::to_path_buf)
        .unwrap_or_else(|| {
            user_settings_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(TRUSTED_FOLDERS_FILENAME)
        });
    let trusted = LoadedTrustedFolders::load(trusted_path);
    if !trusted.errors().is_empty() {
        let errors = trusted
            .errors()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        return Err(SettingsLoadError::FatalConfig(format!(
            "{}\nPlease fix the configuration file and try again.",
            errors.join("\n")
        )));
    }
    Ok(resolve_trust_decision(
        &trusted.rules(),
        workspace_dir,
        process_cwd,
    ))
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn strip_workspace_security_bypasses(settings: &Map<String, Value>) -> Map<String, Value> {
    let mut result = settings.clone();
    let Some(security) = result.get_mut("security").and_then(Value::as_object_mut) else {
        return result;
    };
    security.remove("allowPrivateNetworkHooks");
    security.remove("allowedInsecureVoiceBaseUrls");
    result
}

fn tag_mcp_server_scope(settings: Map<String, Value>, scope: &str) -> Map<String, Value> {
    let mut result = settings;
    let Some(servers) = result.get_mut("mcpServers").and_then(Value::as_object_mut) else {
        return result;
    };
    for config in servers.values_mut() {
        let mut tagged = match config {
            Value::Object(object) => object.clone(),
            Value::Array(values) => values
                .iter()
                .enumerate()
                .map(|(index, value)| (index.to_string(), value.clone()))
                .collect(),
            Value::String(value) => value
                .encode_utf16()
                .enumerate()
                .map(|(index, value)| {
                    (
                        index.to_string(),
                        Value::String(String::from_utf16_lossy(&[value])),
                    )
                })
                .collect(),
            Value::Null | Value::Bool(_) | Value::Number(_) => Map::new(),
        };
        tagged.insert("scope".to_owned(), Value::String(scope.to_owned()));
        *config = Value::Object(tagged);
    }
    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    use serde_json::{Map, Value, json};

    use super::{
        ENV_CORRUPTED_PATH, ENV_WAS_RECOVERED, LoadSettingsOptions, SettingScope,
        SettingsLoadError, SettingsPaths, TRUSTED_FOLDERS_FILENAME, load_settings_from_paths,
        merge_settings, resolve_workspace_trust,
    };

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "canopy-settings-{name}-{}-{}",
                std::process::id(),
                uuid::Uuid::new_v4().simple()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn paths(root: &Path) -> SettingsPaths {
        SettingsPaths::for_test(
            root.join("system.json"),
            root.join("defaults.json"),
            root.join("home/.canopy/settings.json"),
            root.join("workspace/.canopy/settings.json"),
            root.join("home"),
            root.join("workspace"),
            root,
        )
    }

    fn write_json(path: &Path, text: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }

    #[test]
    fn loads_four_scopes_with_source_precedence_and_schema_array_merge_rules() {
        let root = TestDirectory::new("precedence");
        let paths = paths(&root.0);
        write_json(
            &paths.system,
            r#"{"general":{"vimMode":false},"tools":{"exclude":["system"]}}"#,
        );
        write_json(
            &paths.system_defaults,
            r#"{"general":{"vimMode":true},"tools":{"exclude":["default"]}}"#,
        );
        write_json(
            &paths.user,
            r#"{"$version":4,"general":{"vimMode":true},"tools":{"exclude":["user"]}}"#,
        );
        write_json(
            &paths.workspace,
            r#"{"$version":4,"general":{"vimMode":true},"tools":{"exclude":["workspace"]}}"#,
        );

        let mut options = LoadSettingsOptions {
            workspace_trusted: Some(true),
            ..LoadSettingsOptions::default()
        };
        let loaded = load_settings_from_paths(&paths, &mut options).unwrap();
        assert_eq!(loaded.merged["general"]["vimMode"], false);
        assert_eq!(
            loaded.merged["tools"]["exclude"],
            json!(["default", "user", "workspace", "system"])
        );
        assert_eq!(
            loaded.for_scope(SettingScope::Workspace).path,
            paths.workspace
        );
    }

    #[test]
    fn untrusted_workspace_does_not_contribute_settings_or_security_bypass_values() {
        let root = TestDirectory::new("untrusted");
        let paths = paths(&root.0);
        write_json(
            &paths.user,
            r#"{"$version":4,"security":{"allowPrivateNetworkHooks":false},"general":{"vimMode":false}}"#,
        );
        write_json(
            &paths.workspace,
            r#"{"$version":4,"security":{"allowPrivateNetworkHooks":true},"general":{"vimMode":true}}"#,
        );
        let mut options = LoadSettingsOptions {
            workspace_trusted: Some(false),
            ..LoadSettingsOptions::default()
        };
        let loaded = load_settings_from_paths(&paths, &mut options).unwrap();
        assert_eq!(loaded.merged["general"]["vimMode"], false);
        assert_eq!(loaded.merged["security"]["allowPrivateNetworkHooks"], false);

        let mut trusted_workspace = paths.workspace.clone();
        trusted_workspace.pop();
        assert!(trusted_workspace.ends_with(".canopy"));
    }

    #[test]
    fn trusted_workspace_security_bypasses_are_removed_but_other_security_values_merge() {
        let merged = merge_settings(
            &Map::new(),
            &Map::new(),
            &Map::new(),
            json!({"security":{"folderTrust":{"enabled":true},"allowPrivateNetworkHooks":true,"allowedInsecureVoiceBaseUrls":["http://x"],"other":true}})
                .as_object()
                .unwrap(),
            true,
        );
        assert_eq!(merged["security"]["folderTrust"]["enabled"], true);
        assert_eq!(merged["security"]["other"], true);
        assert!(merged["security"].get("allowPrivateNetworkHooks").is_none());
        assert!(
            merged["security"]
                .get("allowedInsecureVoiceBaseUrls")
                .is_none()
        );
    }

    #[test]
    fn migrates_and_persists_settings_but_keeps_original_unresolved_placeholders() {
        let root = TestDirectory::new("migration");
        let paths = paths(&root.0);
        write_json(
            &paths.user,
            "{\n  \"theme\": \"VS\",\n  // retained comment\n  \"model\": \"$MODEL_NAME\"\n}\n",
        );
        let mut options = LoadSettingsOptions {
            process_env: HashMap::from([("MODEL_NAME".to_owned(), "resolved-model".to_owned())]),
            home_env_fallback: Some(HashMap::new()),
            workspace_trusted: Some(true),
            ..LoadSettingsOptions::default()
        };
        let loaded = load_settings_from_paths(&paths, &mut options).unwrap();
        assert_eq!(loaded.user.settings["model"]["name"], "resolved-model");
        assert_eq!(
            loaded.user.original_settings["model"]["name"],
            "$MODEL_NAME"
        );
        assert_eq!(loaded.user.settings["ui"]["theme"], "Default Light");
        let on_disk = fs::read_to_string(&paths.user).unwrap();
        assert!(on_disk.contains("retained comment"));
        assert!(on_disk.contains("\"$version\": 4"), "{on_disk}");
    }

    #[test]
    fn malformed_json_is_backed_up_reset_and_reported_without_crashing() {
        let root = TestDirectory::new("corruption");
        let paths = paths(&root.0);
        write_json(&paths.user, "{ this is not json");
        let mut options = LoadSettingsOptions {
            workspace_trusted: Some(true),
            ..LoadSettingsOptions::default()
        };
        let loaded = load_settings_from_paths(&paths, &mut options).unwrap();
        assert!(loaded.user.settings.is_empty());
        assert_eq!(fs::read_to_string(&paths.user).unwrap(), "{}");
        assert_eq!(
            fs::read_to_string(format!(
                "{}{suffix}",
                paths.user.display(),
                suffix = super::CORRUPTED_SUFFIX
            ))
            .unwrap(),
            "{ this is not json"
        );
        assert!(loaded.corrupted_path.is_some());
        assert!(!loaded.was_recovered);
    }

    #[test]
    fn non_object_settings_are_fatal_and_corruption_env_markers_are_consumed() {
        let root = TestDirectory::new("invalid-root");
        let paths = paths(&root.0);
        write_json(&paths.system, "[]");
        let mut options = LoadSettingsOptions::default();
        assert!(matches!(
            load_settings_from_paths(&paths, &mut options),
            Err(SettingsLoadError::FatalConfig(message)) if message.contains("Settings file is not a valid JSON object.")
        ));

        let corrupted = PathBuf::from(format!(
            "{}{suffix}",
            paths.user.display(),
            suffix = super::CORRUPTED_SUFFIX
        ));
        write_json(&paths.user, "{\"$version\":4}");
        let mut options = LoadSettingsOptions {
            process_env: HashMap::from([
                (
                    ENV_CORRUPTED_PATH.to_owned(),
                    corrupted.to_string_lossy().into_owned(),
                ),
                (ENV_WAS_RECOVERED.to_owned(), "1".to_owned()),
            ]),
            ..LoadSettingsOptions::default()
        };
        assert!(matches!(
            load_settings_from_paths(&paths, &mut options),
            Err(SettingsLoadError::FatalConfig(message))
                if message.contains("Settings file is not a valid JSON object.")
        ));
        assert!(!options.process_env.contains_key(ENV_CORRUPTED_PATH));
        assert!(!options.process_env.contains_key(ENV_WAS_RECOVERED));

        write_json(&paths.system, "{}");
        let mut options = LoadSettingsOptions {
            process_env: HashMap::from([
                (
                    ENV_CORRUPTED_PATH.to_owned(),
                    corrupted.to_string_lossy().into_owned(),
                ),
                (ENV_WAS_RECOVERED.to_owned(), "1".to_owned()),
            ]),
            ..LoadSettingsOptions::default()
        };
        let loaded = load_settings_from_paths(&paths, &mut options).unwrap();
        assert_eq!(loaded.corrupted_path, Some(corrupted.clone()));
        assert!(loaded.was_recovered);
        assert!(!options.process_env.contains_key(ENV_CORRUPTED_PATH));
        assert!(!options.process_env.contains_key(ENV_WAS_RECOVERED));

        write_json(&paths.user, "[]");
        let mut options = LoadSettingsOptions {
            process_env: HashMap::from([
                (
                    ENV_CORRUPTED_PATH.to_owned(),
                    corrupted.to_string_lossy().into_owned(),
                ),
                (ENV_WAS_RECOVERED.to_owned(), "1".to_owned()),
            ]),
            ..LoadSettingsOptions::default()
        };
        assert!(matches!(
            load_settings_from_paths(&paths, &mut options),
            Err(SettingsLoadError::FatalConfig(message))
                if message.contains("Settings file is not a valid JSON object.")
        ));
        assert!(!options.process_env.contains_key(ENV_CORRUPTED_PATH));
        assert!(!options.process_env.contains_key(ENV_WAS_RECOVERED));
    }

    #[test]
    fn root_comments_are_allowed_but_trailing_commas_follow_json_parse_failure_behavior() {
        let root = TestDirectory::new("strict-json");
        let paths = paths(&root.0);
        write_json(
            &paths.user,
            "{ // allowed by strip-json-comments\n \"$version\": 4,\n}",
        );
        let mut options = LoadSettingsOptions {
            workspace_trusted: Some(true),
            ..LoadSettingsOptions::default()
        };
        let loaded = load_settings_from_paths(&paths, &mut options).unwrap();
        assert!(loaded.user.settings.is_empty());
        assert!(loaded.corrupted_path.is_some());
    }

    #[test]
    fn workspace_settings_are_skipped_when_workspace_is_home_or_explicitly_disabled() {
        let root = TestDirectory::new("home-skip");
        let home = root.0.join("home");
        fs::create_dir_all(&home).unwrap();
        let paths = SettingsPaths::for_test(
            root.0.join("system.json"),
            root.0.join("defaults.json"),
            home.join(".canopy/settings.json"),
            home.join(".canopy/settings.json"),
            home.clone(),
            home.clone(),
            root.0.clone(),
        );
        write_json(
            &paths.workspace,
            r#"{"$version":4,"general":{"vimMode":true}}"#,
        );
        let mut options = LoadSettingsOptions::default();
        let loaded = load_settings_from_paths(&paths, &mut options).unwrap();
        assert!(!loaded.workspace_settings_active);
        assert!(loaded.workspace.settings.is_empty());

        let mut options = LoadSettingsOptions {
            skip_workspace_settings: true,
            ..LoadSettingsOptions::default()
        };
        let loaded = load_settings_from_paths(&paths, &mut options).unwrap();
        assert!(!loaded.workspace_settings_active);
    }

    #[test]
    fn home_env_bootstrap_redirects_settings_paths_and_reads_the_redirected_env() {
        let root = TestDirectory::new("home-env-bootstrap");
        let home = root.0.join("home");
        let old_global = home.join(".canopy");
        let new_global = root.0.join("redirected-canopy");
        fs::create_dir_all(&old_global).unwrap();
        fs::create_dir_all(&new_global).unwrap();
        fs::write(
            old_global.join(".env"),
            format!("QWEN_HOME={}\n", new_global.display()),
        )
        .unwrap();
        fs::write(
            new_global.join(".env"),
            "CANOPY_RUNTIME_DIR=/runtime-from-redirect\n",
        )
        .unwrap();

        let mut process_env = HashMap::new();
        super::pre_resolve_home_env_overrides(&mut process_env, &home, &root.0);
        let expected_qwen_home = new_global.to_string_lossy().into_owned();
        assert_eq!(process_env.get("QWEN_HOME"), Some(&expected_qwen_home));
        assert_eq!(
            process_env.get("CANOPY_RUNTIME_DIR").map(String::as_str),
            Some("/runtime-from-redirect")
        );

        let paths =
            SettingsPaths::from_environment_with_env(root.0.join("workspace"), &process_env);
        assert_eq!(paths.user, new_global.join("settings.json"));
    }

    #[test]
    fn trusted_folders_path_override_from_environment_controls_workspace_trust() {
        let root = TestDirectory::new("trusted-folders-path");
        let workspace_dir = root.0.join("workspace");
        fs::create_dir_all(&workspace_dir).unwrap();

        let custom_trusted_path = root.0.join("custom/trustedFolders.json");
        let process_env = HashMap::from([(
            "CANOPY_CODE_TRUSTED_FOLDERS_PATH".to_owned(),
            custom_trusted_path.to_string_lossy().into_owned(),
        )]);
        let discovered_paths =
            SettingsPaths::from_environment_with_env(&workspace_dir, &process_env);
        assert_eq!(
            discovered_paths.trusted_folders_path.as_deref(),
            Some(custom_trusted_path.as_path())
        );

        let test_paths = paths(&root.0);
        let default_trusted_path = test_paths
            .user
            .parent()
            .unwrap()
            .join(TRUSTED_FOLDERS_FILENAME);
        write_json(
            &default_trusted_path,
            &format!("{{\"{}\":\"DO_NOT_TRUST\"}}", workspace_dir.display()),
        );
        write_json(
            &custom_trusted_path,
            &format!("{{\"{}\":\"TRUST_FOLDER\"}}", workspace_dir.display()),
        );

        let settings = json!({"security": {"folderTrust": {"enabled": true}}});
        let trust = resolve_workspace_trust(
            settings.as_object().unwrap(),
            &workspace_dir,
            &root.0,
            &test_paths.user,
            discovered_paths.trusted_folders_path.as_deref(),
        )
        .unwrap();
        assert_eq!(trust, Some(true));
    }

    #[test]
    fn warns_when_qwen_home_hides_existing_legacy_settings() {
        let root = TestDirectory::new("home-redirect-warning");
        let home = root.0.join("home");
        let legacy_dir = home.join(".canopy");
        let active_dir = root.0.join("new-canopy");
        fs::create_dir_all(&legacy_dir).unwrap();
        fs::create_dir_all(&active_dir).unwrap();
        write_json(&legacy_dir.join("settings.json"), r#"{"$version":4}"#);
        let paths = SettingsPaths::for_test(
            root.0.join("system.json"),
            root.0.join("defaults.json"),
            active_dir.join("settings.json"),
            root.0.join("workspace/.canopy/settings.json"),
            home,
            root.0.join("workspace"),
            root.0.clone(),
        );
        let mut options = LoadSettingsOptions {
            process_env: HashMap::from([(
                "QWEN_HOME".to_owned(),
                active_dir.to_string_lossy().into_owned(),
            )]),
            home_env_fallback: Some(HashMap::new()),
            workspace_trusted: Some(true),
            ..LoadSettingsOptions::default()
        };
        let loaded = load_settings_from_paths(&paths, &mut options).unwrap();
        assert!(loaded.migration_warnings[0].contains("Existing config remains at"));
        assert!(loaded.migration_warnings[0].contains("not auto-migrated"));
    }

    #[test]
    fn skip_load_environment_returns_only_the_supplied_process_environment() {
        let root = TestDirectory::new("skip-environment");
        let paths = paths(&root.0);
        fs::create_dir_all(root.0.join("workspace/.canopy")).unwrap();
        fs::write(root.0.join("workspace/.canopy/.env"), "FROM_DOTENV=yes\n").unwrap();
        write_json(
            &paths.user,
            r#"{"$version":4,"env":{"FROM_SETTINGS":"yes"}}"#,
        );
        let mut options = LoadSettingsOptions {
            process_env: HashMap::from([("PRESENT".to_owned(), "yes".to_owned())]),
            skip_load_environment: true,
            workspace_trusted: Some(true),
            ..LoadSettingsOptions::default()
        };
        let loaded = load_settings_from_paths(&paths, &mut options).unwrap();
        assert_eq!(loaded.runtime_environment.effective_env["PRESENT"], "yes");
        assert!(
            !loaded
                .runtime_environment
                .effective_env
                .contains_key("FROM_DOTENV")
        );
        assert!(
            !loaded
                .runtime_environment
                .effective_env
                .contains_key("FROM_SETTINGS")
        );
        assert!(loaded.runtime_environment.env_file_paths.is_empty());
    }

    #[test]
    fn settings_file_env_fallback_loses_to_process_environment_and_missing_vars_stay_literal() {
        let root = TestDirectory::new("env-fallback");
        let paths = paths(&root.0);
        write_json(
            &paths.user,
            r#"{"$version":4,"model":{"name":"$MODEL"},"general":{"preferredEditor":"${EDITOR}"}}"#,
        );
        let mut options = LoadSettingsOptions {
            process_env: HashMap::from([("MODEL".to_owned(), "process-model".to_owned())]),
            home_env_fallback: Some(HashMap::from([
                ("MODEL".to_owned(), "dotenv-model".to_owned()),
                ("EDITOR".to_owned(), "nano".to_owned()),
            ])),
            workspace_trusted: Some(true),
            ..LoadSettingsOptions::default()
        };
        let loaded = load_settings_from_paths(&paths, &mut options).unwrap();
        assert_eq!(loaded.user.settings["model"]["name"], "process-model");
        assert_eq!(loaded.user.settings["general"]["preferredEditor"], "nano");

        let resolved = super::resolve_map(
            &json!({"model": {"name": "$MISSING"}})
                .as_object()
                .unwrap()
                .clone(),
            &HashMap::new(),
        );
        assert_eq!(resolved["model"]["name"], "$MISSING");
    }

    #[test]
    fn mcp_scope_provenance_tracks_the_winning_workspace_or_system_entry() {
        let merged = merge_settings(
            &json!({"mcpServers":{"shared":{"command":"system"}}})
                .as_object()
                .unwrap()
                .clone(),
            &json!({"mcpServers":{"default":{"command":"default"}}})
                .as_object()
                .unwrap()
                .clone(),
            &json!({"mcpServers":{"shared":{"command":"user"}}})
                .as_object()
                .unwrap()
                .clone(),
            &json!({"mcpServers":{"shared":{"command":"workspace"}}})
                .as_object()
                .unwrap()
                .clone(),
            true,
        );
        assert_eq!(merged["mcpServers"]["shared"]["command"], "system");
        assert_eq!(merged["mcpServers"]["shared"]["scope"], "system");
        assert_eq!(merged["mcpServers"]["default"]["scope"], Value::Null);
    }
}
