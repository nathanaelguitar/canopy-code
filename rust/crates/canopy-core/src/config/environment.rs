//! Trust-gated `.env` and `settings.env` activation.
//!
//! This returns a process-environment snapshot rather than mutating global
//! state. Rust 2024 marks process-wide environment mutation unsafe, while a
//! snapshot can be passed directly to every spawned command and session.
//! Source: `packages/cli/src/config/environment.ts`.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use crate::config::loader::{
    LoadSettingsOptions, SettingsLoadError, SettingsPaths, resolve_workspace_trust,
};
use crate::utils::dotenv::parse_dotenv;

pub const SETTINGS_DIRECTORY_NAME: &str = ".canopy";
const DEFAULT_EXCLUDED_ENV_VARS: &[&str] = &["DEBUG", "DEBUG_MODE"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvFileReadFailure {
    pub path: PathBuf,
    pub error: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeEnvironmentSnapshot {
    pub effective_env: HashMap<String, String>,
    pub overlay_keys: Vec<String>,
    pub env_file_paths: Vec<PathBuf>,
    pub env_file_read_failed: bool,
    pub env_file_read_failures: Vec<EnvFileReadFailure>,
}

#[derive(Clone, Debug)]
struct ParsedEnvFile {
    env: HashMap<String, String>,
    is_home_scoped: bool,
    is_canopy_scoped: bool,
}

/// Discovers the same nearest-workspace-to-home candidate chain as Canopy.
/// Home files remain readable in untrusted workspaces; other files are gated
/// by the matching workspace's trust decision.
pub fn find_env_files(
    settings: &Map<String, Value>,
    paths: &SettingsPaths,
    workspace_trusted: Option<bool>,
) -> Result<Vec<PathBuf>, SettingsLoadError> {
    let real_start_dir = resolve_path(&paths.workspace_dir, &paths.process_cwd);
    let global_canopy_dir = paths.user.parent().unwrap_or_else(|| Path::new("."));
    let legacy_canopy_dir = paths.home_dir.join(SETTINGS_DIRECTORY_NAME);
    let has_custom_config_dir =
        normalize_path(global_canopy_dir) != normalize_path(&legacy_canopy_dir);
    let user_level_paths = user_level_env_paths(paths);
    let mut found = Vec::new();
    let mut seen = HashSet::new();

    let mut current_dir = real_start_dir.clone();
    loop {
        if normalize_path(&current_dir) == normalize_path(&paths.home_dir) {
            push_home_candidates(
                &mut found,
                &mut seen,
                settings,
                paths,
                &user_level_paths,
                &real_start_dir,
                workspace_trusted,
                has_custom_config_dir,
            )?;
            return Ok(found);
        }

        let scoped_candidate = current_dir.join(SETTINGS_DIRECTORY_NAME).join(".env");
        if push_candidate(
            scoped_candidate,
            &mut found,
            &mut seen,
            settings,
            paths,
            &user_level_paths,
            &real_start_dir,
            workspace_trusted,
        )? {
            push_home_candidates(
                &mut found,
                &mut seen,
                settings,
                paths,
                &user_level_paths,
                &real_start_dir,
                workspace_trusted,
                has_custom_config_dir,
            )?;
            return Ok(found);
        }

        if push_candidate(
            current_dir.join(".env"),
            &mut found,
            &mut seen,
            settings,
            paths,
            &user_level_paths,
            &real_start_dir,
            workspace_trusted,
        )? {
            push_home_candidates(
                &mut found,
                &mut seen,
                settings,
                paths,
                &user_level_paths,
                &real_start_dir,
                workspace_trusted,
                has_custom_config_dir,
            )?;
            return Ok(found);
        }

        let Some(parent_dir) = current_dir.parent().filter(|parent| *parent != current_dir) else {
            push_home_candidates(
                &mut found,
                &mut seen,
                settings,
                paths,
                &user_level_paths,
                &real_start_dir,
                workspace_trusted,
                has_custom_config_dir,
            )?;
            return Ok(found);
        };
        current_dir = parent_dir.to_owned();
    }
}

pub fn build_runtime_environment(
    settings: &Map<String, Value>,
    paths: &SettingsPaths,
    options: &LoadSettingsOptions,
    workspace_trusted: Option<bool>,
) -> Result<RuntimeEnvironmentSnapshot, SettingsLoadError> {
    build_environment_snapshot(settings, paths, options, workspace_trusted, true)
}

/// Returns the environment overlay used by the initial settings load. It
/// follows `loadEnvironment` filtering and precedence while leaving process
/// state untouched for the Rust caller to apply at its process boundary.
pub fn load_environment(
    settings: &Map<String, Value>,
    paths: &SettingsPaths,
    options: &LoadSettingsOptions,
    workspace_trusted: Option<bool>,
) -> Result<RuntimeEnvironmentSnapshot, SettingsLoadError> {
    build_environment_snapshot(settings, paths, options, workspace_trusted, false)
}

fn build_environment_snapshot(
    settings: &Map<String, Value>,
    paths: &SettingsPaths,
    options: &LoadSettingsOptions,
    workspace_trusted: Option<bool>,
    reload_filtering: bool,
) -> Result<RuntimeEnvironmentSnapshot, SettingsLoadError> {
    let env_file_paths = find_env_files(settings, paths, workspace_trusted)?;
    let user_level_paths = user_level_env_paths(paths);
    let mut files = Vec::new();
    let mut read_failures = Vec::new();

    for path in &env_file_paths {
        match fs::read(path) {
            Ok(bytes) => {
                let parsed = parse_dotenv(&String::from_utf8_lossy(&bytes));
                let normalized = normalize_path(path);
                let is_home_scoped = user_level_paths.contains(&normalized);
                let is_canopy_scoped = is_home_scoped
                    || path.parent().and_then(Path::file_name)
                        == Some(std::ffi::OsStr::new(SETTINGS_DIRECTORY_NAME));
                files.push(ParsedEnvFile {
                    env: parsed,
                    is_home_scoped,
                    is_canopy_scoped,
                });
            }
            Err(error) => read_failures.push(EnvFileReadFailure {
                path: path.clone(),
                error: error.to_string(),
            }),
        }
    }

    let mut effective_env = options.process_env.clone();
    if effective_env
        .get("CLOUD_SHELL")
        .is_some_and(|value| value == "true")
    {
        effective_env.insert(
            "GOOGLE_CLOUD_PROJECT".to_owned(),
            files
                .iter()
                .find_map(|file| {
                    file.env
                        .get("GOOGLE_CLOUD_PROJECT")
                        .filter(|value| !value.is_empty())
                        .cloned()
                })
                .unwrap_or_else(|| "cloudshell-gca".to_owned()),
        );
    }

    for file in &files {
        for (key, value) in &file.env {
            if !can_apply_env_key(file, key, settings, reload_filtering)? {
                continue;
            }
            set_if_effectively_unset(&mut effective_env, key, value);
        }
    }

    if let Some(settings_env) = settings.get("env") {
        for (key, value) in js_object_entries(settings_env) {
            if is_loader_env_key(&key)
                || is_reload_excluded_key(&key)
                || is_hardcoded_project_env_exclusion(&key)
                || (reload_filtering && is_excluded_env_key(settings, &key)?)
            {
                continue;
            }
            if let Some(value) = value.as_str() {
                set_if_effectively_unset(&mut effective_env, &key, value);
            }
        }
    }

    let mut overlay_keys = effective_env
        .iter()
        .filter_map(|(key, value)| {
            (options.process_env.get(key) != Some(value)).then_some(key.clone())
        })
        .collect::<Vec<_>>();
    overlay_keys.sort();
    Ok(RuntimeEnvironmentSnapshot {
        effective_env,
        overlay_keys,
        env_file_paths,
        env_file_read_failed: !read_failures.is_empty(),
        env_file_read_failures: read_failures,
    })
}

fn user_level_env_paths(paths: &SettingsPaths) -> HashSet<PathBuf> {
    let global_dir = paths.user.parent().unwrap_or_else(|| Path::new("."));
    HashSet::from([
        normalize_path(&paths.home_dir.join(".env")),
        normalize_path(&global_dir.join(".env")),
        normalize_path(&paths.home_dir.join(SETTINGS_DIRECTORY_NAME).join(".env")),
    ])
}

#[allow(clippy::too_many_arguments)]
fn push_home_candidates(
    found: &mut Vec<PathBuf>,
    seen: &mut HashSet<PathBuf>,
    settings: &Map<String, Value>,
    paths: &SettingsPaths,
    user_level_paths: &HashSet<PathBuf>,
    real_start_dir: &Path,
    workspace_trusted: Option<bool>,
    has_custom_config_dir: bool,
) -> Result<(), SettingsLoadError> {
    let global_dir = paths.user.parent().unwrap_or_else(|| Path::new("."));
    let mut candidates = vec![global_dir.join(".env")];
    if has_custom_config_dir {
        candidates.push(paths.home_dir.join(SETTINGS_DIRECTORY_NAME).join(".env"));
    }
    candidates.push(paths.home_dir.join(".env"));
    for candidate in candidates {
        push_candidate(
            candidate,
            found,
            seen,
            settings,
            paths,
            user_level_paths,
            real_start_dir,
            workspace_trusted,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn push_candidate(
    candidate: PathBuf,
    found: &mut Vec<PathBuf>,
    seen: &mut HashSet<PathBuf>,
    settings: &Map<String, Value>,
    paths: &SettingsPaths,
    user_level_paths: &HashSet<PathBuf>,
    real_start_dir: &Path,
    workspace_trusted: Option<bool>,
) -> Result<bool, SettingsLoadError> {
    let normalized = normalize_path(&candidate);
    if seen.contains(&normalized) || !candidate.exists() {
        return Ok(false);
    }
    if !can_use_env_file(
        &candidate,
        settings,
        paths,
        user_level_paths,
        real_start_dir,
        workspace_trusted,
    )? {
        return Ok(false);
    }
    seen.insert(normalized);
    found.push(candidate);
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn can_use_env_file(
    file_path: &Path,
    settings: &Map<String, Value>,
    paths: &SettingsPaths,
    user_level_paths: &HashSet<PathBuf>,
    real_start_dir: &Path,
    workspace_trusted: Option<bool>,
) -> Result<bool, SettingsLoadError> {
    if user_level_paths.contains(&normalize_path(file_path)) {
        return Ok(true);
    }
    let Some(directory) = file_path.parent() else {
        return Ok(true);
    };
    let workspace_dir =
        if directory.file_name() == Some(std::ffi::OsStr::new(SETTINGS_DIRECTORY_NAME)) {
            directory.parent().unwrap_or(directory)
        } else {
            directory
        };
    let trust_override = workspace_trusted
        .filter(|_| normalize_path(workspace_dir) == normalize_path(real_start_dir));
    let trusted = match trust_override {
        Some(trusted) => Some(trusted),
        None => resolve_workspace_trust(
            settings,
            workspace_dir,
            &paths.process_cwd,
            &paths.user,
            paths.trusted_folders_path.as_deref(),
        )?,
    };
    Ok(trusted != Some(false))
}

fn is_excluded_env_key(
    settings: &Map<String, Value>,
    key: &str,
) -> Result<bool, SettingsLoadError> {
    let configured = settings
        .get("advanced")
        .and_then(Value::as_object)
        .and_then(|advanced| advanced.get("excludedEnvVars"));
    match configured {
        None | Some(Value::Null) | Some(Value::Bool(false)) => {
            Ok(DEFAULT_EXCLUDED_ENV_VARS.contains(&key))
        }
        Some(Value::Number(value)) if value.as_f64() == Some(0.0) => {
            Ok(DEFAULT_EXCLUDED_ENV_VARS.contains(&key))
        }
        Some(Value::String(value)) if value.is_empty() => {
            Ok(DEFAULT_EXCLUDED_ENV_VARS.contains(&key))
        }
        Some(Value::Array(values)) => Ok(values
            .iter()
            .any(|value| value.as_str().is_some_and(|value| value == key))),
        Some(Value::String(value)) => Ok(value.contains(key)),
        Some(Value::Bool(true) | Value::Number(_) | Value::Object(_)) => {
            Err(SettingsLoadError::InvalidSettingsEnvironment(
                "advanced.excludedEnvVars.includes is not a function".to_owned(),
            ))
        }
    }
}

fn can_apply_env_key(
    file: &ParsedEnvFile,
    key: &str,
    settings: &Map<String, Value>,
    reload_filtering: bool,
) -> Result<bool, SettingsLoadError> {
    if is_loader_env_key(key) {
        return Ok(false);
    }
    if reload_filtering && is_reload_excluded_key(key) {
        return Ok(false);
    }
    if !file.is_home_scoped && is_hardcoded_project_env_exclusion(key) {
        return Ok(false);
    }
    if file.is_canopy_scoped {
        Ok(true)
    } else {
        is_excluded_env_key(settings, key).map(|excluded| !excluded)
    }
}

fn is_reload_excluded_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "qwen_server_token"
            | "ld_library_path"
            | "dyld_library_path"
            | "env"
            | "path"
            | "home"
            | "tmpdir"
            | "tmp"
            | "temp"
    ) || is_hardcoded_project_env_exclusion(key)
}

/// Emulates the `Object.entries(settings.env)` behavior for malformed but
/// parseable values. Arrays and strings expose numeric keys in JavaScript;
/// booleans and numbers produce no entries.
fn js_object_entries(value: &Value) -> Vec<(String, Value)> {
    match value {
        Value::Object(object) => object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        Value::Array(array) => array
            .iter()
            .enumerate()
            .map(|(index, value)| (index.to_string(), value.clone()))
            .collect(),
        Value::String(string) => string
            .encode_utf16()
            .enumerate()
            .map(|(index, unit)| {
                (
                    index.to_string(),
                    Value::String(String::from_utf16_lossy(&[unit])),
                )
            })
            .collect(),
        Value::Null | Value::Bool(_) | Value::Number(_) => Vec::new(),
    }
}

fn set_if_effectively_unset(env: &mut HashMap<String, String>, key: &str, value: &str) {
    if env.get(key).is_none_or(String::is_empty) {
        env.insert(key.to_owned(), value.to_owned());
    }
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !normalized.pop() && !path.is_absolute() {
                    normalized.push("..");
                }
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn resolve_path(path: &Path, cwd: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        cwd.join(path)
    };
    absolute
        .canonicalize()
        .unwrap_or_else(|_| normalize_path(&absolute))
}

fn is_loader_env_key(key: &str) -> bool {
    let canonical = key.to_ascii_lowercase().replace('_', "-");
    canonical.starts_with("bash-func-")
        || [
            "node-options",
            "npm-config-node-options",
            "npm-config-userconfig",
            "npm-config-globalconfig",
            "npm-config-script-shell",
            "npm-config-prefix",
            "node-path",
            "openssl-conf",
            "node-repl-external-module",
            "npm-config-node-gyp",
            "npm-config-init-module",
            "ld-preload",
            "ld-audit",
            "dyld-insert-libraries",
            "bash-env",
            "zdotdir",
        ]
        .contains(&canonical.as_str())
}

fn is_hardcoded_project_env_exclusion(key: &str) -> bool {
    let lower_key = key.to_ascii_lowercase();
    if matches!(
        lower_key.as_str(),
        "qwen_home"
            | "canopy_runtime_dir"
            | "canopy_code_mcp_approvals_path"
            | "canopy_code_trusted_folders_path"
            | "qwen_code_serve"
            | "canopy_code_desktop"
            | "canopy_code_settings_corrupted_path"
            | "canopy_code_settings_was_recovered"
            | "canopy_code_acp_repeated_tool_failure_guard"
            | "canopy_code_memory_project_scope"
            | "canopy_tls_insecure"
            | "node_tls_reject_unauthorized"
            | "node_extra_ca_certs"
            | "ssl_cert_file"
            | "ssl_cert_dir"
            | "curl_ca_bundle"
            | "requests_ca_bundle"
            | "git_ssl_cainfo"
            | "git_ssl_capath"
            | "npm_config_cafile"
            | "npm_config_ca"
            | "npm_config_strict_ssl"
            | "npm_config_strict-ssl"
            | "pip_cert"
            | "curl_home"
            | "wgetrc"
            | "pip_config_file"
            | "git_ssh_command"
            | "git_ssh"
            | "git_exec_path"
            | "git_template_dir"
            | "git_askpass"
            | "git_proxy_command"
            | "git_editor"
            | "git_sequence_editor"
            | "git_external_diff"
            | "git_config_global"
            | "git_config_system"
            | "xdg_config_home"
            | "git_config_count"
            | "git_config_parameters"
            | "ssh_askpass"
            | "lessopen"
            | "lessclose"
            | "node_gyp_force_python"
            | "npm_config_python"
            | "python"
            | "pythonstartup"
            | "visual"
            | "editor"
            | "npm_config_git"
            | "browser"
            | "qwen_cli_entry"
            | "canopy_cdp_mcp_command"
            | "canopy_serve_cdp_tunnel_over_ws"
            | "dev"
    ) {
        return true;
    }
    for prefix in ["git_config_key_", "git_config_value_"] {
        if let Some(suffix) = lower_key.strip_prefix(prefix) {
            return !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit());
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    use serde_json::json;

    use super::{
        SettingsLoadError, build_runtime_environment, find_env_files,
        is_hardcoded_project_env_exclusion, is_loader_env_key, is_reload_excluded_key,
        load_environment,
    };
    use crate::config::loader::{LoadSettingsOptions, SettingsPaths};

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "canopy-env-{}-{}",
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

    fn settings_paths(root: &Path) -> SettingsPaths {
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

    #[test]
    fn discovers_nearest_workspace_env_then_home_scoped_candidates() {
        let root = TestDirectory::new();
        let paths = settings_paths(&root.0);
        fs::create_dir_all(root.0.join("workspace/.canopy")).unwrap();
        fs::create_dir_all(root.0.join("home/.canopy")).unwrap();
        fs::write(root.0.join("workspace/.canopy/.env"), "WORKSPACE=yes").unwrap();
        fs::write(root.0.join("home/.canopy/.env"), "HOME_CANOPY=yes").unwrap();
        fs::write(root.0.join("home/.env"), "HOME=yes").unwrap();

        let files = find_env_files(
            &json!({"security":{"folderTrust":{"enabled":false}}})
                .as_object()
                .unwrap()
                .clone(),
            &paths,
            Some(true),
        )
        .unwrap();
        assert_eq!(
            files,
            vec![
                root.0
                    .join("workspace/.canopy/.env")
                    .canonicalize()
                    .unwrap(),
                root.0.join("home/.canopy/.env"),
                root.0.join("home/.env")
            ]
        );
    }

    #[test]
    fn environment_precedence_is_process_then_nearest_dotenv_then_settings_env() {
        let root = TestDirectory::new();
        let paths = settings_paths(&root.0);
        fs::create_dir_all(root.0.join("workspace/.canopy")).unwrap();
        fs::create_dir_all(root.0.join("home/.canopy")).unwrap();
        fs::write(
            root.0.join("workspace/.canopy/.env"),
            "WORKSPACE=first\nPROCESS=dotenv\nFALLBACK=dotenv\nEMPTY=filled\n",
        )
        .unwrap();
        fs::write(
            root.0.join("home/.canopy/.env"),
            "WORKSPACE=home\nHOME_ONLY=yes\n",
        )
        .unwrap();
        let settings = json!({
            "security":{"folderTrust":{"enabled":false}},
            "env":{"WORKSPACE":"settings","SETTINGS_ONLY":"yes","EXCLUDED":"no"},
            "advanced":{"excludedEnvVars":["EXCLUDED"]}
        })
        .as_object()
        .unwrap()
        .clone();
        let options = LoadSettingsOptions {
            process_env: HashMap::from([
                ("PROCESS".to_owned(), "shell".to_owned()),
                ("EMPTY".to_owned(), String::new()),
            ]),
            ..LoadSettingsOptions::default()
        };
        let snapshot = build_runtime_environment(&settings, &paths, &options, Some(true)).unwrap();
        assert_eq!(snapshot.effective_env["PROCESS"], "shell");
        assert_eq!(snapshot.effective_env["WORKSPACE"], "first");
        assert_eq!(snapshot.effective_env["FALLBACK"], "dotenv");
        assert_eq!(snapshot.effective_env["EMPTY"], "filled");
        assert_eq!(snapshot.effective_env["SETTINGS_ONLY"], "yes");
        assert!(!snapshot.effective_env.contains_key("EXCLUDED"));
        assert_eq!(snapshot.effective_env["HOME_ONLY"], "yes");
        assert!(snapshot.overlay_keys.contains(&"WORKSPACE".to_owned()));
    }

    #[test]
    fn untrusted_workspace_env_is_skipped_but_user_env_still_loads() {
        let root = TestDirectory::new();
        let paths = settings_paths(&root.0);
        fs::create_dir_all(root.0.join("workspace/.canopy")).unwrap();
        fs::create_dir_all(root.0.join("home/.canopy")).unwrap();
        fs::write(root.0.join("workspace/.canopy/.env"), "WORKSPACE=unsafe\n").unwrap();
        fs::write(root.0.join("home/.canopy/.env"), "USER=allowed\n").unwrap();
        let settings = json!({"security":{"folderTrust":{"enabled":false}}})
            .as_object()
            .unwrap()
            .clone();
        let options = LoadSettingsOptions {
            process_env: HashMap::new(),
            ..LoadSettingsOptions::default()
        };
        let snapshot = build_runtime_environment(&settings, &paths, &options, Some(false)).unwrap();
        assert!(!snapshot.effective_env.contains_key("WORKSPACE"));
        assert_eq!(snapshot.effective_env["USER"], "allowed");
    }

    #[test]
    fn loader_and_project_exclusion_keys_match_source_policy() {
        for key in [
            "NODE_OPTIONS",
            "npm_config_node-options",
            "BASH_FUNC_test%%",
            "OPENSSL_CONF",
        ] {
            assert!(is_loader_env_key(key), "{key}");
        }
        assert!(is_hardcoded_project_env_exclusion("git_config_key_17"));
        assert!(!is_hardcoded_project_env_exclusion("GIT_CONFIG_KEY_CACHE"));
        assert!(is_hardcoded_project_env_exclusion("node_extra_ca_certs"));
        assert!(is_hardcoded_project_env_exclusion("qwen_code_serve"));
        assert!(is_reload_excluded_key("PATH"));
        assert!(is_reload_excluded_key("QWEN_SERVER_TOKEN"));
    }

    #[test]
    fn project_dotenv_cannot_redirect_global_settings_or_inject_loader_state() {
        let root = TestDirectory::new();
        let paths = settings_paths(&root.0);
        fs::create_dir_all(root.0.join("workspace/.canopy")).unwrap();
        fs::write(
            root.0.join("workspace/.canopy/.env"),
            "QWEN_HOME=/tmp/attacker\nNODE_OPTIONS=--require=/tmp/attacker.js\nSAFE=value\n",
        )
        .unwrap();
        let options = LoadSettingsOptions {
            process_env: HashMap::new(),
            ..LoadSettingsOptions::default()
        };
        let settings = json!({"security":{"folderTrust":{"enabled":false}}})
            .as_object()
            .unwrap()
            .clone();
        let snapshot = build_runtime_environment(&settings, &paths, &options, Some(true)).unwrap();
        assert!(!snapshot.effective_env.contains_key("QWEN_HOME"));
        assert!(!snapshot.effective_env.contains_key("NODE_OPTIONS"));
        assert_eq!(snapshot.effective_env["SAFE"], "value");
    }

    #[test]
    fn runtime_snapshot_rejects_reload_excluded_file_and_settings_keys() {
        let root = TestDirectory::new();
        let paths = settings_paths(&root.0);
        fs::create_dir_all(root.0.join("workspace/.canopy")).unwrap();
        fs::write(
            root.0.join("workspace/.canopy/.env"),
            "PATH=/attacker/bin\nQWEN_SERVER_TOKEN=dotenv\nSAFE_DOTENV=ok\n",
        )
        .unwrap();
        let settings = json!({
            "security":{"folderTrust":{"enabled":false}},
            "env":{"PATH":"/settings/bin","QWEN_SERVER_TOKEN":"settings","SAFE_SETTINGS":"ok"}
        })
        .as_object()
        .unwrap()
        .clone();
        let options = LoadSettingsOptions {
            process_env: HashMap::new(),
            ..LoadSettingsOptions::default()
        };
        let snapshot = build_runtime_environment(&settings, &paths, &options, Some(true)).unwrap();
        assert!(!snapshot.effective_env.contains_key("PATH"));
        assert!(!snapshot.effective_env.contains_key("QWEN_SERVER_TOKEN"));
        assert_eq!(snapshot.effective_env["SAFE_DOTENV"], "ok");
        assert_eq!(snapshot.effective_env["SAFE_SETTINGS"], "ok");
    }

    #[test]
    fn malformed_env_uses_javascript_object_entries_and_excluded_vars_error_shape() {
        let root = TestDirectory::new();
        let paths = settings_paths(&root.0);
        let options = LoadSettingsOptions {
            process_env: HashMap::new(),
            ..LoadSettingsOptions::default()
        };
        let string_env = json!({
            "security":{"folderTrust":{"enabled":false}},
            "env":"xy"
        })
        .as_object()
        .unwrap()
        .clone();
        let snapshot =
            build_runtime_environment(&string_env, &paths, &options, Some(true)).unwrap();
        assert_eq!(snapshot.effective_env["0"], "x");
        assert_eq!(snapshot.effective_env["1"], "y");

        let invalid_exclusion = json!({
            "security":{"folderTrust":{"enabled":false}},
            "advanced":{"excludedEnvVars":{}},
            "env":{"EXAMPLE":"value"}
        })
        .as_object()
        .unwrap()
        .clone();
        assert!(matches!(
            build_runtime_environment(&invalid_exclusion, &paths, &options, Some(true)),
            Err(SettingsLoadError::InvalidSettingsEnvironment(message))
                if message == "advanced.excludedEnvVars.includes is not a function"
        ));
    }

    #[test]
    fn initial_load_does_not_apply_dotenv_reload_only_exclusions() {
        let root = TestDirectory::new();
        let paths = settings_paths(&root.0);
        let settings = json!({
            "security":{"folderTrust":{"enabled":false}},
            "advanced":{"excludedEnvVars":["SETTINGS_ONLY"]},
            "env":{"SETTINGS_ONLY":"initial-value"}
        })
        .as_object()
        .unwrap()
        .clone();
        let options = LoadSettingsOptions {
            process_env: HashMap::new(),
            ..LoadSettingsOptions::default()
        };
        let snapshot = load_environment(&settings, &paths, &options, Some(true)).unwrap();
        assert_eq!(snapshot.effective_env["SETTINGS_ONLY"], "initial-value");
    }

    #[test]
    fn cloud_shell_project_override_uses_file_then_safe_default() {
        let root = TestDirectory::new();
        let paths = settings_paths(&root.0);
        fs::create_dir_all(root.0.join("workspace")).unwrap();
        fs::write(
            root.0.join("workspace/.env"),
            "GOOGLE_CLOUD_PROJECT=project-from-file\n",
        )
        .unwrap();
        let settings = json!({"security":{"folderTrust":{"enabled":false}}})
            .as_object()
            .unwrap()
            .clone();
        let options = LoadSettingsOptions {
            process_env: HashMap::from([("CLOUD_SHELL".to_owned(), "true".to_owned())]),
            ..LoadSettingsOptions::default()
        };
        let snapshot = build_runtime_environment(&settings, &paths, &options, Some(true)).unwrap();
        assert_eq!(
            snapshot.effective_env["GOOGLE_CLOUD_PROJECT"],
            "project-from-file"
        );

        fs::remove_file(root.0.join("workspace/.env")).unwrap();
        let snapshot = build_runtime_environment(&settings, &paths, &options, Some(true)).unwrap();
        assert_eq!(
            snapshot.effective_env["GOOGLE_CLOUD_PROJECT"],
            "cloudshell-gca"
        );
    }
}
