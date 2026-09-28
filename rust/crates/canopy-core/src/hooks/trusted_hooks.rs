//! Per-project approval storage for executable hooks.
//!
//! Port of `packages/core/src/hooks/trustedHooks.ts`. The file is deliberately
//! private and atomically replaced because it records user-approved commands.

use std::fs;
use std::io;
use std::path::PathBuf;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::storage::Storage;
use crate::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};

pub const TRUSTED_HOOKS_FILENAME: &str = "trusted_hooks.json";
const PRIVATE_FILE_MODE: u32 = 0o600;

/// The hook fields used by `getHookKey` and the user-facing untrusted list.
/// Other execution fields are intentionally omitted because trust identity
/// depends only on the hook kind, optional name, and kind-specific identifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HookConfig {
    Command {
        name: Option<String>,
        command: String,
    },
    Http {
        name: Option<String>,
        url: String,
    },
    Function {
        name: Option<String>,
        id: Option<String>,
    },
    Prompt {
        name: Option<String>,
        prompt: String,
    },
    /// Future or malformed hook types use `name || "unknown"` as their key.
    Other {
        name: Option<String>,
    },
}

impl HookConfig {
    fn name(&self) -> Option<&str> {
        match self {
            Self::Command { name, .. }
            | Self::Http { name, .. }
            | Self::Function { name, .. }
            | Self::Prompt { name, .. }
            | Self::Other { name } => name.as_deref(),
        }
    }

    /// Return the source-compatible stable trust key for this hook.
    pub fn hook_key(&self) -> String {
        let name = self.name().unwrap_or_default();
        match self {
            Self::Command { command, .. } => {
                if name.is_empty() {
                    command.clone()
                } else {
                    format!("{name}:{command}")
                }
            }
            Self::Http { url, .. } => {
                if name.is_empty() {
                    url.clone()
                } else {
                    format!("{name}:{url}")
                }
            }
            Self::Function { id, .. } => {
                let identifier = id.as_deref().unwrap_or("function");
                if name.is_empty() {
                    identifier.to_owned()
                } else {
                    format!("{name}:{identifier}")
                }
            }
            Self::Prompt { prompt, .. } => {
                if name.is_empty() {
                    prompt.clone()
                } else {
                    format!("{name}:{prompt}")
                }
            }
            Self::Other { .. } => {
                if name.is_empty() {
                    "unknown".to_owned()
                } else {
                    name.to_owned()
                }
            }
        }
    }

    fn identifier(&self) -> &str {
        match self {
            Self::Command { name, command } => name
                .as_deref()
                .filter(|name| !name.is_empty())
                .or_else(|| (!command.is_empty()).then_some(command.as_str()))
                .unwrap_or("unknown-hook"),
            Self::Http { name, .. }
            | Self::Function { name, .. }
            | Self::Prompt { name, .. }
            | Self::Other { name } => name
                .as_deref()
                .filter(|name| !name.is_empty())
                .unwrap_or("unknown-hook"),
        }
    }
}

/// An optional event's hook definitions. `None` represents a non-array event
/// value, which the TypeScript implementation ignores.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HookEvent {
    pub event_name: String,
    pub definitions: Option<Vec<Option<HookDefinition>>>,
}

impl HookEvent {
    pub fn new(event_name: impl Into<String>, definitions: Vec<HookDefinition>) -> Self {
        Self {
            event_name: event_name.into(),
            definitions: Some(definitions.into_iter().map(Some).collect()),
        }
    }
}

/// A matcher definition may be malformed or omit its `hooks` array in source
/// data. Those cases are skipped, matching the TypeScript runtime guards.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HookDefinition {
    pub hooks: Option<Vec<Option<HookConfig>>>,
}

impl HookDefinition {
    pub fn new(hooks: Vec<HookConfig>) -> Self {
        Self {
            hooks: Some(hooks.into_iter().map(Some).collect()),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
struct TrustedHooksConfig(IndexMap<String, Vec<String>>);

/// Manages trusted hook keys, scoped to the exact project path.
pub struct TrustedHooksManager {
    config_path: PathBuf,
    trusted_hooks: TrustedHooksConfig,
}

impl Default for TrustedHooksManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TrustedHooksManager {
    /// Load the global Canopy trusted-hook file.
    pub fn new() -> Self {
        Self::from_config_path(Storage::get_global_canopy_dir().join(TRUSTED_HOOKS_FILENAME))
    }

    /// Load a trusted-hook file at an explicit path.
    ///
    /// The path form is useful to embedders with an isolated Canopy root and
    /// keeps the storage behavior independently testable.
    pub fn from_config_path(config_path: impl Into<PathBuf>) -> Self {
        let mut manager = Self {
            config_path: config_path.into(),
            trusted_hooks: TrustedHooksConfig::default(),
        };
        manager.load();
        manager
    }

    fn load(&mut self) {
        let content = match fs::read_to_string(&self.config_path) {
            Ok(content) => content,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return,
            // Like the source's catch, unreadable and invalid JSON files reset
            // the in-memory trust map rather than preventing startup.
            Err(_) => {
                self.trusted_hooks = TrustedHooksConfig::default();
                return;
            }
        };

        match serde_json::from_str(&content) {
            Ok(config) => self.trusted_hooks = config,
            Err(_) => self.trusted_hooks = TrustedHooksConfig::default(),
        }
    }

    /// Return unique untrusted identifiers in first-seen event/definition/
    /// hook order.
    pub fn get_untrusted_hooks(&self, project_path: &str, events: &[HookEvent]) -> Vec<String> {
        let trusted_keys = self
            .trusted_hooks
            .0
            .get(project_path)
            .map(|keys| keys.iter().collect::<std::collections::HashSet<_>>())
            .unwrap_or_default();
        let mut seen_identifiers = std::collections::HashSet::new();
        let mut untrusted = Vec::new();

        for event in events {
            let Some(definitions) = &event.definitions else {
                continue;
            };
            for definition in definitions.iter().flatten() {
                let Some(hooks) = &definition.hooks else {
                    continue;
                };
                for hook in hooks.iter().flatten() {
                    if !trusted_keys.contains(&hook.hook_key()) {
                        let identifier = hook.identifier();
                        if seen_identifiers.insert(identifier.to_owned()) {
                            untrusted.push(identifier.to_owned());
                        }
                    }
                }
            }
        }

        untrusted
    }

    /// Trust every configured hook key for one exact project path.
    ///
    /// Save errors are intentionally swallowed after the in-memory config has
    /// been updated, matching `TrustedHooksManager.trustHooks`.
    pub fn trust_hooks(&mut self, project_path: &str, events: &[HookEvent]) {
        let current = self
            .trusted_hooks
            .0
            .entry(project_path.to_owned())
            .or_default();
        let mut known = current
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();

        for event in events {
            let Some(definitions) = &event.definitions else {
                continue;
            };
            for definition in definitions.iter().flatten() {
                let Some(hooks) = &definition.hooks else {
                    continue;
                };
                for hook in hooks.iter().flatten() {
                    let key = hook.hook_key();
                    if known.insert(key.clone()) {
                        current.push(key);
                    }
                }
            }
        }

        self.save();
    }

    fn save(&self) {
        let Some(parent) = self.config_path.parent() else {
            return;
        };
        let result = (|| {
            fs::create_dir_all(parent)?;
            let contents = serde_json::to_vec_pretty(&self.trusted_hooks)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            atomic_write_file(
                &self.config_path,
                &contents,
                &AtomicWriteOptions {
                    mode: Some(PRIVATE_FILE_MODE),
                    force_mode: true,
                    symlink_policy: SymlinkPolicy::NoFollow,
                    ..AtomicWriteOptions::default()
                },
            )
        })();
        // The source catches and logs write errors, preserving its updated
        // in-memory state. Keep the same non-throwing behavior here.
        let _ = result;
    }

    #[cfg(test)]
    fn config_path(&self) -> &std::path::Path {
        &self.config_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("canopy-trusted-hooks-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn command(command: &str) -> HookConfig {
        HookConfig::Command {
            name: None,
            command: command.to_owned(),
        }
    }

    fn event(hooks: Vec<HookConfig>) -> HookEvent {
        HookEvent::new("PreToolUse", vec![HookDefinition::new(hooks)])
    }

    #[test]
    fn hook_key_matches_each_source_variant_and_name_fallback() {
        assert_eq!(
            HookConfig::Command {
                name: Some("clean".into()),
                command: "echo ok".into(),
            }
            .hook_key(),
            "clean:echo ok"
        );
        assert_eq!(
            HookConfig::Http {
                name: None,
                url: "https://example.test/hook".into(),
            }
            .hook_key(),
            "https://example.test/hook"
        );
        assert_eq!(
            HookConfig::Function {
                name: None,
                id: None,
            }
            .hook_key(),
            "function"
        );
        assert_eq!(
            HookConfig::Function {
                name: Some("named".into()),
                id: None,
            }
            .hook_key(),
            "named:function"
        );
        assert_eq!(
            HookConfig::Prompt {
                name: Some(String::new()),
                prompt: "Check $ARGUMENTS".into(),
            }
            .hook_key(),
            "Check $ARGUMENTS"
        );
        assert_eq!(
            HookConfig::Other {
                name: Some("future-hook".into()),
            }
            .hook_key(),
            "future-hook"
        );
        assert_eq!(HookConfig::Other { name: None }.hook_key(), "unknown");
    }

    #[test]
    fn trust_is_project_scoped_and_keys_are_deduplicated_in_first_seen_order() {
        let directory = TestDir::new();
        let mut manager = TrustedHooksManager::from_config_path(directory.0.join("hooks.json"));
        let events = [
            event(vec![command("one"), command("two"), command("one")]),
            HookEvent::new(
                "PostToolUse",
                vec![HookDefinition::new(vec![command("three")])],
            ),
        ];

        manager.trust_hooks("/project/a", &events);

        assert_eq!(
            manager.trusted_hooks.0.get("/project/a").unwrap(),
            &["one", "two", "three"]
        );
        assert_eq!(
            manager.get_untrusted_hooks("/project/a", &events),
            Vec::<String>::new()
        );
        assert_eq!(
            manager.get_untrusted_hooks("/project/b", &events),
            vec!["one", "two", "three"]
        );
    }

    #[test]
    fn untrusted_names_fall_back_and_deduplicate_without_changing_order() {
        let manager = TrustedHooksManager::from_config_path("/definitely/not/a/config");
        let events = [event(vec![
            HookConfig::Command {
                name: Some("friendly".into()),
                command: "cmd-1".into(),
            },
            command("cmd-2"),
            HookConfig::Http {
                name: None,
                url: "https://example.test".into(),
            },
            command("cmd-2"),
            HookConfig::Function {
                name: None,
                id: Some("function-id".into()),
            },
            command(""),
        ])];

        assert_eq!(
            manager.get_untrusted_hooks("/project/a", &events),
            vec!["friendly", "cmd-2", "unknown-hook"]
        );
        assert_eq!(
            manager.get_untrusted_hooks("/project/a", &[event(vec![command("")])]),
            vec!["unknown-hook"]
        );
    }

    #[test]
    fn malformed_file_recovers_as_empty_then_persists_valid_json() {
        let directory = TestDir::new();
        let path = directory.0.join("trusted_hooks.json");
        fs::write(&path, b"{ this is not json").unwrap();
        let mut manager = TrustedHooksManager::from_config_path(&path);

        assert_eq!(
            manager.get_untrusted_hooks("/project/a", &[event(vec![command("echo ok")])]),
            vec!["echo ok"]
        );
        manager.trust_hooks("/project/a", &[event(vec![command("echo ok")])]);

        let saved: IndexMap<String, Vec<String>> =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(saved.get("/project/a").unwrap(), &["echo ok"]);
    }

    #[test]
    fn saves_with_owner_only_mode_and_heals_existing_permissions() {
        let directory = TestDir::new();
        let path = directory.0.join("trusted_hooks.json");
        fs::write(&path, b"{}").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        }
        let mut manager = TrustedHooksManager::from_config_path(&path);

        manager.trust_hooks("/project/a", &[event(vec![command("safe")])]);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
                PRIVATE_FILE_MODE
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn no_follow_replaces_a_symlink_without_writing_to_its_target() {
        use std::os::unix::fs::symlink;

        let directory = TestDir::new();
        let target = directory.0.join("target.json");
        let path = directory.0.join(TRUSTED_HOOKS_FILENAME);
        fs::write(&target, b"target data").unwrap();
        symlink(&target, &path).unwrap();
        let mut manager = TrustedHooksManager::from_config_path(&path);

        manager.trust_hooks("/project/a", &[event(vec![command("safe")])]);

        assert_eq!(fs::read(&target).unwrap(), b"target data");
        assert!(
            !fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            manager.get_untrusted_hooks("/project/a", &[event(vec![command("safe")])]),
            Vec::<String>::new()
        );
    }

    #[test]
    fn save_failure_is_swallowed_after_in_memory_trust_updates() {
        let directory = TestDir::new();
        let path = directory.0.join("is-a-directory");
        fs::create_dir(&path).unwrap();
        let mut manager = TrustedHooksManager::from_config_path(&path);

        manager.trust_hooks("/project/a", &[event(vec![command("safe")])]);

        assert_eq!(
            manager.get_untrusted_hooks("/project/a", &[event(vec![command("safe")])]),
            Vec::<String>::new()
        );
    }

    #[test]
    fn malformed_event_and_definition_shapes_are_skipped() {
        let manager = TrustedHooksManager::from_config_path("/definitely/not/a/config");
        let events = [
            HookEvent {
                event_name: "wrong-shape".into(),
                definitions: None,
            },
            HookEvent {
                event_name: "null-definition".into(),
                definitions: Some(vec![None]),
            },
            HookEvent {
                event_name: "missing-hooks".into(),
                definitions: Some(vec![Some(HookDefinition::default())]),
            },
        ];
        assert!(
            manager
                .get_untrusted_hooks("/project/a", &events)
                .is_empty()
        );
    }

    #[test]
    fn config_file_uses_global_filename() {
        let directory = TestDir::new();
        let manager =
            TrustedHooksManager::from_config_path(directory.0.join(TRUSTED_HOOKS_FILENAME));
        assert_eq!(
            manager.config_path().file_name().unwrap(),
            TRUSTED_HOOKS_FILENAME
        );
    }
}
