// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Persistent extension favorites, installation scopes, and MCP visibility.

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use std::time::SystemTime;

use indexmap::IndexMap;
use serde::Serialize;
use serde_json::Value;

use crate::utils::atomic_file_write::{AtomicWriteOptions, atomic_write_file};

/// The install/visibility scope chosen for an extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ExtensionScope {
    User,
    Project,
}

/// Persisted extension preference state. Favorites retain their original JSON
/// values because the TypeScript reader accepts any array without filtering.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionPreferences {
    pub favorites: Vec<Value>,
    pub scopes: IndexMap<String, ExtensionScope>,
    pub disabled_mcp_servers: IndexMap<String, Vec<String>>,
}

#[derive(Clone, Debug)]
struct CachedPreferences {
    modified: SystemTime,
    preferences: ExtensionPreferences,
}

/// File-backed extension preferences with mtime caching and clone-safe reads.
pub struct ExtensionPreferencesStore {
    file_path: PathBuf,
    cache: Mutex<Option<CachedPreferences>>,
    mutation_lock: Mutex<()>,
}

impl ExtensionPreferencesStore {
    /// Create a store at the supplied preferences file path.
    pub fn new(file_path: impl Into<PathBuf>) -> Self {
        Self {
            file_path: file_path.into(),
            cache: Mutex::new(None),
            mutation_lock: Mutex::new(()),
        }
    }

    /// Read current state. Missing files and transient filesystem errors
    /// return fresh defaults; only malformed JSON is quarantined.
    pub fn read(&self) -> ExtensionPreferences {
        let metadata = match fs::metadata(&self.file_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return ExtensionPreferences::default();
            }
            Err(error) => {
                self.warn_read_error(&error);
                return ExtensionPreferences::default();
            }
        };
        let modified = metadata.modified().ok();
        if let Some(modified) = modified.as_ref() {
            let cache = lock(&self.cache);
            if let Some(cached) = cache.as_ref().filter(|cached| &cached.modified == modified) {
                return cached.preferences.clone();
            }
        }

        let contents = match fs::read(&self.file_path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return ExtensionPreferences::default();
            }
            Err(error) => {
                self.warn_read_error(&error);
                return ExtensionPreferences::default();
            }
        };
        let parsed = match serde_json::from_slice::<Value>(&contents) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.quarantine_corrupt_file(&error);
                return ExtensionPreferences::default();
            }
        };
        if parsed.is_null() {
            self.warn_read_error(&io::Error::new(
                io::ErrorKind::InvalidData,
                "parsed preferences value is null",
            ));
            return ExtensionPreferences::default();
        }
        let preferences = normalize_preferences(&parsed);
        if let Some(modified) = modified {
            *lock(&self.cache) = Some(CachedPreferences {
                modified,
                preferences: preferences.clone(),
            });
        }
        preferences
    }

    pub fn is_favorite(&self, name: &str) -> bool {
        self.read()
            .favorites
            .iter()
            .any(|favorite| favorite.as_str() == Some(name))
    }

    pub fn get_favorites(&self) -> Vec<Value> {
        self.read().favorites
    }

    /// Toggle a favorite, removing only the first existing occurrence to
    /// match JavaScript `indexOf`/`splice` behavior.
    pub fn toggle_favorite(&self, name: &str) -> io::Result<bool> {
        let _guard = lock(&self.mutation_lock);
        let mut preferences = self.read();
        let existing = preferences
            .favorites
            .iter()
            .position(|favorite| favorite.as_str() == Some(name));
        let now_favorite = if let Some(index) = existing {
            preferences.favorites.remove(index);
            false
        } else {
            preferences.favorites.push(Value::String(name.to_owned()));
            true
        };
        self.write(&preferences)?;
        Ok(now_favorite)
    }

    pub fn get_scope(&self, name: &str) -> Option<ExtensionScope> {
        self.read().scopes.get(name).copied()
    }

    pub fn get_scopes(&self) -> IndexMap<String, ExtensionScope> {
        self.read().scopes
    }

    pub fn set_scope(&self, name: impl Into<String>, scope: ExtensionScope) -> io::Result<()> {
        let _guard = lock(&self.mutation_lock);
        let mut preferences = self.read();
        preferences.scopes.insert(name.into(), scope);
        self.write(&preferences)
    }

    /// Return a clone of the individually disabled server names for an
    /// extension.
    pub fn get_disabled_mcp_servers(&self, extension_name: &str) -> Vec<String> {
        self.read()
            .disabled_mcp_servers
            .get(extension_name)
            .cloned()
            .unwrap_or_default()
    }

    /// Update one extension-scoped server toggle. No-op changes do not write
    /// the file or invalidate the parsed-file cache.
    pub fn set_mcp_server_disabled(
        &self,
        extension_name: &str,
        server_name: &str,
        disabled: bool,
    ) -> io::Result<()> {
        let _guard = lock(&self.mutation_lock);
        let mut preferences = self.read();
        let current = preferences
            .disabled_mcp_servers
            .get(extension_name)
            .cloned()
            .unwrap_or_default();
        if disabled {
            if current.iter().any(|name| name == server_name) {
                return Ok(());
            }
            let mut next = current;
            next.push(server_name.to_owned());
            preferences
                .disabled_mcp_servers
                .insert(extension_name.to_owned(), next);
        } else {
            if !current.iter().any(|name| name == server_name) {
                return Ok(());
            }
            let next: Vec<String> = current
                .into_iter()
                .filter(|name| name != server_name)
                .collect();
            if next.is_empty() {
                preferences
                    .disabled_mcp_servers
                    .shift_remove(extension_name);
            } else {
                preferences
                    .disabled_mcp_servers
                    .insert(extension_name.to_owned(), next);
            }
        }
        self.write(&preferences)
    }

    /// Remove all preference state for an extension. An unchanged file is
    /// left untouched.
    pub fn clear(&self, name: &str) -> io::Result<()> {
        let _guard = lock(&self.mutation_lock);
        let mut preferences = self.read();
        let favorite = preferences
            .favorites
            .iter()
            .position(|favorite| favorite.as_str() == Some(name));
        let mut changed = false;
        if let Some(index) = favorite {
            preferences.favorites.remove(index);
            changed = true;
        }
        if preferences.scopes.shift_remove(name).is_some() {
            changed = true;
        }
        if preferences
            .disabled_mcp_servers
            .shift_remove(name)
            .is_some()
        {
            changed = true;
        }
        if changed {
            self.write(&preferences)?;
        }
        Ok(())
    }

    fn write(&self, preferences: &ExtensionPreferences) -> io::Result<()> {
        if let Some(parent) = self
            .file_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        let contents = serde_json::to_vec_pretty(preferences).map_err(io::Error::other)?;
        // Match the TypeScript atomic writer defaults: follow symlinks and
        // preserve existing permissions (new files use the process umask).
        let options = AtomicWriteOptions::default();
        atomic_write_file(&self.file_path, &contents, &options)?;
        *lock(&self.cache) = None;
        Ok(())
    }

    fn quarantine_corrupt_file(&self, error: &serde_json::Error) {
        let mut quarantine_name = self.file_path.as_os_str().to_os_string();
        quarantine_name.push(".corrupted");
        let quarantine_path = PathBuf::from(quarantine_name);
        match fs::rename(&self.file_path, &quarantine_path) {
            Ok(()) => eprintln!(
                "[warn] Corrupt extension preferences at {} moved aside to {} ({error})",
                self.file_path.display(),
                quarantine_path.display()
            ),
            Err(rename_error) => eprintln!(
                "[warn] Corrupt extension preferences at {} could not be moved aside: {rename_error}",
                self.file_path.display()
            ),
        }
    }

    fn warn_read_error(&self, error: &io::Error) {
        eprintln!(
            "[warn] Could not read extension preferences at {}: {error}. Using defaults for this session.",
            self.file_path.display()
        );
    }
}

fn normalize_preferences(parsed: &Value) -> ExtensionPreferences {
    let mut preferences = ExtensionPreferences::default();
    let Some(parsed) = parsed.as_object() else {
        return preferences;
    };

    if let Some(favorites) = parsed.get("favorites").and_then(Value::as_array) {
        preferences.favorites = favorites.clone();
    }
    if let Some(scopes) = parsed.get("scopes") {
        for_each_named_entry(scopes, |name, value| {
            let scope = match value.as_str() {
                Some("user") => Some(ExtensionScope::User),
                Some("project") => Some(ExtensionScope::Project),
                _ => None,
            };
            if let Some(scope) = scope {
                preferences.scopes.insert(name.to_owned(), scope);
            }
        });
    }
    if let Some(disabled) = parsed.get("disabledMcpServers") {
        for_each_named_entry(disabled, |name, value| {
            let Some(values) = value.as_array() else {
                return;
            };
            let servers: Vec<String> = values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect();
            if !servers.is_empty() {
                preferences
                    .disabled_mcp_servers
                    .insert(name.to_owned(), servers);
            }
        });
    }
    preferences
}

fn for_each_named_entry(value: &Value, mut callback: impl FnMut(&str, &Value)) {
    match value {
        Value::Object(object) => {
            for (name, value) in object {
                callback(name, value);
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                callback(&index.to_string(), value);
            }
        }
        _ => {}
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
