//! File-backed marketplace source registry.
//!
//! This ports the synchronous persistence behavior of
//! `packages/core/src/extension/sourceRegistry.ts`. Raw JSON reads preserve
//! array entries as-is, so malformed-but-valid records survive unrelated
//! mutations just as they do in the TypeScript store. [`read`] additionally
//! offers a typed view and skips entries that do not match [`ExtensionSource`].

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::utils::atomic_file_write::{AtomicWriteOptions, atomic_write_file};

/// Type of marketplace source, using the persisted TypeScript spelling.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ExtensionSourceType {
    Github,
    Git,
    Http,
    Local,
}

impl ExtensionSourceType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Github => "github",
            Self::Git => "git",
            Self::Http => "http",
            Self::Local => "local",
        }
    }
}

/// Persisted marketplace source record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionSource {
    pub name: String,
    pub source: String,
    #[serde(rename = "type")]
    pub source_type: ExtensionSourceType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub added_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_at: Option<String>,
}

impl ExtensionSource {
    pub fn new(
        name: impl Into<String>,
        source: impl Into<String>,
        source_type: ExtensionSourceType,
    ) -> Self {
        Self {
            name: name.into(),
            source: source.into(),
            source_type,
            added_at: None,
            last_updated_at: None,
        }
    }
}

/// Synchronous marketplace source registry at a host-supplied path.
pub struct ExtensionSourceStore {
    file_path: PathBuf,
    mutation_lock: Mutex<()>,
}

impl ExtensionSourceStore {
    pub fn new(file_path: impl Into<PathBuf>) -> Self {
        Self {
            file_path: file_path.into(),
            mutation_lock: Mutex::new(()),
        }
    }

    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// Read only well-formed typed source records. Invalid array entries are
    /// ignored here without treating valid JSON as a corrupt file.
    pub fn read(&self) -> Vec<ExtensionSource> {
        self.read_values()
            .into_iter()
            .filter_map(|value| serde_json::from_value(value).ok())
            .collect()
    }

    /// Read the stored array without schema filtering. A missing file, a
    /// transient read error, a non-array JSON value, or a failed parse returns
    /// an empty list. Only failed JSON parsing triggers quarantine.
    pub fn read_values(&self) -> Vec<Value> {
        let contents = match fs::read(&self.file_path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Vec::new(),
            Err(error) => {
                self.warn_read_error(&error);
                return Vec::new();
            }
        };

        // Node's readFileSync(path, 'utf-8') replaces invalid UTF-8 sequences
        // with U+FFFD. Use the same lossy decode so malformed encoding reaches
        // JSON parsing and is handled as parse corruption when appropriate.
        let contents = String::from_utf8_lossy(&contents);
        let parsed = match serde_json::from_str::<Value>(&contents) {
            Ok(parsed) => parsed,
            Err(_) => {
                self.quarantine_corrupt_file();
                return Vec::new();
            }
        };
        parsed.as_array().cloned().unwrap_or_default()
    }

    /// Add or replace a source when either its name or source string matches.
    pub fn add(&self, source: &ExtensionSource) -> io::Result<()> {
        let _guard = lock(&self.mutation_lock);
        let mut sources = self.read_values();
        sources.retain(|existing| {
            existing.get("name").and_then(Value::as_str) != Some(source.name.as_str())
                && existing.get("source").and_then(Value::as_str) != Some(source.source.as_str())
        });
        let source = serde_json::to_value(source).map_err(io::Error::other)?;
        sources.push(source);
        self.write(&sources)
    }

    /// Remove a marketplace by name. Returns whether a matching row existed.
    pub fn remove(&self, name: &str) -> io::Result<bool> {
        let _guard = lock(&self.mutation_lock);
        let mut sources = self.read_values();
        let original_len = sources.len();
        sources.retain(|source| source.get("name").and_then(Value::as_str) != Some(name));
        if sources.len() == original_len {
            return Ok(false);
        }
        self.write(&sources)?;
        Ok(true)
    }

    fn write(&self, sources: &[Value]) -> io::Result<()> {
        let parent = self
            .file_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let contents = serde_json::to_vec_pretty(sources).map_err(io::Error::other)?;
        // Match atomicWriteFileSync's defaults: follow destination symlinks,
        // retain an existing file mode, and use the process umask for new files.
        atomic_write_file(&self.file_path, &contents, &AtomicWriteOptions::default())
    }

    fn quarantine_corrupt_file(&self) {
        let mut quarantine_name = self.file_path.as_os_str().to_os_string();
        quarantine_name.push(".corrupted");
        let quarantine_path = PathBuf::from(quarantine_name);
        match fs::rename(&self.file_path, &quarantine_path) {
            Ok(()) => eprintln!(
                "[warn] Corrupt extension state file {} moved aside to {}",
                self.file_path.display(),
                quarantine_path.display()
            ),
            Err(rename_error) => eprintln!(
                "[warn] Corrupt extension state file {} could not be moved aside: {rename_error}",
                self.file_path.display()
            ),
        }
    }

    fn warn_read_error(&self, error: &io::Error) {
        eprintln!(
            "[warn] Could not read marketplace registry at {}: {error}. Using an empty source list for this session.",
            self.file_path.display()
        );
    }
}

fn lock(mutex: &Mutex<()>) -> MutexGuard<'_, ()> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
