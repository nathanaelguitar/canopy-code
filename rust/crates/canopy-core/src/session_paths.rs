//! Canonical per-project Canopy session paths and IDs.
//!
//! This mirrors the session path rules in `config/storage.ts` and
//! `services/sessionService.ts`.

use std::path::{Path, PathBuf};

use crate::storage::Storage;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SessionArchiveState {
    #[default]
    Active,
    Archived,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionPaths {
    storage: Storage,
}

/// Resolve the process runtime directory with the same environment priority
/// as Canopy Storage: `CANOPY_RUNTIME_DIR`, then the global Canopy directory
/// selected by `QWEN_HOME` or `<home>/.canopy`.
pub fn resolve_runtime_base_dir(
    canopy_runtime_dir: Option<&str>,
    qwen_home: Option<&str>,
    home_dir: Option<&Path>,
    cwd: &Path,
    temp_dir: &Path,
) -> PathBuf {
    crate::storage::resolve_runtime_base_dir(canopy_runtime_dir, qwen_home, home_dir, cwd, temp_dir)
}

/// Hash the project root with the persisted SHA-256 contract.
pub fn get_project_hash(project_root: &Path) -> String {
    let digest = Sha256::digest(project_root.to_string_lossy().as_bytes());
    format!("{digest:x}")
}

/// Convert a project path to the directory key used by Storage.
pub fn sanitize_cwd(project_root: &Path, windows: bool) -> String {
    let cwd = project_root.to_string_lossy();
    let normalized = if windows {
        cwd.to_lowercase()
    } else {
        cwd.into_owned()
    };
    normalized
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect()
}

/// Match the current UUID-like transcript filename check. It validates the
/// filename alphabet/length, preserving legacy 32-character IDs.
pub fn is_valid_session_id(session_id: &str) -> bool {
    (32..=36).contains(&session_id.len())
        && session_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
}

impl SessionPaths {
    pub fn new(runtime_base_dir: impl Into<PathBuf>, project_root: impl Into<PathBuf>) -> Self {
        Self {
            storage: Storage::with_runtime_base_dir(project_root, runtime_base_dir.into()),
        }
    }

    pub fn runtime_base_dir(&self) -> &Path {
        self.storage.runtime_base_dir()
    }

    pub fn project_root(&self) -> &Path {
        self.storage.get_project_root()
    }

    pub fn project_hash(&self) -> String {
        get_project_hash(self.project_root())
    }

    pub fn project_directory(&self, windows: bool) -> PathBuf {
        self.storage.get_project_dir_for_platform(windows)
    }

    pub fn chats_directory(&self, state: SessionArchiveState, windows: bool) -> PathBuf {
        let active = self.project_directory(windows).join("chats");
        match state {
            SessionArchiveState::Active => active,
            SessionArchiveState::Archived => active.join("archive"),
        }
    }

    pub fn transcript_path(
        &self,
        session_id: &str,
        state: SessionArchiveState,
        windows: bool,
    ) -> Option<PathBuf> {
        if !is_valid_session_id(session_id) {
            return None;
        }
        Some(
            self.chats_directory(state, windows)
                .join(format!("{session_id}.jsonl")),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_directory_obeys_environment_priority_and_path_resolution() {
        let home = Path::new("/Users/example");
        let cwd = Path::new("/work/project");
        let temp = Path::new("/tmp");
        assert_eq!(
            resolve_runtime_base_dir(
                Some("~/canopy-runtime"),
                Some("/legacy"),
                Some(home),
                cwd,
                temp
            ),
            PathBuf::from("/Users/example/canopy-runtime")
        );
        assert_eq!(
            resolve_runtime_base_dir(None, Some(".canopy-local"), Some(home), cwd, temp),
            PathBuf::from("/work/project/.canopy-local")
        );
        assert_eq!(
            resolve_runtime_base_dir(None, None, Some(home), cwd, temp),
            PathBuf::from("/Users/example/.canopy")
        );
    }

    #[test]
    fn project_hash_and_sanitized_directory_are_stable() {
        let paths = SessionPaths::new("/state", "/Users/a.b/project");
        assert_eq!(
            paths.project_hash(),
            "6593fd1a59ea471e64f210cc88b5c122205f5c69a233df266c4e85e0110fc5ff"
        );
        assert_eq!(
            paths.project_directory(false),
            PathBuf::from("/state/projects/-Users-a-b-project")
        );
    }

    #[test]
    fn sanitizer_replaces_one_non_ascii_code_point_with_one_dash() {
        assert_eq!(sanitize_cwd(Path::new("/tmp/🦀"), false), "-tmp--");
    }

    #[test]
    fn transcript_paths_match_active_and_archive_layout_and_validate_ids() {
        let paths = SessionPaths::new("/state", "/work/project");
        let id = "00000000-0000-4000-8000-000000000001";
        assert_eq!(
            paths.transcript_path(id, SessionArchiveState::Active, false),
            Some(PathBuf::from(
                "/state/projects/-work-project/chats/00000000-0000-4000-8000-000000000001.jsonl"
            ))
        );
        assert_eq!(
            paths.transcript_path(id, SessionArchiveState::Archived, false),
            Some(PathBuf::from(
                "/state/projects/-work-project/chats/archive/00000000-0000-4000-8000-000000000001.jsonl"
            ))
        );
        assert!(
            paths
                .transcript_path("../../outside", SessionArchiveState::Active, false)
                .is_none()
        );
    }
}
