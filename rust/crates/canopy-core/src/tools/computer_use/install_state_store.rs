//! File-backed install approval state.
//!
//! This mirrors `packages/core/src/tools/computer-use/install-state.ts`:
//! reads are deliberately tolerant and treat every failure as "not approved",
//! while saves create the containing directory and directly replace the JSON
//! file. In particular, saves preserve the source's non-atomic write behavior.

use super::install_state::{InstallState, install_state_from_json, install_state_to_json};
use super::install_state_path;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Return the file used to persist computer-use install approval under `home`.
pub fn install_state_path_for(home: impl AsRef<Path>) -> PathBuf {
    install_state_path(home)
}

/// Load the approval record. Any missing, unreadable, invalid UTF-8, malformed,
/// or wrongly typed state is treated as not approved, matching the TypeScript
/// implementation's catch-all fallback.
pub fn load_install_state(home: impl AsRef<Path>) -> Option<InstallState> {
    let text = fs::read_to_string(install_state_path_for(home)).ok()?;
    install_state_from_json(&text)
}

/// Persist an approval record as two-space-indented UTF-8 JSON.
///
/// Parent directories are created recursively. As in the TypeScript source,
/// this writes directly to the destination rather than using atomic replace;
/// directory and write failures are returned to the caller.
pub fn save_install_state(home: impl AsRef<Path>, state: &InstallState) -> io::Result<()> {
    let path = install_state_path_for(home);
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "install state path has no parent directory",
        )
    })?;
    fs::create_dir_all(parent)?;
    let json = install_state_to_json(state)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    fs::write(path, json)
}

/// Return whether the persisted record approves this exact package spec.
pub fn is_package_spec_approved(home: impl AsRef<Path>, package_spec: &str) -> bool {
    load_install_state(home).is_some_and(|state| state.approves(package_spec))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

    struct TempHome(PathBuf);

    impl TempHome {
        fn new() -> Self {
            let sequence = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "canopy-install-state-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create temporary home");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempHome {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn sample_state() -> InstallState {
        InstallState {
            approved_package_spec: "cua-driver-rs@0.5.2".to_owned(),
            approved_at_iso: "2026-05-28T10:00:00Z".to_owned(),
        }
    }

    #[test]
    fn path_matches_typescript_layout() {
        let home = TempHome::new();
        assert_eq!(
            install_state_path_for(home.path()),
            home.path()
                .join(".canopy")
                .join("computer-use")
                .join("installed.json")
        );
    }

    #[test]
    fn missing_and_invalid_state_are_treated_as_unapproved() {
        let home = TempHome::new();
        assert_eq!(load_install_state(home.path()), None);
        assert!(!is_package_spec_approved(
            home.path(),
            "cua-driver-rs@0.5.2"
        ));

        let path = install_state_path_for(home.path());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        for malformed in [
            "not-json",
            r#"{"approvedPackageSpec":42,"approvedAtIso":"now"}"#,
            r#"{"approvedPackageSpec":"pkg"}"#,
            r#"{"approvedPackageSpec":"pkg","approvedAtIso":false}"#,
        ] {
            fs::write(&path, malformed.as_bytes()).unwrap();
            assert_eq!(
                load_install_state(home.path()),
                None,
                "input: {malformed:?}"
            );
        }
        fs::write(&path, [0xff, 0xfe]).unwrap();
        assert_eq!(load_install_state(home.path()), None);
    }

    #[test]
    fn save_creates_parent_directories_and_round_trips() {
        let home = TempHome::new();
        let state = sample_state();
        save_install_state(home.path(), &state).unwrap();
        assert_eq!(load_install_state(home.path()), Some(state));
    }

    #[test]
    fn save_serialization_matches_two_space_json_without_trailing_newline() {
        let home = TempHome::new();
        save_install_state(home.path(), &sample_state()).unwrap();
        let contents = fs::read_to_string(install_state_path_for(home.path())).unwrap();
        assert_eq!(
            contents,
            concat!(
                "{\n",
                "  \"approvedPackageSpec\": \"cua-driver-rs@0.5.2\",\n",
                "  \"approvedAtIso\": \"2026-05-28T10:00:00Z\"\n",
                "}"
            )
        );
        assert!(!contents.ends_with('\n'));
    }

    #[test]
    fn load_ignores_unknown_fields_and_approval_requires_exact_match() {
        let home = TempHome::new();
        let path = install_state_path_for(home.path());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            r#"{"approvedPackageSpec":"cua-driver-rs@0.5.2","approvedAtIso":"2026-05-28T10:00:00Z","futureField":true}"#,
        )
        .unwrap();

        assert!(is_package_spec_approved(home.path(), "cua-driver-rs@0.5.2"));
        assert!(!is_package_spec_approved(
            home.path(),
            "cua-driver-rs@0.6.0"
        ));
        assert!(!is_package_spec_approved(
            home.path(),
            " Cua-driver-rs@0.5.2"
        ));
    }

    #[test]
    fn save_propagates_directory_creation_failures() {
        let home = TempHome::new();
        let canopy_path = home.path().join(".canopy");
        fs::write(&canopy_path, "a file where the directory should be").unwrap();
        let error = save_install_state(home.path(), &sample_state()).unwrap_err();
        assert!(matches!(
            error.kind(),
            io::ErrorKind::AlreadyExists | io::ErrorKind::NotADirectory
        ));
    }
}
