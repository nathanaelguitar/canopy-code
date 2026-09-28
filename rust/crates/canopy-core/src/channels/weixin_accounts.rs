//! Credential storage for the Weixin channel.
//!
//! Port of `packages/channels/weixin/src/accounts.ts`. State is stored under
//! `WEIXIN_STATE_DIR` when set, or under
//! `<global Qwen directory>/channels/weixin` otherwise.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const ACCOUNT_FILE: &str = "account.json";
const SOURCE_TEMP_PREFIX: &str = "account.json.";
const SOURCE_TEMP_SUFFIX: &str = ".tmp";
const ATOMIC_TEMP_PREFIX: &str = ".account.json.canopy-";

/// Default Weixin API base URL.
pub const DEFAULT_BASE_URL: &str = "https://ilinkai.weixin.qq.com";

/// Stored account credentials.
///
/// `userId` is omitted from JSON when absent, matching `JSON.stringify` on the
/// optional TypeScript property.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountData {
    pub token: String,
    pub base_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    pub saved_at: String,
}

/// Resolve and create the Weixin state directory.
///
/// An empty override behaves like an unset environment variable, as it does in
/// the TypeScript `process.env.WEIXIN_STATE_DIR || fallback` expression.
pub fn get_state_dir() -> io::Result<PathBuf> {
    let override_dir = std::env::var_os("WEIXIN_STATE_DIR");
    let directory = resolve_state_dir(override_dir.as_deref(), || {
        crate::channels::paths::get_global_qwen_dir()
    })?;
    ensure_state_dir(&directory)?;
    Ok(directory)
}

/// Read the stored JSON account value. Missing, unreadable, or malformed files
/// are treated as no account; state-directory resolution errors are returned.
///
/// This intentionally returns `Value`, not `AccountData`: TypeScript's source
/// only casts parsed JSON at compile time and accepts any valid JSON shape.
pub fn load_account() -> io::Result<Option<Value>> {
    let directory = get_state_dir()?;
    Ok(load_account_in(&directory))
}

/// Save an account using a private, same-directory atomic replacement.
pub fn save_account(data: &AccountData) -> io::Result<()> {
    let directory = get_state_dir()?;
    save_account_in(&directory, data)
}

/// Remove the account and orphaned temporary credential files.
pub fn clear_account() -> io::Result<()> {
    let directory = get_state_dir()?;
    clear_account_in(&directory)
}

fn resolve_state_dir(
    override_dir: Option<&OsStr>,
    global_qwen_dir: impl FnOnce() -> io::Result<PathBuf>,
) -> io::Result<PathBuf> {
    match override_dir.filter(|value| !value.is_empty()) {
        Some(directory) => Ok(PathBuf::from(directory)),
        None => Ok(global_qwen_dir()?.join("channels").join("weixin")),
    }
}

fn ensure_state_dir(directory: &Path) -> io::Result<()> {
    // `exists` follows symlinks like Node's existsSync. An existing non-dir is
    // returned as-is by the source; later file operations then fail or load as
    // empty in the same way.
    if !directory.exists() {
        fs::create_dir_all(directory)?;
    }
    Ok(())
}

fn load_account_in(directory: &Path) -> Option<Value> {
    let bytes = fs::read(directory.join(ACCOUNT_FILE)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn save_account_in(directory: &Path, data: &AccountData) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(data).map_err(io::Error::other)?;
    let options = crate::utils::atomic_file_write::AtomicWriteOptions {
        mode: Some(0o600),
        force_mode: true,
        symlink_policy: crate::utils::atomic_file_write::SymlinkPolicy::NoFollow,
        ..crate::utils::atomic_file_write::AtomicWriteOptions::default()
    };
    crate::utils::atomic_file_write::atomic_write_file(
        directory.join(ACCOUNT_FILE),
        &bytes,
        &options,
    )
}

fn clear_account_in(directory: &Path) -> io::Result<()> {
    let account_path = directory.join(ACCOUNT_FILE);
    if account_path.exists() {
        fs::remove_file(&account_path)?;
    }

    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        if is_account_temp_name(&name) {
            // A failed or interrupted cleanup should not prevent logout from
            // removing other matching credential remnants.
            let _ = fs::remove_file(entry.path());
        }
    }
    Ok(())
}

fn is_account_temp_name(name: &OsStr) -> bool {
    let name = name.to_string_lossy();
    (name.starts_with(SOURCE_TEMP_PREFIX) && name.ends_with(SOURCE_TEMP_SUFFIX))
        || (name.starts_with(ATOMIC_TEMP_PREFIX) && name.ends_with(SOURCE_TEMP_SUFFIX))
}

#[cfg(test)]
mod tests {
    use super::{
        AccountData, DEFAULT_BASE_URL, clear_account_in, ensure_state_dir, is_account_temp_name,
        load_account_in, resolve_state_dir, save_account_in,
    };
    use std::ffi::OsStr;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "canopy-weixin-accounts-{}-{sequence}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).expect("create test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn data() -> AccountData {
        AccountData {
            token: "super-secret-token".to_owned(),
            base_url: DEFAULT_BASE_URL.to_owned(),
            user_id: Some("user-1".to_owned()),
            saved_at: "2026-01-01T00:00:00.000Z".to_owned(),
        }
    }

    fn tmp_files(directory: &Path) -> Vec<String> {
        fs::read_dir(directory)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| is_account_temp_name(OsStr::new(name)))
            .collect()
    }

    #[test]
    fn uses_nonempty_override_and_global_fallback_paths() {
        let global = PathBuf::from("/home/test/.qwen");
        assert_eq!(
            resolve_state_dir(Some(OsStr::new("relative/state")), || {
                panic!("global path should not be read when an override is set")
            })
            .unwrap(),
            PathBuf::from("relative/state")
        );
        assert_eq!(
            resolve_state_dir(Some(OsStr::new("")), || Ok(global.clone())).unwrap(),
            global.join("channels/weixin")
        );
        assert_eq!(
            resolve_state_dir(None, || Ok(global.clone())).unwrap(),
            global.join("channels/weixin")
        );
    }

    #[test]
    fn creates_a_missing_state_directory_recursively() {
        let test_dir = TestDirectory::new();
        let directory = test_dir.0.join("nested/state");
        ensure_state_dir(&directory).unwrap();
        assert!(directory.is_dir());
    }

    #[test]
    fn loads_missing_and_malformed_state_as_no_account() {
        let test_dir = TestDirectory::new();
        assert_eq!(load_account_in(&test_dir.0), None);
        fs::write(test_dir.0.join("account.json"), "not json").unwrap();
        assert_eq!(load_account_in(&test_dir.0), None);
        fs::write(test_dir.0.join("account.json"), r#"{"token":"partial"}"#).unwrap();
        assert_eq!(
            load_account_in(&test_dir.0),
            Some(serde_json::json!({ "token": "partial" }))
        );
    }

    #[test]
    fn saves_exact_json_shape_and_round_trips_account_data() {
        let test_dir = TestDirectory::new();
        save_account_in(&test_dir.0, &data()).unwrap();
        let json = fs::read_to_string(test_dir.0.join("account.json")).unwrap();
        assert_eq!(
            json,
            concat!(
                "{\n",
                "  \"token\": \"super-secret-token\",\n",
                "  \"baseUrl\": \"https://ilinkai.weixin.qq.com\",\n",
                "  \"userId\": \"user-1\",\n",
                "  \"savedAt\": \"2026-01-01T00:00:00.000Z\"\n",
                "}"
            )
        );
        assert_eq!(
            load_account_in(&test_dir.0),
            Some(serde_json::json!({
                "token": "super-secret-token",
                "baseUrl": DEFAULT_BASE_URL,
                "userId": "user-1",
                "savedAt": "2026-01-01T00:00:00.000Z"
            }))
        );
    }

    #[test]
    fn omits_absent_optional_user_id() {
        let test_dir = TestDirectory::new();
        let mut account = data();
        account.user_id = None;
        save_account_in(&test_dir.0, &account).unwrap();
        let json: serde_json::Value =
            serde_json::from_slice(&fs::read(test_dir.0.join("account.json")).unwrap()).unwrap();
        assert!(json.get("userId").is_none());
        assert_eq!(
            load_account_in(&test_dir.0),
            Some(serde_json::json!({
                "token": "super-secret-token",
                "baseUrl": DEFAULT_BASE_URL,
                "savedAt": "2026-01-01T00:00:00.000Z"
            }))
        );
    }

    #[cfg(unix)]
    #[test]
    fn keeps_credentials_private_and_narrows_previous_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let test_dir = TestDirectory::new();
        let account_path = test_dir.0.join("account.json");
        fs::write(&account_path, "{\"token\":\"old\"}").unwrap();
        fs::set_permissions(&account_path, fs::Permissions::from_mode(0o644)).unwrap();

        save_account_in(&test_dir.0, &data()).unwrap();

        assert_eq!(
            fs::metadata(&account_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            load_account_in(&test_dir.0),
            Some(serde_json::json!({
                "token": "super-secret-token",
                "baseUrl": DEFAULT_BASE_URL,
                "userId": "user-1",
                "savedAt": "2026-01-01T00:00:00.000Z"
            }))
        );
    }

    #[cfg(unix)]
    #[test]
    fn replaces_destination_symlink_without_writing_to_its_target() {
        use std::os::unix::fs::symlink;

        let test_dir = TestDirectory::new();
        let victim = test_dir.0.join("victim.txt");
        let account_path = test_dir.0.join("account.json");
        fs::write(&victim, "not the token").unwrap();
        symlink(&victim, &account_path).unwrap();

        save_account_in(&test_dir.0, &data()).unwrap();

        assert!(
            !fs::symlink_metadata(&account_path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read_to_string(victim).unwrap(), "not the token");
        assert_eq!(
            load_account_in(&test_dir.0),
            Some(serde_json::json!({
                "token": "super-secret-token",
                "baseUrl": DEFAULT_BASE_URL,
                "userId": "user-1",
                "savedAt": "2026-01-01T00:00:00.000Z"
            }))
        );
    }

    #[test]
    fn unique_atomic_temps_are_cleaned_after_each_successful_save() {
        let test_dir = TestDirectory::new();
        let planted = test_dir.0.join("account.json.tmp");
        fs::write(&planted, "planted").unwrap();
        for _ in 0..5 {
            save_account_in(&test_dir.0, &data()).unwrap();
            assert_eq!(tmp_files(&test_dir.0), ["account.json.tmp"]);
        }
        assert_eq!(fs::read_to_string(&planted).unwrap(), "planted");
        assert_eq!(fs::read_dir(&test_dir.0).unwrap().count(), 2);
    }

    #[test]
    fn failed_replacement_cleans_its_temp_file() {
        let test_dir = TestDirectory::new();
        fs::create_dir(test_dir.0.join("account.json")).unwrap();

        assert!(save_account_in(&test_dir.0, &data()).is_err());
        assert!(tmp_files(&test_dir.0).is_empty());
    }

    #[test]
    fn clear_removes_account_and_both_source_and_shared_writer_orphans() {
        let test_dir = TestDirectory::new();
        save_account_in(&test_dir.0, &data()).unwrap();
        fs::write(
            test_dir.0.join("account.json.a1b2c3d4e5f6.tmp"),
            "orphaned credential",
        )
        .unwrap();
        fs::write(
            test_dir.0.join(".account.json.canopy-abcd.tmp"),
            "orphaned credential",
        )
        .unwrap();
        fs::write(test_dir.0.join("cursor.txt"), "42").unwrap();

        clear_account_in(&test_dir.0).unwrap();

        assert!(!test_dir.0.join("account.json").exists());
        assert!(tmp_files(&test_dir.0).is_empty());
        assert_eq!(
            fs::read_to_string(test_dir.0.join("cursor.txt")).unwrap(),
            "42"
        );
    }

    #[test]
    fn clear_without_an_account_is_a_noop_and_leaves_unrelated_files() {
        let test_dir = TestDirectory::new();
        fs::write(test_dir.0.join("cursor.txt"), "42").unwrap();
        clear_account_in(&test_dir.0).unwrap();
        assert_eq!(
            fs::read_to_string(test_dir.0.join("cursor.txt")).unwrap(),
            "42"
        );
    }
}
