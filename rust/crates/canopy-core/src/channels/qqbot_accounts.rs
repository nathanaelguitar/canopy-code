//! Credential persistence for QQ Bot channels.
//!
//! Port of `packages/channels/qqbot/src/accounts.ts`. Credentials live at
//! `<global Qwen directory>/channels/<safe name>-credentials.json`.

use crate::channels::paths;
use crate::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};
use serde::Serialize;
use serde_json::Value;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Credentials loaded from the JSON file.
///
/// Values use `serde_json::Value` to preserve the TypeScript source's runtime
/// behavior: it checks JavaScript truthiness but does not validate that parsed
/// values are actually strings.
#[derive(Clone, Debug, PartialEq)]
pub struct Credentials {
    pub app_id: Value,
    pub app_secret: Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CredentialsToSave<'a> {
    app_id: &'a str,
    app_secret: &'a str,
}

/// Build the credential path under the configured global Qwen directory.
pub fn get_creds_file_path(safe_name: &str) -> io::Result<PathBuf> {
    Ok(creds_file_path_in(
        &paths::get_global_qwen_dir()?,
        safe_name,
    ))
}

fn creds_file_path_in(global_qwen_dir: &Path, safe_name: &str) -> PathBuf {
    global_qwen_dir
        .join("channels")
        .join(format!("{safe_name}-credentials.json"))
}

/// Try to load persisted credentials.
///
/// Missing files, read errors, malformed JSON, absent fields, and falsy field
/// values all return `None`, matching the TypeScript loader's `null` result.
pub fn load_credentials(creds_file: impl AsRef<Path>) -> Option<Credentials> {
    let bytes = fs::read(creds_file).ok()?;
    let saved: Value = serde_json::from_slice(&bytes).ok()?;
    let saved = saved.as_object()?;
    let app_id = saved.get("appId")?;
    let app_secret = saved.get("appSecret")?;
    if js_truthy(app_id) && js_truthy(app_secret) {
        Some(Credentials {
            app_id: app_id.clone(),
            app_secret: app_secret.clone(),
        })
    } else {
        None
    }
}

/// Persist credentials and create the global `channels` directory.
///
/// The existing atomic writer creates new files with mode `0o600` while
/// preserving an existing file's mode, matching `writeFileSync`'s `mode`
/// option. Replacement is atomic and follows destination symlinks.
pub fn save_credentials(
    creds_file: impl AsRef<Path>,
    app_id: &str,
    app_secret: &str,
) -> io::Result<()> {
    let global_qwen_dir = paths::get_global_qwen_dir()?;
    save_credentials_in(&global_qwen_dir, creds_file, app_id, app_secret)
}

fn save_credentials_in(
    global_qwen_dir: &Path,
    creds_file: impl AsRef<Path>,
    app_id: &str,
    app_secret: &str,
) -> io::Result<()> {
    fs::create_dir_all(global_qwen_dir.join("channels"))?;
    let contents =
        serde_json::to_vec(&CredentialsToSave { app_id, app_secret }).map_err(io::Error::other)?;
    let options = AtomicWriteOptions {
        mode: Some(0o600),
        force_mode: false,
        symlink_policy: SymlinkPolicy::Follow,
        ..AtomicWriteOptions::default()
    };
    atomic_write_file(creds_file, &contents, &options)
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

#[cfg(test)]
mod tests {
    use super::{
        Credentials, creds_file_path_in, js_truthy, load_credentials, save_credentials_in,
    };
    use serde_json::{Value, json};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "canopy-qqbot-accounts-{}-{sequence}",
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

    fn load(path: &Path) -> Option<Credentials> {
        load_credentials(path)
    }

    #[test]
    fn builds_credential_path_under_channels_directory() {
        assert_eq!(
            creds_file_path_in(Path::new("/tmp/test-qwen"), "mybot"),
            PathBuf::from("/tmp/test-qwen/channels/mybot-credentials.json")
        );
        assert!(
            creds_file_path_in(Path::new("/tmp/test-qwen"), "test_123")
                .ends_with("test_123-credentials.json")
        );
    }

    #[test]
    fn returns_none_for_missing_corrupt_or_partial_credentials() {
        let test_dir = TestDirectory::new();
        let path = test_dir.0.join("credentials.json");
        assert_eq!(load(&path), None);

        fs::write(&path, "not-valid-json").unwrap();
        assert_eq!(load(&path), None);

        fs::write(&path, r#"{"appSecret":"secret-only"}"#).unwrap();
        assert_eq!(load(&path), None);

        fs::write(&path, r#"{"appId":"app-only"}"#).unwrap();
        assert_eq!(load(&path), None);
    }

    #[test]
    fn loads_both_credential_fields() {
        let test_dir = TestDirectory::new();
        let path = test_dir.0.join("credentials.json");
        fs::write(&path, r#"{"appId":"my-app","appSecret":"my-secret"}"#).unwrap();

        assert_eq!(
            load(&path),
            Some(Credentials {
                app_id: json!("my-app"),
                app_secret: json!("my-secret"),
            })
        );
    }

    #[test]
    fn load_uses_javascript_truthiness_and_preserves_truthy_json_values() {
        let test_dir = TestDirectory::new();
        let path = test_dir.0.join("credentials.json");

        for falsy in [json!(null), json!(false), json!(0), json!("")] {
            fs::write(
                &path,
                serde_json::to_vec(&json!({ "appId": falsy, "appSecret": "secret" })).unwrap(),
            )
            .unwrap();
            assert_eq!(load(&path), None);
        }
        fs::write(&path, br#"{"appId":{},"appSecret":[]}"#).unwrap();
        assert_eq!(
            load(&path),
            Some(Credentials {
                app_id: json!({}),
                app_secret: json!([]),
            })
        );
        assert!(js_truthy(&Value::String(" ".to_owned())));
        assert!(js_truthy(&json!(-2)));
    }

    #[test]
    fn saves_compact_json_and_creates_global_channels_directory() {
        let test_dir = TestDirectory::new();
        let global_qwen_dir = test_dir.0.join("qwen");
        let target_parent = test_dir.0.join("custom");
        fs::create_dir_all(&target_parent).unwrap();
        let creds_file = target_parent.join("credentials.json");

        save_credentials_in(&global_qwen_dir, &creds_file, "app-id", "app-secret").unwrap();

        assert!(global_qwen_dir.join("channels").is_dir());
        assert_eq!(
            fs::read_to_string(&creds_file).unwrap(),
            r#"{"appId":"app-id","appSecret":"app-secret"}"#
        );
        assert_eq!(
            load(&creds_file),
            Some(Credentials {
                app_id: json!("app-id"),
                app_secret: json!("app-secret"),
            })
        );
    }

    #[cfg(unix)]
    #[test]
    fn new_credentials_are_private_and_existing_mode_is_preserved() {
        use std::os::unix::fs::PermissionsExt;

        let test_dir = TestDirectory::new();
        let global_qwen_dir = test_dir.0.join("qwen");
        let new_file = test_dir.0.join("new.json");
        save_credentials_in(&global_qwen_dir, &new_file, "a", "b").unwrap();
        assert_eq!(
            fs::metadata(&new_file).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let existing_file = test_dir.0.join("existing.json");
        fs::write(&existing_file, "old").unwrap();
        fs::set_permissions(&existing_file, fs::Permissions::from_mode(0o644)).unwrap();
        save_credentials_in(&global_qwen_dir, &existing_file, "a", "b").unwrap();
        assert_eq!(
            fs::metadata(&existing_file).unwrap().permissions().mode() & 0o777,
            0o644
        );
    }

    #[test]
    fn save_fails_when_the_requested_file_parent_does_not_exist() {
        let test_dir = TestDirectory::new();
        let global_qwen_dir = test_dir.0.join("qwen");
        let missing_parent = test_dir.0.join("not-created").join("credentials.json");

        assert!(save_credentials_in(&global_qwen_dir, missing_parent, "a", "b").is_err());
        assert!(global_qwen_dir.join("channels").is_dir());
    }
}
