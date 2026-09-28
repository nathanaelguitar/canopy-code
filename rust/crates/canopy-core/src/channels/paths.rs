//! Channel storage path helpers.
//!
//! Port of `packages/channels/base/src/paths.ts`: tilde expansion and lexical
//! absolute resolution, global Qwen home selection, workspace realpath
//! fallback, and workspace-scoped directory names.
//!
//! On Unix, home discovery uses `HOME` without an unsafe passwd-database
//! lookup. If it is unset, the global directory falls back to the temp
//! directory; Node may instead recover the account home from the system
//! password database.

use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

/// Expand a leading home marker and resolve the path against the current
/// working directory, lexically normalizing `.` and `..` without requiring
/// the target to exist.
pub fn resolve_path(dir: &str) -> io::Result<PathBuf> {
    let cwd = std::env::current_dir()?;
    let home = home_dir();
    resolve_path_from(dir, &cwd, home.as_deref())
}

/// Return the configured Qwen home, or `~/.qwen` by default. Relative and
/// tilde-prefixed `QWEN_HOME` values are resolved from the current directory.
/// If no home directory is available, the source implementation falls back
/// to `<temp>/.qwen`.
pub fn get_global_qwen_dir() -> io::Result<PathBuf> {
    let cwd = std::env::current_dir()?;
    let configured = std::env::var_os("QWEN_HOME").filter(|value| !value.is_empty());
    let configured = configured.as_deref().map(|value| value.to_string_lossy());
    let home = home_dir();
    qwen_home(
        configured.as_deref(),
        home.as_deref(),
        &node_temp_dir(),
        &cwd,
    )
}

/// Return the global channel state directory (`<global Qwen home>/channels`).
pub fn global_channels_root() -> io::Result<PathBuf> {
    Ok(get_global_qwen_dir()?.join("channels"))
}

/// Canonicalize a workspace path with a realpath attempt. Any realpath error
/// falls back to its resolved spelling, including errors other than not-found,
/// because channel pairing state is best-effort and must not block startup.
pub fn canonicalize_workspace_path(workspace_cwd: &str) -> io::Result<PathBuf> {
    let resolved = resolve_path(workspace_cwd)?;
    Ok(fs::canonicalize(&resolved).unwrap_or(resolved))
}

/// Compute `<sanitized-basename>-<sha256[:12]>` for a workspace path.
///
/// Hashing the full canonical path keeps equal basenames in different
/// directories distinct. Canonicalization also makes symlink spellings share
/// a scope when the target exists.
pub fn get_workspace_scope_dir_name(workspace_cwd: &str) -> io::Result<String> {
    let canonical = canonicalize_workspace_path(workspace_cwd)?;
    let canonical_text = canonical.to_string_lossy();
    let hash = Sha256::digest(canonical_text.as_bytes());
    let hash = hash[..6]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let basename = canonical
        .file_name()
        .map(OsStr::to_string_lossy)
        .unwrap_or_default();
    let basename = sanitize_basename(&basename);
    if basename.is_empty() {
        Ok(hash)
    } else {
        Ok(format!("{basename}-{hash}"))
    }
}

fn resolve_path_from(dir: &str, cwd: &Path, home: Option<&Path>) -> io::Result<PathBuf> {
    let expanded = expand_home(dir, home);
    let absolute = if expanded.is_absolute() {
        expanded
    } else {
        cwd.join(expanded)
    };
    Ok(normalize_absolute(&absolute))
}

fn qwen_home(
    configured: Option<&str>,
    home: Option<&Path>,
    temp: &Path,
    cwd: &Path,
) -> io::Result<PathBuf> {
    if let Some(configured) = configured.filter(|value| !value.is_empty()) {
        resolve_path_from(configured, cwd, home)
    } else if let Some(home) = home.filter(|home| !home.as_os_str().is_empty()) {
        Ok(home.join(".qwen"))
    } else {
        Ok(temp.join(".qwen"))
    }
}

fn expand_home(dir: &str, home: Option<&Path>) -> PathBuf {
    let is_home = dir == "~" || dir.starts_with("~/") || dir.starts_with("~\\");
    if !is_home {
        return PathBuf::from(dir);
    }
    let Some(home) = home else {
        // Node's os.homedir() normally always resolves a home. If the host
        // cannot supply one, resolving the remaining segments against cwd is
        // the closest useful analogue to an empty home string.
        return PathBuf::from(dir.trim_start_matches('~').trim_start_matches(['/', '\\']));
    };
    let mut expanded = home.to_path_buf();
    if dir != "~" {
        for segment in dir[2..]
            .split(['/', '\\'])
            .filter(|segment| !segment.is_empty())
        {
            expanded.push(segment);
        }
    }
    expanded
}

fn normalize_absolute(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if normalized.file_name().is_some() {
                    normalized.pop();
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn sanitize_basename(basename: &str) -> String {
    // JavaScript's regular expression has no Unicode flag, so one non-BMP
    // character produces two underscores (one for each UTF-16 surrogate).
    // Once sanitized, all output is ASCII, making the subsequent `.slice(0,
    // 32)` a simple byte truncation.
    let mut output = String::new();
    for character in basename.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
            output.push(character);
        } else if character.len_utf16() == 2 {
            output.push_str("__");
        } else {
            output.push('_');
        }
    }
    output.truncate(output.len().min(32));
    output
}

fn home_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        if let Some(home) = std::env::var_os("USERPROFILE") {
            return Some(PathBuf::from(home));
        }
        if let (Some(drive), Some(path)) =
            (std::env::var_os("HOMEDRIVE"), std::env::var_os("HOMEPATH"))
        {
            let mut home = PathBuf::from(drive);
            home.push(path);
            return Some(home);
        }
        None
    }
    #[cfg(unix)]
    {
        std::env::var_os("HOME").map(PathBuf::from)
    }
    #[cfg(not(any(unix, windows)))]
    {
        std::env::var_os("HOME").map(PathBuf::from)
    }
}

fn node_temp_dir() -> PathBuf {
    #[cfg(windows)]
    let names = ["TEMP", "TMP"];
    #[cfg(not(windows))]
    let names = ["TMPDIR", "TMP", "TEMP"];
    for name in names {
        if let Some(path) = std::env::var_os(name).filter(|value| !value.is_empty()) {
            return PathBuf::from(path);
        }
    }
    std::env::temp_dir()
}

#[cfg(test)]
mod tests {
    use super::{
        canonicalize_workspace_path, get_workspace_scope_dir_name, normalize_absolute, qwen_home,
        resolve_path_from, sanitize_basename,
    };
    use sha2::{Digest, Sha256};
    use std::fs;
    use std::path::PathBuf;

    fn root() -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(r"C:\")
        } else {
            PathBuf::from("/")
        }
    }

    #[test]
    fn expands_tilde_forms_and_resolves_relative_paths() {
        let cwd = root().join("work").join("repo");
        let home = root().join("Users").join("person");
        assert_eq!(
            resolve_path_from("~", &cwd, Some(&home)).unwrap(),
            normalize_absolute(&home)
        );
        assert_eq!(
            resolve_path_from("~/xomo", &cwd, Some(&home)).unwrap(),
            normalize_absolute(&home.join("xomo"))
        );
        assert_eq!(
            resolve_path_from("~\\xomo", &cwd, Some(&home)).unwrap(),
            normalize_absolute(&home.join("xomo"))
        );
        assert_eq!(
            resolve_path_from("relative/dir", &cwd, Some(&home)).unwrap(),
            normalize_absolute(&cwd.join("relative/dir"))
        );
    }

    #[test]
    fn lexical_resolution_collapses_dot_segments_and_trailing_separators() {
        let cwd = root().join("tmp");
        let missing = cwd.join("scope-missing-norm");
        let first = resolve_path_from(
            &format!("{}{}", missing.display(), std::path::MAIN_SEPARATOR),
            &cwd,
            None,
        )
        .unwrap();
        let second = resolve_path_from(
            &missing
                .join("..")
                .join("scope-missing-norm")
                .to_string_lossy(),
            &cwd,
            None,
        )
        .unwrap();
        assert_eq!(first, missing);
        assert_eq!(second, missing);
    }

    #[test]
    fn qwen_home_selection_resolves_configured_values_and_uses_default() {
        let cwd = root().join("cwd");
        let home = root().join("Users").join("person");
        let temp = root().join("tmp");
        let default = home.join(".qwen");
        assert_eq!(
            qwen_home(Some("relative/config"), Some(&home), &temp, &cwd).unwrap(),
            normalize_absolute(&cwd.join("relative/config"))
        );
        assert_eq!(
            qwen_home(Some("~/custom-qwen"), Some(&home), &temp, &cwd).unwrap(),
            normalize_absolute(&home.join("custom-qwen"))
        );
        assert_eq!(qwen_home(None, Some(&home), &temp, &cwd).unwrap(), default);
        assert_eq!(
            qwen_home(None, None, &temp, &cwd).unwrap(),
            temp.join(".qwen")
        );
    }

    #[test]
    fn nonexistent_paths_keep_the_resolved_spelling_in_scope_names() {
        let missing = std::env::temp_dir().join(format!(
            "canopy-scope-missing-{}-nested",
            std::process::id()
        ));
        let resolved = super::resolve_path(&missing.to_string_lossy()).unwrap();
        assert_eq!(
            canonicalize_workspace_path(&missing.to_string_lossy()).unwrap(),
            resolved
        );
        assert_eq!(
            get_workspace_scope_dir_name(&missing.to_string_lossy()).unwrap(),
            get_workspace_scope_dir_name(&resolved.to_string_lossy()).unwrap()
        );
    }

    #[cfg(unix)]
    #[test]
    fn collapses_symlinked_workspace_paths() {
        let root = std::env::temp_dir().join(format!(
            "canopy-paths-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let target = root.join("target");
        let link = root.join("link");
        fs::create_dir_all(&target).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert_eq!(
            canonicalize_workspace_path(&target.to_string_lossy()).unwrap(),
            canonicalize_workspace_path(&link.to_string_lossy()).unwrap()
        );
        assert_eq!(
            get_workspace_scope_dir_name(&target.to_string_lossy()).unwrap(),
            get_workspace_scope_dir_name(&link.to_string_lossy()).unwrap()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn scope_name_hashes_full_path_and_sanitizes_basename_like_javascript() {
        let name = sanitize_basename("project🧑");
        assert_eq!(name, "project__");
        assert_eq!(sanitize_basename("a b/c?"), "a_b_c_");
        assert_eq!(
            sanitize_basename("abcdefghijklmnopqrstuvwxyz0123456789"),
            "abcdefghijklmnopqrstuvwxyz012345"
        );

        let path = root().join("workspace");
        let path_text = normalize_absolute(&path).to_string_lossy().into_owned();
        let hash = Sha256::digest(path_text.as_bytes());
        let expected_hash = hash[..6]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let scoped = get_workspace_scope_dir_name(&path_text).unwrap();
        assert_eq!(scoped, format!("workspace-{expected_hash}"));
    }
}
