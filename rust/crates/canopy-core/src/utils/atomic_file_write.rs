//! Crash-safe replacement of a local file.
//!
//! The writer creates a unique temporary file beside the destination, writes
//! and syncs it, then renames it over the destination. Keeping the temporary
//! file in the same directory keeps the rename on one filesystem. The parent
//! directory is synced after the rename so the replacement itself is durable
//! across a machine crash on Unix filesystems that support directory fsync.
//!
//! `SymlinkPolicy::Follow` resolves the final symlink chain before writing,
//! including broken links. `SymlinkPolicy::NoFollow` leaves the path untouched
//! so rename replaces a symlink directory entry itself. This module does not
//! create parent directories.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

/// Whether an atomic write follows a symlink at the destination path.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SymlinkPolicy {
    /// Resolve the destination's symlink chain and replace its final target.
    #[default]
    Follow,
    /// Replace the destination directory entry, including a symlink itself.
    NoFollow,
}

/// Options for [`atomic_write_file`].
#[derive(Clone, Debug)]
pub struct AtomicWriteOptions {
    /// Permission bits for a new file, or for an existing file when
    /// `force_mode` is true. Existing file mode wins by default.
    pub mode: Option<u32>,
    /// Apply `mode` even when an existing target has different permission bits.
    /// This has no effect without an explicit `mode`.
    pub force_mode: bool,
    /// Sync the temporary file contents and metadata before rename.
    pub flush: bool,
    /// Destination symlink handling policy.
    pub symlink_policy: SymlinkPolicy,
    /// Number of retries after transient permission errors from rename.
    pub rename_retries: u32,
    /// Base retry delay. Each retry doubles this delay.
    pub retry_delay: Duration,
}

impl Default for AtomicWriteOptions {
    fn default() -> Self {
        Self {
            mode: None,
            force_mode: false,
            flush: true,
            symlink_policy: SymlinkPolicy::Follow,
            rename_retries: 3,
            retry_delay: Duration::from_millis(50),
        }
    }
}

/// Atomically replace `path` with `contents`.
///
/// Existing Unix permission bits are preserved unless `force_mode` is set
/// with an explicit `mode`. New files use `mode`, or `0o666` subject to the
/// process umask. On Unix, the mode is set on the open temporary file before
/// its final sync, so restrictive modes are in place before it becomes
/// visible at the destination.
pub fn atomic_write_file(
    path: impl AsRef<Path>,
    contents: &[u8],
    options: &AtomicWriteOptions,
) -> io::Result<()> {
    let requested_path = path.as_ref();
    let target_path = match options.symlink_policy {
        SymlinkPolicy::Follow => resolve_symlink_chain(requested_path)?,
        SymlinkPolicy::NoFollow => requested_path.to_path_buf(),
    };

    let existing_mode = existing_mode(&target_path, options.symlink_policy)?;
    let desired_mode = if options.force_mode && options.mode.is_some() {
        options.mode
    } else {
        existing_mode.or(options.mode)
    };

    let parent = parent_dir(&target_path);
    let (temporary_path, mut file) = create_temp_file(&target_path, desired_mode)?;
    let mut cleanup = TempFileCleanup::new(temporary_path.clone());

    file.write_all(contents)?;
    #[cfg(unix)]
    if let Some(mode) = desired_mode {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(mode & 0o7777))?;
    }
    if options.flush {
        file.sync_all()?;
    }
    drop(file);

    rename_with_retry(
        &temporary_path,
        &target_path,
        options.rename_retries,
        options.retry_delay,
        |source, destination| fs::rename(source, destination),
    )?;
    cleanup.persist();
    sync_directory(&parent)?;
    Ok(())
}

/// Retry a rename on transient permission errors, using exponential backoff.
/// The closure is public as a small test seam for retry behavior.
pub fn rename_with_retry<F>(
    source: &Path,
    destination: &Path,
    retries: u32,
    delay: Duration,
    mut rename: F,
) -> io::Result<()>
where
    F: FnMut(&Path, &Path) -> io::Result<()>,
{
    for attempt in 0..=retries {
        match rename(source, destination) {
            Ok(()) => return Ok(()),
            Err(error) if is_retryable_rename_error(&error) && attempt < retries => {
                let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
                thread::sleep(delay.saturating_mul(factor));
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("rename retry loop always returns")
}

fn is_retryable_rename_error(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::PermissionDenied
}

fn existing_mode(path: &Path, policy: SymlinkPolicy) -> io::Result<Option<u32>> {
    let metadata = match policy {
        SymlinkPolicy::Follow => fs::metadata(path),
        SymlinkPolicy::NoFollow => fs::symlink_metadata(path),
    };
    let metadata = match metadata {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // A no-follow write replaces a symlink itself. Its mode is not the
        // mode of the new regular file, so only preserve regular-file modes.
        if policy == SymlinkPolicy::NoFollow && !metadata.is_file() {
            return Ok(None);
        }
        Ok(Some(metadata.permissions().mode() & 0o7777))
    }
    #[cfg(not(unix))]
    {
        let _ = (metadata, policy);
        Ok(None)
    }
}

fn create_temp_file(target: &Path, mode: Option<u32>) -> io::Result<(PathBuf, File)> {
    let parent = parent_dir(target);
    let name = target.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "atomic write target has no filename",
        )
    })?;
    #[cfg(not(unix))]
    let _ = mode;

    for _ in 0..32 {
        let temporary_path = parent.join(format!(
            ".{}.canopy-{}.tmp",
            name.to_string_lossy(),
            uuid::Uuid::new_v4().simple()
        ));
        let mut open_options = OpenOptions::new();
        open_options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            open_options.mode(mode.unwrap_or(0o666) & 0o7777);
            open_options.custom_flags(libc::O_NOFOLLOW);
        }
        match open_options.open(&temporary_path) {
            Ok(file) => return Ok((temporary_path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique atomic-write temporary file",
    ))
}

fn resolve_symlink_chain(path: &Path) -> io::Result<PathBuf> {
    let mut current = path.to_path_buf();
    for _ in 0..40 {
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(current),
            Err(error) => return Err(error),
        };
        if !metadata.file_type().is_symlink() {
            return Ok(current);
        }
        let link = fs::read_link(&current)?;
        current = if link.is_absolute() {
            link
        } else {
            // Resolve the parent first so relative links remain correct when
            // an earlier path component is itself a directory symlink.
            parent_dir(&current).canonicalize()?.join(link)
        };
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("too many symbolic link levels resolving {}", path.display()),
    ))
}

fn parent_dir(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

struct TempFileCleanup {
    path: PathBuf,
    persisted: bool,
}

impl TempFileCleanup {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            persisted: false,
        }
    }

    fn persist(&mut self) {
        self.persisted = true;
    }
}

impl Drop for TempFileCleanup {
    fn drop(&mut self) {
        if !self.persisted {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "canopy-atomic-write-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn replaces_file_and_preserves_existing_mode() {
        let directory = TestDir::new();
        let path = directory.0.join("state.json");
        fs::write(&path, b"old").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        }

        atomic_write_file(&path, b"new", &AtomicWriteOptions::default()).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"new");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
                0o640
            );
        }
    }

    #[test]
    fn force_mode_overrides_existing_mode_and_sets_new_file_mode() {
        let directory = TestDir::new();
        let path = directory.0.join("secret");
        fs::write(&path, b"old").unwrap();
        let options = AtomicWriteOptions {
            mode: Some(0o600),
            force_mode: true,
            ..AtomicWriteOptions::default()
        };

        atomic_write_file(&path, b"updated", &options).unwrap();
        let new_path = directory.0.join("new-secret");
        atomic_write_file(&new_path, b"new", &options).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
                0o600
            );
            assert_eq!(
                fs::metadata(&new_path).unwrap().permissions().mode() & 0o7777,
                0o600
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn follow_and_no_follow_have_distinct_symlink_replacement_behavior() {
        use std::os::unix::fs::symlink;

        let directory = TestDir::new();
        let target = directory.0.join("target");
        let link = directory.0.join("link");
        fs::write(&target, b"old").unwrap();
        symlink(&target, &link).unwrap();
        atomic_write_file(&link, b"followed", &AtomicWriteOptions::default()).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"followed");
        assert!(
            fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );

        fs::write(&target, b"target unchanged").unwrap();
        let no_follow = AtomicWriteOptions {
            symlink_policy: SymlinkPolicy::NoFollow,
            ..AtomicWriteOptions::default()
        };
        atomic_write_file(&link, b"replaced link", &no_follow).unwrap();
        assert_eq!(fs::read(&link).unwrap(), b"replaced link");
        assert_eq!(fs::read(&target).unwrap(), b"target unchanged");
        assert!(
            !fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[cfg(unix)]
    #[test]
    fn follow_resolves_broken_symlinks_and_rejects_loops() {
        use std::os::unix::fs::symlink;

        let directory = TestDir::new();
        let broken = directory.0.join("broken");
        let absent_target = directory.0.join("created-target");
        symlink(&absent_target, &broken).unwrap();
        atomic_write_file(&broken, b"created", &AtomicWriteOptions::default()).unwrap();
        assert_eq!(fs::read(&absent_target).unwrap(), b"created");
        assert!(
            fs::symlink_metadata(&broken)
                .unwrap()
                .file_type()
                .is_symlink()
        );

        let loop_a = directory.0.join("loop-a");
        let loop_b = directory.0.join("loop-b");
        symlink(&loop_b, &loop_a).unwrap();
        symlink(&loop_a, &loop_b).unwrap();
        let error = atomic_write_file(&loop_a, b"x", &AtomicWriteOptions::default()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn cleans_up_temporary_file_when_rename_fails() {
        let directory = TestDir::new();
        let path = directory.0.join("state");
        fs::create_dir(&path).unwrap();

        assert!(atomic_write_file(&path, b"new", &AtomicWriteOptions::default()).is_err());
        let leftovers = fs::read_dir(&directory.0)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".canopy-"))
            .count();
        assert_eq!(leftovers, 0);
    }

    #[test]
    fn retries_permission_denied_rename_with_exponential_backoff() {
        let mut attempts = 0;
        rename_with_retry(
            Path::new("source"),
            Path::new("destination"),
            3,
            Duration::ZERO,
            |_, _| {
                attempts += 1;
                if attempts < 3 {
                    Err(io::Error::new(io::ErrorKind::PermissionDenied, "busy"))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
        assert_eq!(attempts, 3);
    }

    #[test]
    fn does_not_retry_non_permission_errors() {
        let mut attempts = 0;
        let error = rename_with_retry(
            Path::new("source"),
            Path::new("destination"),
            3,
            Duration::ZERO,
            |_, _| {
                attempts += 1;
                Err(io::Error::new(io::ErrorKind::NotFound, "missing"))
            },
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert_eq!(attempts, 1);
    }
}
