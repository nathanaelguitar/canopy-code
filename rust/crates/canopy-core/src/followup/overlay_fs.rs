//! Copy-on-write overlay filesystem for speculative execution.
//!
//! Port of `packages/core/src/followup/overlayFs.ts`. Reads resolve to the
//! overlay after a file is redirected for writing; other reads continue to use
//! their original path. Changes are copied back only when `apply_to_real` is
//! called.

use std::fs::{self, DirBuilder};
use std::io;
use std::path::{Component, Path, PathBuf};

use indexmap::IndexMap;
use thiserror::Error;
use uuid::Uuid;

/// Errors returned when a write cannot be safely redirected.
#[derive(Debug, Error)]
pub enum OverlayFsError {
    #[error("Cannot redirect write outside cwd: {0}")]
    OutsideCwd(PathBuf),
    #[error("Cannot redirect write through a path that escapes cwd: {0}")]
    EscapesRoot(PathBuf),
    #[error("overlay filesystem I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Copy-on-write filesystem scoped to one real working directory.
///
/// The returned overlay paths are intended to be handed to tools in place of
/// real paths. This type does not itself intercept operating-system writes.
pub struct OverlayFs {
    real_cwd: PathBuf,
    temp_root: PathBuf,
    overlay_dir: PathBuf,
    written_files: IndexMap<PathBuf, PathBuf>,
}

impl OverlayFs {
    /// Create an overlay rooted at `real_cwd`.
    ///
    /// Relative roots are resolved against the process working directory.
    /// The overlay directory is created lazily on the first redirected write.
    pub fn new(real_cwd: impl AsRef<Path>) -> Self {
        let current_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let real_cwd = absolute_normalized(real_cwd.as_ref(), &current_dir);
        let temp_root = fs::canonicalize(std::env::temp_dir())
            .unwrap_or_else(|_| absolute_normalized(&std::env::temp_dir(), &current_dir));
        let unique_id = unique_id();
        let overlay_dir = temp_root
            .join("canopy-speculation")
            .join(std::process::id().to_string())
            .join(unique_id);

        Self {
            real_cwd,
            temp_root,
            overlay_dir,
            written_files: IndexMap::new(),
        }
    }

    /// Return the unique directory that contains this overlay.
    pub fn get_overlay_dir(&self) -> &Path {
        &self.overlay_dir
    }

    /// Resolve a read path to its overlay copy when the file was redirected.
    /// Unwritten and out-of-root paths are returned unchanged.
    pub fn resolve_read_path(&self, real_path: impl AsRef<Path>) -> PathBuf {
        let real_path = real_path.as_ref();
        self.to_relative(real_path)
            .and_then(|relative| self.written_files.get(&relative))
            .cloned()
            .unwrap_or_else(|| real_path.to_path_buf())
    }

    /// Redirect a write into the overlay, copying the original file on first
    /// write when it is a readable file within the working root.
    pub fn redirect_write(
        &mut self,
        real_path: impl AsRef<Path>,
    ) -> Result<PathBuf, OverlayFsError> {
        let real_path = real_path.as_ref();
        let relative = self
            .to_relative(real_path)
            .filter(|relative| !relative.as_os_str().is_empty())
            .ok_or_else(|| OverlayFsError::OutsideCwd(real_path.to_path_buf()))?;

        if let Some(overlay_path) = self.written_files.get(&relative) {
            return Ok(overlay_path.clone());
        }

        if !self.real_path_stays_in_root(&relative) {
            return Err(OverlayFsError::EscapesRoot(real_path.to_path_buf()));
        }

        self.ensure_overlay_parent(&relative)?;
        let overlay_path = self.overlay_dir.join(&relative);
        let original_path = self.real_cwd.join(&relative);

        // Match the source behavior: copying a directory, unreadable file, or
        // other unsupported original is best effort; the tool can still write
        // a new file at the overlay path.
        if fs::metadata(&original_path).is_ok() {
            let _ = fs::copy(&original_path, &overlay_path);
        }

        self.written_files.insert(relative, overlay_path.clone());
        Ok(overlay_path)
    }

    /// Return an independent snapshot of the relative-to-overlay path map.
    pub fn get_written_files(&self) -> IndexMap<PathBuf, PathBuf> {
        self.written_files.clone()
    }

    /// Best-effort copy of overlay files back to the working tree.
    ///
    /// Missing overlay files and individual copy failures are skipped, as in
    /// the TypeScript implementation. A path that currently resolves through
    /// a symlink outside either root is also skipped.
    pub fn apply_to_real(&self) -> Vec<PathBuf> {
        let mut applied = Vec::new();

        for (relative, overlay_path) in &self.written_files {
            if !self.real_path_stays_in_root(relative)
                || !self.overlay_path_stays_in_root(overlay_path)
            {
                continue;
            }

            let real_path = self.real_cwd.join(relative);
            if real_path.parent().is_none() {
                continue;
            }

            if !self.create_real_parent(relative)
                || !self.real_path_stays_in_root(relative)
                || !self.overlay_path_stays_in_root(overlay_path)
                || fs::copy(overlay_path, &real_path).is_err()
            {
                continue;
            }
            applied.push(real_path);
        }

        applied
    }

    /// Remove only this overlay's unique directory. Cleanup is intentionally
    /// best effort and does not follow a replacement symlink at the leaf.
    pub fn cleanup(&self) {
        let parent = self.overlay_dir.parent();
        let Some(parent) = parent else {
            return;
        };
        if !is_real_directory(&self.temp_root.join("canopy-speculation"))
            || !is_real_directory(parent)
        {
            return;
        }

        match fs::symlink_metadata(&self.overlay_dir) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                let _ = fs::remove_file(&self.overlay_dir);
            }
            Ok(metadata) if metadata.is_dir() => {
                let _ = fs::remove_dir_all(&self.overlay_dir);
            }
            Ok(_) => {
                let _ = fs::remove_file(&self.overlay_dir);
            }
            Err(_) => {}
        }
    }

    fn to_relative(&self, input_path: &Path) -> Option<PathBuf> {
        let absolute = absolute_normalized(input_path, &self.real_cwd);
        absolute
            .strip_prefix(&self.real_cwd)
            .ok()
            .map(Path::to_path_buf)
    }

    fn ensure_overlay_parent(&self, relative: &Path) -> io::Result<()> {
        let speculation_root = self.temp_root.join("canopy-speculation");
        ensure_real_directory(&speculation_root)?;
        let process_root = speculation_root.join(std::process::id().to_string());
        ensure_real_directory(&process_root)?;
        ensure_private_directory(&self.overlay_dir)?;

        let mut overlay_parent = self.overlay_dir.clone();
        let components = relative.components().collect::<Vec<_>>();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            match component {
                Component::Normal(name) => overlay_parent.push(name),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "unsafe relative path",
                    ));
                }
            }
            ensure_real_directory(&overlay_parent)?;
        }
        Ok(())
    }

    fn real_path_stays_in_root(&self, relative: &Path) -> bool {
        let Ok(canonical_root) = fs::canonicalize(&self.real_cwd) else {
            // A not-yet-created working root has no existing symlink children
            // to escape through. Normalized lexical containment still applies.
            return true;
        };
        nearest_existing_canonical(&self.real_cwd.join(relative))
            .is_some_and(|path| path.starts_with(canonical_root))
    }

    fn create_real_parent(&self, relative: &Path) -> bool {
        if fs::create_dir_all(&self.real_cwd).is_err() {
            return false;
        }
        let components = relative.components().collect::<Vec<_>>();
        let mut relative_parent = PathBuf::new();
        let mut real_parent = self.real_cwd.clone();

        for component in components.iter().take(components.len().saturating_sub(1)) {
            let Component::Normal(name) = component else {
                return false;
            };
            relative_parent.push(name);
            real_parent.push(name);
            if !self.real_path_stays_in_root(&relative_parent) {
                return false;
            }

            match fs::metadata(&real_parent) {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => return false,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    if fs::create_dir(&real_parent).is_err()
                        && !fs::metadata(&real_parent).is_ok_and(|metadata| metadata.is_dir())
                    {
                        return false;
                    }
                    if !self.real_path_stays_in_root(&relative_parent) {
                        return false;
                    }
                }
                Err(_) => return false,
            }
        }
        true
    }

    fn overlay_path_stays_in_root(&self, overlay_path: &Path) -> bool {
        let Ok(metadata) = fs::symlink_metadata(&self.overlay_dir) else {
            return false;
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return false;
        }
        let Ok(canonical_root) = fs::canonicalize(&self.overlay_dir) else {
            return false;
        };
        nearest_existing_canonical(overlay_path)
            .is_some_and(|path| path.starts_with(canonical_root))
    }
}

fn absolute_normalized(path: &Path, base: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };

    let mut normalized = PathBuf::new();
    for component in absolute.components() {
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

fn nearest_existing_canonical(path: &Path) -> Option<PathBuf> {
    let mut candidate = path;
    loop {
        match fs::canonicalize(candidate) {
            Ok(canonical) => return Some(canonical),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                candidate = candidate.parent()?;
            }
            Err(_) => return None,
        }
    }
}

fn ensure_real_directory(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("expected a real directory at {}", path.display()),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            create_private_directory(path)?;
            match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
                Ok(_) => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("expected a real directory at {}", path.display()),
                )),
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

fn ensure_private_directory(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("expected a real directory at {}", path.display()),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            create_private_directory(path)?;
            ensure_private_directory(path)
        }
        Err(error) => Err(error),
    }
}

fn create_private_directory(path: &Path) -> io::Result<()> {
    let mut builder = DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

fn is_real_directory(path: &Path) -> bool {
    fs::symlink_metadata(path)
        .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
}

fn unique_id() -> String {
    Uuid::new_v4().simple().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!("canopy-overlay-{label}-{}", unique_id()));
            fs::create_dir_all(&path).expect("create test directory");
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write(path: &Path, contents: &str) {
        fs::create_dir_all(path.parent().expect("test path has parent")).unwrap();
        let mut file = fs::File::create(path).unwrap();
        file.write_all(contents.as_bytes()).unwrap();
    }

    #[test]
    fn copies_existing_file_on_first_write_and_reuses_path() {
        let root = TestDir::new("copy");
        let real_file = root.0.join("src/app.ts");
        write(&real_file, "original content");
        let mut overlay = OverlayFs::new(&root.0);

        let first = overlay.redirect_write(&real_file).unwrap();
        let second = overlay.redirect_write(&real_file).unwrap();

        assert_eq!(first, second);
        assert_eq!(fs::read_to_string(first).unwrap(), "original content");
        overlay.cleanup();
    }

    #[test]
    fn new_file_is_mapped_without_creating_an_empty_file() {
        let root = TestDir::new("new-file");
        let new_file = root.0.join("new-file.ts");
        let mut overlay = OverlayFs::new(&root.0);

        let redirected = overlay.redirect_write(&new_file).unwrap();

        assert!(redirected.ends_with("new-file.ts"));
        assert!(!redirected.exists());
        assert!(
            overlay
                .get_written_files()
                .contains_key(Path::new("new-file.ts"))
        );
        overlay.cleanup();
    }

    #[test]
    fn rejects_absolute_and_relative_paths_outside_the_root() {
        let root = TestDir::new("outside");
        let mut overlay = OverlayFs::new(&root.0);

        let absolute = overlay.redirect_write("/etc/passwd").unwrap_err();
        assert!(
            absolute
                .to_string()
                .contains("Cannot redirect write outside cwd")
        );
        let traversal = overlay
            .redirect_write(root.0.join("../../etc/passwd"))
            .unwrap_err();
        assert!(
            traversal
                .to_string()
                .contains("Cannot redirect write outside cwd")
        );
        overlay.cleanup();
    }

    #[test]
    fn resolves_reads_and_leaves_unwritten_or_external_paths_unchanged() {
        let root = TestDir::new("reads");
        let real_file = root.0.join("file.ts");
        write(&real_file, "original");
        let mut overlay = OverlayFs::new(&root.0);
        let redirected = overlay.redirect_write(&real_file).unwrap();

        assert_eq!(overlay.resolve_read_path(&real_file), redirected);
        let untouched = root.0.join("untouched.ts");
        assert_eq!(overlay.resolve_read_path(&untouched), untouched);
        let external = PathBuf::from("/etc/hosts");
        assert_eq!(overlay.resolve_read_path(&external), external);
        overlay.cleanup();
    }

    #[test]
    fn relative_paths_are_resolved_against_the_overlay_root() {
        let root = TestDir::new("relative");
        let real_file = root.0.join("src/app.ts");
        write(&real_file, "content");
        let mut overlay = OverlayFs::new(&root.0);
        let redirected = overlay.redirect_write(Path::new("src/app.ts")).unwrap();

        assert_eq!(
            overlay.resolve_read_path(Path::new("src/app.ts")),
            redirected
        );
        overlay.cleanup();
    }

    #[test]
    fn applies_existing_and_new_nested_files_back_to_the_root() {
        let root = TestDir::new("apply");
        let existing = root.0.join("existing.ts");
        write(&existing, "before");
        let mut overlay = OverlayFs::new(&root.0);
        let existing_overlay = overlay.redirect_write(&existing).unwrap();
        write(&existing_overlay, "after");

        let new_file = root.0.join("new/deep/file.ts");
        let new_overlay = overlay.redirect_write(&new_file).unwrap();
        write(&new_overlay, "created");

        let applied = overlay.apply_to_real();

        assert_eq!(applied, vec![existing, new_file.clone()]);
        assert_eq!(
            fs::read_to_string(root.0.join("existing.ts")).unwrap(),
            "after"
        );
        assert_eq!(fs::read_to_string(new_file).unwrap(), "created");
        overlay.cleanup();
    }

    #[test]
    fn apply_is_empty_without_writes_and_skips_missing_overlay_files() {
        let root = TestDir::new("empty-apply");
        let mut overlay = OverlayFs::new(&root.0);
        assert!(overlay.apply_to_real().is_empty());

        let new_file = root.0.join("missing.ts");
        let _ = overlay.redirect_write(&new_file).unwrap();
        assert!(overlay.apply_to_real().is_empty());
        assert!(!new_file.exists());
        overlay.cleanup();
    }

    #[test]
    fn written_file_map_is_an_independent_snapshot() {
        let root = TestDir::new("snapshot");
        let real_file = root.0.join("file.ts");
        write(&real_file, "content");
        let mut overlay = OverlayFs::new(&root.0);
        overlay.redirect_write(&real_file).unwrap();

        let mut snapshot = overlay.get_written_files();
        assert_eq!(snapshot.len(), 1);
        snapshot.clear();
        assert_eq!(overlay.get_written_files().len(), 1);
        overlay.cleanup();
    }

    #[test]
    fn cleanup_is_idempotent_and_does_not_follow_a_leaf_symlink() {
        let root = TestDir::new("cleanup");
        let mut overlay = OverlayFs::new(&root.0);
        let real_file = root.0.join("file.ts");
        let overlay_file = overlay.redirect_write(&real_file).unwrap();
        let overlay_dir = overlay.get_overlay_dir().to_path_buf();
        assert!(overlay_dir.exists());

        overlay.cleanup();
        overlay.cleanup();
        assert!(!overlay_dir.exists());

        #[cfg(unix)]
        {
            let external = TestDir::new("cleanup-target");
            write(&external.0.join("keep.txt"), "keep");
            std::os::unix::fs::symlink(&external.0, &overlay_dir).unwrap();
            overlay.cleanup();
            assert_eq!(
                fs::read_to_string(external.0.join("keep.txt")).unwrap(),
                "keep"
            );
            assert!(!overlay_dir.exists());
        }

        let _ = overlay_file;
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_escape_for_redirect_and_apply() {
        let root = TestDir::new("symlink-root");
        let outside = TestDir::new("symlink-outside");
        write(&outside.0.join("secret.txt"), "secret");
        std::os::unix::fs::symlink(&outside.0, root.0.join("linked")).unwrap();
        let mut overlay = OverlayFs::new(&root.0);

        let error = overlay
            .redirect_write(root.0.join("linked/secret.txt"))
            .unwrap_err();
        assert!(matches!(error, OverlayFsError::EscapesRoot(_)));
        assert!(overlay.get_written_files().is_empty());

        fs::remove_file(root.0.join("linked")).unwrap();
        fs::create_dir(root.0.join("linked")).unwrap();
        let nested = root.0.join("linked/new.txt");
        let redirected = overlay.redirect_write(&nested).unwrap();
        write(&redirected, "overlay content");
        fs::remove_dir_all(root.0.join("linked")).unwrap();
        std::os::unix::fs::symlink(&outside.0, root.0.join("linked")).unwrap();
        assert!(overlay.apply_to_real().is_empty());
        assert_eq!(
            fs::read_to_string(outside.0.join("secret.txt")).unwrap(),
            "secret"
        );
        overlay.cleanup();
    }
}
