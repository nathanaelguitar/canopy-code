// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//
//! Age-based cleanup of per-project tool-result files and legacy `.output`
//! artifacts, ported from `packages/core/src/utils/toolResultCleanup.ts`.

use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Cleanup totals returned to the caller.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CleanupResult {
    pub files_deleted: usize,
    pub bytes_freed: u64,
    pub errors: usize,
}

/// Remove old regular files from project `tool-results` directories and old
/// top-level legacy `.output` files beneath `global_temp_dir`.
///
/// Project directories, candidate files, and legacy files are examined in
/// sequence. Symlinks are ignored: metadata is read with `symlink_metadata`,
/// matching the source's `lstat` checks. Missing global paths are quiet; the
/// source does count an error if a path returned by the initial project
/// listing disappears before its project-directory stat.
pub async fn cleanup_old_tool_results(
    global_temp_dir: impl AsRef<Path>,
    max_age_ms: f64,
) -> CleanupResult {
    cleanup_old_tool_results_at(global_temp_dir.as_ref(), max_age_ms, current_time_ms()).await
}

/// Schedule stale-artifact cleanup without delaying runtime startup. The
/// caller resolves and passes the active runtime root before spawning so
/// task-local storage settings do not change the directory used by the task.
pub fn schedule_cleanup_old_tool_results(global_temp_dir: impl Into<PathBuf>, max_age_ms: f64) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let global_temp_dir = global_temp_dir.into();
    let cleanup_task = runtime.spawn(async move {
        let _ = cleanup_old_tool_results(global_temp_dir, max_age_ms).await;
    });
    // Dropping a Tokio JoinHandle detaches the startup cleanup task; keeping
    // the intent explicit avoids accidentally constructing and discarding a
    // future without spawning it.
    drop(cleanup_task);
}

async fn cleanup_old_tool_results_at(
    global_temp_dir: &Path,
    max_age_ms: f64,
    now_ms: f64,
) -> CleanupResult {
    let mut result = CleanupResult::default();
    let project_dirs = match read_directory_names(global_temp_dir).await {
        Ok(entries) => entries,
        Err(_) => return result,
    };

    for project_hash in project_dirs {
        let project_dir = global_temp_dir.join(project_hash);
        cleanup_project_dir(&project_dir, now_ms, max_age_ms, &mut result).await;
    }

    result
}

async fn cleanup_project_dir(
    project_dir: &Path,
    now_ms: f64,
    max_age_ms: f64,
    result: &mut CleanupResult,
) {
    let project_metadata = match tokio::fs::symlink_metadata(project_dir).await {
        Ok(metadata) => metadata,
        Err(_) => {
            // Unlike candidate file ENOENT, every project lstat failure
            // increments the source result's error counter.
            result.errors += 1;
            return;
        }
    };
    if !project_metadata.is_dir() {
        return;
    }

    cleanup_directory(
        &project_dir.join("tool-results"),
        now_ms,
        max_age_ms,
        result,
    )
    .await;
    cleanup_legacy_output_files(project_dir, now_ms, max_age_ms, result).await;
}

async fn cleanup_directory(dir: &Path, now_ms: f64, max_age_ms: f64, result: &mut CleanupResult) {
    // The TypeScript implementation suppresses readdir failures for these
    // nested directories rather than counting them.
    let Ok(entries) = read_directory_names(dir).await else {
        return;
    };
    for entry in entries {
        try_delete_if_old(&dir.join(entry), now_ms, max_age_ms, result).await;
    }
}

async fn cleanup_legacy_output_files(
    dir: &Path,
    now_ms: f64,
    max_age_ms: f64,
    result: &mut CleanupResult,
) {
    // Like cleanupDirectory, a failed project-directory readdir is quiet.
    let Ok(entries) = read_directory_names(dir).await else {
        return;
    };
    for entry in entries {
        if entry.to_string_lossy().ends_with(".output") {
            try_delete_if_old(&dir.join(entry), now_ms, max_age_ms, result).await;
        }
    }
}

async fn try_delete_if_old(
    file_path: &Path,
    now_ms: f64,
    max_age_ms: f64,
    result: &mut CleanupResult,
) {
    let metadata = match tokio::fs::symlink_metadata(file_path).await {
        Ok(metadata) => metadata,
        Err(error) => {
            record_candidate_error(&error, result);
            return;
        }
    };

    // symlink_metadata corresponds to lstat, so symlinks do not count as
    // regular files and their targets are never traversed or deleted.
    if !metadata.file_type().is_file() {
        return;
    }
    let modified_ms = match metadata.modified() {
        Ok(modified) => time_since_epoch_ms(modified),
        Err(error) => {
            record_candidate_error(&error, result);
            return;
        }
    };
    if now_ms - modified_ms < max_age_ms {
        return;
    }

    match tokio::fs::remove_file(file_path).await {
        Ok(()) => {
            result.files_deleted += 1;
            result.bytes_freed = result.bytes_freed.saturating_add(metadata.len());
        }
        Err(error) => record_candidate_error(&error, result),
    }
}

fn record_candidate_error(error: &io::Error, result: &mut CleanupResult) {
    if error.kind() != io::ErrorKind::NotFound {
        result.errors += 1;
    }
}

async fn read_directory_names(dir: &Path) -> io::Result<Vec<OsString>> {
    let mut reader = tokio::fs::read_dir(dir).await?;
    let mut entries = Vec::new();
    while let Some(entry) = reader.next_entry().await? {
        entries.push(entry.file_name());
    }
    Ok(entries)
}

fn current_time_ms() -> f64 {
    // Date.now() returns an integer millisecond timestamp.
    time_since_epoch_ms(SystemTime::now()).floor()
}

fn time_since_epoch_ms(time: SystemTime) -> f64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs_f64() * 1000.0,
        Err(error) => -(error.duration().as_secs_f64() * 1000.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::Duration;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("tool-result-cleanup-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn set_modified_ms(path: &Path, millis: u64) {
        let file = fs::OpenOptions::new().write(true).open(path).unwrap();
        file.set_times(
            fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_millis(millis)),
        )
        .unwrap();
    }

    fn create_old_file(path: &Path, contents: &[u8], now_ms: u64) {
        fs::write(path, contents).unwrap();
        set_modified_ms(path, now_ms - 10_000);
    }

    #[tokio::test]
    async fn missing_global_directory_returns_zero_counts() {
        let root = TestDirectory::new();
        let result = cleanup_old_tool_results(root.path().join("missing"), 100.0).await;
        assert_eq!(result, CleanupResult::default());
    }

    #[tokio::test]
    async fn failed_project_stat_counts_error_including_not_found() {
        let root = TestDirectory::new();
        let mut result = CleanupResult::default();
        cleanup_project_dir(
            &root.path().join("disappeared-project"),
            20_000.0,
            100.0,
            &mut result,
        )
        .await;
        assert_eq!(result.errors, 1);
        assert_eq!(result.files_deleted, 0);
        assert_eq!(result.bytes_freed, 0);
    }

    #[tokio::test]
    async fn deletes_old_tool_results_and_legacy_outputs_and_counts_bytes() {
        let root = TestDirectory::new();
        let now_ms = 20_000_000;
        let project = root.path().join("project-hash");
        let tool_results = project.join("tool-results");
        fs::create_dir_all(&tool_results).unwrap();

        let old_result = tool_results.join("old.bin");
        create_old_file(&old_result, b"abc", now_ms);
        let recent_result = tool_results.join("recent.bin");
        fs::write(&recent_result, b"recent").unwrap();
        set_modified_ms(&recent_result, now_ms - 99);

        let legacy_output = project.join("legacy.output");
        create_old_file(&legacy_output, b"12345", now_ms);
        let unrelated = project.join("keep.txt");
        create_old_file(&unrelated, b"keep", now_ms);
        let nested_dir = tool_results.join("nested");
        fs::create_dir_all(&nested_dir).unwrap();
        let nested_file = nested_dir.join("nested.bin");
        create_old_file(&nested_file, b"nested", now_ms);

        let result = cleanup_old_tool_results_at(root.path(), 100.0, now_ms as f64).await;
        assert_eq!(
            result,
            CleanupResult {
                files_deleted: 2,
                bytes_freed: 8,
                errors: 0,
            }
        );
        assert!(!old_result.exists());
        assert!(!legacy_output.exists());
        assert!(recent_result.exists());
        assert!(unrelated.exists());
        assert!(nested_file.exists());
    }

    #[tokio::test]
    async fn age_boundary_deletes_at_threshold_and_keeps_newer_or_future_files() {
        let root = TestDirectory::new();
        let now_ms = 20_000_000;
        let tool_results = root.path().join("project").join("tool-results");
        fs::create_dir_all(&tool_results).unwrap();

        let exact = tool_results.join("exact.bin");
        fs::write(&exact, b"exact").unwrap();
        set_modified_ms(&exact, now_ms - 100);
        let younger = tool_results.join("younger.bin");
        fs::write(&younger, b"younger").unwrap();
        set_modified_ms(&younger, now_ms - 99);
        let future = tool_results.join("future.bin");
        fs::write(&future, b"future").unwrap();
        set_modified_ms(&future, now_ms + 1);

        let result = cleanup_old_tool_results_at(root.path(), 100.0, now_ms as f64).await;
        assert_eq!(result.files_deleted, 1);
        assert_eq!(result.bytes_freed, 5);
        assert_eq!(result.errors, 0);
        assert!(!exact.exists());
        assert!(younger.exists());
        assert!(future.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_projects_files_and_directories_are_never_followed() {
        use std::os::unix::fs::symlink;

        let root = TestDirectory::new();
        let now_ms = 20_000_000;
        let global = root.path().join("global");
        fs::create_dir(&global).unwrap();
        let project = global.join("real-project");
        let tool_results = project.join("tool-results");
        fs::create_dir_all(&tool_results).unwrap();
        let old_regular = tool_results.join("old.bin");
        create_old_file(&old_regular, b"old", now_ms);

        let outside = root.path().join("outside");
        let outside_results = outside.join("tool-results");
        fs::create_dir_all(&outside_results).unwrap();
        let outside_file = outside_results.join("outside.bin");
        create_old_file(&outside_file, b"outside", now_ms);
        symlink(&outside, global.join("linked-project")).unwrap();
        symlink(&outside_file, tool_results.join("file-link.bin")).unwrap();
        symlink(&outside_results, tool_results.join("directory-link")).unwrap();
        symlink(&outside_file, project.join("legacy-link.output")).unwrap();

        let result = cleanup_old_tool_results_at(&global, 100.0, now_ms as f64).await;
        assert_eq!(result.files_deleted, 1);
        assert_eq!(result.bytes_freed, 3);
        assert_eq!(result.errors, 0);
        assert!(!old_regular.exists());
        assert!(outside_file.exists());
        assert!(
            fs::symlink_metadata(global.join("linked-project"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(
            fs::symlink_metadata(tool_results.join("file-link.bin"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(
            fs::symlink_metadata(tool_results.join("directory-link"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(
            fs::symlink_metadata(project.join("legacy-link.output"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[tokio::test]
    async fn missing_candidate_is_quiet_but_stat_and_delete_failures_count() {
        let root = TestDirectory::new();
        let now_ms = 20_000_000.0;
        let missing = root.path().join("gone.bin");
        let mut result = CleanupResult::default();
        try_delete_if_old(&missing, now_ms, 0.0, &mut result).await;
        assert_eq!(result.errors, 0);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let no_search = root.path().join("no-search");
            fs::create_dir(&no_search).unwrap();
            let stat_path = no_search.join("old.bin");
            create_old_file(&stat_path, b"stat", now_ms as u64);
            fs::set_permissions(&no_search, fs::Permissions::from_mode(0o000)).unwrap();
            let mut stat_result = CleanupResult::default();
            try_delete_if_old(&stat_path, now_ms, 0.0, &mut stat_result).await;
            fs::set_permissions(&no_search, fs::Permissions::from_mode(0o700)).unwrap();
            assert_eq!(stat_result.errors, 1);
            assert!(stat_path.exists());

            let no_write = root.path().join("no-write");
            fs::create_dir(&no_write).unwrap();
            let delete_path = no_write.join("old.bin");
            create_old_file(&delete_path, b"delete", now_ms as u64);
            fs::set_permissions(&no_write, fs::Permissions::from_mode(0o500)).unwrap();
            let mut delete_result = CleanupResult::default();
            try_delete_if_old(&delete_path, now_ms, 0.0, &mut delete_result).await;
            fs::set_permissions(&no_write, fs::Permissions::from_mode(0o700)).unwrap();
            assert_eq!(delete_result.errors, 1);
            assert!(delete_path.exists());
        }
    }

    #[tokio::test]
    async fn missing_tool_results_directory_and_non_project_entries_are_quiet() {
        let root = TestDirectory::new();
        fs::write(root.path().join("not-a-project"), b"file").unwrap();
        fs::create_dir(root.path().join("project-without-tool-results")).unwrap();
        let result = cleanup_old_tool_results_at(root.path(), 1.0, 20_000.0).await;
        assert_eq!(result, CleanupResult::default());
    }
}
