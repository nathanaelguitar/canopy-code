//! Structured prior-read decisions shared by file mutation tools.
//!
//! This checks that a regular file was read as text and that its metadata
//! fingerprint has not changed since the read. Text reads may be partial: the
//! cache's `last_read_was_full` bit is deliberately not part of this policy.
//! Notebook edits use the separate full-read check in `FileReadCache`.

use std::fs;
use std::io;
use std::path::Path;

use serde::Serialize;

use crate::file_read_cache::{FileReadCache, FileReadCheckResult};

const READ_FILE_TOOL: &str = "read_file";
const NOTEBOOK_EDIT_TOOL: &str = "notebook_edit";

/// Stable tool error codes for a prior-read rejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum PriorReadErrorCode {
    #[serde(rename = "edit_requires_prior_read")]
    EditRequiresPriorRead,
    #[serde(rename = "file_changed_since_read")]
    FileChangedSinceRead,
    #[serde(rename = "prior_read_verification_failed")]
    PriorReadVerificationFailed,
    #[serde(rename = "target_is_directory")]
    TargetIsDirectory,
}

impl PriorReadErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EditRequiresPriorRead => "edit_requires_prior_read",
            Self::FileChangedSinceRead => "file_changed_since_read",
            Self::PriorReadVerificationFailed => "prior_read_verification_failed",
            Self::TargetIsDirectory => "target_is_directory",
        }
    }
}

/// Wording used in the model-facing and user-facing rejection messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PriorReadVerb {
    Editing,
    Overwriting,
}

impl PriorReadVerb {
    const fn gerund(self) -> &'static str {
        match self {
            Self::Editing => "editing",
            Self::Overwriting => "overwriting",
        }
    }

    const fn bare(self) -> &'static str {
        match self {
            Self::Editing => "edit",
            Self::Overwriting => "overwrite",
        }
    }

    const fn display_gerund(self) -> &'static str {
        match self {
            Self::Editing => "editing this file",
            Self::Overwriting => "overwriting this file",
        }
    }
}

/// Result of checking whether a mutation is cleared by the session read cache.
///
/// Serialization has the same shape as the TypeScript union: an allowed
/// result contains only `{"ok":true}`, while a rejection contains `ok`,
/// `type`, `rawMessage`, and `displayMessage`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PriorReadDecision {
    pub ok: bool,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<PriorReadErrorCode>,
    #[serde(rename = "rawMessage", skip_serializing_if = "Option::is_none")]
    pub raw_message: Option<String>,
    #[serde(rename = "displayMessage", skip_serializing_if = "Option::is_none")]
    pub display_message: Option<String>,
}

impl PriorReadDecision {
    fn allow() -> Self {
        Self {
            ok: true,
            error_type: None,
            raw_message: None,
            display_message: None,
        }
    }

    fn reject(
        error_type: PriorReadErrorCode,
        raw_message: String,
        display_message: String,
    ) -> Self {
        Self {
            ok: false,
            error_type: Some(error_type),
            raw_message: Some(raw_message),
            display_message: Some(display_message),
        }
    }
}

/// Check whether a file may be edited or overwritten based on the session's
/// `FileReadCache`.
///
/// `expect_existing` is used by post-read and pre-write rechecks. If the path
/// disappears at that point, the read is stale and the helper rejects it. For
/// the initial pre-read check, a missing path is allowed so a new file can be
/// created.
pub fn check_prior_read(
    cache: &FileReadCache,
    file_path: impl AsRef<Path>,
    verb: PriorReadVerb,
    expect_existing: bool,
) -> PriorReadDecision {
    let file_path = file_path.as_ref();
    let metadata = match fs::metadata(file_path) {
        Ok(metadata) => metadata,
        Err(error) => {
            let code = io_error_code(&error);
            return stat_failure_decision(file_path, verb, expect_existing, code.as_deref());
        }
    };

    if metadata.is_dir() {
        let verb_bare = verb.bare();
        return PriorReadDecision::reject(
            PriorReadErrorCode::TargetIsDirectory,
            format!(
                "{} is a directory. The Edit / WriteFile tools only operate on regular files. Use a different mechanism (e.g. the shell tool) if you need to {verb_bare} the contents of this directory.",
                file_path.display()
            ),
            format!("path is a directory; cannot {verb_bare} via this tool."),
        );
    }

    if !metadata.is_file() {
        let verb_bare = verb.bare();
        return PriorReadDecision::reject(
            PriorReadErrorCode::EditRequiresPriorRead,
            format!(
                "{} is a FIFO / socket / character or block device. The Edit / WriteFile tools only operate on regular files; the {READ_FILE_TOOL} tool also rejects these targets. Use a different mechanism (e.g. shell tool with the appropriate command) if you need to {verb_bare} this path.",
                file_path.display()
            ),
            format!("special file; cannot {verb_bare} via this tool."),
        );
    }

    cache_decision(file_path, verb, cache.check(&metadata))
}

fn stat_failure_decision(
    file_path: &Path,
    verb: PriorReadVerb,
    expect_existing: bool,
    code: Option<&str>,
) -> PriorReadDecision {
    if code == Some("ENOENT") {
        if !expect_existing {
            return PriorReadDecision::allow();
        }
        return PriorReadDecision::reject(
            PriorReadErrorCode::FileChangedSinceRead,
            format!(
                "File {} disappeared after the model read it (stat now returns ENOENT). Re-read with the {READ_FILE_TOOL} tool — the path may have been deleted or moved — before retrying {} it.",
                file_path.display(),
                verb.gerund()
            ),
            format!("file disappeared after last read; re-run {READ_FILE_TOOL} first."),
        );
    }

    let code = code.unwrap_or("unknown error");
    PriorReadDecision::reject(
        PriorReadErrorCode::PriorReadVerificationFailed,
        format!(
            "Could not stat {} to verify prior read ({code}). Re-read with the {READ_FILE_TOOL} tool, then retry {} it.",
            file_path.display(),
            verb.gerund()
        ),
        format!(
            "cannot verify prior read of {}; re-run {READ_FILE_TOOL} before {}.",
            file_path.display(),
            verb.display_gerund()
        ),
    )
}

fn cache_decision(
    file_path: &Path,
    verb: PriorReadVerb,
    status: FileReadCheckResult,
) -> PriorReadDecision {
    match status {
        FileReadCheckResult::Fresh(entry)
            if entry.last_read_at.is_some() && entry.last_read_cacheable =>
        {
            // Deliberately do not inspect `last_read_was_full`: an ordinary
            // text edit may be based on a partial read.
            PriorReadDecision::allow()
        }
        FileReadCheckResult::Stale(_) => PriorReadDecision::reject(
            PriorReadErrorCode::FileChangedSinceRead,
            format!(
                "File {} has been modified since you last read it (mtime or size changed). Re-read it with the {READ_FILE_TOOL} tool before {} it to ensure your changes are based on current content.",
                file_path.display(),
                verb.gerund()
            ),
            format!("file changed since last read; re-run {READ_FILE_TOOL} first."),
        ),
        FileReadCheckResult::Unverifiable => {
            let verb_bare = verb.bare();
            PriorReadDecision::reject(
                PriorReadErrorCode::PriorReadVerificationFailed,
                format!(
                    "File {} is on a filesystem that does not provide a verifiable inode identity (ino=0), so the {verb_bare} tool cannot safely confirm a prior read. Use a different mechanism (for example, the shell tool) to {verb_bare} this file.",
                    file_path.display()
                ),
                format!(
                    "cannot verify prior read of {}; use a different mechanism to {verb_bare} it.",
                    file_path.display()
                ),
            )
        }
        FileReadCheckResult::Fresh(entry)
            if entry.last_read_at.is_some() && !entry.last_read_cacheable =>
        {
            let verb_bare = verb.bare();
            PriorReadDecision::reject(
                PriorReadErrorCode::EditRequiresPriorRead,
                format!(
                    "File {} is a binary / image / audio / video / PDF / notebook payload that the {READ_FILE_TOOL} tool returns as a structured value rather than as plain text. The Edit / WriteFile tools cannot mutate that payload safely — re-reading it would not change this. If this is a Jupyter notebook (.ipynb), use the {NOTEBOOK_EDIT_TOOL} tool for cell-level edits after reading it. For other non-text files, use a different mechanism (e.g. shell tool with an appropriate writer) if you need to {verb_bare} it.",
                    file_path.display()
                ),
                format!("non-text payload; cannot {verb_bare} via this tool."),
            )
        }
        FileReadCheckResult::Unknown | FileReadCheckResult::Fresh(_) => {
            let verb_bare = verb.bare();
            let partial_read_guidance = match verb {
                PriorReadVerb::Editing => format!(
                    "(a partial read with offset / limit is fine — you only need to have seen the bytes you intend to {verb_bare})"
                ),
                PriorReadVerb::Overwriting => {
                    "(read the full file — overwriting replaces every byte, so any unseen bytes would be discarded)".to_owned()
                }
            };
            PriorReadDecision::reject(
                PriorReadErrorCode::EditRequiresPriorRead,
                format!(
                    "File {} has not been read in this session. Use the {READ_FILE_TOOL} tool first to load the current content {partial_read_guidance} before {} it.",
                    file_path.display(),
                    verb.gerund()
                ),
                format!(
                    "{READ_FILE_TOOL} required before {}.",
                    verb.display_gerund()
                ),
            )
        }
    }
}

fn io_error_code(error: &io::Error) -> Option<String> {
    let errno = error.raw_os_error()?;
    #[cfg(unix)]
    {
        // Node exposes symbolic errno names in `error.code`; `std::io::Error`
        // only exposes the numeric OS value, so map the portable POSIX names
        // here to preserve that model-facing message.
        match nix::errno::Errno::from_raw(errno) {
            nix::errno::Errno::UnknownErrno => None,
            errno => Some(format!("{errno:?}")),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = errno;
        let name = match error.kind() {
            io::ErrorKind::NotFound => "ENOENT",
            io::ErrorKind::PermissionDenied => "EACCES",
            io::ErrorKind::AlreadyExists => "EEXIST",
            io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => "EINVAL",
            io::ErrorKind::TimedOut => "ETIMEDOUT",
            io::ErrorKind::WouldBlock => "EAGAIN",
            _ => return None,
        };
        Some(name.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use uuid::Uuid;

    struct TempWorkspace(PathBuf);

    impl TempWorkspace {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("prior-read-{}", Uuid::new_v4()));
            fs::create_dir_all(&root).unwrap();
            Self(root)
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn rejection(decision: PriorReadDecision) -> (PriorReadErrorCode, String, String) {
        assert!(!decision.ok);
        (
            decision.error_type.unwrap(),
            decision.raw_message.unwrap(),
            decision.display_message.unwrap(),
        )
    }

    #[test]
    fn missing_new_file_is_allowed_but_expected_existing_file_is_stale() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("missing.txt");
        let cache = FileReadCache::default();

        assert!(check_prior_read(&cache, &path, PriorReadVerb::Overwriting, false).ok);
        let (code, raw, display) = rejection(check_prior_read(
            &cache,
            &path,
            PriorReadVerb::Editing,
            true,
        ));
        assert_eq!(code, PriorReadErrorCode::FileChangedSinceRead);
        assert_eq!(
            raw,
            format!(
                "File {} disappeared after the model read it (stat now returns ENOENT). Re-read with the read_file tool — the path may have been deleted or moved — before retrying editing it.",
                path.display()
            )
        );
        assert_eq!(
            display,
            "file disappeared after last read; re-run read_file first."
        );
    }

    #[test]
    fn non_enoent_stat_failure_fails_closed_with_its_code() {
        let path = Path::new("/private/file.txt");
        let (code, raw, display) = rejection(stat_failure_decision(
            path,
            PriorReadVerb::Overwriting,
            false,
            Some("EACCES"),
        ));
        assert_eq!(code, PriorReadErrorCode::PriorReadVerificationFailed);
        assert_eq!(
            raw,
            "Could not stat /private/file.txt to verify prior read (EACCES). Re-read with the read_file tool, then retry overwriting it."
        );
        assert_eq!(
            display,
            "cannot verify prior read of /private/file.txt; re-run read_file before overwriting this file."
        );
        assert_eq!(
            io_error_code(&io::Error::from_raw_os_error(libc::EACCES)).as_deref(),
            Some("EACCES")
        );
    }

    #[test]
    fn directories_and_other_non_regular_targets_get_distinct_rejections() {
        let workspace = TempWorkspace::new();
        let directory = workspace.0.join("directory");
        fs::create_dir(&directory).unwrap();
        let cache = FileReadCache::default();
        let (code, raw, display) = rejection(check_prior_read(
            &cache,
            &directory,
            PriorReadVerb::Editing,
            false,
        ));
        assert_eq!(code, PriorReadErrorCode::TargetIsDirectory);
        assert_eq!(
            raw,
            format!(
                "{} is a directory. The Edit / WriteFile tools only operate on regular files. Use a different mechanism (e.g. the shell tool) if you need to edit the contents of this directory.",
                directory.display()
            )
        );
        assert_eq!(display, "path is a directory; cannot edit via this tool.");

        #[cfg(unix)]
        {
            use std::os::unix::net::UnixListener;

            let socket = workspace.0.join("socket");
            let listener = UnixListener::bind(&socket).unwrap();
            let (code, raw, display) = rejection(check_prior_read(
                &cache,
                &socket,
                PriorReadVerb::Overwriting,
                false,
            ));
            assert_eq!(code, PriorReadErrorCode::EditRequiresPriorRead);
            assert_eq!(
                raw,
                format!(
                    "{} is a FIFO / socket / character or block device. The Edit / WriteFile tools only operate on regular files; the read_file tool also rejects these targets. Use a different mechanism (e.g. shell tool with the appropriate command) if you need to overwrite this path.",
                    socket.display()
                )
            );
            assert_eq!(display, "special file; cannot overwrite via this tool.");
            drop(listener);
        }
    }

    #[test]
    fn fresh_cacheable_partial_text_read_is_accepted() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("partial.txt");
        fs::write(&path, "first line\nsecond line\n").unwrap();
        let metadata = fs::metadata(&path).unwrap();
        let cache = FileReadCache::default();
        cache.record_read(&path, &metadata, false, true);

        assert!(check_prior_read(&cache, &path, PriorReadVerb::Editing, true).ok);
    }

    #[test]
    fn stale_read_is_rejected_with_the_source_message() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("stale.txt");
        fs::write(&path, "old").unwrap();
        let metadata = fs::metadata(&path).unwrap();
        let cache = FileReadCache::default();
        cache.record_read(&path, &metadata, true, true);
        fs::write(&path, "changed to a different size").unwrap();

        let (code, raw, display) = rejection(check_prior_read(
            &cache,
            &path,
            PriorReadVerb::Editing,
            true,
        ));
        assert_eq!(code, PriorReadErrorCode::FileChangedSinceRead);
        assert_eq!(
            raw,
            format!(
                "File {} has been modified since you last read it (mtime or size changed). Re-read it with the read_file tool before editing it to ensure your changes are based on current content.",
                path.display()
            )
        );
        assert_eq!(
            display,
            "file changed since last read; re-run read_file first."
        );
    }

    #[test]
    fn unverifiable_and_fresh_non_cacheable_reads_are_rejected() {
        let path = Path::new("/workspace/blob.bin");
        let (code, raw, display) = rejection(cache_decision(
            path,
            PriorReadVerb::Overwriting,
            FileReadCheckResult::Unverifiable,
        ));
        assert_eq!(code, PriorReadErrorCode::PriorReadVerificationFailed);
        assert_eq!(
            raw,
            "File /workspace/blob.bin is on a filesystem that does not provide a verifiable inode identity (ino=0), so the overwrite tool cannot safely confirm a prior read. Use a different mechanism (for example, the shell tool) to overwrite this file."
        );
        assert_eq!(
            display,
            "cannot verify prior read of /workspace/blob.bin; use a different mechanism to overwrite it."
        );

        let workspace = TempWorkspace::new();
        let path = workspace.0.join("blob.bin");
        fs::write(&path, [0_u8, 1, 2]).unwrap();
        let metadata = fs::metadata(&path).unwrap();
        let cache = FileReadCache::default();
        cache.record_read(&path, &metadata, true, false);
        let (code, raw, display) = rejection(check_prior_read(
            &cache,
            &path,
            PriorReadVerb::Editing,
            true,
        ));
        assert_eq!(code, PriorReadErrorCode::EditRequiresPriorRead);
        assert_eq!(
            raw,
            format!(
                "File {} is a binary / image / audio / video / PDF / notebook payload that the read_file tool returns as a structured value rather than as plain text. The Edit / WriteFile tools cannot mutate that payload safely — re-reading it would not change this. If this is a Jupyter notebook (.ipynb), use the notebook_edit tool for cell-level edits after reading it. For other non-text files, use a different mechanism (e.g. shell tool with an appropriate writer) if you need to edit it.",
                path.display()
            )
        );
        assert_eq!(display, "non-text payload; cannot edit via this tool.");
    }

    #[test]
    fn never_read_guidance_varies_by_verb_without_requiring_full_text_reads() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("unread.txt");
        fs::write(&path, "text").unwrap();
        let cache = FileReadCache::default();

        let (code, raw, display) = rejection(check_prior_read(
            &cache,
            &path,
            PriorReadVerb::Editing,
            false,
        ));
        assert_eq!(code, PriorReadErrorCode::EditRequiresPriorRead);
        assert_eq!(
            raw,
            format!(
                "File {} has not been read in this session. Use the read_file tool first to load the current content (a partial read with offset / limit is fine — you only need to have seen the bytes you intend to edit) before editing it.",
                path.display()
            )
        );
        assert_eq!(display, "read_file required before editing this file.");

        let (_, raw, display) = rejection(check_prior_read(
            &cache,
            &path,
            PriorReadVerb::Overwriting,
            false,
        ));
        assert!(raw.contains("(read the full file — overwriting replaces every byte"));
        assert_eq!(display, "read_file required before overwriting this file.");
    }

    #[test]
    fn rejection_codes_serialize_to_the_typescript_error_strings() {
        assert_eq!(
            serde_json::to_string(&PriorReadErrorCode::FileChangedSinceRead).unwrap(),
            "\"file_changed_since_read\""
        );
        assert_eq!(
            serde_json::to_value(PriorReadDecision::allow()).unwrap(),
            serde_json::json!({"ok": true})
        );
    }
}
