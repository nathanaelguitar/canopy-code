//! Safe streaming extraction for extension tar archives.
//!
//! Link entries are rejected by a complete archive scan before any output is
//! written. The extraction pass repeats the link check and validates every
//! entry path, so a changed archive cannot use a link or traversal path after
//! the validation pass. File contents are copied in fixed-size chunks and
//! cancellation is polled during both archive reads and writes.

use crate::archive_safety::{
    ArchiveSafetyError, ArchiveScanCancellation, assert_tar_archive_has_no_links,
};
use flate2::read::MultiGzDecoder;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Component, Path, PathBuf};
use tar::{Archive, EntryType};
use thiserror::Error;

const COPY_BUFFER_SIZE: usize = 64 * 1024;

/// Failure while safely extracting a tar or gzip-compressed tar archive.
#[derive(Debug, Error)]
pub enum ArchiveExtractionError {
    #[error("Tar archive extraction cancelled")]
    Cancelled,
    #[error(transparent)]
    Safety(#[from] ArchiveSafetyError),
    #[error("Unable to prepare tar extraction destination: {0}")]
    Destination(#[source] io::Error),
    #[error("Malformed tar archive: {0}")]
    MalformedArchive(String),
    #[error("Tar archive changed after link validation")]
    ArchiveChanged,
    #[error("Tar archive contains an unsupported entry type")]
    UnsupportedEntryType,
    #[error("Tar archive contains a path outside the extraction root")]
    OutOfBoundsPath,
    #[error("Tar archive extraction worker failed: {0}")]
    Worker(#[source] tokio::task::JoinError),
}

/// Extract a plain tar or gzip-compressed tar archive under `destination`.
///
/// The archive is scanned for symlinks and hard links before extraction. The
/// extractor also rejects traversal components, existing symlink parents,
/// and non-regular entry types. It uses a 64 KiB copy buffer; the TypeScript
/// `extractFile` path has no total expanded-size limit, so this API does not
/// impose one either. The existing 100 MiB limit applies to downloaded archive
/// bytes in the caller's download path.
pub async fn extract_tar_gz_archive(
    file: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    cancellation: Option<ArchiveScanCancellation>,
) -> Result<(), ArchiveExtractionError> {
    check_cancelled(cancellation.as_ref())?;
    let file = file.as_ref().to_owned();
    let destination = destination.as_ref().to_owned();

    assert_tar_archive_has_no_links(&file, cancellation.clone())
        .await
        .map_err(map_safety_error)?;
    check_cancelled(cancellation.as_ref())?;

    let worker_cancellation = cancellation.clone();
    let result = tokio::task::spawn_blocking(move || {
        extract_tar_gz_archive_blocking(&file, &destination, worker_cancellation.as_ref())
    })
    .await
    .map_err(ArchiveExtractionError::Worker)?;

    check_cancelled(cancellation.as_ref())?;
    result
}

fn map_safety_error(error: ArchiveSafetyError) -> ArchiveExtractionError {
    match error {
        ArchiveSafetyError::Cancelled => ArchiveExtractionError::Cancelled,
        other => ArchiveExtractionError::Safety(other),
    }
}

fn check_cancelled(
    cancellation: Option<&ArchiveScanCancellation>,
) -> Result<(), ArchiveExtractionError> {
    if cancellation.is_some_and(ArchiveScanCancellation::is_cancelled) {
        Err(ArchiveExtractionError::Cancelled)
    } else {
        Ok(())
    }
}

fn extract_tar_gz_archive_blocking(
    file: &Path,
    destination: &Path,
    cancellation: Option<&ArchiveScanCancellation>,
) -> Result<(), ArchiveExtractionError> {
    check_cancelled(cancellation)?;
    fs::create_dir_all(destination).map_err(ArchiveExtractionError::Destination)?;
    let root = destination
        .canonicalize()
        .map_err(ArchiveExtractionError::Destination)?;
    let input = File::open(file).map_err(ArchiveExtractionError::Destination)?;
    let mut buffered = BufReader::new(input);
    let is_gzip = buffered
        .fill_buf()
        .map_err(|error| map_read_error(error, cancellation))?
        .starts_with(&[0x1f, 0x8b]);
    let reader: Box<dyn Read + Send> = if is_gzip {
        Box::new(MultiGzDecoder::new(buffered))
    } else {
        Box::new(buffered)
    };
    let reader = CancellationReader {
        inner: reader,
        cancellation,
    };
    let mut archive = Archive::new(reader);

    {
        let entries = archive
            .entries()
            .map_err(|error| map_read_error(error, cancellation))?;
        for entry in entries {
            check_cancelled(cancellation)?;
            let mut entry = entry.map_err(|error| map_read_error(error, cancellation))?;
            let entry_type = entry.header().entry_type();
            if entry_type == EntryType::Symlink || entry_type == EntryType::Link {
                return Err(ArchiveExtractionError::ArchiveChanged);
            }

            let destination = match checked_entry_destination(&root, &entry)? {
                Some(destination) => destination,
                None => continue,
            };

            match entry_type {
                EntryType::Directory => ensure_directory(&root, &destination)?,
                EntryType::Regular | EntryType::Continuous => {
                    let mode = entry
                        .header()
                        .mode()
                        .map(|mode| mode & 0o777)
                        .unwrap_or(0o644);
                    let parent = destination
                        .parent()
                        .ok_or(ArchiveExtractionError::OutOfBoundsPath)?;
                    ensure_directory(&root, parent)?;
                    prepare_regular_target(&destination)?;
                    let mut output = open_output_file(&destination, mode)
                        .map_err(ArchiveExtractionError::Destination)?;
                    copy_entry_contents(&mut entry, &mut output, cancellation)?;
                    output
                        .flush()
                        .map_err(ArchiveExtractionError::Destination)?;
                }
                // The preflight scanner consumes PAX and GNU metadata records
                // as tar metadata. Sparse and special-file entries are not
                // needed by extension source archives and are never created.
                _ => return Err(ArchiveExtractionError::UnsupportedEntryType),
            }
        }
    }

    // Check the gzip trailer even when the tar end marker precedes the end of
    // the compressed stream. This also keeps cancellation active during drain.
    let mut reader = archive.into_inner();
    io::copy(&mut reader, &mut io::sink()).map_err(|error| map_read_error(error, cancellation))?;
    check_cancelled(cancellation)
}

fn checked_entry_destination<R: Read>(
    root: &Path,
    entry: &tar::Entry<'_, R>,
) -> Result<Option<PathBuf>, ArchiveExtractionError> {
    let path = entry
        .path()
        .map_err(|error| ArchiveExtractionError::MalformedArchive(error.to_string()))?;
    let mut relative = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(component) => relative.push(component),
            // Tarballs commonly prefix names with `./`; normalize that away.
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(ArchiveExtractionError::OutOfBoundsPath);
            }
        }
    }
    if relative.as_os_str().is_empty() {
        return Ok(None);
    }
    let destination = root.join(relative);
    if !destination.starts_with(root) {
        return Err(ArchiveExtractionError::OutOfBoundsPath);
    }
    Ok(Some(destination))
}

fn ensure_directory(root: &Path, destination: &Path) -> Result<(), ArchiveExtractionError> {
    if !destination.starts_with(root) {
        return Err(ArchiveExtractionError::OutOfBoundsPath);
    }
    let relative = destination
        .strip_prefix(root)
        .map_err(|_| ArchiveExtractionError::OutOfBoundsPath)?;
    let mut current = root.to_owned();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(ArchiveExtractionError::OutOfBoundsPath);
        };
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => return Err(ArchiveExtractionError::OutOfBoundsPath),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        let metadata = fs::symlink_metadata(&current)
                            .map_err(ArchiveExtractionError::Destination)?;
                        if !metadata.is_dir() || metadata.file_type().is_symlink() {
                            return Err(ArchiveExtractionError::OutOfBoundsPath);
                        }
                    }
                    Err(error) => return Err(ArchiveExtractionError::Destination(error)),
                }
            }
            Err(error) => return Err(ArchiveExtractionError::Destination(error)),
        }
    }
    Ok(())
}

fn prepare_regular_target(destination: &Path) -> Result<(), ArchiveExtractionError> {
    match fs::symlink_metadata(destination) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            // Unlink before creating the new file. Truncating an existing
            // regular path could modify data reachable through a hard link.
            fs::remove_file(destination).map_err(ArchiveExtractionError::Destination)
        }
        Ok(_) => Err(ArchiveExtractionError::OutOfBoundsPath),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ArchiveExtractionError::Destination(error)),
    }
}

fn open_output_file(destination: &Path, mode: u32) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(mode)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    #[cfg(not(unix))]
    let _ = mode;
    options.open(destination)
}

fn copy_entry_contents<R: Read, W: Write>(
    input: &mut R,
    output: &mut W,
    cancellation: Option<&ArchiveScanCancellation>,
) -> Result<(), ArchiveExtractionError> {
    let mut buffer = [0_u8; COPY_BUFFER_SIZE];
    loop {
        check_cancelled(cancellation)?;
        let bytes_read = input
            .read(&mut buffer)
            .map_err(|error| map_read_error(error, cancellation))?;
        if bytes_read == 0 {
            return Ok(());
        }
        check_cancelled(cancellation)?;
        output
            .write_all(&buffer[..bytes_read])
            .map_err(ArchiveExtractionError::Destination)?;
    }
}

fn map_read_error(
    error: io::Error,
    cancellation: Option<&ArchiveScanCancellation>,
) -> ArchiveExtractionError {
    if cancellation.is_some_and(ArchiveScanCancellation::is_cancelled) {
        ArchiveExtractionError::Cancelled
    } else {
        ArchiveExtractionError::MalformedArchive(error.to_string())
    }
}

struct CancellationReader<'a, R> {
    inner: R,
    cancellation: Option<&'a ArchiveScanCancellation>,
}

impl<R: Read> Read for CancellationReader<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self
            .cancellation
            .is_some_and(ArchiveScanCancellation::is_cancelled)
        {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "Tar archive extraction cancelled",
            ));
        }
        self.inner.read(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::{ArchiveExtractionError, copy_entry_contents, extract_tar_gz_archive};
    use crate::archive_safety::ArchiveScanCancellation;
    use flate2::{Compression, write::GzEncoder};
    use std::fs::{self, File};
    use std::io::{self, Cursor, Read};
    use std::path::{Path, PathBuf};
    use tar::{Builder, EntryType, Header};
    use uuid::Uuid;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("canopy-tar-extraction-{}", Uuid::new_v4()));
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

    fn write_tar_gz(path: &Path, entries: impl FnOnce(&mut Builder<GzEncoder<File>>)) {
        let output = File::create(path).unwrap();
        let encoder = GzEncoder::new(output, Compression::fast());
        let mut archive = Builder::new(encoder);
        entries(&mut archive);
        let encoder = archive.into_inner().unwrap();
        encoder.finish().unwrap();
    }

    fn append_file(archive: &mut Builder<GzEncoder<File>>, name: &str, data: &[u8]) {
        let mut header = Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive
            .append_data(&mut header, name, Cursor::new(data))
            .unwrap();
    }

    #[tokio::test]
    async fn extracts_regular_files_under_the_destination() {
        let temp = TestDirectory::new();
        let archive_path = temp.path().join("extension.tar.gz");
        let destination = temp.path().join("extracted");
        write_tar_gz(&archive_path, |archive| {
            append_file(archive, "wrapped/extension.json", br#"{"name":"demo"}"#);
            append_file(archive, "wrapped/nested/tool.sh", b"#!/bin/sh\n");
        });

        extract_tar_gz_archive(&archive_path, &destination, None)
            .await
            .unwrap();

        assert_eq!(
            fs::read(destination.join("wrapped/extension.json")).unwrap(),
            br#"{"name":"demo"}"#
        );
        assert_eq!(
            fs::read(destination.join("wrapped/nested/tool.sh")).unwrap(),
            b"#!/bin/sh\n"
        );
    }

    #[tokio::test]
    async fn rejects_links_before_writing_archive_entries() {
        let temp = TestDirectory::new();
        let archive_path = temp.path().join("link.tar.gz");
        let destination = temp.path().join("extracted");
        write_tar_gz(&archive_path, |archive| {
            append_file(archive, "before.txt", b"must not be written");
            let mut header = Header::new_gnu();
            header.set_entry_type(EntryType::symlink());
            header.set_size(0);
            header.set_cksum();
            archive
                .append_link(&mut header, "escape-link", "../../outside.txt")
                .unwrap();
        });

        let error = extract_tar_gz_archive(&archive_path, &destination, None)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ArchiveExtractionError::Safety(
                crate::archive_safety::ArchiveSafetyError::UnsupportedLinks { .. }
            )
        ));
        assert!(!destination.join("before.txt").exists());
        assert!(!destination.join("escape-link").exists());
    }

    #[tokio::test]
    async fn rejects_traversal_paths_without_writing_outside_the_destination() {
        let temp = TestDirectory::new();
        let archive_path = temp.path().join("traversal.tar.gz");
        let destination = temp.path().join("extracted");
        write_tar_gz(&archive_path, |archive| {
            append_file(archive, "../outside.txt", b"outside");
        });

        let error = extract_tar_gz_archive(&archive_path, &destination, None)
            .await
            .unwrap_err();
        assert!(matches!(error, ArchiveExtractionError::OutOfBoundsPath));
        assert!(!temp.path().join("outside.txt").exists());
    }

    #[tokio::test]
    async fn rejects_pre_cancelled_extraction() {
        let temp = TestDirectory::new();
        let archive_path = temp.path().join("cancelled.tar.gz");
        let destination = temp.path().join("extracted");
        write_tar_gz(&archive_path, |archive| {
            append_file(archive, "file.txt", b"content");
        });
        let cancellation = ArchiveScanCancellation::new();
        cancellation.cancel();

        let error = extract_tar_gz_archive(&archive_path, &destination, Some(cancellation))
            .await
            .unwrap_err();
        assert!(matches!(error, ArchiveExtractionError::Cancelled));
        assert!(!destination.exists());
    }

    struct CancellingReader {
        cancellation: ArchiveScanCancellation,
        first_read: bool,
    }

    impl Read for CancellingReader {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            if self.first_read {
                self.first_read = false;
                output[0] = b'x';
                self.cancellation.cancel();
                return Ok(1);
            }
            Ok(0)
        }
    }

    #[test]
    fn checks_cancellation_between_extracted_chunks() {
        let cancellation = ArchiveScanCancellation::new();
        let mut input = CancellingReader {
            cancellation: cancellation.clone(),
            first_read: true,
        };
        let mut output = Vec::new();

        let error = copy_entry_contents(&mut input, &mut output, Some(&cancellation)).unwrap_err();

        assert!(matches!(error, ArchiveExtractionError::Cancelled));
        assert!(output.is_empty());
    }
}
