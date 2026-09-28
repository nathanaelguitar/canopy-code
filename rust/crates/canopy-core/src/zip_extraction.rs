//! Safe, streaming extraction of extension ZIP archives.
//!
//! The implementation follows `packages/core/src/extension/zip-extraction.ts`:
//! it rejects ZIP Unix symlinks and out-of-root paths, verifies every existing
//! parent component without following links, skips `__MACOSX/`, and applies
//! the archived Unix permission bits (or the source defaults).

use crate::utils::cancellation::CancellationToken;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use thiserror::Error;
use zip::ZipArchive;
use zip::result::ZipError;

const ZIP_FILE_TYPE_MASK: u32 = 0xf000;
const ZIP_DIRECTORY_TYPE: u32 = 0x4000;
const ZIP_SYMBOLIC_LINK_TYPE: u32 = 0xa000;
const ZIP_DOS_DIRECTORY_ATTRIBUTE: u32 = 16;
const ZIP_CENTRAL_VERSION_MADE_BY_OFFSET: u64 = 4;
const ZIP_CENTRAL_EXTERNAL_ATTRIBUTES_OFFSET: u64 = 38;
const MAX_REPORTED_ZIP_PATH_LENGTH: usize = 200;
const COPY_BUFFER_SIZE: usize = 64 * 1024;

/// Failure while extracting a ZIP extension archive.
#[derive(Debug, Error)]
pub enum ZipExtractionError {
    #[error("Zip archive extraction cancelled")]
    Cancelled,
    #[error("Target directory is expected to be absolute")]
    TargetDirectoryNotAbsolute,
    #[error("Unable to prepare ZIP extraction destination: {0}")]
    Destination(#[source] io::Error),
    #[error("Unable to read ZIP archive entry: {0}")]
    EntryRead(#[source] io::Error),
    #[error("Unable to read ZIP archive: {0}")]
    Archive(#[from] ZipError),
    #[error("Zip archive contains unsupported symbolic link entry: {0}")]
    UnsupportedSymbolicLink(String),
    #[error("Out of bound path \"{destination}\" found while processing file {entry}")]
    OutOfBoundsEntry { destination: String, entry: String },
    #[error("Out of bound path \"{destination}\" found while preparing extraction")]
    OutOfBoundsDestination { destination: String },
    #[error("Refusing to extract through existing symbolic link: {0}")]
    ExistingSymbolicLink(String),
    #[error("Refusing to extract through non-directory path: {0}")]
    ExistingNonDirectory(String),
    #[error("ZIP extraction worker failed: {0}")]
    Worker(#[source] tokio::task::JoinError),
}

/// Extract `file` under an absolute `destination` directory.
///
/// Work runs on Tokio's blocking pool so ZIP decompression and disk I/O do not
/// block an async executor thread. Cancellation is checked before archive
/// operations and between 64 KiB entry reads/writes.
pub async fn extract_zip_archive(
    file: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    cancellation: Option<CancellationToken>,
) -> Result<(), ZipExtractionError> {
    check_cancelled(cancellation.as_ref())?;
    let file = file.as_ref().to_owned();
    let destination = destination.as_ref().to_owned();
    if !destination.is_absolute() {
        return Err(ZipExtractionError::TargetDirectoryNotAbsolute);
    }

    let worker_cancellation = cancellation.clone();
    let result = tokio::task::spawn_blocking(move || {
        extract_zip_archive_blocking(&file, &destination, worker_cancellation.as_ref())
    })
    .await
    .map_err(ZipExtractionError::Worker)?;

    // Match AbortSignal.throwIfAborted() after extraction and when an I/O or
    // decompression error races with cancellation.
    check_cancelled(cancellation.as_ref())?;
    result
}

fn extract_zip_archive_blocking(
    file: &Path,
    destination: &Path,
    cancellation: Option<&CancellationToken>,
) -> Result<(), ZipExtractionError> {
    check_cancelled(cancellation)?;
    fs::create_dir_all(destination).map_err(ZipExtractionError::Destination)?;
    let root = destination
        .canonicalize()
        .map_err(ZipExtractionError::Destination)?;
    check_cancelled(cancellation)?;

    let archive_file = File::open(file).map_err(ZipExtractionError::EntryRead)?;
    let archive_file = CancellationAwareFile {
        inner: archive_file,
        cancellation: cancellation.cloned(),
    };
    let mut archive = ZipArchive::new(archive_file)?;
    // zip exposes the decoded Unix mode, but its DOS fallback intentionally
    // synthesizes permissions. Read the raw central-directory attributes so
    // defaults and the legacy directory check match the TS implementation.
    let mut attributes_file = CancellationAwareFile {
        inner: File::open(file).map_err(ZipExtractionError::EntryRead)?,
        cancellation: cancellation.cloned(),
    };
    check_cancelled(cancellation)?;

    for index in 0..archive.len() {
        check_cancelled(cancellation)?;
        let mut entry = archive.by_index(index)?;
        let entry_name = entry.name().to_owned();

        // The TypeScript path skips this namespace before checking either the
        // entry type or its path, so preserve that ordering.
        if entry_name.starts_with("__MACOSX/") {
            continue;
        }

        let (mode, creator_os, external_attributes) =
            read_entry_attributes(&mut attributes_file, entry.central_header_start())
                .map_err(ZipExtractionError::EntryRead)?;
        let reported_entry = format_zip_path(&entry_name);
        if mode & ZIP_FILE_TYPE_MASK == ZIP_SYMBOLIC_LINK_TYPE {
            return Err(ZipExtractionError::UnsupportedSymbolicLink(reported_entry));
        }

        let target = resolve_entry_path(&root, &entry_name)?;
        let is_directory = is_directory_entry(&entry_name, mode, creator_os, external_attributes);
        let permissions = (if mode == 0 {
            if is_directory { 0o755 } else { 0o644 }
        } else {
            mode
        }) & 0o777;
        let destination_directory = if is_directory {
            target.clone()
        } else {
            target
                .parent()
                .ok_or_else(|| ZipExtractionError::OutOfBoundsEntry {
                    destination: format_zip_path(&target.to_string_lossy()),
                    entry: reported_entry.clone(),
                })?
                .to_owned()
        };

        ensure_directory_within_root(
            &root,
            &destination_directory,
            is_directory.then_some(permissions),
        )?;
        check_cancelled(cancellation)?;
        if is_directory {
            continue;
        }

        reject_existing_symbolic_link(&target)?;
        check_cancelled(cancellation)?;
        let mut output =
            open_output_file(&target, permissions).map_err(ZipExtractionError::Destination)?;
        copy_entry_contents(&mut entry, &mut output, cancellation)?;
        output.flush().map_err(ZipExtractionError::Destination)?;
    }

    check_cancelled(cancellation)
}

fn read_entry_attributes<R: Read + Seek>(
    reader: &mut R,
    central_header_start: u64,
) -> io::Result<(u32, u8, u32)> {
    let mut signature = [0_u8; 4];
    reader.seek(SeekFrom::Start(central_header_start))?;
    reader.read_exact(&mut signature)?;
    if signature != *b"PK\x01\x02" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ZIP central-directory entry is malformed",
        ));
    }

    let mut version_made_by = [0_u8; 2];
    reader.seek(SeekFrom::Start(
        central_header_start + ZIP_CENTRAL_VERSION_MADE_BY_OFFSET,
    ))?;
    reader.read_exact(&mut version_made_by)?;

    let mut attributes = [0_u8; 4];
    reader.seek(SeekFrom::Start(
        central_header_start + ZIP_CENTRAL_EXTERNAL_ATTRIBUTES_OFFSET,
    ))?;
    reader.read_exact(&mut attributes)?;
    let external_attributes = u32::from_le_bytes(attributes);

    Ok((
        (external_attributes >> 16) & 0xffff,
        version_made_by[1],
        external_attributes,
    ))
}

fn is_directory_entry(name: &str, mode: u32, creator_os: u8, external_attributes: u32) -> bool {
    mode & ZIP_FILE_TYPE_MASK == ZIP_DIRECTORY_TYPE
        || name.ends_with('/')
        || (creator_os == 0 && external_attributes == ZIP_DOS_DIRECTORY_ATTRIBUTE)
}

fn resolve_entry_path(root: &Path, name: &str) -> Result<PathBuf, ZipExtractionError> {
    let entry_path = Path::new(name);
    let unresolved = if entry_path.is_absolute() {
        entry_path.to_owned()
    } else {
        root.join(entry_path)
    };
    let destination = normalize_path(&unresolved);
    if !destination.starts_with(root) {
        return Err(ZipExtractionError::OutOfBoundsEntry {
            destination: format_zip_path(&destination.to_string_lossy()),
            entry: format_zip_path(name),
        });
    }
    Ok(destination)
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                // Absolute paths cannot move above their filesystem root.
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn ensure_directory_within_root(
    root: &Path,
    destination: &Path,
    mode: Option<u32>,
) -> Result<(), ZipExtractionError> {
    if !destination.starts_with(root) {
        return Err(ZipExtractionError::OutOfBoundsDestination {
            destination: format_zip_path(&destination.to_string_lossy()),
        });
    }
    let relative =
        destination
            .strip_prefix(root)
            .map_err(|_| ZipExtractionError::OutOfBoundsDestination {
                destination: format_zip_path(&destination.to_string_lossy()),
            })?;
    let segments = relative.components().collect::<Vec<_>>();
    let mut current = root.to_owned();

    for (index, component) in segments.iter().enumerate() {
        let Component::Normal(segment) = component else {
            return Err(ZipExtractionError::OutOfBoundsDestination {
                destination: format_zip_path(&destination.to_string_lossy()),
            });
        };
        current.push(segment);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(ZipExtractionError::ExistingNonDirectory(format_zip_path(
                    &current.to_string_lossy(),
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let create_result = create_directory(
                    &current,
                    (index + 1 == segments.len()).then_some(mode).flatten(),
                );
                match create_result {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        let metadata = fs::symlink_metadata(&current)
                            .map_err(ZipExtractionError::Destination)?;
                        if metadata.file_type().is_symlink() || !metadata.is_dir() {
                            return Err(ZipExtractionError::ExistingNonDirectory(format_zip_path(
                                &current.to_string_lossy(),
                            )));
                        }
                    }
                    Err(error) => return Err(ZipExtractionError::Destination(error)),
                }
            }
            Err(error) => return Err(ZipExtractionError::Destination(error)),
        }
    }

    let canonical = destination
        .canonicalize()
        .map_err(ZipExtractionError::Destination)?;
    if !canonical.starts_with(root) {
        return Err(ZipExtractionError::OutOfBoundsDestination {
            destination: format_zip_path(&canonical.to_string_lossy()),
        });
    }
    Ok(())
}

fn create_directory(path: &Path, mode: Option<u32>) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        if let Some(mode) = mode {
            builder.mode(mode);
        }
    }
    #[cfg(not(unix))]
    let _ = mode;
    builder.create(path)
}

fn reject_existing_symbolic_link(destination: &Path) -> Result<(), ZipExtractionError> {
    match fs::symlink_metadata(destination) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(ZipExtractionError::ExistingSymbolicLink(format_zip_path(
                &destination.to_string_lossy(),
            )))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ZipExtractionError::Destination(error)),
    }
}

fn open_output_file(destination: &Path, mode: u32) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(mode);
        // Close the symlink race between lstat and open where the platform
        // supports a no-follow open flag.
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(destination)
}

fn copy_entry_contents<R: Read, W: Write>(
    entry: &mut R,
    output: &mut W,
    cancellation: Option<&CancellationToken>,
) -> Result<(), ZipExtractionError> {
    let mut buffer = [0_u8; COPY_BUFFER_SIZE];
    loop {
        check_cancelled(cancellation)?;
        let bytes_read = entry
            .read(&mut buffer)
            .map_err(ZipExtractionError::EntryRead)?;
        if bytes_read == 0 {
            break;
        }
        check_cancelled(cancellation)?;
        output
            .write_all(&buffer[..bytes_read])
            .map_err(ZipExtractionError::Destination)?;
    }
    Ok(())
}

struct CancellationAwareFile {
    inner: File,
    cancellation: Option<CancellationToken>,
}

impl Read for CancellationAwareFile {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "ZIP archive extraction cancelled",
            ));
        }
        self.inner.read(buffer)
    }
}

impl Seek for CancellationAwareFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "ZIP archive extraction cancelled",
            ));
        }
        self.inner.seek(position)
    }
}

fn check_cancelled(cancellation: Option<&CancellationToken>) -> Result<(), ZipExtractionError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        Err(ZipExtractionError::Cancelled)
    } else {
        Ok(())
    }
}

fn format_zip_path(value: &str) -> String {
    let sanitized = strip_ansi_and_control(value);
    if sanitized.encode_utf16().count() <= MAX_REPORTED_ZIP_PATH_LENGTH {
        return sanitized;
    }
    let prefix = take_utf16_prefix(&sanitized, MAX_REPORTED_ZIP_PATH_LENGTH - 3);
    format!("{prefix}...")
}

// This mirrors Node's stripVTControlCharacters followed by removal of all
// remaining C0, DEL, and C1 controls, as used by stripAnsiAndControl.
fn strip_ansi_and_control(text: &str) -> String {
    let characters: Vec<char> = text.chars().collect();
    let mut sanitized = String::with_capacity(text.len());
    let mut index = 0;
    while index < characters.len() {
        let character = characters[index];
        let code = character as u32;

        if character == '\u{1b}' {
            if let Some(next) = characters.get(index + 1).copied() {
                match next {
                    '[' => {
                        index = skip_csi(&characters, index + 2);
                        continue;
                    }
                    ']' => {
                        if let Some(end) = skip_control_string(&characters, index + 2, true) {
                            index = end;
                            continue;
                        }
                    }
                    'P' | 'X' | '^' | '_' => {
                        if let Some(end) = skip_control_string(&characters, index + 2, false) {
                            index = end;
                            continue;
                        }
                    }
                    '\\' | '\u{30}'..='\u{3f}' | '\u{40}'..='\u{5f}' | '\u{60}'..='\u{7e}' => {
                        index += 2;
                        continue;
                    }
                    _ => {}
                }
            }
            index += 1;
            continue;
        }

        match code {
            0x009b => {
                index = skip_csi(&characters, index + 1);
                continue;
            }
            0x009d => {
                if let Some(end) = skip_control_string(&characters, index + 1, true) {
                    index = end;
                    continue;
                }
            }
            0x0090 | 0x0098 | 0x009e | 0x009f => {
                if let Some(end) = skip_control_string(&characters, index + 1, false) {
                    index = end;
                    continue;
                }
            }
            0x0000..=0x001f | 0x007f..=0x009f => {
                index += 1;
                continue;
            }
            _ => {}
        }

        sanitized.push(character);
        index += 1;
    }
    sanitized
}

fn skip_csi(characters: &[char], mut index: usize) -> usize {
    while let Some(character) = characters.get(index).copied() {
        index += 1;
        if ('@'..='~').contains(&character) {
            break;
        }
    }
    index
}

fn skip_control_string(characters: &[char], mut index: usize, allow_bel: bool) -> Option<usize> {
    while let Some(character) = characters.get(index).copied() {
        if allow_bel && character == '\u{7}' {
            return Some(index + 1);
        }
        if character == '\u{1b}' && characters.get(index + 1) == Some(&'\\') {
            return Some(index + 2);
        }
        if character == '\u{9c}' {
            return Some(index + 1);
        }
        index += 1;
    }
    None
}

// JS String.slice() counts UTF-16 code units. Rust cannot represent a lone
// surrogate, so replace a surrogate split at the limit with U+FFFD.
fn take_utf16_prefix(text: &str, max_units: usize) -> String {
    let mut used_units = 0;
    let mut prefix = String::new();
    for character in text.chars() {
        let character_units = character.len_utf16();
        if used_units + character_units > max_units {
            if used_units < max_units {
                prefix.push('\u{fffd}');
            }
            break;
        }
        prefix.push(character);
        used_units += character_units;
    }
    prefix
}

#[cfg(test)]
mod tests {
    use super::{ZipExtractionError, extract_zip_archive, format_zip_path, resolve_entry_path};
    use crate::utils::cancellation::CancellationToken;
    use std::fs;
    use std::io::{Cursor, Write};
    use std::path::{Path, PathBuf};
    use uuid::Uuid;
    use zip::write::{SimpleFileOptions, ZipWriter};
    use zip::{CompressionMethod, System};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("canopy-zip-extraction-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_zip(path: &Path, entries: &[(&str, &[u8])]) {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        for (name, contents) in entries {
            writer
                .start_file(
                    *name,
                    SimpleFileOptions::default().compression_method(CompressionMethod::Deflated),
                )
                .unwrap();
            writer.write_all(contents).unwrap();
        }
        let bytes = writer.finish().unwrap().into_inner();
        fs::write(path, bytes).unwrap();
    }

    fn write_symlink_zip(path: &Path, entry_name: &str) {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        writer
            .start_file(
                entry_name,
                SimpleFileOptions::default()
                    .compression_method(CompressionMethod::Stored)
                    .system(System::Unix),
            )
            .unwrap();
        writer.write_all(b"target").unwrap();
        let mut bytes = writer.finish().unwrap().into_inner();
        let central_header = bytes
            .windows(4)
            .position(|window| window == b"PK\x01\x02")
            .expect("zip writer produced a central directory record");
        let mode = 0o120777_u32 << 16;
        bytes[central_header + 38..central_header + 42].copy_from_slice(&mode.to_le_bytes());
        fs::write(path, bytes).unwrap();
    }

    fn write_dos_directory_zip(path: &Path, entry_name: &str) {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        writer
            .start_file(entry_name, SimpleFileOptions::default())
            .unwrap();
        writer.write_all(b"ignored directory payload").unwrap();
        let mut bytes = writer.finish().unwrap().into_inner();
        let central_header = bytes
            .windows(4)
            .position(|window| window == b"PK\x01\x02")
            .expect("zip writer produced a central directory record");
        // version made by = 0 (DOS), external attributes = 16 (directory).
        bytes[central_header + 4] = 20;
        bytes[central_header + 5] = 0;
        bytes[central_header + 38..central_header + 42].copy_from_slice(&16_u32.to_le_bytes());
        fs::write(path, bytes).unwrap();
    }

    fn write_default_modes_zip(path: &Path) {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        writer
            .start_file("file.txt", SimpleFileOptions::default())
            .unwrap();
        writer.write_all(b"file").unwrap();
        writer
            .add_directory("directory/", SimpleFileOptions::default())
            .unwrap();
        let mut bytes = writer.finish().unwrap().into_inner();
        let central_signature = b"PK\x01\x02";
        for index in 0..bytes.len().saturating_sub(45) {
            if &bytes[index..index + 4] == central_signature {
                bytes[index + 38..index + 42].fill(0);
            }
        }
        fs::write(path, bytes).unwrap();
    }

    #[cfg(unix)]
    fn write_mode_zip(path: &Path, mode: u32) {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        writer
            .start_file(
                "executable.sh",
                SimpleFileOptions::default().system(System::Unix),
            )
            .unwrap();
        writer.write_all(b"#!/bin/sh\n").unwrap();
        let mut bytes = writer.finish().unwrap().into_inner();
        let central_header = bytes
            .windows(4)
            .position(|window| window == b"PK\x01\x02")
            .expect("zip writer produced a central directory record");
        bytes[central_header + 38..central_header + 42]
            .copy_from_slice(&(mode << 16).to_le_bytes());
        fs::write(path, bytes).unwrap();
    }

    #[tokio::test]
    async fn extracts_normal_nested_entries() {
        let temp = TempDir::new();
        let archive = temp.path().join("extension.zip");
        let destination = temp.path().join("installed");
        write_zip(&archive, &[("nested/path/file.txt", b"extension payload")]);

        extract_zip_archive(&archive, &destination, None)
            .await
            .unwrap();

        assert_eq!(
            fs::read(destination.join("nested/path/file.txt")).unwrap(),
            b"extension payload"
        );
    }

    #[tokio::test]
    async fn rejects_traversal_entries() {
        let temp = TempDir::new();
        let archive = temp.path().join("traversal.zip");
        let destination = temp.path().join("installed");
        write_zip(&archive, &[("../escaped.txt", b"outside")]);

        let error = extract_zip_archive(&archive, &destination, None)
            .await
            .unwrap_err();

        assert!(matches!(error, ZipExtractionError::OutOfBoundsEntry { .. }));
        assert!(!temp.path().join("escaped.txt").exists());
    }

    #[tokio::test]
    async fn rejects_unix_symbolic_link_entries() {
        let temp = TempDir::new();
        let archive = temp.path().join("symlink.zip");
        let destination = temp.path().join("installed");
        write_symlink_zip(&archive, "link");

        let error = extract_zip_archive(&archive, &destination, None)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ZipExtractionError::UnsupportedSymbolicLink(ref entry) if entry == "link"
        ));
        assert!(!destination.join("link").exists());
    }

    #[tokio::test]
    async fn applies_default_permissions_and_legacy_dos_directory_detection() {
        #[cfg(unix)]
        use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

        let temp = TempDir::new();
        let archive = temp.path().join("defaults.zip");
        let destination = temp.path().join("installed");
        write_default_modes_zip(&archive);

        extract_zip_archive(&archive, &destination, None)
            .await
            .unwrap();

        assert!(destination.join("directory").is_dir());
        #[cfg(unix)]
        {
            let expected_file = temp.path().join("expected-file");
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true).mode(0o644);
            options.open(&expected_file).unwrap();

            let expected_directory = temp.path().join("expected-directory");
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o755);
            builder.create(&expected_directory).unwrap();

            assert_eq!(
                fs::metadata(destination.join("file.txt"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                fs::metadata(expected_file).unwrap().permissions().mode() & 0o777
            );
            assert_eq!(
                fs::metadata(destination.join("directory"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                fs::metadata(expected_directory)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777
            );
        }

        let dos_archive = temp.path().join("dos-directory.zip");
        let dos_destination = temp.path().join("dos-installed");
        write_dos_directory_zip(&dos_archive, "legacy-directory");
        extract_zip_archive(&dos_archive, &dos_destination, None)
            .await
            .unwrap();
        assert!(dos_destination.join("legacy-directory").is_dir());

        #[cfg(unix)]
        {
            let mode_archive = temp.path().join("mode.zip");
            let mode_destination = temp.path().join("mode-installed");
            let expected_file = temp.path().join("expected-executable");
            write_mode_zip(&mode_archive, 0o104755);
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true).mode(0o755);
            options.open(&expected_file).unwrap();

            extract_zip_archive(&mode_archive, &mode_destination, None)
                .await
                .unwrap();

            assert_eq!(
                fs::metadata(mode_destination.join("executable.sh"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                fs::metadata(expected_file).unwrap().permissions().mode() & 0o777
            );
        }
    }

    #[tokio::test]
    async fn skips_macos_metadata_before_other_validation() {
        let temp = TempDir::new();
        let archive = temp.path().join("metadata.zip");
        let destination = temp.path().join("installed");
        write_zip(
            &archive,
            &[
                ("__MACOSX/../escaped.txt", b"skip traversal"),
                ("__MACOSX/link", b"skip link"),
                ("extension/file.txt", b"keep"),
            ],
        );

        extract_zip_archive(&archive, &destination, None)
            .await
            .unwrap();

        assert_eq!(
            fs::read(destination.join("extension/file.txt")).unwrap(),
            b"keep"
        );
        assert!(!temp.path().join("escaped.txt").exists());
        assert!(!destination.join("__MACOSX").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_preexisting_symlink_parent_components() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new();
        let archive = temp.path().join("parent-link.zip");
        let destination = temp.path().join("installed");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&outside).unwrap();
        write_zip(&archive, &[("linked/escaped.txt", b"outside")]);
        fs::create_dir_all(&destination).unwrap();
        symlink(&outside, destination.join("linked")).unwrap();

        let error = extract_zip_archive(&archive, &destination, None)
            .await
            .unwrap_err();

        assert!(matches!(error, ZipExtractionError::ExistingNonDirectory(_)));
        assert!(!outside.join("escaped.txt").exists());
    }

    #[tokio::test]
    async fn requires_an_absolute_destination() {
        let error = extract_zip_archive("missing.zip", "relative", None)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ZipExtractionError::TargetDirectoryNotAbsolute
        ));
    }

    #[tokio::test]
    async fn observes_cancellation_before_opening_the_archive() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error =
            extract_zip_archive("missing.zip", "/tmp/canopy-zip-cancel", Some(cancellation))
                .await
                .unwrap_err();
        assert!(matches!(error, ZipExtractionError::Cancelled));
    }

    #[test]
    fn diagnostics_strip_terminal_controls_and_truncate_by_utf16_units() {
        assert_eq!(
            format_zip_path("dir/\u{1b}[31mred\u{1b}[0m\u{7f}.txt"),
            "dir/red.txt"
        );

        let long_path = format!("{}x", "a".repeat(200));
        let formatted = format_zip_path(&long_path);
        assert_eq!(formatted, format!("{}...", "a".repeat(197)));
        assert_eq!(formatted.encode_utf16().count(), 200);

        let split_surrogate = format!("{}😀tail", "a".repeat(196));
        let formatted = format_zip_path(&split_surrogate);
        assert_eq!(formatted, format!("{}�...", "a".repeat(196)));
        assert_eq!(formatted.encode_utf16().count(), 200);
    }

    #[test]
    fn normalizes_paths_like_path_resolve_before_root_check() {
        let root = Path::new("/tmp/canopy-root");
        assert_eq!(
            resolve_entry_path(root, "nested/../file.txt").unwrap(),
            root.join("file.txt")
        );
        assert!(matches!(
            resolve_entry_path(root, "../../outside.txt"),
            Err(ZipExtractionError::OutOfBoundsEntry { .. })
        ));
    }
}
