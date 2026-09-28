//! Streaming validation for extension tar archives.
//!
//! Tar entries are inspected without extracting them. Symbolic and hard links
//! are rejected because they can escape the extraction root or overwrite
//! unrelated files when a package is installed.

use flate2::read::MultiGzDecoder;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tar::{Archive, EntryType};
use thiserror::Error;

const MAX_REPORTED_ENTRY_PATH_LENGTH: usize = 200;
const MAX_REPORTED_LINK_ENTRIES: usize = 10;
const MAX_LINK_ENTRIES: usize = 100;

/// Cooperative cancellation handle for a tar scan.
///
/// Cancellation is checked before scanning and on every read performed by the
/// tar decoder, including while it skips file payloads between entries.
#[derive(Clone, Debug, Default)]
pub struct ArchiveScanCancellation(Arc<AtomicBool>);

impl ArchiveScanCancellation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Failure while validating a tar archive.
#[derive(Debug, Error)]
pub enum ArchiveSafetyError {
    #[error("Tar archive scan cancelled")]
    Cancelled,
    #[error("Unable to open tar archive: {0}")]
    Io(#[source] io::Error),
    #[error("Malformed tar archive: {0}")]
    MalformedArchive(String),
    #[error("Tar archive contains unsupported link entry: {paths}")]
    UnsupportedLinks { paths: String },
    #[error("Tar archive contains {count} unsupported link entries: {paths}")]
    UnsupportedLinkCount { count: usize, paths: String },
    #[error("Tar archive contains more than 100 unsupported link entries: {paths}")]
    TooManyLinks { paths: String },
    #[error("Tar archive scan worker failed: {0}")]
    Worker(#[source] tokio::task::JoinError),
}

/// Verify that a plain or gzip-compressed tar archive contains no links.
///
/// The archive is streamed on Tokio's blocking pool so large local archives do
/// not block an async executor thread. Only the first ten sanitized link paths
/// are retained for the error message. More than 100 links uses the same
/// bounded report and the dedicated `more than 100` error used by the Node
/// implementation.
pub async fn assert_tar_archive_has_no_links(
    file: impl AsRef<Path>,
    cancellation: Option<ArchiveScanCancellation>,
) -> Result<(), ArchiveSafetyError> {
    if cancellation
        .as_ref()
        .is_some_and(ArchiveScanCancellation::is_cancelled)
    {
        return Err(ArchiveSafetyError::Cancelled);
    }

    let file = file.as_ref().to_owned();
    let worker_cancellation = cancellation.clone();
    let result =
        tokio::task::spawn_blocking(move || scan_file(&file, worker_cancellation.as_ref()))
            .await
            .map_err(ArchiveSafetyError::Worker)?;

    // Match AbortSignal.throwIfAborted() after stream completion and when an
    // I/O error races with cancellation.
    if cancellation
        .as_ref()
        .is_some_and(ArchiveScanCancellation::is_cancelled)
    {
        return Err(ArchiveSafetyError::Cancelled);
    }
    result
}

fn scan_file(
    file: &Path,
    cancellation: Option<&ArchiveScanCancellation>,
) -> Result<(), ArchiveSafetyError> {
    if cancellation.is_some_and(ArchiveScanCancellation::is_cancelled) {
        return Err(ArchiveSafetyError::Cancelled);
    }

    let file = File::open(file).map_err(ArchiveSafetyError::Io)?;
    let mut buffered = BufReader::new(file);
    let is_gzip = buffered
        .fill_buf()
        .map_err(ArchiveSafetyError::Io)?
        .starts_with(&[0x1f, 0x8b]);

    let reader: Box<dyn Read + Send> = if is_gzip {
        Box::new(MultiGzDecoder::new(buffered))
    } else {
        Box::new(buffered)
    };
    scan_reader(reader, cancellation)
}

fn scan_reader<R: Read>(
    reader: R,
    cancellation: Option<&ArchiveScanCancellation>,
) -> Result<(), ArchiveSafetyError> {
    let reader = CancellationReader {
        inner: reader,
        cancellation,
    };
    let mut archive = Archive::new(reader);
    let mut paths = Vec::with_capacity(MAX_REPORTED_LINK_ENTRIES);
    let mut link_count = 0_usize;

    let entries = archive
        .entries()
        .map_err(|error| map_archive_error(error, cancellation))?;
    for entry in entries {
        if cancellation.is_some_and(ArchiveScanCancellation::is_cancelled) {
            return Err(ArchiveSafetyError::Cancelled);
        }
        let entry = entry.map_err(|error| map_archive_error(error, cancellation))?;
        let entry_type = entry.header().entry_type();
        if entry_type == EntryType::Symlink || entry_type == EntryType::Link {
            link_count = link_count.saturating_add(1);
            if paths.len() < MAX_REPORTED_LINK_ENTRIES {
                let path = String::from_utf8_lossy(entry.path_bytes().as_ref()).into_owned();
                let path = format_entry_path(&path);
                paths.push(if path.is_empty() {
                    "<sanitized empty path>".to_owned()
                } else {
                    path
                });
            }
        }
    }

    // Tar readers stop at the archive terminator, which may be before the end
    // of a gzip member. Drain the compressed stream so CRC/trailer failures
    // remain malformed-archive errors and cancellation still covers all I/O.
    let mut reader = archive.into_inner();
    io::copy(&mut reader, &mut io::sink())
        .map_err(|error| map_archive_error(error, cancellation))?;

    if cancellation.is_some_and(ArchiveScanCancellation::is_cancelled) {
        return Err(ArchiveSafetyError::Cancelled);
    }
    if link_count > MAX_LINK_ENTRIES {
        return Err(ArchiveSafetyError::TooManyLinks {
            paths: paths.join(", "),
        });
    }
    if link_count > 0 {
        let paths = paths.join(", ");
        if link_count == 1 {
            return Err(ArchiveSafetyError::UnsupportedLinks { paths });
        }
        return Err(ArchiveSafetyError::UnsupportedLinkCount {
            count: link_count,
            paths,
        });
    }
    Ok(())
}

fn map_archive_error(
    error: io::Error,
    cancellation: Option<&ArchiveScanCancellation>,
) -> ArchiveSafetyError {
    if cancellation.is_some_and(ArchiveScanCancellation::is_cancelled) {
        ArchiveSafetyError::Cancelled
    } else {
        ArchiveSafetyError::MalformedArchive(error.to_string())
    }
}

fn format_entry_path(entry_path: &str) -> String {
    let sanitized = strip_ansi_and_control(entry_path);
    if sanitized.encode_utf16().count() <= MAX_REPORTED_ENTRY_PATH_LENGTH {
        return sanitized;
    }

    // JS String.slice counts UTF-16 code units. Rust strings cannot contain
    // lone surrogates, so if the slice boundary splits an astral character,
    // use U+FFFD, which is what UTF-8 serialization of that lone surrogate
    // produces. The diagnostic stays valid UTF-8 while preserving JS's limit.
    let prefix = take_utf16_prefix(&sanitized, MAX_REPORTED_ENTRY_PATH_LENGTH - 3);
    format!("{prefix}...")
}

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

/// Match `stripAnsiAndControl`: remove ANSI sequences, then all residual C0,
/// DEL, and C1 control characters. `terminal_safe` intentionally replaces
/// some sequences with spaces, so this path sanitizer removes them directly.
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
                        // Two-byte ESC sequences (including Fe/Fs/Fp escape
                        // forms) contain no printable path content.
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
        if (allow_bel && character == '\u{7}') || character == '\u{009c}' {
            return Some(index + 1);
        }
        if character == '\u{1b}' && characters.get(index + 1) == Some(&'\\') {
            return Some(index + 2);
        }
        index += 1;
    }
    None
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
                "tar archive scan cancelled",
            ));
        }
        self.inner.read(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ArchiveSafetyError, ArchiveScanCancellation, assert_tar_archive_has_no_links,
        format_entry_path, scan_reader,
    };
    use flate2::{Compression, write::GzEncoder};
    use std::fs;
    use std::io::{self, Cursor, Write};
    use std::path::PathBuf;
    use tar::{Builder, EntryType, Header};

    struct TempArchive(PathBuf);

    impl TempArchive {
        fn new(bytes: &[u8]) -> Self {
            let path = std::env::temp_dir().join(format!(
                "canopy-archive-safety-{}.tar",
                uuid::Uuid::new_v4()
            ));
            fs::write(&path, bytes).expect("write archive fixture");
            Self(path)
        }
    }

    impl Drop for TempArchive {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn tar_bytes(entries: &[(String, EntryType)]) -> Vec<u8> {
        let mut builder = Builder::new(Vec::new());
        for (path, entry_type) in entries {
            let mut header = Header::new_gnu();
            header.set_entry_type(*entry_type);
            header.set_size(0);
            header.set_mode(0o644);
            header.set_path(path).expect("valid fixture path");
            if *entry_type == EntryType::Symlink || *entry_type == EntryType::Link {
                header
                    .set_link_name("target")
                    .expect("valid fixture link target");
            }
            header.set_cksum();
            builder
                .append(&header, io::empty())
                .expect("append fixture entry");
        }
        builder.into_inner().expect("finish tar fixture")
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(bytes).expect("write gzip fixture");
        encoder.finish().expect("finish gzip fixture")
    }

    #[tokio::test]
    async fn allows_regular_files_in_plain_and_gzip_archives() {
        for bytes in [
            tar_bytes(&[("normal/file.txt".to_owned(), EntryType::Regular)]),
            gzip(&tar_bytes(&[(
                "normal/file.txt".to_owned(),
                EntryType::Regular,
            )])),
        ] {
            let archive = TempArchive::new(&bytes);
            assert_tar_archive_has_no_links(&archive.0, None)
                .await
                .expect("regular archive is safe");
        }
    }

    #[tokio::test]
    async fn rejects_symbolic_and_hard_links_with_a_capped_path_list() {
        let entries = vec![
            ("symbolic".to_owned(), EntryType::Symlink),
            ("hard".to_owned(), EntryType::Link),
        ];
        let archive = TempArchive::new(&gzip(&tar_bytes(&entries)));
        let error = assert_tar_archive_has_no_links(&archive.0, None)
            .await
            .expect_err("links must fail");
        assert_eq!(
            error.to_string(),
            "Tar archive contains 2 unsupported link entries: symbolic, hard"
        );

        let entries: Vec<_> = (0..101)
            .map(|index| (format!("link-{index}"), EntryType::Symlink))
            .collect();
        let archive = TempArchive::new(&tar_bytes(&entries));
        let error = assert_tar_archive_has_no_links(&archive.0, None)
            .await
            .expect_err("more than 100 links must fail");
        assert_eq!(
            error.to_string(),
            format!(
                "Tar archive contains more than 100 unsupported link entries: {}",
                (0..10)
                    .map(|index| format!("link-{index}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        );
    }

    #[tokio::test]
    async fn exactly_one_hundred_links_uses_the_regular_link_error() {
        let entries: Vec<_> = (0..100)
            .map(|index| (format!("link-{index}"), EntryType::Symlink))
            .collect();
        let archive = TempArchive::new(&tar_bytes(&entries));
        let error = assert_tar_archive_has_no_links(&archive.0, None)
            .await
            .expect_err("links are unsupported even below the upper limit");
        assert_eq!(
            error.to_string(),
            format!(
                "Tar archive contains 100 unsupported link entries: {}",
                (0..10)
                    .map(|index| format!("link-{index}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        );
    }

    #[tokio::test]
    async fn returns_malformed_archive_errors() {
        let archive = TempArchive::new(b"not a tar archive");
        let error = assert_tar_archive_has_no_links(&archive.0, None)
            .await
            .expect_err("invalid tar must fail");
        assert!(matches!(error, ArchiveSafetyError::MalformedArchive(_)));

        let mut corrupt_gzip = gzip(&tar_bytes(&[("file".to_owned(), EntryType::Regular)]));
        let last_byte = corrupt_gzip.last_mut().expect("gzip trailer exists");
        *last_byte ^= 0xff;
        let archive = TempArchive::new(&corrupt_gzip);
        let error = assert_tar_archive_has_no_links(&archive.0, None)
            .await
            .expect_err("invalid gzip trailer must fail");
        assert!(matches!(error, ArchiveSafetyError::MalformedArchive(_)));
    }

    #[tokio::test]
    async fn cancellation_before_scan_and_while_reading_is_reported() {
        let cancellation = ArchiveScanCancellation::new();
        cancellation.cancel();
        let archive = TempArchive::new(&tar_bytes(&[("file".to_owned(), EntryType::Regular)]));
        assert!(matches!(
            assert_tar_archive_has_no_links(&archive.0, Some(cancellation))
                .await
                .expect_err("pre-cancelled scan must fail"),
            ArchiveSafetyError::Cancelled
        ));

        let cancellation = ArchiveScanCancellation::new();
        let mut reader = CancelAfterFirstRead {
            inner: Cursor::new(tar_bytes(&[("file".to_owned(), EntryType::Regular)])),
            cancellation: cancellation.clone(),
            reads: 0,
        };
        let error = scan_reader(&mut reader, Some(&cancellation))
            .expect_err("cancellation during archive read must fail");
        assert!(matches!(error, ArchiveSafetyError::Cancelled));
    }

    struct CancelAfterFirstRead {
        inner: Cursor<Vec<u8>>,
        cancellation: ArchiveScanCancellation,
        reads: usize,
    }

    impl Read for CancelAfterFirstRead {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.reads += 1;
            let read = self.inner.read(buffer)?;
            if self.reads >= 1 {
                self.cancellation.cancel();
            }
            Ok(read)
        }
    }

    #[test]
    fn sanitizes_controls_and_truncates_paths_to_200_characters() {
        assert_eq!(
            format_entry_path("\u{1b}[31mevil\u{7f}\0\u{009b}31m\u{009d}url\u{0007}"),
            "evil"
        );
        assert_eq!(
            format_entry_path("\u{1b}]8;;https://example.test\u{1b}\\click\u{1b}]8;;\u{1b}\\"),
            "click"
        );
        assert_eq!(
            format_entry_path("\u{1b}Psecret\u{1b}\\visible\u{0090}private\u{009c}ok"),
            "visibleok"
        );
        assert_eq!(format_entry_path(&"x".repeat(200)), "x".repeat(200));
        assert_eq!(
            format_entry_path(&"x".repeat(201)),
            format!("{}...", "x".repeat(197))
        );
        assert_eq!(
            format_entry_path(&format!("{}😀{}", "x".repeat(196), "y".repeat(3))),
            format!("{}�...", "x".repeat(196))
        );
    }
}
