//! JSON Lines transcript and metadata storage.
//!
//! This ports the behavior of `packages/core/src/utils/jsonl-utils.ts`:
//! object-only parsing, recovery of adjacent JSON objects after an interrupted
//! append, bounded line reads, serialized concurrent appends, and atomic full
//! file replacement. Appends are synced before returning so an acknowledged
//! record is durable across process termination.

use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// Hard cap for one serialized or decoded JSONL record. This prevents a
/// malformed file or a single oversized tool result from forcing an unbounded
/// temporary allocation in the Rust runtime.
pub const MAX_JSONL_RECORD_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JsonlParseDiagnostic {
    NonObjectValue,
    MalformedLine,
    RecoveredObjects { count: usize },
}

#[derive(Clone, Debug, PartialEq)]
pub struct ParsedJsonlLine {
    pub records: Vec<Value>,
    pub diagnostic: Option<JsonlParseDiagnostic>,
}

/// Recover top-level JSON objects from a physical line, including records
/// joined together without a newline. Braces inside quoted strings and escaped
/// quotes are ignored. Invalid fragments are skipped while later objects are
/// still considered.
pub fn recover_objects_from_line(line: &str) -> Vec<Value> {
    let bytes = line.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escape = false;
    let mut start = None;

    for (index, byte) in bytes.iter().copied().enumerate() {
        if escape {
            escape = false;
            continue;
        }
        if in_string {
            if byte == b'\\' {
                escape = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        if byte == b'"' {
            in_string = true;
            continue;
        }
        if byte == b'{' {
            if depth == 0 {
                start = Some(index);
            }
            depth += 1;
        } else if byte == b'}' {
            if depth == 0 {
                // Match the source parser's recovery behavior: discard an
                // unbalanced close and continue looking for a later object.
                start = None;
                continue;
            }
            depth -= 1;
            if depth == 0 {
                if let Some(begin) = start.take() {
                    if let Ok(value) = serde_json::from_slice::<Value>(&bytes[begin..=index]) {
                        if value.is_object() {
                            out.push(value);
                        }
                    }
                }
            }
        }
    }
    out
}

/// Parse a JSONL line tolerantly. Only JSON objects are transcript records;
/// scalar and array values are discarded so callers can safely inspect fields.
pub fn parse_line_with_diagnostic(line: &str) -> ParsedJsonlLine {
    match serde_json::from_str::<Value>(line) {
        Ok(value) if value.is_object() => ParsedJsonlLine {
            records: vec![value],
            diagnostic: None,
        },
        Ok(_) => ParsedJsonlLine {
            records: Vec::new(),
            diagnostic: Some(JsonlParseDiagnostic::NonObjectValue),
        },
        Err(_) => {
            let records = recover_objects_from_line(line);
            let diagnostic = if records.is_empty() {
                JsonlParseDiagnostic::MalformedLine
            } else {
                JsonlParseDiagnostic::RecoveredObjects {
                    count: records.len(),
                }
            };
            ParsedJsonlLine {
                records,
                diagnostic: Some(diagnostic),
            }
        }
    }
}

/// Parse a JSONL line using the current Canopy recovery rules.
pub fn parse_line_tolerant(line: &str) -> Vec<Value> {
    parse_line_with_diagnostic(line).records
}

/// Read up to `count` object records. Missing files produce an empty result;
/// other I/O errors are returned to the caller.
pub fn read_lines(path: impl AsRef<Path>, count: usize) -> io::Result<Vec<Value>> {
    let file = match File::open(path.as_ref()) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut reader = BufReader::new(file);
    read_from(&mut reader, Some(count))
}

/// Read all object records. Missing files produce an empty result; other
/// errors are returned so user-facing callers can report corruption or access
/// failures rather than silently treating them as an empty session.
pub fn read(path: impl AsRef<Path>) -> io::Result<Vec<Value>> {
    let file = match File::open(path.as_ref()) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut reader = BufReader::new(file);
    read_from(&mut reader, None)
}

/// Read all object records from an already-open buffered file. Callers can use
/// this when they need to open with platform-specific protections such as
/// `O_NOFOLLOW` before passing the handle here.
pub fn read_all_from<R: BufRead>(reader: &mut R) -> io::Result<Vec<Value>> {
    read_from(reader, None)
}

fn read_from<R: BufRead>(reader: &mut R, limit: Option<usize>) -> io::Result<Vec<Value>> {
    let mut records = Vec::new();
    let mut bytes = Vec::new();
    loop {
        if limit.is_some_and(|max| records.len() >= max) {
            break;
        }
        bytes.clear();
        let Some(line) = read_bounded_line(reader, &mut bytes)? else {
            break;
        };
        bytes = line;
        if bytes.last() == Some(&b'\n') {
            bytes.pop();
        }
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
        let line = String::from_utf8_lossy(&bytes);
        if line.trim().is_empty() {
            continue;
        }
        let parsed = parse_line_tolerant(line.trim());
        for value in parsed {
            if limit.is_some_and(|max| records.len() >= max) {
                break;
            }
            records.push(value);
        }
    }
    Ok(records)
}

fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    line: &mut Vec<u8>,
) -> io::Result<Option<Vec<u8>>> {
    let mut started = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(started.then(|| std::mem::take(line)));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let content_len = newline.unwrap_or(available.len());
        if line.len().saturating_add(content_len) > MAX_JSONL_RECORD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("JSONL physical line exceeds {MAX_JSONL_RECORD_BYTES} bytes"),
            ));
        }
        line.extend_from_slice(&available[..content_len]);
        started = true;
        let consumed = newline.map_or(content_len, |index| index + 1);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Some(std::mem::take(line)));
        }
    }
}

type FileLockMap = HashMap<PathBuf, Weak<Mutex<()>>>;
static FILE_LOCKS: OnceLock<Mutex<FileLockMap>> = OnceLock::new();
static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn file_lock(path: &Path) -> Arc<Mutex<()>> {
    let locks = FILE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // Drop dead entries so a long-running process that writes many distinct
    // sessions does not retain one lock key per session forever.
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
    lock
}

/// Append one JSON value and a newline, serializing same-process writers for
/// this path and syncing the file before returning.
pub fn write_line<T: Serialize>(path: impl AsRef<Path>, value: &T) -> io::Result<()> {
    let path = path.as_ref();
    let lock = file_lock(path);
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    ensure_parent(path)?;
    let bytes = encode_line(value)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()
}

/// Synchronous unsynchronized append, matching `writeLineSync`'s explicit
/// contract. Callers sharing a path with concurrent writers must serialize
/// externally.
pub fn write_line_sync<T: Serialize>(path: impl AsRef<Path>, value: &T) -> io::Result<()> {
    let path = path.as_ref();
    ensure_parent(path)?;
    let bytes = encode_line(value)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()
}

/// Serialize one bounded JSONL entry, including its final newline.
pub fn encode_line<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    let mut writer = BoundedVecWriter {
        bytes: Vec::with_capacity(8 * 1024),
        limit: MAX_JSONL_RECORD_BYTES - 1,
    };
    serde_json::to_writer(&mut writer, value).map_err(json_error)?;
    writer.bytes.push(b'\n');
    Ok(writer.bytes)
}

struct BoundedVecWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for BoundedVecWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("JSONL record exceeds {MAX_JSONL_RECORD_BYTES} bytes"),
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Replace a JSONL file atomically. Each item gets its own trailing newline;
/// an empty slice produces a zero-byte file.
pub fn write<T: Serialize>(path: impl AsRef<Path>, values: &[T]) -> io::Result<()> {
    let path = path.as_ref();
    ensure_parent(path)?;
    let target = resolve_symlink_chain(path)?;
    let parent = parent_dir(&target);
    let existing_permissions = match fs::metadata(&target) {
        Ok(metadata) if metadata.is_file() => Some(metadata.permissions()),
        Ok(_) => None,
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };

    let (temp_path, mut temp_file) = create_temp_file(&target)?;
    let result = (|| {
        for value in values {
            serde_json::to_writer(&mut temp_file, value).map_err(json_error)?;
            temp_file.write_all(b"\n")?;
        }
        if let Some(permissions) = existing_permissions {
            temp_file.set_permissions(permissions)?;
        }
        temp_file.sync_all()?;
        drop(temp_file);
        replace_file(&temp_path, &target)?;
        sync_directory(&parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

/// Count non-empty physical lines, including malformed JSON lines.
pub fn count_lines(path: impl AsRef<Path>) -> io::Result<usize> {
    let file = match File::open(path.as_ref()) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let mut reader = BufReader::new(file);
    let mut count = 0;
    let mut line_non_whitespace = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if line_non_whitespace {
                count += 1;
            }
            break;
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let content_len = newline.unwrap_or(available.len());
        if available[..content_len]
            .iter()
            .any(|byte| !matches!(byte, b' ' | b'\t' | b'\r' | 0x0b | 0x0c))
        {
            line_non_whitespace = true;
        }
        reader.consume(newline.map_or(content_len, |index| index + 1));
        if newline.is_some() {
            if line_non_whitespace {
                count += 1;
            }
            line_non_whitespace = false;
        }
    }
    Ok(count)
}

/// Return true only for a regular file with at least one byte.
pub fn exists(path: impl AsRef<Path>) -> bool {
    fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.len() > 0)
        .unwrap_or(false)
}

fn ensure_parent(path: &Path) -> io::Result<()> {
    fs::create_dir_all(parent_dir(path))
}

fn parent_dir(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

fn create_temp_file(target: &Path) -> io::Result<(PathBuf, File)> {
    let parent = parent_dir(target);
    let name = target.file_name().unwrap_or_default().to_string_lossy();
    for _ in 0..32 {
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp = parent.join(format!(".{name}.tmp-{}-{sequence}", std::process::id()));
        match OpenOptions::new().write(true).create_new(true).open(&temp) {
            Ok(file) => return Ok((temp, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique JSONL temporary file",
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
            parent_dir(&current).canonicalize()?.join(link)
        };
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("too many symbolic link levels resolving {}", path.display()),
    ))
}

fn replace_file(source: &Path, target: &Path) -> io::Result<()> {
    match fs::rename(source, target) {
        Ok(()) => Ok(()),
        #[cfg(windows)]
        Err(error) if target.exists() => {
            fs::remove_file(target)?;
            fs::rename(source, target).map_err(|_| error)
        }
        Err(error) => Err(error),
    }
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn json_error(error: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::thread;

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("canopy-jsonl-{}-{sequence}", std::process::id()));
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

    #[test]
    fn recovers_concatenated_objects_without_splitting_string_contents() {
        assert_eq!(
            recover_objects_from_line(r#"{"a":1}{"b":2}"#),
            vec![json!({"a":1}), json!({"b":2})]
        );
        assert_eq!(
            recover_objects_from_line(r#"{"text":"}{ stays in a string"}{"q":"he said \"hi\""}"#),
            vec![
                json!({"text":"}{ stays in a string"}),
                json!({"q":"he said \"hi\""})
            ]
        );
        assert_eq!(
            recover_objects_from_line(r#"{"a":1}{"oops":}{"b":2}"#),
            vec![json!({"a":1}), json!({"b":2})]
        );
        assert!(recover_objects_from_line("not JSON").is_empty());
    }

    #[test]
    fn parser_filters_scalars_arrays_and_reports_recovery() {
        assert_eq!(
            parse_line_tolerant(r#"{"id":"a"}"#),
            vec![json!({"id":"a"})]
        );
        for line in ["null", "42", r#""text""#, "[]", "[1,2]"] {
            assert!(parse_line_tolerant(line).is_empty(), "{line}");
        }
        assert_eq!(
            parse_line_with_diagnostic(r#"{"a":1}{"b":2}"#).diagnostic,
            Some(JsonlParseDiagnostic::RecoveredObjects { count: 2 })
        );
        assert_eq!(
            parse_line_with_diagnostic("null").diagnostic,
            Some(JsonlParseDiagnostic::NonObjectValue)
        );
    }

    #[test]
    fn bounded_writer_rejects_a_record_before_growing_past_its_limit() {
        let mut writer = BoundedVecWriter {
            bytes: Vec::new(),
            limit: 4,
        };
        writer.write_all(b"1234").unwrap();
        let error = writer.write_all(b"5").unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(writer.bytes, b"1234");
    }

    #[test]
    fn reader_rejects_oversized_physical_line_without_buffering_it_all() {
        let mut reader = io::Cursor::new(vec![b'x'; MAX_JSONL_RECORD_BYTES + 1]);
        let mut line = Vec::new();
        let error = read_bounded_line(&mut reader, &mut line).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(line.len() <= MAX_JSONL_RECORD_BYTES);
    }

    #[test]
    fn reads_malformed_lines_and_limits_recovered_records() {
        let dir = TempDir::new();
        let path = dir.path().join("records.jsonl");
        fs::write(
            &path,
            "{\"i\":1}\n{\"i\":2}{\"i\":3}\nnot-json\n{\"i\":4}\n",
        )
        .unwrap();
        assert_eq!(
            read_lines(&path, 3).unwrap(),
            vec![json!({"i":1}), json!({"i":2}), json!({"i":3})]
        );
        assert_eq!(
            read(&path).unwrap(),
            vec![
                json!({"i":1}),
                json!({"i":2}),
                json!({"i":3}),
                json!({"i":4})
            ]
        );
        assert!(
            read_lines(dir.path().join("missing"), 1)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn appends_synced_records_and_serializes_concurrent_callers() {
        let dir = TempDir::new();
        let path = Arc::new(dir.path().join("nested/records.jsonl"));
        let mut workers = Vec::new();
        for worker in 0..8 {
            let path = Arc::clone(&path);
            workers.push(thread::spawn(move || {
                for item in 0..20 {
                    write_line(path.as_ref(), &json!({"worker":worker,"item":item})).unwrap();
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        let records = read(path.as_ref()).unwrap();
        assert_eq!(records.len(), 160);
        assert!(records.iter().all(Value::is_object));
        assert_eq!(count_lines(path.as_ref()).unwrap(), 160);
        assert!(exists(path.as_ref()));
        let raw = fs::read_to_string(path.as_ref()).unwrap();
        assert!(raw.ends_with('\n'));
        assert!(!raw.contains("}{"));
    }

    #[test]
    fn atomic_replacement_handles_empty_files_and_creates_directories() {
        let dir = TempDir::new();
        let path = dir.path().join("a/b/records.jsonl");
        write(&path, &[json!({"v":1}), json!({"v":2})]).unwrap();
        assert_eq!(read(&path).unwrap(), vec![json!({"v":1}), json!({"v":2})]);
        write::<Value>(&path, &[]).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
        assert!(!exists(&path));
        let leftovers = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp-"))
            .count();
        assert_eq!(leftovers, 0);
    }
}
