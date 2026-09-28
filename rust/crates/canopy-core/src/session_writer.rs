//! Cross-process ownership and durable append support for session transcripts.
//!
//! This is the Rust port of the active lease path in
//! `packages/core/src/services/session-writer-lease.ts`. It publishes lock
//! records with a synced temporary file and an atomic hard-link, checks the
//! transcript identity before every append, and hashes the transcript so an
//! external edit cannot be mistaken for metadata noise.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt;
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use uuid::Uuid;

const LOCK_SCHEMA_VERSION: u8 = 2;
const LEGACY_LOCK_SCHEMA_VERSION: u8 = 1;
const ACQUIRE_ATTEMPTS: usize = 8;
const MALFORMED_RETRY_COUNT: usize = 3;
const MALFORMED_RETRY_DELAY_MS: u64 = 50;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionWriterProcessKind {
    Interactive,
    Acp,
    Daemon,
    #[default]
    Unknown,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionWriterErrorKind {
    Conflict,
    Lost,
    TranscriptChanged,
    Unavailable,
}

impl SessionWriterErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Conflict => "session_writer_conflict",
            Self::Lost => "session_writer_lost",
            Self::TranscriptChanged => "session_transcript_changed",
            Self::Unavailable => "session_writer_unavailable",
        }
    }

    pub fn rpc_code(self) -> i32 {
        match self {
            Self::Conflict => -32020,
            Self::Lost => -32021,
            Self::TranscriptChanged => -32022,
            Self::Unavailable => -32023,
        }
    }

    pub fn http_status(self) -> u16 {
        match self {
            Self::Conflict | Self::Lost | Self::TranscriptChanged => 409,
            Self::Unavailable => 503,
        }
    }
}

#[derive(Debug)]
pub struct SessionWriterError {
    pub kind: SessionWriterErrorKind,
    message: String,
    source: Option<io::Error>,
}

impl SessionWriterError {
    pub(crate) fn new(kind: SessionWriterErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    fn caused_by(
        kind: SessionWriterErrorKind,
        message: impl Into<String>,
        source: io::Error,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            source: Some(source),
        }
    }

    pub fn error_kind(&self) -> &'static str {
        self.kind.as_str()
    }

    pub fn rpc_code(&self) -> i32 {
        self.kind.rpc_code()
    }

    pub fn http_status(&self) -> u16 {
        self.kind.http_status()
    }
}

impl fmt::Display for SessionWriterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for SessionWriterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|error| error as &(dyn Error + 'static))
    }
}

type Result<T> = std::result::Result<T, SessionWriterError>;

#[derive(Clone, Debug)]
pub struct AcquireSessionWriterLeaseOptions {
    pub runtime_base_dir: PathBuf,
    pub session_id: String,
    pub transcript_path: PathBuf,
    pub process_kind: SessionWriterProcessKind,
    pub canopy_version: Option<String>,
    /// `true` matches the TypeScript `reclaimPolicy: "local"` behavior.
    pub reclaim_stale_local_owner: bool,
    /// Permit an explicit takeover of a sealed session after its transcript
    /// snapshot has been certified against the on-disk bytes.
    pub allow_certified_takeover: bool,
}

impl AcquireSessionWriterLeaseOptions {
    pub fn new(
        runtime_base_dir: impl Into<PathBuf>,
        session_id: impl Into<String>,
        transcript_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            runtime_base_dir: runtime_base_dir.into(),
            session_id: session_id.into(),
            transcript_path: transcript_path.into(),
            process_kind: SessionWriterProcessKind::Unknown,
            canopy_version: None,
            reclaim_stale_local_owner: true,
            allow_certified_takeover: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LockRecord {
    schema_version: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
    session_id: String,
    owner_id: String,
    pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    process_start_identity: Option<String>,
    hostname: String,
    process_kind: SessionWriterProcessKind,
    acquired_at: String,
    canopy_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sealed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transcript: Option<SealedTranscriptProof>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SealedTranscriptProof {
    relative_path: String,
    exists: bool,
    byte_length: u64,
    sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Fingerprint {
    dev: u64,
    ino: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    nlink: u64,
    birth_time_ns: i128,
    change_time_ns: i128,
    modified_time_ns: i128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TranscriptState {
    exists: bool,
    byte_length: u64,
    fingerprint: Option<Fingerprint>,
}

impl TranscriptState {
    fn missing() -> Self {
        Self {
            exists: false,
            byte_length: 0,
            fingerprint: None,
        }
    }
}

pub fn get_session_writer_lock_path(
    runtime_base_dir: impl AsRef<Path>,
    session_id: &str,
) -> PathBuf {
    runtime_base_dir
        .as_ref()
        .join("tmp")
        .join("session-writer-locks")
        .join(format!("{}.lock", encode_uri_component(session_id)))
}

fn encode_uri_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02X}"));
        }
    }
    encoded
}

fn unavailable(message: impl Into<String>) -> SessionWriterError {
    SessionWriterError::new(SessionWriterErrorKind::Unavailable, message)
}

fn io_unavailable(message: impl Into<String>, error: io::Error) -> SessionWriterError {
    SessionWriterError::caused_by(SessionWriterErrorKind::Unavailable, message, error)
}

fn transcript_changed() -> SessionWriterError {
    SessionWriterError::new(
        SessionWriterErrorKind::TranscriptChanged,
        "The session transcript changed outside its active writer.",
    )
}

fn writer_lost() -> SessionWriterError {
    SessionWriterError::new(
        SessionWriterErrorKind::Lost,
        "Write ownership for this session was lost.",
    )
}

fn writer_conflict() -> SessionWriterError {
    SessionWriterError::new(
        SessionWriterErrorKind::Conflict,
        "This session is already open in another Canopy process.",
    )
}

fn unix_time_ns(seconds: i64, nanos: i64) -> i128 {
    i128::from(seconds) * 1_000_000_000 + i128::from(nanos)
}

#[cfg(unix)]
fn fingerprint(metadata: &Metadata) -> Fingerprint {
    #[cfg(target_os = "macos")]
    use std::os::macos::fs::MetadataExt as MacMetadataExt;
    use std::os::unix::fs::MetadataExt;
    #[cfg(target_os = "macos")]
    let birth_time_ns = unix_time_ns(metadata.st_birthtime(), metadata.st_birthtime_nsec());
    #[cfg(not(target_os = "macos"))]
    let birth_time_ns = 0;
    Fingerprint {
        dev: metadata.dev(),
        ino: metadata.ino(),
        mode: metadata.mode(),
        uid: metadata.uid(),
        gid: metadata.gid(),
        nlink: metadata.nlink(),
        birth_time_ns,
        change_time_ns: unix_time_ns(metadata.ctime(), metadata.ctime_nsec()),
        modified_time_ns: unix_time_ns(metadata.mtime(), metadata.mtime_nsec()),
    }
}

#[cfg(not(unix))]
fn fingerprint(metadata: &Metadata) -> Fingerprint {
    let modified_time_ns = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as i128)
        .unwrap_or_default();
    Fingerprint {
        dev: 0,
        ino: 1,
        mode: u32::from(metadata.permissions().readonly()),
        uid: 0,
        gid: 0,
        nlink: 1,
        birth_time_ns: 0,
        change_time_ns: modified_time_ns,
        modified_time_ns,
    }
}

fn state_from_metadata(metadata: &Metadata) -> Result<TranscriptState> {
    if !metadata.is_file() {
        return Err(unavailable("Session transcript path is not a regular file"));
    }
    let identity = fingerprint(metadata);
    if identity.ino == 0 {
        return Err(unavailable(
            "Session transcript identity could not be verified on this filesystem.",
        ));
    }
    Ok(TranscriptState {
        exists: true,
        byte_length: metadata.len(),
        fingerprint: Some(identity),
    })
}

fn inspect_transcript(path: &Path) -> Result<TranscriptState> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(transcript_changed());
            }
            state_from_metadata(&metadata)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(TranscriptState::missing()),
        Err(error) => Err(io_unavailable(
            "Could not inspect session transcript",
            error,
        )),
    }
}

fn hard_state_matches(left: &TranscriptState, right: &TranscriptState) -> bool {
    if left.exists != right.exists {
        return false;
    }
    match (&left.fingerprint, &right.fingerprint) {
        (Some(left_fp), Some(right_fp)) => {
            left.byte_length == right.byte_length
                && left_fp.dev == right_fp.dev
                && left_fp.ino == right_fp.ino
                && left_fp.mode == right_fp.mode
                && left_fp.uid == right_fp.uid
                && left_fp.gid == right_fp.gid
                && left_fp.nlink == right_fp.nlink
        }
        (None, None) => left.byte_length == right.byte_length,
        _ => false,
    }
}

fn hash_file(path: &Path, expected: &TranscriptState) -> Result<Sha256> {
    if !expected.exists {
        return Ok(Sha256::new());
    }
    let mut file = open_transcript_read(path)?;
    let before = file
        .metadata()
        .map_err(|error| io_unavailable("Could not inspect session transcript", error))?;
    let before_state = state_from_metadata(&before)?;
    if !hard_state_matches(&before_state, expected) {
        return Err(transcript_changed());
    }
    if before_state.byte_length > 0 {
        let mut last = [0_u8; 1];
        file.seek_read(&mut last, before_state.byte_length - 1)
            .map_err(|error| io_unavailable("Could not inspect transcript ending", error))?;
        if last[0] != b'\n' {
            return Err(transcript_changed());
        }
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| io_unavailable("Could not read session transcript", error))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let after = file
        .metadata()
        .map_err(|error| io_unavailable("Could not inspect session transcript", error))?;
    let after_state = state_from_metadata(&after)?;
    if before_state != after_state {
        return Err(transcript_changed());
    }
    Ok(hasher)
}

fn capture_transcript_snapshot(path: &Path) -> Result<(TranscriptState, Sha256)> {
    let before = inspect_transcript(path)?;
    let hasher = hash_file(path, &before)?;
    let after = inspect_transcript(path)?;
    if before != after {
        return Err(transcript_changed());
    }
    Ok((after, hasher))
}

fn sha256_hex(hasher: &Sha256) -> String {
    hasher
        .clone()
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn relative_transcript_path(runtime_base_dir: &Path, transcript_path: &Path) -> Result<String> {
    let relative = transcript_path
        .strip_prefix(runtime_base_dir)
        .map_err(|_| unavailable("Session transcript is outside the runtime base"))?;
    if relative.as_os_str().is_empty() || relative.is_absolute() {
        return Err(unavailable(
            "Session transcript is outside the runtime base",
        ));
    }
    let value = relative.to_string_lossy().replace('\\', "/");
    if value == ".." || value.starts_with("../") || value.is_empty() {
        return Err(unavailable(
            "Session transcript is outside the runtime base",
        ));
    }
    Ok(value)
}

fn sealed_proof_matches(
    sealed: &LockRecord,
    relative_path: &str,
    state: &TranscriptState,
    hasher: &Sha256,
) -> bool {
    sealed.transcript.as_ref().is_some_and(|proof| {
        proof.relative_path == relative_path
            && proof.exists == state.exists
            && proof.byte_length == state.byte_length
            && proof.sha256 == sha256_hex(hasher)
    })
}

trait SeekRead {
    fn seek_read(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize>;
}

#[cfg(unix)]
impl SeekRead for File {
    fn seek_read(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::unix::fs::FileExt;
        self.read_at(buffer, offset)
    }
}

#[cfg(windows)]
impl SeekRead for File {
    fn seek_read(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::windows::fs::FileExt;
        self.seek_read(buffer, offset)
    }
}

fn open_transcript_read(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options
        .open(path)
        .map_err(|error| io_unavailable("Could not open session transcript", error))
}

fn host_name() -> String {
    Command::new("/bin/hostname")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

#[cfg(target_os = "macos")]
fn process_start_identity(pid: u32) -> Option<String> {
    let output = Command::new("/bin/ps")
        .args(["-o", "lstart=", "-p", &pid.to_string()])
        .env("LC_ALL", "C")
        .env("LANG", "C")
        .env("TZ", "UTC")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!value.is_empty()).then(|| format!("darwin:{value}"))
}

#[cfg(target_os = "linux")]
fn process_start_identity(pid: u32) -> Option<String> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    let fields = stat
        .get(stat.rfind(')')? + 1..)?
        .split_whitespace()
        .collect::<Vec<_>>();
    let start_ticks = fields.get(19)?;
    if !start_ticks.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let boot_id = boot_id.trim();
    if boot_id.is_empty() {
        return None;
    }
    Some(format!("linux:{boot_id}:{start_ticks}"))
}

#[cfg(windows)]
fn process_start_identity(pid: u32) -> Option<String> {
    let output = Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            &format!("(Get-Process -Id {pid} -ErrorAction Stop).StartTime.ToUniversalTime().Ticks"),
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let started = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    started
        .bytes()
        .all(|byte| byte.is_ascii_digit())
        .then(|| format!("win32:{started}"))
}

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
fn process_start_identity(_pid: u32) -> Option<String> {
    None
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    Command::new("/bin/kill")
        .args(["-0", &pid.to_string()])
        .output()
        .map(|output| {
            output.status.success()
                || String::from_utf8_lossy(&output.stderr)
                    .to_ascii_lowercase()
                    .contains("operation not permitted")
        })
        .unwrap_or(true)
}

#[cfg(windows)]
fn process_is_alive(pid: u32) -> bool {
    Command::new("tasklist.exe")
        .args(["/FI", &format!("PID eq {pid}")])
        .output()
        .map(|output| {
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains(&pid.to_string())
        })
        .unwrap_or(true)
}

#[cfg(not(any(unix, windows)))]
fn process_is_alive(_pid: u32) -> bool {
    true
}

fn active_record(record: &LockRecord) -> bool {
    record.schema_version == LEGACY_LOCK_SCHEMA_VERSION || record.state.as_deref() == Some("active")
}

fn is_sealed_record(record: &LockRecord) -> bool {
    record.schema_version == LOCK_SCHEMA_VERSION && record.state.as_deref() == Some("sealed")
}

fn valid_sealed_proof(proof: &SealedTranscriptProof) -> bool {
    !proof.relative_path.is_empty()
        && (proof.exists || proof.byte_length == 0)
        && proof.sha256.len() == 64
        && proof
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn record_is_stale(record: &LockRecord, this_host: &str) -> bool {
    if record.hostname != this_host {
        return false;
    }
    if !process_is_alive(record.pid) {
        return true;
    }
    let Some(expected_identity) = record.process_start_identity.as_deref() else {
        return false;
    };
    process_start_identity(record.pid)
        .is_some_and(|current_identity| current_identity != expected_identity)
}

fn parse_lock(raw: &str, expected_session_id: &str) -> Result<LockRecord> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|_| unavailable("Existing session writer lock is malformed"))?;
    let object = value
        .as_object()
        .ok_or_else(|| unavailable("Existing session writer lock is malformed"))?;
    if !object.contains_key("canopy_version")
        || object
            .get("process_start_identity")
            .is_some_and(serde_json::Value::is_null)
        || object
            .get("schema_version")
            .and_then(serde_json::Value::as_u64)
            == Some(LEGACY_LOCK_SCHEMA_VERSION.into())
            && object.contains_key("state")
    {
        return Err(unavailable("Existing session writer lock is malformed"));
    }
    let record: LockRecord = serde_json::from_value(value)
        .map_err(|_| unavailable("Existing session writer lock is malformed"))?;
    let valid_version = matches!(
        record.schema_version,
        LOCK_SCHEMA_VERSION | LEGACY_LOCK_SCHEMA_VERSION
    );
    let valid_state = record.schema_version == LEGACY_LOCK_SCHEMA_VERSION && record.state.is_none()
        || record.schema_version == LOCK_SCHEMA_VERSION
            && (record.state.as_deref() == Some("active")
                || record.state.as_deref() == Some("sealed")
                    && record
                        .sealed_at
                        .as_deref()
                        .is_some_and(|timestamp| DateTime::parse_from_rfc3339(timestamp).is_ok())
                    && record.transcript.as_ref().is_some_and(valid_sealed_proof));
    let valid_owner = !record.session_id.is_empty()
        && !record.owner_id.is_empty()
        && record.pid > 0
        && !record.hostname.is_empty()
        && DateTime::parse_from_rfc3339(&record.acquired_at).is_ok();
    if !valid_version || !valid_state || !valid_owner {
        return Err(unavailable("Existing session writer lock is malformed"));
    }
    if record.session_id != expected_session_id {
        return Err(unavailable(
            "Session writer lock belongs to another session",
        ));
    }
    Ok(record)
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| io_unavailable("Could not sync session writer lock directory", error))
}

fn suffix_path(path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{suffix}", path.display()))
}

fn assert_path_missing(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_unavailable(
            "Could not inspect session writer path",
            error,
        )),
        Ok(_) => Err(unavailable(
            "Session writer transition claim already exists",
        )),
    }
}

fn read_regular_file(path: &Path) -> Result<String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| io_unavailable("Could not inspect session writer record", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(unavailable("Session writer record is not a regular file"));
    }
    fs::read_to_string(path)
        .map_err(|error| io_unavailable("Could not read session writer record", error))
}

fn remove_exact_record(path: &Path, expected_raw: &str) -> Result<()> {
    let raw = match read_regular_file(path) {
        Ok(raw) => raw,
        Err(error)
            if error.source().is_some_and(|source| {
                source
                    .downcast_ref::<io::Error>()
                    .is_some_and(|io_error| io_error.kind() == io::ErrorKind::NotFound)
            }) =>
        {
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    if raw != expected_raw {
        return Err(writer_lost());
    }
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_unavailable(
            "Could not remove session writer record",
            error,
        )),
    }
}

fn release_transition_claim(path: &Path, claim_raw: &str) -> Result<()> {
    match read_regular_file(path) {
        Ok(raw) if raw == claim_raw => remove_exact_record(path, claim_raw),
        Ok(_) => Ok(()),
        Err(error)
            if error.source().is_some_and(|source| {
                source
                    .downcast_ref::<io::Error>()
                    .is_some_and(|io_error| io_error.kind() == io::ErrorKind::NotFound)
            }) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn assert_exact_claim(path: &Path, expected_raw: &str) -> Result<()> {
    match read_regular_file(path) {
        Ok(raw) if raw == expected_raw => Ok(()),
        Ok(_) => Err(unavailable(
            "Session writer transition claim ownership was lost",
        )),
        Err(error) => Err(unavailable(format!(
            "Session writer transition claim could not be verified: {error}"
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
fn link_claimed_primary(
    primary_path: &Path,
    replacement_path: &Path,
    replacement_raw: &str,
    retired_path: &Path,
    source_raw: &str,
    session_id: &str,
    claim_path: &Path,
    claim_raw: &str,
) -> Result<()> {
    for attempt in 0..20 {
        assert_exact_claim(claim_path, claim_raw)?;
        match fs::hard_link(replacement_path, primary_path) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let existing = match read_regular_file(primary_path) {
                    Ok(raw) => raw,
                    Err(read_error)
                        if read_error.source().is_some_and(|source| {
                            source
                                .downcast_ref::<io::Error>()
                                .is_some_and(|io_error| io_error.kind() == io::ErrorKind::NotFound)
                        }) =>
                    {
                        continue;
                    }
                    Err(read_error) => return Err(read_error),
                };
                if existing == source_raw || existing == replacement_raw {
                    return Ok(());
                }
                let candidate = parse_lock(&existing, session_id).is_ok_and(|record| {
                    record.schema_version == LOCK_SCHEMA_VERSION
                        && record.state.as_deref() == Some("active")
                });
                if !candidate {
                    return Err(writer_lost());
                }
                if attempt == 19 {
                    return Err(unavailable(
                        "Session writer primary candidate did not release the claimed path",
                    ));
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(error) => {
                // If the target was already renamed by another participant,
                // retry only while the transition claim remains ours.
                if !retired_path.exists() {
                    return Err(io_unavailable(
                        "Could not install session writer transition",
                        error,
                    ));
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }
    Err(unavailable(
        "Session writer primary transition attempts were exhausted",
    ))
}

#[allow(clippy::too_many_arguments)]
fn transition_exact_primary(
    primary_path: &Path,
    expected_raw: &str,
    replacement_path: &Path,
    replacement_raw: &str,
    retired_path: &Path,
    session_id: &str,
    claim_path: &Path,
    claim_raw: &str,
) -> Result<()> {
    if read_regular_file(primary_path)? != expected_raw {
        return Err(writer_lost());
    }
    assert_path_missing(retired_path)?;
    fs::rename(primary_path, retired_path)
        .map_err(|error| io_unavailable("Could not retire session writer record", error))?;
    if let Err(error) = link_claimed_primary(
        primary_path,
        replacement_path,
        replacement_raw,
        retired_path,
        expected_raw,
        session_id,
        claim_path,
        claim_raw,
    ) {
        let restored = link_claimed_primary(
            primary_path,
            retired_path,
            expected_raw,
            replacement_path,
            replacement_raw,
            session_id,
            claim_path,
            claim_raw,
        );
        if restored.is_ok() {
            let _ = remove_exact_record(retired_path, expected_raw);
            return Err(error);
        }
        return Err(unavailable(format!(
            "Session writer primary transition could not be restored: {error}; {}",
            restored.unwrap_err()
        )));
    }
    if let Some(parent) = primary_path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn create_private_dir_all(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "expected a regular directory",
                ));
            }
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                if parent != path {
                    create_private_dir_all(parent)?;
                }
            }
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    create_private_dir_all(path)
                }
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

fn install_lock_record(lock_path: &Path, record: &LockRecord) -> Result<bool> {
    let raw = serde_json::to_vec(record)
        .map_err(|_| unavailable("Could not serialize session writer lock"))?;
    let temp_path = PathBuf::from(format!("{}.{}.tmp", lock_path.display(), record.owner_id));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = match options.open(&temp_path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(unavailable("Session writer temporary lock already exists"));
        }
        Err(error) => {
            return Err(io_unavailable(
                "Could not create session writer lock",
                error,
            ));
        }
    };
    let write_result = (|| {
        file.write_all(&raw)?;
        file.sync_all()?;
        drop(file);
        match fs::hard_link(&temp_path, lock_path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
            Err(error) => Err(error),
        }
    })();
    let _ = fs::remove_file(&temp_path);
    match write_result {
        Ok(true) => {
            sync_dir(lock_path.parent().unwrap_or_else(|| Path::new(".")))?;
            Ok(true)
        }
        Ok(false) => Ok(false),
        Err(error) => Err(io_unavailable(
            "Could not publish session writer lock",
            error,
        )),
    }
}

fn inspect_lock(path: &Path, session_id: &str) -> Result<Option<(LockRecord, String)>> {
    for attempt in 0..MALFORMED_RETRY_COUNT {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(io_unavailable(
                    "Could not inspect session writer lock",
                    error,
                ));
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(unavailable("Session writer lock is not a regular file"));
        }
        match fs::read_to_string(path) {
            Ok(raw) => match parse_lock(&raw, session_id) {
                Ok(record) => return Ok(Some((record, raw))),
                Err(error) if attempt + 1 < MALFORMED_RETRY_COUNT => {
                    std::thread::sleep(std::time::Duration::from_millis(MALFORMED_RETRY_DELAY_MS));
                    let _ = error;
                }
                Err(error) => return Err(error),
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_unavailable("Could not read session writer lock", error)),
        }
    }
    Err(unavailable("Existing session writer lock is malformed"))
}

fn ensure_transcript_identity_parent(transcript_path: &Path) -> Result<()> {
    let mut candidate = transcript_path.parent();
    while let Some(path) = candidate {
        match fs::metadata(path) {
            Ok(metadata) => {
                if fingerprint(&metadata).ino == 0 {
                    return Err(unavailable(
                        "Session transcript identity could not be verified on this filesystem.",
                    ));
                }
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => candidate = path.parent(),
            Err(error) if error.kind() == io::ErrorKind::NotADirectory => candidate = path.parent(),
            Err(error) => {
                return Err(io_unavailable(
                    "Could not verify transcript filesystem",
                    error,
                ));
            }
        }
    }
    Ok(())
}

fn resolve(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|current| current.join(path))
            .map_err(|error| io_unavailable("Could not resolve session path", error))?
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                let _ = normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    Ok(normalized)
}

/// A process-wide claim for one transcript. Methods take `&mut self` so Rust's
/// borrow checker serializes operations on this lease in the same way that the
/// source implementation's promise tail serializes its async calls.
#[derive(Debug)]
pub struct SessionWriterLease {
    lock_path: PathBuf,
    retired_path: PathBuf,
    lock_record_raw: String,
    expected_state: TranscriptState,
    expected_hasher: Sha256,
    released: bool,
    pub owner_id: String,
    pub session_id: String,
    pub runtime_base_dir: PathBuf,
    pub transcript_path: PathBuf,
}

impl SessionWriterLease {
    pub fn acquire(options: AcquireSessionWriterLeaseOptions) -> Result<Self> {
        if options.session_id.is_empty() {
            return Err(unavailable("Session ID must not be empty"));
        }
        let runtime_base_dir = resolve(&options.runtime_base_dir)?;
        let transcript_path = resolve(&options.transcript_path)?;
        let lock_path = get_session_writer_lock_path(&runtime_base_dir, &options.session_id);
        let lock_dir = lock_path.parent().expect("lock path has a parent");
        create_private_dir_all(lock_dir).map_err(|error| {
            io_unavailable("Could not create session writer lock directory", error)
        })?;
        let lock_dir_metadata = fs::symlink_metadata(lock_dir).map_err(|error| {
            io_unavailable("Could not inspect session writer lock directory", error)
        })?;
        if lock_dir_metadata.file_type().is_symlink() || !lock_dir_metadata.is_dir() {
            return Err(unavailable(
                "Session writer lock directory is not a regular directory",
            ));
        }

        let pid = std::process::id();
        let owner_id = Uuid::new_v4().to_string();
        let process_identity = process_start_identity(pid);
        let record = LockRecord {
            schema_version: LOCK_SCHEMA_VERSION,
            state: Some("active".to_owned()),
            session_id: options.session_id.clone(),
            owner_id: owner_id.clone(),
            pid,
            process_start_identity: process_identity,
            hostname: host_name(),
            process_kind: options.process_kind,
            acquired_at: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            canopy_version: options.canopy_version,
            sealed_at: None,
            transcript: None,
        };
        let lock_record_raw = serde_json::to_string(&record)
            .map_err(|_| unavailable("Could not serialize session writer lock"))?;
        let this_host = record.hostname.clone();
        let claim_path = suffix_path(&lock_path, ".claim");

        for _ in 0..ACQUIRE_ATTEMPTS {
            assert_path_missing(&claim_path)?;
            if install_lock_record(&lock_path, &record)? {
                if let Err(error) = assert_path_missing(&claim_path) {
                    let _ = remove_owned_lock(&lock_path, &owner_id);
                    return Err(error);
                }
                let mut lease = Self::new(
                    lock_path.clone(),
                    lock_record_raw.clone(),
                    record.clone(),
                    runtime_base_dir.clone(),
                    transcript_path.clone(),
                );
                if let Err(error) = lease.capture_initial_snapshot() {
                    let _ = lease.release();
                    return Err(error);
                }
                return Ok(lease);
            }

            let Some((existing, existing_raw)) = inspect_lock(&lock_path, &options.session_id)?
            else {
                continue;
            };
            if !active_record(&existing) {
                if !options.allow_certified_takeover || !is_sealed_record(&existing) {
                    return Err(writer_conflict());
                }
                return Self::take_over_sealed(
                    lock_path.clone(),
                    existing,
                    existing_raw,
                    record.clone(),
                    lock_record_raw.clone(),
                    runtime_base_dir.clone(),
                    transcript_path.clone(),
                );
            }
            if !record_is_stale(&existing, &this_host) {
                return Err(writer_conflict());
            }
            if !options.reclaim_stale_local_owner || existing.hostname != this_host {
                return Err(writer_conflict());
            }

            let reclaim_path = PathBuf::from(format!(
                "{}.reclaim.{}",
                lock_path.display(),
                encode_uri_component(&existing.owner_id)
            ));
            if !install_lock_record(&reclaim_path, &record)? {
                return Err(unavailable(
                    "Could not acquire stale session writer reclaim claim",
                ));
            }
            let reclaim_result = (|| {
                assert_path_missing(&claim_path)?;
                let current = inspect_lock(&lock_path, &options.session_id)?;
                let Some((current_record, current_raw)) = current else {
                    return Ok(false);
                };
                if current_record.owner_id != existing.owner_id
                    || current_raw != existing_raw
                    || !record_is_stale(&current_record, &this_host)
                {
                    return Err(writer_conflict());
                }
                let stale_path = PathBuf::from(format!(
                    "{}.stale.{}.{}",
                    lock_path.display(),
                    pid,
                    Uuid::new_v4()
                ));
                fs::rename(&lock_path, &stale_path).map_err(|error| {
                    io_unavailable("Could not retire stale session writer", error)
                })?;
                let moved = fs::read_to_string(&stale_path).map_err(|error| {
                    io_unavailable("Could not verify stale session writer", error)
                })?;
                if moved != existing_raw {
                    let _ = fs::hard_link(&stale_path, &lock_path);
                    let _ = fs::remove_file(&stale_path);
                    return Err(unavailable("Stale session writer changed during reclaim"));
                }
                fs::remove_file(&stale_path).map_err(|error| {
                    io_unavailable("Could not remove stale session writer", error)
                })?;
                if !install_lock_record(&lock_path, &record)? {
                    return Err(unavailable("Could not publish reclaimed session writer"));
                }
                if let Err(error) = assert_path_missing(&claim_path) {
                    let _ = remove_owned_lock(&lock_path, &owner_id);
                    return Err(error);
                }
                Ok(true)
            })();
            let _ = remove_owned_lock(&reclaim_path, &owner_id);
            match reclaim_result {
                Ok(true) => {
                    let mut lease = Self::new(
                        lock_path.clone(),
                        lock_record_raw.clone(),
                        record.clone(),
                        runtime_base_dir.clone(),
                        transcript_path.clone(),
                    );
                    if let Err(error) = lease.capture_initial_snapshot() {
                        let _ = lease.release();
                        return Err(error);
                    }
                    return Ok(lease);
                }
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(unavailable(
            "Session writer acquisition attempts were exhausted",
        ))
    }

    fn take_over_sealed(
        lock_path: PathBuf,
        observed_record: LockRecord,
        observed_raw: String,
        new_record: LockRecord,
        new_raw: String,
        runtime_base_dir: PathBuf,
        transcript_path: PathBuf,
    ) -> Result<Self> {
        let session_id = new_record.session_id.clone();
        let relative_path = relative_transcript_path(&runtime_base_dir, &transcript_path)?;
        let (proof_state, proof_hasher) = capture_transcript_snapshot(&transcript_path)?;
        if !sealed_proof_matches(
            &observed_record,
            &relative_path,
            &proof_state,
            &proof_hasher,
        ) {
            return Err(transcript_changed());
        }

        let claim_path = suffix_path(&lock_path, ".claim");
        let retired_path = suffix_path(
            &lock_path,
            &format!(
                ".sealed.{}.{}",
                encode_uri_component(&observed_record.owner_id),
                encode_uri_component(&new_record.owner_id)
            ),
        );
        assert_path_missing(&claim_path)?;
        assert_path_missing(&retired_path)?;
        if !install_lock_record(&claim_path, &new_record)? {
            return Err(unavailable(
                "Session writer transition claim already exists",
            ));
        }

        let transition = (|| {
            if read_regular_file(&lock_path)? != observed_raw {
                return Err(writer_conflict());
            }
            let current = parse_lock(&observed_raw, &session_id)?;
            let (current_state, current_hasher) = capture_transcript_snapshot(&transcript_path)?;
            if !sealed_proof_matches(&current, &relative_path, &current_state, &current_hasher) {
                return Err(transcript_changed());
            }
            transition_exact_primary(
                &lock_path,
                &observed_raw,
                &claim_path,
                &new_raw,
                &retired_path,
                &session_id,
                &claim_path,
                &new_raw,
            )?;

            let (after_state, after_hasher) = capture_transcript_snapshot(&transcript_path)?;
            if after_state != proof_state
                || after_hasher.clone().finalize().as_slice()
                    != proof_hasher.clone().finalize().as_slice()
            {
                return Err(transcript_changed());
            }
            let mut lease = Self::new(
                lock_path.clone(),
                new_raw.clone(),
                new_record.clone(),
                runtime_base_dir.clone(),
                transcript_path.clone(),
            );
            lease.expected_state = after_state;
            lease.expected_hasher = after_hasher;
            lease.read_owned_lock()?;
            release_transition_claim(&claim_path, &new_raw)?;
            let _ = remove_exact_record(&retired_path, &observed_raw);
            if let Some(parent) = lock_path.parent() {
                sync_dir(parent)?;
            }
            Ok(lease)
        })();

        match transition {
            Ok(lease) => Ok(lease),
            Err(error) => {
                let current = read_regular_file(&lock_path).ok();
                if current.as_deref() == Some(new_raw.as_str()) {
                    let _ = remove_exact_record(&lock_path, &new_raw);
                }
                if read_regular_file(&retired_path).ok().as_deref() == Some(observed_raw.as_str())
                    && fs::hard_link(&retired_path, &lock_path).is_ok()
                {
                    let _ = remove_exact_record(&retired_path, &observed_raw);
                }
                let _ = release_transition_claim(&claim_path, &new_raw);
                Err(error)
            }
        }
    }

    fn new(
        lock_path: PathBuf,
        lock_record_raw: String,
        record: LockRecord,
        runtime_base_dir: PathBuf,
        transcript_path: PathBuf,
    ) -> Self {
        let owner_id = record.owner_id.clone();
        let session_id = record.session_id.clone();
        let retired_path = PathBuf::from(format!(
            "{}.released.{}",
            lock_path.display(),
            encode_uri_component(&owner_id)
        ));
        Self {
            lock_path,
            retired_path,
            lock_record_raw,
            expected_state: TranscriptState::missing(),
            expected_hasher: Sha256::new(),
            released: false,
            owner_id,
            session_id,
            runtime_base_dir,
            transcript_path,
        }
    }

    fn capture_initial_snapshot(&mut self) -> Result<()> {
        self.expected_state = inspect_transcript(&self.transcript_path)?;
        if !self.expected_state.exists {
            ensure_transcript_identity_parent(&self.transcript_path)?;
        }
        self.expected_hasher = hash_file(&self.transcript_path, &self.expected_state)?;
        self.read_owned_lock()?;
        Ok(())
    }

    pub fn transcript_existed_at_acquire(&self) -> bool {
        self.expected_state.exists
    }

    pub fn is_released(&self) -> bool {
        self.released
    }

    fn read_owned_lock(&self) -> Result<LockRecord> {
        if self.released {
            return Err(writer_lost());
        }
        let metadata = fs::symlink_metadata(&self.lock_path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                writer_lost()
            } else {
                io_unavailable("Could not inspect owned session writer lock", error)
            }
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(writer_lost());
        }
        let raw = fs::read_to_string(&self.lock_path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                writer_lost()
            } else {
                io_unavailable("Could not read owned session writer lock", error)
            }
        })?;
        let record = parse_lock(&raw, &self.session_id).map_err(|_| writer_lost())?;
        if !active_record(&record)
            || record.owner_id != self.owner_id
            || raw != self.lock_record_raw
        {
            return Err(writer_lost());
        }
        Ok(record)
    }

    pub fn assert_owned_and_unchanged(&mut self) -> Result<()> {
        self.read_owned_lock()?;
        let observed = inspect_transcript(&self.transcript_path)?;
        if !hard_state_matches(&observed, &self.expected_state) {
            return Err(transcript_changed());
        }
        if observed == self.expected_state {
            return Ok(());
        }
        let observed_hash = hash_file(&self.transcript_path, &observed)?;
        if observed_hash.clone().finalize().as_slice()
            != self.expected_hasher.clone().finalize().as_slice()
        {
            return Err(transcript_changed());
        }
        self.expected_state = observed;
        self.expected_hasher = observed_hash;
        Ok(())
    }

    pub fn append_json_line<T: Serialize>(&mut self, value: &T) -> Result<()> {
        let bytes = crate::jsonl::encode_line(value).map_err(|error| {
            unavailable(format!(
                "Could not serialize session transcript record: {error}"
            ))
        })?;
        self.assert_owned_and_unchanged()?;
        if let Some(parent) = self.transcript_path.parent() {
            create_private_dir_all(parent)
                .map_err(|error| io_unavailable("Could not create transcript directory", error))?;
        }

        let before = self.expected_state.clone();
        let mut options = OpenOptions::new();
        options.write(true).append(true).read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
            if !before.exists {
                options.create_new(true).mode(0o600);
            }
        }
        #[cfg(not(unix))]
        if !before.exists {
            options.create_new(true);
        }
        let mut file = options.open(&self.transcript_path).map_err(|error| {
            if matches!(
                error.kind(),
                io::ErrorKind::AlreadyExists | io::ErrorKind::NotFound
            ) {
                transcript_changed()
            } else {
                io_unavailable("Could not open session transcript for append", error)
            }
        })?;
        let before_metadata = file
            .metadata()
            .map_err(|error| io_unavailable("Could not inspect transcript before append", error))?;
        let before_open_state = state_from_metadata(&before_metadata)?;
        if (before.exists && !hard_state_matches(&before_open_state, &before))
            || (!before.exists && before_open_state.byte_length != 0)
        {
            return Err(transcript_changed());
        }
        if before_open_state.byte_length > 0 {
            let mut last = [0_u8; 1];
            file.seek_read(&mut last, before_open_state.byte_length - 1)
                .map_err(|error| io_unavailable("Could not inspect transcript ending", error))?;
            if last[0] != b'\n' {
                return Err(transcript_changed());
            }
        }
        self.read_owned_lock()?;
        file.write_all(&bytes)
            .map_err(|error| io_unavailable("Could not append session transcript record", error))?;
        file.sync_all()
            .map_err(|error| io_unavailable("Could not sync session transcript", error))?;
        let after = state_from_metadata(&file.metadata().map_err(|error| {
            io_unavailable("Could not inspect transcript after append", error)
        })?)?;
        let expected_length = before.byte_length + bytes.len() as u64;
        let before_fingerprint = before.fingerprint.as_ref();
        let after_fingerprint = after.fingerprint.as_ref();
        let identity_unchanged = if before.exists {
            before_fingerprint
                .zip(after_fingerprint)
                .is_some_and(|(old, new)| {
                    old.dev == new.dev
                        && old.ino == new.ino
                        && old.mode == new.mode
                        && old.uid == new.uid
                        && old.gid == new.gid
                        && old.nlink == new.nlink
                })
        } else {
            after_fingerprint.is_some()
        };
        if after.byte_length != expected_length || !identity_unchanged {
            return Err(transcript_changed());
        }
        drop(file);
        let path_after = inspect_transcript(&self.transcript_path)?;
        if !hard_state_matches(&path_after, &after) {
            return Err(transcript_changed());
        }
        let mut next_hasher = self.expected_hasher.clone();
        next_hasher.update(&bytes);
        if path_after != after {
            let observed_hasher = hash_file(&self.transcript_path, &path_after)?;
            if observed_hasher.clone().finalize().as_slice()
                != next_hasher.clone().finalize().as_slice()
            {
                return Err(transcript_changed());
            }
            next_hasher = observed_hasher;
        }
        self.expected_state = path_after;
        self.expected_hasher = next_hasher;
        self.read_owned_lock()?;
        Ok(())
    }

    /// Seal the current transcript and atomically replace the active lock with
    /// a proof record for a successor process. A certified successor must
    /// verify the relative path, byte length, and SHA-256 before taking over.
    pub fn seal_for_handoff(&mut self) -> Result<()> {
        if self.released {
            return Ok(());
        }
        self.assert_owned_and_unchanged()?;
        let owned_record = parse_lock(&self.lock_record_raw, &self.session_id)?;
        let relative_path =
            relative_transcript_path(&self.runtime_base_dir, &self.transcript_path)?;
        let (proof_state, proof_hasher) = capture_transcript_snapshot(&self.transcript_path)?;
        if proof_state != self.expected_state
            || proof_hasher.clone().finalize().as_slice()
                != self.expected_hasher.clone().finalize().as_slice()
        {
            return Err(transcript_changed());
        }
        let mut sealed_record = owned_record;
        sealed_record.schema_version = LOCK_SCHEMA_VERSION;
        sealed_record.state = Some("sealed".to_owned());
        sealed_record.sealed_at =
            Some(Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
        sealed_record.transcript = Some(SealedTranscriptProof {
            relative_path,
            exists: proof_state.exists,
            byte_length: proof_state.byte_length,
            sha256: sha256_hex(&proof_hasher),
        });
        let sealed_raw = serde_json::to_string(&sealed_record)
            .map_err(|_| unavailable("Could not serialize sealed session writer record"))?;
        let candidate_path = suffix_path(
            &self.lock_path,
            &format!(".sealed-candidate.{}", encode_uri_component(&self.owner_id)),
        );
        let handoff_retired_path = suffix_path(
            &self.lock_path,
            &format!(".handoff.{}", encode_uri_component(&self.owner_id)),
        );
        let claim_path = suffix_path(&self.lock_path, ".claim");
        assert_path_missing(&candidate_path)?;
        assert_path_missing(&handoff_retired_path)?;
        assert_path_missing(&claim_path)?;
        if !install_lock_record(&candidate_path, &sealed_record)? {
            return Err(unavailable(
                "Session writer sealed candidate already exists",
            ));
        }
        if !install_lock_record(
            &claim_path,
            &parse_lock(&self.lock_record_raw, &self.session_id)?,
        )? {
            let _ = remove_exact_record(&candidate_path, &sealed_raw);
            return Err(unavailable(
                "Session writer transition claim already exists",
            ));
        }

        let transition = (|| {
            self.read_owned_lock()?;
            let (current_state, current_hasher) =
                capture_transcript_snapshot(&self.transcript_path)?;
            if current_state != proof_state
                || current_hasher.clone().finalize().as_slice()
                    != proof_hasher.clone().finalize().as_slice()
            {
                return Err(transcript_changed());
            }
            transition_exact_primary(
                &self.lock_path,
                &self.lock_record_raw,
                &candidate_path,
                &sealed_raw,
                &handoff_retired_path,
                &self.session_id,
                &claim_path,
                &self.lock_record_raw,
            )?;
            let (after_state, after_hasher) = capture_transcript_snapshot(&self.transcript_path)?;
            if after_state != proof_state
                || after_hasher.clone().finalize().as_slice()
                    != proof_hasher.clone().finalize().as_slice()
            {
                return Err(transcript_changed());
            }
            if read_regular_file(&self.lock_path)? != sealed_raw {
                return Err(writer_lost());
            }
            release_transition_claim(&claim_path, &self.lock_record_raw)?;
            let _ = remove_exact_record(&handoff_retired_path, &self.lock_record_raw);
            let _ = remove_exact_record(&candidate_path, &sealed_raw);
            if let Some(parent) = self.lock_path.parent() {
                sync_dir(parent)?;
            }
            self.released = true;
            Ok(())
        })();

        if let Err(error) = transition {
            let current = read_regular_file(&self.lock_path).ok();
            if current.as_deref() == Some(sealed_raw.as_str()) {
                let _ = remove_exact_record(&self.lock_path, &sealed_raw);
            }
            if read_regular_file(&handoff_retired_path).ok().as_deref()
                == Some(self.lock_record_raw.as_str())
                && fs::hard_link(&handoff_retired_path, &self.lock_path).is_ok()
            {
                let _ = remove_exact_record(&handoff_retired_path, &self.lock_record_raw);
            }
            let _ = release_transition_claim(&claim_path, &self.lock_record_raw);
            let _ = remove_exact_record(&candidate_path, &sealed_raw);
            if read_regular_file(&self.lock_path).ok().as_deref()
                != Some(self.lock_record_raw.as_str())
            {
                self.released = true;
            }
            return Err(error);
        }
        Ok(())
    }

    pub fn release(&mut self) -> Result<()> {
        if self.released {
            return Ok(());
        }
        self.read_owned_lock()?;
        fs::rename(&self.lock_path, &self.retired_path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                writer_lost()
            } else {
                io_unavailable("Could not release session writer lock", error)
            }
        })?;
        self.released = true;
        let _ = fs::remove_file(&self.retired_path);
        if let Some(parent) = self.lock_path.parent() {
            sync_dir(parent)?;
        }
        Ok(())
    }
}

fn remove_owned_lock(path: &Path, owner_id: &str) -> Result<()> {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_unavailable("Could not read session writer claim", error)),
    };
    let record: LockRecord =
        serde_json::from_str(&raw).map_err(|_| unavailable("Session writer claim is malformed"))?;
    if record.owner_id != owner_id || !active_record(&record) {
        return Err(writer_lost());
    }
    if !path.exists() {
        return Ok(());
    }
    fs::remove_file(path)
        .map_err(|error| io_unavailable("Could not remove owned session lock", error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("canopy-session-writer-{}", Uuid::new_v4()))
    }

    #[test]
    fn appends_durably_and_releases_lock() {
        let root = temp_dir();
        let transcript = root.join("sessions").join("one.jsonl");
        let mut lease = SessionWriterLease::acquire(AcquireSessionWriterLeaseOptions::new(
            &root,
            "session/one",
            &transcript,
        ))
        .unwrap();
        assert!(!lease.transcript_existed_at_acquire());
        lease
            .append_json_line(&json!({"type":"user","text":"hello"}))
            .unwrap();
        lease
            .append_json_line(&json!({"type":"assistant","text":"hi"}))
            .unwrap();
        assert_eq!(fs::read_to_string(&transcript).unwrap().lines().count(), 2);
        lease.assert_owned_and_unchanged().unwrap();
        lease.release().unwrap();
        assert!(lease.is_released());
        assert!(!get_session_writer_lock_path(&root, "session/one").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn a_second_live_writer_is_rejected() {
        let root = temp_dir();
        let transcript = root.join("one.jsonl");
        let first = SessionWriterLease::acquire(AcquireSessionWriterLeaseOptions::new(
            &root,
            "session-one",
            &transcript,
        ))
        .unwrap();
        let error = SessionWriterLease::acquire(AcquireSessionWriterLeaseOptions::new(
            &root,
            "session-one",
            &transcript,
        ))
        .unwrap_err();
        assert_eq!(error.kind, SessionWriterErrorKind::Conflict);
        drop(first);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn external_transcript_edit_is_detected() {
        let root = temp_dir();
        let transcript = root.join("one.jsonl");
        let mut lease = SessionWriterLease::acquire(AcquireSessionWriterLeaseOptions::new(
            &root,
            "session-one",
            &transcript,
        ))
        .unwrap();
        lease.append_json_line(&json!({"line":1})).unwrap();
        fs::write(&transcript, b"{\"line\":2}\n").unwrap();
        let error = lease.assert_owned_and_unchanged().unwrap_err();
        assert_eq!(error.kind, SessionWriterErrorKind::TranscriptChanged);
        let _ = lease.release();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sealed_handoff_requires_certified_takeover_and_preserves_transcript() {
        let root = temp_dir();
        let transcript = root.join("sessions").join("one.jsonl");
        let mut owner = SessionWriterLease::acquire(AcquireSessionWriterLeaseOptions::new(
            &root,
            "session-one",
            &transcript,
        ))
        .unwrap();
        owner.append_json_line(&json!({"sequence":1})).unwrap();
        owner.seal_for_handoff().unwrap();
        assert!(owner.is_released());

        let error = SessionWriterLease::acquire(AcquireSessionWriterLeaseOptions::new(
            &root,
            "session-one",
            &transcript,
        ))
        .unwrap_err();
        assert_eq!(error.kind, SessionWriterErrorKind::Conflict);

        let mut options = AcquireSessionWriterLeaseOptions::new(&root, "session-one", &transcript);
        options.allow_certified_takeover = true;
        let mut successor = SessionWriterLease::acquire(options).unwrap();
        successor.append_json_line(&json!({"sequence":2})).unwrap();
        assert_eq!(fs::read_to_string(&transcript).unwrap().lines().count(), 2);
        successor.release().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sealed_handoff_proof_rejects_changed_transcript() {
        let root = temp_dir();
        let transcript = root.join("one.jsonl");
        let mut owner = SessionWriterLease::acquire(AcquireSessionWriterLeaseOptions::new(
            &root,
            "session-one",
            &transcript,
        ))
        .unwrap();
        owner.append_json_line(&json!({"sequence":1})).unwrap();
        owner.seal_for_handoff().unwrap();
        fs::write(&transcript, b"{\"sequence\":9}\n").unwrap();

        let mut options = AcquireSessionWriterLeaseOptions::new(&root, "session-one", &transcript);
        options.allow_certified_takeover = true;
        let error = SessionWriterLease::acquire(options).unwrap_err();
        assert_eq!(error.kind, SessionWriterErrorKind::TranscriptChanged);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn dead_local_owner_is_reclaimed() {
        let root = temp_dir();
        let transcript = root.join("one.jsonl");
        let lock_path = get_session_writer_lock_path(&root, "session-one");
        fs::create_dir_all(lock_path.parent().unwrap()).unwrap();
        let stale = json!({
            "schema_version": 2,
            "state": "active",
            "session_id": "session-one",
            "owner_id": "dead-owner",
            "pid": u32::MAX,
            "hostname": host_name(),
            "process_kind": "unknown",
            "acquired_at": Utc::now().to_rfc3339(),
            "canopy_version": null
        });
        fs::write(&lock_path, serde_json::to_vec(&stale).unwrap()).unwrap();

        let mut lease = SessionWriterLease::acquire(AcquireSessionWriterLeaseOptions::new(
            &root,
            "session-one",
            &transcript,
        ))
        .unwrap();
        assert_ne!(lease.owner_id, "dead-owner");
        lease
            .append_json_line(&json!({"after_reclaim":true}))
            .unwrap();
        lease.release().unwrap();
        let _ = fs::remove_dir_all(root);
    }
}
