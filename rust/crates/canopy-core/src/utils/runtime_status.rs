//! Runtime status sidecars for interactive Canopy Code sessions.
//!
//! The JSON on disk uses snake_case field names for compatibility with other
//! Canopy tooling. Unknown fields are ignored when reading so newer writers
//! can add forward-compatible metadata.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Number, Value};

use super::atomic_file_write::{AtomicWriteOptions, atomic_write_file};
use super::cancellation::{CancellationReason, CancellationToken};

pub const RUNTIME_STATUS_SCHEMA_VERSION: u32 = 1;

/// Snapshot of a live Canopy Code session process for external observers.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeStatus {
    pub schema_version: u32,
    /// JavaScript Number semantics are retained for the on-disk PID field.
    pub pid: f64,
    pub session_id: String,
    pub work_dir: String,
    pub hostname: String,
    /// Epoch seconds with millisecond precision when written by this module.
    pub started_at: f64,
    pub canopy_version: Option<String>,
}

/// Fields supplied by the session when writing its status sidecar.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteRuntimeStatusFields {
    pub session_id: String,
    pub work_dir: String,
    /// Defaults to the current process ID.
    pub pid: Option<f64>,
    /// Defaults to JSON null.
    pub canopy_version: Option<String>,
}

/// The only read error exposed by the TypeScript helper is an aborted read.
/// Rust uses a typed cancellation reason in place of AbortSignal.reason.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeStatusReadError {
    pub reason: Option<CancellationReason>,
}

impl fmt::Display for RuntimeStatusReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("runtime status read cancelled")
    }
}

impl std::error::Error for RuntimeStatusReadError {}

/// Write a runtime status file, creating its parent directory as needed.
///
/// The shared atomic writer preserves existing file mode and syncs the new
/// contents before replacing the destination. The returned path is the path
/// passed by the caller, without canonicalization.
pub async fn write_runtime_status(
    file_path: impl AsRef<Path>,
    fields: WriteRuntimeStatusFields,
) -> io::Result<PathBuf> {
    let file_path = file_path.as_ref().to_path_buf();
    let parent = file_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));

    let pid = fields.pid.unwrap_or_else(|| f64::from(std::process::id()));
    let payload = serde_json::json!({
        "schema_version": RUNTIME_STATUS_SCHEMA_VERSION,
        // JSON.stringify converts non-finite JavaScript numbers to null.
        "pid": json_number_or_null(pid),
        "session_id": fields.session_id,
        "work_dir": fields.work_dir,
        "hostname": hostname()?,
        "started_at": epoch_seconds_millis(),
        "canopy_version": fields.canopy_version,
    });
    let contents = serde_json::to_vec_pretty(&payload)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    tokio::fs::create_dir_all(parent).await?;
    atomic_write_file(&file_path, &contents, &AtomicWriteOptions::default())?;
    Ok(file_path)
}

/// Read a runtime status file if it exists and contains a supported record.
///
/// Missing files, I/O failures, malformed JSON after Node-style UTF-8
/// replacement decoding, and records with a wrong schema or field type return
/// Ok(None). Cancellation is checked before and after the asynchronous read
/// and after parsing, matching the source helper's checkpoints. Unknown JSON
/// fields are ignored.
pub async fn read_runtime_status(
    file_path: impl AsRef<Path>,
    cancellation: Option<&CancellationToken>,
) -> Result<Option<RuntimeStatus>, RuntimeStatusReadError> {
    throw_if_cancelled(cancellation)?;

    let read = tokio::fs::read(file_path);
    let result = if let Some(cancellation) = cancellation {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(cancelled_error(cancellation));
            }
            result = read => result,
        }
    } else {
        read.await
    };

    if let Some(cancellation) = cancellation.filter(|token| token.is_cancelled()) {
        return Err(cancelled_error(cancellation));
    }
    let bytes = match result {
        Ok(bytes) => bytes,
        Err(_) => return Ok(None),
    };
    throw_if_cancelled(cancellation)?;

    // Node's UTF-8 file decoding replaces invalid sequences rather than
    // rejecting the entire read. JSON parsing still rejects malformed text.
    let text = String::from_utf8_lossy(&bytes);
    let data: Value = match serde_json::from_str(&text) {
        Ok(data) => data,
        Err(_) => return Ok(None),
    };
    throw_if_cancelled(cancellation)?;

    Ok(parse_runtime_status(&data))
}

/// Remove a runtime status file. All errors are intentionally swallowed so
/// best-effort cleanup cannot disrupt surrounding control flow.
pub async fn clear_runtime_status(file_path: impl AsRef<Path>) {
    let _ = tokio::fs::remove_file(file_path).await;
}

fn parse_runtime_status(data: &Value) -> Option<RuntimeStatus> {
    let object = data.as_object()?;

    // Match the source's strict schema gate before checking the remaining
    // fields. JSON numbers are JavaScript numbers, so 1.0 is also version 1.
    let schema_version = finite_number(object.get("schema_version")?)?;
    if schema_version != f64::from(RUNTIME_STATUS_SCHEMA_VERSION)
        || !is_finite_integer(schema_version)
    {
        return None;
    }

    let pid = finite_number(object.get("pid")?)?;
    if !is_finite_integer(pid) {
        return None;
    }
    let session_id = object.get("session_id")?.as_str()?.to_owned();
    let work_dir = object.get("work_dir")?.as_str()?.to_owned();
    let hostname = object.get("hostname")?.as_str()?.to_owned();
    let started_at = finite_number(object.get("started_at")?)?;
    let canopy_version = match object.get("canopy_version")? {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        _ => return None,
    };

    Some(RuntimeStatus {
        schema_version: RUNTIME_STATUS_SCHEMA_VERSION,
        pid,
        session_id,
        work_dir,
        hostname,
        started_at,
        canopy_version,
    })
}

fn finite_number(value: &Value) -> Option<f64> {
    let number = value.as_f64()?;
    number.is_finite().then_some(number)
}

fn is_finite_integer(number: f64) -> bool {
    number.is_finite() && number.fract() == 0.0
}

fn json_number_or_null(number: f64) -> Value {
    // JavaScript JSON.stringify writes finite integer Numbers without a `.0`
    // suffix. Keep integer fields such as the process ID in that wire shape.
    const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;
    if number.is_finite() && number.fract() == 0.0 && number.abs() <= MAX_SAFE_INTEGER {
        return Value::Number(Number::from(number as i64));
    }
    Number::from_f64(number)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn epoch_seconds_millis() -> f64 {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    milliseconds as f64 / 1_000.0
}

#[cfg(unix)]
fn hostname() -> io::Result<String> {
    nix::unistd::gethostname()
        .map(|hostname| hostname.to_string_lossy().into_owned())
        .map_err(|error| io::Error::other(error.to_string()))
}

#[cfg(windows)]
fn hostname() -> io::Result<String> {
    std::env::var("COMPUTERNAME").map_err(|error| io::Error::new(io::ErrorKind::NotFound, error))
}

#[cfg(not(any(unix, windows)))]
fn hostname() -> io::Result<String> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "hostname lookup is not supported on this platform",
    ))
}

fn throw_if_cancelled(
    cancellation: Option<&CancellationToken>,
) -> Result<(), RuntimeStatusReadError> {
    if let Some(cancellation) = cancellation.filter(|token| token.is_cancelled()) {
        return Err(cancelled_error(cancellation));
    }
    Ok(())
}

fn cancelled_error(cancellation: &CancellationToken) -> RuntimeStatusReadError {
    RuntimeStatusReadError {
        reason: cancellation.reason(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use serde_json::{Value, json};

    use super::{
        RUNTIME_STATUS_SCHEMA_VERSION, RuntimeStatusReadError, WriteRuntimeStatusFields,
        clear_runtime_status, read_runtime_status, write_runtime_status,
    };
    use crate::utils::cancellation::{CancellationReason, CancellationToken};

    fn test_directory() -> PathBuf {
        std::env::temp_dir().join(format!(
            "canopy-runtime-status-{}",
            uuid::Uuid::new_v4().simple()
        ))
    }

    fn valid_payload() -> Value {
        json!({
            "schema_version": RUNTIME_STATUS_SCHEMA_VERSION,
            "pid": 4242,
            "session_id": "session-id",
            "work_dir": "/work/dir",
            "hostname": "canopy-host",
            "started_at": 1720000000.125,
            "canopy_version": null,
        })
    }

    async fn write_payload(path: &Path, value: &Value) {
        fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    }

    #[tokio::test]
    async fn writes_schema_defaults_and_round_trips() {
        let directory = test_directory();
        let path = directory.join("nested/runtime.json");
        let returned = write_runtime_status(
            &path,
            WriteRuntimeStatusFields {
                session_id: "中文-uuid-aaa".to_owned(),
                work_dir: "D:/项目/我的-app".to_owned(),
                pid: Some(4242.0),
                canopy_version: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(returned, path);

        let raw = fs::read_to_string(&path).unwrap();
        let value: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(value["schema_version"], RUNTIME_STATUS_SCHEMA_VERSION);
        assert_eq!(value["pid"], 4242);
        assert_eq!(value["session_id"], "中文-uuid-aaa");
        assert_eq!(value["work_dir"], "D:/项目/我的-app");
        assert!(
            value["hostname"]
                .as_str()
                .is_some_and(|host| !host.is_empty())
        );
        assert!(value["started_at"].as_f64().is_some());
        assert_eq!(value["canopy_version"], Value::Null);

        let status = read_runtime_status(&path, None).await.unwrap().unwrap();
        assert_eq!(status.schema_version, RUNTIME_STATUS_SCHEMA_VERSION);
        assert_eq!(status.pid, 4242.0);
        assert_eq!(status.session_id, "中文-uuid-aaa");
        assert_eq!(status.work_dir, "D:/项目/我的-app");
        assert_eq!(status.canopy_version, None);

        let entries = fs::read_dir(path.parent().unwrap()).unwrap().count();
        assert_eq!(entries, 1, "successful atomic write leaves no temp file");

        write_runtime_status(
            &path,
            WriteRuntimeStatusFields {
                session_id: "resume-session".to_owned(),
                work_dir: "/resumed".to_owned(),
                pid: Some(9001.0),
                canopy_version: Some("0.15.3".to_owned()),
            },
        )
        .await
        .unwrap();
        let overwritten = read_runtime_status(&path, None).await.unwrap().unwrap();
        assert_eq!(overwritten.pid, 9001.0);
        assert_eq!(overwritten.session_id, "resume-session");
        assert_eq!(overwritten.canopy_version.as_deref(), Some("0.15.3"));
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 1);
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn defaults_pid_and_preserves_explicit_version() {
        let directory = test_directory();
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("runtime.json");
        write_runtime_status(
            &path,
            WriteRuntimeStatusFields {
                session_id: "abc".to_owned(),
                work_dir: "/w".to_owned(),
                pid: None,
                canopy_version: Some("0.15.3".to_owned()),
            },
        )
        .await
        .unwrap();
        let value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(value["pid"], std::process::id());
        assert_eq!(value["canopy_version"], "0.15.3");
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn non_finite_write_pid_serializes_as_null_and_is_rejected_on_read() {
        let directory = test_directory();
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("runtime.json");
        write_runtime_status(
            &path,
            WriteRuntimeStatusFields {
                session_id: "abc".to_owned(),
                work_dir: "/w".to_owned(),
                pid: Some(f64::INFINITY),
                canopy_version: None,
            },
        )
        .await
        .unwrap();
        let value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(value["pid"], Value::Null);
        assert!(read_runtime_status(&path, None).await.unwrap().is_none());
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn returns_none_for_absent_malformed_and_unsupported_records() {
        let directory = test_directory();
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("runtime.json");
        assert!(read_runtime_status(&path, None).await.unwrap().is_none());

        fs::write(&path, b"not-json").unwrap();
        assert!(read_runtime_status(&path, None).await.unwrap().is_none());
        write_payload(&path, &json!([1, 2, 3])).await;
        assert!(read_runtime_status(&path, None).await.unwrap().is_none());

        let mut unknown_schema = valid_payload();
        unknown_schema["schema_version"] = json!(RUNTIME_STATUS_SCHEMA_VERSION + 1);
        write_payload(&path, &unknown_schema).await;
        assert!(read_runtime_status(&path, None).await.unwrap().is_none());

        fs::write(&path, [0xff, 0xfe, 0x20, 0x67]).unwrap();
        assert!(read_runtime_status(&path, None).await.unwrap().is_none());
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn validates_required_fields_without_coercion_but_ignores_unknown_fields() {
        let directory = test_directory();
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("runtime.json");

        let mut payload = valid_payload();
        payload["schema_version"] = json!(1.0);
        payload["pid"] = json!(-42);
        payload["future_field"] = json!({"anything": true});
        write_payload(&path, &payload).await;
        assert_eq!(
            read_runtime_status(&path, None).await.unwrap().unwrap().pid,
            -42.0
        );

        for (field, value) in [
            ("pid", json!("1234")),
            ("pid", json!(1.5)),
            ("session_id", Value::Null),
            ("work_dir", json!(["/", "w"])),
            ("hostname", json!({"name": "host"})),
            ("started_at", json!("now")),
            ("started_at", Value::Null),
            ("canopy_version", json!(false)),
        ] {
            let mut invalid = valid_payload();
            invalid[field] = value;
            write_payload(&path, &invalid).await;
            assert!(
                read_runtime_status(&path, None).await.unwrap().is_none(),
                "expected field {field} to be rejected"
            );
        }

        let mut missing_canopy_version = valid_payload();
        missing_canopy_version
            .as_object_mut()
            .unwrap()
            .remove("canopy_version");
        write_payload(&path, &missing_canopy_version).await;
        assert!(read_runtime_status(&path, None).await.unwrap().is_none());
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn replaces_invalid_utf8_inside_json_strings_like_node_decoding() {
        let directory = test_directory();
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("runtime.json");
        let mut bytes = serde_json::to_vec(&valid_payload()).unwrap();
        let hostname_start = bytes
            .windows(b"canopy-host".len())
            .position(|window| window == b"canopy-host")
            .unwrap();
        bytes[hostname_start] = 0xff;
        fs::write(&path, bytes).unwrap();
        let status = read_runtime_status(&path, None).await.unwrap().unwrap();
        assert!(status.hostname.contains(char::REPLACEMENT_CHARACTER));
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn propagates_typed_cancellation_and_clear_swallows_errors() {
        let directory = test_directory();
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("runtime.json");
        let cancellation = CancellationToken::new();
        cancellation.cancel_with_reason("read stopped");
        let error = read_runtime_status(&path, Some(&cancellation))
            .await
            .unwrap_err();
        assert_eq!(
            error,
            RuntimeStatusReadError {
                reason: Some(CancellationReason::Explicit("read stopped".into()))
            }
        );

        fs::write(&path, b"status").unwrap();
        clear_runtime_status(&path).await;
        clear_runtime_status(&path).await;
        assert!(!path.exists());
        clear_runtime_status(directory.join("missing/sidecar.json")).await;
        fs::remove_dir_all(directory).unwrap();
    }
}
