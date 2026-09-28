//! Two-phase, crash-surviving memory diagnostics dump.
//!
//! Port of `packages/core/src/services/memoryDiagnosticsDumper.ts`. The first
//! JSON write is synchronous and happens before the full-collection future is
//! awaited. A caller supplies process/V8 snapshots and an async diagnostics
//! collector: this crate has no V8 heap-statistics or Node process-memory API.
//! Supplying recent [`RuntimeSample`]s reuses their memory values for phase 1,
//! avoiding a second memory sample while the process is under pressure.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::runtime_sample_ring::RuntimeSample;

/// Maximum successful-or-failed dump reservations during one session.
pub const MAX_DUMPS_PER_SESSION: u8 = 3;
/// Minimum interval between dump reservations.
pub const MIN_DUMP_INTERVAL_MS: i64 = 30_000;

type CollectionFuture = Pin<Box<dyn Future<Output = Result<Value, String>> + Send>>;
type DumpFuture = Pin<Box<dyn Future<Output = Option<MemoryDumpResult>> + Send>>;
type FullDiagnosticsCollector = Arc<dyn Fn(String, String) -> CollectionFuture + Send + Sync>;
type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Pressure reason that triggered the dump.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryDumpTrigger {
    Hard,
    Critical,
}

impl MemoryDumpTrigger {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hard => "hard",
            Self::Critical => "critical",
        }
    }

    const fn suggestion(self) -> &'static str {
        match self {
            Self::Critical => {
                "Memory is critically high. Consider running /compress or starting a fresh session to avoid OOM."
            }
            Self::Hard => {
                "Memory pressure detected. Running /compress may help reduce memory usage."
            }
        }
    }
}

/// Injected snapshots that the Node implementation reads from `process` and
/// `node:v8`. `memory_usage` is used only if `recent_samples` is empty.
#[derive(Clone, Debug, Default)]
pub struct MemoryDumpSnapshots {
    /// `process.memoryUsage()`-shaped JSON. Its keys should use source names
    /// such as `heapUsed` and `arrayBuffers`.
    pub memory_usage: Option<Value>,
    /// `v8.getHeapStatistics()`-shaped JSON.
    pub v8_heap_stats: Value,
    /// Session metadata snapshot. Use `{"available":false}` when the chat
    /// client or history count is unavailable.
    pub session: Value,
}

/// Call arguments for one dump attempt.
#[derive(Clone, Debug, Default)]
pub struct MemoryDumpInput {
    pub snapshots: MemoryDumpSnapshots,
    pub recent_samples: Vec<RuntimeSample>,
}

/// Successful two-phase dump result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryDumpResult {
    pub file_path: PathBuf,
    pub trigger: MemoryDumpTrigger,
}

#[derive(Debug, Default)]
struct ReservationState {
    dump_count: u8,
    last_dump_time: i64,
}

/// Writes a small diagnostic file before starting full diagnostics collection.
///
/// The reservation mutex is held only while checking and consuming the cap and
/// cooldown slot. The reservation is synchronous and is never rolled back,
/// including when directory creation, either write, or full collection fails.
pub struct MemoryDiagnosticsDumper {
    project_dir: PathBuf,
    session_id: String,
    canopy_version: String,
    collector: FullDiagnosticsCollector,
    clock: Clock,
    reservation: Mutex<ReservationState>,
}

impl MemoryDiagnosticsDumper {
    /// Build a dumper with the system clock and an injected asynchronous full
    /// diagnostics collector.
    pub fn new<C, Fut>(
        project_dir: impl Into<PathBuf>,
        session_id: impl Into<String>,
        canopy_version: impl Into<String>,
        collector: C,
    ) -> Self
    where
        C: Fn(String, String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, String>> + Send + 'static,
    {
        Self::with_clock(
            project_dir,
            session_id,
            canopy_version,
            collector,
            system_time_millis,
        )
    }

    /// Build a dumper with an injectable Unix-millisecond clock. This is useful
    /// for deterministic cooldown behavior in tests and embedders.
    pub fn with_clock<C, Fut, K>(
        project_dir: impl Into<PathBuf>,
        session_id: impl Into<String>,
        canopy_version: impl Into<String>,
        collector: C,
        clock: K,
    ) -> Self
    where
        C: Fn(String, String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, String>> + Send + 'static,
        K: Fn() -> i64 + Send + Sync + 'static,
    {
        Self {
            project_dir: project_dir.into(),
            session_id: session_id.into(),
            canopy_version: canopy_version.into(),
            collector: Arc::new(move |session_id, version| {
                Box::pin(collector(session_id, version))
            }),
            clock: Arc::new(clock),
            reservation: Mutex::new(ReservationState::default()),
        }
    }

    /// Reset cap and cooldown state when a new session starts.
    pub fn reset_for_new_session(&self) {
        let mut state = self
            .reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.dump_count = 0;
        state.last_dump_time = 0;
    }

    /// Attempt a dump. Reservation and phase 1 run immediately during this
    /// method call, before the returned future is awaited. Returns `None` for a
    /// cap/cooldown skip or any failed filesystem/collection step, matching
    /// the source service's fail-closed behavior. The phase-1 file is left in
    /// place when full collection fails.
    pub fn dump(&self, trigger: MemoryDumpTrigger, input: MemoryDumpInput) -> DumpFuture {
        let (dump_number, reservation_time) = {
            let mut state = self
                .reservation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.dump_count >= MAX_DUMPS_PER_SESSION {
                return Box::pin(std::future::ready(None));
            }
            let reservation_time = (self.clock)();
            if reservation_time.saturating_sub(state.last_dump_time) < MIN_DUMP_INTERVAL_MS {
                return Box::pin(std::future::ready(None));
            }
            state.dump_count += 1;
            state.last_dump_time = reservation_time;
            (state.dump_count, reservation_time)
        };

        let diagnostics_dir = self.project_dir.join("diagnostics");
        if std::fs::create_dir_all(&diagnostics_dir).is_err() {
            return Box::pin(std::future::ready(None));
        }

        let filename_timestamp = iso_timestamp(reservation_time);
        let filename_timestamp = filename_timestamp.replace(':', "-").replace('.', "_");
        let session_prefix =
            String::from_utf16_lossy(&self.session_id.encode_utf16().take(8).collect::<Vec<_>>());
        let filename = format!("memory-{session_prefix}-{filename_timestamp}.json");
        let file_path = diagnostics_dir.join(filename);
        let payload_timestamp = iso_timestamp(reservation_time);

        // Reuse the newest ring sample instead of asking the caller to make a
        // fresh process-memory query on the pressure path. arrayBuffers is not
        // tracked by RuntimeSample and remains zero in this minimal snapshot.
        let memory_usage = input
            .recent_samples
            .last()
            .map(|sample| {
                json!({
                    "rss": sample.rss,
                    "heapUsed": sample.heap_used,
                    "heapTotal": sample.heap_total,
                    "external": sample.external,
                    "arrayBuffers": 0,
                })
            })
            .or(input.snapshots.memory_usage.clone())
            .unwrap_or_else(|| json!({}));

        let phase_one = json!({
            "trigger": trigger.as_str(),
            "dumpNumber": dump_number,
            "timestamp": payload_timestamp,
            "memoryUsage": memory_usage,
            "v8HeapStats": input.snapshots.v8_heap_stats,
            "recentSamples": serialize_samples(&input.recent_samples),
            "session": normalized_session(input.snapshots.session.clone()),
            "suggestion": trigger.suggestion(),
            "collectionComplete": false,
        });
        if write_json(&file_path, &phase_one).is_err() {
            return Box::pin(std::future::ready(None));
        }

        // Creating this future after the phase-one write mirrors the source
        // method's call order; awaiting the returned future performs phase 2.
        let collection = (self.collector)(self.session_id.clone(), self.canopy_version.clone());
        Box::pin(async move {
            let diagnostics = collection.await.ok()?;
            let mut full_payload = Map::new();
            full_payload.insert("trigger".to_owned(), json!(trigger.as_str()));
            full_payload.insert("dumpNumber".to_owned(), json!(dump_number));
            if let Value::Object(diagnostics) = diagnostics {
                full_payload.extend(diagnostics);
            }
            full_payload.insert(
                "recentSamples".to_owned(),
                serialize_samples(&input.recent_samples),
            );
            full_payload.insert(
                "session".to_owned(),
                normalized_session(input.snapshots.session),
            );
            full_payload.insert("suggestion".to_owned(), json!(trigger.suggestion()));
            full_payload.insert("collectionComplete".to_owned(), json!(true));
            if write_json(&file_path, &Value::Object(full_payload)).is_err() {
                return None;
            }

            Some(MemoryDumpResult { file_path, trigger })
        })
    }
}

fn normalized_session(value: Value) -> Value {
    match value {
        Value::Object(_) => value,
        Value::Null => json!({"available": false}),
        _ => json!({"available": false}),
    }
}

fn serialize_samples(samples: &[RuntimeSample]) -> Value {
    Value::Array(
        samples
            .iter()
            .map(|sample| {
                json!({
                    "ts": sample.ts,
                    "rss": sample.rss,
                    "heapUsed": sample.heap_used,
                    "heapTotal": sample.heap_total,
                    "external": sample.external,
                    "cpuPercent": sample.cpu_percent,
                })
            })
            .collect(),
    )
}

fn write_json(path: &Path, value: &Value) -> std::io::Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?;
    std::fs::write(path, bytes)
}

fn iso_timestamp(unix_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(unix_ms)
        .unwrap_or_else(Utc::now)
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn system_time_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_DUMPS_PER_SESSION, MIN_DUMP_INTERVAL_MS, MemoryDiagnosticsDumper, MemoryDumpInput,
        MemoryDumpSnapshots, MemoryDumpTrigger, iso_timestamp,
    };
    use crate::services::runtime_sample_ring::RuntimeSample;
    use serde_json::{Value, json};
    use std::future::Future;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};
    use uuid::Uuid;

    fn unique_dir() -> PathBuf {
        std::env::temp_dir().join(format!("canopy-memory-dump-test-{}", Uuid::new_v4()))
    }

    fn input() -> MemoryDumpInput {
        MemoryDumpInput {
            snapshots: MemoryDumpSnapshots {
                memory_usage: Some(json!({
                    "rss": 900,
                    "heapUsed": 800,
                    "heapTotal": 1000,
                    "external": 70,
                    "arrayBuffers": 5,
                })),
                v8_heap_stats: json!({"used_heap_size": 700}),
                session: json!({"historyEntries": 500}),
            },
            recent_samples: Vec::new(),
        }
    }

    fn make_dumper<C, Fut>(
        dir: PathBuf,
        clock: impl Fn() -> i64 + Send + Sync + 'static,
        collector: C,
    ) -> MemoryDiagnosticsDumper
    where
        C: Fn(String, String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, String>> + Send + 'static,
    {
        MemoryDiagnosticsDumper::with_clock(dir, "session-12345678", "0.17.0", collector, clock)
    }

    fn read_json(path: &std::path::Path) -> Value {
        serde_json::from_slice(&std::fs::read(path).expect("diagnostic file exists"))
            .expect("diagnostic JSON parses")
    }

    #[tokio::test]
    async fn phase_one_is_written_before_collector_is_polled_and_ring_sample_is_reused() {
        let dir = unique_dir();
        let clock_value = 1_780_176_000_123;
        let iso = iso_timestamp(clock_value);
        let filename_timestamp = iso.replace(':', "-").replace('.', "_");
        let expected_path = dir
            .join("diagnostics")
            .join(format!("memory-session--{filename_timestamp}.json"));
        let collector_path = expected_path.clone();
        let collector = move |session_id, version| {
            let expected_path = collector_path.clone();
            async move {
                assert_eq!(session_id, "session-12345678");
                assert_eq!(version, "0.17.0");
                let phase_one = read_json(&expected_path);
                assert_eq!(phase_one["collectionComplete"], false);
                assert_eq!(phase_one["trigger"], "critical");
                assert_eq!(phase_one["memoryUsage"]["rss"], 111);
                assert_eq!(phase_one["memoryUsage"]["heapUsed"], 222);
                assert_eq!(phase_one["memoryUsage"]["arrayBuffers"], 0);
                assert_eq!(phase_one["v8HeapStats"]["used_heap_size"], 700);
                assert_eq!(phase_one["session"]["historyEntries"], 500);
                assert_eq!(phase_one["recentSamples"][0]["cpuPercent"], 12.5);
                Ok(json!({
                    "memoryUsage": {"rss": 999},
                    "extraDiagnostics": "full",
                }))
            }
        };
        let dumper = make_dumper(dir.clone(), move || clock_value, collector);
        let mut dump_input = input();
        dump_input.recent_samples.push(RuntimeSample {
            ts: clock_value,
            rss: 111,
            heap_used: 222,
            heap_total: 333,
            external: 44,
            cpu_percent: 12.5,
        });
        let dump_future = dumper.dump(MemoryDumpTrigger::Critical, dump_input);
        // Rust futures normally defer their body until polling. This API does
        // the phase-one work before returning, so it survives a caller that
        // crashes or abandons the full-collection future.
        assert!(expected_path.exists());
        assert_eq!(read_json(&expected_path)["collectionComplete"], false);
        let result = dump_future.await.expect("phase-two dump succeeds");
        assert_eq!(result.file_path, expected_path);
        assert!(
            result
                .file_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("memory-session--")
        );
        let phase_two = read_json(&result.file_path);
        assert_eq!(phase_two["collectionComplete"], true);
        assert_eq!(phase_two["trigger"], "critical");
        assert!(
            phase_two["suggestion"]
                .as_str()
                .unwrap()
                .contains("critically high")
        );
        assert_eq!(phase_two["recentSamples"][0]["cpuPercent"], 12.5);
        assert_eq!(phase_two["extraDiagnostics"], "full");
        // Collector data supplies the full memory snapshot.
        assert_eq!(phase_two["memoryUsage"]["rss"], 999);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn applies_cap_cooldown_and_reset() {
        let dir = unique_dir();
        let now = Arc::new(AtomicI64::new(60_000));
        let clock = {
            let now = Arc::clone(&now);
            move || now.load(Ordering::SeqCst)
        };
        let dumper = make_dumper(dir.clone(), clock, |_, _| async { Ok(json!({})) });

        assert!(
            dumper
                .dump(MemoryDumpTrigger::Hard, input())
                .await
                .is_some()
        );
        assert!(
            dumper
                .dump(MemoryDumpTrigger::Hard, input())
                .await
                .is_none()
        );
        now.fetch_add(MIN_DUMP_INTERVAL_MS, Ordering::SeqCst);
        assert!(
            dumper
                .dump(MemoryDumpTrigger::Hard, input())
                .await
                .is_some()
        );
        now.fetch_add(MIN_DUMP_INTERVAL_MS, Ordering::SeqCst);
        assert!(
            dumper
                .dump(MemoryDumpTrigger::Hard, input())
                .await
                .is_some()
        );
        now.fetch_add(MIN_DUMP_INTERVAL_MS, Ordering::SeqCst);
        assert!(
            dumper
                .dump(MemoryDumpTrigger::Hard, input())
                .await
                .is_none()
        );

        dumper.reset_for_new_session();
        now.fetch_add(MIN_DUMP_INTERVAL_MS, Ordering::SeqCst);
        assert!(
            dumper
                .dump(MemoryDumpTrigger::Critical, input())
                .await
                .is_some()
        );
        assert_eq!(MAX_DUMPS_PER_SESSION, 3);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn falls_back_to_the_injected_memory_snapshot_without_ring_samples() {
        let dir = unique_dir();
        let clock_value = 1_780_176_000_123;
        let dumper = make_dumper(
            dir.clone(),
            move || clock_value,
            |_, _| async { Ok(json!({})) },
        );
        let dump_future = dumper.dump(MemoryDumpTrigger::Hard, input());
        let phase_one_path = std::fs::read_dir(dir.join("diagnostics"))
            .expect("phase-one file was written synchronously")
            .next()
            .expect("one phase-one file")
            .expect("directory entry")
            .path();
        let phase_one = read_json(&phase_one_path);
        assert_eq!(phase_one["memoryUsage"]["rss"], 900);
        assert_eq!(phase_one["memoryUsage"]["arrayBuffers"], 5);
        assert_eq!(phase_one["collectionComplete"], false);
        assert!(dump_future.await.is_some());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn concurrent_calls_reserve_slots_before_any_collection_is_awaited() {
        let dir = unique_dir();
        let now = Arc::new(AtomicI64::new(60_000));
        let clock = {
            let now = Arc::clone(&now);
            move || now.fetch_add(MIN_DUMP_INTERVAL_MS, Ordering::SeqCst)
        };
        let collector = |_, _| std::future::pending::<Result<Value, String>>();
        let dumper = Arc::new(make_dumper(dir.clone(), clock, collector));
        // Calling dump performs reservation and phase 1 synchronously, even
        // though these returned full-collection futures are never polled.
        let mut attempts: Vec<_> = (0..4)
            .map(|_| {
                let dumper = Arc::clone(&dumper);
                dumper.dump(MemoryDumpTrigger::Hard, input())
            })
            .collect();
        assert_eq!(attempts.len(), 4);
        assert!(attempts.pop().unwrap().await.is_none());
        let remaining_files: Vec<_> = std::fs::read_dir(dir.join("diagnostics"))
            .expect("phase-one files were written before awaiting")
            .map(|entry| entry.expect("diagnostic directory entry").path())
            .collect();
        assert_eq!(remaining_files.len(), MAX_DUMPS_PER_SESSION as usize);
        drop(attempts);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn full_collection_failures_consume_the_reserved_session_slots() {
        let dir = unique_dir();
        let now = Arc::new(AtomicI64::new(60_000));
        let clock = {
            let now = Arc::clone(&now);
            move || now.fetch_add(MIN_DUMP_INTERVAL_MS, Ordering::SeqCst)
        };
        let collector = |_, _| async { Err("full collection failed".to_owned()) };
        let dumper = make_dumper(dir.clone(), clock, collector);
        for _ in 0..MAX_DUMPS_PER_SESSION {
            assert!(
                dumper
                    .dump(MemoryDumpTrigger::Hard, input())
                    .await
                    .is_none()
            );
        }
        assert!(
            dumper
                .dump(MemoryDumpTrigger::Hard, input())
                .await
                .is_none()
        );
        let remaining_files: Vec<_> = std::fs::read_dir(dir.join("diagnostics"))
            .expect("phase-one files remain after collection failures")
            .map(|entry| entry.expect("diagnostic directory entry").path())
            .collect();
        assert_eq!(remaining_files.len(), MAX_DUMPS_PER_SESSION as usize);
        for path in remaining_files {
            assert_eq!(read_json(&path)["collectionComplete"], false);
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
