//! Session-scoped native memory sampling and file-cache cleanup.
//!
//! The shared pressure state machine runs while a CLI session is active. The
//! Rust CLI can evict its file-read cache and queue history microcompaction at
//! a safe provider boundary. Native Rust has no V8 heap counters or explicit
//! garbage-collection hook.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
#[cfg(target_os = "linux")]
use std::{fs::File, io::Read};

use canopy_core::file_read_cache::FileReadCache;
use canopy_core::services::memory_diagnostics::{
    MemoryDiagnosticsProbeOverrides, MemoryDiagnosticsSnapshot, MemoryUsageSnapshot,
    collect_memory_diagnostics,
};
use canopy_core::services::memory_diagnostics_dumper::{
    MemoryDiagnosticsDumper, MemoryDumpInput, MemoryDumpSnapshots,
};
use canopy_core::services::memory_pressure_monitor::MemoryPressureMeasurement;
use canopy_core::services::memory_pressure_monitor::{
    CleanupStepFuture, MemoryPressureLog, MemoryPressureLogLevel, MemoryPressureMonitorHooks,
};
use canopy_core::services::memory_pressure_policy::PressureLevel;
use canopy_core::services::memory_pressure_policy::{
    DEFAULT_PRESSURE_CONFIG, MemoryPressureConfig, validate_memory_pressure_config,
};
use canopy_core::services::memory_pressure_runtime::build_native_memory_pressure_monitor;
use canopy_core::services::native_memory_probe::NativeMemoryProbe;
use canopy_core::services::runtime_sample_ring::RuntimeSample;
use canopy_core::storage::Storage;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(15);

/// A task that samples process RSS during an active model session.
pub struct MemoryPressureTask {
    handle: JoinHandle<()>,
    compaction_requested: Arc<AtomicBool>,
    check_request_sender: mpsc::Sender<()>,
}

impl MemoryPressureTask {
    /// Start sampling while publishing the latest RSS pressure classification
    /// to the native managed-memory task manager. Unsupported platforms or
    /// failed system memory-limit probes leave the session running without it.
    pub fn start_with_pressure_state(
        file_cache: FileReadCache,
        session_id: String,
        pressure_detected: Arc<AtomicBool>,
    ) -> Option<Self> {
        let config = load_config();
        let probe = Arc::new(NativeMemoryProbe::system());
        let compaction_requested = Arc::new(AtomicBool::new(false));
        let cleanup_compaction_requested = Arc::clone(&compaction_requested);
        let uptime_start = Arc::new(Instant::now());
        let session_id = Arc::new(session_id);
        let mut hooks = MemoryPressureMonitorHooks::new(
            || Err("native measurement hook was not installed".to_owned()),
            move |step| cleanup_step(&file_cache, &cleanup_compaction_requested, step),
        );
        hooks.diagnostics_dumper = build_diagnostics_dumper(
            Arc::clone(&probe),
            Arc::clone(&uptime_start),
            Arc::clone(&session_id),
        );
        let process_id = std::process::id();
        let dump_probe = Arc::clone(&probe);
        let dump_uptime_start = Arc::clone(&uptime_start);
        let dump_session_id = Arc::clone(&session_id);
        let canopy_version = env!("CARGO_PKG_VERSION").to_owned();
        hooks.diagnostics_input = Arc::new(move |samples| {
            diagnostics_dump_input(
                samples,
                process_id,
                dump_session_id.as_str(),
                &canopy_version,
                dump_uptime_start.elapsed().as_secs_f64(),
                &dump_probe,
            )
        });
        hooks.log = Arc::new(log_pressure_message);

        let monitor = match build_native_memory_pressure_monitor(config, probe, hooks) {
            Ok(monitor) => Arc::new(monitor),
            Err(error) => {
                eprintln!("[CANOPY] WARNING: Memory-pressure sampling is unavailable: {error}");
                return None;
            }
        };

        let (check_request_sender, mut check_request_receiver) = mpsc::channel(1);
        let monitor_pressure_state = Arc::clone(&pressure_detected);
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            interval.tick().await;
            monitor.perform_check().await;
            publish_pressure_state(&monitor, &monitor_pressure_state);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        while check_request_receiver.try_recv().is_ok() {}
                        if monitor.schedule_check() {
                            monitor.run_scheduled_check().await;
                            publish_pressure_state(&monitor, &monitor_pressure_state);
                        }
                    }
                    request = check_request_receiver.recv() => {
                        if request.is_none() {
                            break;
                        }
                        while check_request_receiver.try_recv().is_ok() {}
                        if monitor.schedule_check() {
                            monitor.run_scheduled_check().await;
                            publish_pressure_state(&monitor, &monitor_pressure_state);
                        }
                    }
                }
            }
        });
        Some(Self {
            handle,
            compaction_requested,
            check_request_sender,
        })
    }

    /// A shared flag consumed by the agent runtime at the next safe point
    /// between provider turns.
    pub fn compaction_requested(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.compaction_requested)
    }

    /// Return a bounded, non-blocking callback that coalesces pressure checks.
    pub fn check_requester(&self) -> Arc<dyn Fn() + Send + Sync> {
        let sender = self.check_request_sender.clone();
        Arc::new(move || {
            let _ = sender.try_send(());
        })
    }
}

fn publish_pressure_state(
    monitor: &canopy_core::services::memory_pressure_monitor::MemoryPressureMonitor,
    pressure_detected: &AtomicBool,
) {
    let under_pressure = monitor.runtime_samples().last().is_some_and(|sample| {
        monitor.get_pressure_level(MemoryPressureMeasurement {
            memory_usage: canopy_core::services::runtime_sample_ring::MemoryUsage {
                rss: sample.rss,
                heap_used: sample.heap_used,
                heap_total: sample.heap_total,
                external: sample.external,
            },
            heap_size_limit_bytes: 0,
        }) != PressureLevel::Normal
    });
    pressure_detected.store(under_pressure, Ordering::Release);
}

fn build_diagnostics_dumper(
    probe: Arc<NativeMemoryProbe>,
    uptime_start: Arc<Instant>,
    session_id: Arc<String>,
) -> Option<Arc<MemoryDiagnosticsDumper>> {
    let cwd = std::env::current_dir().ok()?;
    let project_dir = Storage::new(cwd).get_project_dir();
    let version = env!("CARGO_PKG_VERSION").to_owned();

    Some(Arc::new(MemoryDiagnosticsDumper::new(
        project_dir,
        session_id.as_str(),
        version.clone(),
        move |process_run_id, canopy_version| {
            let probe = Arc::clone(&probe);
            let uptime_start = Arc::clone(&uptime_start);
            let version = version.clone();
            async move {
                let mut snapshot = native_snapshot_metadata(
                    process_run_id,
                    canopy_version,
                    &version,
                    uptime_start.elapsed().as_secs_f64(),
                );
                if let Ok(memory) = probe.memory_usage() {
                    snapshot.memory_usage = MemoryUsageSnapshot {
                        heap_used: memory.heap_used,
                        heap_total: memory.heap_total,
                        rss: memory.rss,
                        external: memory.external,
                        array_buffers: 0,
                    };
                }
                if let Some(cpu) = probe.process_cpu_usage() {
                    snapshot.user_cpu_time = u64::try_from(cpu.user_us).unwrap_or_default();
                    snapshot.system_cpu_time = u64::try_from(cpu.system_us).unwrap_or_default();
                }
                snapshot.max_rss_raw = peak_rss_kib().unwrap_or_default();
                let diagnostics = collect_memory_diagnostics(
                    snapshot,
                    MemoryDiagnosticsProbeOverrides::default(),
                )
                .await;
                serde_json::to_value(diagnostics)
                    .map_err(|error| format!("could not serialize memory diagnostics: {error}"))
            }
        },
    )))
}

fn native_snapshot_metadata(
    session_id: String,
    canopy_version: String,
    package_version: &str,
    uptime_seconds: f64,
) -> MemoryDiagnosticsSnapshot {
    MemoryDiagnosticsSnapshot {
        session_id: Some(session_id),
        canopy_version: Some(canopy_version),
        uptime_seconds,
        node_version: format!("native-rust/{package_version}"),
        ..MemoryDiagnosticsSnapshot::default()
    }
}

fn diagnostics_dump_input(
    recent_samples: Vec<RuntimeSample>,
    process_id: u32,
    session_id: &str,
    canopy_version: &str,
    uptime_seconds: f64,
    probe: &NativeMemoryProbe,
) -> MemoryDumpInput {
    // The monitor has just recorded the RSS sample that caused a hard or
    // critical dump. Reuse that bounded sample rather than issuing another OS
    // query while under pressure. The probe is only a fallback if no sample is
    // available (for example, a future caller invoking the dumper directly).
    let memory_usage = recent_samples
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
        .or_else(|| {
            probe.memory_usage().ok().map(|memory| {
                json!({
                    "rss": memory.rss,
                    "heapUsed": memory.heap_used,
                    "heapTotal": memory.heap_total,
                    "external": memory.external,
                    "arrayBuffers": 0,
                })
            })
        });

    let process_rss = memory_usage
        .as_ref()
        .and_then(|memory| memory.get("rss"))
        .and_then(serde_json::Value::as_u64);
    let system_memory = probe
        .system_memory_snapshot()
        .map(|snapshot| {
            json!({
                "available": true,
                "scope": "host",
                "totalBytes": snapshot.host_total_bytes,
                "availableBytes": snapshot.available_bytes,
                "availableBytesMethod": snapshot.available_bytes_method,
                "effectiveLimitBytes": snapshot.effective_limit_bytes,
            })
        })
        .unwrap_or_else(|_| json!({"available": false, "scope": "host"}));

    MemoryDumpInput {
        snapshots: MemoryDumpSnapshots {
            memory_usage,
            v8_heap_stats: json!({
                "available": false,
                "heapCountersAvailable": false,
                "runtime": "native-rust",
            }),
            session: json!({
                "available": false,
                "sessionId": session_id,
                "processId": process_id,
                "runtime": "native-rust",
                "canopyVersion": canopy_version,
                "platform": native_platform_name(),
                "architecture": std::env::consts::ARCH,
                "uptimeSeconds": uptime_seconds,
                "nativeMemory": {
                    "heapCountersAvailable": false,
                    "processRssBytes": process_rss,
                    "systemMemory": system_memory,
                    "swap": native_swap_snapshot(probe),
                },
            }),
        },
        recent_samples,
    }
}

fn native_swap_snapshot(probe: &NativeMemoryProbe) -> serde_json::Value {
    match probe.system_swap_usage() {
        Ok(swap) => json!({
            "available": true,
            "scope": "system",
            "totalBytes": swap.total_bytes,
            "usedBytes": swap.used_bytes,
            "freeBytes": swap.free_bytes,
        }),
        Err(_) => json!({
            "available": false,
            "scope": "system",
        }),
    }
}

/// Linux exposes lifetime high-water RSS directly in procfs. Bound the read
/// even though this kernel file is normally small so a diagnostics path never
/// performs an unbounded allocation. The current safe process
/// probes do not expose a peak-RSS counter on macOS, so it remains unavailable
/// there rather than reporting current RSS as a lifetime maximum.
pub(super) fn peak_rss_kib() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        const MAX_STATUS_BYTES: u64 = 64 * 1024;
        let mut status = String::with_capacity(MAX_STATUS_BYTES as usize);
        File::open("/proc/self/status")
            .ok()?
            .take(MAX_STATUS_BYTES)
            .read_to_string(&mut status)
            .ok()?;
        parse_linux_peak_rss_kib(&status)
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_peak_rss_kib(status: &str) -> Option<u64> {
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    let mut fields = line.split_whitespace();
    if fields.next()? != "VmHWM:" {
        return None;
    }
    let value = fields.next()?.parse::<u64>().ok()?;
    (fields.next()? == "kB").then_some(value)
}

fn native_platform_name() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        os => os,
    }
}

#[cfg(test)]
mod tests {
    use super::{diagnostics_dump_input, native_snapshot_metadata, parse_linux_peak_rss_kib};
    use canopy_core::services::native_memory_probe::NativeMemoryProbe;
    use canopy_core::services::runtime_sample_ring::RuntimeSample;
    use serde_json::json;

    #[test]
    fn phase_one_input_reuses_the_native_rss_and_cpu_sample() {
        let probe = NativeMemoryProbe::system();
        let input = diagnostics_dump_input(
            vec![RuntimeSample {
                ts: 1_780_176_000_123,
                rss: 123_456_789,
                heap_used: 0,
                heap_total: 0,
                external: 0,
                cpu_percent: 37.25,
            }],
            42,
            "session-abc123",
            "0.1.0",
            99.5,
            &probe,
        );

        assert_eq!(
            input.snapshots.memory_usage,
            Some(json!({
                "rss": 123_456_789,
                "heapUsed": 0,
                "heapTotal": 0,
                "external": 0,
                "arrayBuffers": 0,
            }))
        );
        assert_eq!(input.recent_samples[0].rss, 123_456_789);
        assert_eq!(input.recent_samples[0].cpu_percent, 37.25);
        assert_eq!(input.snapshots.v8_heap_stats["available"], false);
        assert_eq!(input.snapshots.v8_heap_stats["runtime"], "native-rust");
        assert_eq!(input.snapshots.session["sessionId"], "session-abc123");
        assert_eq!(input.snapshots.session["processId"], 42);
        assert_eq!(input.snapshots.session["canopyVersion"], "0.1.0");
        assert_eq!(input.snapshots.session["uptimeSeconds"], 99.5);
    }

    #[test]
    fn parses_linux_peak_rss_from_vm_hwm_only() {
        let status = "Name:\tcanopy\nVmRSS:\t100 kB\nVmHWM:\t2048 kB\n";
        assert_eq!(parse_linux_peak_rss_kib(status), Some(2048));
        assert_eq!(parse_linux_peak_rss_kib("VmRSS:\t100 kB\n"), None);
        assert_eq!(parse_linux_peak_rss_kib("VmHWM:\t2 MB\n"), None);
        assert_eq!(parse_linux_peak_rss_kib("VmHWM:\tnope kB\n"), None);
    }

    #[test]
    fn full_diagnostics_metadata_uses_the_selected_session_id() {
        let snapshot = native_snapshot_metadata(
            "selected-session-123".to_owned(),
            "0.1.0".to_owned(),
            "0.1.0",
            12.75,
        );

        assert_eq!(snapshot.session_id.as_deref(), Some("selected-session-123"));
        assert_eq!(snapshot.canopy_version.as_deref(), Some("0.1.0"));
        assert_eq!(snapshot.node_version, "native-rust/0.1.0");
        assert_eq!(snapshot.uptime_seconds, 12.75);
    }
}

impl Drop for MemoryPressureTask {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

pub(super) fn load_config() -> MemoryPressureConfig {
    let mut config = DEFAULT_PRESSURE_CONFIG;
    let result = (|| {
        config.soft_pressure_ratio =
            ratio_from_env("CANOPY_MEMORY_PRESSURE_SOFT", config.soft_pressure_ratio)?;
        config.hard_pressure_ratio =
            ratio_from_env("CANOPY_MEMORY_PRESSURE_HARD", config.hard_pressure_ratio)?;
        config.critical_ratio =
            ratio_from_env("CANOPY_MEMORY_PRESSURE_CRITICAL", config.critical_ratio)?;
        if let Ok(value) = std::env::var("CANOPY_MEMORY_ENABLE_GC") {
            if ["0", "false", "off", "no"]
                .iter()
                .any(|disabled| value.trim().eq_ignore_ascii_case(disabled))
            {
                config.enable_explicit_gc = false;
            }
        }
        validate_memory_pressure_config(&config).map_err(str::to_owned)
    })();

    if let Err(error) = result {
        eprintln!(
            "[CANOPY] WARNING: Invalid memory pressure config; using defaults. Error: {error}"
        );
        DEFAULT_PRESSURE_CONFIG
    } else {
        config
    }
}

fn ratio_from_env(name: &str, fallback: f64) -> Result<f64, String> {
    let Ok(value) = std::env::var(name) else {
        return Ok(fallback);
    };
    if value.is_empty() {
        return Ok(fallback);
    }
    value
        .parse::<f64>()
        .map_err(|_| format!("{name} must be a finite number"))
}

fn cleanup_step(
    file_cache: &FileReadCache,
    compaction_requested: &AtomicBool,
    step: canopy_core::services::memory_pressure_policy::CleanupStep,
) -> CleanupStepFuture {
    let result = match step {
        canopy_core::services::memory_pressure_policy::CleanupStep::EvictStaleCache => {
            let evicted = file_cache.evict_not_accessed_since(60.0);
            Ok(format!("evicted {evicted} stale file-cache entries"))
        }
        canopy_core::services::memory_pressure_policy::CleanupStep::EvictColdCache => {
            let evicted = file_cache.evict_not_accessed_since(30.0);
            Ok(format!("evicted {evicted} cold file-cache entries"))
        }
        canopy_core::services::memory_pressure_policy::CleanupStep::ClearFileCache => {
            file_cache.clear();
            Ok("cleared file-read cache".to_owned())
        }
        canopy_core::services::memory_pressure_policy::CleanupStep::CompactHistory => {
            compaction_requested.store(true, Ordering::Release);
            Ok("queued history compaction for the next safe provider boundary".to_owned())
        }
        canopy_core::services::memory_pressure_policy::CleanupStep::TriggerGc => {
            static REPORTED: OnceLock<()> = OnceLock::new();
            if REPORTED.set(()).is_ok() {
                eprintln!(
                    "[CANOPY] WARNING: Explicit garbage collection is unavailable in the Rust CLI"
                );
            }
            Ok("explicit garbage collection unavailable".to_owned())
        }
    };
    Box::pin(async move { result.map(|_| ()) })
}

fn log_pressure_message(message: MemoryPressureLog) {
    match message.level {
        MemoryPressureLogLevel::Debug => {}
        MemoryPressureLogLevel::Info => eprintln!("[CANOPY] {}", message.message),
        MemoryPressureLogLevel::Warn => eprintln!("[CANOPY] WARNING: {}", message.message),
        MemoryPressureLogLevel::Error => eprintln!("[CANOPY] ERROR: {}", message.message),
    }
}
