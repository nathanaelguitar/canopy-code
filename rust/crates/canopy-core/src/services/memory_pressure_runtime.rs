//! Native runtime adapter for the provider-neutral memory pressure monitor.
//!
//! The adapter resolves the effective host/cgroup memory limit once at
//! construction, then uses live RSS and optional CPU readings from
//! [`NativeMemoryProbe`] for subsequent monitor samples. Cleanup, clock,
//! telemetry, diagnostics, logging, and event hooks remain caller supplied.

use std::io;
use std::sync::Arc;

use thiserror::Error;

use super::memory_pressure_monitor::{
    MemoryPressureMeasurement, MemoryPressureMonitor, MemoryPressureMonitorHooks,
};
use super::memory_pressure_policy::{MemoryPressureConfig, validate_memory_pressure_config};
use super::native_memory_probe::NativeMemoryProbe;

/// Errors encountered while assembling a native memory pressure monitor.
#[derive(Debug, Error)]
pub enum NativeMemoryRuntimeError {
    #[error("invalid memory pressure configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("could not determine effective memory limit: {0}")]
    EffectiveMemoryLimit(#[source] io::Error),
}

/// Build a monitor backed by native process memory and CPU probes.
///
/// The effective memory limit is read exactly once and stored by the monitor.
/// Each later pressure check reads the current process RSS. Native runtimes do
/// not expose V8 heap subdivisions, so heap usage and heap limit are zero and
/// only RSS contributes to pressure. CPU sampling is optional and safely
/// falls back to zero when the platform probe is unavailable.
///
/// `hooks.measure` and `hooks.cpu_usage` are replaced with native providers;
/// the caller's cleanup, clock, core count, telemetry, diagnostics, event, and
/// log hooks are preserved.
pub fn build_native_memory_pressure_monitor(
    config: MemoryPressureConfig,
    probe: Arc<NativeMemoryProbe>,
    mut hooks: MemoryPressureMonitorHooks,
) -> Result<MemoryPressureMonitor, NativeMemoryRuntimeError> {
    validate_memory_pressure_config(&config).map_err(NativeMemoryRuntimeError::InvalidConfig)?;
    let effective_memory_limit_bytes = probe
        .effective_memory_limit_bytes()
        .map_err(NativeMemoryRuntimeError::EffectiveMemoryLimit)?;

    let memory_probe = Arc::clone(&probe);
    hooks.measure = Arc::new(move || {
        let memory_usage = memory_probe
            .memory_usage()
            .map_err(|error| error.to_string())?;
        Ok(MemoryPressureMeasurement {
            memory_usage,
            heap_size_limit_bytes: 0,
        })
    });

    hooks.cpu_usage = Arc::new(move || probe.process_cpu_usage());

    // Configuration was validated above, before any OS probing, matching
    // the TypeScript constructor's validation order.
    Ok(
        MemoryPressureMonitor::new(config, effective_memory_limit_bytes, hooks)
            .expect("memory pressure configuration was already validated"),
    )
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::services::memory_pressure_monitor::{
        CleanupStepFuture, MemoryPressureLog, MemoryPressureLogLevel,
    };
    use crate::services::memory_pressure_policy::DEFAULT_PRESSURE_CONFIG;
    use crate::services::native_memory_probe::{NativeMemoryProbe, NativeMemoryProbeHooks};

    fn probe(fail_rss: bool) -> Arc<NativeMemoryProbe> {
        let read_file = move |path: &Path| -> io::Result<String> {
            #[cfg(target_os = "macos")]
            let _ = path;
            #[cfg(target_os = "linux")]
            {
                if path == Path::new("/proc/meminfo") {
                    return Ok("MemTotal:       1048576 kB\n".to_owned());
                }
                if path == Path::new("/proc/self/status") {
                    if fail_rss {
                        return Err(io::Error::new(io::ErrorKind::NotFound, "status missing"));
                    }
                    return Ok("Name:\ttest\nVmRSS:\t1024 kB\n".to_owned());
                }
                if path == Path::new("/proc/self/stat") {
                    return Err(io::Error::new(io::ErrorKind::NotFound, "stat missing"));
                }
            }
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "fixture file missing",
            ))
        };

        let run_process = move |program: &str,
                                args: &[String],
                                _timeout: std::time::Duration|
              -> io::Result<String> {
            #[cfg(target_os = "macos")]
            {
                if program == "sysctl" {
                    return Ok("1073741824\n".to_owned());
                }
                if program == "/bin/ps" {
                    if args.get(1).map(String::as_str) == Some("rss=") {
                        if fail_rss {
                            return Err(io::Error::new(io::ErrorKind::NotFound, "ps failed"));
                        }
                        return Ok("1024\n".to_owned());
                    }
                    return Ok("00:00.25 00:00.50\n".to_owned());
                }
            }
            #[cfg(target_os = "linux")]
            {
                if program == "getconf" {
                    return match args.first().map(String::as_str) {
                        Some("PAGE_SIZE") => Ok("4096\n".to_owned()),
                        Some("_PHYS_PAGES") => Ok("262144\n".to_owned()),
                        Some("CLK_TCK") => Ok("100\n".to_owned()),
                        _ => Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "unexpected getconf request",
                        )),
                    };
                }
            }
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("unexpected process probe: {program} {args:?}"),
            ))
        };

        Arc::new(NativeMemoryProbe::with_hooks(NativeMemoryProbeHooks::new(
            read_file,
            run_process,
        )))
    }

    fn hooks(logs: Arc<Mutex<Vec<MemoryPressureLog>>>) -> MemoryPressureMonitorHooks {
        let mut hooks = MemoryPressureMonitorHooks::new(
            || Err("placeholder measurement must be replaced".to_owned()),
            |_| -> CleanupStepFuture { Box::pin(async { Ok(()) }) },
        );
        hooks.log = Arc::new(move |log| logs.lock().unwrap().push(log));
        hooks
    }

    #[tokio::test]
    async fn maps_native_rss_to_monitor_samples_and_preserves_clock_and_telemetry_hooks() {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let telemetry_samples = Arc::new(Mutex::new(Vec::new()));
        let mut hooks = hooks(Arc::clone(&logs));
        hooks.clock_ms = Arc::new(|| 12_345);
        hooks.telemetry_active = Arc::new(|| true);
        hooks.record_telemetry_sample = {
            let telemetry_samples = Arc::clone(&telemetry_samples);
            Arc::new(move |sample| telemetry_samples.lock().unwrap().push(sample))
        };
        let monitor =
            build_native_memory_pressure_monitor(DEFAULT_PRESSURE_CONFIG, probe(false), hooks)
                .expect("native memory monitor");

        monitor.perform_check().await;

        let samples = monitor.runtime_samples();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].rss, 1024 * 1024);
        assert_eq!(samples[0].heap_used, 0);
        assert_eq!(samples[0].heap_total, 0);
        assert_eq!(samples[0].ts, 12_345);
        assert_eq!(*telemetry_samples.lock().unwrap(), samples);
        assert!(logs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn native_rss_probe_failure_is_logged_and_skips_sampling() {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let monitor = build_native_memory_pressure_monitor(
            DEFAULT_PRESSURE_CONFIG,
            probe(true),
            hooks(Arc::clone(&logs)),
        )
        .expect("effective memory limit remains available");

        monitor.perform_check().await;

        assert!(monitor.runtime_samples().is_empty());
        assert!(logs.lock().unwrap().iter().any(|entry| {
            entry.level == MemoryPressureLogLevel::Error
                && entry
                    .message
                    .starts_with("Failed to read memory usage for pressure check:")
        }));
    }
}
