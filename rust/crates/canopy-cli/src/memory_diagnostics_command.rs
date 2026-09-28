// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Interactive native process-memory diagnostics.

use canopy_core::services::memory_diagnostics::{
    MemoryDiagnosticsProbeOverrides, MemoryDiagnosticsSnapshot, MemoryUsageSnapshot,
    collect_memory_diagnostics,
};
use canopy_core::services::memory_pressure_policy::pressure_level;
use canopy_core::services::native_memory_probe::NativeMemoryProbe;
use serde_json::{Value, json};
use std::time::Duration;

const USAGE: &str = "/doctor memory [--json] [--sample]";
const SAMPLE_COUNT: usize = 3;
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

pub async fn run(arguments: &str) -> Result<String, String> {
    let mut json_output = false;
    let mut sample = false;
    for argument in arguments.split_whitespace() {
        match argument {
            "--json" => json_output = true,
            "--sample" => sample = true,
            "--snapshot" => {
                return Err(
                    "Heap snapshots are unavailable in the native Rust runtime; use `--json` for a process-memory report.".to_owned(),
                );
            }
            _ => return Err(format!("Unknown argument: {argument}. Usage: {USAGE}")),
        }
    }

    let probe = NativeMemoryProbe::system();
    let memory = probe
        .memory_usage()
        .map_err(|error| format!("Could not sample native process memory: {error}"))?;
    let cpu = probe.process_cpu_usage();
    let diagnostics = collect_memory_diagnostics(
        MemoryDiagnosticsSnapshot {
            node_version: format!("native-rust/{}", env!("CARGO_PKG_VERSION")),
            memory_usage: MemoryUsageSnapshot {
                heap_used: memory.heap_used,
                heap_total: memory.heap_total,
                rss: memory.rss,
                external: memory.external,
                array_buffers: 0,
            },
            max_rss_raw: super::memory_pressure::peak_rss_kib().unwrap_or_default(),
            user_cpu_time: cpu
                .and_then(|usage| u64::try_from(usage.user_us).ok())
                .unwrap_or_default(),
            system_cpu_time: cpu
                .and_then(|usage| u64::try_from(usage.system_us).ok())
                .unwrap_or_default(),
            ..MemoryDiagnosticsSnapshot::default()
        },
        MemoryDiagnosticsProbeOverrides::default(),
    )
    .await;
    let system_snapshot = probe.system_memory_snapshot().ok();
    let pressure = system_snapshot.map(|system| {
        pressure_level(
            &super::memory_pressure::load_config(),
            memory.rss,
            system.effective_limit_bytes,
            0,
            0,
        )
    });
    let system_memory = system_snapshot.map(|snapshot| {
        json!({
            "hostTotalBytes": snapshot.host_total_bytes,
            "effectiveLimitBytes": snapshot.effective_limit_bytes,
            "availableBytes": snapshot.available_bytes,
            "availableBytesMethod": snapshot.available_bytes_method,
        })
    });
    let samples = if sample {
        let mut values = Vec::with_capacity(SAMPLE_COUNT);
        for index in 1..=SAMPLE_COUNT {
            if index > 1 {
                tokio::time::sleep(SAMPLE_INTERVAL).await;
            }
            let sample = probe
                .memory_usage()
                .map_err(|error| format!("Could not collect memory sample {index}: {error}"))?;
            values.push(json!({ "index": index, "rss": sample.rss }));
        }
        Some(values)
    } else {
        None
    };

    if json_output {
        let mut report = json!({
            "runtime": "native-rust",
            "processId": std::process::id(),
            "uptimeAvailable": false,
            "v8MetricsAvailable": false,
            "nativePressureLevel": pressure.map(|level| level.as_str()),
            "diagnostics": diagnostics,
            "systemMemory": system_memory,
        });
        if let Some(samples) = samples {
            report["samples"] = Value::Array(samples);
        }
        return serde_json::to_string_pretty(&report)
            .map_err(|error| format!("Could not format memory diagnostics: {error}"));
    }

    let mut lines = vec![
        "Memory diagnostics (native Rust)".to_owned(),
        format!("Generated: {}", diagnostics.timestamp),
        format!("PID: {}", std::process::id()),
        format!("Platform: {}", diagnostics.platform),
        format!("RSS: {}", format_bytes(diagnostics.memory_usage.rss)),
        format!(
            "Native RSS pressure: {}",
            pressure.map_or("unavailable", |level| level.as_str())
        ),
        "Runtime heap counters: unavailable (no V8 heap in the native runtime)".to_owned(),
        format!(
            "Peak RSS: {}",
            if diagnostics.resource_usage.max_rss_raw == 0 {
                "unavailable".to_owned()
            } else {
                format_bytes(diagnostics.resource_usage.max_rss)
            }
        ),
        format!(
            "Open file descriptors: {}",
            diagnostics
                .open_file_descriptors
                .map_or_else(|| "unavailable".to_owned(), |value| value.to_string())
        ),
    ];
    if let Some(system_memory) = system_memory {
        lines.extend([
            format!(
                "Host memory: {}",
                format_bytes(system_memory["hostTotalBytes"].as_u64().unwrap_or_default())
            ),
            format!(
                "Effective memory limit: {}",
                format_bytes(
                    system_memory["effectiveLimitBytes"]
                        .as_u64()
                        .unwrap_or_default()
                )
            ),
            format!(
                "Available memory: {}",
                system_memory["availableBytes"]
                    .as_u64()
                    .map_or_else(|| "unavailable".to_owned(), format_bytes)
            ),
        ]);
    }
    if let Some(tree) = &diagnostics.process_tree {
        lines.push(format!(
            "Process tree RSS: {} across {} processes",
            format_bytes(tree.tree_rss),
            tree.process_count
        ));
    }
    lines.push("Risks:".to_owned());
    if diagnostics.analysis.risks.is_empty() {
        lines.push("  None detected by available native probes.".to_owned());
    } else {
        lines.extend(
            diagnostics
                .analysis
                .risks
                .iter()
                .map(|risk| format!("  - {:?}: {}", risk.risk_type, risk.message)),
        );
    }
    lines.push(format!(
        "Recommendation: {}",
        diagnostics.analysis.recommendation
    ));
    if let Some(samples) = samples {
        let first_rss = samples.first().and_then(|sample| sample["rss"].as_i64());
        let last_rss = samples.last().and_then(|sample| sample["rss"].as_i64());
        let delta = first_rss
            .zip(last_rss)
            .map(|(first, last)| signed_bytes(last - first))
            .unwrap_or_else(|| "unavailable".to_owned());
        lines.extend([
            "Memory pressure samples".to_owned(),
            format!("  Sample count: {}", samples.len()),
            format!("  RSS delta: {delta}"),
        ]);
        lines.extend(samples.iter().map(|sample| {
            format!(
                "  #{}: RSS {}",
                sample["index"].as_u64().unwrap_or_default(),
                format_bytes(sample["rss"].as_u64().unwrap_or_default())
            )
        }));
    }
    Ok(lines.join("\n"))
}

fn signed_bytes(bytes: i64) -> String {
    if bytes >= 0 {
        format!("+{}", format_bytes(bytes as u64))
    } else {
        format!("-{}", format_bytes(bytes.unsigned_abs()))
    }
}

fn format_bytes(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / 1_048_576.0)
}
