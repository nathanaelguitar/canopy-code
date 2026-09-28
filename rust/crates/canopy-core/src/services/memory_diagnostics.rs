//! Memory diagnostics port of `packages/core/src/utils/memoryDiagnostics.ts`.
//!
//! Node/V8 counters do not exist in this Rust runtime. The caller injects one
//! [`MemoryDiagnosticsSnapshot`] containing those counters; this module keeps
//! the source JSON schema and risk analysis, and independently probes the
//! optional OS metrics where they are available.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::io;
use std::process::Stdio;
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;

const RSS_HEAP_GAP_RATIO: f64 = 10.0;
const RSS_HEAP_GAP_MIN_BYTES: u64 = 256 * 1024 * 1024;
const NATIVE_MEMORY_PRESSURE_MIN_BYTES: u64 = 64 * 1024 * 1024;
const ACTIVE_HANDLES_THRESHOLD: u64 = 256;
const ACTIVE_REQUESTS_THRESHOLD: u64 = 100;
const OPEN_FD_THRESHOLD: u64 = 500;
const PS_MAX_OUTPUT_BYTES: usize = 1024 * 1024;
const PS_TIMEOUT: Duration = Duration::from_secs(5);

/// Memory fields reported by `process.memoryUsage()` in the source runtime.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryUsageSnapshot {
    pub heap_used: u64,
    pub heap_total: u64,
    pub rss: u64,
    pub external: u64,
    pub array_buffers: u64,
}

/// Injected counterpart to the fields used from `v8.getHeapStatistics()`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct V8HeapStats {
    pub heap_size_limit: u64,
    pub total_heap_size: u64,
    pub used_heap_size: u64,
    pub malloced_memory: u64,
    pub peak_malloced_memory: u64,
    pub detached_contexts: u64,
    pub native_contexts: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct V8HeapSpaceStats {
    pub name: String,
    pub size: u64,
    pub used: u64,
    pub available: u64,
}

/// Resource counters preserve Node's `maxRSS` unit convention (KiB) and its
/// normalized byte value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryResourceUsage {
    #[serde(rename = "maxRSS")]
    pub max_rss: u64,
    #[serde(rename = "maxRSSRaw")]
    pub max_rss_raw: u64,
    #[serde(rename = "maxRSSUnit")]
    pub max_rss_unit: &'static str,
    #[serde(rename = "userCPUTime")]
    pub user_cpu_time: u64,
    #[serde(rename = "systemCPUTime")]
    pub system_cpu_time: u64,
}

impl Default for MemoryResourceUsage {
    fn default() -> Self {
        Self {
            max_rss: 0,
            max_rss_raw: 0,
            max_rss_unit: "KiB",
            user_cpu_time: 0,
            system_cpu_time: 0,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessTreeMemoryUsage {
    pub root_pid: u32,
    pub process_count: usize,
    #[serde(rename = "rootRSS")]
    pub root_rss: u64,
    #[serde(rename = "treeRSS")]
    pub tree_rss: u64,
}

/// All runtime-specific counters are injected by the caller. The field names
/// serialize to the source diagnostics JSON schema.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryDiagnosticsSnapshot {
    pub timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canopy_version: Option<String>,
    pub uptime_seconds: f64,
    pub memory_usage: MemoryUsageSnapshot,
    pub v8_heap_stats: V8HeapStats,
    pub v8_heap_spaces: Option<Vec<V8HeapSpaceStats>>,
    pub max_rss_raw: u64,
    pub user_cpu_time: u64,
    pub system_cpu_time: u64,
    pub active_handles: u64,
    pub active_requests: u64,
    pub platform: String,
    pub node_version: String,
}

impl Default for MemoryDiagnosticsSnapshot {
    fn default() -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            session_id: None,
            canopy_version: None,
            uptime_seconds: 0.0,
            memory_usage: MemoryUsageSnapshot::default(),
            v8_heap_stats: V8HeapStats::default(),
            v8_heap_spaces: None,
            max_rss_raw: 0,
            user_cpu_time: 0,
            system_cpu_time: 0,
            active_handles: 0,
            active_requests: 0,
            platform: current_platform(),
            node_version: "unknown".to_owned(),
        }
    }
}

/// Deterministic optional probe results for callers and tests. A failed probe
/// is represented as `Err`; it becomes `null` in the output without affecting
/// other fields. `None` delegates to the platform probe.
#[derive(Clone, Debug, Default)]
pub struct MemoryDiagnosticsProbeOverrides {
    pub open_file_descriptors: Option<Result<u64, String>>,
    pub smaps_rollup: Option<Result<String, String>>,
    pub process_tree: Option<Result<ProcessTreeMemoryUsage, String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryDiagnostics {
    pub timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canopy_version: Option<String>,
    pub uptime_seconds: f64,
    pub memory_usage: MemoryUsageSnapshot,
    pub v8_heap_stats: V8HeapStats,
    pub v8_heap_spaces: Option<Vec<V8HeapSpaceStats>>,
    pub resource_usage: MemoryResourceUsage,
    pub process_tree: Option<ProcessTreeMemoryUsage>,
    pub active_handles: u64,
    pub active_requests: u64,
    pub open_file_descriptors: Option<u64>,
    pub smaps_rollup: Option<String>,
    pub platform: String,
    pub node_version: String,
    pub analysis: MemoryDiagnosticsAnalysis,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MemoryDiagnosticsAnalysis {
    pub risks: Vec<MemoryRisk>,
    pub recommendation: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryRiskType {
    HeapPressure,
    DetachedContexts,
    ActiveHandles,
    ActiveRequests,
    FdLeak,
    NativeMemoryPressure,
    RssHeapGap,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MemoryRisk {
    #[serde(rename = "type")]
    pub risk_type: MemoryRiskType,
    pub message: String,
}

/// Collect platform probes concurrently, then analyze the injected snapshot.
/// Unsupported files and failed commands are isolated as `None` values.
pub async fn collect_memory_diagnostics(
    snapshot: MemoryDiagnosticsSnapshot,
    overrides: MemoryDiagnosticsProbeOverrides,
) -> MemoryDiagnostics {
    let pid = std::process::id();
    let (open_file_descriptors, smaps_rollup, process_tree) = tokio::join!(
        selected_probe(
            overrides.open_file_descriptors,
            count_open_file_descriptors(),
        ),
        selected_probe(overrides.smaps_rollup, read_proc_smaps_rollup()),
        selected_probe(
            overrides.process_tree,
            collect_process_tree_memory_usage(&snapshot.platform, pid),
        ),
    );

    let diagnostics = MemoryDiagnostics {
        timestamp: snapshot.timestamp,
        session_id: snapshot.session_id,
        canopy_version: snapshot.canopy_version,
        uptime_seconds: snapshot.uptime_seconds,
        memory_usage: snapshot.memory_usage,
        v8_heap_stats: snapshot.v8_heap_stats,
        v8_heap_spaces: snapshot.v8_heap_spaces,
        resource_usage: MemoryResourceUsage {
            max_rss: snapshot.max_rss_raw.saturating_mul(1024),
            max_rss_raw: snapshot.max_rss_raw,
            max_rss_unit: "KiB",
            user_cpu_time: snapshot.user_cpu_time,
            system_cpu_time: snapshot.system_cpu_time,
        },
        process_tree,
        active_handles: snapshot.active_handles,
        active_requests: snapshot.active_requests,
        open_file_descriptors,
        smaps_rollup,
        platform: snapshot.platform,
        node_version: snapshot.node_version,
        analysis: MemoryDiagnosticsAnalysis {
            risks: Vec::new(),
            recommendation: String::new(),
        },
    };

    with_analysis(diagnostics)
}

/// Analyze an already collected diagnostics record. Kept public so callers
/// that persist snapshots can reproduce the same risk summary later.
pub fn analyze_memory_diagnostics(diagnostics: &MemoryDiagnostics) -> MemoryDiagnosticsAnalysis {
    let mut risks = Vec::new();
    let heap_ratio = if diagnostics.v8_heap_stats.heap_size_limit > 0 {
        diagnostics.v8_heap_stats.used_heap_size as f64
            / diagnostics.v8_heap_stats.heap_size_limit as f64
    } else {
        0.0
    };

    if heap_ratio >= 0.75 {
        risks.push(risk(
            MemoryRiskType::HeapPressure,
            format!("Heap usage is {:.1}% of the V8 limit.", heap_ratio * 100.0),
        ));
    }
    if diagnostics.v8_heap_stats.detached_contexts > 0 {
        risks.push(risk(
            MemoryRiskType::DetachedContexts,
            format!(
                "{} detached V8 context(s) detected.",
                diagnostics.v8_heap_stats.detached_contexts
            ),
        ));
    }
    if diagnostics.active_handles > ACTIVE_HANDLES_THRESHOLD {
        risks.push(risk(
            MemoryRiskType::ActiveHandles,
            format!("{} active handle(s) detected.", diagnostics.active_handles),
        ));
    }
    if diagnostics.active_requests > ACTIVE_REQUESTS_THRESHOLD {
        risks.push(risk(
            MemoryRiskType::ActiveRequests,
            format!(
                "{} active request(s) detected.",
                diagnostics.active_requests
            ),
        ));
    }
    if diagnostics
        .open_file_descriptors
        .is_some_and(|count| count > OPEN_FD_THRESHOLD)
    {
        risks.push(risk(
            MemoryRiskType::FdLeak,
            format!(
                "{} open file descriptor(s) detected.",
                diagnostics.open_file_descriptors.unwrap_or_default()
            ),
        ));
    }

    // RSS includes normal runtime overhead; mallocedMemory is the source's
    // deliberately narrower proxy for V8 native pressure.
    let native_memory = diagnostics.v8_heap_stats.malloced_memory;
    if native_memory >= NATIVE_MEMORY_PRESSURE_MIN_BYTES
        && native_memory > diagnostics.memory_usage.heap_used.saturating_mul(2)
    {
        risks.push(risk(
            MemoryRiskType::NativeMemoryPressure,
            format!(
                "V8 native malloced memory ({}) is more than 2× heap used ({}).",
                format_memory_usage(native_memory),
                format_memory_usage(diagnostics.memory_usage.heap_used),
            ),
        ));
    }

    if diagnostics.memory_usage.heap_used > 0
        && diagnostics.memory_usage.rss >= RSS_HEAP_GAP_MIN_BYTES
        && (diagnostics.memory_usage.rss as f64)
            > diagnostics.memory_usage.heap_used as f64 * RSS_HEAP_GAP_RATIO
    {
        risks.push(risk(
            MemoryRiskType::RssHeapGap,
            format!(
                "RSS ({}) is more than {RSS_HEAP_GAP_RATIO:.0}× heap used ({}). Check native addons, libuv buffers, mapped files, or retained tool output.",
                format_memory_usage(diagnostics.memory_usage.rss),
                format_memory_usage(diagnostics.memory_usage.heap_used),
            ),
        ));
    }

    MemoryDiagnosticsAnalysis {
        recommendation: if risks.is_empty() {
            "No obvious leak indicators detected.".to_owned()
        } else {
            format!("{} potential leak indicator(s) found.", risks.len())
        },
        risks,
    }
}

fn with_analysis(mut diagnostics: MemoryDiagnostics) -> MemoryDiagnostics {
    diagnostics.analysis = analyze_memory_diagnostics(&diagnostics);
    diagnostics
}

fn risk(risk_type: MemoryRiskType, message: String) -> MemoryRisk {
    MemoryRisk { risk_type, message }
}

fn format_memory_usage(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let kb = bytes as f64 / KB;
    let mb = bytes as f64 / MB;
    let gb = bytes as f64 / GB;
    // Match the source's branch selection after rounding to one decimal.
    if (kb * 10.0).round() / 10.0 < 1024.0 {
        format!("{kb:.1} KB")
    } else if (mb * 10.0).round() / 10.0 < 1024.0 {
        format!("{mb:.1} MB")
    } else {
        format!("{gb:.2} GB")
    }
}

async fn selected_probe<T, F>(override_result: Option<Result<T, String>>, probe: F) -> Option<T>
where
    F: Future<Output = io::Result<T>>,
{
    match override_result {
        Some(Ok(value)) => Some(value),
        Some(Err(_error)) => None,
        None => optional_probe(probe).await,
    }
}

async fn optional_probe<T, F>(probe: F) -> Option<T>
where
    F: Future<Output = io::Result<T>>,
{
    probe.await.ok()
}

async fn count_open_file_descriptors() -> io::Result<u64> {
    let mut entries = tokio::fs::read_dir("/proc/self/fd").await?;
    let mut count = 0_u64;
    while entries.next_entry().await?.is_some() {
        count = count.saturating_add(1);
    }
    Ok(count)
}

async fn read_proc_smaps_rollup() -> io::Result<String> {
    tokio::fs::read_to_string("/proc/self/smaps_rollup").await
}

/// Parse `ps -axo pid=,ppid=,rss=` rows. Malformed and non-finite rows are
/// ignored, as in the TypeScript parser.
pub fn parse_ps_rows(output: &str) -> Vec<PsRow> {
    output
        .lines()
        .filter_map(|line| {
            let mut columns = line.split_whitespace();
            let pid = columns.next()?.parse::<f64>().ok()?;
            let ppid = columns.next()?.parse::<f64>().ok()?;
            let rss_kib = columns.next()?.parse::<f64>().ok()?;
            if !pid.is_finite() || !ppid.is_finite() || !rss_kib.is_finite() {
                return None;
            }
            // ps emits integral identifiers and RSS values. Reject values that
            // cannot safely serve as identifiers/counters in the Rust schema.
            if pid < 0.0 || ppid < 0.0 || rss_kib < 0.0 {
                return None;
            }
            Some(PsRow {
                pid: pid as u32,
                ppid: ppid as u32,
                rss_kib: rss_kib as u64,
            })
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PsRow {
    pub pid: u32,
    pub ppid: u32,
    pub rss_kib: u64,
}

/// Build process-tree RSS from `ps` rows. Cycles and duplicate descendants are
/// guarded with a visited set, matching the source breadth-first traversal.
pub fn process_tree_from_rows(root_pid: u32, rows: &[PsRow]) -> ProcessTreeMemoryUsage {
    let rows_by_pid: HashMap<u32, PsRow> = rows.iter().copied().map(|row| (row.pid, row)).collect();
    let mut children_by_parent: HashMap<u32, Vec<u32>> = HashMap::new();
    for row in rows {
        children_by_parent
            .entry(row.ppid)
            .or_default()
            .push(row.pid);
    }

    let mut queue = VecDeque::from([root_pid]);
    let mut seen = HashSet::new();
    let mut root_rss = 0_u64;
    let mut tree_rss = 0_u64;
    let mut process_count = 0_usize;
    while let Some(pid) = queue.pop_front() {
        if !seen.insert(pid) {
            continue;
        }
        if let Some(row) = rows_by_pid.get(&pid) {
            let rss_bytes = row.rss_kib.saturating_mul(1024);
            if pid == root_pid {
                root_rss = rss_bytes;
            }
            tree_rss = tree_rss.saturating_add(rss_bytes);
            process_count += 1;
        }
        if let Some(children) = children_by_parent.get(&pid) {
            queue.extend(children.iter().copied());
        }
    }

    ProcessTreeMemoryUsage {
        root_pid,
        process_count,
        root_rss,
        tree_rss,
    }
}

/// Collect process-tree RSS using a fixed executable and argument vector.
/// Output is capped at 1 MiB and the process is killed after five seconds.
pub async fn collect_process_tree_memory_usage(
    platform: &str,
    root_pid: u32,
) -> io::Result<ProcessTreeMemoryUsage> {
    if platform == "win32" || !cfg!(unix) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("process tree RSS probe is unavailable on {platform}"),
        ));
    }

    let mut child = Command::new("ps")
        .args(["-axo", "pid=,ppid=,rss="])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("ps stdout pipe was not available"))?;

    let result = timeout(PS_TIMEOUT, async {
        let mut limited = stdout.take((PS_MAX_OUTPUT_BYTES + 1) as u64);
        let mut bytes = Vec::with_capacity(16 * 1024);
        limited.read_to_end(&mut bytes).await?;
        if bytes.len() > PS_MAX_OUTPUT_BYTES {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ps output exceeded the 1 MiB limit",
            ));
        }
        let status = child.wait().await?;
        if !status.success() {
            return Err(io::Error::other(format!("ps exited with {status}")));
        }
        let output = String::from_utf8(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        Ok(process_tree_from_rows(root_pid, &parse_ps_rows(&output)))
    })
    .await;

    match result {
        Ok(result) => result,
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "ps process-tree probe exceeded five seconds",
            ))
        }
    }
}

fn current_platform() -> String {
    match std::env::consts::OS {
        "macos" => "darwin".to_owned(),
        "windows" => "win32".to_owned(),
        os => os.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture() -> MemoryDiagnosticsSnapshot {
        MemoryDiagnosticsSnapshot {
            timestamp: "2026-05-01T10:00:00.000Z".to_owned(),
            session_id: Some("session-123".to_owned()),
            canopy_version: Some("0.15.6".to_owned()),
            uptime_seconds: 60.0,
            memory_usage: MemoryUsageSnapshot {
                heap_used: 32 * 1024 * 1024,
                heap_total: 40 * 1024 * 1024,
                rss: 100 * 1024 * 1024,
                external: 700,
                array_buffers: 300,
            },
            v8_heap_stats: V8HeapStats {
                heap_size_limit: 40 * 1024 * 1024,
                total_heap_size: 40 * 1024 * 1024,
                used_heap_size: 32 * 1024 * 1024,
                malloced_memory: 80 * 1024 * 1024,
                peak_malloced_memory: 90 * 1024 * 1024,
                detached_contexts: 1,
                native_contexts: 2,
            },
            v8_heap_spaces: Some(vec![V8HeapSpaceStats {
                name: "old_space".to_owned(),
                size: 1000,
                used: 800,
                available: 200,
            }]),
            max_rss_raw: 6,
            user_cpu_time: 10,
            system_cpu_time: 20,
            active_handles: 300,
            active_requests: 3,
            platform: "linux".to_owned(),
            node_version: "v20.19.0".to_owned(),
        }
    }

    #[tokio::test]
    async fn matches_source_snapshot_schema_and_risk_fixture() {
        let diagnostics = collect_memory_diagnostics(
            fixture(),
            MemoryDiagnosticsProbeOverrides {
                open_file_descriptors: Some(Ok(501)),
                smaps_rollup: Some(Ok("Rss: 5000 kB".to_owned())),
                process_tree: Some(Err("not available".to_owned())),
            },
        )
        .await;

        assert_eq!(diagnostics.timestamp, "2026-05-01T10:00:00.000Z");
        assert_eq!(diagnostics.resource_usage.max_rss, 6 * 1024);
        assert_eq!(diagnostics.resource_usage.max_rss_unit, "KiB");
        assert_eq!(diagnostics.process_tree, None);
        assert_eq!(diagnostics.open_file_descriptors, Some(501));
        assert_eq!(diagnostics.smaps_rollup.as_deref(), Some("Rss: 5000 kB"));
        let risk_types: Vec<_> = diagnostics
            .analysis
            .risks
            .iter()
            .map(|risk| risk.risk_type)
            .collect();
        assert_eq!(
            risk_types,
            vec![
                MemoryRiskType::HeapPressure,
                MemoryRiskType::DetachedContexts,
                MemoryRiskType::ActiveHandles,
                MemoryRiskType::FdLeak,
                MemoryRiskType::NativeMemoryPressure,
            ]
        );
        assert!(diagnostics.analysis.risks[4].message.contains("80.0 MB"));
        assert!(diagnostics.analysis.risks[4].message.contains("32.0 MB"));
        assert_eq!(
            diagnostics.analysis.recommendation,
            "5 potential leak indicator(s) found."
        );
        let value = serde_json::to_value(diagnostics).unwrap();
        assert_eq!(
            value["memoryUsage"]["heapUsed"],
            json!(32 * 1024 * 1024_u64)
        );
        assert_eq!(value["v8HeapStats"]["detachedContexts"], json!(1));
        assert_eq!(value["resourceUsage"]["maxRSSRaw"], json!(6));
        assert_eq!(
            value["analysis"]["risks"][0]["type"],
            json!("heap-pressure")
        );
        assert!(value.get("memoryGrowthRate").is_none());
    }

    #[tokio::test]
    async fn optional_probe_failures_are_null_without_suppressing_other_data() {
        let diagnostics = collect_memory_diagnostics(
            MemoryDiagnosticsSnapshot {
                memory_usage: MemoryUsageSnapshot {
                    heap_used: 100,
                    heap_total: 200,
                    rss: 300,
                    external: 10,
                    array_buffers: 5,
                },
                ..MemoryDiagnosticsSnapshot::default()
            },
            MemoryDiagnosticsProbeOverrides {
                open_file_descriptors: Some(Err("unavailable".to_owned())),
                smaps_rollup: Some(Err("unavailable".to_owned())),
                process_tree: Some(Err("unavailable".to_owned())),
            },
        )
        .await;
        assert_eq!(diagnostics.open_file_descriptors, None);
        assert_eq!(diagnostics.smaps_rollup, None);
        assert_eq!(diagnostics.process_tree, None);
        assert_eq!(
            diagnostics.analysis.recommendation,
            "No obvious leak indicators detected."
        );
    }

    #[tokio::test]
    async fn source_threshold_edges_and_optional_process_tree_are_preserved() {
        let mut snapshot = fixture();
        snapshot.memory_usage.heap_used = 1_600;
        snapshot.memory_usage.heap_total = 2_000;
        snapshot.memory_usage.rss = 5_000;
        snapshot.v8_heap_stats.heap_size_limit = 2_000;
        snapshot.v8_heap_stats.used_heap_size = 1_600;
        snapshot.v8_heap_stats.malloced_memory = 32 * 1024 * 1024;
        snapshot.v8_heap_stats.peak_malloced_memory = 32 * 1024 * 1024;
        snapshot.v8_heap_stats.detached_contexts = 0;
        snapshot.active_handles = 200;
        snapshot.active_requests = 100;
        snapshot.max_rss_raw = 4_096;
        let diagnostics = collect_memory_diagnostics(
            snapshot,
            MemoryDiagnosticsProbeOverrides {
                open_file_descriptors: Some(Ok(500)),
                smaps_rollup: Some(Err("not available".to_owned())),
                process_tree: Some(Ok(ProcessTreeMemoryUsage {
                    root_pid: 123,
                    process_count: 3,
                    root_rss: 10 * 1024 * 1024,
                    tree_rss: 25 * 1024 * 1024,
                })),
            },
        )
        .await;

        assert_eq!(diagnostics.resource_usage.max_rss, 4_096 * 1024);
        assert_eq!(diagnostics.resource_usage.max_rss_raw, 4_096);
        assert_eq!(diagnostics.process_tree.as_ref().unwrap().process_count, 3);
        assert_eq!(diagnostics.smaps_rollup, None);
        assert!(
            diagnostics
                .analysis
                .risks
                .iter()
                .any(|risk| { risk.risk_type == MemoryRiskType::HeapPressure })
        );
        assert!(
            !diagnostics
                .analysis
                .risks
                .iter()
                .any(|risk| risk.risk_type == MemoryRiskType::NativeMemoryPressure)
        );
        assert!(
            !diagnostics
                .analysis
                .risks
                .iter()
                .any(|risk| risk.risk_type == MemoryRiskType::ActiveHandles)
        );
    }

    #[tokio::test]
    async fn flags_active_requests_above_the_source_threshold() {
        let mut snapshot = fixture();
        snapshot.memory_usage.heap_used = 100;
        snapshot.memory_usage.rss = 300;
        snapshot.v8_heap_stats.heap_size_limit = 1_000;
        snapshot.v8_heap_stats.used_heap_size = 100;
        snapshot.v8_heap_stats.detached_contexts = 0;
        snapshot.v8_heap_stats.malloced_memory = 0;
        snapshot.active_handles = 0;
        snapshot.active_requests = 101;
        let diagnostics = collect_memory_diagnostics(
            snapshot,
            MemoryDiagnosticsProbeOverrides {
                open_file_descriptors: Some(Err("unavailable".to_owned())),
                smaps_rollup: Some(Err("unavailable".to_owned())),
                process_tree: Some(Err("unavailable".to_owned())),
            },
        )
        .await;
        assert_eq!(
            diagnostics.analysis.risks,
            vec![risk(
                MemoryRiskType::ActiveRequests,
                "101 active request(s) detected.".to_owned()
            )]
        );
    }

    #[test]
    fn parses_process_rows_and_accumulates_only_root_descendants() {
        let rows = parse_ps_rows(
            "  123  1  100\n124 123 20\n125 124 30\n126 1 900\nbad row\n127 123 inf\n",
        );
        let tree = process_tree_from_rows(123, &rows);
        assert_eq!(
            tree,
            ProcessTreeMemoryUsage {
                root_pid: 123,
                process_count: 3,
                root_rss: 100 * 1024,
                tree_rss: 150 * 1024,
            }
        );
    }

    #[test]
    fn source_thresholds_and_exact_risk_messages_are_preserved() {
        let mut diagnostics = MemoryDiagnostics {
            timestamp: "2026-05-01T10:00:00.000Z".to_owned(),
            session_id: None,
            canopy_version: None,
            uptime_seconds: 1.0,
            memory_usage: MemoryUsageSnapshot {
                heap_used: 50 * 1024 * 1024,
                heap_total: 64 * 1024 * 1024,
                rss: 800 * 1024 * 1024,
                external: 0,
                array_buffers: 0,
            },
            v8_heap_stats: V8HeapStats {
                heap_size_limit: 512 * 1024 * 1024,
                total_heap_size: 64 * 1024 * 1024,
                used_heap_size: 50 * 1024 * 1024,
                malloced_memory: 512 * 1024,
                peak_malloced_memory: 1024 * 1024,
                detached_contexts: 0,
                native_contexts: 1,
            },
            v8_heap_spaces: None,
            resource_usage: MemoryResourceUsage::default(),
            process_tree: None,
            active_handles: 256,
            active_requests: 100,
            open_file_descriptors: Some(500),
            smaps_rollup: None,
            platform: "darwin".to_owned(),
            node_version: "v20.19.0".to_owned(),
            analysis: MemoryDiagnosticsAnalysis {
                risks: Vec::new(),
                recommendation: String::new(),
            },
        };
        diagnostics.analysis = analyze_memory_diagnostics(&diagnostics);
        assert_eq!(diagnostics.analysis.risks.len(), 1);
        assert_eq!(
            diagnostics.analysis.risks[0].risk_type,
            MemoryRiskType::RssHeapGap
        );
        assert_eq!(
            diagnostics.analysis.risks[0].message,
            "RSS (800.0 MB) is more than 10× heap used (50.0 MB). Check native addons, libuv buffers, mapped files, or retained tool output."
        );
    }

    #[test]
    fn process_tree_walk_handles_cycles() {
        let rows = [
            PsRow {
                pid: 1,
                ppid: 2,
                rss_kib: 1,
            },
            PsRow {
                pid: 2,
                ppid: 1,
                rss_kib: 2,
            },
        ];
        let tree = process_tree_from_rows(1, &rows);
        assert_eq!(tree.process_count, 2);
        assert_eq!(tree.tree_rss, 3 * 1024);
    }

    #[test]
    fn heap_spaces_preserve_empty_and_null_states() {
        let mut snapshot = fixture();
        snapshot.v8_heap_spaces = Some(Vec::new());
        assert_eq!(
            serde_json::to_value(&snapshot).unwrap()["v8HeapSpaces"],
            json!([])
        );
        snapshot.v8_heap_spaces = None;
        let diagnostics = MemoryDiagnostics {
            timestamp: snapshot.timestamp,
            session_id: snapshot.session_id,
            canopy_version: snapshot.canopy_version,
            uptime_seconds: snapshot.uptime_seconds,
            memory_usage: snapshot.memory_usage,
            v8_heap_stats: snapshot.v8_heap_stats,
            v8_heap_spaces: snapshot.v8_heap_spaces,
            resource_usage: MemoryResourceUsage::default(),
            process_tree: None,
            active_handles: 0,
            active_requests: 0,
            open_file_descriptors: None,
            smaps_rollup: None,
            platform: snapshot.platform,
            node_version: snapshot.node_version,
            analysis: MemoryDiagnosticsAnalysis {
                risks: Vec::new(),
                recommendation: String::new(),
            },
        };
        let value = serde_json::to_value(diagnostics).unwrap();
        assert_eq!(value["v8HeapSpaces"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn unsupported_platform_process_tree_probe_errors_out() {
        let error = collect_process_tree_memory_usage("win32", 1)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    }
}
