//! Native process and container memory measurements for the pressure monitor.
//!
//! This is the OS adapter deliberately kept outside
//! `memory_pressure_monitor.rs`: it samples RSS, resolves host memory and
//! cgroup limits, and optionally samples cumulative process CPU time. The
//! pressure monitor continues to own policy and cleanup behavior.

use super::runtime_sample_ring::{CpuUsage, MemoryUsage};
use std::ffi::OsStr;
use std::io::{self, Read};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

pub const MIN_CGROUP_MEMORY_LIMIT_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_SAFE_CGROUP_MEMORY_LIMIT_BYTES: u64 = 9_007_199_254_740_991;
pub const PROCESS_PROBE_TIMEOUT: Duration = Duration::from_millis(1_000);
const MAX_PROCESS_OUTPUT_BYTES: usize = 4096;
const MAX_SYSTEM_MEMORY_FILE_BYTES: usize = 64 * 1024;
const MAX_CGROUP_MEMORY_FILE_BYTES: usize = 4096;
const CGROUP_V2_MEMORY_LIMIT: &str = "/sys/fs/cgroup/memory.max";
const CGROUP_V1_MEMORY_LIMIT: &str = "/sys/fs/cgroup/memory/memory.limit_in_bytes";

/// Injectable filesystem seam used for procfs and cgroup reads.
pub type FileReader = Arc<dyn Fn(&Path) -> io::Result<String> + Send + Sync>;
/// Optional bounded filesystem seam used for small system-proc metadata reads.
pub type BoundedFileReader = Arc<dyn Fn(&Path, usize) -> io::Result<String> + Send + Sync>;
/// Injectable bounded process seam used by macOS and small OS metadata probes.
pub type ProcessRunner = Arc<dyn Fn(&str, &[String], Duration) -> io::Result<String> + Send + Sync>;

#[derive(Clone)]
pub struct NativeMemoryProbeHooks {
    pub read_file: FileReader,
    pub read_file_bounded: Option<BoundedFileReader>,
    pub run_process: ProcessRunner,
}

impl NativeMemoryProbeHooks {
    pub fn new<R, P>(read_file: R, run_process: P) -> Self
    where
        R: Fn(&Path) -> io::Result<String> + Send + Sync + 'static,
        P: Fn(&str, &[String], Duration) -> io::Result<String> + Send + Sync + 'static,
    {
        Self {
            read_file: Arc::new(read_file),
            read_file_bounded: None,
            run_process: Arc::new(run_process),
        }
    }

    pub fn with_bounded_file_reader<R>(mut self, read_file_bounded: R) -> Self
    where
        R: Fn(&Path, usize) -> io::Result<String> + Send + Sync + 'static,
    {
        self.read_file_bounded = Some(Arc::new(read_file_bounded));
        self
    }
}

/// System-wide swap counters. These are host values, not process-attributed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SystemSwapUsage {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
}

/// Host and effective process memory limits included with native diagnostics.
/// `available_bytes` is the Linux `MemAvailable` value or a macOS VM-page
/// estimate. `available_bytes_method` identifies the source and calculation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SystemMemorySnapshot {
    pub host_total_bytes: u64,
    pub effective_limit_bytes: u64,
    pub available_bytes: Option<u64>,
    pub available_bytes_method: Option<&'static str>,
}

/// Native values that can seed `MemoryPressureMonitor` and its CPU sampler.
/// Rust has no V8 heap statistics, so the `MemoryUsage` heap/external fields
/// stay zero; this sample is intentionally RSS-only.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NativeMemorySample {
    pub memory_usage: MemoryUsage,
    pub effective_memory_limit_bytes: u64,
}

/// Reusable native memory provider. Production callers use `system()`; tests
/// inject virtual procfs/cgroup files and process results through `with_hooks`.
#[derive(Clone)]
pub struct NativeMemoryProbe {
    hooks: NativeMemoryProbeHooks,
    #[cfg(target_os = "linux")]
    linux_clock_ticks_per_second: Arc<OnceLock<Option<u64>>>,
}

impl NativeMemoryProbe {
    pub fn with_hooks(hooks: NativeMemoryProbeHooks) -> Self {
        Self {
            hooks,
            #[cfg(target_os = "linux")]
            linux_clock_ticks_per_second: Arc::new(OnceLock::new()),
        }
    }

    pub fn system() -> Self {
        Self::with_hooks(
            NativeMemoryProbeHooks::new(|path| std::fs::read_to_string(path), run_bounded_process)
                .with_bounded_file_reader(read_bounded_file),
        )
    }

    /// Host total memory, matching `os.totalmem()` on supported platforms.
    pub fn host_total_memory_bytes(&self) -> io::Result<u64> {
        #[cfg(target_os = "linux")]
        {
            if let Ok(meminfo) = (self.hooks.read_file)(Path::new("/proc/meminfo")) {
                if let Some(total) = parse_linux_mem_total_bytes(&meminfo) {
                    return Ok(total);
                }
            }
            let page_size = self.run_process("getconf", &["PAGE_SIZE".into()])?;
            let pages = self.run_process("getconf", &["_PHYS_PAGES".into()])?;
            let page_size = parse_positive_decimal(&page_size).ok_or_else(|| {
                invalid_data("getconf PAGE_SIZE did not return a positive integer")
            })?;
            let pages = parse_positive_decimal(&pages).ok_or_else(|| {
                invalid_data("getconf _PHYS_PAGES did not return a positive integer")
            })?;
            return page_size
                .checked_mul(pages)
                .ok_or_else(|| invalid_data("host memory size overflowed u64"));
        }

        #[cfg(target_os = "macos")]
        {
            let raw = self.run_process("sysctl", &["-n".into(), "hw.memsize".into()])?;
            parse_unsigned_decimal(&raw)
                .ok_or_else(|| invalid_data("sysctl hw.memsize did not return an integer"))
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "host memory sampling is supported on macOS and Linux",
            ))
        }
    }

    /// Effective host/cgroup cap. Cgroup v2 is preferred, then v1, then host
    /// memory, with the same validation rules and paths as the TypeScript
    /// monitor.
    pub fn effective_memory_limit_bytes(&self) -> io::Result<u64> {
        let host_total = self.host_total_memory_bytes()?;
        Ok(effective_memory_limit_from_cgroups(host_total, |path| {
            self.read_cgroup_memory_limit_file(path)
        }))
    }

    /// Best-effort system-memory context for crash diagnostics. The Linux
    /// `/proc/meminfo` read and cgroup limit reads are bounded; macOS host
    /// total comes from `sysctl`, and its available-memory estimate comes from
    /// a bounded, time-limited `vm_stat` process probe.
    pub fn system_memory_snapshot(&self) -> io::Result<SystemMemorySnapshot> {
        #[cfg(target_os = "linux")]
        {
            let meminfo = self.read_system_memory_file(Path::new("/proc/meminfo"))?;
            let host_total_bytes = parse_linux_mem_total_bytes(&meminfo)
                .ok_or_else(|| invalid_data("could not parse Linux MemTotal"))?;
            let available_bytes = parse_linux_mem_available_bytes(&meminfo);
            let effective_limit_bytes =
                effective_memory_limit_from_cgroups(host_total_bytes, |path| {
                    self.read_bounded_cgroup_memory_limit_file(path)
                });
            return Ok(SystemMemorySnapshot {
                host_total_bytes,
                effective_limit_bytes,
                available_bytes,
                available_bytes_method: available_bytes.map(|_| "proc-meminfo:MemAvailable"),
            });
        }

        #[cfg(target_os = "macos")]
        {
            let host_total_bytes = self.host_total_memory_bytes()?;
            let effective_limit_bytes = effective_memory_limit_bytes_from_host(host_total_bytes);
            // `vm_stat` exposes page counts rather than one authoritative
            // available-memory value. Keep its output bounded by the shared
            // no-shell process runner, and treat parse/probe failure as an
            // absent estimate without failing the rest of the snapshot.
            let available_bytes =
                self.run_process("/usr/bin/vm_stat", &[])
                    .ok()
                    .and_then(|output| {
                        parse_macos_available_memory_estimate_bytes(&output, host_total_bytes)
                    });
            return Ok(SystemMemorySnapshot {
                host_total_bytes,
                effective_limit_bytes,
                available_bytes,
                available_bytes_method: available_bytes
                    .map(|_| "vm_stat:free+inactive+speculative pages"),
            });
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "system memory snapshots are supported on macOS and Linux",
            ))
        }
    }

    #[cfg(target_os = "linux")]
    fn read_system_memory_file(&self, path: &Path) -> io::Result<String> {
        let read_bounded = self.hooks.read_file_bounded.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "bounded system-memory reads are unavailable",
            )
        })?;
        read_bounded(path, MAX_SYSTEM_MEMORY_FILE_BYTES)
    }

    fn read_cgroup_memory_limit_file(&self, path: &Path) -> io::Result<String> {
        if let Some(read_bounded) = self.hooks.read_file_bounded.as_ref() {
            read_bounded(path, MAX_CGROUP_MEMORY_FILE_BYTES)
        } else {
            (self.hooks.read_file)(path)
        }
    }

    #[cfg(target_os = "linux")]
    fn read_bounded_cgroup_memory_limit_file(&self, path: &Path) -> io::Result<String> {
        let read_bounded = self.hooks.read_file_bounded.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "bounded cgroup-memory reads are unavailable",
            )
        })?;
        read_bounded(path, MAX_CGROUP_MEMORY_FILE_BYTES)
    }

    /// Current-process resident set size. On Linux this reads `/proc/self/status`
    /// (and falls back to `/proc/self/statm` plus `getconf PAGE_SIZE`); on macOS
    /// it uses `ps` without a shell and enforces a one-second process timeout.
    pub fn current_process_rss_bytes(&self) -> io::Result<u64> {
        #[cfg(target_os = "linux")]
        {
            if let Ok(status) = (self.hooks.read_file)(Path::new("/proc/self/status")) {
                if let Some(rss) = parse_linux_vm_rss_bytes(&status) {
                    return Ok(rss);
                }
            }
            if let Ok(statm) = (self.hooks.read_file)(Path::new("/proc/self/statm")) {
                if let Ok(page_size) = self.run_process("getconf", &["PAGE_SIZE".into()]) {
                    if let (Some(rss_pages), Some(page_size)) = (
                        parse_linux_statm_resident_pages(&statm),
                        parse_positive_decimal(&page_size),
                    ) {
                        if let Some(rss) = rss_pages.checked_mul(page_size) {
                            return Ok(rss);
                        }
                    }
                }
            }
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "could not read current-process RSS from procfs",
            ));
        }

        #[cfg(target_os = "macos")]
        {
            let pid = std::process::id().to_string();
            let output =
                self.run_process("/bin/ps", &["-o".into(), "rss=".into(), "-p".into(), pid])?;
            parse_macos_ps_rss_bytes(&output)
                .ok_or_else(|| invalid_data("ps did not return a numeric RSS in KiB"))
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "process RSS sampling is supported on macOS and Linux",
            ))
        }
    }

    /// Current process memory in the common runtime shape. Native runtimes do
    /// not report V8 heap/external subdivisions, so those fields are zero.
    pub fn memory_usage(&self) -> io::Result<MemoryUsage> {
        Ok(MemoryUsage {
            rss: self.current_process_rss_bytes()?,
            heap_used: 0,
            heap_total: 0,
            external: 0,
        })
    }

    /// Current system-wide swap totals on Linux and macOS. Linux procfs reads
    /// are capped at 64 KiB; the macOS `sysctl` query is no-shell, time-limited,
    /// and output-capped by the shared process runner. Unsupported platforms
    /// return `Unsupported` so diagnostic callers can mark the value absent.
    pub fn system_swap_usage(&self) -> io::Result<SystemSwapUsage> {
        #[cfg(target_os = "linux")]
        {
            let read_file = self.hooks.read_file_bounded.as_ref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "bounded system-memory reads are unavailable",
                )
            })?;
            let meminfo = read_file(Path::new("/proc/meminfo"), MAX_SYSTEM_MEMORY_FILE_BYTES)?;
            return parse_linux_swap_usage_bytes(&meminfo)
                .ok_or_else(|| invalid_data("could not parse Linux SwapTotal/SwapFree"));
        }

        #[cfg(target_os = "macos")]
        {
            let output =
                self.run_process("/usr/sbin/sysctl", &["-n".into(), "vm.swapusage".into()])?;
            return parse_macos_swap_usage_bytes(&output)
                .ok_or_else(|| invalid_data("could not parse macOS vm.swapusage"));
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "system swap sampling is supported on macOS and Linux",
            ))
        }
    }

    /// One-time startup sample containing both RSS and the effective limit.
    /// Use `memory_usage()` for later pressure checks so cgroup discovery is not
    /// repeated on every check.
    pub fn sample(&self) -> io::Result<NativeMemorySample> {
        Ok(NativeMemorySample {
            memory_usage: self.memory_usage()?,
            effective_memory_limit_bytes: self.effective_memory_limit_bytes()?,
        })
    }

    /// Cumulative process CPU time in microseconds. Linux uses procfs ticks and
    /// caches `_SC_CLK_TCK` from `getconf`; macOS uses bounded `ps utime` and
    /// `ps stime` fields. Those macOS counters are separate but only have
    /// centisecond resolution: calling libc `getrusage` directly would require
    /// unsafe code, which this crate forbids. `None` means the optional CPU
    /// metric is unavailable.
    pub fn process_cpu_usage(&self) -> Option<CpuUsage> {
        #[cfg(target_os = "linux")]
        {
            let stat = (self.hooks.read_file)(Path::new("/proc/self/stat")).ok()?;
            let ticks = *self.linux_clock_ticks_per_second.get_or_init(|| {
                self.run_process("getconf", &["CLK_TCK".into()])
                    .ok()
                    .and_then(|output| parse_positive_decimal(&output))
            })?;
            return parse_linux_process_cpu_ticks(&stat, ticks);
        }

        #[cfg(target_os = "macos")]
        {
            let pid = std::process::id().to_string();
            // Keep user and system totals separate. `ps` reports centiseconds;
            // `getrusage` would provide finer resolution, but its libc call is
            // unsafe and this crate forbids unsafe code.
            let output = (self.hooks.run_process)(
                "/bin/ps",
                &[
                    "-o".into(),
                    "utime=".into(),
                    "-o".into(),
                    "stime=".into(),
                    "-p".into(),
                    pid,
                ],
                PROCESS_PROBE_TIMEOUT,
            )
            .ok()?;
            parse_ps_cpu_usage_micros(&output)
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            None
        }
    }

    fn run_process(&self, program: &str, args: &[String]) -> io::Result<String> {
        (self.hooks.run_process)(program, args, PROCESS_PROBE_TIMEOUT)
    }
}

fn read_bounded_file(path: &Path, max_bytes: usize) -> io::Result<String> {
    let file = std::fs::File::open(path)?;
    let mut bytes = Vec::with_capacity(max_bytes.min(MAX_SYSTEM_MEMORY_FILE_BYTES) + 1);
    file.take(max_bytes.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "system-memory file exceeded the read bound",
        ));
    }
    String::from_utf8(bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

impl Default for NativeMemoryProbe {
    fn default() -> Self {
        Self::system()
    }
}

/// Discover cgroup v2 then v1 memory limits from an injectable reader. Invalid,
/// unlimited, implausibly small, unsafe-integer, and above-host values are
/// ignored exactly as they are by the TypeScript monitor.
pub fn effective_memory_limit_from_cgroups<R>(host_total_bytes: u64, mut read_file: R) -> u64
where
    R: FnMut(&Path) -> io::Result<String>,
{
    if let Some(limit) = read_cgroup_memory_limit_with(
        Path::new(CGROUP_V2_MEMORY_LIMIT),
        host_total_bytes,
        &mut read_file,
    ) {
        return limit;
    }
    read_cgroup_memory_limit_with(
        Path::new(CGROUP_V1_MEMORY_LIMIT),
        host_total_bytes,
        &mut read_file,
    )
    .unwrap_or(host_total_bytes)
}

pub fn read_cgroup_memory_limit_with<R>(
    path: &Path,
    host_total_bytes: u64,
    read_file: &mut R,
) -> Option<u64>
where
    R: FnMut(&Path) -> io::Result<String>,
{
    let raw = read_file(path).ok()?;
    parse_cgroup_memory_limit(&raw, host_total_bytes)
}

pub fn parse_cgroup_memory_limit(raw: &str, host_total_bytes: u64) -> Option<u64> {
    let raw = raw.trim();
    if raw == "max" || !is_signed_decimal(raw) {
        return None;
    }
    let limit = raw.parse::<i128>().ok()?;
    if limit <= 0
        || limit < MIN_CGROUP_MEMORY_LIMIT_BYTES as i128
        || limit > MAX_SAFE_CGROUP_MEMORY_LIMIT_BYTES as i128
        || (host_total_bytes > 0 && limit as u64 > host_total_bytes)
    {
        return None;
    }
    Some(limit as u64)
}

pub fn parse_linux_mem_total_bytes(meminfo: &str) -> Option<u64> {
    let line = meminfo
        .lines()
        .find(|line| line.split_ascii_whitespace().next() == Some("MemTotal:"))?;
    let mut columns = line.split_ascii_whitespace();
    let _ = columns.next()?;
    let kilobytes = columns.next()?.parse::<u64>().ok()?;
    if columns.next()? != "kB" {
        return None;
    }
    kilobytes.checked_mul(1024)
}

pub fn parse_linux_mem_available_bytes(meminfo: &str) -> Option<u64> {
    let line = meminfo
        .lines()
        .find(|line| line.split_ascii_whitespace().next() == Some("MemAvailable:"))?;
    let mut columns = line.split_ascii_whitespace();
    let _ = columns.next()?;
    let kilobytes = columns.next()?.parse::<u64>().ok()?;
    if columns.next()? != "kB" {
        return None;
    }
    kilobytes.checked_mul(1024)
}

/// Estimate macOS available memory from the reclaimable VM page states shown
/// by `/usr/bin/vm_stat`. The tool's reported page size is used instead of an
/// assumed 4 KiB page size. Purgeable pages are intentionally omitted because
/// they can overlap other reported page states. This is a diagnostic estimate,
/// not a memory-pressure signal or a promise that memory can be allocated.
pub fn parse_macos_available_memory_estimate_bytes(
    output: &str,
    host_total_bytes: u64,
) -> Option<u64> {
    if host_total_bytes == 0 {
        return None;
    }

    let mut page_size = None;
    let mut free_pages = None;
    let mut inactive_pages = None;
    let mut speculative_pages = None;

    for line in output.lines() {
        let line = line.trim();
        if let Some(header) = line.strip_prefix("Mach Virtual Memory Statistics: (page size of ") {
            if page_size.is_some() {
                return None;
            }
            let raw = header.strip_suffix(" bytes)")?;
            page_size = Some(parse_unsigned_decimal(raw)?);
            continue;
        }

        let Some((label, raw_value)) = line.split_once(':') else {
            continue;
        };
        let target = match label.trim() {
            "Pages free" => &mut free_pages,
            "Pages inactive" => &mut inactive_pages,
            "Pages speculative" => &mut speculative_pages,
            _ => continue,
        };
        if target.is_some() {
            return None;
        }
        let raw_value = raw_value
            .trim()
            .strip_suffix('.')
            .unwrap_or(raw_value.trim());
        *target = Some(parse_unsigned_decimal(raw_value)?);
    }

    let page_size = page_size?;
    if page_size == 0 {
        return None;
    }
    let reclaimable_pages = free_pages?
        .checked_add(inactive_pages?)?
        .checked_add(speculative_pages?)?;
    let estimate = reclaimable_pages.checked_mul(page_size)?;
    (estimate <= host_total_bytes).then_some(estimate)
}

/// Parse Linux `/proc/meminfo` swap counters, whose source units are KiB.
pub fn parse_linux_swap_usage_bytes(meminfo: &str) -> Option<SystemSwapUsage> {
    fn field_kib(meminfo: &str, key: &str) -> Option<u64> {
        let line = meminfo
            .lines()
            .find(|line| line.split_ascii_whitespace().next() == Some(key))?;
        let mut columns = line.split_ascii_whitespace();
        let _ = columns.next()?;
        let value = columns.next()?.parse::<u64>().ok()?;
        (columns.next()? == "kB").then_some(value)
    }

    let total_kib = field_kib(meminfo, "SwapTotal:")?;
    let free_kib = field_kib(meminfo, "SwapFree:")?;
    if free_kib > total_kib {
        return None;
    }
    let total_bytes = total_kib.checked_mul(1024)?;
    let free_bytes = free_kib.checked_mul(1024)?;
    Some(SystemSwapUsage {
        total_bytes,
        used_bytes: total_bytes.checked_sub(free_bytes)?,
        free_bytes,
    })
}

/// Parse the stable field form emitted by `sysctl -n vm.swapusage`, for example
/// `total = 1024.00M used = 0.00M free = 1024.00M`.
pub fn parse_macos_swap_usage_bytes(output: &str) -> Option<SystemSwapUsage> {
    fn field(output: &str, name: &str) -> Option<u64> {
        let fields = output.split_ascii_whitespace().collect::<Vec<_>>();
        let value = fields
            .windows(3)
            .find(|parts| parts[0] == name && parts[1] == "=")?
            .get(2)?;
        parse_macos_size_bytes(value)
    }

    let total_bytes = field(output, "total")?;
    let used_bytes = field(output, "used")?;
    let free_bytes = field(output, "free")?;
    (used_bytes <= total_bytes && free_bytes <= total_bytes).then_some(SystemSwapUsage {
        total_bytes,
        used_bytes,
        free_bytes,
    })
}

fn parse_macos_size_bytes(value: &str) -> Option<u64> {
    let suffix = value.chars().last()?.to_ascii_uppercase();
    let magnitude = value[..value.len().checked_sub(suffix.len_utf8())?]
        .parse::<f64>()
        .ok()?;
    if !magnitude.is_finite() || magnitude < 0.0 {
        return None;
    }
    let multiplier = match suffix {
        'B' => 1.0,
        'K' => 1024.0,
        'M' => 1024.0_f64.powi(2),
        'G' => 1024.0_f64.powi(3),
        'T' => 1024.0_f64.powi(4),
        _ => return None,
    };
    let bytes = magnitude * multiplier;
    (bytes.is_finite() && bytes <= u64::MAX as f64).then_some(bytes.round() as u64)
}

pub fn parse_linux_vm_rss_bytes(status: &str) -> Option<u64> {
    let line = status
        .lines()
        .find(|line| line.split_ascii_whitespace().next() == Some("VmRSS:"))?;
    let mut columns = line.split_ascii_whitespace();
    let _ = columns.next()?;
    let kilobytes = columns.next()?.parse::<u64>().ok()?;
    if columns.next()? != "kB" {
        return None;
    }
    kilobytes.checked_mul(1024)
}

pub fn parse_linux_statm_resident_pages(statm: &str) -> Option<u64> {
    statm.split_ascii_whitespace().nth(1)?.parse().ok()
}

pub fn parse_macos_ps_rss_bytes(output: &str) -> Option<u64> {
    let kibibytes = output
        .split_ascii_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    kibibytes.checked_mul(1024)
}

pub fn parse_linux_process_cpu_ticks(stat: &str, ticks_per_second: u64) -> Option<CpuUsage> {
    if ticks_per_second == 0 {
        return None;
    }
    // The `comm` field is parenthesized and may itself contain spaces or `)`;
    // fields after its final close paren begin at field 3 (`state`).
    let fields = stat
        .rsplit_once(')')?
        .1
        .split_ascii_whitespace()
        .collect::<Vec<_>>();
    let user_ticks = fields.get(11)?.parse::<u64>().ok()?;
    let system_ticks = fields.get(12)?.parse::<u64>().ok()?;
    Some(CpuUsage {
        user_us: ticks_to_micros(user_ticks, ticks_per_second),
        system_us: ticks_to_micros(system_ticks, ticks_per_second),
    })
}

pub fn parse_ps_cpu_time_micros(output: &str) -> Option<i64> {
    let value = output.split_ascii_whitespace().next()?;
    let (days, clock) = if let Some((days, clock)) = value.split_once('-') {
        (days.parse::<u64>().ok()?, clock)
    } else {
        (0, value)
    };
    let parts = clock.split(':').collect::<Vec<_>>();
    let (hours, minutes, seconds) = match parts.as_slice() {
        [minutes, seconds] => (
            0u64,
            minutes.parse::<u64>().ok()?,
            seconds.parse::<f64>().ok()?,
        ),
        [hours, minutes, seconds] => (
            hours.parse::<u64>().ok()?,
            minutes.parse::<u64>().ok()?,
            seconds.parse::<f64>().ok()?,
        ),
        _ => return None,
    };
    if !seconds.is_finite() || seconds < 0.0 || minutes >= 60 || hours >= 24 && days > 0 {
        return None;
    }
    let whole_seconds =
        days as f64 * 86_400.0 + hours as f64 * 3_600.0 + minutes as f64 * 60.0 + seconds;
    if whole_seconds * 1_000_000.0 > i64::MAX as f64 {
        return Some(i64::MAX);
    }
    Some((whole_seconds * 1_000_000.0).round() as i64)
}

pub fn parse_ps_cpu_usage_micros(output: &str) -> Option<CpuUsage> {
    let mut parts = output.split_ascii_whitespace();
    Some(CpuUsage {
        user_us: parse_ps_cpu_time_micros(parts.next()?)?,
        system_us: parse_ps_cpu_time_micros(parts.next()?)?,
    })
}

fn ticks_to_micros(ticks: u64, ticks_per_second: u64) -> i64 {
    let micros = (ticks as u128).saturating_mul(1_000_000) / ticks_per_second as u128;
    micros.min(i64::MAX as u128) as i64
}

#[cfg(target_os = "linux")]
fn parse_positive_decimal(raw: &str) -> Option<u64> {
    let value = parse_unsigned_decimal(raw)?;
    (value > 0).then_some(value)
}

fn parse_unsigned_decimal(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    raw.parse().ok()
}

fn is_signed_decimal(raw: &str) -> bool {
    let digits = raw.strip_prefix('-').unwrap_or(raw);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(target_os = "macos")]
fn effective_memory_limit_bytes_from_host(host_total_bytes: u64) -> u64 {
    effective_memory_limit_from_cgroups(host_total_bytes, |_| {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "cgroup memory limits are only probed on Linux",
        ))
    })
}

/// Run a fixed OS probe without a shell, with a wall-time and output bound.
/// `NativeMemoryProbe` only passes small, known OS queries (`ps`, `sysctl`,
/// `getconf`), and stdout is drained concurrently while retaining at most 4 KiB.
fn run_bounded_process(program: &str, args: &[String], timeout: Duration) -> io::Result<String> {
    let started = Instant::now();
    let mut child = Command::new(OsStr::new(program))
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("native probe process stdout was not captured"))?;
    let stdout_reader = thread::spawn(move || -> io::Result<Vec<u8>> {
        let mut captured = Vec::with_capacity(MAX_PROCESS_OUTPUT_BYTES + 1);
        let mut buffer = [0_u8; 512];
        loop {
            let count = stdout.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            let remaining = MAX_PROCESS_OUTPUT_BYTES + 1 - captured.len();
            captured.extend_from_slice(&buffer[..count.min(remaining)]);
        }
        Ok(captured)
    });
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) if started.elapsed() < timeout => {
                thread::sleep(Duration::from_millis(5));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                break Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("native probe process {program} timed out"),
                ));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                break Err(error);
            }
        }
    };
    let stdout = stdout_reader
        .join()
        .map_err(|_| io::Error::other("native probe stdout reader panicked"))??;
    if stdout.len() > MAX_PROCESS_OUTPUT_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "native probe process output exceeded the 4 KiB bound",
        ));
    }
    let status = status?;
    if !status.success() {
        return Err(io::Error::other(format!(
            "native probe process {program} exited with {status}"
        )));
    }
    String::from_utf8(stdout).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn file_map(files: &[(&str, &str)]) -> HashMap<String, String> {
        files
            .iter()
            .map(|(path, content)| ((*path).to_owned(), (*content).to_owned()))
            .collect()
    }

    #[test]
    fn cgroup_preference_validation_and_host_fallback_match_source() {
        let files = file_map(&[
            (CGROUP_V2_MEMORY_LIMIT, "1073741824\n"),
            (CGROUP_V1_MEMORY_LIMIT, "2147483648\n"),
        ]);
        assert_eq!(
            effective_memory_limit_from_cgroups(8 * 1024 * 1024 * 1024, |path| {
                files
                    .get(path.to_str().unwrap())
                    .cloned()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing"))
            }),
            1024 * 1024 * 1024
        );

        let fallback = file_map(&[
            (CGROUP_V2_MEMORY_LIMIT, "max\n"),
            (CGROUP_V1_MEMORY_LIMIT, "9223372036854771712\n"),
        ]);
        assert_eq!(
            effective_memory_limit_from_cgroups(16 * 1024 * 1024 * 1024, |path| {
                fallback
                    .get(path.to_str().unwrap())
                    .cloned()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing"))
            }),
            16 * 1024 * 1024 * 1024
        );
        assert_eq!(parse_cgroup_memory_limit(" 67108864\n", 0), Some(67108864));
        for bad in [
            "max",
            "0",
            "-1",
            "+67108864",
            "67108863",
            "9007199254740992",
            "not-a-number",
            "1.5",
        ] {
            assert_eq!(parse_cgroup_memory_limit(bad, 0), None, "{bad}");
        }
        assert_eq!(parse_cgroup_memory_limit("134217728", 100_000_000), None);
    }

    #[test]
    fn host_and_process_memory_procfs_parsers_convert_kib_and_pages() {
        assert_eq!(
            parse_linux_mem_total_bytes("MemFree: 2 kB\nMemTotal: 16384 kB\n"),
            Some(16 * 1024 * 1024)
        );
        assert_eq!(
            parse_linux_vm_rss_bytes("Name:\tcanopy\nVmRSS:\t2048 kB\n"),
            Some(2 * 1024 * 1024)
        );
        assert_eq!(parse_linux_statm_resident_pages("10 4 2 0 0 0 0"), Some(4));
        assert_eq!(parse_macos_ps_rss_bytes("12345\n"), Some(12_641_280));
        assert_eq!(parse_macos_ps_rss_bytes("RSS\n"), None);
    }

    #[test]
    fn linux_and_macos_cpu_parsers_return_cumulative_microseconds() {
        let stat = "42 (canopy worker (nested)) S 1 2 3 4 5 6 7 8 9 10 110 20 0";
        assert_eq!(
            parse_linux_process_cpu_ticks(stat, 100),
            Some(CpuUsage {
                user_us: 1_100_000,
                system_us: 200_000,
            })
        );
        assert_eq!(parse_ps_cpu_time_micros("02:03:04\n"), Some(7_384_000_000));
        assert_eq!(
            parse_ps_cpu_time_micros("1-02:03:04\n"),
            Some(93_784_000_000)
        );
        assert_eq!(
            parse_ps_cpu_usage_micros("0:00.01 0:00.02\n"),
            Some(CpuUsage {
                user_us: 10_000,
                system_us: 20_000,
            })
        );
        assert_eq!(parse_ps_cpu_time_micros("bad"), None);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn injected_host_and_cgroup_reads_use_v2_then_v1_without_environment_access() {
        let files = file_map(&[
            ("/proc/meminfo", "MemTotal: 8388608 kB\n"),
            (CGROUP_V2_MEMORY_LIMIT, "max\n"),
            (CGROUP_V1_MEMORY_LIMIT, "2147483648\n"),
        ]);
        let called = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let calls = Arc::clone(&called);
        let file_snapshot = files.clone();
        let probe = NativeMemoryProbe::with_hooks(NativeMemoryProbeHooks::new(
            move |path: &Path| {
                calls
                    .lock()
                    .unwrap()
                    .push(path.to_string_lossy().into_owned());
                file_snapshot
                    .get(&path.to_string_lossy().to_string())
                    .cloned()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing"))
            },
            |_program: &str, _args: &[String], _timeout: Duration| {
                Err(io::Error::other("not used"))
            },
        ));
        assert_eq!(
            probe.host_total_memory_bytes().unwrap(),
            8 * 1024 * 1024 * 1024
        );
        assert_eq!(
            probe.effective_memory_limit_bytes().unwrap(),
            2 * 1024 * 1024 * 1024
        );
        let paths = called.lock().unwrap();
        assert!(paths.iter().any(|path| path == CGROUP_V2_MEMORY_LIMIT));
        assert!(paths.iter().any(|path| path == CGROUP_V1_MEMORY_LIMIT));
    }

    #[test]
    fn injected_process_runner_receives_fixed_args_and_timeout() {
        let seen = Arc::new(std::sync::Mutex::new(
            None::<(String, Vec<String>, Duration)>,
        ));
        let output = Arc::clone(&seen);
        let probe = NativeMemoryProbe::with_hooks(NativeMemoryProbeHooks::new(
            |_path: &Path| Err(io::Error::new(io::ErrorKind::NotFound, "not found")),
            move |program: &str, args: &[String], timeout: Duration| {
                *output.lock().unwrap() = Some((program.to_owned(), args.to_vec(), timeout));
                Ok("4096\n".to_owned())
            },
        ));
        // This parser invocation validates the seam without relying on the
        // current test host's platform-specific RSS path.
        let value = (probe.hooks.run_process)(
            "/bin/ps",
            &["-o".into(), "rss=".into(), "-p".into(), "123".into()],
            PROCESS_PROBE_TIMEOUT,
        )
        .unwrap();
        assert_eq!(parse_macos_ps_rss_bytes(&value), Some(4 * 1024 * 1024));
        let seen = seen.lock().unwrap().clone().unwrap();
        assert_eq!(seen.0, "/bin/ps");
        assert_eq!(
            seen.1.iter().map(String::as_str).collect::<Vec<_>>(),
            ["-o", "rss=", "-p", "123"]
        );
        assert_eq!(seen.2, PROCESS_PROBE_TIMEOUT);
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn system_probe_reads_native_macos_process_and_host_memory() {
        let probe = NativeMemoryProbe::system();
        let sample = probe.sample().expect("macOS RSS and host memory probes");

        assert!(sample.memory_usage.rss > 0);
        assert!(sample.effective_memory_limit_bytes > 0);
        assert_eq!(sample.memory_usage.heap_used, 0);
        assert_eq!(sample.memory_usage.heap_total, 0);
        assert!(probe.process_cpu_usage().is_some());
    }
}
