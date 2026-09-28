//! Provider-neutral port of `memoryPressureMonitor.ts`.
//!
//! The monitor owns pressure decisions, sampling history, cleanup ordering,
//! cooldown/escalation state, diagnostics cadence, and session-generation
//! cancellation. Callers inject process/V8 measurements, a clock and CPU
//! sampler, cleanup operations, telemetry, diagnostics, and event delivery.
//! This module intentionally does not probe the host, read cgroups, access V8,
//! know Canopy `Config`, or assume any particular application runtime.
//!
//! [`MemoryPressureMonitor::perform_check`] is async because cleanup providers
//! may be async. Concurrent checks may be polled on separate tasks: weaker or
//! duplicate work is skipped while a cleanup runs, and the strongest newer
//! recommendation is queued. [`schedule_check`](MemoryPressureMonitor::schedule_check)
//! and [`run_scheduled_check`](MemoryPressureMonitor::run_scheduled_check)
//! provide the source monitor's one-pending-check coalescing state; the caller
//! supplies its own microtask/event-loop scheduling mechanism.

use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use super::memory_diagnostics_dumper::{
    MemoryDiagnosticsDumper, MemoryDumpInput, MemoryDumpTrigger,
};
use super::memory_pressure_policy::{
    CleanupAction, CleanupRecommendation, CleanupStep, DEFAULT_PRESSURE_CONFIG,
    MemoryPressureConfig, PressureLevel, cleanup_action_rank, cleanup_cooldown_ms, pressure_level,
    recommend_cleanup, should_emit_repeated_diagnostic, validate_memory_pressure_config,
};
use super::runtime_sample_ring::{CpuUsage, MemoryUsage, RuntimeSample, RuntimeSampleRing};
use futures_util::FutureExt;

/// Measurements read once for pressure determination and runtime sampling.
/// The effective memory limit is captured separately at monitor construction,
/// matching the source's cgroup/host limit discovery at construction time.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryPressureMeasurement {
    pub memory_usage: MemoryUsage,
    pub heap_size_limit_bytes: u64,
}

/// Injected measurements returned by the caller. `Err` is treated as a failed
/// process-memory sample: the check degrades to normal pressure and records no
/// runtime sample.
pub type MeasurementProvider =
    Arc<dyn Fn() -> Result<MemoryPressureMeasurement, String> + Send + Sync>;
/// Unix-millisecond wall clock. Injecting this makes cooldown/backoff tests
/// deterministic.
pub type ClockProvider = Arc<dyn Fn() -> i64 + Send + Sync>;
/// Cumulative process CPU time provider. `None` or panic safely means zero,
/// matching the source's optional CPU observability fallback.
pub type CpuUsageProvider = Arc<dyn Fn() -> Option<CpuUsage> + Send + Sync>;
/// Async cleanup step. `CompactHistory` errors are logged and swallowed to
/// match the source's best-effort compaction step; errors from other steps
/// fail the current cleanup.
pub type CleanupStepFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
pub type CleanupStepHandler = Arc<dyn Fn(CleanupStep) -> CleanupStepFuture + Send + Sync>;
pub type TelemetryGate = Arc<dyn Fn() -> bool + Send + Sync>;
pub type SampleReporter = Arc<dyn Fn(RuntimeSample) + Send + Sync>;
pub type DumpInputFactory = Arc<dyn Fn(Vec<RuntimeSample>) -> MemoryDumpInput + Send + Sync>;
pub type EventReporter = Arc<dyn Fn(MemoryPressureEvent) + Send + Sync>;
pub type LogReporter = Arc<dyn Fn(MemoryPressureLog) + Send + Sync>;

/// All application- and platform-specific behavior used by the monitor.
#[derive(Clone)]
pub struct MemoryPressureMonitorHooks {
    pub measure: MeasurementProvider,
    pub clock_ms: ClockProvider,
    pub cpu_usage: CpuUsageProvider,
    pub cpu_core_count: usize,
    pub cleanup_step: CleanupStepHandler,
    pub telemetry_active: TelemetryGate,
    pub record_telemetry_sample: SampleReporter,
    /// When set, the monitor invokes `dump` synchronously for hard/critical
    /// pressure (so the dumper's minimal phase-one file is written before
    /// cleanup starts). The full-collection future is detached on a current
    /// Tokio runtime; outside Tokio, phase one is still retained and phase two
    /// is skipped.
    pub diagnostics_dumper: Option<Arc<MemoryDiagnosticsDumper>>,
    /// Supplies session/V8/process fields to the existing dumper. The monitor
    /// overwrites `recent_samples` with its own ring snapshot.
    pub diagnostics_input: DumpInputFactory,
    pub event: EventReporter,
    pub log: LogReporter,
}

impl MemoryPressureMonitorHooks {
    /// Build hooks with deterministic-safe no-op observability and diagnostics.
    /// The measurement and cleanup providers are required.
    pub fn new<M, F>(measure: M, cleanup_step: F) -> Self
    where
        M: Fn() -> Result<MemoryPressureMeasurement, String> + Send + Sync + 'static,
        F: Fn(CleanupStep) -> CleanupStepFuture + Send + Sync + 'static,
    {
        Self {
            measure: Arc::new(measure),
            clock_ms: Arc::new(system_time_millis),
            cpu_usage: Arc::new(|| None),
            cpu_core_count: std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1),
            cleanup_step: Arc::new(cleanup_step),
            telemetry_active: Arc::new(|| false),
            record_telemetry_sample: Arc::new(|_| {}),
            diagnostics_dumper: None,
            diagnostics_input: Arc::new(|_| MemoryDumpInput::default()),
            event: Arc::new(|_| {}),
            log: Arc::new(|_| {}),
        }
    }
}

/// Failure diagnostic emitted at source cadence (3, 10, then every 20).
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryCleanupFailureEvent {
    pub rss: u64,
    pub consecutive_failures: u64,
    pub recommendation: CleanupRecommendation,
    pub error: String,
}

/// Ineffectiveness diagnostic emitted at source cadence.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryCleanupIneffectiveEvent {
    pub rss: u64,
    pub freed_bytes: i128,
    pub freed_ratio: f64,
    pub consecutive_ineffective_cleanups: u64,
    pub recommendation: CleanupRecommendation,
}

/// Structured equivalent of the source EventEmitter events.
#[derive(Clone, Debug, PartialEq)]
pub enum MemoryPressureEvent {
    CleanupFailed(MemoryCleanupFailureEvent),
    CleanupIneffective(MemoryCleanupIneffectiveEvent),
}

impl MemoryPressureEvent {
    pub const fn event_name(&self) -> &'static str {
        match self {
            Self::CleanupFailed(_) => "memory-cleanup-failed",
            Self::CleanupIneffective(_) => "memory-cleanup-ineffective",
        }
    }
}

/// Structured log messages. The injected logger can map levels to Canopy's
/// existing debug logger or another application logging backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryPressureLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryPressureLog {
    pub level: MemoryPressureLogLevel,
    pub message: String,
}

struct MonitorState {
    pending_check: bool,
    cleanup_in_progress: bool,
    cleanup_starting: bool,
    active_cleanup_action: CleanupAction,
    last_cleanup_action: CleanupAction,
    queued_cleanup_recommendation: Option<CleanupRecommendation>,
    last_cleanup_time_ms: i64,
    consecutive_cleanup_failures: u64,
    consecutive_ineffective_cleanups: u64,
    consecutive_ineffective_aggressive_cleanups: u64,
    cleanup_generation: u64,
    has_logged_sampling_error: bool,
    runtime_samples: RuntimeSampleRing,
}

#[derive(Clone, Debug)]
struct CleanupTicket {
    recommendation: CleanupRecommendation,
    generation: u64,
    rss_before: u64,
}

/// Memory pressure monitor state machine.
pub struct MemoryPressureMonitor {
    config: MemoryPressureConfig,
    effective_memory_limit_bytes: u64,
    hooks: MemoryPressureMonitorHooks,
    state: Mutex<MonitorState>,
}

impl MemoryPressureMonitor {
    /// Construct a monitor with a caller-computed host/cgroup effective limit.
    /// No operating-system probing is performed by this crate.
    pub fn new(
        config: MemoryPressureConfig,
        effective_memory_limit_bytes: u64,
        hooks: MemoryPressureMonitorHooks,
    ) -> Result<Self, &'static str> {
        validate_memory_pressure_config(&config)?;
        let clock = Arc::clone(&hooks.clock_ms);
        let cpu = Arc::clone(&hooks.cpu_usage);
        let samples =
            RuntimeSampleRing::with_providers(move || clock(), move || cpu(), hooks.cpu_core_count);
        let monitor = Self {
            config,
            effective_memory_limit_bytes,
            hooks,
            state: Mutex::new(MonitorState {
                pending_check: false,
                cleanup_in_progress: false,
                cleanup_starting: false,
                active_cleanup_action: CleanupAction::None,
                last_cleanup_action: CleanupAction::None,
                queued_cleanup_recommendation: None,
                last_cleanup_time_ms: 0,
                consecutive_cleanup_failures: 0,
                consecutive_ineffective_cleanups: 0,
                consecutive_ineffective_aggressive_cleanups: 0,
                cleanup_generation: 0,
                has_logged_sampling_error: false,
                runtime_samples: samples,
            }),
        };
        if effective_memory_limit_bytes == 0 {
            monitor.log(
                MemoryPressureLogLevel::Warn,
                "Effective memory limit is not positive; RSS pressure checks are disabled"
                    .to_owned(),
            );
        }
        Ok(monitor)
    }

    /// Construct with source defaults.
    pub fn with_defaults(
        effective_memory_limit_bytes: u64,
        hooks: MemoryPressureMonitorHooks,
    ) -> Self {
        Self::new(DEFAULT_PRESSURE_CONFIG, effective_memory_limit_bytes, hooks)
            .expect("source default memory pressure configuration is valid")
    }

    /// Return the consecutive cleanup failure count.
    pub fn consecutive_failures(&self) -> u64 {
        lock(&self.state).consecutive_cleanup_failures
    }

    /// Reset cleanup failure and ineffectiveness counters, matching the source.
    pub fn reset_consecutive_failures(&self) {
        let mut state = lock(&self.state);
        state.consecutive_cleanup_failures = 0;
        state.consecutive_ineffective_cleanups = 0;
        state.consecutive_ineffective_aggressive_cleanups = 0;
    }

    /// Invalidate old async cleanup tails and clear all session-scoped state.
    /// A currently awaited cleanup step cannot be interrupted, but no later
    /// step or completion callback from its generation will run.
    pub fn reset_for_new_session(&self) {
        let dumper = {
            let mut state = lock(&self.state);
            state.cleanup_generation = state.cleanup_generation.wrapping_add(1);
            state.pending_check = false;
            state.cleanup_in_progress = false;
            state.cleanup_starting = false;
            state.active_cleanup_action = CleanupAction::None;
            state.queued_cleanup_recommendation = None;
            state.last_cleanup_action = CleanupAction::None;
            state.last_cleanup_time_ms = 0;
            state.consecutive_cleanup_failures = 0;
            state.consecutive_ineffective_cleanups = 0;
            state.consecutive_ineffective_aggressive_cleanups = 0;
            state.runtime_samples.reset();
            self.hooks.diagnostics_dumper.clone()
        };
        if let Some(dumper) = dumper {
            dumper.reset_for_new_session();
        }
    }

    /// Mark one check pending. Returns `true` only for the first call in a
    /// scheduling turn; the caller should enqueue `run_scheduled_check` once.
    pub fn schedule_check(&self) -> bool {
        let mut state = lock(&self.state);
        if state.pending_check {
            return false;
        }
        state.pending_check = true;
        true
    }

    /// Consume the pending-check bit and perform the scheduled check. Clear the
    /// bit before awaiting cleanup so a later turn can schedule another check,
    /// as the source resets its microtask guard after starting cleanup.
    pub async fn run_scheduled_check(&self) {
        let should_run = {
            let mut state = lock(&self.state);
            if !state.pending_check {
                false
            } else {
                state.pending_check = false;
                true
            }
        };
        if should_run {
            self.perform_check().await;
        }
    }

    /// Evaluate current pressure, record one sample, deliver optional metrics,
    /// and run or queue the corresponding cleanup.
    pub async fn perform_check(&self) {
        let measurement = match self.read_measurement() {
            Ok(measurement) => Some(measurement),
            Err(error) => {
                self.log(
                    MemoryPressureLogLevel::Error,
                    format!("Failed to read memory usage for pressure check: {error}"),
                );
                None
            }
        };

        let pressure = measurement.map_or(PressureLevel::Normal, |sample| {
            pressure_level(
                &self.config,
                sample.memory_usage.rss,
                self.effective_memory_limit_bytes,
                sample.memory_usage.heap_used,
                sample.heap_size_limit_bytes,
            )
        });

        if pressure != PressureLevel::Critical {
            lock(&self.state).consecutive_ineffective_aggressive_cleanups = 0;
        }

        if let Some(measurement) = measurement {
            self.record_runtime_sample(measurement.memory_usage);
        }

        if pressure == PressureLevel::Normal {
            return;
        }
        let recommendation = recommend_cleanup(pressure, self.config.enable_explicit_gc);
        if recommendation.action == CleanupAction::None {
            return;
        }

        if let Some(ticket) = self.start_requested_cleanup(recommendation, pressure) {
            self.run_cleanup_chain(ticket).await;
        }
    }

    /// Determine pressure from an already sampled process/V8 snapshot.
    pub fn get_pressure_level(&self, measurement: MemoryPressureMeasurement) -> PressureLevel {
        pressure_level(
            &self.config,
            measurement.memory_usage.rss,
            self.effective_memory_limit_bytes,
            measurement.memory_usage.heap_used,
            measurement.heap_size_limit_bytes,
        )
    }

    /// Copy recent samples for local diagnostics or caller inspection.
    pub fn runtime_samples(&self) -> Vec<RuntimeSample> {
        lock(&self.state).runtime_samples.get_all()
    }

    fn record_runtime_sample(&self, memory: MemoryUsage) {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let sample = {
                let mut state = lock(&self.state);
                state.runtime_samples.record(memory)
            };
            let telemetry_active =
                safe_call(|| (self.hooks.telemetry_active)()).map_err(panic_message)?;
            if telemetry_active {
                safe_call(|| (self.hooks.record_telemetry_sample)(sample))
                    .map_err(panic_message)?;
            }
            Ok::<(), String>(())
        }));
        match result {
            Err(payload) => self.log_sampling_error(panic_message(payload)),
            Ok(Err(error)) => self.log_sampling_error(error),
            Ok(Ok(())) => {}
        }
    }

    fn log_sampling_error(&self, error: String) {
        let first = {
            let mut state = lock(&self.state);
            let first = !state.has_logged_sampling_error;
            state.has_logged_sampling_error = true;
            first
        };
        let prefix = format!("Runtime sampling failed: {error}");
        self.log(
            if first {
                MemoryPressureLogLevel::Error
            } else {
                MemoryPressureLogLevel::Debug
            },
            prefix,
        );
    }

    fn start_requested_cleanup(
        &self,
        recommendation: CleanupRecommendation,
        pressure: PressureLevel,
    ) -> Option<CleanupTicket> {
        let now_ms = safe_call(|| (self.hooks.clock_ms)()).unwrap_or_default();
        let generation = {
            let state = lock(&self.state);
            let escalating = cleanup_action_rank(recommendation.action)
                > cleanup_action_rank(state.last_cleanup_action);
            let elapsed = now_ms.saturating_sub(state.last_cleanup_time_ms) as f64;
            let cooldown = cleanup_cooldown_ms(
                recommendation.action,
                &self.config,
                state.consecutive_ineffective_aggressive_cleanups,
            );
            if !escalating && elapsed < cooldown {
                return None;
            }
            state.cleanup_generation
        };

        self.dump_diagnostics(pressure);

        {
            let mut state = lock(&self.state);
            if state.cleanup_generation != generation {
                return None;
            }
            if state.cleanup_in_progress || state.cleanup_starting {
                let log_message = self.maybe_queue_cleanup(&mut state, &recommendation);
                drop(state);
                if let Some(message) = log_message {
                    self.log(MemoryPressureLogLevel::Debug, message);
                }
                return None;
            }
            let escalating = cleanup_action_rank(recommendation.action)
                > cleanup_action_rank(state.last_cleanup_action);
            let elapsed = now_ms.saturating_sub(state.last_cleanup_time_ms) as f64;
            let cooldown = cleanup_cooldown_ms(
                recommendation.action,
                &self.config,
                state.consecutive_ineffective_aggressive_cleanups,
            );
            if !escalating && elapsed < cooldown {
                return None;
            }
            // Reserve startup before calling injected measurement code so
            // concurrent checks cannot both begin cleanup. Callbacks run
            // outside the state mutex.
            state.cleanup_starting = true;
            state.active_cleanup_action = recommendation.action;
        }

        self.start_reserved_cleanup(recommendation, generation, now_ms)
    }

    /// Measure and start a reserved cleanup. If the pre-cleanup sample fails,
    /// report that attempt and retry the strongest escalation queued while the
    /// provider was running instead of dropping it with the failed starter.
    fn start_reserved_cleanup(
        &self,
        mut recommendation: CleanupRecommendation,
        generation: u64,
        initial_start_time_ms: i64,
    ) -> Option<CleanupTicket> {
        let mut start_time_ms = initial_start_time_ms;
        loop {
            let rss_before = match self.read_measurement() {
                Ok(measurement) => measurement.memory_usage.rss,
                Err(error) => {
                    self.record_cleanup_failure(&recommendation, error, generation);
                    let next_recommendation = {
                        let mut state = lock(&self.state);
                        if state.cleanup_generation != generation || !state.cleanup_starting {
                            return None;
                        }
                        if let Some(next) = state.queued_cleanup_recommendation.take() {
                            state.active_cleanup_action = next.action;
                            Some(next)
                        } else {
                            state.cleanup_starting = false;
                            state.active_cleanup_action = CleanupAction::None;
                            return None;
                        }
                    };
                    recommendation = next_recommendation?;
                    start_time_ms = safe_call(|| (self.hooks.clock_ms)()).unwrap_or_default();
                    continue;
                }
            };

            // The cooldown timestamp starts when the cleanup is actually
            // ready to run, after diagnostics and the pre-cleanup sample.
            start_time_ms = safe_call(|| (self.hooks.clock_ms)()).unwrap_or(start_time_ms);
            let mut state = lock(&self.state);
            if state.cleanup_generation != generation || !state.cleanup_starting {
                return None;
            }
            state.cleanup_starting = false;
            state.cleanup_in_progress = true;
            state.active_cleanup_action = recommendation.action;
            state.last_cleanup_action = recommendation.action;
            state.last_cleanup_time_ms = start_time_ms;
            return Some(CleanupTicket {
                recommendation,
                generation,
                rss_before,
            });
        }
    }

    fn maybe_queue_cleanup(
        &self,
        state: &mut MonitorState,
        recommendation: &CleanupRecommendation,
    ) -> Option<String> {
        let requested_rank = cleanup_action_rank(recommendation.action);
        let active_rank = cleanup_action_rank(state.active_cleanup_action);
        let queued_rank = state
            .queued_cleanup_recommendation
            .as_ref()
            .map(|queued| cleanup_action_rank(queued.action))
            .unwrap_or(0);
        if requested_rank > active_rank && requested_rank > queued_rank {
            state.queued_cleanup_recommendation = Some(recommendation.clone());
            Some(format!(
                "Queued escalated cleanup \"{}\" while \"{}\" is in progress",
                recommendation.action.as_str(),
                state.active_cleanup_action.as_str()
            ))
        } else {
            Some("Cleanup already in progress, skipping".to_owned())
        }
    }

    fn dump_diagnostics(&self, pressure: PressureLevel) {
        let Some(dumper) = self.hooks.diagnostics_dumper.as_ref() else {
            return;
        };
        let trigger = match pressure {
            PressureLevel::Hard => MemoryDumpTrigger::Hard,
            PressureLevel::Critical => MemoryDumpTrigger::Critical,
            PressureLevel::Normal | PressureLevel::Soft => return,
        };
        let samples = self.runtime_samples();
        let input = match safe_call(|| (self.hooks.diagnostics_input)(samples.clone())) {
            Ok(mut input) => {
                input.recent_samples = samples;
                input
            }
            Err(payload) => {
                self.log(
                    MemoryPressureLogLevel::Error,
                    format!(
                        "Memory diagnostics input failed: {}",
                        panic_message(payload)
                    ),
                );
                return;
            }
        };
        let dump = match safe_call(|| dumper.dump(trigger, input)) {
            Ok(dump) => dump,
            Err(payload) => {
                self.log(
                    MemoryPressureLogLevel::Error,
                    format!("Memory diagnostics dump failed: {}", panic_message(payload)),
                );
                return;
            }
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = dump.await;
            });
        }
    }

    async fn run_cleanup_chain(&self, first: CleanupTicket) {
        let mut cancellation_guard = CleanupCancellationGuard {
            monitor: self,
            generation: first.generation,
            armed: true,
        };
        let mut ticket = Some(first);
        while let Some(current) = ticket {
            self.run_one_cleanup(current.clone()).await;
            ticket = self.finish_and_take_queued(current.generation);
        }
        cancellation_guard.disarm();
    }

    async fn run_one_cleanup(&self, ticket: CleanupTicket) {
        for step in &ticket.recommendation.steps {
            if !self.generation_is_current(ticket.generation) {
                return;
            }
            let result = match safe_call(|| (self.hooks.cleanup_step)(*step)) {
                Ok(future) => match AssertUnwindSafe(future).catch_unwind().await {
                    Ok(result) => result,
                    Err(payload) => Err(panic_message(payload)),
                },
                Err(payload) => Err(panic_message(payload)),
            };
            if let Err(error) = result {
                if *step == CleanupStep::CompactHistory {
                    self.log(
                        MemoryPressureLogLevel::Error,
                        format!("[COMPACT_HISTORY] failed: {error}"),
                    );
                } else {
                    self.record_cleanup_failure(&ticket.recommendation, error, ticket.generation);
                    return;
                }
            }
            // Equivalent to the source's Promise.resolve() boundary between
            // cleanup steps, allowing concurrent checks to queue escalation.
            tokio::task::yield_now().await;
        }

        if !self.generation_is_current(ticket.generation) {
            return;
        }
        lock(&self.state).consecutive_cleanup_failures = 0;

        // The TS implementation uses setImmediate before measuring the result.
        // Yielding here gives other scheduled checks a chance to queue work.
        tokio::task::yield_now().await;
        if !self.generation_is_current(ticket.generation) {
            return;
        }
        match self.read_measurement() {
            Ok(after) => self.log_cleanup_result(
                ticket.rss_before,
                after.memory_usage.rss,
                &ticket.recommendation,
                ticket.generation,
            ),
            Err(error) => self.log(
                MemoryPressureLogLevel::Error,
                format!("Cleanup measurement failed: {error}"),
            ),
        }
    }

    fn finish_and_take_queued(&self, generation: u64) -> Option<CleanupTicket> {
        let queued = {
            let mut state = lock(&self.state);
            if state.cleanup_generation != generation {
                return None;
            }
            state.cleanup_in_progress = false;
            state.active_cleanup_action = CleanupAction::None;
            let queued = state.queued_cleanup_recommendation.take();
            if let Some(recommendation) = queued.as_ref() {
                state.cleanup_starting = true;
                state.active_cleanup_action = recommendation.action;
            }
            queued
        }?;

        let now_ms = safe_call(|| (self.hooks.clock_ms)()).unwrap_or_default();
        self.start_reserved_cleanup(queued, generation, now_ms)
    }

    fn log_cleanup_result(
        &self,
        rss_before: u64,
        rss_after: u64,
        recommendation: &CleanupRecommendation,
        generation: u64,
    ) {
        let freed = rss_before as i128 - rss_after as i128;
        let freed_ratio = if rss_before > 0 {
            freed as f64 / rss_before as f64
        } else {
            0.0
        };
        self.log(
            MemoryPressureLogLevel::Info,
            format!(
                "Cleanup \"{}\" completed; RSS delta {freed} bytes ({:.1}%)",
                recommendation.action.as_str(),
                freed_ratio * 100.0
            ),
        );

        let event = {
            let mut state = lock(&self.state);
            if state.cleanup_generation != generation {
                return;
            }
            if freed_ratio < 0.01 {
                state.consecutive_ineffective_cleanups += 1;
                if recommendation.action == CleanupAction::Aggressive {
                    state.consecutive_ineffective_aggressive_cleanups += 1;
                }
                let count = state.consecutive_ineffective_cleanups;
                should_emit_repeated_diagnostic(count).then(|| {
                    MemoryPressureEvent::CleanupIneffective(MemoryCleanupIneffectiveEvent {
                        rss: rss_after,
                        freed_bytes: freed,
                        freed_ratio,
                        consecutive_ineffective_cleanups: count,
                        recommendation: recommendation.clone(),
                    })
                })
            } else {
                state.consecutive_ineffective_cleanups = 0;
                if recommendation.action == CleanupAction::Aggressive {
                    state.consecutive_ineffective_aggressive_cleanups = 0;
                }
                None
            }
        };
        if let Some(event) = event {
            self.log(
                MemoryPressureLogLevel::Warn,
                format!(
                    "Cleanup \"{}\" has been ineffective {} times consecutively",
                    recommendation.action.as_str(),
                    match &event {
                        MemoryPressureEvent::CleanupIneffective(value) => {
                            value.consecutive_ineffective_cleanups
                        }
                        MemoryPressureEvent::CleanupFailed(_) => 0,
                    }
                ),
            );
            self.emit_safely(event);
        }
    }

    fn record_cleanup_failure(
        &self,
        recommendation: &CleanupRecommendation,
        error: String,
        generation: u64,
    ) {
        let rss = match self.read_measurement() {
            Ok(measurement) => measurement.memory_usage.rss,
            Err(rss_error) => {
                self.log(
                    MemoryPressureLogLevel::Error,
                    format!("Failed to read RSS after cleanup failure: {rss_error}"),
                );
                0
            }
        };
        let failure_count = {
            let mut state = lock(&self.state);
            if state.cleanup_generation != generation {
                return;
            }
            state.consecutive_cleanup_failures += 1;
            state.consecutive_cleanup_failures
        };
        self.log(
            MemoryPressureLogLevel::Error,
            format!(
                "Cleanup \"{}\" failed: {error}; consecutive failures: {failure_count}",
                recommendation.action.as_str()
            ),
        );
        if should_emit_repeated_diagnostic(failure_count) {
            self.emit_safely(MemoryPressureEvent::CleanupFailed(
                MemoryCleanupFailureEvent {
                    rss,
                    consecutive_failures: failure_count,
                    recommendation: recommendation.clone(),
                    error,
                },
            ));
        }
    }

    fn emit_safely(&self, event: MemoryPressureEvent) {
        let event_name = event.event_name();
        if safe_call(|| (self.hooks.event)(event)).is_err() {
            self.log(
                MemoryPressureLogLevel::Error,
                format!("{event_name} handler threw"),
            );
        }
    }

    fn read_measurement(&self) -> Result<MemoryPressureMeasurement, String> {
        match safe_call(|| (self.hooks.measure)()) {
            Ok(Ok(measurement)) => Ok(measurement),
            Ok(Err(error)) => Err(error),
            Err(payload) => Err(panic_message(payload)),
        }
    }

    fn generation_is_current(&self, generation: u64) -> bool {
        lock(&self.state).cleanup_generation == generation
    }

    fn log(&self, level: MemoryPressureLogLevel, message: String) {
        let log = MemoryPressureLog { level, message };
        // Logging is observational: logger panics must not interrupt cleanup.
        let _ = safe_call(|| (self.hooks.log)(log));
    }
}

/// If a caller drops `perform_check` while a provider future is suspended,
/// release the active slot so future checks are not permanently blocked.
/// `reset_for_new_session` advances the generation and performs its own reset,
/// so an old cancelled tail cannot mutate new-session state.
struct CleanupCancellationGuard<'a> {
    monitor: &'a MemoryPressureMonitor,
    generation: u64,
    armed: bool,
}

impl CleanupCancellationGuard<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CleanupCancellationGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = lock(&self.monitor.state);
        if state.cleanup_generation == self.generation {
            state.cleanup_in_progress = false;
            state.cleanup_starting = false;
            state.active_cleanup_action = CleanupAction::None;
            state.queued_cleanup_recommendation = None;
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn safe_call<T>(callback: impl FnOnce() -> T) -> Result<T, Box<dyn Any + Send>> {
    catch_unwind(AssertUnwindSafe(callback))
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else {
        "callback panicked".to_owned()
    }
}

fn system_time_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

    fn measurement(rss: u64, heap_used: u64, heap_limit: u64) -> MemoryPressureMeasurement {
        MemoryPressureMeasurement {
            memory_usage: MemoryUsage {
                rss,
                heap_used,
                heap_total: heap_limit,
                external: 0,
            },
            heap_size_limit_bytes: heap_limit,
        }
    }

    fn monitor_with(
        config: MemoryPressureConfig,
        memory: Arc<AtomicU64>,
        now: Arc<AtomicI64>,
        calls: Arc<Mutex<Vec<CleanupStep>>>,
    ) -> MemoryPressureMonitor {
        let mut hooks = MemoryPressureMonitorHooks::new(
            {
                let memory = Arc::clone(&memory);
                move || Ok(measurement(memory.load(Ordering::SeqCst), 0, 0))
            },
            {
                let calls = Arc::clone(&calls);
                move |step| -> CleanupStepFuture {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        calls.lock().unwrap().push(step);
                        Ok(())
                    })
                }
            },
        );
        hooks.clock_ms = {
            let now = Arc::clone(&now);
            Arc::new(move || now.load(Ordering::SeqCst))
        };
        hooks.cpu_core_count = 1;
        MemoryPressureMonitor::new(config, 16_000, hooks).unwrap()
    }

    #[test]
    fn config_validation_preserves_source_boundaries_and_order() {
        let mut config = DEFAULT_PRESSURE_CONFIG;
        config.soft_pressure_ratio = 0.3;
        config.hard_pressure_ratio = 0.7;
        config.critical_ratio = 0.98;
        config.cleanup_cooldown_ms = 0.0;
        assert_eq!(validate_memory_pressure_config(&config), Ok(()));

        config.soft_pressure_ratio = config.hard_pressure_ratio;
        assert_eq!(
            validate_memory_pressure_config(&config),
            Err("softPressureRatio must be < hardPressureRatio")
        );
        config = DEFAULT_PRESSURE_CONFIG;
        config.cleanup_cooldown_ms = f64::NAN;
        assert_eq!(
            validate_memory_pressure_config(&config),
            Err("cleanupCooldownMs must be a non-negative number")
        );
    }

    #[test]
    fn pressure_uses_stronger_rss_or_heap_ratio_and_zero_limits_disable_metric() {
        let monitor = MemoryPressureMonitor::with_defaults(
            16_000,
            MemoryPressureMonitorHooks::new(
                || Ok(measurement(0, 0, 0)),
                |_| -> CleanupStepFuture { Box::pin(async { Ok(()) }) },
            ),
        );
        assert_eq!(
            monitor.get_pressure_level(measurement(8_000, 0, 0)),
            PressureLevel::Soft
        );
        assert_eq!(
            monitor.get_pressure_level(measurement(1, 600, 1_000)),
            PressureLevel::Soft
        );
        assert_eq!(
            monitor.get_pressure_level(measurement(1, 1_000, 0)),
            PressureLevel::Normal
        );
    }

    #[tokio::test]
    async fn soft_cleanup_uses_policy_step_and_samples_even_without_telemetry() {
        let memory = Arc::new(AtomicU64::new(9_000));
        let now = Arc::new(AtomicI64::new(10_000));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut config = DEFAULT_PRESSURE_CONFIG;
        config.cleanup_cooldown_ms = 0.0;
        let monitor = monitor_with(config, memory, now, Arc::clone(&calls));

        monitor.perform_check().await;

        assert_eq!(*calls.lock().unwrap(), vec![CleanupStep::EvictStaleCache]);
        assert_eq!(monitor.runtime_samples().len(), 1);
    }

    #[tokio::test]
    async fn first_hard_pressure_dump_includes_the_rss_sample_that_triggered_it() {
        let dir = std::env::temp_dir().join(format!(
            "canopy-first-memory-dump-test-{}",
            uuid::Uuid::new_v4()
        ));
        let dumper = Arc::new(MemoryDiagnosticsDumper::new(
            dir.clone(),
            "first-sample-session",
            "test-version",
            |_, _| async { std::future::pending::<Result<serde_json::Value, String>>().await },
        ));
        let mut hooks = MemoryPressureMonitorHooks::new(
            || Ok(measurement(11_000, 0, 0)),
            |_| -> CleanupStepFuture { Box::pin(async { Ok(()) }) },
        );
        // Make the ring's first record share its construction tick. It must
        // still retain the fresh RSS value for the synchronous phase-one dump.
        hooks.clock_ms = Arc::new(|| 10_000);
        hooks.diagnostics_dumper = Some(dumper);
        let mut config = DEFAULT_PRESSURE_CONFIG;
        config.cleanup_cooldown_ms = 0.0;
        let monitor = MemoryPressureMonitor::new(config, 16_000, hooks).unwrap();

        monitor.perform_check().await;

        let mut entries = std::fs::read_dir(dir.join("diagnostics"))
            .expect("first pressure check wrote its phase-one diagnostic")
            .map(|entry| entry.expect("diagnostic directory entry").path());
        let path = entries.next().expect("one diagnostic dump");
        assert!(entries.next().is_none());
        let dump: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).expect("phase-one diagnostic exists"))
                .expect("phase-one diagnostic is valid JSON");
        assert_eq!(dump["collectionComplete"], false);
        assert_eq!(dump["memoryUsage"]["rss"], 11_000);
        assert_eq!(dump["recentSamples"][0]["rss"], 11_000);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn diagnostics_cadence_counts_ineffective_successes_not_failures() {
        let memory = Arc::new(AtomicU64::new(9_000));
        let now = Arc::new(AtomicI64::new(10_000));
        let mut config = DEFAULT_PRESSURE_CONFIG;
        config.cleanup_cooldown_ms = 0.0;
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut hooks = MemoryPressureMonitorHooks::new(
            {
                let memory = Arc::clone(&memory);
                move || Ok(measurement(memory.load(Ordering::SeqCst), 0, 0))
            },
            |_| -> CleanupStepFuture { Box::pin(async { Ok(()) }) },
        );
        hooks.clock_ms = {
            let now = Arc::clone(&now);
            Arc::new(move || now.load(Ordering::SeqCst))
        };
        hooks.event = {
            let events = Arc::clone(&events);
            Arc::new(move |event| events.lock().unwrap().push(event))
        };
        let observed = MemoryPressureMonitor::new(config, 16_000, hooks).unwrap();
        for _ in 0..3 {
            observed.perform_check().await;
            now.fetch_add(1, Ordering::SeqCst);
        }
        assert_eq!(observed.consecutive_failures(), 0);
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            MemoryPressureEvent::CleanupIneffective(event)
                if event.consecutive_ineffective_cleanups == 3 && event.freed_ratio == 0.0
        ));
    }

    #[tokio::test]
    async fn repeated_cleanup_errors_increment_failures_and_emit_at_three() {
        let memory = Arc::new(AtomicU64::new(9_000));
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut config = DEFAULT_PRESSURE_CONFIG;
        config.cleanup_cooldown_ms = 0.0;
        let mut hooks = MemoryPressureMonitorHooks::new(
            {
                let memory = Arc::clone(&memory);
                move || Ok(measurement(memory.load(Ordering::SeqCst), 0, 0))
            },
            |_| -> CleanupStepFuture { Box::pin(async { Err("cache failure".to_owned()) }) },
        );
        hooks.event = {
            let events = Arc::clone(&events);
            Arc::new(move |event| events.lock().unwrap().push(event))
        };
        let monitor = MemoryPressureMonitor::new(config, 16_000, hooks).unwrap();

        for _ in 0..3 {
            monitor.perform_check().await;
        }

        assert_eq!(monitor.consecutive_failures(), 3);
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            MemoryPressureEvent::CleanupFailed(event)
                if event.consecutive_failures == 3 && event.error == "cache failure"
        ));
    }

    #[tokio::test]
    async fn compaction_errors_are_best_effort_and_keep_ordered_later_steps() {
        let memory = Arc::new(AtomicU64::new(11_000));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut config = DEFAULT_PRESSURE_CONFIG;
        config.cleanup_cooldown_ms = 0.0;
        config.enable_explicit_gc = false;
        let mut hooks = MemoryPressureMonitorHooks::new(
            {
                let memory = Arc::clone(&memory);
                move || Ok(measurement(memory.load(Ordering::SeqCst), 0, 0))
            },
            {
                let calls = Arc::clone(&calls);
                move |step| -> CleanupStepFuture {
                    calls.lock().unwrap().push(step);
                    Box::pin(async move {
                        if step == CleanupStep::CompactHistory {
                            Err("chat unavailable".to_owned())
                        } else {
                            Ok(())
                        }
                    })
                }
            },
        );
        hooks.clock_ms = Arc::new(|| 10_000);
        let monitor = MemoryPressureMonitor::new(config, 16_000, hooks).unwrap();

        monitor.perform_check().await;

        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                CleanupStep::EvictColdCache,
                CleanupStep::CompactHistory,
                CleanupStep::ClearFileCache,
            ]
        );
        assert_eq!(monitor.consecutive_failures(), 0);
    }

    #[tokio::test]
    async fn stronger_pressure_queues_until_active_cleanup_finishes() {
        let memory = Arc::new(AtomicU64::new(9_000));
        let now = Arc::new(AtomicI64::new(10_000));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut config = DEFAULT_PRESSURE_CONFIG;
        config.cleanup_cooldown_ms = 60_000.0;
        let entered = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new(tokio::sync::Notify::new());
        let entered_for_cleanup = Arc::clone(&entered);
        let mut hooks = MemoryPressureMonitorHooks::new(
            {
                let memory = Arc::clone(&memory);
                move || Ok(measurement(memory.load(Ordering::SeqCst), 0, 0))
            },
            {
                let calls = Arc::clone(&calls);
                let gate = Arc::clone(&gate);
                move |step| -> CleanupStepFuture {
                    let calls = Arc::clone(&calls);
                    let entered = Arc::clone(&entered_for_cleanup);
                    let gate = Arc::clone(&gate);
                    Box::pin(async move {
                        calls.lock().unwrap().push(step);
                        if step == CleanupStep::EvictStaleCache {
                            entered.notify_one();
                            gate.notified().await;
                        }
                        Ok(())
                    })
                }
            },
        );
        hooks.clock_ms = {
            let now = Arc::clone(&now);
            Arc::new(move || now.load(Ordering::SeqCst))
        };
        let monitor = Arc::new(MemoryPressureMonitor::new(config, 16_000, hooks).unwrap());
        let first = {
            let monitor = Arc::clone(&monitor);
            tokio::spawn(async move { monitor.perform_check().await })
        };
        entered.notified().await;
        memory.store(11_000, Ordering::SeqCst);
        monitor.perform_check().await;
        memory.store(14_000, Ordering::SeqCst);
        monitor.perform_check().await;
        assert!(!calls.lock().unwrap().contains(&CleanupStep::TriggerGc));
        gate.notify_one();
        first.await.unwrap();
        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                CleanupStep::EvictStaleCache,
                CleanupStep::EvictColdCache,
                CleanupStep::CompactHistory,
                CleanupStep::ClearFileCache,
                CleanupStep::TriggerGc,
            ]
        );
    }

    #[tokio::test]
    async fn reset_generation_stops_remaining_steps_of_old_cleanup() {
        let memory = Arc::new(AtomicU64::new(14_000));
        let now = Arc::new(AtomicI64::new(10_000));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Notify::new());
        let entered = Arc::new(tokio::sync::Notify::new());
        let mut config = DEFAULT_PRESSURE_CONFIG;
        config.cleanup_cooldown_ms = 0.0;
        let entered_for_cleanup = Arc::clone(&entered);
        let mut hooks = MemoryPressureMonitorHooks::new(
            {
                let memory = Arc::clone(&memory);
                move || Ok(measurement(memory.load(Ordering::SeqCst), 0, 0))
            },
            {
                let calls = Arc::clone(&calls);
                let gate = Arc::clone(&gate);
                move |step| -> CleanupStepFuture {
                    let calls = Arc::clone(&calls);
                    let entered = Arc::clone(&entered_for_cleanup);
                    let gate = Arc::clone(&gate);
                    Box::pin(async move {
                        calls.lock().unwrap().push(step);
                        if step == CleanupStep::EvictColdCache {
                            entered.notify_one();
                            gate.notified().await;
                        }
                        Ok(())
                    })
                }
            },
        );
        hooks.clock_ms = {
            let now = Arc::clone(&now);
            Arc::new(move || now.load(Ordering::SeqCst))
        };
        let monitor = Arc::new(MemoryPressureMonitor::new(config, 16_000, hooks).unwrap());
        let task = {
            let monitor = Arc::clone(&monitor);
            tokio::spawn(async move { monitor.perform_check().await })
        };
        entered.notified().await;
        monitor.reset_for_new_session();
        gate.notify_one();
        task.await.unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![CleanupStep::EvictColdCache]);
        assert_eq!(monitor.consecutive_failures(), 0);
        assert!(monitor.runtime_samples().is_empty());
    }

    #[test]
    fn schedule_guard_coalesces_until_consumed() {
        let monitor = MemoryPressureMonitor::with_defaults(
            16_000,
            MemoryPressureMonitorHooks::new(
                || Ok(measurement(0, 0, 0)),
                |_| -> CleanupStepFuture { Box::pin(async { Ok(()) }) },
            ),
        );
        assert!(monitor.schedule_check());
        assert!(!monitor.schedule_check());
    }
}
