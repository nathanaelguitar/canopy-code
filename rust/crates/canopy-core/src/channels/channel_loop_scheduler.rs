//! Tokio scheduler for persistent channel loops.
//!
//! Port of `packages/channels/base/src/ChannelLoopScheduler.ts`. The scheduler
//! polls the injected cron resolver, coalesces overlapping ticks, and allows no
//! more than five loop prompts to be in flight at once.

use super::channel_loop_store::{
    ChannelLoop, ChannelLoopPatch, ChannelLoopStatus, ChannelLoopStore, PatchField,
};
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::future::BoxFuture;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};

const MAX_RESULT_PREVIEW_LENGTH: usize = 500;
const MAX_ERROR_LENGTH: usize = 1_000;
const MAX_CONCURRENT_LOOP_FIRES: usize = 5;

pub type NextFireTime =
    Arc<dyn Fn(&str, DateTime<Utc>) -> Result<DateTime<Utc>, String> + Send + Sync>;
pub type SchedulerClock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;
pub type ShouldContinue =
    Arc<dyn Fn() -> BoxFuture<'static, io::Result<bool>> + Send + Sync + 'static>;

/// Options passed to a channel adapter for one scheduled loop.
#[derive(Clone)]
pub struct ChannelLoopRunnerOptions {
    /// The channel's prompt timeout budget, in milliseconds.
    pub timeout_ms: u64,
    /// Rechecks scheduler generation and whether the persistent job is enabled.
    pub should_continue: ShouldContinue,
}

impl ChannelLoopRunnerOptions {
    pub async fn should_continue(&self) -> io::Result<bool> {
        (self.should_continue)().await
    }
}

/// A channel adapter capable of executing a loop prompt.
pub trait ChannelLoopRunner: Send + Sync {
    fn run_loop_prompt<'a>(
        &'a self,
        job: ChannelLoop,
        options: ChannelLoopRunnerOptions,
    ) -> BoxFuture<'a, Result<Option<String>, ChannelLoopRunError>>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelLoopSkipReason {
    CancelCommand,
    Clear,
    Dropped,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ChannelLoopRunError {
    #[error("{message}")]
    Failed { message: String },
    #[error("{message}")]
    Skipped {
        message: String,
        reason: ChannelLoopSkipReason,
    },
}

impl ChannelLoopRunError {
    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
        }
    }

    pub fn skipped(message: impl Into<String>, reason: ChannelLoopSkipReason) -> Self {
        Self::Skipped {
            message: message.into(),
            reason,
        }
    }

    fn is_skipped(&self) -> bool {
        matches!(self, Self::Skipped { .. })
    }

    fn message(&self) -> &str {
        match self {
            Self::Failed { message } | Self::Skipped { message, .. } => message,
        }
    }
}

/// Configuration for a scheduler instance.
pub struct ChannelLoopSchedulerOptions {
    pub store: Arc<ChannelLoopStore>,
    pub channels: HashMap<String, Arc<dyn ChannelLoopRunner>>,
    pub next_fire_time: NextFireTime,
    pub now: SchedulerClock,
    pub max_consecutive_failures: f64,
    pub interval_ms: u64,
    pub loop_timeout_ms: u64,
}

impl ChannelLoopSchedulerOptions {
    pub fn new<N>(
        store: Arc<ChannelLoopStore>,
        channels: HashMap<String, Arc<dyn ChannelLoopRunner>>,
        next_fire_time: N,
    ) -> Self
    where
        N: Fn(&str, DateTime<Utc>) -> Result<DateTime<Utc>, String> + Send + Sync + 'static,
    {
        Self {
            store,
            channels,
            next_fire_time: Arc::new(next_fire_time),
            now: Arc::new(Utc::now),
            max_consecutive_failures: 5.0,
            interval_ms: 60_000,
            loop_timeout_ms: 5 * 60_000,
        }
    }

    pub fn with_clock<N>(mut self, now: N) -> Self
    where
        N: Fn() -> DateTime<Utc> + Send + Sync + 'static,
    {
        self.now = Arc::new(now);
        self
    }

    pub fn with_interval_ms(mut self, interval_ms: u64) -> Self {
        self.interval_ms = interval_ms;
        self
    }

    pub fn with_loop_timeout_ms(mut self, loop_timeout_ms: u64) -> Self {
        self.loop_timeout_ms = loop_timeout_ms;
        self
    }

    pub fn with_max_consecutive_failures(mut self, max_consecutive_failures: f64) -> Self {
        self.max_consecutive_failures = max_consecutive_failures;
        self
    }
}

/// Polls persistent loop definitions and dispatches due prompts to channel
/// runners. `start` and `stop` control only the periodic timer; already running
/// adapter futures are allowed to settle. Generation and persisted running
/// markers prevent the same stale writes as the TypeScript scheduler.
pub struct ChannelLoopScheduler {
    inner: Arc<SchedulerInner>,
}

struct SchedulerInner {
    store: Arc<ChannelLoopStore>,
    channels: HashMap<String, Arc<dyn ChannelLoopRunner>>,
    next_fire_time: NextFireTime,
    now: SchedulerClock,
    max_consecutive_failures: f64,
    interval_ms: u64,
    loop_timeout_ms: u64,
    generation: AtomicU64,
    recovery_epoch: AtomicU64,
    next_token: AtomicU64,
    in_flight: Mutex<HashMap<String, u64>>,
    running_tick: Mutex<Option<RunningTick>>,
    timer: Mutex<Option<TimerTask>>,
}

struct RunningTick {
    id: u64,
    completion: watch::Sender<Option<SharedTickResult>>,
}

struct InFlightGuard {
    inner: Arc<SchedulerInner>,
    job_id: String,
    token: u64,
}

struct TimerTask {
    stop: watch::Sender<bool>,
    _join: JoinHandle<()>,
}

#[derive(Clone, Debug)]
struct SharedTickError {
    kind: io::ErrorKind,
    message: String,
}

type SharedTickResult = Result<(), SharedTickError>;

impl SharedTickError {
    fn from_io(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            message: error.to_string(),
        }
    }

    fn into_io(self) -> io::Error {
        io::Error::new(self.kind, self.message)
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let mut in_flight = lock_unpoisoned(&self.inner.in_flight);
        if in_flight.get(&self.job_id) == Some(&self.token) {
            in_flight.remove(&self.job_id);
        }
    }
}

impl ChannelLoopScheduler {
    pub fn new(options: ChannelLoopSchedulerOptions) -> Self {
        Self {
            inner: Arc::new(SchedulerInner {
                store: options.store,
                channels: options.channels,
                next_fire_time: options.next_fire_time,
                now: options.now,
                max_consecutive_failures: options.max_consecutive_failures,
                interval_ms: options.interval_ms,
                loop_timeout_ms: options.loop_timeout_ms,
                generation: AtomicU64::new(0),
                recovery_epoch: AtomicU64::new(0),
                next_token: AtomicU64::new(0),
                in_flight: Mutex::new(HashMap::new()),
                running_tick: Mutex::new(None),
                timer: Mutex::new(None),
            }),
        }
    }

    /// Starts startup reconciliation, an immediate tick, and a Tokio interval.
    /// Calling this without an active Tokio runtime returns `NotConnected`.
    pub fn start(&self) -> io::Result<()> {
        if self.inner.interval_ms == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "channel loop scheduler interval must be greater than zero",
            ));
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|error| io::Error::new(io::ErrorKind::NotConnected, error))?;
        let mut timer = lock_unpoisoned(&self.inner.timer);
        if timer.is_some() {
            return Ok(());
        }

        let generation = self.inner.generation.load(Ordering::Acquire);
        let (stop, receiver) = watch::channel(false);
        let inner = Arc::clone(&self.inner);
        let join = runtime.spawn(async move {
            inner.run_timer(generation, receiver).await;
        });
        *timer = Some(TimerTask { stop, _join: join });
        Ok(())
    }

    /// Stops future timer ticks and invalidates any currently executing run.
    /// Rust cannot cancel a channel adapter future safely, so it may finish.
    /// Error handling checks the generation; successful runs commit only while
    /// their persisted `runningSince` marker still matches.
    pub fn stop(&self) {
        self.inner.generation.fetch_add(1, Ordering::AcqRel);
        lock_unpoisoned(&self.inner.in_flight).clear();
        lock_unpoisoned(&self.inner.running_tick).take();
        if let Some(timer) = lock_unpoisoned(&self.inner.timer).take() {
            timer.stop.send_replace(true);
        }
    }

    /// Marks an ACP/bridge replacement. Errors from prompts that were already
    /// running at this point are recorded without incrementing failure counts.
    pub fn mark_bridge_recovery(&self) {
        self.inner.recovery_epoch.fetch_add(1, Ordering::AcqRel);
    }

    /// Runs a coalesced scheduling tick. This returns after due jobs have been
    /// dispatched, not after their channel prompts complete.
    pub async fn tick(&self) -> io::Result<()> {
        self.inner.tick().await
    }
}

impl Drop for ChannelLoopScheduler {
    fn drop(&mut self) {
        self.stop();
    }
}

impl SchedulerInner {
    async fn tick(self: &Arc<Self>) -> io::Result<()> {
        let generation = self.generation.load(Ordering::Acquire);
        let (tick_id, sender, mut receiver) = {
            let mut running = lock_unpoisoned(&self.running_tick);
            if let Some(current) = running.as_ref() {
                let receiver = current.completion.subscribe();
                (None, current.completion.clone(), receiver)
            } else {
                let tick_id = self.next_token.fetch_add(1, Ordering::Relaxed) + 1;
                let (sender, receiver) = watch::channel(None);
                *running = Some(RunningTick {
                    id: tick_id,
                    completion: sender.clone(),
                });
                (Some(tick_id), sender, receiver)
            }
        };

        if let Some(tick_id) = tick_id {
            let inner = Arc::clone(self);
            tokio::spawn(async move {
                let result = inner.run_tick(generation).await;
                let shared_result = result
                    .as_ref()
                    .map(|_| ())
                    .map_err(SharedTickError::from_io);
                {
                    let mut running = lock_unpoisoned(&inner.running_tick);
                    if running.as_ref().is_some_and(|tick| tick.id == tick_id) {
                        *running = None;
                    }
                }
                sender.send_replace(Some(shared_result));
            });
        }

        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result.map_err(SharedTickError::into_io);
            }
            if receiver.changed().await.is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "channel loop tick task ended before reporting its result",
                ));
            }
        }
    }

    async fn run_tick(self: &Arc<Self>, generation: u64) -> io::Result<()> {
        let now = (self.now)();
        let jobs = self.store.list().await?;
        if self.generation.load(Ordering::Acquire) != generation {
            return Ok(());
        }

        let mut in_flight = lock_unpoisoned(&self.in_flight);
        let slots = MAX_CONCURRENT_LOOP_FIRES.saturating_sub(in_flight.len());
        if slots == 0 {
            return Ok(());
        }

        let mut launches = Vec::with_capacity(slots);
        for job in jobs {
            if launches.len() >= slots {
                break;
            }
            if !job.enabled
                || !self.channels.contains_key(&job.channel_name)
                || in_flight.contains_key(&job.id)
                || !self.is_due(&job, now)
            {
                continue;
            }
            let token = self.next_token.fetch_add(1, Ordering::Relaxed) + 1;
            in_flight.insert(job.id.clone(), token);
            launches.push((job, token));
        }
        drop(in_flight);

        for (job, token) in launches {
            let inner = Arc::clone(self);
            tokio::spawn(async move {
                inner.fire_once(job, now, generation, token).await;
            });
        }
        Ok(())
    }

    fn is_due(&self, job: &ChannelLoop, now: DateTime<Utc>) -> bool {
        let anchor = match last_anchor(job) {
            Ok(anchor) => anchor,
            Err(error) => {
                eprintln!("[scheduler] invalid anchor for loop {}: {error}", job.id);
                return false;
            }
        };
        match (self.next_fire_time)(&job.cron, anchor) {
            Ok(next) => next <= now,
            Err(error) => {
                eprintln!("[scheduler] invalid cron for loop {}: {error}", job.id);
                false
            }
        }
    }

    async fn fire_once(
        self: &Arc<Self>,
        job: ChannelLoop,
        now: DateTime<Utc>,
        generation: u64,
        token: u64,
    ) {
        let _in_flight_guard = InFlightGuard {
            inner: Arc::clone(self),
            job_id: job.id.clone(),
            token,
        };
        if let Err(error) = self.fire(job.clone(), now, generation).await {
            eprintln!("[scheduler] unhandled error for loop {}: {error}", job.id);
        }
    }

    async fn fire(
        self: &Arc<Self>,
        job: ChannelLoop,
        now: DateTime<Utc>,
        generation: u64,
    ) -> io::Result<()> {
        let Some(channel) = self.channels.get(&job.channel_name).cloned() else {
            return Ok(());
        };
        let Some(latest_job) = self.find_job(&job.id).await?.filter(|job| job.enabled) else {
            return Ok(());
        };
        if self.generation.load(Ordering::Acquire) != generation {
            return Ok(());
        }

        let running_since = format_time(now);
        let mut recovery_epoch = self.recovery_epoch.load(Ordering::Acquire);
        if let Err(error) = self
            .store
            .update(
                &latest_job.id,
                ChannelLoopPatch {
                    running_since: PatchField::Set(Some(running_since.clone())),
                    last_fired_at: PatchField::Set(Some(running_since.clone())),
                    ..ChannelLoopPatch::default()
                },
            )
            .await
        {
            self.handle_run_error(
                &latest_job.id,
                now,
                generation,
                recovery_epoch,
                Some(&running_since),
                &error.to_string(),
            )
            .await;
            return Ok(());
        }
        if self.generation.load(Ordering::Acquire) != generation {
            self.clear_running_since(&latest_job.id, Some(&running_since))
                .await;
            return Ok(());
        }

        recovery_epoch = self.recovery_epoch.load(Ordering::Acquire);
        let should_continue = self.make_should_continue(latest_job.id.clone(), generation);
        let run_result = channel
            .run_loop_prompt(
                latest_job.clone(),
                ChannelLoopRunnerOptions {
                    timeout_ms: self.loop_timeout_ms,
                    should_continue,
                },
            )
            .await;

        match run_result {
            Err(error) if error.is_skipped() => {
                self.record_skipped(&latest_job.id, &running_since).await;
                Ok(())
            }
            Err(error) => {
                self.handle_run_error(
                    &latest_job.id,
                    now,
                    generation,
                    recovery_epoch,
                    Some(&running_since),
                    error.message(),
                )
                .await;
                Ok(())
            }
            Ok(result_preview) => {
                let Some(current_job) = self.find_job(&latest_job.id).await? else {
                    return Ok(());
                };
                if current_job.running_since.as_deref() != Some(&running_since) {
                    return Ok(());
                }

                let mut patch = ChannelLoopPatch {
                    last_fired_at: PatchField::Set(Some(running_since.clone())),
                    last_finished_at: PatchField::Set(Some(format_time((self.now)()))),
                    last_result_preview: PatchField::Set(
                        result_preview.as_deref().map(truncate_result_preview),
                    ),
                    last_status: PatchField::Set(Some(ChannelLoopStatus::Ok)),
                    last_error: PatchField::Set(None),
                    consecutive_failures: PatchField::Set(0.0),
                    running_since: PatchField::Set(None),
                    run_count: PatchField::Set(current_job.run_count + 1.0),
                    ..ChannelLoopPatch::default()
                };
                if !current_job.recurring {
                    patch.enabled = PatchField::Set(false);
                }
                if let Err(error) = self.store.update(&latest_job.id, patch).await {
                    eprintln!(
                        "[scheduler] loop {} succeeded but status persist failed: {error}",
                        latest_job.id
                    );
                    self.clear_running_since(&latest_job.id, Some(&running_since))
                        .await;
                }
                Ok(())
            }
        }
    }

    async fn handle_run_error(
        &self,
        job_id: &str,
        now: DateTime<Utc>,
        generation: u64,
        recovery_epoch: u64,
        expected_running_since: Option<&str>,
        message: &str,
    ) {
        let current_job = match self.find_job(job_id).await {
            Ok(job) => job,
            Err(find_error) => {
                eprintln!("[scheduler] findJob failed in catch for loop {job_id}: {find_error}");
                self.clear_running_since(job_id, expected_running_since)
                    .await;
                return;
            }
        };
        let Some(current_job) = current_job.filter(|job| job.enabled) else {
            self.clear_running_since(job_id, expected_running_since)
                .await;
            return;
        };
        if self.generation.load(Ordering::Acquire) != generation {
            self.clear_running_since(job_id, expected_running_since)
                .await;
            return;
        }
        if recovery_epoch != self.recovery_epoch.load(Ordering::Acquire) {
            let patch = ChannelLoopPatch {
                last_finished_at: PatchField::Set(Some(format_time((self.now)()))),
                last_status: PatchField::Set(Some(ChannelLoopStatus::Error)),
                last_error: PatchField::Set(Some(truncate_error(message))),
                running_since: PatchField::Set(None),
                ..ChannelLoopPatch::default()
            };
            if self.store.update(job_id, patch).await.is_err() {
                self.clear_running_since(job_id, expected_running_since)
                    .await;
            }
            return;
        }
        self.record_failure(&current_job, now, message).await;
    }

    fn make_should_continue(self: &Arc<Self>, job_id: String, generation: u64) -> ShouldContinue {
        let inner = Arc::clone(self);
        Arc::new(move || {
            let inner = Arc::clone(&inner);
            let job_id = job_id.clone();
            Box::pin(async move {
                if inner.generation.load(Ordering::Acquire) != generation {
                    return Ok(false);
                }
                Ok(inner
                    .find_job(&job_id)
                    .await?
                    .is_some_and(|job| job.enabled))
            })
        })
    }

    async fn find_job(&self, id: &str) -> io::Result<Option<ChannelLoop>> {
        Ok(self
            .store
            .list()
            .await?
            .into_iter()
            .find(|job| job.id == id))
    }

    async fn clear_running_since(&self, id: &str, expected: Option<&str>) {
        let result = async {
            if let Some(expected) = expected {
                let current_job = self.find_job(id).await?;
                if current_job
                    .as_ref()
                    .and_then(|job| job.running_since.as_deref())
                    != Some(expected)
                {
                    return Ok::<(), io::Error>(());
                }
            }
            self.store
                .update(
                    id,
                    ChannelLoopPatch {
                        running_since: PatchField::Set(None),
                        ..ChannelLoopPatch::default()
                    },
                )
                .await?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            eprintln!("[scheduler] failed to clear running state for loop {id}: {error}");
        }
    }

    async fn record_skipped(&self, id: &str, expected_running_since: &str) {
        let result = async {
            let Some(current_job) = self.find_job(id).await? else {
                return Ok::<(), io::Error>(());
            };
            if current_job.running_since.as_deref() != Some(expected_running_since) {
                return Ok(());
            }
            self.store
                .update(
                    id,
                    ChannelLoopPatch {
                        last_finished_at: PatchField::Set(Some(format_time((self.now)()))),
                        running_since: PatchField::Set(None),
                        ..ChannelLoopPatch::default()
                    },
                )
                .await?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            eprintln!("[scheduler] failed to record skipped loop {id}: {error}");
        }
    }

    async fn record_failure(&self, job: &ChannelLoop, now: DateTime<Utc>, message: &str) {
        let consecutive_failures = job.consecutive_failures + 1.0;
        let mut patch = ChannelLoopPatch {
            last_fired_at: PatchField::Set(Some(format_time(now))),
            last_finished_at: PatchField::Set(Some(format_time((self.now)()))),
            last_status: PatchField::Set(Some(ChannelLoopStatus::Error)),
            last_error: PatchField::Set(Some(truncate_error(message))),
            last_result_preview: PatchField::Set(None),
            consecutive_failures: PatchField::Set(consecutive_failures),
            running_since: PatchField::Set(None),
            run_count: PatchField::Set(job.run_count + 1.0),
            ..ChannelLoopPatch::default()
        };
        if !job.recurring {
            patch.enabled = PatchField::Set(false);
        }
        if consecutive_failures >= self.max_consecutive_failures {
            patch.enabled = PatchField::Set(false);
            eprintln!(
                "[scheduler] loop {} auto-disabled after {} consecutive failures",
                job.id, consecutive_failures
            );
        }
        if let Err(error) = self.store.update(&job.id, patch).await {
            eprintln!(
                "[scheduler] loop {} failure persist failed: {error}",
                job.id
            );
            self.clear_running_since(&job.id, None).await;
        }
    }

    async fn reconcile_startup_state(&self) -> io::Result<()> {
        let jobs = self.store.list().await?;
        let stale_running: Vec<_> = jobs
            .iter()
            .filter(|job| job.running_since.is_some())
            .map(|job| job.id.clone())
            .collect();
        for id in &stale_running {
            self.clear_running_since(id, None).await;
        }
        let enabled_count = jobs.iter().filter(|job| job.enabled).count();
        eprintln!(
            "[scheduler] started, tick interval {}ms, jobs {}, enabled {}, cleared stale running {}",
            self.interval_ms,
            jobs.len(),
            enabled_count,
            stale_running.len()
        );
        Ok(())
    }

    async fn run_timer(self: Arc<Self>, generation: u64, mut stop: watch::Receiver<bool>) {
        let mut interval = time::interval(Duration::from_millis(self.interval_ms));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // Tokio's first interval tick is immediate. Consume it so the initial
        // tick below is followed by a delay matching setInterval semantics.
        interval.tick().await;

        match self.reconcile_startup_state().await {
            Ok(()) => {
                if self.generation.load(Ordering::Acquire) == generation && !*stop.borrow() {
                    if let Err(error) = self.tick().await {
                        eprintln!("[scheduler] initial tick failed: {error}");
                    }
                }
            }
            Err(error) => eprintln!("[scheduler] initial tick failed: {error}"),
        }

        loop {
            tokio::select! {
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        break;
                    }
                }
                _ = interval.tick() => {
                    if self.generation.load(Ordering::Acquire) != generation {
                        break;
                    }
                    if let Err(error) = self.tick().await {
                        eprintln!("[scheduler] interval tick failed: {error}");
                    }
                }
            }
        }
    }
}

fn last_anchor(job: &ChannelLoop) -> io::Result<DateTime<Utc>> {
    let anchor = match (&job.last_fired_at, &job.last_finished_at) {
        (Some(last_fired), Some(last_finished)) => {
            match (
                DateTime::parse_from_rfc3339(last_fired),
                DateTime::parse_from_rfc3339(last_finished),
            ) {
                (Ok(fired), Ok(finished)) if fired > finished => last_fired,
                _ => last_finished,
            }
        }
        (Some(last_fired), None) => last_fired,
        (None, Some(last_finished)) => last_finished,
        (None, None) => &job.created_at,
    };
    parse_time(anchor)
}

fn parse_time(value: &str) -> io::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn format_time(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn truncate_result_preview(text: &str) -> String {
    truncate_utf16_safe(text, MAX_RESULT_PREVIEW_LENGTH)
}

fn truncate_error(message: &str) -> String {
    truncate_utf16_safe(message, MAX_ERROR_LENGTH)
}

/// Retains the source's UTF-16 length caps while ending on a valid Rust UTF-8
/// boundary if the next scalar would straddle the cap.
fn truncate_utf16_safe(text: &str, max_units: usize) -> String {
    let mut units = 0;
    let end = text
        .char_indices()
        .find_map(|(index, ch)| {
            let next_units = units + ch.len_utf16();
            if next_units > max_units {
                Some(index)
            } else {
                units = next_units;
                None
            }
        })
        .unwrap_or(text.len());
    text[..end].to_owned()
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::{
        ChannelLoopRunError, ChannelLoopRunner, ChannelLoopRunnerOptions, ChannelLoopScheduler,
        ChannelLoopSchedulerOptions, ChannelLoopSkipReason, MAX_RESULT_PREVIEW_LENGTH,
        truncate_error, truncate_result_preview,
    };
    use crate::channels::channel_loop_store::{
        ChannelLoop, ChannelLoopInput, ChannelLoopPatch, ChannelLoopStatus, ChannelLoopStore,
        PatchField, SessionTarget,
    };
    use chrono::{DateTime, Utc};
    use futures_util::future::BoxFuture;
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Semaphore;
    use tokio::time::timeout;
    use uuid::Uuid;

    const NOW_TEXT: &str = "2026-06-30T01:05:30.000Z";

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(NOW_TEXT)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn created_at() -> DateTime<Utc> {
        now() - chrono::Duration::minutes(5)
    }

    fn temp_path() -> PathBuf {
        let directory =
            std::env::temp_dir().join(format!("canopy-loop-scheduler-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        directory.join("loops.json")
    }

    fn input(id: impl Into<String>) -> ChannelLoopInput {
        ChannelLoopInput {
            channel_name: "feishu-main".to_owned(),
            target: SessionTarget {
                channel_name: "feishu-main".to_owned(),
                sender_id: "alice".to_owned(),
                chat_id: "chat-1".to_owned(),
                thread_id: None,
                is_group: Some(false),
                extra: serde_json::Map::new(),
            },
            cwd: "/repo".to_owned(),
            cron: "* * * * *".to_owned(),
            prompt: "summarize".to_owned(),
            label: Some(id.into()),
            recurring: true,
            created_by: "alice".to_owned(),
            extra: serde_json::Map::new(),
        }
    }

    fn store() -> Arc<ChannelLoopStore> {
        static IDS: AtomicUsize = AtomicUsize::new(0);
        Arc::new(ChannelLoopStore::with_factories(
            temp_path(),
            created_at,
            || format!("job-{}", IDS.fetch_add(1, Ordering::SeqCst)),
        ))
    }

    #[derive(Clone)]
    enum Outcome {
        Success(Option<String>),
        Error(ChannelLoopRunError),
    }

    struct TestRunner {
        calls: AtomicUsize,
        last_timeout_ms: AtomicUsize,
        outcome: Mutex<Outcome>,
        gate: Option<Arc<Semaphore>>,
        honor_should_continue: bool,
        on_run: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    }

    impl TestRunner {
        fn new(outcome: Outcome) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                last_timeout_ms: AtomicUsize::new(0),
                outcome: Mutex::new(outcome),
                gate: None,
                honor_should_continue: true,
                on_run: Mutex::new(None),
            }
        }

        fn blocked(outcome: Outcome, permits: usize) -> Self {
            Self {
                gate: Some(Arc::new(Semaphore::new(permits))),
                ..Self::new(outcome)
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl ChannelLoopRunner for TestRunner {
        fn run_loop_prompt<'a>(
            &'a self,
            _job: ChannelLoop,
            options: ChannelLoopRunnerOptions,
        ) -> BoxFuture<'a, Result<Option<String>, ChannelLoopRunError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.last_timeout_ms
                .store(options.timeout_ms as usize, Ordering::SeqCst);
            if let Some(on_run) = self
                .on_run
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
            {
                on_run();
            }
            let gate = self.gate.clone();
            let outcome = self
                .outcome
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            let honor_should_continue = self.honor_should_continue;
            Box::pin(async move {
                if let Some(gate) = gate {
                    let permit = gate
                        .acquire()
                        .await
                        .map_err(|error| ChannelLoopRunError::failed(error.to_string()))?;
                    permit.forget();
                }
                if honor_should_continue
                    && !options
                        .should_continue()
                        .await
                        .map_err(|error| ChannelLoopRunError::failed(error.to_string()))?
                {
                    return Err(ChannelLoopRunError::skipped(
                        "loop disabled before completion",
                        ChannelLoopSkipReason::Dropped,
                    ));
                }
                match outcome {
                    Outcome::Success(result) => Ok(result),
                    Outcome::Error(error) => Err(error),
                }
            })
        }
    }

    struct StaleThenCurrentRunner {
        calls: AtomicUsize,
        finished: AtomicUsize,
        first_gate: Arc<Semaphore>,
        second_gate: Arc<Semaphore>,
    }

    impl ChannelLoopRunner for StaleThenCurrentRunner {
        fn run_loop_prompt<'a>(
            &'a self,
            _job: ChannelLoop,
            _options: ChannelLoopRunnerOptions,
        ) -> BoxFuture<'a, Result<Option<String>, ChannelLoopRunError>> {
            let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
            let gate = if call_index == 0 {
                Arc::clone(&self.first_gate)
            } else {
                Arc::clone(&self.second_gate)
            };
            let result = if call_index == 0 {
                "stale result"
            } else {
                "current result"
            };
            let finished = &self.finished;
            Box::pin(async move {
                let permit = gate
                    .acquire()
                    .await
                    .map_err(|error| ChannelLoopRunError::failed(error.to_string()))?;
                permit.forget();
                finished.fetch_add(1, Ordering::SeqCst);
                Ok(Some(result.to_owned()))
            })
        }
    }

    fn scheduler(store: Arc<ChannelLoopStore>, runner: Arc<TestRunner>) -> ChannelLoopScheduler {
        let channels = HashMap::from([(
            "feishu-main".to_owned(),
            runner as Arc<dyn ChannelLoopRunner>,
        )]);
        let mut options = ChannelLoopSchedulerOptions::new(store, channels, |_, after| {
            Ok(if after < now() {
                now() - chrono::Duration::seconds(60)
            } else {
                now() + chrono::Duration::seconds(60)
            })
        })
        .with_clock(now);
        options.loop_timeout_ms = 1_234;
        ChannelLoopScheduler::new(options)
    }

    async fn wait_for_calls(runner: &TestRunner, expected: usize) {
        timeout(Duration::from_secs(2), async {
            while runner.calls() < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("runner call count did not reach expected value");
    }

    #[tokio::test]
    async fn coalesces_ticks_and_records_bounded_success_preview() {
        let store = store();
        let job = store.create(input("summary")).await.unwrap();
        let runner = Arc::new(TestRunner::new(Outcome::Success(Some("x".repeat(600)))));
        let scheduler = scheduler(Arc::clone(&store), Arc::clone(&runner));

        let (first, second) = tokio::join!(scheduler.tick(), scheduler.tick());
        first.unwrap();
        second.unwrap();
        wait_for_calls(&runner, 1).await;
        assert_eq!(runner.calls(), 1);

        timeout(Duration::from_secs(2), async {
            loop {
                let current = store.list().await.unwrap().remove(0);
                if current.last_status == Some(ChannelLoopStatus::Ok) {
                    let expected = "x".repeat(500);
                    assert_eq!(
                        current.last_result_preview.as_deref(),
                        Some(expected.as_str())
                    );
                    assert_eq!(current.run_count, 1.0);
                    assert_eq!(current.consecutive_failures, 0.0);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(runner.last_timeout_ms.load(Ordering::SeqCst), 1_234);
        assert_eq!(store.list().await.unwrap()[0].id, job.id);
    }

    #[tokio::test]
    async fn caps_concurrent_fires_at_five_and_dispatches_remaining_work_later() {
        let store = store();
        for index in 0..6 {
            store.create(input(format!("job {index}"))).await.unwrap();
        }
        let runner = Arc::new(TestRunner::blocked(Outcome::Success(None), 0));
        let scheduler = scheduler(Arc::clone(&store), Arc::clone(&runner));

        scheduler.tick().await.unwrap();
        wait_for_calls(&runner, 5).await;
        assert_eq!(runner.calls(), 5);

        runner.gate.as_ref().unwrap().add_permits(5);
        timeout(Duration::from_secs(2), async {
            while runner.calls() < 5
                || store
                    .list()
                    .await
                    .unwrap()
                    .iter()
                    .any(|job| job.running_since.is_some())
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        scheduler.tick().await.unwrap();
        wait_for_calls(&runner, 6).await;
        runner.gate.as_ref().unwrap().add_permits(1);
    }

    #[tokio::test]
    async fn failures_are_truncated_counted_and_auto_disable_at_the_limit() {
        let store = store();
        let created = store.create(input("summary")).await.unwrap();
        store
            .update(
                &created.id,
                ChannelLoopPatch {
                    consecutive_failures: PatchField::Set(4.0),
                    ..ChannelLoopPatch::default()
                },
            )
            .await
            .unwrap();
        let runner = Arc::new(TestRunner::new(Outcome::Error(
            ChannelLoopRunError::failed("e".repeat(1_200)),
        )));
        let scheduler = scheduler(Arc::clone(&store), Arc::clone(&runner));

        scheduler.tick().await.unwrap();
        wait_for_calls(&runner, 1).await;

        timeout(Duration::from_secs(2), async {
            loop {
                let job = store.list().await.unwrap().remove(0);
                if job.last_status == Some(ChannelLoopStatus::Error) {
                    assert!(!job.enabled);
                    let expected = "e".repeat(1_000);
                    assert_eq!(job.last_error.as_deref(), Some(expected.as_str()));
                    assert_eq!(job.consecutive_failures, 5.0);
                    assert_eq!(job.run_count, 1.0);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn skipped_and_recovery_aborted_runs_do_not_increment_failures() {
        let store = store();
        let created = store.create(input("summary")).await.unwrap();
        let runner = Arc::new(TestRunner::new(Outcome::Error(
            ChannelLoopRunError::skipped("cancelled", ChannelLoopSkipReason::CancelCommand),
        )));
        let initial_scheduler = scheduler(Arc::clone(&store), Arc::clone(&runner));
        initial_scheduler.tick().await.unwrap();
        wait_for_calls(&runner, 1).await;
        timeout(Duration::from_secs(2), async {
            loop {
                let job = store.list().await.unwrap().remove(0);
                if job.running_since.is_none() {
                    assert_eq!(job.consecutive_failures, 0.0);
                    assert_eq!(job.run_count, 0.0);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        store
            .update(
                &created.id,
                ChannelLoopPatch {
                    last_fired_at: PatchField::Set(Some("2026-06-30T01:00:00.000Z".to_owned())),
                    last_finished_at: PatchField::Set(Some("2026-06-30T01:00:00.000Z".to_owned())),
                    ..ChannelLoopPatch::default()
                },
            )
            .await
            .unwrap();

        let runner = Arc::new(TestRunner::new(Outcome::Error(
            ChannelLoopRunError::failed("bridge replaced"),
        )));
        let recovery_scheduler = Arc::new(scheduler(Arc::clone(&store), Arc::clone(&runner)));
        *runner
            .on_run
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new({
            let scheduler = Arc::clone(&recovery_scheduler);
            move || scheduler.mark_bridge_recovery()
        }));
        recovery_scheduler.tick().await.unwrap();
        wait_for_calls(&runner, 1).await;
        timeout(Duration::from_secs(2), async {
            loop {
                let job = store.list().await.unwrap().remove(0);
                if job.last_status == Some(ChannelLoopStatus::Error) {
                    assert_eq!(job.last_error.as_deref(), Some("bridge replaced"));
                    assert_eq!(job.consecutive_failures, 0.0);
                    assert_eq!(job.run_count, 0.0);
                    assert!(job.enabled);
                    assert_eq!(job.running_since, None);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn startup_reconciliation_clears_persisted_running_markers() {
        let store = store();
        let job = store.create(input("summary")).await.unwrap();
        store
            .update(
                &job.id,
                ChannelLoopPatch {
                    running_since: PatchField::Set(Some("2026-06-30T00:00:00.000Z".to_owned())),
                    ..ChannelLoopPatch::default()
                },
            )
            .await
            .unwrap();
        let runner = Arc::new(TestRunner::new(Outcome::Success(None)));
        let channels = HashMap::from([(
            "feishu-main".to_owned(),
            runner as Arc<dyn ChannelLoopRunner>,
        )]);
        let scheduler = ChannelLoopScheduler::new(
            ChannelLoopSchedulerOptions::new(Arc::clone(&store), channels, |_, _| {
                Ok(now() + chrono::Duration::seconds(60))
            })
            .with_clock(now)
            .with_interval_ms(60_000),
        );
        scheduler.start().unwrap();

        timeout(Duration::from_secs(2), async {
            loop {
                if store.list().await.unwrap()[0].running_since.is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        scheduler.stop();
    }

    #[tokio::test]
    async fn a_completed_replacement_run_fences_the_older_success_marker() {
        let store = store();
        store.create(input("summary")).await.unwrap();
        let runner = Arc::new(StaleThenCurrentRunner {
            calls: AtomicUsize::new(0),
            finished: AtomicUsize::new(0),
            first_gate: Arc::new(Semaphore::new(0)),
            second_gate: Arc::new(Semaphore::new(0)),
        });
        let channels = HashMap::from([(
            "feishu-main".to_owned(),
            runner.clone() as Arc<dyn ChannelLoopRunner>,
        )]);
        let scheduler = ChannelLoopScheduler::new(
            ChannelLoopSchedulerOptions::new(Arc::clone(&store), channels, |_, _| {
                Ok(now() - chrono::Duration::seconds(60))
            })
            .with_clock(now),
        );
        scheduler.tick().await.unwrap();
        timeout(Duration::from_secs(2), async {
            while runner.calls.load(Ordering::SeqCst) < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        scheduler.stop();
        scheduler.tick().await.unwrap();
        timeout(Duration::from_secs(2), async {
            while runner.calls.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        runner.second_gate.add_permits(1);
        timeout(Duration::from_secs(2), async {
            while runner.finished.load(Ordering::SeqCst) < 1 {
                tokio::task::yield_now().await;
            }
            loop {
                let job = store.list().await.unwrap().remove(0);
                if job.last_result_preview.as_deref() == Some("current result")
                    && job.running_since.is_none()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        runner.first_gate.add_permits(1);
        timeout(Duration::from_secs(2), async {
            while runner.finished.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
            while !scheduler
                .inner
                .in_flight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::task::yield_now().await;
        let job = store.list().await.unwrap().remove(0);
        assert_eq!(job.last_result_preview.as_deref(), Some("current result"));
        assert_eq!(job.run_count, 1.0);
    }

    #[test]
    fn truncation_respects_caps_and_keeps_unicode_boundaries() {
        assert_eq!(
            truncate_result_preview(&"x".repeat(600)).len(),
            MAX_RESULT_PREVIEW_LENGTH
        );
        assert_eq!(truncate_error(&"e".repeat(1_200)).len(), 1_000);
        let truncated = truncate_result_preview(&format!("{}🦀tail", "x".repeat(499)));
        assert_eq!(truncated, "x".repeat(499));
    }

    #[tokio::test]
    async fn should_continue_observes_a_job_disabled_while_its_prompt_waits() {
        let store = store();
        let job = store.create(input("summary")).await.unwrap();
        let runner = Arc::new(TestRunner::blocked(
            Outcome::Success(Some("late".into())),
            0,
        ));
        let scheduler = scheduler(Arc::clone(&store), Arc::clone(&runner));
        scheduler.tick().await.unwrap();
        wait_for_calls(&runner, 1).await;

        store.disable(&job.id).await.unwrap();
        runner.gate.as_ref().unwrap().add_permits(1);
        timeout(Duration::from_secs(2), async {
            loop {
                let current = store.list().await.unwrap().remove(0);
                if current.running_since.is_none() {
                    assert_eq!(current.last_result_preview, None);
                    assert_eq!(current.last_status, None);
                    assert_eq!(current.run_count, 0.0);
                    assert_eq!(current.consecutive_failures, 0.0);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
