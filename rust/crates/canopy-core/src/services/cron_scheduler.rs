//! Bounded, in-memory cron and loop-wakeup scheduling.
//!
//! This is the session-only runtime counterpart to
//! `packages/core/src/services/cronScheduler.ts`. Durable task file support is
//! deliberately kept in `cron_tasks_file`; this scheduler does not load or
//! write that store until the native runtime has a durable owner lifecycle.

use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use chrono::{DateTime, Local, SecondsFormat, TimeZone, Utc};
use indexmap::IndexMap;
use serde::Serialize;
use thiserror::Error;

use crate::services::cron_scheduler_primitives::{
    WAKEUP_MAX_SECONDS, WAKEUP_MIN_SECONDS, clamp_wakeup_seconds, compute_jitter_ms,
};
use crate::services::cron_tasks_file::generate_cron_task_id;
use crate::utils::cron_parser::{matches, next_fire_time, parse_cron};

pub const MAX_JOBS: usize = 50;
pub const DEFAULT_RECURRING_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
pub const MAX_WAKEUP_CHAIN_AGE: Duration = Duration::from_secs(24 * 60 * 60);
pub const MAX_CRON_PROMPT_BYTES: usize = 64 * 1024;
pub const MAX_WAKEUP_PROMPT_CHARS: usize = 10_000;
pub const SCHEDULER_TICK_INTERVAL: Duration = Duration::from_secs(1);

const MINUTE_MS: i64 = 60_000;

pub type CronFireHandler = Arc<dyn Fn(CronJob) + Send + Sync + 'static>;

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CronJob {
    pub id: String,
    pub cron_expr: String,
    pub prompt: String,
    pub recurring: bool,
    /// Unix epoch milliseconds.
    pub created_at: i64,
    /// `None` means recurring jobs do not expire.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fire_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_fired_at: Option<i64>,
    pub jitter_ms: i64,
    pub durable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub todo_work_chain_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WakeupSchedule {
    pub id: String,
    pub scheduled_for: String,
    pub clamped_delay_seconds: u32,
    pub was_clamped: bool,
    pub replaced_id: Option<String>,
}

#[derive(Debug, Error)]
pub enum CronSchedulerError {
    #[error("Invalid cron expression: {0}")]
    InvalidCron(String),
    #[error("{0}")]
    NoFireTime(String),
    #[error("Maximum number of cron jobs ({MAX_JOBS}) reached. Delete some jobs first.")]
    MaximumJobs,
    #[error("Cron prompt must not be empty.")]
    EmptyPrompt,
    #[error("Cron prompt exceeds the {MAX_CRON_PROMPT_BYTES}-byte session limit.")]
    PromptTooLong,
    #[error("Loop wakeup prompt exceeds {MAX_WAKEUP_PROMPT_CHARS} characters.")]
    WakeupPromptTooLong,
    #[error(
        "Cannot schedule a loop wakeup: the scheduler is disabled for this session. Restart the session to re-enable."
    )]
    Disabled,
    #[error(
        "Loop wakeup chain exceeded the 24h session limit. Omit LoopWakeup to end this loop, or start a new session."
    )]
    WakeupChainExceeded,
    #[error("Could not start the cron scheduler timer: {0}")]
    ThreadStart(String),
}

struct SchedulerState {
    jobs: IndexMap<String, CronJob>,
    wakeups: IndexMap<String, CronJob>,
    wakeup_chain_started_at: Option<i64>,
    recurring_max_age_ms: Option<i64>,
    disabled: bool,
    running: bool,
    generation: u64,
    on_fire: Option<CronFireHandler>,
}

struct SchedulerInner {
    state: Mutex<SchedulerState>,
    worker: Mutex<Option<JoinHandle<()>>>,
    stop_signal: (Mutex<bool>, Condvar),
}

/// Owns up to 50 in-memory cron jobs and one pending session wakeup.
///
/// `start` runs a one-second timer on a dedicated thread so the core service
/// does not depend on a Tokio runtime. Callbacks run outside the scheduler lock.
pub struct CronScheduler {
    inner: Arc<SchedulerInner>,
}

impl Default for CronScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for CronScheduler {
    fn drop(&mut self) {
        self.stop();
    }
}

impl CronScheduler {
    pub fn new() -> Self {
        Self::with_recurring_max_age(Some(DEFAULT_RECURRING_MAX_AGE))
    }

    /// Build a scheduler with a recurring-job age cap. `None` disables expiry.
    pub fn with_recurring_max_age(max_age: Option<Duration>) -> Self {
        let recurring_max_age_ms = max_age
            .filter(|age| !age.is_zero())
            .map(|age| age.as_millis().min(i64::MAX as u128) as i64);
        Self {
            inner: Arc::new(SchedulerInner {
                state: Mutex::new(SchedulerState {
                    jobs: IndexMap::new(),
                    wakeups: IndexMap::new(),
                    wakeup_chain_started_at: None,
                    recurring_max_age_ms,
                    disabled: false,
                    running: false,
                    generation: 0,
                    on_fire: None,
                }),
                worker: Mutex::new(None),
                stop_signal: (Mutex::new(false), Condvar::new()),
            }),
        }
    }

    /// Create a session-only cron job. The expression must parse and have a
    /// matching date within the parser's four-year search window.
    pub fn create(
        &self,
        cron_expr: impl Into<String>,
        prompt: impl Into<String>,
        recurring: bool,
    ) -> Result<CronJob, CronSchedulerError> {
        let cron_expr = cron_expr.into();
        let prompt = prompt.into();
        validate_prompt(&prompt)?;
        parse_cron(&cron_expr)
            .map_err(|error| CronSchedulerError::InvalidCron(error.to_string()))?;
        let now = Local::now();
        next_fire_time(&cron_expr, &now)
            .map_err(|error| CronSchedulerError::NoFireTime(error.to_string()))?;
        let now_ms = now.timestamp_millis();
        let mut state = lock(&self.inner.state);
        if state.jobs.len() >= MAX_JOBS {
            return Err(CronSchedulerError::MaximumJobs);
        }
        let id = unique_id(&state.jobs, &state.wakeups);
        let jitter_ms = compute_jitter_ms(&id, &cron_expr, recurring, &now);
        let job = CronJob {
            id: id.clone(),
            cron_expr,
            prompt,
            recurring,
            created_at: now_ms,
            expires_at: if recurring {
                state
                    .recurring_max_age_ms
                    .map(|max_age| now_ms.saturating_add(max_age))
            } else {
                None
            },
            fire_at_ms: None,
            // Prevent firing in the creation minute.
            last_fired_at: Some(now_ms.div_euclid(MINUTE_MS) * MINUTE_MS),
            jitter_ms,
            durable: false,
            todo_work_chain_id: None,
        };
        state.jobs.insert(id, job.clone());
        Ok(job)
    }

    /// Schedule a second-resolution one-shot loop wakeup. A new wakeup
    /// replaces the previous pending wakeup and the whole re-arm chain is
    /// limited to 24 hours from its first schedule.
    pub fn schedule_wakeup(
        &self,
        delay_seconds: f64,
        prompt: impl Into<String>,
        todo_work_chain_id: Option<String>,
    ) -> Result<WakeupSchedule, CronSchedulerError> {
        let prompt = prompt.into();
        validate_prompt(&prompt)?;
        if prompt.chars().count() > MAX_WAKEUP_PROMPT_CHARS {
            return Err(CronSchedulerError::WakeupPromptTooLong);
        }

        let mut state = lock(&self.inner.state);
        if state.disabled {
            return Err(CronSchedulerError::Disabled);
        }
        let clamped_delay_seconds = clamp_wakeup_seconds(delay_seconds);
        let rounded_delay_seconds = if delay_seconds.is_finite() {
            Some(delay_seconds.round())
        } else {
            None
        };
        let was_clamped = rounded_delay_seconds.is_none_or(|rounded| {
            rounded < WAKEUP_MIN_SECONDS as f64 || rounded > WAKEUP_MAX_SECONDS as f64
        });
        let now_ms = Utc::now().timestamp_millis();
        let fire_at_ms = now_ms.saturating_add(i64::from(clamped_delay_seconds) * 1_000);
        let replaced_id = state.wakeups.first().map(|(id, _)| id.clone());
        if state.wakeup_chain_started_at.is_none() {
            state.wakeup_chain_started_at = Some(now_ms);
        }
        state.wakeups.clear();
        let chain_started_at = state.wakeup_chain_started_at.unwrap_or(now_ms);
        let chain_deadline =
            chain_started_at.saturating_add(MAX_WAKEUP_CHAIN_AGE.as_millis() as i64);
        if fire_at_ms > chain_deadline {
            return Err(CronSchedulerError::WakeupChainExceeded);
        }

        let id = unique_id(&state.jobs, &state.wakeups);
        let job = CronJob {
            id: id.clone(),
            cron_expr: "@wakeup".to_owned(),
            prompt,
            recurring: false,
            created_at: now_ms,
            expires_at: None,
            fire_at_ms: Some(fire_at_ms),
            last_fired_at: None,
            jitter_ms: 0,
            durable: false,
            todo_work_chain_id,
        };
        state.wakeups.insert(id.clone(), job);

        let scheduled_for = DateTime::<Utc>::from_timestamp_millis(fire_at_ms)
            .unwrap_or_else(Utc::now)
            .to_rfc3339_opts(SecondsFormat::Millis, true);
        Ok(WakeupSchedule {
            id,
            scheduled_for,
            clamped_delay_seconds,
            was_clamped,
            replaced_id,
        })
    }

    /// Cancel a job or pending wakeup by id.
    pub fn delete(&self, id: &str) -> bool {
        let mut state = lock(&self.inner.state);
        state.jobs.shift_remove(id).is_some() || state.wakeups.shift_remove(id).is_some()
    }

    pub fn cancel_wakeup(&self, id: &str) -> bool {
        lock(&self.inner.state).wakeups.shift_remove(id).is_some()
    }

    pub fn cancel_all_wakeups(&self) -> usize {
        let mut state = lock(&self.inner.state);
        let count = state.wakeups.len();
        state.wakeups.clear();
        count
    }

    /// Snapshot jobs in insertion order, followed by the pending wakeup.
    pub fn list(&self) -> Vec<CronJob> {
        let state = lock(&self.inner.state);
        state
            .jobs
            .values()
            .chain(state.wakeups.values())
            .cloned()
            .collect()
    }

    pub fn size(&self) -> usize {
        let state = lock(&self.inner.state);
        state.jobs.len() + state.wakeups.len()
    }

    pub fn session_size(&self) -> usize {
        self.size()
    }

    pub fn running(&self) -> bool {
        lock(&self.inner.state).running
    }

    pub fn disabled(&self) -> bool {
        lock(&self.inner.state).disabled
    }

    pub fn recurring_max_age(&self) -> Option<Duration> {
        lock(&self.inner.state)
            .recurring_max_age_ms
            .map(|milliseconds| Duration::from_millis(milliseconds.max(0) as u64))
    }

    /// Start the timer, or replace its delivery callback if it is already
    /// running. `stop` leaves jobs queryable and permits a later restart.
    pub fn start(&self, on_fire: CronFireHandler) -> Result<(), CronSchedulerError> {
        let mut worker_slot = lock(&self.inner.worker);
        let generation = {
            let mut state = lock(&self.inner.state);
            if state.disabled {
                return Err(CronSchedulerError::Disabled);
            }
            state.on_fire = Some(on_fire);
            if state.running {
                return Ok(());
            }
            state.running = true;
            state.generation = state.generation.wrapping_add(1);
            state.generation
        };
        if worker_slot.is_some() {
            // A previous callback stopped its own worker. Dropping the stale
            // JoinHandle detaches an already-exiting thread.
            worker_slot.take();
        }
        *lock(&self.inner.stop_signal.0) = false;
        let weak = Arc::downgrade(&self.inner);
        let handle = thread::Builder::new()
            .name("canopy-cron-scheduler".to_owned())
            .spawn(move || timer_loop(weak, generation))
            .map_err(|error| {
                let mut state = lock(&self.inner.state);
                state.running = false;
                state.on_fire = None;
                CronSchedulerError::ThreadStart(error.to_string())
            })?;
        *worker_slot = Some(handle);
        Ok(())
    }

    /// Stop ticking, discard session wakeups, and retain cron jobs.
    pub fn stop(&self) {
        let mut worker_slot = lock(&self.inner.worker);
        {
            let mut state = lock(&self.inner.state);
            state.running = false;
            state.generation = state.generation.wrapping_add(1);
            state.on_fire = None;
            state.wakeups.clear();
            state.wakeup_chain_started_at = None;
        }
        *lock(&self.inner.stop_signal.0) = true;
        self.inner.stop_signal.1.notify_all();
        let handle = worker_slot.take();
        drop(worker_slot);
        if let Some(handle) = handle {
            if handle.thread().id() != thread::current().id() {
                let _ = handle.join();
            }
        }
    }

    /// Permanently disable this scheduler instance, then stop it.
    pub fn disable(&self) {
        {
            let mut state = lock(&self.inner.state);
            state.disabled = true;
        }
        self.stop();
    }

    /// Clear jobs and stop the timer.
    pub fn destroy(&self) {
        self.stop();
        let mut state = lock(&self.inner.state);
        state.jobs.clear();
        state.wakeups.clear();
        state.wakeup_chain_started_at = None;
    }

    /// Perform one tick using the current local wall clock.
    pub fn tick(&self) -> usize {
        self.tick_at(Local::now())
    }

    /// Perform one tick at an explicit local date. Returns the number of
    /// deliveries. This also provides a deterministic host/test seam.
    pub fn tick_at(&self, now: DateTime<Local>) -> usize {
        tick_inner(&self.inner, now.timestamp_millis(), None)
    }
}

fn timer_loop(inner: Weak<SchedulerInner>, generation: u64) {
    loop {
        let Some(strong) = inner.upgrade() else {
            break;
        };
        let stopped = lock(&strong.stop_signal.0);
        let (stopped, _) = strong
            .stop_signal
            .1
            .wait_timeout_while(stopped, SCHEDULER_TICK_INTERVAL, |stopped| !*stopped)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *stopped {
            break;
        }
        drop(stopped);
        let should_tick = {
            let state = lock(&strong.state);
            state.running && state.generation == generation
        };
        if !should_tick {
            break;
        }
        tick_inner(&strong, Local::now().timestamp_millis(), Some(generation));
    }
}

fn tick_inner(
    inner: &Arc<SchedulerInner>,
    current_ms: i64,
    expected_generation: Option<u64>,
) -> usize {
    if Local.timestamp_millis_opt(current_ms).single().is_none() {
        return 0;
    }
    let mut fired = Vec::new();
    let on_fire = {
        let mut state = lock(&inner.state);
        if state.disabled
            || expected_generation
                .is_some_and(|generation| !state.running || state.generation != generation)
        {
            return 0;
        }
        let current_minute_ms = current_ms.div_euclid(MINUTE_MS) * MINUTE_MS;
        let mut remove_ids = Vec::new();
        for job in state.jobs.values_mut() {
            let jitter_window_minutes =
                job.jitter_ms.unsigned_abs().div_ceil(MINUTE_MS as u64) as i64;
            let mut matched_minute_ms = None;
            for offset in -jitter_window_minutes..=jitter_window_minutes {
                let candidate_ms =
                    current_minute_ms.saturating_add(offset.saturating_mul(MINUTE_MS));
                let Some(candidate) = Local.timestamp_millis_opt(candidate_ms).single() else {
                    continue;
                };
                if !matches(&job.cron_expr, &candidate).unwrap_or(false) {
                    continue;
                }
                let fire_at = candidate_ms.saturating_add(job.jitter_ms);
                if current_ms >= fire_at
                    && matched_minute_ms.is_none_or(|matched| candidate_ms > matched)
                {
                    matched_minute_ms = Some(candidate_ms);
                }
            }
            let Some(matched_minute_ms) = matched_minute_ms else {
                continue;
            };
            if job
                .last_fired_at
                .is_some_and(|last_fired_at| last_fired_at >= matched_minute_ms)
            {
                continue;
            }
            job.last_fired_at = Some(matched_minute_ms);
            let expired = job.recurring
                && job
                    .expires_at
                    .is_some_and(|expires_at| current_ms >= expires_at);
            if !job.recurring || expired {
                remove_ids.push(job.id.clone());
            }
            fired.push(job.clone());
        }
        for id in remove_ids {
            state.jobs.shift_remove(&id);
        }

        let due_wakeups = state
            .wakeups
            .values()
            .filter(|job| job.fire_at_ms.is_some_and(|fire_at| fire_at <= current_ms))
            .map(|job| job.id.clone())
            .collect::<Vec<_>>();
        for id in due_wakeups {
            if let Some(job) = state.wakeups.shift_remove(&id) {
                fired.push(job);
            }
        }
        state.on_fire.clone()
    };

    if let Some(on_fire) = on_fire {
        for job in &fired {
            on_fire(job.clone());
        }
    }
    fired.len()
}

fn unique_id(jobs: &IndexMap<String, CronJob>, wakeups: &IndexMap<String, CronJob>) -> String {
    loop {
        let id = generate_cron_task_id();
        if !jobs.contains_key(&id) && !wakeups.contains_key(&id) {
            return id;
        }
    }
}

fn validate_prompt(prompt: &str) -> Result<(), CronSchedulerError> {
    if prompt.trim().is_empty() {
        return Err(CronSchedulerError::EmptyPrompt);
    }
    if prompt.len() > MAX_CRON_PROMPT_BYTES {
        return Err(CronSchedulerError::PromptTooLong);
    }
    Ok(())
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
