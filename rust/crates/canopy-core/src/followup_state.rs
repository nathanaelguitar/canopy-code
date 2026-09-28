//! Follow-up suggestion state and controller, ported from
//! `packages/core/src/followup/followupState.ts`.
//!
//! The controller owns the same delayed-show and accept-debounce transitions
//! as the TypeScript implementation. Calls to user callbacks are made without
//! holding the controller lock so callbacks may safely call back into it.

use serde::{Deserialize, Serialize};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

/// Delay before a generated suggestion becomes visible.
pub const SUGGESTION_DELAY_MS: u64 = 300;
/// Duration for which a successful accept blocks another accept.
pub const ACCEPT_DEBOUNCE_MS: u64 = 100;

/// State passed to consumers of the suggestion controller.
///
/// Serialization uses the TypeScript field names (`isVisible`, `shownAt`).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowupState {
    pub suggestion: Option<String>,
    pub is_visible: bool,
    pub shown_at: i64,
}

impl FollowupState {
    /// Returns the initial empty state used by the TypeScript controller.
    pub fn initial() -> Self {
        Self {
            suggestion: None,
            is_visible: false,
            shown_at: 0,
        }
    }
}

impl Default for FollowupState {
    fn default() -> Self {
        Self::initial()
    }
}

/// The input method attached to an accepted suggestion, when supplied.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AcceptMethod {
    Tab,
    Enter,
    Right,
}

/// Whether accepted text came from the controller's visible suggestion or a
/// fallback supplied by the caller.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AcceptSource {
    Live,
    Fallback,
}

/// Follow-up outcome passed to telemetry callbacks. Optional keys are omitted
/// in serialized JSON in the same way undefined JavaScript object properties
/// are omitted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FollowupOutcome {
    pub outcome: FollowupOutcomeKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accept_method: Option<AcceptMethod>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accept_source: Option<AcceptSource>,
    pub time_ms: i64,
    pub suggestion_length: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FollowupOutcomeKind {
    Accepted,
    Ignored,
}

/// Callback called synchronously whenever state is applied.
pub type StateChangeCallback = Arc<dyn Fn(FollowupState) + Send + Sync + 'static>;
/// Callback invoked for accepted text. It is looked up when the queued accept
/// action runs, matching the TypeScript getter behavior.
pub type AcceptCallback = Arc<dyn Fn(String) + Send + Sync + 'static>;
pub type GetAcceptCallback = Arc<dyn Fn() -> Option<AcceptCallback> + Send + Sync + 'static>;
pub type OutcomeCallback = Arc<dyn Fn(FollowupOutcome) + Send + Sync + 'static>;

/// Controller callbacks and feature flag.
#[derive(Clone)]
pub struct FollowupControllerOptions {
    pub enabled: bool,
    pub on_state_change: StateChangeCallback,
    pub get_on_accept: Option<GetAcceptCallback>,
    pub on_outcome: Option<OutcomeCallback>,
}

impl FollowupControllerOptions {
    pub fn new(on_state_change: StateChangeCallback) -> Self {
        Self {
            enabled: true,
            on_state_change,
            get_on_accept: None,
            on_outcome: None,
        }
    }

    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn with_get_on_accept(mut self, callback: GetAcceptCallback) -> Self {
        self.get_on_accept = Some(callback);
        self
    }

    pub fn with_on_outcome(mut self, callback: OutcomeCallback) -> Self {
        self.on_outcome = Some(callback);
        self
    }
}

/// Options for accepting a suggestion.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AcceptOptions {
    pub skip_on_accept: bool,
    pub fallback_text: Option<String>,
}

#[derive(Debug, Error, Eq, PartialEq)]
#[error("creating a follow-up controller requires an active Tokio runtime")]
pub struct FollowupRuntimeUnavailable;

#[derive(Default)]
struct ControllerInner {
    state: FollowupState,
    accepting: bool,
    suggestion_timer: Option<JoinHandle<()>>,
    suggestion_generation: u64,
    accept_timer: Option<JoinHandle<()>>,
    accept_generation: u64,
}

/// Framework-independent follow-up controller. It uses the current Tokio
/// runtime for the delayed show, queued accept callback, and debounce timeout.
pub struct FollowupController {
    enabled: bool,
    on_state_change: StateChangeCallback,
    get_on_accept: Option<GetAcceptCallback>,
    on_outcome: Option<OutcomeCallback>,
    runtime: Handle,
    inner: Arc<Mutex<ControllerInner>>,
}

impl FollowupController {
    /// Creates a controller using the current Tokio runtime.
    pub fn new(options: FollowupControllerOptions) -> Result<Self, FollowupRuntimeUnavailable> {
        let runtime = Handle::try_current().map_err(|_| FollowupRuntimeUnavailable)?;
        Ok(Self::with_runtime(options, runtime))
    }

    /// Creates a controller with an explicit runtime handle.
    pub fn with_runtime(options: FollowupControllerOptions, runtime: Handle) -> Self {
        Self {
            enabled: options.enabled,
            on_state_change: options.on_state_change,
            get_on_accept: options.get_on_accept,
            on_outcome: options.on_outcome,
            runtime,
            inner: Arc::new(Mutex::new(ControllerInner::default())),
        }
    }

    /// Returns a snapshot of the current state.
    pub fn state(&self) -> FollowupState {
        lock(&self.inner).state.clone()
    }

    /// Sets a suggestion. `None` and the empty string clear immediately;
    /// other strings are shown after [`SUGGESTION_DELAY_MS`].
    ///
    /// Every call cancels the prior pending show. A nonempty value supplied
    /// while disabled leaves the current state untouched after that cancel,
    /// matching the TypeScript controller.
    pub fn set_suggestion(&self, text: Option<&str>) {
        let mut inner = lock(&self.inner);
        cancel_suggestion_timer(&mut inner);

        let Some(text) = text.filter(|text| !text.is_empty()) else {
            inner.state = FollowupState::initial();
            let state = inner.state.clone();
            drop(inner);
            (self.on_state_change)(state);
            return;
        };

        if !self.enabled {
            return;
        }

        let suggestion = text.to_owned();
        let generation = inner.suggestion_generation;
        let weak_inner = Arc::downgrade(&self.inner);
        let on_state_change = Arc::clone(&self.on_state_change);
        let timer = self.runtime.spawn(async move {
            tokio::time::sleep(Duration::from_millis(SUGGESTION_DELAY_MS)).await;
            let Some(inner) = weak_inner.upgrade() else {
                return;
            };
            let state = {
                let mut inner = lock(&inner);
                if inner.suggestion_generation != generation {
                    return;
                }
                inner.suggestion_timer = None;
                inner.state = FollowupState {
                    suggestion: Some(suggestion),
                    is_visible: true,
                    shown_at: now_ms(),
                };
                inner.state.clone()
            };
            on_state_change(state);
        });
        inner.suggestion_timer = Some(timer);
    }

    /// Accepts the live suggestion or, if none exists, the supplied fallback.
    /// The outcome is reported synchronously; the current accept callback is
    /// looked up and invoked in a queued Tokio task, then the debounce lock is
    /// released after [`ACCEPT_DEBOUNCE_MS`].
    pub fn accept(&self, method: Option<AcceptMethod>, options: AcceptOptions) {
        let current_state = {
            let mut inner = lock(&self.inner);
            if inner.accepting {
                return;
            }
            cancel_suggestion_timer(&mut inner);
            inner.accepting = true;
            inner.state.clone()
        };

        let is_live = current_state
            .suggestion
            .as_deref()
            .is_some_and(|suggestion| !suggestion.is_empty());
        // JavaScript uses nullish coalescing for text selection. In the
        // impossible-through-normal-controller state Some("") still wins over
        // fallback text, then fails the following truthiness check.
        let text = current_state
            .suggestion
            .clone()
            .or_else(|| options.fallback_text.clone());
        let Some(text) = text.filter(|text| !text.is_empty()) else {
            lock(&self.inner).accepting = false;
            return;
        };

        let outcome = FollowupOutcome {
            outcome: FollowupOutcomeKind::Accepted,
            accept_method: method,
            accept_source: Some(if is_live {
                AcceptSource::Live
            } else {
                AcceptSource::Fallback
            }),
            time_ms: elapsed_ms(current_state.shown_at, now_ms()),
            suggestion_length: js_string_length(&text),
        };
        invoke_outcome(&self.on_outcome, outcome);

        self.apply_state(FollowupState::initial());

        let get_on_accept = self.get_on_accept.clone();
        let on_accept_text = text;
        let skip_on_accept = options.skip_on_accept;
        let weak_inner = Arc::downgrade(&self.inner);
        let runtime = self.runtime.clone();
        self.runtime.spawn(async move {
            let callback_result = catch_unwind(AssertUnwindSafe(|| {
                if !skip_on_accept {
                    if let Some(callback) = get_on_accept.and_then(|get| get()) {
                        callback(on_accept_text);
                    }
                }
            }));
            if let Err(error) = callback_result {
                log_callback_panic("onAccept", error);
            }

            let Some(inner_arc) = weak_inner.upgrade() else {
                return;
            };
            let mut inner = lock(&inner_arc);
            cancel_accept_timer(&mut inner);
            let generation = inner.accept_generation;
            let timer_inner = Arc::downgrade(&inner_arc);
            let timer = runtime.spawn(async move {
                tokio::time::sleep(Duration::from_millis(ACCEPT_DEBOUNCE_MS)).await;
                let Some(inner) = timer_inner.upgrade() else {
                    return;
                };
                let mut inner = lock(&inner);
                if inner.accept_generation != generation {
                    return;
                }
                inner.accept_timer = None;
                inner.accepting = false;
            });
            inner.accept_timer = Some(timer);
        });
    }

    /// Dismisses and clears a suggestion. An ignored outcome is emitted only
    /// when the suggestion was visible and nonempty.
    pub fn dismiss(&self) {
        let current_state = {
            let mut inner = lock(&self.inner);
            cancel_suggestion_timer(&mut inner);
            if !inner.state.is_visible
                && inner.state.suggestion.as_deref().is_none_or(str::is_empty)
            {
                return;
            }
            inner.state.clone()
        };

        if current_state.is_visible
            && let Some(suggestion) = current_state
                .suggestion
                .as_deref()
                .filter(|suggestion| !suggestion.is_empty())
        {
            invoke_outcome(
                &self.on_outcome,
                FollowupOutcome {
                    outcome: FollowupOutcomeKind::Ignored,
                    accept_method: None,
                    accept_source: None,
                    time_ms: elapsed_ms(current_state.shown_at, now_ms()),
                    suggestion_length: js_string_length(suggestion),
                },
            );
        }
        self.apply_state(FollowupState::initial());
    }

    /// Cancels both timers, unlocks acceptance, and applies the initial state.
    pub fn clear(&self) {
        {
            let mut inner = lock(&self.inner);
            cancel_timers(&mut inner);
            inner.accepting = false;
        }
        self.apply_state(FollowupState::initial());
    }

    /// Cancels both timers and unlocks acceptance without changing state.
    pub fn cleanup(&self) {
        let mut inner = lock(&self.inner);
        cancel_timers(&mut inner);
        inner.accepting = false;
    }

    fn apply_state(&self, state: FollowupState) {
        lock(&self.inner).state = state.clone();
        (self.on_state_change)(state);
    }
}

impl Drop for FollowupController {
    fn drop(&mut self) {
        let mut inner = lock(&self.inner);
        cancel_timers(&mut inner);
        inner.accepting = false;
    }
}

fn lock(inner: &Arc<Mutex<ControllerInner>>) -> MutexGuard<'_, ControllerInner> {
    inner
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn cancel_suggestion_timer(inner: &mut ControllerInner) {
    inner.suggestion_generation = inner.suggestion_generation.wrapping_add(1);
    if let Some(timer) = inner.suggestion_timer.take() {
        timer.abort();
    }
}

fn cancel_accept_timer(inner: &mut ControllerInner) {
    inner.accept_generation = inner.accept_generation.wrapping_add(1);
    if let Some(timer) = inner.accept_timer.take() {
        timer.abort();
    }
}

fn cancel_timers(inner: &mut ControllerInner) {
    cancel_suggestion_timer(inner);
    cancel_accept_timer(inner);
}

fn invoke_outcome(callback: &Option<OutcomeCallback>, outcome: FollowupOutcome) {
    if let Some(callback) = callback
        && let Err(error) = catch_unwind(AssertUnwindSafe(|| callback(outcome)))
    {
        log_callback_panic("onOutcome", error);
    }
}

fn log_callback_panic(callback: &str, error: Box<dyn std::any::Any + Send>) {
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic payload");
    eprintln!("[followup] {callback} callback threw: {message}");
}

fn now_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(i64::MAX as u128) as i64,
        Err(error) => -(error.duration().as_millis().min(i64::MAX as u128) as i64),
    }
}

fn elapsed_ms(shown_at: i64, now: i64) -> i64 {
    if shown_at > 0 {
        now.saturating_sub(shown_at)
    } else {
        0
    }
}

fn js_string_length(text: &str) -> u64 {
    text.encode_utf16().count().min(u64::MAX as usize) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::sleep;

    fn state_collector() -> (StateChangeCallback, Arc<Mutex<Vec<FollowupState>>>) {
        let states = Arc::new(Mutex::new(Vec::new()));
        let collected = Arc::clone(&states);
        let callback: StateChangeCallback = Arc::new(move |state| {
            collected.lock().unwrap().push(state);
        });
        (callback, states)
    }

    fn outcome_collector() -> (OutcomeCallback, Arc<Mutex<Vec<FollowupOutcome>>>) {
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let collected = Arc::clone(&outcomes);
        let callback: OutcomeCallback = Arc::new(move |outcome| {
            collected.lock().unwrap().push(outcome);
        });
        (callback, outcomes)
    }

    #[test]
    fn initial_state_and_outcome_keep_typescript_json_shape() {
        assert_eq!(
            serde_json::to_value(FollowupState::initial()).unwrap(),
            json!({"suggestion": null, "isVisible": false, "shownAt": 0})
        );
        assert_eq!(
            serde_json::to_value(FollowupOutcome {
                outcome: FollowupOutcomeKind::Ignored,
                accept_method: None,
                accept_source: None,
                time_ms: 5,
                suggestion_length: 3,
            })
            .unwrap(),
            json!({"outcome": "ignored", "time_ms": 5, "suggestion_length": 3})
        );
    }

    #[test]
    fn string_length_counts_utf16_code_units_like_javascript() {
        assert_eq!(js_string_length("hello"), 5);
        assert_eq!(js_string_length("é"), 1);
        assert_eq!(js_string_length("🎸"), 2);
    }

    #[tokio::test]
    async fn show_is_delayed_and_replacing_suggestion_cancels_old_timer() {
        let (on_state_change, states) = state_collector();
        let controller =
            FollowupController::new(FollowupControllerOptions::new(on_state_change)).unwrap();

        controller.set_suggestion(Some("first"));
        sleep(Duration::from_millis(100)).await;
        controller.set_suggestion(Some("second"));
        sleep(Duration::from_millis(220)).await;
        assert!(states.lock().unwrap().is_empty());
        sleep(Duration::from_millis(110)).await;

        let states = states.lock().unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].suggestion.as_deref(), Some("second"));
        assert!(states[0].is_visible);
        assert!(states[0].shown_at > 0);
        drop(states);
        controller.cleanup();
    }

    #[tokio::test]
    async fn clear_and_disabled_set_match_source_edge_cases() {
        let (on_state_change, states) = state_collector();
        let controller = FollowupController::new(
            FollowupControllerOptions::new(on_state_change).with_enabled(false),
        )
        .unwrap();

        controller.set_suggestion(Some(""));
        assert_eq!(
            states.lock().unwrap().as_slice(),
            &[FollowupState::initial()]
        );
        controller.set_suggestion(Some("disabled"));
        assert_eq!(states.lock().unwrap().len(), 1);
        controller.cleanup();
    }

    #[tokio::test]
    async fn accepts_fallback_text_reports_source_and_invokes_latest_callback() {
        let (on_state_change, states) = state_collector();
        let (on_outcome, outcomes) = outcome_collector();
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(Mutex::new(None::<AcceptCallback>));
        let active_getter = Arc::clone(&active);
        let getter: GetAcceptCallback = Arc::new(move || active_getter.lock().unwrap().clone());
        *active.lock().unwrap() = {
            let calls = Arc::clone(&first_calls);
            Some(Arc::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            }) as AcceptCallback)
        };
        let controller = FollowupController::new(
            FollowupControllerOptions::new(on_state_change)
                .with_get_on_accept(getter)
                .with_on_outcome(on_outcome),
        )
        .unwrap();
        *active.lock().unwrap() = {
            let calls = Arc::clone(&second_calls);
            Some(Arc::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            }) as AcceptCallback)
        };

        controller.accept(
            Some(AcceptMethod::Right),
            AcceptOptions {
                skip_on_accept: false,
                fallback_text: Some("🎸".to_owned()),
            },
        );
        assert_eq!(
            outcomes.lock().unwrap().as_slice(),
            &[FollowupOutcome {
                outcome: FollowupOutcomeKind::Accepted,
                accept_method: Some(AcceptMethod::Right),
                accept_source: Some(AcceptSource::Fallback),
                time_ms: 0,
                suggestion_length: 2,
            }]
        );
        assert_eq!(
            states.lock().unwrap().as_slice(),
            &[FollowupState::initial()]
        );
        tokio::task::yield_now().await;
        assert_eq!(first_calls.load(Ordering::SeqCst), 0);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
        controller.cleanup();
    }

    #[tokio::test]
    async fn live_text_wins_over_fallback_and_dismiss_emits_ignored_once() {
        let (on_state_change, _) = state_collector();
        let (on_outcome, outcomes) = outcome_collector();
        let controller = FollowupController::new(
            FollowupControllerOptions::new(on_state_change).with_on_outcome(on_outcome),
        )
        .unwrap();
        controller.set_suggestion(Some("live 🎸"));
        sleep(Duration::from_millis(SUGGESTION_DELAY_MS + 40)).await;
        controller.accept(
            Some(AcceptMethod::Tab),
            AcceptOptions {
                skip_on_accept: true,
                fallback_text: Some("ignored fallback".to_owned()),
            },
        );
        let accepted = outcomes.lock().unwrap()[0].clone();
        assert_eq!(accepted.accept_source, Some(AcceptSource::Live));
        assert_eq!(accepted.suggestion_length, js_string_length("live 🎸"));
        assert_eq!(accepted.accept_method, Some(AcceptMethod::Tab));

        // clear resets the debounce lock so a new visible suggestion can be
        // exercised synchronously after its delayed show.
        controller.clear();
        controller.set_suggestion(Some("ignored"));
        sleep(Duration::from_millis(SUGGESTION_DELAY_MS + 40)).await;
        controller.dismiss();
        controller.dismiss();
        let outcomes = outcomes.lock().unwrap();
        assert_eq!(outcomes.len(), 2);
        assert_eq!(outcomes[1].outcome, FollowupOutcomeKind::Ignored);
        assert_eq!(outcomes[1].suggestion_length, 7);
        controller.cleanup();
    }

    #[tokio::test]
    async fn empty_accept_is_noop_and_callback_panics_do_not_break_clear() {
        let (on_state_change, states) = state_collector();
        let on_outcome: OutcomeCallback = Arc::new(|_| panic!("telemetry failure"));
        let accept_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&accept_calls);
        let on_accept: AcceptCallback = Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            panic!("accept failure");
        });
        let getter: GetAcceptCallback = Arc::new(move || Some(Arc::clone(&on_accept)));
        let controller = FollowupController::new(
            FollowupControllerOptions::new(on_state_change)
                .with_get_on_accept(getter)
                .with_on_outcome(on_outcome),
        )
        .unwrap();

        controller.accept(None, AcceptOptions::default());
        assert!(states.lock().unwrap().is_empty());
        controller.accept(
            None,
            AcceptOptions {
                skip_on_accept: false,
                fallback_text: Some("fallback".to_owned()),
            },
        );
        tokio::task::yield_now().await;
        assert_eq!(accept_calls.load(Ordering::SeqCst), 1);
        controller.clear();
        assert_eq!(controller.state(), FollowupState::initial());
        assert_eq!(
            states.lock().unwrap().as_slice(),
            &[FollowupState::initial(), FollowupState::initial()]
        );
        controller.cleanup();
    }

    #[tokio::test]
    async fn accept_debounce_blocks_rapid_repeats_then_releases() {
        let (on_state_change, _) = state_collector();
        let (on_outcome, outcomes) = outcome_collector();
        let controller = FollowupController::new(
            FollowupControllerOptions::new(on_state_change).with_on_outcome(on_outcome),
        )
        .unwrap();
        let fallback = || AcceptOptions {
            skip_on_accept: true,
            fallback_text: Some("next".to_owned()),
        };

        controller.accept(None, fallback());
        controller.accept(None, fallback());
        assert_eq!(outcomes.lock().unwrap().len(), 1);

        sleep(Duration::from_millis(ACCEPT_DEBOUNCE_MS + 30)).await;
        controller.accept(None, fallback());
        assert_eq!(outcomes.lock().unwrap().len(), 2);
        controller.cleanup();
    }

    #[tokio::test]
    async fn cleanup_cancels_pending_show_without_emitting_state() {
        let (on_state_change, states) = state_collector();
        let controller =
            FollowupController::new(FollowupControllerOptions::new(on_state_change)).unwrap();
        controller.set_suggestion(Some("pending"));
        sleep(Duration::from_millis(30)).await;
        controller.cleanup();
        sleep(Duration::from_millis(SUGGESTION_DELAY_MS + 30)).await;
        assert!(states.lock().unwrap().is_empty());
    }
}
