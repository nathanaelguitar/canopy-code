use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

/// Why a cancellation token was cancelled.
///
/// JavaScript permits an arbitrary value as an abort reason. Rust callers use
/// an optional, typed reason instead; an omitted reason remains `None`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CancellationReason {
    Explicit(Arc<str>),
    /// Matches the source helper's `TimeoutError` reason.
    Timeout,
}

impl From<String> for CancellationReason {
    fn from(value: String) -> Self {
        Self::Explicit(Arc::from(value))
    }
}

impl From<&str> for CancellationReason {
    fn from(value: &str) -> Self {
        Self::Explicit(Arc::from(value))
    }
}

impl From<Arc<str>> for CancellationReason {
    fn from(value: Arc<str>) -> Self {
        Self::Explicit(value)
    }
}

#[derive(Default)]
struct StateData {
    cancelled: bool,
    reason: Option<CancellationReason>,
    parents: Vec<ParentLink>,
    children: HashMap<u64, Weak<State>>,
    cleanup_hooks: Vec<Weak<CleanupInner>>,
    wakers: HashMap<u64, Waker>,
    next_id: u64,
}

struct State {
    data: Mutex<StateData>,
}

struct ParentLink {
    parent: Weak<State>,
    child_id: u64,
}

impl State {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            data: Mutex::new(StateData::default()),
        })
    }

    fn cancel(state: &Arc<Self>, reason: Option<CancellationReason>) -> bool {
        let (parents, children, cleanups, wakers, reason) = {
            let mut data = state.data.lock().unwrap_or_else(|e| e.into_inner());
            if data.cancelled {
                return false;
            }
            data.cancelled = true;
            data.reason = reason;
            let reason = data.reason.clone();
            (
                std::mem::take(&mut data.parents),
                std::mem::take(&mut data.children),
                std::mem::take(&mut data.cleanup_hooks),
                std::mem::take(&mut data.wakers),
                reason,
            )
        };

        for link in parents {
            remove_child_link(&link.parent, link.child_id);
        }
        for cleanup in cleanups.into_iter().filter_map(|weak| weak.upgrade()) {
            cleanup.run();
        }
        for child in children.into_values().filter_map(|weak| weak.upgrade()) {
            Self::cancel(&child, reason.clone());
        }
        for waker in wakers.into_values() {
            waker.wake();
        }
        true
    }
}

impl Drop for State {
    fn drop(&mut self) {
        let data = self.data.get_mut().unwrap_or_else(|e| e.into_inner());
        for link in data.parents.drain(..) {
            remove_child_link(&link.parent, link.child_id);
        }
    }
}

fn next_id(data: &mut StateData) -> u64 {
    let id = data.next_id;
    data.next_id = data.next_id.wrapping_add(1);
    id
}

fn remove_child_link(parent: &Weak<State>, child_id: u64) {
    if let Some(parent) = parent.upgrade() {
        parent
            .data
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .children
            .remove(&child_id);
    }
}

/// A clonable, thread-safe cancellation token.
///
/// Child tokens observe parent cancellation but never cancel their parents.
/// Parent registrations are weak and are removed when a child is cancelled or
/// dropped. Unlike JavaScript abort events, Rust callers poll [`is_cancelled`]
/// or await [`cancelled`]; there is no event-listener ordering or listener cap.
/// Keep both parent and child tokens alive while propagation is needed. When
/// independent threads cancel competing sources, the first cancellation to
/// reach the token supplies its reason.
#[derive(Clone)]
pub struct CancellationToken {
    state: Arc<State>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            state: State::new(),
        }
    }

    /// Create a child which is cancelled with this token's reason.
    pub fn child(&self) -> Self {
        let child = Self::new();
        add_parent(&self.state, &child.state);
        child
    }

    /// Whether this token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.state
            .data
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cancelled
    }

    /// The first cancellation reason, if one was supplied.
    pub fn reason(&self) -> Option<CancellationReason> {
        self.state
            .data
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reason
            .clone()
    }

    /// Cancel once without attaching a reason.
    pub fn cancel(&self) -> bool {
        State::cancel(&self.state, None)
    }

    /// Cancel once with an explicit reason.
    pub fn cancel_with_reason(&self, reason: impl Into<CancellationReason>) -> bool {
        State::cancel(&self.state, Some(reason.into()))
    }

    /// A future that resolves with the reason once this token is cancelled.
    pub fn cancelled(&self) -> Cancelled {
        Cancelled {
            state: Arc::clone(&self.state),
            waiter_id: None,
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Future returned by [`CancellationToken::cancelled`].
pub struct Cancelled {
    state: Arc<State>,
    waiter_id: Option<u64>,
}

impl Future for Cancelled {
    type Output = Option<CancellationReason>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut data = this.state.data.lock().unwrap_or_else(|e| e.into_inner());
        if data.cancelled {
            if let Some(id) = this.waiter_id.take() {
                data.wakers.remove(&id);
            }
            return Poll::Ready(data.reason.clone());
        }

        let id = *this.waiter_id.get_or_insert_with(|| next_id(&mut data));
        match data.wakers.get(&id) {
            Some(old) if old.will_wake(cx.waker()) => {}
            _ => {
                data.wakers.insert(id, cx.waker().clone());
            }
        }
        Poll::Pending
    }
}

impl Drop for Cancelled {
    fn drop(&mut self) {
        if let Some(id) = self.waiter_id {
            self.state
                .data
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .wakers
                .remove(&id);
        }
    }
}

/// Create a child token, or an independent token when `parent` is `None`.
pub fn create_child_cancellation_token(parent: Option<&CancellationToken>) -> CancellationToken {
    parent.map_or_else(CancellationToken::new, CancellationToken::child)
}

fn add_parent(parent: &Arc<State>, child: &Arc<State>) {
    let mut parent_data = parent.data.lock().unwrap_or_else(|e| e.into_inner());
    parent_data
        .children
        .retain(|_, weak_child| weak_child.strong_count() != 0);

    if parent_data.cancelled {
        let reason = parent_data.reason.clone();
        drop(parent_data);
        State::cancel(child, reason);
        return;
    }

    let mut child_data = child.data.lock().unwrap_or_else(|e| e.into_inner());
    if child_data.cancelled {
        return;
    }

    let child_id = next_id(&mut parent_data);
    parent_data.children.insert(child_id, Arc::downgrade(child));
    child_data.parents.push(ParentLink {
        parent: Arc::downgrade(parent),
        child_id,
    });
}

/// A cancellation token plus the registrations and timeout owned by a
/// combined operation. Dropping its [`CancellationCleanup`] also cleans up.
pub struct CombinedCancellation {
    pub token: CancellationToken,
    pub cleanup: CancellationCleanup,
}

/// Idempotently detach source tokens and stop a pending timeout.
pub struct CancellationCleanup {
    inner: Arc<CleanupInner>,
}

struct CleanupInner {
    done: AtomicBool,
    token: Weak<State>,
    timer: Mutex<Option<Arc<TimeoutControl>>>,
}

impl CleanupInner {
    fn run(&self) {
        if self.done.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(token) = self.token.upgrade() {
            detach_parents(&token);
        }
        if let Some(timer) = self.timer.lock().unwrap_or_else(|e| e.into_inner()).take() {
            timer.stop();
        }
    }

    fn install_timer(&self, timer: Arc<TimeoutControl>) -> bool {
        let mut current = self.timer.lock().unwrap_or_else(|e| e.into_inner());
        if self.done.load(Ordering::Acquire) {
            timer.stop();
            return false;
        }
        *current = Some(timer);
        true
    }
}

impl CancellationCleanup {
    pub fn cleanup(&self) {
        self.inner.run();
    }
}

impl Drop for CancellationCleanup {
    fn drop(&mut self) {
        self.inner.run();
    }
}

fn detach_parents(state: &Arc<State>) {
    let parents = {
        let mut data = state.data.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut data.parents)
    };
    for link in parents {
        remove_child_link(&link.parent, link.child_id);
    }
}

const TIMER_PENDING: u8 = 0;
const TIMER_STOPPED: u8 = 1;
const TIMER_FIRED: u8 = 2;

struct TimeoutControl {
    state: AtomicU8,
    gate: Mutex<()>,
    wake: Condvar,
}

impl TimeoutControl {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicU8::new(TIMER_PENDING),
            gate: Mutex::new(()),
            wake: Condvar::new(),
        })
    }

    fn stop(&self) {
        let _ = self.state.compare_exchange(
            TIMER_PENDING,
            TIMER_STOPPED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.wake.notify_all();
    }

    fn start(self: &Arc<Self>, delay: Duration, token: Weak<State>) {
        let control = Arc::clone(self);
        thread::spawn(move || {
            let guard = control.gate.lock().unwrap_or_else(|e| e.into_inner());
            let (_guard, result) = control
                .wake
                .wait_timeout_while(guard, delay, |_| {
                    control.state.load(Ordering::Acquire) == TIMER_PENDING
                })
                .unwrap_or_else(|e| e.into_inner());
            if result.timed_out()
                && control
                    .state
                    .compare_exchange(
                        TIMER_PENDING,
                        TIMER_FIRED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                if let Some(token) = token.upgrade() {
                    State::cancel(&token, Some(CancellationReason::Timeout));
                }
            }
        });
    }
}

/// Combine source tokens and an optional timeout. `None` entries are ignored;
/// a zero timeout is disabled. The timer uses a small sleeping OS thread so it
/// also works without a Tokio runtime. Cancellation and explicit cleanup stop
/// it promptly and remove all source registrations.
pub fn combine_cancellation_tokens<'a>(
    inputs: impl IntoIterator<Item = Option<&'a CancellationToken>>,
    timeout: Option<Duration>,
) -> CombinedCancellation {
    let token = CancellationToken::new();
    let cleanup = CancellationCleanup {
        inner: Arc::new(CleanupInner {
            done: AtomicBool::new(false),
            token: Arc::downgrade(&token.state),
            timer: Mutex::new(None),
        }),
    };
    token
        .state
        .data
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .cleanup_hooks
        .push(Arc::downgrade(&cleanup.inner));

    for input in inputs.into_iter().flatten() {
        add_parent(&input.state, &token.state);
        if token.is_cancelled() {
            break;
        }
    }

    if !token.is_cancelled() {
        if let Some(delay) = timeout.filter(|delay| !delay.is_zero()) {
            let control = TimeoutControl::new();
            if cleanup.inner.install_timer(Arc::clone(&control)) {
                control.start(delay, Arc::downgrade(&token.state));
            }
        }
    }

    CombinedCancellation { token, cleanup }
}

#[cfg(test)]
mod tests {
    use super::{
        CancellationReason, CancellationToken, combine_cancellation_tokens,
        create_child_cancellation_token,
    };
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    #[test]
    fn token_cancellation_is_idempotent_and_keeps_first_reason() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        assert!(token.cancel_with_reason("first"));
        assert!(!token.cancel_with_reason("second"));
        assert_eq!(token.reason(), Some(CancellationReason::from("first")));
    }

    #[test]
    fn child_inherits_parent_abort_reason_without_cancelling_parent() {
        let parent = CancellationToken::new();
        let child = parent.child();
        assert!(parent.cancel_with_reason("parent-reason"));
        assert!(child.is_cancelled());
        assert_eq!(
            child.reason(),
            Some(CancellationReason::from("parent-reason"))
        );
    }

    #[test]
    fn cancelling_child_does_not_cancel_parent_and_unregisters_it() {
        let parent = CancellationToken::new();
        let child = parent.child();
        assert_eq!(parent.state.data.lock().unwrap().children.len(), 1);
        assert!(child.cancel_with_reason("child-reason"));
        assert!(!parent.is_cancelled());
        assert_eq!(parent.state.data.lock().unwrap().children.len(), 0);
    }

    #[test]
    fn already_cancelled_parent_cancels_child_without_registration() {
        let parent = CancellationToken::new();
        parent.cancel_with_reason("pre-cancelled");
        let child = parent.child();
        assert!(child.is_cancelled());
        assert_eq!(
            child.reason(),
            Some(CancellationReason::from("pre-cancelled"))
        );
        assert!(parent.state.data.lock().unwrap().children.is_empty());
    }

    #[test]
    fn optional_parent_creates_an_independent_token_when_absent() {
        let child = create_child_cancellation_token(None);
        assert!(!child.is_cancelled());
        child.cancel();
        assert!(child.is_cancelled());
    }

    #[test]
    fn many_short_lived_children_do_not_accumulate_parent_registrations() {
        let parent = CancellationToken::new();
        for _ in 0..1_000 {
            parent.child().cancel();
        }
        assert!(parent.state.data.lock().unwrap().children.is_empty());
    }

    #[test]
    fn dropping_child_unregisters_it_from_parent() {
        let parent = CancellationToken::new();
        {
            let _child = parent.child();
            assert_eq!(parent.state.data.lock().unwrap().children.len(), 1);
        }
        assert!(parent.state.data.lock().unwrap().children.is_empty());
    }

    #[test]
    fn combined_token_cancels_from_any_input_and_cleans_the_others() {
        let first = CancellationToken::new();
        let second = CancellationToken::new();
        let combined = combine_cancellation_tokens([Some(&first), Some(&second)], None);
        assert_eq!(first.state.data.lock().unwrap().children.len(), 1);
        assert_eq!(second.state.data.lock().unwrap().children.len(), 1);
        first.cancel_with_reason("from-first");
        assert!(combined.token.is_cancelled());
        assert_eq!(
            combined.token.reason(),
            Some(CancellationReason::from("from-first"))
        );
        assert!(first.state.data.lock().unwrap().children.is_empty());
        assert!(second.state.data.lock().unwrap().children.is_empty());
    }

    #[test]
    fn combined_token_handles_pre_cancelled_and_missing_inputs() {
        let aborted = CancellationToken::new();
        aborted.cancel_with_reason("pre");
        let other = CancellationToken::new();
        let combined = combine_cancellation_tokens([None, Some(&aborted), Some(&other)], None);
        assert!(combined.token.is_cancelled());
        assert_eq!(
            combined.token.reason(),
            Some(CancellationReason::from("pre"))
        );
        assert!(other.state.data.lock().unwrap().children.is_empty());
    }

    #[test]
    fn explicit_cleanup_is_idempotent_and_keeps_combined_token_live() {
        let source = CancellationToken::new();
        let combined = combine_cancellation_tokens([Some(&source)], None);
        combined.cleanup.cleanup();
        combined.cleanup.cleanup();
        assert!(!combined.token.is_cancelled());
        assert!(source.state.data.lock().unwrap().children.is_empty());
    }

    #[test]
    fn timeout_cancels_and_auto_cleans_source_links() {
        let source = CancellationToken::new();
        let combined =
            combine_cancellation_tokens([Some(&source)], Some(Duration::from_millis(20)));
        std::thread::sleep(Duration::from_millis(100));
        assert!(combined.token.is_cancelled());
        assert_eq!(combined.token.reason(), Some(CancellationReason::Timeout));
        assert!(source.state.data.lock().unwrap().children.is_empty());
    }

    #[test]
    fn cleanup_stops_pending_timeout() {
        let combined =
            combine_cancellation_tokens(std::iter::empty(), Some(Duration::from_millis(40)));
        combined.cleanup.cleanup();
        std::thread::sleep(Duration::from_millis(80));
        assert!(!combined.token.is_cancelled());
    }

    #[test]
    fn zero_timeout_is_disabled() {
        let combined = combine_cancellation_tokens(std::iter::empty(), Some(Duration::ZERO));
        std::thread::sleep(Duration::from_millis(5));
        assert!(!combined.token.is_cancelled());
        combined.cleanup.cleanup();
    }

    #[test]
    fn cancellation_future_observes_cancellation_and_reason() {
        let token = CancellationToken::new();
        let mut future = Box::pin(token.cancelled());
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
        token.cancel_with_reason("done");
        assert_eq!(
            future.as_mut().poll(&mut cx),
            Poll::Ready(Some(CancellationReason::from("done")))
        );
    }

    #[test]
    fn dropped_cancellation_future_unregisters_its_waker() {
        let token = CancellationToken::new();
        let mut future = Box::pin(token.cancelled());
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(token.state.data.lock().unwrap().wakers.len(), 1);
        drop(future);
        assert!(token.state.data.lock().unwrap().wakers.is_empty());
    }

    #[test]
    fn dropping_combined_operation_cleans_up_while_token_clone_survives() {
        let source = CancellationToken::new();
        let token = {
            let combined = combine_cancellation_tokens([Some(&source)], None);
            combined.token.clone()
        };
        assert!(!token.is_cancelled());
        assert!(source.state.data.lock().unwrap().children.is_empty());
        source.cancel();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn reason_is_shared_cheaply_across_child_tokens() {
        let parent = CancellationToken::new();
        let child = parent.child();
        parent.cancel_with_reason(Arc::<str>::from("shared"));
        assert_eq!(
            child.reason(),
            Some(CancellationReason::Explicit(Arc::from("shared")))
        );
    }
}
