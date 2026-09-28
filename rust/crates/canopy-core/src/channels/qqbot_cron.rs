//! QQ Bot cron and non-prompt text-chunk buffering.
//!
//! This module extracts `QQChannel.handleCronTextChunk` / `runCronFlow`.
//! Channel readiness, prompt stream ownership, route lookup, sending, and
//! logging are supplied by [`QqbotCronHooks`]; no bridge or persistence state
//! is owned here.

use crate::channels::sanitize::sanitize_log_text;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::task::JoinHandle;

pub const MAX_BUFFER_LENGTH: usize = 4096;
pub const INITIAL_FLUSH_DELAY: Duration = Duration::from_secs(2);
pub const IMMEDIATE_FLUSH_DELAY: Duration = Duration::ZERO;
pub const FIRST_RETRY_DELAY: Duration = Duration::from_secs(5);
pub const SECOND_RETRY_DELAY: Duration = Duration::from_secs(10);

pub type CronFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CronSendErrorCode {
    RateLimited,
    RetryExhausted,
    ActiveMsgDisabled,
    FallbackFailed,
}

impl CronSendErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RateLimited => "RATE_LIMITED",
            Self::RetryExhausted => "RETRY_EXHAUSTED",
            Self::ActiveMsgDisabled => "ACTIVE_MSG_DISABLED",
            Self::FallbackFailed => "FALLBACK_FAILED",
        }
    }

    fn is_permanent(self) -> bool {
        matches!(
            self,
            Self::RetryExhausted | Self::ActiveMsgDisabled | Self::FallbackFailed
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CronSendError {
    /// API/transport errors have no delivery code and follow the transient path.
    pub code: Option<CronSendErrorCode>,
    pub message: String,
}

impl CronSendError {
    pub fn delivery(code: CronSendErrorCode, message: impl Into<String>) -> Self {
        Self {
            code: Some(code),
            message: message.into(),
        }
    }

    pub fn transport(message: impl Into<String>) -> Self {
        Self {
            code: None,
            message: message.into(),
        }
    }
}

/// Dependencies supplied by the QQ channel adapter.
pub trait QqbotCronHooks: Send + Sync + 'static {
    fn is_ready(&self) -> bool;
    fn stream_state_is_active(&self, session_id: &str) -> bool;
    fn target_for_session(&self, session_id: &str) -> Option<String>;
    fn send_message<'a>(
        &'a self,
        chat_id: &'a str,
        text: &'a str,
    ) -> CronFuture<'a, Result<(), CronSendError>>;

    fn log(&self, message: &str) {
        eprintln!("{message}");
    }
}

/// Injectable timer seam. The production implementation uses Tokio timers;
/// tests can record and manually release delays without waiting in real time.
pub trait CronSleeper: Send + Sync + 'static {
    fn sleep(&self, duration: Duration) -> CronFuture<'_, ()>;
}

#[derive(Default)]
pub struct TokioCronSleeper;

impl CronSleeper for TokioCronSleeper {
    fn sleep(&self, duration: Duration) -> CronFuture<'_, ()> {
        Box::pin(tokio::time::sleep(duration))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CronChunkDisposition {
    Buffered { flush_immediately: bool },
    DroppedNotReady,
    DroppedOutsideCronFlow,
    DroppedForActiveStream,
    DroppedAfterDisconnect,
}

#[derive(Clone)]
pub struct QqbotCronBuffer {
    inner: Arc<Inner>,
}

struct Inner {
    hooks: Arc<dyn QqbotCronHooks>,
    sleeper: Arc<dyn CronSleeper>,
    flush_limit: usize,
    flow_depth: AtomicUsize,
    disconnected: AtomicBool,
    entries: Mutex<HashMap<String, Arc<Mutex<CronEntry>>>>,
}

struct CronEntry {
    buffer: String,
    pending_retry: Option<String>,
    retry_count: u8,
    timer_generation: u64,
    timer: Option<JoinHandle<()>>,
}

enum TimerStage {
    Initial,
    RetryOne(String),
    RetryTwo(String),
}

impl QqbotCronBuffer {
    pub fn new(hooks: Arc<dyn QqbotCronHooks>, configured_flush_length: Option<usize>) -> Self {
        Self::with_sleeper(hooks, Arc::new(TokioCronSleeper), configured_flush_length)
    }

    pub fn with_sleeper(
        hooks: Arc<dyn QqbotCronHooks>,
        sleeper: Arc<dyn CronSleeper>,
        configured_flush_length: Option<usize>,
    ) -> Self {
        let flush_limit = configured_flush_length
            .filter(|length| *length > 0 && *length <= MAX_BUFFER_LENGTH)
            .unwrap_or(MAX_BUFFER_LENGTH);
        Self {
            inner: Arc::new(Inner {
                hooks,
                sleeper,
                flush_limit,
                flow_depth: AtomicUsize::new(0),
                disconnected: AtomicBool::new(false),
                entries: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Run a cron callback while marking its emitted text chunks as cron
    /// output. The depth increments when this method is called and is released
    /// when the returned future completes, errors, or is dropped.
    pub fn with_cron_flow<F, Fut, T>(&self, callback: F) -> impl Future<Output = T> + Send + 'static
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.inner.flow_depth.fetch_add(1, Ordering::AcqRel);
        let guard = FlowGuard {
            inner: Arc::clone(&self.inner),
        };
        async move {
            let _guard = guard;
            callback().await
        }
    }

    pub fn cron_flow_depth(&self) -> usize {
        self.inner.flow_depth.load(Ordering::Acquire)
    }

    /// Capture the cron-flow gate synchronously, then defer buffer mutation by
    /// one Tokio scheduler turn, matching the source's `setImmediate`.
    pub fn handle_text_chunk(
        &self,
        session_id: impl Into<String>,
        text: impl Into<String>,
    ) -> impl Future<Output = CronChunkDisposition> + Send + 'static {
        let was_in_cron_flow = self.inner.flow_depth.load(Ordering::Acquire) > 0;
        let inner = Arc::clone(&self.inner);
        let session_id = session_id.into();
        let text = text.into();
        async move {
            tokio::task::yield_now().await;
            if !inner.hooks.is_ready() {
                inner.hooks.log(&format!(
                    "[QQ] Cron text chunk dropped (not ready): {} for session {}",
                    sanitize_log_text(&text, 64),
                    sanitize_log_text(&session_id, 32),
                ));
                return CronChunkDisposition::DroppedNotReady;
            }
            if !was_in_cron_flow {
                return CronChunkDisposition::DroppedOutsideCronFlow;
            }
            if inner.hooks.stream_state_is_active(&session_id) {
                return CronChunkDisposition::DroppedForActiveStream;
            }
            if inner.disconnected.load(Ordering::Acquire) {
                return CronChunkDisposition::DroppedAfterDisconnect;
            }

            let flush_immediately = {
                let mut entries = lock(&inner.entries);
                if inner.disconnected.load(Ordering::Acquire) {
                    return CronChunkDisposition::DroppedAfterDisconnect;
                }
                let entry = entries
                    .entry(session_id.clone())
                    .or_insert_with(|| {
                        Arc::new(Mutex::new(CronEntry {
                            buffer: String::new(),
                            pending_retry: None,
                            retry_count: 0,
                            timer_generation: 0,
                            timer: None,
                        }))
                    })
                    .clone();
                let mut state = lock(&entry);
                if let Some(timer) = state.timer.take() {
                    timer.abort();
                    if let Some(pending_retry) = state.pending_retry.take() {
                        state.buffer.insert_str(0, &pending_retry);
                    }
                }
                state.buffer.push_str(&text);
                let flush_immediately = utf16_length(&state.buffer) >= inner.flush_limit;
                schedule_timer_state_locked(
                    &inner,
                    &session_id,
                    &entry,
                    &mut state,
                    if flush_immediately {
                        IMMEDIATE_FLUSH_DELAY
                    } else {
                        INITIAL_FLUSH_DELAY
                    },
                    TimerStage::Initial,
                );
                flush_immediately
            };
            CronChunkDisposition::Buffered { flush_immediately }
        }
    }

    /// Cancel all queued timers, discard buffered cron messages, and reset the
    /// flow depth during channel disconnect. Returns the number of entries
    /// containing unsent buffered text.
    pub fn disconnect(&self) -> usize {
        self.inner.disconnected.store(true, Ordering::Release);
        self.inner.flow_depth.store(0, Ordering::Release);
        let entries = std::mem::take(&mut *lock(&self.inner.entries));
        let mut dropped_count = 0;
        for entry in entries.into_values() {
            let mut state = lock(&entry);
            if let Some(timer) = state.timer.take() {
                timer.abort();
            }
            if !state.buffer.is_empty() {
                dropped_count += 1;
            }
        }
        if dropped_count > 0 {
            self.inner.hooks.log(&format!(
                "[QQ] Disconnect: discarding {dropped_count} buffered cron message(s)"
            ));
        }
        dropped_count
    }

    pub fn buffered_session_count(&self) -> usize {
        lock(&self.inner.entries).len()
    }
}

struct FlowGuard {
    inner: Arc<Inner>,
}

impl Drop for FlowGuard {
    fn drop(&mut self) {
        let _ = self
            .inner
            .flow_depth
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |depth| {
                depth.checked_sub(1)
            });
    }
}

fn schedule_timer(
    inner: &Arc<Inner>,
    session_id: &str,
    entry: &Arc<Mutex<CronEntry>>,
    delay: Duration,
    stage: TimerStage,
) {
    let entries = lock(&inner.entries);
    schedule_timer_locked(inner, &entries, session_id, entry, delay, stage);
}

fn schedule_timer_locked(
    inner: &Arc<Inner>,
    entries: &HashMap<String, Arc<Mutex<CronEntry>>>,
    session_id: &str,
    entry: &Arc<Mutex<CronEntry>>,
    delay: Duration,
    stage: TimerStage,
) {
    if inner.disconnected.load(Ordering::Acquire)
        || !entries
            .get(session_id)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
    {
        return;
    }
    let mut state = lock(entry);
    schedule_timer_state_locked(inner, session_id, entry, &mut state, delay, stage);
}

fn schedule_timer_state_locked(
    inner: &Arc<Inner>,
    session_id: &str,
    entry: &Arc<Mutex<CronEntry>>,
    state: &mut CronEntry,
    delay: Duration,
    stage: TimerStage,
) {
    if let Some(timer) = state.timer.take() {
        timer.abort();
    }
    state.timer_generation = state.timer_generation.wrapping_add(1);
    let generation = state.timer_generation;
    let weak_inner = Arc::downgrade(inner);
    let weak_entry = Arc::downgrade(entry);
    let session_id = session_id.to_owned();
    let sleeper = Arc::clone(&inner.sleeper);
    state.timer = Some(tokio::spawn(async move {
        sleeper.sleep(delay).await;
        let (Some(inner), Some(entry)) = (weak_inner.upgrade(), weak_entry.upgrade()) else {
            return;
        };
        {
            let mut state = lock(&entry);
            if state.timer_generation != generation {
                return;
            }
            // Drop this handle without aborting the currently running timer
            // task, allowing a concurrent chunk to schedule another timer.
            state.timer = None;
            if matches!(&stage, TimerStage::RetryOne(_) | TimerStage::RetryTwo(_)) {
                state.pending_retry = None;
            }
        }
        match stage {
            TimerStage::Initial => flush_initial(inner, session_id, entry).await,
            TimerStage::RetryOne(to_flush) => {
                retry_flush(inner, session_id, entry, 1, to_flush).await
            }
            TimerStage::RetryTwo(to_flush) => {
                retry_flush(inner, session_id, entry, 2, to_flush).await
            }
        }
    }));
}

async fn flush_initial(inner: Arc<Inner>, session_id: String, entry: Arc<Mutex<CronEntry>>) {
    if inner.disconnected.load(Ordering::Acquire) || !is_current(&inner, &session_id, &entry) {
        return;
    }
    let to_flush = {
        let mut state = lock(&entry);
        std::mem::take(&mut state.buffer)
    };
    if to_flush.is_empty() {
        inner.hooks.log(&format!(
            "[QQ] Cron flush dropped: no target for session {}, lost 0 chars",
            sanitize_log_text(&session_id, 32),
        ));
        remove_if_current(&inner, &session_id, &entry);
        return;
    }
    let Some(chat_id) = inner.hooks.target_for_session(&session_id) else {
        inner.hooks.log(&format!(
            "[QQ] Cron flush dropped: no target for session {}, lost {} chars",
            sanitize_log_text(&session_id, 32),
            utf16_length(&to_flush),
        ));
        remove_if_current(&inner, &session_id, &entry);
        return;
    };

    match inner.hooks.send_message(&chat_id, &to_flush).await {
        Ok(()) => {
            if lock(&entry).buffer.is_empty() {
                remove_if_current(&inner, &session_id, &entry);
            }
        }
        Err(error) => {
            log_send_error(
                &inner,
                "Cron flush send error",
                &session_id,
                &error,
                &to_flush,
            );
            if error.code.is_some_and(CronSendErrorCode::is_permanent) {
                remove_if_current(&inner, &session_id, &entry);
                return;
            }
            {
                let mut state = lock(&entry);
                state.pending_retry = Some(to_flush.clone());
                state.retry_count = 1;
            }
            schedule_timer(
                &inner,
                &session_id,
                &entry,
                FIRST_RETRY_DELAY,
                TimerStage::RetryOne(to_flush),
            );
        }
    }
}

async fn retry_flush(
    inner: Arc<Inner>,
    session_id: String,
    entry: Arc<Mutex<CronEntry>>,
    attempt: u8,
    to_flush: String,
) {
    if inner.disconnected.load(Ordering::Acquire) || !is_current(&inner, &session_id, &entry) {
        return;
    }
    let Some(chat_id) = inner.hooks.target_for_session(&session_id) else {
        inner.hooks.log(&format!(
            "[QQ] Cron flush dropped after retry: no target for session {}",
            sanitize_log_text(&session_id, 32),
        ));
        remove_if_current(&inner, &session_id, &entry);
        return;
    };

    match inner.hooks.send_message(&chat_id, &to_flush).await {
        Ok(()) => {
            if lock(&entry).buffer.is_empty() {
                remove_if_current(&inner, &session_id, &entry);
            }
        }
        Err(error) => {
            let label = if attempt == 1 {
                "Cron flush retry failed"
            } else {
                "Cron flush re-retry failed"
            };
            log_send_error(&inner, label, &session_id, &error, &to_flush);
            if error.code.is_some_and(CronSendErrorCode::is_permanent) {
                if attempt == 2 || lock(&entry).buffer.is_empty() {
                    remove_if_current(&inner, &session_id, &entry);
                }
                return;
            }
            if attempt == 1 {
                {
                    let mut state = lock(&entry);
                    state.retry_count = state.retry_count.saturating_add(1);
                    state.pending_retry = Some(to_flush.clone());
                }
                let delay = if lock(&entry).retry_count == 2 {
                    SECOND_RETRY_DELAY
                } else {
                    FIRST_RETRY_DELAY
                };
                schedule_timer(
                    &inner,
                    &session_id,
                    &entry,
                    delay,
                    TimerStage::RetryTwo(to_flush),
                );
            } else {
                inner.hooks.log(&format!(
                    "[QQ] Cron flush retries exhausted, dropped {} chars for session {}",
                    utf16_length(&to_flush),
                    sanitize_log_text(&session_id, 32),
                ));
                remove_if_current(&inner, &session_id, &entry);
            }
        }
    }
}

fn log_send_error(
    inner: &Inner,
    label: &str,
    session_id: &str,
    error: &CronSendError,
    attempted_text: &str,
) {
    let code = error.code.map(CronSendErrorCode::as_str);
    let code_suffix = code.map(|code| format!(" ({code})")).unwrap_or_default();
    let detail = if label == "Cron flush re-retry failed" {
        format!(
            "{}, toFlush={}, session={}",
            sanitize_log_text(&error.message, 200),
            utf16_length(attempted_text),
            sanitize_log_text(session_id, 32),
        )
    } else {
        sanitize_log_text(&error.message, 200)
    };
    inner
        .hooks
        .log(&format!("[QQ] {label}{code_suffix}: {detail}"));
}

fn is_current(inner: &Inner, session_id: &str, entry: &Arc<Mutex<CronEntry>>) -> bool {
    lock(&inner.entries)
        .get(session_id)
        .is_some_and(|current| Arc::ptr_eq(current, entry))
}

fn remove_if_current(inner: &Inner, session_id: &str, entry: &Arc<Mutex<CronEntry>>) {
    let removed = {
        let mut entries = lock(&inner.entries);
        if entries
            .get(session_id)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            entries.remove(session_id)
        } else {
            None
        }
    };
    if let Some(removed) = removed {
        if let Some(timer) = lock(&removed).timer.take() {
            timer.abort();
        }
    }
}

fn utf16_length(text: &str) -> usize {
    text.encode_utf16().count()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{
        CronChunkDisposition, CronFuture, CronSendError, CronSendErrorCode, CronSleeper,
        FIRST_RETRY_DELAY, IMMEDIATE_FLUSH_DELAY, INITIAL_FLUSH_DELAY, MAX_BUFFER_LENGTH,
        QqbotCronBuffer, QqbotCronHooks, SECOND_RETRY_DELAY, utf16_length,
    };
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};

    struct ScheduledTimer {
        delay: Duration,
        release: oneshot::Sender<()>,
    }

    struct ManualSleeper {
        scheduled: mpsc::UnboundedSender<ScheduledTimer>,
    }

    impl ManualSleeper {
        fn channel() -> (Arc<Self>, mpsc::UnboundedReceiver<ScheduledTimer>) {
            let (scheduled, receiver) = mpsc::unbounded_channel();
            (Arc::new(Self { scheduled }), receiver)
        }
    }

    impl CronSleeper for ManualSleeper {
        fn sleep(&self, delay: Duration) -> CronFuture<'_, ()> {
            let (release, wait) = oneshot::channel();
            let _ = self.scheduled.send(ScheduledTimer { delay, release });
            Box::pin(async move {
                let _ = wait.await;
            })
        }
    }

    struct TestHooks {
        ready: AtomicBool,
        active_streams: Mutex<HashSet<String>>,
        targets: Mutex<HashMap<String, String>>,
        responses: Mutex<VecDeque<Result<(), CronSendError>>>,
        calls: Mutex<Vec<(String, String)>>,
        call_sender: mpsc::UnboundedSender<(String, String)>,
        logs: Mutex<Vec<String>>,
    }

    impl TestHooks {
        fn channel(
            responses: impl IntoIterator<Item = Result<(), CronSendError>>,
        ) -> (Arc<Self>, mpsc::UnboundedReceiver<(String, String)>) {
            let (call_sender, receiver) = mpsc::unbounded_channel();
            (
                Arc::new(Self {
                    ready: AtomicBool::new(true),
                    active_streams: Mutex::new(HashSet::new()),
                    targets: Mutex::new(HashMap::from([("session".to_owned(), "chat".to_owned())])),
                    responses: Mutex::new(responses.into_iter().collect()),
                    calls: Mutex::new(Vec::new()),
                    call_sender,
                    logs: Mutex::new(Vec::new()),
                }),
                receiver,
            )
        }

        fn calls(&self) -> Vec<(String, String)> {
            self.calls.lock().unwrap().clone()
        }

        fn logs(&self) -> Vec<String> {
            self.logs.lock().unwrap().clone()
        }
    }

    impl QqbotCronHooks for TestHooks {
        fn is_ready(&self) -> bool {
            self.ready.load(Ordering::Acquire)
        }

        fn stream_state_is_active(&self, session_id: &str) -> bool {
            self.active_streams.lock().unwrap().contains(session_id)
        }

        fn target_for_session(&self, session_id: &str) -> Option<String> {
            self.targets.lock().unwrap().get(session_id).cloned()
        }

        fn send_message<'a>(
            &'a self,
            chat_id: &'a str,
            text: &'a str,
        ) -> CronFuture<'a, Result<(), CronSendError>> {
            let call = (chat_id.to_owned(), text.to_owned());
            self.calls.lock().unwrap().push(call.clone());
            let _ = self.call_sender.send(call);
            let response = self.responses.lock().unwrap().pop_front().unwrap_or(Ok(()));
            Box::pin(async move { response })
        }

        fn log(&self, message: &str) {
            self.logs.lock().unwrap().push(message.to_owned());
        }
    }

    fn test_buffer(
        hooks: Arc<TestHooks>,
        sleeper: Arc<ManualSleeper>,
        flush_limit: Option<usize>,
    ) -> QqbotCronBuffer {
        QqbotCronBuffer::with_sleeper(hooks, sleeper, flush_limit)
    }

    async fn emit(
        buffer: &QqbotCronBuffer,
        session_id: &'static str,
        text: &'static str,
    ) -> CronChunkDisposition {
        let buffer = buffer.clone();
        let inner_buffer = buffer.clone();
        buffer
            .with_cron_flow(move || async move {
                inner_buffer.handle_text_chunk(session_id, text).await
            })
            .await
    }

    async fn next_timer(
        receiver: &mut mpsc::UnboundedReceiver<ScheduledTimer>,
        expected_delay: Duration,
    ) -> ScheduledTimer {
        let timer = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("timer schedule should arrive")
            .expect("timer channel should remain open");
        assert_eq!(timer.delay, expected_delay);
        timer
    }

    async fn next_call(
        receiver: &mut mpsc::UnboundedReceiver<(String, String)>,
    ) -> (String, String) {
        tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("send attempt should arrive")
            .expect("send channel should remain open")
    }

    async fn wait_for_buffer_count(buffer: &QqbotCronBuffer, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while buffer.buffered_session_count() != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("buffer count should settle");
    }

    fn release(timer: ScheduledTimer) {
        let _ = timer.release.send(());
    }

    #[tokio::test]
    async fn readiness_flow_and_active_stream_gates_prevent_buffer_creation() {
        let (hooks, _calls) = TestHooks::channel([]);
        let (sleeper, mut timers) = ManualSleeper::channel();
        let buffer = test_buffer(Arc::clone(&hooks), sleeper, None);

        hooks.ready.store(false, Ordering::Release);
        assert_eq!(
            emit(&buffer, "session", "not ready").await,
            CronChunkDisposition::DroppedNotReady
        );
        hooks.ready.store(true, Ordering::Release);
        assert_eq!(
            buffer
                .handle_text_chunk("session", "outside cron flow")
                .await,
            CronChunkDisposition::DroppedOutsideCronFlow
        );
        hooks
            .active_streams
            .lock()
            .unwrap()
            .insert("session".to_owned());
        assert_eq!(
            emit(&buffer, "session", "owned by stream").await,
            CronChunkDisposition::DroppedForActiveStream
        );
        assert_eq!(buffer.buffered_session_count(), 0);
        assert!(timers.try_recv().is_err());
    }

    #[tokio::test]
    async fn chunks_coalesce_and_debounce_at_two_seconds() {
        let (hooks, mut calls) = TestHooks::channel([]);
        let (sleeper, mut timers) = ManualSleeper::channel();
        let buffer = test_buffer(hooks, sleeper, None);

        assert_eq!(
            emit(&buffer, "session", "hello ").await,
            CronChunkDisposition::Buffered {
                flush_immediately: false
            }
        );
        let old_timer = next_timer(&mut timers, INITIAL_FLUSH_DELAY).await;
        assert_eq!(
            emit(&buffer, "session", "world").await,
            CronChunkDisposition::Buffered {
                flush_immediately: false
            }
        );
        let current_timer = next_timer(&mut timers, INITIAL_FLUSH_DELAY).await;
        assert!(
            old_timer.release.is_closed(),
            "new chunk cancels the prior timer"
        );
        release(current_timer);

        assert_eq!(
            next_call(&mut calls).await,
            ("chat".to_owned(), "hello world".to_owned())
        );
        wait_for_buffer_count(&buffer, 0).await;
    }

    #[tokio::test]
    async fn buffer_limit_uses_utf16_length_and_schedules_immediately() {
        let (hooks, mut calls) = TestHooks::channel([]);
        let (sleeper, mut timers) = ManualSleeper::channel();
        let buffer = test_buffer(hooks, sleeper, Some(4));
        let text = "😀😀";

        assert_eq!(utf16_length(text), 4);
        assert_eq!(
            emit(&buffer, "session", text).await,
            CronChunkDisposition::Buffered {
                flush_immediately: true
            }
        );
        release(next_timer(&mut timers, IMMEDIATE_FLUSH_DELAY).await);
        assert_eq!(
            next_call(&mut calls).await,
            ("chat".to_owned(), text.to_owned())
        );
        wait_for_buffer_count(&buffer, 0).await;
        assert_eq!(MAX_BUFFER_LENGTH, 4096);
    }

    #[tokio::test]
    async fn retry_flow_uses_five_then_ten_seconds_and_stops_after_three_attempts() {
        let errors = [
            Err(CronSendError::delivery(
                CronSendErrorCode::RateLimited,
                "rate limited",
            )),
            Err(CronSendError::transport("temporary network error")),
            Err(CronSendError::delivery(
                CronSendErrorCode::RateLimited,
                "rate limited again",
            )),
        ];
        let (hooks, mut calls) = TestHooks::channel(errors);
        let (sleeper, mut timers) = ManualSleeper::channel();
        let buffer = test_buffer(Arc::clone(&hooks), sleeper, None);
        let text = "retry!!";

        emit(&buffer, "session", text).await;
        release(next_timer(&mut timers, INITIAL_FLUSH_DELAY).await);
        assert_eq!(
            next_call(&mut calls).await,
            ("chat".to_owned(), text.to_owned())
        );

        release(next_timer(&mut timers, FIRST_RETRY_DELAY).await);
        assert_eq!(
            next_call(&mut calls).await,
            ("chat".to_owned(), text.to_owned())
        );

        release(next_timer(&mut timers, SECOND_RETRY_DELAY).await);
        assert_eq!(
            next_call(&mut calls).await,
            ("chat".to_owned(), text.to_owned())
        );
        wait_for_buffer_count(&buffer, 0).await;

        assert_eq!(hooks.calls().len(), 3);
        assert!(
            hooks
                .logs()
                .iter()
                .any(|message| message.contains("Cron flush re-retry failed")
                    && message.contains("toFlush=7"))
        );
        assert!(
            hooks
                .logs()
                .iter()
                .any(|message| message.contains("Cron flush retries exhausted"))
        );
    }

    #[tokio::test]
    async fn transient_retry_rechecks_route_and_drops_when_target_disappears() {
        let (hooks, mut calls) = TestHooks::channel([Err(CronSendError::delivery(
            CronSendErrorCode::RateLimited,
            "rate limited",
        ))]);
        let (sleeper, mut timers) = ManualSleeper::channel();
        let buffer = test_buffer(Arc::clone(&hooks), sleeper, None);

        emit(&buffer, "session", "route guard").await;
        release(next_timer(&mut timers, INITIAL_FLUSH_DELAY).await);
        assert_eq!(next_call(&mut calls).await.1, "route guard");
        hooks.targets.lock().unwrap().remove("session");

        release(next_timer(&mut timers, FIRST_RETRY_DELAY).await);
        wait_for_buffer_count(&buffer, 0).await;
        assert_eq!(hooks.calls().len(), 1);
        assert!(
            hooks
                .logs()
                .iter()
                .any(|message| message.contains("dropped after retry: no target"))
        );
    }

    #[tokio::test]
    async fn permanent_delivery_codes_clean_up_without_retry() {
        for code in [
            CronSendErrorCode::RetryExhausted,
            CronSendErrorCode::ActiveMsgDisabled,
            CronSendErrorCode::FallbackFailed,
        ] {
            let (hooks, mut calls) = TestHooks::channel([Err(CronSendError::delivery(
                code,
                "permanent delivery error",
            ))]);
            let (sleeper, mut timers) = ManualSleeper::channel();
            let buffer = test_buffer(hooks, sleeper, None);

            emit(&buffer, "session", "terminal").await;
            release(next_timer(&mut timers, INITIAL_FLUSH_DELAY).await);
            assert_eq!(next_call(&mut calls).await.1, "terminal");
            wait_for_buffer_count(&buffer, 0).await;
            assert!(timers.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn initial_flush_drops_text_when_route_has_no_target() {
        let (hooks, mut calls) = TestHooks::channel([]);
        let (sleeper, mut timers) = ManualSleeper::channel();
        let buffer = test_buffer(Arc::clone(&hooks), sleeper, None);
        hooks.targets.lock().unwrap().remove("session");

        emit(&buffer, "session", "orphaned text").await;
        release(next_timer(&mut timers, INITIAL_FLUSH_DELAY).await);
        wait_for_buffer_count(&buffer, 0).await;
        assert!(calls.try_recv().is_err());
        assert!(
            hooks
                .logs()
                .iter()
                .any(|message| message.contains("Cron flush dropped: no target"))
        );
    }

    #[tokio::test]
    async fn empty_chunk_schedules_then_cleans_up_without_sending() {
        let (hooks, mut calls) = TestHooks::channel([]);
        let (sleeper, mut timers) = ManualSleeper::channel();
        let buffer = test_buffer(Arc::clone(&hooks), sleeper, None);

        assert_eq!(
            emit(&buffer, "session", "").await,
            CronChunkDisposition::Buffered {
                flush_immediately: false
            }
        );
        release(next_timer(&mut timers, INITIAL_FLUSH_DELAY).await);
        wait_for_buffer_count(&buffer, 0).await;
        assert!(calls.try_recv().is_err());
        assert!(
            hooks
                .logs()
                .iter()
                .any(|message| message.contains("lost 0 chars"))
        );
    }

    #[tokio::test]
    async fn new_chunk_cancels_pending_retry_and_merges_it_before_the_new_text() {
        let (hooks, mut calls) = TestHooks::channel([
            Err(CronSendError::delivery(
                CronSendErrorCode::RateLimited,
                "rate limited",
            )),
            Ok(()),
        ]);
        let (sleeper, mut timers) = ManualSleeper::channel();
        let buffer = test_buffer(hooks, sleeper, None);

        emit(&buffer, "session", "old").await;
        release(next_timer(&mut timers, INITIAL_FLUSH_DELAY).await);
        assert_eq!(next_call(&mut calls).await.1, "old");
        let retry_timer = next_timer(&mut timers, FIRST_RETRY_DELAY).await;

        emit(&buffer, "session", " new").await;
        tokio::task::yield_now().await;
        assert!(retry_timer.release.is_closed());
        release(next_timer(&mut timers, INITIAL_FLUSH_DELAY).await);
        assert_eq!(
            next_call(&mut calls).await,
            ("chat".to_owned(), "old new".to_owned())
        );
        wait_for_buffer_count(&buffer, 0).await;
        assert_eq!(
            calls.try_recv().unwrap_err(),
            mpsc::error::TryRecvError::Empty
        );
    }

    #[tokio::test]
    async fn disconnect_cancels_timers_discards_buffers_and_resets_flow_depth() {
        let (hooks, mut calls) = TestHooks::channel([]);
        let (sleeper, mut timers) = ManualSleeper::channel();
        let buffer = test_buffer(hooks, sleeper, None);

        emit(&buffer, "session", "pending").await;
        let timer = next_timer(&mut timers, INITIAL_FLUSH_DELAY).await;
        let pending_flow = buffer.with_cron_flow(std::future::pending::<()>);
        assert_eq!(buffer.cron_flow_depth(), 1);
        assert_eq!(buffer.disconnect(), 1);
        assert_eq!(buffer.cron_flow_depth(), 0);
        drop(pending_flow);
        tokio::task::yield_now().await;
        assert!(timer.release.is_closed());
        assert_eq!(buffer.buffered_session_count(), 0);
        assert!(calls.try_recv().is_err());
    }

    #[tokio::test]
    async fn cron_flow_depth_handles_nesting_errors_and_dropped_futures() {
        let (hooks, _calls) = TestHooks::channel([]);
        let (sleeper, _timers) = ManualSleeper::channel();
        let buffer = test_buffer(hooks, sleeper, None);
        let nested = buffer.clone();
        let outer = buffer.with_cron_flow(move || async move {
            assert_eq!(nested.cron_flow_depth(), 1);
            let inner = nested.with_cron_flow(|| async {});
            assert_eq!(nested.cron_flow_depth(), 2);
            inner.await;
            assert_eq!(nested.cron_flow_depth(), 1);
        });
        assert_eq!(buffer.cron_flow_depth(), 1);
        outer.await;
        assert_eq!(buffer.cron_flow_depth(), 0);

        let failed = buffer.with_cron_flow(|| async { Err::<(), _>("cron failed") });
        assert_eq!(failed.await, Err("cron failed"));
        assert_eq!(buffer.cron_flow_depth(), 0);

        let dropped = buffer.with_cron_flow(std::future::pending::<()>);
        assert_eq!(buffer.cron_flow_depth(), 1);
        drop(dropped);
        assert_eq!(buffer.cron_flow_depth(), 0);
    }
}
