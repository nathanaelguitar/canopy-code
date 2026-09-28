//! Progressive multi-message delivery for channel responses.
//!
//! Port of packages/channels/base/src/BlockStreamer.ts. Text is buffered until
//! a preferred boundary, a size limit, an idle timeout, or an explicit flush.
//! Sends run on one Tokio worker so blocks are delivered in order.
//!
//! JavaScript measures string length and slice positions in UTF-16 code units.
//! The port uses UTF-16 for thresholds and boundary selection, then maps split
//! points back to Rust UTF-8 byte offsets. Rust strings cannot contain lone
//! surrogates, so a split limit that lands inside a supplementary character is
//! rounded back to the preceding scalar boundary (or forward by one scalar if
//! that would otherwise make no progress).

use futures_util::FutureExt;
use std::error::Error;
use std::future::Future;
use std::io;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// A send operation's error. Send errors are deliberately swallowed, matching
/// the TypeScript implementation's catch behavior.
pub type BlockSendError = Box<dyn Error + Send + Sync + 'static>;

type SendFuture = Pin<Box<dyn Future<Output = Result<(), BlockSendError>> + Send + 'static>>;
type SendCallback = Arc<dyn Fn(String) -> SendFuture + Send + Sync + 'static>;

/// Timer abstraction used by idle emission. Implementations can provide a
/// controllable clock for deterministic tests.
pub trait BlockStreamerClock: Send + Sync + 'static {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

struct TokioClock;

impl BlockStreamerClock for TokioClock {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

/// Buffering and timer thresholds for a BlockStreamer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockStreamerOptions {
    /// Minimum UTF-16 code units before an idle or paragraph-boundary send.
    pub min_chars: usize,
    /// Force a split when the buffer reaches this many UTF-16 code units.
    pub max_chars: usize,
    /// Emit after this much inactivity. A zero duration disables idle sends.
    pub idle: Duration,
}

impl Default for BlockStreamerOptions {
    fn default() -> Self {
        Self {
            min_chars: 400,
            max_chars: 1_000,
            idle: Duration::from_millis(1_500),
        }
    }
}

enum WorkerMessage {
    Send(String),
    Barrier(oneshot::Sender<()>),
}

struct State {
    buffer: String,
    timer_generation: u64,
    timer_abort: Option<tokio::task::AbortHandle>,
    block_count: u64,
}

struct Inner {
    options: BlockStreamerOptions,
    state: Mutex<State>,
    tx: mpsc::UnboundedSender<WorkerMessage>,
    runtime: Handle,
    clock: Arc<dyn BlockStreamerClock>,
}

/// Accumulates streamed text and emits completed blocks through a serialized
/// asynchronous send callback.
///
/// Construct this while a Tokio runtime is active. push and stop are
/// synchronous like their TypeScript counterparts; flush is async because it
/// waits for every queued send. The saved runtime handle lets later pushes
/// schedule idle timers even when called from a non-runtime thread.
pub struct BlockStreamer {
    inner: Arc<Inner>,
    // Dropping a Tokio JoinHandle detaches the worker. The channel sender in
    // Inner closing then lets it drain its queue and exit.
    _worker: JoinHandle<()>,
}

impl BlockStreamer {
    /// Construct a streamer. max_chars must be positive so forced splitting
    /// always makes progress. A Tokio runtime must be active at construction.
    pub fn new<F, Fut, E>(options: BlockStreamerOptions, send: F) -> io::Result<Self>
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Error + Send + Sync + 'static,
    {
        Self::new_with_clock(options, send, Arc::new(TokioClock))
    }

    /// Construct a streamer with an injected idle timer. This is primarily a
    /// deterministic test seam; production callers should usually use new.
    pub fn new_with_clock<F, Fut, E>(
        options: BlockStreamerOptions,
        send: F,
        clock: Arc<dyn BlockStreamerClock>,
    ) -> io::Result<Self>
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Error + Send + Sync + 'static,
    {
        if options.max_chars == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "BlockStreamer max_chars must be greater than zero",
            ));
        }
        let runtime = Handle::try_current().map_err(|_| {
            io::Error::other("BlockStreamer must be created inside a Tokio runtime")
        })?;

        let send: SendCallback = Arc::new(move |text| {
            let future = send(text);
            Box::pin(async move {
                future
                    .await
                    .map_err(|error| Box::new(error) as BlockSendError)
            })
        });
        let (tx, mut rx) = mpsc::unbounded_channel::<WorkerMessage>();
        let worker_send = send.clone();
        let worker = runtime.spawn(async move {
            while let Some(message) = rx.recv().await {
                match message {
                    WorkerMessage::Send(text) => {
                        // A rejected promise and a synchronous throw from the
                        // JS callback both become a swallowed send failure.
                        if let Ok(future) =
                            std::panic::catch_unwind(AssertUnwindSafe(|| worker_send(text)))
                        {
                            let _ = AssertUnwindSafe(future).catch_unwind().await;
                        }
                    }
                    WorkerMessage::Barrier(done) => {
                        let _ = done.send(());
                    }
                }
            }
        });

        Ok(Self {
            inner: Arc::new(Inner {
                options,
                state: Mutex::new(State {
                    buffer: String::new(),
                    timer_generation: 0,
                    timer_abort: None,
                    block_count: 0,
                }),
                tx,
                runtime,
                clock,
            }),
            _worker: worker,
        })
    }

    /// Feed a new text chunk from the agent stream.
    pub fn push(&self, chunk: &str) {
        let mut state = lock_state(&self.inner);
        if let Some(timer) = state.timer_abort.take() {
            timer.abort();
        }
        state.buffer.push_str(chunk);
        state.timer_generation = state.timer_generation.wrapping_add(1);
        let generation = state.timer_generation;

        check_emit(&self.inner, &mut state);
        if !state.buffer.is_empty() && !self.inner.options.idle.is_zero() {
            let weak = Arc::downgrade(&self.inner);
            let clock = self.inner.clock.clone();
            let idle = self.inner.options.idle;
            let timer = self.inner.runtime.spawn(async move {
                clock.sleep(idle).await;
                fire_idle_timer(weak, generation);
            });
            state.timer_abort = Some(timer.abort_handle());
        }
    }

    /// Flush all remaining buffered text and wait for sends queued before the
    /// flush barrier to finish.
    pub async fn flush(&self) {
        let (done_tx, done_rx) = oneshot::channel();
        {
            let mut state = lock_state(&self.inner);
            if let Some(timer) = state.timer_abort.take() {
                timer.abort();
            }
            state.timer_generation = state.timer_generation.wrapping_add(1);
            if !state.buffer.is_empty() {
                let text = std::mem::take(&mut state.buffer);
                emit_block(&self.inner, &mut state, &text);
            }
            // Queue the barrier while holding the same lock as push/timer
            // emissions so it separates earlier blocks from later pushes.
            let _ = self.inner.tx.send(WorkerMessage::Barrier(done_tx));
        }
        let _ = done_rx.await;
    }

    /// Drop buffered text and invalidate the pending idle timer. Sends already
    /// queued continue in order, matching the TypeScript stop() behavior.
    pub fn stop(&self) {
        let mut state = lock_state(&self.inner);
        if let Some(timer) = state.timer_abort.take() {
            timer.abort();
        }
        state.timer_generation = state.timer_generation.wrapping_add(1);
        state.buffer.clear();
    }

    /// Number of non-empty, trimmed blocks queued so far.
    pub fn block_count(&self) -> u64 {
        lock_state(&self.inner).block_count
    }
}

impl Drop for BlockStreamer {
    fn drop(&mut self) {
        if let Some(timer) = lock_state(&self.inner).timer_abort.take() {
            timer.abort();
        }
    }
}

fn lock_state(inner: &Inner) -> MutexGuard<'_, State> {
    inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn fire_idle_timer(weak: Weak<Inner>, generation: u64) {
    let Some(inner) = weak.upgrade() else {
        return;
    };
    let mut state = lock_state(&inner);
    if state.timer_generation != generation {
        return;
    }
    state.timer_abort = None;
    if utf16_len(&state.buffer) >= inner.options.min_chars {
        let text = std::mem::take(&mut state.buffer);
        emit_block(&inner, &mut state, &text);
    }
}

fn check_emit(inner: &Inner, state: &mut State) {
    while utf16_len(&state.buffer) >= inner.options.max_chars {
        let split = find_break_point(&state.buffer, inner.options.max_chars);
        debug_assert!(split > 0 && split <= state.buffer.len());
        let remainder = state.buffer.split_off(split);
        let text = std::mem::replace(&mut state.buffer, remainder);
        emit_block(inner, state, &text);
    }

    if utf16_len(&state.buffer) >= inner.options.min_chars {
        let split = find_block_boundary(&state.buffer, inner.options.min_chars);
        if split > 0 {
            let remainder = state.buffer.split_off(split);
            let text = std::mem::replace(&mut state.buffer, remainder);
            emit_block(inner, state, &text);
        }
    }
}

fn emit_block(inner: &Inner, state: &mut State, text: &str) {
    let trimmed = js_trim(text);
    if trimmed.is_empty() {
        return;
    }
    state.block_count = state.block_count.saturating_add(1);
    let _ = inner.tx.send(WorkerMessage::Send(trimmed));
}

fn find_block_boundary(text: &str, min_chars: usize) -> usize {
    let Some(index) = text.rfind("\n\n") else {
        return 0;
    };
    if utf16_len(&text[..index]) < min_chars {
        return 0;
    }
    index + 2
}

/// Find the best break at or before max_pos UTF-16 units, preferring a
/// paragraph marker, then newline, then space, and finally the size limit.
fn find_break_point(text: &str, max_pos: usize) -> usize {
    let prefix_end = byte_index_at_utf16(text, max_pos);
    let sub = &text[..prefix_end];
    if let Some(index) = sub.rfind("\n\n") {
        if index > 0 {
            return index + 2;
        }
    }
    if let Some(index) = sub.rfind('\n') {
        if index > 0 {
            return index + 1;
        }
    }
    if let Some(index) = sub.rfind(' ') {
        if index > 0 {
            return index + 1;
        }
    }
    if prefix_end > 0 {
        return prefix_end;
    }
    // max_pos may fall inside the first supplementary character. JS can split
    // its surrogate pair; Rust cannot, so consume that scalar to make progress.
    text.char_indices()
        .next()
        .map(|(index, ch)| index + ch.len_utf8())
        .unwrap_or(0)
}

fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

/// Return a UTF-8 byte boundary at or before the requested UTF-16 code unit.
fn byte_index_at_utf16(text: &str, units: usize) -> usize {
    if units == 0 {
        return 0;
    }
    let mut seen = 0;
    for (byte_index, ch) in text.char_indices() {
        let next = seen + ch.len_utf16();
        if next > units {
            return byte_index;
        }
        seen = next;
        if seen == units {
            return byte_index + ch.len_utf8();
        }
    }
    text.len()
}

fn js_trim(text: &str) -> String {
    text.trim_matches(is_js_trim_whitespace).to_owned()
}

fn is_js_trim_whitespace(ch: char) -> bool {
    matches!(
        ch,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'..='\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::oneshot;

    #[derive(Default)]
    struct ManualClock {
        sleepers: Mutex<VecDeque<oneshot::Sender<()>>>,
    }

    impl ManualClock {
        fn pending(&self) -> usize {
            self.sleepers.lock().unwrap().len()
        }

        fn advance_one(&self) {
            loop {
                let sleeper = self.sleepers.lock().unwrap().pop_front().unwrap();
                if sleeper.send(()).is_ok() {
                    return;
                }
            }
        }
    }

    impl BlockStreamerClock for ManualClock {
        fn sleep(&self, _duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            let (wake, asleep) = oneshot::channel();
            self.sleepers.lock().unwrap().push_back(wake);
            Box::pin(async move {
                let _ = asleep.await;
            })
        }
    }

    async fn wait_for_sleepers(clock: &ManualClock, count: usize) {
        for _ in 0..100 {
            if clock.pending() >= count {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(clock.pending(), count);
    }

    fn options(min_chars: usize, max_chars: usize, idle: Duration) -> BlockStreamerOptions {
        BlockStreamerOptions {
            min_chars,
            max_chars,
            idle,
        }
    }

    fn streamer<F, Fut, E>(
        min_chars: usize,
        max_chars: usize,
        idle: Duration,
        send: F,
    ) -> BlockStreamer
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Error + Send + Sync + 'static,
    {
        BlockStreamer::new(options(min_chars, max_chars, idle), send).unwrap()
    }

    async fn collecting_streamer(
        min_chars: usize,
        max_chars: usize,
        idle: Duration,
    ) -> (BlockStreamer, Arc<Mutex<Vec<String>>>) {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let destination = sent.clone();
        let block_streamer = streamer(min_chars, max_chars, idle, move |text| {
            let destination = destination.clone();
            async move {
                destination.lock().unwrap().push(text);
                Ok::<_, io::Error>(())
            }
        });
        (block_streamer, sent)
    }

    async fn collecting_streamer_with_clock(
        min_chars: usize,
        max_chars: usize,
        idle: Duration,
        clock: Arc<ManualClock>,
    ) -> (BlockStreamer, Arc<Mutex<Vec<String>>>) {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let destination = sent.clone();
        let block_streamer = BlockStreamer::new_with_clock(
            options(min_chars, max_chars, idle),
            move |text| {
                let destination = destination.clone();
                async move {
                    destination.lock().unwrap().push(text);
                    Ok::<_, io::Error>(())
                }
            },
            clock,
        )
        .unwrap();
        (block_streamer, sent)
    }

    #[tokio::test]
    async fn does_not_emit_below_minimum() {
        let (streamer, sent) = collecting_streamer(20, 60, Duration::ZERO).await;
        streamer.push("short");
        assert_eq!(streamer.block_count(), 0);
        streamer.flush().await;
        assert_eq!(*sent.lock().unwrap(), ["short"]);
        assert_eq!(streamer.block_count(), 1);
    }

    #[tokio::test]
    async fn emits_paragraph_boundary_after_minimum_and_flushes_remainder() {
        let (streamer, sent) = collecting_streamer(10, 60, Duration::ZERO).await;
        streamer.push("Hello world, this is a paragraph.\n\nSecond part");
        assert_eq!(streamer.block_count(), 1);
        streamer.flush().await;
        assert_eq!(
            *sent.lock().unwrap(),
            ["Hello world, this is a paragraph.", "Second part"]
        );
    }

    #[tokio::test]
    async fn does_not_emit_paragraph_boundary_before_minimum() {
        let (streamer, sent) = collecting_streamer(100, 200, Duration::ZERO).await;
        streamer.push("Short.\n\nAlso short.");
        assert_eq!(streamer.block_count(), 0);
        streamer.flush().await;
        assert_eq!(*sent.lock().unwrap(), ["Short.\n\nAlso short."]);
    }

    #[tokio::test]
    async fn force_splits_at_space_when_maximum_is_reached() {
        let (streamer, sent) = collecting_streamer(10, 30, Duration::ZERO).await;
        streamer.push("aaaa bbbb cccc dddd eeee ffff gggg hhhh");
        streamer.flush().await;
        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert!(utf16_len(&sent[0]) <= 30);
        assert_eq!(
            format!("{} {}", sent[0], sent[1]),
            "aaaa bbbb cccc dddd eeee ffff gggg hhhh"
        );
    }

    #[tokio::test]
    async fn force_splits_at_limit_without_break_points() {
        let (streamer, sent) = collecting_streamer(5, 10, Duration::ZERO).await;
        streamer.push("abcdefghijklmnop");
        streamer.flush().await;
        assert_eq!(*sent.lock().unwrap(), ["abcdefghij", "klmnop"]);
    }

    #[tokio::test]
    async fn force_split_prefers_paragraph_then_newline_then_space() {
        let (streamer, sent) = collecting_streamer(5, 30, Duration::ZERO).await;
        streamer.push("line one\n\nline two\nline three xx");
        streamer.flush().await;
        assert_eq!(sent.lock().unwrap()[0], "line one");

        let (streamer, sent) = collecting_streamer(5, 18, Duration::ZERO).await;
        streamer.push("line one\nline two xx");
        streamer.flush().await;
        assert_eq!(*sent.lock().unwrap(), ["line one", "line two xx"]);

        let (streamer, sent) = collecting_streamer(5, 13, Duration::ZERO).await;
        streamer.push("aaaa bbbb cccc");
        streamer.flush().await;
        assert_eq!(sent.lock().unwrap()[0], "aaaa bbbb");
    }

    #[tokio::test]
    async fn idle_timer_emits_after_minimum() {
        let clock = Arc::new(ManualClock::default());
        let (streamer, sent) =
            collecting_streamer_with_clock(5, 100, Duration::from_millis(20), clock.clone()).await;
        streamer.push("Hello world");
        wait_for_sleepers(&clock, 1).await;
        clock.advance_one();
        streamer.flush().await;
        assert_eq!(*sent.lock().unwrap(), ["Hello world"]);
    }

    #[tokio::test]
    async fn stale_idle_timer_does_not_emit_after_later_push() {
        let clock = Arc::new(ManualClock::default());
        let (streamer, sent) =
            collecting_streamer_with_clock(5, 500, Duration::from_millis(10), clock.clone()).await;
        streamer.push("Hello ");
        wait_for_sleepers(&clock, 1).await;
        let stale_generation = lock_state(&streamer.inner).timer_generation;
        streamer.push("world");
        wait_for_sleepers(&clock, 2).await;
        fire_idle_timer(Arc::downgrade(&streamer.inner), stale_generation);
        assert_eq!(streamer.block_count(), 0);
        clock.advance_one();
        tokio::task::yield_now().await;
        assert_eq!(streamer.block_count(), 1);
        streamer.flush().await;
        assert_eq!(*sent.lock().unwrap(), ["Hello world"]);
    }

    #[tokio::test]
    async fn idle_does_not_emit_below_minimum_but_flush_does() {
        let clock = Arc::new(ManualClock::default());
        let (streamer, sent) =
            collecting_streamer_with_clock(100, 500, Duration::from_millis(10), clock.clone())
                .await;
        streamer.push("tiny");
        wait_for_sleepers(&clock, 1).await;
        clock.advance_one();
        tokio::task::yield_now().await;
        assert_eq!(streamer.block_count(), 0);
        streamer.flush().await;
        assert_eq!(*sent.lock().unwrap(), ["tiny"]);
    }

    #[tokio::test]
    async fn zero_idle_disables_timer() {
        let clock = Arc::new(ManualClock::default());
        let (streamer, sent) =
            collecting_streamer_with_clock(10, 100, Duration::ZERO, clock.clone()).await;
        streamer.push("Hello world, no timer");
        tokio::task::yield_now().await;
        assert_eq!(clock.pending(), 0);
        assert_eq!(streamer.block_count(), 0);
        streamer.flush().await;
        assert_eq!(*sent.lock().unwrap(), ["Hello world, no timer"]);
    }

    #[tokio::test]
    async fn stop_clears_buffer_and_invalidates_idle_timer() {
        let clock = Arc::new(ManualClock::default());
        let (streamer, sent) =
            collecting_streamer_with_clock(5, 100, Duration::from_millis(10), clock.clone()).await;
        streamer.push("Hello world");
        wait_for_sleepers(&clock, 1).await;
        let stale_generation = lock_state(&streamer.inner).timer_generation;
        streamer.stop();
        fire_idle_timer(Arc::downgrade(&streamer.inner), stale_generation);
        streamer.flush().await;
        assert!(sent.lock().unwrap().is_empty());
        assert_eq!(streamer.block_count(), 0);
    }

    #[tokio::test]
    async fn stop_only_drops_current_buffer_and_later_pushes_work() {
        let (streamer, sent) = collecting_streamer(100, 500, Duration::ZERO).await;
        streamer.push("discard me");
        streamer.stop();
        streamer.push("keep me");
        streamer.flush().await;
        assert_eq!(*sent.lock().unwrap(), ["keep me"]);
    }

    #[tokio::test]
    async fn trims_blocks_and_skips_empty_blocks() {
        let (streamer, sent) = collecting_streamer(1, 100, Duration::ZERO).await;
        streamer.push("  \n  Hello world  \n\n  Next  ");
        streamer.flush().await;
        assert!(sent.lock().unwrap().iter().all(|text| text == text.trim()));

        let (streamer, sent) = collecting_streamer(1, 100, Duration::ZERO).await;
        streamer.push("\n\n\n\n");
        streamer.flush().await;
        assert!(sent.lock().unwrap().is_empty());
        assert_eq!(streamer.block_count(), 0);
    }

    #[tokio::test]
    async fn sends_are_serialized_and_flush_waits_for_them() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(AtomicUsize::new(0));
        let order_for_send = order.clone();
        let started_for_send = started.clone();
        let streamer = streamer(5, 20, Duration::ZERO, move |text| {
            let order = order_for_send.clone();
            let started = started_for_send.clone();
            async move {
                let index = started.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
                order.lock().unwrap().push((index, text));
                Ok::<_, io::Error>(())
            }
        });
        streamer.push("aaaa bbbb cccc dddd eeee ffff");
        streamer.flush().await;
        let order = order.lock().unwrap();
        assert!(order.len() >= 2);
        for (expected, (actual, _)) in order.iter().enumerate() {
            assert_eq!(*actual, expected);
        }
    }

    #[tokio::test]
    async fn send_errors_are_swallowed_and_later_sends_continue() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_send = attempts.clone();
        let streamer = streamer(5, 10, Duration::ZERO, move |_text| {
            let attempts = attempts_for_send.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(io::Error::other("channel is gone"))
            }
        });
        streamer.push("abcdefghijklmno");
        streamer.flush().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(streamer.block_count(), 2);
    }

    #[tokio::test]
    async fn paragraph_check_uses_last_boundary_and_sends_combined_prefix() {
        let (streamer, sent) = collecting_streamer(5, 200, Duration::ZERO).await;
        streamer.push("Para one.\n\nPara two.\n\nPara three.");
        streamer.flush().await;
        assert_eq!(
            *sent.lock().unwrap(),
            ["Para one.\n\nPara two.", "Para three."]
        );
    }

    #[tokio::test]
    async fn thresholds_and_splits_count_utf16_units() {
        let (streamer, sent) = collecting_streamer(3, 3, Duration::ZERO).await;
        streamer.push("😀ab"); // Four UTF-16 units, three Unicode scalars.
        streamer.flush().await;
        assert_eq!(*sent.lock().unwrap(), ["😀a", "b"]);
    }

    #[tokio::test]
    async fn astral_character_at_split_limit_is_never_corrupted() {
        let (streamer, sent) = collecting_streamer(1, 1, Duration::ZERO).await;
        streamer.push("😀x");
        streamer.flush().await;
        assert_eq!(*sent.lock().unwrap(), ["😀", "x"]);
    }

    #[tokio::test]
    async fn empty_flush_is_a_no_op_but_still_safe_to_await() {
        let (streamer, sent) = collecting_streamer(20, 60, Duration::ZERO).await;
        streamer.flush().await;
        assert!(sent.lock().unwrap().is_empty());
        assert_eq!(streamer.block_count(), 0);
    }

    #[test]
    fn javascript_trim_includes_bom_but_not_next_line() {
        assert_eq!(js_trim("\u{FEFF}text\u{FEFF}"), "text");
        assert_eq!(js_trim("\u{0085}text\u{0085}"), "\u{0085}text\u{0085}");
    }

    #[tokio::test]
    async fn zero_maximum_is_rejected() {
        let result = BlockStreamer::new(options(1, 0, Duration::ZERO), |_| async {
            Ok::<_, io::Error>(())
        });
        assert!(matches!(
            result,
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
    }
}
