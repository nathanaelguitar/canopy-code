//! Durable polling-loop support for channel adapters.
//!
//! Port of the reusable behavior in `PollingChannelBase.ts`: cursor restore
//! and persistence, single-loop start/stop, configured polling intervals, and
//! bounded exponential error backoff. Platform API calls remain adapter-owned.

use super::paths::global_channels_root;
use super::sanitize::sanitize_log_text;
use crate::utils::atomic_file_write::{AtomicWriteOptions, atomic_write_file};
use futures_util::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::task::JoinHandle;
use tokio::time;

const DEFAULT_POLL_INTERVAL_MS: f64 = 60_000.0;
const INITIAL_BACKOFF_MS: u64 = 2_000;
const MAX_BACKOFF_MS: u64 = 30_000;

/// A platform adapter's one-poll operation. The mutable cursor is available
/// for the full asynchronous poll, matching subclasses' access to `this.cursor`.
pub trait PollingTask<C>: Send + Sync + 'static
where
    C: Send,
{
    fn poll_once<'a>(&'a self, cursor: &'a mut C) -> BoxFuture<'a, Result<(), String>>;
}

struct PollingState {
    running: bool,
    generation: u64,
    stop: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<()>>,
}

struct PollingInner<C> {
    name: String,
    cursor_path: PathBuf,
    poll_interval: Duration,
    cursor: AsyncMutex<C>,
    poller: Arc<dyn PollingTask<C>>,
    state: Mutex<PollingState>,
}

/// Generic polling loop shared by platform channel adapters.
///
/// Cursor I/O uses a synced same-directory atomic replacement so interruption
/// cannot leave a partially written cursor. A stop request ends the sleep and
/// waits for an active poll to settle, as in the TypeScript base class.
pub struct PollingChannelBase<C> {
    inner: Arc<PollingInner<C>>,
}

impl<C> Clone for PollingChannelBase<C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<C> PollingChannelBase<C>
where
    C: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    /// Create a polling loop rooted at an explicit `channels` directory.
    /// Invalid, missing, and non-object cursor files fall back to
    /// `initial_cursor`, matching the source's best-effort load behavior.
    pub fn new(
        name: impl Into<String>,
        initial_cursor: C,
        poller: Arc<dyn PollingTask<C>>,
        channels_dir: impl Into<PathBuf>,
        configured_interval_ms: Option<f64>,
    ) -> Self {
        let name = name.into();
        let cursor_path = cursor_path(&name, &channels_dir.into());
        let cursor = load_cursor(&cursor_path).unwrap_or(initial_cursor);
        Self {
            inner: Arc::new(PollingInner {
                name,
                cursor_path,
                poll_interval: interval_from_ms(configured_interval_ms),
                cursor: AsyncMutex::new(cursor),
                poller,
                state: Mutex::new(PollingState {
                    running: false,
                    generation: 0,
                    stop: None,
                    task: None,
                }),
            }),
        }
    }

    /// Create a polling loop using the current global Qwen channels directory.
    pub fn from_global(
        name: impl Into<String>,
        initial_cursor: C,
        poller: Arc<dyn PollingTask<C>>,
        configured_interval_ms: Option<f64>,
    ) -> io::Result<Self> {
        Ok(Self::new(
            name,
            initial_cursor,
            poller,
            global_channels_root()?,
            configured_interval_ms,
        ))
    }

    /// Return a detached snapshot of the current cursor.
    pub async fn cursor_snapshot(&self) -> C {
        self.inner.cursor.lock().await.clone()
    }

    /// Persist the current cursor immediately.
    pub async fn save_cursor(&self) -> io::Result<()> {
        let cursor = self.inner.cursor.lock().await;
        persist_cursor(&self.inner.cursor_path, &*cursor)
    }

    /// Start the polling task if one is not already running.
    ///
    /// A Tokio runtime must be active. Calling this method again while the
    /// task is running is a no-op.
    pub fn start_poll_loop(&self) -> io::Result<()> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "PollingChannelBase requires an active Tokio runtime",
            )
        })?;
        let mut state = self
            .inner
            .state
            .lock()
            .expect("polling state mutex poisoned");
        if state.running {
            return Ok(());
        }
        state.running = true;
        state.generation = state.generation.wrapping_add(1);
        let generation = state.generation;
        let (stop, stop_rx) = watch::channel(false);
        let inner = self.inner.clone();
        state.stop = Some(stop);
        state.task = Some(runtime.spawn(async move {
            run_loop(inner, stop_rx, generation).await;
        }));
        Ok(())
    }

    /// Request that the polling task stop after its active poll settles.
    pub fn stop_poll_loop(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("polling state mutex poisoned");
        state.running = false;
        if let Some(stop) = state.stop.take() {
            stop.send_replace(true);
        }
    }

    /// Stop the polling task and wait for its current poll and cursor write.
    pub async fn stop_poll_loop_and_wait(&self) {
        let task = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("polling state mutex poisoned");
            state.running = false;
            if let Some(stop) = state.stop.take() {
                stop.send_replace(true);
            }
            state.task.take()
        };
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    pub fn poll_interval(&self) -> Duration {
        self.inner.poll_interval
    }

    pub fn cursor_file(&self) -> &Path {
        &self.inner.cursor_path
    }
}

fn interval_from_ms(configured: Option<f64>) -> Duration {
    let milliseconds = configured
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(DEFAULT_POLL_INTERVAL_MS);
    Duration::from_millis(milliseconds.ceil().max(1.0) as u64)
}

fn cursor_path(name: &str, channels_dir: &Path) -> PathBuf {
    // JavaScript's non-Unicode regular expression replaces each UTF-16 code
    // unit outside the ASCII allowlist. This also keeps the encoded part below
    // 200 characters for names containing supplementary characters.
    let encoded: String = name
        .encode_utf16()
        .take(200)
        .map(|unit| {
            if unit <= 0x7f
                && ((unit as u8).is_ascii_alphanumeric() || matches!(unit as u8, b'_' | b'-'))
            {
                unit as u8 as char
            } else {
                '_'
            }
        })
        .collect();
    let digest = Sha256::digest(name.as_bytes());
    let short_hash: String = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    channels_dir.join(format!("{encoded}-{short_hash}-poll-cursor.json"))
}

fn load_cursor<C>(path: &Path) -> Option<C>
where
    C: DeserializeOwned,
{
    let raw = fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(raw.trim()).ok()?;
    if !value.is_object() {
        return None;
    }
    serde_json::from_value(value).ok()
}

fn persist_cursor<C: Serialize>(path: &Path, cursor: &C) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(cursor).map_err(io::Error::other)?;
    bytes.push(b'\n');
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    atomic_write_file(path, &bytes, &AtomicWriteOptions::default())
}

async fn run_loop<C>(inner: Arc<PollingInner<C>>, mut stop: watch::Receiver<bool>, generation: u64)
where
    C: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    let mut consecutive_errors = 0u32;
    loop {
        if *stop.borrow() {
            break;
        }

        let result = {
            let mut cursor = inner.cursor.lock().await;
            match inner.poller.poll_once(&mut *cursor).await {
                Ok(()) => persist_cursor(&inner.cursor_path, &*cursor),
                Err(error) => Err(io::Error::other(error)),
            }
        };
        match result {
            Ok(()) => consecutive_errors = 0,
            Err(error) => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                let exponent = consecutive_errors.saturating_sub(1).min(31);
                let backoff_ms = INITIAL_BACKOFF_MS
                    .saturating_mul(1u64 << exponent)
                    .min(MAX_BACKOFF_MS);
                eprintln!(
                    "[Channel:{}] poll error (attempt {}), backing off {}ms: {}",
                    sanitize_log_text(&inner.name, 128),
                    consecutive_errors,
                    backoff_ms,
                    sanitize_log_text(&error.to_string(), 512),
                );
                if wait_or_stopped(&mut stop, Duration::from_millis(backoff_ms)).await {
                    break;
                }
                continue;
            }
        }

        if wait_or_stopped(&mut stop, inner.poll_interval).await {
            break;
        }
    }
    let mut state = inner.state.lock().expect("polling state mutex poisoned");
    if state.generation == generation {
        state.running = false;
    }
}

async fn wait_or_stopped(stop: &mut watch::Receiver<bool>, delay: Duration) -> bool {
    if *stop.borrow() {
        return true;
    }
    tokio::select! {
        _ = time::sleep(delay) => false,
        changed = stop.changed() => changed.is_err() || *stop.borrow(),
    }
}

#[cfg(test)]
mod tests {
    use super::{PollingChannelBase, PollingTask, cursor_path, interval_from_ms};
    use futures_util::future::BoxFuture;
    use serde::{Deserialize, Serialize};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct Cursor {
        ts: String,
        count: usize,
    }

    struct TestPoller {
        should_fail: AtomicBool,
        calls: AtomicUsize,
    }

    impl PollingTask<Cursor> for TestPoller {
        fn poll_once<'a>(&'a self, cursor: &'a mut Cursor) -> BoxFuture<'a, Result<(), String>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if self.should_fail.load(Ordering::SeqCst) {
                    return Err("poll failed".into());
                }
                cursor.count += 1;
                cursor.ts = "updated".into();
                Ok(())
            })
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("canopy-polling-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn initial() -> Cursor {
        Cursor {
            ts: "initial".into(),
            count: 0,
        }
    }

    fn make_poller() -> Arc<TestPoller> {
        Arc::new(TestPoller {
            should_fail: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        })
    }

    #[test]
    fn interval_accepts_only_finite_positive_values() {
        assert_eq!(interval_from_ms(Some(30_000.0)), Duration::from_secs(30));
        assert_eq!(interval_from_ms(Some(0.2)), Duration::from_millis(1));
        assert_eq!(interval_from_ms(Some(0.0)), Duration::from_secs(60));
        assert_eq!(interval_from_ms(Some(f64::NAN)), Duration::from_secs(60));
        assert_eq!(interval_from_ms(None), Duration::from_secs(60));
    }

    #[test]
    fn cursor_paths_are_bounded_and_distinct_for_long_names() {
        let root = Path::new("/tmp/channels");
        let prefix = "x".repeat(250);
        let first = cursor_path(&format!("{prefix}-alpha"), root);
        let second = cursor_path(&format!("{prefix}-beta"), root);
        assert!(first.file_name().unwrap().len() < 255);
        assert_ne!(first, second);
        let astral = cursor_path("agent-🎸", root);
        assert!(
            astral
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("agent-__-")
        );
    }

    #[tokio::test]
    async fn missing_or_corrupt_cursor_uses_initial_and_success_persists() {
        let directory = temp_dir("cursor");
        let poller = make_poller();
        let loop_ = PollingChannelBase::new("poller", initial(), poller, &directory, None);
        assert_eq!(loop_.cursor_snapshot().await, initial());
        {
            let mut cursor = loop_.inner.cursor.lock().await;
            cursor.count = 9;
            cursor.ts = "restored".into();
        }
        loop_.save_cursor().await.unwrap();
        let raw = std::fs::read_to_string(loop_.cursor_file()).unwrap();
        assert!(raw.ends_with('\n'));
        let restored =
            PollingChannelBase::new("poller", initial(), make_poller(), &directory, None);
        assert_eq!(restored.cursor_snapshot().await.count, 9);
        std::fs::write(restored.cursor_file(), "[]").unwrap();
        let corrupt = PollingChannelBase::new("poller", initial(), make_poller(), &directory, None);
        assert_eq!(corrupt.cursor_snapshot().await, initial());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn start_is_idempotent_success_saves_and_stop_waits_for_active_poll() {
        let directory = temp_dir("loop");
        let poller = make_poller();
        let loop_ =
            PollingChannelBase::new("poller", initial(), poller.clone(), &directory, Some(10.0));
        loop_.start_poll_loop().unwrap();
        loop_.start_poll_loop().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while poller.calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        loop_.stop_poll_loop_and_wait().await;
        assert_eq!(poller.calls.load(Ordering::SeqCst), 1);
        assert_eq!(loop_.cursor_snapshot().await.count, 1);
        let reloaded =
            PollingChannelBase::new("poller", initial(), make_poller(), &directory, None);
        assert_eq!(reloaded.cursor_snapshot().await.count, 1);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn poll_errors_back_off_and_stop_interrupts_the_wait() {
        let directory = temp_dir("error");
        let poller = make_poller();
        poller.should_fail.store(true, Ordering::SeqCst);
        let loop_ =
            PollingChannelBase::new("poller", initial(), poller.clone(), &directory, Some(10.0));
        loop_.start_poll_loop().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while poller.calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let before = tokio::time::Instant::now();
        loop_.stop_poll_loop_and_wait().await;
        assert!(before.elapsed() < Duration::from_millis(500));
        assert_eq!(loop_.cursor_snapshot().await.count, 0);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
