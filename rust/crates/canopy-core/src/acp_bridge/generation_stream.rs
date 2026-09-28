//! Single-consumer, bounded queue for request-scoped generation events.
//!
//! The queue deliberately has no replay or fan-out: each generated event
//! belongs to the request that initiated it. `push` is non-blocking and
//! returns `false` when the bounded buffer is full, matching the TypeScript
//! producer contract. A waiting receiver is handed the next value directly,
//! including when capacity is zero.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::oneshot;

struct Inner<T, E> {
    capacity: usize,
    next_waiter_id: AtomicU64,
    state: Mutex<State<T, E>>,
}

struct State<T, E> {
    values: VecDeque<T>,
    waiter: Option<Waiter<T, E>>,
    closed: bool,
    failure: Option<Arc<E>>,
}

struct Waiter<T, E> {
    id: u64,
    sender: oneshot::Sender<Delivery<T, E>>,
}

enum Delivery<T, E> {
    Value(T),
    Closed,
    Failed(Arc<E>),
}

/// Shared producer/consumer handle for one request's generation events.
///
/// Cloning this handle shares the same bounded queue. At most one pending
/// `recv` is accepted at a time. Buffered values are drained before a stored
/// failure is returned, as in the TypeScript async iterator.
pub struct GenerationStreamQueue<T, E = String> {
    inner: Arc<Inner<T, E>>,
}

impl<T, E> Clone for GenerationStreamQueue<T, E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T, E> GenerationStreamQueue<T, E> {
    /// Create a queue with room for at most `capacity` buffered values.
    ///
    /// A zero-capacity queue acts as a rendezvous: `push` succeeds only while
    /// a receiver is already waiting.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                capacity,
                next_waiter_id: AtomicU64::new(1),
                state: Mutex::new(State {
                    values: VecDeque::new(),
                    waiter: None,
                    closed: false,
                    failure: None,
                }),
            }),
        }
    }

    /// Add a value without waiting. Returns `false` after close or when full.
    pub fn push(&self, value: T) -> bool {
        let mut state = lock(&self.inner.state);
        if state.closed {
            return false;
        }

        if let Some(waiter) = state.waiter.take() {
            return match waiter.sender.send(Delivery::Value(value)) {
                Ok(()) => true,
                // A canceled Rust receive future has no TypeScript analogue.
                // Recover its value into the bounded buffer when there is room.
                Err(Delivery::Value(value)) if state.values.len() < self.inner.capacity => {
                    state.values.push_back(value);
                    true
                }
                Err(_) => false,
            };
        }

        if state.values.len() >= self.inner.capacity {
            return false;
        }
        state.values.push_back(value);
        true
    }

    /// Close the queue. Already-buffered values remain readable first.
    pub fn close(&self) {
        let mut state = lock(&self.inner.state);
        if state.closed {
            return;
        }
        state.closed = true;
        if let Some(waiter) = state.waiter.take() {
            let _ = waiter.sender.send(Delivery::Closed);
        }
    }

    /// Close the queue and propagate `error` after buffered values are read.
    ///
    /// The error is retained and returned from every later receive, matching
    /// repeated `next()` calls on the TypeScript implementation after failure.
    pub fn fail(&self, error: E) {
        let mut state = lock(&self.inner.state);
        if state.closed {
            return;
        }
        let error = Arc::new(error);
        state.failure = Some(Arc::clone(&error));
        state.closed = true;
        if let Some(waiter) = state.waiter.take() {
            let _ = waiter.sender.send(Delivery::Failed(error));
        }
    }

    pub fn is_closed(&self) -> bool {
        lock(&self.inner.state).closed
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Receive one value, wait for a producer, or return close/failure.
    ///
    /// A second concurrent pending receive returns
    /// [`GenerationStreamReceiveError::ConcurrentReader`] without disturbing
    /// the first. Dropping a pending receive future unregisters its waiter.
    pub async fn recv(&self) -> Result<Option<T>, GenerationStreamReceiveError<E>> {
        let (waiter_id, receiver) = {
            let mut state = lock(&self.inner.state);
            if let Some(value) = state.values.pop_front() {
                return Ok(Some(value));
            }
            if let Some(error) = &state.failure {
                return Err(GenerationStreamReceiveError::Failed(Arc::clone(error)));
            }
            if state.closed {
                return Ok(None);
            }
            if state.waiter.is_some() {
                return Err(GenerationStreamReceiveError::ConcurrentReader);
            }

            let (sender, receiver) = oneshot::channel();
            let waiter_id = self.inner.next_waiter_id.fetch_add(1, Ordering::Relaxed);
            state.waiter = Some(Waiter {
                id: waiter_id,
                sender,
            });
            (waiter_id, receiver)
        };

        let _pending = PendingReaderGuard {
            inner: Arc::clone(&self.inner),
            waiter_id,
        };

        match receiver.await {
            Ok(Delivery::Value(value)) => Ok(Some(value)),
            Ok(Delivery::Closed) => Ok(None),
            Ok(Delivery::Failed(error)) => Err(GenerationStreamReceiveError::Failed(error)),
            Err(_) => Err(GenerationStreamReceiveError::WaiterDropped),
        }
    }

    /// Adapt the queue to a Rust async stream of result items.
    ///
    /// Like repeated TypeScript iterator `next()` calls, a failed queue yields
    /// the same retained failure again on each subsequent poll. Consumers that
    /// prefer explicit control can call [`Self::recv`] in a loop instead.
    pub fn into_stream(
        self,
    ) -> impl futures_util::Stream<Item = Result<T, GenerationStreamReceiveError<E>>> {
        futures_util::stream::unfold(self, |queue| async move {
            match queue.recv().await {
                Ok(Some(value)) => Some((Ok(value), queue)),
                Ok(None) => None,
                Err(error) => Some((Err(error), queue)),
            }
        })
    }
}

struct PendingReaderGuard<T, E> {
    inner: Arc<Inner<T, E>>,
    waiter_id: u64,
}

impl<T, E> Drop for PendingReaderGuard<T, E> {
    fn drop(&mut self) {
        let mut state = lock(&self.inner.state);
        if state
            .waiter
            .as_ref()
            .is_some_and(|waiter| waiter.id == self.waiter_id)
        {
            state.waiter.take();
        }
    }
}

/// Receive-side failures from a generation stream queue.
#[derive(Debug)]
pub enum GenerationStreamReceiveError<E> {
    /// Another receive is already waiting on this single-consumer queue.
    ConcurrentReader,
    /// The producer failed. The shared error can be inspected or cloned.
    Failed(Arc<E>),
    /// The producer-side waiter was dropped unexpectedly.
    WaiterDropped,
}

impl<E: fmt::Display> fmt::Display for GenerationStreamReceiveError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConcurrentReader => {
                f.write_str("GenerationStreamQueue supports only one pending reader")
            }
            Self::Failed(error) => write!(f, "{error}"),
            Self::WaiterDropped => f.write_str("generation stream waiter was dropped"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for GenerationStreamReceiveError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Failed(error) => Some(error.as_ref()),
            Self::ConcurrentReader | Self::WaiterDropped => None,
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
