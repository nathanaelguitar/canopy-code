//! Bounded per-session bridge event replay and live fanout.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::compaction_engine::{
    CompactionMemoryStats, JournalGrowthRegistry, JournalLimits, LiveReplayMode,
    SessionCompactionOptions, SessionReplaySnapshot, TurnBoundaryCompactionEngine,
};

pub const EVENT_SCHEMA_VERSION: u8 = 1;
pub const DEFAULT_MAX_QUEUED: usize = 256;
pub const DEFAULT_MAX_QUEUED_BYTES: usize = 2 * 1024 * 1024;
pub const DEFAULT_REPLAY_BUDGET_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_REPLAY_RING_BUDGET_BYTES: usize = 32 * 1024 * 1024;
pub const DEFAULT_RING_SIZE: usize = 8_000;
pub const DEFAULT_MAX_SUBSCRIBERS: usize = 64;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BridgeEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    pub v: u8,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_id: Option<String>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub originator_client_id: Option<String>,
}

impl BridgeEvent {
    pub fn new(event_type: impl Into<String>, data: Value) -> Self {
        Self {
            id: None,
            v: EVENT_SCHEMA_VERSION,
            event_type: event_type.into(),
            data,
            prompt_id: None,
            meta: None,
            originator_client_id: None,
        }
    }
    fn serialized_bytes(&self) -> usize {
        serde_json::to_vec(self).map_or(0, |value| value.len())
    }
}

#[derive(Clone, Debug)]
pub struct EventBusOptions {
    pub ring_size: usize,
    pub max_subscribers: usize,
    pub max_queued: usize,
    pub max_queued_bytes: usize,
    pub max_event_bytes: usize,
    pub replay_ring_budget_bytes: usize,
    pub replay_budget_bytes: usize,
    /// Compacted turn replay byte cap. Separate from the reconnect ring and
    /// per-subscribe replay burst budgets.
    pub compacted_replay_max_bytes: u64,
    /// Baseline in-flight journal event cap. `Some` values are explicit pins
    /// and therefore disable a derived adaptive growth pool.
    pub max_journal_events: Option<u64>,
    /// Baseline in-flight journal byte cap. `Some` values are explicit pins
    /// and therefore disable a derived adaptive growth pool.
    pub max_journal_bytes: Option<u64>,
    /// Optional daemon-wide pool for in-flight journal growth, in bytes.
    pub journal_growth_pool_bytes: Option<u64>,
    /// Shared across session buses/runtimes that must account against the
    /// same daemon-wide pool.
    pub journal_growth_registry: Option<Arc<JournalGrowthRegistry>>,
}
impl Default for EventBusOptions {
    fn default() -> Self {
        Self {
            ring_size: DEFAULT_RING_SIZE,
            max_subscribers: DEFAULT_MAX_SUBSCRIBERS,
            max_queued: DEFAULT_MAX_QUEUED,
            max_queued_bytes: DEFAULT_MAX_QUEUED_BYTES,
            max_event_bytes: DEFAULT_MAX_EVENT_BYTES,
            replay_ring_budget_bytes: DEFAULT_REPLAY_RING_BUDGET_BYTES,
            replay_budget_bytes: DEFAULT_REPLAY_BUDGET_BYTES,
            compacted_replay_max_bytes:
                super::replay_window_limits::DEFAULT_COMPACTED_REPLAY_MAX_BYTES,
            max_journal_events: None,
            max_journal_bytes: None,
            journal_growth_pool_bytes: None,
            journal_growth_registry: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriberLimitExceededError {
    pub maximum: usize,
}
impl std::fmt::Display for SubscriberLimitExceededError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "event bus subscriber limit exceeded ({})", self.maximum)
    }
}
impl std::error::Error for SubscriberLimitExceededError {}

#[derive(Clone, Debug, Default)]
pub struct SubscribeOptions {
    pub last_event_id: Option<u64>,
    pub epoch: Option<String>,
    pub max_queued: Option<usize>,
}
struct Subscriber {
    sender: mpsc::Sender<BridgeEvent>,
    queued_bytes: Arc<AtomicUsize>,
    max_queued_bytes: usize,
}
struct State {
    next_id: u64,
    closed: bool,
    ring: VecDeque<(BridgeEvent, usize)>,
    ring_bytes: usize,
    subscribers: HashMap<Uuid, Subscriber>,
    compaction: TurnBoundaryCompactionEngine,
}

pub struct SessionEventBus {
    epoch: String,
    options: EventBusOptions,
    state: Mutex<State>,
}
impl Default for SessionEventBus {
    fn default() -> Self {
        Self::new(EventBusOptions::default())
    }
}

impl SessionEventBus {
    pub fn new(options: EventBusOptions) -> Self {
        let max_journal_events = options
            .max_journal_events
            .unwrap_or(super::replay_window_limits::DEFAULT_MAX_JOURNAL_EVENTS);
        let max_journal_bytes = options
            .max_journal_bytes
            .unwrap_or(super::replay_window_limits::DEFAULT_MAX_JOURNAL_BYTES);
        // Explicitly pinned journal caps take precedence over a derived pool,
        // matching the serve layer. The reconnect ring fields above are not
        // part of this decision.
        let journal_growth_pool_bytes =
            if options.max_journal_events.is_some() || options.max_journal_bytes.is_some() {
                None
            } else {
                options.journal_growth_pool_bytes
            };
        let compaction = TurnBoundaryCompactionEngine::new(SessionCompactionOptions {
            max_replay_bytes: options.compacted_replay_max_bytes,
            max_journal_events,
            max_journal_bytes,
            journal_growth_pool_bytes,
            growth_registry: options.journal_growth_registry.clone(),
        })
        .expect("ACP event bus journal limits must be valid positive safe integers");
        Self {
            epoch: Uuid::new_v4().to_string(),
            options,
            state: Mutex::new(State {
                next_id: 1,
                closed: false,
                ring: VecDeque::new(),
                ring_bytes: 0,
                subscribers: HashMap::new(),
                compaction,
            }),
        }
    }
    pub fn epoch(&self) -> &str {
        &self.epoch
    }
    pub fn last_event_id(&self) -> u64 {
        lock(&self.state).next_id.saturating_sub(1)
    }
    pub fn subscriber_count(&self) -> usize {
        lock(&self.state).subscribers.len()
    }

    /// Assign sequence/version, retain the reconnect suffix, then fan out.
    /// An oversized frame or closed bus is a no-op, matching the TS
    /// publish contract's undefined return.
    pub fn publish(&self, mut event: BridgeEvent) -> Option<BridgeEvent> {
        let mut state = lock(&self.state);
        if state.closed {
            return None;
        }
        event.v = EVENT_SCHEMA_VERSION;
        event.id = Some(state.next_id);
        let bytes = event.serialized_bytes();
        if bytes > self.options.max_event_bytes {
            return None;
        }
        state.next_id = state.next_id.saturating_add(1);
        state.ring.push_back((event.clone(), bytes));
        state.ring_bytes += bytes;
        state.compaction.ingest(event.clone(), Some(bytes as u64));
        while state.ring.len() > self.options.ring_size
            || (state.ring.len() > 1 && state.ring_bytes > self.options.replay_ring_budget_bytes)
        {
            if let Some((_, removed)) = state.ring.pop_front() {
                state.ring_bytes = state.ring_bytes.saturating_sub(removed);
            }
        }
        let ids: Vec<Uuid> = state.subscribers.keys().copied().collect();
        for id in ids {
            let Some(subscriber) = state.subscribers.get(&id) else {
                continue;
            };
            let queued = subscriber.queued_bytes.load(Ordering::Relaxed);
            if queued.saturating_add(bytes) > subscriber.max_queued_bytes {
                let evicted = BridgeEvent::new(
                    "client_evicted",
                    json!({"reason":"queue_bytes_overflow", "droppedAfter":event.id.map(|id|id.saturating_sub(1)), "triggerEventType":event.event_type,"triggerEventBytes":bytes}),
                );
                let _ = subscriber.sender.try_send(evicted);
                state.subscribers.remove(&id);
                continue;
            }
            match subscriber.sender.try_send(event.clone()) {
                Ok(()) => {
                    subscriber.queued_bytes.fetch_add(bytes, Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    let evicted = BridgeEvent::new(
                        "client_evicted",
                        json!({"reason":"queue_overflow", "droppedAfter":event.id.map(|id|id.saturating_sub(1)), "triggerEventType":event.event_type,"triggerEventBytes":bytes}),
                    );
                    let _ = subscriber.sender.try_send(evicted);
                    state.subscribers.remove(&id);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    state.subscribers.remove(&id);
                }
            }
        }
        Some(event)
    }

    /// Seed historical frames with fresh ids and clear the live ring because
    /// those frames are not a contiguous ring suffix.
    pub fn seed_replay_events(&self, events: Vec<BridgeEvent>) -> Vec<BridgeEvent> {
        let mut state = lock(&self.state);
        if state.closed {
            return Vec::new();
        }
        let mut seeded = Vec::with_capacity(events.len());
        for mut event in events {
            event.v = EVENT_SCHEMA_VERSION;
            event.id = Some(state.next_id);
            state.next_id = state.next_id.saturating_add(1);
            seeded.push(event);
        }
        state.compaction.seed_replay_events(seeded.clone());
        state.ring.clear();
        state.ring_bytes = 0;
        seeded
    }

    pub fn subscribe(
        &self,
        opts: SubscribeOptions,
    ) -> Result<EventSubscription, SubscriberLimitExceededError> {
        let mut state = lock(&self.state);
        if state.closed {
            let (_tx, rx) = mpsc::channel(1);
            return Ok(EventSubscription {
                receiver: rx,
                id: None,
                queued_bytes: Arc::new(AtomicUsize::new(0)),
            });
        }
        if state.subscribers.len() >= self.options.max_subscribers {
            return Err(SubscriberLimitExceededError {
                maximum: self.options.max_subscribers,
            });
        }
        let max_queued = opts.max_queued.unwrap_or(self.options.max_queued).max(1);
        // Replay frames are delivered ahead of live frames and count against
        // the fixed ring/replay budgets rather than the smaller live queue.
        let (sender, receiver) =
            mpsc::channel(max_queued.max(state.ring.len().saturating_add(2)).max(1));
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        let mut replay: Vec<BridgeEvent> = Vec::new();
        if let Some(last_id) = opts.last_event_id {
            let earliest = state
                .ring
                .front()
                .and_then(|(event, _)| event.id)
                .unwrap_or(state.next_id);
            let epoch_mismatch = opts
                .epoch
                .as_deref()
                .is_some_and(|epoch| epoch != self.epoch);
            let epoch_reset = epoch_mismatch || last_id >= state.next_id;
            let reason = if epoch_reset {
                Some("epoch_reset")
            } else if state.ring.is_empty() && last_id < state.next_id.saturating_sub(1) {
                Some("seeded_replay_not_in_ring")
            } else if !state.ring.is_empty() && earliest > last_id.saturating_add(1) {
                Some("ring_evicted")
            } else {
                None
            };
            if let Some(reason) = reason {
                replay.push(BridgeEvent::new("state_resync_required", json!({"reason":reason, "lastDeliveredId":last_id, "earliestAvailableId":earliest, "detail":if epoch_mismatch {Some("epoch_mismatch")} else {None}})));
            }
            let mut replay_bytes = 0usize;
            for (event, bytes) in &state.ring {
                if event.id.is_some_and(|id| id > last_id) {
                    if replay_bytes.saturating_add(*bytes) > self.options.replay_budget_bytes {
                        replay.push(BridgeEvent::new("state_resync_required", json!({"reason":"replay_budget_exceeded", "lastDeliveredId":last_id, "earliestAvailableId":earliest})));
                        break;
                    }
                    replay_bytes += *bytes;
                    replay.push(event.clone());
                }
            }
        }
        for event in replay {
            let bytes = event.serialized_bytes();
            let _ = sender.try_send(event);
            queued_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        let id = Uuid::new_v4();
        state.subscribers.insert(
            id,
            Subscriber {
                sender,
                queued_bytes: queued_bytes.clone(),
                max_queued_bytes: self.options.max_queued_bytes,
            },
        );
        Ok(EventSubscription {
            receiver,
            id: Some(id),
            queued_bytes,
        })
    }

    pub fn unsubscribe(&self, subscription: &EventSubscription) {
        if let Some(id) = subscription.id {
            lock(&self.state).subscribers.remove(&id);
        }
    }
    pub fn close(&self) {
        let mut state = lock(&self.state);
        state.closed = true;
        state.subscribers.clear();
        state.compaction.close();
    }

    pub fn replay_snapshot(&self, mode: LiveReplayMode) -> SessionReplaySnapshot {
        lock(&self.state).compaction.snapshot(mode)
    }

    pub fn live_journal_snapshot(&self, mode: LiveReplayMode) -> Vec<BridgeEvent> {
        lock(&self.state).compaction.snapshot(mode).live_journal
    }

    pub fn journal_limits(&self) -> JournalLimits {
        lock(&self.state).compaction.journal_limits()
    }

    pub fn compaction_memory_stats(&self) -> CompactionMemoryStats {
        lock(&self.state).compaction.memory_stats()
    }
}

pub struct EventSubscription {
    receiver: mpsc::Receiver<BridgeEvent>,
    id: Option<Uuid>,
    queued_bytes: Arc<AtomicUsize>,
}
impl EventSubscription {
    pub async fn recv(&mut self) -> Option<BridgeEvent> {
        let event = self.receiver.recv().await?;
        self.queued_bytes
            .fetch_sub(event.serialized_bytes(), Ordering::Relaxed);
        Some(event)
    }
    pub fn id(&self) -> Option<Uuid> {
        self.id
    }
}
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publishes_monotonic_ids_and_replays_after_cursor() {
        let bus = SessionEventBus::new(EventBusOptions {
            ring_size: 3,
            ..Default::default()
        });
        assert_eq!(
            bus.publish(BridgeEvent::new("one", json!({}))).unwrap().id,
            Some(1)
        );
        assert_eq!(
            bus.publish(BridgeEvent::new("two", json!({}))).unwrap().id,
            Some(2)
        );
        let mut sub = bus
            .subscribe(SubscribeOptions {
                last_event_id: Some(1),
                epoch: Some(bus.epoch().into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(sub.recv().await.unwrap().event_type, "two");
        bus.publish(BridgeEvent::new("three", json!({}))).unwrap();
        assert_eq!(sub.recv().await.unwrap().id, Some(3));
    }

    #[tokio::test]
    async fn evicted_cursor_gets_resync_notice_and_bus_close_ends_stream() {
        let bus = SessionEventBus::new(EventBusOptions {
            ring_size: 1,
            ..Default::default()
        });
        bus.publish(BridgeEvent::new("one", json!({})));
        bus.publish(BridgeEvent::new("two", json!({})));
        let mut sub = bus
            .subscribe(SubscribeOptions {
                last_event_id: Some(0),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            sub.recv().await.unwrap().event_type,
            "state_resync_required"
        );
        assert_eq!(sub.recv().await.unwrap().event_type, "two");
        bus.close();
    }
}
