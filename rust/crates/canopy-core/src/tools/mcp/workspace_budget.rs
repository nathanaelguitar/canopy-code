use indexmap::{IndexMap, IndexSet};
use serde::Serialize;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

use super::pool_key::McpTransportKind;

pub const MCP_BUDGET_WARN_FRACTION: f64 = 0.75;
pub const MCP_BUDGET_REARM_FRACTION: f64 = 0.375;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum McpBudgetMode {
    Enforce,
    Warn,
    Off,
}

impl McpBudgetMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enforce => "enforce",
            Self::Warn => "warn",
            Self::Off => "off",
        }
    }
}

/// Serializable transport label used in MCP budget event payloads.
///
/// This intentionally mirrors the transport union in the TypeScript contract;
/// `McpTransportKind` in `pool_key` supplies the pool's transport detection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum McpBudgetTransport {
    Stdio,
    Sse,
    Http,
    Websocket,
    Sdk,
    Unknown,
}

impl McpBudgetTransport {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stdio => "stdio",
            Self::Sse => "sse",
            Self::Http => "http",
            Self::Websocket => "websocket",
            Self::Sdk => "sdk",
            Self::Unknown => "unknown",
        }
    }
}

impl From<McpTransportKind> for McpBudgetTransport {
    fn from(value: McpTransportKind) -> Self {
        match value {
            McpTransportKind::Stdio => Self::Stdio,
            McpTransportKind::Sse => Self::Sse,
            McpTransportKind::Http => Self::Http,
            McpTransportKind::Websocket => Self::Websocket,
            McpTransportKind::Sdk => Self::Sdk,
            McpTransportKind::Unknown => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum McpRefusalReason {
    #[serde(rename = "budget_exhausted")]
    BudgetExhausted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct McpRefusedServer {
    pub name: String,
    pub transport: McpBudgetTransport,
    pub reason: McpRefusalReason,
}

/// Event payloads match `McpBudgetEvent` in `mcp-client-manager.ts`.
/// Serialization uses the same discriminant and camel-case field names as the
/// source callback objects.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum McpBudgetEvent {
    BudgetWarning {
        #[serde(rename = "liveCount")]
        live_count: usize,
        #[serde(rename = "reservedCount")]
        reserved_count: usize,
        budget: f64,
        #[serde(rename = "thresholdRatio")]
        threshold_ratio: f64,
        mode: McpBudgetMode,
    },
    RefusedBatch {
        #[serde(rename = "refusedServers")]
        refused_servers: Vec<McpRefusedServer>,
        budget: f64,
        #[serde(rename = "liveCount")]
        live_count: usize,
        #[serde(rename = "reservedCount")]
        reserved_count: usize,
        mode: McpBudgetMode,
    },
}

pub type BudgetEventHandler = Box<dyn Fn(McpBudgetEvent) + Send + Sync + 'static>;

/// Workspace-scoped MCP slot budget and warning/refusal state machine.
///
/// Reservation and refusal sets preserve insertion order, matching JavaScript
/// `Set`/`Map` iteration. Reservations are keyed by server name so multiple
/// pool entries with the same name share one slot.
pub struct WorkspaceMcpBudget {
    client_budget: Option<f64>,
    mode: McpBudgetMode,
    on_event: Option<BudgetEventHandler>,
    reserved_slots: IndexSet<String>,
    reservation_refs: HashMap<String, usize>,
    pending_refusal_names: IndexSet<String>,
    pending_refusal_transports: IndexMap<String, McpBudgetTransport>,
    last_refused_server_names: Vec<String>,
    warn_armed: bool,
    bulk_pass_depth: usize,
}

impl WorkspaceMcpBudget {
    pub fn new(client_budget: Option<f64>, mode: McpBudgetMode) -> Self {
        Self {
            client_budget,
            mode,
            on_event: None,
            reserved_slots: IndexSet::new(),
            reservation_refs: HashMap::new(),
            pending_refusal_names: IndexSet::new(),
            pending_refusal_transports: IndexMap::new(),
            last_refused_server_names: Vec::new(),
            warn_armed: true,
            bulk_pass_depth: 0,
        }
    }

    /// Attach the synchronous event callback. Off mode deliberately discards
    /// the callback as a second guard against accidental telemetry.
    pub fn with_event_handler(
        mut self,
        handler: impl Fn(McpBudgetEvent) + Send + Sync + 'static,
    ) -> Self {
        if self.mode != McpBudgetMode::Off {
            self.on_event = Some(Box::new(handler));
        }
        self
    }

    pub const fn get_mode(&self) -> McpBudgetMode {
        self.mode
    }

    pub const fn get_budget(&self) -> Option<f64> {
        self.client_budget
    }

    /// Return a detached, insertion-ordered reservation snapshot.
    pub fn get_reserved_slots(&self) -> Vec<String> {
        self.reserved_slots.iter().cloned().collect()
    }

    /// Read the last completed refusal batch. It is cleared by the next
    /// outermost `begin_bulk_pass`, matching the snapshot route's lifetime.
    pub fn get_refused_server_names(&self) -> &[String] {
        &self.last_refused_server_names
    }

    pub fn get_reserved_count(&self) -> usize {
        self.reserved_slots.len()
    }

    /// Atomically check the cap and reserve this server name.
    pub fn try_reserve(&mut self, server_name: impl Into<String>) -> ReserveResult {
        let server_name = server_name.into();
        if self.reserved_slots.contains(&server_name) {
            return ReserveResult::AlreadyHeld;
        }
        if self.client_budget.is_none() || self.mode == McpBudgetMode::Off {
            return ReserveResult::Reserved;
        }
        if self.mode == McpBudgetMode::Enforce
            && (self.reserved_slots.len() as f64) >= self.client_budget.unwrap_or_default()
        {
            return ReserveResult::Refused;
        }
        self.reserved_slots.insert(server_name.clone());
        self.reservation_refs.insert(server_name, 1);
        self.evaluate_state();
        ReserveResult::Reserved
    }

    /// Reserve one manager's lease on a workspace slot. Several session
    /// managers can refer to the same server name while consuming one unique
    /// workspace slot; each successful manager admission must release only its
    /// own lease when it detaches.
    pub fn try_reserve_ref(&mut self, server_name: impl Into<String>) -> ReserveResult {
        let server_name = server_name.into();
        if self.reserved_slots.contains(&server_name) {
            let refs = self.reservation_refs.entry(server_name).or_insert(1);
            *refs = refs.saturating_add(1);
            return ReserveResult::AlreadyHeld;
        }
        self.try_reserve(server_name)
    }

    /// Release a held slot. Returns whether the set contained the name.
    pub fn release(&mut self, server_name: &str) -> bool {
        let removed = self.reserved_slots.shift_remove(server_name);
        if removed {
            self.reservation_refs.remove(server_name);
            self.evaluate_state();
        }
        removed
    }

    /// Release one manager's lease, freeing the unique workspace slot only
    /// after its final manager reference is gone.
    pub fn release_ref(&mut self, server_name: &str) -> bool {
        if let Some(refs) = self.reservation_refs.get_mut(server_name) {
            if *refs > 1 {
                *refs -= 1;
                return true;
            }
        }
        self.release(server_name)
    }

    /// Record an enforce-mode refusal. Outside a bulk pass it emits a
    /// one-entry batch immediately; within a pass names are coalesced until
    /// the outermost `end_bulk_pass`.
    pub fn record_refusal(
        &mut self,
        server_name: impl Into<String>,
        transport: impl Into<McpBudgetTransport>,
    ) {
        if self.mode != McpBudgetMode::Enforce {
            return;
        }
        let server_name = server_name.into();
        self.pending_refusal_names.insert(server_name.clone());
        self.pending_refusal_transports
            .insert(server_name, transport.into());
        if self.bulk_pass_depth == 0 {
            self.flush_refused_batch();
        }
    }

    /// Open a possibly nested discovery pass. Only the outermost begin clears
    /// the previous pass's snapshot-visible refusal names.
    pub fn begin_bulk_pass(&mut self) {
        if self.bulk_pass_depth == 0 {
            self.last_refused_server_names.clear();
        }
        self.bulk_pass_depth += 1;
    }

    /// Close a pass. Unmatched calls are harmless; only the outermost close
    /// flushes the accumulated refusal batch.
    pub fn end_bulk_pass(&mut self) {
        if self.bulk_pass_depth == 0 {
            return;
        }
        self.bulk_pass_depth -= 1;
        if self.bulk_pass_depth == 0 {
            self.flush_refused_batch();
        }
    }

    fn flush_refused_batch(&mut self) {
        if self.pending_refusal_names.is_empty() {
            return;
        }
        let Some(budget) = self
            .client_budget
            .filter(|_| self.mode == McpBudgetMode::Enforce)
        else {
            // Defensive parity with the source: discard an impossible pending
            // refusal if this instance has no usable enforcement budget.
            self.pending_refusal_names.clear();
            self.pending_refusal_transports.clear();
            return;
        };

        let mut refused_servers = Vec::with_capacity(self.pending_refusal_names.len());
        let mut names = Vec::with_capacity(self.pending_refusal_names.len());
        for name in &self.pending_refusal_names {
            let transport = self
                .pending_refusal_transports
                .get(name)
                .copied()
                .unwrap_or(McpBudgetTransport::Unknown);
            refused_servers.push(McpRefusedServer {
                name: name.clone(),
                transport,
                reason: McpRefusalReason::BudgetExhausted,
            });
            names.push(name.clone());
        }
        self.last_refused_server_names = names;
        self.pending_refusal_names.clear();
        self.pending_refusal_transports.clear();

        self.dispatch_event(McpBudgetEvent::RefusedBatch {
            refused_servers,
            budget,
            live_count: self.reserved_slots.len(),
            reserved_count: self.reserved_slots.len(),
            mode: McpBudgetMode::Enforce,
        });
    }

    fn evaluate_state(&mut self) {
        let Some(budget) = self.client_budget else {
            return;
        };
        if self.mode == McpBudgetMode::Off {
            return;
        }
        let ratio = self.reserved_slots.len() as f64 / budget;
        if self.warn_armed && ratio >= MCP_BUDGET_WARN_FRACTION {
            self.warn_armed = false;
            let reserved_count = self.reserved_slots.len();
            self.dispatch_event(McpBudgetEvent::BudgetWarning {
                live_count: reserved_count,
                reserved_count,
                budget,
                threshold_ratio: MCP_BUDGET_WARN_FRACTION,
                mode: self.mode,
            });
        } else if !self.warn_armed && ratio < MCP_BUDGET_REARM_FRACTION {
            self.warn_armed = true;
        }
    }

    fn dispatch_event(&self, event: McpBudgetEvent) {
        if let Some(handler) = &self.on_event {
            // Source callbacks are protected by try/catch: a consumer failure
            // must not unwind a reservation or break discovery.
            let _ = catch_unwind(AssertUnwindSafe(|| handler(event)));
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReserveResult {
    Reserved,
    AlreadyHeld,
    Refused,
}

#[cfg(test)]
mod tests {
    use super::{
        MCP_BUDGET_REARM_FRACTION, MCP_BUDGET_WARN_FRACTION, McpBudgetEvent, McpBudgetMode,
        McpBudgetTransport, McpRefusalReason, ReserveResult, WorkspaceMcpBudget,
    };
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};

    fn event_collector() -> (
        Arc<Mutex<Vec<McpBudgetEvent>>>,
        impl Fn(McpBudgetEvent) + Send + Sync + 'static,
    ) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let target = Arc::clone(&events);
        (events, move |event| target.lock().unwrap().push(event))
    }

    fn serialized_events(events: &[McpBudgetEvent]) -> Vec<Value> {
        events
            .iter()
            .map(|event| serde_json::to_value(event).unwrap())
            .collect()
    }

    #[test]
    fn reserve_modes_preserve_names_and_slot_order() {
        let mut enforced = WorkspaceMcpBudget::new(Some(2.0), McpBudgetMode::Enforce);
        assert_eq!(enforced.try_reserve("a"), ReserveResult::Reserved);
        assert_eq!(enforced.try_reserve("a"), ReserveResult::AlreadyHeld);
        assert_eq!(enforced.try_reserve("b"), ReserveResult::Reserved);
        assert_eq!(enforced.try_reserve("c"), ReserveResult::Refused);
        assert_eq!(
            enforced.get_reserved_slots(),
            vec!["a".to_owned(), "b".to_owned()]
        );

        let mut warn = WorkspaceMcpBudget::new(Some(1.0), McpBudgetMode::Warn);
        assert_eq!(warn.try_reserve("a"), ReserveResult::Reserved);
        assert_eq!(warn.try_reserve("b"), ReserveResult::Reserved);
        assert_eq!(warn.get_reserved_count(), 2);

        let mut off = WorkspaceMcpBudget::new(Some(1.0), McpBudgetMode::Off);
        assert_eq!(off.try_reserve("a"), ReserveResult::Reserved);
        assert_eq!(off.get_reserved_count(), 0);

        let mut unlimited = WorkspaceMcpBudget::new(None, McpBudgetMode::Enforce);
        assert_eq!(unlimited.try_reserve("a"), ReserveResult::Reserved);
        assert_eq!(unlimited.get_reserved_count(), 0);
    }

    #[test]
    fn release_is_idempotent_and_releases_capacity() {
        let mut budget = WorkspaceMcpBudget::new(Some(1.0), McpBudgetMode::Enforce);
        assert_eq!(budget.try_reserve("a"), ReserveResult::Reserved);
        assert_eq!(budget.try_reserve("b"), ReserveResult::Refused);
        assert!(budget.release("a"));
        assert!(!budget.release("a"));
        assert_eq!(budget.try_reserve("b"), ReserveResult::Reserved);
    }

    #[test]
    fn warning_hysteresis_uses_inclusive_warn_and_strict_rearm_thresholds() {
        assert_eq!(MCP_BUDGET_WARN_FRACTION, 0.75);
        assert_eq!(MCP_BUDGET_REARM_FRACTION, 0.375);
        let (events, handler) = event_collector();
        let mut budget =
            WorkspaceMcpBudget::new(Some(8.0), McpBudgetMode::Enforce).with_event_handler(handler);
        for name in ["a", "b", "c", "d", "e"] {
            assert_eq!(budget.try_reserve(name), ReserveResult::Reserved);
        }
        assert!(events.lock().unwrap().is_empty());
        budget.try_reserve("f"); // 6/8 is the inclusive 75% crossing.
        budget.try_reserve("g");
        assert_eq!(events.lock().unwrap().len(), 1);
        budget.release("a"); // 6/8 -> 5/8, still armed off.
        budget.release("b"); // 5/8 -> 4/8.
        budget.release("c"); // 5/8 -> 4/8.
        for name in ["h", "i", "j"] {
            budget.try_reserve(name);
        }
        assert_eq!(events.lock().unwrap().len(), 1);
        budget.release("d"); // 6/8 -> 5/8
        budget.release("e"); // 5/8 -> 4/8
        budget.release("f"); // 5/8 -> 4/8
        budget.release("g"); // 4/8 -> 3/8, equality must not re-arm.
        budget.release("h"); // 3/8 -> 2/8, strictly below 37.5%.
        for name in ["k", "l", "m", "n"] {
            budget.try_reserve(name);
        }
        assert_eq!(events.lock().unwrap().len(), 2);
        let snapshot = serialized_events(&events.lock().unwrap());
        assert_eq!(snapshot[0]["kind"], "budget_warning");
        assert_eq!(snapshot[0]["mode"], "enforce");
        assert_eq!(snapshot[0]["thresholdRatio"], 0.75);
        assert_eq!(snapshot[0]["liveCount"], 6);
        assert_eq!(snapshot[0]["reservedCount"], 6);
    }

    #[test]
    fn refused_batch_coalesces_in_order_and_keeps_last_transport_for_duplicate() {
        let (events, handler) = event_collector();
        let mut budget =
            WorkspaceMcpBudget::new(Some(1.0), McpBudgetMode::Enforce).with_event_handler(handler);
        budget.begin_bulk_pass();
        budget.try_reserve("held");
        // The reservation itself reaches 100% and emits its independent
        // budget-warning event before the refusal batch is being exercised.
        events.lock().unwrap().clear();
        budget.record_refusal("b", McpBudgetTransport::Stdio);
        budget.record_refusal("c", McpBudgetTransport::Sse);
        budget.record_refusal("b", McpBudgetTransport::Http);
        assert!(events.lock().unwrap().is_empty());
        budget.end_bulk_pass();
        assert_eq!(
            budget.get_refused_server_names(),
            &["b".to_owned(), "c".to_owned()]
        );

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        let McpBudgetEvent::RefusedBatch {
            refused_servers,
            live_count,
            reserved_count,
            mode,
            ..
        } = &events[0]
        else {
            panic!("expected refused_batch event");
        };
        assert_eq!(*live_count, 1);
        assert_eq!(*reserved_count, 1);
        assert_eq!(*mode, McpBudgetMode::Enforce);
        assert_eq!(refused_servers[0].name, "b");
        assert_eq!(refused_servers[0].transport, McpBudgetTransport::Http);
        assert_eq!(refused_servers[0].reason, McpRefusalReason::BudgetExhausted);
        assert_eq!(refused_servers[1].name, "c");
        assert_eq!(refused_servers[1].transport, McpBudgetTransport::Sse);

        let serialized = serde_json::to_value(&events[0]).unwrap();
        assert_eq!(serialized["kind"], "refused_batch");
        assert_eq!(serialized["mode"], "enforce");
        assert_eq!(serialized["refusedServers"][0]["name"], "b");
        assert_eq!(serialized["refusedServers"][0]["transport"], "http");
        assert_eq!(
            serialized["refusedServers"][0]["reason"],
            "budget_exhausted"
        );
    }

    #[test]
    fn out_of_pass_refusal_flushes_singleton_batch_and_snapshot() {
        let (events, handler) = event_collector();
        let mut budget =
            WorkspaceMcpBudget::new(Some(0.0), McpBudgetMode::Enforce).with_event_handler(handler);
        budget.record_refusal("lazy", McpBudgetTransport::Unknown);
        assert_eq!(budget.get_refused_server_names(), &["lazy".to_owned()]);
        assert_eq!(events.lock().unwrap().len(), 1);
    }

    #[test]
    fn nested_passes_only_clear_and_flush_at_outer_boundaries() {
        let (events, handler) = event_collector();
        let mut budget =
            WorkspaceMcpBudget::new(Some(1.0), McpBudgetMode::Enforce).with_event_handler(handler);
        budget.begin_bulk_pass();
        budget.record_refusal("first", McpBudgetTransport::Stdio);
        budget.end_bulk_pass();
        assert_eq!(budget.get_refused_server_names(), &["first".to_owned()]);

        budget.begin_bulk_pass();
        budget.record_refusal("second", McpBudgetTransport::Sdk);
        budget.begin_bulk_pass();
        assert!(budget.get_refused_server_names().is_empty());
        budget.end_bulk_pass();
        assert_eq!(events.lock().unwrap().len(), 1);
        budget.end_bulk_pass();
        assert_eq!(events.lock().unwrap().len(), 2);
        assert_eq!(budget.get_refused_server_names(), &["second".to_owned()]);
    }

    #[test]
    fn off_mode_discards_callback_and_callback_panics_do_not_escape() {
        let panic_called = Arc::new(Mutex::new(false));
        let marker = Arc::clone(&panic_called);
        let mut off = WorkspaceMcpBudget::new(Some(1.0), McpBudgetMode::Off)
            .with_event_handler(move |_| *marker.lock().unwrap() = true);
        off.try_reserve("a");
        off.try_reserve("b");
        assert!(!*panic_called.lock().unwrap());

        let mut budget = WorkspaceMcpBudget::new(Some(1.0), McpBudgetMode::Warn)
            .with_event_handler(|_| panic!("consumer failure"));
        assert_eq!(budget.try_reserve("a"), ReserveResult::Reserved);
        assert_eq!(budget.get_reserved_count(), 1);
    }

    #[test]
    fn getters_return_immutable_or_detached_snapshots() {
        let mut budget = WorkspaceMcpBudget::new(Some(3.0), McpBudgetMode::Warn);
        budget.try_reserve("a");
        budget.try_reserve("b");
        let mut slots = budget.get_reserved_slots();
        slots.push("changed".to_owned());
        assert_eq!(
            budget.get_reserved_slots(),
            vec!["a".to_owned(), "b".to_owned()]
        );
        assert_eq!(budget.get_mode(), McpBudgetMode::Warn);
        assert_eq!(budget.get_budget(), Some(3.0));
    }

    #[test]
    fn unmatched_end_and_non_enforce_refusals_are_noops() {
        let (events, handler) = event_collector();
        let mut budget =
            WorkspaceMcpBudget::new(Some(1.0), McpBudgetMode::Warn).with_event_handler(handler);
        budget.end_bulk_pass();
        budget.record_refusal("ignored", McpBudgetTransport::Stdio);
        assert!(budget.get_refused_server_names().is_empty());
        assert!(events.lock().unwrap().is_empty());
    }

    #[test]
    fn reason_and_transport_json_use_the_source_string_literals() {
        assert_eq!(
            serde_json::to_value(McpBudgetTransport::Websocket).unwrap(),
            json!("websocket")
        );
        assert_eq!(
            serde_json::to_value(McpRefusalReason::BudgetExhausted).unwrap(),
            json!("budget_exhausted")
        );
    }
}
