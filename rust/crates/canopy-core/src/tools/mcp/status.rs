//! MCP server connection status and process-wide status snapshots.
//!
//! Mirrors `packages/core/src/tools/mcp-status.ts`. Listener callbacks run
//! after the registry lock is released and receive a snapshot of the listener
//! list taken when the update began.
//!
//! The Rust API requires listeners to be `Send + Sync` and removes them by
//! registration ID; the TypeScript API removes the first matching callback
//! reference. Concurrent Rust updates may interleave listener delivery,
//! whereas JavaScript dispatch is synchronous on its event loop. The callback
//! itself is synchronous in both APIs.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum McpClientStatus {
    Disconnected,
    Connecting,
    Connected,
}

/// Name matching the TypeScript registry's status type.
pub type McpServerStatus = McpClientStatus;

impl McpClientStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Connecting => "connecting",
            Self::Connected => "connected",
        }
    }
}

pub type McpStatusListenerId = u64;
pub type McpStatusListener = Arc<dyn Fn(&str, Option<McpClientStatus>) + Send + Sync>;

#[derive(Default)]
struct McpStatusRegistryInner {
    statuses: HashMap<String, McpClientStatus>,
    listeners: Vec<(McpStatusListenerId, McpStatusListener)>,
    next_listener_id: McpStatusListenerId,
}

/// Thread-safe MCP status registry. Removing a known server sends `None` to
/// the current listener snapshot; removing an unknown server is a no-op.
#[derive(Default)]
pub struct McpServerStatusRegistry {
    inner: Mutex<McpStatusRegistryInner>,
}

impl McpServerStatusRegistry {
    pub fn add_listener(&self, listener: McpStatusListener) -> McpStatusListenerId {
        let mut inner = lock(&self.inner);
        let id = inner.next_listener_id;
        inner.next_listener_id = inner.next_listener_id.saturating_add(1);
        inner.listeners.push((id, listener));
        id
    }

    /// Remove a listener by its registration ID. A listener already copied
    /// into an in-flight notification snapshot will still receive that event.
    pub fn remove_listener(&self, listener_id: McpStatusListenerId) {
        let mut inner = lock(&self.inner);
        if let Some(index) = inner
            .listeners
            .iter()
            .position(|(id, _)| *id == listener_id)
        {
            inner.listeners.remove(index);
        }
    }

    pub fn update(&self, server_name: &str, status: McpClientStatus) {
        let listeners = {
            let mut inner = lock(&self.inner);
            inner.statuses.insert(server_name.to_owned(), status);
            listener_snapshot(&inner)
        };
        notify(listeners, server_name, Some(status));
    }

    pub fn remove(&self, server_name: &str) {
        let listeners = {
            let mut inner = lock(&self.inner);
            if inner.statuses.remove(server_name).is_none() {
                return;
            }
            listener_snapshot(&inner)
        };
        notify(listeners, server_name, None);
    }

    /// Return the stored status, or `Disconnected` when no status is stored.
    pub fn get(&self, server_name: &str) -> McpClientStatus {
        lock(&self.inner)
            .statuses
            .get(server_name)
            .copied()
            .unwrap_or(McpClientStatus::Disconnected)
    }

    /// Return an owned snapshot. Removing or changing entries in the returned
    /// map cannot mutate registry state.
    pub fn get_all(&self) -> HashMap<String, McpClientStatus> {
        lock(&self.inner).statuses.clone()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn listener_snapshot(inner: &McpStatusRegistryInner) -> Vec<McpStatusListener> {
    inner
        .listeners
        .iter()
        .map(|(_, listener)| Arc::clone(listener))
        .collect()
}

fn notify(listeners: Vec<McpStatusListener>, server_name: &str, status: Option<McpClientStatus>) {
    for listener in listeners {
        listener(server_name, status);
    }
}

static MCP_SERVER_STATUS_REGISTRY: OnceLock<McpServerStatusRegistry> = OnceLock::new();

/// Process-wide registry used by MCP client runtimes and UI status views.
pub fn mcp_server_status_registry() -> &'static McpServerStatusRegistry {
    MCP_SERVER_STATUS_REGISTRY.get_or_init(McpServerStatusRegistry::default)
}

/// Register a callback for process-wide MCP status updates.
pub fn add_mcp_status_change_listener(listener: McpStatusListener) -> McpStatusListenerId {
    mcp_server_status_registry().add_listener(listener)
}

/// Remove a process-wide status callback by its registration ID.
pub fn remove_mcp_status_change_listener(listener_id: McpStatusListenerId) {
    mcp_server_status_registry().remove_listener(listener_id);
}

/// Update one server's process-wide status and notify the listener snapshot.
pub fn update_mcp_server_status(server_name: &str, status: McpClientStatus) {
    mcp_server_status_registry().update(server_name, status);
}

/// Remove a server from the process-wide registry.
pub fn remove_mcp_server_status(server_name: &str) {
    mcp_server_status_registry().remove(server_name);
}

/// Get a server's process-wide status, defaulting to `Disconnected`.
pub fn get_mcp_server_status(server_name: &str) -> McpClientStatus {
    mcp_server_status_registry().get(server_name)
}

/// Get an owned snapshot of all tracked process-wide server statuses.
pub fn get_all_mcp_server_statuses() -> HashMap<String, McpClientStatus> {
    mcp_server_status_registry().get_all()
}

#[cfg(test)]
mod tests {
    use super::{McpClientStatus, McpServerStatusRegistry};
    use std::sync::{Arc, Mutex};

    #[test]
    fn update_remove_and_get_all_preserve_registry_snapshots() {
        let registry = McpServerStatusRegistry::default();
        let events = Arc::new(Mutex::new(Vec::new()));
        let event_log = Arc::clone(&events);
        registry.add_listener(Arc::new(move |name, status| {
            event_log.lock().unwrap().push((name.to_owned(), status));
        }));

        assert_eq!(registry.get("alpha"), McpClientStatus::Disconnected);
        registry.update("alpha", McpClientStatus::Connecting);
        registry.update("alpha", McpClientStatus::Connected);

        let mut snapshot = registry.get_all();
        assert_eq!(snapshot.get("alpha"), Some(&McpClientStatus::Connected));
        snapshot.remove("alpha");
        assert_eq!(registry.get("alpha"), McpClientStatus::Connected);

        registry.remove("alpha");
        registry.remove("missing");

        assert_eq!(registry.get("alpha"), McpClientStatus::Disconnected);
        assert!(registry.get_all().is_empty());
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                ("alpha".to_owned(), Some(McpClientStatus::Connecting)),
                ("alpha".to_owned(), Some(McpClientStatus::Connected)),
                ("alpha".to_owned(), None),
            ]
        );
    }

    #[test]
    fn listener_mutation_uses_a_stable_dispatch_snapshot() {
        let registry = Arc::new(McpServerStatusRegistry::default());
        let events = Arc::new(Mutex::new(Vec::new()));

        let second_listener = {
            let events = Arc::clone(&events);
            Arc::new(move |_: &str, _: Option<McpClientStatus>| {
                events.lock().unwrap().push("second");
            })
        };
        let second_id = registry.add_listener(second_listener);

        let registry_for_listener = Arc::clone(&registry);
        let events_for_listener = Arc::clone(&events);
        let third_id = Arc::new(Mutex::new(None));
        let third_id_for_listener = Arc::clone(&third_id);
        registry.add_listener(Arc::new(move |_, _| {
            events_for_listener.lock().unwrap().push("first");
            registry_for_listener.remove_listener(second_id);
            let events_for_third = Arc::clone(&events_for_listener);
            let new_id = registry_for_listener.add_listener(Arc::new(move |_, _| {
                events_for_third.lock().unwrap().push("third");
            }));
            *third_id_for_listener.lock().unwrap() = Some(new_id);
        }));

        registry.update("alpha", McpClientStatus::Connecting);
        assert_eq!(*events.lock().unwrap(), vec!["second", "first"]);

        events.lock().unwrap().clear();
        registry.update("alpha", McpClientStatus::Connected);
        assert_eq!(*events.lock().unwrap(), vec!["first", "third"]);

        if let Some(id) = *third_id.lock().unwrap() {
            registry.remove_listener(id);
        }
    }
}
