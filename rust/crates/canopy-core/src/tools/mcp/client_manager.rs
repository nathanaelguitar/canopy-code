//! Per-session MCP manager layered over [`super::transport_pool`].
//!
//! Discovery passes are serialized and diffed by server/transport identity.
//! A manager owns exactly one pool reference per configured server and releases
//! those references deterministically on removal or shutdown.

use super::client_runtime::{McpClientError, McpRequestOptions};
use super::pool_key::{connection_id_of, mcp_transport_of};
use super::transport_pool::{McpPooledConnection, McpTransportPool, McpTransportPoolError};
use super::workspace_budget::{
    McpBudgetMode, McpBudgetTransport, ReserveResult, WorkspaceMcpBudget,
};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

#[derive(Debug, Error)]
pub enum McpClientManagerError {
    #[error("MCP server '{0}' is not connected for this session")]
    NotConnected(String),
    #[error(transparent)]
    Client(#[from] McpClientError),
    #[error(transparent)]
    Pool(#[from] McpTransportPoolError),
}

#[derive(Clone, Debug, Default)]
pub struct McpManagerDiscoveryReport {
    pub snapshots: HashMap<String, super::client_runtime::McpDiscoverySnapshot>,
    /// Discovery is best effort across configured servers; failures are
    /// reported per server and do not discard successful siblings.
    pub errors: HashMap<String, String>,
}

pub struct McpClientManager {
    pool: McpTransportPool,
    session_id: String,
    connections: Mutex<HashMap<String, Arc<McpPooledConnection>>>,
    workspace_budget: Option<Arc<Mutex<WorkspaceMcpBudget>>>,
    budget_reservations: Mutex<HashMap<String, u64>>,
    lifecycle: Mutex<ManagerLifecycle>,
    discovery_lock: AsyncMutex<()>,
}

#[derive(Default)]
struct ManagerLifecycle {
    next_ticket: u64,
    active_by_name: HashMap<String, HashSet<u64>>,
    invalidated: HashSet<u64>,
}

impl ManagerLifecycle {
    fn register(&mut self, server_name: &str) -> u64 {
        self.next_ticket = self.next_ticket.wrapping_add(1).max(1);
        let ticket = self.next_ticket;
        self.active_by_name
            .entry(server_name.to_owned())
            .or_default()
            .insert(ticket);
        ticket
    }

    fn is_active(&self, server_name: &str, ticket: u64) -> bool {
        !self.invalidated.contains(&ticket)
            && self
                .active_by_name
                .get(server_name)
                .is_some_and(|tickets| tickets.contains(&ticket))
    }

    fn finish(&mut self, server_name: &str, ticket: u64) {
        if let Some(tickets) = self.active_by_name.get_mut(server_name) {
            tickets.remove(&ticket);
            if tickets.is_empty() {
                self.active_by_name.remove(server_name);
            }
        }
        self.invalidated.remove(&ticket);
    }

    fn invalidate_name(&mut self, server_name: &str) {
        if let Some(tickets) = self.active_by_name.get(server_name) {
            self.invalidated.extend(tickets.iter().copied());
        }
    }

    fn invalidate_all(&mut self) {
        self.invalidated.extend(
            self.active_by_name
                .values()
                .flat_map(|tickets| tickets.iter().copied()),
        );
    }
}

impl McpClientManager {
    pub fn new(pool: McpTransportPool, session_id: impl Into<String>) -> Self {
        Self {
            pool,
            session_id: session_id.into(),
            connections: Mutex::new(HashMap::new()),
            workspace_budget: None,
            budget_reservations: Mutex::new(HashMap::new()),
            lifecycle: Mutex::new(ManagerLifecycle::default()),
            discovery_lock: AsyncMutex::new(()),
        }
    }

    /// Attach the workspace-shared admission ledger. Hosts should pass the
    /// same `Arc` to every session manager in a workspace.
    pub fn with_workspace_budget(mut self, budget: Arc<Mutex<WorkspaceMcpBudget>>) -> Self {
        self.workspace_budget = Some(budget);
        self
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Connect servers concurrently, coalesce concurrent discovery passes,
    /// retain unchanged handles, and release stale or removed server refs.
    pub async fn discover_all(&self, servers: &Map<String, Value>) -> McpManagerDiscoveryReport {
        let _discovery = self.discovery_lock.lock().await;
        let _budget_pass = self.workspace_budget.as_ref().map(|budget| {
            lock(budget).begin_bulk_pass();
            WorkspaceBudgetBulkPass {
                budget: Arc::clone(budget),
            }
        });
        let desired = servers
            .iter()
            .map(|(name, config)| (name.clone(), connection_id_of(name, config)))
            .collect::<HashMap<_, _>>();
        let candidates = {
            let mut lifecycle = lock(&self.lifecycle);
            let mut connections = lock(&self.connections);
            let stale_names = connections
                .iter()
                .filter(|(name, connection)| {
                    desired
                        .get(*name)
                        .is_none_or(|id| id != connection.transport_id())
                })
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>();
            for name in stale_names {
                if let Some(connection) = connections.remove(&name) {
                    connection.release();
                    if !desired.contains_key(&name) {
                        self.release_budget_name_locked(&name);
                    }
                }
            }
            let mut candidates = Vec::new();
            for (name, config) in servers {
                if let Some(connection) = connections.get(name) {
                    if desired
                        .get(name)
                        .is_some_and(|id| id == connection.transport_id())
                    {
                        connection.update_config(config.clone());
                    }
                } else {
                    let ticket = lifecycle.register(name);
                    candidates.push((name.clone(), config.clone(), ticket));
                }
            }
            candidates
        };
        let mut report = McpManagerDiscoveryReport::default();
        let mut pending = Vec::with_capacity(candidates.len());
        for (name, config, ticket) in candidates {
            let mut acquisition = PendingAcquisition::new(self, name.clone(), ticket);
            match self.try_reserve_budget_name(&name, &config, ticket) {
                BudgetAdmission::Admitted(lease) => {
                    acquisition.budget_lease = lease;
                    pending.push((name, config, acquisition));
                }
                BudgetAdmission::Refused(reason) => {
                    report.errors.insert(name, reason);
                    acquisition.abort();
                }
                BudgetAdmission::Invalidated => acquisition.abort(),
            }
        }
        let pool = self.pool.clone();
        let session_id = self.session_id.clone();
        let acquired = futures_util::future::join_all(pending.into_iter().map(
            |(name, config, acquisition)| {
                let pool = pool.clone();
                let session_id = session_id.clone();
                async move {
                    let result = pool.acquire(name.clone(), config, session_id).await;
                    (name, result, acquisition)
                }
            },
        ))
        .await;

        for (name, result, acquisition) in acquired {
            match result {
                Ok(connection) => {
                    if let Some(snapshot) = acquisition.commit(connection) {
                        report.snapshots.insert(name, snapshot);
                    }
                }
                Err(error) => {
                    acquisition.abort();
                    report.errors.insert(name, error.to_string());
                }
            }
        }
        for (name, connection) in lock(&self.connections).iter() {
            report
                .snapshots
                .entry(name.clone())
                .or_insert_with(|| connection.snapshot());
        }
        report
    }

    pub fn connection(&self, server_name: &str) -> Option<Arc<McpPooledConnection>> {
        lock(&self.connections).get(server_name).cloned()
    }

    pub async fn call_tool(
        &self,
        server_name: &str,
        tool_name: &str,
        arguments: &Map<String, Value>,
        options: McpRequestOptions,
    ) -> Result<Value, McpClientManagerError> {
        let connection = self
            .connection(server_name)
            .ok_or_else(|| McpClientManagerError::NotConnected(server_name.to_owned()))?;
        connection
            .call_tool(tool_name, arguments, options)
            .await
            .map_err(McpClientManagerError::from)
    }

    pub async fn read_resource(
        &self,
        server_name: &str,
        uri: &str,
        options: McpRequestOptions,
    ) -> Result<Value, McpClientManagerError> {
        let connection = self
            .connection(server_name)
            .ok_or_else(|| McpClientManagerError::NotConnected(server_name.to_owned()))?;
        connection
            .read_resource(uri, options)
            .await
            .map_err(McpClientManagerError::from)
    }

    pub fn release_server(&self, server_name: &str) {
        let mut lifecycle = lock(&self.lifecycle);
        lifecycle.invalidate_name(server_name);
        if let Some(connection) = lock(&self.connections).remove(server_name) {
            connection.release();
        }
        self.release_budget_name_locked(server_name);
    }

    pub fn stop(&self) {
        let mut lifecycle = lock(&self.lifecycle);
        lifecycle.invalidate_all();
        let mut connections = lock(&self.connections);
        for connection in connections.values() {
            connection.release();
        }
        connections.clear();
        self.release_all_budget_names_locked();
        self.pool.release_session(&self.session_id);
    }

    fn try_reserve_budget_name(
        &self,
        server_name: &str,
        config: &Value,
        ticket: u64,
    ) -> BudgetAdmission {
        let lifecycle = lock(&self.lifecycle);
        if !lifecycle.is_active(server_name, ticket) {
            return BudgetAdmission::Invalidated;
        }
        let mut held_names = lock(&self.budget_reservations);
        if let Some(lease) = held_names.get(server_name) {
            return BudgetAdmission::Admitted(Some(*lease));
        }
        let Some(workspace_budget) = self.workspace_budget.as_ref() else {
            return BudgetAdmission::Admitted(None);
        };
        let mut budget = lock(workspace_budget);
        if budget.get_mode() == McpBudgetMode::Off || budget.get_budget().is_none() {
            return BudgetAdmission::Admitted(None);
        }
        match budget.try_reserve_ref(server_name.to_owned()) {
            ReserveResult::Refused => {
                let reserved_count = budget.get_reserved_count();
                let limit = budget.get_budget().unwrap_or_default();
                budget.record_refusal(
                    server_name.to_owned(),
                    McpBudgetTransport::from(mcp_transport_of(config)),
                );
                BudgetAdmission::Refused(format!(
                    "MCP workspace client budget exhausted (budget={limit}, reserved_count={reserved_count})"
                ))
            }
            ReserveResult::Reserved | ReserveResult::AlreadyHeld => {
                held_names.insert(server_name.to_owned(), ticket);
                BudgetAdmission::Admitted(Some(ticket))
            }
        }
    }

    fn commit_acquisition(
        &self,
        server_name: &str,
        ticket: u64,
        budget_lease: Option<u64>,
        connection: McpPooledConnection,
    ) -> Option<super::client_runtime::McpDiscoverySnapshot> {
        let mut lifecycle = lock(&self.lifecycle);
        let ticket_is_valid = lifecycle.is_active(server_name, ticket);
        let lease_is_valid = budget_lease
            .is_none_or(|lease| lock(&self.budget_reservations).get(server_name) == Some(&lease));
        lifecycle.finish(server_name, ticket);
        if !ticket_is_valid || !lease_is_valid {
            self.release_budget_lease_locked(server_name, budget_lease);
            drop(lifecycle);
            connection.release();
            return None;
        }

        let snapshot = connection.snapshot();
        let previous = lock(&self.connections).insert(server_name.to_owned(), Arc::new(connection));
        if let Some(previous) = previous {
            previous.release();
        }
        Some(snapshot)
    }

    fn abort_acquisition(&self, server_name: &str, ticket: u64, budget_lease: Option<u64>) {
        let mut lifecycle = lock(&self.lifecycle);
        lifecycle.finish(server_name, ticket);
        self.release_budget_lease_locked(server_name, budget_lease);
    }

    fn release_budget_name_locked(&self, server_name: &str) {
        let lease = lock(&self.budget_reservations).remove(server_name);
        if let (Some(_), Some(workspace_budget)) = (lease, self.workspace_budget.as_ref()) {
            lock(workspace_budget).release_ref(server_name);
        }
    }

    fn release_budget_lease_locked(&self, server_name: &str, lease: Option<u64>) {
        let Some(lease) = lease else {
            return;
        };
        let mut held_names = lock(&self.budget_reservations);
        if held_names.get(server_name) == Some(&lease) {
            held_names.remove(server_name);
            if let Some(workspace_budget) = self.workspace_budget.as_ref() {
                lock(workspace_budget).release_ref(server_name);
            }
        }
    }

    fn release_all_budget_names_locked(&self) {
        let names = lock(&self.budget_reservations)
            .drain()
            .map(|(name, _)| name)
            .collect::<Vec<_>>();
        if let Some(workspace_budget) = self.workspace_budget.as_ref() {
            let mut budget = lock(workspace_budget);
            for server_name in names {
                budget.release_ref(&server_name);
            }
        }
    }
}

impl Drop for McpClientManager {
    fn drop(&mut self) {
        let mut lifecycle = lock(&self.lifecycle);
        lifecycle.invalidate_all();
        let mut connections = lock(&self.connections);
        for connection in connections.values() {
            connection.release();
        }
        connections.clear();
        self.release_all_budget_names_locked();
        self.pool.release_session(&self.session_id);
    }
}

enum BudgetAdmission {
    Admitted(Option<u64>),
    Refused(String),
    Invalidated,
}

struct PendingAcquisition<'a> {
    manager: &'a McpClientManager,
    server_name: String,
    ticket: u64,
    budget_lease: Option<u64>,
    finished: bool,
}

impl<'a> PendingAcquisition<'a> {
    fn new(manager: &'a McpClientManager, server_name: String, ticket: u64) -> Self {
        Self {
            manager,
            server_name,
            ticket,
            budget_lease: None,
            finished: false,
        }
    }

    fn commit(
        mut self,
        connection: McpPooledConnection,
    ) -> Option<super::client_runtime::McpDiscoverySnapshot> {
        let snapshot = self.manager.commit_acquisition(
            &self.server_name,
            self.ticket,
            self.budget_lease,
            connection,
        );
        self.finished = true;
        snapshot
    }

    fn abort(mut self) {
        self.manager
            .abort_acquisition(&self.server_name, self.ticket, self.budget_lease);
        self.finished = true;
    }
}

impl Drop for PendingAcquisition<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.manager
                .abort_acquisition(&self.server_name, self.ticket, self.budget_lease);
            self.finished = true;
        }
    }
}

struct WorkspaceBudgetBulkPass {
    budget: Arc<Mutex<WorkspaceMcpBudget>>,
}

impl Drop for WorkspaceBudgetBulkPass {
    fn drop(&mut self) {
        lock(&self.budget).end_bulk_pass();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}
