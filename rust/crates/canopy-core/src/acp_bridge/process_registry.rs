//! Tracks ACP child starts and shutdown as one admission-controlled set.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use tokio::sync::watch;
use uuid::Uuid;

use crate::acp_bridge::channel::{AcpChannelExitInfo, ChannelFuture};

type TerminateFn = dyn Fn() -> ChannelFuture<'static, Result<(), String>> + Send + Sync;
type KillSyncFn = dyn Fn() + Send + Sync;

struct ChildControl {
    exit: watch::Receiver<Option<AcpChannelExitInfo>>,
    terminate: Box<TerminateFn>,
    kill_sync: Box<KillSyncFn>,
}

#[derive(Clone)]
pub struct TrackedChildProcess {
    inner: Arc<ChildControl>,
}
impl TrackedChildProcess {
    pub fn new(
        exit: watch::Receiver<Option<AcpChannelExitInfo>>,
        terminate: impl Fn() -> ChannelFuture<'static, Result<(), String>> + Send + Sync + 'static,
        kill_sync: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(ChildControl {
                exit,
                terminate: Box::new(terminate),
                kill_sync: Box::new(kill_sync),
            }),
        }
    }
    pub async fn terminate(&self) -> Result<(), String> {
        (self.inner.terminate)().await
    }
    pub fn kill_sync(&self) {
        (self.inner.kill_sync)();
    }
    pub fn exit_info(&self) -> Option<AcpChannelExitInfo> {
        self.inner.exit.borrow().clone()
    }
}

#[derive(Default)]
struct State {
    reservations: HashSet<Uuid>,
    children: HashMap<Uuid, TrackedChildProcess>,
    draining: bool,
}
#[derive(Clone, Default)]
pub struct ProcessRegistry {
    state: Arc<Mutex<State>>,
}

pub struct ProcessReservation {
    registry: Weak<Mutex<State>>,
    id: Uuid,
    settled: bool,
}

impl ProcessRegistry {
    pub fn reserve(&self) -> Result<ProcessReservation, &'static str> {
        let mut state = lock(&self.state);
        if state.draining {
            return Err("ACP process registry is draining");
        }
        let id = Uuid::new_v4();
        state.reservations.insert(id);
        Ok(ProcessReservation {
            registry: Arc::downgrade(&self.state),
            id,
            settled: false,
        })
    }
    pub fn active_process_count(&self) -> usize {
        lock(&self.state).children.len()
    }
    pub fn committed_process_count(&self) -> usize {
        let state = lock(&self.state);
        state.children.len() + state.reservations.len()
    }
    pub fn kill_all_sync(&self) {
        let children = {
            let mut state = lock(&self.state);
            state.draining = true;
            state.children.values().cloned().collect::<Vec<_>>()
        };
        for child in children {
            child.kill_sync();
        }
    }
    pub async fn shutdown(&self) -> Result<(), String> {
        let children = {
            let mut state = lock(&self.state);
            state.draining = true;
            state.children.values().cloned().collect::<Vec<_>>()
        };
        let mut failures = Vec::new();
        for child in children {
            if let Err(error) = child.terminate().await {
                failures.push(error);
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "ACP child process shutdown failed: {}",
                failures.join("; ")
            ))
        }
    }
}

impl ProcessReservation {
    /// Commit the child before exposing it to concurrent shutdown or memory
    /// accounting. Its exit watcher removes the committed slot automatically.
    pub fn attach(
        mut self,
        child: TrackedChildProcess,
    ) -> Result<TrackedChildProcess, &'static str> {
        let Some(registry) = self.registry.upgrade() else {
            return Err("ACP process registry was dropped");
        };
        {
            let mut state = lock(&registry);
            if !state.reservations.remove(&self.id) {
                return Err("ACP process reservation is no longer active");
            }
            state.children.insert(self.id, child.clone());
            self.settled = true;
            if state.draining {
                child.kill_sync();
            }
        }
        let weak = Arc::downgrade(&registry);
        let id = self.id;
        let mut exited = child.inner.exit.clone();
        tokio::spawn(async move {
            if exited.borrow().is_none() {
                let _ = exited.changed().await;
            }
            if let Some(registry) = weak.upgrade() {
                lock(&registry).children.remove(&id);
            }
        });
        Ok(child)
    }
    pub fn cancel(&mut self) {
        if self.settled {
            return;
        }
        if let Some(registry) = self.registry.upgrade() {
            lock(&registry).reservations.remove(&self.id);
        }
        self.settled = true;
    }
}
impl Drop for ProcessReservation {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reservations_count_before_attach_and_exit_releases_slot() {
        let registry = ProcessRegistry::default();
        let reservation = registry.reserve().unwrap();
        assert_eq!(registry.committed_process_count(), 1);
        let (exit_tx, exit) = watch::channel(None);
        let child = reservation
            .attach(TrackedChildProcess::new(
                exit,
                || Box::pin(async { Ok(()) }),
                || {},
            ))
            .unwrap();
        assert_eq!(registry.active_process_count(), 1);
        exit_tx
            .send(Some(AcpChannelExitInfo {
                exit_code: Some(0),
                signal_code: None,
            }))
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(registry.committed_process_count(), 0);
        drop(child);
    }
}
