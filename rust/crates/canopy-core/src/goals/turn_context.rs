//! Async task-local Goal turn context.
//!
//! This mirrors the source `AsyncLocalStorage<GoalTurnPermit>` helper. Tokio
//! task-local values follow an async scope, but `tokio::spawn` does not inherit
//! them automatically; capture and use [`GoalTurnContext::spawn`] when a child
//! task must retain the current permit.

use std::future::Future;

use crate::goals::protocol::GoalTurnPermit;

tokio::task_local! {
    static GOAL_TURN_CONTEXT: Option<GoalTurnPermit>;
}

/// A captured Goal turn permit that can be reapplied to async work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoalTurnContext {
    permit: GoalTurnPermit,
}

impl GoalTurnContext {
    /// Capture the permit bound to the current Tokio task, if any.
    pub fn capture() -> Option<Self> {
        GOAL_TURN_CONTEXT
            .try_with(Clone::clone)
            .ok()
            .flatten()
            .map(|permit| Self { permit })
    }

    /// Return the captured permit.
    pub fn permit(&self) -> &GoalTurnPermit {
        &self.permit
    }

    /// Run a future with this turn permit bound to the current task.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        GOAL_TURN_CONTEXT
            .scope(Some(self.permit.clone()), future)
            .await
    }

    /// Spawn a child task that inherits this captured turn permit.
    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(GOAL_TURN_CONTEXT.scope(Some(self.permit.clone()), future))
    }
}

/// Run an async operation inside a Goal turn scope.
pub async fn run_with_goal_turn_context<F: Future>(permit: GoalTurnPermit, future: F) -> F::Output {
    GOAL_TURN_CONTEXT.scope(Some(permit), future).await
}

/// Run an operation with Goal context explicitly cleared.
///
/// This mirrors `AsyncLocalStorage.exit`: a tool call without a permit must
/// not inherit an enclosing turn's permit while it executes.
pub async fn run_without_goal_turn_context<F: Future>(future: F) -> F::Output {
    GOAL_TURN_CONTEXT.scope(None, future).await
}

/// Return a clone of the permit bound to the current Tokio task, if any.
pub fn current_goal_turn_permit() -> Option<GoalTurnPermit> {
    GOAL_TURN_CONTEXT.try_with(Clone::clone).ok().flatten()
}

/// Spawn a child task that inherits the current Goal turn context when one is
/// present. If called outside a Goal scope, it behaves like `tokio::spawn`.
pub fn spawn_with_current_goal_turn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match GOAL_TURN_CONTEXT.try_with(Clone::clone) {
        Ok(Some(permit)) => tokio::spawn(GOAL_TURN_CONTEXT.scope(Some(permit), future)),
        Err(_) => tokio::spawn(future),
        Ok(None) => tokio::spawn(GOAL_TURN_CONTEXT.scope(None, future)),
    }
}
