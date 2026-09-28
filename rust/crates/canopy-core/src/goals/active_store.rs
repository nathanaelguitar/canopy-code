use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ActiveGoal {
    pub condition: String,
    pub iterations: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deferred_evaluations: Option<f64>,
    pub set_at: f64,
    pub tokens_at_start: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reason: Option<String>,
    pub hook_id: String,
}

pub fn active_goal_equals(left: Option<&ActiveGoal>, right: Option<&ActiveGoal>) -> bool {
    let (Some(left), Some(right)) = (left, right) else {
        return left.is_none() && right.is_none();
    };
    left.condition == right.condition
        && js_number_equals(left.iterations, right.iterations)
        && js_number_equals(left.set_at, right.set_at)
        && js_number_equals(left.tokens_at_start, right.tokens_at_start)
        && left.last_reason == right.last_reason
        && left.hook_id == right.hook_id
}

fn js_number_equals(left: f64, right: f64) -> bool {
    (left.is_finite() && right.is_finite() && left == right)
        || (!left.is_finite() && !right.is_finite())
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalTerminalKind {
    Achieved,
    Aborted,
    Failed,
}

impl GoalTerminalKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Achieved => "achieved",
            Self::Aborted => "aborted",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GoalTerminalEvent {
    pub kind: GoalTerminalKind,
    pub condition: String,
    pub iterations: f64,
    pub duration_ms: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_message: Option<String>,
}

pub type GoalTerminalObserver = Arc<dyn Fn(&GoalTerminalEvent) + Send + Sync + 'static>;

#[derive(Default)]
struct ActiveGoalStore {
    active_goals: HashMap<String, ActiveGoal>,
    observers: HashMap<String, GoalTerminalObserver>,
    last_terminal: HashMap<String, GoalTerminalEvent>,
}

static STORE: OnceLock<Mutex<ActiveGoalStore>> = OnceLock::new();

fn store() -> MutexGuard<'static, ActiveGoalStore> {
    STORE
        .get_or_init(|| Mutex::new(ActiveGoalStore::default()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub fn get_active_goal(session_id: &str) -> Option<ActiveGoal> {
    store().active_goals.get(session_id).cloned()
}

pub fn set_active_goal(session_id: impl Into<String>, goal: ActiveGoal) {
    store().active_goals.insert(session_id.into(), goal);
}

pub fn clear_active_goal(session_id: &str) -> Option<ActiveGoal> {
    store().active_goals.remove(session_id)
}

pub fn record_goal_iteration(
    session_id: &str,
    last_reason: impl Into<String>,
) -> Option<ActiveGoal> {
    let mut store = store();
    let current = store.active_goals.get(session_id)?;
    let updated = ActiveGoal {
        iterations: current.iterations + 1.0,
        deferred_evaluations: Some(0.0),
        last_reason: Some(last_reason.into()),
        ..current.clone()
    };
    store
        .active_goals
        .insert(session_id.to_owned(), updated.clone());
    Some(updated)
}

pub fn record_goal_deferral(session_id: &str) -> Option<ActiveGoal> {
    let mut store = store();
    let current = store.active_goals.get(session_id)?;
    let updated = ActiveGoal {
        deferred_evaluations: Some(current.deferred_evaluations.unwrap_or(0.0) + 1.0),
        ..current.clone()
    };
    store
        .active_goals
        .insert(session_id.to_owned(), updated.clone());
    Some(updated)
}

pub fn reset_goal_deferrals(session_id: &str) -> Option<ActiveGoal> {
    let mut store = store();
    let current = store.active_goals.get(session_id)?;
    if current.deferred_evaluations == Some(0.0) {
        return Some(current.clone());
    }
    let updated = ActiveGoal {
        deferred_evaluations: Some(0.0),
        ..current.clone()
    };
    store
        .active_goals
        .insert(session_id.to_owned(), updated.clone());
    Some(updated)
}

/// Test-only escape hatch. Production code should always scope state by session.
pub fn __reset_active_goal_store_for_tests() {
    let mut store = store();
    store.active_goals.clear();
    store.observers.clear();
    store.last_terminal.clear();
}

pub fn set_goal_terminal_observer(
    session_id: impl Into<String>,
    observer: impl Fn(&GoalTerminalEvent) + Send + Sync + 'static,
) {
    store()
        .observers
        .insert(session_id.into(), Arc::new(observer));
}

pub fn clear_goal_terminal_observer(session_id: &str) {
    store().observers.remove(session_id);
}

pub fn notify_goal_terminal(session_id: &str, event: GoalTerminalEvent) {
    let observer = {
        let mut store = store();
        store
            .last_terminal
            .insert(session_id.to_owned(), event.clone());
        store.observers.get(session_id).cloned()
    };
    if let Some(observer) = observer {
        let _ = catch_unwind(AssertUnwindSafe(|| observer(&event)));
    }
}

pub fn get_last_goal_terminal(session_id: &str) -> Option<GoalTerminalEvent> {
    store().last_terminal.get(session_id).cloned()
}

pub fn set_last_goal_terminal(session_id: impl Into<String>, event: Option<GoalTerminalEvent>) {
    let mut store = store();
    let session_id = session_id.into();
    if let Some(event) = event {
        store.last_terminal.insert(session_id, event);
    } else {
        store.last_terminal.remove(&session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn goal() -> ActiveGoal {
        ActiveGoal {
            condition: "write a hello world script".to_owned(),
            iterations: 0.0,
            deferred_evaluations: None,
            set_at: 1_000.0,
            tokens_at_start: 100.0,
            last_reason: None,
            hook_id: "hook-1".to_owned(),
        }
    }

    fn terminal() -> GoalTerminalEvent {
        GoalTerminalEvent {
            kind: GoalTerminalKind::Achieved,
            condition: "write a hello world script".to_owned(),
            iterations: 2.0,
            duration_ms: 300.0,
            last_reason: Some("verified".to_owned()),
            system_message: None,
        }
    }

    #[test]
    fn stores_and_clears_goals_by_session() {
        assert!(get_active_goal("active-store:missing").is_none());
        set_active_goal("active-store:one", goal());
        let mut second = goal();
        second.condition = "another goal".to_owned();
        set_active_goal("active-store:two", second);

        assert_eq!(
            get_active_goal("active-store:one").unwrap().condition,
            "write a hello world script"
        );
        assert_eq!(
            get_active_goal("active-store:two").unwrap().condition,
            "another goal"
        );
        assert_eq!(
            clear_active_goal("active-store:one").unwrap().condition,
            "write a hello world script"
        );
        assert!(get_active_goal("active-store:one").is_none());
        assert!(clear_active_goal("active-store:missing").is_none());
    }

    #[test]
    fn iteration_deferral_and_reset_preserve_the_source_transitions() {
        assert!(record_goal_iteration("active-store:missing", "noop").is_none());
        assert!(record_goal_deferral("active-store:missing").is_none());
        assert!(reset_goal_deferrals("active-store:missing").is_none());

        set_active_goal("active-store:unset-deferrals", goal());
        assert_eq!(
            reset_goal_deferrals("active-store:unset-deferrals")
                .unwrap()
                .deferred_evaluations,
            Some(0.0)
        );

        set_active_goal("active-store:iterations", goal());
        let deferred = record_goal_deferral("active-store:iterations").unwrap();
        assert_eq!(deferred.deferred_evaluations, Some(1.0));
        let reset = reset_goal_deferrals("active-store:iterations").unwrap();
        assert_eq!(reset.deferred_evaluations, Some(0.0));
        let unchanged = reset_goal_deferrals("active-store:iterations").unwrap();
        assert_eq!(unchanged, reset);

        let iterated =
            record_goal_iteration("active-store:iterations", "still missing tests").unwrap();
        assert_eq!(iterated.iterations, 1.0);
        assert_eq!(iterated.deferred_evaluations, Some(0.0));
        assert_eq!(iterated.last_reason.as_deref(), Some("still missing tests"));
        assert_eq!(get_active_goal("active-store:iterations"), Some(iterated));
    }

    #[test]
    fn equality_ignores_deferred_evaluations_and_matches_json_number_semantics() {
        assert!(active_goal_equals(None, None));
        let left = goal();
        assert!(active_goal_equals(Some(&left), Some(&goal())));
        assert!(active_goal_equals(
            Some(&ActiveGoal {
                deferred_evaluations: Some(1.0),
                ..goal()
            }),
            Some(&ActiveGoal {
                deferred_evaluations: Some(9.0),
                ..goal()
            })
        ));
        assert!(!active_goal_equals(Some(&left), None));

        let non_finite = ActiveGoal {
            iterations: f64::NAN,
            ..goal()
        };
        let infinity = ActiveGoal {
            iterations: f64::INFINITY,
            ..goal()
        };
        assert!(active_goal_equals(Some(&non_finite), Some(&infinity)));
    }

    #[test]
    fn terminal_observers_are_isolated_and_last_event_can_be_restored_or_cleared() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observer_calls = calls.clone();
        set_goal_terminal_observer("active-store:observer", move |_| {
            observer_calls.fetch_add(1, Ordering::SeqCst);
        });
        notify_goal_terminal("active-store:observer", terminal());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            get_last_goal_terminal("active-store:observer"),
            Some(terminal())
        );

        set_goal_terminal_observer("active-store:panic", |_| panic!("observer failure"));
        notify_goal_terminal("active-store:panic", terminal());
        assert_eq!(
            get_last_goal_terminal("active-store:panic"),
            Some(terminal())
        );
        clear_goal_terminal_observer("active-store:panic");

        set_last_goal_terminal("active-store:restored", Some(terminal()));
        assert_eq!(
            get_last_goal_terminal("active-store:restored"),
            Some(terminal())
        );
        set_last_goal_terminal("active-store:restored", None);
        assert!(get_last_goal_terminal("active-store:restored").is_none());
    }
}
