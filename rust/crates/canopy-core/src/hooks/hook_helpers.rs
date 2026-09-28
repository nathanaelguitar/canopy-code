//! Small, host-neutral helpers shared by hook event construction.
//!
//! These functions mirror `context-usage.ts` and `detectTodoChanges` from
//! `packages/core/src/hooks/types.ts`.

use std::collections::HashMap;

use crate::tools::todo_write::{TodoItem, TodoStatus};

use super::event_inputs::ContextUsageData;

/// Builds Stop-event context usage data when both measurements are valid.
///
/// This matches the TypeScript helper: missing, non-finite, zero, or negative
/// context sizes and input-token counts produce no context usage payload.
pub fn build_context_usage(
    context_window_size: Option<f64>,
    input_tokens: f64,
) -> Option<ContextUsageData> {
    let context_window_size = context_window_size?;
    if !context_window_size.is_finite()
        || context_window_size <= 0.0
        || !input_tokens.is_finite()
        || input_tokens <= 0.0
    {
        return None;
    }

    Some(ContextUsageData {
        context_usage: input_tokens / context_window_size,
        context_limit: context_window_size,
        input_tokens,
    })
}

/// The todo items newly created or newly marked complete between two lists.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TodoChanges {
    pub created: Vec<TodoItem>,
    pub completed: Vec<TodoItem>,
}

/// Detects newly added todos and transitions into the completed state.
///
/// The output follows `new_todos` order. As in JavaScript's `Map` construction,
/// duplicate IDs in `old_todos` use the last old item; duplicate IDs in
/// `new_todos` are examined independently and may therefore appear more than
/// once in the result.
pub fn detect_todo_changes(old_todos: &[TodoItem], new_todos: &[TodoItem]) -> TodoChanges {
    let old_by_id = old_todos
        .iter()
        .map(|todo| (todo.id.as_str(), todo))
        .collect::<HashMap<_, _>>();

    let mut changes = TodoChanges::default();
    for todo in new_todos {
        match old_by_id.get(todo.id.as_str()) {
            None => changes.created.push(todo.clone()),
            Some(old)
                if old.status != TodoStatus::Completed && todo.status == TodoStatus::Completed =>
            {
                changes.completed.push(todo.clone());
            }
            Some(_) => {}
        }
    }
    changes
}
