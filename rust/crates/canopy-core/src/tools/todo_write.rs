use std::collections::{HashMap, HashSet, VecDeque};
#[cfg(unix)]
use std::fs::File;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::session_paths::is_valid_session_id;
use crate::tool_response_finalizer::ToolExecutionOutput;
use crate::utils::xml::escape_system_reminder_tags;

const MAX_TODO_FILE_BYTES: usize = 4 * 1024 * 1024;
const MAX_TODO_ITEMS: usize = 2_000;
const MAX_ACTIVE_TODO_CONTEXT_CHARS: usize = 800;
const MAX_CONCURRENT_TODO_VALIDATION_HOOKS: usize = 16;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TodoStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TodoItem {
    pub id: String,
    pub content: String,
    pub status: TodoStatus,
    #[serde(rename = "blockedBy", skip_serializing_if = "Option::is_none")]
    pub blocked_by: Option<Vec<String>>,
    #[serde(flatten, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct TodoPlanState {
    #[serde(skip_serializing_if = "Option::is_none")]
    plan_id: Option<String>,
    todos: Vec<TodoItem>,
    session_id: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
struct TodoChanges {
    created: Vec<TodoItem>,
    completed: Vec<TodoItem>,
}

/// The phase in which a todo lifecycle hook is being dispatched.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TodoWriteHookPhase {
    /// Hooks may approve or block a proposed todo change before it is saved.
    Validation,
    /// Hooks run after persistence and are intended for side effects.
    PostWrite,
}

/// A todo lifecycle event passed to an injected [`TodoWriteHookRuntime`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TodoWriteHookEvent {
    TodoCreated {
        todo: TodoItem,
    },
    TodoCompleted {
        todo: TodoItem,
        previous_status: TodoStatus,
    },
}

/// A validation hook's decision. Post-write decisions are ignored.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TodoWriteHookDecision {
    Allow,
    Block,
}

/// The result returned by a todo lifecycle hook.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TodoWriteHookResult {
    pub decision: Option<TodoWriteHookDecision>,
    pub reason: Option<String>,
}

impl TodoWriteHookResult {
    pub fn allow() -> Self {
        Self {
            decision: Some(TodoWriteHookDecision::Allow),
            reason: None,
        }
    }

    pub fn block(reason: impl Into<String>) -> Self {
        Self {
            decision: Some(TodoWriteHookDecision::Block),
            reason: Some(reason.into()),
        }
    }
}

/// Host-supplied executor for todo lifecycle hooks.
///
/// Validation runs created hooks concurrently, then completed hooks
/// concurrently, using up to 16 scoped threads per batch. Post-write calls run
/// sequentially: created hooks first, then completed hooks.
pub trait TodoWriteHookRuntime: Send + Sync {
    fn execute(
        &self,
        event: &TodoWriteHookEvent,
        final_todos: &[TodoItem],
        phase: TodoWriteHookPhase,
    ) -> Result<TodoWriteHookResult, String>;
}

pub struct TodoWriteTool {
    todo_directory: PathBuf,
    session_id: Option<String>,
    hook_runtime: Option<Arc<dyn TodoWriteHookRuntime>>,
}

impl TodoWriteTool {
    pub fn new(runtime_base_dir: impl AsRef<Path>) -> Self {
        Self {
            todo_directory: runtime_base_dir.as_ref().join("todos"),
            session_id: None,
            hook_runtime: None,
        }
    }

    /// Install an optional host hook executor. Without one, todo writes retain
    /// their existing behavior and do not dispatch lifecycle events.
    pub fn with_hook_runtime(mut self, runtime: Arc<dyn TodoWriteHookRuntime>) -> Self {
        self.hook_runtime = Some(runtime);
        self
    }

    pub fn select_session(&mut self, session_id: &str) -> Result<(), String> {
        if !is_valid_session_id(session_id) {
            return Err("todo_write requires a valid Canopy session ID".to_owned());
        }
        self.session_id = Some(session_id.to_owned());
        Ok(())
    }

    pub fn execute(&self, args: &Value) -> Result<ToolExecutionOutput, String> {
        let session_id = self
            .session_id
            .as_deref()
            .ok_or_else(|| "todo_write requires a selected session".to_owned())?;
        let path = self.todo_path(session_id);
        let previous = read_plan(&path, session_id)?;
        let candidate = if args.get("modified_by_user").and_then(Value::as_bool) == Some(true)
            && args.get("modified_content").is_some()
        {
            let content = args
                .get("modified_content")
                .and_then(Value::as_str)
                .ok_or_else(|| "modified_content must be a JSON string".to_owned())?;
            let parsed: Value = serde_json::from_str(content)
                .map_err(|error| format!("modified_content is invalid JSON: {error}"))?;
            parsed.get("todos").cloned().unwrap_or(Value::Null)
        } else {
            args.get("todos").cloned().unwrap_or(Value::Null)
        };
        let todos = parse_and_validate_todos(&candidate)?;
        let changes = detect_changes(&previous.todos, &todos);
        let (created_hook_events, completed_hook_events) = if self.hook_runtime.is_some() {
            detect_hook_events(&previous.todos, &todos)
        } else {
            (Vec::new(), Vec::new())
        };

        if let Some(runtime) = &self.hook_runtime {
            let created_results =
                run_validation_hooks(runtime.as_ref(), &created_hook_events, &todos)?;
            for (event, result) in created_hook_events.iter().zip(created_results) {
                if result.decision == Some(TodoWriteHookDecision::Block) {
                    return Ok(blocked_hook_output(event, result));
                }
            }

            let completed_results =
                run_validation_hooks(runtime.as_ref(), &completed_hook_events, &todos)?;
            for (event, result) in completed_hook_events.iter().zip(completed_results) {
                if result.decision == Some(TodoWriteHookDecision::Block) {
                    return Ok(blocked_hook_output(event, result));
                }
            }
        }

        let starts_new_plan = !todos.is_empty()
            && (previous.todos.is_empty()
                || (previous
                    .todos
                    .iter()
                    .all(|todo| todo.status == TodoStatus::Completed)
                    && previous.todos != todos));
        let active_plan_id = if todos.is_empty() {
            None
        } else if starts_new_plan || previous.plan_id.is_none() {
            Some(Uuid::new_v4().to_string())
        } else {
            previous.plan_id.clone()
        };
        let result_plan_id = active_plan_id.clone().or_else(|| previous.plan_id.clone());
        let new_state = TodoPlanState {
            plan_id: active_plan_id,
            todos: todos.clone(),
            session_id: session_id.to_owned(),
        };
        write_plan(&path, &new_state)?;

        let post_write_error = self.hook_runtime.as_ref().and_then(|runtime| {
            run_post_write_hooks(
                runtime.as_ref(),
                &created_hook_events,
                &completed_hook_events,
                &todos,
            )
            .err()
        });

        let mut output = if todos.is_empty() {
            "Todo list has been cleared.\n\n<system-reminder>\nYour todo list is now empty. DO NOT mention this explicitly to the user. You have no pending tasks in your todo list.\n</system-reminder>".to_owned()
        } else {
            let serialized = serde_json::to_string(&todos)
                .map_err(|error| format!("could not serialize todo list: {error}"))?;
            format!(
                "Todos have been modified successfully. Ensure that you continue to use the todo list to track your progress. Please proceed with the current tasks if applicable\n\n<system-reminder>\nYour todo list has changed. DO NOT mention this explicitly to the user. Here are the latest contents of your todo list:\n\n{serialized}. Continue on with the tasks at hand if applicable.\n</system-reminder>"
            )
        };
        if let Some(error) = post_write_error {
            output.push_str(&format!(
                "\n\n<system-reminder>\nTodos were persisted successfully, but post-write hooks failed with error: {error}. Do not tell the user the write failed; only handle any follow-up hook issues if needed.\n</system-reminder>"
            ));
        }
        let mut display = json!({
            "type":"todo_list",
            "todos":todos,
            "changes":changes,
        });
        if let Some(plan_id) = result_plan_id {
            display["planId"] = Value::String(plan_id);
        }
        Ok(ToolExecutionOutput::with_display(output, display))
    }

    pub fn read_todos_for_session(&self, session_id: &str) -> Result<Vec<TodoItem>, String> {
        if !is_valid_session_id(session_id) {
            return Err("todo_write requires a valid Canopy session ID".to_owned());
        }
        Ok(read_plan(&self.todo_path(session_id), session_id)?.todos)
    }

    pub fn list_sessions(&self) -> Result<Vec<String>, String> {
        let entries = match fs::read_dir(&self.todo_directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(format!("could not list todo sessions: {error}")),
        };
        let mut sessions = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| format!("could not read todo entry: {error}"))?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            if let Some(session_id) = path.file_stem().and_then(|name| name.to_str()) {
                sessions.push(session_id.to_owned());
            }
        }
        sessions.sort();
        Ok(sessions)
    }

    fn todo_path(&self, session_id: &str) -> PathBuf {
        self.todo_directory.join(format!("{session_id}.json"))
    }
}

fn parse_and_validate_todos(value: &Value) -> Result<Vec<TodoItem>, String> {
    let values = value
        .as_array()
        .ok_or_else(|| "Parameter \"todos\" must be an array.".to_owned())?;
    if values.len() > MAX_TODO_ITEMS {
        return Err(format!(
            "Todo list exceeds the {MAX_TODO_ITEMS}-item safety limit."
        ));
    }

    let mut todos = Vec::with_capacity(values.len());
    let mut estimated_bytes = 0usize;
    for value in values {
        let object = value
            .as_object()
            .ok_or_else(|| "Each todo must be an object.".to_owned())?;
        let id = object.get("id").and_then(Value::as_str).unwrap_or_default();
        if id.trim().is_empty() || id.encode_utf16().count() > 500 {
            return Err(
                "Each todo must have a non-empty \"id\" string of at most 500 characters."
                    .to_owned(),
            );
        }
        let content = object
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if content.trim().is_empty() {
            return Err("Each todo must have a non-empty \"content\" string.".to_owned());
        }
        let status = match object.get("status").and_then(Value::as_str) {
            Some("pending") => TodoStatus::Pending,
            Some("in_progress") => TodoStatus::InProgress,
            Some("completed") => TodoStatus::Completed,
            _ => {
                return Err(
                    "Each todo must have a valid \"status\" (pending, in_progress, completed)."
                        .to_owned(),
                );
            }
        };
        let blocked_by = match object.get("blockedBy") {
            None => None,
            Some(value) => {
                let values = value.as_array().ok_or_else(|| {
                    "Each todo \"blockedBy\" value must be an array of non-empty Todo IDs of at most 500 characters."
                        .to_owned()
                })?;
                let mut blocked_by = Vec::with_capacity(values.len());
                for dependency in values {
                    let dependency = dependency.as_str().unwrap_or_default();
                    if dependency.trim().is_empty() || dependency.encode_utf16().count() > 500 {
                        return Err(
                            "Each todo \"blockedBy\" value must be an array of non-empty Todo IDs of at most 500 characters."
                                .to_owned(),
                        );
                    }
                    blocked_by.push(dependency.to_owned());
                }
                Some(blocked_by)
            }
        };
        estimated_bytes = estimated_bytes
            .saturating_add(id.len())
            .saturating_add(content.len())
            .saturating_add(
                blocked_by
                    .as_ref()
                    .into_iter()
                    .flatten()
                    .map(String::len)
                    .sum::<usize>(),
            );
        if estimated_bytes > MAX_TODO_FILE_BYTES {
            return Err(format!(
                "Todo list exceeds the {MAX_TODO_FILE_BYTES}-byte safety limit."
            ));
        }
        let mut extra = object.clone();
        extra.remove("id");
        extra.remove("content");
        extra.remove("status");
        extra.remove("blockedBy");
        todos.push(TodoItem {
            id: id.to_owned(),
            content: content.to_owned(),
            status,
            blocked_by,
            extra,
        });
    }

    validate_dependency_graph(&todos)?;
    Ok(todos)
}

fn validate_dependency_graph(todos: &[TodoItem]) -> Result<(), String> {
    let mut indices = HashMap::with_capacity(todos.len());
    for (index, todo) in todos.iter().enumerate() {
        if indices.insert(todo.id.as_str(), index).is_some() {
            return Err("Todo IDs must be unique within the array.".to_owned());
        }
    }

    let mut remaining = vec![0usize; todos.len()];
    let mut dependents = vec![Vec::new(); todos.len()];
    for (index, todo) in todos.iter().enumerate() {
        let mut unique = HashSet::new();
        for dependency in todo.blocked_by.iter().flatten() {
            if !unique.insert(dependency.as_str()) {
                return Err(format!(
                    "Todo \"{}\" must not contain duplicate blockedBy references.",
                    todo.id
                ));
            }
            if dependency == &todo.id {
                return Err(format!("Todo \"{}\" must not depend on itself.", todo.id));
            }
            let Some(&dependency_index) = indices.get(dependency.as_str()) else {
                return Err(format!(
                    "Todo \"{}\" references unknown dependency \"{}\".",
                    todo.id, dependency
                ));
            };
            remaining[index] += 1;
            dependents[dependency_index].push(index);
        }
    }

    let mut ready = VecDeque::new();
    for (index, count) in remaining.iter().enumerate() {
        if *count == 0 {
            ready.push_back(index);
        }
    }
    let mut visited = 0usize;
    while let Some(index) = ready.pop_front() {
        visited += 1;
        for dependent in &dependents[index] {
            remaining[*dependent] -= 1;
            if remaining[*dependent] == 0 {
                ready.push_back(*dependent);
            }
        }
    }
    if visited != todos.len() {
        return Err("Todo dependencies must not contain a cycle.".to_owned());
    }
    Ok(())
}

fn detect_changes(old_todos: &[TodoItem], new_todos: &[TodoItem]) -> TodoChanges {
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

fn detect_hook_events(
    old_todos: &[TodoItem],
    new_todos: &[TodoItem],
) -> (Vec<TodoWriteHookEvent>, Vec<TodoWriteHookEvent>) {
    let old_by_id = old_todos
        .iter()
        .map(|todo| (todo.id.as_str(), todo))
        .collect::<HashMap<_, _>>();
    let mut created = Vec::new();
    let mut completed = Vec::new();
    for todo in new_todos {
        match old_by_id.get(todo.id.as_str()) {
            None => created.push(TodoWriteHookEvent::TodoCreated { todo: todo.clone() }),
            Some(old)
                if old.status != TodoStatus::Completed && todo.status == TodoStatus::Completed =>
            {
                completed.push(TodoWriteHookEvent::TodoCompleted {
                    todo: todo.clone(),
                    previous_status: old.status.clone(),
                });
            }
            Some(_) => {}
        }
    }
    (created, completed)
}

fn blocked_hook_output(
    event: &TodoWriteHookEvent,
    result: TodoWriteHookResult,
) -> ToolExecutionOutput {
    let (event_name, fallback_reason, message_prefix) = match event {
        TodoWriteHookEvent::TodoCreated { .. } => (
            "TodoCreated",
            "Hook blocked todo creation",
            "Todo creation blocked",
        ),
        TodoWriteHookEvent::TodoCompleted { .. } => (
            "TodoCompleted",
            "Hook blocked todo completion",
            "Todo completion blocked",
        ),
    };
    let reason = result
        .reason
        .filter(|reason| !reason.is_empty())
        .unwrap_or_else(|| fallback_reason.to_owned());
    let message = format!("{message_prefix}: {reason}");
    let reminder = format!(
        "Todo list was not modified because a {event_name} hook blocked the operation: {reason}"
    );
    ToolExecutionOutput::with_display(
        format!("{message}\n\n<system-reminder>\n{reminder}\n</system-reminder>"),
        Value::String(message),
    )
}

fn run_validation_hooks(
    runtime: &dyn TodoWriteHookRuntime,
    events: &[TodoWriteHookEvent],
    final_todos: &[TodoItem],
) -> Result<Vec<TodoWriteHookResult>, String> {
    if events.is_empty() {
        return Ok(Vec::new());
    }

    let next_event = AtomicUsize::new(0);
    let results = Mutex::new((0..events.len()).map(|_| None).collect::<Vec<_>>());
    let worker_count = events.len().min(MAX_CONCURRENT_TODO_VALIDATION_HOOKS);
    std::thread::scope(|scope| {
        for _ in 0..worker_count {
            scope.spawn(|| {
                loop {
                    let index = next_event.fetch_add(1, Ordering::Relaxed);
                    let Some(event) = events.get(index) else {
                        break;
                    };
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        runtime.execute(event, final_todos, TodoWriteHookPhase::Validation)
                    }))
                    .unwrap_or_else(|_| Err("todo validation hook panicked".to_owned()));
                    results.lock().unwrap()[index] = Some(result);
                }
            });
        }
    });

    let results = results
        .into_inner()
        .map_err(|_| "todo validation hook results were poisoned".to_owned())?;
    let mut hook_results = Vec::with_capacity(results.len());
    let mut first_error = None;
    for result in results {
        let Some(result) = result else {
            return Err("todo validation hook did not return a result".to_owned());
        };
        match result {
            Ok(result) => hook_results.push(result),
            Err(error) => {
                hook_results.push(TodoWriteHookResult::default());
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(hook_results),
    }
}

fn run_post_write_hooks(
    runtime: &dyn TodoWriteHookRuntime,
    created_events: &[TodoWriteHookEvent],
    completed_events: &[TodoWriteHookEvent],
    final_todos: &[TodoItem],
) -> Result<(), String> {
    for event in created_events.iter().chain(completed_events) {
        runtime.execute(event, final_todos, TodoWriteHookPhase::PostWrite)?;
    }
    Ok(())
}

/// Produce an active-work reminder update from the user-visible todo display.
/// `None` means the display is not a todo list; `Some(None)` clears the active
/// reminder because the list has no unfinished items.
pub fn active_todo_reminder_update(display: &Value) -> Option<Option<String>> {
    if display.get("type").and_then(Value::as_str) != Some("todo_list") {
        return None;
    }
    let todos = display.get("todos").and_then(Value::as_array)?;
    let active = todos
        .iter()
        .filter_map(|todo| {
            let status = todo.get("status").and_then(Value::as_str)?;
            if status == "completed" {
                return None;
            }
            let content = todo.get("content").and_then(Value::as_str)?;
            Some(format!("- [{status}] {content}"))
        })
        .collect::<Vec<_>>();
    if active.is_empty() {
        return Some(None);
    }

    let serialized = escape_system_reminder_tags(&active.join("\n"));
    let (context, truncated) = truncate_utf16(&serialized, MAX_ACTIVE_TODO_CONTEXT_CHARS);
    Some(Some(format!(
        "<system-reminder>\nThe current task still has unfinished todo items:\n{context}{}\nKeep the todo list current and continue the task. Do not treat a successful intermediate tool call as task completion.\n</system-reminder>",
        if truncated { "\n[truncated]" } else { "" }
    )))
}

fn truncate_utf16(value: &str, max_units: usize) -> (String, bool) {
    let mut used_units = 0usize;
    let mut byte_end = 0usize;
    for (byte_index, character) in value.char_indices() {
        let character_units = character.len_utf16();
        if used_units.saturating_add(character_units) > max_units {
            return (value[..byte_end].to_owned(), true);
        }
        used_units += character_units;
        byte_end = byte_index + character.len_utf8();
    }
    (value[..byte_end].to_owned(), false)
}

fn read_plan(path: &Path, session_id: &str) -> Result<TodoPlanState, String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(TodoPlanState {
                session_id: session_id.to_owned(),
                ..TodoPlanState::default()
            });
        }
        Err(error) => return Err(format!("could not read todo list: {error}")),
    };
    let metadata = file
        .metadata()
        .map_err(|error| format!("could not inspect todo list: {error}"))?;
    if !metadata.is_file() {
        return Err("todo list path is not a regular file".to_owned());
    }
    if metadata.len() > MAX_TODO_FILE_BYTES as u64 {
        return Err(format!(
            "todo list exceeds the {MAX_TODO_FILE_BYTES}-byte read limit"
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take((MAX_TODO_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("could not read todo list: {error}"))?;
    if bytes.len() > MAX_TODO_FILE_BYTES {
        return Err(format!(
            "todo list exceeds the {MAX_TODO_FILE_BYTES}-byte read limit"
        ));
    }
    let state: TodoPlanState = serde_json::from_slice(&bytes)
        .map_err(|error| format!("todo list JSON is invalid: {error}"))?;
    if state.session_id != session_id {
        return Err("todo list belongs to a different session".to_owned());
    }
    parse_and_validate_todos(&serde_json::to_value(&state.todos).unwrap_or(Value::Null))?;
    Ok(state)
}

fn write_plan(path: &Path, state: &TodoPlanState) -> Result<(), String> {
    let bytes = serde_json::to_vec(state)
        .map_err(|error| format!("could not serialize todo list: {error}"))?;
    if bytes.len() > MAX_TODO_FILE_BYTES {
        return Err(format!(
            "todo list exceeds the {MAX_TODO_FILE_BYTES}-byte write limit"
        ));
    }
    atomic_write(path, &bytes).map_err(|error| format!("could not persist todo list: {error}"))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "todo path has no filename"))?;
    let mut temporary = None;
    for _ in 0..8 {
        let candidate = parent.join(format!(
            ".{}.tmp-{}",
            file_name.to_string_lossy(),
            Uuid::new_v4().simple()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        match options.open(&candidate) {
            Ok(file) => {
                temporary = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    let (temporary_path, mut file) = temporary.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a todo-list temporary file",
        )
    })?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary_path, path)?;
        #[cfg(unix)]
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

pub fn function_declaration() -> Value {
    json!({
        "name":"todo_write",
        "description":"Creates and manages a concise, user-visible task list for complex or multi-step work.",
        "parameters":{
            "type":"OBJECT",
            "properties":{
                "todos":{
                    "type":"ARRAY",
                    "description":"The updated todo list",
                    "items":{
                        "type":"OBJECT",
                        "properties":{
                            "content":{"type":"STRING","minLength":1},
                            "status":{"type":"STRING","enum":["pending","in_progress","completed"]},
                            "id":{"type":"STRING","maxLength":500},
                            "blockedBy":{"type":"ARRAY","items":{"type":"STRING","maxLength":500},"uniqueItems":true,"description":"Todo IDs that must be completed before this item"}
                        },
                        "required":["content","status","id"],
                        "additionalProperties":false
                    }
                }
            },
            "required":["todos"]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Barrier, Mutex};

    struct TempDirectory(PathBuf);

    impl TempDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("canopy-todos-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn tool(directory: &TempDirectory, session_id: &str) -> TodoWriteTool {
        let mut tool = TodoWriteTool::new(&directory.0);
        tool.select_session(session_id).unwrap();
        tool
    }

    fn todos(value: Value) -> Value {
        json!({"todos":value})
    }

    struct RecordingHookRuntime {
        validation_barrier: Option<Arc<Barrier>>,
        block_validation_id: Option<String>,
        fail_post_write_id: Option<String>,
        calls: Mutex<Vec<(TodoWriteHookPhase, RecordedHookKind, String)>>,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RecordedHookKind {
        Created,
        Completed,
    }

    impl RecordingHookRuntime {
        fn new() -> Self {
            Self {
                validation_barrier: None,
                block_validation_id: None,
                fail_post_write_id: None,
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl TodoWriteHookRuntime for RecordingHookRuntime {
        fn execute(
            &self,
            event: &TodoWriteHookEvent,
            _final_todos: &[TodoItem],
            phase: TodoWriteHookPhase,
        ) -> Result<TodoWriteHookResult, String> {
            let (kind, todo_id) = match event {
                TodoWriteHookEvent::TodoCreated { todo } => {
                    (RecordedHookKind::Created, todo.id.clone())
                }
                TodoWriteHookEvent::TodoCompleted { todo, .. } => {
                    (RecordedHookKind::Completed, todo.id.clone())
                }
            };
            self.calls
                .lock()
                .unwrap()
                .push((phase, kind, todo_id.clone()));

            if phase == TodoWriteHookPhase::Validation {
                if let Some(barrier) = &self.validation_barrier {
                    barrier.wait();
                }
                if self.block_validation_id.as_deref() == Some(todo_id.as_str()) {
                    return Ok(TodoWriteHookResult::block("test policy"));
                }
            }
            if phase == TodoWriteHookPhase::PostWrite
                && self.fail_post_write_id.as_deref() == Some(todo_id.as_str())
            {
                return Err("test post-write failure".to_owned());
            }
            Ok(TodoWriteHookResult::allow())
        }
    }

    #[test]
    fn validates_unique_ids_dependencies_and_cycles() {
        let valid = json!([
            {"id":"design","content":"Design","status":"completed"},
            {"id":"build","content":"Build","status":"in_progress","blockedBy":["design"]}
        ]);
        assert!(parse_and_validate_todos(&valid).is_ok());
        assert_eq!(
            parse_and_validate_todos(&json!([
                {"id":"same","content":"One","status":"pending"},
                {"id":"same","content":"Two","status":"pending"}
            ]))
            .unwrap_err(),
            "Todo IDs must be unique within the array."
        );
        assert!(
            parse_and_validate_todos(&json!([
                {"id":"a","content":"A","status":"pending","blockedBy":["b"]},
                {"id":"b","content":"B","status":"pending","blockedBy":["a"]}
            ]))
            .unwrap_err()
            .contains("must not contain a cycle")
        );
    }

    #[test]
    fn validation_block_keeps_the_previous_plan_unmodified() {
        let directory = TempDirectory::new();
        let session_id = "55555555-5555-4555-8555-555555555555";
        let tool = tool(&directory, session_id);
        tool.execute(&todos(json!([{
            "id":"existing","content":"Existing task","status":"in_progress"
        }])))
        .unwrap();

        let runtime = Arc::new(RecordingHookRuntime {
            block_validation_id: Some("blocked".to_owned()),
            ..RecordingHookRuntime::new()
        });
        let tool = tool.with_hook_runtime(runtime.clone());

        let output = tool
            .execute(&todos(json!([
                {"id":"existing","content":"Existing task","status":"completed"},
                {"id":"blocked","content":"Blocked task","status":"pending"}
            ])))
            .unwrap();

        assert!(output.output.contains("Todo creation blocked: test policy"));
        let persisted = tool.read_todos_for_session(session_id).unwrap();
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].status, TodoStatus::InProgress);
        let calls = runtime.calls.lock().unwrap();
        assert_eq!(
            calls.as_slice(),
            &[(
                TodoWriteHookPhase::Validation,
                RecordedHookKind::Created,
                "blocked".to_owned()
            )]
        );
    }

    #[test]
    fn validation_runs_concurrently_and_post_write_runs_in_todo_order() {
        let directory = TempDirectory::new();
        let runtime = Arc::new(RecordingHookRuntime {
            validation_barrier: Some(Arc::new(Barrier::new(3))),
            fail_post_write_id: Some("b".to_owned()),
            ..RecordingHookRuntime::new()
        });
        let tool = tool(&directory, "66666666-6666-4666-8666-666666666666")
            .with_hook_runtime(runtime.clone());

        let output = tool
            .execute(&todos(json!([
                {"id":"a","content":"A","status":"pending"},
                {"id":"b","content":"B","status":"pending"},
                {"id":"c","content":"C","status":"pending"}
            ])))
            .unwrap();

        assert!(
            output
                .output
                .contains("Todos were persisted successfully, but post-write hooks failed")
        );
        assert_eq!(
            tool.read_todos_for_session("66666666-6666-4666-8666-666666666666")
                .unwrap()
                .len(),
            3
        );

        let calls = runtime.calls.lock().unwrap();
        let validation_ids = calls
            .iter()
            .filter(|(phase, _, _)| *phase == TodoWriteHookPhase::Validation)
            .map(|(_, _, id)| id.as_str())
            .collect::<HashSet<_>>();
        assert_eq!(validation_ids, HashSet::from(["a", "b", "c"]));
        let post_write_ids = calls
            .iter()
            .filter(|(phase, _, _)| *phase == TodoWriteHookPhase::PostWrite)
            .map(|(_, _, id)| id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(post_write_ids, ["a", "b"]);
    }

    #[test]
    fn mixed_hook_events_keep_created_and_completed_family_order() {
        let directory = TempDirectory::new();
        let session_id = "77777777-7777-4777-8777-777777777777";
        let tool = tool(&directory, session_id);
        tool.execute(&todos(json!([
            {"id":"a","content":"A","status":"pending"},
            {"id":"b","content":"B","status":"in_progress"}
        ])))
        .unwrap();

        let runtime = Arc::new(RecordingHookRuntime {
            validation_barrier: Some(Arc::new(Barrier::new(2))),
            ..RecordingHookRuntime::new()
        });
        let tool = tool.with_hook_runtime(runtime.clone());
        tool.execute(&todos(json!([
            {"id":"a","content":"A","status":"completed"},
            {"id":"new-a","content":"New A","status":"pending"},
            {"id":"b","content":"B","status":"completed"},
            {"id":"new-b","content":"New B","status":"pending"}
        ])))
        .unwrap();

        let calls = runtime.calls.lock().unwrap();
        let validation_calls = calls
            .iter()
            .filter(|(phase, _, _)| *phase == TodoWriteHookPhase::Validation)
            .map(|(_, kind, id)| (*kind, id.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            validation_calls[..2]
                .iter()
                .map(|(_, id)| *id)
                .collect::<HashSet<_>>(),
            HashSet::from(["new-a", "new-b"])
        );
        assert_eq!(
            validation_calls[2..]
                .iter()
                .map(|(_, id)| *id)
                .collect::<HashSet<_>>(),
            HashSet::from(["a", "b"])
        );
        assert!(
            validation_calls[..2]
                .iter()
                .all(|(kind, _)| *kind == RecordedHookKind::Created)
        );
        assert!(
            validation_calls[2..]
                .iter()
                .all(|(kind, _)| *kind == RecordedHookKind::Completed)
        );

        let post_write_calls = calls
            .iter()
            .filter(|(phase, _, _)| *phase == TodoWriteHookPhase::PostWrite)
            .map(|(_, kind, id)| (*kind, id.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            post_write_calls,
            [
                (RecordedHookKind::Created, "new-a"),
                (RecordedHookKind::Created, "new-b"),
                (RecordedHookKind::Completed, "a"),
                (RecordedHookKind::Completed, "b")
            ]
        );
    }

    #[test]
    fn rejects_missing_self_and_duplicate_dependencies() {
        for (value, expected) in [
            (
                json!([{"id":"a","content":"A","status":"pending","blockedBy":["missing"]}]),
                "references unknown dependency",
            ),
            (
                json!([{"id":"a","content":"A","status":"pending","blockedBy":["a"]}]),
                "must not depend on itself",
            ),
            (
                json!([{"id":"a","content":"A","status":"pending","blockedBy":["b","b"]},{"id":"b","content":"B","status":"pending"}]),
                "duplicate blockedBy references",
            ),
        ] {
            assert!(
                parse_and_validate_todos(&value)
                    .unwrap_err()
                    .contains(expected)
            );
        }
    }

    #[test]
    fn persists_one_plan_per_session_and_starts_a_new_plan_after_completion() {
        let directory = TempDirectory::new();
        let first_session = tool(&directory, "11111111-1111-4111-8111-111111111111");
        let second_session = tool(&directory, "22222222-2222-4222-8222-222222222222");
        let first = first_session
            .execute(&todos(json!([{"id":"a","content":"A","status":"pending"}])))
            .unwrap();
        let state = read_plan(
            &first_session.todo_path("11111111-1111-4111-8111-111111111111"),
            "11111111-1111-4111-8111-111111111111",
        )
        .unwrap();
        let plan_id = state.plan_id.unwrap();
        assert!(
            first
                .output
                .contains("Todos have been modified successfully")
        );

        first_session
            .execute(&todos(
                json!([{"id":"a","content":"A","status":"completed"}]),
            ))
            .unwrap();
        let finished = read_plan(
            &first_session.todo_path("11111111-1111-4111-8111-111111111111"),
            "11111111-1111-4111-8111-111111111111",
        )
        .unwrap();
        assert_eq!(finished.plan_id.as_deref(), Some(plan_id.as_str()));

        first_session
            .execute(&todos(json!([{"id":"b","content":"B","status":"pending"}])))
            .unwrap();
        let next = read_plan(
            &first_session.todo_path("11111111-1111-4111-8111-111111111111"),
            "11111111-1111-4111-8111-111111111111",
        )
        .unwrap();
        assert_ne!(next.plan_id.as_deref(), Some(plan_id.as_str()));
        assert!(
            second_session
                .read_todos_for_session("22222222-2222-4222-8222-222222222222")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn clears_the_plan_and_preserves_unknown_item_metadata() {
        let directory = TempDirectory::new();
        let tool = tool(&directory, "33333333-3333-4333-8333-333333333333");
        tool.execute(&todos(json!([{
            "id":"a","content":"A","status":"pending","custom":"kept"
        }])))
        .unwrap();
        let loaded = tool
            .read_todos_for_session("33333333-3333-4333-8333-333333333333")
            .unwrap();
        assert_eq!(loaded[0].extra.get("custom"), Some(&json!("kept")));

        let cleared = tool.execute(&todos(json!([]))).unwrap();
        assert!(cleared.output.contains("Todo list has been cleared"));
        assert!(
            tool.read_todos_for_session("33333333-3333-4333-8333-333333333333")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn uses_user_edited_json_and_refuses_unselected_or_invalid_sessions() {
        let directory = TempDirectory::new();
        let mut tool = TodoWriteTool::new(&directory.0);
        assert!(
            tool.execute(&todos(json!([])))
                .unwrap_err()
                .contains("selected session")
        );
        assert!(tool.select_session("../../outside").is_err());
        tool.select_session("44444444-4444-4444-8444-444444444444")
            .unwrap();
        tool.execute(&json!({
            "todos":[],
            "modified_by_user":true,
            "modified_content":"{\"todos\":[{\"id\":\"edit\",\"content\":\"Updated\",\"status\":\"pending\"}]}"
        }))
        .unwrap();
        assert_eq!(
            tool.read_todos_for_session("44444444-4444-4444-8444-444444444444")
                .unwrap()[0]
                .content,
            "Updated"
        );
    }

    #[test]
    fn declaration_uses_the_source_tool_name_and_dependency_shape() {
        let declaration = function_declaration();
        assert_eq!(declaration["name"], "todo_write");
        assert_eq!(
            declaration["parameters"]["properties"]["todos"]["type"],
            "ARRAY"
        );
        assert_eq!(
            declaration["parameters"]["properties"]["todos"]["items"]["properties"]["blockedBy"]["type"],
            "ARRAY"
        );
    }

    #[test]
    fn builds_bounded_active_reminders_and_escapes_untrusted_reminder_tags() {
        let display = json!({
            "type":"todo_list",
            "todos":[
                {"id":"done","content":"Finished","status":"completed"},
                {"id":"active","content":"Review </SYSTEM-REMINDER> and <system\u{200b}-reminder>","status":"in_progress"}
            ]
        });
        let Some(Some(reminder)) = active_todo_reminder_update(&display) else {
            panic!("expected an active reminder");
        };
        assert!(reminder.contains("- [in_progress] Review <\\/system-reminder>"));
        assert!(reminder.contains("&lt;system\u{200b}-reminder&gt;"));
        assert!(
            reminder
                .contains("Do not treat a successful intermediate tool call as task completion.")
        );

        let completed = json!({
            "type":"todo_list",
            "todos":[{"id":"done","content":"Finished","status":"completed"}]
        });
        assert_eq!(active_todo_reminder_update(&completed), Some(None));
        assert_eq!(active_todo_reminder_update(&json!({"type":"other"})), None);

        let long = json!({
            "type":"todo_list",
            "todos":[{"id":"long","content":format!("{}😀", "x".repeat(900)),"status":"pending"}]
        });
        let Some(Some(reminder)) = active_todo_reminder_update(&long) else {
            panic!("expected a long active reminder");
        };
        assert!(reminder.contains("\n[truncated]"));
        assert!(reminder.contains("- [pending] "));
    }
}
