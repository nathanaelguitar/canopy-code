use serde_json::{Map, Value, json};

use crate::tools::shell::ShellTool;

const LIST_COMMAND_LIMIT: usize = 500;
const LIST_PATH_LIMIT: usize = 1_000;

pub fn task_list_function_declaration() -> Value {
    json!({
        "name":"task_list",
        "description":"List managed background shell tasks in this session, including their status and output file.",
        "parameters":{
            "type":"OBJECT",
            "properties":{},
            "required":[],
            "additionalProperties":false
        }
    })
}

pub fn task_stop_function_declaration() -> Value {
    json!({
        "name":"task_stop",
        "description":"Request cancellation of a running managed background shell by its task ID.",
        "parameters":{
            "type":"OBJECT",
            "properties":{
                "task_id":{"type":"STRING","description":"The background shell ID returned when the command started."}
            },
            "required":["task_id"],
            "additionalProperties":false
        }
    })
}

pub fn list_background_shell_tasks(shell: &ShellTool, args: &Value) -> Result<String, String> {
    let object = args
        .as_object()
        .ok_or_else(|| "task_list arguments must be an object.".to_owned())?;
    if !object.is_empty() {
        return Err("task_list does not accept parameters in the Rust preview.".to_owned());
    }
    let tasks = shell.background_shell_tasks();
    if tasks.is_empty() {
        return Ok("No background shell tasks.".to_owned());
    }
    let mut lines = vec![format!("Managed background shell tasks ({}):", tasks.len())];
    for task in tasks {
        lines.push(format!(
            "{} [{}] {}",
            task.id,
            task.status.as_str(),
            single_line(&task.command, LIST_COMMAND_LIMIT)
        ));
        let mut details = Vec::new();
        if let Some(pid) = task.pid {
            details.push(format!("pid={pid}"));
        }
        details.push(format!(
            "output={}",
            single_line(&task.output_file, LIST_PATH_LIMIT)
        ));
        if let Some(exit_code) = task.exit_code {
            details.push(format!("exit_code={exit_code}"));
        }
        if let Some(error) = task.error.as_deref() {
            details.push(format!("error={}", single_line(error, LIST_COMMAND_LIMIT)));
        }
        lines.push(format!("  {}", details.join("; ")));
    }
    Ok(lines.join("\n"))
}

pub fn stop_background_shell_task(shell: &ShellTool, args: &Value) -> Result<String, String> {
    let object = args
        .as_object()
        .ok_or_else(|| "task_stop arguments must be an object.".to_owned())?;
    if !has_only_keys(object, &["task_id"]) {
        return Err("task_stop accepts only the task_id parameter.".to_owned());
    }
    let task_id = object
        .get("task_id")
        .and_then(Value::as_str)
        .filter(|task_id| !task_id.trim().is_empty())
        .ok_or_else(|| "task_id must be a non-empty string.".to_owned())?;

    let Some(task) = shell.background_shell_task(task_id) else {
        return Err(format!(
            "Error: No background shell found with ID \"{}\".",
            single_line(task_id, 200)
        ));
    };
    match shell.request_cancel_background_shell(task_id) {
        None => Err(format!(
            "Error: No background shell found with ID \"{}\".",
            single_line(task_id, 200)
        )),
        Some(false) => {
            let status = shell
                .background_shell_task(task_id)
                .map_or(task.status, |task| task.status);
            Err(format!(
                "Error: Background shell \"{}\" is not running (status: {}).",
                single_line(task_id, 200),
                status.as_str()
            ))
        }
        Some(true) => Ok(format!(
            "Cancellation requested for background shell \"{}\". Final status will be visible with task_list after the process drains; captured output remains at {}.\nCommand: {}",
            task.id,
            single_line(&task.output_file, LIST_PATH_LIMIT),
            single_line(&task.command, LIST_COMMAND_LIMIT)
        )),
    }
}

fn has_only_keys(object: &Map<String, Value>, allowed: &[&str]) -> bool {
    object.keys().all(|key| allowed.contains(&key.as_str()))
}

fn single_line(value: &str, limit: usize) -> String {
    let mut output = String::new();
    let mut characters = value.chars();
    for character in characters.by_ref().take(limit) {
        match character {
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => {}
            character => output.push(character),
        }
    }
    if characters.next().is_some() {
        output.push('…');
    }
    output
}
