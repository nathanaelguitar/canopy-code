//! Native tool contracts for session-only cron jobs and loop wakeups.
//!
//! Durable creation is rejected explicitly until the native session runtime
//! owns the scheduled-task file, lock, reload, and catch-up lifecycle.

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::services::cron_scheduler::{CronScheduler, CronSchedulerError, MAX_WAKEUP_PROMPT_CHARS};
use crate::services::cron_scheduler_primitives::{WAKEUP_MAX_SECONDS, WAKEUP_MIN_SECONDS};
use crate::tool_response_finalizer::ToolExecutionOutput;
use crate::utils::cron_display::human_readable_cron;

pub const CRON_CREATE: &str = "cron_create";
pub const CRON_LIST: &str = "cron_list";
pub const CRON_DELETE: &str = "cron_delete";
pub const LOOP_WAKEUP: &str = "loop_wakeup";

/// Tool declarations in the same provider schema shape as other native Rust
/// tools. Names and argument keys match the TypeScript built-ins.
pub fn function_declarations() -> Vec<Value> {
    vec![
        json!({
            "name": CRON_CREATE,
            "description": "Schedule a session-only prompt using a standard five-field cron expression in local time. The Rust CLI currently rejects cron creation when its live prompt-delivery loop is not connected to the timer. Set durable=true only when persistence is available; this Rust runtime reports that durable task lifecycle is unavailable.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "cron": {"type": "STRING", "description": "Cron expression: minute hour day-of-month month day-of-week, for example */5 * * * *."},
                    "prompt": {"type": "STRING", "description": "Prompt to enqueue when the schedule fires."},
                    "recurring": {"type": "BOOLEAN", "description": "Whether the schedule repeats; defaults to true."},
                    "durable": {"type": "BOOLEAN", "description": "Whether to persist across restarts. Durable lifecycle is unavailable in the current Rust scheduler."}
                },
                "required": ["cron", "prompt"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": CRON_LIST,
            "description": "List session-only cron jobs and pending loop wakeups.",
            "parameters": {"type": "OBJECT", "properties": {}, "required": [], "additionalProperties": false}
        }),
        json!({
            "name": CRON_DELETE,
            "description": "Cancel a session-only cron job or pending loop wakeup by id.",
            "parameters": {
                "type": "OBJECT",
                "properties": {"id": {"type": "STRING", "description": "Job or wakeup id."}},
                "required": ["id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": LOOP_WAKEUP,
            "description": "Schedule a session-only one-shot continuation for a self-paced loop. The Rust CLI currently rejects wakeups when its live prompt-delivery loop is not connected to the timer. Delay is clamped to 60–3600 seconds; a wakeup chain is limited to 24 hours.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "delaySeconds": {"type": "NUMBER", "description": "Seconds from now to wake up; clamped to [60, 3600]."},
                    "prompt": {"type": "STRING", "maxLength": MAX_WAKEUP_PROMPT_CHARS, "description": "Continuation prompt to enqueue when the wakeup fires."},
                    "reason": {"type": "STRING", "description": "Short explanation of the selected delay, shown to the user."}
                },
                "required": ["delaySeconds", "prompt"],
                "additionalProperties": false
            }
        }),
    ]
}

/// Execute one of the four cron-related tools against the caller's session
/// scheduler. The host remains responsible for permission review and for
/// delivering fired prompts into its live agent loop.
pub fn execute_cron_tool(
    scheduler: &CronScheduler,
    tool_name: &str,
    args: &Value,
) -> Result<ToolExecutionOutput, String> {
    let object = args
        .as_object()
        .ok_or_else(|| format!("{tool_name} arguments must be an object."))?;
    match tool_name {
        CRON_CREATE => create(scheduler, object),
        CRON_LIST => list(scheduler, object),
        CRON_DELETE => delete(scheduler, object),
        LOOP_WAKEUP => loop_wakeup(scheduler, object),
        _ => Err(format!("Unknown cron tool `{tool_name}`.")),
    }
}

fn create(
    scheduler: &CronScheduler,
    args: &serde_json::Map<String, Value>,
) -> Result<ToolExecutionOutput, String> {
    only_keys(
        args,
        &["cron", "prompt", "recurring", "durable"],
        CRON_CREATE,
    )?;
    let cron = required_string(args, "cron", CRON_CREATE)?;
    let prompt = required_string(args, "prompt", CRON_CREATE)?.trim();
    let recurring = optional_bool(args, "recurring")?.unwrap_or(true);
    if optional_bool(args, "durable")? == Some(true) {
        return Err(
            "Durable cron jobs are not available in the Rust runtime yet: the scheduled-task file, lock ownership, reload, and catch-up lifecycle are not wired to this session scheduler.".to_owned(),
        );
    }

    let job = scheduler
        .create(cron, prompt, recurring)
        .map_err(|error| format!("Error creating cron job: {error}"))?;
    let display_schedule = human_readable_cron(&job.cron_expr);
    let display = format!("Scheduled {} ({display_schedule})", job.id);
    let expiry = match scheduler.recurring_max_age() {
        Some(age) => {
            let days = age.as_secs_f64() / (24 * 60 * 60) as f64;
            format!(
                "Auto-expires after {} days. Use CronDelete to cancel sooner.",
                format_number(days)
            )
        }
        None => "Never auto-expires. Use CronDelete to cancel.".to_owned(),
    };
    let where_text = "Session-only (not written to disk, dies when Canopy Code exits)";
    let output = if job.recurring {
        format!(
            "Scheduled recurring job {} ({}). {}. {}",
            job.id, job.cron_expr, where_text, expiry
        )
    } else {
        format!(
            "Scheduled one-shot task {} ({}). {}. It will fire once then auto-delete.",
            job.id, job.cron_expr, where_text
        )
    };
    Ok(ToolExecutionOutput::with_display(
        output,
        json!({"displayText": display}),
    ))
}

fn list(
    scheduler: &CronScheduler,
    args: &serde_json::Map<String, Value>,
) -> Result<ToolExecutionOutput, String> {
    only_keys(args, &[], CRON_LIST)?;
    let jobs = scheduler.list();
    if jobs.is_empty() {
        let empty = "No active cron jobs or loop wakeups.";
        return Ok(ToolExecutionOutput::with_display(
            empty,
            json!({"displayText": empty}),
        ));
    }

    let llm_lines = jobs
        .iter()
        .map(|job| {
            let kind = if job.recurring {
                "recurring"
            } else {
                "one-shot"
            };
            let schedule = display_schedule(job);
            let prompt = if job.cron_expr == "@wakeup" {
                truncate_prompt(&job.prompt, 60)
            } else {
                job.prompt.clone()
            };
            format!(
                "{} — {} ({kind}) [session-only]: {prompt}",
                job.id, schedule
            )
        })
        .collect::<Vec<_>>();
    let display_lines = jobs
        .iter()
        .map(|job| format!("{} {} [session-only]", job.id, display_schedule(job)))
        .collect::<Vec<_>>();
    Ok(ToolExecutionOutput::with_display(
        llm_lines.join("\n"),
        json!({"displayText": display_lines.join("\n")}),
    ))
}

fn delete(
    scheduler: &CronScheduler,
    args: &serde_json::Map<String, Value>,
) -> Result<ToolExecutionOutput, String> {
    only_keys(args, &["id"], CRON_DELETE)?;
    let id = required_string(args, "id", CRON_DELETE)?;
    if scheduler.delete(id) {
        Ok(ToolExecutionOutput::with_display(
            format!("Cancelled job {id}."),
            json!({"displayText": format!("Cancelled {id}")}),
        ))
    } else {
        Err(format!("Job {id} not found."))
    }
}

fn loop_wakeup(
    scheduler: &CronScheduler,
    args: &serde_json::Map<String, Value>,
) -> Result<ToolExecutionOutput, String> {
    only_keys(args, &["delaySeconds", "prompt", "reason"], LOOP_WAKEUP)?;
    let delay_seconds = args
        .get("delaySeconds")
        .and_then(Value::as_f64)
        .ok_or_else(|| "delaySeconds must be a finite number.".to_owned())?;
    if !delay_seconds.is_finite() {
        return Err("delaySeconds must be a finite number.".to_owned());
    }
    let prompt = required_string(args, "prompt", LOOP_WAKEUP)?.trim();
    if prompt.is_empty() {
        return Err("Loop wakeup prompt must not be empty.".to_owned());
    }
    if prompt.chars().count() > MAX_WAKEUP_PROMPT_CHARS {
        return Err(format!(
            "Loop wakeup prompt must not exceed {MAX_WAKEUP_PROMPT_CHARS} characters."
        ));
    }
    let reason = args
        .get("reason")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|reason| !reason.is_empty());
    let schedule = scheduler
        .schedule_wakeup(delay_seconds, prompt, None)
        .map_err(|error| match error {
            CronSchedulerError::Disabled => {
                "Loop wakeups are disabled for the rest of this session (token limit reached). Restart the session to re-enable.".to_owned()
            }
            other => format!("Error scheduling loop wakeup: {other}"),
        })?;

    let requested = format_requested(delay_seconds);
    let mut lines = vec![format!("Scheduled loop wakeup {}.", schedule.id)];
    if let Some(replaced_id) = schedule.replaced_id.as_deref() {
        lines.push(format!("Replaced pending wakeup {replaced_id}."));
    }
    lines.push(format!(
        "Scheduled for: {} (in {}s).",
        schedule.scheduled_for, schedule.clamped_delay_seconds
    ));
    if schedule.was_clamped {
        lines.push(format!(
            "Requested {requested} was clamped to the [{WAKEUP_MIN_SECONDS}, {WAKEUP_MAX_SECONDS}] s range."
        ));
    }
    if let Some(reason) = reason {
        lines.push(format!("Reason: {reason}."));
    }
    lines.push(
        "Session-only one-shot; not persisted. Call LoopWakeup again before ending the turn to keep the loop alive; omit it to end the loop.".to_owned(),
    );
    let return_display = format!(
        "Loop wakeup {} scheduled for {}{}",
        schedule.id,
        schedule.scheduled_for,
        reason.map_or_else(String::new, |reason| format!(" — {reason}"))
    );
    Ok(ToolExecutionOutput::with_display(
        lines.join("\n"),
        json!({"displayText": return_display}),
    ))
}

fn display_schedule(job: &crate::services::cron_scheduler::CronJob) -> String {
    if job.cron_expr == "@wakeup" {
        if let Some(fire_at_ms) = job.fire_at_ms {
            if let Some(fire_at) = DateTime::<Utc>::from_timestamp_millis(fire_at_ms) {
                return format!(
                    "wakeup at {}",
                    fire_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                );
            }
        }
    }
    human_readable_cron(&job.cron_expr)
}

fn required_string<'a>(
    args: &'a serde_json::Map<String, Value>,
    key: &str,
    tool: &str,
) -> Result<&'a str, String> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{tool} parameter `{key}` must be a non-empty string."))
}

fn optional_bool(args: &serde_json::Map<String, Value>, key: &str) -> Result<Option<bool>, String> {
    args.get(key)
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| format!("{key} must be a boolean."))
        })
        .transpose()
}

fn only_keys(
    args: &serde_json::Map<String, Value>,
    allowed: &[&str],
    tool: &str,
) -> Result<(), String> {
    if let Some(key) = args.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(format!("{tool} does not accept the `{key}` parameter."));
    }
    Ok(())
}

fn truncate_prompt(prompt: &str, max_chars: usize) -> String {
    let mut chars = prompt.chars();
    let prefix = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        let mut prefix = prompt
            .chars()
            .take(max_chars.saturating_sub(3))
            .collect::<String>();
        prefix.push_str("...");
        prefix
    } else {
        prefix
    }
}

fn format_requested(delay_seconds: f64) -> String {
    format!("{delay_seconds}s")
}

fn format_number(days: f64) -> String {
    if days.fract() == 0.0 {
        format!("{}", days as u64)
    } else {
        days.to_string()
    }
}
