//! Autonomous captcha-solving loop for the browser surface.
//!
//! The tool captures tab screenshots through the same CDP path as
//! `get_browser_state`'s `include_screenshot` and submits them to a
//! caller-provided multimodal solver endpoint (an OpenAI-compatible
//! `chat/completions` POST is the default contract: the image goes in
//! as an `image_url` data URL, the answer comes back as text). Between
//! solver rounds the loop drives trusted browser input — click and
//! type — through the same CDP session `browser_click`/`browser_type`
//! use, then re-captures to verify.
//!
//! Registered by default from `register_browser_tools` like the rest
//! of the browser surface. Requires an exact binding like any other
//! mutation, and no-ops into a structured error if the solver
//! endpoint environment is missing.
//!
//! Environment variables (read per invocation, cheap):
//!
//! - `QWEN_VISION_URL` — full chat-completions endpoint.
//! - `QWEN_VISION_KEY` — bearer token.
//! - `QWEN_VISION_MODEL` — vision-capable model id.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::protocol::ToolResult;
use crate::tool::{ProtectedResourceOwnership, Tool, ToolDef, ToolRegistry};
use crate::tool_args::ArgsExt;

use super::cdp_ws::CdpConnection;
use super::engine::BrowserEngine;
use super::tools::{browser_protected_resource_scope, browser_resource_ownership, require_explicit_session};

// ── Tunables ─────────────────────────────────────────────────────────────────

/// A solver round with no visible change after the dispatched input is
/// almost always a wrong answer burning another capture round. Cap the
/// damage; the tool surfaces the transcript so callers can retry with a
/// bigger budget deliberately.
const MAX_ATTEMPTS_DEFAULT: u64 = 6;

/// Solver HTTP round budget.
const SOLVER_TIMEOUT: Duration = Duration::from_secs(45);

/// Polling slice between a dispatched action and the next capture.
/// Captchas animate/challenge-refresh asynchronously, so a short dwell
/// plus a fresh capture beats one long fixed sleep.
const SETTLE_POLL_MS: u64 = 400;
const SETTLE_ROUNDS: u32 = 8;

// ── Tool definition ──────────────────────────────────────────────────────────

pub struct BrowserCaptchaSolverTool {
    engine: Arc<BrowserEngine>,
    def: ToolDef,
}

impl BrowserCaptchaSolverTool {
    pub fn new(engine: Arc<BrowserEngine>) -> Self {
        let def = ToolDef {
            name: "browser_solve_captcha".into(),
            description: "Attempt to solve an on-page captcha autonomously. Captures the \
                bound tab's viewport (same CDP route as get_browser_state \
                include_screenshot), submits it to the configured multimodal solver, and \
                drives the answer back into the page via trusted input, then re-captures to \
                verify. Requires an exact binding (same as mutations). The solver endpoint \
                is read from QWEN_VISION_URL / QWEN_VISION_KEY / QWEN_VISION_MODEL at call \
                time; if unset the tool returns a structured error rather than guessing."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "target_id": super::tools::schema_target_id(),
                    "tab_id": super::tools::schema_tab_id(),
                    "session": super::tools::schema_session(),
                    "max_attempts": {
                        "type": "integer",
                        "default": 6,
                        "minimum": 1,
                        "maximum": 12,
                        "description": "Solver round cap. Each round is one screenshot \
                            + one model call + zero or more input actions."
                    },
                    "prompt_hint": {
                        "type": "string",
                        "description": "Extra solver context, e.g. \"click every \
                            traffic light\" or \"type the 4 characters you see\"."
                    },
                    "click_answer": {
                        "type": "boolean",
                        "default": false,
                        "description": "Solver returns absolute viewport CSS \
                            coordinates to click (comma, semicolon, whitespace, or \
                            JSON-array separated pairs) instead of text to type."
                    },
                    "submit_selector_hint": {
                        "type": "string",
                        "description": "Optional CSS selector to click after a typed \
                            answer (e.g. `#submit`). Omitted means Enter."
                    },
                },
                "additionalProperties": true
            }),
            read_only: false,
            destructive: false,
            idempotent: false,
            open_world: true,
        };
        Self { engine, def }
    }
}

#[async_trait]
impl Tool for BrowserCaptchaSolverTool {
    fn def(&self) -> &ToolDef {
        &self.def
    }

    async fn protected_resource_ownership(
        &self,
        adapter_id: &str,
        args: &Value,
    ) -> ProtectedResourceOwnership {
        if adapter_id == "browser_bound_input" {
            browser_resource_ownership(&self.engine, args)
        } else {
            ProtectedResourceOwnership::UserOwned
        }
    }

    async fn protected_resource_scope(
        &self,
        adapter_id: &str,
        args: &Value,
    ) -> Result<Option<Value>, String> {
        if adapter_id == "browser_bound_input" {
            browser_protected_resource_scope(&self.engine, args, "browser_solve_captcha").await
        } else {
            Ok(None)
        }
    }

    async fn invoke(&self, args: Value) -> ToolResult {
        let (target_id, tab_id) =
            match (args.require_str("target_id"), args.require_str("tab_id")) {
                (Ok(t), Ok(tab)) => (t, tab),
                (Err(e), _) | (_, Err(e)) => return e,
            };
        let session = match require_explicit_session(&args) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let max_attempts = args.u64_or("max_attempts", MAX_ATTEMPTS_DEFAULT).clamp(1, 12);
        let prompt_hint = args.opt_str("prompt_hint").unwrap_or_default();
        let click_answer = args.bool_or("click_answer", false);
        let submit_selector_hint = args.opt_str("submit_selector_hint");

        let solver = match SolverEndpoint::from_env() {
            Ok(endpoint) => endpoint,
            Err(missing) => {
                return ToolResult::error(format!(
                    "captcha solving needs a vision endpoint; set {missing} \
                     (an OpenAI-compatible chat/completions URL is expected)"
                ))
            }
        };

        let validated = match self
            .engine
            .revalidate_for_mutation(&session, &target_id, Some(&tab_id))
            .await
        {
            Ok(v) => v,
            Err(refusal) => return refusal.to_tool_result(),
        };
        let conn = validated.conn.clone();
        let cdp = validated.cdp_session;

        let mut rounds: Vec<Value> = Vec::new();
        let mut attempts_used = 0u64;
        let mut solved = false;

        for attempt in 1..=max_attempts {
            attempts_used = attempt;
            // Fresh capture every round: captchas animate and refresh
            // unpredictably, so every round must see the live viewport.
            let shot = match capture_viewport(&conn, &cdp).await {
                Ok(shot) => shot,
                Err(detail) => {
                    return outcome_tool_result(CaptchaOutcome {
                        solved: false,
                        attempts_used,
                        rounds,
                        error: Some(format!("screenshot capture failed: {detail}")),
                    });
                }
            };
            let instruction = build_instruction(attempt, max_attempts, &prompt_hint, click_answer);
            let answer = match solver.ask(&shot, &instruction).await {
                Ok(answer) => answer,
                Err(detail) => {
                    return outcome_tool_result(CaptchaOutcome {
                        solved: false,
                        attempts_used,
                        rounds,
                        error: Some(format!("solver request failed: {detail}")),
                    });
                }
            };
            let round = json!({ "attempt": attempt, "model_answer": answer.text });
            rounds.push(round);

            if answer.signals_stop {
                solved = true;
                break;
            }

            if click_answer {
                match dispatch_solution_clicks(&conn, &cdp, &answer.text).await {
                    Ok(n) => {
                        if let Some(round) = rounds.last_mut() {
                            round["clicked_points"] = json!(n);
                        }
                    }
                    Err(detail) => {
                        return outcome_tool_result(CaptchaOutcome {
                            solved: false,
                            attempts_used,
                            rounds,
                            error: Some(detail),
                        });
                    }
                }
            } else {
                let text = answer.text.trim();
                if text.is_empty() {
                    if let Some(round) = rounds.last_mut() {
                        round["input_skipped"] = json!("empty answer");
                    }
                } else {
                    if let Err(detail) = type_text(&conn, &cdp, text).await {
                        return outcome_tool_result(CaptchaOutcome {
                            solved: false,
                            attempts_used,
                            rounds,
                            error: Some(format!("typing failed: {detail}")),
                        });
                    }
                    match &submit_selector_hint {
                        Some(selector) => {
                            if let Err(detail) = click_selector(&conn, &cdp, selector).await {
                                return outcome_tool_result(CaptchaOutcome {
                                    solved: false,
                                    attempts_used,
                                    rounds,
                                    error: Some(format!("submit click failed: {detail}")),
                                });
                            }
                        }
                        None => {
                            if let Err(detail) = press_enter(&conn, &cdp).await {
                                return outcome_tool_result(CaptchaOutcome {
                                    solved: false,
                                    attempts_used,
                                    rounds,
                                    error: Some(format!("Enter dispatch failed: {detail}")),
                                });
                            }
                        }
                    }
                }
            }

            // Dwell then verify. The loop's convergence signal is the
            // solver itself (next round's capture): a captcha that keeps
            // rendering new challenges means the prior answer was wrong.
            for _ in 0..SETTLE_ROUNDS {
                tokio::time::sleep(Duration::from_millis(SETTLE_POLL_MS)).await;
            }
        }

        outcome_tool_result(CaptchaOutcome {
            solved,
            attempts_used,
            rounds,
            error: None,
        })
    }
}

// ── Outcome envelope ─────────────────────────────────────────────────────────

struct CaptchaOutcome {
    solved: bool,
    attempts_used: u64,
    rounds: Vec<Value>,
    error: Option<String>,
}

fn outcome_tool_result(outcome: CaptchaOutcome) -> ToolResult {
    let status = if outcome.solved { "ok" } else { "incomplete" };
    let mut result = ToolResult::text(format!(
        "captcha solving {status}: solved={}, attempts used={}",
        outcome.solved, outcome.attempts_used
    ));
    result.structured_content = Some(json!({
        "status": status,
        "solved": outcome.solved,
        "attempts_used": outcome.attempts_used,
        "rounds": outcome.rounds,
        "error": outcome.error,
    }));
    result
}

// ── Solver endpoint (OpenAI-compatible chat completions) ─────────────────────

struct SolverEndpoint {
    url: String,
    key: String,
    model: String,
    agent: ureq::Agent,
}

struct SolverAnswer {
    text: String,
    /// The solver may report the captcha is already cleared (e.g. on a
    /// verify round where the challenge has visibly gone). A bare
    /// "solved"/"done"/"none" answer short-circuits the loop instead of
    /// dispatching nonsense input.
    signals_stop: bool,
}

impl SolverEndpoint {
    fn from_env() -> Result<Self, String> {
        let missing = |name: &str| format!("{name} (environment variable) is not set");
        let url = std::env::var("QWEN_VISION_URL")
            .map_err(|_| missing("QWEN_VISION_URL"))?
            .trim_end_matches('/')
            .to_owned();
        let key = std::env::var("QWEN_VISION_KEY").map_err(|_| missing("QWEN_VISION_KEY"))?;
        let model =
            std::env::var("QWEN_VISION_MODEL").map_err(|_| missing("QWEN_VISION_MODEL"))?;
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(SOLVER_TIMEOUT))
            .build()
            .new_agent();
        Ok(Self { url, key, model, agent })
    }

    async fn ask(&self, png_base64: &str, instruction: &str) -> Result<SolverAnswer, String> {
        let payload = json!({
            "model": self.model,
            "max_tokens": 256,
            "temperature": 0.0,
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": instruction },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": format!("data:image/png;base64,{png_base64}")
                        }
                    }
                ]
            }]
        });
        // ureq is blocking; the tool dispatches on the runtime's blocking
        // pool so a 45 s solver round never starves the CDP readers.
        let agent = self.agent.clone();
        let url = self.url.clone();
        let key = self.key.clone();
        let payload = payload.clone();
        // Spawned as blocking work: `spawn_blocking` keeps the
        // multi-threaded runtime responsive while ureq's socket is parked.
        let reply = tokio::task::spawn_blocking(move || {
            agent
                .post(&url)
                .header("Content-Type", "application/json")
                .header("Authorization", &format!("Bearer {key}"))
                .send_json(&payload)
        })
        .await
        .map_err(|error| format!("solver task join failed: {error}"))?
        .map_err(|error| format!("solver HTTP call failed: {error}"))?;
        let body: Value = reply
            .into_body()
            .read_json()
            .map_err(|error| format!("solver response was not JSON: {error}"))?;
        let text = body
            .get("choices")
            .and_then(|choices| choices.get(0))
            .and_then(|choice| choice.get("message"))
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str)
            .ok_or_else(|| format!("solver response missing choices[0].message.content: {body}"))?
            .trim()
            .to_owned();
        let normalized = text.to_ascii_lowercase();
        let signals_stop = ["solved", "already complete", "no captcha", "clear"]
            .iter()
            .any(|marker| normalized.contains(marker));
        Ok(SolverAnswer { text, signals_stop })
    }
}

// ── Instruction template ─────────────────────────────────────────────────────

fn build_instruction(
    attempt: u64,
    max_attempts: u64,
    prompt_hint: &str,
    click_answer: bool,
) -> String {
    let shape = if click_answer {
        "You are controlling a browser. Reply with ONLY the click coordinates, one per \
         line, formatted as `x,y` pairs in viewport CSS pixels (the image dimensions). \
         No prose, no markdown."
    } else {
        "Read the captcha on screen and reply with ONLY the exact characters to type \
         into the visible input field, in order. No prose, no markdown, no spaces \
         unless the challenge shows them."
    };
    let attempt_note = if attempt > 1 {
        format!(
            " This is attempt {attempt} of {max_attempts}; a previous answer was wrong. \
             Look very carefully at the current challenge."
        )
    } else {
        String::new()
    };
    let hint = if prompt_hint.is_empty() {
        String::new()
    } else {
        format!(" Task context from the caller: {prompt_hint}")
    };
    format!("{shape}{attempt_note}{hint}")
}

// ── CDP primitives (same route as browser_click / browser_type) ─────────────

async fn capture_viewport(
    conn: &Arc<CdpConnection>,
    cdp: &str,
) -> Result<String, String> {
    let metrics = conn
        .call(Some(cdp), "Page.getLayoutMetrics", json!({}))
        .await
        .map_err(|error| format!("Page.getLayoutMetrics failed: {error}"))?;
    let viewport = metrics
        .get("cssVisualViewport")
        .or_else(|| metrics.get("visualViewport"))
        .ok_or_else(|| "Page.getLayoutMetrics missing visual viewport".to_owned())?;
    let number = |field: &str| -> Result<f64, String> {
        viewport
            .get(field)
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite())
            .ok_or_else(|| format!("missing or non-finite visual viewport field {field}"))
    };
    let page_x = number("pageX")?;
    let page_y = number("pageY")?;
    let width = number("clientWidth")?;
    let height = number("clientHeight")?;
    if page_x < 0.0 || page_y < 0.0 || width <= 0.0 || height <= 0.0 {
        return Err("implausible visual viewport metrics".to_owned());
    }
    let reply = conn
        .call(
            Some(cdp),
            "Page.captureScreenshot",
            json!({
                "format": "png",
                "fromSurface": true,
                "captureBeyondViewport": false,
                "clip": {
                    "x": page_x,
                    "y": page_y,
                    "width": width,
                    "height": height,
                    "scale": 1.0,
                }
            }),
        )
        .await
        .map_err(|error| format!("Page.captureScreenshot failed: {error}"))?;
    reply
        .get("data")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| "Page.captureScreenshot returned no data".to_owned())
}

async fn type_text(
    conn: &Arc<CdpConnection>,
    cdp: &str,
    text: &str,
) -> Result<(), String> {
    conn.call(Some(cdp), "Input.insertText", json!({ "text": text }))
        .await
        .map_err(|error| format!("Input.insertText failed: {error}"))?;
    Ok(())
}

async fn press_enter(
    conn: &Arc<CdpConnection>,
    cdp: &str,
) -> Result<(), String> {
    for phase in ["keyDown", "keyUp"] {
        conn.call(
            Some(cdp),
            "Input.dispatchKeyEvent",
            json!({
                "type": phase,
                "key": "Enter",
                "code": "Enter",
                "windowsVirtualKeyCode": 13,
                "nativeVirtualKeyCode": 13,
                "text": if phase == "keyDown" { "\r" } else { "" },
            }),
        )
        .await
        .map_err(|error| format!("Input.dispatchKeyEvent({phase}) failed: {error}"))?;
    }
    Ok(())
}

async fn click_selector(
    conn: &Arc<CdpConnection>,
    cdp: &str,
    selector: &str,
) -> Result<(), String> {
    let quad_center = |box_model: &Value| -> Option<(f64, f64)> {
        let quad = box_model
            .get("model")?
            .get("quad")?
            .as_array()?;
        if quad.len() < 4 {
            return None;
        }
        let mut sum_x = 0.0;
        let mut sum_y = 0.0;
        for corner in quad {
            let x = corner.get(0)?.as_f64()?;
            let y = corner.get(1)?.as_f64()?;
            sum_x += x;
            sum_y += y;
        }
        Some((sum_x / quad.len() as f64, sum_y / quad.len() as f64))
    };
    let resolved = conn
        .call(
            Some(cdp),
            "Runtime.evaluate",
            json!({
                "expression": format!("document.querySelector({})", serde_json::to_string(selector).map_err(|e| e.to_string())?),
                "returnByValue": false,
            }),
        )
        .await
        .map_err(|error| format!("Runtime.evaluate for selector failed: {error}"))?;
    let object_id = resolved
        .pointer("/result/objectId")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("selector did not resolve to a live node: {selector}"))?
        .to_owned();
    let box_model = conn
        .call(
            Some(cdp),
            "DOM.getBoxModel",
            json!({ "objectId": object_id }),
        )
        .await
        .map_err(|error| format!("DOM.getBoxModel failed: {error}"))?;
    let (center_x, center_y) = quad_center(&box_model)
        .ok_or_else(|| format!("selector {selector} has no visible bounding box"))?;
    for phase in ["mousePressed", "mouseReleased"] {
        conn.call(
            Some(cdp),
            "Input.dispatchMouseEvent",
            json!({
                "type": phase,
                "x": center_x,
                "y": center_y,
                "button": "left",
                "clickCount": 1,
            }),
        )
        .await
        .map_err(|error| format!("Input.dispatchMouseEvent failed: {error}"))?;
    }
    Ok(())
}

/// Parse and dispatch the solver's coordinate answer. Accepts the
/// pragmatic shapes a vision model actually emits: `x1,y1 x2,y2 …`,
/// semicolon-separated pairs, or a bare JSON array of `{x, y}` objects
/// or `[x, y]` pairs. Coordinates that are non-finite or fall outside a
/// plausible viewport (negative, or beyond 10000 CSS px) are skipped; a
/// round with zero usable points surfaces as an error rather than
/// silently submitting.
async fn dispatch_solution_clicks(
    conn: &Arc<CdpConnection>,
    cdp: &str,
    model_answer: &str,
) -> Result<usize, String> {
    let plausible = |v: f64| v.is_finite() && (0.0..=10_000.0).contains(&v);
    let parsed: Vec<(f64, f64)> = if model_answer.trim_start().starts_with('[') {
        serde_json::from_str::<Value>(model_answer.trim())
            .ok()
            .and_then(|parsed| parsed.as_array().cloned())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| {
                if let Ok(pair) = serde_json::from_value::<Vec<f64>>(entry.clone()) {
                    if pair.len() == 2 && plausible(pair[0]) && plausible(pair[1]) {
                        return Some((pair[0], pair[1]));
                    }
                }
                let x = entry.get("x")?.as_f64()?;
                let y = entry.get("y")?.as_f64()?;
                if plausible(x) && plausible(y) {
                    Some((x, y))
                } else {
                    None
                }
            })
            .collect()
    } else {
        model_answer
            .split(|separator: char| separator == ';' || separator.is_whitespace())
            .filter_map(|token| {
                let (x_raw, y_raw) = token.split_once(',')?;
                let x = x_raw.trim().parse::<f64>().ok()?;
                let y = y_raw.trim().parse::<f64>().ok()?;
                if plausible(x) && plausible(y) {
                    Some((x, y))
                } else {
                    None
                }
            })
            .collect()
    };
    if parsed.is_empty() {
        return Err(format!(
            "no usable click coordinates parsed from model answer {model_answer:?}"
        ));
    }
    for (x, y) in &parsed {
        for phase in ["mousePressed", "mouseReleased"] {
            conn.call(
                Some(cdp),
                "Input.dispatchMouseEvent",
                json!({
                    "type": phase,
                    "x": x,
                    "y": y,
                    "button": "left",
                    "clickCount": 1,
                }),
            )
            .await
            .map_err(|error| format!("Input.dispatchMouseEvent failed at ({x},{y}): {error}"))?;
        }
    }
    Ok(parsed.len())
}

// ── Registration ─────────────────────────────────────────────────────────────

/// Registered from `register_browser_tools` alongside the standard
/// surface — see the module docs.
pub fn register_captcha_solver(engine: &Arc<BrowserEngine>, registry: &mut ToolRegistry) {
    registry.register(Box::new(BrowserCaptchaSolverTool::new(engine.clone())));
}