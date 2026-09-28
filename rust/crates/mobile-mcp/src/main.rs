use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use mobile_mcp::logger;
use mobile_mcp::protocol::handle_json_line;
use mobile_mcp::tools::MobileServer;
use serde_json::{Value, json};

const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const MAX_QUEUED_REQUESTS: usize = 4;
const MAX_QUEUED_RESPONSES: usize = 4;
const BIN_NAME: &str = env!("CARGO_BIN_NAME");

enum CliMode {
    Help,
    Version,
    Stdio,
    Listen(String),
}

struct StdioWork {
    line: Vec<u8>,
    request_id_key: Option<String>,
    response_id: Option<Value>,
    is_notification: bool,
    cancelled: Arc<AtomicBool>,
}

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let mode = match parse_cli_args(&args) {
        Ok(mode) => mode,
        Err(error) => {
            logger::error(&error);
            std::process::exit(1);
        }
    };
    match mode {
        CliMode::Help => print_help(),
        CliMode::Version => println!("{}", mobile_mcp::SERVER_VERSION),
        CliMode::Stdio => {
            if let Err(error) = run_stdio() {
                logger::error(&format!("mobile-mcp: {error}"));
                std::process::exit(1);
            }
        }
        CliMode::Listen(value) => {
            let (host, port) = match mobile_mcp::sse::parse_listen(&value) {
                Ok(listen) => listen,
                Err(error) => {
                    logger::error(&error);
                    std::process::exit(1);
                }
            };
            if let Err(error) = mobile_mcp::sse::run(&host, port) {
                logger::error(&format!("mobile-mcp: {error}"));
                std::process::exit(1);
            }
        }
    }
}

fn parse_cli_args(args: &[String]) -> Result<CliMode, String> {
    if args.iter().any(|arg| arg == "--version" || arg == "-V") {
        return Ok(CliMode::Version);
    }
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Ok(CliMode::Help);
    }

    let mut listen = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--stdio" => {}
            "--listen" => {
                let Some(value) = args.get(index + 1) else {
                    return Err("error: option '--listen <listen>' argument missing".into());
                };
                listen = Some(value.clone());
                index += 1;
            }
            value if value.starts_with("--listen=") => {
                listen = Some(value["--listen=".len()..].to_owned());
            }
            value if value.starts_with('-') => {
                return Err(format!("error: unknown option '{value}'"));
            }
            value => {
                return Err(format!(
                    "error: too many arguments. Expected 0 arguments but got 1. ('{value}')"
                ));
            }
        }
        index += 1;
    }

    Ok(listen.map_or(CliMode::Stdio, CliMode::Listen))
}

fn print_help() {
    println!(
        "Usage: {BIN_NAME} [options]\n\nOptions:\n  --version             output the version number\n  --listen <listen>     Start SSE server on [host:]port\n  --stdio               Start stdio server (default)\n  -h, --help            display help for command"
    );
}

fn run_stdio() -> io::Result<()> {
    let stdin = io::stdin();
    let stopping = Arc::new(AtomicBool::new(false));
    let signal_stopping = Arc::clone(&stopping);
    ctrlc::set_handler(move || signal_stopping.store(true, Ordering::Release)).map_err(
        |error| io::Error::other(format!("failed to install shutdown handler: {error}")),
    )?;

    let (input_sender, input_receiver) = mpsc::sync_channel(1);
    let input_stopping = Arc::clone(&stopping);
    let input_reader = thread::Builder::new()
        .name("mobile-mcp-stdio-input".into())
        .spawn(move || {
            let mut input = stdin.lock();
            loop {
                if input_stopping.load(Ordering::Acquire) {
                    break;
                }
                let result = read_bounded_line(&mut input, MAX_FRAME_BYTES);
                let finished = !matches!(&result, Ok(Some(_)));
                if input_sender.send(result).is_err() || finished {
                    break;
                }
            }
        })?;
    let (work_sender, work_receiver) = mpsc::sync_channel::<StdioWork>(MAX_QUEUED_REQUESTS);
    let (response_sender, response_receiver) = mpsc::sync_channel::<Value>(MAX_QUEUED_RESPONSES);
    let pending_requests = Arc::new(Mutex::new(HashMap::<String, Arc<AtomicBool>>::new()));

    let worker_pending = Arc::clone(&pending_requests);
    let worker_responses = response_sender.clone();
    let worker = thread::Builder::new()
        .name("mobile-mcp-stdio-dispatch".into())
        .spawn(move || {
            let mut server = MobileServer::default();
            while let Ok(work) = work_receiver.recv() {
                let response = match handle_json_line(&mut server, &work.line) {
                    Ok(response) => response,
                    Err(error) => Some(json!({
                        "jsonrpc":"2.0",
                        "id":null,
                        "error":{"code":-32700,"message":error.to_string()}
                    })),
                };

                let cancelled = if let Some(key) = &work.request_id_key {
                    let mut pending = worker_pending
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if pending
                        .get(key)
                        .is_some_and(|current| Arc::ptr_eq(current, &work.cancelled))
                    {
                        pending.remove(key);
                    }
                    work.cancelled.load(Ordering::Acquire)
                } else {
                    false
                };

                // The TypeScript SDK suppresses a handler result or error once
                // notifications/cancelled has aborted that request. The tool
                // itself remains synchronous and does not receive its signal.
                if !cancelled
                    && let Some(response) = response
                    && worker_responses.send(response).is_err()
                {
                    break;
                }
            }
            // Dropping `server` here runs MobileServer's recording cleanup.
        })?;

    let output = thread::Builder::new()
        .name("mobile-mcp-stdio-output".into())
        .spawn(move || {
            let stdout = io::stdout();
            let mut writer = io::BufWriter::new(stdout.lock());
            while let Ok(response) = response_receiver.recv() {
                write_message(&mut writer, &response)?;
            }
            Ok::<(), io::Error>(())
        })?;

    logger::error("mobile-mcp server running on stdio");

    let mut input_finished = false;
    let input_result = (|| -> io::Result<()> {
        loop {
            if stopping.load(Ordering::Acquire) {
                logger::error("mobile-mcp stdio shutdown requested");
                break;
            }
            let next = match input_receiver.recv_timeout(Duration::from_millis(50)) {
                Ok(Ok(next)) => next,
                Ok(Err(error)) => {
                    input_finished = true;
                    return Err(error);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    input_finished = true;
                    break;
                }
            };
            let Some((line, oversized)) = next else {
                input_finished = true;
                break;
            };
            if oversized {
                response_sender
                    .send(json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Request exceeds the 8 MiB frame limit"}}))
                    .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "stdout worker stopped"))?;
                continue;
            }
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }

            if let Some(target) = cancellation_notification(&line) {
                if let Some(key) = target {
                    let pending = pending_requests
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(cancelled) = pending.get(&key) {
                        cancelled.store(true, Ordering::Release);
                    }
                }
                continue;
            }

            let request_value = serde_json::from_slice::<Value>(&line).ok();
            let request_id = request_value.as_ref().and_then(response_request_id);
            let request_id_key = request_value.as_ref().and_then(trackable_request_id);
            let is_notification = request_value.as_ref().is_some_and(is_valid_notification);
            let cancelled = Arc::new(AtomicBool::new(false));
            if let Some(key) = request_id_key.as_ref() {
                pending_requests
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(key.clone(), Arc::clone(&cancelled));
            }
            let work = StdioWork {
                line,
                request_id_key,
                response_id: request_id,
                is_notification,
                cancelled,
            };
            match work_sender.try_send(work) {
                Ok(()) => {}
                Err(mpsc::TrySendError::Full(work)) => {
                    if let Some(key) = &work.request_id_key {
                        remove_pending_request(&pending_requests, key, &work.cancelled);
                    }
                    if !work.is_notification {
                        response_sender
                            .send(json!({
                                "jsonrpc":"2.0",
                                "id":work.response_id.unwrap_or(Value::Null),
                                "error":{"code":-32603,"message":"Server is busy; too many requests are pending"}
                            }))
                            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "stdout worker stopped"))?;
                    }
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "stdio dispatch worker stopped",
                    ));
                }
            }
        }
        Ok(())
    })();

    if input_finished {
        input_reader
            .join()
            .map_err(|_| io::Error::other("stdio input worker panicked"))?;
    }
    drop(work_sender);
    let worker_result = worker
        .join()
        .map_err(|_| io::Error::other("stdio dispatch worker panicked"));
    drop(response_sender);
    let output_result = output
        .join()
        .map_err(|_| io::Error::other("stdio output worker panicked"))?;
    input_result?;
    worker_result?;
    output_result
}

fn cancellation_notification(line: &[u8]) -> Option<Option<String>> {
    let value = serde_json::from_slice::<Value>(line).ok()?;
    let object = value.as_object()?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || object.get("method").and_then(Value::as_str) != Some("notifications/cancelled")
        || object.contains_key("id")
        || object
            .keys()
            .any(|key| !matches!(key.as_str(), "jsonrpc" | "method" | "params"))
        || object
            .get("params")
            .is_some_and(|params| !params.is_object())
    {
        return None;
    }
    let key = object
        .get("params")
        .and_then(Value::as_object)
        .and_then(|params| params.get("requestId"))
        .and_then(request_id_key);
    Some(key)
}

fn trackable_request_id(value: &Value) -> Option<String> {
    let object = value.as_object()?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || !matches!(
            object.get("method").and_then(Value::as_str),
            Some("initialize" | "ping" | "tools/list" | "tools/call")
        )
    {
        return None;
    }
    request_id_key(object.get("id")?)
}

fn response_request_id(value: &Value) -> Option<Value> {
    let object = value.as_object()?;
    let id = object.get("id")?;
    request_id_key(id).map(|_| id.clone())
}

fn request_id_key(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(format!("s:{value}")),
        Value::Number(value) => {
            let number = value.as_f64()?;
            (number.is_finite() && number.fract() == 0.0).then(|| {
                if number == 0.0 {
                    "n:0".to_owned()
                } else {
                    format!("n:{number}")
                }
            })
        }
        _ => None,
    }
}

fn is_valid_notification(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    !object.contains_key("id")
        && object.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
        && object.get("method").is_some_and(Value::is_string)
        && !object
            .keys()
            .any(|key| !matches!(key.as_str(), "jsonrpc" | "method" | "params"))
        && object.get("params").is_none_or(Value::is_object)
}

fn remove_pending_request(
    pending: &Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
    key: &str,
    token: &Arc<AtomicBool>,
) {
    let mut pending = pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if pending
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, token))
    {
        pending.remove(key);
    }
}

fn read_bounded_line(
    reader: &mut impl BufRead,
    limit: usize,
) -> io::Result<Option<(Vec<u8>, bool)>> {
    let mut line = Vec::new();
    let mut oversized = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if line.is_empty() && !oversized {
                Ok(None)
            } else {
                Ok(Some((line, oversized)))
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        let room = limit.saturating_add(1).saturating_sub(line.len());
        if room > 0 {
            line.extend_from_slice(&available[..consumed.min(room)]);
        }
        if line.len() > limit {
            oversized = true;
            line.clear();
        }
        let done = newline.is_some();
        reader.consume(consumed);
        if done {
            return Ok(Some((line, oversized)));
        }
    }
}

fn write_message(writer: &mut impl Write, message: &serde_json::Value) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, message)?;
    writer.write_all(b"\n")?;
    writer.flush()
}
