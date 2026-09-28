use std::net::SocketAddr;
use std::time::Duration;

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use reqwest::Url;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, client_async_tls_with_config};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
const FINISH_TIMEOUT: Duration = Duration::from_secs(60);
const RETRY_DELAY: Duration = Duration::from_millis(200);
const CLOSE_TIMEOUT: Duration = Duration::from_millis(250);
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
pub(super) const MAX_AUDIO_FRAME_BYTES: usize = 32 * 1024;
const MAX_TRANSCRIPT_BYTES: usize = 1024 * 1024;
const MAX_SERVER_ERROR_BYTES: usize = 200;

pub(super) const AUDIO_QUEUE_CAPACITY: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Transport {
    Qwen,
    DashScopeTask,
}

pub(super) struct Config {
    pub transport: Transport,
    pub model: String,
    pub base_url: Url,
    pub api_key: Option<String>,
    pub language: Option<String>,
    pub keyterms_context: Option<String>,
}

pub(super) enum AudioCommand {
    Audio(Vec<u8>),
    Finish { submit: bool },
}

pub(super) struct Output {
    pub text: String,
    pub submit: bool,
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct OpenedSocket {
    socket: Socket,
    task_id: Option<String>,
}

pub(super) async fn transcribe(
    config: Config,
    addresses: Vec<SocketAddr>,
    mut audio: mpsc::Receiver<AudioCommand>,
) -> Result<Output, String> {
    if addresses.is_empty() {
        return Err("voice model DNS lookup returned no addresses".to_owned());
    }
    let mut attempts = 0;
    let opened = loop {
        let result = tokio::time::timeout(CONNECT_TIMEOUT, async {
            match config.transport {
                Transport::Qwen => open_qwen(&config, &addresses).await,
                Transport::DashScopeTask => open_dashscope(&config, &addresses).await,
            }
        })
        .await
        .unwrap_or_else(|_| Err("Voice stream connection timed out".to_owned()));
        match result {
            Ok(opened) => break opened,
            Err(error) if attempts == 0 && retryable_open_error(&error) => {
                attempts += 1;
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    };
    let mut socket = opened.socket;

    let result = match config.transport {
        Transport::Qwen => run_qwen(&mut socket, &mut audio).await,
        Transport::DashScopeTask => {
            run_dashscope(
                &mut socket,
                opened.task_id.as_deref().unwrap_or_default(),
                &mut audio,
            )
            .await
        }
    };
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, socket.close(None)).await;
    result
}

async fn open_qwen(config: &Config, addresses: &[SocketAddr]) -> Result<OpenedSocket, String> {
    let url = stream_url(config, Transport::Qwen)?;
    let mut socket = connect(&url, &config.api_key, addresses).await?;
    tokio::time::timeout(CONNECT_TIMEOUT, async {
        loop {
            let message = socket
                .next()
                .await
                .ok_or_else(|| "Qwen realtime connection closed before it was ready".to_owned())?
                .map_err(|_| "Qwen realtime connection failed".to_owned())?;
            match message {
                Message::Text(text) => {
                    let event = parse_json(text.as_str())?;
                    match event.get("type").and_then(Value::as_str) {
                        Some("session.created") => {
                            let mut transcription = serde_json::Map::new();
                            if let Some(language) = &config.language {
                                transcription.insert("language".to_owned(), json!(language));
                            }
                            if let Some(keyterms) = &config.keyterms_context {
                                transcription.insert("corpus_text".to_owned(), json!(keyterms));
                            }
                            send_qwen_json(
                                &mut socket,
                                json!({
                                    "type": "session.update",
                                    "session": {
                                        "input_audio_format": "pcm",
                                        "sample_rate": 16000,
                                        "input_audio_transcription": Value::Object(transcription),
                                        "turn_detection": null,
                                    }
                                }),
                            )
                            .await?;
                        }
                        Some("session.updated") => return Ok(()),
                        Some("error")
                        | Some("conversation.item.input_audio_transcription.failed") => {
                            return Err(server_error(&event, "Qwen realtime setup failed"));
                        }
                        _ => {}
                    }
                }
                Message::Close(_) => {
                    return Err("Qwen realtime connection closed before it was ready".to_owned());
                }
                Message::Ping(payload) => {
                    socket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|_| "Qwen realtime connection failed".to_owned())?;
                }
                _ => {}
            }
        }
    })
    .await
    .map_err(|_| "Qwen realtime connection timed out".to_owned())??;
    Ok(OpenedSocket {
        socket,
        task_id: None,
    })
}

async fn open_dashscope(config: &Config, addresses: &[SocketAddr]) -> Result<OpenedSocket, String> {
    let url = stream_url(config, Transport::DashScopeTask)?;
    let mut socket = connect(&url, &config.api_key, addresses).await?;
    let task_id = uuid::Uuid::new_v4().to_string();
    let mut parameters = json!({"format": "pcm", "sample_rate": 16000});
    if let Some(language) = &config.language {
        parameters["language_hints"] = json!([language]);
    }
    send_json(
        &mut socket,
        json!({
            "header": {"action": "run-task", "task_id": task_id, "streaming": "duplex"},
            "payload": {
                "task_group": "audio",
                "task": "asr",
                "function": "recognition",
                "model": config.model,
                "parameters": parameters,
                "input": {}
            }
        }),
    )
    .await?;
    tokio::time::timeout(CONNECT_TIMEOUT, async {
        loop {
            let message = socket
                .next()
                .await
                .ok_or_else(|| "DashScope stream closed before task start".to_owned())?
                .map_err(|_| "DashScope stream connection failed".to_owned())?;
            match message {
                Message::Text(text) => {
                    let event = parse_json(text.as_str())?;
                    match event.pointer("/header/event").and_then(Value::as_str) {
                        Some("task-started") => return Ok(()),
                        Some("task-failed") => {
                            return Err(server_error(&event, "DashScope ASR task failed"));
                        }
                        _ => {}
                    }
                }
                Message::Close(_) => {
                    return Err("DashScope stream closed before task start".to_owned());
                }
                Message::Ping(payload) => {
                    socket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|_| "DashScope stream connection failed".to_owned())?;
                }
                _ => {}
            }
        }
    })
    .await
    .map_err(|_| "DashScope stream connection timed out".to_owned())??;
    Ok(OpenedSocket {
        socket,
        task_id: Some(task_id),
    })
}

async fn run_qwen(
    socket: &mut Socket,
    audio: &mut mpsc::Receiver<AudioCommand>,
) -> Result<Output, String> {
    let mut transcript = String::new();
    let mut submit = false;
    let mut finishing = false;
    let mut finish = Box::pin(tokio::time::sleep(FINISH_TIMEOUT));
    loop {
        tokio::select! {
            _ = &mut finish, if finishing => return Err("Qwen realtime finish timed out".to_owned()),
            command = audio.recv(), if !finishing => {
                match command {
                    Some(AudioCommand::Audio(pcm)) => {
                        if pcm.len() > MAX_AUDIO_FRAME_BYTES {
                            return Err("voice audio frame exceeded the size limit".to_owned());
                        }
                        if !pcm.is_empty() {
                            send_qwen_json(socket, json!({
                                "type": "input_audio_buffer.append",
                                "audio": base64::engine::general_purpose::STANDARD.encode(pcm),
                            })).await?;
                        }
                    }
                    Some(AudioCommand::Finish { submit: should_submit }) => {
                        submit = should_submit;
                        finishing = true;
                        finish.as_mut().reset(tokio::time::Instant::now() + FINISH_TIMEOUT);
                        send_qwen_json(socket, json!({"type": "input_audio_buffer.commit"})).await?;
                        send_qwen_json(socket, json!({"type": "session.finish"})).await?;
                    }
                    None => return Err("Qwen realtime stream was cancelled".to_owned()),
                }
            }
            message = socket.next() => {
                let message = message.ok_or_else(|| "Qwen realtime connection closed unexpectedly. Transcript may be incomplete.".to_owned())?
                    .map_err(|_| "Qwen realtime connection failed".to_owned())?;
                match message {
                    Message::Text(text) => {
                        let event = parse_json(text.as_str())?;
                        match event.get("type").and_then(Value::as_str) {
                            Some("conversation.item.input_audio_transcription.completed") => {
                                if let Some(text) = event.get("transcript").and_then(Value::as_str) {
                                    append_transcript(&mut transcript, text)?;
                                }
                            }
                            Some("conversation.item.input_audio_transcription.failed") | Some("error") => {
                                return Err(server_error(&event, "Qwen realtime transcription failed"));
                            }
                            Some("session.finished") => return Ok(Output { text: transcript.trim().to_owned(), submit }),
                            _ => {}
                        }
                    }
                    Message::Ping(payload) => socket.send(Message::Pong(payload)).await.map_err(|_| "Qwen realtime connection failed".to_owned())?,
                    Message::Close(_) => return Err("Qwen realtime connection closed unexpectedly. Transcript may be incomplete.".to_owned()),
                    _ => {}
                }
            }
        }
    }
}

async fn run_dashscope(
    socket: &mut Socket,
    task_id: &str,
    audio: &mut mpsc::Receiver<AudioCommand>,
) -> Result<Output, String> {
    let mut transcript = String::new();
    let mut submit = false;
    let mut finishing = false;
    let mut finish = Box::pin(tokio::time::sleep(FINISH_TIMEOUT));
    loop {
        tokio::select! {
            _ = &mut finish, if finishing => return Err("Voice stream finish timed out".to_owned()),
            command = audio.recv(), if !finishing => {
                match command {
                    Some(AudioCommand::Audio(pcm)) => {
                        if pcm.len() > MAX_AUDIO_FRAME_BYTES {
                            return Err("voice audio frame exceeded the size limit".to_owned());
                        }
                        if !pcm.is_empty() {
                            send_audio(socket, pcm).await?;
                        }
                    }
                    Some(AudioCommand::Finish { submit: should_submit }) => {
                        submit = should_submit;
                        finishing = true;
                        finish.as_mut().reset(tokio::time::Instant::now() + FINISH_TIMEOUT);
                        send_json(socket, json!({
                            "header": {"action": "finish-task", "task_id": task_id, "streaming": "duplex"},
                            "payload": {"input": {}}
                        })).await?;
                    }
                    None => return Err("DashScope stream was cancelled".to_owned()),
                }
            }
            message = socket.next() => {
                let message = message.ok_or_else(|| "Voice stream connection closed unexpectedly. Transcript may be incomplete.".to_owned())?
                    .map_err(|_| "DashScope stream connection failed".to_owned())?;
                match message {
                    Message::Text(text) => {
                        let event = parse_json(text.as_str())?;
                        match event.pointer("/header/event").and_then(Value::as_str) {
                            Some("result-generated") => {
                                if let Some(sentence) = event.pointer("/payload/output/sentence") {
                                    if sentence.get("heartbeat").and_then(Value::as_bool) != Some(true)
                                        && sentence.get("sentence_end").and_then(Value::as_bool) == Some(true)
                                        && let Some(text) = sentence.get("text").and_then(Value::as_str)
                                    {
                                        append_transcript(&mut transcript, text)?;
                                    }
                                }
                            }
                            Some("task-finished") if finishing => return Ok(Output { text: transcript.trim().to_owned(), submit }),
                            Some("task-failed") => return Err(server_error(&event, "DashScope ASR task failed")),
                            Some("task-started") => {}
                            _ => {}
                        }
                    }
                    Message::Ping(payload) => socket.send(Message::Pong(payload)).await.map_err(|_| "DashScope stream connection failed".to_owned())?,
                    Message::Close(_) => return Err("Voice stream connection closed unexpectedly. Transcript may be incomplete.".to_owned()),
                    _ => {}
                }
            }
        }
    }
}

async fn connect(
    url: &Url,
    api_key: &Option<String>,
    addresses: &[SocketAddr],
) -> Result<Socket, String> {
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|_| "could not create voice WebSocket request".to_owned())?;
    if let Some(api_key) = api_key {
        let value = format!("Bearer {api_key}");
        let header = value
            .parse()
            .map_err(|_| "voice API key is not a valid WebSocket header".to_owned())?;
        request.headers_mut().insert(AUTHORIZATION, header);
    }
    let mut last_error = None;
    for address in addresses {
        let connected = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(address)).await;
        let stream = match connected {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => {
                last_error = Some(error.to_string());
                continue;
            }
            Err(_) => return Err("voice WebSocket connection timed out".to_owned()),
        };
        let config = WebSocketConfig::default()
            .read_buffer_size(16 * 1024)
            .write_buffer_size(16 * 1024)
            .max_write_buffer_size(MAX_MESSAGE_BYTES + 16 * 1024)
            .max_message_size(Some(MAX_MESSAGE_BYTES))
            .max_frame_size(Some(MAX_MESSAGE_BYTES));
        return tokio::time::timeout(
            CONNECT_TIMEOUT,
            client_async_tls_with_config(request, stream, Some(config), None),
        )
        .await
        .map_err(|_| "voice WebSocket handshake timed out".to_owned())?
        .map(|(socket, _)| socket)
        .map_err(|error| {
            if let tokio_tungstenite::tungstenite::Error::Http(response) = error {
                format!(
                    "voice WebSocket request failed (HTTP {})",
                    response.status()
                )
            } else {
                "voice WebSocket request failed".to_owned()
            }
        });
    }
    Err(last_error
        .map(|_| "voice WebSocket connection failed".to_owned())
        .unwrap_or_else(|| "voice WebSocket connection failed".to_owned()))
}

fn stream_url(config: &Config, transport: Transport) -> Result<Url, String> {
    let mut url = config.base_url.clone();
    let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
    url.set_scheme(scheme)
        .map_err(|_| "voice WebSocket URL has an unsupported scheme".to_owned())?;
    let mut prefix = url.path().trim_end_matches('/').to_owned();
    for suffix in ["/compatible-mode/v1", "/v1"] {
        if prefix.ends_with(suffix) {
            prefix.truncate(prefix.len() - suffix.len());
            break;
        }
    }
    let path = match transport {
        Transport::Qwen => format!("{prefix}/api-ws/v1/realtime"),
        Transport::DashScopeTask => format!("{prefix}/api-ws/v1/inference"),
    };
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    if transport == Transport::Qwen {
        url.query_pairs_mut().append_pair("model", &config.model);
    }
    Ok(url)
}

async fn send_json(socket: &mut Socket, value: Value) -> Result<(), String> {
    tokio::time::timeout(
        CONNECT_TIMEOUT,
        socket.send(Message::Text(value.to_string().into())),
    )
    .await
    .map_err(|_| "voice WebSocket write timed out".to_owned())?
    .map_err(|_| "voice WebSocket connection failed".to_owned())
}

async fn send_qwen_json(socket: &mut Socket, mut value: Value) -> Result<(), String> {
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "event_id".to_owned(),
            json!(uuid::Uuid::new_v4().to_string()),
        );
    }
    send_json(socket, value).await
}

async fn send_audio(socket: &mut Socket, audio: Vec<u8>) -> Result<(), String> {
    tokio::time::timeout(CONNECT_TIMEOUT, socket.send(Message::Binary(audio.into())))
        .await
        .map_err(|_| "voice WebSocket write timed out".to_owned())?
        .map_err(|_| "voice WebSocket connection failed".to_owned())
}

fn parse_json(text: &str) -> Result<Value, String> {
    if text.len() > MAX_MESSAGE_BYTES {
        return Err("voice WebSocket response exceeded the size limit".to_owned());
    }
    serde_json::from_str(text).map_err(|_| "voice WebSocket returned invalid JSON".to_owned())
}

fn append_transcript(transcript: &mut String, next: &str) -> Result<(), String> {
    let next = next.trim();
    if next.is_empty() {
        return Ok(());
    }
    let extra = next.len() + usize::from(!transcript.is_empty());
    if transcript.len().saturating_add(extra) > MAX_TRANSCRIPT_BYTES {
        return Err("voice transcript exceeded the size limit".to_owned());
    }
    if !transcript.is_empty() {
        transcript.push(' ');
    }
    transcript.push_str(next);
    Ok(())
}

fn server_error(event: &Value, fallback: &str) -> String {
    let code = event
        .pointer("/error/code")
        .or_else(|| event.pointer("/header/error_code"))
        .and_then(Value::as_str);
    let message = event
        .pointer("/error/message")
        .or_else(|| event.pointer("/header/error_message"))
        .and_then(Value::as_str)
        .unwrap_or(fallback);
    let mut safe = message
        .chars()
        .filter(|character| !character.is_control())
        .take(MAX_SERVER_ERROR_BYTES)
        .collect::<String>();
    if let Some(code) = code.filter(|code| !code.is_empty()) {
        format!("{fallback} ({code}): {safe}")
    } else {
        if safe.is_empty() {
            safe = fallback.to_owned();
        }
        safe
    }
}

fn retryable_open_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    let permanent = [
        "400",
        "401",
        "403",
        "404",
        "410",
        "422",
        "429",
        "unauthorized",
        "forbidden",
        "model_not_supported",
    ]
    .iter()
    .any(|marker| lower.contains(marker));
    let rate_limited = ["ratelimit", "rate-limit", "rate_limit", "rate limit"]
        .iter()
        .any(|marker| lower.contains(marker));
    !permanent && !rate_limited
}
