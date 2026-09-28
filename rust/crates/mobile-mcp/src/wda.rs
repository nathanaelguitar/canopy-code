//! Bounded WebDriverAgent client for physical iOS devices.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use base64::Engine;
use serde_json::{Map, Value, json};

const WDA_HOST: &str = "127.0.0.1";
const WDA_PORT: u16 = 8100;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_HEADER_BYTES: usize = 32 * 1024;
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_SOURCE_NODES: usize = 100_000;
const MAX_SCREEN_ELEMENTS: usize = 10_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WdaError {
    Actionable(String),
    Failure(String),
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct WdaClient;

impl WdaClient {
    pub fn new() -> Self {
        Self
    }

    /// Match the iOS robot's tunnel and forwarding probes using bounded local
    /// TCP connects. Only loopback is reachable from this client.
    pub fn port_is_listening(port: u16) -> bool {
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        TcpStream::connect_timeout(&address, CONNECT_TIMEOUT).is_ok()
    }

    /// A failed connection, non-200 response, or malformed status response
    /// means WDA is not ready, matching the TypeScript health probe.
    pub fn is_running(&self) -> bool {
        self.json_request("GET", "/status", None)
            .ok()
            .is_some_and(|(status, value)| {
                status == 200 && value.pointer("/value/ready") == Some(&Value::Bool(true))
            })
    }

    pub fn screen_size(&self) -> Result<(u32, u32, f64), WdaError> {
        self.with_session(|client, session| client.screen_size_in_session(session))
    }

    pub fn screenshot(&self) -> Result<Vec<u8>, WdaError> {
        let (_status, value) = self.json_request("GET", "/screenshot", None)?;
        let encoded = value
            .get("value")
            .and_then(Value::as_str)
            .ok_or_else(|| WdaError::Failure("Invalid WebDriver screenshot response".into()))?;
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|error| {
                WdaError::Failure(format!("Invalid WebDriver screenshot data: {error}"))
            })
    }

    pub fn tap(&self, x: f64, y: f64) -> Result<(), WdaError> {
        self.with_session(|client, session| {
            let actions = json!({
                "actions": [{
                    "type": "pointer",
                    "id": "finger1",
                    "parameters": { "pointerType": "touch" },
                    "actions": [
                        { "type": "pointerMove", "duration": 0, "x": x, "y": y },
                        { "type": "pointerDown", "button": 0 },
                        { "type": "pause", "duration": 100 },
                        { "type": "pointerUp", "button": 0 }
                    ]
                }]
            });
            client.request(
                "POST",
                &format!("/session/{session}/actions"),
                Some(&actions),
            )?;
            Ok(())
        })
    }

    pub fn double_tap(&self, x: f64, y: f64) -> Result<(), WdaError> {
        self.with_session(|client, session| {
            let actions = json!({
                "actions": [{
                    "type": "pointer",
                    "id": "finger1",
                    "parameters": { "pointerType": "touch" },
                    "actions": [
                        { "type": "pointerMove", "duration": 0, "x": x, "y": y },
                        { "type": "pointerDown", "button": 0 },
                        { "type": "pause", "duration": 50 },
                        { "type": "pointerUp", "button": 0 },
                        { "type": "pause", "duration": 100 },
                        { "type": "pointerDown", "button": 0 },
                        { "type": "pause", "duration": 50 },
                        { "type": "pointerUp", "button": 0 }
                    ]
                }]
            });
            client.request(
                "POST",
                &format!("/session/{session}/actions"),
                Some(&actions),
            )?;
            Ok(())
        })
    }

    pub fn long_press(&self, x: f64, y: f64, duration_ms: f64) -> Result<(), WdaError> {
        self.with_session(|client, session| {
            let actions = json!({
                "actions": [{
                    "type": "pointer",
                    "id": "finger1",
                    "parameters": { "pointerType": "touch" },
                    "actions": [
                        { "type": "pointerMove", "duration": 0, "x": x, "y": y },
                        { "type": "pointerDown", "button": 0 },
                        { "type": "pause", "duration": duration_ms },
                        { "type": "pointerUp", "button": 0 }
                    ]
                }]
            });
            client.request(
                "POST",
                &format!("/session/{session}/actions"),
                Some(&actions),
            )?;
            Ok(())
        })
    }

    pub fn swipe(&self, direction: &str) -> Result<(), WdaError> {
        self.with_session(|client, session| {
            let (width, height, _scale) = client.screen_size_in_session(session)?;
            let vertical_distance = (f64::from(height) * 0.6).floor();
            let horizontal_distance = (f64::from(width) * 0.6).floor();
            let center_x = (f64::from(width) / 2.0).floor();
            let center_y = (f64::from(height) / 2.0).floor();
            let (x0, y0, x1, y1) = match direction {
                "up" => (
                    center_x,
                    center_y + (vertical_distance / 2.0).floor(),
                    center_x,
                    center_y - (vertical_distance / 2.0).floor(),
                ),
                "down" => (
                    center_x,
                    center_y - (vertical_distance / 2.0).floor(),
                    center_x,
                    center_y + (vertical_distance / 2.0).floor(),
                ),
                "left" => (
                    center_x + (horizontal_distance / 2.0).floor(),
                    center_y,
                    center_x - (horizontal_distance / 2.0).floor(),
                    center_y,
                ),
                "right" => (
                    center_x - (horizontal_distance / 2.0).floor(),
                    center_y,
                    center_x + (horizontal_distance / 2.0).floor(),
                    center_y,
                ),
                _ => {
                    return Err(WdaError::Actionable(format!(
                        "Swipe direction \"{direction}\" is not supported"
                    )));
                }
            };
            client.perform_swipe(session, x0, y0, x1, y1)
        })
    }

    pub fn swipe_from_coordinate(
        &self,
        x: f64,
        y: f64,
        direction: &str,
        distance: f64,
    ) -> Result<(), WdaError> {
        let (x1, y1) = match direction {
            "up" => (x, y - distance),
            "down" => (x, y + distance),
            "left" => (x - distance, y),
            "right" => (x + distance, y),
            _ => {
                return Err(WdaError::Actionable(format!(
                    "Swipe direction \"{direction}\" is not supported"
                )));
            }
        };
        self.with_session(|client, session| client.perform_swipe(session, x, y, x1, y1))
    }

    pub fn open_url(&self, url: &str) -> Result<(), WdaError> {
        self.with_session(|client, session| {
            let body = json!({ "url": url });
            client.request("POST", &format!("/session/{session}/url"), Some(&body))?;
            Ok(())
        })
    }

    pub fn get_elements_on_screen(&self) -> Result<Vec<Value>, WdaError> {
        let (_status, source) = self.json_request("GET", "/source/?format=json", None)?;
        let root = source
            .get("value")
            .ok_or_else(|| WdaError::Failure("Invalid WebDriver source response".into()))?;
        let mut elements = Vec::new();
        let mut visited = 0;
        collect_screen_elements(root, &mut elements, &mut visited)?;
        Ok(elements)
    }

    pub fn set_orientation(&self, orientation: &str) -> Result<(), WdaError> {
        self.with_session(|client, session| {
            let body = json!({ "orientation": orientation.to_ascii_uppercase() });
            client.request(
                "POST",
                &format!("/session/{session}/orientation"),
                Some(&body),
            )?;
            Ok(())
        })
    }

    pub fn get_orientation(&self) -> Result<String, WdaError> {
        self.with_session(|client, session| {
            let (_status, value) =
                client.json_request("GET", &format!("/session/{session}/orientation"), None)?;
            value
                .get("value")
                .and_then(Value::as_str)
                .map(str::to_ascii_lowercase)
                .ok_or_else(|| WdaError::Failure("Invalid WebDriver orientation response".into()))
        })
    }

    pub fn send_keys(&self, keys: &str) -> Result<(), WdaError> {
        self.with_session(|client, session| {
            let body = json!({ "value": [keys] });
            client.request("POST", &format!("/session/{session}/wda/keys"), Some(&body))?;
            Ok(())
        })
    }

    pub fn press_button(&self, button: &str) -> Result<(), WdaError> {
        if button == "ENTER" {
            return self.send_keys("\n");
        }
        if !matches!(button, "HOME" | "VOLUME_UP" | "VOLUME_DOWN") {
            return Err(WdaError::Actionable(format!(
                "Button \"{button}\" is not supported"
            )));
        }
        self.with_session(|client, session| {
            let body = json!({ "name": button });
            client.json_request(
                "POST",
                &format!("/session/{session}/wda/pressButton"),
                Some(&body),
            )?;
            Ok(())
        })
    }

    fn with_session<T>(
        &self,
        action: impl FnOnce(&Self, &str) -> Result<T, WdaError>,
    ) -> Result<T, WdaError> {
        let session = self.create_session()?;
        let result = action(self, &session);
        // Always release the short-lived session, including after action or
        // response parsing failures. Cleanup must not mask the tool result.
        let _ = self.json_request("DELETE", &format!("/session/{session}"), None);
        result
    }

    fn create_session(&self) -> Result<String, WdaError> {
        let request = json!({
            "capabilities": { "alwaysMatch": { "platformName": "iOS" } }
        });
        let response = self.request("POST", "/session", Some(&request))?;
        if !(200..300).contains(&response.status) {
            return Err(WdaError::Actionable(format!(
                "Failed to create WebDriver session: {} {}",
                response.status,
                String::from_utf8_lossy(&response.body)
            )));
        }
        let value: Value = serde_json::from_slice(&response.body).map_err(|error| {
            WdaError::Failure(format!("Invalid WebDriver session response: {error}"))
        })?;
        let session = value
            .pointer("/value/sessionId")
            .and_then(Value::as_str)
            .filter(|session| {
                !session.is_empty()
                    && session
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            })
            .ok_or_else(|| WdaError::Actionable(format!("Invalid session response: {value}")))?;
        Ok(session.to_owned())
    }

    fn json_request(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<(u16, Value), WdaError> {
        let response = self.request(method, path, body)?;
        let value = serde_json::from_slice(&response.body).map_err(|error| {
            WdaError::Failure(format!("Invalid WebDriver response from {path}: {error}"))
        })?;
        Ok((response.status, value))
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<HttpResponse, WdaError> {
        if !path.starts_with('/')
            || path
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte == b' ')
        {
            return Err(WdaError::Failure("Invalid WebDriver request path".into()));
        }
        let addresses = (WDA_HOST, WDA_PORT)
            .to_socket_addrs()
            .map_err(|error| WdaError::Failure(format!("WebDriverAgent address error: {error}")))?;
        let mut last_error = None;
        let mut stream = None;
        for address in addresses {
            match TcpStream::connect_timeout(&address, CONNECT_TIMEOUT) {
                Ok(connected) => {
                    stream = Some(connected);
                    break;
                }
                Err(error) => last_error = Some(error),
            }
        }
        let mut stream = stream.ok_or_else(|| {
            WdaError::Failure(format!(
                "Failed to connect to WebDriverAgent: {}",
                last_error.map_or_else(|| "no loopback address".into(), |error| error.to_string())
            ))
        })?;
        stream
            .set_nodelay(true)
            .map_err(|error| WdaError::Failure(format!("WebDriverAgent socket error: {error}")))?;

        let body = body.map(serde_json::to_vec).transpose().map_err(|error| {
            WdaError::Failure(format!("Failed encoding WebDriver request: {error}"))
        })?;
        if body
            .as_ref()
            .is_some_and(|body| body.len() > MAX_REQUEST_BYTES)
        {
            return Err(WdaError::Failure(
                "WebDriverAgent request exceeded the 4 MiB limit".into(),
            ));
        }
        let body = body.as_deref().unwrap_or_default();
        let headers = if body.is_empty() {
            format!(
                "{method} {path} HTTP/1.1\r\nHost: localhost:{WDA_PORT}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
            )
        } else {
            format!(
                "{method} {path} HTTP/1.1\r\nHost: localhost:{WDA_PORT}\r\nAccept: application/json\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
        };
        let deadline = Instant::now() + IO_TIMEOUT;
        write_before_deadline(&mut stream, headers.as_bytes(), deadline)?;
        write_before_deadline(&mut stream, body, deadline)?;

        let mut response = Vec::with_capacity(8192);
        let mut buffer = [0_u8; 8192];
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(WdaError::Failure(
                    "WebDriverAgent request exceeded the 10 second I/O deadline".into(),
                ));
            }
            stream.set_read_timeout(Some(remaining)).map_err(|error| {
                WdaError::Failure(format!("WebDriverAgent socket error: {error}"))
            })?;
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    response.extend_from_slice(&buffer[..count]);
                    if response.len() > MAX_HEADER_BYTES + MAX_BODY_BYTES + 4 {
                        return Err(WdaError::Failure(
                            "WebDriverAgent response exceeded the 16 MiB limit".into(),
                        ));
                    }
                    if let Some(header_end) = find_header_end(&response) {
                        if header_end > MAX_HEADER_BYTES {
                            return Err(WdaError::Failure(
                                "WebDriverAgent response headers exceeded the 32 KiB limit".into(),
                            ));
                        }
                        if let Some(length) = response_content_length(&response[..header_end])? {
                            if length > MAX_BODY_BYTES {
                                return Err(WdaError::Failure(
                                    "WebDriverAgent response exceeded the 16 MiB limit".into(),
                                ));
                            }
                            if response.len() >= header_end + 4 + length {
                                break;
                            }
                        }
                    } else if response.len() > MAX_HEADER_BYTES {
                        return Err(WdaError::Failure(
                            "WebDriverAgent response headers exceeded the 32 KiB limit".into(),
                        ));
                    }
                }
                Err(error) => {
                    return Err(WdaError::Failure(format!(
                        "Failed reading WebDriverAgent response: {error}"
                    )));
                }
            }
        }
        parse_http_response(response)
    }

    fn screen_size_in_session(&self, session: &str) -> Result<(u32, u32, f64), WdaError> {
        let (_status, value) =
            self.json_request("GET", &format!("/session/{session}/wda/screen"), None)?;
        let width = value
            .pointer("/value/screenSize/width")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| WdaError::Failure("Invalid WebDriver screen size response".into()))?;
        let height = value
            .pointer("/value/screenSize/height")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| WdaError::Failure("Invalid WebDriver screen size response".into()))?;
        let scale = value
            .pointer("/value/scale")
            .and_then(Value::as_f64)
            .filter(|scale| *scale > 0.0)
            .unwrap_or(1.0);
        Ok((width, height, scale))
    }

    fn perform_swipe(
        &self,
        session: &str,
        x0: f64,
        y0: f64,
        x1: f64,
        y1: f64,
    ) -> Result<(), WdaError> {
        let actions = json!({
            "actions": [{
                "type": "pointer",
                "id": "finger1",
                "parameters": { "pointerType": "touch" },
                "actions": [
                    { "type": "pointerMove", "duration": 0, "x": x0, "y": y0 },
                    { "type": "pointerDown", "button": 0 },
                    { "type": "pointerMove", "duration": 1000, "x": x1, "y": y1 },
                    { "type": "pointerUp", "button": 0 }
                ]
            }]
        });
        let response = self.request(
            "POST",
            &format!("/session/{session}/actions"),
            Some(&actions),
        )?;
        if !(200..300).contains(&response.status) {
            return Err(WdaError::Actionable(format!(
                "WebDriver actions request failed: {} {}",
                response.status,
                String::from_utf8_lossy(&response.body)
            )));
        }
        let _ = self.request("DELETE", &format!("/session/{session}/actions"), None);
        Ok(())
    }
}

fn write_before_deadline(
    stream: &mut TcpStream,
    bytes: &[u8],
    deadline: Instant,
) -> Result<(), WdaError> {
    let mut offset = 0;
    while offset < bytes.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(WdaError::Failure(
                "WebDriverAgent request exceeded the 10 second I/O deadline".into(),
            ));
        }
        stream
            .set_write_timeout(Some(remaining))
            .map_err(|error| WdaError::Failure(format!("WebDriverAgent socket error: {error}")))?;
        match stream.write(&bytes[offset..]) {
            Ok(0) => {
                return Err(WdaError::Failure(
                    "WebDriverAgent request failed: socket closed while writing".into(),
                ));
            }
            Ok(count) => offset += count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => {
                return Err(WdaError::Failure(format!(
                    "WebDriverAgent request failed: {error}"
                )));
            }
        }
    }
    Ok(())
}

fn collect_screen_elements(
    source: &Value,
    output: &mut Vec<Value>,
    visited: &mut usize,
) -> Result<(), WdaError> {
    *visited += 1;
    if *visited > MAX_SOURCE_NODES {
        return Err(WdaError::Failure(format!(
            "WebDriver source exceeded the {MAX_SOURCE_NODES} node limit"
        )));
    }

    let accepted_type = source
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|value| {
            matches!(
                value,
                "TextField" | "Button" | "Switch" | "Icon" | "SearchField" | "StaticText" | "Image"
            )
        });
    let rect = source.get("rect").and_then(Value::as_object);
    let visible = source.get("isVisible").and_then(Value::as_str) == Some("1")
        && rect
            .and_then(|rect| rect.get("x"))
            .and_then(Value::as_f64)
            .is_some_and(|x| x >= 0.0)
        && rect
            .and_then(|rect| rect.get("y"))
            .and_then(Value::as_f64)
            .is_some_and(|y| y >= 0.0);
    let has_non_null_accessibility_field = ["label", "name", "rawIdentifier"]
        .iter()
        .any(|field| source.get(*field).map_or(true, |value| !value.is_null()));

    if accepted_type && visible && has_non_null_accessibility_field {
        if output.len() >= MAX_SCREEN_ELEMENTS {
            return Err(WdaError::Failure(format!(
                "WebDriver source exceeded the {MAX_SCREEN_ELEMENTS} screen-element limit"
            )));
        }
        let mut element = Map::new();
        if let Some(value) = source.get("type") {
            element.insert("type".into(), value.clone());
        }
        for (source_field, output_field) in [
            ("label", "label"),
            ("name", "name"),
            ("value", "value"),
            ("rawIdentifier", "identifier"),
        ] {
            if let Some(value) = source.get(source_field) {
                element.insert(output_field.into(), value.clone());
            }
        }
        let mut element_rect = Map::new();
        for field in ["x", "y", "width", "height"] {
            if let Some(value) = rect.and_then(|rect| rect.get(field)) {
                element_rect.insert(field.into(), value.clone());
            }
        }
        element.insert("rect".into(), Value::Object(element_rect));
        output.push(Value::Object(element));
    }

    if let Some(children) = source.get("children").and_then(Value::as_array) {
        for child in children {
            collect_screen_elements(child, output, visited)?;
        }
    }
    Ok(())
}

fn find_header_end(response: &[u8]) -> Option<usize> {
    response.windows(4).position(|window| window == b"\r\n\r\n")
}

fn response_content_length(headers: &[u8]) -> Result<Option<usize>, WdaError> {
    let headers = std::str::from_utf8(headers).map_err(|error| {
        WdaError::Failure(format!("Invalid WebDriver response headers: {error}"))
    })?;
    for line in headers.split("\r\n").skip(1) {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                return value.trim().parse::<usize>().map(Some).map_err(|error| {
                    WdaError::Failure(format!("Invalid WebDriver Content-Length: {error}"))
                });
            }
        }
    }
    Ok(None)
}

fn parse_http_response(response: Vec<u8>) -> Result<HttpResponse, WdaError> {
    let header_end = find_header_end(&response)
        .ok_or_else(|| WdaError::Failure("Invalid WebDriver HTTP response headers".into()))?;
    if header_end > MAX_HEADER_BYTES {
        return Err(WdaError::Failure(
            "WebDriverAgent response headers exceeded the 32 KiB limit".into(),
        ));
    }
    let headers = std::str::from_utf8(&response[..header_end]).map_err(|error| {
        WdaError::Failure(format!("Invalid WebDriver response headers: {error}"))
    })?;
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| WdaError::Failure("Invalid WebDriver HTTP status line".into()))?;
    let start = header_end + 4;
    let bytes = &response[start..];
    let transfer_chunked = headers.lines().skip(1).any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("transfer-encoding")
                && value.to_ascii_lowercase().contains("chunked")
        })
    });
    let body = if transfer_chunked {
        decode_chunked(bytes)?
    } else if let Some(length) = response_content_length(&response[..header_end])? {
        if bytes.len() < length {
            return Err(WdaError::Failure(
                "Truncated WebDriver HTTP response body".into(),
            ));
        }
        bytes[..length].to_vec()
    } else {
        bytes.to_vec()
    };
    if body.len() > MAX_BODY_BYTES {
        return Err(WdaError::Failure(
            "WebDriverAgent response exceeded the 16 MiB limit".into(),
        ));
    }
    Ok(HttpResponse { status, body })
}

fn decode_chunked(mut bytes: &[u8]) -> Result<Vec<u8>, WdaError> {
    let mut output = Vec::new();
    loop {
        let line_end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| WdaError::Failure("Invalid chunked WebDriver response".into()))?;
        let line = std::str::from_utf8(&bytes[..line_end])
            .map_err(|error| WdaError::Failure(format!("Invalid HTTP chunk length: {error}")))?;
        let length = usize::from_str_radix(line.split(';').next().unwrap_or_default().trim(), 16)
            .map_err(|error| {
            WdaError::Failure(format!("Invalid HTTP chunk length: {error}"))
        })?;
        bytes = &bytes[line_end + 2..];
        if length == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(length) > MAX_BODY_BYTES
            || bytes.len() < length.saturating_add(2)
            || bytes[length..length + 2] != *b"\r\n"
        {
            return Err(WdaError::Failure(
                "Invalid or oversized chunked WebDriver response".into(),
            ));
        }
        output.extend_from_slice(&bytes[..length]);
        bytes = &bytes[length + 2..];
    }
}
