//! Small bounded HTTP/SSE transport for the legacy `/mcp` endpoint.

use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::{Value, json};
use uuid::Uuid;

use crate::logger;
use crate::protocol::{ProtocolError, handle_json_line};
use crate::tools::MobileServer;

const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
const STREAM_HEARTBEAT: Duration = Duration::from_secs(10);
const IO_TIMEOUT: Duration = Duration::from_secs(15);

pub fn parse_listen(input: &str) -> Result<(String, u16), String> {
    let listen = input.trim();
    let (host, raw_port) = match listen.rfind(':') {
        Some(index) if index > 0 => (&listen[..index], &listen[index + 1..]),
        _ => ("localhost", listen),
    };
    let raw_port = raw_port.trim_start();
    let (negative, digits) = match raw_port.as_bytes().first() {
        Some(b'-') => (true, &raw_port[1..]),
        Some(b'+') => (false, &raw_port[1..]),
        _ => (false, raw_port),
    };
    let digit_count = digits.bytes().take_while(u8::is_ascii_digit).count();
    let port = (!negative && digit_count > 0)
        .then(|| digits[..digit_count].parse::<u16>().ok())
        .flatten()
        .filter(|port| *port != 0);
    let mut host = host.trim().to_owned();
    if host.starts_with('[') && host.ends_with(']') {
        host = host[1..host.len() - 1].to_owned();
    }
    match (host.is_empty(), port) {
        (false, Some(port)) => Ok((host, port)),
        _ => Err(format!(
            "Invalid --listen value \"{listen}\". Expected [host:]port with port 1-65535."
        )),
    }
}

pub fn run(host: &str, port: u16) -> io::Result<()> {
    let addresses = (host, port).to_socket_addrs()?;
    let mut listener = None;
    let mut last_bind_error = None;
    for address in addresses {
        match TcpListener::bind(address) {
            Ok(bound) => {
                listener = Some(bound);
                break;
            }
            Err(error) => last_bind_error = Some(error),
        }
    }
    let listener = listener.ok_or_else(|| {
        last_bind_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "host resolved to no addresses",
            )
        })
    })?;
    listener.set_nonblocking(true)?;
    let stopping = Arc::new(AtomicBool::new(false));
    let signal_stopping = Arc::clone(&stopping);
    ctrlc::set_handler(move || signal_stopping.store(true, Ordering::Release)).map_err(
        |error| io::Error::other(format!("failed to install shutdown handler: {error}")),
    )?;

    let token = std::env::var("MOBILEMCP_AUTH")
        .ok()
        .filter(|token| !token.is_empty());
    if token.is_none() {
        logger::error(
            "WARNING: MOBILEMCP_AUTH is not set. The SSE server will accept unauthenticated connections. Set MOBILEMCP_AUTH to require Bearer token authentication.",
        );
    }
    logger::error(&format!(
        "{} {} sse server listening on http://{}:{}/mcp",
        crate::SERVER_NAME,
        crate::SERVER_VERSION,
        host,
        port
    ));

    let sessions = Arc::new(SessionRegistry::default());
    let server = Arc::new(Mutex::new(MobileServer::default()));
    let mut accept_error = None;
    while !stopping.load(Ordering::Acquire) {
        let (stream, _) = match listener.accept() {
            Ok(connection) => connection,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(25));
                continue;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                accept_error = Some(error);
                break;
            }
        };
        let _ = stream.set_read_timeout(Some(IO_TIMEOUT));
        let _ = stream.set_write_timeout(Some(IO_TIMEOUT));
        let sessions = Arc::clone(&sessions);
        let server = Arc::clone(&server);
        if let Err(error) = serve_connection(stream, token.as_deref(), &sessions, &server) {
            if error.kind() != io::ErrorKind::UnexpectedEof
                && error.kind() != io::ErrorKind::ConnectionReset
                && error.kind() != io::ErrorKind::BrokenPipe
            {
                logger::error(&format!("mobile-mcp SSE request failed: {error}"));
            }
        }
    }
    sessions.close_active();
    sessions.join_stream();
    logger::error("mobile-mcp SSE server stopped");
    accept_error.map_or(Ok(()), Err)
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
enum RequestReadError {
    Malformed,
    HeadersTooLarge,
    BodyTooLarge,
}

fn read_request(reader: &mut impl BufRead) -> io::Result<Result<HttpRequest, RequestReadError>> {
    let request_line = match read_http_line(reader, MAX_REQUEST_LINE_BYTES) {
        Ok(line) => line,
        Err(RequestReadError::HeadersTooLarge) => {
            return Ok(Err(RequestReadError::HeadersTooLarge));
        }
        Err(error) => return Ok(Err(error)),
    };
    let Ok(request_line) = std::str::from_utf8(&request_line) else {
        return Ok(Err(RequestReadError::Malformed));
    };
    let mut parts = request_line.split_ascii_whitespace();
    let (Some(method), Some(target), Some(version)) = (parts.next(), parts.next(), parts.next())
    else {
        return Ok(Err(RequestReadError::Malformed));
    };
    if parts.next().is_some()
        || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
        || !method.bytes().all(is_token_byte)
        || !target.starts_with('/')
    {
        return Ok(Err(RequestReadError::Malformed));
    }

    let mut headers = Vec::new();
    let mut total_header_bytes = request_line.len() + 2;
    loop {
        let line = match read_http_line(reader, MAX_HEADER_BYTES) {
            Ok(line) => line,
            Err(RequestReadError::HeadersTooLarge) => {
                return Ok(Err(RequestReadError::HeadersTooLarge));
            }
            Err(error) => return Ok(Err(error)),
        };
        total_header_bytes = total_header_bytes.saturating_add(line.len() + 2);
        if total_header_bytes > MAX_HEADER_BYTES {
            return Ok(Err(RequestReadError::HeadersTooLarge));
        }
        if line.is_empty() {
            break;
        }
        let Some(colon) = line.iter().position(|byte| *byte == b':') else {
            return Ok(Err(RequestReadError::Malformed));
        };
        let (name, rest) = line.split_at(colon);
        let value = &rest[1..];
        if name.is_empty()
            || !name.iter().copied().all(is_token_byte)
            || value
                .iter()
                .any(|byte| (*byte < 0x20 && *byte != b'\t') || *byte == 0x7f)
        {
            return Ok(Err(RequestReadError::Malformed));
        }
        let (Ok(name), Ok(value)) = (std::str::from_utf8(name), std::str::from_utf8(value)) else {
            return Ok(Err(RequestReadError::Malformed));
        };
        headers.push((name.to_ascii_lowercase(), value.trim().to_owned()));
    }

    if header_values(&headers, "transfer-encoding")
        .next()
        .is_some()
    {
        return Ok(Err(RequestReadError::Malformed));
    }
    let length = {
        let mut lengths = header_values(&headers, "content-length");
        match lengths.next() {
            None => 0,
            Some(value)
                if lengths.next().is_none()
                    && !value.is_empty()
                    && value.bytes().all(|byte| byte.is_ascii_digit()) =>
            {
                match value.parse::<usize>() {
                    Ok(length) if length <= MAX_BODY_BYTES => length,
                    Ok(_) => return Ok(Err(RequestReadError::BodyTooLarge)),
                    Err(_) => return Ok(Err(RequestReadError::Malformed)),
                }
            }
            Some(_) => return Ok(Err(RequestReadError::Malformed)),
        }
    };
    let mut body = vec![0; length];
    if reader.read_exact(&mut body).is_err() {
        return Ok(Err(RequestReadError::Malformed));
    }
    Ok(Ok(HttpRequest {
        method: method.to_owned(),
        target: target.to_owned(),
        headers,
        body,
    }))
}

fn read_http_line(reader: &mut impl BufRead, limit: usize) -> Result<Vec<u8>, RequestReadError> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().map_err(|_| RequestReadError::Malformed)?;
        if available.is_empty() {
            return Err(RequestReadError::Malformed);
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let count = newline.map_or(available.len(), |index| index + 1);
        if line.len().saturating_add(count) > limit {
            return Err(RequestReadError::HeadersTooLarge);
        }
        line.extend_from_slice(&available[..count]);
        reader.consume(count);
        if newline.is_some() {
            if !line.ends_with(b"\r\n") {
                return Err(RequestReadError::Malformed);
            }
            line.truncate(line.len() - 2);
            return Ok(line);
        }
    }
}

fn is_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn header_values<'a>(
    headers: &'a [(String, String)],
    name: &'a str,
) -> impl Iterator<Item = &'a str> + 'a {
    headers
        .iter()
        .filter(move |(header, _)| header == name)
        .map(|(_, value)| value.as_str())
}

fn authorized(request: &HttpRequest, token: Option<&str>) -> bool {
    let Some(token) = token else {
        return true;
    };
    let mut values = header_values(&request.headers, "authorization");
    matches!(
        (values.next(), values.next()),
        (Some(value), None) if value == format!("Bearer {token}")
    )
}

fn has_origin(request: &HttpRequest) -> bool {
    header_values(&request.headers, "origin").any(|origin| !origin.is_empty())
}

#[derive(Default)]
struct SessionRegistry {
    active: Mutex<Option<ActiveSession>>,
    stream_thread: Mutex<Option<JoinHandle<()>>>,
}

struct ActiveSession {
    id: String,
    sender: mpsc::SyncSender<Value>,
}

impl SessionRegistry {
    fn lock(&self) -> MutexGuard<'_, Option<ActiveSession>> {
        self.active
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn open(&self) -> Option<(String, mpsc::Receiver<Value>)> {
        let mut active = self.lock();
        if active.is_some() {
            return None;
        }
        let id = Uuid::new_v4().to_string();
        let (sender, receiver) = mpsc::sync_channel(1);
        *active = Some(ActiveSession {
            id: id.clone(),
            sender,
        });
        Some((id, receiver))
    }

    fn sender(&self, id: &str) -> Option<mpsc::SyncSender<Value>> {
        self.lock()
            .as_ref()
            .filter(|session| session.id == id)
            .map(|session| session.sender.clone())
    }

    fn close(&self, id: &str) {
        let mut active = self.lock();
        if active.as_ref().is_some_and(|session| session.id == id) {
            *active = None;
        }
    }

    fn close_active(&self) {
        *self.lock() = None;
    }

    fn store_stream_thread(&self, handle: JoinHandle<()>) {
        *self
            .stream_thread
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(handle);
    }

    fn join_stream(&self) {
        let handle = self
            .stream_thread
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if handle.is_some_and(|handle| handle.join().is_err()) {
            logger::error("mobile-mcp SSE stream worker panicked during shutdown");
        }
    }
}

fn serve_connection(
    stream: TcpStream,
    token: Option<&str>,
    sessions: &Arc<SessionRegistry>,
    server: &Arc<Mutex<MobileServer>>,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream);
    let request = match read_request(&mut reader)? {
        Ok(request) => request,
        Err(error) => {
            let status = match error {
                RequestReadError::Malformed => 400,
                RequestReadError::HeadersTooLarge => 431,
                RequestReadError::BodyTooLarge => 413,
            };
            return write_http_response(reader.into_inner(), status, "text/plain", b"Bad request");
        }
    };
    let mut stream = reader.into_inner();

    if !authorized(&request, token) {
        return write_json_response(&mut stream, 401, json!({"error":"Unauthorized"}));
    }
    if has_origin(&request) {
        return write_json_response(
            &mut stream,
            403,
            json!({"error":"Cross-origin requests are not allowed"}),
        );
    }
    if request.method == "OPTIONS" {
        return write_http_response(stream, 403, "text/plain", b"");
    }
    let (path, query) = request
        .target
        .split_once('?')
        .map_or((request.target.as_str(), ""), |(path, query)| (path, query));
    if path != "/mcp" {
        return write_json_response(&mut stream, 404, json!({"error":"Not found"}));
    }

    match request.method.as_str() {
        "GET" => {
            let Some((id, receiver)) = sessions.open() else {
                return write_json_response(
                    &mut stream,
                    409,
                    json!({"error":"Another client is already connected. Disconnect the existing client first."}),
                );
            };
            let stream_sessions = Arc::clone(sessions);
            let cleanup_sessions = Arc::clone(sessions);
            let cleanup_id = id.clone();
            let stream_worker = thread::Builder::new()
                .name("mobile-mcp-sse".into())
                .spawn(move || write_sse_stream(stream, &id, receiver, &stream_sessions));
            match stream_worker {
                Ok(handle) => sessions.store_stream_thread(handle),
                Err(error) => {
                    cleanup_sessions.close(&cleanup_id);
                    return Err(error);
                }
            }
            Ok(())
        }
        "POST" => {
            if !content_type_is_json(&request.headers) {
                return write_http_response(
                    stream,
                    400,
                    "text/plain",
                    b"Unsupported content-type; expected application/json",
                );
            }
            let Some(session_id) = query_session_id(query) else {
                return write_http_response(stream, 400, "text/plain", b"Missing sessionId");
            };
            let Some(sender) = sessions.sender(&session_id) else {
                return write_http_response(stream, 400, "text/plain", b"Invalid sessionId");
            };
            let message = match handle_json_line(
                &mut server.lock().unwrap_or_else(|poison| poison.into_inner()),
                &request.body,
            ) {
                Ok(response) => response,
                Err(ProtocolError::InvalidJson(_)) => {
                    return write_http_response(stream, 400, "text/plain", b"Invalid JSON");
                }
                Err(error) => Some(
                    json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":error.to_string()}}),
                ),
            };
            if let Some(message) = message {
                if sender.send(message).is_err() {
                    return write_http_response(stream, 410, "text/plain", b"SSE session closed");
                }
            }
            write_http_response(stream, 202, "text/plain", b"Accepted")
        }
        _ => write_json_response(&mut stream, 405, json!({"error":"Method not allowed"})),
    }
}

fn content_type_is_json(headers: &[(String, String)]) -> bool {
    let mut values = header_values(headers, "content-type");
    let Some(value) = values.next() else {
        return false;
    };
    values.next().is_none()
        && value
            .split(';')
            .next()
            .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

fn query_session_id(query: &str) -> Option<String> {
    let mut found = None;
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        if name == "sessionId" {
            if found.is_some() || value.is_empty() {
                return None;
            }
            found = Some(value.to_owned());
        }
    }
    found
}

fn write_sse_stream(
    mut stream: TcpStream,
    id: &str,
    receiver: mpsc::Receiver<Value>,
    sessions: &SessionRegistry,
) {
    let result = (|| -> io::Result<()> {
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        stream.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache, no-transform\r\nConnection: keep-alive\r\n\r\n",
        )?;
        write!(stream, "event: endpoint\ndata: /mcp?sessionId={id}\n\n")?;
        stream.flush()?;
        loop {
            match receiver.recv_timeout(STREAM_HEARTBEAT) {
                Ok(message) => {
                    stream.write_all(b"event: message\ndata: ")?;
                    serde_json::to_writer(&mut stream, &message)?;
                    stream.write_all(b"\n\n")?;
                    stream.flush()?;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    stream.write_all(b": keepalive\n\n")?;
                    stream.flush()?;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        Ok(())
    })();
    if let Err(error) = result {
        if error.kind() != io::ErrorKind::BrokenPipe
            && error.kind() != io::ErrorKind::ConnectionReset
            && error.kind() != io::ErrorKind::TimedOut
        {
            logger::error(&format!("mobile-mcp SSE stream ended: {error}"));
        }
    }
    sessions.close(id);
}

fn write_json_response(stream: &mut TcpStream, status: u16, value: Value) -> io::Result<()> {
    let body = serde_json::to_vec(&value)?;
    write_http_response(stream, status, "application/json", &body)
}

fn write_http_response(
    mut stream: impl Write,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    let reason = match status {
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        410 => "Gone",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        _ => "Internal Server Error",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::{
        HttpRequest, MAX_BODY_BYTES, MAX_HEADER_BYTES, MobileServer, RequestReadError,
        SessionRegistry, authorized, has_origin, parse_listen, read_request, serve_connection,
    };
    use serde_json::{Value, json};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn request(headers: &[(&str, &str)]) -> HttpRequest {
        HttpRequest {
            method: "GET".into(),
            target: "/mcp".into(),
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_ascii_lowercase(), (*value).into()))
                .collect(),
            body: vec![],
        }
    }

    #[test]
    fn parses_listen_host_port_and_ipv6() {
        assert_eq!(parse_listen("8123"), Ok(("localhost".into(), 8123)));
        assert_eq!(parse_listen(" 8123tail"), Ok(("localhost".into(), 8123)));
        assert_eq!(parse_listen("host:+81xyz"), Ok(("host".into(), 81)));
        assert_eq!(
            parse_listen("127.0.0.1:65535"),
            Ok(("127.0.0.1".into(), 65535))
        );
        assert_eq!(parse_listen("[::1]:8111"), Ok(("::1".into(), 8111)));
        assert!(parse_listen("0").is_err());
        assert!(parse_listen("host:65536").is_err());
        assert!(parse_listen("host:").is_err());
    }

    #[test]
    fn requires_exact_bearer_authorization_when_configured() {
        assert!(authorized(&request(&[]), None));
        assert!(!authorized(&request(&[]), Some("secret")));
        assert!(authorized(
            &request(&[("Authorization", "Bearer secret")]),
            Some("secret")
        ));
        assert!(!authorized(
            &request(&[("authorization", "bearer secret")]),
            Some("secret")
        ));
        assert!(!authorized(
            &request(&[
                ("authorization", "Bearer secret"),
                ("authorization", "Bearer secret")
            ]),
            Some("secret")
        ));
        assert!(has_origin(&request(&[("origin", "https://example.test")])));
        assert!(!has_origin(&request(&[("origin", "")])));
    }

    #[test]
    fn bounds_http_headers_and_body_before_allocating_payload() {
        let large_header = format!(
            "GET /mcp HTTP/1.1\r\nX-Large: {}\r\n\r\n",
            "x".repeat(MAX_HEADER_BYTES)
        );
        assert!(matches!(
            read_request(&mut BufReader::new(large_header.as_bytes())).unwrap(),
            Err(RequestReadError::HeadersTooLarge)
        ));

        let too_large_body = format!(
            "POST /mcp?sessionId=x HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY_BYTES + 1
        );
        assert!(matches!(
            read_request(&mut BufReader::new(too_large_body.as_bytes())).unwrap(),
            Err(RequestReadError::BodyTooLarge)
        ));
    }

    #[test]
    fn only_one_sse_session_can_be_active_and_close_is_id_scoped() {
        let registry = SessionRegistry::default();
        let (first, _receiver) = registry.open().expect("first session opens");
        assert!(registry.open().is_none());
        registry.close("some-other-session");
        assert!(registry.open().is_none());
        registry.close(&first);
        assert!(registry.open().is_some());
    }

    #[test]
    fn rejects_cross_origin_and_options_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener binds");
        let address = listener.local_addr().unwrap();
        let sessions = Arc::new(SessionRegistry::default());
        let server = Arc::new(Mutex::new(MobileServer::default()));
        for (request, expected) in [
            (
                "GET /mcp HTTP/1.1\r\nHost: localhost\r\nOrigin: https://example.test\r\n\r\n",
                "HTTP/1.1 403 Forbidden\r\n",
            ),
            (
                "OPTIONS /mcp HTTP/1.1\r\nHost: localhost\r\n\r\n",
                "HTTP/1.1 403 Forbidden\r\n",
            ),
        ] {
            let mut client = TcpStream::connect(address).unwrap();
            client.write_all(request.as_bytes()).unwrap();
            let (stream, _) = listener.accept().unwrap();
            serve_connection(stream, None, &sessions, &server).unwrap();
            let mut response = String::new();
            BufReader::new(client)
                .read_to_string(&mut response)
                .unwrap();
            assert!(response.starts_with(expected));
        }
    }

    #[test]
    fn routes_posted_json_rpc_ping_over_sse() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener binds");
        let address = listener.local_addr().unwrap();
        let sessions = Arc::new(SessionRegistry::default());
        let server = Arc::new(Mutex::new(MobileServer::default()));

        let mut event_stream = TcpStream::connect(address).expect("connect SSE client");
        event_stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        event_stream
            .write_all(b"GET /mcp HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let (server_stream, _) = listener.accept().unwrap();
        serve_connection(server_stream, None, &sessions, &server).unwrap();

        let mut events = BufReader::new(event_stream.try_clone().unwrap());
        let mut line = String::new();
        loop {
            line.clear();
            events.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
        }
        line.clear();
        events.read_line(&mut line).unwrap();
        assert_eq!(line, "event: endpoint\n");
        line.clear();
        events.read_line(&mut line).unwrap();
        let endpoint = line
            .strip_prefix("data: /mcp?sessionId=")
            .and_then(|line| line.strip_suffix('\n'))
            .expect("endpoint event has a session URL");
        let session_id = endpoint.to_owned();
        line.clear();
        events.read_line(&mut line).unwrap();
        assert_eq!(line, "\n");

        let ping = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
        let mut post = TcpStream::connect(address).expect("connect POST client");
        write!(
            post,
            "POST /mcp?sessionId={session_id} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{ping}",
            ping.len()
        )
        .unwrap();
        let (post_stream, _) = listener.accept().unwrap();
        serve_connection(post_stream, None, &sessions, &server).unwrap();
        let mut post_response = String::new();
        BufReader::new(post)
            .read_to_string(&mut post_response)
            .unwrap();
        assert!(post_response.starts_with("HTTP/1.1 202 Accepted\r\n"));

        line.clear();
        events.read_line(&mut line).unwrap();
        assert_eq!(line, "event: message\n");
        line.clear();
        events.read_line(&mut line).unwrap();
        let payload = line
            .strip_prefix("data: ")
            .and_then(|line| line.strip_suffix('\n'))
            .expect("message event has JSON data");
        let response: Value = serde_json::from_str(payload).unwrap();
        assert_eq!(response, json!({"jsonrpc":"2.0","id":1,"result":{}}));

        sessions.close(&session_id);
        drop(events);
        drop(event_stream);
    }
}
