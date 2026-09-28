//! DingTalk outbound image marker handling, validation, and upload.
//!
//! Port of `packages/channels/dingtalk/src/outbound-image.ts`. Files are
//! canonicalized before containment checks, read through one opened descriptor,
//! and validated against both their filename extension and magic bytes. The
//! multipart upload transport is injectable so tests never call DingTalk.

use reqwest::Client;
use reqwest::header::CONTENT_TYPE;
use serde_json::Value;
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{future::Future, pin::Pin};

const MEDIA_UPLOAD_API: &str = "https://oapi.dingtalk.com/media/upload";
const MEDIA_UPLOAD_TIMEOUT: Duration = Duration::from_millis(30_000);
const MAX_IMAGE_BYTES: u64 = 20 * 1024 * 1024;
const MAX_API_MESSAGE_UTF16_UNITS: usize = 200;
const IMAGE_EXTENSIONS: &[&str] = &[".png", ".jpg", ".jpeg", ".gif", ".bmp"];
const AUTH_ERROR_CODES: &[i64] = &[40014, 42001];

pub type DingtalkImageFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageMarker {
    /// UTF-16 code-unit offset, matching JavaScript string indexes.
    pub start: usize,
    /// UTF-16 code-unit offset, matching JavaScript string indexes.
    pub end: usize,
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedImage {
    pub data: Vec<u8>,
    pub file_name: String,
    pub mime_type: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedImageOptions {
    pub workspace_dir: PathBuf,
    pub temporary_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DingTalkMediaUploadError {
    pub message: String,
    pub auth_failure: bool,
}

impl DingTalkMediaUploadError {
    fn new(message: impl Into<String>, auth_failure: bool) -> Self {
        Self {
            message: message.into(),
            auth_failure,
        }
    }

    pub fn name(&self) -> &'static str {
        "DingTalkMediaUploadError"
    }
}

impl fmt::Display for DingTalkMediaUploadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for DingTalkMediaUploadError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DingTalkImageUploadRequest {
    pub url: String,
    pub content_type: String,
    pub body: Vec<u8>,
    pub timeout: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DingTalkImageUploadResponse {
    pub status: u16,
    /// Body read failures are distinct from request failures: the TypeScript
    /// implementation reports them as an invalid JSON response.
    pub body: Result<Vec<u8>, String>,
}

/// Test seam for the multipart POST. Implementations own the HTTP exchange and
/// return response bytes separately from request errors to preserve the
/// source's fetch-versus-JSON error classification.
pub trait DingTalkImageUploadTransport: Send + Sync {
    fn post_multipart<'a>(
        &'a self,
        request: DingTalkImageUploadRequest,
    ) -> DingtalkImageFuture<'a, Result<DingTalkImageUploadResponse, String>>;
}

/// Finds visible `[IMAGE: path]` markers outside code spans and code fences.
pub fn find_image_markers(text: &str) -> Vec<ImageMarker> {
    let visible = mask_code(text);
    let units: Vec<u16> = visible.encode_utf16().collect();
    let mut markers = Vec::new();
    let mut offset = 0;

    while offset < units.len() {
        if units[offset] != b'[' as u16 || !starts_image_prefix(&units, offset) {
            offset += 1;
            continue;
        }

        let mut path_start = offset + 7; // `[IMAGE:`
        while path_start < units.len() && is_ecmascript_whitespace_unit(units[path_start]) {
            path_start += 1;
        }
        let mut close = path_start;
        while close < units.len() && !matches!(units[close], 0x005d | 0x000d | 0x000a) {
            close += 1;
        }
        if close == path_start || close >= units.len() || units[close] != b']' as u16 {
            offset += 1;
            continue;
        }

        let path = String::from_utf16_lossy(&units[path_start..close]);
        let path = trim_ecmascript_whitespace(&path);
        if !path.is_empty() {
            markers.push(ImageMarker {
                start: offset,
                end: close + 1,
                path: path.to_owned(),
            });
        }
        offset = close + 1;
    }

    markers
}

/// Replaces markers from right to left so earlier UTF-16 offsets remain valid.
pub fn replace_image_markers(
    text: &str,
    markers: &[ImageMarker],
    replacements: &[String],
) -> Result<String, String> {
    if markers.len() != replacements.len() {
        return Err("Image marker replacement count mismatch".to_owned());
    }

    let mut result = text.to_owned();
    for index in (0..markers.len()).rev() {
        let marker = &markers[index];
        let start = byte_index_for_utf16(&result, marker.start);
        let end = byte_index_for_utf16(&result, marker.end);
        result = format!(
            "{}{}{}",
            &result[..start],
            replacements[index],
            &result[end..]
        );
    }
    Ok(result)
}

/// Replaces an incomplete visible `[I...` suffix with a safe placeholder.
/// A trailing bare `[` is retained because it may be ordinary streamed text.
pub fn strip_partial_image_marker(text: &str) -> String {
    let visible = mask_code(text);
    let units: Vec<u16> = visible.encode_utf16().collect();
    let Some(start) = find_partial_marker_start(&units) else {
        return text.to_owned();
    };
    let byte_index = byte_index_for_utf16(text, start);
    format!("{}[Image pending]", &text[..byte_index])
}

/// Masks complete image markers and incomplete marker suffixes for streaming
/// output while preserving image-like text inside inline/fenced code.
pub fn sanitize_streaming_image_markers(text: &str) -> Result<String, String> {
    let markers = find_image_markers(text);
    let replacements = vec!["[Image pending]".to_owned(); markers.len()];
    let replaced = replace_image_markers(text, &markers, &replacements)?;
    Ok(strip_partial_image_marker(&replaced))
}

/// Reads and validates an image through a single opened file descriptor.
pub fn read_validated_image(
    image_path: &Path,
    options: &ValidatedImageOptions,
) -> Result<ValidatedImage, String> {
    if !image_path.is_absolute() {
        return Err(format!(
            "Image path must be absolute: {}",
            image_path.display()
        ));
    }

    let extension = node_extension(image_path).to_ascii_lowercase();
    if !IMAGE_EXTENSIONS.contains(&extension.as_str()) {
        return Err(format!("Image extension not allowed: {extension}"));
    }

    let real_path = fs::canonicalize(image_path)
        .map_err(|_| format!("Image file not found: {}", image_path.display()))?;
    let workspace = fs::canonicalize(&options.workspace_dir).map_err(|error| error.to_string())?;
    let temporary_dir = options
        .temporary_dir
        .as_deref()
        .map(Path::to_path_buf)
        .unwrap_or_else(std::env::temp_dir);
    let temporary = fs::canonicalize(temporary_dir).map_err(|error| error.to_string())?;
    if !is_inside(&real_path, &workspace) && !is_inside(&real_path, &temporary) {
        return Err(format!(
            "Image path outside allowed directories: {}",
            real_path.display()
        ));
    }

    let mut file = File::open(&real_path).map_err(|error| error.to_string())?;
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err(format!("Not a regular file: {}", real_path.display()));
    }
    if metadata.len() > MAX_IMAGE_BYTES {
        return Err(format!(
            "Image too large: {} bytes (max {})",
            metadata.len(),
            MAX_IMAGE_BYTES
        ));
    }

    let mut header = [0_u8; 16];
    let mut header_len = 0;
    while header_len < header.len() {
        let bytes_read = file
            .read(&mut header[header_len..])
            .map_err(|error| error.to_string())?;
        if bytes_read == 0 {
            break;
        }
        header_len += bytes_read;
    }
    let mime_type = detect_image_mime(&header[..header_len])?;
    let expected_mime = expected_mime(&extension).expect("extension was allowlisted");
    if mime_type != expected_mime {
        return Err(format!(
            "Image type mismatch: {extension} expects {expected_mime} but got {mime_type}"
        ));
    }

    file.seek(SeekFrom::Start(0))
        .map_err(|error| error.to_string())?;
    let mut data = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut data)
        .map_err(|error| error.to_string())?;
    let file_name = real_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();

    Ok(ValidatedImage {
        data,
        file_name,
        mime_type: mime_type.to_owned(),
    })
}

/// Uploads a previously validated image using the production reqwest transport.
pub async fn upload_dingtalk_image(
    client: &Client,
    image: &ValidatedImage,
    access_token: &str,
) -> Result<String, DingTalkMediaUploadError> {
    let transport = ReqwestDingTalkImageUploadTransport { client };
    upload_dingtalk_image_with_transport(&transport, image, access_token).await
}

/// Uploads an image through the provided testable transport.
pub async fn upload_dingtalk_image_with_transport(
    transport: &dyn DingTalkImageUploadTransport,
    image: &ValidatedImage,
    access_token: &str,
) -> Result<String, DingTalkMediaUploadError> {
    let request = build_upload_request(image, access_token).map_err(|_| {
        DingTalkMediaUploadError::new(
            "DingTalk media upload failed: network request failed",
            false,
        )
    })?;
    let response = transport.post_multipart(request).await.map_err(|_| {
        DingTalkMediaUploadError::new(
            "DingTalk media upload failed: network request failed",
            false,
        )
    })?;

    let payload = match response.body {
        Ok(body) => serde_json::from_slice::<Value>(&body).ok(),
        Err(_) => None,
    };
    let Some(payload) = payload else {
        return Err(DingTalkMediaUploadError::new(
            format!(
                "DingTalk media upload failed: HTTP {} invalid JSON response",
                response.status
            ),
            response.status == 401,
        ));
    };
    let payload = payload.as_object();
    let errcode = payload
        .and_then(|payload| payload.get("errcode"))
        .and_then(Value::as_f64);
    if !(200..300).contains(&response.status) || errcode.is_some_and(|code| code != 0.0) {
        let message = payload.and_then(|payload| payload.get("errmsg"));
        let detail = sanitize_api_message(message, access_token);
        let errcode_detail = errcode
            .map(javascript_number_to_string)
            .map(|code| format!(" errcode={code}"))
            .unwrap_or_default();
        let message_detail = if detail.is_empty() {
            String::new()
        } else {
            format!(" {detail}")
        };
        let auth_failure = response.status == 401
            || errcode.is_some_and(|code| {
                AUTH_ERROR_CODES
                    .iter()
                    .any(|auth_code| code == *auth_code as f64)
            });
        return Err(DingTalkMediaUploadError::new(
            format!(
                "DingTalk media upload failed: HTTP {}{errcode_detail}{message_detail}",
                response.status
            ),
            auth_failure,
        ));
    }

    // Match the TypeScript conditional: an empty snake-case value wins over a
    // populated camel-case alias, then fails the final truthiness check.
    let media_id = match payload.and_then(|payload| payload.get("media_id")) {
        Some(Value::String(media_id)) => Some(media_id.as_str()),
        _ => payload
            .and_then(|payload| payload.get("mediaId"))
            .and_then(Value::as_str),
    };
    match media_id.filter(|media_id| !media_id.is_empty()) {
        Some(media_id) => Ok(media_id.to_owned()),
        None => Err(DingTalkMediaUploadError::new(
            "DingTalk media upload failed: response did not include a MediaID",
            false,
        )),
    }
}

fn build_upload_request(
    image: &ValidatedImage,
    access_token: &str,
) -> Result<DingTalkImageUploadRequest, String> {
    let mut url = reqwest::Url::parse(MEDIA_UPLOAD_API).map_err(|error| error.to_string())?;
    url.query_pairs_mut()
        .append_pair("access_token", access_token)
        .append_pair("type", "image");

    // reqwest's multipart feature is not enabled in the workspace. Construct
    // the standard single-file form part directly and send it through `.body`.
    let boundary = format!("----CanopyDingTalkImage{}", uuid::Uuid::new_v4().simple());
    let filename = escape_multipart_filename(&image.file_name);
    let mime_type = safe_multipart_mime_type(&image.mime_type);
    let mut body = Vec::with_capacity(image.data.len() + filename.len() + 256);
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"media\"; filename=\"{filename}\"\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(format!("Content-Type: {mime_type}\r\n\r\n").as_bytes());
    body.extend_from_slice(&image.data);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

    Ok(DingTalkImageUploadRequest {
        url: url.into(),
        content_type: format!("multipart/form-data; boundary={boundary}"),
        body,
        timeout: MEDIA_UPLOAD_TIMEOUT,
    })
}

fn escape_multipart_filename(filename: &str) -> String {
    let mut escaped = String::with_capacity(filename.len());
    for character in filename.chars() {
        match character {
            '"' => escaped.push_str("%22"),
            '\r' => escaped.push_str("%0D"),
            '\n' => escaped.push_str("%0A"),
            _ => escaped.push(character),
        }
    }
    escaped
}

fn safe_multipart_mime_type(mime_type: &str) -> String {
    if mime_type.is_empty()
        || !mime_type
            .bytes()
            .all(|byte| byte.is_ascii() && !byte.is_ascii_control())
    {
        "application/octet-stream".to_owned()
    } else {
        mime_type.to_ascii_lowercase()
    }
}

fn sanitize_api_message(message: Option<&Value>, access_token: &str) -> String {
    let mut message = message
        .filter(|message| !message.is_null())
        .map(javascript_string)
        .unwrap_or_default();
    if !access_token.is_empty() {
        message = message.replace(access_token, "[redacted]");
    }

    let mut sanitized = String::with_capacity(message.len().min(MAX_API_MESSAGE_UTF16_UNITS));
    let mut in_control_run = false;
    for character in message.chars() {
        if matches!(character, '\r' | '\n' | '\t') {
            if !in_control_run {
                sanitized.push(' ');
                in_control_run = true;
            }
        } else {
            in_control_run = false;
            sanitized.push(character);
        }
    }
    let end = byte_index_for_utf16(&sanitized, MAX_API_MESSAGE_UTF16_UNITS);
    sanitized.truncate(end);
    sanitized
}

fn javascript_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(number) => number
            .as_f64()
            .map(javascript_number_to_string)
            .unwrap_or_else(|| number.to_string()),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(|value| match value {
                Value::Null => String::new(),
                _ => javascript_string(value),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

fn javascript_number_to_string(number: f64) -> String {
    if number == 0.0 {
        return "0".to_owned();
    }
    if number.fract() == 0.0 && number.abs() < 1.0e21 {
        return format!("{number:.0}");
    }
    number.to_string()
}

fn node_extension(path: &Path) -> String {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return String::new();
    };
    let Some(dot) = file_name.rfind('.') else {
        return String::new();
    };
    if dot == 0 && !file_name[1..].contains('.') {
        return String::new();
    }
    file_name[dot..].to_owned()
}

fn expected_mime(extension: &str) -> Option<&'static str> {
    match extension {
        ".png" => Some("image/png"),
        ".jpg" | ".jpeg" => Some("image/jpeg"),
        ".gif" => Some("image/gif"),
        ".bmp" => Some("image/bmp"),
        _ => None,
    }
}

fn detect_image_mime(data: &[u8]) -> Result<&'static str, String> {
    if data.starts_with(&[0x89, 0x50, 0x4e, 0x47]) {
        return Ok("image/png");
    }
    if data.starts_with(&[0xff, 0xd8, 0xff]) {
        return Ok("image/jpeg");
    }
    if data.starts_with(b"GIF") {
        return Ok("image/gif");
    }
    if data.starts_with(b"BM") {
        return Ok("image/bmp");
    }
    Err("Unrecognized image format".to_owned())
}

fn is_inside(path: &Path, directory: &Path) -> bool {
    path == directory || path.starts_with(directory)
}

fn mask_code(text: &str) -> String {
    let units: Vec<u16> = text.encode_utf16().collect();
    let mut masked = units.clone();
    let mut offset = 0;

    while offset < units.len() {
        if units[offset] != b'`' as u16 {
            offset += 1;
            continue;
        }

        let mut run_length = 1;
        while units.get(offset + run_length) == Some(&(b'`' as u16)) {
            run_length += 1;
        }
        let closing = find_backtick_run(&units, offset + run_length, run_length);
        let newline = if run_length >= 3 {
            None
        } else {
            units[offset + run_length..]
                .iter()
                .position(|unit| *unit == b'\n' as u16)
                .map(|relative| offset + run_length + relative)
        };
        let closes_before_newline =
            closing.is_some_and(|closing| newline.is_none_or(|newline| closing < newline));
        let end = if closes_before_newline {
            closing.unwrap() + run_length
        } else {
            newline.unwrap_or(units.len())
        };
        for unit in masked.iter_mut().take(end).skip(offset) {
            if *unit != b'\n' as u16 {
                *unit = b' ' as u16;
            }
        }
        offset = end;
    }

    String::from_utf16_lossy(&masked)
}

fn find_backtick_run(units: &[u16], start: usize, length: usize) -> Option<usize> {
    if length == 0 || start > units.len() || length > units.len().saturating_sub(start) {
        return None;
    }
    (start..=units.len() - length).find(|offset| {
        units[*offset..*offset + length]
            .iter()
            .all(|unit| *unit == b'`' as u16)
    })
}

fn starts_image_prefix(units: &[u16], offset: usize) -> bool {
    let prefix = b"[IMAGE:";
    units
        .get(offset..offset + prefix.len())
        .is_some_and(|candidate| {
            candidate.iter().zip(prefix).all(|(unit, expected)| {
                if expected.is_ascii_alphabetic() {
                    *unit == expected.to_ascii_lowercase() as u16
                        || *unit == expected.to_ascii_uppercase() as u16
                } else {
                    *unit == *expected as u16
                }
            })
        })
}

fn find_partial_marker_start(units: &[u16]) -> Option<usize> {
    for start in 0..units.len().saturating_sub(1) {
        if units[start] != b'[' as u16 || !unit_eq_ascii_case(units[start + 1], b'I') {
            continue;
        }
        let mut cursor = start + 2;
        for letter in *b"MAGE" {
            if units
                .get(cursor)
                .is_some_and(|unit| unit_eq_ascii_case(*unit, letter))
            {
                cursor += 1;
            } else {
                break;
            }
        }
        if units.get(cursor) == Some(&(b':' as u16)) {
            cursor += 1;
            if units[cursor..]
                .iter()
                .any(|unit| matches!(*unit, 0x005d | 0x000d | 0x000a))
            {
                continue;
            }
            cursor = units.len();
        }
        if cursor == units.len() {
            return Some(start);
        }
    }
    None
}

fn unit_eq_ascii_case(unit: u16, byte: u8) -> bool {
    unit == byte.to_ascii_lowercase() as u16 || unit == byte.to_ascii_uppercase() as u16
}

fn is_ecmascript_whitespace_unit(unit: u16) -> bool {
    matches!(
        unit,
        0x0009..=0x000d
            | 0x0020
            | 0x00a0
            | 0x1680
            | 0x2000..=0x200a
            | 0x2028..=0x2029
            | 0x202f
            | 0x205f
            | 0x3000
            | 0xfeff
    )
}

fn trim_ecmascript_whitespace(value: &str) -> &str {
    value.trim_matches(is_ecmascript_whitespace)
}

fn is_ecmascript_whitespace(ch: char) -> bool {
    matches!(
        ch,
        '\u{0009}'..='\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'..='\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

fn byte_index_for_utf16(text: &str, units: usize) -> usize {
    let mut consumed = 0;
    for (byte_index, character) in text.char_indices() {
        let character_units = character.len_utf16();
        if consumed + character_units > units {
            return byte_index;
        }
        consumed += character_units;
        if consumed == units {
            return byte_index + character.len_utf8();
        }
    }
    text.len()
}

struct ReqwestDingTalkImageUploadTransport<'a> {
    client: &'a Client,
}

impl DingTalkImageUploadTransport for ReqwestDingTalkImageUploadTransport<'_> {
    fn post_multipart<'a>(
        &'a self,
        request: DingTalkImageUploadRequest,
    ) -> DingtalkImageFuture<'a, Result<DingTalkImageUploadResponse, String>> {
        Box::pin(async move {
            let response = self
                .client
                .post(request.url)
                .header(CONTENT_TYPE, request.content_type)
                .timeout(request.timeout)
                .body(request.body)
                .send()
                .await
                .map_err(|error| error.to_string())?;
            let status = response.status().as_u16();
            let body = response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|error| error.to_string());
            Ok(DingTalkImageUploadResponse { status, body })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AUTH_ERROR_CODES, DingTalkImageUploadRequest, DingTalkImageUploadResponse,
        DingTalkImageUploadTransport, DingTalkMediaUploadError, DingtalkImageFuture, ImageMarker,
        ValidatedImage, ValidatedImageOptions, find_image_markers, read_validated_image,
        replace_image_markers, sanitize_streaming_image_markers, strip_partial_image_marker,
        upload_dingtalk_image_with_transport,
    };
    use serde_json::{Value, json};
    use std::collections::VecDeque;
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const PNG_DATA: &[u8] = &[0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00];

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(prefix: &str) -> Self {
            static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "{prefix}-{}-{time}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn options(workspace: &Path, temporary: &Path) -> ValidatedImageOptions {
        ValidatedImageOptions {
            workspace_dir: workspace.to_path_buf(),
            temporary_dir: Some(temporary.to_path_buf()),
        }
    }

    #[test]
    fn finds_visible_markers_and_replaces_them_outside_inline_and_fenced_code() {
        let text = [
            "before",
            "[IMAGE: /tmp/real.png]",
            "```text",
            "[IMAGE: /tmp/fenced.png]",
            "```",
            "`[IMAGE: /tmp/inline.png]`",
            "``[IMAGE: /tmp/double-inline.png]``",
            "after",
        ]
        .join("\n");
        let markers = find_image_markers(&text);
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].path, "/tmp/real.png");
        assert_eq!(
            replace_image_markers(&text, &markers, &["![image](media-id)".to_owned()]).unwrap(),
            [
                "before",
                "![image](media-id)",
                "```text",
                "[IMAGE: /tmp/fenced.png]",
                "```",
                "`[IMAGE: /tmp/inline.png]`",
                "``[IMAGE: /tmp/double-inline.png]``",
                "after",
            ]
            .join("\n")
        );
    }

    #[test]
    fn replaces_repeated_markers_by_source_position_and_checks_counts() {
        let text = [
            "`[IMAGE: /tmp/same.png]`",
            "[IMAGE: /tmp/same.png]",
            "[IMAGE: /tmp/same.png]",
        ]
        .join("\n");
        let markers = find_image_markers(&text);
        assert_eq!(markers.len(), 2);
        assert_eq!(
            replace_image_markers(&text, &markers, &["first".to_owned(), "second".to_owned()])
                .unwrap(),
            ["`[IMAGE: /tmp/same.png]`", "first", "second"].join("\n")
        );
        assert_eq!(
            replace_image_markers(&text, &markers, &["one".to_owned()]).unwrap_err(),
            "Image marker replacement count mismatch"
        );
    }

    #[test]
    fn marker_offsets_use_utf16_and_paths_are_trimmed() {
        let text = "🖼️ [image: \u{feff} /tmp/pic.png \u{feff}]";
        let markers = find_image_markers(text);
        assert_eq!(markers.len(), 1);
        assert_eq!(
            markers[0].start,
            text.encode_utf16()
                .take_while(|unit| *unit != b'[' as u16)
                .count()
        );
        assert_eq!(markers[0].path, "/tmp/pic.png");
        assert_eq!(
            replace_image_markers(text, &markers, &["[Image pending]".to_owned()]).unwrap(),
            "🖼️ [Image pending]"
        );
    }

    #[test]
    fn partial_marker_stripping_preserves_bare_brackets_and_code() {
        assert_eq!(strip_partial_image_marker("value is arr["), "value is arr[");
        assert_eq!(strip_partial_image_marker("see [1] and ["), "see [1] and [");
        assert_eq!(strip_partial_image_marker("see [I"), "see [Image pending]");
        assert_eq!(
            strip_partial_image_marker("see [IMAGE: /tmp/pic.png"),
            "see [Image pending]"
        );
        assert_eq!(strip_partial_image_marker("`[IMAGE"), "`[IMAGE");
    }

    #[test]
    fn streaming_sanitization_hides_complete_and_partial_paths_but_preserves_code() {
        assert_eq!(
            sanitize_streaming_image_markers("before [IMAGE: /Users/private/image.png] after")
                .unwrap(),
            "before [Image pending] after"
        );
        assert_eq!(
            sanitize_streaming_image_markers("before [IMAGE: /Users/private/image").unwrap(),
            "before [Image pending]"
        );
        assert_eq!(
            sanitize_streaming_image_markers("before [IMAGE: /Users/private/[image").unwrap(),
            "before [Image pending]"
        );
        let code = [
            "`[IMAGE: /Users/inline.png]`",
            "```text",
            "[IMAGE: /Users/fenced.png]",
            "```",
        ]
        .join("\n");
        assert_eq!(sanitize_streaming_image_markers(&code).unwrap(), code);
    }

    #[test]
    fn finds_multiple_case_insensitive_markers_and_skips_empty_paths() {
        let text = "[image: /tmp/a.png] [IMAGE:   ] [Image:\n/tmp/b.png]";
        let markers = find_image_markers(text);
        assert_eq!(markers.len(), 2);
        assert_eq!(markers[0].path, "/tmp/a.png");
        assert_eq!(markers[1].path, "/tmp/b.png");
    }

    #[test]
    fn reads_a_regular_image_inside_canonical_workspace() {
        let workspace = TestDir::new("dingtalk-image-workspace");
        let image_path = workspace.path().join("image.png");
        fs::write(&image_path, PNG_DATA).unwrap();
        let image = read_validated_image(&image_path, &options(workspace.path(), workspace.path()))
            .unwrap();
        assert_eq!(image.file_name, "image.png");
        assert_eq!(image.mime_type, "image/png");
        assert_eq!(image.data, PNG_DATA);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_symlink_that_escapes_allowed_directories() {
        use std::os::unix::fs::symlink;

        let workspace = TestDir::new("dingtalk-image-workspace");
        let outside = TestDir::new("dingtalk-image-outside");
        let outside_image = outside.path().join("outside.png");
        let linked_image = workspace.path().join("linked.png");
        fs::write(&outside_image, PNG_DATA).unwrap();
        symlink(&outside_image, &linked_image).unwrap();
        let error =
            read_validated_image(&linked_image, &options(workspace.path(), workspace.path()))
                .unwrap_err();
        assert!(error.contains("outside allowed directories"));
    }

    #[test]
    fn allows_canonical_paths_inside_the_temporary_directory() {
        let workspace = TestDir::new("dingtalk-image-workspace");
        let temporary = TestDir::new("dingtalk-image-temp");
        let image_path = temporary.path().join("temp.png");
        fs::write(&image_path, PNG_DATA).unwrap();
        let image = read_validated_image(&image_path, &options(workspace.path(), temporary.path()))
            .unwrap();
        assert_eq!(image.file_name, "temp.png");
    }

    #[test]
    fn rejects_relative_paths_and_unsupported_extensions() {
        let workspace = TestDir::new("dingtalk-image-workspace");
        assert!(
            read_validated_image(
                Path::new("relative.png"),
                &options(workspace.path(), workspace.path())
            )
            .unwrap_err()
            .contains("must be absolute")
        );
        let image_path = workspace.path().join("image.webp");
        fs::write(&image_path, PNG_DATA).unwrap();
        assert!(
            read_validated_image(&image_path, &options(workspace.path(), workspace.path()))
                .unwrap_err()
                .contains("extension not allowed")
        );
    }

    #[test]
    fn validates_extensions_against_png_jpeg_gif_and_bmp_magic() {
        let workspace = TestDir::new("dingtalk-image-workspace");
        let samples: [(&str, &[u8], &str); 5] = [
            ("UPPER.PNG", PNG_DATA, "image/png"),
            ("photo.jpg", &[0xff, 0xd8, 0xff, 0x00], "image/jpeg"),
            ("photo.jpeg", &[0xff, 0xd8, 0xff], "image/jpeg"),
            ("animation.gif", b"GIF89a", "image/gif"),
            ("bitmap.bmp", b"BM\0\0", "image/bmp"),
        ];
        for (name, bytes, mime_type) in samples {
            let path = workspace.path().join(name);
            fs::write(&path, bytes).unwrap();
            assert_eq!(
                read_validated_image(&path, &options(workspace.path(), workspace.path()))
                    .unwrap()
                    .mime_type,
                mime_type
            );
        }
    }

    #[test]
    fn rejects_extension_magic_mismatches_and_unrecognized_formats() {
        let workspace = TestDir::new("dingtalk-image-workspace");
        let mismatch = workspace.path().join("image.jpg");
        fs::write(&mismatch, PNG_DATA).unwrap();
        assert!(
            read_validated_image(&mismatch, &options(workspace.path(), workspace.path()))
                .unwrap_err()
                .contains("Image type mismatch")
        );

        let unknown = workspace.path().join("unknown.png");
        fs::write(&unknown, b"not an image").unwrap();
        assert_eq!(
            read_validated_image(&unknown, &options(workspace.path(), workspace.path()))
                .unwrap_err(),
            "Unrecognized image format"
        );
    }

    #[test]
    fn rejects_directories_and_oversized_files_before_reading_data() {
        let workspace = TestDir::new("dingtalk-image-workspace");
        let directory = workspace.path().join("directory.png");
        fs::create_dir(&directory).unwrap();
        assert!(
            read_validated_image(&directory, &options(workspace.path(), workspace.path()))
                .unwrap_err()
                .contains("Not a regular file")
        );

        let oversized = workspace.path().join("large.png");
        let mut file = File::create(&oversized).unwrap();
        file.write_all(PNG_DATA).unwrap();
        file.set_len(20 * 1024 * 1024 + 1).unwrap();
        assert!(
            read_validated_image(&oversized, &options(workspace.path(), workspace.path()))
                .unwrap_err()
                .contains("Image too large")
        );
    }

    struct MockUploadTransport {
        responses: Mutex<VecDeque<Result<DingTalkImageUploadResponse, String>>>,
        requests: Mutex<Vec<DingTalkImageUploadRequest>>,
    }

    impl MockUploadTransport {
        fn new(
            responses: impl IntoIterator<Item = Result<DingTalkImageUploadResponse, String>>,
        ) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn requests(&self) -> Vec<DingTalkImageUploadRequest> {
            self.requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl DingTalkImageUploadTransport for MockUploadTransport {
        fn post_multipart<'a>(
            &'a self,
            request: DingTalkImageUploadRequest,
        ) -> DingtalkImageFuture<'a, Result<DingTalkImageUploadResponse, String>> {
            self.requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(request);
            let response = self
                .responses
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
                .unwrap_or_else(|| Err("no mock response".to_owned()));
            Box::pin(async move { response })
        }
    }

    fn upload_response(status: u16, payload: Value) -> DingTalkImageUploadResponse {
        DingTalkImageUploadResponse {
            status,
            body: serde_json::to_vec(&payload).map_err(|error| error.to_string()),
        }
    }

    fn valid_image() -> ValidatedImage {
        ValidatedImage {
            data: PNG_DATA.to_vec(),
            file_name: "image.png".to_owned(),
            mime_type: "image/png".to_owned(),
        }
    }

    #[tokio::test]
    async fn uploads_multipart_image_with_query_timeout_and_media_id() {
        let transport = MockUploadTransport::new([Ok(upload_response(
            200,
            json!({ "errcode": 0, "media_id": "@lAL-test-media-id" }),
        ))]);
        assert_eq!(
            upload_dingtalk_image_with_transport(&transport, &valid_image(), "access-token")
                .await
                .unwrap(),
            "@lAL-test-media-id"
        );
        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0]
                .url
                .starts_with("https://oapi.dingtalk.com/media/upload?")
        );
        assert!(requests[0].url.contains("access_token=access-token"));
        assert!(requests[0].url.contains("type=image"));
        assert_eq!(requests[0].timeout, Duration::from_millis(30_000));
        assert!(
            requests[0]
                .content_type
                .starts_with("multipart/form-data; boundary=")
        );
        let body = String::from_utf8_lossy(&requests[0].body);
        assert!(body.contains("name=\"media\"; filename=\"image.png\""));
        assert!(body.contains("Content-Type: image/png"));
        assert!(
            requests[0]
                .body
                .windows(PNG_DATA.len())
                .any(|window| window == PNG_DATA)
        );
    }

    #[tokio::test]
    async fn accepts_camel_case_media_id_when_snake_case_is_not_a_string() {
        let transport = MockUploadTransport::new([Ok(upload_response(
            200,
            json!({ "errcode": 0, "media_id": 3, "mediaId": "camel-id" }),
        ))]);
        assert_eq!(
            upload_dingtalk_image_with_transport(&transport, &valid_image(), "token")
                .await
                .unwrap(),
            "camel-id"
        );
    }

    #[tokio::test]
    async fn returns_safe_http_api_errors_and_classifies_auth_failures() {
        for auth_code in AUTH_ERROR_CODES {
            let transport = MockUploadTransport::new([Ok(upload_response(
                200,
                json!({ "errcode": auth_code, "errmsg": "expired" }),
            ))]);
            let error = upload_dingtalk_image_with_transport(&transport, &valid_image(), "token")
                .await
                .unwrap_err();
            assert!(error.auth_failure);
            assert!(error.to_string().contains(&format!("errcode={auth_code}")));
        }

        let transport =
            MockUploadTransport::new([Ok(upload_response(401, json!({ "errmsg": "no auth" })))]);
        assert!(
            upload_dingtalk_image_with_transport(&transport, &valid_image(), "token")
                .await
                .unwrap_err()
                .auth_failure
        );

        let transport = MockUploadTransport::new([Ok(upload_response(
            200,
            json!({ "errcode": 40035, "errmsg": "bad request" }),
        ))]);
        assert!(
            !upload_dingtalk_image_with_transport(&transport, &valid_image(), "token")
                .await
                .unwrap_err()
                .auth_failure
        );
    }

    #[tokio::test]
    async fn redacts_token_flattens_control_whitespace_and_caps_api_detail() {
        let transport = MockUploadTransport::new([Ok(upload_response(
            400,
            json!({
                "errcode": 40035,
                "errmsg": "token\r\nsecret and token\tagain"
            }),
        ))]);
        let error = upload_dingtalk_image_with_transport(&transport, &valid_image(), "token")
            .await
            .unwrap_err();
        assert!(!error.to_string().contains("token"));
        assert!(
            error
                .to_string()
                .contains("[redacted] secret and [redacted] again")
        );

        let transport = MockUploadTransport::new([Ok(upload_response(
            400,
            json!({ "errcode": 1, "errmsg": "x".repeat(500) }),
        ))]);
        let error = upload_dingtalk_image_with_transport(&transport, &valid_image(), "")
            .await
            .unwrap_err();
        let detail = error.message.split_once(" errcode=1 ").unwrap().1;
        assert_eq!(detail.encode_utf16().count(), 200);
    }

    #[tokio::test]
    async fn request_and_response_parse_failures_have_distinct_safe_errors() {
        let transport =
            MockUploadTransport::new([Err("request URL included access_token=secret".to_owned())]);
        let error = upload_dingtalk_image_with_transport(&transport, &valid_image(), "secret")
            .await
            .unwrap_err();
        assert_eq!(
            error.message,
            "DingTalk media upload failed: network request failed"
        );
        assert!(!error.auth_failure);
        assert!(!error.to_string().contains("secret"));

        let transport = MockUploadTransport::new([Ok(DingTalkImageUploadResponse {
            status: 401,
            body: Err("failed reading response body".to_owned()),
        })]);
        let error = upload_dingtalk_image_with_transport(&transport, &valid_image(), "secret")
            .await
            .unwrap_err();
        assert_eq!(
            error.message,
            "DingTalk media upload failed: HTTP 401 invalid JSON response"
        );
        assert!(error.auth_failure);

        let transport = MockUploadTransport::new([Ok(DingTalkImageUploadResponse {
            status: 200,
            body: Ok(b"not json".to_vec()),
        })]);
        assert!(
            upload_dingtalk_image_with_transport(&transport, &valid_image(), "secret")
                .await
                .unwrap_err()
                .message
                .contains("HTTP 200 invalid JSON response")
        );
    }

    #[tokio::test]
    async fn missing_and_empty_media_ids_fail_and_empty_snake_case_wins() {
        for payload in [json!({ "errcode": 0 }), json!({ "media_id": "" })] {
            let transport = MockUploadTransport::new([Ok(upload_response(200, payload))]);
            let error = upload_dingtalk_image_with_transport(&transport, &valid_image(), "token")
                .await
                .unwrap_err();
            assert!(!error.auth_failure);
            assert!(error.message.contains("did not include a MediaID"));
        }
        let transport = MockUploadTransport::new([Ok(upload_response(
            200,
            json!({ "errcode": 0, "media_id": "", "mediaId": "ignored" }),
        ))]);
        assert!(
            upload_dingtalk_image_with_transport(&transport, &valid_image(), "token")
                .await
                .is_err()
        );
    }

    #[test]
    fn upload_error_has_expected_name_and_display() {
        let error = DingTalkMediaUploadError {
            message: "failed".to_owned(),
            auth_failure: true,
        };
        assert_eq!(error.name(), "DingTalkMediaUploadError");
        assert_eq!(error.to_string(), "failed");
    }

    #[test]
    fn marker_replacement_handles_utf16_and_descending_offsets() {
        let text = "🖼️ [IMAGE: one.png] and [IMAGE: two.png]";
        let markers = find_image_markers(text);
        assert_eq!(markers.len(), 2);
        assert_eq!(markers[0].start, 4);
        assert_eq!(
            replace_image_markers(text, &markers, &["one".to_owned(), "two".to_owned()]).unwrap(),
            "🖼️ one and two"
        );
        let manual = [ImageMarker {
            start: 0,
            end: 1,
            path: "".to_owned(),
        }];
        assert_eq!(
            replace_image_markers("abc", &manual, &["x".to_owned()]).unwrap(),
            "xbc"
        );
    }
}
