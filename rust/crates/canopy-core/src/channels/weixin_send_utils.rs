//! Weixin outbound text and image-path utilities.
//!
//! Port of the non-cryptographic helpers in
//! `packages/channels/weixin/src/send.ts`. Encrypted image upload remains
//! separate because it depends on the AES media implementation.

use super::weixin_api::{
    ApiHttpClient, ApiRuntime, WeixinApiError, send_message, send_message_with_http,
};
use super::weixin_types::{
    MessageItem, MessageItemType, MessageState, MessageType, TextItem, WeixinMessage,
};
use regex::Regex;
use reqwest::Client;
use serde_json::to_value;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;
use uuid::Uuid;

const MAX_IMAGE_SIZE: u64 = 20 * 1024 * 1024;
const ALLOWED_EXTENSIONS: &[&str] = &[".png", ".jpg", ".jpeg", ".gif", ".webp"];

type MarkdownTransform = (Regex, &'static str);

fn markdown_transforms() -> &'static [MarkdownTransform] {
    static TRANSFORMS: OnceLock<Vec<MarkdownTransform>> = OnceLock::new();
    TRANSFORMS.get_or_init(|| {
        let whitespace =
            r"[\t-\r \u{00a0}\u{1680}\u{2000}-\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}\u{feff}]";
        let patterns: Vec<(String, &'static str)> = vec![
            (r"```[\s\S]*?\n([\s\S]*?)```".to_owned(), "$1"),
            (r"`([^`]+)`".to_owned(), "$1"),
            (r"\*\*\*([^\r\n\u{2028}\u{2029}]+?)\*\*\*".to_owned(), "$1"),
            (r"\*\*([^\r\n\u{2028}\u{2029}]+?)\*\*".to_owned(), "$1"),
            (r"\*([^\r\n\u{2028}\u{2029}]+?)\*".to_owned(), "$1"),
            (r"___([^\r\n\u{2028}\u{2029}]+?)___".to_owned(), "$1"),
            (r"__([^\r\n\u{2028}\u{2029}]+?)__".to_owned(), "$1"),
            (r"_([^\r\n\u{2028}\u{2029}]+?)_".to_owned(), "$1"),
            (r"~~([^\r\n\u{2028}\u{2029}]+?)~~".to_owned(), "$1"),
            (
                format!(r"(?m)(^|[\r\u{{2028}}\u{{2029}}])#{{1,6}}{whitespace}+"),
                "$1",
            ),
            (r"!\[([^\]]*)\]\([^)]+\)".to_owned(), "[$1]"),
            (r"\[([^\]]+)\]\(([^)]+)\)".to_owned(), "$1 ($2)"),
            (
                format!(r"(?m)(^|[\r\u{{2028}}\u{{2029}}])>{whitespace}+"),
                "$1",
            ),
            (
                r"(?m)(^|[\r\u{2028}\u{2029}])([-*_]{3,})($|[\r\u{2028}\u{2029}])"
                    .to_owned(),
                "$1---$3",
            ),
            (
                format!(r"(?m)(^|[\r\u{{2028}}\u{{2029}}]){whitespace}*[-*+]{whitespace}+"),
                "$1- ",
            ),
            (
                format!(r"(?m)(^|[\r\u{{2028}}\u{{2029}}]){whitespace}*([0-9]+)\.{whitespace}+"),
                "$1$2. ",
            ),
            (r"\n{3,}".to_owned(), "\n\n"),
        ];
        patterns
        .into_iter()
        .map(|(pattern, replacement)| {
            (
                Regex::new(&pattern).expect("static Markdown transform regex is valid"),
                replacement,
            )
        })
        .collect()
    })
}

/// Convert Markdown to plain text using the source's ordered replacements.
pub fn markdown_to_plain_text(text: &str) -> String {
    let transformed = markdown_transforms()
        .iter()
        .fold(text.to_owned(), |text, (regex, replacement)| {
            regex.replace_all(&text, *replacement).into_owned()
        });
    trim_ecmascript_whitespace(&transformed).to_owned()
}

/// Detect a supported image format from its magic bytes.
pub fn detect_image_mime(data: &[u8]) -> Result<&'static str, String> {
    if data.starts_with(&[0x89, 0x50, 0x4e, 0x47]) {
        return Ok("image/png");
    }
    if data.starts_with(b"GIF") {
        return Ok("image/gif");
    }
    // WEBP shares its RIFF header with formats such as WAV and AVI. Check the
    // format marker at bytes 8-11 rather than accepting every RIFF file.
    if data.starts_with(b"RIFF") && data.get(8..12) == Some(b"WEBP") {
        return Ok("image/webp");
    }
    if data.starts_with(&[0xff, 0xd8, 0xff]) {
        return Ok("image/jpeg");
    }
    Err("Unrecognized image format: magic bytes do not match any supported type".to_owned())
}

/// Resolve and validate an image file against the source extension, size,
/// directory, and magic-byte checks. Returns its canonical real path.
pub fn validate_image_path(
    image_path: impl AsRef<Path>,
    workspace_dirs: &[PathBuf],
) -> Result<PathBuf, String> {
    let resolved = resolve_path(image_path.as_ref()).map_err(|error| error.to_string())?;
    let extension = node_extension(&resolved).to_ascii_lowercase();
    if !ALLOWED_EXTENSIONS.contains(&extension.as_str()) {
        return Err(format!(
            "Image extension not allowed: {extension} (path: {})",
            resolved.display()
        ));
    }

    let real = fs::canonicalize(&resolved)
        .map_err(|_| format!("Image file not found: {}", resolved.display()))?;
    let metadata = fs::metadata(&real).map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err(format!("Not a regular file: {}", real.display()));
    }
    if metadata.len() > MAX_IMAGE_SIZE {
        return Err(format!(
            "Image too large: {} bytes (max {MAX_IMAGE_SIZE})",
            metadata.len()
        ));
    }

    let allowed_dirs = allowed_directories(workspace_dirs)?;
    if !allowed_dirs
        .iter()
        .any(|directory| is_inside(&real, directory))
    {
        let display_dirs = format_allowed_directories(&allowed_dirs);
        return Err(format!(
            "Image path outside allowed directories: {}. Allowed directories: {display_dirs}",
            real.display()
        ));
    }

    // Read the header from one opened descriptor. This is the source's
    // 16-byte signature check and avoids a second path-based read here.
    let mut file = File::open(&real).map_err(|error| error.to_string())?;
    let mut header = [0_u8; 16];
    let bytes_read = file.read(&mut header).map_err(|error| error.to_string())?;
    let mime = detect_image_mime(&header[..bytes_read])?;
    let expected = expected_mime(&extension).expect("extension was checked above");
    if mime != expected {
        return Err(format!(
            "Image type mismatch: ext={extension} expects {expected} but got {mime}"
        ));
    }

    Ok(real)
}

/// Input fields for sending a Weixin text message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SendTextParams<'a> {
    pub to: &'a str,
    pub text: &'a str,
    pub base_url: &'a str,
    pub token: &'a str,
    pub context_token: &'a str,
}

/// Send text through the production Weixin HTTP API client.
pub async fn send_text(client: &Client, params: SendTextParams<'_>) -> Result<(), WeixinApiError> {
    let message = make_text_message(params);
    send_message(
        client,
        params.base_url,
        params.token,
        Some(to_value(message).expect("Weixin text message is serializable")),
    )
    .await
}

/// Mockable variant of [`send_text`] for transport and message-construction tests.
pub async fn send_text_with_http(
    client: &dyn ApiHttpClient,
    runtime: &dyn ApiRuntime,
    params: SendTextParams<'_>,
) -> Result<(), WeixinApiError> {
    let message = make_text_message(params);
    send_message_with_http(
        client,
        runtime,
        params.base_url,
        params.token,
        Some(to_value(message).expect("Weixin text message is serializable")),
    )
    .await
}

fn make_text_message(params: SendTextParams<'_>) -> WeixinMessage {
    WeixinMessage {
        to_user_id: Some(params.to.to_owned()),
        from_user_id: Some(String::new()),
        client_id: Some(Uuid::new_v4().to_string()),
        message_type: Some(MessageType::BOT),
        message_state: Some(MessageState::FINISH),
        context_token: Some(params.context_token.to_owned()),
        item_list: Some(vec![MessageItem {
            r#type: Some(MessageItemType::TEXT),
            text_item: Some(TextItem {
                text: Some(markdown_to_plain_text(params.text)),
            }),
            ..MessageItem::default()
        }]),
        ..WeixinMessage::default()
    }
}

fn resolve_path(path: &Path) -> std::io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };

    let mut resolved = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            component => resolved.push(component.as_os_str()),
        }
    }
    Ok(resolved)
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
        ".webp" => Some("image/webp"),
        _ => None,
    }
}

fn allowed_directories(workspace_dirs: &[PathBuf]) -> Result<Vec<PathBuf>, String> {
    let temporary_dir = std::env::temp_dir();
    let mut directories = Vec::new();
    add_allowed_directory(&mut directories, Path::new("/tmp"), false);
    add_allowed_directory(&mut directories, Path::new("/tmp"), true);
    add_allowed_directory(&mut directories, &temporary_dir, false);
    add_allowed_directory(&mut directories, &temporary_dir, true);

    for workspace_dir in workspace_dirs {
        let resolved = resolve_path(workspace_dir).map_err(|error| error.to_string())?;
        let real = fs::canonicalize(&resolved).map_err(|error| error.to_string())?;
        push_unique(&mut directories, real);
    }
    Ok(directories)
}

fn add_allowed_directory(directories: &mut Vec<PathBuf>, path: &Path, canonicalize: bool) {
    let directory = if canonicalize {
        match fs::canonicalize(path) {
            Ok(directory) => directory,
            Err(_) => return,
        }
    } else {
        path.to_path_buf()
    };
    push_unique(directories, directory);
}

fn push_unique(directories: &mut Vec<PathBuf>, directory: PathBuf) {
    if !directories.iter().any(|existing| existing == &directory) {
        directories.push(directory);
    }
}

fn is_inside(path: &Path, directory: &Path) -> bool {
    path == directory || path.strip_prefix(directory).is_ok()
}

fn format_allowed_directories(directories: &[PathBuf]) -> String {
    let mut formatted = Vec::<String>::new();
    for directory in directories {
        let mut display = directory.display().to_string();
        while display.len() > 1 && (display.ends_with('/') || display.ends_with('\\')) {
            if display.as_bytes().get(1) == Some(&b':') && display.len() == 3 {
                break;
            }
            display.pop();
        }
        if !formatted.contains(&display) {
            formatted.push(display);
        }
    }
    formatted.join(", ")
}

fn trim_ecmascript_whitespace(text: &str) -> &str {
    const ECMASCRIPT_WHITESPACE: &[char] = &[
        '\u{0009}', '\u{000a}', '\u{000b}', '\u{000c}', '\u{000d}', '\u{0020}', '\u{00a0}',
        '\u{1680}', '\u{2000}', '\u{2001}', '\u{2002}', '\u{2003}', '\u{2004}', '\u{2005}',
        '\u{2006}', '\u{2007}', '\u{2008}', '\u{2009}', '\u{200a}', '\u{2028}', '\u{2029}',
        '\u{202f}', '\u{205f}', '\u{3000}', '\u{feff}',
    ];
    text.trim_matches(ECMASCRIPT_WHITESPACE)
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_IMAGE_SIZE, SendTextParams, detect_image_mime, markdown_to_plain_text,
        send_text_with_http, validate_image_path,
    };
    use crate::channels::weixin_api::{
        ApiFuture, ApiHttpClient, ApiHttpRequest, ApiHttpResponse, ApiRuntime, ApiTransportError,
    };
    use serde_json::{Value, json};
    use std::fs::{self, File, OpenOptions};
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::time::Duration;
    use uuid::Uuid;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn under(directory: &Path) -> Self {
            let path = directory.join(format!("canopy-weixin-send-utils-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).expect("create test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_png(path: &Path) {
        fs::write(path, b"\x89PNG\r\n\x1a\nimage-data").expect("write png fixture");
    }

    #[test]
    fn markdown_transformations_keep_the_source_replacement_order() {
        for (input, expected) in [
            ("```js\nconst x = 1;\n```", "const x = 1;"),
            ("use `npm install`", "use npm install"),
            ("***bold italic***", "bold italic"),
            ("**bold** and *italic*", "bold and italic"),
            ("**first\nsecond**", "**first\nsecond**"),
            ("___bold___ and __also bold__", "bold and also bold"),
            ("_italic_ and ~~deleted~~", "italic and deleted"),
            ("# Title\n## Subtitle", "Title\nSubtitle"),
            ("# Title\u{2028}## Subtitle", "Title\u{2028}Subtitle"),
            ("##\u{feff}Title", "Title"),
            ("##\u{0085}Title", "##\u{0085}Title"),
            ("![alt](https://img.png)", "[alt]"),
            (
                "[click here](https://example.com)",
                "click here (https://example.com)",
            ),
            ("> quoted text", "quoted text"),
            (
                "---\n* item 1\n- item 2\n1. item",
                "---\n- item 1\n- item 2\n1. item",
            ),
            ("a\n\n\n\nb", "a\n\nb"),
            ("  \n hello \n  ", "hello"),
        ] {
            assert_eq!(markdown_to_plain_text(input), expected, "input: {input:?}");
        }
    }

    #[test]
    fn image_magic_detection_handles_supported_formats_and_riff_decoys() {
        assert_eq!(detect_image_mime(b"\x89PNG").unwrap(), "image/png");
        assert_eq!(detect_image_mime(b"GIF89a").unwrap(), "image/gif");
        assert_eq!(
            detect_image_mime(b"RIFF\x1a\0\0\0WEBP").unwrap(),
            "image/webp"
        );
        assert_eq!(
            detect_image_mime(b"\xff\xd8\xffjpeg").unwrap(),
            "image/jpeg"
        );
        assert!(detect_image_mime(b"RIFF\x24\0\0\0WAVE").is_err());
        assert!(detect_image_mime(b"\0\0\0\0").is_err());
    }

    #[test]
    fn image_path_validation_checks_extensions_files_size_and_magic() {
        let temp = TestDirectory::under(&std::env::temp_dir());

        let valid = temp.0.join("photo.PNG");
        write_png(&valid);
        assert_eq!(
            validate_image_path(&valid, &[]).unwrap(),
            fs::canonicalize(&valid).unwrap()
        );

        for (name, signature) in [
            ("photo.jpg", &b"\xff\xd8\xffjpeg"[..]),
            ("photo.jpeg", &b"\xff\xd8\xffjpeg"[..]),
            ("animation.gif", &b"GIF89a"[..]),
            ("clip.webp", &b"RIFF\x1a\0\0\0WEBP"[..]),
        ] {
            let image = temp.0.join(name);
            fs::write(&image, signature).unwrap();
            assert!(validate_image_path(&image, &[]).is_ok(), "{name}");
        }

        let invalid_extension = temp.0.join("photo.txt");
        fs::write(&invalid_extension, b"text").unwrap();
        assert!(
            validate_image_path(&invalid_extension, &[])
                .unwrap_err()
                .contains("Image extension not allowed: .txt")
        );

        let missing = temp.0.join("missing.png");
        assert!(
            validate_image_path(&missing, &[])
                .unwrap_err()
                .contains("Image file not found")
        );

        let directory = temp.0.join("directory.png");
        fs::create_dir(&directory).unwrap();
        assert!(
            validate_image_path(&directory, &[])
                .unwrap_err()
                .contains("Not a regular file")
        );

        let huge = temp.0.join("huge.png");
        let huge_file = File::create(&huge).unwrap();
        huge_file.set_len(MAX_IMAGE_SIZE + 1).unwrap();
        assert!(
            validate_image_path(&huge, &[])
                .unwrap_err()
                .contains("Image too large")
        );

        let at_limit = temp.0.join("at-limit.png");
        write_png(&at_limit);
        OpenOptions::new()
            .write(true)
            .open(&at_limit)
            .unwrap()
            .set_len(MAX_IMAGE_SIZE)
            .unwrap();
        assert!(validate_image_path(&at_limit, &[]).is_ok());

        let mismatch = temp.0.join("actually-jpeg.png");
        fs::write(&mismatch, b"\xff\xd8\xffjpeg").unwrap();
        assert!(
            validate_image_path(&mismatch, &[])
                .unwrap_err()
                .contains("Image type mismatch: ext=.png expects image/png but got image/jpeg")
        );
    }

    #[test]
    fn image_paths_allow_tmp_and_caller_workspace_but_reject_sibling_prefixes() {
        let temp = TestDirectory::under(&std::env::temp_dir());
        let temp_image = temp.0.join("tmp.png");
        write_png(&temp_image);
        assert!(validate_image_path(&temp_image, &[]).is_ok());

        let home = PathBuf::from(std::env::var_os("HOME").expect("test environment has HOME"));
        let root = TestDirectory::under(&home);
        let workspace = root.0.join("workspace");
        let sibling = root.0.join("workspace-extra");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&sibling).unwrap();
        let allowed_image = workspace.join("allowed.png");
        let sibling_image = sibling.join("escape.png");
        write_png(&allowed_image);
        write_png(&sibling_image);

        assert!(validate_image_path(&allowed_image, std::slice::from_ref(&workspace)).is_ok());
        let error =
            validate_image_path(&sibling_image, std::slice::from_ref(&workspace)).unwrap_err();
        assert!(error.contains("Image path outside allowed directories"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let link = workspace.join("outside.png");
            symlink(&sibling_image, &link).unwrap();
            let error = validate_image_path(&link, std::slice::from_ref(&workspace)).unwrap_err();
            assert!(error.contains("Image path outside allowed directories"));
        }
    }

    struct CaptureHttp {
        requests: Mutex<Vec<ApiHttpRequest>>,
    }

    impl ApiHttpClient for CaptureHttp {
        fn execute<'a>(
            &'a self,
            request: ApiHttpRequest,
        ) -> ApiFuture<'a, Result<ApiHttpResponse, ApiTransportError>> {
            Box::pin(async move {
                self.requests.lock().unwrap().push(request);
                Ok(ApiHttpResponse::json(200, json!({ "ret": 0 })))
            })
        }
    }

    struct FakeRuntime;

    impl ApiRuntime for FakeRuntime {
        fn random_uin_bytes(&self) -> [u8; 4] {
            [1, 2, 3, 4]
        }

        fn sleep<'a>(&'a self, _duration: Duration) -> ApiFuture<'a, ()> {
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn send_text_posts_source_message_json_through_the_injected_transport() {
        let http = CaptureHttp {
            requests: Mutex::new(Vec::new()),
        };
        send_text_with_http(
            &http,
            &FakeRuntime,
            SendTextParams {
                to: "user-123",
                text: "**hello**\n\nworld",
                base_url: "https://api.example.test",
                token: "token-abc",
                context_token: "ctx-456",
            },
        )
        .await
        .unwrap();

        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].url,
            "https://api.example.test/ilink/bot/sendmessage"
        );
        assert_eq!(requests[0].method, "POST");
        assert_eq!(
            requests[0]
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
                .map(|(_, value)| value.as_str()),
            Some("Bearer token-abc")
        );

        let mut body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        let client_id = body["msg"]["client_id"].as_str().unwrap();
        assert_eq!(Uuid::parse_str(client_id).unwrap().to_string(), client_id);
        body["msg"]["client_id"] = json!("<uuid-v4>");
        assert_eq!(
            body,
            json!({
                "msg": {
                    "to_user_id": "user-123",
                    "from_user_id": "",
                    "client_id": "<uuid-v4>",
                    "message_type": 2,
                    "message_state": 2,
                    "context_token": "ctx-456",
                    "item_list": [{
                        "type": 1,
                        "text_item": { "text": "hello\n\nworld" }
                    }]
                },
                "base_info": { "channel_version": "2.1.3" }
            })
        );
    }
}
