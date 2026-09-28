// Copyright 2025 Google LLC
// SPDX-License-Identifier: Apache-2.0
//
//! MIME hints, filename extensions, and magic-byte sniffing for fetched files.
//!
//! This mirrors `packages/core/src/utils/binary-content.ts`. Magic bytes take
//! precedence over names and MIME declarations, while recognized names take
//! precedence over the MIME map when the body has no known signature.

use regex::Regex;
use reqwest::Url;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const MIME_EXTENSIONS: &[(&str, &str)] = &[
    ("application/pdf", "pdf"),
    ("application/zip", "zip"),
    (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "docx",
    ),
    (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "xlsx",
    ),
    (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "pptx",
    ),
    ("application/msword", "doc"),
    ("application/vnd.ms-excel", "xls"),
    ("application/vnd.ms-powerpoint", "ppt"),
    ("application/gzip", "gz"),
    ("application/x-gzip", "gz"),
    ("application/x-tar", "tar"),
    ("application/x-7z-compressed", "7z"),
    ("application/x-rar-compressed", "rar"),
    ("application/vnd.rar", "rar"),
    ("application/wasm", "wasm"),
    ("application/java-archive", "jar"),
    ("audio/mpeg", "mp3"),
    ("audio/wav", "wav"),
    ("audio/ogg", "ogg"),
    ("video/mp4", "mp4"),
    ("video/webm", "webm"),
    ("image/png", "png"),
    ("image/jpeg", "jpg"),
    ("image/gif", "gif"),
    ("image/webp", "webp"),
    ("image/svg+xml", "svg"),
];

const KNOWN_EXTENSIONS: &[&str] = &[
    "pdf", "zip", "docx", "xlsx", "pptx", "doc", "xls", "ppt", "gz", "tar", "7z", "rar", "wasm",
    "jar", "mp3", "wav", "ogg", "mp4", "webm", "png", "jpg", "gif", "webp", "svg", "jpeg", "bin",
];

/// True when the declared content type normally carries non-text bytes.
/// Unknown `application/*` types default to text; magic-byte sniffing handles
/// mislabeled binary bodies separately.
pub fn is_binary_content_type(content_type: &str) -> bool {
    if content_type.is_empty() {
        return false;
    }
    let mime_type = normalized_mime_type(content_type);
    if mime_type.starts_with("text/") || mime_type.ends_with("+json") || mime_type.ends_with("+xml")
    {
        return false;
    }
    if ["image/", "audio/", "video/", "font/"]
        .iter()
        .any(|prefix| mime_type.starts_with(prefix))
    {
        return true;
    }
    if [
        "application/pdf",
        "application/zip",
        "application/gzip",
        "application/x-gzip",
        "application/octet-stream",
        "application/msword",
        "application/vnd.ms-excel",
        "application/vnd.ms-powerpoint",
        "application/x-tar",
        "application/x-7z-compressed",
        "application/x-rar-compressed",
        "application/vnd.rar",
        "application/wasm",
        "application/java-archive",
    ]
    .contains(&mime_type.as_str())
    {
        return true;
    }
    mime_type.starts_with("application/vnd.openxmlformats")
        || mime_type.starts_with("application/vnd.oasis.opendocument")
}

/// Choose the extension in the source MIME map, or `bin` when unknown.
pub fn extension_for_mime_type(mime_type: Option<&str>) -> &'static str {
    let Some(mime_type) = mime_type.filter(|value| !value.is_empty()) else {
        return "bin";
    };
    let normalized = normalized_mime_type(mime_type);
    MIME_EXTENSIONS
        .iter()
        .find_map(|(mime, extension)| (*mime == normalized).then_some(*extension))
        .unwrap_or("bin")
}

fn normalized_mime_type(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

/// The source used to determine the effective extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExtensionSource {
    Magic,
    Name,
    Mime,
    Fallback,
}

/// Effective file kind inferred from fetched bytes and response metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SniffedFileKind {
    /// Effective extension without a leading dot.
    pub extension: String,
    /// Effective display MIME type; may be the raw served content type.
    pub mime_type: String,
    /// Whether the bytes identified an unambiguous format.
    pub magic_matched: bool,
    pub extension_source: ExtensionSource,
}

fn extension_from_filename(name: Option<&str>) -> Option<String> {
    let name = name?;
    if name.is_empty() {
        return None;
    }
    let extension = name.rsplit('.').next()?.trim();
    if extension.is_empty() {
        return None;
    }
    let normalized = extension.to_ascii_lowercase();
    // TypeScript compares its already-lowercased extension with the original
    // filename. Thus a bare `PDF` is recognized while a bare `pdf` is not.
    if normalized == name {
        return None;
    }
    KNOWN_EXTENSIONS
        .contains(&normalized.as_str())
        .then_some(normalized)
}

fn filename_from_content_disposition(value: &str) -> Option<String> {
    static STAR: OnceLock<Regex> = OnceLock::new();
    static PLAIN: OnceLock<Regex> = OnceLock::new();
    let star = STAR.get_or_init(|| {
        Regex::new(r"(?i)filename\*\s*=\s*[^']*'[^']*'([^;]+)")
            .expect("constant filename* regex is valid")
    });
    let plain = PLAIN.get_or_init(|| {
        Regex::new(r#"(?i)filename\s*=\s*"?([^";]+)"?"#).expect("constant filename regex is valid")
    });

    if let Some(captured) = star
        .captures(value)
        .and_then(|captures| captures.get(1))
        .map(|matched| matched.as_str())
    {
        let captured = captured.trim();
        // The source checks the raw regex capture before trimming, so a
        // whitespace-only filename* resolves empty but still outranks filename=.
        return Some(percent_decode_uri_component(captured).unwrap_or_else(|| captured.to_owned()));
    }
    plain
        .captures(value)
        .and_then(|captures| captures.get(1))
        .map(|matched| matched.as_str().trim().to_owned())
}

/// Decode the URI component syntax used by `decodeURIComponent`; unlike form
/// decoding, `+` remains a literal plus. Malformed escapes or invalid UTF-8
/// return `None`, so callers retain the original filename parameter.
fn percent_decode_uri_component(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = hex_value(*bytes.get(index + 1)?)?;
            let low = hex_value(*bytes.get(index + 2)?)?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn extension_from_url(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let filename = parsed.path().rsplit('/').next()?;
    extension_from_filename(Some(filename))
}

/// Infer a fetched file's format using magic bytes, response names, then MIME.
///
/// Priority: magic bytes → Content-Disposition filename → URL path extension
/// → Content-Type map → `bin` fallback. ZIP containers are refined to Office
/// OpenXML/JAR extensions when a recognized filename or MIME hint is present.
pub fn sniff_file_kind(
    bytes: &[u8],
    content_type: &str,
    content_disposition: &str,
    url: &str,
) -> SniffedFileKind {
    let disposition_extension = filename_from_content_disposition(content_disposition)
        .as_deref()
        .and_then(|filename| extension_from_filename(Some(filename)));
    let url_extension = extension_from_url(url);

    if bytes.starts_with(b"%PDF-") {
        return magic_kind("pdf", "application/pdf");
    }
    if bytes.starts_with(&[0x50, 0x4b, 0x03, 0x04]) {
        let mime_extension = extension_for_mime_type(Some(content_type));
        let zip_hint = disposition_extension
            .as_deref()
            .or(url_extension.as_deref())
            .unwrap_or(mime_extension);
        let extension = if ["docx", "xlsx", "pptx", "jar"].contains(&zip_hint) {
            zip_hint
        } else {
            "zip"
        };
        return SniffedFileKind {
            extension: extension.to_owned(),
            mime_type: if content_type.is_empty() {
                "application/zip".to_owned()
            } else {
                content_type.to_owned()
            },
            magic_matched: true,
            extension_source: ExtensionSource::Magic,
        };
    }
    if bytes.starts_with(&[0x1f, 0x8b]) {
        return magic_kind("gz", "application/gzip");
    }
    if bytes.len() >= 7 && bytes.starts_with(b"Rar!") && bytes[4] == 0x1a && bytes[5] == 0x07 {
        return magic_kind("rar", "application/vnd.rar");
    }
    if bytes.len() >= 8 && bytes[0] == 0x89 && bytes.get(1..4) == Some(b"PNG") {
        return magic_kind("png", "image/png");
    }
    if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        return magic_kind("jpg", "image/jpeg");
    }
    if bytes.starts_with(b"GIF8") {
        return magic_kind("gif", "image/gif");
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        return magic_kind("webp", "image/webp");
    }

    if let Some(extension) = disposition_extension.or(url_extension) {
        return SniffedFileKind {
            extension,
            mime_type: content_type.to_owned(),
            magic_matched: false,
            extension_source: ExtensionSource::Name,
        };
    }
    let mime_extension = extension_for_mime_type(Some(content_type));
    SniffedFileKind {
        extension: mime_extension.to_owned(),
        mime_type: content_type.to_owned(),
        magic_matched: false,
        extension_source: if mime_extension != "bin" {
            ExtensionSource::Mime
        } else {
            ExtensionSource::Fallback
        },
    }
}

fn magic_kind(extension: &str, mime_type: &str) -> SniffedFileKind {
    SniffedFileKind {
        extension: extension.to_owned(),
        mime_type: mime_type.to_owned(),
        magic_matched: true,
        extension_source: ExtensionSource::Magic,
    }
}

/// Heuristic for identifying mislabeled binary bodies from their first 8192
/// bytes. NUL or more than two UTF-8 replacement characters means binary.
pub fn looks_like_text(bytes: &[u8]) -> bool {
    let window = &bytes[..bytes.len().min(8192)];
    if window.contains(&0) {
        return false;
    }
    String::from_utf8_lossy(window)
        .chars()
        .filter(|character| *character == '\u{fffd}')
        .count()
        <= 2
}

/// Result of persisting fetched bytes under an extension-derived filename.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PersistBinaryResult {
    Written {
        filepath: PathBuf,
        size: usize,
        ext: String,
    },
    Error(String),
}

/// Write raw bytes beneath `dir`, creating private directories and a private
/// file on Unix. Existing file permissions remain unchanged, matching
/// `writeFile(..., { mode: 0o600 })` semantics.
pub async fn persist_binary_content(
    bytes: &[u8],
    extension: &str,
    dir: impl AsRef<Path>,
    persist_id: &str,
) -> PersistBinaryResult {
    let dir = dir.as_ref();
    let filepath = dir.join(format!("{persist_id}.{extension}"));
    let write_result = async {
        create_private_dir(dir)?;
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&filepath).await?;
        use tokio::io::AsyncWriteExt;
        file.write_all(bytes).await?;
        // Tokio may defer buffered filesystem writes until flush/drop. The
        // source writeFile promise resolves only after bytes are written, so
        // await flush before reporting the persistence operation complete.
        file.flush().await?;
        Ok::<(), io::Error>(())
    }
    .await;

    match write_result {
        Ok(()) => PersistBinaryResult::Written {
            filepath,
            size: bytes.len(),
            ext: extension.to_owned(),
        },
        Err(error) => PersistBinaryResult::Error(error.to_string()),
    }
}

fn create_private_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700).create(dir)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir_all(dir)
    }
}

/// Format bytes using the source helper's decimal-unit thresholds and one
/// fractional digit, omitting a trailing `.0`.
pub fn format_byte_size(size_in_bytes: usize) -> String {
    let kb = size_in_bytes as f64 / 1024.0;
    if kb < 1.0 {
        return format!("{size_in_bytes} bytes");
    }
    if kb < 1024.0 {
        return format_scaled(kb, "KB");
    }
    let mb = kb / 1024.0;
    if mb < 1024.0 {
        return format_scaled(mb, "MB");
    }
    format_scaled(mb / 1024.0, "GB")
}

fn format_scaled(value: f64, unit: &str) -> String {
    let formatted = format!("{value:.1}");
    format!(
        "{}{unit}",
        formatted.strip_suffix(".0").unwrap_or(&formatted)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const TEXT: &[u8] = b"plain old text";
    const URL: &str = "https://example.com/files/download";

    #[test]
    fn content_type_classification_matches_mime_rules() {
        for mime in [
            "text/html",
            "text/plain; charset=utf-8",
            "application/json",
            "application/vnd.api+json",
            "application/xml",
            "image/svg+xml",
            "application/javascript",
            "application/yaml",
            "application/x-ndjson",
            "",
        ] {
            assert!(!is_binary_content_type(mime), "{mime}");
        }
        for mime in [
            "application/wasm",
            "application/vnd.ms-powerpoint",
            "application/x-tar",
            "application/x-rar-compressed",
            "application/vnd.rar",
            "application/java-archive",
            "font/woff2",
            "application/pdf",
            "application/zip",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/octet-stream",
            "image/png",
            "audio/mpeg",
            "video/mp4",
            "application/vnd.oasis.opendocument.text",
        ] {
            assert!(is_binary_content_type(mime), "{mime}");
        }
    }

    #[test]
    fn extension_mapping_strips_parameters_and_falls_back_to_bin() {
        assert_eq!(
            extension_for_mime_type(Some("application/pdf; charset=binary")),
            "pdf"
        );
        assert_eq!(extension_for_mime_type(Some("image/jpeg")), "jpg");
        assert_eq!(
            extension_for_mime_type(Some("application/x-7z-compressed")),
            "7z"
        );
        assert_eq!(extension_for_mime_type(Some("who/knows")), "bin");
        assert_eq!(extension_for_mime_type(None), "bin");
    }

    #[test]
    fn sniffing_prioritizes_magic_then_disposition_then_url_then_mime() {
        let pdf = sniff_file_kind(b"%PDF-1.4 junk", "application/octet-stream", "", URL);
        assert_eq!(pdf.extension, "pdf");
        assert_eq!(pdf.mime_type, "application/pdf");
        assert!(pdf.magic_matched);
        assert_eq!(pdf.extension_source, ExtensionSource::Magic);

        let zip = [0x50, 0x4b, 0x03, 0x04, 0, 0];
        let by_disposition = sniff_file_kind(
            &zip,
            "application/octet-stream",
            "attachment; filename=\"report.xlsx\"",
            "https://example.com/deck.pptx",
        );
        assert_eq!(by_disposition.extension, "xlsx");
        assert_eq!(by_disposition.extension_source, ExtensionSource::Magic);

        let by_url = sniff_file_kind(
            &zip,
            "application/octet-stream",
            "",
            "https://example.com/deck.pptx",
        );
        assert_eq!(by_url.extension, "pptx");
        assert_eq!(sniff_file_kind(&zip, "", "", URL).extension, "zip");
        assert_eq!(
            sniff_file_kind(
                &zip,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "",
                URL,
            )
            .extension,
            "xlsx"
        );
        assert_eq!(
            sniff_file_kind(&zip, "application/java-archive", "", URL).extension,
            "jar"
        );
    }

    #[test]
    fn sniffing_recognizes_gzip_rar_and_image_signatures() {
        assert_eq!(
            sniff_file_kind(&[0x1f, 0x8b, 0x08], "", "", URL).extension,
            "gz"
        );
        for bytes in [
            vec![0x52, 0x61, 0x72, 0x21, 0x1a, 0x07, 0x00],
            vec![0x52, 0x61, 0x72, 0x21, 0x1a, 0x07, 0x01, 0x00],
        ] {
            let kind = sniff_file_kind(&bytes, "application/octet-stream", "", URL);
            assert_eq!(kind.extension, "rar");
            assert_eq!(kind.mime_type, "application/vnd.rar");
        }
        let cases: &[(&[u8], &str, &str)] = &[
            (
                &[0x89, b'P', b'N', b'G', 13, 10, 26, 10],
                "png",
                "image/png",
            ),
            (&[0xff, 0xd8, 0xff, 0xe0], "jpg", "image/jpeg"),
            (b"GIF89a....", "gif", "image/gif"),
            (b"RIFFxxxxWEBPVP8 ", "webp", "image/webp"),
        ];
        for (bytes, extension, mime) in cases {
            let kind = sniff_file_kind(bytes, "", "", URL);
            assert_eq!(kind.extension, *extension);
            assert_eq!(kind.mime_type, *mime);
            assert!(kind.magic_matched);
        }
    }

    #[test]
    fn sniffing_decodes_filename_star_and_ignores_unknown_extensions() {
        let decoded = sniff_file_kind(
            TEXT,
            "application/octet-stream",
            "attachment; filename*=UTF-8''r13031cp.pdf",
            URL,
        );
        assert_eq!(decoded.extension, "pdf");
        assert_eq!(decoded.extension_source, ExtensionSource::Name);
        let language = sniff_file_kind(TEXT, "", "attachment; filename*=UTF-8'en'report.xlsx", URL);
        assert_eq!(language.extension, "xlsx");

        let url_name = sniff_file_kind(TEXT, "", "", "https://example.com/archive.tar");
        assert_eq!(url_name.extension, "tar");
        assert_eq!(url_name.extension_source, ExtensionSource::Name);
        assert_eq!(
            sniff_file_kind(
                TEXT,
                "application/octet-stream",
                "",
                "https://example.com/audio/track.mp3?sig=abc",
            )
            .extension,
            "mp3"
        );
        assert_eq!(
            sniff_file_kind(TEXT, "application/pdf", "", URL).extension_source,
            ExtensionSource::Mime
        );
        assert_eq!(
            sniff_file_kind(TEXT, "who/knows", "", URL).extension_source,
            ExtensionSource::Fallback
        );
        assert_eq!(
            sniff_file_kind(TEXT, "", "attachment; filename=\"script.exe\"", URL).extension,
            "bin"
        );
        assert_eq!(
            sniff_file_kind(TEXT, "", "attachment; filename*=UTF-8''bad%ZZ.pdf", URL).extension,
            "pdf"
        );
        assert_eq!(
            sniff_file_kind(
                TEXT,
                "",
                "attachment; filename*=UTF-8''   ; filename=\"report.pdf\"",
                URL
            )
            .extension,
            "bin"
        );
        assert_eq!(
            sniff_file_kind(TEXT, "", "attachment; filename=\"PDF\"", URL).extension_source,
            ExtensionSource::Name
        );
    }

    #[test]
    fn text_heuristic_checks_nul_and_replacement_count_in_prefix() {
        assert!(looks_like_text("plain text\nwith 中文 too\n".as_bytes()));
        assert!(!looks_like_text(&[0x68, 0x00, 0x69]));
        assert!(!looks_like_text(&[0x81; 64]));
        assert!(looks_like_text(&[0x81; 2]));
        let mut body_after_window = vec![b'a'; 8192];
        body_after_window.push(0);
        assert!(looks_like_text(&body_after_window));
    }

    #[test]
    fn formats_byte_sizes_with_source_thresholds() {
        for (bytes, expected) in [
            (0, "0 bytes"),
            (512, "512 bytes"),
            (1024, "1KB"),
            (1536, "1.5KB"),
            (10 * 1024 * 1024, "10MB"),
            (3 * 1024 * 1024 * 1024usize, "3GB"),
        ] {
            assert_eq!(format_byte_size(bytes), expected);
        }
    }

    #[tokio::test]
    async fn persistence_writes_bytes_with_private_mode_and_creates_directories() {
        let root = std::env::temp_dir().join(format!("binary-content-{}", uuid::Uuid::new_v4()));
        let dir = root.join("nested").join("files");
        let bytes = [0x25, 0x50, 0x44, 0x46, 0x00, 0xff];
        let result = persist_binary_content(&bytes, "pdf", &dir, "webfetch-test-1").await;
        let path = dir.join("webfetch-test-1.pdf");
        assert_eq!(
            result,
            PersistBinaryResult::Written {
                filepath: path.clone(),
                size: bytes.len(),
                ext: "pdf".to_owned(),
            }
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn persistence_returns_errors_for_directory_path_collisions() {
        let root = std::env::temp_dir().join(format!("binary-content-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let occupied = root.join("occupied");
        fs::write(&occupied, b"not a directory").unwrap();
        let result = persist_binary_content(b"x", "bin", &occupied, "id").await;
        assert!(matches!(result, PersistBinaryResult::Error(_)));
        fs::remove_dir_all(root).unwrap();
    }
}
