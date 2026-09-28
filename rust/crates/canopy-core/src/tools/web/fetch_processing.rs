// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//!
//! Response classification, persistence, extraction, and metadata projection
//! for WebFetch. This consumes the bounded response produced by
//! `fetch_policy`; networking, HTML conversion, and side-query execution stay
//! at the host boundary.
//!
//! The converter is injectable so a host can use its chosen HTML-to-Markdown
//! implementation. If it is missing or fails, decoded response text is kept.
//! Side-query summarization and trusted-host query shortcuts are not ported
//! here; [`FetchProcessedResponse::raw_projection`] returns the content for a
//! caller to process or present directly.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::pdf::{default_pdf_range, extract_pdf_text, get_pdf_page_count, validate_pdf_size};
use crate::storage::Storage;
use crate::tool_response_finalizer::MAX_SESSION_TOOL_RESULT_BYTES;
use crate::tools::web::fetch_policy::FetchPolicyResponse;
use crate::utils::binary_content::{
    ExtensionSource, PersistBinaryResult, SniffedFileKind, format_byte_size,
    is_binary_content_type, looks_like_text, persist_binary_content, sniff_file_kind,
};
use thiserror::Error;
use uuid::Uuid;

/// Host-owned session counter for persisted tool-result bytes. The host should
/// share this with other tool-result writers so WebFetch binaries consume the
/// same 500MB session allowance.
pub trait FetchSessionByteBudget: Send {
    fn bytes_written(&self) -> u64;
    fn track_bytes(&mut self, delta: i64);
}

/// Injectable HTML-to-Markdown conversion boundary. Conversion errors are
/// recorded on the processed result and fall back to decoded raw text.
pub trait HtmlToMarkdownConverter: Send + Sync {
    fn convert(&self, html: &str) -> Result<String, String>;
}

impl<F> HtmlToMarkdownConverter for F
where
    F: Fn(&str) -> Result<String, String> + Send + Sync,
{
    fn convert(&self, html: &str) -> Result<String, String> {
        self(html)
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum FetchProcessingError {
    #[error("Request failed with status code {status} {status_text}")]
    HttpStatus { status: u16, status_text: String },
    #[error(
        "Fetched {body_bytes} bytes of binary content ({content_type}) but the session's tool-result disk budget is exhausted ({used_bytes} bytes used of {max_bytes})."
    )]
    SessionDiskBudget {
        body_bytes: usize,
        content_type: String,
        used_bytes: u64,
        max_bytes: u64,
    },
    #[error(
        "Fetched {body_bytes} bytes of binary content ({content_type}) but failed to save it: {reason}"
    )]
    PersistBinary {
        body_bytes: usize,
        content_type: String,
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedFetchBinary {
    pub filepath: PathBuf,
    pub size: usize,
    /// Sniffed MIME when available, otherwise the response Content-Type.
    pub mime_type: String,
}

/// Normalized response text plus the metadata needed for WebFetch's user and
/// model-facing result projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchProcessedResponse {
    pub requested_url: String,
    pub final_url: String,
    pub status: u16,
    pub status_text: String,
    pub content_type: String,
    pub byte_length: usize,
    /// Truncated Markdown/text/PDF text; empty for binary data without text.
    pub content: String,
    pub is_binary: bool,
    pub persisted: Option<PersistedFetchBinary>,
    /// Non-fatal extraction failure. The original binary remains persisted.
    pub pdf_extraction_error: Option<String>,
    /// Non-fatal conversion failure. `content` then contains decoded HTML.
    pub html_conversion_error: Option<String>,
    /// Classification output is retained for diagnostics and projections.
    pub sniffed_kind: SniffedFileKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchRawProjection {
    pub llm_content: String,
    pub return_display: String,
    pub result_file_paths: Option<Vec<String>>,
}

impl FetchProcessedResponse {
    /// Metadata prefix passed to the model, matching the TypeScript tool.
    pub fn metadata_header(&self) -> String {
        let url_line = if self.final_url != self.requested_url {
            format!("URL: {} (final: {})", self.requested_url, self.final_url)
        } else {
            format!("URL: {}", self.requested_url)
        };
        let status_text = if self.status_text.is_empty() {
            "OK"
        } else {
            &self.status_text
        };
        let content_type = if self.content_type.is_empty() {
            "unknown"
        } else {
            &self.content_type
        };
        format!(
            "{url_line}\nStatus: {} {status_text} | Content-Type: {content_type} | Size: {} bytes",
            self.status,
            format_decimal_grouped(self.byte_length)
        )
    }

    /// Optional persisted-file note appended to WebFetch's visible result.
    pub fn binary_note(&self) -> Option<String> {
        let persisted = self.persisted.as_ref()?;
        let mime = if persisted.mime_type.is_empty() {
            if self.content_type.is_empty() {
                "unknown"
            } else {
                &self.content_type
            }
        } else {
            &persisted.mime_type
        };
        let path = persisted.filepath.to_string_lossy();
        Some(format!(
            "\n\n[Binary content ({mime}, {}) saved to {path}.{}]",
            format_byte_size(persisted.size),
            crate::tools::web::web_fetch_read_hint(&path)
        ))
    }

    /// Raw result projection for hosts that have not wired the side-query
    /// stage. Binary files with no extractable text produce metadata and a
    /// file note without inventing a summary.
    pub fn raw_projection(&self) -> FetchRawProjection {
        let header = self.metadata_header();
        let binary_note = self.binary_note().unwrap_or_default();
        let body = if self.persisted.is_some() && self.content.is_empty() {
            "[No text could be extracted from this binary content.]"
        } else {
            &self.content
        };
        let llm_content = format!("{header}\n\n{body}{binary_note}");
        let persisted_path = self
            .persisted
            .as_ref()
            .map(|persisted| persisted.filepath.to_string_lossy().into_owned());
        let return_display = crate::tools::web::format_web_fetch_display(
            self.byte_length,
            self.status,
            &self.status_text,
            &self.requested_url,
            persisted_path.as_deref(),
        );
        FetchRawProjection {
            llm_content,
            return_display,
            result_file_paths: persisted_path.map(|path| vec![path]),
        }
    }
}

/// Classify a response using MIME, magic bytes, and recognized filenames.
/// Printable `application/octet-stream` with no recognized kind is treated as
/// text, while NUL/invalid UTF-8 and signature-matched files remain binary.
pub fn classify_fetch_body(
    body: &[u8],
    content_type: &str,
    content_disposition: &str,
    final_url: &str,
) -> (bool, SniffedFileKind) {
    let sniffed = sniff_file_kind(body, content_type, content_disposition, final_url);
    let mut is_binary = is_binary_content_type(content_type)
        || sniffed.magic_matched
        || (content_type.is_empty()
            && sniffed.extension_source == ExtensionSource::Name
            && sniffed.extension != "svg");

    let normalized_content_type = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if is_binary
        && normalized_content_type == "application/octet-stream"
        && !sniffed.magic_matched
        && sniffed.extension_source == ExtensionSource::Fallback
        && looks_like_text(body)
    {
        is_binary = false;
    }

    (is_binary, sniffed)
}

/// Process a successful fetch-policy response. Non-success status and binary
/// persistence/budget failures are fatal; PDF and HTML conversion failures
/// retain the fetched bytes/text and fall back to an empty/raw body.
pub async fn process_fetch_response(
    response: FetchPolicyResponse,
    requested_url: impl Into<String>,
    storage: &Storage,
    byte_budget: &mut dyn FetchSessionByteBudget,
    html_converter: Option<&dyn HtmlToMarkdownConverter>,
) -> Result<FetchProcessedResponse, FetchProcessingError> {
    if response.status < 200 || response.status >= 300 {
        return Err(FetchProcessingError::HttpStatus {
            status: response.status,
            status_text: response.status_text,
        });
    }

    let requested_url = requested_url.into();
    let content_type = response.content_type;
    let byte_length = response.body.len();
    let (is_binary, sniffed_kind) = classify_fetch_body(
        &response.body,
        &content_type,
        &response.content_disposition,
        &response.final_url,
    );

    let mut persisted = None;
    let mut pdf_extraction_error = None;
    let mut html_conversion_error = None;
    let content;

    if is_binary {
        let used_bytes = byte_budget.bytes_written();
        if used_bytes.saturating_add(byte_length as u64) > MAX_SESSION_TOOL_RESULT_BYTES {
            return Err(FetchProcessingError::SessionDiskBudget {
                body_bytes: byte_length,
                content_type: if content_type.is_empty() {
                    "unknown content type".to_owned()
                } else {
                    content_type.clone()
                },
                used_bytes,
                max_bytes: MAX_SESSION_TOOL_RESULT_BYTES,
            });
        }

        // Reserve synchronously before the async write, and roll the charge
        // back if persistence fails. Successful PDF text extraction does not
        // affect the file's disk accounting.
        byte_budget.track_bytes(byte_length as i64);
        let persist_id = unique_persist_id();
        let persisted_result = persist_binary_content(
            &response.body,
            &sniffed_kind.extension,
            storage.get_tool_results_dir(),
            &persist_id,
        )
        .await;
        let (filepath, size) = match persisted_result {
            PersistBinaryResult::Written { filepath, size, .. } => (filepath, size),
            PersistBinaryResult::Error(reason) => {
                byte_budget.track_bytes(-(byte_length as i64));
                return Err(FetchProcessingError::PersistBinary {
                    body_bytes: byte_length,
                    content_type: if content_type.is_empty() {
                        "unknown content type".to_owned()
                    } else {
                        content_type.clone()
                    },
                    reason,
                });
            }
        };

        let mime_type = if sniffed_kind.mime_type.is_empty() {
            content_type.clone()
        } else {
            sniffed_kind.mime_type.clone()
        };
        if sniffed_kind.extension == "pdf" {
            match extract_fetched_pdf_text(&filepath, size as u64).await {
                Ok(text) if !text.trim().is_empty() => {
                    content = crate::tools::web::truncate_web_fetch_text(&text);
                }
                Ok(_) => content = String::new(),
                Err(error) => {
                    pdf_extraction_error = Some(error);
                    content = String::new();
                }
            }
        } else {
            content = String::new();
        }
        persisted = Some(PersistedFetchBinary {
            filepath,
            size,
            mime_type,
        });
    } else {
        let decoded = String::from_utf8_lossy(&response.body).into_owned();
        if content_type.contains("text/html") {
            match html_converter {
                Some(converter) => match converter.convert(&decoded) {
                    Ok(markdown) => {
                        content = crate::tools::web::truncate_web_fetch_text(&markdown);
                    }
                    Err(error) => {
                        html_conversion_error = Some(error);
                        content = crate::tools::web::truncate_web_fetch_text(&decoded);
                    }
                },
                None => {
                    html_conversion_error = Some(
                        "HTML-to-Markdown conversion is not configured; returning raw text"
                            .to_owned(),
                    );
                    content = crate::tools::web::truncate_web_fetch_text(&decoded);
                }
            }
        } else {
            content = crate::tools::web::truncate_web_fetch_text(&decoded);
        }
    }

    Ok(FetchProcessedResponse {
        requested_url,
        final_url: response.final_url,
        status: response.status,
        status_text: response.status_text,
        content_type,
        byte_length,
        content,
        is_binary,
        persisted,
        pdf_extraction_error,
        html_conversion_error,
        sniffed_kind,
    })
}

async fn extract_fetched_pdf_text(path: &Path, file_size: u64) -> Result<String, String> {
    validate_pdf_size(file_size, false)?;
    let page_count = get_pdf_page_count(path).await;
    let display_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("document.pdf");
    // The shared Rust extractor uses bounded page ranges. This matches the
    // read_file safety limits; WebFetch's TypeScript extractor can attempt
    // larger documents, so PDFs outside these bounds fall back to the saved
    // file note and are left for a dedicated PDF reader.
    let range = default_pdf_range(page_count, file_size, display_name)?;
    extract_pdf_text(path, range).await
}

fn unique_persist_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("webfetch-{millis}-{}", Uuid::new_v4().simple())
}

fn format_decimal_grouped(value: usize) -> String {
    let digits = value.to_string();
    let first_group_len = match digits.len() % 3 {
        0 => 3,
        remainder => remainder,
    };
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    grouped.push_str(&digits[..first_group_len]);
    for chunk in digits.as_bytes()[first_group_len..].chunks(3) {
        grouped.push(',');
        grouped.push_str(std::str::from_utf8(chunk).unwrap_or_default());
    }
    grouped
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[derive(Default)]
    struct MemoryBudget {
        written: u64,
    }

    impl FetchSessionByteBudget for MemoryBudget {
        fn bytes_written(&self) -> u64 {
            self.written
        }

        fn track_bytes(&mut self, delta: i64) {
            if delta >= 0 {
                self.written = self.written.saturating_add(delta as u64);
            } else {
                self.written = self.written.saturating_sub(delta.unsigned_abs());
            }
        }
    }

    fn response(content_type: &str, body: &[u8], final_url: &str) -> FetchPolicyResponse {
        FetchPolicyResponse {
            status: 200,
            status_text: "OK".to_owned(),
            content_type: content_type.to_owned(),
            content_disposition: String::new(),
            body: body.to_vec(),
            final_url: final_url.to_owned(),
        }
    }

    fn temp_storage() -> (Storage, PathBuf) {
        let root = std::env::temp_dir().join(format!("web-fetch-processing-{}", Uuid::new_v4()));
        let storage = Storage::with_runtime_base_dir(root.join("project"), &root);
        (storage, root)
    }

    #[test]
    fn classifies_mislabelled_pdf_and_printable_octet_stream() {
        let pdf_bytes = b"%PDF-1.4\nnot a complete PDF";
        let (pdf_binary, pdf_kind) = classify_fetch_body(
            pdf_bytes,
            "application/octet-stream",
            "",
            "https://example.test/document",
        );
        assert!(pdf_binary);
        assert_eq!(pdf_kind.extension, "pdf");
        assert!(pdf_kind.magic_matched);

        let text_bytes = b"commit 1a2b\nAuthor: engineer\n";
        let (text_binary, text_kind) = classify_fetch_body(
            text_bytes,
            "application/octet-stream",
            "",
            "https://example.test/commit",
        );
        assert!(!text_binary);
        assert_eq!(text_kind.extension, "bin");
        assert_eq!(text_kind.extension_source, ExtensionSource::Fallback);
    }

    #[test]
    fn recognized_headerless_binary_filename_is_persistable() {
        let (binary, kind) = classify_fetch_body(
            b"opaque bytes",
            "",
            "attachment; filename=archive.zip",
            "https://example.test/download",
        );
        assert!(binary);
        assert_eq!(kind.extension, "zip");
        assert_eq!(kind.extension_source, ExtensionSource::Name);
    }

    #[tokio::test]
    async fn html_converter_is_injectable_and_failure_falls_back_to_raw_text() {
        let (storage, root) = temp_storage();
        let mut budget = MemoryBudget::default();
        let converter =
            |_html: &str| -> Result<String, String> { Err("converter unavailable".to_owned()) };
        let processed = process_fetch_response(
            response(
                "text/html; charset=utf-8",
                b"<html><body><h1>Raw title</h1></body></html>",
                "https://cdn.example.test/page",
            ),
            "https://example.test/page",
            &storage,
            &mut budget,
            Some(&converter),
        )
        .await
        .unwrap();

        assert!(processed.content.contains("<h1>Raw title</h1>"));
        assert_eq!(
            processed.html_conversion_error.as_deref(),
            Some("converter unavailable")
        );
        assert!(processed.persisted.is_none());
        let projection = processed.raw_projection();
        assert!(projection.llm_content.contains("Status: 200 OK"));
        assert!(
            projection
                .llm_content
                .contains("URL: https://example.test/page (final: https://cdn.example.test/page)")
        );
        assert!(projection.return_display.contains("from example.test"));
        assert_eq!(budget.bytes_written(), 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn persists_pdf_and_keeps_binary_when_text_extraction_fails() {
        let (storage, root) = temp_storage();
        let mut budget = MemoryBudget::default();
        let body = b"%PDF-1.4\nnot a parseable PDF";
        let processed = process_fetch_response(
            response(
                "application/octet-stream",
                body,
                "https://example.test/file",
            ),
            "https://example.test/file",
            &storage,
            &mut budget,
            None,
        )
        .await
        .unwrap();

        let persisted = processed.persisted.as_ref().unwrap();
        assert_eq!(
            persisted.filepath.extension().and_then(|ext| ext.to_str()),
            Some("pdf")
        );
        assert_eq!(fs::read(&persisted.filepath).unwrap(), body);
        assert_eq!(processed.content, "");
        assert!(processed.pdf_extraction_error.is_some());
        assert_eq!(budget.bytes_written(), body.len() as u64);
        let projection = processed.raw_projection();
        assert!(
            projection
                .llm_content
                .contains("No text could be extracted")
        );
        assert!(projection.llm_content.contains("reads PDFs natively"));
        assert_eq!(projection.result_file_paths.unwrap().len(), 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn rejects_binary_when_session_disk_budget_is_exhausted() {
        let (storage, root) = temp_storage();
        let mut budget = MemoryBudget {
            written: MAX_SESSION_TOOL_RESULT_BYTES,
        };
        let result = process_fetch_response(
            response(
                "application/pdf",
                b"%PDF-1.4\n",
                "https://example.test/file.pdf",
            ),
            "https://example.test/file.pdf",
            &storage,
            &mut budget,
            None,
        )
        .await;

        assert!(matches!(
            result,
            Err(FetchProcessingError::SessionDiskBudget { .. })
        ));
        assert_eq!(budget.bytes_written(), MAX_SESSION_TOOL_RESULT_BYTES);
        assert!(!storage.get_tool_results_dir().exists());
        fs::remove_dir_all(root).unwrap_or_default();
    }

    #[tokio::test]
    async fn releases_reserved_session_bytes_if_binary_write_fails() {
        let (storage, root) = temp_storage();
        let blocking_path = storage.get_project_temp_dir();
        fs::create_dir_all(blocking_path.parent().unwrap()).unwrap();
        fs::write(blocking_path, "blocking directory creation").unwrap();
        let mut budget = MemoryBudget::default();
        let body = b"opaque binary";
        let result = process_fetch_response(
            response(
                "application/octet-stream",
                body,
                "https://example.test/download.bin",
            ),
            "https://example.test/download.bin",
            &storage,
            &mut budget,
            None,
        )
        .await;

        assert!(matches!(
            result,
            Err(FetchProcessingError::PersistBinary { .. })
        ));
        assert_eq!(budget.bytes_written(), 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn non_success_status_is_a_processing_error() {
        let (storage, root) = temp_storage();
        let mut not_found = response("text/plain", b"missing", "https://example.test/missing");
        not_found.status = 404;
        not_found.status_text = "Not Found".to_owned();
        let mut budget = MemoryBudget::default();

        let result = process_fetch_response(
            not_found,
            "https://example.test/missing",
            &storage,
            &mut budget,
            None,
        )
        .await;

        assert_eq!(
            result.unwrap_err(),
            FetchProcessingError::HttpStatus {
                status: 404,
                status_text: "Not Found".to_owned(),
            }
        );
        fs::remove_dir_all(root).unwrap_or_default();
    }
}
