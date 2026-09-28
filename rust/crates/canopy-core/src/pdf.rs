//! Bounded PDF text extraction used by `read_file`.

use std::ffi::OsString;
use std::io;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::time::timeout;

use crate::utils::request_tokenizer::estimate_text_tokens;

pub const MAX_PDF_PAGES_PER_READ: usize = 20;
pub const MAX_PDF_FULL_TEXT_PAGES: u64 = 10;
pub const MAX_PDF_FULL_TEXT_SIZE_BYTES: u64 = 100 * 1024 * 1024;
pub const MAX_PDF_PAGED_TEXT_SIZE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_PDF_PAGE_NUMBER: usize = 1_000_000;
const MAX_PDF_TEXT_OUTPUT_UTF16: usize = 100_000;
const MAX_PDF_TEXT_OUTPUT_BYTES: usize = MAX_PDF_TEXT_OUTPUT_UTF16 * 4;
const MAX_PDF_STDERR_BYTES: usize = 32 * 1024;
const PDF_TEXT_TIMEOUT: Duration = Duration::from_secs(30);
const PDF_INFO_TIMEOUT: Duration = Duration::from_secs(10);
const PDF_PAGE_COUNT_SIZE_HEURISTIC_BYTES: u64 = 100 * 1024;
const PDF_INFO_OUTPUT_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PdfPageRange {
    pub first_page: usize,
    pub last_page: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PdfPageRangeError {
    Invalid,
    OpenEnded,
    TooManyPages,
}

/// Parse the page forms accepted by Canopy. An open-ended range is returned
/// separately so callers can retain Canopy's specific validation message.
pub fn parse_pdf_page_range(pages: &str) -> Result<PdfPageRange, PdfPageRangeError> {
    let trimmed = pages.trim();
    if trimmed.is_empty() {
        return Err(PdfPageRangeError::Invalid);
    }

    let (first_page, last_page) = if let Some((first, last)) = trimmed.split_once('-') {
        if last.contains('-') {
            return Err(PdfPageRangeError::Invalid);
        }
        let first_page = parse_page_number(first.trim())?;
        if last.trim().is_empty() {
            return Err(PdfPageRangeError::OpenEnded);
        }
        let last_page = parse_page_number(last.trim())?;
        if last_page < first_page {
            return Err(PdfPageRangeError::Invalid);
        }
        (first_page, last_page)
    } else {
        let page = parse_page_number(trimmed)?;
        (page, page)
    };

    if last_page - first_page + 1 > MAX_PDF_PAGES_PER_READ {
        return Err(PdfPageRangeError::TooManyPages);
    }
    Ok(PdfPageRange {
        first_page,
        last_page,
    })
}

fn parse_page_number(value: &str) -> Result<usize, PdfPageRangeError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(PdfPageRangeError::Invalid);
    }
    value
        .parse::<usize>()
        .ok()
        .filter(|page| (1..=MAX_PDF_PAGE_NUMBER).contains(page))
        .ok_or(PdfPageRangeError::Invalid)
}

pub async fn extract_pdf_text(path: &Path, range: PdfPageRange) -> Result<String, String> {
    extract_pdf_text_with_command(path, range, "pdftotext", PDF_TEXT_TIMEOUT).await
}

pub async fn get_pdf_page_count(path: &Path) -> Option<u64> {
    let args = [OsString::from("--"), path.as_os_str().to_owned()];
    let output = run_bounded_command(
        "pdfinfo",
        &args,
        PDF_INFO_TIMEOUT,
        PDF_INFO_OUTPUT_BYTES,
        MAX_PDF_STDERR_BYTES,
    )
    .await
    .ok()?;
    if !output.status.success() || output.stdout_exceeded {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            (key.trim() == "Pages")
                .then(|| value.trim().parse::<u64>().ok())
                .flatten()
        })
}

pub fn default_pdf_range(
    page_count: Option<u64>,
    file_size_bytes: u64,
    display_name: &str,
) -> Result<PdfPageRange, String> {
    let had_pdfinfo = page_count.is_some();
    let effective_page_count =
        page_count.unwrap_or_else(|| file_size_bytes.div_ceil(PDF_PAGE_COUNT_SIZE_HEURISTIC_BYTES));
    if effective_page_count == 0 {
        return Err(format!("PDF \"{display_name}\" contains no pages."));
    }
    if effective_page_count > MAX_PDF_FULL_TEXT_PAGES {
        let source = if had_pdfinfo {
            "has"
        } else {
            "appears to have about"
        };
        return Err(format!(
            "PDF \"{display_name}\" {source} {effective_page_count} pages, which is too many to read at once. Use the 'pages' parameter to read a specific page range such as '1-5'. Maximum {MAX_PDF_PAGES_PER_READ} pages per request."
        ));
    }
    let last_page = usize::try_from(effective_page_count)
        .map_err(|_| "PDF page count cannot be represented on this platform.".to_owned())?;
    Ok(PdfPageRange {
        first_page: 1,
        last_page,
    })
}

pub fn validate_pdf_size(file_size_bytes: u64, has_page_range: bool) -> Result<(), String> {
    let size_mb = file_size_bytes as f64 / (1024.0 * 1024.0);
    if has_page_range && file_size_bytes > MAX_PDF_PAGED_TEXT_SIZE_BYTES {
        return Err(format!(
            "PDF file is too large for page-range text extraction: {size_mb:.2}MB exceeds the 512MB limit. Split the document into smaller files before retrying."
        ));
    }
    if !has_page_range && file_size_bytes > MAX_PDF_FULL_TEXT_SIZE_BYTES {
        return Err(format!(
            "PDF file is too large for full text extraction: {size_mb:.2}MB exceeds the 100MB limit. Use the 'pages' parameter to read a narrower range, or split the document into smaller files before retrying."
        ));
    }
    Ok(())
}

async fn extract_pdf_text_with_command(
    path: &Path,
    range: PdfPageRange,
    command: &str,
    duration: Duration,
) -> Result<String, String> {
    let args = [
        OsString::from("-layout"),
        OsString::from("-f"),
        OsString::from(range.first_page.to_string()),
        OsString::from("-l"),
        OsString::from(range.last_page.to_string()),
        OsString::from("--"),
        path.as_os_str().to_owned(),
        OsString::from("-"),
    ];
    let output = match run_bounded_command(
        command,
        &args,
        duration,
        MAX_PDF_TEXT_OUTPUT_BYTES,
        MAX_PDF_STDERR_BYTES,
    )
    .await
    {
        Ok(output) => output,
        Err(BoundedCommandError::Start(error)) if error.kind() == io::ErrorKind::NotFound => {
            return Err("pdftotext is not installed. Install poppler-utils to enable PDF text extraction (e.g. `apt-get install poppler-utils` or `brew install poppler`).".to_owned());
        }
        Err(BoundedCommandError::Timeout) => {
            return Err(format!(
                "pdftotext timed out after {}s. The PDF may be unusually large or complex; try a narrower page range.",
                duration.as_secs()
            ));
        }
        Err(error) => return Err(format!("could not run pdftotext: {error}")),
    };

    if !output.status.success() {
        let diagnostic = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        return Err(if diagnostic.is_empty() {
            format!("pdftotext exited with status {}", output.status)
        } else {
            format!("pdftotext could not read the requested pages: {diagnostic}")
        });
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let (mut text, char_truncated) = truncate_utf16(&text, MAX_PDF_TEXT_OUTPUT_UTF16);
    if text.trim().is_empty() {
        return Err("The requested PDF pages contain no extractable text.".to_owned());
    }

    if output.stdout_exceeded || char_truncated {
        if !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str(
            "\n... [PDF text truncated at 100000 characters; use a narrower page range.]",
        );
    }

    let estimated_tokens = estimate_text_tokens(&text).saturating_add(16);
    if estimated_tokens > 12_000 {
        return Err(build_pdf_text_too_large_guidance(range, estimated_tokens));
    }
    Ok(text)
}

#[derive(Debug)]
struct BoundedCommandOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    stdout_exceeded: bool,
}

#[derive(Debug)]
enum BoundedCommandError {
    Start(io::Error),
    Timeout,
    Wait(io::Error),
    Output(String),
}

impl std::fmt::Display for BoundedCommandError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Start(error) => write!(formatter, "could not start child process: {error}"),
            Self::Timeout => formatter.write_str("child process timed out"),
            Self::Wait(error) => write!(formatter, "could not wait for child process: {error}"),
            Self::Output(error) => formatter.write_str(error),
        }
    }
}

async fn run_bounded_command(
    command: &str,
    args: &[OsString],
    duration: Duration,
    stdout_limit: usize,
    stderr_limit: usize,
) -> Result<BoundedCommandOutput, BoundedCommandError> {
    let mut child = Command::new(command)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(BoundedCommandError::Start)?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| BoundedCommandError::Output("stdout pipe was not available".to_owned()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| BoundedCommandError::Output("stderr pipe was not available".to_owned()))?;
    let stdout_task = tokio::spawn(read_capped(stdout, stdout_limit));
    let stderr_task = tokio::spawn(read_capped(stderr, stderr_limit));

    let status = match timeout(duration, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(error)) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            return Err(BoundedCommandError::Wait(error));
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            return Err(BoundedCommandError::Timeout);
        }
    };

    let (stdout, stdout_exceeded) = stdout_task
        .await
        .map_err(|error| BoundedCommandError::Output(error.to_string()))?
        .map_err(|error| BoundedCommandError::Output(error.to_string()))?;
    let (stderr, _) = stderr_task
        .await
        .map_err(|error| BoundedCommandError::Output(error.to_string()))?
        .map_err(|error| BoundedCommandError::Output(error.to_string()))?;
    Ok(BoundedCommandOutput {
        status,
        stdout,
        stderr,
        stdout_exceeded,
    })
}

async fn read_capped<R: AsyncRead + Unpin>(
    mut reader: R,
    limit: usize,
) -> io::Result<(Vec<u8>, bool)> {
    let mut output = Vec::with_capacity(limit.min(64 * 1024));
    let mut chunk = [0u8; 8192];
    let mut exceeded = false;
    loop {
        let count = reader.read(&mut chunk).await?;
        if count == 0 {
            break;
        }
        let remaining = limit.saturating_sub(output.len());
        let kept = count.min(remaining);
        output.extend_from_slice(&chunk[..kept]);
        exceeded |= kept < count;
    }
    Ok((output, exceeded))
}

fn truncate_utf16(value: &str, limit: usize) -> (String, bool) {
    let mut output = String::with_capacity(value.len().min(limit));
    let mut units = 0usize;
    for character in value.chars() {
        let character_units = character.len_utf16();
        if units.saturating_add(character_units) > limit {
            return (output, true);
        }
        output.push(character);
        units += character_units;
    }
    (output, false)
}

fn build_pdf_text_too_large_guidance(range: PdfPageRange, estimated_tokens: u64) -> String {
    let prefix = format!(
        "PDF text extracted from the requested pages is too large to return safely ({estimated_tokens} estimated tokens; limit 12000)."
    );
    if range.first_page == range.last_page {
        format!(
            "{prefix} The selected page exceeds the output limit. Split the page content externally or extract a smaller section with another tool."
        )
    } else {
        let suggested_end = range.first_page + (range.last_page - range.first_page) / 2;
        if suggested_end == range.first_page {
            format!(
                "{prefix} Use the 'pages' parameter with a single page, for example '{}'.",
                range.first_page
            )
        } else {
            format!(
                "{prefix} Use the 'pages' parameter with fewer pages, for example '{}-{}' or a single page.",
                range.first_page, suggested_end
            )
        }
    }
}

#[cfg(test)]
pub(crate) fn simple_text_pdf_fixture(text: &str) -> Vec<u8> {
    let stream = format!("BT /F1 12 Tf 72 220 Td ({text}) Tj ET");
    let objects = [
        "<< /Type /Catalog /Pages 2 0 R >>".to_owned(),
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_owned(),
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 300] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>".to_owned(),
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".to_owned(),
        format!("<< /Length {} >>\nstream\n{stream}\nendstream", stream.len()),
    ];
    let mut pdf = String::from("%PDF-1.4\n");
    let mut offsets = Vec::with_capacity(objects.len());
    for (index, object) in objects.iter().enumerate() {
        offsets.push(pdf.len());
        pdf.push_str(&format!("{} 0 obj\n{object}\nendobj\n", index + 1));
    }
    let xref_offset = pdf.len();
    pdf.push_str(&format!("xref\n0 {}\n", objects.len() + 1));
    pdf.push_str("0000000000 65535 f \n");
    for offset in offsets {
        pdf.push_str(&format!("{offset:010} 00000 n \n"));
    }
    pdf.push_str(&format!(
        "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n",
        objects.len() + 1
    ));
    pdf.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_and_closed_page_ranges() {
        assert_eq!(
            parse_pdf_page_range(" 5 "),
            Ok(PdfPageRange {
                first_page: 5,
                last_page: 5
            })
        );
        assert_eq!(
            parse_pdf_page_range("1 - 20"),
            Ok(PdfPageRange {
                first_page: 1,
                last_page: 20
            })
        );
    }

    #[test]
    fn rejects_invalid_open_ended_and_overlong_ranges() {
        for invalid in ["", "0", "1.5", "1-2-3", "1x-2", "999999999999999999999"] {
            assert_eq!(
                parse_pdf_page_range(invalid),
                Err(PdfPageRangeError::Invalid)
            );
        }
        assert_eq!(
            parse_pdf_page_range("3-"),
            Err(PdfPageRangeError::OpenEnded)
        );
        assert_eq!(
            parse_pdf_page_range("1-21"),
            Err(PdfPageRangeError::TooManyPages)
        );
        assert_eq!(parse_pdf_page_range("5-4"), Err(PdfPageRangeError::Invalid));
    }

    #[test]
    fn reads_small_pdfs_in_full_and_requires_ranges_for_large_pdfs() {
        assert_eq!(
            default_pdf_range(Some(10), 0, "ten-pages.pdf"),
            Ok(PdfPageRange {
                first_page: 1,
                last_page: 10
            })
        );
        let page_count_guidance = default_pdf_range(Some(11), 0, "eleven-pages.pdf").unwrap_err();
        assert!(page_count_guidance.contains("has 11 pages"));
        assert!(page_count_guidance.contains("Maximum 20 pages per request"));

        assert_eq!(
            default_pdf_range(None, 10 * PDF_PAGE_COUNT_SIZE_HEURISTIC_BYTES, "small.pdf"),
            Ok(PdfPageRange {
                first_page: 1,
                last_page: 10
            })
        );
        let size_guidance = default_pdf_range(
            None,
            10 * PDF_PAGE_COUNT_SIZE_HEURISTIC_BYTES + 1,
            "large.pdf",
        )
        .unwrap_err();
        assert!(size_guidance.contains("appears to have about 11 pages"));
    }

    #[test]
    fn enforces_separate_full_and_page_range_pdf_size_limits() {
        assert!(validate_pdf_size(MAX_PDF_FULL_TEXT_SIZE_BYTES, false).is_ok());
        assert!(
            validate_pdf_size(MAX_PDF_FULL_TEXT_SIZE_BYTES + 1, false)
                .unwrap_err()
                .contains("full text extraction")
        );
        assert!(validate_pdf_size(MAX_PDF_PAGED_TEXT_SIZE_BYTES, true).is_ok());
        assert!(
            validate_pdf_size(MAX_PDF_PAGED_TEXT_SIZE_BYTES + 1, true)
                .unwrap_err()
                .contains("page-range text extraction")
        );
    }

    #[tokio::test]
    async fn passes_a_closed_page_range_as_separate_child_arguments() {
        use std::os::unix::fs::PermissionsExt;
        use uuid::Uuid;

        let directory = std::env::temp_dir().join(format!("canopy-pdf-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let command = directory.join("fake-pdftotext");
        std::fs::write(
            &command,
            "#!/bin/sh\ntest \"$1\" = \"-layout\" && test \"$2\" = \"-f\" && test \"$3\" = \"2\" && test \"$4\" = \"-l\" && test \"$5\" = \"4\" && test \"$6\" = \"--\" && test \"$7\" = \"document.pdf\" && test \"$8\" = \"-\" || exit 12\nprintf 'selected page text\\n'\n",
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&command).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&command, permissions).unwrap();

        let text = extract_pdf_text_with_command(
            Path::new("document.pdf"),
            PdfPageRange {
                first_page: 2,
                last_page: 4,
            },
            command.to_str().unwrap(),
            Duration::from_secs(2),
        )
        .await
        .unwrap();

        assert_eq!(text, "selected page text\n");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn child_output_capture_keeps_only_its_configured_byte_limit() {
        let input = std::io::Cursor::new(vec![b'x'; 512 * 1024]);

        let (output, exceeded) = read_capped(input, 1024).await.unwrap();

        assert_eq!(output, vec![b'x'; 1024]);
        assert!(exceeded);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn kills_a_pdftotext_process_that_exceeds_its_deadline() {
        use std::os::unix::fs::PermissionsExt;
        use uuid::Uuid;

        let directory = std::env::temp_dir().join(format!("canopy-pdf-timeout-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let command = directory.join("slow-pdftotext");
        std::fs::write(&command, "#!/bin/sh\nexec sleep 3\n").unwrap();
        let mut permissions = std::fs::metadata(&command).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&command, permissions).unwrap();

        let error = extract_pdf_text_with_command(
            Path::new("document.pdf"),
            PdfPageRange {
                first_page: 1,
                last_page: 1,
            },
            command.to_str().unwrap(),
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();

        assert!(error.contains("timed out"));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn extracts_text_from_a_real_pdf_when_poppler_is_installed() {
        use uuid::Uuid;

        let available = std::process::Command::new("pdftotext")
            .arg("-v")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success());
        if !available {
            return;
        }

        let directory = std::env::temp_dir().join(format!("canopy-pdf-real-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("document.pdf");
        std::fs::write(&path, simple_text_pdf_fixture("Canopy PDF verification")).unwrap();

        let text = extract_pdf_text(
            &path,
            PdfPageRange {
                first_page: 1,
                last_page: 1,
            },
        )
        .await
        .unwrap();

        assert!(text.contains("Canopy PDF verification"));
        std::fs::remove_dir_all(directory).unwrap();
    }
}
