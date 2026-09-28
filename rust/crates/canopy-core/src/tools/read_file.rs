use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, ErrorKind, Read};
use std::path::{Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use encoding_rs::{Encoding, UTF_8};
use serde_json::{Value, json};

use crate::file_discovery::FileDiscoveryService;
use crate::file_read_cache::FileReadCache;
use crate::pdf::{
    PdfPageRangeError, default_pdf_range, extract_pdf_text, get_pdf_page_count,
    parse_pdf_page_range, validate_pdf_size,
};
use crate::providers::openai_request::InputModalities;
use crate::tool_response_finalizer::ToolExecutionOutput;
use crate::utils::read_text_range::{
    ReadTextRangeError, ReadTextRangeFromHandleRequest, read_text_range_from_handle,
};

const DEFAULT_LINE_LIMIT: usize = 2_000;
const MAX_LINE_BYTES: usize = 2 * 1024 * 1024;
const MAX_OUTPUT_BYTES: usize = 8 * 1024 * 1024;
const MAX_NOTEBOOK_BYTES: u64 = 8 * 1024 * 1024;
const MAX_INLINE_BASE64_BYTES: usize = 10_380_902;

#[derive(Clone, Copy)]
enum TextEncoding {
    Utf8,
    Utf16Le,
    Utf16Be,
    Utf32Le,
    Utf32Be,
    Legacy(&'static Encoding),
}

#[derive(Clone, Copy)]
enum MediaModality {
    Image,
    Audio,
    Video,
}

enum FileKind {
    Text,
    Svg,
    Notebook,
    Pdf,
    Media {
        mime_type: String,
        modality: MediaModality,
    },
    Binary,
}

pub struct ReadFileTool {
    workspace_root: PathBuf,
    file_discovery: FileDiscoveryService,
    file_read_cache: FileReadCache,
    cache_enabled: bool,
}

impl ReadFileTool {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self, String> {
        Self::new_with_cache(workspace_root, FileReadCache::default())
    }

    pub fn new_with_cache(
        workspace_root: impl AsRef<Path>,
        file_read_cache: FileReadCache,
    ) -> Result<Self, String> {
        let workspace_root = std::fs::canonicalize(workspace_root.as_ref())
            .map_err(|error| format!("could not resolve workspace root: {error}"))?;
        if !workspace_root.is_dir() {
            return Err("workspace root is not a directory".to_owned());
        }
        let file_discovery = FileDiscoveryService::new(&workspace_root, None)?;
        Ok(Self {
            workspace_root,
            file_discovery,
            file_read_cache,
            cache_enabled: true,
        })
    }

    /// Enable or disable both unchanged-read shortcuts and cache updates.
    ///
    /// Disabling the cache is useful when a caller transforms or compacts the
    /// conversation without updating the cache's history-residency markers.
    pub fn with_cache_enabled(mut self, enabled: bool) -> Self {
        self.cache_enabled = enabled;
        self
    }

    pub async fn execute(&self, args: &Value) -> Result<String, String> {
        self.execute_with_modalities(args, InputModalities::default())
            .await
            .map(|output| output.output)
    }

    pub async fn execute_with_modalities(
        &self,
        args: &Value,
        modalities: InputModalities,
    ) -> Result<ToolExecutionOutput, String> {
        let requested_path = args
            .get("file_path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .ok_or_else(|| "The 'file_path' parameter must be non-empty.".to_owned())?;
        let requested_path = unescape_path(requested_path);
        let requested_path = Path::new(&requested_path);
        if !requested_path.is_absolute() {
            return Err(format!(
                "File path must be absolute, but was relative: {}. You must provide an absolute path.",
                requested_path.display()
            ));
        }

        let offset = optional_non_negative_integer(args, "offset")?.unwrap_or(0);
        let line_limit = optional_positive_integer(args, "limit")?.unwrap_or(DEFAULT_LINE_LIMIT);
        let extension = requested_path
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        let pages = match args.get("pages") {
            None | Some(Value::Null) => None,
            Some(Value::String(pages)) => {
                let pages = pages.trim();
                (!pages.is_empty()).then_some(pages.to_owned())
            }
            Some(_) => return Err("pages must be a string".to_owned()),
        };
        let full_read_request =
            args.get("offset").is_none() && args.get("limit").is_none() && pages.is_none();
        if extension == "ipynb" && (args.get("offset").is_some() || args.get("limit").is_some()) {
            return Err("offset and limit are not supported for Jupyter notebook (.ipynb) files. Notebooks are always read in full with structured cell output.".to_owned());
        }
        if pages.is_some() && extension == "ipynb" {
            return Err("pages is not supported for Jupyter notebook (.ipynb) files. Notebooks are always read in full with structured cell output.".to_owned());
        }
        let page_range = pages
            .as_deref()
            .map(parse_pdf_page_range)
            .transpose()
            .map_err(|error| match error {
                PdfPageRangeError::Invalid => format!(
                    "Invalid pages parameter: '{}'. Use formats like '5' or '1-10'.",
                    pages.as_deref().unwrap_or_default()
                ),
                PdfPageRangeError::OpenEnded => format!(
                    "Open-ended page ranges (e.g. '3-') are not supported; specify an explicit end page within the {}-page limit (e.g. '3-22').",
                    crate::pdf::MAX_PDF_PAGES_PER_READ
                ),
                PdfPageRangeError::TooManyPages => format!(
                    "Pages range exceeds maximum of {} pages per request.",
                    crate::pdf::MAX_PDF_PAGES_PER_READ
                ),
            })?;
        if page_range.is_some() && extension != "pdf" {
            return Err("pages is only supported for PDF files.".to_owned());
        }

        let canonical_path = std::fs::canonicalize(requested_path).map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                format!("File not found: {}", requested_path.display())
            } else {
                format!("could not resolve file path: {error}")
            }
        })?;
        if !canonical_path.starts_with(&self.workspace_root) {
            return Err("read_file is restricted to files inside the workspace".to_owned());
        }
        if let Some(ignore_file) =
            canopy_ignore_source(&self.file_discovery, &canonical_path, &self.workspace_root)
        {
            return Err(format!(
                "File path '{}' is ignored by {ignore_file} pattern(s).",
                requested_path.display()
            ));
        }

        let metadata = std::fs::metadata(&canonical_path)
            .map_err(|error| format!("could not inspect file: {error}"))?;
        if metadata.is_dir() {
            return Err("Path is a directory.".to_owned());
        }
        if !metadata.is_file() {
            return Err("Cannot read a non-regular file.".to_owned());
        }

        let file = open_regular_file(&canonical_path)
            .map_err(|error| format!("could not open file: {error}"))?;
        let read_metadata = file.metadata().map_err(|error| error.to_string())?;
        if !read_metadata.is_file() {
            return Err("Cannot read a non-regular file.".to_owned());
        }
        let file_kind = detect_file_kind(&canonical_path, &file, read_metadata.len());
        let unchanged_read = if self.cache_enabled
            && full_read_request
            && !is_local_auto_memory_path(&canonical_path, &self.workspace_root)
        {
            match self.file_read_cache.check(&read_metadata) {
                crate::file_read_cache::FileReadCheckResult::Fresh(entry) => {
                    entry.last_read_was_full
                        && entry.last_read_cacheable
                        && entry.read_resident_in_history
                        && entry.last_read_at.is_some()
                        && entry.last_write_at.is_none_or(|last_write_at| {
                            entry
                                .last_read_at
                                .is_some_and(|last_read_at| last_read_at > last_write_at)
                        })
                }
                _ => false,
            }
        } else {
            false
        };
        if unchanged_read {
            return Ok(ToolExecutionOutput::text(unchanged_read_message(
                &canonical_path,
                &self.workspace_root,
            )));
        }
        if matches!(&file_kind, FileKind::Binary) {
            self.record_read(&canonical_path, &read_metadata, full_read_request, false);
            return Ok(ToolExecutionOutput::text(format!(
                "Cannot display content of binary file: {}",
                relative_display_path(&canonical_path, &self.workspace_root)
            )));
        }
        if matches!(&file_kind, FileKind::Svg) && read_metadata.len() > 1024 * 1024 {
            self.record_read(&canonical_path, &read_metadata, full_read_request, false);
            return Ok(ToolExecutionOutput::text(format!(
                "Cannot display content of SVG file larger than 1MB: {}",
                relative_display_path(&canonical_path, &self.workspace_root)
            )));
        }
        if matches!(&file_kind, FileKind::Pdf) {
            if page_range.is_none() && modalities.pdf {
                let display_name = canonical_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("document.pdf");
                let output = inline_media_output(
                    &file,
                    read_metadata.len(),
                    "application/pdf",
                    display_name,
                    "PDF",
                )?;
                self.record_read(&canonical_path, &read_metadata, false, false);
                return Ok(output);
            }
            validate_pdf_size(read_metadata.len(), page_range.is_some())?;
            let display_name = canonical_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("document.pdf");
            let range = match page_range {
                Some(range) => range,
                None => {
                    let page_count = get_pdf_page_count(&canonical_path).await;
                    default_pdf_range(page_count, read_metadata.len(), display_name)?
                }
            };
            let content = extract_pdf_text(&canonical_path, range).await?;
            self.record_read(&canonical_path, &read_metadata, false, false);
            return Ok(ToolExecutionOutput::text(content));
        }
        if let FileKind::Media {
            mime_type,
            modality,
        } = &file_kind
        {
            let (supported, label) = match *modality {
                MediaModality::Image => (modalities.image, "image"),
                MediaModality::Audio => (modalities.audio, "audio"),
                MediaModality::Video => (modalities.video, "video"),
            };
            if !supported {
                let display_name = canonical_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("media file");
                return Ok(ToolExecutionOutput::text(unsupported_modality_message(
                    label,
                    display_name,
                )));
            }
            let display_name = canonical_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("media file");
            let output =
                inline_media_output(&file, read_metadata.len(), mime_type, display_name, label)?;
            self.record_read(&canonical_path, &read_metadata, false, false);
            return Ok(output);
        }
        if matches!(&file_kind, FileKind::Notebook) {
            if read_metadata.len() > MAX_NOTEBOOK_BYTES {
                return Err(format!(
                    "Notebook exceeds the {MAX_NOTEBOOK_BYTES}-byte structured-read limit."
                ));
            }
            let mut bytes = Vec::with_capacity(read_metadata.len() as usize);
            file.take(MAX_NOTEBOOK_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(|error| format!("could not read notebook: {error}"))?;
            if bytes.len() as u64 > MAX_NOTEBOOK_BYTES {
                return Err(format!(
                    "Notebook exceeds the {MAX_NOTEBOOK_BYTES}-byte structured-read limit."
                ));
            }
            let raw = String::from_utf8(bytes)
                .map_err(|_| "Notebook file is not valid UTF-8 JSON.".to_owned())?;
            let notebook = crate::notebook::render_notebook(&raw)?;
            self.record_read(
                &canonical_path,
                &read_metadata,
                full_read_request && !notebook.is_truncated,
                false,
            );
            return Ok(ToolExecutionOutput::text(notebook.content));
        }
        let mut reader = BufReader::new(
            file.try_clone()
                .map_err(|error| format!("could not reopen file: {error}"))?,
        );
        let text_encoding = detect_text_encoding(&mut reader, &file, read_metadata.len())
            .map_err(|error| format!("could not read file: {error}"))?;

        let text_offset = if matches!(&file_kind, FileKind::Svg) {
            0
        } else {
            offset
        };
        let text_line_limit = if matches!(&file_kind, FileKind::Svg) {
            usize::MAX
        } else {
            line_limit
        };

        // Bind the range read to the descriptor opened after workspace and
        // ignore checks. The path-based helper would reopen the canonical path
        // and could observe a replacement between validation and reading.
        match read_text_range_from_handle(
            &file,
            ReadTextRangeFromHandleRequest {
                offset: Some(text_offset),
                limit: Some(text_line_limit),
                file_size: read_metadata.len(),
                max_output_bytes: MAX_OUTPUT_BYTES,
                // The source read_file path uses the helper's unbounded scan
                // default. The captured descriptor size is still a hard EOF.
                max_scan_bytes: u64::MAX,
                cancellation: None,
            },
        ) {
            Ok(range) => {
                let had_content = !range.content.is_empty();
                let mut output = range.content;
                let emitted_lines = if range.truncated_by_bytes {
                    // Keep the existing whole-line output contract when the
                    // byte budget interrupts a line: the old reader omitted
                    // that line and resumed from its line offset.
                    if let Some(last_newline) = output.rfind('\n') {
                        output.truncate(last_newline + 1);
                        output.bytes().filter(|byte| *byte == b'\n').count()
                    } else {
                        output.clear();
                        0
                    }
                } else {
                    text_line_limit
                };
                output = trim_range_line_ends(&output);

                let line_window_truncated =
                    text_offset.saturating_add(text_line_limit) < range.original_line_count;
                let truncated = range.truncated_by_bytes
                    || range.next_byte_offset.is_some()
                    || line_window_truncated;

                // The range utility omits the final line terminator when it
                // stops at a requested line boundary. The ReadFile contract
                // preserves that terminator, including on paginated reads.
                let returned_a_range = had_content || range.next_byte_offset.is_some();
                let last_byte_is_newline =
                    if !range.truncated_by_bytes && had_content && read_metadata.len() > 0 {
                        let mut last_byte = [0u8; 1];
                        crate::utils::read_text_range::ReadAt::read_at(
                            &file,
                            &mut last_byte,
                            read_metadata.len() - 1,
                        )
                        .is_ok_and(|read| read == 1 && last_byte[0] == b'\n')
                    } else {
                        false
                    };
                if returned_a_range
                    && !range.truncated_by_bytes
                    && (range.next_byte_offset.is_some() || last_byte_is_newline)
                    && !output.ends_with('\n')
                {
                    output.push('\n');
                }

                if truncated {
                    let next_offset = text_offset.saturating_add(emitted_lines);
                    if !output.is_empty() && !output.ends_with('\n') {
                        output.push('\n');
                    }
                    output.push_str(&format!(
                        "\n[File read truncated. Continue with offset {next_offset} and a positive limit.]"
                    ));
                }
                self.record_read(
                    &canonical_path,
                    &read_metadata,
                    full_read_request && !truncated,
                    extension != "ipynb",
                );
                return Ok(ToolExecutionOutput::text(output));
            }
            // The Rust range helper deliberately streams UTF-8-compatible
            // text only. Preserve ReadFile's existing support for BOM-marked
            // UTF-16/32 and detected legacy encodings through the bounded
            // decoder below.
            Err(ReadTextRangeError::LargeNonUtf8Text { .. }) => {}
            Err(error) => return Err(format!("could not read file: {error}")),
        }

        let mut reached_eof_while_skipping = false;
        for _ in 0..text_offset {
            if read_bounded_line(&mut reader, text_encoding)
                .map_err(|error| format!("could not read file: {error}"))?
                .is_none()
            {
                reached_eof_while_skipping = true;
                break;
            }
        }

        let mut output = String::new();
        let mut output_bytes = 0usize;
        let mut emitted_lines = 0usize;
        let mut truncated = false;
        while !reached_eof_while_skipping && emitted_lines < text_line_limit {
            let Some(mut line) = read_bounded_line(&mut reader, text_encoding)
                .map_err(|error| format!("could not read file: {error}"))?
            else {
                break;
            };
            let has_newline = strip_line_ending(&mut line, text_encoding);
            let line = decode_text_line(&line, text_encoding).trim_end().to_owned();
            let line_bytes = line.len() + usize::from(has_newline);
            if output_bytes.saturating_add(line_bytes) > MAX_OUTPUT_BYTES {
                truncated = true;
                break;
            }
            output.push_str(&line);
            if has_newline {
                output.push('\n');
            }
            output_bytes = output_bytes.saturating_add(line_bytes);
            emitted_lines = emitted_lines.saturating_add(1);
        }

        if emitted_lines == text_line_limit
            && !reached_eof_while_skipping
            && read_bounded_line(&mut reader, text_encoding)
                .map_err(|error| format!("could not read file: {error}"))?
                .is_some()
        {
            truncated = true;
        }
        if truncated {
            let next_offset = text_offset.saturating_add(emitted_lines);
            if !output.is_empty() && !output.ends_with('\n') {
                output.push('\n');
            }
            output.push_str(&format!(
                "\n[File read truncated. Continue with offset {next_offset} and a positive limit.]"
            ));
        }
        let full_read = full_read_request && !truncated;
        self.record_read(
            &canonical_path,
            &read_metadata,
            full_read,
            extension != "ipynb",
        );
        Ok(ToolExecutionOutput::text(output))
    }

    fn record_read(&self, path: &Path, metadata: &std::fs::Metadata, full: bool, cacheable: bool) {
        if self.cache_enabled {
            self.file_read_cache
                .record_read(path, metadata, full, cacheable);
        }
    }
}

pub(crate) fn unescape_path(value: &str) -> String {
    #[cfg(windows)]
    {
        value.to_owned()
    }
    #[cfg(not(windows))]
    {
        let mut output = String::with_capacity(value.len());
        let mut characters = value.chars().peekable();
        while let Some(character) = characters.next() {
            if character == '\\'
                && characters
                    .peek()
                    .is_some_and(|next| is_shell_special_character(*next))
            {
                output.push(characters.next().unwrap_or_default());
            } else {
                output.push(character);
            }
        }
        output
    }
}

#[cfg(not(windows))]
fn is_shell_special_character(character: char) -> bool {
    matches!(
        character,
        ' ' | '\t'
            | '('
            | ')'
            | '['
            | ']'
            | '{'
            | '}'
            | ';'
            | '|'
            | '*'
            | '?'
            | '$'
            | '`'
            | '\''
            | '"'
            | '#'
            | '&'
            | '<'
            | '>'
            | '!'
            | '~'
            | ','
    )
}

pub(crate) fn canopy_ignore_source(
    file_discovery: &FileDiscoveryService,
    file_path: &Path,
    workspace_root: &Path,
) -> Option<String> {
    let file_path = file_path.to_string_lossy();
    if file_discovery.should_canopy_ignore_file(&file_path) {
        return file_discovery
            .get_canopy_ignore_file_display_for_path(&file_path)
            .map(str::to_owned);
    }

    let mut directory = Path::new(file_path.as_ref())
        .parent()
        .map(Path::to_path_buf);
    while let Some(path) = directory {
        if !path.starts_with(workspace_root) {
            break;
        }
        let mut path = path.to_string_lossy().replace('\\', "/");
        if !path.ends_with('/') {
            path.push('/');
        }
        if file_discovery.should_canopy_ignore_file(&path) {
            return file_discovery
                .get_canopy_ignore_file_display_for_path(&path)
                .map(str::to_owned);
        }
        if Path::new(path.trim_end_matches('/')) == workspace_root {
            break;
        }
        directory = Path::new(path.trim_end_matches('/'))
            .parent()
            .map(Path::to_path_buf);
    }
    None
}

fn is_local_auto_memory_path(path: &Path, workspace_root: &Path) -> bool {
    path.strip_prefix(workspace_root)
        .ok()
        .is_some_and(|relative| {
            let mut components = relative.components();
            components
                .next()
                .is_some_and(|component| component.as_os_str().to_str() == Some(".canopy"))
                && components
                    .next()
                    .is_some_and(|component| component.as_os_str().to_str() == Some("memory"))
        })
}

fn trim_range_line_ends(content: &str) -> String {
    content
        .split('\n')
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
}

fn unchanged_read_message(path: &Path, workspace_root: &Path) -> String {
    let relative_path = path
        .strip_prefix(workspace_root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/");
    format!(
        "[File {relative_path} unchanged since last read in this session — the full content was provided earlier in this conversation. If you cannot retrieve that prior content (e.g. after context compaction) or you suspect the file was modified outside the read/edit tools (shell command, MCP tool, another process), re-read with explicit offset/limit to fetch current content.]"
    )
}

fn detect_file_kind(path: &Path, file: &File, file_size: u64) -> FileKind {
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| format!(".{}", extension.to_ascii_lowercase()))
        .unwrap_or_default();
    if matches!(extension.as_str(), ".ts" | ".mts" | ".cts" | ".tsx") {
        return FileKind::Text;
    }
    if extension == ".svg" {
        return FileKind::Svg;
    }
    if extension == ".ipynb" {
        return FileKind::Notebook;
    }

    let mime_type = if extension == ".m4v" {
        Some("video/x-m4v")
    } else {
        mime_guess::from_path(path).first_raw()
    };
    if let Some(mime_type) = mime_type {
        if mime_type.starts_with("image/") {
            return FileKind::Media {
                mime_type: mime_type.to_owned(),
                modality: MediaModality::Image,
            };
        }
        if mime_type.starts_with("audio/") {
            return FileKind::Media {
                mime_type: mime_type.to_owned(),
                modality: MediaModality::Audio,
            };
        }
        if mime_type.starts_with("video/") {
            return FileKind::Media {
                mime_type: mime_type.to_owned(),
                modality: MediaModality::Video,
            };
        }
        if mime_type == "application/pdf" {
            return FileKind::Pdf;
        }
        if is_text_mime(mime_type) {
            return FileKind::Text;
        }
    }

    const BINARY_EXTENSIONS: &[&str] = &[
        ".bin", ".exe", ".dll", ".so", ".dylib", ".class", ".jar", ".war", ".zip", ".tar", ".gz",
        ".bz2", ".rar", ".7z", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".odt", ".ods",
        ".odp", ".pyc", ".pyo", ".obj", ".o", ".a", ".lib", ".wasm",
    ];
    if BINARY_EXTENSIONS.contains(&extension.as_str()) {
        return FileKind::Binary;
    }

    const KNOWN_TEXT_EXTENSIONS: &[&str] = &[
        ".c",
        ".cc",
        ".cpp",
        ".cxx",
        ".h",
        ".hh",
        ".hpp",
        ".hxx",
        ".inl",
        ".tpp",
        ".py",
        ".pyi",
        ".pyw",
        ".pyx",
        ".rs",
        ".go",
        ".gradle",
        ".groovy",
        ".java",
        ".kt",
        ".kts",
        ".sc",
        ".scala",
        ".cs",
        ".fs",
        ".fsi",
        ".fsx",
        ".vb",
        ".m",
        ".mm",
        ".swift",
        ".cljc",
        ".cljs",
        ".clj",
        ".edn",
        ".erl",
        ".ex",
        ".exs",
        ".hrl",
        ".hs",
        ".lhs",
        ".ml",
        ".mli",
        ".astro",
        ".jsx",
        ".svelte",
        ".vue",
        ".bash",
        ".dart",
        ".fish",
        ".lua",
        ".php",
        ".pl",
        ".pm",
        ".ps1",
        ".r",
        ".rb",
        ".sh",
        ".zsh",
        ".cr",
        ".nim",
        ".sol",
        ".zig",
        ".gql",
        ".graphql",
        ".proto",
        ".sql",
        ".thrift",
        ".adoc",
        ".bib",
        ".org",
        ".rst",
        ".tex",
        ".cfg",
        ".cmake",
        ".conf",
        ".containerfile",
        ".dockerfile",
        ".hcl",
        ".ini",
        ".mk",
        ".nomad",
        ".properties",
        ".tf",
        ".tfvars",
        ".toml",
    ];
    if KNOWN_TEXT_EXTENSIONS.contains(&extension.as_str()) {
        return FileKind::Text;
    }

    const KNOWN_TEXT_BASENAMES: &[&str] = &[
        "Dockerfile",
        "Containerfile",
        "Makefile",
        "GNUmakefile",
        "Jenkinsfile",
        "Vagrantfile",
        "Rakefile",
        "Gemfile",
        "Procfile",
        "BUILD",
        "WORKSPACE",
        "CMakeLists.txt",
        "go.mod",
        "go.sum",
        "go.work",
        "Cargo.lock",
        "Pipfile",
        "Pipfile.lock",
        "poetry.lock",
        "package-lock.json",
        "yarn.lock",
        "pnpm-lock.yaml",
        "requirements.txt",
        ".gitignore",
        ".gitattributes",
        ".dockerignore",
        ".npmignore",
        ".editorconfig",
        ".env",
        ".bashrc",
        ".zshrc",
        ".profile",
        "LICENSE",
        "COPYING",
        "AUTHORS",
        "CHANGELOG",
        "README",
        "NOTICE",
    ];
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| KNOWN_TEXT_BASENAMES.contains(&name))
    {
        return FileKind::Text;
    }

    if is_binary_file(file, file_size) {
        FileKind::Binary
    } else {
        FileKind::Text
    }
}

fn is_text_mime(mime_type: &str) -> bool {
    mime_type.starts_with("text/")
        || mime_type.ends_with("+xml")
        || mime_type.ends_with("+json")
        || matches!(
            mime_type,
            "application/javascript"
                | "application/ecmascript"
                | "application/node"
                | "application/json"
                | "application/xml"
                | "application/toml"
        )
}

fn is_binary_file(file: &File, file_size: u64) -> bool {
    if file_size == 0 {
        return false;
    }
    let mut sample = vec![0u8; usize::try_from(file_size.min(4_096)).unwrap_or(4_096)];
    let bytes_read = match read_file_prefix(file, &mut sample) {
        Ok(bytes_read) => bytes_read,
        Err(_) => return false,
    };
    sample.truncate(bytes_read);
    if sample.is_empty() || detect_unicode_bom(&sample).is_some() {
        return false;
    }
    let mut non_printable = 0usize;
    for byte in &sample {
        if *byte == 0 {
            return true;
        }
        if *byte < 9 || (*byte > 13 && *byte < 32) {
            non_printable += 1;
        }
    }
    non_printable as f64 / sample.len() as f64 > 0.3
}

fn read_file_prefix(file: &File, buffer: &mut [u8]) -> io::Result<usize> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_at(buffer, 0)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        file.seek_read(buffer, 0)
    }
    #[cfg(not(any(unix, windows)))]
    {
        use std::io::{Seek, SeekFrom};
        let mut reader = file.try_clone()?;
        reader.seek(SeekFrom::Start(0))?;
        reader.read(buffer)
    }
}

fn unsupported_modality_message(modality: &str, display_name: &str) -> String {
    format!(
        "[Unsupported {modality} file: \"{display_name}\". This model does not support {modality} input. The read_file tool cannot process this type of file either. To handle this file, try using skills if applicable, or any tools installed at system wide, or let the user know you cannot process this type of file.]"
    )
}

fn relative_display_path(path: &Path, workspace_root: &Path) -> String {
    path.strip_prefix(workspace_root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn inline_media_output(
    file: &File,
    file_size: u64,
    mime_type: &str,
    display_name: &str,
    modality: &str,
) -> Result<ToolExecutionOutput, String> {
    let encoded_size = file_size
        .checked_add(2)
        .and_then(|length| length.checked_div(3))
        .and_then(|length| length.checked_mul(4))
        .and_then(|length| usize::try_from(length).ok())
        .ok_or_else(|| "Media file is too large to encode safely.".to_owned())?;
    if encoded_size > MAX_INLINE_BASE64_BYTES {
        return Err(format!(
            "File exceeds the 10MB data URI limit after base64 encoding ({:.2}MB encoded).",
            encoded_size as f64 / (1024 * 1024) as f64
        ));
    }

    let source = file
        .try_clone()
        .map_err(|error| format!("could not reopen media file: {error}"))?;
    let mut bytes = Vec::with_capacity(file_size as usize);
    source
        .take(file_size.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| format!("could not read media file: {error}"))?;
    if bytes.len() as u64 != file_size {
        return Err("Media file changed while it was being read; retry the read.".to_owned());
    }
    let data = BASE64_STANDARD.encode(bytes);
    Ok(ToolExecutionOutput::with_parts(
        format!("Read {modality} file: {display_name}"),
        vec![json!({
            "inlineData": {
                "mimeType": mime_type,
                "data": data,
                "displayName": display_name
            }
        })],
    ))
}

pub fn function_declaration() -> Value {
    json!({
        "name":"read_file",
        "description":"Read a text file (including UTF-8/16/32 BOM text), Jupyter notebook, PDF, image, audio, or video file inside the current workspace. Media is attached only when the selected model supports that modality and the encoded file is under 10MB. file_path must be an absolute path. offset is a zero-based line offset and limit is the maximum number of lines to return. Small PDFs are read in full; use pages to select an explicit range for larger PDFs. Maximum 20 pages per request.",
        "parameters":{
            "type":"OBJECT",
            "properties":{
                "file_path":{"type":"STRING","description":"Absolute path to a text or supported media file inside the current workspace."},
                "offset":{"type":"INTEGER","description":"Optional zero-based line offset."},
                "limit":{"type":"INTEGER","description":"Optional maximum number of lines to return."},
                "pages":{"type":"STRING","description":"Optional PDF page range (1-indexed), such as '5' or '1-10'. Maximum 20 pages per request; specify an explicit end page."}
            },
            "required":["file_path"]
        }
    })
}

fn optional_non_negative_integer(args: &Value, key: &str) -> Result<Option<usize>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| format!("{key} must be a non-negative integer")),
    }
}

fn optional_positive_integer(args: &Value, key: &str) -> Result<Option<usize>, String> {
    let value = optional_non_negative_integer(args, key)?;
    if matches!(value, Some(0)) {
        return Err(format!("{key} must be a positive integer"));
    }
    Ok(value)
}

fn detect_text_encoding(
    reader: &mut BufReader<File>,
    source: &File,
    file_size: u64,
) -> io::Result<TextEncoding> {
    let prefix = reader.fill_buf()?;
    if let Some((encoding, bom_bytes)) = detect_unicode_bom(prefix) {
        reader.consume(bom_bytes);
        return Ok(encoding);
    }
    Ok(detect_legacy_encoding(source, file_size).unwrap_or(TextEncoding::Utf8))
}

fn detect_unicode_bom(bytes: &[u8]) -> Option<(TextEncoding, usize)> {
    if bytes.starts_with(&[0xff, 0xfe, 0x00, 0x00]) {
        Some((TextEncoding::Utf32Le, 4))
    } else if bytes.starts_with(&[0x00, 0x00, 0xfe, 0xff]) {
        Some((TextEncoding::Utf32Be, 4))
    } else if bytes.starts_with(&[0xef, 0xbb, 0xbf]) {
        Some((TextEncoding::Utf8, 3))
    } else if bytes.starts_with(&[0xff, 0xfe]) {
        Some((TextEncoding::Utf16Le, 2))
    } else if bytes.starts_with(&[0xfe, 0xff]) {
        Some((TextEncoding::Utf16Be, 2))
    } else {
        None
    }
}

fn detect_legacy_encoding(file: &File, file_size: u64) -> Option<TextEncoding> {
    if file_size == 0 {
        return None;
    }
    const SAMPLE_BYTES: usize = 8 * 1024;
    let mut detector = EncodingDetector::new(Iso2022JpDetection::Deny);
    let mut buffer = [0u8; SAMPLE_BYTES];
    let mut offset = 0u64;
    let mut utf8_valid = true;
    let mut utf8_pending = Vec::with_capacity(4);

    loop {
        let bytes_to_read =
            usize::try_from((file_size - offset).min(SAMPLE_BYTES as u64)).unwrap_or(SAMPLE_BYTES);
        let bytes_read = match read_file_at(file, &mut buffer[..bytes_to_read], offset) {
            Ok(bytes_read) => bytes_read,
            Err(_) => return None,
        };
        let is_last = bytes_read == 0 || offset + bytes_read as u64 >= file_size;
        if utf8_valid && !feed_utf8_validator(&mut utf8_pending, &buffer[..bytes_read]) {
            utf8_valid = false;
        }
        detector.feed(&buffer[..bytes_read], is_last);
        offset = offset.saturating_add(bytes_read as u64);
        if is_last {
            break;
        }
    }

    if utf8_valid && utf8_pending.is_empty() {
        return None;
    }
    let encoding = detector.guess(None, Utf8Detection::Allow);
    if encoding == UTF_8 {
        None
    } else {
        Some(TextEncoding::Legacy(encoding))
    }
}

fn feed_utf8_validator(pending: &mut Vec<u8>, chunk: &[u8]) -> bool {
    pending.extend_from_slice(chunk);
    match std::str::from_utf8(pending) {
        Ok(_) => {
            pending.clear();
            true
        }
        Err(error) if error.error_len().is_none() => {
            let valid_bytes = error.valid_up_to();
            pending.drain(..valid_bytes);
            pending.len() <= 3
        }
        Err(_) => false,
    }
}

fn read_file_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_at(buffer, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        file.seek_read(buffer, offset)
    }
    #[cfg(not(any(unix, windows)))]
    {
        use std::io::{Seek, SeekFrom};
        let mut reader = file.try_clone()?;
        reader.seek(SeekFrom::Start(offset))?;
        reader.read(buffer)
    }
}

fn read_bounded_line(
    reader: &mut BufReader<File>,
    encoding: TextEncoding,
) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    if matches!(
        encoding,
        TextEncoding::Utf16Le
            | TextEncoding::Utf16Be
            | TextEncoding::Utf32Le
            | TextEncoding::Utf32Be
    ) {
        let unit_size = match encoding {
            TextEncoding::Utf16Le | TextEncoding::Utf16Be => 2,
            TextEncoding::Utf32Le | TextEncoding::Utf32Be => 4,
            TextEncoding::Utf8 | TextEncoding::Legacy(_) => {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "byte-oriented encodings cannot use fixed-width line reads",
                ));
            }
        };
        loop {
            let mut unit = [0u8; 4];
            let mut read = 0;
            while read < unit_size {
                match reader.read(&mut unit[read..unit_size])? {
                    0 => {
                        return Ok((!line.is_empty()).then_some(line));
                    }
                    count => read += count,
                }
            }
            line.extend_from_slice(&unit[..unit_size]);
            if line.len() > MAX_LINE_BYTES {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    format!("line exceeds the {MAX_LINE_BYTES}-byte read limit"),
                ));
            }
            let is_line_feed = match encoding {
                TextEncoding::Utf16Le => unit[..2] == [0x0a, 0x00],
                TextEncoding::Utf16Be => unit[..2] == [0x00, 0x0a],
                TextEncoding::Utf32Le => unit == [0x0a, 0x00, 0x00, 0x00],
                TextEncoding::Utf32Be => unit == [0x00, 0x00, 0x00, 0x0a],
                TextEncoding::Utf8 | TextEncoding::Legacy(_) => false,
            };
            if is_line_feed {
                return Ok(Some(line));
            }
        }
    }
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok((!line.is_empty()).then_some(line));
        }
        let consumed = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if line.len().saturating_add(consumed) > MAX_LINE_BYTES {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("line exceeds the {MAX_LINE_BYTES}-byte read limit"),
            ));
        }
        line.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if line.last() == Some(&b'\n') {
            return Ok(Some(line));
        }
    }
}

fn strip_line_ending(line: &mut Vec<u8>, encoding: TextEncoding) -> bool {
    match encoding {
        TextEncoding::Utf8 => {
            if line.last() != Some(&b'\n') {
                return false;
            }
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        TextEncoding::Legacy(_) => {
            if line.last() != Some(&b'\n') {
                return false;
            }
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        TextEncoding::Utf16Le => {
            if !line.ends_with(&[0x0a, 0x00]) {
                return false;
            }
            line.truncate(line.len() - 2);
            if line.ends_with(&[0x0d, 0x00]) {
                line.truncate(line.len() - 2);
            }
        }
        TextEncoding::Utf16Be => {
            if !line.ends_with(&[0x00, 0x0a]) {
                return false;
            }
            line.truncate(line.len() - 2);
            if line.ends_with(&[0x00, 0x0d]) {
                line.truncate(line.len() - 2);
            }
        }
        TextEncoding::Utf32Le => {
            if !line.ends_with(&[0x0a, 0x00, 0x00, 0x00]) {
                return false;
            }
            line.truncate(line.len() - 4);
            if line.ends_with(&[0x0d, 0x00, 0x00, 0x00]) {
                line.truncate(line.len() - 4);
            }
        }
        TextEncoding::Utf32Be => {
            if !line.ends_with(&[0x00, 0x00, 0x00, 0x0a]) {
                return false;
            }
            line.truncate(line.len() - 4);
            if line.ends_with(&[0x00, 0x00, 0x00, 0x0d]) {
                line.truncate(line.len() - 4);
            }
        }
    }
    true
}

fn decode_text_line(bytes: &[u8], encoding: TextEncoding) -> String {
    match encoding {
        TextEncoding::Utf8 => String::from_utf8_lossy(bytes).into_owned(),
        TextEncoding::Legacy(encoding) => {
            encoding.decode_without_bom_handling(bytes).0.into_owned()
        }
        TextEncoding::Utf16Le | TextEncoding::Utf16Be => {
            let units = bytes
                .chunks_exact(2)
                .map(|pair| match encoding {
                    TextEncoding::Utf16Le => u16::from_le_bytes([pair[0], pair[1]]),
                    TextEncoding::Utf16Be => u16::from_be_bytes([pair[0], pair[1]]),
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>();
            String::from_utf16_lossy(&units)
        }
        TextEncoding::Utf32Le | TextEncoding::Utf32Be => bytes
            .chunks_exact(4)
            .map(|word| {
                let code_point = match encoding {
                    TextEncoding::Utf32Le => {
                        u32::from_le_bytes([word[0], word[1], word[2], word[3]])
                    }
                    TextEncoding::Utf32Be => {
                        u32::from_be_bytes([word[0], word[1], word[2], word[3]])
                    }
                    _ => unreachable!(),
                };
                char::from_u32(code_point).unwrap_or('\u{fffd}')
            })
            .collect(),
    }
}

fn open_regular_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options.open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    struct TempWorkspace(PathBuf);

    impl TempWorkspace {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("canopy-read-file-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn returns_supported_images_as_bounded_inline_data() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("picture.png");
        std::fs::write(&path, [0u8, 1, 2]).unwrap();
        let tool = ReadFileTool::new(&workspace.0).unwrap();

        let output = tool
            .execute_with_modalities(
                &json!({"file_path":path.to_str().unwrap()}),
                InputModalities {
                    image: true,
                    ..InputModalities::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(output.output, "Read image file: picture.png");
        assert_eq!(output.parts[0]["inlineData"]["mimeType"], "image/png");
        assert_eq!(output.parts[0]["inlineData"]["data"], "AAEC");
        assert_eq!(output.parts[0]["inlineData"]["displayName"], "picture.png");
        assert!(
            tool.execute(&json!({"file_path":path.to_str().unwrap()}))
                .await
                .unwrap()
                .contains("does not support image input")
        );
    }

    #[tokio::test]
    async fn detects_additional_media_text_overrides_and_binary_files() {
        let workspace = TempWorkspace::new();
        let bitmap = workspace.0.join("picture.bmp");
        std::fs::write(&bitmap, [1u8, 2, 3, 4]).unwrap();
        let typescript = workspace.0.join("module.ts");
        std::fs::write(&typescript, "export const value = 1;\n").unwrap();
        let kotlin = workspace.0.join("source.kt");
        std::fs::write(&kotlin, [b'a', 0, b'b']).unwrap();
        let unknown_binary = workspace.0.join("payload.unknown");
        std::fs::write(&unknown_binary, [0u8, 1, 2, 3]).unwrap();
        let tool = ReadFileTool::new(&workspace.0).unwrap();

        let image = tool
            .execute_with_modalities(
                &json!({"file_path":bitmap}),
                InputModalities {
                    image: true,
                    ..InputModalities::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(image.parts[0]["inlineData"]["mimeType"], "image/bmp");
        assert_eq!(
            tool.execute(&json!({"file_path":typescript}))
                .await
                .unwrap(),
            "export const value = 1;\n"
        );
        assert_eq!(
            tool.execute(&json!({"file_path":kotlin})).await.unwrap(),
            "a\0b"
        );
        assert!(
            tool.execute(&json!({"file_path":unknown_binary}))
                .await
                .unwrap()
                .contains("Cannot display content of binary file")
        );
    }

    #[tokio::test]
    async fn rejects_media_that_exceeds_the_base64_data_uri_limit() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("large.png");
        std::fs::write(&path, vec![0u8; MAX_INLINE_BASE64_BYTES]).unwrap();
        let tool = ReadFileTool::new(&workspace.0).unwrap();

        let error = tool
            .execute_with_modalities(
                &json!({"file_path":path.to_str().unwrap()}),
                InputModalities {
                    image: true,
                    ..InputModalities::default()
                },
            )
            .await
            .unwrap_err();
        assert!(error.contains("exceeds the 10MB data URI limit"));
    }

    #[tokio::test]
    async fn reads_a_requested_zero_based_line_range() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("notes.txt");
        std::fs::write(&path, "zero\none\ntwo\n").unwrap();
        let file_read_cache = FileReadCache::default();
        let tool = ReadFileTool::new_with_cache(&workspace.0, file_read_cache.clone()).unwrap();

        let content = tool
            .execute(&json!({"file_path":path,"offset":1,"limit":1}))
            .await
            .unwrap();

        assert!(content.starts_with("one\n"));
        assert!(content.contains("offset 2"));
        file_read_cache.ensure_prior_read(&path, "editing").unwrap();
        std::fs::write(&path, "changed on disk").unwrap();
        assert!(
            file_read_cache
                .ensure_prior_read(&path, "editing")
                .unwrap_err()
                .contains("changed since it was read")
        );
    }

    #[tokio::test]
    async fn streams_deep_line_ranges_from_a_large_utf8_file() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("large.txt");
        let line_count = 5_300_000;
        std::fs::write(&path, b"x\n".repeat(line_count)).unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() > 10 * 1024 * 1024);
        let tool = ReadFileTool::new(&workspace.0).unwrap();

        let content = tool
            .execute(&json!({"file_path":path,"offset":5_200_000,"limit":1}))
            .await
            .unwrap();

        assert!(content.starts_with("x\n"));
        assert!(content.contains("Continue with offset 5200001"));
    }

    #[tokio::test]
    async fn unchanged_fast_path_requires_a_full_read_still_in_history() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("notes.txt");
        std::fs::write(&path, "alpha\nbeta\n").unwrap();
        let cache = FileReadCache::default();
        let tool = ReadFileTool::new_with_cache(&workspace.0, cache.clone()).unwrap();

        assert_eq!(
            tool.execute(&json!({"file_path":path})).await.unwrap(),
            "alpha\nbeta\n"
        );
        let unchanged = tool.execute(&json!({"file_path":path})).await.unwrap();
        assert!(unchanged.contains("unchanged since last read in this session"));
        assert!(unchanged.contains("notes.txt"));

        let metadata = std::fs::metadata(&path).unwrap();
        assert!(cache.mark_read_evicted_from_history(&metadata));
        assert_eq!(
            tool.execute(&json!({"file_path":path})).await.unwrap(),
            "alpha\nbeta\n"
        );
    }

    #[tokio::test]
    async fn unchanged_fast_path_skips_auto_memory_and_disabled_cache() {
        let workspace = TempWorkspace::new();
        let memory_dir = workspace.0.join(".canopy/memory");
        std::fs::create_dir_all(&memory_dir).unwrap();
        let memory_path = memory_dir.join("MEMORY.md");
        std::fs::write(&memory_path, "remember this\n").unwrap();
        let cache = FileReadCache::default();
        let tool = ReadFileTool::new_with_cache(&workspace.0, cache.clone()).unwrap();

        tool.execute(&json!({"file_path":memory_path}))
            .await
            .unwrap();
        assert_eq!(
            tool.execute(&json!({"file_path":memory_path}))
                .await
                .unwrap(),
            "remember this\n"
        );

        let ordinary_path = workspace.0.join("ordinary.txt");
        std::fs::write(&ordinary_path, "read each time\n").unwrap();
        let uncached = ReadFileTool::new_with_cache(&workspace.0, cache.clone())
            .unwrap()
            .with_cache_enabled(false);
        assert_eq!(
            uncached
                .execute(&json!({"file_path":ordinary_path}))
                .await
                .unwrap(),
            "read each time\n"
        );
        assert!(matches!(
            cache.check(&std::fs::metadata(ordinary_path).unwrap()),
            crate::file_read_cache::FileReadCheckResult::Unknown
        ));
    }

    #[tokio::test]
    async fn strips_utf8_bom_and_records_empty_out_of_range_reads_as_partial() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("notes.txt");
        std::fs::write(&path, b"\xef\xbb\xbfalpha\nbeta\n").unwrap();
        let cache = FileReadCache::default();
        let tool = ReadFileTool::new_with_cache(&workspace.0, cache.clone()).unwrap();

        assert_eq!(
            tool.execute(&json!({"file_path":path})).await.unwrap(),
            "alpha\nbeta\n"
        );
        assert_eq!(
            tool.execute(&json!({"file_path":path,"offset":20,"limit":2}))
                .await
                .unwrap(),
            ""
        );
        let partial = workspace.0.join("partial.txt");
        std::fs::write(&partial, "one\ntwo\n").unwrap();
        assert_eq!(
            tool.execute(&json!({"file_path":partial,"offset":20,"limit":2}))
                .await
                .unwrap(),
            ""
        );
        let metadata = std::fs::metadata(partial).unwrap();
        match cache.check(&metadata) {
            crate::file_read_cache::FileReadCheckResult::Fresh(entry) => {
                assert!(!entry.last_read_was_full);
                assert!(entry.last_read_cacheable);
            }
            other => panic!("expected a fresh cache entry, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reads_utf16_and_utf32_bom_text_in_both_byte_orders() {
        let workspace = TempWorkspace::new();
        let cases = [
            ("utf16le.txt", encode_utf16_bom("alpha\r\nbeta\n", true)),
            ("utf16be.txt", encode_utf16_bom("alpha\r\nbeta\n", false)),
            ("utf32le.txt", encode_utf32_bom("alpha\r\nbeta\n", true)),
            ("utf32be.txt", encode_utf32_bom("alpha\r\nbeta\n", false)),
        ];
        let tool = ReadFileTool::new(&workspace.0).unwrap();

        for (name, bytes) in cases {
            let path = workspace.0.join(name);
            std::fs::write(&path, bytes).unwrap();
            let content = tool.execute(&json!({"file_path":path})).await.unwrap();
            assert_eq!(content, "alpha\nbeta\n", "encoding fixture {name}");
        }
    }

    #[tokio::test]
    async fn decodes_detected_legacy_single_byte_text_without_a_bom() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("legacy-encoding.txt");
        std::fs::write(&path, [b'a', 0xff, b'\n']).unwrap();
        let tool = ReadFileTool::new(&workspace.0).unwrap();

        assert_eq!(
            tool.execute(&json!({"file_path":path})).await.unwrap(),
            "aÿ\n"
        );
    }

    #[tokio::test]
    async fn detects_and_decodes_multibyte_legacy_text_without_a_bom() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("legacy-multibyte.txt");
        let mut bytes = Vec::new();
        for _ in 0..12 {
            bytes.extend_from_slice(&[0xd6, 0xd0, 0xce, 0xc4]);
        }
        std::fs::write(&path, bytes).unwrap();
        let tool = ReadFileTool::new(&workspace.0).unwrap();

        assert_eq!(
            tool.execute(&json!({"file_path":path})).await.unwrap(),
            "中文".repeat(12)
        );
    }

    #[tokio::test]
    async fn rejects_notebook_paging_and_validates_pdf_page_reads() {
        let workspace = TempWorkspace::new();
        let notebook = workspace.0.join("notebook.ipynb");
        let tool = ReadFileTool::new(&workspace.0).unwrap();
        assert!(
            tool.execute(&json!({"file_path":notebook,"offset":0,"limit":5}))
                .await
                .unwrap_err()
                .contains("not supported for Jupyter notebook")
        );
        std::fs::write(&notebook, r#"{"cells":[],"metadata":{},"nbformat":4}"#).unwrap();
        let cache = FileReadCache::default();
        let notebook_reader = ReadFileTool::new_with_cache(&workspace.0, cache.clone()).unwrap();
        let notebook_output = notebook_reader
            .execute(&json!({"file_path":notebook}))
            .await
            .unwrap();
        assert_eq!(notebook_output, "(empty notebook)");
        assert!(
            cache
                .ensure_prior_read(&notebook, "editing")
                .unwrap_err()
                .contains("non-text payload")
        );

        let pdf = workspace.0.join("document.pdf");
        std::fs::write(&pdf, b"%PDF-1.4\n").unwrap();
        assert!(
            tool.execute(&json!({"file_path":pdf,"pages":"3-"}))
                .await
                .unwrap_err()
                .contains("Open-ended page ranges")
        );
        assert!(
            tool.execute(&json!({"file_path":pdf,"pages":"1-21"}))
                .await
                .unwrap_err()
                .contains("maximum of 20 pages")
        );
        assert!(
            tool.execute(&json!({"file_path":pdf,"pages":"1-3"}))
                .await
                .unwrap_err()
                .contains("pdftotext")
        );
    }

    #[tokio::test]
    async fn reads_selected_pdf_page_text_through_read_file_when_poppler_is_installed() {
        let available = std::process::Command::new("pdftotext")
            .arg("-v")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|status| status.success());
        if !available {
            return;
        }

        let workspace = TempWorkspace::new();
        let path = workspace.0.join("document.pdf");
        std::fs::write(
            &path,
            crate::pdf::simple_text_pdf_fixture("Read file PDF integration"),
        )
        .unwrap();
        let tool = ReadFileTool::new(&workspace.0).unwrap();

        let content = tool
            .execute(&json!({"file_path":path,"pages":"1"}))
            .await
            .unwrap();

        assert!(content.contains("Read file PDF integration"));

        let full_content = tool.execute(&json!({"file_path":path})).await.unwrap();
        assert!(full_content.contains("Read file PDF integration"));
    }

    #[tokio::test]
    async fn rejects_paths_that_resolve_outside_the_workspace() {
        let workspace = TempWorkspace::new();
        let outside = std::env::temp_dir().join(format!("canopy-outside-{}.txt", Uuid::new_v4()));
        std::fs::write(&outside, "secret").unwrap();
        let tool = ReadFileTool::new(&workspace.0).unwrap();

        let error = tool
            .execute(&json!({"file_path":outside}))
            .await
            .unwrap_err();

        let _ = std::fs::remove_file(outside);
        assert!(error.contains("restricted to files inside the workspace"));
    }

    #[tokio::test]
    async fn rejects_canopy_ignored_files_and_unescapes_shell_paths() {
        let workspace = TempWorkspace::new();
        std::fs::write(workspace.0.join(".canopyignore"), "ignored/\n").unwrap();
        let ignored_dir = workspace.0.join("ignored");
        std::fs::create_dir_all(&ignored_dir).unwrap();
        let ignored_path = ignored_dir.join("file.txt");
        std::fs::write(&ignored_path, "hidden from the model").unwrap();
        let spaced_path = workspace.0.join("visible file.txt");
        std::fs::write(&spaced_path, "readable").unwrap();
        let tool = ReadFileTool::new(&workspace.0).unwrap();

        let error = tool
            .execute(&json!({"file_path":ignored_path}))
            .await
            .unwrap_err();
        assert!(error.contains("is ignored by .canopyignore pattern(s)"));

        #[cfg(not(windows))]
        {
            let escaped = spaced_path.to_string_lossy().replace(' ', "\\ ");
            assert_eq!(
                tool.execute(&json!({"file_path":escaped})).await.unwrap(),
                "readable"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_a_workspace_symlink_that_targets_outside() {
        use std::os::unix::fs::symlink;

        let workspace = TempWorkspace::new();
        let outside = std::env::temp_dir().join(format!("canopy-symlink-{}.txt", Uuid::new_v4()));
        std::fs::write(&outside, "secret").unwrap();
        let link = workspace.0.join("linked.txt");
        symlink(&outside, &link).unwrap();
        let tool = ReadFileTool::new(&workspace.0).unwrap();

        let error = tool.execute(&json!({"file_path":link})).await.unwrap_err();

        let _ = std::fs::remove_file(outside);
        assert!(error.contains("restricted to files inside the workspace"));
    }

    fn encode_utf16_bom(text: &str, little_endian: bool) -> Vec<u8> {
        let mut bytes = if little_endian {
            vec![0xff, 0xfe]
        } else {
            vec![0xfe, 0xff]
        };
        for unit in text.encode_utf16() {
            bytes.extend(if little_endian {
                unit.to_le_bytes()
            } else {
                unit.to_be_bytes()
            });
        }
        bytes
    }

    fn encode_utf32_bom(text: &str, little_endian: bool) -> Vec<u8> {
        let mut bytes = if little_endian {
            vec![0xff, 0xfe, 0x00, 0x00]
        } else {
            vec![0x00, 0x00, 0xfe, 0xff]
        };
        for character in text.chars() {
            let code_point = character as u32;
            bytes.extend(if little_endian {
                code_point.to_le_bytes()
            } else {
                code_point.to_be_bytes()
            });
        }
        bytes
    }
}
