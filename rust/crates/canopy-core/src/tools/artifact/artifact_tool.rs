//! Permission-aware tool for publishing self-contained interactive HTML.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tokio::io::AsyncReadExt;

use crate::browser_launch::BrowserLaunchOptions;
use crate::tool_response_finalizer::ToolExecutionOutput;
use crate::utils::cancellation::CancellationToken;

use super::create_publisher::create_artifact_publisher;
use super::html::{
    MAX_ARTIFACT_BYTES, byte_length, sanitize_artifact_title, validate_self_contained,
    wrap_artifact_html,
};
use super::publisher::{
    ArtifactPublisher, ArtifactPublisherConfig, ArtifactPublisherKind, PublishArtifactInput,
    artifact_id_from_path,
};

const DESCRIPTION: &str = "Publishes a self-contained HTML page as an interactive Artifact, optionally opens it in the browser depending on settings, and returns a shareable link when a remote host is configured. Use it to turn session output into a durable, interactive page — a PR walkthrough, an architecture tour, or a project dashboard.\n\nWorkflow:\n- Write the page to a file first, then call Artifact with that file's absolute path.\n- Write a BODY-ONLY fragment: no <!doctype>, <html>, <head>, or <body> tags — they are added at publish time, along with a minimal CSS reset.\n- Self-contained only: inline all CSS and JS; embed images/fonts as data: URIs. No external scripts, stylesheets, fonts, or remote images.\n- Responsive: use relative units, flex/grid, and max-width:100% on media; wide content (tables, diagrams, code) should scroll inside its own overflow-x:auto container.\n- Set a concise title; it names the browser tab.\n\nTo update an artifact, call Artifact again with the same file path: it redeploys to the same URL. A different path creates a separate Artifact.\n\nSet artifact.autoOpen=false in settings.json, or CANOPY_ARTIFACT_NO_AUTO_OPEN=1, to publish without launching a browser.";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactToolParams {
    pub file_path: PathBuf,
    pub title: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ArtifactToolConfig {
    pub auto_open: bool,
    pub publisher: ArtifactPublisherConfig,
}

pub struct ArtifactTool {
    publisher: Box<dyn ArtifactPublisher>,
    auto_open: bool,
}

impl ArtifactTool {
    pub fn new(config: ArtifactToolConfig) -> Self {
        Self::with_publisher(
            config.auto_open,
            create_artifact_publisher(config.publisher),
        )
    }

    pub fn with_publisher(auto_open: bool, publisher: Box<dyn ArtifactPublisher>) -> Self {
        let auto_open = auto_open
            && std::env::var("CANOPY_ARTIFACT_NO_AUTO_OPEN").map_or(true, |value| value != "1")
            && crate::browser_launch::should_attempt_browser_launch();
        Self {
            publisher,
            auto_open,
        }
    }

    pub fn function_declaration() -> Value {
        json!({
            "name": "artifact",
            "description": DESCRIPTION,
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "file_path": {
                        "type": "STRING",
                        "description": "Absolute path to the body-only HTML fragment file to publish."
                    },
                    "title": {
                        "type": "STRING",
                        "description": "Concise title for the artifact (names the browser tab and listing)."
                    }
                },
                "required": ["file_path"]
            }
        })
    }

    /// Artifact publishing always requires user approval because it writes to
    /// a global artifact directory or uploads the page to a remote service.
    pub fn default_permission(&self) -> &'static str {
        "ask"
    }

    pub fn confirmation_prompt(&self, file_path: &Path) -> String {
        let path = file_path.display();
        if self.publisher.kind() == ArtifactPublisherKind::Local {
            let open_suffix = if self.auto_open {
                " and open it in your browser"
            } else {
                ""
            };
            format!("Publish {path} as an interactive Artifact{open_suffix}.")
        } else {
            let backend = match self.publisher.kind() {
                ArtifactPublisherKind::Host => "custom upload",
                ArtifactPublisherKind::Oss => "oss",
                ArtifactPublisherKind::Local => "local",
            };
            let open_suffix = if self.auto_open {
                " and open its shareable link in your browser"
            } else {
                ""
            };
            format!(
                "Publish {path} as an interactive Artifact. This uploads the page to a remote host ({backend}){open_suffix}."
            )
        }
    }

    pub fn parse_params(args: &Value) -> Result<ArtifactToolParams, String> {
        let object = args
            .as_object()
            .ok_or_else(|| "Artifact arguments must be an object.".to_owned())?;
        let raw_path = object
            .get("file_path")
            .and_then(Value::as_str)
            .ok_or_else(|| "Missing or invalid \"file_path\".".to_owned())?;
        let raw_path = unescape_path(raw_path.trim());
        if raw_path.is_empty() {
            return Err("Missing or empty \"file_path\"".to_owned());
        }
        let file_path = PathBuf::from(raw_path);
        if !file_path.is_absolute() {
            return Err(format!(
                "File path must be absolute: {}",
                file_path.display()
            ));
        }
        let title = object
            .get("title")
            .filter(|value| !value.is_null())
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| "Artifact title must be a string.".to_owned())
            })
            .transpose()?;
        Ok(ArtifactToolParams { file_path, title })
    }

    pub async fn execute_args(
        &self,
        args: &Value,
        cancellation: &CancellationToken,
    ) -> Result<ToolExecutionOutput, String> {
        let params = Self::parse_params(args)?;
        self.execute(&params, cancellation).await
    }

    pub async fn execute(
        &self,
        params: &ArtifactToolParams,
        cancellation: &CancellationToken,
    ) -> Result<ToolExecutionOutput, String> {
        if cancellation.is_cancelled() {
            return Ok(cancelled_result());
        }
        let fragment = match read_bounded(&params.file_path, cancellation).await {
            Ok(Some(fragment)) => fragment,
            Ok(None) => return Ok(cancelled_result()),
            Err(ReadFailure::TooLarge) => {
                return Err(format!(
                    "Artifact is too large (source exceeds the {MAX_ARTIFACT_BYTES} byte limit). Trim the content or split it across multiple artifacts."
                ));
            }
            Err(ReadFailure::NotFound) => {
                return Err(format!(
                    "Artifact source file not found: {}. Write the page content to this file first.",
                    params.file_path.display()
                ));
            }
            Err(ReadFailure::Other(error)) => {
                return Err(format!(
                    "Error reading artifact source file '{}': {error}",
                    params.file_path.display()
                ));
            }
        };
        let fragment = String::from_utf8_lossy(&fragment).into_owned();
        if let Some(error) = validate_self_contained(&fragment) {
            return Err(error);
        }

        let title_input = params
            .title
            .as_deref()
            .map(str::to_owned)
            .unwrap_or_else(|| title_from_path(&params.file_path));
        let title = sanitize_artifact_title(Some(&title_input));
        let html = wrap_artifact_html(&fragment, Some(&title));
        let size_bytes = byte_length(&html);
        if size_bytes > MAX_ARTIFACT_BYTES {
            return Err(format!(
                "Artifact is too large ({size_bytes} bytes > {MAX_ARTIFACT_BYTES} byte limit). Trim the content or split it across multiple artifacts."
            ));
        }
        let id = artifact_id_from_path(&params.file_path)?;
        let published = match self
            .publisher
            .publish(
                &PublishArtifactInput {
                    id,
                    title: title.clone(),
                    html,
                },
                cancellation,
            )
            .await
        {
            Ok(published) => published,
            Err(_) if cancellation.is_cancelled() => return Ok(cancelled_result()),
            Err(error) if error == "Artifact publishing was cancelled." => {
                return Ok(cancelled_result());
            }
            Err(error) => return Err(format!("Failed to publish artifact: {error}")),
        };

        if self.auto_open && !cancellation.is_cancelled() {
            let allowed_file_paths = published
                .file_path
                .as_ref()
                .map(|path| PathBuf::from(path))
                .into_iter()
                .collect();
            if let Err(error) = crate::browser_launch::open_browser_securely_with_options(
                &published.url,
                BrowserLaunchOptions {
                    allow_file: true,
                    allowed_file_paths,
                },
            ) {
                eprintln!("Failed to open browser for artifact \"{title}\": {error}");
            }
        }

        let llm_content = format!(
            "Published artifact \"{title}\" to {}. Share or open this URL to view the interactive page. Re-run Artifact with the same file path to update it.",
            published.url
        );
        let mut output = ToolExecutionOutput::with_display(
            llm_content,
            json!({
                "displayText": format!("Published artifact **{title}**\n\n{}", published.url)
            }),
        );
        if let Some(file_path) = published.file_path.as_deref() {
            output.result_file_paths.push(file_path.to_owned());
        }
        output.artifacts.push(json!({
            "kind": "html",
            "storage": "published",
            "title": title,
            "url": published.url,
            "managedId": published.id,
            "mimeType": "text/html",
            "sizeBytes": size_bytes,
        }));
        Ok(output)
    }
}

enum ReadFailure {
    TooLarge,
    NotFound,
    Other(String),
}

async fn read_bounded(
    path: &Path,
    cancellation: &CancellationToken,
) -> Result<Option<Vec<u8>>, ReadFailure> {
    let read = async {
        let file = tokio::fs::File::open(path).await.map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                ReadFailure::NotFound
            } else {
                ReadFailure::Other(error.to_string())
            }
        })?;
        let mut file = file.take((MAX_ARTIFACT_BYTES + 1) as u64);
        let mut bytes = Vec::with_capacity(MAX_ARTIFACT_BYTES.min(64 * 1024));
        file.read_to_end(&mut bytes)
            .await
            .map_err(|error| ReadFailure::Other(error.to_string()))?;
        if bytes.len() > MAX_ARTIFACT_BYTES {
            return Err(ReadFailure::TooLarge);
        }
        Ok(bytes)
    };
    tokio::select! {
        _ = cancellation.cancelled() => Ok(None),
        result = read => result.map(Some),
    }
}

fn title_from_path(path: &Path) -> String {
    let basename = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_default();
    let name = basename.as_ref();
    if name.to_ascii_lowercase().ends_with(".html") {
        name[..name.len() - 5].to_owned()
    } else if name.to_ascii_lowercase().ends_with(".htm") {
        name[..name.len() - 4].to_owned()
    } else {
        basename.into_owned()
    }
}

fn unescape_path(value: &str) -> String {
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
                && characters.peek().is_some_and(|next| {
                    matches!(*next, ' ' | '\t')
                        || matches!(
                            *next,
                            '(' | ')'
                                | '['
                                | ']'
                                | '{'
                                | '}'
                                | ';'
                                | '|'
                                | '*'
                                | '?'
                                | '$'
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
                        || *next as u32 == 0x60
                })
            {
                if let Some(next) = characters.next() {
                    output.push(next);
                }
            } else {
                output.push(character);
            }
        }
        output
    }
}

fn cancelled_result() -> ToolExecutionOutput {
    ToolExecutionOutput::text("Artifact publishing was cancelled.")
}
