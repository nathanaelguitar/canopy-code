//! Register an artifact's metadata with the active session.
//!
//! This tool never reads, writes, uploads, publishes, or verifies the target.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::tool_response_finalizer::ToolExecutionOutput;

pub const ARTIFACT_TITLE_MAX_LENGTH: usize = 200;
pub const ARTIFACT_WORKSPACE_PATH_MAX_LENGTH: usize = 500;
const ARTIFACT_DESCRIPTION_MAX_LENGTH: usize = 1_000;
const ARTIFACT_MANAGED_ID_MAX_LENGTH: usize = 200;
const ARTIFACT_MIME_TYPE_MAX_LENGTH: usize = 120;
const ARTIFACT_METADATA_MAX_BYTES: usize = 4_096;

const DESCRIPTION: &str = "Registers a session artifact so clients can show it in an artifacts panel. Use it after creating a useful file, URL, image, report, notebook, or other intermediate result that the user may want to open later, unless the producing tool already returned artifact metadata. For example, write_file automatically records HTML, image, PDF, and notebook files it writes inside the workspace, so do not call record_artifact again for the same workspacePath; still call it for other formats such as Markdown, CSV, JSON, and plain text, and for files produced outside write_file. When the session creates a remote resource, such as a pull request, issue, or comment submitted via gh, record its URL with kind \"link\" and the url locator so the user can reopen it later.\n\nThis tool only records metadata. It does not publish, upload, read, write, or verify the referenced resource. Provide exactly one locator: workspacePath, managedId, or url. Use the Artifact tool, not record_artifact, for published interactive HTML artifacts.";

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordArtifactParams {
    #[serde(default)]
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Map<String, Value>>,
}

pub struct RecordArtifactTool {
    workspace_root: PathBuf,
}

impl RecordArtifactTool {
    pub fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            workspace_root: workspace_root.into(),
        }
    }

    pub fn function_declaration() -> Value {
        function_declaration()
    }

    pub fn execute(&self, args: &Value) -> Result<ToolExecutionOutput, String> {
        let params = parse_params(args)?;
        let artifact = build_artifact(params, &self.workspace_root)?;
        let title = artifact["title"].as_str().unwrap_or_default();
        Ok(ToolExecutionOutput {
            output: format!("Recorded artifact \"{title}\"."),
            display: Some(json!({"displayText":format!("Recorded artifact **{title}**.")})),
            artifacts: vec![artifact],
            ..ToolExecutionOutput::default()
        })
    }
}

pub fn function_declaration() -> Value {
    json!({
        "name":"record_artifact",
        "description":DESCRIPTION,
        "parameters":{
            "type":"OBJECT",
            "properties":{
                "title":{"type":"STRING","description":"Concise title shown in the client artifact list."},
                "kind":{"type":"STRING","enum":["file","link","html","image","video","audio","pdf","notebook","other"],"description":"Best-effort artifact type for client rendering."},
                "storage":{"type":"STRING","enum":["workspace","external_url","managed"],"description":"Storage class. Omit it to infer from the provided locator."},
                "description":{"type":"STRING","description":"Optional short description for the user."},
                "workspacePath":{"type":"STRING","description":"Workspace-relative path for a file produced in the current workspace."},
                "managedId":{"type":"STRING","description":"Opaque identifier for a resource managed by an extension or tool."},
                "url":{"type":"STRING","description":"HTTP or HTTPS URL that the user can open for details."},
                "mimeType":{"type":"STRING","description":"Optional MIME type."},
                "sizeBytes":{"type":"INTEGER","minimum":0,"description":"Optional size in bytes."},
                "metadata":{"type":"OBJECT","additionalProperties":{"anyOf":[{"type":"STRING"},{"type":"NUMBER"},{"type":"BOOLEAN"},{"type":"NULL"}]},"description":"Small primitive metadata bag for client-specific display hints."}
            },
            "required":["title"]
        }
    })
}

pub fn parse_params(args: &Value) -> Result<RecordArtifactParams, String> {
    let object = args
        .as_object()
        .ok_or_else(|| "record_artifact arguments must be an object".to_owned())?;
    let mut params: RecordArtifactParams = serde_json::from_value(Value::Object(object.clone()))
        .map_err(|_| "record_artifact arguments have invalid field types".to_owned())?;
    params.title = params.title.trim().to_owned();

    validate_string(
        Some(&params.title),
        "title",
        ARTIFACT_TITLE_MAX_LENGTH,
        true,
    )?;
    params.description = trim_optional(params.description);
    params.workspace_path = trim_optional(params.workspace_path);
    params.managed_id = trim_optional(params.managed_id);
    params.url = trim_optional(params.url);
    params.mime_type = trim_optional(params.mime_type);

    validate_string(
        params.description.as_deref(),
        "description",
        ARTIFACT_DESCRIPTION_MAX_LENGTH,
        false,
    )?;
    validate_string(
        params.mime_type.as_deref(),
        "mimeType",
        ARTIFACT_MIME_TYPE_MAX_LENGTH,
        false,
    )?;
    if let Some(kind) = params.kind.as_deref()
        && !matches!(
            kind,
            "file" | "link" | "html" | "image" | "video" | "audio" | "pdf" | "notebook" | "other"
        )
    {
        return Err("\"kind\" must be a supported artifact kind".to_owned());
    }
    if let Some(storage) = params.storage.as_deref()
        && !matches!(storage, "workspace" | "external_url" | "managed")
    {
        return Err("\"storage\" must be workspace, external_url, or managed".to_owned());
    }

    let locator_count = [
        params.workspace_path.is_some(),
        params.managed_id.is_some(),
        params.url.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if locator_count != 1 {
        return Err(
            "Provide exactly one of \"workspacePath\", \"managedId\", or \"url\"".to_owned(),
        );
    }

    let inferred_storage = if params.workspace_path.is_some() {
        "workspace"
    } else if params.managed_id.is_some() {
        "managed"
    } else {
        "external_url"
    };
    if params
        .storage
        .as_deref()
        .is_some_and(|value| value != inferred_storage)
    {
        return Err(format!(
            "\"storage\" must be \"{inferred_storage}\" for the provided locator"
        ));
    }
    if let Some(path) = params.workspace_path.as_deref() {
        validate_workspace_path(path)?;
    }
    if let Some(id) = params.managed_id.as_deref() {
        validate_string(Some(id), "managedId", ARTIFACT_MANAGED_ID_MAX_LENGTH, true)?;
        if id.contains('/')
            || id.contains('\\')
            || id.contains("..")
            || Path::new(id).is_absolute()
            || Path::new(id).has_root()
            || windows_path_is_absolute(id)
        {
            return Err("\"managedId\" must be an opaque managed resource id".to_owned());
        }
    }
    if let Some(url) = params.url.as_deref() {
        let parsed = url::Url::parse(url).map_err(|_| "\"url\" must be a valid URL".to_owned())?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err("\"url\" must use http or https".to_owned());
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err("\"url\" must not include credentials".to_owned());
        }
    }
    if let Some(size_bytes) = params.size_bytes
        && size_bytes > 9_007_199_254_740_991
    {
        return Err("\"sizeBytes\" must be a non-negative safe integer".to_owned());
    }
    if let Some(metadata) = params.metadata.as_ref() {
        validate_metadata(metadata)?;
    }

    Ok(params)
}

fn build_artifact(params: RecordArtifactParams, workspace_root: &Path) -> Result<Value, String> {
    let storage = params.storage.unwrap_or_else(|| {
        if params.workspace_path.is_some() {
            "workspace".to_owned()
        } else if params.managed_id.is_some() {
            "managed".to_owned()
        } else {
            "external_url".to_owned()
        }
    });
    let mut artifact = Map::new();
    artifact.insert("title".to_owned(), json!(params.title));
    if let Some(kind) = params.kind {
        artifact.insert("kind".to_owned(), json!(kind));
    }
    artifact.insert("storage".to_owned(), json!(storage));
    for (key, value) in [
        ("description", params.description),
        ("workspacePath", params.workspace_path),
        ("managedId", params.managed_id),
        ("url", params.url),
        ("mimeType", params.mime_type),
    ] {
        if let Some(value) = value {
            artifact.insert(key.to_owned(), json!(value));
        }
    }
    if let Some(size_bytes) = params.size_bytes {
        artifact.insert("sizeBytes".to_owned(), json!(size_bytes));
    }
    if let Some(metadata) = params.metadata {
        artifact.insert("metadata".to_owned(), Value::Object(metadata));
    }
    // The workspace root is intentionally not used to probe the locator: this
    // tool records metadata and never verifies that a referenced file exists.
    let _ = workspace_root;
    Ok(Value::Object(artifact))
}

fn validate_workspace_path(value: &str) -> Result<(), String> {
    validate_string(
        Some(value),
        "workspacePath",
        ARTIFACT_WORKSPACE_PATH_MAX_LENGTH,
        true,
    )?;
    if Path::new(value).is_absolute()
        || windows_path_is_absolute(value)
        || value.as_bytes().get(1) == Some(&b':')
    {
        return Err("\"workspacePath\" must be relative to the workspace".to_owned());
    }
    let normalized = value.replace('\\', "/");
    let mut depth = 0i64;
    for component in normalized.split('/') {
        match component {
            "" | "." => {}
            ".." => depth -= 1,
            _ => depth += 1,
        }
        if depth < 0 {
            return Err("\"workspacePath\" must stay inside the workspace".to_owned());
        }
    }
    Ok(())
}

fn validate_metadata(metadata: &Map<String, Value>) -> Result<(), String> {
    for (key, value) in metadata {
        if key.is_empty() {
            return Err("\"metadata\" keys must not be empty".to_owned());
        }
        if utf16_length(key) > 120 {
            return Err("\"metadata\" keys must be 120 characters or fewer".to_owned());
        }
        if has_control_character(key, false) || has_unsafe_display_payload(key) {
            return Err("\"metadata\" keys contain unsafe content".to_owned());
        }
        if !(value.is_null() || value.is_string() || value.is_number() || value.is_boolean()) {
            return Err("\"metadata\" values must be primitive".to_owned());
        }
        if value.as_f64().is_some_and(|number| !number.is_finite()) {
            return Err("\"metadata\" numbers must be finite".to_owned());
        }
        if let Some(string) = value.as_str()
            && (has_control_character(string, false) || has_unsafe_display_payload(string))
        {
            return Err("\"metadata\" string values contain unsafe content".to_owned());
        }
    }
    let bytes = serde_json::to_vec(metadata).map_or(usize::MAX, |bytes| bytes.len());
    if bytes > ARTIFACT_METADATA_MAX_BYTES {
        return Err("\"metadata\" must be 4096 bytes or fewer".to_owned());
    }
    Ok(())
}

fn validate_string(
    value: Option<&str>,
    field: &str,
    max_length: usize,
    required: bool,
) -> Result<(), String> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return if required {
            Err(format!("Missing or empty \"{field}\""))
        } else {
            Ok(())
        };
    };
    if utf16_length(value) > max_length {
        return Err(format!("\"{field}\" exceeds {max_length} characters"));
    }
    if has_control_character(value, field == "description") {
        return Err(format!("\"{field}\" contains control characters"));
    }
    if matches!(
        field,
        "title" | "description" | "mimeType" | "workspacePath" | "managedId"
    ) && has_unsafe_display_payload(value)
    {
        return Err(format!("\"{field}\" contains unsafe markup"));
    }
    Ok(())
}

fn has_control_character(value: &str, allow_line_whitespace: bool) -> bool {
    value.chars().any(|character| {
        if allow_line_whitespace && matches!(character, '\t' | '\n' | '\r') {
            return false;
        }
        let code = character as u32;
        code <= 0x1f
            || code == 0x7f
            || (0x200b..=0x200f).contains(&code)
            || matches!(code, 0x2028 | 0x2029 | 0xfeff)
            || (0x202a..=0x202e).contains(&code)
            || (0x2066..=0x2069).contains(&code)
    })
}

fn has_unsafe_display_payload(value: &str) -> bool {
    static UNSAFE: OnceLock<Regex> = OnceLock::new();
    UNSAFE
        .get_or_init(|| {
            Regex::new(r#"(?i)<\s*/?[a-z!]|&(?:#[0-9]+|#x[0-9a-f]+|[a-z][a-z0-9]+);|javascript\s*:|data\s*:\s*(?:text/(?:html|javascript)|application/javascript|image/svg\+xml)|(?:^|[\s"'`<])on[a-z][a-z0-9-]*\s*="#)
                .expect("valid artifact display-safety regex")
        })
        .is_match(value)
}

fn windows_path_is_absolute(value: &str) -> bool {
    let bytes = value.as_bytes();
    (bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\'))
        || value.starts_with("\\\\")
}

fn trim_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_owned();
        (!value.is_empty()).then_some(value)
    })
}

fn utf16_length(value: &str) -> usize {
    value.encode_utf16().count()
}
