//! Conversion of Gemini extension manifests to Canopy extension configs.
//!
//! This mirrors `convertGeminiToCanopyConfig` in
//! `packages/core/src/extension/gemini-converter.ts`. The manifest's mapped
//! values remain arbitrary JSON, as the TypeScript converter passes them
//! through without interpreting them.

use serde_json::{Map, Value};
use std::fs;
use std::path::{Path, PathBuf};

const GEMINI_MANIFEST_FILE: &str = "gemini-extension.json";

/// Errors produced while reading and converting a Gemini extension manifest.
#[derive(Debug, thiserror::Error)]
pub enum GeminiConfigError {
    #[error(
        "Gemini extension config at {config_file_path} resolves through a symlink outside the extension"
    )]
    ManifestOutsideExtension { config_file_path: PathBuf },
    #[error("failed to read Gemini extension config at {path}: {io_error}")]
    ReadManifest {
        path: PathBuf,
        #[source]
        io_error: std::io::Error,
    },
    #[error("failed to parse Gemini extension config at {path}: {json_error}")]
    ParseManifest {
        path: PathBuf,
        #[source]
        json_error: serde_json::Error,
    },
    #[error("Gemini extension config must have name and version fields")]
    MissingRequiredFields,
}

/// Read and convert `gemini-extension.json` from an extension directory.
///
/// The manifest and extension directory are canonicalized before the file is
/// read. Missing paths and broken symlinks fail the same confinement check as
/// links that resolve outside the extension. Output contains only the fields
/// directly mapped by the TypeScript converter; absent optional fields are
/// omitted, while present values (including `null` and values of unexpected
/// JSON types) are preserved.
pub fn convert_gemini_to_canopy_config(
    extension_dir: impl AsRef<Path>,
) -> Result<Value, GeminiConfigError> {
    let extension_dir = extension_dir.as_ref();
    let config_file_path = extension_dir.join(GEMINI_MANIFEST_FILE);

    if !real_path_within(&config_file_path, extension_dir) {
        return Err(GeminiConfigError::ManifestOutsideExtension { config_file_path });
    }

    let bytes =
        fs::read(&config_file_path).map_err(|io_error| GeminiConfigError::ReadManifest {
            path: config_file_path.clone(),
            io_error,
        })?;
    // Node's `readFileSync(path, 'utf-8')` replaces malformed UTF-8 sequences
    // before JSON.parse sees the resulting string.
    let contents = String::from_utf8_lossy(&bytes);
    let gemini_config: Value =
        serde_json::from_str(&contents).map_err(|json_error| GeminiConfigError::ParseManifest {
            path: config_file_path.clone(),
            json_error,
        })?;

    if !gemini_config.get("name").is_some_and(js_truthy)
        || !gemini_config.get("version").is_some_and(js_truthy)
    {
        return Err(GeminiConfigError::MissingRequiredFields);
    }

    let mut canopy_config = Map::new();
    for field in [
        "name",
        "version",
        "mcpServers",
        "contextFileName",
        "settings",
    ] {
        if let Some(value) = gemini_config.get(field) {
            canopy_config.insert(field.to_owned(), value.clone());
        }
    }

    Ok(Value::Object(canopy_config))
}

/// Check path-component containment for already-resolved absolute paths.
///
/// This is the Rust counterpart to the TypeScript converter's exported
/// `isPathWithin`; component-aware comparison avoids accepting a sibling such
/// as `/extensions-evil` as a child of `/extensions`.
pub fn is_path_within(path: &Path, root: &Path) -> bool {
    path == root || path.starts_with(root)
}

/// Return whether an existing path resolves through symlinks inside an
/// existing extension root.
pub fn real_path_within(target: &Path, root: &Path) -> bool {
    let (Ok(real_target), Ok(real_root)) = (fs::canonicalize(target), fs::canonicalize(root))
    else {
        return false;
    };

    is_path_within(&real_target, &real_root)
}

/// Detect whether a directory has a Gemini extension manifest.
///
/// Missing manifests and manifests that escape the directory through a
/// symlink return `Ok(false)`. As in the TypeScript detector, I/O and malformed
/// JSON errors are surfaced to the caller, while a valid non-object or an
/// object without string-valued `name` and `version` fields returns false.
pub fn is_gemini_extension_config(
    extension_dir: impl AsRef<Path>,
) -> Result<bool, GeminiConfigError> {
    let extension_dir = extension_dir.as_ref();
    let config_file_path = extension_dir.join(GEMINI_MANIFEST_FILE);
    if !config_file_path.exists() {
        return Ok(false);
    }
    if !real_path_within(&config_file_path, extension_dir) {
        return Ok(false);
    }

    let bytes =
        fs::read(&config_file_path).map_err(|io_error| GeminiConfigError::ReadManifest {
            path: config_file_path.clone(),
            io_error,
        })?;
    let contents = String::from_utf8_lossy(&bytes);
    let parsed_config: Value =
        serde_json::from_str(&contents).map_err(|json_error| GeminiConfigError::ParseManifest {
            path: config_file_path,
            json_error,
        })?;

    Ok(parsed_config.get("name").is_some_and(Value::is_string)
        && parsed_config.get("version").is_some_and(Value::is_string))
}

/// JavaScript truthiness for JSON-representable values.
fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        // JavaScript treats empty arrays and objects as truthy.
        Value::Array(_) | Value::Object(_) => true,
    }
}
