//! Permission-aware image generation saved as a workspace artifact.

use std::fs;
use std::path::{Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::providers::openai_request::InputModalities;
use crate::services::image_generation::{GeneratedImage, ImageGenerationRequest, generate_image};
use crate::storage::Storage;
use crate::tool_response_finalizer::ToolExecutionOutput;
use crate::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};
use crate::utils::cancellation::CancellationToken;

const MIN_TOTAL_PIXELS: u128 = 512 * 512;
const MAX_TOTAL_PIXELS: u128 = 2048 * 2048;
const MAX_PROMPT_CHARS: usize = 10_000;
const MAX_INLINE_IMAGE_BYTES: usize = 4 * 1024 * 1024;

/// The image model route selected by the host's settings resolver.
///
/// API keys are passed separately so host settings can resolve them from its
/// effective environment without writing credentials into settings snapshots.
#[derive(Clone)]
pub struct ImageGenerationToolConfig {
    pub model: String,
    pub base_url: String,
    pub api_key_env: String,
    pub api_key: Option<String>,
}

/// Validated arguments for one image generation call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageGenParams {
    pub prompt: String,
    pub size: Option<String>,
}

/// Native counterpart of the TypeScript `ImageGenTool`.
pub struct ImageGenTool {
    workspace_root: PathBuf,
    session_id: String,
    modalities: InputModalities,
    config: ImageGenerationToolConfig,
}

impl ImageGenTool {
    pub fn new(
        workspace_root: impl AsRef<Path>,
        session_id: impl Into<String>,
        modalities: InputModalities,
        config: ImageGenerationToolConfig,
    ) -> Result<Self, String> {
        let workspace_root = fs::canonicalize(workspace_root.as_ref())
            .map_err(|error| format!("could not resolve workspace root: {error}"))?;
        if !workspace_root.is_dir() {
            return Err("workspace root is not a directory".to_owned());
        }
        Ok(Self {
            workspace_root,
            session_id: session_id.into(),
            modalities,
            config,
        })
    }

    /// Change the session folder after the host creates or resumes a session.
    pub fn select_session(&mut self, session_id: &str) {
        self.session_id = session_id.to_owned();
    }

    pub fn model_name(&self) -> &str {
        &self.config.model
    }

    pub fn function_declaration() -> Value {
        json!({
            "name": "image_gen",
            "description": "Generates a PNG image with the configured image model and saves it as a workspace artifact. Use size in width*height form when the user requests a specific aspect ratio.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "prompt": {
                        "type": "STRING",
                        "minLength": 1,
                        "maxLength": MAX_PROMPT_CHARS,
                        "description": "Detailed text description of the image to generate."
                    },
                    "size": {
                        "type": "STRING",
                        "pattern": "^\\d+\\*\\d+$",
                        "description": "Optional output size in width*height form, for example 1536*864."
                    }
                },
                "required": ["prompt"]
            }
        })
    }

    pub fn parse_params(args: &Value) -> Result<ImageGenParams, String> {
        let prompt = args
            .get("prompt")
            .and_then(Value::as_str)
            .ok_or_else(|| "The image prompt must be a string.".to_owned())?;

        let size = args
            .get("size")
            .filter(|value| !value.is_null())
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| "Image size must be a string.".to_owned())
            })
            .transpose()?;
        Self::validate_params(ImageGenParams {
            prompt: prompt.to_owned(),
            size,
        })
    }

    fn validate_params(mut params: ImageGenParams) -> Result<ImageGenParams, String> {
        params.prompt = params.prompt.trim().to_owned();
        if params.prompt.is_empty() {
            return Err("The image prompt must be non-empty.".to_owned());
        }
        if params.prompt.encode_utf16().count() > MAX_PROMPT_CHARS {
            return Err(format!(
                "The image prompt must not exceed {MAX_PROMPT_CHARS} characters."
            ));
        }
        params.size = params.size.filter(|size| !size.is_empty());
        if let Some(size) = params.size.as_deref() {
            validate_size(size)?;
        }
        Ok(params)
    }

    /// Generate an image, persist it atomically under the current session, and
    /// return bounded model-visible content plus host artifact metadata.
    pub async fn execute(
        &self,
        params: &ImageGenParams,
        cancellation: &CancellationToken,
    ) -> Result<ToolExecutionOutput, String> {
        let params = Self::validate_params(params.clone())?;
        let Some(api_key) = self
            .config
            .api_key
            .as_deref()
            .map(str::trim)
            .filter(|key| !key.is_empty())
        else {
            return Err(format!(
                "Image generation requires the {} environment variable.",
                self.config.api_key_env
            ));
        };
        if cancellation.is_cancelled() {
            return Err("Image generation was cancelled.".to_owned());
        }

        let session_dir = Storage::sanitize_plan_session_id(&self.session_id);
        let output_dir = self
            .workspace_root
            .join(".canopy")
            .join("generated-images")
            .join(session_dir);
        ensure_workspace_directory(&output_dir, &self.workspace_root)?;
        if cancellation.is_cancelled() {
            return Err("Image generation was cancelled.".to_owned());
        }

        let generated = generate_image(
            &ImageGenerationRequest {
                base_url: &self.config.base_url,
                api_key,
                model: &self.config.model,
                prompt: &params.prompt,
                size: params.size.as_deref(),
            },
            Some(cancellation),
        )
        .await
        .map_err(|error| format!("Image generation failed: {error}"))?;

        if cancellation.is_cancelled() {
            return Err("Image generation was cancelled.".to_owned());
        }
        ensure_workspace_directory(&output_dir, &self.workspace_root)?;

        let output_path = output_dir.join(format!("{}.png", Uuid::new_v4().simple()));
        atomic_write_file(
            &output_path,
            &generated.bytes,
            &AtomicWriteOptions {
                mode: Some(0o600),
                force_mode: true,
                symlink_policy: SymlinkPolicy::NoFollow,
                ..AtomicWriteOptions::default()
            },
        )
        .map_err(|error| format!("could not save generated image: {error}"))?;
        ensure_workspace_directory(&output_dir, &self.workspace_root)?;

        Ok(self.build_result(&params, &output_path, &generated))
    }

    fn build_result(
        &self,
        params: &ImageGenParams,
        output_path: &Path,
        generated: &GeneratedImage,
    ) -> ToolExecutionOutput {
        let absolute_path = output_path.to_string_lossy().into_owned();
        let workspace_path = output_path
            .strip_prefix(&self.workspace_root)
            .unwrap_or(output_path)
            .to_string_lossy()
            .replace('\\', "/");
        let mut metadata = serde_json::Map::new();
        metadata.insert("model".to_owned(), json!(self.config.model));
        if let Some(request_id) = generated.request_id.as_deref() {
            metadata.insert("requestId".to_owned(), json!(request_id));
        }
        if let Some(size) = params.size.as_deref() {
            metadata.insert("size".to_owned(), json!(size));
        }
        let artifact = json!({
            "title": "Generated image",
            "kind": "image",
            "storage": "workspace",
            "workspacePath": workspace_path,
            "mimeType": generated.mime_type,
            "sizeBytes": generated.bytes.len(),
            "metadata": metadata,
        });
        let output = format!("Generated image saved to {absolute_path}.");
        let mut result = ToolExecutionOutput::with_display(
            output.clone(),
            json!({"displayText": format!("Generated image saved to **{absolute_path}**.")}),
        );
        result.artifacts.push(artifact);
        result.result_file_paths.push(absolute_path);
        if self.modalities.image && generated.bytes.len() <= MAX_INLINE_IMAGE_BYTES {
            result.parts.push(json!({
                "inlineData": {
                    "mimeType": generated.mime_type,
                    "data": BASE64_STANDARD.encode(&generated.bytes),
                }
            }));
        }
        result
    }
}

fn validate_size(size: &str) -> Result<(), String> {
    let Some((width, height)) = size.split_once('*') else {
        return Err("Image size must use width*height form, for example 1536*864.".to_owned());
    };
    if width.is_empty()
        || height.is_empty()
        || !width.bytes().all(|byte| byte.is_ascii_digit())
        || !height.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("Image size must use width*height form, for example 1536*864.".to_owned());
    }
    let width = width.parse::<u128>().unwrap_or(u128::MAX);
    let height = height.parse::<u128>().unwrap_or(u128::MAX);
    let total_pixels = width.saturating_mul(height);
    if !(MIN_TOTAL_PIXELS..=MAX_TOTAL_PIXELS).contains(&total_pixels) {
        return Err("Image size total pixels must be between 512*512 and 2048*2048.".to_owned());
    }
    Ok(())
}

fn ensure_workspace_directory(path: &Path, workspace_root: &Path) -> Result<(), String> {
    let relative = path
        .strip_prefix(workspace_root)
        .map_err(|_| "Generated image path must stay inside the workspace.".to_owned())?;
    let mut current = workspace_root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(
                    "Generated image path contains an unsafe directory component.".to_owned(),
                );
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(create_error)
                        if create_error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(create_error) => {
                        return Err(format!(
                            "could not prepare generated image directory: {create_error}"
                        ));
                    }
                }
            }
            Err(error) => {
                return Err(format!(
                    "could not inspect generated image directory: {error}"
                ));
            }
        }
        let metadata = fs::symlink_metadata(&current)
            .map_err(|error| format!("could not inspect generated image directory: {error}"))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err("Generated image path contains an unsafe directory component.".to_owned());
        }
        let resolved = fs::canonicalize(&current)
            .map_err(|error| format!("could not resolve generated image directory: {error}"))?;
        if !resolved.starts_with(workspace_root) {
            return Err("Generated image path must stay inside the workspace.".to_owned());
        }
    }
    Storage::assert_path_within_directory(path, workspace_root)
        .map_err(|_| "Generated image path must stay inside the workspace.".to_owned())?;
    if !current.starts_with(workspace_root) {
        return Err("Generated image path must stay inside the workspace.".to_owned());
    }
    Ok(())
}
