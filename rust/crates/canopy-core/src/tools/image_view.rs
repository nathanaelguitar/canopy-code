//! Bounded image crops for the native `zoom_image` tool.

use std::fs::OpenOptions;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use image::codecs::png::PngDecoder;
use image::codecs::webp::WebPDecoder;
use image::imageops::FilterType;
use image::{DynamicImage, GenericImageView, ImageDecoder, ImageFormat, ImageReader, Limits};
use jpeg_encoder::{ColorType as JpegColorType, Encoder as JpegEncoder, SamplingFactor};
use serde_json::{Value, json};

use crate::file_discovery::FileDiscoveryService;
use crate::providers::openai_request::InputModalities;
use crate::tool_response_finalizer::ToolExecutionOutput;
use crate::tools::read_file::{canopy_ignore_source, unescape_path};

const MAX_SOURCE_BYTES: u64 = 100 * 1024 * 1024;
const MAX_DECODE_ALLOC_BYTES: u64 = 96 * 1024 * 1024;
const MAX_IMAGE_DIMENSION: u32 = 32_768;
const MAX_OUTPUT_BYTES: usize = 9 * 1024 * 1024;
const MAX_IMAGE_EDGE: u32 = 1_568;
const MAX_IMAGE_PATCHES: u64 = 1_568;
const IMAGE_PATCH_SIZE: u32 = 28;
const MAX_UPSCALE: u32 = 8;
const JPEG_QUALITY: u8 = 92;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NormalizedRegion {
    pub x1: u16,
    pub y1: u16,
    pub x2: u16,
    pub y2: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ImageSize {
    width: u32,
    height: u32,
}

/// Workspace-scoped, native counterpart of TypeScript `ZoomImageTool`.
pub struct ZoomImageTool {
    workspace_root: PathBuf,
    file_discovery: FileDiscoveryService,
}

impl ZoomImageTool {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self, String> {
        Self::new_with_custom_ignore_files(workspace_root, None)
    }

    pub fn new_with_custom_ignore_files(
        workspace_root: impl AsRef<Path>,
        custom_ignore_files: Option<&[String]>,
    ) -> Result<Self, String> {
        let workspace_root = std::fs::canonicalize(workspace_root.as_ref())
            .map_err(|error| format!("could not resolve workspace root: {error}"))?;
        if !workspace_root.is_dir() {
            return Err("workspace root is not a directory".to_owned());
        }
        let file_discovery = FileDiscoveryService::new(&workspace_root, custom_ignore_files)?;
        Ok(Self {
            workspace_root,
            file_discovery,
        })
    }

    pub fn function_declaration() -> Value {
        json!({
            "name": "zoom_image",
            "description": "Crops a region from a full-resolution static image and returns a magnified view. Coordinates are integers normalized from 0 to 1000 against the displayed image, with (0,0) at top-left and (1000,1000) at bottom-right. Use this when text, numbers, lines, or other details are too small to inspect confidently. You may call it repeatedly; coordinates always refer to the original full-resolution image, never to a previously returned view.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "file_path": {
                        "type": "STRING",
                        "description": "Absolute path to a static PNG, JPEG, or WebP image."
                    },
                    "x1": {"type": "INTEGER", "minimum": 0, "maximum": 1000, "description": "Left edge in normalized image coordinates."},
                    "y1": {"type": "INTEGER", "minimum": 0, "maximum": 1000, "description": "Top edge in normalized image coordinates."},
                    "x2": {"type": "INTEGER", "minimum": 0, "maximum": 1000, "description": "Right edge in normalized image coordinates."},
                    "y2": {"type": "INTEGER", "minimum": 0, "maximum": 1000, "description": "Bottom edge in normalized image coordinates."}
                },
                "required": ["file_path", "x1", "y1", "x2", "y2"]
            }
        })
    }

    /// Decode, orient, crop, and encode a bounded normalized image region.
    pub fn execute(
        &self,
        args: &Value,
        modalities: InputModalities,
    ) -> Result<ToolExecutionOutput, String> {
        if !modalities.image {
            return Err("zoom_image requires a model that accepts image inputs, but the current model does not. Switch to an image-capable model to zoom images.".to_owned());
        }
        let (requested_path, region) = parse_params(args)?;
        let requested = Path::new(&requested_path);
        if !requested.is_absolute() {
            return Err(format!(
                "File path must be absolute, but was relative: {requested_path}."
            ));
        }
        let canonical_path = std::fs::canonicalize(requested).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                format!("Image file not found: {requested_path}")
            } else {
                format!("could not resolve image path: {error}")
            }
        })?;
        if !canonical_path.starts_with(&self.workspace_root) {
            return Err("zoom_image is restricted to files inside the workspace".to_owned());
        }
        if let Some(ignore_file) = canopy_ignore_source(
            &self.file_discovery,
            requested,
            &self.workspace_root,
        )
        .or_else(|| {
            canopy_ignore_source(&self.file_discovery, &canonical_path, &self.workspace_root)
        }) {
            return Err(format!(
                "File path '{}' is ignored by {ignore_file} pattern(s).",
                requested.display()
            ));
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let mut file = options
            .open(&canonical_path)
            .map_err(|error| format!("could not open image file: {error}"))?;
        let metadata = file
            .metadata()
            .map_err(|error| format!("could not inspect image file: {error}"))?;
        if metadata.is_dir() {
            return Err(format!("Image path is a directory: {requested_path}"));
        }
        if !metadata.is_file() {
            return Err(format!(
                "Image path is not a regular file: {requested_path}"
            ));
        }
        if metadata.len() > MAX_SOURCE_BYTES {
            return Err(format!(
                "Image file exceeds the 100 MB source limit: {requested_path}"
            ));
        }

        let mut bytes = Vec::with_capacity(metadata.len().min(MAX_SOURCE_BYTES) as usize);
        file.by_ref()
            .take(MAX_SOURCE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| format!("could not read image file: {error}"))?;
        if bytes.len() as u64 > MAX_SOURCE_BYTES {
            return Err(format!(
                "Image file exceeds the 100 MB source limit: {requested_path}"
            ));
        }

        let (image, source_width, source_height) = decode_static_image(&bytes, &requested_path)?;
        let crop = normalized_crop(source_width, source_height, region);
        let output_size = bounded_size(crop.width, crop.height, MAX_UPSCALE);
        let crop_view = image::imageops::crop_imm(&image, crop.x, crop.y, crop.width, crop.height);
        let resized = image::imageops::resize(
            &*crop_view,
            output_size.width,
            output_size.height,
            FilterType::Lanczos3,
        );
        drop(image);

        let mut rgb = image::RgbImage::new(output_size.width, output_size.height);
        for (x, y, pixel) in resized.enumerate_pixels() {
            let [red, green, blue, alpha] = pixel.0;
            let alpha = u16::from(alpha);
            let flatten = |channel: u8| {
                ((u16::from(channel) * alpha + 255 * (255 - alpha) + 127) / 255) as u8
            };
            rgb.put_pixel(
                x,
                y,
                image::Rgb([flatten(red), flatten(green), flatten(blue)]),
            );
        }
        drop(resized);

        let mut output_bytes = Vec::new();
        let jpeg_width = u16::try_from(output_size.width)
            .map_err(|_| format!("Could not encode image overview: {requested_path}"))?;
        let jpeg_height = u16::try_from(output_size.height)
            .map_err(|_| format!("Could not encode image overview: {requested_path}"))?;
        let mut encoder = JpegEncoder::new(&mut output_bytes, JPEG_QUALITY);
        encoder.set_sampling_factor(SamplingFactor::R_4_4_4);
        encoder
            .encode(rgb.as_raw(), jpeg_width, jpeg_height, JpegColorType::Rgb)
            .map_err(|_| format!("Failed to render image overview: {requested_path}"))?;
        if output_bytes.len() > MAX_OUTPUT_BYTES {
            return Err(format!(
                "Rendered image exceeds the 9 MB output limit: {requested_path}"
            ));
        }

        let text = format!(
            "Zoomed normalized region ({},{})-({},{}) from {}. Oriented source: {}x{}; source crop: {}x{}; returned view: {}x{}.",
            region.x1,
            region.y1,
            region.x2,
            region.y2,
            requested_path,
            source_width,
            source_height,
            crop.width,
            crop.height,
            output_size.width,
            output_size.height,
        );
        let data = BASE64_STANDARD.encode(output_bytes);
        let display_path = canonical_path
            .strip_prefix(&self.workspace_root)
            .unwrap_or(&canonical_path)
            .to_string_lossy();
        Ok(ToolExecutionOutput {
            output: text,
            parts: vec![json!({
                "inlineData": {
                    "mimeType": "image/jpeg",
                    "data": data
                }
            })],
            display: Some(json!({"displayText": format!("Zoomed image: {display_path}")})),
            ..ToolExecutionOutput::default()
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Crop {
    x: u32,
    y: u32,
    width: u32,
    height: u32,
}

pub fn parse_params(args: &Value) -> Result<(String, NormalizedRegion), String> {
    let path = args
        .get("file_path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| "The 'file_path' parameter must be non-empty.".to_owned())?;
    let path = unescape_path(path);
    let coordinate = |name: &str| -> Result<u16, String> {
        let value = args
            .get(name)
            .and_then(Value::as_u64)
            .filter(|value| *value <= 1_000)
            .ok_or_else(|| format!("{name} must be an integer between 0 and 1000."))?;
        Ok(value as u16)
    };
    let region = NormalizedRegion {
        x1: coordinate("x1")?,
        y1: coordinate("y1")?,
        x2: coordinate("x2")?,
        y2: coordinate("y2")?,
    };
    if region.x1 >= region.x2 {
        return Err("x1 must be less than x2.".to_owned());
    }
    if region.y1 >= region.y2 {
        return Err("y1 must be less than y2.".to_owned());
    }
    Ok((path, region))
}

fn decode_static_image(bytes: &[u8], file_path: &str) -> Result<(DynamicImage, u32, u32), String> {
    let mut reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|_| decode_error(file_path))?;
    let format = reader
        .format()
        .ok_or_else(|| unsupported_error(file_path))?;
    if !matches!(
        format,
        ImageFormat::Png | ImageFormat::Jpeg | ImageFormat::WebP
    ) {
        return Err(unsupported_error(file_path));
    }
    let animated = match format {
        ImageFormat::Png => PngDecoder::new(Cursor::new(bytes))
            .and_then(|decoder| decoder.is_apng())
            .map_err(|_| decode_error(file_path))?,
        ImageFormat::WebP => WebPDecoder::new(Cursor::new(bytes))
            .map(|decoder| decoder.has_animation())
            .map_err(|_| decode_error(file_path))?,
        ImageFormat::Jpeg => false,
        _ => false,
    };
    if animated {
        return Err(format!("Only static images are supported: {file_path}"));
    }

    let mut limits = Limits::default();
    limits.max_image_width = Some(MAX_IMAGE_DIMENSION);
    limits.max_image_height = Some(MAX_IMAGE_DIMENSION);
    limits.max_alloc = Some(MAX_DECODE_ALLOC_BYTES);
    reader.limits(limits);
    let mut decoder = reader.into_decoder().map_err(|_| decode_error(file_path))?;
    let (encoded_width, encoded_height) = decoder.dimensions();
    if encoded_width == 0
        || encoded_height == 0
        || encoded_width > MAX_IMAGE_DIMENSION
        || encoded_height > MAX_IMAGE_DIMENSION
        || decoder.total_bytes() > MAX_DECODE_ALLOC_BYTES
    {
        return Err(format!(
            "Image exceeds native decode limits ({} MiB decoded data and {} pixels per edge): {file_path}",
            MAX_DECODE_ALLOC_BYTES / (1024 * 1024),
            MAX_IMAGE_DIMENSION
        ));
    }
    let orientation = decoder.orientation().map_err(|_| decode_error(file_path))?;
    let mut image = DynamicImage::from_decoder(decoder).map_err(|_| decode_error(file_path))?;
    image.apply_orientation(orientation);
    let (width, height) = image.dimensions();
    if width == 0 || height == 0 {
        return Err(decode_error(file_path));
    }
    Ok((image, width, height))
}

fn decode_error(file_path: &str) -> String {
    format!(
        "Failed to decode image (file may be corrupt or not a static PNG, JPEG, or WebP): {file_path}"
    )
}

fn unsupported_error(file_path: &str) -> String {
    format!("Unsupported image. Expected a static PNG, JPEG, or WebP file: {file_path}")
}

fn normalized_crop(width: u32, height: u32, region: NormalizedRegion) -> Crop {
    let left = ((f64::from(region.x1) / 1_000.0) * f64::from(width)).floor() as u32;
    let top = ((f64::from(region.y1) / 1_000.0) * f64::from(height)).floor() as u32;
    let right = ((f64::from(region.x2) / 1_000.0) * f64::from(width)).ceil() as u32;
    let bottom = ((f64::from(region.y2) / 1_000.0) * f64::from(height)).ceil() as u32;
    let x = left.min(width - 1);
    let y = top.min(height - 1);
    let right = right.min(width).max(x + 1);
    let bottom = bottom.min(height).max(y + 1);
    Crop {
        x,
        y,
        width: right - x,
        height: bottom - y,
    }
}

fn fits_visual_budget(size: ImageSize) -> bool {
    size.width <= MAX_IMAGE_EDGE
        && size.height <= MAX_IMAGE_EDGE
        && u64::from(size.width.div_ceil(IMAGE_PATCH_SIZE))
            .saturating_mul(u64::from(size.height.div_ceil(IMAGE_PATCH_SIZE)))
            <= MAX_IMAGE_PATCHES
}

fn bounded_size(width: u32, height: u32, max_upscale: u32) -> ImageSize {
    let width_is_long_edge = width >= height;
    let max_long_edge = MAX_IMAGE_EDGE.min(width.max(height).saturating_mul(max_upscale));
    let (mut low, mut high) = (1, max_long_edge);
    let mut best = ImageSize {
        width: 1,
        height: 1,
    };
    while low <= high {
        let long_edge = low + (high - low) / 2;
        let candidate = if width_is_long_edge {
            ImageSize {
                width: long_edge,
                height: ((f64::from(height) / f64::from(width) * f64::from(long_edge)).round()
                    as u32)
                    .max(1),
            }
        } else {
            ImageSize {
                width: ((f64::from(width) / f64::from(height) * f64::from(long_edge)).round()
                    as u32)
                    .max(1),
                height: long_edge,
            }
        };
        if fits_visual_budget(candidate) {
            best = candidate;
            low = long_edge.saturating_add(1);
        } else {
            if long_edge == 0 {
                break;
            }
            high = long_edge - 1;
        }
    }
    best
}
