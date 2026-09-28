//! Request token estimation ported from `packages/core/src/utils/request-tokenizer`.
//!
//! The estimator intentionally remains a guardrail approximation rather than a
//! model tokenizer. Image parsing is bounds checked and malformed or unsupported
//! image data falls back to the source implementation's 512x512 estimate.

use std::time::Instant;

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const TOKEN_ESTIMATE_UNITS_PER_TOKEN: u64 = 20;
/// Bound temporary memory used while decoding image metadata from caller input.
pub const MAX_IMAGE_METADATA_BASE64_BYTES: usize = 64 * 1024 * 1024;
pub const SUPPORTED_IMAGE_MIME_TYPES: [&str; 8] = [
    "image/bmp",
    "image/gif",
    "image/jpeg",
    "image/jpg",
    "image/png",
    "image/tiff",
    "image/webp",
    "image/heic",
];

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBreakdown {
    pub text_tokens: u64,
    pub image_tokens: u64,
    pub audio_tokens: u64,
    pub other_tokens: u64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenCalculationResult {
    pub total_tokens: u64,
    pub breakdown: TokenBreakdown,
    pub processing_time: f64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageMetadata {
    pub width: u32,
    pub height: u32,
    pub mime_type: String,
    pub data_size: usize,
}

pub fn is_supported_image_mime_type(mime_type: &str) -> bool {
    SUPPORTED_IMAGE_MIME_TYPES.contains(&mime_type)
}

pub fn supported_image_formats_string() -> String {
    SUPPORTED_IMAGE_MIME_TYPES
        .iter()
        .map(|mime_type| {
            mime_type
                .strip_prefix("image/")
                .unwrap_or(mime_type)
                .to_uppercase()
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn unsupported_image_format_warning() -> String {
    format!(
        "Only the following image formats are supported: {}. Other formats may not work as expected.",
        supported_image_formats_string()
    )
}

/// Estimate text tokens using 0.25 units per ASCII UTF-16 code unit and 1.1
/// units per non-ASCII UTF-16 code unit, rounded up to a whole token.
pub fn estimate_text_tokens(text: &str) -> u64 {
    estimate_text_token_units(text).div_ceil(TOKEN_ESTIMATE_UNITS_PER_TOKEN)
}

/// Return fixed point token units so streaming callers can accumulate without
/// floating point drift. JavaScript's source implementation counts UTF-16 code
/// units, so astral characters count as two non-ASCII units here as well.
pub fn estimate_text_token_units(text: &str) -> u64 {
    let mut ascii = 0_u64;
    let mut non_ascii = 0_u64;
    for unit in text.encode_utf16() {
        if unit < 128 {
            ascii = ascii.saturating_add(1);
        } else {
            non_ascii = non_ascii.saturating_add(1);
        }
    }
    ascii
        .saturating_mul(5)
        .saturating_add(non_ascii.saturating_mul(22))
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TextTokenizer;

impl TextTokenizer {
    pub fn calculate_tokens(&self, text: &str) -> u64 {
        estimate_text_tokens(text)
    }

    pub fn calculate_tokens_batch(&self, texts: &[String]) -> Vec<u64> {
        texts
            .iter()
            .map(|text| self.calculate_tokens(text))
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ImageTokenizer;

impl ImageTokenizer {
    pub fn extract_image_metadata(&self, base64_data: &str, mime_type: &str) -> ImageMetadata {
        let Some(clean_data) = strip_data_url_prefix(base64_data) else {
            return fallback_metadata(base64_data, mime_type);
        };

        if !is_supported_image_mime_type(mime_type) {
            return fallback_metadata(base64_data, mime_type);
        }

        let Some(bytes) = decode_base64_compat(clean_data) else {
            return fallback_metadata(base64_data, mime_type);
        };
        let (width, height) = dimensions(&bytes, mime_type).unwrap_or((512, 512));
        ImageMetadata {
            width,
            height,
            mime_type: mime_type.to_owned(),
            data_size: bytes.len(),
        }
    }

    pub fn calculate_tokens(&self, metadata: &ImageMetadata) -> u64 {
        calculate_image_tokens(metadata.width, metadata.height)
    }

    pub fn calculate_tokens_batch(&self, images: &[(String, String)]) -> Vec<u64> {
        images
            .iter()
            .map(|(data, mime_type)| {
                let metadata = self.extract_image_metadata(data, mime_type);
                self.calculate_tokens(&metadata)
            })
            .collect()
    }
}

fn fallback_metadata(base64_data: &str, mime_type: &str) -> ImageMetadata {
    ImageMetadata {
        width: 512,
        height: 512,
        mime_type: mime_type.to_owned(),
        // Buffer.from(..., 'base64') is attempted in the normal path. The
        // TypeScript fallback instead estimates from the original JS string.
        data_size: base64_data.encode_utf16().count().saturating_mul(3) / 4,
    }
}

fn strip_data_url_prefix(data: &str) -> Option<&str> {
    if !data.starts_with("data:") {
        return Some(data);
    }
    let (header, payload) = data.split_once(',')?;
    if !header.ends_with(";base64") {
        return Some(data);
    }
    let media_type = header.strip_prefix("data:")?.strip_suffix(";base64")?;
    if media_type.is_empty() || media_type.contains(';') {
        return Some(data);
    }
    Some(payload)
}

fn decode_base64_compat(data: &str) -> Option<Vec<u8>> {
    if data.len() > MAX_IMAGE_METADATA_BASE64_BYTES {
        return None;
    }
    let filtered = data
        .bytes()
        .filter(|byte| {
            byte.is_ascii_alphanumeric() || matches!(*byte, b'+' | b'/' | b'=' | b'-' | b'_')
        })
        .collect::<Vec<_>>();
    let filtered = std::str::from_utf8(&filtered).ok()?;
    STANDARD
        .decode(filtered)
        .or_else(|_| STANDARD_NO_PAD.decode(filtered))
        .or_else(|_| URL_SAFE.decode(filtered))
        .or_else(|_| URL_SAFE_NO_PAD.decode(filtered))
        .ok()
}

/// Calculate image tokens using 28x28 pixel tiles, a four-tile minimum, a
/// 16,384-tile maximum, and two vision marker tokens.
pub fn calculate_image_tokens(width: u32, height: u32) -> u64 {
    const PIXELS_PER_TOKEN: f64 = 28.0 * 28.0;
    const MIN_PIXELS: f64 = 4.0 * PIXELS_PER_TOKEN;
    const MAX_PIXELS: f64 = 16_384.0 * PIXELS_PER_TOKEN;
    const SPECIAL_TOKENS: u64 = 2;

    // Zero dimensions produce NaN in the JS scaling path. Keep malformed
    // headers deterministic and safe by using the documented minimum.
    if width == 0 || height == 0 {
        return 6;
    }

    let original_width = f64::from(width);
    let original_height = f64::from(height);
    let mut h_bar = (original_height / 28.0).round() * 28.0;
    let mut w_bar = (original_width / 28.0).round() * 28.0;
    let normalized_area = h_bar * w_bar;

    if normalized_area > MAX_PIXELS {
        let beta = ((original_height * original_width) / MAX_PIXELS).sqrt();
        h_bar = (original_height / beta / 28.0).floor() * 28.0;
        w_bar = (original_width / beta / 28.0).floor() * 28.0;
    } else if normalized_area < MIN_PIXELS {
        let beta = (MIN_PIXELS / (original_height * original_width)).sqrt();
        h_bar = (original_height * beta / 28.0).ceil() * 28.0;
        w_bar = (original_width * beta / 28.0).ceil() * 28.0;
    }

    let image_tokens = ((h_bar * w_bar) / PIXELS_PER_TOKEN).floor();
    if !image_tokens.is_finite() || image_tokens < 0.0 {
        return 6;
    }
    (image_tokens as u64).saturating_add(SPECIAL_TOKENS)
}

fn dimensions(data: &[u8], mime_type: &str) -> Option<(u32, u32)> {
    if mime_type.contains("png") {
        return png_dimensions(data);
    }
    if mime_type.contains("jpeg") || mime_type.contains("jpg") {
        return jpeg_dimensions(data);
    }
    if mime_type.contains("webp") {
        return webp_dimensions(data);
    }
    if mime_type.contains("gif") {
        return gif_dimensions(data);
    }
    if mime_type.contains("bmp") {
        return bmp_dimensions(data);
    }
    if mime_type.contains("tiff") {
        return tiff_dimensions(data);
    }
    if mime_type.contains("heic") {
        return heic_dimensions(data).or(Some((512, 512)));
    }
    Some((512, 512))
}

fn png_dimensions(data: &[u8]) -> Option<(u32, u32)> {
    const SIGNATURE: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];
    if data.len() < 24 || data.get(..8)? != SIGNATURE {
        return None;
    }
    Some((read_u32_be(data, 16)?, read_u32_be(data, 20)?))
}

fn jpeg_dimensions(data: &[u8]) -> Option<(u32, u32)> {
    if data.len() < 4 || data.get(..2)? != [0xff, 0xd8] {
        return None;
    }
    let mut offset = 2_usize;
    while offset < data.len().saturating_sub(8) {
        if data.get(offset) != Some(&0xff) {
            offset += 1;
            continue;
        }
        let marker = *data.get(offset + 1)?;
        if matches!(marker, 0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf) {
            let height = u32::from(read_u16_be(data, offset + 5)?);
            let width = u32::from(read_u16_be(data, offset + 7)?);
            return Some((width, height));
        }
        let segment_length = usize::from(read_u16_be(data, offset + 2)?);
        offset = offset.checked_add(2)?.checked_add(segment_length)?;
    }
    None
}

fn webp_dimensions(data: &[u8]) -> Option<(u32, u32)> {
    if data.len() < 16 || data.get(..4)? != b"RIFF" || data.get(8..12)? != b"WEBP" {
        return None;
    }
    match data.get(12..16)? {
        b"VP8 " if data.len() >= 30 => Some((
            u32::from(read_u16_le(data, 26)? & 0x3fff),
            u32::from(read_u16_le(data, 28)? & 0x3fff),
        )),
        b"VP8L" if data.len() >= 25 && data.get(20) == Some(&0x2f) => {
            let bits = read_u32_le(data, 21)?;
            Some(((bits & 0x3fff) + 1, ((bits >> 14) & 0x3fff) + 1))
        }
        b"VP8X" if data.len() >= 30 => Some((
            read_u24_le(data, 24)?.saturating_add(1),
            read_u24_le(data, 27)?.saturating_add(1),
        )),
        _ => None,
    }
}

fn gif_dimensions(data: &[u8]) -> Option<(u32, u32)> {
    if data.len() < 10 || !matches!(data.get(..6)?, b"GIF87a" | b"GIF89a") {
        return None;
    }
    Some((
        u32::from(read_u16_le(data, 6)?),
        u32::from(read_u16_le(data, 8)?),
    ))
}

fn bmp_dimensions(data: &[u8]) -> Option<(u32, u32)> {
    if data.len() < 26 || data.get(..2)? != b"BM" {
        return None;
    }
    let width = read_u32_le(data, 18)?;
    let height = read_i32_le(data, 22)?.unsigned_abs();
    Some((width, height))
}

fn tiff_dimensions(data: &[u8]) -> Option<(u32, u32)> {
    if data.len() < 8 {
        return None;
    }
    let little_endian = match data.get(..2)? {
        b"II" => true,
        b"MM" => false,
        _ => return None,
    };
    let magic = read_u16(data, 2, little_endian)?;
    if magic != 42 {
        return None;
    }
    let ifd_offset = usize::try_from(read_u32(data, 4, little_endian)?).ok()?;
    if ifd_offset >= data.len() {
        return None;
    }
    let entry_count = usize::from(read_u16(data, ifd_offset, little_endian)?);
    let mut width = 0_u32;
    let mut height = 0_u32;
    for index in 0..entry_count {
        let entry_offset = ifd_offset
            .checked_add(2)?
            .checked_add(index.checked_mul(12)?)?;
        if entry_offset.checked_add(12)? > data.len() {
            break;
        }
        let tag = read_u16(data, entry_offset, little_endian)?;
        let field_type = read_u16(data, entry_offset + 2, little_endian)?;
        let value = if field_type == 3 {
            u32::from(read_u16(data, entry_offset + 8, little_endian)?)
        } else {
            read_u32(data, entry_offset + 8, little_endian)?
        };
        match tag {
            0x0100 => width = value,
            0x0101 => height = value,
            _ => {}
        }
        if width > 0 && height > 0 {
            break;
        }
    }
    (width > 0 && height > 0).then_some((width, height))
}

fn heic_dimensions(data: &[u8]) -> Option<(u32, u32)> {
    if data.len() < 12 || data.get(4..8)? != b"ftyp" {
        return None;
    }
    if !matches!(data.get(8..12)?, b"heic" | b"heix" | b"hevc" | b"hevx") {
        return None;
    }
    let mut offset = 0_usize;
    while offset < data.len().saturating_sub(8) {
        let box_size = usize::try_from(read_u32_be(data, offset)?).ok()?;
        let box_type = data.get(offset + 4..offset + 8)?;
        if box_type == b"meta" {
            let box_end = offset.checked_add(box_size)?.min(data.len());
            let mut inner = offset.checked_add(12)?; // box header + version/flags
            while inner < box_end.saturating_sub(8) {
                let inner_size = usize::try_from(read_u32_be(data, inner)?).ok()?;
                let inner_type = data.get(inner + 4..inner + 8)?;
                if inner_type == b"ispe" && inner.checked_add(20)? <= data.len() {
                    return Some((
                        read_u32_be(data, inner + 12)?,
                        read_u32_be(data, inner + 16)?,
                    ));
                }
                if inner_size == 0 {
                    break;
                }
                inner = inner.checked_add(inner_size.max(1))?;
            }
        }
        if box_size == 0 {
            break;
        }
        offset = offset.checked_add(box_size.max(1))?;
    }
    None
}

fn read_u16_be(data: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_be_bytes(
        data.get(offset..offset.checked_add(2)?)?.try_into().ok()?,
    ))
}
fn read_u16_le(data: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        data.get(offset..offset.checked_add(2)?)?.try_into().ok()?,
    ))
}
fn read_u32_be(data: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        data.get(offset..offset.checked_add(4)?)?.try_into().ok()?,
    ))
}
fn read_u32_le(data: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        data.get(offset..offset.checked_add(4)?)?.try_into().ok()?,
    ))
}
fn read_i32_le(data: &[u8], offset: usize) -> Option<i32> {
    Some(i32::from_le_bytes(
        data.get(offset..offset.checked_add(4)?)?.try_into().ok()?,
    ))
}
fn read_u24_le(data: &[u8], offset: usize) -> Option<u32> {
    let bytes = data.get(offset..offset.checked_add(3)?)?;
    Some(u32::from(bytes[0]) | (u32::from(bytes[1]) << 8) | (u32::from(bytes[2]) << 16))
}
fn read_u16(data: &[u8], offset: usize, little_endian: bool) -> Option<u16> {
    if little_endian {
        read_u16_le(data, offset)
    } else {
        read_u16_be(data, offset)
    }
}
fn read_u32(data: &[u8], offset: usize, little_endian: bool) -> Option<u32> {
    if little_endian {
        read_u32_le(data, offset)
    } else {
        read_u32_be(data, offset)
    }
}

#[derive(Default)]
struct GroupedContents {
    text_token_units: u64,
    image_tokens: u64,
    audio_tokens: u64,
    other_token_units: u64,
}

/// Request token estimator over the JSON shape accepted by the Google GenAI
/// `CountTokensParameters` API. Provider-specific fields are retained as
/// serialized input when they do not match a known content part.
#[derive(Clone, Copy, Debug, Default)]
pub struct RequestTokenizer;

/// Source-compatible public alias exported as `RequestTokenEstimator`.
pub type RequestTokenEstimator = RequestTokenizer;

impl RequestTokenizer {
    pub fn calculate_tokens(&self, request: &Value) -> TokenCalculationResult {
        let started = Instant::now();
        let grouped = group_contents(request.get("contents"));
        if grouped.text_token_units == 0
            && grouped.image_tokens == 0
            && grouped.audio_tokens == 0
            && grouped.other_token_units == 0
        {
            return TokenCalculationResult {
                processing_time: started.elapsed().as_secs_f64() * 1000.0,
                ..TokenCalculationResult::default()
            };
        }

        let text_tokens = grouped
            .text_token_units
            .div_ceil(TOKEN_ESTIMATE_UNITS_PER_TOKEN);
        let image_tokens = grouped.image_tokens;
        let audio_tokens = grouped.audio_tokens;
        let other_tokens = grouped
            .other_token_units
            .div_ceil(TOKEN_ESTIMATE_UNITS_PER_TOKEN);
        let breakdown = TokenBreakdown {
            text_tokens,
            image_tokens,
            audio_tokens,
            other_tokens,
        };
        let total_tokens = breakdown
            .text_tokens
            .saturating_add(breakdown.image_tokens)
            .saturating_add(breakdown.audio_tokens)
            .saturating_add(breakdown.other_tokens);
        TokenCalculationResult {
            total_tokens,
            breakdown,
            processing_time: started.elapsed().as_secs_f64() * 1000.0,
        }
    }
}

fn group_contents(contents: Option<&Value>) -> GroupedContents {
    let mut grouped = GroupedContents::default();
    let Some(contents) = contents else {
        return grouped;
    };
    match contents {
        Value::Array(items) => {
            for item in items {
                process_content(item, &mut grouped);
            }
        }
        Value::Null => {}
        other => process_content(other, &mut grouped),
    }
    grouped
}

fn process_content(content: &Value, grouped: &mut GroupedContents) {
    if let Some(text) = content.as_str() {
        if has_non_whitespace_js_code_unit(text) {
            grouped.text_token_units = grouped
                .text_token_units
                .saturating_add(estimate_text_token_units(text));
        }
        return;
    }
    if let Some(parts) = content.get("parts").filter(|parts| json_truthy(parts)) {
        if let Some(parts) = parts.as_array() {
            for part in parts {
                process_part(part, grouped);
            }
        }
        return;
    }
    process_part(content, grouped);
}

fn process_part(part: &Value, grouped: &mut GroupedContents) {
    if let Some(text) = part.as_str() {
        if has_non_whitespace_js_code_unit(text) {
            grouped.text_token_units = grouped
                .text_token_units
                .saturating_add(estimate_text_token_units(text));
        }
        return;
    }
    if let Some(text) = part
        .get("text")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
    {
        grouped.text_token_units = grouped
            .text_token_units
            .saturating_add(estimate_text_token_units(text));
        return;
    }
    if let Some(inline_data) = part.get("inlineData").filter(|value| json_truthy(value)) {
        let mime_type = inline_data
            .get("mimeType")
            .and_then(Value::as_str)
            .unwrap_or("");
        let data = inline_data
            .get("data")
            .and_then(Value::as_str)
            .unwrap_or("");
        if mime_type.starts_with("image/") {
            let metadata = ImageTokenizer.extract_image_metadata(data, mime_type);
            grouped.image_tokens = grouped
                .image_tokens
                .saturating_add(ImageTokenizer.calculate_tokens(&metadata));
            return;
        }
        if mime_type.starts_with("audio/") {
            let size = (data.encode_utf16().count() as u64).saturating_mul(3) / 4;
            grouped.audio_tokens = grouped
                .audio_tokens
                .saturating_add((size.div_ceil(100)).max(10));
            return;
        }
    }
    for key in ["fileData", "functionCall", "functionResponse"] {
        if let Some(value) = part.get(key).filter(|value| json_truthy(value)) {
            if let Ok(serialized) = serde_json::to_string(value) {
                grouped.other_token_units = grouped
                    .other_token_units
                    .saturating_add(estimate_text_token_units(&serialized));
            }
            return;
        }
    }
    if let Ok(serialized) = serde_json::to_string(part) {
        if serialized != "{}" {
            grouped.other_token_units = grouped
                .other_token_units
                .saturating_add(estimate_text_token_units(&serialized));
        }
    }
}

fn has_non_whitespace_js_code_unit(text: &str) -> bool {
    text.chars()
        .any(|character| !is_ecmascript_whitespace(character))
}

fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000a}'
            | '\u{000b}'
            | '\u{000c}'
            | '\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

fn json_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ONE_PIXEL_PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChAI9jU77yQAAAABJRU5ErkJggg==";

    #[test]
    fn text_token_estimates_match_utf16_unit_policy() {
        assert_eq!(estimate_text_tokens(""), 0);
        assert_eq!(estimate_text_tokens("hello"), 2);
        assert_eq!(estimate_text_token_units("hello"), 25);
        assert_eq!(estimate_text_token_units("😀"), 44);
        assert_eq!(estimate_text_tokens("😀"), 3);
    }

    #[test]
    fn image_scaling_applies_minimum_and_maximum() {
        assert_eq!(calculate_image_tokens(1, 1), 6);
        assert_eq!(calculate_image_tokens(28, 28), 6);
        assert_eq!(calculate_image_tokens(512, 512), 326);
        assert_eq!(calculate_image_tokens(8192, 8192), 16_386);
        assert_eq!(calculate_image_tokens(0, 20), 6);
    }

    #[test]
    fn png_metadata_extracts_dimensions_and_data_size() {
        let metadata = ImageTokenizer.extract_image_metadata(ONE_PIXEL_PNG, "image/png");
        assert_eq!((metadata.width, metadata.height), (1, 1));
        assert_eq!(metadata.mime_type, "image/png");
        assert_eq!(metadata.data_size, 70);
        let data_url = format!("data:image/png;base64,{ONE_PIXEL_PNG}");
        let prefixed = ImageTokenizer.extract_image_metadata(&data_url, "image/png");
        assert_eq!((prefixed.width, prefixed.height), (1, 1));
        assert_eq!(prefixed.data_size, 70);
    }

    #[test]
    fn malformed_and_unsupported_images_fall_back_without_panicking() {
        let malformed = ImageTokenizer.extract_image_metadata("not-an-image", "image/png");
        assert_eq!((malformed.width, malformed.height), (512, 512));
        let unsupported = ImageTokenizer.extract_image_metadata(ONE_PIXEL_PNG, "image/avif");
        assert_eq!((unsupported.width, unsupported.height), (512, 512));
        assert!(!is_supported_image_mime_type("image/avif"));
        assert_eq!(
            supported_image_formats_string(),
            "BMP, GIF, JPEG, JPG, PNG, TIFF, WEBP, HEIC"
        );
    }

    #[test]
    fn extracts_jpeg_webp_gif_bmp_tiff_and_heic_dimensions() {
        let jpeg = [
            0xff, 0xd8, 0xff, 0xc0, 0x00, 0x11, 0x08, 0x00, 0x32, 0x00, 0x64, 0x00,
        ];
        let jpeg = STANDARD.encode(jpeg);
        let jpeg_metadata = ImageTokenizer.extract_image_metadata(&jpeg, "image/jpeg");
        assert_eq!((jpeg_metadata.width, jpeg_metadata.height), (100, 50));

        let mut webp = vec![0; 30];
        webp[0..4].copy_from_slice(b"RIFF");
        webp[8..12].copy_from_slice(b"WEBP");
        webp[12..16].copy_from_slice(b"VP8X");
        webp[24..27].copy_from_slice(&[99, 0, 0]);
        webp[27..30].copy_from_slice(&[79, 0, 0]);
        let webp = STANDARD.encode(webp);
        let webp_metadata = ImageTokenizer.extract_image_metadata(&webp, "image/webp");
        assert_eq!((webp_metadata.width, webp_metadata.height), (100, 80));

        let mut gif = vec![0; 10];
        gif[0..6].copy_from_slice(b"GIF89a");
        gif[6..8].copy_from_slice(&2_u16.to_le_bytes());
        gif[8..10].copy_from_slice(&3_u16.to_le_bytes());
        let gif_metadata =
            ImageTokenizer.extract_image_metadata(&STANDARD.encode(gif), "image/gif");
        assert_eq!((gif_metadata.width, gif_metadata.height), (2, 3));

        let mut bmp = vec![0; 26];
        bmp[0..2].copy_from_slice(b"BM");
        bmp[18..22].copy_from_slice(&100_u32.to_le_bytes());
        bmp[22..26].copy_from_slice(&(-50_i32).to_le_bytes());
        let bmp_metadata =
            ImageTokenizer.extract_image_metadata(&STANDARD.encode(bmp), "image/bmp");
        assert_eq!((bmp_metadata.width, bmp_metadata.height), (100, 50));

        let mut tiff = vec![0; 38];
        tiff[0..2].copy_from_slice(b"MM");
        tiff[2..4].copy_from_slice(&42_u16.to_be_bytes());
        tiff[4..8].copy_from_slice(&8_u32.to_be_bytes());
        tiff[8..10].copy_from_slice(&2_u16.to_be_bytes());
        tiff[10..12].copy_from_slice(&0x0100_u16.to_be_bytes());
        tiff[12..14].copy_from_slice(&3_u16.to_be_bytes());
        tiff[14..18].copy_from_slice(&1_u32.to_be_bytes());
        tiff[18..20].copy_from_slice(&800_u16.to_be_bytes());
        tiff[22..24].copy_from_slice(&0x0101_u16.to_be_bytes());
        tiff[24..26].copy_from_slice(&3_u16.to_be_bytes());
        tiff[26..30].copy_from_slice(&1_u32.to_be_bytes());
        tiff[30..32].copy_from_slice(&600_u16.to_be_bytes());
        let tiff_metadata =
            ImageTokenizer.extract_image_metadata(&STANDARD.encode(tiff), "image/tiff");
        assert_eq!((tiff_metadata.width, tiff_metadata.height), (800, 600));

        let mut heic = vec![0; 48];
        heic[0..4].copy_from_slice(&16_u32.to_be_bytes());
        heic[4..8].copy_from_slice(b"ftyp");
        heic[8..12].copy_from_slice(b"heic");
        heic[16..20].copy_from_slice(&32_u32.to_be_bytes());
        heic[20..24].copy_from_slice(b"meta");
        heic[28..32].copy_from_slice(&20_u32.to_be_bytes());
        heic[32..36].copy_from_slice(b"ispe");
        heic[40..44].copy_from_slice(&1920_u32.to_be_bytes());
        heic[44..48].copy_from_slice(&1080_u32.to_be_bytes());
        let heic_metadata =
            ImageTokenizer.extract_image_metadata(&STANDARD.encode(heic), "image/heic");
        assert_eq!((heic_metadata.width, heic_metadata.height), (1920, 1080));
    }

    #[test]
    fn request_groups_text_images_audio_and_other_parts() {
        let request = json!({
            "contents": [{ "role": "user", "parts": [
                { "text": "Hello" },
                { "inlineData": { "mimeType": "image/png", "data": ONE_PIXEL_PNG } },
                { "inlineData": { "mimeType": "audio/wav", "data": "a".repeat(1000) } },
                { "functionCall": { "name": "inspect", "args": { "x": 1 } } }
            ] }]
        });
        let result = RequestTokenizer.calculate_tokens(&request);
        assert_eq!(result.breakdown.text_tokens, 2);
        assert_eq!(result.breakdown.image_tokens, 6);
        assert_eq!(result.breakdown.audio_tokens, 10);
        assert!(result.breakdown.other_tokens > 0);
        assert_eq!(
            result.total_tokens,
            result.breakdown.text_tokens
                + result.breakdown.image_tokens
                + result.breakdown.audio_tokens
                + result.breakdown.other_tokens
        );
    }

    #[test]
    fn empty_and_direct_part_content_shapes_are_supported() {
        assert_eq!(
            RequestTokenizer
                .calculate_tokens(&json!({"contents": []}))
                .total_tokens,
            0
        );
        let request = json!({"contents": {"text": "direct"}});
        assert_eq!(
            RequestTokenizer
                .calculate_tokens(&request)
                .breakdown
                .text_tokens,
            2
        );
        let split_text = json!({"contents": [{"parts": [{"text":"h"}, {"text":"i"}]}]});
        assert_eq!(
            RequestTokenizer
                .calculate_tokens(&split_text)
                .breakdown
                .text_tokens,
            1
        );
        let non_ascii_space = json!({"contents": "\u{0085}"});
        assert_eq!(
            RequestTokenizer
                .calculate_tokens(&non_ascii_space)
                .breakdown
                .text_tokens,
            2
        );
    }
}
