//! Cross-platform PNG / JPEG / crosshair / resize helpers.
//!
//! These functions are pure consumers of the [`image`] crate with no
//! platform-specific dependencies, so they were perfect candidates for
//! deduplication. Until 2026-05 they lived as near-identical copies in
//! `platform-{macos,windows,linux}/src/capture.rs` — see
//! [`CUA_DRIVER_RS_DEDUP_AUDIT.md`] for the full audit trail.
//!
//! Each platform's `capture.rs` still owns its own
//! `screenshot_window_bytes` / `screenshot_display_bytes` (CGImage on
//! macOS, BitBlt+PrintWindow on Windows, XGetImage / ImageMagick
//! `import` on Linux). Everything downstream of "I have RGBA pixels" —
//! PNG encoding, JPEG encoding, downscaling to a max long edge, drawing
//! a crosshair, reading width/height from an IHDR — is here.
//!
//! No public API removed: the platform `capture.rs` modules re-export
//! the same function names so existing callers keep compiling.

use anyhow::{Result, anyhow, bail};
use image::{ColorType, DynamicImage, ImageBuffer, ImageDecoder, ImageFormat, Limits};
use std::borrow::Cow;

/// Maximum decoded source-image and native pixel-buffer size accepted by CUA.
/// The limits include 8K UHD screenshots (7680x4320) in RGBA8 while rejecting
/// pathological dimensions before a CPU pixel buffer is allocated.
pub const MAX_DECODED_IMAGE_BYTES: u64 = 128 * 1024 * 1024;
pub const MAX_DECODED_IMAGE_PIXELS: u64 = 36_000_000;
pub const MAX_DECODED_IMAGE_DIMENSION: u32 = 32_768;
pub const MAX_CAPTURE_RGBA_BYTES: u64 = MAX_DECODED_IMAGE_BYTES;
/// Maximum encoded PNG size accepted from a native screen-capture backend.
pub const MAX_CAPTURE_PNG_BYTES: usize = 128 * 1024 * 1024;

/// Reject native capture dimensions before allocating a CPU pixel buffer.
/// This keeps the raw RGBA/BGRA buffer within the same 128 MiB ceiling used
/// by image decoding, while retaining 8K UHD captures.
pub fn validate_native_capture_dimensions(width: u32, height: u32) -> Result<usize> {
    if width == 0 || height == 0 {
        bail!("native capture dimensions must be non-zero (got {width}x{height})");
    }
    if width > MAX_DECODED_IMAGE_DIMENSION || height > MAX_DECODED_IMAGE_DIMENSION {
        bail!(
            "native capture dimensions {width}x{height} exceed the CUA edge limit of {MAX_DECODED_IMAGE_DIMENSION}px"
        );
    }
    let pixels = u64::from(width)
        .checked_mul(u64::from(height))
        .ok_or_else(|| anyhow!("native capture dimensions overflow the CUA limits"))?;
    if pixels > MAX_DECODED_IMAGE_PIXELS {
        bail!(
            "native capture dimensions {width}x{height} exceed the CUA pixel limit of {MAX_DECODED_IMAGE_PIXELS}"
        );
    }
    let bytes = pixels
        .checked_mul(4)
        .ok_or_else(|| anyhow!("native capture byte size overflows the CUA limits"))?;
    if bytes > MAX_CAPTURE_RGBA_BYTES {
        bail!(
            "native capture {width}x{height} requires {bytes} RGBA bytes, above the CUA capture limit of {MAX_CAPTURE_RGBA_BYTES} bytes"
        );
    }
    usize::try_from(bytes)
        .map_err(|_| anyhow!("native capture byte size does not fit this platform's address space"))
}

/// Validate an encoded capture before returning it or expanding it to base64.
pub fn validate_captured_png(png_bytes: &[u8]) -> Result<(u32, u32)> {
    if png_bytes.len() > MAX_CAPTURE_PNG_BYTES {
        bail!(
            "native screenshot PNG is {} bytes, above the CUA capture limit of {MAX_CAPTURE_PNG_BYTES} bytes",
            png_bytes.len()
        );
    }
    let (width, height) = png_dimensions(png_bytes)?;
    validate_native_capture_dimensions(width, height)?;
    Ok((width, height))
}

fn image_decode_limits() -> Limits {
    let mut limits = Limits::default();
    limits.max_image_width = Some(MAX_DECODED_IMAGE_DIMENSION);
    limits.max_image_height = Some(MAX_DECODED_IMAGE_DIMENSION);
    limits.max_alloc = Some(MAX_DECODED_IMAGE_BYTES);
    limits
}

fn check_decode_dimensions(width: u32, height: u32) -> Result<()> {
    let pixels = u64::from(width)
        .checked_mul(u64::from(height))
        .ok_or_else(|| anyhow!("PNG dimensions overflow the CUA image decode limits"))?;
    if width == 0 || height == 0 {
        bail!("PNG dimensions must be non-zero (got {width}x{height})");
    }
    if width > MAX_DECODED_IMAGE_DIMENSION || height > MAX_DECODED_IMAGE_DIMENSION {
        bail!(
            "PNG dimensions {width}x{height} exceed the CUA image decode edge limit of {MAX_DECODED_IMAGE_DIMENSION}px"
        );
    }
    if pixels > MAX_DECODED_IMAGE_PIXELS {
        bail!(
            "PNG dimensions {width}x{height} exceed the CUA image decode limit of {MAX_DECODED_IMAGE_PIXELS} pixels"
        );
    }
    Ok(())
}

fn bounded_png_decoder<'a>(
    png_bytes: &'a [u8],
) -> Result<image::codecs::png::PngDecoder<std::io::Cursor<&'a [u8]>>> {
    // IHDR parsing lets us reject oversized dimensions before the codec sets
    // up any decode buffers.
    let (width, height) = png_dimensions(png_bytes)?;
    check_decode_dimensions(width, height)?;

    let decoder = image::codecs::png::PngDecoder::with_limits(
        std::io::Cursor::new(png_bytes),
        image_decode_limits(),
    )
    .map_err(|error| anyhow!("PNG decoder rejected input under the CUA image limits: {error}"))?;
    let decoded_bytes = decoder.total_bytes();
    if decoded_bytes > MAX_DECODED_IMAGE_BYTES {
        bail!(
            "PNG decoded buffer requires {decoded_bytes} bytes, above the CUA image decode limit of {MAX_DECODED_IMAGE_BYTES} bytes"
        );
    }
    Ok(decoder)
}

/// Decode a captured PNG into a bounded dynamic image for platform-side crop
/// and conversion operations. Native capture dimensions are preserved; this
/// only rejects a source PNG that exceeds the decode bounds above.
pub fn decode_png_bounded(png_bytes: &[u8]) -> Result<DynamicImage> {
    DynamicImage::from_decoder(bounded_png_decoder(png_bytes)?)
        .map_err(|error| anyhow!("PNG decode failed within the CUA image limits: {error}"))
}

fn decoded_buffer_len(decoder: &impl ImageDecoder) -> Result<usize> {
    usize::try_from(decoder.total_bytes())
        .map_err(|_| anyhow!("PNG decoded buffer size does not fit this platform's address space"))
}

// ── PNG → JPEG ────────────────────────────────────────────────────────────

/// Convert raw PNG bytes to JPEG at the given quality (1-95).
///
/// Strips alpha (RGBA → RGB) since JPEG doesn't carry alpha. Quality is
/// clamped to the encoder's accepted range by the underlying `image`
/// crate.
pub fn png_bytes_to_jpeg(png_bytes: &[u8], quality: u8) -> Result<Vec<u8>> {
    let decoder = bounded_png_decoder(png_bytes)?;
    let (w, h) = decoder.dimensions();
    let color = decoder.color_type();
    let mut buf = vec![0u8; decoded_buffer_len(&decoder)?];
    decoder.read_image(&mut buf)?;

    // RGBA → RGB if needed (drop the alpha channel — JPEG can't store it).
    let rgb_buf: Vec<u8> = if color == ColorType::Rgba8 {
        buf.chunks_exact(4)
            .flat_map(|px| [px[0], px[1], px[2]])
            .collect()
    } else {
        buf
    };

    let mut jpeg_bytes = Vec::new();
    let mut enc = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg_bytes, quality);
    enc.encode(&rgb_buf, w, h, ColorType::Rgb8.into())?;
    Ok(jpeg_bytes)
}

// ── Downscale ─────────────────────────────────────────────────────────────

/// Downscale `png_bytes` so neither dimension exceeds `max_dim`.
///
/// `max_dim == 0` is treated as "no cap"; the original bytes are
/// returned unchanged. Aspect ratio is preserved. The resampler is
/// Lanczos3 — same choice the platform crates made before extraction.
pub fn resize_png_if_needed(png_bytes: &[u8], max_dim: u32) -> Result<Vec<u8>> {
    Ok(resize_png_if_needed_cow(Cow::Borrowed(png_bytes), max_dim)?.into_owned())
}

/// Owned-input variant of [`resize_png_if_needed`].
///
/// The common capture path already owns the encoded PNG. Keeping that buffer
/// avoids copying an unchanged screenshot before base64 encoding. When a
/// resize is needed, the compressed source is released as soon as decoding has
/// produced the pixel buffer.
pub fn resize_png_if_needed_owned(png_bytes: Vec<u8>, max_dim: u32) -> Result<Vec<u8>> {
    Ok(resize_png_if_needed_cow(Cow::Owned(png_bytes), max_dim)?.into_owned())
}

fn resize_png_if_needed_cow<'a>(png_bytes: Cow<'a, [u8]>, max_dim: u32) -> Result<Cow<'a, [u8]>> {
    if max_dim == 0 {
        return Ok(png_bytes);
    }
    let (w, h) = png_dimensions(png_bytes.as_ref())?;
    if w <= max_dim && h <= max_dim {
        return Ok(png_bytes);
    }
    let scale = (max_dim as f64) / (w.max(h) as f64);
    let new_w = (w as f64 * scale).round() as u32;
    let new_h = (h as f64 * scale).round() as u32;

    let decoder = bounded_png_decoder(png_bytes.as_ref())?;
    let color = decoder.color_type();
    let mut buf = vec![0u8; decoded_buffer_len(&decoder)?];
    decoder.read_image(&mut buf)?;
    drop(png_bytes);

    let img = match color {
        ColorType::Rgba8 => DynamicImage::ImageRgba8(
            ImageBuffer::from_raw(w, h, buf).ok_or_else(|| anyhow!("invalid RGBA buffer"))?,
        ),
        ColorType::Rgb8 => DynamicImage::ImageRgb8(
            ImageBuffer::from_raw(w, h, buf).ok_or_else(|| anyhow!("invalid RGB buffer"))?,
        ),
        _ => bail!("unsupported color type for resize: {color:?}"),
    };

    let resized = img.resize(new_w, new_h, image::imageops::FilterType::Lanczos3);
    let mut out = Vec::new();
    resized.write_to(&mut std::io::Cursor::new(&mut out), ImageFormat::Png)?;
    Ok(Cow::Owned(out))
}

// ── Crosshair ─────────────────────────────────────────────────────────────

/// Draw a red crosshair at pixel (cx, cy) on a PNG and write it to `path`.
/// Used by `click`'s `debug_image_out` param to verify coordinate spaces.
/// The crosshair uses top-left-origin coords matching the click tool's
/// convention.
///
/// Tilde-expands `~` in `path` to `$HOME` (or `%USERPROFILE%` via the same
/// env-var name on Windows).
pub fn write_crosshair_png(png_bytes: &[u8], cx: f64, cy: f64, path: &str) -> Result<()> {
    let mut img = decode_png_to_rgba8(png_bytes)?;
    draw_crosshair(&mut img, cx, cy);

    let path = if let Some(rest) = path.strip_prefix('~') {
        let home = std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .map_err(|_| {
                anyhow::anyhow!(
                    "Cannot expand `~` in path {path:?}: neither HOME nor USERPROFILE is set"
                )
            })?;
        if home.is_empty() {
            anyhow::bail!("Cannot expand `~` in path {path:?}: HOME/USERPROFILE is empty");
        }
        format!("{home}{rest}")
    } else {
        path.to_owned()
    };
    img.save_with_format(&path, ImageFormat::Png)?;
    Ok(())
}

/// Draw a red crosshair at pixel (cx, cy) and return the modified PNG bytes.
/// Used by the recording subsystem's click-marker callback to produce
/// `click.png` without a temp file.
pub fn crosshair_png_bytes(png_bytes: &[u8], cx: f64, cy: f64) -> Result<Vec<u8>> {
    let mut img = decode_png_to_rgba8(png_bytes)?;
    draw_crosshair(&mut img, cx, cy);

    let mut out = Vec::new();
    DynamicImage::ImageRgba8(img)
        .write_to(&mut std::io::Cursor::new(&mut out), ImageFormat::Png)?;
    Ok(out)
}

/// Internal: decode PNG to a mutable RGBA8 image buffer.
fn decode_png_to_rgba8(png_bytes: &[u8]) -> Result<ImageBuffer<image::Rgba<u8>, Vec<u8>>> {
    let decoder = bounded_png_decoder(png_bytes)?;
    let (w, h) = decoder.dimensions();
    let color = decoder.color_type();
    let mut buf = vec![0u8; decoded_buffer_len(&decoder)?];
    decoder.read_image(&mut buf)?;

    let img: DynamicImage = match color {
        ColorType::Rgba8 => DynamicImage::ImageRgba8(
            ImageBuffer::from_raw(w, h, buf).ok_or_else(|| anyhow!("invalid RGBA buffer"))?,
        ),
        ColorType::Rgb8 => DynamicImage::ImageRgb8(
            ImageBuffer::from_raw(w, h, buf).ok_or_else(|| anyhow!("invalid RGB buffer"))?,
        ),
        _ => bail!("unsupported color type for crosshair: {color:?}"),
    };
    Ok(img.into_rgba8())
}

/// Internal: draw the red ring + horizontal + vertical arms in place.
fn draw_crosshair(img: &mut ImageBuffer<image::Rgba<u8>, Vec<u8>>, cx: f64, cy: f64) {
    let w = img.width();
    let h = img.height();

    let ring_r = (w as f64 / 80.0).max(6.0) as i32;
    let arm_len = (w as f64 / 40.0).max(12.0) as i32;
    let line_w = ((w as f64 / 400.0).max(1.5)) as i32;
    let red = image::Rgba([255u8, 26, 26, 242]);
    let cx = cx as i32;
    let cy = cy as i32;

    // Horizontal + vertical arms.
    for lw in 0..=line_w {
        let off = lw - line_w / 2;
        for dx in -arm_len..=arm_len {
            if let Some(p) = img.get_pixel_mut_checked(
                (cx + dx).clamp(0, w as i32 - 1) as u32,
                (cy + off).clamp(0, h as i32 - 1) as u32,
            ) {
                *p = red;
            }
        }
        for dy in -arm_len..=arm_len {
            if let Some(p) = img.get_pixel_mut_checked(
                (cx + off).clamp(0, w as i32 - 1) as u32,
                (cy + dy).clamp(0, h as i32 - 1) as u32,
            ) {
                *p = red;
            }
        }
    }

    // Stroked ring.
    let steps = (ring_r * 12).max(48) as usize;
    for i in 0..steps {
        let theta = 2.0 * std::f64::consts::PI * i as f64 / steps as f64;
        let rx = (cx as f64 + ring_r as f64 * theta.cos()) as i32;
        let ry = (cy as f64 + ring_r as f64 * theta.sin()) as i32;
        for lw in 0..=line_w {
            let off = lw - line_w / 2;
            for dx in 0..=1 {
                let fx = (rx + off + dx).clamp(0, w as i32 - 1) as u32;
                let fy = (ry + off).clamp(0, h as i32 - 1) as u32;
                *img.get_pixel_mut(fx, fy) = red;
            }
        }
    }
}

// ── PNG dimensions ────────────────────────────────────────────────────────

/// Parse width and height from a PNG file's IHDR chunk.
///
/// Pure byte-slice parsing — does not depend on the `image` crate's
/// decoder. Used by `resize_png_if_needed`'s fast-path size check.
pub fn png_dimensions(data: &[u8]) -> Result<(u32, u32)> {
    // PNG signature: 8 bytes.
    // Then IHDR chunk: 4-byte length, 4-byte "IHDR", 4-byte width, 4-byte height.
    if data.len() < 24 {
        bail!("PNG data too small");
    }
    const PNG_SIG: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];
    if data[..8] != PNG_SIG {
        bail!("not a PNG: signature mismatch");
    }
    if &data[12..16] != b"IHDR" {
        bail!("not a PNG: missing IHDR chunk");
    }
    let w = u32::from_be_bytes([data[16], data[17], data[18], data[19]]);
    let h = u32::from_be_bytes([data[20], data[21], data[22], data[23]]);
    Ok((w, h))
}

// ── Raw RGBA → PNG (used by Windows + Linux capture paths) ────────────────

/// Encode raw RGBA bytes (top-down, row-major) as a PNG.
///
/// Uses the `image` crate's encoder rather than the hand-rolled
/// uncompressed-PNG path that Windows + Linux previously carried — that
/// path was a workspace-local optimization that traded ~5x larger output
/// for less code, but the `image` crate's encoder is fast enough (it
/// already ships in every consumer of `image`, no new code paths) and
/// the smaller output saves more bytes downstream than the encode cost.
///
/// Caller guarantees `rgba.len() == w * h * 4`.
pub fn encode_rgba_to_png(rgba: &[u8], w: u32, h: u32) -> Result<Vec<u8>> {
    validate_rgba_size(rgba, w, h)?;
    encode_rgba_to_png_owned(rgba.to_vec(), w, h)
}

/// Encode an owned raw RGBA buffer as PNG without cloning the pixel data.
pub fn encode_rgba_to_png_owned(rgba: Vec<u8>, w: u32, h: u32) -> Result<Vec<u8>> {
    validate_rgba_size(&rgba, w, h)?;
    let buf: ImageBuffer<image::Rgba<u8>, Vec<u8>> = ImageBuffer::from_raw(w, h, rgba)
        .ok_or_else(|| anyhow!("invalid RGBA buffer for w={w} h={h}"))?;
    let mut out = Vec::new();
    DynamicImage::ImageRgba8(buf)
        .write_to(&mut std::io::Cursor::new(&mut out), ImageFormat::Png)?;
    if out.len() > MAX_CAPTURE_PNG_BYTES {
        bail!(
            "encoded native screenshot is {} bytes, above the CUA capture limit of {MAX_CAPTURE_PNG_BYTES} bytes",
            out.len()
        );
    }
    Ok(out)
}

fn validate_rgba_size(rgba: &[u8], w: u32, h: u32) -> Result<()> {
    let expected = validate_native_capture_dimensions(w, h)?;
    if rgba.len() != expected {
        bail!(
            "encode_rgba_to_png: buffer size {} != w({w}) * h({h}) * 4",
            rgba.len()
        );
    }
    Ok(())
}

/// Encode raw BGRA bytes (top-down, row-major) as a PNG.
///
/// Windows GDI gives us BGRA; we swap channels in-place then defer to
/// [`encode_rgba_to_png_owned`]. Caller guarantees the buffer's size invariant.
pub fn encode_bgra_to_png(bgra: &[u8], w: u32, h: u32) -> Result<Vec<u8>> {
    validate_rgba_size(bgra, w, h)?;
    encode_bgra_to_png_owned(bgra.to_vec(), w, h)
}

/// Encode an owned raw BGRA buffer as PNG without cloning the pixel data.
pub fn encode_bgra_to_png_owned(mut bgra: Vec<u8>, w: u32, h: u32) -> Result<Vec<u8>> {
    validate_rgba_size(&bgra, w, h)?;
    for px in bgra.chunks_exact_mut(4) {
        px.swap(0, 2); // B ↔ R
    }
    encode_rgba_to_png_owned(bgra, w, h)
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn png_dimensions_round_trip() {
        // Build a tiny 3x2 RGBA image then read its dimensions back.
        let rgba = vec![0xFFu8; 3 * 2 * 4];
        let png = encode_rgba_to_png(&rgba, 3, 2).expect("encode");
        let (w, h) = png_dimensions(&png).expect("dimensions");
        assert_eq!((w, h), (3, 2));
    }

    #[test]
    fn png_dimensions_rejects_non_png() {
        assert!(png_dimensions(&[0; 100]).is_err());
        assert!(png_dimensions(b"not a PNG file at all").is_err());
        assert!(png_dimensions(&[]).is_err());
    }

    #[test]
    fn resize_no_op_when_within_bound() {
        let rgba = vec![0u8; 10 * 10 * 4];
        let png = encode_rgba_to_png(&rgba, 10, 10).unwrap();
        let resized = resize_png_if_needed(&png, 100).unwrap();
        // Returned bytes should match the input exactly (no re-encode).
        assert_eq!(resized, png);
    }

    #[test]
    fn resize_no_op_when_max_dim_zero() {
        let rgba = vec![0u8; 100 * 100 * 4];
        let png = encode_rgba_to_png(&rgba, 100, 100).unwrap();
        let resized = resize_png_if_needed(&png, 0).unwrap();
        assert_eq!(resized, png);
    }

    #[test]
    fn resize_downscales_to_long_edge() {
        // 200x100 → max_dim=50 → 50x25 (long edge dictates).
        let rgba = vec![0u8; 200 * 100 * 4];
        let png = encode_rgba_to_png(&rgba, 200, 100).unwrap();
        let resized = resize_png_if_needed(&png, 50).unwrap();
        let (w, h) = png_dimensions(&resized).unwrap();
        assert_eq!(w, 50);
        assert_eq!(h, 25);
    }

    #[test]
    fn png_to_jpeg_strips_alpha() {
        let rgba = vec![0x80u8; 4 * 4 * 4];
        let png = encode_rgba_to_png(&rgba, 4, 4).unwrap();
        let jpeg = png_bytes_to_jpeg(&png, 80).unwrap();
        // JPEG file signature: FF D8 FF.
        assert_eq!(&jpeg[..3], &[0xFF, 0xD8, 0xFF]);
    }

    #[test]
    fn crosshair_returns_valid_png() {
        let rgba = vec![0u8; 40 * 40 * 4];
        let png = encode_rgba_to_png(&rgba, 40, 40).unwrap();
        let marked = crosshair_png_bytes(&png, 20.0, 20.0).unwrap();
        let (w, h) = png_dimensions(&marked).unwrap();
        assert_eq!((w, h), (40, 40));
    }

    #[test]
    fn bgra_to_png_swaps_channels() {
        // 1x1 BGRA pixel: B=10, G=20, R=30, A=40.
        let bgra = vec![10u8, 20, 30, 40];
        let png = encode_bgra_to_png(&bgra, 1, 1).unwrap();
        // Decode and check the RGBA bytes have R↔B swapped.
        let decoder = image::codecs::png::PngDecoder::new(std::io::Cursor::new(&png)).unwrap();
        let mut decoded = vec![0u8; decoder.total_bytes() as usize];
        decoder.read_image(&mut decoded).unwrap();
        assert_eq!(decoded, vec![30u8, 20, 10, 40]); // RGBA: R=30, G=20, B=10, A=40
    }
}
