//! Window screenshot on Linux.
//!
//! Strategy (in order of preference):
//! 1. `xwd -id <xid> -silent | xwdtopnm | pnmtopng` (X11, no focus change)
//! 2. `import -window <xid> png:-` (ImageMagick, widely available)
//! 3. `scrot -u <file>` (focused window fallback)
//! 4. XGetImage via x11rb (pure Rust, no subprocess)

use anyhow::{Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use std::io::Read;
use std::process::{Command, Stdio};

/// Capture a window by X11 XID. Returns raw PNG bytes.
pub fn screenshot_window_bytes(xid: u64) -> Result<Vec<u8>> {
    // Try `import -window <xid> png:-` (ImageMagick).
    if let Ok(bytes) = capture_via_import(xid) {
        return Ok(bytes);
    }
    // Fallback: x11rb XGetImage → returns (b64, w, h); decode the b64 back.
    let (b64, _, _) = capture_via_xgetimage(xid)?;
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD.decode(&b64)?;
    cua_driver_core::image_utils::validate_captured_png(&bytes)?;
    Ok(bytes)
}

/// Capture a window by X11 XID. Returns (base64_png, width, height).
pub fn screenshot_window(xid: u64) -> Result<(String, u32, u32)> {
    // Try `import -window <xid> png:-` (ImageMagick).
    if let Ok(bytes) = capture_via_import(xid) {
        let (w, h) = cua_driver_core::image_utils::png_dimensions(&bytes)?;
        return Ok((BASE64.encode(&bytes), w, h));
    }

    // Fallback: x11rb XGetImage.
    capture_via_xgetimage(xid)
}

fn capture_via_import(xid: u64) -> Result<Vec<u8>> {
    let mut command = Command::new("import");
    command.args(["-window", &xid.to_string(), "png:-"]);
    run_bounded_png_command(&mut command, "import")
}

/// Capture a PNG from a subprocess without retaining unbounded stdout.
pub(crate) fn run_bounded_png_command(command: &mut Command, label: &str) -> Result<Vec<u8>> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("{label} did not expose stdout"))?;
    let mut bytes = Vec::new();
    if let Err(error) = stdout
        .by_ref()
        .take(cua_driver_core::image_utils::MAX_CAPTURE_PNG_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
    {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error.into());
    }
    drop(stdout);
    if bytes.len() > cua_driver_core::image_utils::MAX_CAPTURE_PNG_BYTES {
        let _ = child.kill();
        let _ = child.wait();
        bail!(
            "{label} screenshot exceeds the CUA {}-byte capture limit",
            cua_driver_core::image_utils::MAX_CAPTURE_PNG_BYTES
        );
    }
    let status = child.wait()?;
    if !status.success() || bytes.is_empty() {
        bail!("{label} failed to produce a screenshot (status {status})");
    }
    cua_driver_core::image_utils::validate_captured_png(&bytes)?;
    Ok(bytes)
}

fn capture_via_xgetimage(xid: u64) -> Result<(String, u32, u32)> {
    use x11rb::protocol::xproto::*;
    use x11rb::rust_connection::RustConnection;

    let (conn, _) = RustConnection::connect(None)?;
    let window = xid as u32;

    let geom = conn.get_geometry(window)?.reply()?;
    let w = geom.width as u32;
    let h = geom.height as u32;
    let rgba_len = cua_driver_core::image_utils::validate_native_capture_dimensions(w, h)?;

    let img = conn
        .get_image(
            ImageFormat::Z_PIXMAP,
            window,
            0,
            0,
            w as u16,
            h as u16,
            !0u32,
        )?
        .reply()?;

    // The raw data is BGRA or BGRX depending on depth.
    // Encode as a minimal PNG.
    let bytes = img.data;
    let (bpp, has_alpha) = match img.depth {
        32 => (4usize, true),
        24 => (4usize, false),
        _ => bail!("Unsupported depth: {}", img.depth),
    };

    // Convert to RGBA.
    let mut rgba = Vec::with_capacity(rgba_len);
    for chunk in bytes.chunks_exact(bpp) {
        let (b, g, r) = (chunk[0], chunk[1], chunk[2]);
        let a = if has_alpha { chunk[3] } else { 255 };
        rgba.extend_from_slice(&[r, g, b, a]);
    }

    drop(bytes);
    let png = cua_driver_core::image_utils::encode_rgba_to_png_owned(rgba, w, h)?;
    Ok((BASE64.encode(&png), w, h))
}

/// Public version of png_dimensions for use in tool code.
pub fn png_dimensions_pub(data: &[u8]) -> Result<(u32, u32)> {
    cua_driver_core::image_utils::png_dimensions(data)
}

// NOTE: the previously-inline `png_dimensions`, `write_uncompressed_png`,
// `write_png_chunk`, `zlib_store`, `adler32` (and `crc32_ieee` below)
// were extracted to `cua_driver_core::image_utils` in the 2026-05 dedup
// audit so all three platforms call the same code. See
// `CUA_DRIVER_RS_DEDUP_AUDIT.md`. RGBA-encoding callers below now go
// through `cua_driver_core::image_utils::encode_rgba_to_png`.

/// Capture the primary display (root window) as raw PNG bytes.
///
/// Dispatch:
/// - Native Wayland (`CUA_DRIVER_RS_ENABLE_WAYLAND=1` + Wayland session):
///   routes through [`crate::wayland::screenshot_display_dispatch`] which
///   owns the complete GNOME helper → wlroots screencopy →
///   ext-image-copy-capture-v1 → portal Screenshot → X11 cascade. An
///   available GNOME helper's capture failure is terminal.
/// - X11 / Wayland-disabled: ImageMagick `import` → x11rb `XGetImage`.
pub fn screenshot_display_bytes() -> Result<Vec<u8>> {
    screenshot_display_bytes_with_dispatch(
        crate::wayland::is_wayland(),
        crate::wayland::screenshot_display_dispatch,
        screenshot_display_bytes_x11,
    )
}

fn screenshot_display_bytes_with_dispatch(
    wayland_enabled: bool,
    wayland_capture: impl FnOnce() -> Result<Vec<u8>>,
    x11_capture: impl FnOnce() -> Result<Vec<u8>>,
) -> Result<Vec<u8>> {
    let bytes = if wayland_enabled {
        wayland_capture()
    } else {
        x11_capture()
    }?;
    cua_driver_core::image_utils::validate_captured_png(&bytes)?;
    Ok(bytes)
}

/// X11-only display capture path — extracted so the wayland cascade in
/// [`crate::wayland::screenshot_display_dispatch`] can call it as a final
/// fallback without re-entering [`screenshot_display_bytes`] (which would
/// loop forever once we're on Wayland).
pub(crate) fn screenshot_display_bytes_x11() -> Result<Vec<u8>> {
    // Try `import -window root png:-` (ImageMagick).
    let mut command = Command::new("import");
    command.args(["-window", "root", "png:-"]);
    if let Ok(bytes) = run_bounded_png_command(&mut command, "import") {
        return Ok(bytes);
    }
    // Fallback: x11rb XGetImage on the root window.
    use x11rb::connection::Connection;
    use x11rb::protocol::xproto::*;
    use x11rb::rust_connection::RustConnection;
    let (conn, screen_num) = RustConnection::connect(None)
        .map_err(|e| anyhow::anyhow!("{e}{}", crate::no_display_hint()))?;
    let root = conn.setup().roots[screen_num].root;
    // Get root geometry.
    let geom = conn.get_geometry(root)?.reply()?;
    let w = geom.width as u32;
    let h = geom.height as u32;
    // WSLg / headless XWayland quirk: the X server connects but the root
    // window reports a 0-px geometry until a real output is attached.
    // `get_image` with w/h == 0 yields an empty buffer that later decodes
    // to null/zero dimensions downstream. Fail with an actionable, typed
    // error instead of emitting a 0-px image. See issue #2005.
    if w == 0 || h == 0 {
        anyhow::bail!(
            "X11 root window reports a 0x0 geometry — no usable display to capture.{}",
            crate::no_display_hint()
        );
    }
    let rgba_len = cua_driver_core::image_utils::validate_native_capture_dimensions(w, h)?;
    let img = conn
        .get_image(ImageFormat::Z_PIXMAP, root, 0, 0, w as u16, h as u16, !0u32)?
        .reply()?;
    let bytes = img.data;
    let bpp = match img.depth {
        32 | 24 => 4usize,
        _ => anyhow::bail!("Unsupported depth"),
    };
    let mut rgba = Vec::with_capacity(rgba_len);
    for chunk in bytes.chunks_exact(bpp) {
        let (b, g, r) = (chunk[0], chunk[1], chunk[2]);
        rgba.extend_from_slice(&[r, g, b, 255]);
    }
    drop(bytes);
    cua_driver_core::image_utils::encode_rgba_to_png_owned(rgba, w, h)
}

/// Capture the primary display, returning (base64_png, width, height).
pub fn screenshot_display() -> Result<(String, u32, u32)> {
    let png_bytes = screenshot_display_bytes()?;
    let (w, h) = cua_driver_core::image_utils::validate_captured_png(&png_bytes)?;
    Ok((BASE64.encode(&png_bytes), w, h))
}

// PNG/JPEG/resize/crosshair helpers — re-exports of the shared
// `cua_driver_core::image_utils` module. The previous file-local copies were
// near-identical to the macOS and Windows versions; the dedup-audit
// (2026-05) moved them all to one place.

/// Convert PNG bytes to JPEG at the given quality (1–95).
pub fn png_bytes_to_jpeg(png_bytes: &[u8], quality: u8) -> Result<Vec<u8>> {
    cua_driver_core::image_utils::png_bytes_to_jpeg(png_bytes, quality)
}

/// Downscale `png_bytes` so neither dimension exceeds `max_dim`.
/// If `max_dim == 0` or the image already fits, returns a copy of the
/// original bytes unchanged.
pub fn resize_png_if_needed(png_bytes: &[u8], max_dim: u32) -> Result<Vec<u8>> {
    cua_driver_core::image_utils::resize_png_if_needed(png_bytes, max_dim)
}

/// Owned-input variant that reuses the capture buffer when no resize is needed.
pub fn resize_png_if_needed_owned(png_bytes: Vec<u8>, max_dim: u32) -> Result<Vec<u8>> {
    cua_driver_core::image_utils::resize_png_if_needed_owned(png_bytes, max_dim)
}

/// Draw a red crosshair at pixel (cx, cy) on a PNG image and return
/// modified PNG bytes. Used by recording's click-marker callback to
/// produce click.png.
pub fn crosshair_png_bytes(png_bytes: &[u8], cx: f64, cy: f64) -> Result<Vec<u8>> {
    cua_driver_core::image_utils::crosshair_png_bytes(png_bytes, cx, cy)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn available_gnome_helper_failure_is_terminal_at_public_boundary() {
        let x11_called = Cell::new(false);

        let result = screenshot_display_bytes_with_dispatch(
            true,
            || Err(anyhow::anyhow!("GNOME compositor helper capture failed")),
            || {
                x11_called.set(true);
                Ok(vec![1, 2, 3])
            },
        );

        assert_eq!(
            result.unwrap_err().to_string(),
            "GNOME compositor helper capture failed"
        );
        assert!(!x11_called.get(), "public boundary retried X11 capture");
    }

    #[test]
    fn wayland_disabled_uses_x11_capture() {
        let wayland_called = Cell::new(false);

        let result = screenshot_display_bytes_with_dispatch(
            false,
            || {
                wayland_called.set(true);
                Err(anyhow::anyhow!("Wayland capture should not run"))
            },
            || Ok(vec![1, 2, 3]),
        );

        assert_eq!(result.unwrap(), vec![1, 2, 3]);
        assert!(!wayland_called.get());
    }
}
