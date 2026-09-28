//! Opt-in normalized-coordinate shim matching `packages/mobile-mcp/src/coord-norm.ts`.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScreenSize {
    pub width: u32,
    pub height: u32,
}

static SCREEN_SIZES: OnceLock<Mutex<HashMap<String, ScreenSize>>> = OnceLock::new();

pub fn normalized_enabled() -> bool {
    std::env::var("MOBILE_MCP_COORDINATE_SPACE").is_ok_and(|value| value == "1")
}

pub fn coordinate_scale() -> u32 {
    std::env::var("MOBILE_MCP_COORDINATE_SCALE")
        .ok()
        .and_then(|raw| parse_int_base10_prefix(&raw))
        .unwrap_or(1000)
}

fn parse_int_base10_prefix(raw: &str) -> Option<u32> {
    let raw = raw.trim_start_matches(is_javascript_whitespace);
    let bytes = raw.as_bytes();
    let mut index = 0;
    let negative = match bytes.first() {
        Some(b'-') => {
            index = 1;
            true
        }
        Some(b'+') => {
            index = 1;
            false
        }
        _ => false,
    };

    let mut value = 0_u32;
    let mut saw_digit = false;
    while let Some(byte @ b'0'..=b'9') = bytes.get(index).copied() {
        saw_digit = true;
        value = value.checked_mul(10)?.checked_add(u32::from(byte - b'0'))?;
        index += 1;
    }

    if saw_digit && !negative && value > 0 {
        Some(value)
    } else {
        None
    }
}

fn is_javascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}

pub fn norm_to_px(norm: f64, dimension: u32, scale: u32) -> f64 {
    ((norm / f64::from(scale)) * f64::from(dimension)).round()
}

pub fn has_coord_fields(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "mobile_click_on_screen_at_coordinates"
            | "mobile_double_tap_on_screen"
            | "mobile_long_press_on_screen_at_coordinates"
            | "mobile_swipe_on_screen"
    )
}

pub fn denormalize_args(
    tool_name: &str,
    args: &mut Value,
    screen: ScreenSize,
    scale: u32,
) -> Result<(), String> {
    if !has_coord_fields(tool_name) {
        return Ok(());
    }
    let Some(object) = args.as_object_mut() else {
        return Ok(());
    };
    for (field, dimension) in [("x", screen.width), ("y", screen.height)] {
        if let Some(value) = object.get(field).and_then(Value::as_f64) {
            validate_normalized(field, value, scale)?;
            object.insert(
                field.to_owned(),
                Value::from(norm_to_px(value, dimension, scale)),
            );
        }
    }
    if tool_name == "mobile_swipe_on_screen" {
        if let Some(distance) = object.get("distance").and_then(Value::as_f64) {
            validate_normalized("distance", distance, scale)?;
            let vertical = object
                .get("direction")
                .and_then(Value::as_str)
                .is_some_and(|direction| matches!(direction, "up" | "down"));
            let dimension = if vertical {
                screen.height
            } else {
                screen.width
            };
            object.insert(
                "distance".to_owned(),
                Value::from(norm_to_px(distance, dimension, scale)),
            );
        }
    }
    Ok(())
}

fn validate_normalized(field: &str, value: f64, scale: u32) -> Result<(), String> {
    if !(0.0..=f64::from(scale)).contains(&value) {
        return Err(format!(
            "Coordinate '{field}' value {value} is out of the normalized range [0, {scale}]. Use normalized coordinates (0-{scale}), not pixel coordinates."
        ));
    }
    Ok(())
}

pub fn cache_screen_size(device: &str, width: u32, height: u32) {
    SCREEN_SIZES
        .get_or_init(Default::default)
        .lock()
        .expect("screen-size cache poisoned")
        .insert(device.to_owned(), ScreenSize { width, height });
}

pub fn invalidate_screen_size(device: &str) {
    if let Some(cache) = SCREEN_SIZES.get() {
        cache
            .lock()
            .expect("screen-size cache poisoned")
            .remove(device);
    }
}

pub fn get_cached_screen_size(device: &str) -> Option<ScreenSize> {
    SCREEN_SIZES
        .get()
        .and_then(|cache| cache.lock().ok()?.get(device).copied())
}

pub fn ingest_screen_size(device: &str, response: &str) {
    let Some(rest) = response.strip_prefix("Screen size is ") else {
        return;
    };
    let Some(dimensions) = rest.strip_suffix(" pixels") else {
        return;
    };
    let Some((width, height)) = dimensions.split_once('x') else {
        return;
    };
    if let (Ok(width), Ok(height)) = (width.parse(), height.parse()) {
        cache_screen_size(device, width, height);
    }
}

pub fn rewrite_description(description: &str, scale: u32) -> String {
    description.replace("in pixels", &format!("in 0-{scale} normalized coordinates"))
}
