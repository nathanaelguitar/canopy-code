//! Shared memory-size formatting, ported from
//! `packages/core/src/utils/formatters.ts`.

pub const BYTES_PER_KB: u64 = 1024;
pub const BYTES_PER_MB: u64 = BYTES_PER_KB * 1024;
pub const BYTES_PER_GB: u64 = BYTES_PER_MB * 1024;

/// Format a byte count using binary units. Unit selection uses the value
/// rounded to one decimal place, matching the displayed KB/MB figure.
pub fn format_memory_usage(bytes: f64) -> String {
    let kb = bytes / BYTES_PER_KB as f64;
    if to_fixed_number(kb, 1) < 1024.0 {
        return format!("{} KB", js_to_fixed(kb, 1));
    }

    let mb = bytes / BYTES_PER_MB as f64;
    if to_fixed_number(mb, 1) < 1024.0 {
        return format!("{} MB", js_to_fixed(mb, 1));
    }

    format!("{} GB", js_to_fixed(bytes / BYTES_PER_GB as f64, 2))
}

/// `Number(value.toFixed(digits))`, kept separate because the rounded value
/// determines which unit is printed.
fn to_fixed_number(value: f64, digits: usize) -> f64 {
    js_to_fixed(value, digits).parse().unwrap_or(value)
}

/// Match JavaScript `Number.prototype.toFixed` for the one- and two-digit
/// precisions used by this formatter. Rust's standard float formatter does
/// not promise JavaScript's exact-binary rounding rule (ties choose the
/// larger integer for the absolute value), so compute that integer directly
/// from the binary64 significand and exponent.
fn js_to_fixed(value: f64, digits: usize) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Infinity".to_owned()
        } else {
            "Infinity".to_owned()
        };
    }

    let negative = value < 0.0;
    let absolute = value.abs();
    let sign = if negative { "-" } else { "" };
    // ECMA-262 delegates to Number::toString for magnitudes at or above 1e21.
    if absolute >= 1e21 {
        return format!("{sign}{}", js_large_number_to_string(absolute));
    }

    let scale = 10_u128.pow(digits as u32);
    let rounded = round_binary64_scaled(absolute, scale);
    let integer = rounded / scale;
    if digits == 0 {
        return format!("{sign}{integer}");
    }
    let fraction = rounded % scale;
    format!("{sign}{integer}.{fraction:0digits$}")
}

fn round_binary64_scaled(value: f64, scale: u128) -> u128 {
    if value == 0.0 {
        return 0;
    }

    let bits = value.to_bits();
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1_u64 << 52) - 1);
    let (significand, exponent) = if exponent_bits == 0 {
        (fraction as u128, -1074)
    } else {
        (
            ((1_u64 << 52) | fraction) as u128,
            exponent_bits - 1023 - 52,
        )
    };

    // The largest value reaching this path is less than 1e21. For the
    // formatter's one/two decimal places, this product and any left shift fit
    // in u128.
    let numerator = significand * scale;
    let binary_exponent = exponent;
    if binary_exponent >= 0 {
        numerator << binary_exponent as u32
    } else {
        let shift = (-binary_exponent) as u32;
        if shift >= 128 {
            return 0;
        }
        let integer = numerator >> shift;
        let remainder_mask = (1_u128 << shift) - 1;
        let remainder = numerator & remainder_mask;
        let halfway = 1_u128 << (shift - 1);
        integer + if remainder >= halfway { 1 } else { 0 }
    }
}

fn js_large_number_to_string(value: f64) -> String {
    let scientific = format!("{value:e}");
    let Some((mantissa, exponent)) = scientific.split_once('e') else {
        return scientific;
    };
    let mantissa = mantissa.strip_suffix(".0").unwrap_or(mantissa);
    let exponent: i32 = exponent.parse().unwrap_or_default();
    format!("{mantissa}e{exponent:+}")
}
