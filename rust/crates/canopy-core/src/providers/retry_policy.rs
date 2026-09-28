//! Shared retry delay policy and provider `Retry-After` parsing.
//!
//! The delay calculation is pure apart from caller-injected clock and random
//! values. Callers should pass a random value only when they enable jitter;
//! tests can therefore pin both HTTP-date handling and jitter without global
//! time or RNG state.

use chrono::DateTime;
use std::collections::HashMap;

use reqwest::header::HeaderMap;
use serde_json::Value;

/// Largest delay representable by Node.js `setTimeout`.
///
/// Keeping this bound preserves the source policy's protection against
/// oversized timer values overflowing to an immediate retry.
pub const MAX_TIMEOUT_MS: f64 = 2_147_483_647.0;

/// Whether a response's `Retry-After` header affects the retry delay.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RetryAfterMode {
    /// Ignore `Retry-After`; this is the default behavior.
    #[default]
    Ignore,
    /// Treat a valid `Retry-After` delay as a floor on exponential backoff.
    Minimum,
}

/// Header representations accepted from provider SDK errors.
///
/// This covers native HTTP header maps, plain string maps, arbitrary JSON
/// objects, and Headers-like objects represented by an injected `.get()`
/// callback. Object keys are matched case-insensitively. A getter is called
/// with the normalized name `retry-after`.
#[derive(Clone, Copy)]
pub enum RetryAfterHeaderSource<'a> {
    Http(&'a HeaderMap),
    StringMap(&'a HashMap<String, String>),
    JsonObject(&'a Value),
    Getter(&'a dyn Fn(&str) -> Option<String>),
}

impl std::fmt::Debug for RetryAfterHeaderSource<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(_) => formatter.write_str("Http(..)"),
            Self::StringMap(_) => formatter.write_str("StringMap(..)"),
            Self::JsonObject(_) => formatter.write_str("JsonObject(..)"),
            Self::Getter(_) => formatter.write_str("Getter(..)"),
        }
    }
}

/// Header locations supported on provider SDK errors. Direct headers are
/// checked first; response headers are consulted only when the direct source
/// does not yield a string value, matching the source utility's nullish
/// fallback behavior.
#[derive(Clone, Copy, Debug, Default)]
pub struct RetryAfterHeaders<'a> {
    pub direct: Option<RetryAfterHeaderSource<'a>>,
    pub response: Option<RetryAfterHeaderSource<'a>>,
}

/// Options for [`get_retry_delay_ms`]. Delays use floating-point milliseconds
/// to preserve fractional `Retry-After` seconds and jitter math.
#[derive(Clone, Copy, Debug)]
pub struct RetryDelayPolicyOptions<'a> {
    pub attempt: i64,
    pub initial_delay_ms: f64,
    pub max_delay_ms: f64,
    pub error: Option<RetryAfterHeaders<'a>>,
    pub retry_after_mode: RetryAfterMode,
    pub retry_after_max_delay_ms: Option<f64>,
    pub jitter_ratio: f64,
}

impl<'a> RetryDelayPolicyOptions<'a> {
    pub fn new(attempt: i64, initial_delay_ms: f64, max_delay_ms: f64) -> Self {
        Self {
            attempt,
            initial_delay_ms,
            max_delay_ms,
            error: None,
            retry_after_mode: RetryAfterMode::Ignore,
            retry_after_max_delay_ms: None,
            jitter_ratio: 0.0,
        }
    }
}

/// Calculates a capped exponential retry delay, optionally applying jitter.
///
/// `now_ms` is used only for HTTP-date `Retry-After` values. `random` is called
/// only when jitter is enabled and no positive `Retry-After` value is honored.
pub fn get_retry_delay_ms(
    options: &RetryDelayPolicyOptions<'_>,
    now_ms: i64,
    mut random: impl FnMut() -> f64,
) -> f64 {
    let normalized_attempt = options.attempt.max(1);
    let delay_ceiling_ms = options.max_delay_ms.min(MAX_TIMEOUT_MS);
    // 2^31 already exceeds any supported timeout. Capping the exponent avoids
    // overflowing for very large attempt counts in persistent retry mode.
    let exponent = normalized_attempt.saturating_sub(1).min(31) as i32;
    let capped_exponential_delay_ms =
        (options.initial_delay_ms * 2_f64.powi(exponent)).min(delay_ceiling_ms);

    let retry_after_ms = match (options.retry_after_mode, options.error) {
        (RetryAfterMode::Minimum, Some(error)) => get_retry_after_delay_ms(error, now_ms),
        _ => None,
    };

    if let Some(retry_after_ms) = retry_after_ms.filter(|delay| *delay > 0.0) {
        let retry_after_cap_ms = options
            .retry_after_max_delay_ms
            .unwrap_or(options.max_delay_ms)
            .min(MAX_TIMEOUT_MS);
        return capped_exponential_delay_ms.max(retry_after_ms.min(retry_after_cap_ms));
    }

    if options.jitter_ratio <= 0.0 {
        return capped_exponential_delay_ms;
    }

    let jitter = capped_exponential_delay_ms * options.jitter_ratio * (random() * 2.0 - 1.0);
    (capped_exponential_delay_ms + jitter)
        .max(0.0)
        .min(delay_ceiling_ms)
}

/// Extracts and parses `Retry-After` from direct or response headers.
///
/// Decimal seconds (including fractional seconds) are accepted only in RFC
/// delay-seconds form. Otherwise the value is parsed as an HTTP date. Past
/// dates produce zero; malformed values produce `None`.
pub fn get_retry_after_delay_ms(error: RetryAfterHeaders<'_>, now_ms: i64) -> Option<f64> {
    let value = error
        .direct
        .and_then(retry_after_header)
        .or_else(|| error.response.and_then(retry_after_header))?;
    parse_retry_after_value(&value, now_ms)
}

fn retry_after_header(source: RetryAfterHeaderSource<'_>) -> Option<String> {
    match source {
        RetryAfterHeaderSource::Http(headers) => headers
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        RetryAfterHeaderSource::StringMap(headers) => headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
            .map(|(_, value)| value.clone()),
        RetryAfterHeaderSource::JsonObject(value) => value
            .as_object()?
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
            .and_then(|(_, value)| value.as_str())
            .map(str::to_owned),
        RetryAfterHeaderSource::Getter(get) => get("retry-after"),
    }
}

fn parse_retry_after_value(value: &str, now_ms: i64) -> Option<f64> {
    let trimmed = value.trim();
    if is_plain_decimal_seconds(trimmed) {
        let seconds = trimmed.parse::<f64>().ok()?;
        if seconds.is_finite() && seconds >= 0.0 {
            return Some((seconds * 1000.0).min(MAX_TIMEOUT_MS));
        }
    }

    let retry_at = DateTime::parse_from_rfc2822(trimmed).ok()?;
    let retry_at_ms = retry_at.timestamp_millis();
    let delay_ms = i128::from(retry_at_ms) - i128::from(now_ms);
    if delay_ms <= 0 {
        Some(0.0)
    } else {
        Some((delay_ms as f64).min(MAX_TIMEOUT_MS))
    }
}

/// Matches the source's RFC delay-seconds grammar: digits, optionally followed
/// by a decimal point and one or more digits.
fn is_plain_decimal_seconds(value: &str) -> bool {
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fractional = parts.next();
    !whole.is_empty()
        && whole.bytes().all(|byte| byte.is_ascii_digit())
        && match fractional {
            None => true,
            Some(fractional) => {
                !fractional.is_empty()
                    && fractional.bytes().all(|byte| byte.is_ascii_digit())
                    && parts.next().is_none()
            }
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderName, HeaderValue};

    const NOW_MS: i64 = 1_767_225_600_000; // 2026-01-01T00:00:00.000Z

    fn with_retry_after(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "retry-after",
            HeaderValue::from_bytes(value.as_bytes()).expect("valid test header"),
        );
        headers
    }

    fn options<'a>(attempt: i64, initial: f64, max: f64) -> RetryDelayPolicyOptions<'a> {
        RetryDelayPolicyOptions::new(attempt, initial, max)
    }

    #[test]
    fn calculates_capped_exponential_delays_and_normalizes_zero_attempt() {
        let first = options(0, 60_000.0, 300_000.0);
        let second = options(2, 60_000.0, 300_000.0);
        let capped = options(10, 60_000.0, 300_000.0);
        assert_eq!(get_retry_delay_ms(&first, NOW_MS, || 0.5), 60_000.0);
        assert_eq!(get_retry_delay_ms(&second, NOW_MS, || 0.5), 120_000.0);
        assert_eq!(get_retry_delay_ms(&capped, NOW_MS, || 0.5), 300_000.0);
    }

    #[test]
    fn uses_retry_after_as_a_minimum_and_preserves_exponential_floor() {
        let retry_after = with_retry_after("180");
        let mut first = options(1, 60_000.0, 300_000.0);
        first.error = Some(RetryAfterHeaders {
            direct: Some(RetryAfterHeaderSource::Http(&retry_after)),
            response: None,
        });
        first.retry_after_mode = RetryAfterMode::Minimum;
        assert_eq!(get_retry_delay_ms(&first, NOW_MS, || 1.0), 180_000.0);

        let short_retry_after = with_retry_after("30");
        let mut second = options(2, 60_000.0, 300_000.0);
        second.error = Some(RetryAfterHeaders {
            direct: Some(RetryAfterHeaderSource::Http(&short_retry_after)),
            response: None,
        });
        second.retry_after_mode = RetryAfterMode::Minimum;
        assert_eq!(get_retry_delay_ms(&second, NOW_MS, || 0.5), 120_000.0);
    }

    #[test]
    fn caps_retry_after_but_not_the_exponential_floor() {
        let retry_after = with_retry_after("600");
        let mut capped_after = options(1, 60_000.0, 300_000.0);
        capped_after.error = Some(RetryAfterHeaders {
            direct: Some(RetryAfterHeaderSource::Http(&retry_after)),
            response: None,
        });
        capped_after.retry_after_mode = RetryAfterMode::Minimum;
        capped_after.retry_after_max_delay_ms = Some(300_000.0);
        assert_eq!(get_retry_delay_ms(&capped_after, NOW_MS, || 0.5), 300_000.0);

        let shorter_after = with_retry_after("30");
        let mut exponential_wins = options(4, 60_000.0, 300_000.0);
        exponential_wins.error = Some(RetryAfterHeaders {
            direct: Some(RetryAfterHeaderSource::Http(&shorter_after)),
            response: None,
        });
        exponential_wins.retry_after_mode = RetryAfterMode::Minimum;
        exponential_wins.retry_after_max_delay_ms = Some(100_000.0);
        assert_eq!(
            get_retry_delay_ms(&exponential_wins, NOW_MS, || 0.5),
            300_000.0
        );
    }

    #[test]
    fn skips_jitter_when_retry_after_is_honored() {
        let retry_after = with_retry_after("180");
        let mut policy = options(1, 60_000.0, 300_000.0);
        policy.error = Some(RetryAfterHeaders {
            direct: Some(RetryAfterHeaderSource::Http(&retry_after)),
            response: None,
        });
        policy.retry_after_mode = RetryAfterMode::Minimum;
        policy.jitter_ratio = 0.3;
        assert_eq!(
            get_retry_delay_ms(&policy, NOW_MS, || panic!("not used")),
            180_000.0
        );
    }

    #[test]
    fn applies_injected_jitter_and_clamps_it_to_delay_ceiling() {
        let mut high = options(2, 100.0, 250.0);
        high.jitter_ratio = 0.3;
        assert_eq!(get_retry_delay_ms(&high, NOW_MS, || 1.0), 250.0);
        assert_eq!(get_retry_delay_ms(&high, NOW_MS, || 0.0), 140.0);
    }

    #[test]
    fn reads_direct_response_and_case_insensitive_headers() {
        let direct = with_retry_after("180");
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: Some(RetryAfterHeaderSource::Http(&direct)),
                    response: None,
                },
                NOW_MS
            ),
            Some(180_000.0)
        );

        let response = with_retry_after("180");
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: None,
                    response: Some(RetryAfterHeaderSource::Http(&response)),
                },
                NOW_MS
            ),
            Some(180_000.0)
        );

        let mut mixed_case = HeaderMap::new();
        mixed_case.insert(
            HeaderName::from_bytes(b"Retry-After").expect("header name"),
            HeaderValue::from_static("180"),
        );
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: Some(RetryAfterHeaderSource::Http(&mixed_case)),
                    response: None,
                },
                NOW_MS
            ),
            Some(180_000.0)
        );

        let invalid_direct = HeaderMap::from_iter([(
            HeaderName::from_static("retry-after"),
            HeaderValue::from_bytes(&[0xff]).expect("opaque header value"),
        )]);
        let valid_response = with_retry_after("180");
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: Some(RetryAfterHeaderSource::Http(&invalid_direct)),
                    response: Some(RetryAfterHeaderSource::Http(&valid_response)),
                },
                NOW_MS
            ),
            Some(180_000.0)
        );
    }

    #[test]
    fn supports_string_maps_json_objects_and_headers_like_getters() {
        let string_map = HashMap::from([("Retry-After".to_owned(), "180".to_owned())]);
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: Some(RetryAfterHeaderSource::StringMap(&string_map)),
                    response: None,
                },
                NOW_MS
            ),
            Some(180_000.0)
        );

        let json_headers = serde_json::json!({"Retry-After": "1.5"});
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: Some(RetryAfterHeaderSource::JsonObject(&json_headers)),
                    response: None,
                },
                NOW_MS
            ),
            Some(1_500.0)
        );

        let getter = |name: &str| {
            assert_eq!(name, "retry-after");
            Some("30".to_owned())
        };
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: Some(RetryAfterHeaderSource::Getter(&getter)),
                    response: None,
                },
                NOW_MS
            ),
            Some(30_000.0)
        );
    }

    #[test]
    fn falls_back_to_response_only_when_direct_lookup_has_no_string_value() {
        let non_string_json = serde_json::json!({"Retry-After": 180});
        let response = with_retry_after("45");
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: Some(RetryAfterHeaderSource::JsonObject(&non_string_json)),
                    response: Some(RetryAfterHeaderSource::Http(&response)),
                },
                NOW_MS
            ),
            Some(45_000.0)
        );

        let malformed_direct = HashMap::from([("retry-after".to_owned(), "invalid".to_owned())]);
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: Some(RetryAfterHeaderSource::StringMap(&malformed_direct)),
                    response: Some(RetryAfterHeaderSource::Http(&response)),
                },
                NOW_MS
            ),
            None,
            "a direct string header is parsed before response fallback"
        );
    }

    #[test]
    fn parses_http_dates_using_an_injected_clock_and_returns_zero_for_past_dates() {
        let future = with_retry_after("Thu, 01 Jan 2026 00:03:00 GMT");
        let past = with_retry_after("Thu, 01 Jan 2026 00:00:00 GMT");
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: Some(RetryAfterHeaderSource::Http(&future)),
                    response: None,
                },
                NOW_MS
            ),
            Some(180_000.0)
        );
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: Some(RetryAfterHeaderSource::Http(&past)),
                    response: None,
                },
                NOW_MS + 180_000
            ),
            Some(0.0)
        );

        let far_future = with_retry_after("Fri, 01 Jan 2100 00:00:00 GMT");
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: Some(RetryAfterHeaderSource::Http(&far_future)),
                    response: None,
                },
                NOW_MS
            ),
            Some(MAX_TIMEOUT_MS)
        );
    }

    #[test]
    fn rejects_malformed_and_non_rfc_numeric_values() {
        for value in ["not a retry-after value", "0x10", "1e3", ".5", "1."] {
            let headers = with_retry_after(value);
            assert_eq!(
                get_retry_after_delay_ms(
                    RetryAfterHeaders {
                        direct: Some(RetryAfterHeaderSource::Http(&headers)),
                        response: None,
                    },
                    NOW_MS
                ),
                None,
                "value: {value}"
            );
        }
    }

    #[test]
    fn handles_fractional_seconds_and_caps_large_values() {
        let fractional = with_retry_after("1.5");
        let oversized = with_retry_after("2592000");
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: Some(RetryAfterHeaderSource::Http(&fractional)),
                    response: None,
                },
                NOW_MS
            ),
            Some(1_500.0)
        );
        assert_eq!(
            get_retry_after_delay_ms(
                RetryAfterHeaders {
                    direct: Some(RetryAfterHeaderSource::Http(&oversized)),
                    response: None,
                },
                NOW_MS
            ),
            Some(MAX_TIMEOUT_MS)
        );
    }

    #[test]
    fn ignores_retry_after_by_default_even_when_error_is_present() {
        let retry_after = with_retry_after("180");
        let mut policy = options(1, 60_000.0, 300_000.0);
        policy.error = Some(RetryAfterHeaders {
            direct: Some(RetryAfterHeaderSource::Http(&retry_after)),
            response: None,
        });
        assert_eq!(get_retry_delay_ms(&policy, NOW_MS, || 0.5), 60_000.0);
    }

    #[test]
    fn caps_large_attempts_and_oversized_max_delay_at_timer_limit() {
        assert_eq!(
            get_retry_delay_ms(&options(5_000, 1_000.0, 300_000.0), NOW_MS, || 0.5),
            300_000.0
        );
        let oversized_max = options(5_000, 1_000.0, 5_000_000_000.0);
        assert_eq!(
            get_retry_delay_ms(&oversized_max, NOW_MS, || 0.5),
            MAX_TIMEOUT_MS
        );

        let mut jittered = oversized_max;
        jittered.jitter_ratio = 0.3;
        assert_eq!(
            get_retry_delay_ms(&jittered, NOW_MS, || 1.0),
            MAX_TIMEOUT_MS
        );
    }
}
