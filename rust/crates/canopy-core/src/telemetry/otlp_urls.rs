//! Resolve signal-specific HTTP OTLP exporter endpoints.
//!
//! This mirrors `packages/core/src/telemetry/otlp-urls.ts`. `reqwest::Url`
//! uses the WHATWG URL implementation from the `url` crate, which provides
//! the same hierarchical URL parsing, path serialization, and percent
//! encoding model as JavaScript's `URL` for the HTTP URLs used by OTLP.

use std::error::Error;
use std::fmt;

use reqwest::Url;

/// An OTLP signal whose HTTP exporter uses a signal-specific path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpSignal {
    Traces,
    Logs,
    Metrics,
}

impl OtlpSignal {
    const fn path(self) -> &'static str {
        match self {
            Self::Traces => "v1/traces",
            Self::Logs => "v1/logs",
            Self::Metrics => "v1/metrics",
        }
    }
}

/// Failure to parse an OTLP endpoint as an absolute URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpUrlError {
    message: String,
}

impl fmt::Display for OtlpUrlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid OTLP endpoint URL: {}", self.message)
    }
}

impl Error for OtlpUrlError {}

/// Resolve a base endpoint to the HTTP OTLP endpoint for `signal`.
///
/// If the normalized pathname already ends with the signal path, the URL is
/// returned unchanged. Otherwise, the signal path is appended after removing
/// trailing `/` characters from the pathname. Query and fragment components
/// are retained. Opaque URLs have an immutable pathname under the WHATWG URL
/// standard, so they are returned unchanged when an append would be needed.
pub fn resolve_http_otlp_url(
    base_endpoint: &str,
    signal: OtlpSignal,
) -> Result<String, OtlpUrlError> {
    let mut url = Url::parse(base_endpoint).map_err(|error| OtlpUrlError {
        message: error.to_string(),
    })?;
    let signal_path = signal.path();
    let normalized_path = url.path().trim_end_matches('/');

    if normalized_path.ends_with(signal_path) || url.cannot_be_a_base() {
        return Ok(url.into());
    }

    url.set_path(&format!("{normalized_path}/{signal_path}"));
    Ok(url.into())
}

#[cfg(test)]
mod tests {
    use super::{OtlpSignal, resolve_http_otlp_url};

    #[test]
    fn appends_each_signal_path() {
        for (signal, path) in [
            (OtlpSignal::Traces, "v1/traces"),
            (OtlpSignal::Logs, "v1/logs"),
            (OtlpSignal::Metrics, "v1/metrics"),
        ] {
            assert_eq!(
                resolve_http_otlp_url("https://collector.example", signal).unwrap(),
                format!("https://collector.example/{path}")
            );
        }
    }

    #[test]
    fn preserves_query_and_fragment_when_appending() {
        assert_eq!(
            resolve_http_otlp_url(
                "https://collector.example/base?token=a%2Fb#section",
                OtlpSignal::Traces,
            )
            .unwrap(),
            "https://collector.example/base/v1/traces?token=a%2Fb#section"
        );
    }

    #[test]
    fn leaves_a_full_signal_path_unchanged_including_trailing_slashes() {
        assert_eq!(
            resolve_http_otlp_url(
                "https://collector.example/base/v1/traces///?token=x#anchor",
                OtlpSignal::Traces,
            )
            .unwrap(),
            "https://collector.example/base/v1/traces///?token=x#anchor"
        );
    }

    #[test]
    fn removes_only_trailing_slashes_before_appending() {
        assert_eq!(
            resolve_http_otlp_url("https://collector.example/base///", OtlpSignal::Logs).unwrap(),
            "https://collector.example/base/v1/logs"
        );
        assert_eq!(
            resolve_http_otlp_url("https://collector.example//", OtlpSignal::Logs).unwrap(),
            "https://collector.example/v1/logs"
        );
    }

    #[test]
    fn uses_suffix_match_for_existing_signal_path() {
        // The TypeScript implementation tests endsWith(signalPath), so this
        // intentionally also recognizes a path such as `/notv1/traces`.
        assert_eq!(
            resolve_http_otlp_url("https://collector.example/notv1/traces", OtlpSignal::Traces)
                .unwrap(),
            "https://collector.example/notv1/traces"
        );
    }

    #[test]
    fn uses_whatwg_url_canonicalization_and_path_encoding() {
        assert_eq!(
            resolve_http_otlp_url(
                "HTTPS://EXAMPLE.COM:443/base%20path?query=a b#frag",
                OtlpSignal::Metrics,
            )
            .unwrap(),
            "https://example.com/base%20path/v1/metrics?query=a%20b#frag"
        );
    }

    #[test]
    fn opaque_url_pathname_assignment_is_a_noop() {
        assert_eq!(
            resolve_http_otlp_url(
                "mailto:collector@example.com?subject=otlp#anchor",
                OtlpSignal::Traces
            )
            .unwrap(),
            "mailto:collector@example.com?subject=otlp#anchor"
        );
    }

    #[test]
    fn rejects_non_absolute_or_malformed_urls() {
        assert!(resolve_http_otlp_url("collector.example", OtlpSignal::Traces).is_err());
        assert!(resolve_http_otlp_url("http://", OtlpSignal::Traces).is_err());
    }
}
