//! Platform-independent telemetry helpers ported from Canopy's TypeScript core.
//!
//! This module currently covers safe hook-name labels, trace/span identifier
//! generation, daemon workspace directory keys, and OTLP HTTP endpoint
//! normalization. It does not yet replace the OpenTelemetry SDK, exporters, or
//! Canopy's telemetry runtime.

pub mod daemon_tracing;
pub mod otlp_urls;
pub mod sanitize;
pub mod trace_id_utils;

pub use daemon_tracing::hash_daemon_workspace;
pub use otlp_urls::{OtlpSignal, OtlpUrlError, resolve_http_otlp_url};
pub use sanitize::sanitize_hook_name;
pub use trace_id_utils::{derive_trace_id, random_hex_string, random_span_id};
