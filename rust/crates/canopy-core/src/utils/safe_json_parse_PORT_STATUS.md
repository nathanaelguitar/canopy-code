# `safeJsonParse` Rust port status

`safe_json_parse.rs` ports `packages/core/src/utils/safeJsonParse.ts` using
`serde_json::Value` and the workspace's `jsonrepair-rs` dependency. It first
uses strict JSON parsing, attempts repair only on failure, and returns a caller
fallback if the input is absent/empty or both parse paths fail. The default
entrypoint uses an empty object fallback.

Focused Rust tests cover valid objects, arrays, nested structures, primitive
JSON values, single-quoted strings, unquoted keys, trailing commas, line
comments, nullish/empty input, custom fallbacks, irreparable input, whitespace,
and the source's behavior of treating completely unquoted text as a repaired
JSON string.

The Rust API models fallback and output values as `serde_json::Value`; it cannot
preserve a JavaScript fallback object's reference identity or TypeScript's
unchecked generic cast. Rust's typed `Option<&str>` models null/undefined but
cannot receive non-string runtime values. The `jsonrepair-rs` implementation
may differ from the npm `jsonrepair` package on edge cases. `serde_json::Value`
also cannot represent JavaScript `NaN`/infinity results or guarantee identical
numeric precision for out-of-range values. The source debug error logging is
not reproduced here.

The module is exported from `utils/mod.rs`. No source logging is reproduced.

## Verification

On macOS ARM64, `cargo test -p canopy-core utils:: --locked --offline` passed
all six safe-parse tests within 182 utility tests. The temporary integration
harness was removed after export.
