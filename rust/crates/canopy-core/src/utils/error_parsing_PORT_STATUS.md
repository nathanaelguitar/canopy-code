# `errorParsing.ts` Rust port status

Source and regressions inspected: `packages/core/src/utils/errorParsing.ts`
and `packages/core/src/utils/errorParsing.test.ts`.

No Rust utility previously produced these user-facing API error strings.
`providers/retry_error_classification.rs` parses provider payloads for retry
decisions, but it does not format user-facing messages and is not an overlap.

Implemented in `error_parsing.rs`:

- Provider API JSON parsing after an optional text prefix, optional status
  text, one level of nested API-error unwrapping, and numeric-code-only 429
  handling.
- Structured errors, native error messages with cause details, AggregateError
  cause aggregation, and the source's 1000 UTF-16-unit cap for native Error
  messages.
- Distinct Gemini and Vertex quota guidance, the shared default guidance for
  other auth types, and the Canopy OAuth/friendly quota passthroughs.
- Idempotency detection for existing `[API Error: ...]`, known 429 suffixes,
  and `Quota exhausted: ...` messages.
- Fourteen focused in-file regression tests based on the TypeScript tests.

The caller must map native error instances into `ErrorDetails` and cause
variants; a cyclic JavaScript cause graph cannot be represented by these owned
Rust values. UTF-16-aware message truncation does not split a Rust Unicode
scalar to reproduce JavaScript's possible lone-surrogate slice boundary.

## Integration and verification

The module is exported from `rust/crates/canopy-core/src/utils/mod.rs`. No
manifest or dependency change was required; it uses the existing
`serde_json` dependency.

Verified with:

- `cargo test -p canopy-core --test error_parsing_port_harness --locked --offline` — 14 passed before export.
- Native utility-module verification is pending.
