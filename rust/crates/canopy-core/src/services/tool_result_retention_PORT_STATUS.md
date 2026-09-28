# `tool-result-retention.ts` Rust parity status

Sources and tests inspected: `packages/core/src/utils/tool-result-retention.ts`,
`packages/core/src/utils/tool-result-retention.test.ts`, and the shared
`compactionInputSlimming.ts` part estimator.

The Rust analyzer in `tool_result_retention.rs` matched the reviewed TypeScript
behavior for response counting, shared part-size estimates, raw UTF-16 output
length, per-tool/fallback budgets, strict threshold comparison, output/error
nullish precedence, and both truncation sentinels. No production logic change
was required.

Added focused regressions for newline-dense raw sizing and `output ?? error`
precedence across null, empty-string, and numeric outputs. Existing in-file
tests also cover UTF-16 lengths, media estimates, custom budgets, sentinels,
multiple function responses, and non-finite thresholds.

The TypeScript circular-payload case has no direct equivalent with
`serde_json::Value`, which cannot represent cyclic values. No shared module
exports or manifests were changed.

Cargo verification is pending the parent agent's consolidated run, as
requested. `rustfmt --edition 2024 crates/canopy-core/src/services/tool_result_retention.rs`
is the formatter command for this slice.
