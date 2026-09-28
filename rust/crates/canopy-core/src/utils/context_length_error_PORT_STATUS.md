# `contextLengthError.ts` Rust port status

Source: `packages/core/src/utils/contextLengthError.ts` and
`packages/core/src/utils/contextLengthError.test.ts`.

`context_length_error.rs` ports recursive error-text collection (four nested
levels), embedded JSON extraction, fragment trimming and de-duplication,
context overflow classification, fragment-local timeout vetoes, and the four
token-count extraction patterns in their source precedence order. ECMAScript
whitespace trimming, ASCII word boundaries/digits, JavaScript enumerable-key
ordering, optional count fields, and throwing accessor fallback behavior are
covered by retained Rust unit tests.

The Rust boundary uses `ContextLengthErrorValue`. Convert JSON provider errors
with `ContextLengthErrorValue::from_json`; construct `ContextLengthErrorValue::Error`
when Error `name`/`message`/`cause` semantics are needed. JSON cannot represent
accessors or object identity/cycles, so those cases require explicit Rust
values. The owned Rust tree cannot model a true cyclic JavaScript object; the
source's four-level depth limit bounds collection in either implementation.

No manifest changes were needed (`regex` and `serde_json` are existing
dependencies). The module is exported from `utils/mod.rs`. On Darwin arm64,
`cargo test -p canopy-core utils::context_length_error --locked --offline`
passed all 13 retained unit tests.
