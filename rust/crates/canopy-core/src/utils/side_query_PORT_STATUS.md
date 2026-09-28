# `sideQuery.ts` Rust port status

Source inspected: `packages/core/src/utils/sideQuery.ts` and
`packages/core/src/utils/sideQuery.test.ts`.

`side_query.rs` implements the shared request policy behind an injected
executor. The host supplies model preferences and the `output-language.md`
path; it also owns provider creation and authentication. The module resolves
model fallback, prompt IDs, generation defaults, output-language instruction
loading/appending, retry-cap forwarding, text/JSON response handling, and
caller-supplied validation hooks. Cancellation tokens and optional absolute
deadlines cover both preference-file reads and executor calls.

Parity boundary: the source calls the shared `SchemaValidator` directly. Rust
injects a `SideQueryJsonValidator` so the host can reuse its JSON Schema engine;
the provider-neutral core does not bundle Ajv-equivalent schema compilation.
Custom text and JSON validation callbacks run after that JSON-schema hook, in
the same order as the source.

## Verification

- `rustfmt --edition 2024 rust/crates/canopy-core/src/utils/side_query.rs` — passed.
- `git diff --check` for the new module — passed.
- Cargo was held while shared source changes are active.
