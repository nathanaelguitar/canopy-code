# `internalPromptIds.ts` Rust port status

Source and regressions inspected: `packages/core/src/utils/internalPromptIds.ts`
and `packages/core/src/utils/internalPromptIds.test.ts`.

Implemented in `internal_prompt_ids.rs`:

- Exact recognition of `prompt_suggestion`, `forked_query`, and `speculation`.
- Case-sensitive `side-query:` prefix recognition, including the empty suffix.
- `None`, empty strings, and IDs containing but not starting with the prefix
  remain non-internal.
- Nine focused in-file tests matching the nine TypeScript test cases.

The module is exported from `utils/mod.rs`. No dependency was needed.

Verified with:

- `cargo test -p canopy-core --test internal_prompt_ids_port_harness --locked --offline` — 9 passed before export.
- The native exported-module run is pending.
