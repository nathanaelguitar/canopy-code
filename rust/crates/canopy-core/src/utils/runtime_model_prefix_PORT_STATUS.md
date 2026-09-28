# `runtimeModelPrefix.ts` Rust port status

Source and regression cases inspected: `packages/core/src/utils/runtimeModelPrefix.ts`
and the `stripRuntimeSnapshotPrefix` cases in `packages/core/src/utils/modelId.test.ts`.

Implemented in `runtime_model_prefix.rs`:

- The `$runtime|` constant and dependency-free prefix stripping helper.
- Repeated prefix removal, bare IDs, delimiter-containing IDs, malformed
  prefixes with no model portion, and malformed nested prefixes.
- Seven focused in-file tests, including all four source regressions.

`resolveModelId` and the rest of `modelId.ts` are outside this utility slice.
The module is exported from `utils/mod.rs`.

Verified with:

- `cargo test -p canopy-core --test runtime_model_prefix_port_harness --locked --offline` — 7 passed before export.
- The native exported-module run is pending.
