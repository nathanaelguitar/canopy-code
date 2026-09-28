# Hook configuration loader port status

`config_loader.rs` ports configuration ingestion from
`packages/core/src/hooks/hookRegistry.ts`. It reads user hooks, project hooks,
then active extensions in that order; preserves JSON event/definition order;
skips reserved keys and malformed entries; validates command, HTTP, function,
and prompt configs; and delegates duplicate identity and source stamping to
`HookRegistry::initialize`.

## Integration

Add `pub mod config_loader;` to `rust/crates/canopy-core/src/hooks/mod.rs`.
Call `load_hook_entries(&HookConfigSources, function_resolver)` and initialize
or reload the registry with the returned `entries`. The result also contains
recoverable `issues` for host logging or feedback.

For JSON function hooks, provide a `JsonFunctionHookResolver` that can produce
a typed config with a callback, and pass the same resolver to the native
dispatch adapter. The loader checks callback availability during ingestion;
the adapter resolves the JSON config again when that hook executes, so the
resolver should be deterministic and safe to call repeatedly.

## Host responsibilities and parity notes

- The host reads settings and extension manifests, selects sources, decides
  project-folder trust, and supplies project hooks only when that policy
  permits them. The loader does not read files or enforce trust.
- The host still owns feedback presentation and logging; malformed inputs are
  returned as structured issues and skipped.
- The loader accepts JSON-compatible function configs only when the injected
  callback resolver finds a runtime callback. Arbitrary JavaScript callbacks
  cannot be represented in JSON.
- Correctly typed matcher and sequential fields are preserved. Values of the
  wrong JSON type are treated as absent because the Rust registry entry types
  cannot retain them; TypeScript's static config types generally exclude them.
- Non-function configs receive their source field through the registry's
  existing initialization path, including prompt configs, matching the
  TypeScript implementation. Function config JSON is not source-stamped.
- serde_json's `preserve_order` feature retains event-key order. Duplicate
  entries keep the first occurrence under the Rust registry identity rule.

The file was formatted. No tests or compilation were run.
