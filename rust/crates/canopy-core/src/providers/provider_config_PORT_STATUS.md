# Provider config install-plan port status

`provider_config.rs` ports `packages/core/src/providers/provider-config.ts` for
arbitrary provider configurations. It reuses the preset module's model,
generation-config, advanced-config, and setup-input types, and the install
module's patch, merge, legacy-credential, and selection types.

The builder handles static and callback-derived environment keys and model
prefixes; fixed, editable, and custom model lists; prebuilt model overrides;
advanced generation settings; ordered custom-header merging; custom ownership
callbacks and statically derived ownership; identity-based ownership; the
default prepend-and-remove-owned merge strategy; dotted metadata-key rejection;
provider version state; initial model selection; and the wider plan's optional
legacy-credential and display fields. The source builder itself leaves legacy
credentials and plan display absent, so the Rust result does too. Provider
labels, descriptions, URL choices, UI labels, documentation URLs, and API-key
validation callbacks remain available on `ProviderConfig` for host setup UI.

## Adapter seams and parity limits

- Environment-key, model-prefix, ownership, API-key-validation, and dynamic
  documentation callbacks are represented by `Arc`-backed `Send + Sync`
  closures. `ProviderConfigInstallPlan::into_install_plan` drops its UI-only
  display field and adapts the rest to the existing runtime installer plan.
- Environment variables use the install crate's ordered `Vec<(String,
String)>`; provider-state maps use `IndexMap` to preserve metadata write
  order.
- Plan version hashes are built from an ordered JSON projection so spec and
  advanced generation fields follow TypeScript's object insertion order.
  Prebuilt model property order is unavailable through the shared Rust model
  struct, so its serde field order is used. Modality keys likewise follow the
  shared Rust `ConfiguredModalities` order.
- `ModelSpec.context_window_size` uses `serde_json::Number` to retain numeric
  values. Advanced input values still arrive as `f64`, matching the reused
  `AdvancedProviderConfig` type.
- The Rust builder returns `ProviderConfigError` instead of throwing a JS
  `Error`; messages match the source for empty models and dotted metadata IDs.
- Provider loading, setup UI, settings persistence, API-key validation
  invocation, and runtime install/apply wiring remain host responsibilities.

`provider_config` is exported from `providers/mod.rs` and passes
`cargo check -p canopy-core --locked`. Provider setup UI, host-level provider
loading, settings persistence, and runtime install/apply wiring remain
unported.
