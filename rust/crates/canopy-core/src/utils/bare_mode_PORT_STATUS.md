# `bareMode` Rust port status

`bare_mode.rs` ports the `QWEN_CODE_SIMPLE` name, four truthy tokens
(`1`, `true`, `yes`, `on`), case-insensitive matching, edge trimming, and the
CLI flag's `=== true` behavior. The core mode check accepts an injected
environment lookup closure; `is_bare_mode` provides the process-environment
wrapper.

Four focused tests cover accepted/rejected tokens, ECMAScript whitespace
including BOM and non-trimmed NEL/zero-width characters, CLI short-circuiting,
and injected lookup behavior. The module is exported from `utils/mod.rs`;
native CLI caller wiring remains open.

Rust environment access returns `None` for non-Unicode OS environment values;
Node exposes environment values as strings. This difference is not relevant to
the ASCII mode tokens under normal shell environments.
