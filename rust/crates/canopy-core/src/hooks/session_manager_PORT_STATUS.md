# Session Hooks Manager Port Status

`session_manager.rs` ports per-session/event registration order, generated and
caller-supplied function-hook IDs, add/remove/clear/list/count operations, and
tool-alias plus anchored-pattern matching. Function hooks use the Rust
`FunctionHookConfig` and `FunctionHookCallback`; command and HTTP hook configs
remain `serde_json::Value` payloads.

The module is exported from `hooks/mod.rs` and passes
`cargo check -p canopy-core --lib --locked`. A host adapter is still needed
for callback/config registration. Returned Rust vectors are snapshots;
the TypeScript manager returns its internal array from `getHooksForEvent`.
Regexes use Rust `regex` syntax, so JavaScript-only constructs and some
newline/anchor corner cases can differ; invalid Rust regexes fall back to exact
string equality as in the source. Generated IDs keep the source timestamp plus
seven lowercase base-36 characters, using UUID-derived randomness instead of
`Math.random()`. Debug logging is not wired in this module.

No tests were added or run.
