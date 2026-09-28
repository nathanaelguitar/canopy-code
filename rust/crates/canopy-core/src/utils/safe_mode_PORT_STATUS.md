# `safe-mode` Rust port status

`safe_mode.rs` ports `isSafeModeEnv` and the `CANOPY_CODE_SAFE_MODE` key. It
reuses `bare_mode::is_truthy`, so the accepted tokens, case folding, and
ECMAScript whitespace behavior stay identical to bare-mode parsing. The
environment lookup is injectable through `is_safe_mode_env_with`; a process
environment wrapper is also provided.

Three focused tests cover the exact environment key, accepted token spellings,
missing and rejected values, case handling, and ECMAScript BOM/NEL trim edges.
The module and its `bare_mode` dependency are exported from `utils/mod.rs`;
native CLI caller wiring remains open.

Non-Unicode operating-system environment values map to `None` through Rust's
`std::env::var`; Node exposes environment values as strings.
