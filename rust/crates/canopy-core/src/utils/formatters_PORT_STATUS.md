# Formatter port status

`formatters.rs` ports `packages/core/src/utils/formatters.ts` and its
`formatMemoryUsage` cases. It preserves the 1024-based KB/MB/GB constants,
chooses units after one-decimal JavaScript-style rounding, and formats KB/MB
to one decimal and GB to two decimals.

The private `js_to_fixed` helper implements exact binary64 scaling and
JavaScript's tie-toward-larger-magnitude rule for the one/two decimal places
used here. For `toFixed`'s `|x| >= 1e21` fallback it normalizes Rust's
scientific notation to include JavaScript's positive exponent sign. The module
is exported from `utils/mod.rs`. `rustfmt --edition 2024 --check` passed for the
module and its export; Cargo tests and builds were not run.
