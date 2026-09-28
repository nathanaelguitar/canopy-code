# `terminalSafe.ts` Rust port status

Source: `packages/core/src/utils/terminalSafe.ts` and
`packages/core/src/utils/terminalSafe.test.ts`.

`terminal_safe.rs` ports OSC/CSI/SS2/SS3/DCS and residual C0/C1 terminal
control filtering, the C0/C1 and bidi-control display sanitizer, and
single-line notification-label normalization with an 80-code-point cap. It
exports equivalent compiled regexes for call sites that need the shared
patterns. Rust strings cannot contain lone UTF-16 surrogates, but code-point
counting matches the source's spread-based truncation boundary.

Eleven retained unit tests cover the source sanitizer and label cases plus
the public bidi predicate. On Darwin arm64, all 11 passed as part of
`cargo test -p canopy-core utils:: --locked --offline`. The shared helpers are
used by background monitor and shell labels, resource reference display, ACP
display text, and Qoder transcript conversion. Native TUI tool-result rendering
and session-title formatting still use local sanitizers.
