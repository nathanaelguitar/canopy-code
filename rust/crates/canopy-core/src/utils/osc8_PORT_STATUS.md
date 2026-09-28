# `osc8.ts` Rust port status

Source: `packages/core/src/utils/osc8.ts` and `packages/core/src/utils/osc8.test.ts`.

`osc8.rs` ports the OSC 8 envelope, C0/DEL/C1/bidi/line-separator sanitizer,
tmux ESC-doubling and screen DCS wrappers, the exported environment-key list,
and the support detector. Environment lookups happen during every call.
Detection keeps the source precedence: opt-outs, TTY guard, explicit force,
CI/TeamCity and multiplexer refusals, terminal environment and version
heuristics, then the VTE gate that refuses both dotted and packed VTE 0.50.0.
The Rust detector also preserves the source's empty-versus-missing environment
distinctions and VTE / Konsole packed-version parsing.

Rust cannot accept Node's abstract `WriteStream`. `supports_hyperlinks()`
checks process stdout with `IsTerminal`; callers for another stream use
`supports_hyperlinks_for_stream(Option<bool>)`, where a missing or false TTY
state refuses OSC 8. Environment values that are not valid Unicode are treated
as absent. Rust strings cannot represent lone UTF-16 surrogates. Version and
force parsing use IEEE-754 `f64`, matching JavaScript's numeric precision for
ordinary values; extreme decimal inputs can still differ in parsing details.

This sanitizer intentionally remains separate from `terminal_safe.rs`:
OSC payloads remove TAB, DEL, U+200E/U+200F, U+2028/U+2029, and bidi controls;
the display sanitizer has a different policy (for example, it preserves TAB,
DEL, and line/paragraph separators). No manifest changes are needed.

The module is exported from `utils/mod.rs`. All 15 retained Rust unit tests
passed on macOS in the native `cargo test -p canopy-core utils:: --locked
--offline` run, which passed 182 utility tests in total. The temporary
integration harness was removed after its initial focused run.
