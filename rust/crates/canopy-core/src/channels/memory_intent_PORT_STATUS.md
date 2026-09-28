# Channel memory intent Rust port

Implemented in `memory_intent.rs`, exported by `channels/mod.rs`, and used by
the native Weixin host before ACP session routing. The deterministic parser
handles the commands locally; unambiguous natural-language intents are
classified through the configured temporary ACP path.

The parser mirrors the TypeScript match order and includes Chinese and English
remember/list/inspect/remove/update/clear/confirmation forms, ASCII-only
positive safe-integer page parsing, lowercase memory ID validation, leading
slash-command exclusion, unsafe invisible removal, and ECMAScript Unicode
trimming. It locally implements the narrow invisible-removal operation because
the existing Rust sanitizer's equivalent helper is private and its public
sanitizers replace those characters with spaces instead of removing them.

Focused unit tests are included in the module; all six passed through a direct
`rustc --test` harness using the workspace's cached `regex` artifact.
`rustfmt --edition 2024 --check` passed, and the focused harness ran all six
module tests successfully. The full Rust workspace also compiles with the
module exported. Remaining likely parity edge cases are differences between
Rust regex Unicode case folding and JavaScript `/iu`, which this module covers
for the ASCII case variants used by the supported English commands.
