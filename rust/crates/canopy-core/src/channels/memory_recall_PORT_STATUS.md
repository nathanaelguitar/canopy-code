# Channel memory recall port status

Ported to `memory_recall.rs`:

- Immutable prepared recall indexes and one-shot selection.
- Latin and decimal runs require at least two Unicode code points; Han,
  Hiragana, Katakana, and Hangul use adjacent bigrams.
- Unique-term overlap scores, stable input-order ties, short no-match fallback,
  three-entry cap, and 1,200-code-point text budget.
- An oversized first relevant entry is truncated with the source suffix while
  later entries that do not fit are skipped.
- The source unsafe-invisible set is applied with separator replacement before
  tokenization.
- Entry creation/update metadata is cloned and retained.

The module is exported by `channels/mod.rs`. Native Telegram, QQ, and Weixin
hosts use it to add bounded, untrusted channel-memory context to ordinary
prompts in non-single session scopes. Rust CLI and ACP managed-memory recall
uses the separate `canopy_core::memory` selector.

## Normalization dependency

`normalize_for_recall` uses `UnicodeNormalization::nfkc()` from
`unicode-normalization = 0.1.25`, then Rust's Unicode lowercase conversion, then
the source invisible-character replacement. The dependency should be wired as
`unicode-normalization = "0.1.25"` in `[workspace.dependencies]` and
`unicode-normalization.workspace = true` in `canopy-core`'s `[dependencies]`.
Version 0.1.25 declares Rust 1.36 MSRV and `MIT OR Apache-2.0` licensing, so it
fits the workspace's Rust 1.85 floor and permissive-license preference. Its
NFKC data is Unicode 17.0.

The Rust lowercase tables and regex script-property tables follow the Rust
toolchain/crate Unicode data versions, which may differ from the JavaScript
runtime's Unicode version for newer assignments or casing rules. The standard
fixture cases (fullwidth Latin, compatibility ligatures, separators, and the
four requested script families) are covered, but version-specific edge cases
can still differ.

Rust `str` also cannot represent lone UTF-16 surrogates. For valid Unicode text,
Rust scalar-value counts match the source's JavaScript code-point counts. Regex
script-property tables can differ from the JavaScript runtime's Unicode version
for recently assigned characters.

## Verification

- `rustfmt --edition 2024` completed for `memory_recall.rs`.
- A temporary external Cargo harness compiled the module with `regex` and
  `unicode-normalization` and ran all 14 focused unit tests successfully.
- A temporary external Cargo harness compiled the module and ran all 14
  focused tests successfully. The full Rust workspace compiles with the
  module exported and integrated into the three native channel hosts.
