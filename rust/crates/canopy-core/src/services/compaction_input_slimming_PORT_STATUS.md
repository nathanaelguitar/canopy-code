# `compactionInputSlimming.ts` Rust parity status

Source and regressions inspected: `packages/core/src/services/compactionInputSlimming.ts`
and `packages/core/src/services/compactionInputSlimming.test.ts`.

Confirmed behavior in `compaction_input_slimming.rs`:

- Defaults and env > settings > default precedence for image estimates and
  compaction tuning, including finite/range checks and safe-integer counts.
- Text and raw output sizes use UTF-16 units; inline media uses the configured
  token estimate times four, and tool responses include the 64-character
  wrapper floor plus nested `functionResponse.parts` estimates.
- Unsupported `inlineData`/`fileData` is replaced with sanitized image or
  document text; supported MIME modalities and the default MIME type match the
  source. The shared nested-parts accessor is also used by Rust image
  collection/restoration services.
- Input values remain unmodified. Slimming now lazily builds an owned history
  only after finding a replacement, avoiding transient deep clones on the
  common no-change path; unchanged parts are copied only when needed in a
  changed owned result.

Fixed two config-parsing parity gaps: whitespace-only count env values now
fall through to settings/default instead of parsing as zero, and env trimming
now follows ECMAScript whitespace, including BOM. Added focused regressions for
those cases and for unchanged/supported parts producing no replacements.

Rust `String` cannot represent an unmatched UTF-16 surrogate. Therefore
`sanitizeMimeForPlaceholder(...).slice(0, 128)` can produce a JS string that
Rust cannot represent if the cut lands inside a supplementary-plane character;
Rust retains only complete Unicode scalars in that case. When actual media is
replaced, the owned Rust `Value` result also deep-clones preserved values,
where TypeScript object spreads retain references to immutable values.

No shared module exports or manifests changed. Formatter command passed:
`rustfmt --edition 2024 crates/canopy-core/src/services/compaction_input_slimming.rs`.
Cargo verification is pending the parent's consolidated run.
