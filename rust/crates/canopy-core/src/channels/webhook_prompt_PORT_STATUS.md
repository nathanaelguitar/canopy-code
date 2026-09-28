# Channel webhook prompt port status

Source: `packages/channels/base/src/ChannelWebhookTask.ts`.

`webhook_prompt.rs` ports target resolution, unattended prompt construction,
bounded title/summary/payload handling, the explicit untrusted-event policy
text, and the user-visible display projection. It uses the shared Rust
`sanitize_quoted_text`, `sanitize_prompt_text`, and `truncate_code_points`
helpers. Target lookup preserves the separate unknown-source and
unknown-target errors and their TypeScript message text. Optional `threadId`,
`isGroup`, and summary values remain optional; an explicit `false` group flag
is preserved.

The title, summary, serialized payload, and complete prompt use the same 500,
1,000, 6,000, and 8,500 code-point caps as TypeScript. Event type, source, and
target chat are quoted-sanitized to 128 code points. The display projection
sanitizes and caps title/summary identically, joins non-empty values with two
newlines, and omits absent or empty values.

## Parity differences and limits

- Payloads are `serde_json::Value`, so Rust cannot represent JavaScript-only
  values such as `undefined`, functions, symbols, `BigInt`, cyclic objects, or
  lone UTF-16 surrogates. Serialization therefore cannot reproduce every
  behavior or exception from `JSON.stringify` on arbitrary runtime objects.
- The workspace enables `serde_json`'s `preserve_order` feature. JSON parsed
  into a `Value` retains object insertion order, and the intermediate pretty
  output uses the same two-space indentation as
  `JSON.stringify(payload, null, 2)`. `sanitizePromptText` then folds those
  JSON line breaks to spaces before the payload is embedded, as in TypeScript.
  Values constructed from unordered Rust maps do not promise JavaScript
  property insertion order. JavaScript also enumerates integer-index property
  names before ordinary string keys, which serde_json does not reorder
  specially.
- JSON string escaping is serializer-defined; byte-for-byte escaping can vary
  for characters such as `/`, non-ASCII separators, or HTML-sensitive text.
  The prompt remains sanitized after serialization and is bounded either way.
- Rust strings contain valid Unicode scalar values only, so lone-surrogate
  handling is not applicable. Code-point caps match the source behavior for
  valid strings.
- The Rust API accepts only the task data/config shape and does not resolve
  `secretEnv` or run a webhook. Those responsibilities remain in the adapter.

Focused tests cover target-resolution error distinctions, optional target
fields, prompt policy text, sanitization and caps, display projection, and
insertion-ordered pretty JSON.
