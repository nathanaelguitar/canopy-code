# Weixin outbound send utility Rust port status

Source: the non-cryptographic helpers in
`packages/channels/weixin/src/send.ts` and focused cases in
`packages/channels/weixin/src/send.test.ts`.

## Implemented

- `markdown_to_plain_text` applies the same replacement sequence for fenced
  and inline code, emphasis, headings, links/images, quotes, rules, lists,
  repeated blank lines, and ECMAScript whitespace trimming.
- `detect_image_mime` recognizes PNG, JPEG, GIF, and WebP signatures; RIFF
  files without a `WEBP` marker are rejected.
- `validate_image_path` resolves and canonicalizes paths, checks the lowercased
  filename extension, regular-file status, the 20 MiB limit, and extension to
  magic-byte agreement. It allows `/tmp`, the canonical temporary directory,
  and canonicalized caller workspace roots. Containment uses path components,
  so a sibling such as `workspace-extra` cannot pass for `workspace`; symlinks
  are checked at their canonical target.
- `send_text` and the injected-transport `send_text_with_http` build the source
  message shape with a UUID client ID, bot/finished constants, context token,
  and markdown-normalized text. The API module adds `base_info` and performs
  the existing retry behavior.

Five focused module tests cover transformation cases, all magic formats and a
RIFF decoy, path extension/file/size/signature checks (including the exact size
boundary), temp and workspace allowlists plus sibling/symlink escapes, and the
serialized send request through the injected API transport.

## Verification and remaining adapter gaps

An isolated offline Cargo harness importing the actual Weixin API, wire-type,
and send utility modules passed **26/26** tests (including all 5 send-utility
tests). `rustfmt --check` and a trailing-whitespace scan passed. The harness is
under `/tmp`; workspace manifests and the lockfile were not changed.

The module still needs an export from `channels/mod.rs`; the parent port task
owns that shared file. The Rust channel adapter must call `send_text` and pass
its HTTP client and text parameters. Encrypted `sendImage` is intentionally
unported: AES-128-ECB, digest/key handling, CDN upload, and the image-message
send step remain dependent on the separate AES media port. `validate_image_path`
is available for that later image flow.
