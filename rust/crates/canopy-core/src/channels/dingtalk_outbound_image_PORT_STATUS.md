# DingTalk outbound image Rust port status

Source: `packages/channels/dingtalk/src/outbound-image.ts` and
`packages/channels/dingtalk/src/outbound-image.test.ts`.

## Implemented

- `find_image_markers`, `replace_image_markers`,
  `strip_partial_image_marker`, and `sanitize_streaming_image_markers` mask
  inline and fenced code, retain UTF-16 source offsets, replace from the end,
  and preserve a bare trailing bracket while hiding incomplete `[I...` paths.
- `read_validated_image` requires an absolute allowed extension, canonicalizes
  the workspace/temp roots and target, rejects paths outside either root,
  opens once and checks regular-file status and the 20 MiB limit before reading,
  then validates the magic bytes against the requested extension.
- `upload_dingtalk_image` and `upload_dingtalk_image_with_transport` post a
  single `media` file part to DingTalk with the encoded access-token query,
  image type, and 30-second timeout. The injectable transport separates
  request errors from response-body/JSON errors for deterministic tests.
- API error details redact the access token, flatten control whitespace, and
  cap the message to 200 UTF-16 units. HTTP 401 and DingTalk auth codes 40014
  and 42001 are marked as authentication failures.

The workspace reqwest dependency does not enable `multipart`. The production
adapter builds the standard single-file multipart body directly, so no manifest
or lockfile changes are needed. Rust strings cannot represent lone UTF-16
surrogates; marker offsets count UTF-16 units and any externally supplied split
inside a supplementary character rounds back to the scalar boundary.

## Verification and remaining integration

The module contains 21 offline tests covering marker masking/replacement,
partial suffixes, UTF-16 offsets, canonical path containment, all four image
signatures and extension checks, file kind/size validation, multipart request
shape, upload response aliases/errors, token redaction, timeout, and
authentication classification. All 21 pass in an isolated offline Cargo
harness using workspace dependencies (`reqwest`, `serde_json`, and `uuid`).

The channel module registry must add
`pub mod dingtalk_outbound_image;` to
`rust/crates/canopy-core/src/channels/mod.rs`. DingTalk adapter call sites are
not wired yet.
