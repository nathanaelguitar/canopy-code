# Weixin image send port status

Ported `sendImage` from `packages/channels/weixin/src/send.ts` to
`weixin_send_image.rs`.

## Implemented

- The four stages preserve source ordering: validate and read the local image, request upload credentials, encrypt and upload to the CDN, then send a bot image message.
- Image path checks reuse `weixin_send_utils::validate_image_path`. Raw size and lowercase MD5 are calculated from the file bytes. AES-128-ECB/PKCS#7 output size, hexadecimal `filekey` and AES key, and base64-of-hex `media.aes_key` match the TypeScript wire fields.
- API calls reuse `weixin_api` production and mockable methods, preserving its auth headers, retry behavior, CDN host/HTTPS checks, and API error propagation.
- The production random source uses the existing UUID-backed OS CSPRNG path, taking 16 unconstrained bytes from independent UUID v4 values for each key. Tests inject fixed key bytes.
- Focused tests inspect the three outgoing request bodies, uploaded ciphertext, message fields, auth header, metadata, and failures at upload URL, CDN upload, message send, and path validation.

## Remaining integration

`channels/mod.rs` needs to export `weixin_send_image`. The shared API module, manifests, and ledger were not changed for this slice.

## Verification

- `rustfmt --edition 2024 rust/crates/canopy-core/src/channels/weixin_send_image.rs` completed.
- An isolated offline Cargo harness including the actual image sender and its API, crypto, path, and wire-type dependencies passed **42/42** tests. This includes all 5 new image sender tests; the other tests cover the reused modules.
- The harness verifies exact upload URL JSON, encrypted body and uploaded size, CDN request headers, final image-message JSON, and early termination/error propagation at each API step. No external network access was used.
