# Weixin media port status

Ported `packages/channels/weixin/src/media.ts` to `weixin_media.rs`.

## Coverage

- `parse_aes_key` accepts base64 encoded raw 16-byte keys and base64 encoded 32-character hexadecimal keys. Its decoder preserves Node's permissive base64 behavior for ignored characters, URL-safe symbols, whitespace, and missing padding.
- AES-128-ECB encryption and decryption use PKCS#7 padding, including the full padding block for empty plaintext. The block primitive is checked against the NIST AES-128 ECB known-answer vector.
- `compute_md5` returns lowercase hexadecimal MD5 for the Weixin protocol.
- CDN URL construction matches JavaScript `encodeURIComponent` for reserved ASCII and UTF-8 bytes.
- The injectable HTTP client covers successful download/decryption, the 40-second timeout setting, transport errors, and non-success HTTP status handling.

## Dependency and compatibility notes

- Uses `aes 0.8.4` (MIT OR Apache-2.0). `aes 0.9.3` requires Rust 1.89, so it cannot be used while this workspace declares Rust 1.85. Version 0.8.4 supports Rust 1.56 and preserves the existing workspace MSRV.
- Uses `md-5 0.11.0` (MIT OR Apache-2.0), which declares Rust 1.85.
- ECB and MD5 are unauthenticated/cryptographically weak constructions. They remain here only for wire compatibility with the existing Weixin media protocol.
- The public Rust downloader returns `Result<Vec<u8>, WeixinMediaError>` so transport, HTTP status, key, and padding failures remain observable like the TypeScript promise rejection. The reqwest adapter applies the request timeout while receiving the response body.
- No live CDN request was made. HTTP behavior is covered through the injected mock transport; the reqwest adapter itself is not exercised against an external service.

## Verification

- `rustfmt --edition 2024 crates/canopy-core/src/channels/weixin_media.rs` completed.
- An isolated Cargo harness that includes the module passed all 11 unit tests with `cargo test --manifest-path /tmp/weixin-media-harness/Cargo.toml --locked --offline`.
- `cargo check -p canopy-core` completed after the dependency update.
- `aes 0.8.4`, `md-5 0.11.0`, and their new locked crypto dependencies declare Rust minimums no higher than 1.85. The installed compiler is Rust 1.98; a Rust 1.85 compiler binary is not installed here, so no direct 1.85 compile was run.
