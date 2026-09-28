# MCP OAuth token storage Rust port status

The Rust token storage ports the plaintext file backend, encrypted file backend,
and macOS Keychain backend used by `packages/core/src/mcp/token-storage`.
`CANOPY_CODE_FORCE_ENCRYPTED_FILE_STORAGE=true` selects protected storage; on
macOS it prefers Keychain and falls back to the encrypted file.

## Implemented

- The token file uses `mcp-oauth-tokens-v2.json` with an insertion-ordered JSON
  object keyed by server name. The secret file uses
  `extension-secrets-v1.json` and preserves service-scoped secret maps.
- AES-256-GCM encryption matches the TypeScript envelope (`16-byte IV`,
  `iv:tag:ciphertext` lowercase-hex fields). The key uses the same scrypt
  password, host/user salt string, and default Node scrypt parameters, so the
  Rust encrypted file can read and write files created by TypeScript.
- Credentials, expiration filtering, delete/clear/list behavior, and secret
  CRUD follow their TypeScript counterparts. Encrypted writes use private
  atomic replacement; reads reject symlinks on Unix and are capped at 8 MiB.
- `KeychainTokenStorage` uses the same generic-password service/account
  contract as Keytar, including sanitized server account names, secret prefixes,
  credential enumeration, expiry filtering, availability probe, and clear-all
  behavior. It uses the Security Framework bindings and runs operations on
  Tokio's blocking pool.
- `ConfiguredTokenStorage` preserves the source plaintext default. Under
  `CANOPY_CODE_FORCE_ENCRYPTED_FILE_STORAGE=true`, it selects the macOS
  Keychain if the set/get/delete probe succeeds, otherwise the encrypted file.
  `CANOPY_CODE_FORCE_FILE_STORAGE=true` skips the Keychain and uses the
  encrypted file. Both interactive CLI OAuth and ACP OAuth use this selector.

## Remaining gaps

- The native Keychain backend currently targets macOS. Windows and Linux use
  the encrypted-file fallback; their native credential stores are not ported.
- Keychain access was compile-checked on macOS but not exercised against a live
  user keychain in this session. Cross-runtime credential interchange still
  needs runtime verification.
- `ConfiguredTokenStorage` implements `SecretStorage` by delegating to the
  selected protected backend. Extension settings are not wired to it yet.
- Like the TypeScript file backend, read/modify/write operations do not
  coordinate across multiple processes. Rust additionally caps encrypted file
  reads and writes at 8 MiB.

## Verification

- `rustfmt --edition 2024` completed for the storage modules and CLI call sites.
- `cargo check --manifest-path rust/Cargo.toml -p canopy-cli --locked --offline`
  passed after the storage integration. Three existing `mcp_host.rs` warnings
  remain. No tests were added or run.
