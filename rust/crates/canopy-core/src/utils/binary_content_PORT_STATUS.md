# `binary-content.ts` Rust port status

Source: `packages/core/src/utils/binary-content.ts` and
`packages/core/src/utils/binary-content.test.ts`.

Implemented in `binary_content.rs`:

- Binary content-type classification, including the explicit application MIME
  list, OpenXML/OpenDocument prefixes, and text-like `+json`/`+xml` exceptions.
- MIME-to-extension mapping and the recognized extension whitelist.
- Content-Disposition filename parsing, RFC 5987 `filename*` precedence and
  URI decoding, plus URL-path extension extraction.
- Magic-byte sniffing for PDF, ZIP/OpenXML/JAR, gzip, RAR, PNG, JPEG, GIF, and
  WebP, with source-compatible priority and MIME results.
- The 8192-byte UTF-8/NUL text heuristic, binary persistence with Unix 0700
  directories and 0600 newly created files, and byte-size formatting. Tokio
  writes are flushed before success is returned, matching `writeFile`'s
  completion behavior.
- Nine retained in-file unit tests based on the TypeScript utility tests.

No dependency or manifest change is needed: the workspace already includes
`regex`, `reqwest` (for URL parsing), and Tokio filesystem/I/O support.

## Integration and verification

The module is exported from `rust/crates/canopy-core/src/utils/mod.rs`; its
unit tests now run as part of the canopy-core test target.

Verified with:

- `cargo test -p canopy-core utils:: --locked --offline` — 130 matching tests
  passed, including all 9 binary-content tests.
- `rustfmt --edition 2024 --check rust/crates/canopy-core/src/utils/binary_content.rs`.

Rust callers receive `PathBuf` for a persisted filepath and `usize` for byte
sizes, reflecting native Rust path and byte-length types. On non-Unix targets,
the permission modes are not applied, matching the source test's platform
skip for file mode checks.

## Remaining wiring

No source behavior in the named TypeScript utility remains unported. The
module is integrated into `tools/web/fetch_processing.rs`, which uses its MIME,
filename, and magic-byte classification before persisting binary fetch
responses. PDF extraction continues from the persisted file.
