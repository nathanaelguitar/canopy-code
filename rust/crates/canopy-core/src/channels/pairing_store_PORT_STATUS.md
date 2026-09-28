# PairingStore Rust port

`pairing_store.rs` ports the file-backed behavior from
`packages/channels/base/src/PairingStore.ts` as `FilePairingStore`, which
implements the channel `PairingStore` trait.

Implemented behavior:

- Reuses live requests by typed subject ID; limits each sender to one request
  and applies the shared three-request cap.
- Expires requests after one hour, creates eight-character codes from the
  source's ambiguity-free alphabet, supports legacy requests without a
  `subject`, and keeps user and group approvals in separate files.
- Supports approve, list, inspect, and revoke operations. Approval persists
  the allowlist before consuming a request, and rejects an unreadable group
  allowlist without consuming the request.
- Uses the workspace scope and global channels root from `channels::paths`.
  Scoped state is isolated by workspace, common channel names retain their
  filename spelling, other names use `encodeURIComponent`-compatible encoding,
  and legacy files are copied into each scope behind a per-channel sentinel.
- Replaces state files atomically, serializes in-process read/modify/write
  operations, and applies `0700` directory and `0600` file permissions on
  Unix. Legacy migration is best effort and does not overwrite scoped state.
- Propagates storage errors through the gate trait and `SenderGate`/
  `GroupGate` APIs. The TypeScript store's forgiving reads remain forgiving;
  errors from request and approval writes stay visible.

The trait has been changed to return `io::Result` so filesystem failures are
not converted into a pairing rejection. Callers of gate methods now need to
handle `io::Result` as well. The crate module export is left to the integrating
change, as requested.

Remaining limits: the mutation mutex coordinates stores in one process;
separate processes can still race on a read/modify/write cycle. Files are
atomically replaced, so readers will not observe partial JSON. Native Windows
permission behavior is not equivalent to Unix owner-only mode bits, and macOS
runtime permission behavior has not been exercised here.

Focused unit tests cover request reuse/caps, expiry, typed approvals, workspace
isolation, path encoding, grandfather migration and retry, strict group
allowlist handling, write-failure preservation, atomic updates, and Unix
permissions. Run them after `pairing_store` and `paths` are exported from
`channels/mod.rs` with:

```sh
cargo test -p canopy-core channels::pairing_store --locked
```
