# Weixin account storage port

`weixin_accounts.rs` ports `packages/channels/weixin/src/accounts.ts` and its
focused tests. It honors a nonempty `WEIXIN_STATE_DIR` override, otherwise uses
`<global Qwen directory>/channels/weixin`, creates a missing directory, treats
missing/unreadable/malformed JSON as absent, and serializes saved accounts with
the source's camelCase JSON fields. An absent `userId` is omitted. Loads return
any successfully parsed JSON `Value`, matching the source's runtime-only type
assertion rather than validating the declared TypeScript interface.

Saves reuse the core atomic-file writer with mode `0600`, forced mode
replacement, and no-follow destination handling. It creates an exclusive,
random same-directory temporary file, replaces the destination atomically, and
cleans up temporary files on errors. This replaces a destination symlink rather
than following it. The shared writer names temps `.account.json.canopy-*.tmp`;
logout also sweeps the source's `account.json.*.tmp` names so orphaned
credentials from either naming scheme are removed. On non-Unix platforms the
shared writer cannot enforce POSIX mode bits. Callers that need the declared
`AccountData` shape must deserialize the returned value explicitly.

Focused tests cover path override/fallback selection, recursive directory
creation, permissive missing/corrupt loads, exact JSON shape and optional-field
omission, private replacement and mode narrowing, destination-symlink
replacement, successful and failed temp cleanup, and account/orphan clearing
while preserving unrelated files. Exact generated-temp-path symlink collision
is enforced by the shared writer's exclusive create and `O_NOFOLLOW` path but
is not pinned by a deterministic test in this module.

Verification: the standalone offline Cargo harness passed **24/24** tests,
including 11 module tests and the path/atomic-writer helper tests;
`rustfmt --check` and `git diff --check` passed. The harness lives under `/tmp`
and does not alter workspace manifests or the lockfile.
