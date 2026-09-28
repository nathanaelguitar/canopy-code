# ACP session media port status

`session_media.rs` ports the session-scoped image store, per-item/session
limits, media-reference validation, duplicate-reference rejection, base64
resolution, missing-file cleanup, explicit removal, and the unavailable-media
marker. `resolve_content_degrading` now mirrors the TypeScript degrade path:
non-media blocks and successfully resolved media are preserved, only invalid
or gone media references are omitted, and the result reports the retained
blocks, resolved blocks, and degraded count. Operational I/O failures continue
to propagate. Successful repeated media IDs are memoized within one degrading
call. `SessionMediaResolveMemo` now provides caller-shared, per-media single
flight resolution. Pass the same memo to `resolve_content_with_memo` or
`resolve_content_degrading_with_memo` across concurrent calls to share an
in-flight read and base64 encode, then reuse the successful result. Failed
initializations are not cached; waiters can retry and later calls can retry as
well. The original methods remain and create call-local memos by default.

The degrading helper has no runtime caller yet. Rust `serve` has not ported
the promoted mid-turn message dispatch that uses this path in TypeScript. Rust
callers must pass the same memo explicitly; there is no automatic runtime-wide
memo sharing.

No tests were added or run for this media slice.
