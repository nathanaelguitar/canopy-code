# Channel loop store port

`channel_loop_store.rs` ports `packages/channels/base/src/ChannelLoopStore.ts`
and its focused tests. It includes typed session targets and loops, strict
top-level JSON parsing, entry validation with invalid-entry skipping, legacy
`runCount` normalization, target filtering, collision-safe IDs, serialized
per-instance updates, enabled-target caps, status patches, and private
same-directory atomic file replacement.

Behavior differences and limits:

- Rust methods are async and return `io::Result`; serialization is guarded by
  a Tokio mutex. As in the source, separate store instances/processes do not
  share an update lock. Dropping a Rust future can cancel a queued operation;
  JavaScript promises in the source are not cancelable.
- The parser retains unknown loop and target JSON fields, but typed create and
  patch inputs expose only known fields.
- Unix uses private `0700` directories and `0600` files. Permission changes
  are no-ops on non-Unix platforms. Same-directory rename is atomic on macOS
  and other POSIX systems; replacing an existing destination may differ on
  Windows.
- Rust rejects non-finite numbers that are not representable in JSON and uses
  `f64` for the source's JavaScript numeric fields.

Focused tests: 11 passed in an isolated temporary Cargo harness. The
repository-workspace test command could not run with `--locked` because the
shared workspace lockfile is stale; no manifest or lockfile was changed.
