# Session registry port status

Source: packages/core/src/services/session-registry.ts,
packages/core/src/utils/process-liveness.ts, and their TypeScript tests.

Process-liveness audit: compared the helper with `session_registry.rs` on
2026-09-25. Linux and Unix behavior matches for positive integer PID checks,
signal-zero success, `EPERM`/`EACCES` as existence, Linux-only zombie exclusion,
and conservative handling when `/proc/<pid>/stat` cannot be read. The Linux
identity path matches the source's final-`)` stat parsing, field-22 starttime,
success-only boot-ID cache, and PID-namespace inode lookup. The TypeScript
source still returns no process token outside Linux. The pre-existing Windows
gap remains: Rust currently returns false for every non-Unix PID, so Windows
`EACCES` cannot be treated as an existing process until a Windows
process-query backend is added.

Implemented in session_registry.rs:

- Schema 1 record serialization and validation, 64 KiB bound, strict decimal
  PID candidate names, filename/PID agreement, optional-field normalization,
  and unknown-field removal.
- Best-effort registration, refusal on newer or unreadable records and
  foreign identity collisions, captured registration path for later patch and
  unregister, mutable-field patching, and conservative unregister.
- 0700 registry directory and 0600 record on Unix; atomic replacement uses the
  existing no-follow writer. Enumeration is newest first, leaves foreign
  namespace/boot records alone, rechecks a stale candidate before unlinking,
  and sweeps stale source-format or Rust atomic-writer temp files.
- Linux boot ID caching on success, namespace inode, /proc/<pid>/stat
  starttime parsing anchored after the final ), signal-zero liveness,
  EPERM/EACCES handling, and zombie exclusion.
- The environment trait injects global-root resolution, PID, identity reads,
  liveness, and time for deterministic tests.

macOS limitation inherited from the source implementation: the TypeScript
helper returns no process-start token and no PID-namespace identity outside
Linux. Records therefore have procStart: null and pidNs: null; liveness
uses kill(pid, 0) only. Although the `libc` dependency exposes Apple's
process-start query, calling it requires unsafe FFI, which this crate forbids.
Using `ps` would add a subprocess per record to an interactive enumeration;
the source intentionally avoids subprocesses for this token and returns null
on macOS. Rust retains that contract, so PID reuse and different machines
sharing one Canopy home cannot be distinguished on macOS. Windows is not
implemented equivalently: the dependency-free safe liveness helper has no
OpenProcess implementation, so that target needs a dedicated port before
the registry can safely enumerate Windows peers.

The module is exported from `services/mod.rs`. At an earlier validation point,
all 17 focused regression tests passed as native `canopy-core` unit tests. The
entry points are synchronous
and process-local rather than asynchronous like the TypeScript service, and
their debug log wording is not ported. The shared Rust atomic writer does not
implement the TypeScript writer's foreign-UID in-place fallback; a failed
atomic write is reported as a best-effort registration/patch failure.
Historical validation on Darwin arm64: `cargo test -p canopy-core
services::session_registry --locked --offline` passed all 17 tests. Tests were
not run for the current port slice.

Not ported yet: the TypeScript debug-logger messages for best-effort failures
are omitted; call results and safety behavior are preserved. The TypeScript
atomic writer also has a foreign-UID in-place fallback that the shared Rust
atomic writer does not provide. Registry directories are tightened to 0700,
so normal same-user registry use does not need that fallback, but unusual
ownership layouts are not identical. This implementation uses synchronous
filesystem calls, while the TypeScript entry points are async.
