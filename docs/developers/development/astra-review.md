# Astra overlay review

**Status:** selective fixes adopted; experimental agent-loop overlay deferred

## Review result

The Astra bundle targets commit
`403250faed1aac3fb6db39b4d421e94377282502`, while the canonical repository is
now substantially newer. Its installer was therefore not run against the
working tree. The bundle is useful as a design review and as a source of two
small, independently testable fixes, but it is not a mergeable repository
snapshot.

The included standalone validation was not reproducible as published: the
suite reached 24 passing tests and failed its installer fixture on the 25th
test while trying to parse the backup path from installer output. It also did
not include a full repository build, native integration tests, or a live
provider benchmark.

## Changes adopted

- Legacy tool aliases now use an own-property check. Names such as
  `constructor`, `toString`, and `__proto__` no longer resolve inherited
  `Object.prototype` values as aliases.
- Producer-level head/tail previews now avoid splitting UTF-16 surrogate pairs
  when a Unicode character lands on the truncation boundary.
- The ACP reconnect ring is now bounded by serialized bytes as well as event
  count. It keeps one oversized newest event so the existing resume/resync
  protocol can still report the older gap. The bus tracks retained serialized
  bytes for diagnostics and clears the ring on seed/close.

These changes have focused regression tests. The byte cap is a retention
guard, not a claim about JavaScript heap size: object overhead, duplicate
buffers, provider state, and other processes still need measurement.

## Deferred Astra components

ObservationPack, evidence reduction, compaction economics, and action fusion
remain out of the live agent loop. In the bundle, only ObservationPack was
wired, behind an opt-in environment variable; the other components were
callable modules without native-runtime integration. Enabling them here would
change request prefixes, provider caching behavior, transcript projections,
archive lifetimes, and scheduler semantics without full-repository validation.

The right follow-up is a recorded workload benchmark that compares canonical
history, model-facing request size, cache behavior, retained bytes, and task
quality. Saved UTF-8 bytes are not token or dollar savings, and a disk archive
does not reduce memory unless the original payload becomes unreachable.

## Memory-architecture direction

The event-ring finding from the accompanying memory RCA is actionable without
a Rust rewrite. First measure ring bytes, active history, artifacts, queues,
pending requests, process memory categories, and system pressure. Then wire
the ring statistics into daemon diagnostics and add a reconnect workload test.

The first diagnostics slice is now wired: daemon session status exposes the
ring's current event count, serialized bytes, and both configured caps, with
the same additive shape mirrored in the TypeScript SDK. The bridge regression
suite asserts the byte invariant during adaptive journal growth, and the event
bus suite covers byte-driven eviction and oversized newest events.

Only after the bounded TypeScript path has been benchmarked should we compare a
Rust session/replay engine. Rust would remove V8 from a migrated component,
but it would not fix an unbounded retention policy, duplicate ownership across
the process boundary, or an unbounded message channel by itself.
