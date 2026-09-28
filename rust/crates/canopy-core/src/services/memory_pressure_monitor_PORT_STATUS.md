# `memoryPressureMonitor.ts` Rust port status

Source inspected: `packages/core/src/services/memoryPressureMonitor.ts` and
`packages/core/src/services/memoryPressureMonitor.test.ts`, compared with
`memory_pressure_monitor.rs` and the native `memory_pressure_runtime.rs`
adapter.

The Rust monitor preserves the source's one-pending-check coalescing,
recommendation ranking, cooldown/escalation ordering, session-generation
cancellation, sequential cleanup steps, best-effort compaction handling,
failure counts, and result measurement. The runtime adapter supplies native
RSS/CPU sampling while the caller continues to provide cleanup, scheduling,
clock, and reporting hooks.

This audit fixed a startup race in `start_reserved_cleanup`: a stronger
recommendation may arrive while the pre-cleanup RSS sample is running. If that
sample fails, the monitor records the failure and retries the strongest queued
recommendation while retaining the startup reservation. The same loop is used
when starting an already queued recommendation, so a failed queued-start sample
can also yield to a still stronger recommendation queued during that sample.
With no queued recommendation, the reservation is released. The cleanup
cooldown timestamp is sampled when cleanup is ready to begin, after diagnostics
and the successful pre-cleanup sample.

The hard/critical dump path records the current measurement in the runtime ring
before invoking diagnostics. The dumper synchronously writes phase one from the
newest ring sample before cleanup starts, so the first crash-surviving file
contains the pressure-triggering RSS in both `memoryUsage.rss` and
`recentSamples[0].rss`, including on the monitor's first same-tick check. The
CLI also includes that same sampled value as `session.nativeMemory.processRssBytes`
and adds a best-effort system-memory snapshot with host total, Linux available
memory when reported, and the effective host/cgroup cap. Linux procfs and
cgroup reads use fixed byte limits; failed or unsupported probes mark the
snapshot unavailable. If phase two completes, its fuller memory probe replaces
`memoryUsage` with a later reading; `recentSamples` and the native RSS field
continue to preserve the pressure-triggering reading.

On macOS, the snapshot now also makes a bounded, time-limited call to
`/usr/bin/vm_stat`. When its page size and free, inactive, and speculative
page counts parse cleanly, `availableBytes` is their checked sum multiplied by
the reported page size, and `availableBytesMethod` identifies that calculation.
The no-shell process runner caps execution at one second and stdout at 4 KiB;
probe or parse failure leaves the estimate absent while retaining the host and
effective-limit fields. This is only a rough reclaimable-page estimate: macOS
does not expose one definitive available-memory value through this probe, VM
page states can change immediately, and other reclaimable or compressed memory
is not modeled. It is diagnostic context only and does not affect pressure
classification, cleanup thresholds, or OOM prevention.

Source parity caveat: the TypeScript monitor classifies the greater of RSS
pressure and V8 heap pressure. Native Rust currently classifies RSS only because
it cannot query V8 heap counters. Native phase-one diagnostics identify V8
fields as unavailable and use zero for `arrayBuffers`. The native full
collection takes another RSS reading, so its `memoryUsage.rss` can differ
slightly from the exact pressure-triggering RSS retained in phase one and
`recentSamples`.

The native CLI and ACP task samples once promptly at session startup, then
keeps the 15-second periodic cadence. `AgentRuntime` also requests a check after
every attempted tool execution. Those requests travel through a capacity-one
channel, so bursts coalesce; one task serially runs scheduled checks and awaits
cleanup before handling another request. The existing `schedule_check` bit
also coalesces requests when a check is already scheduled.

## Verification

- `rustfmt --edition 2024 rust/crates/canopy-core/src/services/memory_pressure_monitor.rs rust/crates/canopy-core/src/services/memory_diagnostics_dumper.rs` — passed.
- `cargo check --manifest-path rust/Cargo.toml -p canopy-core --locked` — passed.
- The diagnostic snapshot wiring also passed targeted `rustfmt --check`,
  `cargo check -p canopy-cli --locked --offline`, and `git diff --check`. The
  CLI check reports the three existing warnings in `mcp_host.rs`.
- Tests were not run for this slice.

Known runtime boundary: the caller chooses how to schedule `schedule_check`;
native CLI and ACP use the bounded task signal described above. Dropping an
in-flight Rust future cancels its remaining async cleanup work; generation
checks prevent stale work from updating a reset session, but cannot interrupt a
cleanup callback already executing synchronously.
