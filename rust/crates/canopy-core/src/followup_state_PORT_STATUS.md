# Follow-up suggestion state Rust port status

`followup_state.rs` ports the framework-independent controller from
`packages/core/src/followup/followupState.ts`: delayed display and cancellation,
accept debounce, fallback versus live accept telemetry, dismissal, clearing,
callback lookup, and callback panic isolation. State and telemetry field names
retain their JSON shapes, and suggestion length uses JavaScript-compatible
UTF-16 units.

Nine focused tests pass. Rust schedules accepted callbacks with `tokio::spawn`,
which approximates but is not identical to JavaScript's microtask queue. React
and Ink wrappers and the full speculation runtime are not ported here.
