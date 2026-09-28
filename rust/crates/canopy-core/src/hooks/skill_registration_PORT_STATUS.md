# Skill hook registration port status

Ported `packages/core/src/hooks/registerSkillHooks.ts` to
`skill_registration.rs`.

The module registers command and HTTP hooks from `SkillConfig.hooks` into the
existing `SessionHooksManager`, skips function and unknown hook types, uses an
empty matcher when it is absent or empty, sets `CANOPY_SKILL_ROOT` on command
hook environments when a skill root is available, and returns the number of
registered hooks. Unregistration remains a no-op returning zero, matching the
TypeScript implementation.

## Parity gaps

- Rust skill hook settings use a `HashMap`, so event-key insertion order from
  JavaScript objects is unavailable. Registration sorts event names for
  deterministic behavior; matcher and hook vector order is retained.
- Malformed hook JSON and unknown event names are skipped because Rust's
  `HookEventName` is closed and the normal skill loader filters supported
  entries before runtime registration.
- The TypeScript debug and info log messages are not emitted here; the Rust
  core crate does not currently expose the same debug logger.
- A Rust `PathBuf` skill root is converted lossily to text for the environment
  value.

## Export needed

Add this line to `hooks/mod.rs`:

```rust
pub mod skill_registration;
```
