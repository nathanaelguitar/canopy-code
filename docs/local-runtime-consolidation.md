# Local Canopy Code runtime

As of 2026-09-06, the canonical macOS Canopy Code checkout is:

`/Users/nathanaelguitar/canopy-code`

It was cloned from the DGX git bundle at commit `c0db1d36f8` and contains the
DGX daemon/TUI fixes, including interactive question rendering, remote-control
attention notifications, mobile autocorrect, and the remote-control delivery
retry/path fix. The source of truth for future syncs is the DGX checkout:

`/home/nathanaelguitar/repos/qwen-code`

## Host runtime policy

- Do not use Homebrew's `canopy`, `qwen-code`, or Node installation as the
  active runtime.
- The host build uses the checksum-verified official Node `v22.14.0` runtime at
  `/Users/nathanaelguitar/.local/node-v22.14.0-darwin-arm64`.
- The user-owned launcher is `/Users/nathanaelguitar/bin/canopy` and points at
  the checked-in CLI build in this checkout.
- The old Homebrew launcher at `/opt/homebrew/bin/canopy` is not the canonical
  launcher and must not be selected by PATH ordering.

## Rebuilding after DGX changes

On the DGX, finish and verify changes in the canonical checkout, then create a
new bundle:

```sh
cd /home/nathanaelguitar/repos/qwen-code
git bundle create /tmp/canopy-code-dgx.bundle --all
```

Transfer the bundle to the Mac, fetch it into this checkout, and fast-forward
only after reviewing the resulting history and tests. Never copy a Linux
`dist/` binary to the Mac; rebuild locally for Apple Silicon:

```sh
cd /Users/nathanaelguitar/canopy-code
/Users/nathanaelguitar/.local/node-v22.14.0-darwin-arm64/bin/npm ci
/Users/nathanaelguitar/.local/node-v22.14.0-darwin-arm64/bin/npm --workspace packages/cli run typecheck
/Users/nathanaelguitar/.local/node-v22.14.0-darwin-arm64/bin/npm --workspace packages/cli run build
```

## Archived checkouts

The former local checkouts were moved, not deleted, to:

`/Users/nathanaelguitar/.canopy-backups/20260906-dgx-consolidation/`

That archive contains the old `canopy-code` tree, the Aether/iOS
`AetherChat-dev-tools` tree, and the DGX bundle used for this consolidation.
The Aether/iOS repository remains available there for its product history, but
its nested `dev-tools` checkout is deprecated as a Canopy Code CLI source.
