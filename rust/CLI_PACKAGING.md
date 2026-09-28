# Rust CLI packaging

Run `./scripts/package-cli.sh` from `rust/` or invoke it from another working
directory. It builds `canopy-cli` in release mode with the locked workspace
dependencies, then assembles `dist/<host-target-triple>/` with this layout:

```text
canopy
examples/
  ... contents of packages/cli/src/commands/extensions/examples ...
bundled/
  skills/
    ... contents of packages/core/src/skills/bundled ...
```

The executable-relative `bundled/skills` directory is the path checked by the
native skill resolver. The sibling `examples/` directory supplies the
boilerplates listed by `canopy extensions new <path> [template]`. The script
replaces only the generated package for that target triple. Cargo build output
defaults to `rust/target`; set `CARGO_TARGET_DIR` to use another build cache.

Pass a Cargo target triple to build and package a cross-target binary, for
example `./scripts/package-cli.sh aarch64-unknown-linux-gnu`. The package is
written to `dist/<target-triple>/`; the target's linker and toolchain must be
configured in Cargo first. Windows targets receive a `canopy.exe` filename.

This package flow is separate from the npm `canopy` entrypoint. Release
automation still needs to invoke the script and publish the resulting package
directory for each supported platform.
