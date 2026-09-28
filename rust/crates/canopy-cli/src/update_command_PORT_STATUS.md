# Native update command port status

`update_command.rs` provides `pub fn run(args: &[String]) -> Result<(), String>` for a future `canopy update` dispatcher branch. The command has no required arguments and supports `--help` / `-h`.

## Ported behavior

- Checks the `latest` npm dist-tag with a five-second HTTP timeout. Nightly builds also compare the `nightly` tag and use the same preference rule as the TypeScript command when both tags have the same major, minor, and patch version.
- Uses `npm_config_registry` or `NPM_CONFIG_REGISTRY` when set, otherwise `https://registry.npmjs.org`. The response is bounded to 128 KiB and versions are compared with SemVer.
- Reads the installed version from a verified Node standalone manifest when the running executable is that install's bundled Node runtime; otherwise it compares the compile-time Rust package version. Reports that version as up to date or reports an available version. `DEV=true` skips the check with the TypeScript-compatible development-mode message.
- Automatically updates a detected Node standalone installation through `standalone_update_command.rs`; other installation types receive the existing manual guidance.
- Prints manual update guidance for git checkouts, npx/pnpx/bunx, Homebrew paths, and detected global pnpm/yarn/bun installs. Other executable locations receive npm global-install instructions. Nightly releases use the `@nightly` tag.
- Does not run an installer or package manager for non-standalone installations.

## Gaps from TypeScript

- The standalone updater handles the existing Node bundle layout. It does not update native Rust release artifacts, perform a first-time npm-to-standalone migration, or recreate or repair shell PATH wrappers. `/doctor rollback` restores a detected standalone installation's retained `.old` directory on non-Windows systems; Windows displays the TypeScript manual-recovery guidance. The updater also rolls back a failed promotion automatically.
- The updater embeds the same Ed25519 key as the TypeScript implementation. The TypeScript source labels this a test key pending replacement with the production release key; signature checks remain optional unless `CANOPY_REQUIRE_SIGNATURE=1` is set. Release signing configuration must be finalized before relying on signatures as production trust.
- Windows applies the staged update through a detached batch helper after the current process exits. It logs to `canopy-update.log`; if recovery also fails, manual recovery may be needed. Windows ARM and other targets outside the upstream archive list are unsupported.
- Native registry access does not read npm's `.npmrc` files or npm authentication settings. It honors only the registry environment variables above; private registries that require npm credentials may fail.
- Installation detection is path-based and intentionally less complete: it does not run `brew list`, detect local package-manager lockfiles, or check whether a global install directory needs `sudo`.
- Native Rust installations compare the compile-time `CARGO_PKG_VERSION`; the TypeScript command reads the installed npm package version. At the current checkout those versions differ (`0.1.0` in the Rust workspace and `0.21.11` in the CLI package), so release version synchronization is required before shipping native update checks.
- Native CLI error handling exits with its existing command error status, rather than setting the TypeScript process exit code to 1. The native command also uses English strings because this CLI module has no i18n initialization.

## Integration and dependencies

- Dispatcher: `main.rs` routes `canopy update` to `update_command::run` and lists it in root help.
- Dependencies: direct CLI dependencies now include `base64`, `chrono`, `flate2`, `futures-util`, `reqwest`, `ring`, `semver`, `sha2`, `tar`, Tokio, `uuid`, `walkdir`, and `zip`. All archive crates and `ring` were already present in the workspace lockfile; the CLI package dependency list was updated manually.
- Verification: `rustfmt` and `git diff --check` only. No tests or Cargo commands were run, per task instructions.
