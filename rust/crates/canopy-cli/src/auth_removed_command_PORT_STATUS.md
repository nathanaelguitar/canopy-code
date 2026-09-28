# Rust legacy auth command

`auth_removed_command.rs` mirrors the current TypeScript `canopy auth` command:
every invocation prints the migration notice directing interactive users to
`/auth` and headless users to provider environment variables or settings.
Terminal colors are enabled only for a TTY when `NO_COLOR` is unset.

This ports the deprecation shim, not provider login, OAuth, or credential
management. No tests or Cargo checks were run specifically for this command;
the parent workspace check covers compilation.
