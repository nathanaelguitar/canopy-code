# Rust extension scaffolding command

`extensions_new_command.rs` ports `canopy extensions new <path> [template]`.
Without a template it creates the destination exclusively and writes a minimal
`canopy-extension.json` with the destination basename and version `1.0.0`. With
a template it validates the name against immediate example directories, then
copies only regular files and directories. It rejects traversal and symlinks,
and removes an incomplete destination if creation fails.

Templates resolve from `examples/` beside the executable for packaged Rust
distributions and fall back to the repository examples directory in a
checkout. `rust/scripts/package-cli.sh` includes the sibling examples tree.
The command prints the `canopy extensions link` next step after creating the
extension.

The native command reports template read errors in English and has no
interactive yargs completion or localized messages. No tests or Cargo checks
were run specifically for this command; the parent workspace check covers the
compiled Rust CLI.
