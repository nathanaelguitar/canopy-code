# Rust extension update command port status

The standalone command implementation is in `extensions_update_command.rs`.
It accepts one exact installed extension name or `--all`, scans the installed
user-extension inventory (including disabled extensions), and uses the
journaled `ExtensionArtifactOperation::Update` transaction to replace an
artifact while retaining its activation policy.

Update preparation is shared with the native installer through
`prepare_extension_update_artifact` in `extensions_install_command.rs`. It
reuses the bounded source acquisition, archive extraction, conversion, stable
identity check, staging validation, and temporary-file cleanup paths. Existing
`.env` and settings-selector files are copied into the stage only when they are
regular files, so unchanged settings and the associated secret bundle remain
available after the atomic swap. Before preparation, the command now probes
Git refs, GitHub release tags, and npm dist-tags. If a bounded probe confirms
the stored commit or release tag is still current, the command skips download
and conversion. A changed, failed, or inconclusive probe continues through the
shared preparation path; the prepared artifact is compared again before the
journaled update transaction commits. It then rescans inventory and reports a
warning if the committed extension cannot be found there.

Archive updates use the installer's existing archive validation path. Tar
archives are scanned before extraction and reject symbolic and hard links; the
extraction pass checks paths again, rejects traversal, symlink parents, and
unsupported entry types, and writes files in bounded chunks. ZIP extraction
skips `__MACOSX/` entries and rejects symbolic links, paths outside the
extraction root, and symlink parents. Both paths flatten a single wrapped
extension directory when eligible and require a supported extension manifest
at the root or inside that one directory before conversion. The Rust path
provides the same effective link and traversal protections as TypeScript; its
TAR extractor additionally rejects special entry types that extension
archives do not need. Extraction errors may differ in wording from TypeScript.

The command is wired through the Rust CLI at
`canopy extensions update ...`, passing the arguments following `update` to
`extensions_update_command::run`.

## Remaining differences from TypeScript

- Local sources and archive URLs still require acquisition and conversion to
  compare versions. Archive URL install metadata does not persist an ETag,
  Last-Modified value, or content digest that would let the update command skip
  that work safely. The npm probe reads `NPM_TOKEN` and registry-scoped
  `_authToken` entries from the workspace and user `.npmrc`, following the same
  origin and path matching rules as TypeScript. Redirects and invalid or
  inconclusive probe responses fall through to acquisition, which resolves
  npm credentials through the install path.
- Rust now reads and preserves TypeScript's `networkPolicy` field. For stored
  `public` policy, Git, GitHub release, and npm probes, plus Git, GitHub release,
  npm, archive, and nested Claude-plugin source acquisition resolve each direct
  request host, reject any blocked DNS result, pin the selected public address,
  and disable HTTP proxies; manual HTTP redirects are checked and pinned one
  hop at a time. Git uses the equivalent `curloptResolve` mapping, disables
  redirects and proxies, clears ambient Git configuration, and requires Git
  2.37 or newer. Probe timeouts and response-size limits remain in place; a
  failed probe falls through to acquisition, which applies the same policy
  again. Direct Rust installs can opt into this validated acquisition path
  with `--network-policy public`; omitting the flag preserves the TypeScript
  install CLI default of no explicit policy. The TypeScript `ExtensionManager`
  API accepts `networkPolicy: 'public'`, while its install CLI does not expose
  that option. The Rust CLI also has no cancellation signal to propagate
  through DNS and network acquisition as TypeScript does.
- The CLI has no interactive settings reconfiguration callback. This port
  preserves settings when declarations are unchanged and rejects updates that
  add, remove, or change the effective sensitivity of a setting.
- The Rust command refreshes its on-disk inventory after commit. It cannot
  refresh the TypeScript process's in-memory extension manager, loaded tools,
  commands, skills, or MCP clients because the Rust CLI currently has no
  corresponding live manager lifecycle.
- Conversion warnings are surfaced as `extension_conversion_warning`; the
  installer converters do not yet expose the TypeScript runtime's complete
  structured warning set. The post-commit inventory scan reports
  `extension_inventory_refresh_failed` when the new artifact is absent or
  unreadable.
- For all-updates, Rust processes candidates sequentially and continues after
  an individual failure. The TypeScript implementation schedules updates in
  parallel and its command-level catch may suppress the aggregate success
  output when one update rejects.
- An inconclusive Rust probe falls through to full acquisition, so a remote
  probe error can still be followed by a successful update. TypeScript records
  all-update probe errors and skips those extensions before its update phase.

No tests were run. `rustfmt --edition 2024 --check` and
`cargo check -p canopy-cli --locked --offline` passed; Cargo reported only the
existing dead-code warnings in unrelated CLI modules.
