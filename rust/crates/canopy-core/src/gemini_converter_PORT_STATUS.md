# Gemini extension converter status

`gemini_converter.rs` and `gemini_package_converter.rs` port the Gemini
manifest/config projection and complete package conversion from
`packages/core/src/extension/gemini-converter.ts`.

## Covered

- Reads `gemini-extension.json` only when its canonical path remains under the
  extension root. Missing/broken/outside manifest paths fail conversion;
  detection returns false for missing or escaping manifests.
- Config conversion retains the source's JavaScript truthiness requirement for
  `name` and `version`, and copies only `name`, `version`, `mcpServers`,
  `contextFileName`, and `settings`, preserving their JSON values.
- Detection surfaces read and malformed-JSON errors, and returns false for
  non-object JSON or values without string `name` and `version`, matching the
  source helper's heuristic.
- Package conversion creates a unique private directory under the OS temp
  directory, copies regular files/directories, and dereferences only symlinks
  whose canonical targets stay inside the package root. Broken links, escaping
  links, and special files are skipped. A directory-cycle guard prevents
  recursive internal symlinks from exhausting the stack.
- Non-hidden `.toml` files under `commands/` convert recursively. A non-empty
  string `description` becomes YAML frontmatter; the string `prompt` becomes
  the body. Successful conversions remove the original TOML. Per-file errors
  leave the source file and return a warning; fatal package errors best-effort
  remove the temporary directory.
- `canopy-extension.json` is written as pretty JSON after file conversion.

## Remaining gaps

- The Rust converter API is not yet called by the extension installer,
  marketplace install/update, or UI flows. Those callers still need to adopt
  the returned directory and own its later cleanup/commit lifecycle.
- Hosts must forward the returned command warnings to their diagnostic logger.
- Rust `toml` parse diagnostics and edge-case compatibility can differ from
  `@iarna/toml`; common TOML 1.0 command documents and conversion structure are
  supported, but byte-for-byte error wording is not promised.
- The source implementation does not guard internal symlink cycles. Rust stops
  those cycles safely, so package contents below a recursive cycle can differ.
- No package-conversion runtime parity check has been run in this port slice.
