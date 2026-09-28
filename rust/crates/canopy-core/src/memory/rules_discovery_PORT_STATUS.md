# Rules discovery port status

`rules_discovery.rs` ports the baseline and conditional rule flow from
`packages/core/src/utils/rulesDiscovery.ts`:

- Parses optional normalized YAML frontmatter (`description` and `paths`),
  strips complete and unclosed HTML comment openers, and skips empty bodies.
- Recursively collects regular `.md` files from global rules and trusted
  project rules, skips unreadable paths, and sorts by UTF-16 code-unit order
  before applying absolute-path exclusion globs.
- Preserves global-before-project ordering, baseline/conditional separation,
  rule count, and source-marked output paths with forward slashes.
- Provides a session-scoped `ConditionalRulesRegistry` that matches original
  and realpath-resolved project-relative paths, injects each matching rule
  once, and exposes total/injected counts.

Native CLI and ACP construct the registry from startup memory and append
matching rules after filesystem tool calls.

`load_rules_with_global_dir` accepts an explicit global Canopy directory for
runtime-owned storage; `load_rules` uses the process `Storage` default.

Known differences: exclusion and conditional globs use `globset`, so advanced
picomatch-only syntax may differ. YAML uses the existing Rust YAML adapter and
may differ on unusual JavaScript `String(value)` coercions or malformed scalar
frontmatter. Directory/read errors are skipped as best effort. The module is
exported as `memory::rules_discovery` and is used by native CLI and ACP startup
memory loading. Conditional-rule matching is wired after filesystem tool calls
in both hosts. Broader host parity and glob/YAML edge cases remain to be
consolidated.
