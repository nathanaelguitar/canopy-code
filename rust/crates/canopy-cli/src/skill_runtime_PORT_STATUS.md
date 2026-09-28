# Native CLI skill runtime status

The native `canopy run` path now discovers project, user, custom-directory,
extension, and bundled skills through `canopy-core::skills::SkillManager`. It
respects `skills.disabledLevels`, `skills.disabled`, `skills.defaultDisabled`,
and `skills.enabled`, plus safe and bare mode. The `skill` function declaration
is included only when enabled by the configured core/exclusion tool settings.

The model receives an XML-escaped `<available_skills>` reminder. Unconditional
skills are listed at prompt setup; path-gated skills become visible after a
successful filesystem tool call matches their `paths` patterns. Invoking a
listed skill validates its name, asks for approval under the CLI's current
default permission policy, and returns the skill base directory and manifest
body. A skill's valid `allowedTools` entries become session-scoped allow rules
after its first approved invocation; configured ask and deny rules retain
precedence. Repeated invocation returns a short already-loaded response.

Direct `/<skill>` invocations are also handled by the native interactive loop
in TUI and line mode. They use the skill's manifest body and file directory,
apply only its declared `allowedTools` rules, honor `skills.disabled`,
`slashCommands.disabled`, bare mode, and `user-invocable: false`, using
case-insensitive command-name matching. `skills.defaultDisabled`/`skills.enabled`
remain part of model-facing skill availability filtering. Normal tool approval
continues to apply to tools outside the skill's declared allow rules.
Arguments are appended to the prompt and written verbatim to a private
session-scoped `.canopy/tmp/s-<session>/canopy-skill-args-<skill>.txt` file;
a bare invocation clears that record and warns in the prompt if it cannot be
revoked.

## Remaining parity gaps

- Direct skill commands support up to five stacked invocations such as
  `/first-skill /second-skill describe the task`. Each skill receives an empty
  argument string, prior per-skill argument files are revoked, declared tool
  allow rules are applied, and the remaining text is sent with the combined
  skill content. The native TUI completes eligible skill names at the root and
  subsequent stack positions. Additional recognized skill tokens remain in
  the prompt and produce a warning. TypeScript's slash-command processor also
  records command usage telemetry; that surface is not wired into the native
  CLI.
- This is file-skill execution, not the full TypeScript `SkillTool` surface. It
  does not register skill hooks, honor per-skill model overrides, or emit skill
  telemetry.
- Neither the current TypeScript skill runtime nor the native CLI has a
  referenced-file hydration format or implementation. Both return the skill
  body with its base directory so the model can load referenced files through
  available tools when needed.
- MCP model-invocable prompts are not merged into the skill listing and are
  not a fallback when a file skill is missing.
- The CLI does not start skill filesystem watchers. Skill files and settings
  are read into the manager cache at session start; edits and settings changes
  take effect in a later session.
- The bundled-skill directory can be set with `CANOPY_BUNDLED_SKILLS_DIR`.
  Otherwise the CLI checks for `bundled/skills` beside the executable, then
  falls back to the repository's `packages/core/src/skills/bundled` source
  directory embedded as a build-time path. `rust/scripts/package-cli.sh` now
  assembles a standalone Rust CLI package with sibling `bundled/skills` copied
  from the core source tree. The npm `canopy` entrypoint and release automation
  have not switched to or published that Rust package flow yet.

No tests were run for this slice. From `rust/`, `rustfmt --check --edition
2024 crates/canopy-cli/src/main.rs` and `cargo check -p canopy-cli --locked
--offline` passed.
