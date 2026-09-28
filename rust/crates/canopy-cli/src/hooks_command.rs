//! Native shim for the top-level `canopy hooks` command.
//!
//! Hook configuration is managed from the interactive UI in the TypeScript
//! CLI. The command itself does not load or mutate hook configuration.

const USAGE: &str = "Usage: canopy hooks";

/// Returns whether `command` names this command or its singular alias.
pub fn handles(command: &str) -> bool {
    matches!(command, "hooks" | "hook")
}

/// Mirrors the non-interactive TypeScript command: valid invocation is silent
/// and succeeds. Help and version are disabled for this command there, so
/// trailing arguments are rejected here as well.
pub fn run(args: &[String]) -> Result<(), String> {
    if let Some(argument) = args.first() {
        return Err(format!("{USAGE}\nUnknown argument: {argument}"));
    }

    // The TypeScript command emits a debug-only message through its
    // session-bound logger. The native CLI has no equivalent logger attached
    // to standalone command dispatch; its observable behavior remains silent.
    Ok(())
}
