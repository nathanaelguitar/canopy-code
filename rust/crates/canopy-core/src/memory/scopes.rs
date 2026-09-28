//! Public project-memory scope contract.
//!
//! Port of `packages/core/src/memory/scopes.ts`. The implementation lives in
//! `paths` so configuration parsing and path resolution share one enum; this
//! module keeps the source subpath import as a Rust module boundary.

pub use super::paths::{MEMORY_PROJECT_SCOPES, MemoryProjectScope, resolve_memory_project_scope};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_the_two_supported_scope_values() {
        assert_eq!(MEMORY_PROJECT_SCOPES, ["git-root", "workspace"]);
    }

    #[test]
    fn resolves_unknown_values_to_git_root_and_preserves_the_value_for_warning() {
        assert_eq!(
            resolve_memory_project_scope(Some("future")),
            (MemoryProjectScope::GitRoot, Some("future".to_owned()))
        );
    }
}
