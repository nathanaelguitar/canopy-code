//! Context filename constants and process-wide filename selection.
//!
//! This mirrors `packages/core/src/memory/const.ts`. The global selection is
//! synchronized because Rust callers may access it from multiple threads.

use std::sync::{OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub const DEFAULT_CONTEXT_FILENAME: &str = "CANOPY.md";
pub const AGENT_CONTEXT_FILENAME: &str = "AGENTS.md";
/// Per-developer project context at `<projectRoot>/.canopy/CANOPY.local.md`.
/// It is a fixed-slot supplement, not part of the upward-search filename list.
pub const LOCAL_CONTEXT_FILENAME: &str = "CANOPY.local.md";
pub const MEMORY_SECTION_HEADER: &str = "## Canopy Added Memories";

/// A single or multiple filename input accepted by [`set_gemini_md_filename`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContextFilenameInput {
    One(String),
    Many(Vec<String>),
}

impl From<String> for ContextFilenameInput {
    fn from(filename: String) -> Self {
        Self::One(filename)
    }
}

impl From<&str> for ContextFilenameInput {
    fn from(filename: &str) -> Self {
        Self::One(filename.to_owned())
    }
}

impl From<Vec<String>> for ContextFilenameInput {
    fn from(filenames: Vec<String>) -> Self {
        Self::Many(filenames)
    }
}

impl From<Vec<&str>> for ContextFilenameInput {
    fn from(filenames: Vec<&str>) -> Self {
        Self::Many(filenames.into_iter().map(str::to_owned).collect())
    }
}

impl<const N: usize> From<[&str; N]> for ContextFilenameInput {
    fn from(filenames: [&str; N]) -> Self {
        Self::Many(filenames.into_iter().map(str::to_owned).collect())
    }
}

static CURRENT_CONTEXT_FILENAMES: OnceLock<RwLock<Vec<String>>> = OnceLock::new();

fn is_javascript_trim_char(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'..='\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}

fn trim_javascript_whitespace(value: &str) -> &str {
    value.trim_matches(is_javascript_trim_char)
}

fn filenames_state() -> &'static RwLock<Vec<String>> {
    CURRENT_CONTEXT_FILENAMES.get_or_init(|| {
        RwLock::new(vec![
            DEFAULT_CONTEXT_FILENAME.to_owned(),
            AGENT_CONTEXT_FILENAME.to_owned(),
        ])
    })
}

fn read_filenames() -> RwLockReadGuard<'static, Vec<String>> {
    filenames_state()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_filenames() -> RwLockWriteGuard<'static, Vec<String>> {
    filenames_state()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Replace the process-wide filename selection.
///
/// A blank single name and an empty list are no-ops. For a non-empty list,
/// each item is trimmed and retained, including items that become empty.
pub fn set_gemini_md_filename(filename: impl Into<ContextFilenameInput>) {
    match filename.into() {
        ContextFilenameInput::One(filename) => {
            let filename = trim_javascript_whitespace(&filename);
            if !filename.is_empty() {
                *write_filenames() = vec![filename.to_owned()];
            }
        }
        ContextFilenameInput::Many(filenames) => {
            if !filenames.is_empty() {
                *write_filenames() = filenames
                    .into_iter()
                    .map(|filename| trim_javascript_whitespace(&filename).to_owned())
                    .collect();
            }
        }
    }
}

/// Return the first non-blank configured filename, or `CANOPY.md` if none is
/// usable. List entries are trimmed before they are returned.
pub fn get_current_gemini_md_filename() -> String {
    read_filenames()
        .iter()
        .find_map(|filename| {
            let trimmed = trim_javascript_whitespace(filename);
            (!trimmed.is_empty()).then(|| trimmed.to_owned())
        })
        .unwrap_or_else(|| DEFAULT_CONTEXT_FILENAME.to_owned())
}

/// Return a snapshot of all configured filenames in their configured order.
pub fn get_all_gemini_md_filenames() -> Vec<String> {
    read_filenames().clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_selection_matches_source_contract() {
        assert_eq!(
            get_all_gemini_md_filenames(),
            [DEFAULT_CONTEXT_FILENAME, AGENT_CONTEXT_FILENAME]
        );
        assert_eq!(get_current_gemini_md_filename(), DEFAULT_CONTEXT_FILENAME);

        set_gemini_md_filename("  CUSTOM.md  ");
        assert_eq!(get_current_gemini_md_filename(), "CUSTOM.md");
        assert_eq!(get_all_gemini_md_filenames(), ["CUSTOM.md"]);

        set_gemini_md_filename("  ");
        assert_eq!(get_all_gemini_md_filenames(), ["CUSTOM.md"]);
        set_gemini_md_filename(Vec::<String>::new());
        assert_eq!(get_all_gemini_md_filenames(), ["CUSTOM.md"]);

        set_gemini_md_filename("\u{FEFF} BOM.md \u{FEFF}");
        assert_eq!(get_current_gemini_md_filename(), "BOM.md");

        set_gemini_md_filename(["  ", " AGENTS.md ", "NOTES.md"]);
        assert_eq!(get_all_gemini_md_filenames(), ["", "AGENTS.md", "NOTES.md"]);
        assert_eq!(get_current_gemini_md_filename(), "AGENTS.md");

        set_gemini_md_filename([" ", "\t"]);
        assert_eq!(get_all_gemini_md_filenames(), ["", ""]);
        assert_eq!(get_current_gemini_md_filename(), DEFAULT_CONTEXT_FILENAME);

        // ECMAScript trim does not treat NEXT LINE (U+0085) as whitespace.
        set_gemini_md_filename("\u{0085}name\u{0085}");
        assert_eq!(get_current_gemini_md_filename(), "\u{0085}name\u{0085}");

        // Keep the process-global value at its source default for other tests.
        set_gemini_md_filename([DEFAULT_CONTEXT_FILENAME, AGENT_CONTEXT_FILENAME]);
    }
}
