//! Build an argv-form `git notes` invocation for commit attribution metadata.
//!
//! Port of `packages/core/src/services/attributionTrailer.ts`. The caller
//! supplies the captured commit hash so a later HEAD movement cannot redirect
//! the note to a different commit.

use serde_json::Value;

const GIT_NOTES_REF: &str = "refs/notes/ai-attribution";
pub const MAX_NOTE_BYTES: usize = 30 * 1024;

/// Safe process invocation: the JSON note is one argv value and never passes
/// through a shell parser.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitNotesCommand {
    pub command: String,
    pub args: Vec<String>,
}

/// Build a `git notes add` invocation, returning `None` when JSON encoding
/// fails or the UTF-8 payload exceeds the source's 30 KiB cap.
pub fn build_git_notes_command(note: &Value, target_commit: &str) -> Option<GitNotesCommand> {
    let note_json = serde_json::to_string(note).ok()?;
    if note_json.len() > MAX_NOTE_BYTES {
        return None;
    }
    Some(GitNotesCommand {
        command: "git".to_owned(),
        args: vec![
            "notes".to_owned(),
            format!("--ref={GIT_NOTES_REF}"),
            "add".to_owned(),
            "-f".to_owned(),
            "-m".to_owned(),
            note_json,
            target_commit.to_owned(),
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::{MAX_NOTE_BYTES, build_git_notes_command};
    use serde_json::{Value, json};

    const TARGET_SHA: &str = "abc1234567890abcdef1234567890abcdef12345";

    fn sample_note() -> Value {
        json!({
            "version":1,
            "generator":"Canopy-Coder",
            "files":{
                "src/main.ts":{"aiChars":150,"humanChars":50,"percent":75},
                "src/utils.ts":{"aiChars":0,"humanChars":200,"percent":0}
            },
            "summary":{
                "aiPercent":38,
                "aiChars":150,
                "humanChars":250,
                "totalFilesTouched":2,
                "surfaces":["cli"]
            },
            "surfaceBreakdown":{"cli":{"aiChars":150,"percent":38}},
            "excludedGenerated":["package-lock.json"],
            "excludedGeneratedCount":1,
            "promptCount":3
        })
    }

    #[test]
    fn builds_the_git_notes_argv_for_the_captured_commit() {
        let note = sample_note();
        let command = build_git_notes_command(&note, TARGET_SHA).unwrap();
        assert_eq!(command.command, "git");
        assert_eq!(
            &command.args[..5],
            [
                "notes",
                "--ref=refs/notes/ai-attribution",
                "add",
                "-f",
                "-m"
            ]
        );
        assert_eq!(command.args[5], serde_json::to_string(&note).unwrap());
        assert_eq!(command.args.last().map(String::as_str), Some(TARGET_SHA));
        assert_ne!(command.args.last().map(String::as_str), Some("HEAD"));
    }

    #[test]
    fn keeps_json_payload_in_one_literal_argument() {
        let note = json!({"files":{"it's-a-file.ts":{"percent":67}}});
        let command = build_git_notes_command(&note, TARGET_SHA).unwrap();
        let parsed: Value = serde_json::from_str(&command.args[5]).unwrap();
        assert_eq!(parsed["files"]["it's-a-file.ts"]["percent"], 67);
        assert_eq!(command.args.len(), 7);
    }

    #[test]
    fn rejects_a_note_above_the_utf8_byte_limit() {
        let note = json!({"body":"x".repeat(MAX_NOTE_BYTES)});
        assert!(build_git_notes_command(&note, TARGET_SHA).is_none());

        let under_limit = json!({"body":"x".repeat(MAX_NOTE_BYTES - 11)});
        assert!(build_git_notes_command(&under_limit, TARGET_SHA).is_some());
    }
}
