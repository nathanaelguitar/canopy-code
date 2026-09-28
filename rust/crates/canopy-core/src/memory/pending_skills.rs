//! Staging for newly created project skills that need user confirmation.
//!
//! Port of `packages/core/src/memory/pending-skills.ts`. Existing skills are
//! left live, while new direct children of `.canopy/skills` are moved into a
//! task-namespaced pending directory until accepted or rejected.

use std::collections::HashSet;
use std::io;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::skills::{
    SKILL_FILE_NAME, get_pending_skills_root, get_project_skills_root, is_project_skill_path,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingSkill {
    /// Skill directory name, for example `auto-skill-foo`.
    pub name: String,
    /// One-line description parsed from frontmatter (may be empty).
    pub description: String,
    /// Manifest path while staged under the pending root.
    pub staged_manifest_path: PathBuf,
    /// Manifest path the skill occupies after acceptance.
    pub final_manifest_path: PathBuf,
}

#[derive(Debug, Error)]
pub enum PendingSkillError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error(
        "Cannot accept \"{name}\": staged directory is missing and it is not in the skills root."
    )]
    MissingForAccept { name: String },
}

/// Move newly created, direct-child skill directories from the live skills
/// root into a pending directory. Manifest read failures are intentionally
/// ignored because touched files may have been deleted before staging; errors
/// from directory creation, replacement, or rename are returned to the caller.
pub async fn stage_skill_dirs(
    touched_files: &[PathBuf],
    project_root: impl AsRef<Path>,
    pre_existing_dir_names: &HashSet<String>,
    task_id: &str,
) -> Result<Vec<PendingSkill>, PendingSkillError> {
    let project_root = project_root.as_ref();
    let skills_root = get_project_skills_root(project_root);
    let pending_root = get_pending_skills_root(project_root);
    let absolute_skills_root = absolute_lexical(&skills_root)?;
    let mut seen = HashSet::<String>::new();
    let mut result = Vec::new();

    for file in touched_files {
        if !is_project_skill_path(file, project_root) {
            continue;
        }
        if file.file_name().and_then(|name| name.to_str()) != Some(SKILL_FILE_NAME) {
            continue;
        }

        // Node's path.resolve(projectRoot, file) treats an absolute `file` as
        // already resolved and otherwise resolves it relative to projectRoot.
        let skill_manifest = absolute_lexical(&resolve_against(project_root, file))?;
        let Some(skill_dir) = skill_manifest.parent() else {
            continue;
        };
        if skill_dir.parent() != Some(absolute_skills_root.as_path()) {
            continue; // direct child only
        }
        let Some(dir_name) = skill_dir.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let dir_name = dir_name.to_owned();
        if !seen.insert(dir_name.clone()) {
            continue;
        }
        if pre_existing_dir_names.contains(&dir_name) {
            continue;
        }

        // Match the TypeScript implementation: read from the skills root,
        // rather than the touched path, and skip any read failure.
        let final_dir = skills_root.join(&dir_name);
        let final_manifest_path = final_dir.join(SKILL_FILE_NAME);
        let content = match tokio::fs::read(&final_manifest_path).await {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(_) => continue,
        };

        // The task id prevents same-named skills from separate review batches
        // from replacing one another while both await confirmation.
        let staged_dir = pending_root.join(task_id).join(&dir_name);
        let staged_parent = staged_dir.parent().unwrap_or(&pending_root);
        tokio::fs::create_dir_all(staged_parent).await?;
        remove_path_force(&staged_dir).await?;
        tokio::fs::rename(&skill_dir, &staged_dir).await?;

        result.push(PendingSkill {
            name: dir_name,
            description: parse_description(&content),
            staged_manifest_path: staged_dir.join(SKILL_FILE_NAME),
            final_manifest_path,
        });
    }
    Ok(result)
}

/// Promote a staged skill to the live skills root. Re-accept is harmless if
/// the staged directory is gone but the final directory already exists.
pub async fn accept_pending_skill(pending: &PendingSkill) -> Result<(), PendingSkillError> {
    let staged_dir = pending
        .staged_manifest_path
        .parent()
        .unwrap_or(Path::new(""));
    let final_dir = pending
        .final_manifest_path
        .parent()
        .unwrap_or(Path::new(""));

    // `fs.access` is only used to distinguish the already-handled case. The
    // source catches any access error, so mirror that boundary here.
    if tokio::fs::metadata(staged_dir).await.is_err() {
        if tokio::fs::metadata(final_dir).await.is_ok() {
            return Ok(());
        }
        return Err(PendingSkillError::MissingForAccept {
            name: pending.name.clone(),
        });
    }

    if let Some(parent) = final_dir.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    remove_path_force(final_dir).await?;
    tokio::fs::rename(staged_dir, final_dir).await?;
    Ok(())
}

/// Delete the staged copy only. A missing path is treated as already rejected.
pub async fn reject_pending_skill(pending: &PendingSkill) -> Result<(), PendingSkillError> {
    let staged_dir = pending
        .staged_manifest_path
        .parent()
        .unwrap_or(Path::new(""));
    remove_path_force(staged_dir).await?;
    Ok(())
}

fn parse_description(content: &str) -> String {
    let Some(frontmatter) = extract_frontmatter(content) else {
        return String::new();
    };
    let Some(line) = frontmatter
        .split('\n')
        .find(|line| line.starts_with("description:"))
    else {
        return String::new();
    };

    // Match /^description:[ \t]*(.*?)[ \t]*$/m: only horizontal whitespace
    // is accepted around the value, so a blank field cannot capture the next
    // YAML field.
    let value = line["description:".len()..].trim_matches(&[' ', '\t'][..]);
    let value = value.trim_matches(is_javascript_trim_space);
    if value.len() >= 2 {
        let first = value.as_bytes()[0];
        let last = *value.as_bytes().last().unwrap();
        if (first == b'"' || first == b'\'') && last == first {
            return value[1..value.len() - 1].to_owned();
        }
    }
    value.to_owned()
}

fn extract_frontmatter(content: &str) -> Option<&str> {
    let opening = content.strip_prefix("---")?;
    let opening_newline = opening.find('\n')?;
    let opening_delimiter = opening[..opening_newline]
        .strip_suffix('\r')
        .unwrap_or(&opening[..opening_newline]);
    if !opening_delimiter
        .bytes()
        .all(|byte| byte == b' ' || byte == b'\t')
    {
        return None;
    }

    let raw = &opening[opening_newline + 1..];
    let close_start = find_closing_frontmatter(raw)?;
    if close_start == 0 {
        return None;
    }
    // The source regex consumes the newline immediately before the closing
    // delimiter, so it is not part of the captured frontmatter.
    let captured_end = raw[..close_start]
        .strip_suffix('\n')
        .map(|value| value.strip_suffix('\r').unwrap_or(value).len())
        .unwrap_or(close_start);
    Some(&raw[..captured_end])
}

fn find_closing_frontmatter(frontmatter_and_body: &str) -> Option<usize> {
    let bytes = frontmatter_and_body.as_bytes();
    let mut line_start = 0;
    while line_start <= bytes.len() {
        let line_end = bytes[line_start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|offset| line_start + offset)
            .unwrap_or(bytes.len());
        let mut logical_end = line_end;
        if line_end < bytes.len() && logical_end > line_start && bytes[logical_end - 1] == b'\r' {
            logical_end -= 1;
        }
        let line = &frontmatter_and_body[line_start..logical_end];
        if let Some(after_dashes) = line.strip_prefix("---") {
            if after_dashes
                .bytes()
                .all(|byte| byte == b' ' || byte == b'\t')
            {
                return Some(line_start);
            }
        }
        if line_end == bytes.len() {
            break;
        }
        line_start = line_end + 1;
    }
    None
}

async fn remove_path_force(path: &Path) -> io::Result<()> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        tokio::fs::remove_dir_all(path).await
    } else {
        tokio::fs::remove_file(path).await
    }
}

fn resolve_against(project_root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        project_root.join(path)
    }
}

fn absolute_lexical(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() && !normalized.has_root() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "path traverses above its root",
                    ));
                }
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    Ok(normalized)
}

fn is_javascript_trim_space(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'..='\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_project(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "canopy-pending-skill-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    async fn make_skill(root: &Path, name: &str, description: &str) -> PathBuf {
        let dir = get_project_skills_root(root).join(format!("auto-skill-{name}"));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let manifest = dir.join(SKILL_FILE_NAME);
        tokio::fs::write(
            &manifest,
            format!("---\nname: {name}\ndescription: {description}\n---\nbody\n"),
        )
        .await
        .unwrap();
        manifest
    }

    #[tokio::test]
    async fn stages_new_direct_child_and_parses_description() {
        let root = temp_project("stage");
        let manifest = make_skill(&root, "alpha", "does alpha").await;
        let staged = stage_skill_dirs(std::slice::from_ref(&manifest), &root, &HashSet::new(), "")
            .await
            .unwrap();
        assert_eq!(staged.len(), 1);
        assert_eq!(staged[0].name, "auto-skill-alpha");
        assert_eq!(staged[0].description, "does alpha");
        assert!(!tokio::fs::try_exists(manifest).await.unwrap());
        assert!(
            tokio::fs::try_exists(&staged[0].staged_manifest_path)
                .await
                .unwrap()
        );
        assert!(
            staged[0]
                .staged_manifest_path
                .starts_with(get_pending_skills_root(&root))
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn skips_existing_nested_and_outside_skills() {
        let root = temp_project("eligibility");
        let existing = make_skill(&root, "existing", "old").await;
        let mut pre_existing = HashSet::new();
        pre_existing.insert("auto-skill-existing".to_owned());
        assert!(
            stage_skill_dirs(std::slice::from_ref(&existing), &root, &pre_existing, "")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(tokio::fs::try_exists(&existing).await.unwrap());

        let nested = get_project_skills_root(&root).join("parent/nested/SKILL.md");
        tokio::fs::create_dir_all(nested.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&nested, "---\ndescription: nested\n---\n")
            .await
            .unwrap();
        let outside = root.join("outside/SKILL.md");
        tokio::fs::create_dir_all(outside.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&outside, "---\ndescription: outside\n---\n")
            .await
            .unwrap();
        assert!(
            stage_skill_dirs(&[nested, outside], &root, &HashSet::new(), "")
                .await
                .unwrap()
                .is_empty()
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn deduplicates_and_skips_missing_manifests() {
        let root = temp_project("dedupe");
        let manifest = make_skill(&root, "same", "same").await;
        let missing = get_project_skills_root(&root)
            .join("missing")
            .join(SKILL_FILE_NAME);
        let staged = stage_skill_dirs(
            &[manifest.clone(), manifest.clone(), missing],
            &root,
            &HashSet::new(),
            "",
        )
        .await
        .unwrap();
        assert_eq!(staged.len(), 1);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn task_id_namespaces_same_named_batches() {
        let root = temp_project("namespace");
        let first = make_skill(&root, "duplicate", "first").await;
        let [first_pending] = stage_skill_dirs(&[first], &root, &HashSet::new(), "task-a")
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let second = make_skill(&root, "duplicate", "second").await;
        let [second_pending] = stage_skill_dirs(&[second], &root, &HashSet::new(), "task-b")
            .await
            .unwrap()
            .try_into()
            .unwrap();
        assert_ne!(
            first_pending.staged_manifest_path,
            second_pending.staged_manifest_path
        );
        assert!(
            tokio::fs::try_exists(first_pending.staged_manifest_path)
                .await
                .unwrap()
        );
        assert!(
            tokio::fs::try_exists(second_pending.staged_manifest_path)
                .await
                .unwrap()
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn description_parser_preserves_empty_fields_and_removes_one_quote_pair() {
        assert_eq!(
            parse_description("---\ndescription:\nsource: auto\n---\nbody"),
            ""
        );
        assert_eq!(
            parse_description("---\r\ndescription:  \"A skill\" \r\nsource: auto\r\n---\r\n"),
            "A skill"
        );
        assert_eq!(parse_description("description: outside\n"), "");
        assert_eq!(parse_description("---\ndescription: 'one'\n---\n"), "one");
    }

    #[tokio::test]
    async fn accept_promotes_and_is_idempotent_but_reports_lost_skill() {
        let root = temp_project("accept");
        let manifest = make_skill(&root, "accept", "yes").await;
        let [pending] = stage_skill_dirs(&[manifest], &root, &HashSet::new(), "task")
            .await
            .unwrap()
            .try_into()
            .unwrap();
        accept_pending_skill(&pending).await.unwrap();
        assert!(
            tokio::fs::try_exists(&pending.final_manifest_path)
                .await
                .unwrap()
        );
        assert!(
            !tokio::fs::try_exists(&pending.staged_manifest_path)
                .await
                .unwrap()
        );
        accept_pending_skill(&pending).await.unwrap();
        reject_pending_skill(&pending).await.unwrap();
        assert!(
            tokio::fs::try_exists(&pending.final_manifest_path)
                .await
                .unwrap()
        );

        let lost = PendingSkill {
            name: "lost".into(),
            description: String::new(),
            staged_manifest_path: get_pending_skills_root(&root)
                .join("task/lost")
                .join(SKILL_FILE_NAME),
            final_manifest_path: get_project_skills_root(&root)
                .join("lost")
                .join(SKILL_FILE_NAME),
        };
        assert!(matches!(
            accept_pending_skill(&lost).await,
            Err(PendingSkillError::MissingForAccept { .. })
        ));
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn reject_removes_only_staged_copy_and_missing_reject_is_ok() {
        let root = temp_project("reject");
        let manifest = make_skill(&root, "reject", "no").await;
        let [pending] = stage_skill_dirs(&[manifest], &root, &HashSet::new(), "task")
            .await
            .unwrap()
            .try_into()
            .unwrap();
        // Put a live skill at the final location; rejecting the staged copy
        // must not affect it.
        tokio::fs::create_dir_all(pending.final_manifest_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&pending.final_manifest_path, "confirmed")
            .await
            .unwrap();
        reject_pending_skill(&pending).await.unwrap();
        reject_pending_skill(&pending).await.unwrap();
        assert_eq!(
            tokio::fs::read_to_string(&pending.final_manifest_path)
                .await
                .unwrap(),
            "confirmed"
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }
}
