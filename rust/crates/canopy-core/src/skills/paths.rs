use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

pub const PROJECT_SKILLS_RELATIVE_DIR: &str = ".canopy/skills";
pub const ARCHIVED_SKILLS_RELATIVE_DIR: &str = ".canopy/archived-skills";
pub const PENDING_SKILLS_RELATIVE_DIR: &str = ".canopy/pending-skills";
pub const SKILL_FILE_NAME: &str = "SKILL.md";

pub fn get_project_skills_root(project_root: impl AsRef<Path>) -> PathBuf {
    project_root.as_ref().join(".canopy").join("skills")
}

pub fn get_archived_skills_root(project_root: impl AsRef<Path>) -> PathBuf {
    project_root
        .as_ref()
        .join(".canopy")
        .join("archived-skills")
}

/// Return the staging root for skills awaiting user confirmation.
///
/// This is deliberately a sibling of `.canopy/skills` so skill discovery,
/// which scans only the project skills root, cannot load unconfirmed skills.
pub fn get_pending_skills_root(project_root: impl AsRef<Path>) -> PathBuf {
    project_root.as_ref().join(".canopy").join("pending-skills")
}

/// Check lexical containment in the project `.canopy/skills` directory.
/// This does not dereference symlinks; use [`assert_real_project_skill_path`]
/// before filesystem operations that can follow them.
pub fn is_project_skill_path(file_path: impl AsRef<Path>, project_root: impl AsRef<Path>) -> bool {
    let file_path = file_path.as_ref();
    let project_root = project_root.as_ref();
    let Ok(skills_root) = absolute_lexical(&get_project_skills_root(project_root)) else {
        return false;
    };
    let candidate = if file_path.is_absolute() {
        file_path.to_path_buf()
    } else {
        project_root.join(file_path)
    };
    let Ok(resolved) = absolute_lexical(&candidate) else {
        return false;
    };
    resolved == skills_root || resolved.starts_with(&skills_root)
}

pub fn assert_project_skill_path(
    target_path: impl AsRef<Path>,
    project_root: impl AsRef<Path>,
) -> Result<(), String> {
    if is_project_skill_path(target_path.as_ref(), project_root.as_ref()) {
        return Ok(());
    }
    Err(format!(
        "Skills writes are restricted to {}. Use the Skills UI to manage user or bundled skills.",
        get_project_skills_root(project_root).display()
    ))
}

/// Enforce lexical containment and reject symlink traversal outside the real
/// project skill root. Missing targets are allowed so this can guard a new
/// file before it is created. A dangling symlink at any checked component is
/// rejected because a future write could otherwise target an arbitrary path.
pub fn assert_real_project_skill_path(
    target_path: impl AsRef<Path>,
    project_root: impl AsRef<Path>,
) -> Result<(), String> {
    let target_path = target_path.as_ref();
    let project_root = project_root.as_ref();
    assert_project_skill_path(target_path, project_root)?;

    let skills_root = absolute_lexical(&get_project_skills_root(project_root))
        .map_err(|error| format!("could not resolve project skills path: {error}"))?;
    let real_skills_root = match fs::canonicalize(&skills_root) {
        Ok(path) => path,
        // Match the TypeScript behavior: if the skill root cannot be resolved,
        // there is no existing root whose symlink chain can be traversed.
        Err(_) => return Ok(()),
    };
    let mut check = if target_path.is_absolute() {
        target_path.to_path_buf()
    } else {
        project_root.join(target_path)
    };
    check = absolute_lexical(&check)
        .map_err(|error| format!("could not resolve skill target path: {error}"))?;

    loop {
        match fs::canonicalize(&check) {
            Ok(real) => {
                if real != real_skills_root && !real.starts_with(&real_skills_root) {
                    return Err(format!(
                        "Skills write blocked: symlink traversal detected — resolved path \"{}\" is outside the project skills directory \"{}\".",
                        real.display(),
                        real_skills_root.display()
                    ));
                }
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match fs::symlink_metadata(&check) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(format!(
                            "Skills write blocked: dangling symlink detected at \"{}\".",
                            check.display()
                        ));
                    }
                    Ok(_) => {}
                    Err(lstat_error) if lstat_error.kind() == io::ErrorKind::NotFound => {}
                    Err(lstat_error) => return Err(lstat_error.to_string()),
                }
                let Some(parent) = check.parent() else {
                    return Ok(());
                };
                if parent == check {
                    return Ok(());
                }
                check = parent.to_path_buf();
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

/// Match the source helper's trim/lowercase/ASCII-name replacement contract.
pub fn sanitize_skill_name(name: &str) -> String {
    let lowered = name.trim_matches(is_javascript_trim_space).to_lowercase();
    let mut result = String::new();
    for unit in lowered.encode_utf16() {
        match unit {
            0x41..=0x5a => result.push(char::from_u32(u32::from(unit) + 32).unwrap()),
            0x61..=0x7a | 0x30..=0x39 | 0x2d => {
                result.push(char::from_u32(u32::from(unit)).unwrap());
            }
            _ => result.push('-'),
        }
    }
    result
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
            other => normalized.push(other.as_os_str()),
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "canopy-skills-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ))
    }

    #[test]
    fn project_skill_paths_use_component_containment_and_expected_roots() {
        let root = temp_root("containment");
        let skill = root.join(".canopy/skills/my-skill/SKILL.md");
        let sibling = root.join(".canopy/skills-evil/SKILL.md");
        assert_eq!(get_project_skills_root(&root), root.join(".canopy/skills"));
        assert_eq!(
            get_archived_skills_root(&root),
            root.join(".canopy/archived-skills")
        );
        assert_eq!(
            get_pending_skills_root(&root),
            root.join(".canopy/pending-skills")
        );
        assert!(is_project_skill_path(skill, &root));
        assert!(!is_project_skill_path(sibling, &root));
        assert!(assert_project_skill_path(".canopy/skills/x/SKILL.md", &root).is_ok());
        assert!(
            assert_project_skill_path(".canopy/skills-evil/x", &root)
                .unwrap_err()
                .contains("Skills writes are restricted to")
        );
    }

    #[test]
    fn skill_names_match_ascii_sanitization_including_non_bmp_code_units() {
        assert_eq!(sanitize_skill_name(" My Skill! "), "my-skill-");
        assert_eq!(sanitize_skill_name("STRAẞE"), "stra-e");
        assert_eq!(sanitize_skill_name("crab 🦀"), "crab---");
    }

    #[cfg(unix)]
    #[test]
    fn real_path_guard_accepts_internal_and_root_symlink_but_rejects_escape_and_dangling() {
        use std::os::unix::fs::symlink;

        let temp = temp_root("realpath");
        let project = temp.join("project");
        let skills = project.join(".canopy/skills");
        let outside = temp.join("outside");
        fs::create_dir_all(&skills).unwrap();
        fs::create_dir_all(&outside).unwrap();

        let internal = skills.join("new-skill/SKILL.md");
        assert!(assert_real_project_skill_path(&internal, &project).is_ok());

        symlink(&outside, skills.join("escape")).unwrap();
        let escape = skills.join("escape/evil.md");
        assert!(
            assert_real_project_skill_path(&escape, &project)
                .unwrap_err()
                .contains("symlink traversal detected")
        );

        symlink(temp.join("missing"), skills.join("dangling")).unwrap();
        let dangling = skills.join("dangling/evil.md");
        assert!(
            assert_real_project_skill_path(&dangling, &project)
                .unwrap_err()
                .contains("dangling symlink detected")
        );

        fs::remove_file(skills.join("escape")).unwrap();
        fs::remove_file(skills.join("dangling")).unwrap();
        let real_skills = project.join(".canopy/real-skills");
        fs::rename(&skills, &real_skills).unwrap();
        symlink(&real_skills, &skills).unwrap();
        assert!(
            assert_real_project_skill_path(skills.join("new-skill/SKILL.md"), &project).is_ok()
        );

        fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn real_path_guard_allows_a_missing_skills_root() {
        let temp = temp_root("missing-root");
        let project = temp.join("project");
        let target = project.join(".canopy/skills/new/SKILL.md");
        assert!(assert_real_project_skill_path(target, &project).is_ok());
    }
}
