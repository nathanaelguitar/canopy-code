use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SymlinkTargetFailure {
    NotDirectory,
    Invalid(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SymlinkTargetCheck {
    Valid { real_path: PathBuf },
    Invalid { reason: SymlinkTargetFailure },
}

/// Resolve a skill directory symlink and ensure its target is a directory.
/// Targets outside the containing skills directory are allowed to support
/// user-managed shared skill repositories.
pub fn validate_symlink_target(skill_dir: impl AsRef<Path>) -> SymlinkTargetCheck {
    let real_path = match fs::canonicalize(skill_dir.as_ref()) {
        Ok(path) => path,
        Err(error) => {
            return SymlinkTargetCheck::Invalid {
                reason: SymlinkTargetFailure::Invalid(error.to_string()),
            };
        }
    };
    match fs::metadata(&real_path) {
        Ok(metadata) if metadata.is_dir() => SymlinkTargetCheck::Valid { real_path },
        Ok(_) => SymlinkTargetCheck::Invalid {
            reason: SymlinkTargetFailure::NotDirectory,
        },
        Err(error) => SymlinkTargetCheck::Invalid {
            reason: SymlinkTargetFailure::Invalid(error.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root() -> PathBuf {
        std::env::temp_dir().join(format!(
            "canopy-symlink-scope-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ))
    }

    #[cfg(unix)]
    #[test]
    fn accepts_directory_targets_inside_or_outside_the_skills_root() {
        use std::os::unix::fs::symlink;

        let root = temp_root();
        let base = root.join("skills");
        let outside = root.join("shared");
        fs::create_dir_all(&base).unwrap();
        fs::create_dir_all(&outside).unwrap();
        symlink(&outside, base.join("shared-skill")).unwrap();

        let real_outside = fs::canonicalize(&outside).unwrap();
        assert!(matches!(
            validate_symlink_target(base.join("shared-skill")),
            SymlinkTargetCheck::Valid { ref real_path } if real_path == &real_outside
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn rejects_dangling_links_and_links_to_files() {
        use std::os::unix::fs::symlink;

        let root = temp_root();
        let base = root.join("skills");
        fs::create_dir_all(&base).unwrap();
        let file = root.join("skill.txt");
        fs::write(&file, "not a directory").unwrap();
        symlink(&file, base.join("file-link")).unwrap();
        symlink(root.join("missing"), base.join("dangling-link")).unwrap();

        assert_eq!(
            validate_symlink_target(base.join("file-link")),
            SymlinkTargetCheck::Invalid {
                reason: SymlinkTargetFailure::NotDirectory
            }
        );
        assert!(matches!(
            validate_symlink_target(base.join("dangling-link")),
            SymlinkTargetCheck::Invalid {
                reason: SymlinkTargetFailure::Invalid(_)
            }
        ));
        fs::remove_dir_all(root).unwrap();
    }
}
