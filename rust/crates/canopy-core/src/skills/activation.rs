//! Path-based conditional skill activation, ported from skill-activation.ts.

use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use globset::{GlobBuilder, GlobMatcher};

use super::types::SkillConfig;

pub type InvalidPatternHandler<'a> = dyn Fn(&SkillConfig, &str, &str) + Send + Sync + 'a;
pub const MAX_SKILL_ACTIVATION_PATTERN_CODE_UNITS: usize = 65_536;

#[derive(Clone, Debug)]
struct CompiledSkill {
    name: String,
    matchers: Vec<GlobMatcher>,
}

pub struct SkillActivationRegistry {
    compiled: Vec<CompiledSkill>,
    activated: Mutex<indexmap::IndexSet<String>>,
    project_root: PathBuf,
}

pub fn split_conditional_skills(skills: &[SkillConfig]) -> (Vec<SkillConfig>, Vec<SkillConfig>) {
    let mut unconditional = Vec::new();
    let mut conditional = Vec::new();
    for skill in skills {
        if skill.paths.as_ref().is_some_and(|paths| !paths.is_empty()) {
            conditional.push(skill.clone());
        } else {
            unconditional.push(skill.clone());
        }
    }
    (unconditional, conditional)
}

impl SkillActivationRegistry {
    pub fn new(
        conditional_skills: &[SkillConfig],
        project_root: impl Into<PathBuf>,
        on_invalid_pattern: Option<&InvalidPatternHandler<'_>>,
    ) -> Self {
        let compiled = conditional_skills
            .iter()
            .map(|skill| {
                let mut matchers = Vec::new();
                for pattern in skill.paths.as_deref().unwrap_or_default() {
                    let matcher = if pattern.encode_utf16().count()
                        > MAX_SKILL_ACTIVATION_PATTERN_CODE_UNITS
                    {
                        Err(format!(
                            "glob pattern is longer than the {MAX_SKILL_ACTIVATION_PATTERN_CODE_UNITS} UTF-16 code unit limit"
                        ))
                    } else {
                        GlobBuilder::new(pattern)
                            .literal_separator(true)
                            .backslash_escape(false)
                            .build()
                            .map(|glob| glob.compile_matcher())
                            .map_err(|error| error.to_string())
                    };
                    match matcher {
                        Ok(matcher) => matchers.push(matcher),
                        Err(error) => {
                            if let Some(handler) = on_invalid_pattern {
                                handler(skill, pattern, &error);
                            }
                        }
                    }
                }
                CompiledSkill {
                    name: skill.name.clone(),
                    matchers,
                }
            })
            .collect();
        Self {
            compiled,
            activated: Mutex::new(indexmap::IndexSet::new()),
            project_root: project_root.into(),
        }
    }

    pub async fn match_and_consume(&self, file_path: impl AsRef<Path>) -> Vec<String> {
        if self.compiled.is_empty() {
            return Vec::new();
        }
        let relative_paths =
            resolve_symlink_aware_relative_paths(file_path.as_ref(), &self.project_root).await;
        if relative_paths.is_empty() {
            return Vec::new();
        }
        let mut activated = lock(&self.activated);
        let mut newly_activated = Vec::new();
        for compiled in &self.compiled {
            if activated.contains(&compiled.name) {
                continue;
            }
            if relative_paths.iter().any(|relative| {
                compiled
                    .matchers
                    .iter()
                    .any(|matcher| matcher.is_match(relative))
            }) {
                activated.insert(compiled.name.clone());
                newly_activated.push(compiled.name.clone());
            }
        }
        newly_activated
    }

    pub fn is_activated(&self, name: &str) -> bool {
        lock(&self.activated).contains(name)
    }

    pub fn get_activated_names(&self) -> indexmap::IndexSet<String> {
        lock(&self.activated).clone()
    }

    pub fn total_count(&self) -> usize {
        self.compiled.len()
    }

    pub fn activated_count(&self) -> usize {
        lock(&self.activated).len()
    }
}

/// Return a normalized project-relative path, or None when it escapes the root.
pub fn resolve_project_relative_path(
    file_path: impl AsRef<Path>,
    project_root: impl AsRef<Path>,
) -> Option<String> {
    let root = absolute_lexical(project_root.as_ref(), None)?;
    let file = absolute_lexical(file_path.as_ref(), Some(&root))?;
    let relative = file.strip_prefix(&root).ok()?;
    if relative.is_absolute()
        || relative
            .components()
            .next()
            .is_some_and(|component| component == Component::ParentDir)
    {
        return None;
    }
    Some(path_to_glob_string(relative))
}

/// Check the input path and its realpath-relative form. A failed realpath falls
/// back to the original relative form; a lexically external path is rejected.
pub async fn resolve_symlink_aware_relative_paths(
    file_path: impl AsRef<Path>,
    project_root: impl AsRef<Path>,
) -> Vec<String> {
    let file_path = file_path.as_ref();
    let project_root = project_root.as_ref();
    let Some(absolute_root) = absolute_lexical(project_root, None) else {
        return Vec::new();
    };
    let Some(absolute_file) = absolute_lexical(file_path, Some(&absolute_root)) else {
        return Vec::new();
    };
    let Some(original) = resolve_project_relative_path(&absolute_file, &absolute_root) else {
        return Vec::new();
    };
    let mut paths = vec![original.clone()];
    let Ok(real_file) = tokio::fs::canonicalize(&absolute_file).await else {
        return paths;
    };
    if real_file == absolute_file {
        return paths;
    }
    let Ok(real_root) = tokio::fs::canonicalize(&absolute_root).await else {
        return paths;
    };
    if let Some(real_relative) = resolve_project_relative_path(&real_file, &real_root)
        && real_relative != original
    {
        paths.push(real_relative);
    }
    paths
}

fn absolute_lexical(path: &Path, base: Option<&Path>) -> Option<PathBuf> {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else if let Some(base) = base {
        base.join(path)
    } else {
        std::env::current_dir().ok()?.join(path)
    };
    let candidate = if candidate.is_absolute() {
        candidate
    } else {
        std::env::current_dir().ok()?.join(candidate)
    };
    let mut normalized = PathBuf::new();
    for component in candidate.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if normalized.file_name().is_some() {
                    normalized.pop();
                } else if !normalized.has_root() {
                    normalized.push(component.as_os_str());
                }
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    Some(normalized)
}

fn path_to_glob_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::skills::types::SkillLevel;

    fn make_skill(name: &str, paths: Option<Vec<&str>>) -> SkillConfig {
        SkillConfig {
            name: name.to_owned(),
            description: "test skill".to_owned(),
            allowed_tools: None,
            hooks: None,
            model: None,
            level: SkillLevel::Project,
            file_path: PathBuf::from(format!("/project/.canopy/skills/{name}/SKILL.md")),
            skill_root: None,
            body: String::new(),
            extension_name: None,
            argument_hint: None,
            when_to_use: None,
            disable_model_invocation: None,
            user_invocable: None,
            paths: paths.map(|values| values.into_iter().map(str::to_owned).collect()),
            priority: None,
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "canopy-skill-activation-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn split_preserves_order_and_empty_paths_are_unconditional() {
        let skills = [
            make_skill("always", None),
            make_skill("conditional", Some(vec!["src/**/*.rs"])),
            make_skill("empty", Some(Vec::new())),
        ];
        let (unconditional, conditional) = split_conditional_skills(&skills);
        assert_eq!(
            unconditional
                .iter()
                .map(|skill| skill.name.as_str())
                .collect::<Vec<_>>(),
            ["always", "empty"]
        );
        assert_eq!(
            conditional
                .iter()
                .map(|skill| skill.name.as_str())
                .collect::<Vec<_>>(),
            ["conditional"]
        );
    }

    #[tokio::test]
    async fn activations_are_returned_once_and_remain_active() {
        let registry = SkillActivationRegistry::new(
            &[
                make_skill("rust", Some(vec!["src/**/*.rs"])),
                make_skill("tests", Some(vec!["tests/**/*.rs"])),
            ],
            "/project",
            None,
        );
        assert_eq!(registry.total_count(), 2);
        assert_eq!(
            registry.match_and_consume("/project/src/lib.rs").await,
            ["rust"]
        );
        assert_eq!(
            registry.match_and_consume("src/other.rs").await,
            Vec::<String>::new()
        );
        assert!(registry.is_activated("rust"));
        assert!(!registry.is_activated("tests"));
        assert_eq!(registry.activated_count(), 1);
        assert_eq!(
            registry
                .get_activated_names()
                .into_iter()
                .collect::<Vec<_>>(),
            ["rust"]
        );
    }

    #[tokio::test]
    async fn broad_globs_include_dotfiles() {
        let registry = SkillActivationRegistry::new(
            &[make_skill("dotfiles", Some(vec!["**/*.js"]))],
            "/project",
            None,
        );
        assert_eq!(
            registry.match_and_consume("/project/.eslintrc.js").await,
            ["dotfiles"]
        );
    }

    #[tokio::test]
    async fn invalid_patterns_call_handler_and_leave_valid_patterns_usable() {
        let errors = Arc::new(Mutex::new(Vec::<(String, String, String)>::new()));
        let captured = errors.clone();
        let handler = move |skill: &SkillConfig, pattern: &str, error: &str| {
            captured.lock().unwrap().push((
                skill.name.clone(),
                pattern.to_owned(),
                error.to_owned(),
            ));
        };
        let oversized = "x".repeat(MAX_SKILL_ACTIVATION_PATTERN_CODE_UNITS + 1);
        let registry = SkillActivationRegistry::new(
            &[make_skill(
                "mixed",
                Some(vec!["[unterminated", &oversized, "src/**/*.ts"]),
            )],
            "/project",
            Some(&handler),
        );
        assert_eq!(registry.total_count(), 1);
        assert_eq!(
            registry.match_and_consume("/project/src/app.ts").await,
            ["mixed"]
        );
        let errors = errors.lock().unwrap();
        assert_eq!(errors.len(), 2);
        assert!(errors.iter().all(|(name, _, _)| name == "mixed"));
        assert!(errors[1].2.contains("65536"));
    }

    #[test]
    fn relative_paths_normalize_and_reject_parent_escape() {
        assert_eq!(
            resolve_project_relative_path("/project/src/../src/lib.rs", "/project"),
            Some("src/lib.rs".to_owned())
        );
        assert_eq!(
            resolve_project_relative_path("/outside/lib.rs", "/project"),
            None
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_file_matches_original_and_real_relative_paths() {
        use std::os::unix::fs::symlink;

        let temp = temp_root("symlink");
        let project = temp.join("project");
        let source = project.join("src");
        tokio::fs::create_dir_all(&source).await.unwrap();
        tokio::fs::write(source.join("lib.rs"), "test")
            .await
            .unwrap();
        symlink(&source, project.join("linked-src")).unwrap();

        assert_eq!(
            resolve_symlink_aware_relative_paths(project.join("linked-src/lib.rs"), &project).await,
            ["linked-src/lib.rs", "src/lib.rs"]
        );
        let registry = SkillActivationRegistry::new(
            &[make_skill("rust", Some(vec!["src/**/*.rs"]))],
            &project,
            None,
        );
        assert_eq!(
            registry
                .match_and_consume(project.join("linked-src/lib.rs"))
                .await,
            ["rust"]
        );
        tokio::fs::remove_dir_all(temp).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_project_root_resolves_consistently() {
        use std::os::unix::fs::symlink;

        let temp = temp_root("root-link");
        let real_project = temp.join("real-project");
        let source = real_project.join("src");
        tokio::fs::create_dir_all(&source).await.unwrap();
        tokio::fs::write(source.join("main.rs"), "test")
            .await
            .unwrap();
        let linked_project = temp.join("linked-project");
        symlink(&real_project, &linked_project).unwrap();

        assert_eq!(
            resolve_symlink_aware_relative_paths(
                linked_project.join("src/main.rs"),
                &linked_project
            )
            .await,
            ["src/main.rs"]
        );
        let registry = SkillActivationRegistry::new(
            &[make_skill("rust", Some(vec!["src/**/*.rs"]))],
            &linked_project,
            None,
        );
        assert_eq!(
            registry
                .match_and_consume(linked_project.join("src/main.rs"))
                .await,
            ["rust"]
        );
        tokio::fs::remove_dir_all(temp).await.unwrap();
    }
}
