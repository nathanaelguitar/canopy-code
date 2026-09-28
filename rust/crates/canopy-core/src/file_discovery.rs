use std::collections::HashMap;
use std::fs;
use std::path::{Component, Path, PathBuf};

use ignore::Match;
use ignore::gitignore::{Gitignore, GitignoreBuilder};

const CANOPY_IGNORE_FILE: &str = ".canopyignore";
const DEFAULT_CUSTOM_IGNORE_FILES: [&str; 2] = [".agentignore", ".aiignore"];

#[derive(Clone, Debug)]
pub struct FileFilteringOptions {
    pub respect_git_ignore: bool,
    pub respect_canopy_ignore: bool,
}

impl Default for FileFilteringOptions {
    fn default() -> Self {
        Self {
            respect_git_ignore: true,
            respect_canopy_ignore: true,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileFilterReport {
    pub filtered_paths: Vec<String>,
    pub git_ignored_count: usize,
    pub canopy_ignored_count: usize,
}

struct CanopyIgnoreSource {
    file_name: String,
    matcher: Gitignore,
}

type GitignoreMatchers = Vec<(PathBuf, Gitignore)>;

/// Applies the repository's `.gitignore`, `.canopyignore`, and configured
/// agent-ignore rules to workspace-relative paths.
pub struct FileDiscoveryService {
    project_root: PathBuf,
    lexical_project_root: PathBuf,
    git_ignore_enabled: bool,
    canopy_sources: Vec<CanopyIgnoreSource>,
    canopy_patterns: Vec<String>,
    canopy_ignore_file_names: Vec<String>,
}

impl FileDiscoveryService {
    pub fn new(
        project_root: impl AsRef<Path>,
        custom_ignore_files: Option<&[String]>,
    ) -> Result<Self, String> {
        let requested_root = project_root.as_ref();
        let lexical_project_root = if requested_root.is_absolute() {
            lexical_normalize(requested_root)
        } else {
            let current_dir = std::env::current_dir()
                .map_err(|error| format!("could not resolve file discovery root: {error}"))?;
            lexical_normalize(&current_dir.join(requested_root))
        };
        let project_root = fs::canonicalize(requested_root)
            .map_err(|error| format!("could not resolve file discovery root: {error}"))?;
        if !project_root.is_dir() {
            return Err("file discovery root is not a directory".to_owned());
        }

        let custom_ignore_files = normalize_custom_ignore_file_names(custom_ignore_files);
        let mut canopy_ignore_file_names = vec![CANOPY_IGNORE_FILE.to_owned()];
        canopy_ignore_file_names.extend(custom_ignore_files);
        let mut canopy_sources = Vec::new();
        let mut canopy_patterns = Vec::new();

        for file_name in &canopy_ignore_file_names {
            let source_path = project_root.join(file_name);
            let Some(patterns) = read_ignore_patterns(&source_path) else {
                continue;
            };
            if patterns.is_empty() {
                continue;
            }

            let mut builder = GitignoreBuilder::new(&project_root);
            for pattern in &patterns {
                builder
                    .add_line(None, pattern)
                    .map_err(|error| format!("invalid pattern in {file_name}: {error}"))?;
            }
            let matcher = builder
                .build()
                .map_err(|error| format!("could not compile {file_name}: {error}"))?;
            canopy_patterns.extend(patterns);
            canopy_sources.push(CanopyIgnoreSource {
                file_name: file_name.clone(),
                matcher,
            });
        }

        Ok(Self {
            git_ignore_enabled: has_git_repository(&project_root),
            project_root,
            lexical_project_root,
            canopy_sources,
            canopy_patterns,
            canopy_ignore_file_names,
        })
    }

    pub fn filter_files(
        &self,
        file_paths: &[String],
        options: &FileFilteringOptions,
    ) -> Result<Vec<String>, String> {
        Ok(self
            .filter_files_with_report(file_paths, options)?
            .filtered_paths)
    }

    pub fn filter_files_with_report(
        &self,
        file_paths: &[String],
        options: &FileFilteringOptions,
    ) -> Result<FileFilterReport, String> {
        let mut report = FileFilterReport::default();
        let mut git_matchers = HashMap::<PathBuf, GitignoreMatchers>::new();

        for file_path in file_paths {
            if options.respect_git_ignore
                && self.git_ignore_enabled
                && self.should_git_ignore_file_with_cache(file_path, &mut git_matchers)?
            {
                report.git_ignored_count += 1;
                continue;
            }
            if options.respect_canopy_ignore && self.should_canopy_ignore_file(file_path) {
                report.canopy_ignored_count += 1;
                continue;
            }
            report.filtered_paths.push(file_path.clone());
        }
        Ok(report)
    }

    pub fn should_ignore_file(
        &self,
        file_path: &str,
        options: &FileFilteringOptions,
    ) -> Result<bool, String> {
        let mut git_matchers = HashMap::<PathBuf, GitignoreMatchers>::new();
        if options.respect_git_ignore
            && self.git_ignore_enabled
            && self.should_git_ignore_file_with_cache(file_path, &mut git_matchers)?
        {
            return Ok(true);
        }
        Ok(options.respect_canopy_ignore && self.should_canopy_ignore_file(file_path))
    }

    pub fn should_git_ignore_file(&self, file_path: &str) -> Result<bool, String> {
        let mut git_matchers = HashMap::<PathBuf, GitignoreMatchers>::new();
        self.should_git_ignore_file_with_cache(file_path, &mut git_matchers)
    }

    pub fn should_canopy_ignore_file(&self, file_path: &str) -> bool {
        let Some((relative_path, is_directory)) = self.normalized_canopy_relative_path(file_path)
        else {
            return false;
        };
        self.canopy_sources
            .iter()
            .any(|source| canopy_match_ignores(&source.matcher, &relative_path, is_directory))
    }

    pub fn get_canopy_ignore_patterns(&self) -> &[String] {
        &self.canopy_patterns
    }

    pub fn get_canopy_ignore_file_names(&self) -> &[String] {
        &self.canopy_ignore_file_names
    }

    pub fn get_canopy_ignore_file_display_for_path(&self, file_path: &str) -> Option<&str> {
        let (relative_path, is_directory) = self.normalized_canopy_relative_path(file_path)?;
        self.canopy_sources
            .iter()
            .find(|source| canopy_match_ignores(&source.matcher, &relative_path, is_directory))
            .map(|source| source.file_name.as_str())
    }

    pub fn get_canopy_ignore_file_names_display(&self) -> String {
        self.canopy_ignore_file_names.join(", ")
    }

    fn should_git_ignore_file_with_cache(
        &self,
        file_path: &str,
        matchers: &mut HashMap<PathBuf, GitignoreMatchers>,
    ) -> Result<bool, String> {
        let Some((relative_path, is_directory)) = self.normalized_relative_path(file_path) else {
            return Ok(false);
        };
        let Some(parent) = self
            .project_root
            .join(&relative_path)
            .parent()
            .map(Path::to_path_buf)
        else {
            return Ok(false);
        };
        if !matchers.contains_key(&parent) {
            let matcher = self.gitignore_for_directory(&parent)?;
            matchers.insert(parent.clone(), matcher);
        }
        let absolute_path = self.project_root.join(relative_path);
        let mut ignored = false;
        for (root, matcher) in &matchers[&parent] {
            if !absolute_path.starts_with(root) {
                continue;
            }
            match matcher.matched_path_or_any_parents(&absolute_path, is_directory) {
                Match::None => {}
                Match::Ignore(_) => ignored = true,
                Match::Whitelist(_) => ignored = false,
            }
        }
        Ok(ignored)
    }

    fn gitignore_for_directory(&self, leaf_dir: &Path) -> Result<GitignoreMatchers, String> {
        let Some(git_root) = find_git_repository_root(&self.project_root) else {
            return Ok(Vec::new());
        };
        let relative_leaf = leaf_dir
            .strip_prefix(&git_root)
            .map_err(|error| format!("could not relativize ignore path: {error}"))?;

        let mut matchers = Vec::new();
        let mut repository_builder = GitignoreBuilder::new(&git_root);
        repository_builder
            .add_line(None, ".git")
            .map_err(|error| format!("could not add the .git ignore rule: {error}"))?;
        let exclude_file = git_root.join(".git").join("info").join("exclude");
        if let Some(patterns) = read_ignore_patterns(&exclude_file) {
            for pattern in patterns {
                repository_builder
                    .add_line(Some(exclude_file.clone()), &pattern)
                    .map_err(|error| format!("invalid pattern in .git/info/exclude: {error}"))?;
            }
        }
        matchers.push((
            git_root.clone(),
            repository_builder
                .build()
                .map_err(|error| format!("could not compile git ignore rules: {error}"))?,
        ));

        let mut directories = vec![git_root.clone()];
        let mut current_dir = git_root.clone();
        for component in relative_leaf.components() {
            if let Component::Normal(part) = component {
                current_dir.push(part);
                directories.push(current_dir.clone());
            }
        }

        for directory in directories {
            if directory != git_root && path_is_ignored(&matchers, &directory, true) {
                break;
            }
            let ignore_file = directory.join(".gitignore");
            let Some(patterns) = read_ignore_patterns(&ignore_file) else {
                continue;
            };
            if patterns.is_empty() {
                continue;
            }
            let mut builder = GitignoreBuilder::new(&directory);
            for pattern in patterns {
                builder
                    .add_line(Some(ignore_file.clone()), &pattern)
                    .map_err(|error| {
                        format!("invalid pattern in {}: {error}", ignore_file.display())
                    })?;
            }
            let matcher = builder
                .build()
                .map_err(|error| format!("could not compile {}: {error}", ignore_file.display()))?;
            matchers.push((directory, matcher));
        }

        Ok(matchers)
    }

    fn normalized_relative_path(&self, file_path: &str) -> Option<(PathBuf, bool)> {
        if file_path.is_empty() || file_path.contains('\0') {
            return None;
        }
        let is_directory = file_path.ends_with('/');
        let requested = Path::new(file_path);
        let absolute = if requested.is_absolute() {
            lexical_normalize(requested)
        } else {
            lexical_normalize(&self.project_root.join(requested))
        };
        let relative = absolute.strip_prefix(&self.project_root).ok()?;
        if relative.as_os_str().is_empty() {
            return None;
        }

        let mut normalized = PathBuf::new();
        for component in relative.components() {
            match component {
                Component::Normal(part) => normalized.push(part),
                Component::CurDir => {}
                _ => return None,
            }
        }
        Some((normalized, is_directory))
    }

    fn normalized_canopy_relative_path(&self, file_path: &str) -> Option<(PathBuf, bool)> {
        // The source rejects root-relative backslash paths before resolving.
        // On POSIX a backslash is otherwise a literal byte, but the source
        // converts every backslash in the resulting relative string to '/'.
        if file_path.starts_with('\\') || file_path == "/" || file_path.contains('\0') {
            return None;
        }
        if file_path.is_empty() {
            return None;
        }
        let is_directory = file_path.ends_with('/');
        let requested = Path::new(file_path);
        let absolute = if requested.is_absolute() {
            lexical_normalize(requested)
        } else {
            lexical_normalize(&self.lexical_project_root.join(requested))
        };
        let relative = absolute
            .strip_prefix(&self.lexical_project_root)
            .or_else(|_| absolute.strip_prefix(&self.project_root))
            .ok()?;
        if relative.as_os_str().is_empty() {
            return None;
        }
        let mut relative_path = PathBuf::new();
        for component in relative.components() {
            match component {
                Component::Normal(part) => relative_path.push(part),
                Component::CurDir => {}
                _ => return None,
            }
        }
        let normalized = relative_path.to_string_lossy().replace('\\', "/");
        let normalized = lexical_normalize(Path::new(&normalized));
        if normalized.as_os_str().is_empty()
            || normalized.is_absolute()
            || normalized
                .components()
                .any(|component| matches!(component, Component::ParentDir))
        {
            return None;
        }
        Some((normalized, is_directory))
    }
}

/// Reproduce `ignore().ignores(path)`: an ignored parent directory keeps its
/// descendants ignored, even when a later negation names one of those files.
/// Walking parents before the candidate matters because
/// `Gitignore::matched_path_or_any_parents` gives an exact child whitelist
/// precedence over an ignored parent, which the JavaScript `ignore` package
/// does not do.
fn canopy_match_ignores(matcher: &Gitignore, relative_path: &Path, is_directory: bool) -> bool {
    let components: Vec<_> = relative_path.components().collect();
    let mut current = PathBuf::new();
    for (index, component) in components.iter().enumerate() {
        current.push(component.as_os_str());
        let is_candidate = index + 1 == components.len();
        let is_dir = !is_candidate || is_directory;
        match matcher.matched(&current, is_dir) {
            Match::Ignore(_) => return true,
            Match::None | Match::Whitelist(_) if is_candidate => return false,
            Match::None | Match::Whitelist(_) => {}
        }
    }
    false
}

fn normalize_custom_ignore_file_names(custom: Option<&[String]>) -> Vec<String> {
    let mut result = Vec::new();
    for candidate in custom
        .unwrap_or(&[])
        .iter()
        .map(|name| trim_ecmascript_whitespace(name).replace('\\', "/"))
    {
        if candidate.is_empty()
            || candidate.starts_with('/')
            || Path::new(&candidate).is_absolute()
            || candidate.contains('\0')
            || candidate == CANOPY_IGNORE_FILE
            || candidate.split('/').any(|component| component == "..")
            || result.contains(&candidate)
        {
            continue;
        }
        result.push(candidate);
    }
    if custom.is_none() {
        DEFAULT_CUSTOM_IGNORE_FILES
            .iter()
            .map(|name| (*name).to_owned())
            .collect()
    } else {
        result
    }
}

fn read_ignore_patterns(path: &Path) -> Option<Vec<String>> {
    let content = fs::read(path).ok()?;
    let content = String::from_utf8_lossy(&content);
    Some(
        content
            .split('\n')
            .map(|line| line.strip_suffix('\r').unwrap_or(line))
            .filter(|line| !trim_ecmascript_whitespace(line).is_empty() && !line.starts_with('#'))
            .map(str::to_owned)
            .collect(),
    )
}

fn trim_ecmascript_whitespace(value: &str) -> &str {
    value.trim_matches(is_ecmascript_whitespace)
}

fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000A}'
            | '\u{000B}'
            | '\u{000C}'
            | '\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200A}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202F}'
                | '\u{205F}'
                | '\u{3000}'
                | '\u{FEFF}'
    )
}

fn has_git_repository(directory: &Path) -> bool {
    find_git_repository_root(directory).is_some()
}

fn find_git_repository_root(directory: &Path) -> Option<PathBuf> {
    let mut current = Some(directory);
    while let Some(path) = current {
        if path.join(".git").exists() {
            return Some(path.to_path_buf());
        }
        current = path.parent();
    }
    None
}

fn path_is_ignored(matchers: &GitignoreMatchers, path: &Path, is_directory: bool) -> bool {
    let mut ignored = false;
    for (root, matcher) in matchers {
        if !path.starts_with(root) {
            continue;
        }
        match matcher.matched_path_or_any_parents(path, is_directory) {
            Match::None => {}
            Match::Ignore(_) => ignored = true,
            Match::Whitelist(_) => ignored = false,
        }
    }
    ignored
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push(component.as_os_str());
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    struct TempWorkspace(PathBuf);

    impl TempWorkspace {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("canopy-file-discovery-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write(root: &Path, name: &str, content: &str) {
        let path = root.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    #[test]
    fn reports_git_and_canopy_counts_in_source_precedence_order() {
        let workspace = TempWorkspace::new();
        fs::write(workspace.0.join(".git"), "worktree marker").unwrap();
        write(&workspace.0, ".gitignore", "*.log\n");
        write(&workspace.0, ".canopyignore", "private.txt\n*.log\n");
        let service = FileDiscoveryService::new(&workspace.0, None).unwrap();
        assert_eq!(
            service.get_canopy_ignore_file_names(),
            [".canopyignore", ".agentignore", ".aiignore"]
        );

        let report = service
            .filter_files_with_report(
                &[
                    "visible.txt".into(),
                    "trace.log".into(),
                    "private.txt".into(),
                ],
                &FileFilteringOptions::default(),
            )
            .unwrap();

        assert_eq!(report.filtered_paths, ["visible.txt"]);
        assert_eq!(report.git_ignored_count, 1);
        assert_eq!(report.canopy_ignored_count, 1);
    }

    #[test]
    fn applies_nested_gitignore_rules_relative_to_their_source_directory() {
        let workspace = TempWorkspace::new();
        fs::write(workspace.0.join(".git"), "worktree marker").unwrap();
        write(
            &workspace.0,
            "packages/.gitignore",
            "/only-here.txt\n*.tmp\n",
        );
        let service = FileDiscoveryService::new(&workspace.0, None).unwrap();

        assert!(
            service
                .should_git_ignore_file("packages/only-here.txt")
                .unwrap()
        );
        assert!(
            service
                .should_git_ignore_file("packages/sub/keep.tmp")
                .unwrap()
        );
        assert!(
            !service
                .should_git_ignore_file("other/only-here.txt")
                .unwrap()
        );
        assert!(!service.should_git_ignore_file("other/keep.tmp").unwrap());
    }

    #[test]
    fn does_not_load_nested_ignore_rules_beneath_an_ignored_directory() {
        let workspace = TempWorkspace::new();
        fs::write(workspace.0.join(".git"), "worktree marker").unwrap();
        write(&workspace.0, ".gitignore", "ignored/\n");
        write(&workspace.0, "ignored/.gitignore", "!visible.txt\n");
        let service = FileDiscoveryService::new(&workspace.0, None).unwrap();

        assert!(
            service
                .should_git_ignore_file("ignored/visible.txt")
                .unwrap()
        );
    }

    #[test]
    fn supports_configured_custom_ignore_files_and_source_lookup() {
        let workspace = TempWorkspace::new();
        write(&workspace.0, ".cursorignore", "secret.txt\n");
        let custom = vec![".cursorignore".to_owned(), "../outside".to_owned()];
        let service = FileDiscoveryService::new(&workspace.0, Some(&custom)).unwrap();

        assert!(service.should_canopy_ignore_file("secret.txt"));
        assert_eq!(
            service.get_canopy_ignore_file_display_for_path("secret.txt"),
            Some(".cursorignore")
        );
        assert_eq!(
            service.get_canopy_ignore_file_names_display(),
            ".canopyignore, .cursorignore"
        );
        assert_eq!(
            service.get_canopy_ignore_file_names(),
            [".canopyignore", ".cursorignore"]
        );
    }

    #[test]
    fn rejects_paths_that_escape_the_file_discovery_root() {
        let workspace = TempWorkspace::new();
        write(&workspace.0, ".canopyignore", "secret.txt\n");
        let service = FileDiscoveryService::new(&workspace.0, None).unwrap();

        assert!(!service.should_canopy_ignore_file("../secret.txt"));
        assert!(!service.should_canopy_ignore_file("/tmp/secret.txt"));
    }

    #[test]
    fn canopy_directory_patterns_cover_descendants_and_report_the_source() {
        let workspace = TempWorkspace::new();
        write(&workspace.0, ".canopyignore", "ignored_dir/\n");
        let service = FileDiscoveryService::new(&workspace.0, Some(&[])).unwrap();

        assert!(service.should_canopy_ignore_file("ignored_dir/"));
        assert!(service.should_canopy_ignore_file("ignored_dir/file.txt"));
        assert_eq!(
            service.get_canopy_ignore_file_display_for_path("ignored_dir/file.txt"),
            Some(".canopyignore")
        );
    }

    #[test]
    fn canopy_negations_cannot_reinclude_children_of_ignored_directories() {
        let workspace = TempWorkspace::new();
        write(
            &workspace.0,
            ".canopyignore",
            "ignored/\n!ignored/keep.txt\nother/**\n!other/keep.txt\n",
        );
        let service = FileDiscoveryService::new(&workspace.0, Some(&[])).unwrap();

        // The JavaScript `ignore` package refuses to re-include a file below
        // an ignored parent, but a `**` pattern that does not ignore the parent
        // itself can be negated for a file at that level.
        assert!(service.should_canopy_ignore_file("ignored/keep.txt"));
        assert!(!service.should_canopy_ignore_file("other/keep.txt"));
        assert!(service.should_canopy_ignore_file("other/deep/file.txt"));
    }

    #[test]
    fn custom_ignore_negations_do_not_clear_matches_from_another_source() {
        let workspace = TempWorkspace::new();
        write(&workspace.0, ".canopyignore", "secrets/**\n");
        write(&workspace.0, ".agentignore", "!secrets/**\n");
        let service = FileDiscoveryService::new(&workspace.0, None).unwrap();

        assert!(service.should_canopy_ignore_file("secrets/token.txt"));
        assert_eq!(
            service.get_canopy_ignore_file_display_for_path("secrets/token.txt"),
            Some(".canopyignore")
        );
    }

    #[test]
    fn canopy_paths_use_forward_slashes_and_reject_rooted_backslashes() {
        let workspace = TempWorkspace::new();
        write(
            &workspace.0,
            ".canopyignore",
            "nested/file.txt\n..secret.log\n",
        );
        let service = FileDiscoveryService::new(&workspace.0, Some(&[])).unwrap();

        // On POSIX, the source resolves this as a literal backslash in the
        // path and then normalizes that relative separator to '/'.
        assert!(service.should_canopy_ignore_file("nested\\file.txt"));
        assert!(service.should_canopy_ignore_file("..secret.log"));
        assert!(service.should_canopy_ignore_file("nested\\..\\..secret.log"));
        assert!(!service.should_canopy_ignore_file("\\nested\\file.txt"));
        let absolute = workspace.0.join("nested/file.txt");
        assert_eq!(
            service
                .normalized_canopy_relative_path(absolute.to_str().unwrap())
                .unwrap(),
            (PathBuf::from("nested/file.txt"), false)
        );
        assert!(service.should_canopy_ignore_file(&absolute.to_string_lossy()));
    }

    #[test]
    fn canopy_patterns_and_custom_names_trim_ecmascript_whitespace() {
        let names = normalize_custom_ignore_file_names(Some(&[
            "\u{feff}.cursorignore\u{feff}".into(),
            " .cursorignore ".into(),
            "\u{0085}.special\u{0085}".into(),
        ]));
        assert_eq!(names, [".cursorignore", "\u{0085}.special\u{0085}"]);

        let workspace = TempWorkspace::new();
        write(
            &workspace.0,
            ".canopyignore",
            "\u{feff}\r\n\u{0085}\r\n  #literal\r\n# comment\r\n  \r\n",
        );
        let service = FileDiscoveryService::new(&workspace.0, Some(&[])).unwrap();
        assert_eq!(
            service.get_canopy_ignore_patterns(),
            ["\u{0085}", "  #literal"]
        );
        assert!(service.should_canopy_ignore_file("  #literal"));
        assert!(!service.should_canopy_ignore_file("#literal"));
    }

    #[cfg(windows)]
    #[test]
    fn rejects_windows_absolute_custom_ignore_file_names() {
        assert!(
            normalize_custom_ignore_file_names(Some(&[
                r"C:\outside.ignore".into(),
                r"\server\share.ignore".into(),
            ]))
            .is_empty()
        );
    }
}
