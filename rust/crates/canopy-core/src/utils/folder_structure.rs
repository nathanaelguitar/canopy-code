//! Recursive, bounded directory-tree formatting.
//!
//! This mirrors `packages/core/src/utils/getFolderStructure.ts`. It is kept
//! separate from the direct-entry `list_directory` tool because the two have
//! different output, filtering, and item-budget contracts.

use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::{Component, MAIN_SEPARATOR, Path, PathBuf};

use regex::Regex;

use crate::file_discovery::{FileDiscoveryService, FileFilteringOptions};

const DEFAULT_MAX_ITEMS: usize = 20;
const TRUNCATION_INDICATOR: &str = "...";
const DEFAULT_IGNORED_FOLDERS: [&str; 3] = ["node_modules", ".git", "dist"];

/// Options for [`get_folder_structure`].
pub struct FolderStructureOptions<'a> {
    /// Maximum number of files and directories included. The root is not
    /// counted. Defaults to 20.
    pub max_items: usize,
    /// Replacement for the case-sensitive default ignored-folder names.
    pub ignored_folders: Option<&'a HashSet<String>>,
    /// Optional regular expression matched against each file's name.
    pub file_include_pattern: Option<&'a Regex>,
    /// Optional workspace ignore service. Ignored files are omitted and
    /// ignored directories are shown as `...` leaves.
    pub file_service: Option<&'a FileDiscoveryService>,
    /// Ignore-rule switches used when `file_service` is supplied.
    pub file_filtering_options: Option<&'a FileFilteringOptions>,
}

impl<'a> Default for FolderStructureOptions<'a> {
    fn default() -> Self {
        Self {
            max_items: DEFAULT_MAX_ITEMS,
            ignored_folders: None,
            file_include_pattern: None,
            file_service: None,
            file_filtering_options: None,
        }
    }
}

#[derive(Debug)]
struct FolderNode {
    name: String,
    path: PathBuf,
    files: Vec<String>,
    subfolders: Vec<usize>,
    is_ignored: bool,
    has_more_files: bool,
    has_more_subfolders: bool,
}

impl FolderNode {
    fn new(name: String, path: PathBuf) -> Self {
        Self {
            name,
            path,
            files: Vec::new(),
            subfolders: Vec::new(),
            is_ignored: false,
            has_more_files: false,
            has_more_subfolders: false,
        }
    }
}

/// Build a recursive directory tree using a breadth-first scan and format it
/// like the TypeScript folder-context helper. Errors are returned in the
/// source helper's user-facing string format.
pub fn get_folder_structure(
    directory: impl AsRef<Path>,
    options: FolderStructureOptions<'_>,
) -> String {
    let requested_path = directory.as_ref();
    let resolved_path = match resolve_path(requested_path) {
        Ok(path) => path,
        Err(error) => {
            let display = requested_path.display();
            return format!("Error processing directory \"{display}\": {error}");
        }
    };

    let nodes = match read_structure(&resolved_path, &options) {
        Ok(Some(nodes)) => nodes,
        Ok(None) => {
            return format!(
                "Error: Could not read directory \"{}\". Check path and permissions.",
                resolved_path.display()
            );
        }
        Err(error) => {
            return format!(
                "Error processing directory \"{}\": {error}",
                resolved_path.display()
            );
        }
    };

    let mut structure_lines = Vec::new();
    format_node(&nodes, 0, "", true, true, &mut structure_lines);
    format!(
        "Showing up to {} items:\n\n{}{}\n{}",
        options.max_items,
        resolved_path.display(),
        MAIN_SEPARATOR,
        structure_lines.join("\n")
    )
}

fn read_structure(
    root_path: &Path,
    options: &FolderStructureOptions<'_>,
) -> Result<Option<Vec<FolderNode>>, String> {
    let root_name = root_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut nodes = vec![FolderNode::new(root_name, root_path.to_path_buf())];
    let mut queue = VecDeque::from([0usize]);
    let mut processed_paths = HashSet::new();
    let mut current_item_count = 0usize;

    while let Some(node_index) = queue.pop_front() {
        let current_path = nodes[node_index].path.clone();
        if !processed_paths.insert(current_path.clone()) {
            continue;
        }

        if current_item_count >= options.max_items {
            // The folder was already added to its parent, but its contents
            // were never examined. Preserve the source's visible marker.
            nodes[node_index].has_more_subfolders = true;
            continue;
        }

        let entries = match read_sorted_entries(&current_path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if current_path == root_path {
                    return Ok(None);
                }
                continue;
            }
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => continue,
            Err(error) => return Err(error.to_string()),
        };

        let mut files = Vec::new();
        let mut has_more_files = false;
        for entry in &entries {
            if !entry.is_file {
                continue;
            }
            if current_item_count >= options.max_items {
                has_more_files = true;
                break;
            }

            if is_ignored_by_service(&entry.path, false, options)? {
                continue;
            }
            if options
                .file_include_pattern
                .is_some_and(|pattern| !pattern.is_match(&entry.name))
            {
                continue;
            }

            files.push(entry.name.clone());
            current_item_count += 1;
        }
        nodes[node_index].files = files;
        nodes[node_index].has_more_files = has_more_files;

        let mut subfolders = Vec::new();
        let mut has_more_subfolders = false;
        for entry in &entries {
            if !entry.is_directory {
                continue;
            }
            if current_item_count >= options.max_items {
                has_more_subfolders = true;
                break;
            }

            let is_ignored = is_ignored_by_service(&entry.path, true, options)?
                || ignored_folder_name(&entry.name, options.ignored_folders);
            let mut node = FolderNode::new(entry.name.clone(), entry.path.clone());
            node.is_ignored = is_ignored;
            let child_index = nodes.len();
            nodes.push(node);
            subfolders.push(child_index);
            current_item_count += 1;
            if !is_ignored {
                queue.push_back(child_index);
            }
        }
        nodes[node_index].subfolders = subfolders;
        nodes[node_index].has_more_subfolders = has_more_subfolders;
    }

    Ok(Some(nodes))
}

struct DirectoryEntry {
    name: String,
    path: PathBuf,
    is_file: bool,
    is_directory: bool,
}

fn read_sorted_entries(directory: &Path) -> io::Result<Vec<DirectoryEntry>> {
    let mut entries = fs::read_dir(directory)?
        .map(|entry| {
            let entry = entry?;
            let file_type = entry.file_type()?;
            Ok(DirectoryEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                path: entry.path(),
                is_file: file_type.is_file(),
                is_directory: file_type.is_dir(),
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    // Rust has no locale collation in std; scalar ordering is deterministic
    // and matches the source's ASCII ordering for ordinary file names.
    entries.sort_by(|left, right| locale_compare_approx(&left.name, &right.name));
    Ok(entries)
}

fn locale_compare_approx(left: &str, right: &str) -> Ordering {
    left.cmp(right)
}

fn is_ignored_by_service(
    path: &Path,
    is_directory: bool,
    options: &FolderStructureOptions<'_>,
) -> Result<bool, String> {
    let Some(service) = options.file_service else {
        return Ok(false);
    };
    let filtering = options.file_filtering_options.cloned().unwrap_or_default();
    let mut path_string = path.to_string_lossy().replace('\\', "/");
    if is_directory && !path_string.ends_with('/') {
        path_string.push('/');
    }
    service
        .should_ignore_file(&path_string, &filtering)
        .map_err(|error| error.to_string())
}

fn ignored_folder_name(name: &str, custom_ignored: Option<&HashSet<String>>) -> bool {
    match custom_ignored {
        Some(names) => names.contains(name),
        None => DEFAULT_IGNORED_FOLDERS.contains(&name),
    }
}

fn format_node(
    nodes: &[FolderNode],
    node_index: usize,
    current_indent: &str,
    is_last_child: bool,
    is_root: bool,
    builder: &mut Vec<String>,
) {
    let node = &nodes[node_index];
    let connector = if is_last_child {
        "└───"
    } else {
        "├───"
    };
    if !is_root || node.is_ignored {
        builder.push(format!(
            "{current_indent}{connector}{}{MAIN_SEPARATOR}{}",
            node.name,
            if node.is_ignored {
                TRUNCATION_INDICATOR
            } else {
                ""
            }
        ));
    }

    let indent_for_children = if is_root {
        String::new()
    } else {
        format!(
            "{current_indent}{}",
            if is_last_child { "    " } else { "│   " }
        )
    };

    let file_count = node.files.len();
    for (index, name) in node.files.iter().enumerate() {
        let is_last_file =
            index + 1 == file_count && node.subfolders.is_empty() && !node.has_more_subfolders;
        let file_connector = if is_last_file {
            "└───"
        } else {
            "├───"
        };
        builder.push(format!("{indent_for_children}{file_connector}{name}"));
    }
    if node.has_more_files {
        let is_last_indicator = node.subfolders.is_empty() && !node.has_more_subfolders;
        let file_connector = if is_last_indicator {
            "└───"
        } else {
            "├───"
        };
        builder.push(format!(
            "{indent_for_children}{file_connector}{TRUNCATION_INDICATOR}"
        ));
    }

    let subfolder_count = node.subfolders.len();
    for (index, child_index) in node.subfolders.iter().enumerate() {
        let is_last_subfolder = index + 1 == subfolder_count && !node.has_more_subfolders;
        format_node(
            nodes,
            *child_index,
            &indent_for_children,
            is_last_subfolder,
            false,
            builder,
        );
    }
    if node.has_more_subfolders {
        builder.push(format!("{indent_for_children}└───{TRUNCATION_INDICATOR}"));
    }
}

fn resolve_path(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    Ok(lexical_normalize(&absolute))
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
    use std::collections::HashSet;
    use uuid::Uuid;

    struct TempDirectory(PathBuf);

    impl TempDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("canopy-folder-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn write_file(&self, relative: &str) {
            let path = self.0.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, "").unwrap();
        }
    }

    impl Drop for TempDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn tree(root: &TempDirectory, options: FolderStructureOptions<'_>) -> String {
        get_folder_structure(&root.0, options)
    }

    #[test]
    fn formats_files_before_directories_and_expands_directories_breadth_first() {
        let root = TempDirectory::new();
        root.write_file("z.txt");
        root.write_file("alpha/deep/leaf.txt");
        root.write_file("beta/file.txt");

        let output = tree(&root, FolderStructureOptions::default());
        let expected = format!(
            "Showing up to 20 items:\n\n{}{MAIN_SEPARATOR}\n├───z.txt\n├───alpha{MAIN_SEPARATOR}\n│   └───deep{MAIN_SEPARATOR}\n│       └───leaf.txt\n└───beta{MAIN_SEPARATOR}\n    └───file.txt",
            root.0.display()
        );
        assert_eq!(output, expected);
    }

    #[test]
    fn marks_ignored_directories_and_filters_files() {
        let root = TempDirectory::new();
        root.write_file("keep.rs");
        root.write_file("skip.txt");
        root.write_file("node_modules/pkg/index.js");
        root.write_file("private-dir/secret.txt");
        fs::write(root.0.join(".canopyignore"), "skip.txt\nprivate-dir/\n").unwrap();
        let service = FileDiscoveryService::new(&root.0, None).unwrap();
        let pattern = Regex::new(r"\.rs$").unwrap();

        let output = tree(
            &root,
            FolderStructureOptions {
                file_service: Some(&service),
                file_include_pattern: Some(&pattern),
                ..FolderStructureOptions::default()
            },
        );
        assert!(output.contains("├───keep.rs"));
        assert!(output.contains(&format!("node_modules{MAIN_SEPARATOR}...")));
        assert!(output.contains(&format!("private-dir{MAIN_SEPARATOR}...")));
        assert!(!output.contains("skip.txt"));
        assert!(!output.contains("secret.txt"));
        assert!(!output.contains("index.js"));
    }

    #[test]
    fn applies_a_custom_case_sensitive_ignored_folder_set() {
        let root = TempDirectory::new();
        root.write_file("ignored/hidden.txt");
        root.write_file("visible/visible.txt");
        let ignored = HashSet::from(["ignored".to_owned()]);
        // Verify the case-sensitive predicate directly because many default
        // macOS volumes cannot contain both `ignored` and `Ignored` paths.
        assert!(ignored_folder_name("ignored", Some(&ignored)));
        assert!(!ignored_folder_name("Ignored", Some(&ignored)));

        let output = tree(
            &root,
            FolderStructureOptions {
                ignored_folders: Some(&ignored),
                ..FolderStructureOptions::default()
            },
        );
        assert!(output.contains(&format!("ignored{MAIN_SEPARATOR}...")));
        assert!(output.contains("visible.txt"));
        assert!(!output.contains("hidden.txt"));
    }

    #[test]
    fn annotates_queued_folders_that_were_not_read_when_budget_runs_out() {
        let root = TempDirectory::new();
        for index in 0..5 {
            root.write_file(&format!("folder-{index}/child.txt"));
        }

        let output = tree(
            &root,
            FolderStructureOptions {
                max_items: 4,
                ..FolderStructureOptions::default()
            },
        );
        let expected = format!(
            "Showing up to 4 items:\n\n{}{MAIN_SEPARATOR}\n├───folder-0{MAIN_SEPARATOR}\n│   └───...\n├───folder-1{MAIN_SEPARATOR}\n│   └───...\n├───folder-2{MAIN_SEPARATOR}\n│   └───...\n├───folder-3{MAIN_SEPARATOR}\n│   └───...\n└───...",
            root.0.display()
        );
        assert_eq!(output, expected);
    }

    #[test]
    fn emits_file_and_folder_truncation_markers_at_the_combined_budget() {
        let root = TempDirectory::new();
        root.write_file("a.txt");
        root.write_file("b.txt");
        root.write_file("child/hidden.txt");

        let output = tree(
            &root,
            FolderStructureOptions {
                max_items: 1,
                ..FolderStructureOptions::default()
            },
        );
        let expected = format!(
            "Showing up to 1 items:\n\n{}{MAIN_SEPARATOR}\n├───a.txt\n├───...\n└───...",
            root.0.display()
        );
        assert_eq!(output, expected);
    }

    #[test]
    fn returns_the_source_framing_for_a_missing_root() {
        let root = TempDirectory::new();
        let missing = root.0.join("missing");
        assert_eq!(
            get_folder_structure(&missing, FolderStructureOptions::default()),
            format!(
                "Error: Could not read directory \"{}\". Check path and permissions.",
                missing.display()
            )
        );
    }
}
