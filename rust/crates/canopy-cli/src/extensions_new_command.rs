// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Create a new extension from a packaged example or a minimal manifest.

use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

const USAGE: &str = "Usage: canopy extensions new <path> [template]";
const EXAMPLES_SOURCE: &str = "../../../packages/cli/src/commands/extensions/examples";

struct TemplateChoices {
    names: Vec<String>,
    read_failed: bool,
}

pub fn run(args: &[String]) -> Result<(), String> {
    if args
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        print_help();
        return Ok(());
    }
    let (destination, template) = parse_args(args)?;
    let examples_directory = examples_directory();
    let choices = read_template_choices(&examples_directory);

    if let Some(template) = template.as_deref() {
        validate_template_name(template)?;
        if choices.names.is_empty() {
            return Err(if choices.read_failed {
                "Extension templates could not be read in this installation.".to_owned()
            } else {
                "No boilerplate templates are available in this installation.".to_owned()
            });
        }
        if !choices.names.iter().any(|choice| choice == template) {
            return Err(format!(
                "Unknown extension template \"{}\". Available templates: {}.",
                safe_display(template),
                choices.names.join(", ")
            ));
        }
        create_from_template(&destination, &examples_directory, template)?;
        println!(
            "Successfully created new extension from template \"{}\" at {}.",
            safe_display(template),
            safe_display(&destination)
        );
    } else {
        create_minimal_extension(&destination)?;
        println!(
            "Successfully created new extension at {}.",
            safe_display(&destination)
        );
    }
    println!(
        "You can install this using \"canopy extensions link {}\" to test it out.",
        safe_display(&destination)
    );
    Ok(())
}

fn parse_args(args: &[String]) -> Result<(String, Option<String>), String> {
    match args {
        [path] if !path.is_empty() && !path.starts_with('-') => Ok((path.clone(), None)),
        [path, template]
            if !path.is_empty()
                && !path.starts_with('-')
                && !template.is_empty()
                && !template.starts_with('-') =>
        {
            Ok((path.clone(), Some(template.clone())))
        }
        [] => Err(format!("{USAGE}\nA destination path is required.")),
        [path, ..] if path.starts_with('-') => Err(format!("{USAGE}\nUnknown option: {path}")),
        _ => Err(format!(
            "{USAGE}\nExpected a path and at most one template."
        )),
    }
}

fn validate_template_name(template: &str) -> Result<(), String> {
    let mut components = Path::new(template).components();
    let is_one_normal_component =
        matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none();
    if template.is_empty()
        || template == "."
        || template == ".."
        || template.contains('/')
        || template.contains('\\')
        || template.chars().any(char::is_control)
        || !is_one_normal_component
    {
        return Err(format!("Invalid extension template name: {template}"));
    }
    Ok(())
}

fn examples_directory() -> PathBuf {
    if let Ok(executable) = std::env::current_exe()
        && let Some(parent) = executable.parent()
    {
        let packaged = parent.join("examples");
        if packaged.is_dir() {
            return packaged;
        }
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(EXAMPLES_SOURCE)
}

fn read_template_choices(examples_directory: &Path) -> TemplateChoices {
    let entries = match fs::read_dir(examples_directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return TemplateChoices {
                names: Vec::new(),
                read_failed: false,
            };
        }
        Err(error) => {
            eprintln!(
                "Warning: failed to read extension templates: {}",
                safe_display(&error.to_string())
            );
            return TemplateChoices {
                names: Vec::new(),
                read_failed: true,
            };
        }
    };

    let mut names = Vec::new();
    let mut read_failed = false;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                eprintln!(
                    "Warning: failed to read extension templates: {}",
                    safe_display(&error.to_string())
                );
                read_failed = true;
                continue;
            }
        };
        match entry.file_type() {
            Ok(file_type) if file_type.is_dir() => {
                if let Some(name) = entry.file_name().to_str() {
                    names.push(name.to_owned());
                }
            }
            Ok(_) => {}
            Err(error) => {
                eprintln!(
                    "Warning: failed to read extension templates: {}",
                    safe_display(&error.to_string())
                );
                read_failed = true;
            }
        }
    }
    names.sort();
    TemplateChoices { names, read_failed }
}

fn create_from_template(
    destination: &str,
    examples_directory: &Path,
    template: &str,
) -> Result<(), String> {
    let examples_root = fs::canonicalize(examples_directory)
        .map_err(|error| format!("Could not resolve extension templates directory: {error}"))?;
    let template_path = examples_root.join(template);
    let template_metadata = fs::symlink_metadata(&template_path)
        .map_err(|error| format!("Could not read extension template: {error}"))?;
    if !template_metadata.is_dir() || template_metadata.file_type().is_symlink() {
        return Err(format!(
            "Extension template \"{}\" is not a directory.",
            safe_display(template)
        ));
    }
    let canonical_template = fs::canonicalize(&template_path)
        .map_err(|error| format!("Could not resolve extension template: {error}"))?;
    if !canonical_template.starts_with(&examples_root) {
        return Err(
            "Extension template resolves outside the packaged examples directory.".to_owned(),
        );
    }

    let destination_path = Path::new(destination);
    create_new_directory(destination_path)?;
    let result =
        copy_directory_contents(&canonical_template, destination_path, &canonical_template);
    if let Err(error) = result {
        let cleanup_error = fs::remove_dir_all(destination_path).err();
        return Err(match cleanup_error {
            Some(cleanup_error) => {
                format!("{error}; could not remove incomplete extension directory: {cleanup_error}")
            }
            None => error,
        });
    }
    Ok(())
}

fn copy_directory_contents(
    source: &Path,
    destination: &Path,
    source_root: &Path,
) -> Result<(), String> {
    for entry in
        fs::read_dir(source).map_err(|error| format!("Could not read template files: {error}"))?
    {
        let entry = entry.map_err(|error| format!("Could not read template entry: {error}"))?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry
            .file_type()
            .map_err(|error| format!("Could not inspect template entry: {error}"))?;
        if file_type.is_symlink() {
            return Err(format!(
                "Extension template contains a symbolic link: {}",
                safe_display(&source_path.to_string_lossy())
            ));
        }
        if file_type.is_dir() {
            let canonical_source = fs::canonicalize(&source_path)
                .map_err(|error| format!("Could not resolve template directory: {error}"))?;
            if !canonical_source.starts_with(source_root) {
                return Err("Extension template directory resolves outside its root.".to_owned());
            }
            fs::create_dir(&destination_path)
                .map_err(|error| format!("Could not create extension directory: {error}"))?;
            copy_directory_contents(&canonical_source, &destination_path, source_root)?;
        } else if file_type.is_file() {
            let canonical_source = fs::canonicalize(&source_path)
                .map_err(|error| format!("Could not resolve template file: {error}"))?;
            if !canonical_source.starts_with(source_root) {
                return Err("Extension template file resolves outside its root.".to_owned());
            }
            fs::copy(&canonical_source, &destination_path)
                .map_err(|error| format!("Could not copy extension template file: {error}"))?;
        } else {
            return Err(format!(
                "Extension template contains an unsupported file: {}",
                safe_display(&source_path.to_string_lossy())
            ));
        }
    }
    Ok(())
}

fn create_minimal_extension(destination: &str) -> Result<(), String> {
    let destination_path = Path::new(destination);
    create_new_directory(destination_path)?;
    let extension_name = destination_path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default();
    let manifest = format!(
        "{{\n  \"name\": {},\n  \"version\": \"1.0.0\"\n}}",
        serde_json::to_string(extension_name).map_err(|error| error.to_string())?
    );
    if let Err(error) = fs::write(destination_path.join("canopy-extension.json"), manifest) {
        let cleanup_error = fs::remove_dir_all(destination_path).err();
        return Err(match cleanup_error {
            Some(cleanup_error) => {
                format!(
                    "Could not write extension manifest: {error}; could not remove incomplete extension directory: {cleanup_error}"
                )
            }
            None => format!("Could not write extension manifest: {error}"),
        });
    }
    Ok(())
}

fn create_new_directory(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            return Err(format!(
                "Path already exists: {}",
                safe_display(&path.to_string_lossy())
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("Could not inspect destination path: {error}")),
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|error| format!("Could not create destination parent directory: {error}"))?;
    fs::create_dir(path).map_err(|error| {
        if error.kind() == io::ErrorKind::AlreadyExists {
            format!(
                "Path already exists: {}",
                safe_display(&path.to_string_lossy())
            )
        } else {
            format!("Could not create extension directory: {error}")
        }
    })
}

fn safe_display(value: &str) -> String {
    canopy_core::utils::terminal_safe::strip_terminal_control_sequences(value)
}

fn print_help() {
    println!("{USAGE}");
    let examples = examples_directory();
    let choices = read_template_choices(&examples);
    if choices.names.is_empty() {
        println!("No boilerplate templates are available in this installation.");
    } else {
        println!("Available templates: {}", choices.names.join(", "));
    }
}
