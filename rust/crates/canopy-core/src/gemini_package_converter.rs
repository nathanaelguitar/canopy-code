//! Complete Gemini extension package conversion.
//!
//! This ports `convertGeminiExtensionPackage`, including a private temporary
//! output directory, source-root symlink confinement, TOML command projection,
//! and removal of the temporary tree when a fatal package-level error occurs.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use crate::gemini_converter::{GeminiConfigError, convert_gemini_to_canopy_config, is_path_within};
use crate::utils::yaml::{self, StringifyOptions};

/// Successfully converted package data.
///
/// Per-command TOML failures are non-fatal in the source implementation. They
/// leave the original file in place and are returned here so a host can emit
/// the same warning that the TypeScript debug logger emits.
#[derive(Clone, Debug, PartialEq)]
pub struct GeminiExtensionPackage {
    pub config: Value,
    pub converted_dir: PathBuf,
    pub warnings: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum GeminiPackageConversionError {
    #[error(transparent)]
    Config(#[from] GeminiConfigError),
    #[error("failed to create temporary Gemini extension directory at {path}: {source}")]
    CreateTemporaryDirectory {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to resolve Gemini extension directory at {path}: {source}")]
    ResolveExtensionDirectory {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(
        "failed to copy Gemini extension package from {source_path} to {destination_path}: {source}"
    )]
    CopyPackage {
        source_path: PathBuf,
        destination_path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("Gemini extension commands path is not a directory: {path}")]
    CommandsPathNotDirectory { path: PathBuf },
    #[error("failed to enumerate Gemini extension commands at {path}: {source}")]
    EnumerateCommands {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to write converted Gemini extension config at {path}: {source}")]
    WriteConvertedConfig {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// Convert a Gemini extension directory into a temporary Canopy package.
///
/// All regular files and directories are copied. Symlinks are dereferenced
/// only when their canonical target remains under the canonical extension
/// root; broken links and links to special files are skipped. TOML files under
/// `commands/` are converted independently: a failed command conversion is
/// recorded as a warning and leaves that TOML file untouched. A fatal copy,
/// directory-enumeration, or manifest-write error removes the temporary tree
/// before returning.
pub fn convert_gemini_extension_package(
    extension_dir: impl AsRef<Path>,
) -> Result<GeminiExtensionPackage, GeminiPackageConversionError> {
    let extension_dir = extension_dir.as_ref();
    let config = convert_gemini_to_canopy_config(extension_dir)?;
    let converted_dir = create_temporary_directory()?;

    let conversion = (|| {
        let source_root = fs::canonicalize(extension_dir).map_err(|source| {
            GeminiPackageConversionError::ResolveExtensionDirectory {
                path: extension_dir.to_path_buf(),
                source,
            }
        })?;
        copy_directory_confined(&source_root, &converted_dir, &source_root).map_err(|source| {
            GeminiPackageConversionError::CopyPackage {
                source_path: source_root.clone(),
                destination_path: converted_dir.clone(),
                source,
            }
        })?;

        let commands_dir = converted_dir.join("commands");
        let mut warnings = Vec::new();
        if commands_dir.exists() {
            if !commands_dir.is_dir() {
                return Err(GeminiPackageConversionError::CommandsPathNotDirectory {
                    path: commands_dir,
                });
            }
            convert_commands_directory(&commands_dir, &mut warnings)?;
        }

        let canopy_config_path = converted_dir.join("canopy-extension.json");
        let serialized_config = serde_json::to_vec_pretty(&config).map_err(|source| {
            // Values parsed from JSON are always serializable. Keep an I/O
            // shaped error here so the package error remains a single write
            // failure surface if that invariant ever changes.
            io::Error::new(io::ErrorKind::InvalidData, source)
        });
        let serialized_config = serialized_config.map_err(|source| {
            GeminiPackageConversionError::WriteConvertedConfig {
                path: canopy_config_path.clone(),
                source,
            }
        })?;
        fs::write(&canopy_config_path, serialized_config).map_err(|source| {
            GeminiPackageConversionError::WriteConvertedConfig {
                path: canopy_config_path,
                source,
            }
        })?;

        Ok(GeminiExtensionPackage {
            config: config.clone(),
            converted_dir: converted_dir.clone(),
            warnings,
        })
    })();

    if conversion.is_err() {
        // TypeScript intentionally treats temp cleanup as best-effort.
        let _ = fs::remove_dir_all(&converted_dir);
    }
    conversion
}

/// Convert one Gemini TOML command document to the Canopy Markdown form.
///
/// A non-empty string `description` produces YAML frontmatter. A missing,
/// non-string, or empty description produces only the prompt body. Other TOML
/// fields are ignored, matching `convertTomlToMarkdown`.
pub fn convert_toml_to_markdown(toml_content: &str) -> Result<String, String> {
    let parsed = toml::from_str::<toml::Value>(toml_content)
        .map_err(|error| format!("Failed to parse TOML: {error}"))?;
    let table = parsed
        .as_table()
        .ok_or_else(|| "TOML content must be an object".to_owned())?;
    let prompt = table
        .get("prompt")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| "TOML must contain a \"prompt\" field".to_owned())?;
    let description = table
        .get("description")
        .and_then(toml::Value::as_str)
        .filter(|description| !description.is_empty());

    if let Some(description) = description {
        let mut frontmatter = Map::new();
        frontmatter.insert(
            "description".to_owned(),
            Value::String(description.to_owned()),
        );
        let frontmatter = yaml::stringify(
            &frontmatter,
            Some(StringifyOptions {
                line_width: Some(0),
                min_content_width: None,
            }),
        );
        Ok(format!(
            "---\n{}\n---\n\n{}\n",
            frontmatter.trim_end(),
            prompt
        ))
    } else {
        Ok(format!("{prompt}\n"))
    }
}

/// Return whether a string parses as TOML. Empty TOML is valid.
pub fn is_toml_format(content: &str) -> bool {
    toml::from_str::<toml::Value>(content).is_ok()
}

fn convert_commands_directory(
    commands_dir: &Path,
    warnings: &mut Vec<String>,
) -> Result<(), GeminiPackageConversionError> {
    let mut toml_files = Vec::new();
    collect_toml_files(commands_dir, commands_dir, &mut toml_files).map_err(|source| {
        GeminiPackageConversionError::EnumerateCommands {
            path: commands_dir.to_path_buf(),
            source,
        }
    })?;
    toml_files.sort();

    for (relative_path, toml_path) in toml_files {
        let conversion_result = (|| -> Result<(), String> {
            // Node's UTF-8 file reads replace malformed sequences rather than
            // failing, so use the same replacement behavior here.
            let bytes = fs::read(&toml_path).map_err(|error| error.to_string())?;
            let toml_content = String::from_utf8_lossy(&bytes);
            let markdown = convert_toml_to_markdown(&toml_content)?;
            let markdown_path = toml_path.with_extension("md");
            fs::write(&markdown_path, markdown).map_err(|error| error.to_string())?;
            fs::remove_file(&toml_path).map_err(|error| error.to_string())?;
            Ok(())
        })();

        if let Err(error) = conversion_result {
            warnings.push(format!(
                "Warning: Failed to convert command file {}: {error}",
                relative_path.display()
            ));
        }
    }
    Ok(())
}

/// Like `glob('**/*.toml', { dot: false, nodir: true })`, this visits regular
/// TOML files recursively, does not descend into dot-directories, and excludes
/// dot-files. The converted package has no symlinks because its copy phase
/// dereferences confined links.
fn collect_toml_files(
    commands_root: &Path,
    directory: &Path,
    files: &mut Vec<(PathBuf, PathBuf)>,
) -> io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_name = entry.file_name();
        if file_name.to_string_lossy().starts_with('.') {
            continue;
        }

        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_toml_files(commands_root, &path, files)?;
        } else if file_type.is_file() && path.extension().is_some_and(|ext| ext == "toml") {
            let relative_path = path
                .strip_prefix(commands_root)
                .map(Path::to_path_buf)
                .unwrap_or_else(|_| path.clone());
            files.push((relative_path, path));
        }
    }
    Ok(())
}

fn create_temporary_directory() -> Result<PathBuf, GeminiPackageConversionError> {
    let base = std::env::temp_dir();
    for _ in 0..8 {
        let path = base.join(format!("canopy-extension{}", uuid::Uuid::new_v4().simple()));
        match fs::create_dir(&path) {
            Ok(()) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    if let Err(source) =
                        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                    {
                        let _ = fs::remove_dir_all(&path);
                        return Err(GeminiPackageConversionError::CreateTemporaryDirectory {
                            path,
                            source,
                        });
                    }
                }
                return Ok(path);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(GeminiPackageConversionError::CreateTemporaryDirectory {
                    path,
                    source,
                });
            }
        }
    }

    Err(GeminiPackageConversionError::CreateTemporaryDirectory {
        path: base.join("canopy-extension<unique-suffix>"),
        source: io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a unique temporary directory",
        ),
    })
}

fn copy_directory_confined(source: &Path, destination: &Path, root: &Path) -> io::Result<()> {
    copy_directory_confined_inner(source, destination, root, &mut HashSet::new())
}

fn copy_directory_confined_inner(
    source: &Path,
    destination: &Path,
    root: &Path,
    active_directories: &mut HashSet<PathBuf>,
) -> io::Result<()> {
    fs::create_dir_all(destination)?;

    let real_source = match fs::canonicalize(source) {
        Ok(real_source) if is_path_within(&real_source, root) => real_source,
        // The TypeScript implementation skips escaping/broken links. This
        // guard also closes a source-path swap between directory enumeration
        // and recursion.
        _ => return Ok(()),
    };
    if !active_directories.insert(real_source.clone()) {
        // The TypeScript implementation has no cycle guard; stop an internal
        // directory symlink cycle instead of recursing until stack exhaustion.
        return Ok(());
    }

    let result = (|| {
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let source_path = entry.path();
            let destination_path = destination.join(entry.file_name());
            let file_type = entry.file_type()?;

            if file_type.is_dir() {
                copy_directory_confined_inner(
                    &source_path,
                    &destination_path,
                    root,
                    active_directories,
                )?;
            } else if file_type.is_symlink() {
                let Ok(real_path) = fs::canonicalize(&source_path) else {
                    // Broken links are ignored by the source converter.
                    continue;
                };
                if !is_path_within(&real_path, root) {
                    continue;
                }
                let Ok(target_metadata) = fs::metadata(&real_path) else {
                    continue;
                };
                if target_metadata.is_dir() {
                    copy_directory_confined_inner(
                        &real_path,
                        &destination_path,
                        root,
                        active_directories,
                    )?;
                } else if target_metadata.is_file() {
                    fs::copy(&real_path, &destination_path)?;
                }
            } else if file_type.is_file() {
                let Ok(real_path) = fs::canonicalize(&source_path) else {
                    continue;
                };
                if is_path_within(&real_path, root) {
                    fs::copy(&real_path, &destination_path)?;
                }
            }
            // Sockets, FIFOs, and device nodes are skipped like in TypeScript.
        }
        Ok(())
    })();

    active_directories.remove(&real_source);
    result
}
