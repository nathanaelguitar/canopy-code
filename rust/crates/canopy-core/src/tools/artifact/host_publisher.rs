//! Shell-free publisher that invokes a configured upload command.

use std::io;
use std::path::PathBuf;
use std::process::Stdio;

use tokio::io::AsyncReadExt;
use tokio::process::Command;
use uuid::Uuid;

use crate::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};
use crate::utils::cancellation::CancellationToken;

use super::publisher::{
    ArtifactHostConfig, ArtifactPublisher, ArtifactPublisherKind, PublishArtifactInput,
    PublishedArtifact, PublisherFuture,
};

const MAX_COMMAND_OUTPUT_BYTES: usize = 10 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct HostPublisher {
    config: ArtifactHostConfig,
}

impl HostPublisher {
    pub fn new(config: ArtifactHostConfig) -> Self {
        Self { config }
    }
}

impl ArtifactPublisher for HostPublisher {
    fn kind(&self) -> ArtifactPublisherKind {
        ArtifactPublisherKind::Host
    }

    fn publish<'a>(
        &'a self,
        input: &'a PublishArtifactInput,
        cancellation: &'a CancellationToken,
    ) -> PublisherFuture<'a> {
        Box::pin(async move {
            let upload_command = self.config.upload_command.trim();
            let url_template = self.config.url_template.trim();
            if upload_command.is_empty() {
                return Err("artifact.host.uploadCommand is not configured (set it to e.g. \"aws s3 cp {file} s3://bucket/{key}\").".to_owned());
            }
            if url_template.is_empty() {
                return Err("artifact.host.urlTemplate is not configured (set it to e.g. \"https://bucket.example.com/{key}\").".to_owned());
            }
            if !upload_command.contains("{file}") {
                return Err("artifact.host.uploadCommand must include the {file} placeholder (the local HTML path to upload).".to_owned());
            }
            if !upload_command.contains("{key}") {
                return Err("artifact.host.uploadCommand must include the {key} placeholder so the upload destination matches the returned URL.".to_owned());
            }
            if !url_template.contains("{key}") {
                return Err("artifact.host.urlTemplate must include the {key} placeholder (the remote object key).".to_owned());
            }
            if url_template.contains("{file}") {
                return Err(
                    "artifact.host.urlTemplate must not include {file}; only {key} is supported."
                        .to_owned(),
                );
            }

            let prefix = normalize_key_prefix(self.config.key_prefix.as_deref(), "host")?;
            let key = format!("{prefix}/{}/index.html", input.id);
            let temp_dir = std::env::temp_dir().join(format!("canopy-art-{}", Uuid::new_v4()));
            create_private_directory(&temp_dir)?;
            let file = temp_dir.join("index.html");
            let publish_result = async {
                atomic_write_file(
                    &file,
                    input.html.as_bytes(),
                    &AtomicWriteOptions {
                        mode: Some(0o600),
                        force_mode: true,
                        symlink_policy: SymlinkPolicy::NoFollow,
                        ..AtomicWriteOptions::default()
                    },
                )
                .map_err(|error| format!("could not stage artifact for upload: {error}"))?;
                if cancellation.is_cancelled() {
                    return Err("Artifact publishing was cancelled.".to_owned());
                }
                let argv = tokenize_command(upload_command)?;
                let mut argv = argv.into_iter().map(|token| {
                    token
                        .replace("{file}", &file.to_string_lossy())
                        .replace("{key}", &key)
                });
                let command = argv
                    .next()
                    .filter(|command| !command.is_empty())
                    .ok_or_else(|| "artifact.host.uploadCommand is empty.".to_owned())?;
                let args = argv.collect::<Vec<_>>();
                run_upload_command(&command, &args, cancellation).await?;
                if cancellation.is_cancelled() {
                    return Err("Artifact publishing was cancelled.".to_owned());
                }
                Ok(())
            }
            .await;
            let _ = tokio::fs::remove_dir_all(&temp_dir).await;
            publish_result?;

            Ok(PublishedArtifact {
                id: input.id.clone(),
                url: url_template.replace("{key}", &key),
                file_path: None,
            })
        })
    }
}

pub fn tokenize_command(command: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut started = false;
    let mut quote = None;
    for character in command.chars() {
        if let Some(delimiter) = quote {
            if character == delimiter {
                quote = None;
            } else {
                current.push(character);
            }
        } else if character == '"' || character == '\'' {
            quote = Some(character);
            started = true;
        } else if character.is_whitespace() {
            if started {
                tokens.push(std::mem::take(&mut current));
                started = false;
            }
        } else {
            current.push(character);
            started = true;
        }
    }
    if quote.is_some() {
        return Err("Unterminated quote in uploadCommand.".to_owned());
    }
    if started {
        tokens.push(current);
    }
    Ok(tokens)
}

fn normalize_key_prefix(raw: Option<&str>, kind: &str) -> Result<String, String> {
    let prefix = raw.unwrap_or("").trim_matches('/');
    let prefix = if prefix.is_empty() && raw.is_none_or(str::is_empty) {
        "artifacts"
    } else {
        prefix
    };
    if prefix.is_empty() {
        return Err(format!(
            "artifact.{kind}.keyPrefix must not be empty or \"/\" after stripping slashes."
        ));
    }
    if prefix.chars().any(|character| {
        character == '#' || character == '?' || character == '%' || character.is_whitespace()
    }) {
        return Err(format!(
            "artifact.{kind}.keyPrefix must not contain #, ?, %, or whitespace."
        ));
    }
    Ok(prefix.to_owned())
}

#[cfg(unix)]
fn create_private_directory(path: &PathBuf) -> Result<(), String> {
    use std::os::unix::fs::DirBuilderExt;
    let mut builder = std::fs::DirBuilder::new();
    builder.mode(0o700);
    builder
        .create(path)
        .map_err(|error| format!("could not secure artifact upload directory: {error}"))
}

#[cfg(not(unix))]
fn create_private_directory(path: &PathBuf) -> Result<(), String> {
    std::fs::create_dir(path)
        .map_err(|error| format!("could not create artifact upload directory: {error}"))?;
    Ok(())
}

async fn run_upload_command(
    command: &str,
    args: &[String],
    cancellation: &CancellationToken,
) -> Result<(), String> {
    let command = command.to_owned();
    let args = args.to_vec();
    let mut run = tokio::spawn(async move { run_command(command, args).await });
    tokio::select! {
        _ = cancellation.cancelled() => {
            run.abort();
            let _ = run.await;
            Err("Artifact publishing was cancelled.".to_owned())
        }
        result = &mut run => result
            .map_err(|error| format!("upload command task failed: {error}"))?,
    }
}

async fn run_command(command: String, args: Vec<String>) -> Result<(), String> {
    let mut child = Command::new(&command)
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| format!("could not start upload command {command}: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "upload command stdout was not captured".to_owned())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "upload command stderr was not captured".to_owned())?;
    let output = async {
        tokio::try_join!(read_limited(stdout), read_limited(stderr), async {
            child
                .wait()
                .await
                .map_err(|error| format!("upload command failed: {error}"))
        })
    };
    tokio::pin!(output);
    let (stdout, stderr, status) = output.await?;
    if !status.success() {
        let stderr = String::from_utf8_lossy(&stderr).trim().to_owned();
        let stdout = String::from_utf8_lossy(&stdout).trim().to_owned();
        let detail = if !stderr.is_empty() {
            stderr
        } else if !stdout.is_empty() {
            stdout
        } else {
            format!("exit status {status}")
        };
        return Err(format!("artifact upload command failed: {detail}"));
    }
    Ok(())
}

async fn read_limited<R>(mut reader: R) -> Result<Vec<u8>, String>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut output = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        let count = reader
            .read(&mut buffer)
            .await
            .map_err(|error: io::Error| format!("could not read upload command output: {error}"))?;
        if count == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(count) > MAX_COMMAND_OUTPUT_BYTES {
            return Err(format!(
                "artifact upload command output exceeded the {MAX_COMMAND_OUTPUT_BYTES} byte limit"
            ));
        }
        output.extend_from_slice(&buffer[..count]);
    }
}
