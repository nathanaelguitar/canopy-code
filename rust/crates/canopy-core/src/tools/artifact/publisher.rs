//! Shared contract and configuration for interactive artifact publishers.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use sha1::{Digest, Sha1};

use crate::utils::cancellation::CancellationToken;

pub type PublisherFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PublishedArtifact, String>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactPublisherKind {
    Local,
    Host,
    Oss,
}

#[derive(Clone, Debug)]
pub struct PublishArtifactInput {
    pub id: String,
    pub title: String,
    pub html: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedArtifact {
    pub id: String,
    pub url: String,
    pub file_path: Option<String>,
}

pub trait ArtifactPublisher: Send + Sync {
    fn kind(&self) -> ArtifactPublisherKind;

    fn publish<'a>(
        &'a self,
        input: &'a PublishArtifactInput,
        cancellation: &'a CancellationToken,
    ) -> PublisherFuture<'a>;
}

#[derive(Clone, Debug, Default)]
pub struct ArtifactHostConfig {
    pub upload_command: String,
    pub url_template: String,
    pub key_prefix: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ArtifactOssConfig {
    pub bucket: String,
    pub endpoint: String,
    pub key_prefix: Option<String>,
    pub acl: Option<String>,
    pub public_base_url: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub enum ArtifactPublisherConfig {
    #[default]
    Local,
    Host(ArtifactHostConfig),
    Oss(ArtifactOssConfig),
}

/// Mirrors path.resolve for an absolute input without following symlinks.
/// Artifact identity is path-based so re-publishing the same source is stable.
pub fn artifact_id_from_path(path: &Path) -> Result<String, String> {
    if !path.is_absolute() {
        return Err(format!("File path must be absolute: {}", path.display()));
    }
    let mut normalized = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            std::path::Component::RootDir => normalized.push(component.as_os_str()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::Normal(part) => normalized.push(part),
        }
    }
    let digest = Sha1::digest(normalized.to_string_lossy().as_bytes());
    Ok(digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}
