//! Local file:// artifact publisher.

use std::path::PathBuf;

use url::Url;

use crate::storage::Storage;
use crate::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};
use crate::utils::cancellation::CancellationToken;

use super::publisher::{
    ArtifactPublisher, ArtifactPublisherKind, PublishArtifactInput, PublishedArtifact,
    PublisherFuture,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct LocalPublisher;

impl ArtifactPublisher for LocalPublisher {
    fn kind(&self) -> ArtifactPublisherKind {
        ArtifactPublisherKind::Local
    }

    fn publish<'a>(
        &'a self,
        input: &'a PublishArtifactInput,
        cancellation: &'a CancellationToken,
    ) -> PublisherFuture<'a> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err("Artifact publishing was cancelled.".to_owned());
            }
            let path: PathBuf = Storage::get_global_canopy_dir()
                .join("artifacts")
                .join(&input.id)
                .join("index.html");
            let parent = path
                .parent()
                .ok_or_else(|| "artifact destination has no parent directory".to_owned())?;
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| format!("could not create artifact directory: {error}"))?;
            if cancellation.is_cancelled() {
                return Err("Artifact publishing was cancelled.".to_owned());
            }
            atomic_write_file(
                &path,
                input.html.as_bytes(),
                &AtomicWriteOptions {
                    mode: Some(0o600),
                    force_mode: true,
                    symlink_policy: SymlinkPolicy::NoFollow,
                    ..AtomicWriteOptions::default()
                },
            )
            .map_err(|error| format!("could not write local artifact: {error}"))?;
            if cancellation.is_cancelled() {
                return Err("Artifact publishing was cancelled.".to_owned());
            }
            let url = Url::from_file_path(&path)
                .map_err(|_| "could not build file URL for local artifact".to_owned())?
                .to_string();
            Ok(PublishedArtifact {
                id: input.id.clone(),
                url,
                file_path: Some(path.to_string_lossy().into_owned()),
            })
        })
    }
}
