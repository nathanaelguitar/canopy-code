//! Publisher factory. Misconfigured remote routes fail during publication.

use super::host_publisher::HostPublisher;
use super::local_publisher::LocalPublisher;
use super::oss_publisher::OssPublisher;
use super::publisher::{ArtifactPublisher, ArtifactPublisherConfig};

pub fn create_artifact_publisher(config: ArtifactPublisherConfig) -> Box<dyn ArtifactPublisher> {
    match config {
        ArtifactPublisherConfig::Local => Box::new(LocalPublisher),
        ArtifactPublisherConfig::Host(config) => Box::new(HostPublisher::new(config)),
        ArtifactPublisherConfig::Oss(config) => Box::new(OssPublisher::new(config)),
    }
}
