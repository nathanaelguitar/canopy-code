//! Interactive HTML artifact validation and publishing.

pub mod artifact_tool;
pub mod create_publisher;
pub mod host_publisher;
pub mod html;
pub mod local_publisher;
pub mod oss_publisher;
pub mod publisher;

pub use artifact_tool::{ArtifactTool, ArtifactToolConfig, ArtifactToolParams};
pub use publisher::{
    ArtifactHostConfig, ArtifactOssConfig, ArtifactPublisher, ArtifactPublisherConfig,
    ArtifactPublisherKind, PublishArtifactInput, PublishedArtifact, artifact_id_from_path,
};
