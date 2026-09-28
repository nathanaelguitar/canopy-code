//! Host-independent contracts for Canopy's `cua-driver-rs` computer-use tools.
//!
//! This module ports the pinned asset/schema metadata, permission and call
//! policy, install-approval state, bootstrap flow, driver downloader, and MCP
//! result projection from `packages/core/src/tools/computer-use/`. Production
//! host adapters and live tool registration are still outside this slice.

pub mod agent_tool_adapter;
pub mod bootstrap;
pub mod client;
mod constants;
pub mod downloader;
mod install_state;
pub mod install_state_store;
mod permission_detector;
pub mod result;
mod schemas;
pub mod tool_policy;

pub use agent_tool_adapter::{
    ComputerUseAdapterError, ComputerUseAdapterOptions, ComputerUseAgentToolAdapter,
    ComputerUseAuthorizationFuture, ComputerUseCallAuthorizer, ComputerUseClientFuture,
    ComputerUseComposedToolExecutor, ComputerUseDriverClient, MAX_COMPUTER_USE_ARGUMENT_BYTES,
    NativeComputerUseDriverClient,
};
pub use bootstrap::{
    BootstrapFuture, BootstrapHost, BootstrapOptions, PermissionKind, PermissionProbeResult,
    StatusDaemon, parse_permissions_status, run_bootstrap,
};
pub use client::{
    ComputerUseClient, ComputerUseClientOptions, ComputerUseProgress,
    DEFAULT_COMPUTER_USE_IDLE_TIMEOUT_MS, MAX_COMPUTER_USE_IDLE_TIMEOUT_MS,
    is_transport_closed_error,
};
pub use constants::{
    AssetTarget, CUA_DRIVER_VERSION, GITHUB_RELEASE_BASE, MAX_IMAGE_DIMENSION_ENV, OSS_MIRROR_BASE,
    approval_key, binary_path, computer_use_root, install_state_path, resolve_asset_target,
    resolve_asset_urls, resolve_checksum_urls, resolve_max_image_dimension, version_dir,
};
pub use downloader::{
    ArchiveExtractor, InstallOptions, SystemArchiveExtractor, ensure_installed, find_installed,
    parse_checksums,
};
pub use install_state::{InstallState, install_state_from_json, install_state_to_json};
pub use install_state_store::{
    install_state_path_for, is_package_spec_approved, load_install_state, save_install_state,
};
pub use permission_detector::{PermissionErrorKind, detect_permission_error};
pub use result::{build_display_text, build_llm_content, stringify_structured};
pub use schemas::{
    ComputerUseToolSchema, canopy_tool_name, computer_use_tool_names, computer_use_tool_schema,
};
pub use tool_policy::{coerce_types, is_high_risk_call};

#[cfg(test)]
mod tests;
