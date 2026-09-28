mod config;
mod context;
mod mcp;
mod memory_content;
mod provider;
mod proxy;

pub use config::{
    AutoRecallConfig, ConfigurationError, ExternalContextConfig, ProviderConfig, load_config,
};
pub use context::{ExternalContextItem, normalize_search_query, render_external_context};
pub use mcp::{
    EXTERNAL_CONTEXT_SERVER_PROFILE, McpServerProfile, run_stdio, run_stdio_with_profile,
};
pub use memory_content::{MAX_MEMORY_CONTENT_CHARACTERS, is_valid_memory_content};
pub use provider::{
    ProviderClient, ProviderError, RememberResult, remember_with_timeout, search_with_timeout,
};

pub const SERVER_NAME: &str = "external-context";
pub const SERVER_VERSION: &str = "1.0.0";
