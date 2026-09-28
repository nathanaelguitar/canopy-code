//! Marketplace source loading and source-registry metadata updates.
//!
//! This service fetches or reads a marketplace JSON document, parses its
//! metadata, and atomically adds or replaces the corresponding source record.
//! It does not install plugins, resolve plugin contents, or run an update
//! lifecycle; those installer steps remain unported.

use std::io;
use std::path::{Path, PathBuf};

use serde_json::Value;
use tokio::io::AsyncReadExt;

use crate::extension_install_source::parse_install_source;
use crate::extension_marketplace_fetch::{
    MarketplaceFetchError, MarketplaceFetchOptions, MarketplaceTransport,
    fetch_marketplace_text_from_source,
};
use crate::extension_network_policy::ExtensionAddressResolver;
use crate::extension_source_projection::parse_extension_source_type;
use crate::extension_source_store::{ExtensionSource, ExtensionSourceStore};
use crate::extensions::redact_url_credentials;
use crate::utils::cancellation::CancellationToken;

/// A loaded marketplace document paired with the registry metadata for it.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadedMarketplaceSource {
    pub source: ExtensionSource,
    pub config: Value,
}

/// Errors reported while adding or refreshing marketplace source metadata.
#[derive(Debug, thiserror::Error)]
pub enum MarketplaceSourceServiceError {
    #[error(transparent)]
    Fetch(#[from] MarketplaceFetchError),
    #[error("Could not update marketplace source registry: {0}")]
    Store(#[from] io::Error),
    #[error(
        "No marketplace found at \"{source_text}\". Expected a .claude-plugin/marketplace.json."
    )]
    NoMarketplace { source_text: String },
    #[error(
        "\"{source_text}\" looks like a single extension, not a marketplace. Install it directly with: /extensions install {source_text}"
    )]
    SingleExtension { source_text: String },
}

/// Coordinates bounded marketplace loading with the persisted source registry.
pub struct MarketplaceSourceService<'a, R, T> {
    store: &'a ExtensionSourceStore,
    resolver: &'a R,
    transport: &'a T,
    fetch_options: MarketplaceFetchOptions,
}

impl<'a, R, T> MarketplaceSourceService<'a, R, T>
where
    R: ExtensionAddressResolver,
    T: MarketplaceTransport,
{
    pub fn new(
        store: &'a ExtensionSourceStore,
        resolver: &'a R,
        transport: &'a T,
        fetch_options: MarketplaceFetchOptions,
    ) -> Self {
        Self {
            store,
            resolver,
            transport,
            fetch_options,
        }
    }

    /// Load and parse a marketplace document without changing the registry.
    /// Local paths are read directly; remote requests retain the fetcher's
    /// timeout, response-size, redirect, and public-network protections.
    pub async fn load_source(
        &self,
        source: &str,
        github_token: Option<&str>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Option<Value>, MarketplaceFetchError> {
        let trimmed = source.trim();
        let text = match read_local_source_text(trimmed, self.fetch_options.max_body_bytes).await {
            Some(text) => Some(text),
            None => {
                fetch_marketplace_text_from_source(
                    trimmed,
                    github_token,
                    &self.fetch_options,
                    self.resolver,
                    self.transport,
                    cancellation,
                )
                .await?
            }
        };
        let Some(text) = text else {
            return Ok(None);
        };
        Ok(serde_json::from_str(&text).ok())
    }

    /// Fetch a marketplace and add its source metadata to the registry.
    /// The config's non-empty `name` is used for display, otherwise the
    /// trimmed input is used, matching the TypeScript source registry.
    pub async fn add_source(
        &self,
        source: &str,
        github_token: Option<&str>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<LoadedMarketplaceSource, MarketplaceSourceServiceError> {
        let trimmed = source.trim();
        let Some(config) = self
            .load_source(trimmed, github_token, cancellation)
            .await?
        else {
            return Err(self.classify_missing_source(trimmed).await);
        };

        let now = now_iso8601();
        let name = config
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .unwrap_or(trimmed)
            .to_owned();
        let source = ExtensionSource {
            name,
            source: trimmed.to_owned(),
            source_type: parse_extension_source_type(trimmed),
            added_at: Some(now.clone()),
            last_updated_at: Some(now),
        };
        self.store.add(&source)?;
        Ok(LoadedMarketplaceSource { source, config })
    }

    /// Refresh a registered source. A missing source or an unsuccessful fetch
    /// returns `None` without modifying stored metadata. On success, the
    /// original source and `addedAt` are kept while the display name and
    /// `lastUpdatedAt` are replaced from the latest marketplace document.
    pub async fn refresh_source(
        &self,
        name: &str,
        github_token: Option<&str>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Option<LoadedMarketplaceSource>, MarketplaceSourceServiceError> {
        let Some(existing) = self
            .store
            .read()
            .into_iter()
            .find(|source| source.name == name)
        else {
            return Ok(None);
        };
        let Some(config) = self
            .load_source(&existing.source, github_token, cancellation)
            .await?
        else {
            return Ok(None);
        };

        let display_name = config
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .unwrap_or(&existing.name)
            .to_owned();
        let updated = ExtensionSource {
            name: display_name,
            source: existing.source,
            source_type: existing.source_type,
            added_at: existing.added_at,
            last_updated_at: Some(now_iso8601()),
        };
        self.store.add(&updated)?;
        Ok(Some(LoadedMarketplaceSource {
            source: updated,
            config,
        }))
    }

    async fn classify_missing_source(&self, source: &str) -> MarketplaceSourceServiceError {
        let local_path_exists = tokio::fs::metadata(source).await.is_ok();
        let redacted = redact_url_credentials(source);
        match parse_install_source(source, local_path_exists) {
            Ok(_install_metadata) => MarketplaceSourceServiceError::SingleExtension {
                source_text: redacted,
            },
            Err(_) => MarketplaceSourceServiceError::NoMarketplace {
                source_text: redacted,
            },
        }
    }
}

async fn read_local_source_text(source: &str, maximum_bytes: usize) -> Option<String> {
    let source_path = Path::new(source);
    let metadata = tokio::fs::metadata(source_path).await.ok()?;
    let config_path = if metadata.is_dir() {
        source_path.join(".claude-plugin").join("marketplace.json")
    } else if metadata.is_file() {
        PathBuf::from(source_path)
    } else {
        return None;
    };

    let file = tokio::fs::File::open(config_path).await.ok()?;
    let read_limit = u64::try_from(maximum_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut bytes = Vec::new();
    file.take(read_limit).read_to_end(&mut bytes).await.ok()?;
    if bytes.len() > maximum_bytes {
        return None;
    }
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

fn now_iso8601() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
