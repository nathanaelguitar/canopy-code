// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//!
//! End-to-end WebFetch download and processing orchestration.
//!
//! The service owns its connection pool and response cache. Hosts should keep
//! one service per session/storage scope and may call it again after changing
//! sessions; storage changes clear cached entries so persisted paths from an
//! earlier project are never returned in the new project.

use std::time::Duration;

use thiserror::Error;

use crate::storage::Storage;

use super::fetch_cache::{FetchCacheKey, WebFetchResponseCache};
use super::fetch_plan::{fetch_with_https_upgrade, upgraded_https_url};
use super::fetch_policy::{
    FetchPolicyClient, FetchPolicyError, FetchPolicyOptions, FetchPolicyResult,
};
use super::fetch_processing::{
    FetchProcessedResponse, FetchProcessingError, FetchSessionByteBudget, HtmlToMarkdownConverter,
    process_fetch_response,
};
use super::{rewrite_github_blob_url, validate_web_fetch_url};

const FETCH_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;
const MAX_REDIRECTS: usize = 10;

/// Requested output preference. All response bodies are normalized to text
/// after download; this value only controls HTTP content negotiation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FetchContentFormat {
    #[default]
    Auto,
    Markdown,
    Html,
    Text,
}

impl FetchContentFormat {
    pub const fn accept_header(self) -> &'static str {
        match self {
            Self::Auto => "text/markdown, text/html;q=0.9, text/plain;q=0.8, */*;q=0.1",
            Self::Markdown => "text/markdown, */*;q=0.1",
            Self::Html => "text/html, */*;q=0.1",
            Self::Text => "text/plain, */*;q=0.1",
        }
    }
}

/// A redirect outside the approved origin. The caller should surface this to
/// the user and require a separate WebFetch invocation for the destination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchRedirect {
    pub original_url: String,
    pub redirect_url: String,
    pub status: u16,
}

/// Result of fetching and processing a URL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WebFetchOutcome {
    Processed(FetchProcessedResponse),
    Redirect(FetchRedirect),
}

#[derive(Debug, Error)]
pub enum WebFetchServiceError {
    #[error("The 'url' is invalid: {0}")]
    InvalidUrl(String),
    #[error(transparent)]
    Fetch(#[from] FetchPolicyError),
    #[error(transparent)]
    Processing(#[from] FetchProcessingError),
}

/// Reusable WebFetch download service.
pub struct WebFetchService {
    client: FetchPolicyClient,
    cache: WebFetchResponseCache,
    storage_scope: Option<Storage>,
}

impl WebFetchService {
    pub fn new() -> Result<Self, reqwest::Error> {
        Self::new_with_proxy(None)
    }

    /// Create a WebFetch service using an optional CLI/settings proxy. When
    /// absent, reqwest's environment proxy behavior is retained.
    pub fn new_with_proxy(proxy_url: Option<&str>) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: FetchPolicyClient::new_with_proxy(proxy_url)?,
            cache: WebFetchResponseCache::new(),
            storage_scope: None,
        })
    }

    /// Fetch one approved URL, optionally using the session cache, and
    /// normalize/persist the returned content. Dropping this future cancels
    /// network transfer and processing awaits that support cancellation.
    ///
    /// `requested_url` is validated and GitHub blob links are rewritten before
    /// the request, matching the URL used for permission approval in the
    /// TypeScript implementation. The value is preserved in response metadata.
    pub async fn fetch(
        &mut self,
        requested_url: &str,
        session_id: &str,
        format: FetchContentFormat,
        cli_version: &str,
        storage: &Storage,
        byte_budget: &mut dyn FetchSessionByteBudget,
        html_converter: Option<&dyn HtmlToMarkdownConverter>,
    ) -> Result<WebFetchOutcome, WebFetchServiceError> {
        validate_web_fetch_url(requested_url)
            .map_err(|error| WebFetchServiceError::InvalidUrl(error.to_owned()))?;
        self.bind_storage(storage);

        let normalized_url = rewrite_github_blob_url(requested_url);
        let upgraded_url = upgraded_https_url(&normalized_url)?;
        let cache_url = upgraded_url.as_deref().unwrap_or(normalized_url.as_str());
        let accept_header = format.accept_header();
        let cache_key = FetchCacheKey::new(session_id, accept_header, cache_url);
        if let Some(cached) = self.cache.get(&cache_key, &normalized_url) {
            return Ok(WebFetchOutcome::Processed(cached));
        }

        let options = FetchPolicyOptions {
            timeout: FETCH_TIMEOUT,
            max_bytes: MAX_RESPONSE_BYTES,
            max_redirects: MAX_REDIRECTS,
            headers: vec![
                ("Accept".to_owned(), accept_header.to_owned()),
                (
                    "User-Agent".to_owned(),
                    format!(
                        "CanopyCode/{cli_version} ({}; {})",
                        node_platform(),
                        node_arch()
                    ),
                ),
            ],
        };
        let planned = fetch_with_https_upgrade(&self.client, &normalized_url, &options).await?;

        match planned.result {
            FetchPolicyResult::CrossOriginRedirect {
                original_url,
                redirect_url,
                status,
            } => Ok(WebFetchOutcome::Redirect(FetchRedirect {
                original_url,
                redirect_url,
                status,
            })),
            FetchPolicyResult::Response(response) => {
                let processed = process_fetch_response(
                    response,
                    normalized_url,
                    storage,
                    byte_budget,
                    html_converter,
                )
                .await?;
                // The key uses the effective HTTPS URL. Never cache an HTTP
                // fallback response under that key, which would allow a later
                // explicit HTTPS request to receive downgraded content.
                self.cache
                    .insert(cache_key, processed.clone(), planned.used_http_fallback);
                Ok(WebFetchOutcome::Processed(processed))
            }
        }
    }

    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }

    fn bind_storage(&mut self, storage: &Storage) {
        if self
            .storage_scope
            .as_ref()
            .is_some_and(|current| current != storage)
        {
            self.cache.clear();
        }
        self.storage_scope = Some(storage.clone());
    }
}

fn node_platform() -> &'static str {
    if cfg!(target_os = "macos") {
        "darwin"
    } else if cfg!(target_os = "windows") {
        "win32"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else {
        std::env::consts::OS
    }
}

fn node_arch() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        arch => arch,
    }
}
