// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//!
//! Session-owned response cache for WebFetch, mirroring the 15-minute,
//! 32-entry cache in `packages/core/src/tools/web-fetch.ts`.
//!
//! The host should own one cache alongside a session's `Storage` and replace
//! it when that storage is relocated. This deliberately has no process-global
//! registry: cached persisted paths are only visible through the owning
//! cache instance. Keys still include the session ID so session swaps cannot
//! reuse entries if a host retains an instance longer than intended.

use crate::tools::web::fetch_processing::FetchProcessedResponse;
use crate::utils::lru_cache::LruCache;
use std::time::{SystemTime, UNIX_EPOCH};

pub const WEB_FETCH_CACHE_TTL_MS: u64 = 15 * 60 * 1000;
pub const WEB_FETCH_CACHE_CAPACITY: usize = 32;
pub const WEB_FETCH_MAX_CACHEABLE_UTF16_CHARS: usize = 2 * 1024 * 1024;

/// Cache identity matches the TypeScript composite key. Keeping the fields
/// separate avoids delimiter collisions while preserving all three inputs.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FetchCacheKey {
    pub session_id: String,
    pub accept_header: String,
    pub fetch_url: String,
}

impl FetchCacheKey {
    pub fn new(
        session_id: impl Into<String>,
        accept_header: impl Into<String>,
        fetch_url: impl Into<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            accept_header: accept_header.into(),
            fetch_url: fetch_url.into(),
        }
    }
}

#[derive(Clone, Debug)]
struct CachedFetchResponse {
    fetched_at_ms: u64,
    response: FetchProcessedResponse,
}

/// An LRU cache owned by one host session/storage scope. It stores fully
/// processed response metadata and model-facing content, including any
/// persisted path from the owning storage directory.
pub struct WebFetchResponseCache {
    entries: LruCache<FetchCacheKey, CachedFetchResponse>,
}

impl Default for WebFetchResponseCache {
    fn default() -> Self {
        Self::new()
    }
}

impl WebFetchResponseCache {
    pub fn new() -> Self {
        Self {
            entries: LruCache::new(WEB_FETCH_CACHE_CAPACITY),
        }
    }

    pub fn get(
        &mut self,
        key: &FetchCacheKey,
        requested_url: &str,
    ) -> Option<FetchProcessedResponse> {
        self.get_at(key, requested_url, current_time_ms())
    }

    pub fn insert(
        &mut self,
        key: FetchCacheKey,
        response: FetchProcessedResponse,
        used_http_fallback: bool,
    ) -> bool {
        self.insert_at(key, response, current_time_ms(), used_http_fallback)
    }

    /// Get a fresh cached response and promote it to most-recently-used.
    /// `requested_url` is per invocation in the source tool, so it is applied
    /// to the cloned metadata even when the fetch URL matches a prior call.
    pub fn get_at(
        &mut self,
        key: &FetchCacheKey,
        requested_url: &str,
        now_ms: u64,
    ) -> Option<FetchProcessedResponse> {
        let cached = (*self.entries.get(key)?).clone();
        // Match JS's `Date.now() - fetchedAt < TTL`; a backwards wall-clock
        // adjustment leaves an entry fresh until the clock catches up.
        if now_ms.saturating_sub(cached.fetched_at_ms) >= WEB_FETCH_CACHE_TTL_MS {
            return None;
        }
        let mut response = cached.response;
        response.requested_url = requested_url.to_owned();
        Some(response)
    }

    /// Insert an already processed response. Fallback responses are never
    /// cached under the upgraded HTTPS key, error statuses are excluded, and
    /// the cache-size guard uses UTF-16 code units like JavaScript String.len.
    /// Returns true when the entry was stored.
    pub fn insert_at(
        &mut self,
        key: FetchCacheKey,
        response: FetchProcessedResponse,
        fetched_at_ms: u64,
        used_http_fallback: bool,
    ) -> bool {
        if used_http_fallback
            || !(200..300).contains(&response.status)
            || utf16_char_count(&response.content) > WEB_FETCH_MAX_CACHEABLE_UTF16_CHARS
        {
            return false;
        }
        self.entries.set(
            key,
            CachedFetchResponse {
                fetched_at_ms,
                response,
            },
        );
        true
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

fn utf16_char_count(value: &str) -> usize {
    value.encode_utf16().count()
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::binary_content::ExtensionSource;

    fn response(requested_url: &str, content: &str) -> FetchProcessedResponse {
        FetchProcessedResponse {
            requested_url: requested_url.to_owned(),
            final_url: "https://example.test/final".to_owned(),
            status: 200,
            status_text: "OK".to_owned(),
            content_type: "text/plain".to_owned(),
            byte_length: content.len(),
            content: content.to_owned(),
            is_binary: false,
            persisted: None,
            pdf_extraction_error: None,
            html_conversion_error: None,
            sniffed_kind: crate::utils::binary_content::SniffedFileKind {
                extension: "bin".to_owned(),
                mime_type: "text/plain".to_owned(),
                magic_matched: false,
                extension_source: ExtensionSource::Fallback,
            },
        }
    }

    fn key(session: &str, accept: &str, url: &str) -> FetchCacheKey {
        FetchCacheKey::new(session, accept, url)
    }

    #[test]
    fn key_includes_session_accept_header_and_fetch_url() {
        let mut cache = WebFetchResponseCache::new();
        let original_key = key("session-a", "text/markdown", "https://example.test/page");
        assert!(cache.insert_at(
            original_key.clone(),
            response("http://example.test/page", "cached body"),
            100,
            false,
        ));

        assert!(
            cache
                .get_at(&original_key, "https://example.test/page", 101)
                .is_some()
        );
        assert!(
            cache
                .get_at(
                    &key("session-b", "text/markdown", "https://example.test/page"),
                    "https://example.test/page",
                    101,
                )
                .is_none()
        );
        assert!(
            cache
                .get_at(
                    &key("session-a", "text/html", "https://example.test/page"),
                    "https://example.test/page",
                    101,
                )
                .is_none()
        );
        assert!(
            cache
                .get_at(
                    &key("session-a", "text/markdown", "https://example.test/other"),
                    "https://example.test/other",
                    101,
                )
                .is_none()
        );
    }

    #[test]
    fn ttl_is_fifteen_minutes_and_current_request_url_is_projected() {
        let mut cache = WebFetchResponseCache::new();
        let cache_key = key("session", "accept", "https://example.test/page");
        cache.insert_at(
            cache_key.clone(),
            response("http://example.test/page", "content"),
            1_000,
            false,
        );

        let fresh = cache
            .get_at(
                &cache_key,
                "https://example.test/page",
                1_000 + WEB_FETCH_CACHE_TTL_MS - 1,
            )
            .unwrap();
        assert_eq!(fresh.requested_url, "https://example.test/page");
        assert_eq!(fresh.content, "content");
        assert!(
            cache
                .get_at(
                    &cache_key,
                    "https://example.test/page",
                    1_000 + WEB_FETCH_CACHE_TTL_MS,
                )
                .is_none()
        );
    }

    #[test]
    fn skips_fallback_error_and_oversized_content_entries() {
        let mut cache = WebFetchResponseCache::new();
        let fallback_key = key("s", "a", "https://example.test/fallback");
        assert!(!cache.insert_at(
            fallback_key.clone(),
            response("http://example.test/fallback", "plaintext fallback"),
            1,
            true,
        ));
        assert!(
            cache
                .get_at(&fallback_key, "http://example.test/fallback", 2)
                .is_none()
        );

        let error_key = key("s", "a", "https://example.test/error");
        let mut error_response = response("https://example.test/error", "error body");
        error_response.status = 503;
        assert!(!cache.insert_at(error_key, error_response, 1, false));

        let oversized_key = key("s", "a", "https://example.test/large");
        let mut oversized = response("https://example.test/large", "");
        oversized.content = "😀".repeat(WEB_FETCH_MAX_CACHEABLE_UTF16_CHARS / 2 + 1);
        assert!(!cache.insert_at(oversized_key, oversized, 1, false));
    }

    #[test]
    fn accepts_the_exact_utf16_limit() {
        let mut cache = WebFetchResponseCache::new();
        let cache_key = key("s", "a", "https://example.test/limit");
        let mut at_limit = response("https://example.test/limit", "");
        at_limit.content = "😀".repeat(WEB_FETCH_MAX_CACHEABLE_UTF16_CHARS / 2);
        assert!(cache.insert_at(cache_key.clone(), at_limit, 1, false));
        assert!(
            cache
                .get_at(&cache_key, "https://example.test/limit", 2)
                .is_some()
        );
    }

    #[test]
    fn evicts_lru_at_capacity_and_promotes_hits() {
        let mut cache = WebFetchResponseCache::new();
        let make_key = |index| {
            FetchCacheKey::new("session", "accept", format!("https://example.test/{index}"))
        };
        for index in 0..WEB_FETCH_CACHE_CAPACITY {
            let cache_key = make_key(index);
            assert!(cache.insert_at(
                cache_key,
                response("https://example.test", &index.to_string()),
                1,
                false,
            ));
        }

        let key_zero = make_key(0);
        assert!(
            cache
                .get_at(&key_zero, "https://example.test/0", 2)
                .is_some()
        );
        let newest = make_key(WEB_FETCH_CACHE_CAPACITY);
        assert!(cache.insert_at(
            newest,
            response("https://example.test/new", "new"),
            3,
            false,
        ));

        assert!(
            cache
                .get_at(&key_zero, "https://example.test/0", 4)
                .is_some()
        );
        assert!(
            cache
                .get_at(&make_key(1), "https://example.test/1", 4)
                .is_none()
        );
    }

    #[test]
    fn cache_instances_do_not_share_persisted_response_paths() {
        let mut first_session = WebFetchResponseCache::new();
        let mut second_session = WebFetchResponseCache::new();
        let cache_key = key("session", "accept", "https://example.test/report.pdf");
        let mut persisted = response("https://example.test/report.pdf", "extracted text");
        persisted.is_binary = true;
        persisted.persisted = Some(crate::tools::web::fetch_processing::PersistedFetchBinary {
            filepath: "/session-a/tool-results/report.pdf".into(),
            size: 100,
            mime_type: "application/pdf".to_owned(),
        });
        assert!(first_session.insert_at(cache_key.clone(), persisted, 1, false));

        let hit = first_session
            .get_at(&cache_key, "https://example.test/report.pdf", 2)
            .unwrap();
        assert_eq!(
            hit.persisted.as_ref().unwrap().filepath,
            std::path::PathBuf::from("/session-a/tool-results/report.pdf")
        );
        assert!(
            second_session
                .get_at(&cache_key, "https://example.test/report.pdf", 2)
                .is_none()
        );
    }
}
