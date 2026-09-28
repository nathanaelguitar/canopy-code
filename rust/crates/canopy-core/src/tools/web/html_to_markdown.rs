// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! HTML-to-Markdown conversion for WebFetch.
//!
//! This adapter uses the MIT-licensed `html2md-rs` parser with WebFetch's
//! source-specific omissions: images, scripts, styles, and noscript content
//! are removed, while link destinations are retained.

use html2md_rs::structs::{NodeType, ToMdConfig};
use html2md_rs::to_md::safe_from_html_to_md_with_config;

use super::fetch_processing::HtmlToMarkdownConverter;

/// HTML converter configured to match WebFetch's Turndown rules.
#[derive(Debug)]
pub struct TurndownCompatibleHtmlConverter {
    config: ToMdConfig,
}

impl TurndownCompatibleHtmlConverter {
    pub fn new() -> Self {
        let mut config = ToMdConfig::default();
        config.ignore_rendering = ["img", "script", "style", "noscript"]
            .into_iter()
            .map(NodeType::from_tag_str)
            .collect();
        Self { config }
    }
}

impl Default for TurndownCompatibleHtmlConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl HtmlToMarkdownConverter for TurndownCompatibleHtmlConverter {
    fn convert(&self, html: &str) -> Result<String, String> {
        safe_from_html_to_md_with_config(html.to_owned(), &self.config)
            .map_err(|error| error.to_string())
    }
}
