//! Registry of prompts discovered from MCP servers (`prompts/list`).
//!
//! Source: `packages/core/src/prompts/prompt-registry.ts`. Duplicate prompt
//! names are stored under `<server_name>_<name>`, matching the source
//! registry's rename-on-collision behavior. Returned `Arc` handles preserve
//! object identity for callers holding a prompt across registry updates.
//!
//! Prompt list ordering uses Rust's Unicode scalar lexicographic order. The
//! TypeScript source uses the host runtime's locale-sensitive `localeCompare`,
//! so ordering can differ for case, accents, and punctuation. The Rust
//! `McpPrompt` also retains the original protocol object in `raw`; a collision
//! rename updates `name` but leaves that raw server payload unchanged.
//! This module has no logger, so it does not emit the source's collision
//! warning.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::tools::mcp::client_runtime::McpPrompt;

/// Shared handle returned by prompt registry lookups.
pub type SharedMcpPrompt = Arc<McpPrompt>;

/// In-memory registry of prompts advertised by connected MCP servers.
///
/// The map is keyed by the prompt's registered name. A `BTreeMap` makes
/// iteration deterministic; the public list methods explicitly sort by name
/// to make their ordering contract clear.
#[derive(Default)]
pub struct PromptRegistry {
    prompts: BTreeMap<String, SharedMcpPrompt>,
}

impl PromptRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a prompt, prefixing its server name when its name is taken.
    ///
    /// This mirrors the source's single collision check: if the original name
    /// exists, the prompt is inserted under `<server_name>_<name>`, replacing
    /// any existing entry under that resulting name.
    pub fn register_prompt(&mut self, mut prompt: McpPrompt) {
        if self.prompts.contains_key(&prompt.name) {
            prompt.name = format!("{}_{}", prompt.server_name, prompt.name);
        }
        self.prompts.insert(prompt.name.clone(), Arc::new(prompt));
    }

    /// Return all registered prompts ordered by name.
    pub fn get_all_prompts(&self) -> Vec<SharedMcpPrompt> {
        let mut prompts = self.prompts.values().cloned().collect::<Vec<_>>();
        prompts.sort_by(|left, right| left.name.cmp(&right.name));
        prompts
    }

    /// Look up a registered prompt by name.
    pub fn get_prompt(&self, name: &str) -> Option<SharedMcpPrompt> {
        self.prompts.get(name).cloned()
    }

    /// Return prompts from one server, ordered by prompt name.
    pub fn get_prompts_by_server(&self, server_name: &str) -> Vec<SharedMcpPrompt> {
        let mut prompts = self
            .prompts
            .values()
            .filter(|prompt| prompt.server_name == server_name)
            .cloned()
            .collect::<Vec<_>>();
        prompts.sort_by(|left, right| left.name.cmp(&right.name));
        prompts
    }

    /// Remove every registered prompt.
    pub fn clear(&mut self) {
        self.prompts.clear();
    }

    /// Remove every prompt advertised by `server_name`.
    pub fn remove_prompts_by_server(&mut self, server_name: &str) {
        self.prompts
            .retain(|_, prompt| prompt.server_name != server_name);
    }

    /// Number of currently registered prompts.
    pub fn len(&self) -> usize {
        self.prompts.len()
    }

    /// Whether the registry contains no prompts.
    pub fn is_empty(&self) -> bool {
        self.prompts.is_empty()
    }
}
