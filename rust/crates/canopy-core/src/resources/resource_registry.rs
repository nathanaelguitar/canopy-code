//! Registry of resources discovered from MCP servers (`resources/list`).
//!
//! Source: `packages/core/src/resources/resource-registry.ts`. Resource
//! identity is the `(server_name, uri)` pair, so separate servers may expose
//! the same URI without replacing one another. Shared `Arc` handles preserve
//! the source registry's object identity when values are returned to callers.

use std::collections::HashMap;
use std::sync::Arc;

use crate::tools::mcp::client_runtime::McpResource;

/// Shared handle returned by resource registry lookups.
pub type SharedMcpResource = Arc<McpResource>;

/// In-memory registry of resources advertised by connected MCP servers.
///
/// A tuple key is used instead of concatenating strings with a separator. It
/// represents the intended pair identity directly and cannot collide even if
/// a caller supplies unusual strings containing NUL characters.
#[derive(Default)]
pub struct ResourceRegistry {
    resources: HashMap<(String, String), SharedMcpResource>,
}

impl ResourceRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a resource, replacing a prior registration with the same
    /// `(server_name, uri)` identity.
    pub fn register_resource(&mut self, resource: McpResource) {
        let key = (resource.server_name.clone(), resource.uri.clone());
        self.resources.insert(key, Arc::new(resource));
    }

    /// Return all resources ordered by server name, then URI.
    ///
    /// Ordering follows Rust's Unicode scalar (lexicographic) ordering. The
    /// TypeScript implementation uses the host runtime's locale-sensitive
    /// `localeCompare`; Rust's standard library has no matching collation API.
    pub fn get_all_resources(&self) -> Vec<SharedMcpResource> {
        let mut resources = self.resources.values().cloned().collect::<Vec<_>>();
        resources.sort_by(|left, right| {
            left.server_name
                .cmp(&right.server_name)
                .then_with(|| left.uri.cmp(&right.uri))
        });
        resources
    }

    /// Return the resources from one server ordered by URI.
    pub fn get_resources_by_server(&self, server_name: &str) -> Vec<SharedMcpResource> {
        let mut resources = self
            .resources
            .values()
            .filter(|resource| resource.server_name == server_name)
            .cloned()
            .collect::<Vec<_>>();
        resources.sort_by(|left, right| left.uri.cmp(&right.uri));
        resources
    }

    /// Look up one resource by its `(server_name, uri)` identity.
    pub fn get_resource(&self, server_name: &str, uri: &str) -> Option<SharedMcpResource> {
        self.resources
            .get(&(server_name.to_owned(), uri.to_owned()))
            .cloned()
    }

    /// Remove every registered resource.
    pub fn clear(&mut self) {
        self.resources.clear();
    }

    /// Remove every resource advertised by `server_name`.
    pub fn remove_resources_by_server(&mut self, server_name: &str) {
        self.resources
            .retain(|(registered_server, _), _| registered_server != server_name);
    }

    /// Number of currently registered resources.
    pub fn len(&self) -> usize {
        self.resources.len()
    }

    /// Whether the registry contains no resources.
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn resource(uri: &str, server_name: &str, name: &str) -> McpResource {
        McpResource {
            server_name: server_name.to_owned(),
            uri: uri.to_owned(),
            name: name.to_owned(),
            description: None,
            mime_type: None,
            raw: json!({ "uri": uri, "name": name }),
        }
    }

    #[test]
    fn resource_is_addressable_by_server_and_uri() {
        let mut registry = ResourceRegistry::new();
        registry.register_resource(resource("file:///a.txt", "server-a", "a"));

        let found = registry.get_resource("server-a", "file:///a.txt").unwrap();
        assert_eq!(found.name, "a");
        assert!(registry.get_resource("server-b", "file:///a.txt").is_none());
    }

    #[test]
    fn same_uri_from_different_servers_does_not_collide() {
        let mut registry = ResourceRegistry::new();
        registry.register_resource(resource("file:///shared.txt", "server-a", "from a"));
        registry.register_resource(resource("file:///shared.txt", "server-b", "from b"));

        assert_eq!(
            registry
                .get_resource("server-a", "file:///shared.txt")
                .unwrap()
                .name,
            "from a"
        );
        assert_eq!(
            registry
                .get_resource("server-b", "file:///shared.txt")
                .unwrap()
                .name,
            "from b"
        );
        assert_eq!(registry.get_all_resources().len(), 2);
    }

    #[test]
    fn re_registration_replaces_the_pair_and_preserves_old_shared_handle() {
        let mut registry = ResourceRegistry::new();
        registry.register_resource(resource("file:///a.txt", "server-a", "old"));
        let first = registry.get_resource("server-a", "file:///a.txt").unwrap();
        registry.register_resource(resource("file:///a.txt", "server-a", "new"));
        let current = registry.get_resource("server-a", "file:///a.txt").unwrap();

        assert_eq!(current.name, "new");
        assert_eq!(first.name, "old");
        assert!(!Arc::ptr_eq(&first, &current));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn get_all_is_empty_then_sorted_by_server_and_uri() {
        let mut registry = ResourceRegistry::new();
        assert!(registry.get_all_resources().is_empty());
        registry.register_resource(resource("file:///z.txt", "server-b", "z"));
        registry.register_resource(resource("file:///a.txt", "server-b", "a"));
        registry.register_resource(resource("file:///m.txt", "server-a", "m"));

        let sorted = registry
            .get_all_resources()
            .iter()
            .map(|resource| format!("{}:{}", resource.server_name, resource.uri))
            .collect::<Vec<_>>();
        assert_eq!(
            sorted,
            [
                "server-a:file:///m.txt",
                "server-b:file:///a.txt",
                "server-b:file:///z.txt",
            ]
        );
    }

    #[test]
    fn get_resources_by_server_is_filtered_and_sorted_by_uri() {
        let mut registry = ResourceRegistry::new();
        registry.register_resource(resource("file:///z.txt", "server-a", "z"));
        registry.register_resource(resource("file:///a.txt", "server-a", "a"));
        registry.register_resource(resource("file:///c.txt", "server-b", "c"));

        let found = registry.get_resources_by_server("server-a");
        assert_eq!(
            found
                .iter()
                .map(|resource| resource.uri.as_str())
                .collect::<Vec<_>>(),
            ["file:///a.txt", "file:///z.txt"]
        );
        assert!(registry.get_resources_by_server("unknown").is_empty());
    }

    #[test]
    fn clear_removes_all_entries() {
        let mut registry = ResourceRegistry::new();
        registry.register_resource(resource("file:///a.txt", "server-a", "a"));
        registry.register_resource(resource("file:///b.txt", "server-b", "b"));

        registry.clear();

        assert!(registry.is_empty());
        assert!(registry.get_all_resources().is_empty());
    }

    #[test]
    fn remove_server_preserves_other_servers_and_unknown_removal_is_noop() {
        let mut registry = ResourceRegistry::new();
        registry.register_resource(resource("file:///a.txt", "server-a", "a"));
        registry.register_resource(resource("file:///b.txt", "server-a", "b"));
        registry.register_resource(resource("file:///c.txt", "server-b", "c"));

        registry.remove_resources_by_server("unknown");
        assert_eq!(registry.len(), 3);
        registry.remove_resources_by_server("server-a");

        let remaining = registry.get_all_resources();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].uri, "file:///c.txt");
    }

    #[test]
    fn pair_key_does_not_confuse_embedded_nul_characters() {
        let mut registry = ResourceRegistry::new();
        registry.register_resource(resource("c", "a\0b", "first"));
        registry.register_resource(resource("b\0c", "a", "second"));

        assert_eq!(registry.len(), 2);
        assert_eq!(registry.get_resource("a\0b", "c").unwrap().name, "first");
        assert_eq!(registry.get_resource("a", "b\0c").unwrap().name, "second");
    }

    #[test]
    fn resource_payload_is_retained() {
        let mut registry = ResourceRegistry::new();
        let mut resource = resource("custom://document", "server-a", "doc");
        resource.description = Some("details".to_owned());
        resource.mime_type = Some("text/plain".to_owned());
        resource.raw = json!({
            "uri": "custom://document",
            "name": "doc",
            "title": "Document",
            "annotations": { "priority": 0.7 },
            "_meta": { "vendor": { "revision": 2 } }
        });
        registry.register_resource(resource);

        let stored = registry
            .get_resource("server-a", "custom://document")
            .unwrap();
        assert_eq!(stored.description.as_deref(), Some("details"));
        assert_eq!(stored.mime_type.as_deref(), Some("text/plain"));
        assert_eq!(stored.raw["title"], "Document");
        assert_eq!(stored.raw["_meta"]["vendor"]["revision"], 2);
    }
}
