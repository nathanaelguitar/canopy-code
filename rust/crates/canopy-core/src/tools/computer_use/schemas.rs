use std::collections::BTreeMap;
use std::sync::LazyLock;

use serde::Deserialize;
use serde_json::Value;

/// JSON schema and provider-facing description for one pinned cua-driver tool.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ComputerUseToolSchema {
    pub description: String,
    pub parameter_schema: Value,
}

#[derive(Deserialize)]
struct ComputerUseCatalog {
    tool_names: Vec<String>,
    schemas: BTreeMap<String, ComputerUseToolSchema>,
}

static CATALOG: LazyLock<ComputerUseCatalog> = LazyLock::new(|| {
    serde_json::from_str(include_str!("schemas-v0.5.2.json"))
        .expect("checked-in computer-use schema catalog must be valid JSON")
});

/// Canonically ordered upstream tool names for cua-driver-rs v0.5.2.
pub fn computer_use_tool_names() -> &'static [String] {
    &CATALOG.tool_names
}

/// Look up the pinned schema by upstream cua-driver tool name.
pub fn computer_use_tool_schema(name: &str) -> Option<&'static ComputerUseToolSchema> {
    CATALOG.schemas.get(name)
}

/// Return the registered Canopy name for a known upstream tool.
pub fn canopy_tool_name(upstream_name: &str) -> Option<String> {
    CATALOG
        .schemas
        .contains_key(upstream_name)
        .then(|| format!("computer_use__{upstream_name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_names_and_schema_keys_are_the_same_35_tools() {
        assert_eq!(computer_use_tool_names().len(), 35);
        assert_eq!(CATALOG.schemas.len(), computer_use_tool_names().len());
        assert!(computer_use_tool_schema("get_window_state").is_some());
        assert!(computer_use_tool_schema("not_a_tool").is_none());
        assert_eq!(
            canopy_tool_name("click").as_deref(),
            Some("computer_use__click")
        );
        assert_eq!(canopy_tool_name("not_a_tool"), None);
    }

    #[test]
    fn schemas_retain_json_schema_descriptions_and_open_object_flags() {
        let schema = computer_use_tool_schema("click").unwrap();
        assert!(schema.description.contains("element_index"));
        assert_eq!(schema.parameter_schema["type"], "object");
        assert_eq!(schema.parameter_schema["additionalProperties"], false);
        assert_eq!(schema.parameter_schema["required"][0], "pid");

        let session = computer_use_tool_schema("start_session").unwrap();
        assert_eq!(session.parameter_schema["additionalProperties"], true);
    }
}
