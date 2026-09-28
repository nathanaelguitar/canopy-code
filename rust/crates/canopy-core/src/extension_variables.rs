// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Public extension variable contracts and hydration helpers.
//!
//! The implementation is shared with [`crate::extensions`] so existing
//! extension callers keep one source of truth for placeholder and JavaScript
//! replacement semantics.

pub use crate::extensions::{
    JsonValue, VARIABLE_SCHEMA, VariableContext, VariableDefinition, VariableSchema,
    hydrate_string, recursively_hydrate_strings, substitute_hook_variables, validate_variables,
};

/// Context used while loading an extension, matching `LoadExtensionContext`
/// from the TypeScript extension schema.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadExtensionContext {
    pub extension_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_dir: Option<String>,
}
