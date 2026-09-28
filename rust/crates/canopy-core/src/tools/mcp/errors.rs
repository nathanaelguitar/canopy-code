//! Typed runtime MCP server-add errors from `mcp-errors.ts`.

use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpBudgetWouldExceedError {
    pub server_name: String,
}

impl McpBudgetWouldExceedError {
    pub const NAME: &'static str = "McpBudgetWouldExceedError";
    pub const CODE: &'static str = "mcp_budget_would_exceed";

    pub fn new(server_name: impl Into<String>) -> Self {
        Self {
            server_name: server_name.into(),
        }
    }

    pub const fn name(&self) -> &'static str {
        Self::NAME
    }

    pub const fn code(&self) -> &'static str {
        Self::CODE
    }

    pub fn message(&self) -> String {
        self.to_string()
    }
}

impl Display for McpBudgetWouldExceedError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Adding '{}' would exceed workspace MCP budget",
            self.server_name
        )
    }
}

impl Error for McpBudgetWouldExceedError {}

/// Optional spawn failure fields. Empty fields are omitted from the error's
/// message, matching JavaScript `JSON.stringify` on the corresponding object.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpServerSpawnFailureDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpServerSpawnFailedError {
    pub server_name: String,
    pub details: McpServerSpawnFailureDetails,
}

impl McpServerSpawnFailedError {
    pub const NAME: &'static str = "McpServerSpawnFailedError";
    pub const CODE: &'static str = "mcp_server_spawn_failed";

    pub fn new(server_name: impl Into<String>, details: McpServerSpawnFailureDetails) -> Self {
        Self {
            server_name: server_name.into(),
            details,
        }
    }

    pub const fn name(&self) -> &'static str {
        Self::NAME
    }

    pub const fn code(&self) -> &'static str {
        Self::CODE
    }

    pub fn message(&self) -> String {
        self.to_string()
    }
}

impl Display for McpServerSpawnFailedError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let details_json = serde_json::to_string(&self.details).unwrap_or_else(|_| "{}".into());
        write!(
            formatter,
            "Failed to spawn MCP server '{}': {details_json}",
            self.server_name
        )
    }
}

impl Error for McpServerSpawnFailedError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidMcpConfigError {
    pub server_name: String,
    pub reason: String,
}

impl InvalidMcpConfigError {
    pub const NAME: &'static str = "InvalidMcpConfigError";
    pub const CODE: &'static str = "invalid_config";

    pub fn new(server_name: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            server_name: server_name.into(),
            reason: reason.into(),
        }
    }

    pub const fn name(&self) -> &'static str {
        Self::NAME
    }

    pub const fn code(&self) -> &'static str {
        Self::CODE
    }

    pub fn message(&self) -> String {
        self.to_string()
    }
}

impl Display for InvalidMcpConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Invalid MCP server config for '{}': {}",
            self.server_name, self.reason
        )
    }
}

impl Error for InvalidMcpConfigError {}

#[cfg(test)]
mod tests {
    use super::{
        InvalidMcpConfigError, McpBudgetWouldExceedError, McpServerSpawnFailedError,
        McpServerSpawnFailureDetails,
    };

    #[test]
    fn budget_error_preserves_code_name_server_and_message() {
        let error = McpBudgetWouldExceedError::new("echo");
        assert_eq!(error.name(), "McpBudgetWouldExceedError");
        assert_eq!(error.code(), "mcp_budget_would_exceed");
        assert_eq!(error.server_name, "echo");
        assert_eq!(
            error.message(),
            "Adding 'echo' would exceed workspace MCP budget"
        );
    }

    #[test]
    fn spawn_error_preserves_optional_details_and_json_message_shape() {
        let error = McpServerSpawnFailedError::new(
            "echo",
            McpServerSpawnFailureDetails {
                exit_code: Some(7),
                stderr: Some("missing executable".into()),
                timeout: Some(false),
            },
        );
        assert_eq!(error.name(), "McpServerSpawnFailedError");
        assert_eq!(error.code(), "mcp_server_spawn_failed");
        assert_eq!(error.server_name, "echo");
        assert_eq!(
            error.message(),
            "Failed to spawn MCP server 'echo': {\"exitCode\":7,\"stderr\":\"missing executable\",\"timeout\":false}"
        );

        let sparse = McpServerSpawnFailedError::new(
            "echo",
            McpServerSpawnFailureDetails {
                stderr: Some("timed out".into()),
                timeout: Some(true),
                ..McpServerSpawnFailureDetails::default()
            },
        );
        assert_eq!(
            sparse.message(),
            "Failed to spawn MCP server 'echo': {\"stderr\":\"timed out\",\"timeout\":true}"
        );
    }

    #[test]
    fn invalid_config_error_preserves_code_name_reason_and_message() {
        let error = InvalidMcpConfigError::new("echo", "missing command or URL");
        assert_eq!(error.name(), "InvalidMcpConfigError");
        assert_eq!(error.code(), "invalid_config");
        assert_eq!(error.server_name, "echo");
        assert_eq!(error.reason, "missing command or URL");
        assert_eq!(
            error.message(),
            "Invalid MCP server config for 'echo': missing command or URL"
        );
    }
}
