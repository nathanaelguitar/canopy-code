//! Restricted filesystem adapter for managed-memory extraction forks.

use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Mutex;

use canopy_core::agent_runtime::AgentToolExecutor;
use canopy_core::file_read_cache::FileReadCache;
use canopy_core::memory::{
    AutoMemoryPaths, MemoryScopedAgentConfig, MemoryScopedAgentConfigOptions,
    MemoryScopedBasePermissionManager,
};
use canopy_core::permissions::{PermissionCheckContext, PermissionDecision, PermissionRuleSet};
use canopy_core::providers::openai_request::InputModalities;
use canopy_core::shell_read_only::AstMemoryShellReadOnlyChecker;
use canopy_core::tool_response_finalizer::ToolExecutionOutput;
use canopy_core::tools::edit_file::EditFileTool;
use canopy_core::tools::glob::GlobTool;
use canopy_core::tools::grep::GrepTool;
use canopy_core::tools::list_directory::ListDirectoryTool;
use canopy_core::tools::read_file::ReadFileTool;
use canopy_core::tools::shell::ShellTool;
use canopy_core::tools::write_file::WriteFileTool;
use canopy_core::turn::ToolCallRequestInfo;
use canopy_core::utils::cancellation::CancellationToken;
use serde_json::{Value, json};

struct MemoryRootTools {
    root: PathBuf,
    read_file: ReadFileTool,
    list_directory: ListDirectoryTool,
    glob: GlobTool,
    grep: GrepTool,
    edit_file: EditFileTool,
    write_file: WriteFileTool,
}

impl MemoryRootTools {
    fn new(root: &Path, cache: &FileReadCache) -> Result<Option<Self>, String> {
        let canonical_root = match fs::canonicalize(root) {
            Ok(path) if path.is_dir() => path,
            Ok(_) => return Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(format!("could not resolve managed-memory root: {error}"));
            }
        };

        Ok(Some(Self {
            root: canonical_root.clone(),
            read_file: ReadFileTool::new_with_cache(&canonical_root, cache.clone())?,
            list_directory: ListDirectoryTool::new(&canonical_root, None)?,
            glob: GlobTool::new(&canonical_root, None)?,
            grep: GrepTool::new_with_cache(&canonical_root, None, None, cache.clone())?,
            edit_file: EditFileTool::new(&canonical_root, cache.clone())?,
            write_file: WriteFileTool::new(&canonical_root, cache.clone())?,
        }))
    }
}

struct BaseMemoryPermissions {
    rules: PermissionRuleSet,
    core_tools: Option<Vec<String>>,
    excluded_tools: Vec<String>,
}

impl MemoryScopedBasePermissionManager for BaseMemoryPermissions {
    fn has_relevant_rules(&self, _context: &PermissionCheckContext<'_>) -> bool {
        // Evaluating a rule set with no matching entries returns Default, so
        // it is safe and simpler to let the native matcher decide every time.
        true
    }

    fn has_matching_ask_rule(&self, context: &PermissionCheckContext<'_>) -> bool {
        self.rules.evaluate(context) == PermissionDecision::Ask
    }

    fn find_matching_deny_rule(&self, context: &PermissionCheckContext<'_>) -> Option<String> {
        (self.rules.evaluate(context) == PermissionDecision::Deny)
            .then(|| "permissions.deny rule".to_owned())
    }

    fn evaluate(&self, context: &PermissionCheckContext<'_>) -> PermissionDecision {
        self.rules.evaluate(context)
    }

    fn is_tool_enabled(&self, tool_name: &str) -> bool {
        canopy_core::tool_utils::is_tool_enabled(
            tool_name,
            self.core_tools.as_deref(),
            Some(&self.excluded_tools),
        )
    }
}

#[derive(Default)]
struct WrittenPaths {
    touched: Vec<String>,
    written: Vec<String>,
}

/// Executes only the memory planner's filesystem tools, and only with paths
/// inside the manager-derived project and user memory roots.
pub struct NativeMemoryExtractionTools {
    paths: AutoMemoryPaths,
    config: MemoryScopedAgentConfig,
    permissions: BaseMemoryPermissions,
    project: Option<MemoryRootTools>,
    user: Option<MemoryRootTools>,
    workspace_root: PathBuf,
    shell: ShellTool,
    shell_cancellation: CancellationToken,
    written_paths: Mutex<WrittenPaths>,
}

impl NativeMemoryExtractionTools {
    pub fn new(
        paths: AutoMemoryPaths,
        rules: PermissionRuleSet,
        core_tools: Option<Vec<String>>,
        excluded_tools: Vec<String>,
        environment: HashMap<String, String>,
    ) -> Result<Self, String> {
        let workspace_root = fs::canonicalize(paths.project_root())
            .map_err(|error| format!("could not resolve managed-memory workspace: {error}"))?;
        if !workspace_root.is_dir() {
            return Err("managed-memory workspace root is not a directory".to_owned());
        }
        let cache = FileReadCache::default();
        let project = MemoryRootTools::new(&paths.auto_memory_root(), &cache)?;
        if project.is_none() {
            return Err("project managed-memory root is unavailable".to_owned());
        }
        let user = MemoryRootTools::new(&paths.user_auto_memory_root(), &cache)?;
        let shell = ShellTool::new_with_env(&workspace_root, environment)?;
        let config = MemoryScopedAgentConfig::new(
            paths.clone(),
            MemoryScopedAgentConfigOptions {
                allow_shell: true,
                bypass_base_ask_for_scoped_paths: false,
                include_user_memory: true,
                protect_pinned_memory: true,
                restrict_reads_to_memory_paths: true,
            },
        );
        Ok(Self {
            paths,
            config,
            permissions: BaseMemoryPermissions {
                rules,
                core_tools,
                excluded_tools,
            },
            project,
            user,
            workspace_root,
            shell,
            shell_cancellation: CancellationToken::new(),
            written_paths: Mutex::new(WrittenPaths::default()),
        })
    }

    pub fn tool_declarations(&self) -> Vec<Value> {
        let tools = [
            (
                "read_file",
                canopy_core::tools::read_file::function_declaration(),
            ),
            (
                "grep_search",
                canopy_core::tools::grep::function_declaration(),
            ),
            ("glob", canopy_core::tools::glob::function_declaration()),
            (
                "list_directory",
                canopy_core::tools::list_directory::function_declaration(),
            ),
            (
                "write_file",
                canopy_core::tools::write_file::function_declaration(),
            ),
            (
                "edit",
                canopy_core::tools::edit_file::function_declaration(),
            ),
            (
                "run_shell_command",
                canopy_core::tools::shell::function_declaration(),
            ),
        ];
        tools
            .into_iter()
            .filter_map(|(name, mut declaration)| {
                if !self.config.is_tool_enabled(name, Some(&self.permissions)) {
                    return None;
                }
                declaration["name"] = json!(name);
                if let Some(description) = declaration
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                {
                    declaration["description"] = json!(format!(
                        "{description} Managed-memory extraction restricts this tool to the configured project and user memory roots."
                    ));
                }
                if name == "run_shell_command" {
                    declaration["description"] = json!(
                        "Run a foreground shell command in the workspace. Only commands classified as read-only by the Bash AST safety policy are allowed."
                    );
                    if let Some(properties) = declaration
                        .get_mut("parameters")
                        .and_then(|parameters| parameters.get_mut("properties"))
                        .and_then(Value::as_object_mut)
                    {
                        properties.remove("is_background");
                    }
                }
                Some(declaration)
            })
            .collect()
    }

    pub fn files_touched(&self) -> Vec<String> {
        self.written_paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .touched
            .clone()
    }

    pub fn files_written(&self) -> Vec<String> {
        self.written_paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .written
            .clone()
    }

    pub fn shell_cancellation_token(&self) -> CancellationToken {
        self.shell_cancellation.clone()
    }

    fn path_arg<'a>(&self, call: &'a ToolCallRequestInfo) -> Result<&'a str, String> {
        let key = match call.name.as_str() {
            "read_file" | "write_file" | "edit" => "file_path",
            "grep_search" | "glob" | "list_directory" => "path",
            "run_shell_command" => return Err("shell commands do not use a file path".to_owned()),
            _ => {
                return Err(format!(
                    "Tool `{}` is unavailable to memory extraction.",
                    call.name
                ));
            }
        };
        call.args
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .ok_or_else(|| format!("{key} must be an explicit absolute memory path."))
    }

    fn root_for_path(&self, path: &Path) -> Option<&MemoryRootTools> {
        let resolved = realpath_existing_or_new(path)?;
        self.project
            .as_ref()
            .filter(|tools| resolved.starts_with(&tools.root))
            .or_else(|| {
                self.user
                    .as_ref()
                    .filter(|tools| resolved.starts_with(&tools.root))
            })
    }

    async fn authorize(&self, call: &ToolCallRequestInfo, path: &Path) -> Result<(), String> {
        let canonical_tool = match call.name.as_str() {
            "grep_search" => "grep_search",
            "edit" => "edit",
            other => other,
        };
        if !canopy_core::tool_utils::is_tool_enabled(
            canonical_tool,
            self.permissions.core_tools.as_deref(),
            Some(&self.permissions.excluded_tools),
        ) {
            return Err(format!(
                "Tool `{canonical_tool}` is disabled by the configured tools.core/tools.exclude settings."
            ));
        }
        if !path.is_absolute() {
            return Err(format!(
                "Managed-memory tools require an absolute path: {}",
                path.display()
            ));
        }
        if !self.config.is_allowed_memory_path(Some(path)) || self.root_for_path(path).is_none() {
            return Err(format!(
                "{} is outside the managed-memory roots.",
                path.display()
            ));
        }

        let context = PermissionCheckContext {
            tool_name: canonical_tool,
            command: None,
            file_path: Some(path),
            domain: None,
            specifier: None,
            tool_params: Some(&call.args),
            project_root: self.config.project_root(),
            cwd: self.paths.project_root(),
        };
        let scoped = self
            .config
            .evaluate(
                &context,
                Some(&self.permissions),
                &AstMemoryShellReadOnlyChecker,
            )
            .await;
        if scoped == PermissionDecision::Allow {
            return Ok(());
        }

        // `glob` is deliberately not a scoped tool in the shared permission
        // policy, so this host adds its path check above and accepts only a
        // base-policy Default or Allow after that check. Ask is never silently
        // upgraded for background extraction.
        if canonical_tool == "glob" && scoped == PermissionDecision::Default {
            return Ok(());
        }
        Err(match scoped {
            PermissionDecision::Deny => {
                format!("{canonical_tool} blocked by managed-memory scope or permissions.deny.")
            }
            PermissionDecision::Ask => format!(
                "{canonical_tool} requires approval and was denied in background memory extraction."
            ),
            PermissionDecision::Default => {
                format!("{canonical_tool} is not allowed by managed-memory scope.")
            }
            PermissionDecision::Allow => unreachable!(),
        })
    }

    async fn authorize_shell(
        &self,
        call: &ToolCallRequestInfo,
        command: &str,
        cwd: &Path,
    ) -> Result<(), String> {
        if !canopy_core::tool_utils::is_tool_enabled(
            "run_shell_command",
            self.permissions.core_tools.as_deref(),
            Some(&self.permissions.excluded_tools),
        ) {
            return Err(
                "Tool `run_shell_command` is disabled by the configured tools.core/tools.exclude settings."
                    .to_owned(),
            );
        }
        let context = PermissionCheckContext {
            tool_name: "run_shell_command",
            command: Some(command),
            file_path: None,
            domain: None,
            specifier: None,
            tool_params: Some(&call.args),
            project_root: &self.workspace_root,
            cwd,
        };
        let scoped = self
            .config
            .evaluate(
                &context,
                Some(&self.permissions),
                &AstMemoryShellReadOnlyChecker,
            )
            .await;
        match scoped {
            PermissionDecision::Allow => Ok(()),
            PermissionDecision::Deny => Err(
                "run_shell_command blocked by managed-memory read-only scope or permissions.deny."
                    .to_owned(),
            ),
            PermissionDecision::Ask => Err(
                "run_shell_command requires approval and was denied in background memory extraction."
                    .to_owned(),
            ),
            PermissionDecision::Default => Err(
                "run_shell_command is not allowed by managed-memory scope.".to_owned(),
            ),
        }
    }

    async fn execute_shell(&self, call: &ToolCallRequestInfo) -> Result<String, String> {
        let command = call
            .args
            .get("command")
            .and_then(Value::as_str)
            .filter(|command| !command.trim().is_empty())
            .ok_or_else(|| "Command cannot be empty.".to_owned())?;
        if command.len() > 64 * 1024 {
            return Err("Shell command exceeds the 64 KiB input limit.".to_owned());
        }
        if call
            .args
            .get("is_background")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err("Managed-memory shell commands must run in the foreground.".to_owned());
        }
        let cwd = match call.args.get("directory") {
            None => self.workspace_root.clone(),
            Some(Value::String(directory)) if !directory.trim().is_empty() => {
                let requested = Path::new(directory);
                if !requested.is_absolute() {
                    return Err("Directory must be an absolute path.".to_owned());
                }
                let canonical = fs::canonicalize(requested)
                    .map_err(|error| format!("could not resolve command directory: {error}"))?;
                if !canonical.starts_with(&self.workspace_root) {
                    return Err(
                        "Shell commands are restricted to directories inside the workspace."
                            .to_owned(),
                    );
                }
                if !canonical.is_dir() {
                    return Err(format!(
                        "Command directory is not a directory: {}",
                        canonical.display()
                    ));
                }
                canonical
            }
            Some(_) => return Err("Directory must be a non-empty absolute path.".to_owned()),
        };
        self.authorize_shell(call, command, &cwd).await?;

        let mut args = call.args.clone();
        let object = args
            .as_object_mut()
            .ok_or_else(|| "Shell arguments must be an object.".to_owned())?;
        object.insert(
            "directory".to_owned(),
            Value::String(cwd.to_string_lossy().into_owned()),
        );
        object.insert("is_background".to_owned(), Value::Bool(false));
        self.shell
            .execute_with_cancellation(&args, self.shell_cancellation.clone())
            .await
    }

    fn record_touched(&self, paths: impl IntoIterator<Item = PathBuf>) {
        let mut tracked = self
            .written_paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for path in paths {
            push_unique(&mut tracked.touched, normalized_path_string(&path));
        }
    }

    fn record_written(&self, path: &Path) {
        let mut tracked = self
            .written_paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let path = normalized_path_string(path);
        push_unique(&mut tracked.touched, path.clone());
        push_unique(&mut tracked.written, path);
    }
}

impl AgentToolExecutor for NativeMemoryExtractionTools {
    fn execute<'a>(
        &'a self,
        call: &'a ToolCallRequestInfo,
    ) -> Pin<Box<dyn Future<Output = Result<ToolExecutionOutput, String>> + Send + 'a>> {
        Box::pin(async move {
            if call.name == "run_shell_command" {
                return self
                    .execute_shell(call)
                    .await
                    .map(ToolExecutionOutput::text);
            }
            let path = PathBuf::from(self.path_arg(call)?);
            self.authorize(call, &path).await?;
            let root = self
                .root_for_path(&path)
                .expect("path was authorized inside a configured root");
            let output = match call.name.as_str() {
                "read_file" => {
                    root.read_file
                        .execute_with_modalities(&call.args, InputModalities::default())
                        .await?
                }
                "list_directory" => {
                    let result = root.list_directory.execute(&call.args)?;
                    if let Some(error) = result.error {
                        return Err(error.message);
                    }
                    ToolExecutionOutput::text(result.llm_content)
                }
                "glob" => {
                    let result = root.glob.execute(&call.args)?;
                    self.record_touched(result.result_file_paths.clone());
                    ToolExecutionOutput {
                        output: result.llm_content,
                        result_file_paths: result
                            .result_file_paths
                            .into_iter()
                            .map(|path| path.to_string_lossy().into_owned())
                            .collect(),
                        ..ToolExecutionOutput::default()
                    }
                }
                "grep_search" => {
                    let result = root.grep.execute(&call.args)?;
                    self.record_touched(result.result_file_paths.clone());
                    ToolExecutionOutput {
                        output: result.llm_content,
                        result_file_paths: result
                            .result_file_paths
                            .into_iter()
                            .map(|path| path.to_string_lossy().into_owned())
                            .collect(),
                        ..ToolExecutionOutput::default()
                    }
                }
                "write_file" => {
                    ToolExecutionOutput::text(root.write_file.execute(&call.args, true)?)
                }
                "edit" => ToolExecutionOutput::text(root.edit_file.execute(&call.args, true)?),
                _ => {
                    return Err(format!(
                        "Tool `{}` is unavailable to memory extraction.",
                        call.name
                    ));
                }
            };

            if call.name == "write_file" || call.name == "edit" {
                self.record_written(&path);
            } else if call.name == "read_file" || call.name == "list_directory" {
                self.record_touched([path]);
            }
            Ok(output)
        })
    }

    fn is_side_effecting(&self, tool_name: &str) -> bool {
        matches!(tool_name, "write_file" | "edit")
    }

    fn is_concurrency_safe(&self, call: &ToolCallRequestInfo) -> bool {
        matches!(
            call.name.as_str(),
            "read_file" | "grep_search" | "glob" | "list_directory" | "run_shell_command"
        )
    }
}

fn realpath_existing_or_new(path: &Path) -> Option<PathBuf> {
    if let Ok(path) = fs::canonicalize(path) {
        return Some(path);
    }
    let mut missing = Vec::new();
    let mut current = path.to_path_buf();
    loop {
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => return None,
            Ok(_) => {
                let mut resolved = fs::canonicalize(current).ok()?;
                for component in missing.iter().rev() {
                    resolved.push(component);
                }
                return Some(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = current.file_name()?.to_os_string();
                missing.push(name);
                current = current.parent()?.to_path_buf();
            }
            Err(_) => return None,
        }
    }
}

fn normalized_path_string(path: &Path) -> String {
    fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .into_owned()
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}
