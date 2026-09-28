use std::fs::OpenOptions;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::file_read_cache::FileReadCache;
use crate::notebook::{NotebookEditParams, apply_notebook_edit};
use crate::services::commit_attribution::CommitAttributionService;
use crate::services::file_history::FileHistoryService;
use crate::tools::write_file::{WriteFilePreview, WriteFileTool};
use serde_json::{Value, json};
use tokio::sync::Mutex;

const MAX_NOTEBOOK_BYTES: u64 = 8 * 1024 * 1024;

pub struct NotebookEditTool {
    workspace_root: PathBuf,
    writer: WriteFileTool,
}

impl NotebookEditTool {
    pub fn new(
        workspace_root: impl AsRef<Path>,
        file_read_cache: FileReadCache,
    ) -> Result<Self, String> {
        let workspace_root = std::fs::canonicalize(workspace_root.as_ref())
            .map_err(|error| format!("could not resolve workspace root: {error}"))?;
        let writer = WriteFileTool::new(&workspace_root, file_read_cache.clone())?;
        Ok(Self {
            workspace_root,
            writer,
        })
    }

    /// Attach the session's shared file-history service to this notebook tool.
    pub fn with_file_history(mut self, file_history: Arc<Mutex<FileHistoryService>>) -> Self {
        self.writer = self.writer.with_file_history(file_history);
        self
    }

    /// Replace the session's shared file-history service after construction.
    pub fn set_file_history(&mut self, file_history: Arc<Mutex<FileHistoryService>>) {
        self.writer.set_file_history(file_history);
    }

    /// Attach the host's session-scoped commit-attribution service.
    pub fn with_commit_attribution(
        mut self,
        service: Arc<std::sync::Mutex<CommitAttributionService>>,
    ) -> Self {
        self.writer = self.writer.with_commit_attribution(service);
        self
    }

    /// Replace the session's shared commit-attribution service after
    /// construction.
    pub fn set_commit_attribution(
        &mut self,
        service: Arc<std::sync::Mutex<CommitAttributionService>>,
    ) {
        self.writer.set_commit_attribution(service);
    }

    pub fn preview(&self, args: &Value) -> Result<WriteFilePreview, String> {
        let (_path, requested_path, content, _) = self.calculate(args)?;
        self.writer
            .preview_notebook(&json!({"file_path":requested_path,"content":content}))
    }

    pub fn execute(&self, args: &Value, approved: bool) -> Result<String, String> {
        if !approved {
            return Err("notebook_edit was not approved; no file was changed.".to_owned());
        }
        let (path, requested_path, content, requires_read_after_write) = self.calculate(args)?;
        self.writer.execute_notebook(
            &json!({"file_path":requested_path,"content":content}),
            true,
            requires_read_after_write,
        )?;
        let params = parse_params(args)?;
        let mode = params.edit_mode.as_deref().unwrap_or("replace");
        let cell = params.cell_id.as_deref().unwrap_or("beginning");
        let summary = if mode == "delete" {
            format!(
                "Notebook {} has been updated. delete cell {cell}.",
                path.display()
            )
        } else {
            format!(
                "Notebook {} has been updated. {mode} cell {cell}.\n\nUpdated source:\n\n---\n\n{}",
                path.display(),
                params.new_source.as_deref().unwrap_or_default()
            )
        };
        Ok(summary)
    }

    fn calculate(&self, args: &Value) -> Result<(PathBuf, PathBuf, String, bool), String> {
        let params = parse_params(args)?;
        if !params
            .notebook_path
            .to_ascii_lowercase()
            .ends_with(".ipynb")
        {
            return Err("File must be a Jupyter notebook (.ipynb).".to_owned());
        }
        let requested_path = PathBuf::from(&params.notebook_path);
        if !requested_path.is_absolute() {
            return Err(format!(
                "Notebook path must be absolute: {}",
                requested_path.display()
            ));
        }
        let path = std::fs::canonicalize(&requested_path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                format!("Notebook file not found: {}", requested_path.display())
            } else {
                format!("could not resolve notebook path: {error}")
            }
        })?;
        if !path.starts_with(&self.workspace_root) {
            return Err("notebook_edit is restricted to files inside the workspace".to_owned());
        }
        self.writer
            .ensure_notebook_prior_read(&path, &requested_path, false)?;
        let (raw, _metadata) = read_notebook(&path)?;
        self.writer
            .ensure_notebook_prior_read(&path, &requested_path, true)?;
        let result = apply_notebook_edit(&raw, &params)?;
        Ok((
            path,
            requested_path,
            result.updated_content,
            result.requires_read_after_write,
        ))
    }
}

fn parse_params(args: &Value) -> Result<NotebookEditParams, String> {
    let notebook_path = args
        .get("notebook_path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| "The 'notebook_path' parameter must be non-empty.".to_owned())?;
    let optional_string = |key: &str| -> Result<Option<String>, String> {
        match args.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err(format!("{key} must be a string.")),
        }
    };
    let params = NotebookEditParams {
        notebook_path: notebook_path.to_owned(),
        cell_id: optional_string("cell_id")?,
        new_source: optional_string("new_source")?,
        cell_type: optional_string("cell_type")?,
        edit_mode: optional_string("edit_mode")?,
    };
    if params
        .cell_type
        .as_deref()
        .is_some_and(|kind| !matches!(kind, "code" | "markdown"))
    {
        return Err("cell_type must be 'code' or 'markdown'.".to_owned());
    }
    if params
        .edit_mode
        .as_deref()
        .is_some_and(|mode| !matches!(mode, "replace" | "insert" | "delete"))
    {
        return Err("edit_mode must be 'replace', 'insert', or 'delete'.".to_owned());
    }
    let mode = params.edit_mode.as_deref().unwrap_or("replace");
    if mode != "insert" && params.cell_id.is_none() {
        return Err("cell_id is required for replace and delete operations.".to_owned());
    }
    if mode != "delete" && params.new_source.is_none() {
        return Err(format!(
            "new_source is required when edit_mode is \"{mode}\"."
        ));
    }
    Ok(params)
}

fn read_notebook(path: &Path) -> Result<(String, std::fs::Metadata), String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|error| format!("could not read notebook {}: {error}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("could not inspect notebook: {error}"))?;
    if !metadata.is_file() {
        return Err(format!(
            "Path is not a regular notebook file: {}",
            path.display()
        ));
    }
    if metadata.len() > MAX_NOTEBOOK_BYTES {
        return Err(format!(
            "Notebook exceeds the {MAX_NOTEBOOK_BYTES}-byte edit limit."
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_NOTEBOOK_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("could not read notebook: {error}"))?;
    if bytes.len() as u64 > MAX_NOTEBOOK_BYTES {
        return Err(format!(
            "Notebook exceeds the {MAX_NOTEBOOK_BYTES}-byte edit limit."
        ));
    }
    let raw = String::from_utf8(bytes)
        .map_err(|_| "Notebook file is not valid UTF-8 JSON.".to_owned())?;
    Ok((raw, metadata))
}

pub fn function_declaration() -> Value {
    json!({
        "name":"notebook_edit",
        "description":"Edit a Jupyter notebook (.ipynb) safely at the cell level. Existing notebooks must have been fully read first. The CLI shows the full diff and requires approval.",
        "parameters":{
            "type":"OBJECT",
            "properties":{
                "notebook_path":{"type":"STRING","description":"Absolute .ipynb path inside the current workspace."},
                "cell_id":{"type":"STRING","description":"Cell ID shown by read_file. Required except for insert."},
                "new_source":{"type":"STRING","description":"New cell source for replace and insert."},
                "cell_type":{"type":"STRING","enum":["code","markdown"]},
                "edit_mode":{"type":"STRING","enum":["replace","insert","delete"]}
            },
            "required":["notebook_path"]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::read_file::ReadFileTool;
    use uuid::Uuid;

    struct TempWorkspace(PathBuf);

    impl TempWorkspace {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("canopy-notebook-edit-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn full_read(cache: &FileReadCache, root: &Path, path: &Path) {
        ReadFileTool::new_with_cache(root, cache.clone())
            .unwrap()
            .execute(&json!({"file_path":path}))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn requires_a_full_structured_read_and_approval_before_cell_replace() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("demo.ipynb");
        std::fs::write(
            &path,
            r#"{"nbformat":4,"nbformat_minor":5,"metadata":{"language_info":{"name":"python"}},"cells":[{"cell_type":"code","id":"a","source":["print(1)\n"],"metadata":{},"outputs":[{"output_type":"stream","text":"1\n"}],"execution_count":1}]}"#,
        )
        .unwrap();
        let cache = FileReadCache::default();
        let tool = NotebookEditTool::new(&workspace.0, cache.clone()).unwrap();
        let args = json!({"notebook_path":path,"cell_id":"a","new_source":"print(2)"});
        assert_eq!(
            tool.preview(&args).unwrap_err(),
            format!(
                "Notebook {} has not been fully read in this session. Use the read_file tool first, without offset or limit, before editing cells.",
                path.display()
            )
        );

        full_read(&cache, &workspace.0, &path).await;
        let preview = tool.preview(&args).unwrap();
        assert!(preview.diff.contains("print(2)"));
        assert!(
            tool.execute(&args, false)
                .unwrap_err()
                .contains("not approved")
        );
        assert!(std::fs::read_to_string(&path).unwrap().contains("print(1)"));
        tool.execute(&args, true).unwrap();
        let updated: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(updated["cells"][0]["source"], json!(["print(2)"]));
        assert_eq!(updated["cells"][0]["outputs"], json!([]));
        assert_eq!(updated["cells"][0]["execution_count"], Value::Null);
        assert!(!cache.ensure_prior_read(&path, "editing").is_ok());
    }

    #[tokio::test]
    async fn inserts_and_deletes_cells_and_invalidates_when_fallback_ids_can_shift() {
        let workspace = TempWorkspace::new();
        let path = workspace.0.join("demo.ipynb");
        std::fs::write(
            &path,
            r#"{"nbformat":4,"nbformat_minor":5,"metadata":{},"cells":[{"cell_type":"markdown","source":["A"],"metadata":{}},{"cell_type":"markdown","source":["B"],"metadata":{}}]}"#,
        )
        .unwrap();
        let cache = FileReadCache::default();
        full_read(&cache, &workspace.0, &path).await;
        let tool = NotebookEditTool::new(&workspace.0, cache.clone()).unwrap();
        let insert = json!({"notebook_path":path,"edit_mode":"insert","new_source":"Intro"});
        tool.execute(&insert, true).unwrap();
        let notebook: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(notebook["cells"][0]["source"], json!(["Intro"]));
        assert!(matches!(
            cache.check(&std::fs::metadata(&path).unwrap()),
            crate::file_read_cache::FileReadCheckResult::Unknown
        ));
    }

    #[test]
    fn rejects_external_notebooks_and_malformed_edit_arguments() {
        let workspace = TempWorkspace::new();
        let outside = TempWorkspace::new();
        let path = outside.0.join("outside.ipynb");
        std::fs::write(&path, r#"{"cells":[],"metadata":{}}"#).unwrap();
        let tool = NotebookEditTool::new(&workspace.0, FileReadCache::default()).unwrap();
        assert!(
            tool.preview(&json!({"notebook_path":path,"edit_mode":"insert","new_source":"x"}))
                .unwrap_err()
                .contains("inside the workspace")
        );
        assert!(tool
            .preview(&json!({"notebook_path":workspace.0.join("file.txt"),"edit_mode":"insert","new_source":"x"}))
            .unwrap_err()
            .contains(".ipynb"));
    }
}
