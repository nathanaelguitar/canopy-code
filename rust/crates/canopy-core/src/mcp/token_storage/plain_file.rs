use super::{OAuthCredentials, TokenStorage, TokenStorageError};
use indexmap::IndexMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use uuid::Uuid;

const MAX_TOKEN_FILE_BYTES: u64 = 8 * 1024 * 1024;
static DID_WARN_PLAINTEXT: OnceLock<()> = OnceLock::new();

/// Plaintext MCP OAuth token-file backend used when encrypted storage is not
/// required. Files are written with owner-only permissions and atomic replace.
pub struct PlainFileTokenStorage {
    path: PathBuf,
}

impl PlainFileTokenStorage {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Save one token using Canopy's source-compatible credential envelope.
    pub async fn save_token(
        &self,
        server_name: impl Into<String>,
        token: super::OAuthToken,
        client_id: Option<String>,
        token_url: Option<String>,
        mcp_server_url: Option<String>,
    ) -> Result<(), TokenStorageError> {
        let credentials = OAuthCredentials {
            server_name: server_name.into(),
            token,
            client_id,
            token_url,
            mcp_server_url,
            updated_at: now_unix_millis(),
        };
        self.set_credentials(credentials).await
    }

    fn read_all(&self) -> Result<IndexMap<String, OAuthCredentials>, TokenStorageError> {
        let file = match open_read_no_follow(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(IndexMap::new()),
            Err(error) => return Err(TokenStorageError::Io(error)),
        };
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(TokenStorageError::InvalidCredentials(
                "Token storage path is not a regular file".to_owned(),
            ));
        }
        if metadata.len() > MAX_TOKEN_FILE_BYTES {
            return Err(TokenStorageError::InvalidCredentials(format!(
                "Token file exceeds the {MAX_TOKEN_FILE_BYTES}-byte read limit"
            )));
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_TOKEN_FILE_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_TOKEN_FILE_BYTES {
            return Err(TokenStorageError::InvalidCredentials(format!(
                "Token file exceeds the {MAX_TOKEN_FILE_BYTES}-byte read limit"
            )));
        }
        // Match the source store's best-effort recovery: malformed or unreadable
        // token JSON is treated as an empty store by getAllCredentials().
        let credentials: Vec<OAuthCredentials> = match serde_json::from_slice(&bytes) {
            Ok(credentials) => credentials,
            Err(_) => return Ok(IndexMap::new()),
        };
        let mut result = IndexMap::with_capacity(credentials.len());
        for credential in credentials {
            result.insert(credential.server_name.clone(), credential);
        }
        Ok(result)
    }

    fn persist(
        &self,
        credentials: &IndexMap<String, OAuthCredentials>,
    ) -> Result<(), TokenStorageError> {
        let values: Vec<&OAuthCredentials> = credentials.values().collect();
        let bytes = serde_json::to_vec_pretty(&values)?;
        if bytes.len() as u64 > MAX_TOKEN_FILE_BYTES {
            return Err(TokenStorageError::InvalidCredentials(format!(
                "Token file exceeds the {MAX_TOKEN_FILE_BYTES}-byte write limit"
            )));
        }
        atomic_write_owner_only(&self.path, &bytes)?;
        warn_plaintext_once(&self.path);
        Ok(())
    }
}

impl TokenStorage for PlainFileTokenStorage {
    async fn get_credentials(
        &self,
        server_name: &str,
    ) -> Result<Option<OAuthCredentials>, TokenStorageError> {
        Ok(self.read_all()?.shift_remove(server_name))
    }

    async fn set_credentials(
        &self,
        credentials: OAuthCredentials,
    ) -> Result<(), TokenStorageError> {
        let mut all = self.read_all()?;
        all.insert(credentials.server_name.clone(), credentials);
        self.persist(&all)
    }

    async fn delete_credentials(&self, server_name: &str) -> Result<(), TokenStorageError> {
        let mut all = self.read_all()?;
        if all.shift_remove(server_name).is_none() {
            return Ok(());
        }
        if all.is_empty() {
            match fs::remove_file(&self.path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(TokenStorageError::Io(error)),
            }
        } else {
            self.persist(&all)
        }
    }

    async fn list_servers(&self) -> Result<Vec<String>, TokenStorageError> {
        Ok(self.read_all()?.into_keys().collect())
    }

    async fn get_all_credentials(
        &self,
    ) -> Result<IndexMap<String, OAuthCredentials>, TokenStorageError> {
        self.read_all()
    }

    async fn clear_all(&self) -> Result<(), TokenStorageError> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(TokenStorageError::Io(error)),
        }
    }
}

fn now_unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

pub(super) fn open_read_no_follow(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options.open(path)
}

pub(super) fn atomic_write_owner_only(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "token path has no filename"))?;
    let mut temporary = None;
    for _ in 0..8 {
        let candidate = parent.join(format!(
            ".{}.tmp-{}",
            file_name.to_string_lossy(),
            Uuid::new_v4().simple()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        match options.open(&candidate) {
            Ok(file) => {
                temporary = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    let (temporary_path, mut file) = temporary.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a token-file temporary file",
        )
    })?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary_path, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

fn warn_plaintext_once(path: &Path) {
    if DID_WARN_PLAINTEXT.set(()).is_ok() {
        eprintln!(
            "Warning: MCP OAuth tokens are stored unencrypted at {}. Set CANOPY_CODE_FORCE_ENCRYPTED_FILE_STORAGE=true to require encrypted file storage.",
            path.display()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::PlainFileTokenStorage;
    use crate::mcp::token_storage::{OAuthCredentials, OAuthToken, TokenStorage};
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    struct TempDirectory(PathBuf);

    impl TempDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("canopy-mcp-token-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn credential(name: &str) -> OAuthCredentials {
        OAuthCredentials {
            server_name: name.to_owned(),
            token: OAuthToken {
                access_token: "access-token".to_owned(),
                refresh_token: Some("refresh-token".to_owned()),
                expires_at: Some(4_000_000_000_000),
                token_type: "Bearer".to_owned(),
                scope: Some("read write".to_owned()),
            },
            client_id: Some("client".to_owned()),
            token_url: Some("https://example.com/token".to_owned()),
            mcp_server_url: Some("https://example.com/mcp".to_owned()),
            updated_at: 123,
        }
    }

    #[tokio::test]
    async fn reads_and_writes_the_source_json_array_contract() {
        let directory = TempDirectory::new();
        let path = directory.0.join("mcp-oauth-tokens.json");
        let storage = PlainFileTokenStorage::new(&path);
        storage.set_credentials(credential("one")).await.unwrap();

        let saved: Vec<OAuthCredentials> =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].server_name, "one");
        assert_eq!(
            storage.get_credentials("one").await.unwrap(),
            Some(saved[0].clone())
        );
        assert_eq!(storage.list_servers().await.unwrap(), ["one"]);
        assert_eq!(
            storage.get_all_credentials().await.unwrap().get("one"),
            Some(&saved[0])
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[tokio::test]
    async fn deletes_last_item_and_clears_missing_files_idempotently() {
        let directory = TempDirectory::new();
        let path = directory.0.join("mcp-oauth-tokens.json");
        let storage = PlainFileTokenStorage::new(&path);
        storage.set_credentials(credential("one")).await.unwrap();
        storage.delete_credentials("one").await.unwrap();
        assert!(!path.exists());
        storage.delete_credentials("missing").await.unwrap();
        storage.clear_all().await.unwrap();
    }

    #[tokio::test]
    async fn reads_existing_credentials_without_rewriting_them() {
        let directory = TempDirectory::new();
        let path = directory.0.join("mcp-oauth-tokens.json");
        fs::write(
            &path,
            serde_json::to_vec(&vec![credential("legacy")]).unwrap(),
        )
        .unwrap();
        let storage = PlainFileTokenStorage::new(&path);
        assert_eq!(
            storage.get_credentials("legacy").await.unwrap(),
            Some(credential("legacy"))
        );
    }
}
