use super::{
    EncryptedFileTokenStorage, OAuthCredentials, PlainFileTokenStorage, SecretStorage,
    TokenStorage, TokenStorageError,
};
use indexmap::IndexMap;
use std::path::PathBuf;
use tokio::sync::OnceCell;

pub const FORCE_ENCRYPTED_FILE_ENV_VAR: &str = "CANOPY_CODE_FORCE_ENCRYPTED_FILE_STORAGE";
const FORCE_FILE_STORAGE_ENV_VAR: &str = "CANOPY_CODE_FORCE_FILE_STORAGE";
const DEFAULT_SERVICE_NAME: &str = "canopy-code-oauth";

enum Backend {
    Plain(PlainFileTokenStorage),
    Encrypted(EncryptedFileTokenStorage),
    #[cfg(target_os = "macos")]
    Keychain(super::KeychainTokenStorage),
}

/// Source-compatible MCP OAuth backend selection. The legacy plaintext token
/// list stays the default. Encrypted mode uses the macOS Keychain when usable,
/// then falls back to the encrypted file backend; `CANOPY_CODE_FORCE_FILE_STORAGE`
/// skips the Keychain while retaining encrypted file storage.
pub struct ConfiguredTokenStorage {
    backend: OnceCell<Backend>,
    plain_token_file: PathBuf,
    global_config_dir: PathBuf,
    use_encrypted_storage: bool,
}

impl ConfiguredTokenStorage {
    pub fn new(
        plain_token_file: impl Into<PathBuf>,
        global_config_dir: impl Into<PathBuf>,
    ) -> Self {
        let use_encrypted_storage =
            std::env::var(FORCE_ENCRYPTED_FILE_ENV_VAR).is_ok_and(|value| value == "true");
        Self {
            backend: OnceCell::new(),
            plain_token_file: plain_token_file.into(),
            global_config_dir: global_config_dir.into(),
            use_encrypted_storage,
        }
    }

    /// Reports whether the caller requested the protected keychain/encrypted
    /// storage mode. The concrete backend is selected lazily on first use.
    pub fn requests_encrypted_storage(&self) -> bool {
        self.use_encrypted_storage
    }

    async fn selected_backend(&self) -> &Backend {
        self.backend
            .get_or_init(|| async {
                if !self.use_encrypted_storage {
                    return Backend::Plain(PlainFileTokenStorage::new(
                        self.plain_token_file.clone(),
                    ));
                }

                let force_file_storage =
                    std::env::var(FORCE_FILE_STORAGE_ENV_VAR).is_ok_and(|value| value == "true");
                #[cfg(target_os = "macos")]
                if !force_file_storage {
                    let keychain = super::KeychainTokenStorage::new(DEFAULT_SERVICE_NAME);
                    if keychain.is_available().await.unwrap_or(false) {
                        return Backend::Keychain(keychain);
                    }
                }
                #[cfg(not(target_os = "macos"))]
                let _ = force_file_storage;

                Backend::Encrypted(EncryptedFileTokenStorage::new(
                    self.global_config_dir.clone(),
                    DEFAULT_SERVICE_NAME,
                ))
            })
            .await
    }
}

impl TokenStorage for ConfiguredTokenStorage {
    async fn get_credentials(
        &self,
        server_name: &str,
    ) -> Result<Option<OAuthCredentials>, TokenStorageError> {
        match self.selected_backend().await {
            Backend::Plain(storage) => storage.get_credentials(server_name).await,
            Backend::Encrypted(storage) => storage.get_credentials(server_name).await,
            #[cfg(target_os = "macos")]
            Backend::Keychain(storage) => storage.get_credentials(server_name).await,
        }
    }

    async fn set_credentials(
        &self,
        credentials: OAuthCredentials,
    ) -> Result<(), TokenStorageError> {
        match self.selected_backend().await {
            Backend::Plain(storage) => storage.set_credentials(credentials).await,
            Backend::Encrypted(storage) => storage.set_credentials(credentials).await,
            #[cfg(target_os = "macos")]
            Backend::Keychain(storage) => storage.set_credentials(credentials).await,
        }
    }

    async fn delete_credentials(&self, server_name: &str) -> Result<(), TokenStorageError> {
        match self.selected_backend().await {
            Backend::Plain(storage) => storage.delete_credentials(server_name).await,
            Backend::Encrypted(storage) => storage.delete_credentials(server_name).await,
            #[cfg(target_os = "macos")]
            Backend::Keychain(storage) => storage.delete_credentials(server_name).await,
        }
    }

    async fn list_servers(&self) -> Result<Vec<String>, TokenStorageError> {
        match self.selected_backend().await {
            Backend::Plain(storage) => storage.list_servers().await,
            Backend::Encrypted(storage) => storage.list_servers().await,
            #[cfg(target_os = "macos")]
            Backend::Keychain(storage) => storage.list_servers().await,
        }
    }

    async fn get_all_credentials(
        &self,
    ) -> Result<IndexMap<String, OAuthCredentials>, TokenStorageError> {
        match self.selected_backend().await {
            Backend::Plain(storage) => storage.get_all_credentials().await,
            Backend::Encrypted(storage) => storage.get_all_credentials().await,
            #[cfg(target_os = "macos")]
            Backend::Keychain(storage) => storage.get_all_credentials().await,
        }
    }

    async fn clear_all(&self) -> Result<(), TokenStorageError> {
        match self.selected_backend().await {
            Backend::Plain(storage) => storage.clear_all().await,
            Backend::Encrypted(storage) => storage.clear_all().await,
            #[cfg(target_os = "macos")]
            Backend::Keychain(storage) => storage.clear_all().await,
        }
    }
}

impl SecretStorage for ConfiguredTokenStorage {
    async fn is_available(&self) -> Result<bool, TokenStorageError> {
        match self.selected_backend().await {
            Backend::Plain(_) => Ok(false),
            Backend::Encrypted(storage) => storage.is_available().await,
            #[cfg(target_os = "macos")]
            Backend::Keychain(storage) => storage.is_available().await,
        }
    }

    async fn set_secret(&self, key: &str, value: &str) -> Result<(), TokenStorageError> {
        match self.selected_backend().await {
            Backend::Plain(_) => Err(TokenStorageError::Unavailable),
            Backend::Encrypted(storage) => storage.set_secret(key, value).await,
            #[cfg(target_os = "macos")]
            Backend::Keychain(storage) => storage.set_secret(key, value).await,
        }
    }

    async fn get_secret(&self, key: &str) -> Result<Option<String>, TokenStorageError> {
        match self.selected_backend().await {
            Backend::Plain(_) => Err(TokenStorageError::Unavailable),
            Backend::Encrypted(storage) => storage.get_secret(key).await,
            #[cfg(target_os = "macos")]
            Backend::Keychain(storage) => storage.get_secret(key).await,
        }
    }

    async fn delete_secret(&self, key: &str) -> Result<(), TokenStorageError> {
        match self.selected_backend().await {
            Backend::Plain(_) => Err(TokenStorageError::Unavailable),
            Backend::Encrypted(storage) => storage.delete_secret(key).await,
            #[cfg(target_os = "macos")]
            Backend::Keychain(storage) => storage.delete_secret(key).await,
        }
    }

    async fn list_secrets(&self) -> Result<Vec<String>, TokenStorageError> {
        match self.selected_backend().await {
            Backend::Plain(_) => Err(TokenStorageError::Unavailable),
            Backend::Encrypted(storage) => storage.list_secrets().await,
            #[cfg(target_os = "macos")]
            Backend::Keychain(storage) => storage.list_secrets().await,
        }
    }
}
