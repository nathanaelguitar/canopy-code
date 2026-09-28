use super::plain_file::{atomic_write_owner_only, open_read_no_follow};
use super::{BaseTokenStorage, OAuthCredentials, SecretStorage, TokenStorage, TokenStorageError};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::aes::{Aes256, cipher::consts::U16};
use aes_gcm::{AesGcm, Nonce};
use indexmap::IndexMap;
use scrypt::{Params, scrypt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use uuid::Uuid;

const MAX_ENCRYPTED_FILE_BYTES: u64 = 8 * 1024 * 1024;
const ENCRYPTION_PASSWORD: &[u8] = b"canopy-code-oauth";
const ENCRYPTION_SALT_SUFFIX: &str = "canopy-code";
const KEY_LENGTH: usize = 32;
const NONCE_LENGTH: usize = 16;
const TAG_LENGTH: usize = 16;
const TOKEN_FILE_NAME: &str = "mcp-oauth-tokens-v2.json";
const SECRET_FILE_NAME: &str = "extension-secrets-v1.json";

type NodeCompatibleAes256Gcm = AesGcm<Aes256, U16>;
type SecretFileContents = IndexMap<String, IndexMap<String, String>>;

/// AES-256-GCM file backend compatible with the TypeScript
/// `FileTokenStorage` format. The 16-byte nonce and `iv:tag:ciphertext` hex
/// envelope intentionally match the existing Node writer.
pub struct EncryptedFileTokenStorage {
    token_file_path: PathBuf,
    secret_file_path: PathBuf,
    service_name: String,
    encryption_key: OnceLock<Result<[u8; KEY_LENGTH], String>>,
}

impl EncryptedFileTokenStorage {
    pub fn new(config_dir: impl Into<PathBuf>, service_name: impl Into<String>) -> Self {
        let config_dir = config_dir.into();
        Self {
            token_file_path: config_dir.join(TOKEN_FILE_NAME),
            secret_file_path: config_dir.join(SECRET_FILE_NAME),
            service_name: service_name.into(),
            encryption_key: OnceLock::new(),
        }
    }

    pub fn token_file_path(&self) -> &Path {
        &self.token_file_path
    }

    pub fn secret_file_path(&self) -> &Path {
        &self.secret_file_path
    }

    fn encryption_key(&self) -> Result<&[u8; KEY_LENGTH], TokenStorageError> {
        self.encryption_key
            .get_or_init(derive_encryption_key)
            .as_ref()
            .map_err(|error| {
                TokenStorageError::InvalidCredentials(format!(
                    "could not derive encrypted token storage key: {error}"
                ))
            })
    }

    fn read_tokens(
        &self,
        allow_missing: bool,
    ) -> Result<IndexMap<String, OAuthCredentials>, TokenStorageError> {
        match read_encrypted_json(&self.token_file_path, self.encryption_key()?, allow_missing)? {
            Some(tokens) => Ok(tokens),
            None if allow_missing => Ok(IndexMap::new()),
            None => Err(TokenStorageError::InvalidCredentials(
                "Token file does not exist".to_owned(),
            )),
        }
    }

    fn save_tokens(
        &self,
        tokens: &IndexMap<String, OAuthCredentials>,
    ) -> Result<(), TokenStorageError> {
        write_encrypted_json(&self.token_file_path, tokens, self.encryption_key()?, true)
    }

    fn read_secrets(&self) -> Result<SecretFileContents, TokenStorageError> {
        Ok(
            read_encrypted_json(&self.secret_file_path, self.encryption_key()?, true)?
                .unwrap_or_default(),
        )
    }

    fn save_secrets(&self, secrets: &SecretFileContents) -> Result<(), TokenStorageError> {
        write_encrypted_json(
            &self.secret_file_path,
            secrets,
            self.encryption_key()?,
            false,
        )
    }
}

impl TokenStorage for EncryptedFileTokenStorage {
    async fn get_credentials(
        &self,
        server_name: &str,
    ) -> Result<Option<OAuthCredentials>, TokenStorageError> {
        let credentials = self.read_tokens(false)?.shift_remove(server_name);
        Ok(credentials.filter(|credentials| !BaseTokenStorage::is_token_expired(credentials)))
    }

    async fn set_credentials(
        &self,
        mut credentials: OAuthCredentials,
    ) -> Result<(), TokenStorageError> {
        BaseTokenStorage::validate_credentials(&credentials)?;
        let mut tokens = self.read_tokens(true)?;
        credentials.updated_at = now_unix_millis();
        tokens.insert(credentials.server_name.clone(), credentials);
        self.save_tokens(&tokens)
    }

    async fn delete_credentials(&self, server_name: &str) -> Result<(), TokenStorageError> {
        let mut tokens = self.read_tokens(false)?;
        if tokens.shift_remove(server_name).is_none() {
            return Err(TokenStorageError::CredentialsNotFound(
                server_name.to_owned(),
            ));
        }
        if tokens.is_empty() {
            remove_if_present(&self.token_file_path)?;
            Ok(())
        } else {
            self.save_tokens(&tokens)
        }
    }

    async fn list_servers(&self) -> Result<Vec<String>, TokenStorageError> {
        Ok(self.read_tokens(false)?.into_keys().collect())
    }

    async fn get_all_credentials(
        &self,
    ) -> Result<IndexMap<String, OAuthCredentials>, TokenStorageError> {
        Ok(self
            .read_tokens(false)?
            .into_iter()
            .filter(|(_, credentials)| !BaseTokenStorage::is_token_expired(credentials))
            .collect())
    }

    async fn clear_all(&self) -> Result<(), TokenStorageError> {
        remove_if_present(&self.token_file_path)
    }
}

impl SecretStorage for EncryptedFileTokenStorage {
    async fn is_available(&self) -> Result<bool, TokenStorageError> {
        Ok(true)
    }

    async fn set_secret(&self, key: &str, value: &str) -> Result<(), TokenStorageError> {
        let mut secrets = self.read_secrets()?;
        secrets
            .entry(self.service_name.clone())
            .or_default()
            .insert(key.to_owned(), value.to_owned());
        self.save_secrets(&secrets)
    }

    async fn get_secret(&self, key: &str) -> Result<Option<String>, TokenStorageError> {
        Ok(self
            .read_secrets()?
            .get(&self.service_name)
            .and_then(|secrets| secrets.get(key))
            .cloned())
    }

    async fn delete_secret(&self, key: &str) -> Result<(), TokenStorageError> {
        let mut secrets = self.read_secrets()?;
        if let Some(service_secrets) = secrets.get_mut(&self.service_name) {
            service_secrets.shift_remove(key);
            if service_secrets.is_empty() {
                secrets.shift_remove(&self.service_name);
            }
            self.save_secrets(&secrets)?;
        }
        Ok(())
    }

    async fn list_secrets(&self) -> Result<Vec<String>, TokenStorageError> {
        Ok(self
            .read_secrets()?
            .get(&self.service_name)
            .map(|secrets| secrets.keys().cloned().collect())
            .unwrap_or_default())
    }
}

fn read_encrypted_json<T: DeserializeOwned>(
    path: &Path,
    key: &[u8; KEY_LENGTH],
    allow_missing: bool,
) -> Result<Option<T>, TokenStorageError> {
    let file = match open_read_no_follow(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound && allow_missing => return Ok(None),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(TokenStorageError::InvalidCredentials(
                "Token file does not exist".to_owned(),
            ));
        }
        Err(error) => return Err(TokenStorageError::Io(error)),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(TokenStorageError::InvalidCredentials(
            "Encrypted token storage path is not a regular file".to_owned(),
        ));
    }
    if metadata.len() > MAX_ENCRYPTED_FILE_BYTES {
        return Err(TokenStorageError::InvalidCredentials(format!(
            "Encrypted token file exceeds the {MAX_ENCRYPTED_FILE_BYTES}-byte read limit"
        )));
    }
    let mut encrypted = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_ENCRYPTED_FILE_BYTES + 1)
        .read_to_end(&mut encrypted)?;
    if encrypted.len() as u64 > MAX_ENCRYPTED_FILE_BYTES {
        return Err(TokenStorageError::InvalidCredentials(format!(
            "Encrypted token file exceeds the {MAX_ENCRYPTED_FILE_BYTES}-byte read limit"
        )));
    }
    let encrypted = std::str::from_utf8(&encrypted).map_err(|_| corrupted_file())?;
    let plaintext = decrypt(key, encrypted)?;
    serde_json::from_slice(&plaintext)
        .map(Some)
        .map_err(TokenStorageError::from)
}

fn write_encrypted_json<T: Serialize>(
    path: &Path,
    value: &T,
    key: &[u8; KEY_LENGTH],
    pretty: bool,
) -> Result<(), TokenStorageError> {
    let json = if pretty {
        serde_json::to_vec_pretty(value)?
    } else {
        serde_json::to_vec(value)?
    };
    let encrypted = encrypt(key, &json)?;
    if encrypted.len() as u64 > MAX_ENCRYPTED_FILE_BYTES {
        return Err(TokenStorageError::InvalidCredentials(format!(
            "Encrypted token file exceeds the {MAX_ENCRYPTED_FILE_BYTES}-byte write limit"
        )));
    }
    ensure_private_parent(path)?;
    atomic_write_owner_only(path, encrypted.as_bytes())?;
    Ok(())
}

fn encrypt(key: &[u8; KEY_LENGTH], plaintext: &[u8]) -> Result<String, TokenStorageError> {
    let nonce_bytes = *Uuid::new_v4().as_bytes();
    let nonce = Nonce::<U16>::from(nonce_bytes);
    let cipher = NodeCompatibleAes256Gcm::new_from_slice(key).map_err(|_| {
        TokenStorageError::InvalidCredentials("invalid encrypted token storage key".to_owned())
    })?;
    let ciphertext_and_tag = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|_| TokenStorageError::InvalidCredentials("token encryption failed".to_owned()))?;
    let split = ciphertext_and_tag.len().saturating_sub(TAG_LENGTH);
    if split + TAG_LENGTH != ciphertext_and_tag.len() {
        return Err(TokenStorageError::InvalidCredentials(
            "token encryption returned an invalid authentication tag".to_owned(),
        ));
    }
    Ok(format!(
        "{}:{}:{}",
        encode_hex(&nonce_bytes),
        encode_hex(&ciphertext_and_tag[split..]),
        encode_hex(&ciphertext_and_tag[..split]),
    ))
}

fn decrypt(key: &[u8; KEY_LENGTH], encrypted: &str) -> Result<Vec<u8>, TokenStorageError> {
    let mut parts = encrypted.split(':');
    let Some(nonce_hex) = parts.next() else {
        return Err(corrupted_file());
    };
    let Some(tag_hex) = parts.next() else {
        return Err(corrupted_file());
    };
    let Some(ciphertext_hex) = parts.next() else {
        return Err(corrupted_file());
    };
    if parts.next().is_some() {
        return Err(corrupted_file());
    }
    let nonce_bytes = decode_hex(nonce_hex).ok_or_else(corrupted_file)?;
    let tag = decode_hex(tag_hex).ok_or_else(corrupted_file)?;
    let mut ciphertext = decode_hex(ciphertext_hex).ok_or_else(corrupted_file)?;
    if nonce_bytes.len() != NONCE_LENGTH || tag.len() != TAG_LENGTH {
        return Err(corrupted_file());
    }
    ciphertext.extend_from_slice(&tag);
    let nonce_array: [u8; NONCE_LENGTH] = nonce_bytes.try_into().map_err(|_| corrupted_file())?;
    let nonce = Nonce::<U16>::from(nonce_array);
    let cipher = NodeCompatibleAes256Gcm::new_from_slice(key).map_err(|_| corrupted_file())?;
    cipher
        .decrypt(&nonce, ciphertext.as_ref())
        .map_err(|_| corrupted_file())
}

fn corrupted_file() -> TokenStorageError {
    TokenStorageError::InvalidCredentials("Token file corrupted".to_owned())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if value.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        bytes.push((high << 4) | low);
    }
    Some(bytes)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn derive_encryption_key() -> Result<[u8; KEY_LENGTH], String> {
    let hostname = system_hostname().map_err(|error| error.to_string())?;
    let username = system_username().map_err(|error| error.to_string())?;
    let salt = format!("{hostname}-{username}-{ENCRYPTION_SALT_SUFFIX}");
    // Node's crypto.scryptSync uses N=16384, r=8, p=1 when no options are
    // supplied. Retaining those parameters keeps this file interoperable.
    let params = Params::new(14, 8, 1).map_err(|error| error.to_string())?;
    let mut key = [0_u8; KEY_LENGTH];
    scrypt(ENCRYPTION_PASSWORD, salt.as_bytes(), &params, &mut key)
        .map_err(|error| error.to_string())?;
    Ok(key)
}

#[cfg(unix)]
fn system_hostname() -> io::Result<String> {
    nix::unistd::gethostname()
        .map(|hostname| hostname.to_string_lossy().into_owned())
        .map_err(|error| io::Error::other(error.to_string()))
}

#[cfg(windows)]
fn system_hostname() -> io::Result<String> {
    std::env::var("COMPUTERNAME").map_err(|error| io::Error::other(error.to_string()))
}

#[cfg(not(any(unix, windows)))]
fn system_hostname() -> io::Result<String> {
    std::env::var("HOSTNAME").map_err(|error| io::Error::other(error.to_string()))
}

#[cfg(all(unix, not(target_os = "redox")))]
fn system_username() -> io::Result<String> {
    nix::unistd::User::from_uid(nix::unistd::Uid::effective())
        .map_err(|error| io::Error::other(error.to_string()))?
        .map(|user| user.name)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "current user is absent from the password database",
            )
        })
}

#[cfg(windows)]
fn system_username() -> io::Result<String> {
    std::env::var("USERNAME").map_err(|error| io::Error::other(error.to_string()))
}

#[cfg(any(target_os = "redox", not(any(unix, windows))))]
fn system_username() -> io::Result<String> {
    std::env::var("USER").map_err(|error| io::Error::other(error.to_string()))
}

fn ensure_private_parent(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(parent) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if parent.is_dir() {
                Ok(())
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

fn remove_if_present(path: &Path) -> Result<(), TokenStorageError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(TokenStorageError::Io(error)),
    }
}

fn now_unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
